/**
 * @file test_shm_operations.c
 * @brief Testes de dados SHM com recursos reais, concorrência e falhas controladas.
 * @ingroup tests
 * @details As primitivas de semáforos são reais, exceto nas chamadas selecionadas
 * para falhar. Esperas usam alarm(), sem polling ou atrasos para sincronização.
 * Cada processo filho fecha apenas seu estado local; o pai remove seus recursos.
 */
#include <errno.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/sem.h>
#include <sys/shm.h>
#include <sys/wait.h>
#include <unistd.h>

#include "mpmc/config.h"
#include "mpmc/ipc/sem.h"
#include "shm_backend_internal.h"

/** @brief Quantidade de processos de cada papel no cenário concorrente. */
#define WORKERS_PER_ROLE 2
/** @brief Mensagens por produtor, suficientes para exercer backpressure. */
#define MESSAGES_PER_WORKER 64

/** @brief Contadores de verificações e falhas da suíte no processo pai. */
static unsigned checks, failures;
/** @brief Backend pertencente ao pai, registrado para cleanup inclusive em falhas. */
static shm_backend_t *owned_backend;
/** @brief Filhos próprios ainda não recolhidos; zero indica posição livre. */
static pid_t children[2 * WORKERS_PER_ROLE];
/** @brief Indica expiração do prazo sem executar cleanup no tratador de sinal. */
static volatile sig_atomic_t alarm_seen;
/** @brief Ações e máscara originais, restauradas após o cleanup da suíte. */
static struct sigaction previous_alarm_action, previous_chld_action;
static sigset_t previous_signal_mask;
/** @brief Indica que a configuração de sinais foi concluída e deve ser restaurada. */
static bool signals_configured;
/** @brief Contadores e chamadas escolhidas para falhar; zero desabilita a injeção. */
static unsigned apply_calls, change_calls, fail_apply_call, fail_change_call;
/** @brief Categoria e causa devolvidas pela chamada escolhida. */
static mpmc_result_t injected_error;

/** @brief Registra uma propriedade sem depender de assert(), inclusive em release. */
/** @brief Volume deste cenário; não é um limite da aplicação. */
static constexpr size_t TEST_STRUCTURE_COUNT = 1024;

#define CHECK(condition)                                                                           \
    do {                                                                                           \
        ++checks;                                                                                  \
        if (!(condition)) {                                                                        \
            ++failures;                                                                            \
            fprintf(stderr, "FALHA %s:%d: %s\n", __FILE__, __LINE__, #condition);                  \
        }                                                                                          \
    } while (0)

/** @brief Avalia uma operação uma vez e confere categoria e causa preservada. */
#define CHECK_RESULT(call, expected_status, expected_errno)                                        \
    do {                                                                                           \
        mpmc_result_t result_ = (call);                                                            \
        CHECK(result_.status == (expected_status));                                                \
        CHECK(result_.system_errno == (expected_errno));                                           \
    } while (0)

static void cleanup(void);
static void finish(void);
static bool configure_signals(void);
static bool create_backend(void);
static void destroy_backend(void);
static message_t sample_message(unsigned sequence, uint64_t operation);
static bool same_message(const message_t *left, const message_t *right);
static void on_alarm(int signal_number);
static void on_child(int signal_number);
static void arm_timeout(unsigned seconds);
static bool read_message(int fd, message_t *message);
static void run_child(unsigned role, int read_fd, int write_fd);
static bool spawn_blocked(
    unsigned slot,
    bool sending,
    size_t structure,
    const message_t *message,
    int ready[2],
    int result[2]
);
static bool read_ready(int fd);
static void collect_children(void);

/**
 * @brief Delega a semop real, exceto na chamada escolhida para falhar.
 * @param semid Conjunto passado à primitiva real.
 * @param operations Vetor passado sem alterações à primitiva.
 * @param count Quantidade de operações.
 * @return Estrutura mpmc_result_t com status/errno injetados (MPMC_ERR_SYS/EACCES,
 * MPMC_ERR_SHUTDOWN/EIDRM ou MPMC_ERR_INTERRUPTED/EINTR), ou resultado integral de
 * sysv_sem_apply(), incluindo MPMC_OK com system_errno zero e suas categorias de erro.
 */
mpmc_result_t mpmc_test_sem_apply(int semid, struct sembuf *operations, size_t count) {
    if (++apply_calls == fail_apply_call) return injected_error;
    return sysv_sem_apply(semid, operations, count);
}

/**
 * @brief Intercepta somente a chamada escolhida de aquisição/liberação do contador.
 * @param semid Conjunto real.
 * @param index Índice passado à primitiva.
 * @param delta Alteração solicitada.
 * @param nowait Política de espera passada sem alterações.
 * @return Estrutura mpmc_result_t com a mesma política de injeção de
 * mpmc_test_sem_apply(); caso contrário, propaga status e system_errno de sysv_sem_change().
 */
mpmc_result_t mpmc_test_sem_change(int semid, unsigned index, short delta, bool nowait) {
    if (++change_calls == fail_change_call) return injected_error;
    return sysv_sem_change(semid, index, delta, nowait);
}

/** @brief Confere saídas preservadas em argumentos inválidos e recepção sem itens. */
static void test_arguments_and_empty(void) {
    if (!create_backend()) return;
    message_t message = sample_message(7, 42), out = message;
    uint64_t count = UINT64_MAX;
    CHECK_RESULT(shm_backend_send(nullptr, 1, &message), MPMC_ERR_INVALID_ARG, 0);
    CHECK_RESULT(shm_backend_send(owned_backend, 1, nullptr), MPMC_ERR_INVALID_ARG, 0);
    CHECK_RESULT(shm_backend_receive(nullptr, 1, &out, false), MPMC_ERR_INVALID_ARG, 0);
    CHECK_RESULT(shm_backend_receive(owned_backend, 1, nullptr, false), MPMC_ERR_INVALID_ARG, 0);
    CHECK_RESULT(shm_backend_consumed_count(nullptr, 1, &count), MPMC_ERR_INVALID_ARG, 0);
    CHECK_RESULT(shm_backend_consumed_count(owned_backend, 1, nullptr), MPMC_ERR_INVALID_ARG, 0);
    const size_t invalid_ids[] = {0, 3, SIZE_MAX};
    for (size_t i = 0; i < sizeof invalid_ids / sizeof invalid_ids[0]; ++i) {
        CHECK_RESULT(
            shm_backend_send(owned_backend, invalid_ids[i], &message), MPMC_ERR_INVALID_INDEX, 0
        );
        CHECK_RESULT(
            shm_backend_receive(owned_backend, invalid_ids[i], &out, false),
            MPMC_ERR_INVALID_INDEX,
            0
        );
        CHECK_RESULT(
            shm_backend_consumed_count(owned_backend, invalid_ids[i], &count),
            MPMC_ERR_INVALID_INDEX,
            0
        );
    }
    CHECK(same_message(&message, &out) && count == UINT64_MAX);
    CHECK_RESULT(shm_backend_receive(owned_backend, 1, &out, false), MPMC_ERR_WOULD_BLOCK, EAGAIN);
    CHECK(same_message(&message, &out));
    CHECK_RESULT(shm_backend_consumed_count(owned_backend, 1, &count), MPMC_OK, 0);
    CHECK(count == 0);
    CHECK(owned_backend->segments[0].ring->count == 0);
    destroy_backend();
}

/** @brief Confere FIFO após volta dos índices, metadados, isolamento e totais independentes. */
static void test_fifo_and_counts(void) {
    if (!create_backend()) return;
    unsigned capacity = owned_backend->segments[0].ring->capacity;
    for (unsigned i = 0; i < capacity; ++i) {
        message_t message = sample_message(i + 1, 11);
        CHECK_RESULT(shm_backend_send(owned_backend, 1, &message), MPMC_OK, 0);
    }
    message_t isolated = sample_message(99, 22);
    CHECK_RESULT(shm_backend_send(owned_backend, 2, &isolated), MPMC_OK, 0);
    unsigned removed = capacity / 2;
    for (unsigned i = 0; i < removed; ++i) {
        message_t out, expected = sample_message(i + 1, 11);
        CHECK_RESULT(shm_backend_receive(owned_backend, 1, &out, false), MPMC_OK, 0);
        CHECK(same_message(&out, &expected));
    }
    for (unsigned i = 0; i < removed; ++i) {
        message_t message = sample_message(capacity + i + 1, 11);
        CHECK_RESULT(shm_backend_send(owned_backend, 1, &message), MPMC_OK, 0);
    }
    for (unsigned i = removed; i < capacity + removed; ++i) {
        message_t out, expected = sample_message(i + 1, 11);
        CHECK_RESULT(shm_backend_receive(owned_backend, 1, &out, false), MPMC_OK, 0);
        CHECK(same_message(&out, &expected));
    }
    uint64_t count = 0;
    CHECK_RESULT(shm_backend_consumed_count(owned_backend, 1, &count), MPMC_OK, 0);
    CHECK(count == capacity + removed);
    CHECK_RESULT(shm_backend_consumed_count(owned_backend, 2, &count), MPMC_OK, 0);
    CHECK(count == 0);
    message_t out;
    CHECK_RESULT(shm_backend_receive(owned_backend, 2, &out, false), MPMC_OK, 0);
    CHECK(same_message(&out, &isolated));
    CHECK_RESULT(shm_backend_consumed_count(owned_backend, 2, &count), MPMC_OK, 0);
    CHECK(count == 1);
    int semid = owned_backend->segments[0].semid;
    CHECK(semctl(semid, SHM_SEM_MUTEX, GETVAL) == 1);
    CHECK(semctl(semid, SHM_SEM_EMPTY, GETVAL) == (int)capacity);
    CHECK(semctl(semid, SHM_SEM_FULL, GETVAL) == 0);
    destroy_backend();
}

/** @brief Interrompe esperas reais e confere que aquisições incompletas não mantêm reservas. */
static void test_interruption_and_mutex(void) {
    if (!create_backend()) return;
    message_t sentinel = sample_message(9, 44), out = sentinel;
    int semid = owned_backend->segments[0].semid;
    unsigned capacity = owned_backend->segments[0].ring->capacity;
    arm_timeout(1);
    CHECK_RESULT(shm_backend_receive(owned_backend, 1, &out, true), MPMC_ERR_INTERRUPTED, EINTR);
    alarm(0);
    CHECK(alarm_seen && same_message(&out, &sentinel));

    CHECK_RESULT(sysv_sem_change(semid, SHM_SEM_MUTEX, -1, false), MPMC_OK, 0);
    arm_timeout(1);
    CHECK_RESULT(shm_backend_send(owned_backend, 1, &sentinel), MPMC_ERR_INTERRUPTED, EINTR);
    alarm(0);
    CHECK(alarm_seen && semctl(semid, SHM_SEM_EMPTY, GETVAL) == (int)capacity);
    CHECK(owned_backend->segments[0].ring->count == 0);
    uint64_t count = UINT64_MAX;
    arm_timeout(1);
    CHECK_RESULT(shm_backend_consumed_count(owned_backend, 1, &count), MPMC_ERR_INTERRUPTED, EINTR);
    alarm(0);
    CHECK(alarm_seen && count == UINT64_MAX);
    CHECK_RESULT(sysv_sem_change(semid, SHM_SEM_MUTEX, 1, false), MPMC_OK, 0);

    CHECK_RESULT(shm_backend_send(owned_backend, 1, &sentinel), MPMC_OK, 0);
    CHECK_RESULT(sysv_sem_change(semid, SHM_SEM_MUTEX, -1, false), MPMC_OK, 0);
    arm_timeout(1);
    CHECK_RESULT(shm_backend_receive(owned_backend, 1, &out, false), MPMC_ERR_INTERRUPTED, EINTR);
    alarm(0);
    CHECK(alarm_seen && same_message(&out, &sentinel));
    CHECK(semctl(semid, SHM_SEM_FULL, GETVAL) == 1);
    CHECK(owned_backend->segments[0].ring->count == 1);
    CHECK_RESULT(sysv_sem_change(semid, SHM_SEM_MUTEX, 1, false), MPMC_OK, 0);
    for (unsigned i = 1; i < capacity; ++i) {
        CHECK_RESULT(shm_backend_send(owned_backend, 1, &sentinel), MPMC_OK, 0);
    }
    arm_timeout(1);
    CHECK_RESULT(shm_backend_send(owned_backend, 1, &sentinel), MPMC_ERR_INTERRUPTED, EINTR);
    alarm(0);
    CHECK(alarm_seen && owned_backend->segments[0].ring->count == capacity);
    CHECK(semctl(semid, SHM_SEM_MUTEX, GETVAL) == 1);
    destroy_backend();
}

/** @brief Diferencia falha anterior à alteração do buffer de falha na publicação final. */
static void test_error_propagation(void) {
    const mpmc_result_t errors[] = {
        {.status = MPMC_ERR_SYS, .system_errno = EACCES},
        {.status = MPMC_ERR_SHUTDOWN, .system_errno = EIDRM},
        {.status = MPMC_ERR_INTERRUPTED, .system_errno = EINTR},
    };
    for (unsigned operation = 0; operation < 3; ++operation) {
        for (unsigned stage = 1; stage <= 2; ++stage) {
            for (size_t e = 0; e < sizeof errors / sizeof errors[0]; ++e) {
                if (stage == 2 && errors[e].status == MPMC_ERR_INTERRUPTED) continue;
                if (!create_backend()) return;
                message_t message = sample_message(5, 55), sentinel = sample_message(99, 99);
                message_t out = sentinel;
                uint64_t count = UINT64_MAX;
                if (operation == 1) {
                    CHECK_RESULT(shm_backend_send(owned_backend, 1, &message), MPMC_OK, 0);
                }
                apply_calls = change_calls = 0;
                injected_error = errors[e];
                if (operation == 2)
                    fail_change_call = stage;
                else
                    fail_apply_call = stage;
                mpmc_result_t result;
                if (operation == 0)
                    result = shm_backend_send(owned_backend, 1, &message);
                else if (operation == 1)
                    result = shm_backend_receive(owned_backend, 1, &out, true);
                else
                    result = shm_backend_consumed_count(owned_backend, 1, &count);
                errno = EIO;
                CHECK(
                    result.status == errors[e].status
                    && result.system_errno == errors[e].system_errno
                );
                shm_ring_t *ring = owned_backend->segments[0].ring;
                if (operation == 0) CHECK(ring->count == (stage == 2 ? 1 : 0));
                if (operation == 1) {
                    CHECK(ring->count == (stage == 2 ? 0 : 1));
                    CHECK(ring->consumed_total == (stage == 2 ? 1 : 0));
                    CHECK(same_message(&out, stage == 2 ? &message : &sentinel));
                }
                if (operation == 2) CHECK(count == UINT64_MAX);
                destroy_backend();
            }
        }
    }
}

/**
 * @brief Exerce dois produtores e dois consumidores na mesma estrutura com IPC real.
 * @details O pipe transporta as observações dos consumidores. Sua ordem pode divergir
 * da ordem de retirada, por escalonamento; a checagem exige cada identidade uma vez.
 */
static void test_concurrent_workers(void) {
    if (!create_backend()) return;
    int observations[2];
    int rc = pipe(observations);
    CHECK(rc == 0);
    if (rc != 0) return;
    for (unsigned role = 0; role < 2 * WORKERS_PER_ROLE; ++role) {
        pid_t child = fork();
        CHECK(child >= 0);
        if (child == 0) run_child(role, observations[0], observations[1]);
        if (child < 0) {
            close(observations[0]);
            close(observations[1]);
            cleanup();
            return;
        }
        children[role] = child;
    }
    close(observations[1]);
    bool seen[WORKERS_PER_ROLE][MESSAGES_PER_WORKER] = {};
    for (unsigned i = 0; i < WORKERS_PER_ROLE * MESSAGES_PER_WORKER; ++i) {
        message_t out;
        bool read_ok = read_message(observations[0], &out);
        CHECK(read_ok);
        if (!read_ok) break;
        bool valid = out.operation_id >= 1 && out.operation_id <= WORKERS_PER_ROLE
            && out.sequence >= 1 && out.sequence <= MESSAGES_PER_WORKER;
        CHECK(valid);
        if (!valid) continue;
        unsigned producer = (unsigned)out.operation_id - 1;
        CHECK(!seen[producer][out.sequence - 1]);
        seen[producer][out.sequence - 1] = true;
        message_t expected = sample_message(out.sequence, out.operation_id);
        expected.sender_pid = children[WORKERS_PER_ROLE + producer];
        CHECK(same_message(&out, &expected));
    }
    close(observations[0]);
    collect_children();
    if (owned_backend == nullptr) return;
    for (unsigned p = 0; p < WORKERS_PER_ROLE; ++p) {
        for (unsigned i = 0; i < MESSAGES_PER_WORKER; ++i) CHECK(seen[p][i]);
    }
    uint64_t count = 0;
    CHECK_RESULT(shm_backend_consumed_count(owned_backend, 1, &count), MPMC_OK, 0);
    CHECK(count == WORKERS_PER_ROLE * MESSAGES_PER_WORKER);
    CHECK(owned_backend->segments[0].ring->count == 0);
    destroy_backend();
}

/**
 * @brief Cria o pipe de prontidão e o de resultado usados por spawn_blocked().
 * @return true se ambos foram criados; false após fechar o que havia sido aberto.
 */
static bool open_event_pipes(int ready[2], int result[2]) {
    int rc = pipe(ready);
    CHECK(rc == 0);
    if (rc != 0) return false;
    rc = pipe(result);
    CHECK(rc == 0);
    if (rc != 0) {
        close(ready[0]);
        close(ready[1]);
        return false;
    }
    return true;
}

/**
 * @brief Envio em estrutura cheia bloqueia só o remetente e retoma após um consumo.
 * @details O filho avisa por pipe antes de chamar shm_backend_send(). Com a estrutura 1
 * cheia (EMPTY == 0), o envio só pode concluir depois que o pai consome uma mensagem;
 * enquanto isso a estrutura 2 deve operar normalmente. Nenhuma espera depende de tempo:
 * apenas de eventos de pipe, SIGCHLD e do alarm() de segurança.
 */
static void test_full_queue_blocks_only_its_sender(void) {
    if (!create_backend()) return;
    unsigned capacity = owned_backend->segments[0].ring->capacity;
    int semid = owned_backend->segments[0].semid;
    message_t out;
    CHECK_RESULT(shm_backend_receive(owned_backend, 1, &out, false), MPMC_ERR_WOULD_BLOCK, EAGAIN);
    for (unsigned i = 1; i <= capacity; ++i) {
        message_t message = sample_message(i, 11);
        CHECK_RESULT(shm_backend_send(owned_backend, 1, &message), MPMC_OK, 0);
    }
    CHECK(owned_backend->segments[0].ring->count == capacity);
    CHECK(semctl(semid, SHM_SEM_EMPTY, GETVAL) == 0);
    CHECK(semctl(semid, SHM_SEM_FULL, GETVAL) == (int)capacity);

    int ready[2], result[2];
    if (!open_event_pipes(ready, result)) return;
    message_t late = sample_message(1000, 11);
    if (!spawn_blocked(0, true, 1, &late, ready, result)) {
        close(ready[0]);
        close(result[0]);
        cleanup();
        return;
    }
    bool started = read_ready(ready[0]);
    CHECK(started);
    close(ready[0]);
    close(result[0]);
    if (!started) {
        cleanup();
        return;
    }

    /* Com a estrutura 1 cheia, a 2 não é afetada: ciclo completo sem bloqueio. */
    CHECK_RESULT(shm_backend_receive(owned_backend, 2, &out, false), MPMC_ERR_WOULD_BLOCK, EAGAIN);
    for (unsigned i = 1; i <= capacity; ++i) {
        message_t message = sample_message(i, 22);
        CHECK_RESULT(shm_backend_send(owned_backend, 2, &message), MPMC_OK, 0);
    }
    for (unsigned i = 1; i <= capacity; ++i) {
        message_t expected = sample_message(i, 22);
        CHECK_RESULT(shm_backend_receive(owned_backend, 2, &out, false), MPMC_OK, 0);
        CHECK(same_message(&out, &expected));
    }
    CHECK_RESULT(shm_backend_receive(owned_backend, 2, &out, false), MPMC_ERR_WOULD_BLOCK, EAGAIN);
    CHECK(owned_backend->segments[0].ring->count == capacity);
    CHECK(semctl(semid, SHM_SEM_EMPTY, GETVAL) == 0);

    /* Um consumo na estrutura 1 libera uma vaga e o envio pendente conclui. */
    message_t first = sample_message(1, 11);
    CHECK_RESULT(shm_backend_receive(owned_backend, 1, &out, false), MPMC_OK, 0);
    CHECK(same_message(&out, &first));
    collect_children();
    if (owned_backend == nullptr) return;

    for (unsigned i = 2; i <= capacity; ++i) {
        message_t expected = sample_message(i, 11);
        CHECK_RESULT(shm_backend_receive(owned_backend, 1, &out, false), MPMC_OK, 0);
        CHECK(same_message(&out, &expected));
    }
    message_t expected_late = sample_message(1000, 11);
    CHECK_RESULT(shm_backend_receive(owned_backend, 1, &out, false), MPMC_OK, 0);
    CHECK(same_message(&out, &expected_late));
    CHECK_RESULT(shm_backend_receive(owned_backend, 1, &out, false), MPMC_ERR_WOULD_BLOCK, EAGAIN);

    uint64_t count = 0;
    CHECK_RESULT(shm_backend_consumed_count(owned_backend, 1, &count), MPMC_OK, 0);
    CHECK(count == capacity + 1);
    CHECK_RESULT(shm_backend_consumed_count(owned_backend, 2, &count), MPMC_OK, 0);
    CHECK(count == capacity);
    CHECK(semctl(semid, SHM_SEM_MUTEX, GETVAL) == 1);
    CHECK(semctl(semid, SHM_SEM_EMPTY, GETVAL) == (int)capacity);
    CHECK(semctl(semid, SHM_SEM_FULL, GETVAL) == 0);
    destroy_backend();
}

/**
 * @brief Recepção bloqueante em estrutura vazia só retoma com dado da própria estrutura.
 * @details O pai envia primeiro à estrutura 1 e só depois à 2; o filho, bloqueado na 2,
 * deve receber exatamente a mensagem da 2, com todos os metadados, e a da 1 permanece.
 */
static void test_blocking_receive_resumes_on_own_structure(void) {
    if (!create_backend()) return;
    int ready[2], result[2];
    if (!open_event_pipes(ready, result)) return;
    if (!spawn_blocked(0, false, 2, nullptr, ready, result)) {
        close(ready[0]);
        close(result[0]);
        cleanup();
        return;
    }
    bool started = read_ready(ready[0]);
    CHECK(started);
    close(ready[0]);
    if (!started) {
        close(result[0]);
        cleanup();
        return;
    }

    message_t other = sample_message(5, 1);
    message_t wanted = sample_message(6, 2);
    CHECK_RESULT(shm_backend_send(owned_backend, 1, &other), MPMC_OK, 0);
    CHECK_RESULT(shm_backend_send(owned_backend, 2, &wanted), MPMC_OK, 0);

    message_t received;
    bool got = read_message(result[0], &received);
    CHECK(got);
    close(result[0]);
    collect_children();
    if (owned_backend == nullptr) return;
    CHECK(got && same_message(&received, &wanted));

    message_t out;
    CHECK_RESULT(shm_backend_receive(owned_backend, 2, &out, false), MPMC_ERR_WOULD_BLOCK, EAGAIN);
    CHECK_RESULT(shm_backend_receive(owned_backend, 1, &out, false), MPMC_OK, 0);
    CHECK(same_message(&out, &other));
    uint64_t count = 0;
    CHECK_RESULT(shm_backend_consumed_count(owned_backend, 1, &count), MPMC_OK, 0);
    CHECK(count == 1);
    CHECK_RESULT(shm_backend_consumed_count(owned_backend, 2, &count), MPMC_OK, 0);
    CHECK(count == 1);
    destroy_backend();
}

/**
 * @brief Backend com o máximo documentado de estruturas: índices 1..p independentes.
 * @details shm_backend_create() só rejeita zero; o limite superior é validado na camada
 * de argumentos do programa, portanto aqui não se espera erro para p + 1 na criação. Com
 * TEST_STRUCTURE_COUNT estruturas, cada índice recebe uma mensagem própria; as retiradas
 * devolvem a mensagem do mesmo índice e o índice p + 1 é inválido. Se o sistema operacional
 * esgotar seus limites System V (ENOSPC/ENOMEM/EMFILE), o cenário é dispensado com aviso,
 * sem contar como sucesso nem como falha; qualquer outra falha é registrada.
 */
static void test_maximum_structure_count(void) {
    cleanup();
    apply_calls = change_calls = fail_apply_call = fail_change_call = 0;
    arm_timeout(10);
    mpmc_result_t created = shm_backend_create(&owned_backend, TEST_STRUCTURE_COUNT, nullptr);
    if (created.status == MPMC_ERR_SYS
        && (created.system_errno == ENOSPC || created.system_errno == ENOMEM
            || created.system_errno == EMFILE)) {
        fprintf(
            stderr,
            "AVISO: limite System V do sistema impede %zu estruturas (errno %d); cenário "
            "dispensado\n",
            TEST_STRUCTURE_COUNT,
            created.system_errno
        );
        return;
    }
    CHECK(created.status == MPMC_OK && created.system_errno == 0);
    if (created.status != MPMC_OK) return;
    CHECK(owned_backend->producer_count == TEST_STRUCTURE_COUNT);

    /* Cada estrutura possui segmento e conjunto de semáforos próprios. */
    bool distinct = true;
    for (size_t a = 0; a < TEST_STRUCTURE_COUNT && distinct; ++a) {
        for (size_t b = a + 1; b < TEST_STRUCTURE_COUNT; ++b) {
            if (owned_backend->segments[a].shmid == owned_backend->segments[b].shmid
                || owned_backend->segments[a].semid == owned_backend->segments[b].semid) {
                distinct = false;
                break;
            }
        }
    }
    CHECK(distinct);

    for (size_t id = 1; id <= TEST_STRUCTURE_COUNT; ++id) {
        message_t message = sample_message((unsigned)id, id);
        CHECK_RESULT(shm_backend_send(owned_backend, id, &message), MPMC_OK, 0);
    }
    for (size_t id = TEST_STRUCTURE_COUNT; id >= 1; --id) {
        message_t out, expected = sample_message((unsigned)id, id);
        uint64_t count = UINT64_MAX;
        CHECK(owned_backend->segments[id - 1].ring->count == 1);
        CHECK_RESULT(shm_backend_receive(owned_backend, id, &out, false), MPMC_OK, 0);
        CHECK(same_message(&out, &expected));
        CHECK_RESULT(
            shm_backend_receive(owned_backend, id, &out, false), MPMC_ERR_WOULD_BLOCK, EAGAIN
        );
        CHECK_RESULT(shm_backend_consumed_count(owned_backend, id, &count), MPMC_OK, 0);
        CHECK(count == 1);
    }

    const size_t first_shmid = (size_t)owned_backend->segments[0].shmid;
    const size_t last_semid = (size_t)owned_backend->segments[TEST_STRUCTURE_COUNT - 1].semid;
    message_t message = sample_message(1, 1), out = message;
    uint64_t count = UINT64_MAX;
    CHECK_RESULT(
        shm_backend_send(owned_backend, TEST_STRUCTURE_COUNT + 1, &message),
        MPMC_ERR_INVALID_INDEX,
        0
    );
    CHECK_RESULT(
        shm_backend_receive(owned_backend, TEST_STRUCTURE_COUNT + 1, &out, false),
        MPMC_ERR_INVALID_INDEX,
        0
    );
    CHECK_RESULT(
        shm_backend_consumed_count(owned_backend, TEST_STRUCTURE_COUNT + 1, &count),
        MPMC_ERR_INVALID_INDEX,
        0
    );
    CHECK(same_message(&message, &out) && count == UINT64_MAX);

    destroy_backend();
    struct shmid_ds info;
    CHECK(shmctl((int)first_shmid, IPC_STAT, &info) == -1);
    CHECK(semctl((int)last_semid, SHM_SEM_MUTEX, GETVAL) == -1);
}

/** @brief Confere destruição dos segmentos e conjuntos reais após uso do backend. */
static void test_owned_resources_removed(void) {
    if (!create_backend()) return;
    int shmid = owned_backend->segments[0].shmid;
    int semid = owned_backend->segments[0].semid;
    destroy_backend();
    struct shmid_ds info;
    CHECK(shmctl(shmid, IPC_STAT, &info) == -1);
    CHECK(errno == EINVAL || errno == EIDRM);
    CHECK(semctl(semid, SHM_SEM_MUTEX, GETVAL) == -1);
    CHECK(errno == EINVAL || errno == EIDRM);
}

/** @brief Executa os cenários; retorna zero se todas as propriedades forem satisfeitas. */
int main(void) {
    if (atexit(finish) != 0 || !configure_signals()) return EXIT_FAILURE;
    test_arguments_and_empty();
    test_fifo_and_counts();
    test_interruption_and_mutex();
    test_error_propagation();
    test_concurrent_workers();
    test_full_queue_blocks_only_its_sender();
    test_blocking_receive_resumes_on_own_structure();
    test_maximum_structure_count();
    test_owned_resources_removed();
    finish();
    printf("%u verificações de operações SHM, %u falhas\n", checks, failures);
    return failures == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}

/** @brief Encerra somente filhos próprios, recolhe-os e remove os recursos do backend. */
static void cleanup(void) {
    alarm(0);
    for (size_t i = 0; i < sizeof children / sizeof children[0]; ++i) {
        if (children[i] > 0) (void)kill(children[i], SIGKILL);
    }
    for (size_t i = 0; i < sizeof children / sizeof children[0]; ++i) {
        if (children[i] <= 0) continue;
        while (waitpid(children[i], nullptr, 0) < 0 && errno == EINTR) {
        }
        children[i] = 0;
    }
    if (owned_backend != nullptr) {
        shm_backend_t *backend = owned_backend;
        owned_backend = nullptr;
        (void)shm_backend_destroy(backend, nullptr);
    }
}

/**
 * @brief Instala ações capturáveis e libera os sinais reservados pela suíte.
 * @details Usa sigaction() sem SA_RESTART para interromper IPC/read no Linux e
 * macOS. SIGCHLD possui ação capturável antes do fork para não depender de sinais
 * de ação padrão ignorada serem mantidos por sigwait() no macOS.
 * @return true se ações e máscara foram configuradas; false após restaurar o estado parcial.
 */
static bool configure_signals(void) {
    struct sigaction action = {.sa_handler = on_alarm, .sa_flags = 0};
    sigemptyset(&action.sa_mask);
    if (sigprocmask(SIG_SETMASK, nullptr, &previous_signal_mask) != 0) return false;
    if (sigaction(SIGALRM, &action, &previous_alarm_action) != 0) return false;
    action.sa_handler = on_child;
    if (sigaction(SIGCHLD, &action, &previous_chld_action) != 0) {
        (void)sigaction(SIGALRM, &previous_alarm_action, nullptr);
        return false;
    }
    sigset_t active = previous_signal_mask;
    sigdelset(&active, SIGALRM);
    sigdelset(&active, SIGCHLD);
    if (sigprocmask(SIG_SETMASK, &active, nullptr) != 0) {
        (void)sigaction(SIGALRM, &previous_alarm_action, nullptr);
        (void)sigaction(SIGCHLD, &previous_chld_action, nullptr);
        return false;
    }
    signals_configured = true;
    return true;
}

/** @brief Limpa recursos, drena eventual alarme pendente e restaura ações/máscara originais. */
static void finish(void) {
    cleanup();
    if (!signals_configured) return;
    sigset_t alarms, pending;
    sigemptyset(&alarms);
    sigaddset(&alarms, SIGALRM);
    CHECK(sigprocmask(SIG_BLOCK, &alarms, nullptr) == 0);
    if (sigpending(&pending) == 0 && sigismember(&pending, SIGALRM) == 1) {
        int signal_number;
        CHECK(sigwait(&alarms, &signal_number) == 0);
    }
    CHECK(sigaction(SIGALRM, &previous_alarm_action, nullptr) == 0);
    CHECK(sigaction(SIGCHLD, &previous_chld_action, nullptr) == 0);
    CHECK(sigprocmask(SIG_SETMASK, &previous_signal_mask, nullptr) == 0);
    signals_configured = false;
}

/** @brief Prepara dois buffers privados e um prazo de dez segundos por cenário. */
static bool create_backend(void) {
    cleanup();
    apply_calls = change_calls = fail_apply_call = fail_change_call = 0;
    arm_timeout(10);
    mpmc_result_t result = shm_backend_create(&owned_backend, 2, nullptr);
    CHECK(result.status == MPMC_OK && result.system_errno == 0);
    return result.status == MPMC_OK;
}

/** @brief Destrói o contexto e verifica a liberação normal sem reaproveitar o ponteiro. */
static void destroy_backend(void) {
    fail_apply_call = fail_change_call = 0;
    shm_backend_t *backend = owned_backend;
    owned_backend = nullptr;
    CHECK_RESULT(shm_backend_destroy(backend, nullptr), MPMC_OK, 0);
    alarm(0);
}

/** @brief Constrói metadados determinísticos para verificar sua preservação no transporte. */
static message_t sample_message(unsigned sequence, uint64_t operation) {
    return (message_t){
        .sender_pid = getpid(),
        .sequence = sequence,
        .sent_at = {.tv_sec = (time_t)operation, .tv_nsec = (long)sequence},
        .operation_id = operation,
    };
}

/** @brief Compara campos lógicos, sem presumir conteúdo dos bytes de padding. */
static bool same_message(const message_t *left, const message_t *right) {
    return left->sender_pid == right->sender_pid && left->sequence == right->sequence
        && left->sent_at.tv_sec == right->sent_at.tv_sec
        && left->sent_at.tv_nsec == right->sent_at.tv_nsec
        && left->operation_id == right->operation_id;
}

/** @brief Marca o timeout; chamadas bloqueantes retornam EINTR fora do tratador. */
static void on_alarm(int signal_number) {
    (void)signal_number;
    alarm_seen = 1;
}

/** @brief Mantém SIGCHLD capturável; a coleta é feita fora do tratador. */
static void on_child(int signal_number) {
    (void)signal_number;
}

/** @brief Agenda o prazo e limpa a indicação do alarme anterior. */
static void arm_timeout(unsigned seconds) {
    alarm_seen = 0;
    alarm(seconds);
}

/** @brief Lê uma mensagem inteira do pipe, abandonando a espera se o prazo vencer. */
static bool read_message(int fd, message_t *message) {
    size_t offset = 0;
    while (offset < sizeof *message && !alarm_seen) {
        ssize_t received = read(fd, (unsigned char *)message + offset, sizeof *message - offset);
        if (received < 0 && errno == EINTR) continue;
        if (received <= 0) return false;
        offset += (size_t)received;
    }
    return offset == sizeof *message;
}

/** @brief Executa o papel herdado e termina sem executar o cleanup global do pai. */
static void run_child(unsigned role, int read_fd, int write_fd) {
    close(read_fd);
    arm_timeout(10);
    bool ok = true;
    for (unsigned i = 0; i < MESSAGES_PER_WORKER && ok; ++i) {
        if (role < WORKERS_PER_ROLE) {
            message_t message;
            ok = shm_backend_receive(owned_backend, 1, &message, true).status == MPMC_OK;
            if (ok) {
                ssize_t written;
                do {
                    written = write(write_fd, &message, sizeof message);
                } while (written < 0 && errno == EINTR && !alarm_seen);
                ok = written == (ssize_t)sizeof message;
            }
        } else {
            message_t message = sample_message(i + 1, role - WORKERS_PER_ROLE + 1);
            ok = shm_backend_send(owned_backend, 1, &message).status == MPMC_OK;
        }
    }
    close(write_fd);
    if (shm_backend_worker_close(owned_backend, nullptr).status != MPMC_OK) ok = false;
    _exit(ok ? EXIT_SUCCESS : EXIT_FAILURE);
}

/** @brief Espera um byte de prontidão do filho; abandona a espera se o prazo vencer. */
static bool read_ready(int fd) {
    unsigned char byte;
    ssize_t received;
    do {
        received = read(fd, &byte, 1);
    } while (received < 0 && errno == EINTR && !alarm_seen);
    return received == 1;
}

/**
 * @brief Cria um filho que sinaliza prontidão e executa uma única operação bloqueante.
 * @details O filho escreve um byte em ready imediatamente antes de chamar
 * shm_backend_send() ou shm_backend_receive(bloqueante). No caso de recepção, a
 * mensagem recebida é escrita em result. O filho fecha somente seu estado local.
 * @return true se o fork funcionou; o pai fecha as pontas de escrita dos dois pipes.
 */
static bool spawn_blocked(
    unsigned slot,
    bool sending,
    size_t structure,
    const message_t *message,
    int ready[2],
    int result[2]
) {
    pid_t child = fork();
    CHECK(child >= 0);
    if (child < 0) return false;
    if (child == 0) {
        close(ready[0]);
        close(result[0]);
        arm_timeout(10);
        unsigned char byte = 1;
        bool ok = write(ready[1], &byte, 1) == 1;
        close(ready[1]);
        if (ok && sending) {
            ok = shm_backend_send(owned_backend, structure, message).status == MPMC_OK;
        } else if (ok) {
            message_t out;
            ok = shm_backend_receive(owned_backend, structure, &out, true).status == MPMC_OK
                && write(result[1], &out, sizeof out) == (ssize_t)sizeof out;
        }
        close(result[1]);
        if (shm_backend_worker_close(owned_backend, nullptr).status != MPMC_OK) ok = false;
        _exit(ok ? EXIT_SUCCESS : EXIT_FAILURE);
    }
    children[slot] = child;
    close(ready[1]);
    close(result[1]);
    return true;
}

/**
 * @brief Recolhe filhos por eventos, com alarm() e sigwait(), sem consultas periódicas.
 * @details Bloqueia SIGCHLD/SIGALRM antes de verificar o filho. A ação capturável
 * de SIGCHLD já foi instalada antes do fork, inclusive para macOS. Não usa sigtimedwait().
 */
static void collect_children(void) {
    if (alarm_seen) {
        CHECK(false);
        cleanup();
        return;
    }
    sigset_t events, old_mask;
    sigemptyset(&events);
    sigaddset(&events, SIGCHLD);
    sigaddset(&events, SIGALRM);
    int rc = sigprocmask(SIG_BLOCK, &events, &old_mask);
    CHECK(rc == 0);
    if (rc != 0) {
        cleanup();
        return;
    }
    bool ok = !alarm_seen;
    CHECK(ok);
    for (size_t i = 0; i < sizeof children / sizeof children[0] && ok; ++i) {
        if (children[i] <= 0) continue;
        int status;
        pid_t waited;
        for (;;) {
            waited = waitpid(children[i], &status, WNOHANG);
            if (waited == children[i]) break;
            if (waited < 0 && errno == EINTR) continue;
            if (waited < 0) break;
            int signal_number;
            rc = sigwait(&events, &signal_number);
            if (rc != 0 || signal_number == SIGALRM) break;
        }
        CHECK(waited == children[i]);
        if (waited != children[i]) {
            ok = false;
            break;
        }
        children[i] = 0;
        CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 0);
    }
    CHECK(sigprocmask(SIG_SETMASK, &old_mask, nullptr) == 0);
    if (!ok) cleanup();
}
