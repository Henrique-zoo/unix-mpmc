/**
 * @file test_msg_backend.c
 * @brief Testa a consolidação de consumo, a política de ACK entre processos e
 * as filas de dados.
 * @ingroup tests
 * @details Os consumidores são criados antes do registro de ACK no produtor,
 * verificando que a solicitação chega pelo envelope e não pelo heap herdado.
 * As esperas de confirmação usam alarm() como limite de tempo. Verifica o
 * isolamento entre filas, a ordem FIFO e o recebimento não bloqueante. Também
 * testa a consolidação de eventos e a política de ACK entre processos.
 */
#include <errno.h>
#include <limits.h>
#include <signal.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/msg.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "mpmc/ipc/msg.h"
#include "mpmc/ipc/msg_backend.h"

static int failed(const char *text);
static void on_alarm(int signal_number);
static void check_no_orphan_queues(void);
static void test_arguments(void);
static void test_queue_isolation(void);
static void test_fifo(void);
static void test_nonblocking_receive(void);
static void check_message(const message_t *received, const message_t *expected);
static void test_send_uses_blocking_mode(void);
static void test_queue_cleanup(void);
static void test_send_backpressure(void);
static void test_queue_cleanup_remove_failure(void);
static void test_three_queues(void);
static void test_would_block_publishes_nothing(void);
static void test_contract_violations(void);
static void test_ack_control_operations(void);
mpmc_result_t mpmc_test_msg_create(int *out_msqid);
mpmc_result_t mpmc_test_msg_remove(int msqid);
mpmc_result_t mpmc_test_msg_send(
    int msqid,
    long type,
    const void *payload,
    size_t size,
    bool nowait
);
mpmc_result_t mpmc_test_msg_receive(
    int msqid,
    long desired_type,
    void *out_payload,
    size_t capacity,
    bool nowait,
    ssize_t *out_size
);
mpmc_result_t mpmc_test_msg_receive_typed(
    int msqid,
    long desired_type,
    void *out_payload,
    size_t capacity,
    bool nowait,
    ssize_t *out_size,
    long *out_type
);
int mpmc_test_nanosleep(const struct timespec *request, struct timespec *remaining);

/**
 * @brief Quantidade de verificações executadas nesta suíte.
 */
static unsigned checks;

/**
 * @brief Quantidade de verificações que registraram falha nesta suíte.
 */
static unsigned failures;

/**
 * @brief Limite de identificadores registrados durante a injeção de falhas.
 */
#define TEST_QUEUE_LIMIT 8u

/**
 * @brief Número de chamadas realizadas ao substituto de criação.
 */
static unsigned create_calls;

/**
 * @brief Número da chamada de criação que deve falhar; zero desabilita a falha.
 */
static unsigned fail_create_call;

/**
 * @brief Indica se os identificadores de filas devem ser registrados.
 */
static bool track_queue_cleanup;

/**
 * @brief Identificadores das filas criadas durante o cenário de falha.
 */
static int created_queue_ids[TEST_QUEUE_LIMIT];

/**
 * @brief Resultados das tentativas de remoção das filas.
 */
static mpmc_result_t removed_queue_results[TEST_QUEUE_LIMIT];

/**
 * @brief Identificadores das filas cuja remoção foi solicitada.
 */
static int removed_queue_ids[TEST_QUEUE_LIMIT];

/**
 * @brief Quantidade de filas criadas e registradas.
 */
static size_t created_queue_count;

/**
 * @brief Quantidade de tentativas de remoção registradas.
 */
static size_t removed_queue_count;

/**
 * @brief Quantidade de envios realizados pelo backend durante o teste.
 */
static unsigned send_calls;

/**
 * @brief Último valor de nowait recebido pelo substituto de envio.
 */
static bool last_send_nowait;

/** @brief Tipo da última mensagem enviada pelo backend. */
static long last_send_type;

/**
 * @brief Identificador da última fila utilizada pelo substituto de envio.
 */
static int last_send_msqid = -1;

/** @brief Capacidade da fila de ACK simulada. */
#define ACK_CONTROL_MAX 32u

typedef struct {
    long type;
    uint64_t operation_id;
    ssize_t size;
} ack_control_message_t;

static bool ack_control_active;
static bool ack_control_send_active;
static int ack_control_queue = -1;
static int ack_control_data_queue = -1;
static ack_control_message_t ack_control_messages[ACK_CONTROL_MAX];
static size_t ack_control_message_count;
static unsigned ack_control_send_would_block;
static unsigned ack_control_receive_calls;
static unsigned ack_control_sleep_calls;
static mpmc_result_t ack_control_receive_failure;

/**
 * @brief Quantidade de chamadas realizadas ao substituto de remoção.
 */
static unsigned remove_calls;

/**
 * @brief Número da chamada de remoção que deve falhar; zero desabilita a falha.
 */
static unsigned fail_remove_call;

/**
 * @brief Capacidade do registro de todas as filas criadas durante a suíte.
 */
#define ALL_QUEUES_MAX 128u

/**
 * @brief Identificadores de todas as filas criadas pelo backend durante a
 * suíte.
 */
static int all_queue_ids[ALL_QUEUES_MAX];

/**
 * @brief Quantidade de identificadores registrados em all_queue_ids.
 */
static size_t all_queue_count;

/**
 * @brief Substitui a criação de filas durante os testes.
 * @param out_msqid Identificador da fila criada.
 * @return Resultado da criação real ou erro simulado.
 * @details Permite provocar uma falha em uma chamada específica de criação.
 */
mpmc_result_t mpmc_test_msg_create(int *out_msqid) {
    ++create_calls;

    if (fail_create_call != 0 && create_calls == fail_create_call) {
        return (mpmc_result_t){
            .status = MPMC_ERR_SYS,
            .system_errno = ENOSPC,
        };
    }

    mpmc_result_t result = sysv_msg_create(out_msqid);

    if (result.status == MPMC_OK && all_queue_count < ALL_QUEUES_MAX) {
        all_queue_ids[all_queue_count++] = *out_msqid;
    }

    if (track_queue_cleanup && result.status == MPMC_OK && created_queue_count < TEST_QUEUE_LIMIT) {
        created_queue_ids[created_queue_count++] = *out_msqid;
    }

    return result;
}

/**
 * @brief Substitui a remoção de filas e registra os resultados.
 * @param msqid Identificador da fila a remover.
 * @return Resultado da remoção real.
 */
mpmc_result_t mpmc_test_msg_remove(int msqid) {
    ++remove_calls;

    mpmc_result_t result;

    if (fail_remove_call != 0 && remove_calls == fail_remove_call) {
        result = (mpmc_result_t){
            .status = MPMC_ERR_SYS,
            .system_errno = EACCES,
        };
    } else {
        result = sysv_msg_remove(msqid);
    }

    if (track_queue_cleanup && removed_queue_count < TEST_QUEUE_LIMIT) {
        removed_queue_ids[removed_queue_count] = msqid;
        removed_queue_results[removed_queue_count] = result;
        ++removed_queue_count;
    }

    return result;
}

/**
 * @brief Registra o modo de envio solicitado pelo backend.
 * @param msqid Identificador da fila.
 * @param type Tipo da mensagem.
 * @param payload Corpo da mensagem.
 * @param size Tamanho do corpo.
 * @param nowait Indica se o envio deve ser não bloqueante.
 * @return Resultado do envio real.
 */
mpmc_result_t mpmc_test_msg_send(
    int msqid,
    long type,
    const void *payload,
    size_t size,
    bool nowait
) {
    ++send_calls;
    last_send_nowait = nowait;
    last_send_type = type;
    last_send_msqid = msqid;

    if (ack_control_active && ack_control_send_active && msqid == ack_control_data_queue) {
        if (ack_control_send_would_block != 0) {
            --ack_control_send_would_block;
            return (mpmc_result_t){
                .status = MPMC_ERR_WOULD_BLOCK,
                .system_errno = EAGAIN,
            };
        }
        return (mpmc_result_t){.status = MPMC_OK};
    }

    return sysv_msg_send(msqid, type, payload, size, nowait);
}

/**
 * @brief Injeta uma confirmação tipada na fila de controle simulada.
 */
static void ack_control_check(bool condition) {
    ++checks;
    if (!condition) {
        ++failures;
        fprintf(stderr, "FALHA %s:%d: controle ACK\n", __FILE__, __LINE__);
    }
}

static void ack_control_push(long type, uint64_t operation_id, ssize_t size) {
    ack_control_check(ack_control_message_count < ACK_CONTROL_MAX);
    if (ack_control_message_count == ACK_CONTROL_MAX) return;
    ack_control_messages[ack_control_message_count++] = (ack_control_message_t){
        .type = type,
        .operation_id = operation_id,
        .size = size,
    };
}

/**
 * @brief Intercepta msgrcv() somente na fila ACK controlada pelo teste.
 */
mpmc_result_t mpmc_test_msg_receive(
    int msqid,
    long desired_type,
    void *out_payload,
    size_t capacity,
    bool nowait,
    ssize_t *out_size
) {
    if (!ack_control_active || msqid != ack_control_queue) {
        return sysv_msg_receive(msqid, desired_type, out_payload, capacity, nowait, out_size);
    }

    ++ack_control_receive_calls;
    if (ack_control_receive_failure.status != MPMC_OK) {
        mpmc_result_t result = ack_control_receive_failure;
        ack_control_receive_failure = (mpmc_result_t){.status = MPMC_OK};
        return result;
    }

    for (size_t index = 0; index < ack_control_message_count; ++index) {
        if (ack_control_messages[index].type != desired_type) continue;
        ack_control_message_t message = ack_control_messages[index];
        for (size_t next = index + 1; next < ack_control_message_count; ++next) {
            ack_control_messages[next - 1] = ack_control_messages[next];
        }
        --ack_control_message_count;
        if (message.size < 0 || (size_t)message.size > capacity) {
            return (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = E2BIG};
        }
        if ((size_t)message.size >= sizeof(message.operation_id)) {
            memcpy(out_payload, &message.operation_id, sizeof(message.operation_id));
        }
        *out_size = message.size;
        return (mpmc_result_t){.status = MPMC_OK};
    }

    if (nowait) {
        return (mpmc_result_t){.status = MPMC_ERR_WOULD_BLOCK, .system_errno = ENOMSG};
    }
    return (mpmc_result_t){.status = MPMC_ERR_INTERRUPTED, .system_errno = EINTR};
}

/**
 * @brief Usa msgrcv() tipado real; o backend só a utiliza para a fila de dados.
 */
mpmc_result_t mpmc_test_msg_receive_typed(
    int msqid,
    long desired_type,
    void *out_payload,
    size_t capacity,
    bool nowait,
    ssize_t *out_size,
    long *out_type
) {
    return sysv_msg_receive_typed(
        msqid, desired_type, out_payload, capacity, nowait, out_size, out_type
    );
}

/**
 * @brief Torna o pequeno backoff do ACK determinístico na suíte.
 */
int mpmc_test_nanosleep(const struct timespec *request, struct timespec *remaining) {
    if (ack_control_active) {
        ack_control_check(request != nullptr);
        ack_control_check(request != nullptr && request->tv_sec == 0);
        ack_control_check(request != nullptr && request->tv_nsec == 1000000);
        ++ack_control_sleep_calls;
        return 0;
    }
    return nanosleep(request, remaining);
}

/**
 * @brief Registra uma verificação sem interromper a suíte.
 * @param condition Expressão lógica avaliada uma única vez.
 */
#define CHECK(condition)                                                                           \
    do {                                                                                           \
        ++checks;                                                                                  \
        if (!(condition)) {                                                                        \
            ++failures;                                                                            \
            fprintf(stderr, "FALHA %s:%d: %s\n", __FILE__, __LINE__, #condition);                  \
        }                                                                                          \
    } while (0)

/**
 * @brief Executa uma operação e confere status e system_errno.
 * @param call Expressão que retorna mpmc_result_t.
 * @param expected_status Categoria de resultado esperada.
 * @param expected_errno Valor esperado para o errno armazenado no resultado.
 */
#define CHECK_RESULT(call, expected_status, expected_errno)                                        \
    do {                                                                                           \
        mpmc_result_t result_ = (call);                                                            \
        CHECK(result_.status == (expected_status));                                                \
        CHECK(result_.system_errno == (expected_errno));                                           \
    } while (0)

/**
 * @brief Confere argumentos inválidos do backend.
 */
static void test_arguments(void) {
    msg_backend_t *backend = nullptr;

    CHECK_RESULT(msg_backend_create(nullptr, 2), MPMC_ERR_INVALID_ARG, 0);

    /* O sentinela só é comparado, nunca desreferenciado. */
    int marker = 0;
    msg_backend_t *sentinel = (msg_backend_t *)&marker;

    backend = sentinel;
    CHECK_RESULT(msg_backend_create(&backend, 0), MPMC_ERR_INVALID_ARG, 0);
    CHECK(backend == nullptr);

    backend = sentinel;
    CHECK_RESULT(msg_backend_create(&backend, SIZE_MAX), MPMC_ERR_INVALID_ARG, 0);
    CHECK(backend == nullptr);

    msg_backend_destroy(nullptr);
    msg_backend_worker_close(nullptr);

    message_t message = {
        .sender_pid = getpid(),
        .sequence = 1,
        .operation_id = 1,
    };
    message_t received = {};

    CHECK_RESULT(msg_backend_send(nullptr, 1, &message), MPMC_ERR_INVALID_ARG, 0);

    CHECK_RESULT(msg_backend_receive(nullptr, 1, &received, false), MPMC_ERR_INVALID_ARG, 0);

    CHECK_RESULT(msg_backend_receive(nullptr, 1, nullptr, false), MPMC_ERR_INVALID_ARG, 0);
}

/**
 * @brief Verifica que cada índice lógico acessa somente sua própria fila.
 */
static void test_queue_isolation(void) {
    msg_backend_t *backend = nullptr;
    mpmc_result_t result = msg_backend_create(&backend, 2);

    CHECK(result.status == MPMC_OK);
    CHECK(result.system_errno == 0);
    CHECK(backend != nullptr);

    if (result.status != MPMC_OK || backend == nullptr) return;

    message_t first = {
        .sender_pid = getpid(),
        .sequence = 1,
        .sent_at = {.tv_sec = 10, .tv_nsec = 100},
        .operation_id = 10,
    };

    message_t second = {
        .sender_pid = getpid(),
        .sequence = 2,
        .sent_at = {.tv_sec = 20, .tv_nsec = 200},
        .operation_id = 20,
    };

    message_t received = {};

    /* Os índices válidos devem estar no intervalo 1..producer_count. */
    CHECK_RESULT(msg_backend_send(backend, 0, &first), MPMC_ERR_INVALID_ARG, 0);
    CHECK_RESULT(msg_backend_send(backend, 3, &first), MPMC_ERR_INVALID_ARG, 0);

    CHECK_RESULT(msg_backend_receive(backend, 0, &received, false), MPMC_ERR_INVALID_ARG, 0);
    CHECK_RESULT(msg_backend_receive(backend, 3, &received, false), MPMC_ERR_INVALID_ARG, 0);

    /* Ponteiros nulos devem ser rejeitados. */
    CHECK_RESULT(msg_backend_send(backend, 1, nullptr), MPMC_ERR_INVALID_ARG, 0);
    CHECK_RESULT(msg_backend_receive(backend, 1, nullptr, false), MPMC_ERR_INVALID_ARG, 0);

    /* A mensagem enviada à fila 1 não deve aparecer na fila 2. */
    CHECK_RESULT(msg_backend_send(backend, 1, &first), MPMC_OK, 0);

    CHECK_RESULT(msg_backend_receive(backend, 2, &received, false), MPMC_ERR_WOULD_BLOCK, ENOMSG);

    /* A mensagem deve continuar disponível na fila 1. */
    result = msg_backend_receive(backend, 1, &received, false);
    CHECK(result.status == MPMC_OK);
    CHECK(result.system_errno == 0);

    if (result.status == MPMC_OK) {
        check_message(&received, &first);
    }

    /* A mensagem enviada à fila 2 não deve aparecer na fila 1. */
    CHECK_RESULT(msg_backend_send(backend, 2, &second), MPMC_OK, 0);

    CHECK_RESULT(msg_backend_receive(backend, 1, &received, false), MPMC_ERR_WOULD_BLOCK, ENOMSG);

    /* A mensagem deve estar disponível na fila 2. */
    result = msg_backend_receive(backend, 2, &received, false);
    CHECK(result.status == MPMC_OK);
    CHECK(result.system_errno == 0);

    if (result.status == MPMC_OK) {
        check_message(&received, &second);
    }

    msg_backend_destroy(backend);
}

/**
 * @brief Verifica que mensagens com diferentes tipos preservam a ordem FIFO.
 * @details A primeira mensagem utiliza mtype 1 e a segunda utiliza mtype 2,
 * pois somente a segunda operação possui registro de ACK.
 */
static void test_fifo(void) {
    msg_backend_t *backend = nullptr;
    mpmc_result_t result = msg_backend_create(&backend, 1);

    CHECK(result.status == MPMC_OK);
    CHECK(result.system_errno == 0);
    CHECK(backend != nullptr);

    if (result.status != MPMC_OK || backend == nullptr) return;

    message_t first = {
        .sender_pid = getpid(),
        .sequence = 1,
        .sent_at = {.tv_sec = 30, .tv_nsec = 100},
        .operation_id = 30,
    };

    message_t second = {
        .sender_pid = getpid(),
        .sequence = 2,
        .sent_at = {.tv_sec = 31, .tv_nsec = 200},
        .operation_id = 31,
    };

    message_t received = {};

    /* Somente a segunda operação deve solicitar ACK. */
    result = msg_backend_ack_begin(backend, 31, 1);
    CHECK(result.status == MPMC_OK);
    CHECK(result.system_errno == 0);

    if (result.status != MPMC_OK) {
        msg_backend_destroy(backend);
        return;
    }

    result = msg_backend_send(backend, 1, &first);
    CHECK(result.status == MPMC_OK);
    CHECK(result.system_errno == 0);

    if (result.status != MPMC_OK) {
        (void)msg_backend_ack_end(backend, 31);
        msg_backend_destroy(backend);
        return;
    }

    result = msg_backend_send(backend, 1, &second);
    CHECK(result.status == MPMC_OK);
    CHECK(result.system_errno == 0);

    if (result.status != MPMC_OK) {
        (void)msg_backend_ack_end(backend, 31);
        msg_backend_destroy(backend);
        return;
    }

    /* A primeira mensagem deve ser recebida primeiro. */
    result = msg_backend_receive(backend, 1, &received, false);
    CHECK(result.status == MPMC_OK);
    CHECK(result.system_errno == 0);

    if (result.status != MPMC_OK) {
        (void)msg_backend_ack_end(backend, 31);
        msg_backend_destroy(backend);
        return;
    }

    check_message(&received, &first);

    /* A segunda mensagem deve ser recebida em seguida. */
    result = msg_backend_receive(backend, 1, &received, false);
    CHECK(result.status == MPMC_OK);
    CHECK(result.system_errno == 0);

    if (result.status != MPMC_OK) {
        (void)msg_backend_ack_end(backend, 31);
        msg_backend_destroy(backend);
        return;
    }

    /*
     * A espera por ACK só é executada quando a segunda mensagem foi
     * efetivamente recebida e corresponde à operação registrada.
     */
    bool ack_message_received = received.operation_id == second.operation_id;

    check_message(&received, &second);

    struct sigaction action = {.sa_handler = on_alarm};
    struct sigaction previous;
    sigemptyset(&action.sa_mask);

    int rc = sigaction(SIGALRM, &action, &previous);
    CHECK(rc == 0);

    if (rc == 0) {
        if (ack_message_received) {
            alarm(2);
            result = msg_backend_ack_wait(backend, 31);
            alarm(0);

            CHECK(result.status == MPMC_OK);
            CHECK(result.system_errno == 0);
        }

        CHECK(sigaction(SIGALRM, &previous, nullptr) == 0);
    }

    CHECK_RESULT(msg_backend_ack_end(backend, 31), MPMC_OK, 0);

    msg_backend_destroy(backend);
}

/**
 * @brief Verifica que o recebimento não bloqueante não espera por mensagens.
 */
static void test_nonblocking_receive(void) {
    msg_backend_t *backend = nullptr;
    mpmc_result_t result = msg_backend_create(&backend, 1);

    CHECK(result.status == MPMC_OK);
    CHECK(result.system_errno == 0);
    CHECK(backend != nullptr);

    if (result.status != MPMC_OK || backend == nullptr) return;

    message_t received = {};
    message_t message = {
        .sender_pid = getpid(),
        .sequence = 1,
        .sent_at = {.tv_sec = 40, .tv_nsec = 100},
        .operation_id = 40,
    };

    /* A fila vazia deve retornar WOULD_BLOCK imediatamente. */
    CHECK_RESULT(msg_backend_receive(backend, 1, &received, false), MPMC_ERR_WOULD_BLOCK, ENOMSG);

    CHECK_RESULT(msg_backend_send(backend, 1, &message), MPMC_OK, 0);

    result = msg_backend_receive(backend, 1, &received, false);
    CHECK(result.status == MPMC_OK);
    CHECK(result.system_errno == 0);

    if (result.status == MPMC_OK) {
        check_message(&received, &message);
    }

    /* A fila vazia deve retornar WOULD_BLOCK sem esperar por mensagens. */
    struct sigaction action = {.sa_handler = on_alarm};
    struct sigaction previous;
    sigemptyset(&action.sa_mask);

    int rc = sigaction(SIGALRM, &action, &previous);
    CHECK(rc == 0);

    if (rc == -1) {
        msg_backend_destroy(backend);
        return;
    }

    alarm(2);
    result = msg_backend_receive(backend, 1, &received, false);
    alarm(0);

    CHECK(result.status == MPMC_ERR_WOULD_BLOCK);
    CHECK(result.system_errno == ENOMSG);

    CHECK(sigaction(SIGALRM, &previous, nullptr) == 0);

    msg_backend_destroy(backend);
}

/**
 * @brief Confere todos os campos de uma mensagem.
 * @param received Mensagem recebida.
 * @param expected Mensagem esperada.
 * @details Compara os campos individualmente, sem considerar padding.
 */
static void check_message(const message_t *received, const message_t *expected) {
    CHECK(received->sender_pid == expected->sender_pid);
    CHECK(received->sequence == expected->sequence);
    CHECK(received->sent_at.tv_sec == expected->sent_at.tv_sec);
    CHECK(received->sent_at.tv_nsec == expected->sent_at.tv_nsec);
    CHECK(received->operation_id == expected->operation_id);
}

/**
 * @brief Verifica que o backend solicita envio bloqueante.
 * @details O envio bloqueante permite que msgsnd() aplique backpressure
 * quando a capacidade da fila é atingida.
 */
static void test_send_uses_blocking_mode(void) {
    msg_backend_t *backend = nullptr;
    mpmc_result_t result = msg_backend_create(&backend, 1);

    CHECK(result.status == MPMC_OK);
    CHECK(backend != nullptr);

    if (result.status != MPMC_OK || backend == nullptr) return;

    message_t message = {
        .sender_pid = getpid(),
        .sequence = 1,
        .operation_id = 50,
    };

    send_calls = 0;
    last_send_nowait = true;

    result = msg_backend_send(backend, 1, &message);

    CHECK(result.status == MPMC_OK);
    CHECK(result.system_errno == 0);
    CHECK(send_calls == 1);
    CHECK(!last_send_nowait);

    msg_backend_destroy(backend);
}

/**
 * @brief Verifica a remoção das filas após falhas parciais na criação.
 * @details Simula falhas em cada uma das cinco chamadas de criação
 * e verifica se todos os recursos criados anteriormente foram removidos.
 */
static void test_queue_cleanup(void) {
    for (unsigned fail_call = 1; fail_call <= 5; ++fail_call) {
        msg_backend_t *backend = nullptr;

        create_calls = 0;
        fail_create_call = fail_call;
        created_queue_count = 0;
        removed_queue_count = 0;
        track_queue_cleanup = true;

        mpmc_result_t result = msg_backend_create(&backend, 3);

        track_queue_cleanup = false;
        fail_create_call = 0;

        size_t expected_created = fail_call - 1;

        CHECK(result.status == MPMC_ERR_SYS);
        CHECK(result.system_errno == ENOSPC);
        CHECK(backend == nullptr);

        CHECK(create_calls == fail_call);
        CHECK(created_queue_count == expected_created);
        CHECK(removed_queue_count == expected_created);

        if (created_queue_count == expected_created && removed_queue_count == expected_created) {
            for (size_t index = 0; index < expected_created; ++index) {
                CHECK(removed_queue_ids[index] == created_queue_ids[index]);
                CHECK(removed_queue_results[index].status == MPMC_OK);
                CHECK(removed_queue_results[index].system_errno == 0);
            }
        }

        /*
         * Proteção adicional para não deixar filas no kernel
         * caso alguma verificação de remoção falhe.
         */
        for (size_t index = 0; index < created_queue_count; ++index) {
            bool removed = false;

            for (size_t removed_index = 0; removed_index < removed_queue_count; ++removed_index) {
                if (removed_queue_ids[removed_index] == created_queue_ids[index]
                    && removed_queue_results[removed_index].status == MPMC_OK) {
                    removed = true;
                    break;
                }
            }

            if (!removed) {
                (void)sysv_msg_remove(created_queue_ids[index]);
            }
        }
    }
}

/**
 * @brief Verifica que o envio bloqueia quando a fila está cheia.
 * @details Preenche a fila até atingir sua capacidade, inicia um produtor
 * filho e verifica que ele só consegue concluir o envio após um consumo.
 */
static void test_send_backpressure(void) {
    msg_backend_t *backend = nullptr;
    mpmc_result_t result = msg_backend_create(&backend, 1);

    CHECK(result.status == MPMC_OK);
    CHECK(backend != nullptr);

    if (result.status != MPMC_OK || backend == nullptr) return;

    message_t message = {
        .sender_pid = getpid(),
        .sequence = 1,
        .operation_id = 60,
    };

    result = msg_backend_send(backend, 1, &message);

    CHECK(result.status == MPMC_OK);
    CHECK(last_send_msqid >= 0);

    if (result.status != MPMC_OK || last_send_msqid < 0) {
        msg_backend_destroy(backend);
        return;
    }

    int queue_id = last_send_msqid;
    bool queue_full = false;

    /*
     * Preenche a fila diretamente até que o kernel informe
     * que não há mais espaço disponível.
     */
    for (size_t index = 0; index < 100000; ++index) {
        message_t filler = {
            .sender_pid = getpid(),
            .sequence = index + 2,
            .operation_id = 60,
        };

        result = sysv_msg_send(queue_id, 1, &filler, sizeof(filler), true);

        if (result.status == MPMC_ERR_WOULD_BLOCK) {
            queue_full = true;
            break;
        }

        if (result.status != MPMC_OK) {
            break;
        }
    }

    CHECK(queue_full);

    if (!queue_full) {
        msg_backend_destroy(backend);
        return;
    }

    int ready_pipe[2];

    if (pipe(ready_pipe) == -1) {
        CHECK(false);
        msg_backend_destroy(backend);
        return;
    }

    pid_t child = fork();

    if (child == -1) {
        CHECK(false);
        close(ready_pipe[0]);
        close(ready_pipe[1]);
        msg_backend_destroy(backend);
        return;
    }

    if (child == 0) {
        close(ready_pipe[0]);

        char ready = 1;

        if (write(ready_pipe[1], &ready, sizeof(ready)) != sizeof(ready)) {
            _exit(2);
        }

        close(ready_pipe[1]);

        message_t pending = {
            .sender_pid = getpid(),
            .sequence = 100001,
            .operation_id = 60,
        };

        result = msg_backend_send(backend, 1, &pending);
        msg_backend_worker_close(backend);

        _exit(result.status == MPMC_OK ? 0 : 1);
    }

    close(ready_pipe[1]);

    char ready = 0;
    ssize_t bytes_read = read(ready_pipe[0], &ready, sizeof(ready));

    close(ready_pipe[0]);

    CHECK(bytes_read == sizeof(ready));

    if (bytes_read != sizeof(ready)) {
        (void)kill(child, SIGTERM);
        (void)waitpid(child, nullptr, 0);
        msg_backend_destroy(backend);
        return;
    }

    /*
     * Dá oportunidade para o filho executar o envio.
     * Enquanto a fila estiver cheia, o envio bloqueante não deve concluir.
     */
    struct timespec delay = {
        .tv_sec = 0,
        .tv_nsec = 50000000,
    };

    while (nanosleep(&delay, &delay) == -1 && errno == EINTR) {
    }

    int status = 0;
    pid_t waited = waitpid(child, &status, WNOHANG);

    CHECK(waited == 0);

    bool child_already_finished = waited == child;

    if (waited == -1) {
        CHECK(false);
        (void)kill(child, SIGTERM);
        (void)waitpid(child, nullptr, 0);
        msg_backend_destroy(backend);
        return;
    }

    /*
     * Consome uma mensagem para liberar espaço na fila.
     */
    message_t received = {};

    result = msg_backend_receive(backend, 1, &received, false);

    CHECK(result.status == MPMC_OK);

    if (!child_already_finished) {
        waited = waitpid(child, &status, 0);
        CHECK(waited == child);
    }

    if (waited == child) {
        CHECK(WIFEXITED(status));
        CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 0);
    }

    msg_backend_destroy(backend);
}

/**
 * @brief Verifica se a limpeza continua após uma falha de remoção.
 * @details Simula uma falha na remoção da segunda fila e verifica
 * se as demais filas também recebem uma tentativa de remoção.
 */
static void test_queue_cleanup_remove_failure(void) {
    msg_backend_t *backend = nullptr;

    create_calls = 0;
    fail_create_call = 5;

    remove_calls = 0;
    fail_remove_call = 2;

    created_queue_count = 0;
    removed_queue_count = 0;
    track_queue_cleanup = true;

    mpmc_result_t result = msg_backend_create(&backend, 3);

    track_queue_cleanup = false;
    fail_create_call = 0;
    fail_remove_call = 0;

    CHECK(result.status == MPMC_ERR_SYS);
    CHECK(result.system_errno == ENOSPC);
    CHECK(backend == nullptr);

    /* Três filas de dados e uma fila de eventos foram criadas. */
    CHECK(created_queue_count == 4);

    /* Todas as quatro filas devem receber uma tentativa de remoção. */
    CHECK(remove_calls == 4);
    CHECK(removed_queue_count == 4);

    if (created_queue_count == 4 && removed_queue_count == 4) {
        for (size_t index = 0; index < 4; ++index) {
            CHECK(removed_queue_ids[index] == created_queue_ids[index]);

            if (index == 1) {
                CHECK(removed_queue_results[index].status == MPMC_ERR_SYS);
                CHECK(removed_queue_results[index].system_errno == EACCES);
            } else {
                CHECK(removed_queue_results[index].status == MPMC_OK);
                CHECK(removed_queue_results[index].system_errno == 0);
            }
        }

        /* Única limpeza: a fila cuja remoção foi simulada como falha ainda existe.
         */
        for (size_t index = 0; index < created_queue_count; ++index) {
            if (removed_queue_results[index].status != MPMC_OK) {
                result = sysv_msg_remove(created_queue_ids[index]);
                CHECK(result.status == MPMC_OK);
            }
        }
    }
}

/**
 * @brief Verifica a independência de três filas lógicas.
 * @details Envia uma mensagem a cada índice, recebe na ordem inversa e confirma
 * que cada fila devolve somente a própria mensagem e fica vazia em seguida. O
 * índice seguinte ao último (p + 1) deve ser rejeitado.
 */
static void test_three_queues(void) {
    msg_backend_t *backend = nullptr;
    mpmc_result_t result = msg_backend_create(&backend, 3);

    CHECK(result.status == MPMC_OK);
    CHECK(backend != nullptr);

    if (result.status != MPMC_OK || backend == nullptr) return;

    message_t sent[4] = {};

    for (size_t id = 1; id <= 3; ++id) {
        sent[id] = (message_t){
            .sender_pid = getpid(),
            .sequence = id,
            .sent_at = {.tv_sec = 50 + (time_t)id, .tv_nsec = 100},
            .operation_id = 100 + id,
        };

        CHECK_RESULT(msg_backend_send(backend, id, &sent[id]), MPMC_OK, 0);
    }

    CHECK_RESULT(msg_backend_send(backend, 4, &sent[1]), MPMC_ERR_INVALID_ARG, 0);

    for (size_t id = 3; id >= 1; --id) {
        message_t received = {};

        result = msg_backend_receive(backend, id, &received, false);
        CHECK(result.status == MPMC_OK);
        CHECK(result.system_errno == 0);

        if (result.status == MPMC_OK) {
            check_message(&received, &sent[id]);
        }

        CHECK_RESULT(
            msg_backend_receive(backend, id, &received, false), MPMC_ERR_WOULD_BLOCK, ENOMSG
        );
    }

    msg_backend_destroy(backend);
}

/**
 * @brief Verifica que nenhuma fila criada pelo backend continua no kernel.
 * @details Consulta cada identificador registrado pelo substituto de criação.
 * Filas que ainda existirem são contadas como vazamento e removidas, para não
 * deixar recursos System V após a suíte.
 */
static void check_no_orphan_queues(void) {
    size_t leaked = 0;

    /* Um registro cheio descartaria identificadores e esconderia vazamentos. */
    CHECK(all_queue_count < ALL_QUEUES_MAX);

    for (size_t index = 0; index < all_queue_count; ++index) {
        struct msqid_ds info;

        if (msgctl(all_queue_ids[index], IPC_STAT, &info) == 0) {
            ++leaked;
            (void)msgctl(all_queue_ids[index], IPC_RMID, nullptr);
        }
    }

    CHECK(leaked == 0);
}

/**
 * @brief Verifica que o recebimento bloqueante espera por uma mensagem.
 * @details Um filho pede uma mensagem a uma fila vazia com blocking=true. Se o
 * recebimento retornasse sem esperar, o filho terminaria com erro antes do
 * envio. Depois do envio do pai, o filho deve receber exatamente a mensagem
 * enviada.
 */
static void test_blocking_receive(void) {
    msg_backend_t *backend = nullptr;
    mpmc_result_t result = msg_backend_create(&backend, 1);

    CHECK(result.status == MPMC_OK);
    CHECK(backend != nullptr);

    if (result.status != MPMC_OK || backend == nullptr) return;

    message_t message = {
        .sender_pid = getpid(),
        .sequence = 1,
        .sent_at = {.tv_sec = 70, .tv_nsec = 100},
        .operation_id = 70,
    };

    pid_t child = fork();

    if (child == -1) {
        CHECK(false);
        msg_backend_destroy(backend);
        return;
    }

    if (child == 0) {
        message_t received = {};
        mpmc_result_t child_result = msg_backend_receive(backend, 1, &received, true);
        bool matches = child_result.status == MPMC_OK;

        matches = matches && received.sequence == message.sequence;
        matches = matches && received.operation_id == message.operation_id;

        msg_backend_worker_close(backend);
        _exit(matches ? 0 : 1);
    }

    /* O filho deve ficar bloqueado na fila vazia; retorno antecipado seria erro.
     */
    struct timespec delay = {
        .tv_sec = 0,
        .tv_nsec = 50000000,
    };

    while (nanosleep(&delay, &delay) == -1 && errno == EINTR) {
    }

    int status = 0;
    pid_t waited = waitpid(child, &status, WNOHANG);

    CHECK(waited == 0);

    if (waited == 0) {
        result = msg_backend_send(backend, 1, &message);
        CHECK(result.status == MPMC_OK);

        if (result.status != MPMC_OK) {
            (void)kill(child, SIGTERM);
        }

        waited = waitpid(child, &status, 0);
    }

    CHECK(waited == child);

    if (waited == child) {
        CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 0);
    }

    msg_backend_destroy(backend);
}

/**
 * @brief Verifica a ordem FIFO e o esgotamento de uma fila com várias
 * mensagens.
 * @details Envia oito mensagens, recebe todas na ordem de inserção e confirma
 * que a tentativa seguinte, sem bloqueio, informa fila vazia.
 */
static void test_fifo_order(void) {
    msg_backend_t *backend = nullptr;
    mpmc_result_t result = msg_backend_create(&backend, 1);

    CHECK(result.status == MPMC_OK);
    CHECK(backend != nullptr);

    if (result.status != MPMC_OK || backend == nullptr) return;

    for (size_t index = 1; index <= 8; ++index) {
        message_t message = {
            .sender_pid = getpid(),
            .sequence = index,
            .sent_at = {.tv_sec = 60, .tv_nsec = (long)index},
            .operation_id = 80,
        };

        CHECK_RESULT(msg_backend_send(backend, 1, &message), MPMC_OK, 0);
    }

    for (size_t index = 1; index <= 8; ++index) {
        message_t expected = {
            .sender_pid = getpid(),
            .sequence = index,
            .sent_at = {.tv_sec = 60, .tv_nsec = (long)index},
            .operation_id = 80,
        };
        message_t received = {};

        result = msg_backend_receive(backend, 1, &received, false);
        CHECK(result.status == MPMC_OK);
        CHECK(result.system_errno == 0);

        if (result.status == MPMC_OK) {
            check_message(&received, &expected);
        }
    }

    message_t extra = {};

    CHECK_RESULT(msg_backend_receive(backend, 1, &extra, false), MPMC_ERR_WOULD_BLOCK, ENOMSG);

    msg_backend_destroy(backend);
}

/**
 * @brief Verifica que um recebimento sem mensagem não publica evento nem conta
 * consumo.
 * @details Com a fila vazia, receive não bloqueante retorna WOULD_BLOCK e o
 * contador consolidado permanece zero. Depois de uma retirada válida, o
 * contador passa a um, o que mostra que a contagem funciona e que o zero
 * anterior era real.
 */
static void test_would_block_publishes_nothing(void) {
    msg_backend_t *backend = nullptr;
    mpmc_result_t result = msg_backend_create(&backend, 1);

    CHECK(result.status == MPMC_OK);
    CHECK(backend != nullptr);

    if (result.status != MPMC_OK || backend == nullptr) return;

    message_t out = {};
    uint64_t count = UINT64_MAX;

    CHECK_RESULT(msg_backend_receive(backend, 1, &out, false), MPMC_ERR_WOULD_BLOCK, ENOMSG);
    CHECK_RESULT(msg_backend_poll_events(backend), MPMC_OK, 0);
    CHECK_RESULT(msg_backend_consumed_count(backend, 1, &count), MPMC_OK, 0);
    CHECK(count == 0);

    message_t message = {.sender_pid = getpid(), .sequence = 1, .operation_id = 90};

    CHECK_RESULT(msg_backend_send(backend, 1, &message), MPMC_OK, 0);
    CHECK_RESULT(msg_backend_receive(backend, 1, &out, false), MPMC_OK, 0);
    CHECK_RESULT(msg_backend_poll_events(backend), MPMC_OK, 0);
    CHECK_RESULT(msg_backend_consumed_count(backend, 1, &count), MPMC_OK, 0);
    CHECK(count == 1);

    msg_backend_destroy(backend);
}

/**
 * @brief Verifica que corpo de tamanho errado e tipo desconhecido retornam
 * MPMC_ERR_STATE.
 * @details Injeta diretamente na fila de dados mensagens fora do contrato. O
 * backend deve classificá-las como violação de estado (system_errno zero) e não
 * contá-las como consumo.
 */
static void test_contract_violations(void) {
    msg_backend_t *backend = nullptr;
    mpmc_result_t result = msg_backend_create(&backend, 1);

    CHECK(result.status == MPMC_OK);
    CHECK(backend != nullptr);

    if (result.status != MPMC_OK || backend == nullptr) return;

    message_t message = {.sender_pid = getpid(), .sequence = 1, .operation_id = 91};
    message_t out = {};
    uint64_t count = UINT64_MAX;

    /* Um envio e uma retirada válidos revelam o identificador da fila de dados.
     */
    CHECK_RESULT(msg_backend_send(backend, 1, &message), MPMC_OK, 0);

    int queue_id = last_send_msqid;

    CHECK(queue_id >= 0);
    CHECK_RESULT(msg_backend_receive(backend, 1, &out, false), MPMC_OK, 0);

    if (queue_id >= 0) {
        unsigned char too_short[4] = {};

        /* Corpo menor que sizeof(message_t). */
        CHECK_RESULT(sysv_msg_send(queue_id, 1, too_short, sizeof(too_short), true), MPMC_OK, 0);
        CHECK_RESULT(msg_backend_receive(backend, 1, &out, false), MPMC_ERR_STATE, 0);

        /* Tipo diferente de 1 e 2. */
        CHECK_RESULT(sysv_msg_send(queue_id, 3, &message, sizeof(message), true), MPMC_OK, 0);
        CHECK_RESULT(msg_backend_receive(backend, 1, &out, false), MPMC_ERR_STATE, 0);
    }

    /* Somente a primeira retirada, válida, foi contada. */
    CHECK_RESULT(msg_backend_poll_events(backend), MPMC_OK, 0);
    CHECK_RESULT(msg_backend_consumed_count(backend, 1, &count), MPMC_OK, 0);
    CHECK(count == 1);

    msg_backend_destroy(backend);
}

/**
 * @brief Verifica o ciclo de controle de ACK por operação com entradas
 * determinísticas.
 */
static void test_ack_control_operations(void) {
    size_t queue_start = all_queue_count;
    msg_backend_t *backend = nullptr;
    mpmc_result_t result = msg_backend_create(&backend, 1);
    CHECK(result.status == MPMC_OK);
    CHECK(backend != nullptr);
    if (result.status != MPMC_OK || backend == nullptr) return;

    CHECK(all_queue_count == queue_start + 3);
    if (all_queue_count < queue_start + 3) {
        msg_backend_destroy(backend);
        return;
    }

    ack_control_queue = all_queue_ids[queue_start + 2];
    ack_control_data_queue = all_queue_ids[queue_start];
    ack_control_message_count = 0;
    ack_control_receive_calls = 0;
    ack_control_sleep_calls = 0;
    ack_control_receive_failure = (mpmc_result_t){.status = MPMC_OK};
    ack_control_active = true;

    CHECK_RESULT(msg_backend_ack_begin(nullptr, 101, 1), MPMC_ERR_INVALID_ARG, 0);
    CHECK_RESULT(msg_backend_ack_begin(backend, 0, 1), MPMC_ERR_INVALID_ARG, 0);
    CHECK_RESULT(msg_backend_ack_begin(backend, UINT64_MAX, 1), MPMC_ERR_INVALID_ARG, 0);
    CHECK_RESULT(msg_backend_ack_begin(backend, 100, 0), MPMC_ERR_INVALID_ARG, 0);
    CHECK_RESULT(msg_backend_ack_begin(backend, 100, 1025), MPMC_ERR_INVALID_ARG, 0);
    CHECK_RESULT(msg_backend_ack_wait(backend, 999), MPMC_ERR_STATE, 0);

    CHECK_RESULT(msg_backend_ack_begin(backend, 101, 2), MPMC_OK, 0);
    CHECK_RESULT(msg_backend_ack_begin(backend, 101, 2), MPMC_ERR_STATE, 0);
    CHECK_RESULT(msg_backend_ack_begin(backend, 202, 1), MPMC_OK, 0);

    /* A fila cheia força uma drenagem seletiva antes do segundo envio. */
    ack_control_push(101, 101, (ssize_t)sizeof(uint64_t));
    ack_control_push(202, 202, (ssize_t)sizeof(uint64_t));
    ack_control_send_active = true;
    ack_control_send_would_block = 1;
    unsigned sends_before = send_calls;
    message_t message = {.sender_pid = getpid(), .sequence = 1, .operation_id = 101};
    result = msg_backend_send(backend, 1, &message);
    CHECK(result.status == MPMC_OK);
    CHECK(result.system_errno == 0);
    CHECK(send_calls == sends_before + 2);
    CHECK(last_send_type == 2);
    CHECK(last_send_nowait);
    CHECK(ack_control_sleep_calls == 1);
    CHECK(ack_control_message_count == 1);
    CHECK(ack_control_messages[0].operation_id == 202);

    /* Confirmação recebida antes da espera permanece contabilizada no registro.
     */
    ack_control_push(101, 101, (ssize_t)sizeof(uint64_t));
    result = msg_backend_ack_wait(backend, 101);
    CHECK(result.status == MPMC_OK);
    CHECK(result.system_errno == 0);
    unsigned receives_after_wait = ack_control_receive_calls;
    CHECK_RESULT(msg_backend_ack_wait(backend, 101), MPMC_OK, 0);
    CHECK(ack_control_receive_calls == receives_after_wait);

    /* A confirmação de outra operação não foi consumida pelo wait anterior. */
    result = msg_backend_ack_wait(backend, 202);
    CHECK(result.status == MPMC_OK);
    CHECK(result.system_errno == 0);

    /* O encerramento drena ACKs excedentes e remove só o registro solicitado. */
    ack_control_push(101, 101, (ssize_t)sizeof(uint64_t));
    CHECK_RESULT(msg_backend_ack_end(backend, 101), MPMC_OK, 0);
    CHECK(ack_control_message_count == 0);
    CHECK_RESULT(msg_backend_ack_wait(backend, 101), MPMC_ERR_STATE, 0);
    CHECK_RESULT(msg_backend_ack_end(backend, 101), MPMC_OK, 0);

    /* Sem registro ativo, a mensagem não solicita ACK e mantém envio bloqueante.
     */
    message.sequence = 2;
    CHECK_RESULT(msg_backend_send(backend, 1, &message), MPMC_OK, 0);
    CHECK(last_send_type == 1);
    CHECK(!last_send_nowait);
    ack_control_send_active = false;

    /* Uma interrupção não apaga o progresso nem o registro da operação. */
    CHECK_RESULT(msg_backend_ack_begin(backend, 303, 1), MPMC_OK, 0);
    ack_control_receive_failure = (mpmc_result_t){
        .status = MPMC_ERR_INTERRUPTED,
        .system_errno = EINTR,
    };
    CHECK_RESULT(msg_backend_ack_wait(backend, 303), MPMC_ERR_INTERRUPTED, EINTR);
    ack_control_push(303, 303, (ssize_t)sizeof(uint64_t));
    CHECK_RESULT(msg_backend_ack_wait(backend, 303), MPMC_OK, 0);
    CHECK_RESULT(msg_backend_ack_end(backend, 303), MPMC_OK, 0);

    /* Corpo inválido não conta como ACK; a operação pode receber um ACK válido
     * depois. */
    CHECK_RESULT(msg_backend_ack_begin(backend, 404, 1), MPMC_OK, 0);
    ack_control_push(404, 404, (ssize_t)sizeof(uint32_t));
    CHECK_RESULT(msg_backend_ack_wait(backend, 404), MPMC_ERR_STATE, 0);
    ack_control_push(404, 404, (ssize_t)sizeof(uint64_t));
    CHECK_RESULT(msg_backend_ack_wait(backend, 404), MPMC_OK, 0);
    CHECK_RESULT(msg_backend_ack_end(backend, 404), MPMC_OK, 0);

    /* Liberação também remove o registro se detectar ACK pendente malformado. */
    CHECK_RESULT(msg_backend_ack_begin(backend, 505, 1), MPMC_OK, 0);
    ack_control_push(505, 505, (ssize_t)sizeof(uint32_t));
    CHECK_RESULT(msg_backend_ack_end(backend, 505), MPMC_ERR_STATE, 0);
    CHECK_RESULT(msg_backend_ack_wait(backend, 505), MPMC_ERR_STATE, 0);

    CHECK_RESULT(msg_backend_ack_end(backend, 202), MPMC_OK, 0);
    ack_control_active = false;
    ack_control_send_active = false;
    ack_control_queue = -1;
    ack_control_data_queue = -1;
    msg_backend_destroy(backend);
}

/**
 * @brief Executa os testes das filas de dados, cenário de dois consumidores e
 * confirmações seletivas.
 * @return Zero se todos os cenários passarem; um se falha.
 */
int main(void) {
    test_arguments();
    test_queue_isolation();
    test_three_queues();
    test_fifo_order();
    test_fifo();
    test_nonblocking_receive();
    test_would_block_publishes_nothing();
    test_contract_violations();
    test_ack_control_operations();
    test_blocking_receive();
    test_send_uses_blocking_mode();
    test_queue_cleanup();
    test_send_backpressure();
    test_queue_cleanup_remove_failure();

    msg_backend_t *parent = nullptr;
    mpmc_result_t result = msg_backend_create(&parent, 2);
    if (result.status != MPMC_OK || parent == nullptr) {
        return failed("msg_backend_create falhou");
    }

    message_t first = {.sender_pid = getpid(), .sequence = 1, .operation_id = 1};
    message_t second = {.sender_pid = getpid(), .sequence = 2, .operation_id = 2};

    pid_t child_one = fork();
    if (child_one == -1) {
        msg_backend_destroy(parent);
        return failed("primeiro fork falhou");
    }
    if (child_one == 0) {
        message_t received;
        result = msg_backend_receive(parent, 1, &received, true);
        msg_backend_worker_close(parent);
        _exit(result.status == MPMC_OK ? 0 : 1);
    }

    pid_t child_two = fork();
    if (child_two == -1) {
        (void)kill(child_one, SIGTERM);
        (void)waitpid(child_one, nullptr, 0);
        msg_backend_destroy(parent);
        return failed("segundo fork falhou");
    }
    if (child_two == 0) {
        message_t received;
        result = msg_backend_receive(parent, 1, &received, true);
        msg_backend_worker_close(parent);
        _exit(result.status == MPMC_OK ? 0 : 1);
    }

    /* Os consumidores ja existem quando o produtor registra sua operacao de ACK.
     */
    if (msg_backend_ack_begin(parent, 2, 1).status != MPMC_OK
        || msg_backend_send(parent, 1, &first).status != MPMC_OK
        || msg_backend_send(parent, 1, &second).status != MPMC_OK) {
        (void)kill(child_one, SIGTERM);
        (void)kill(child_two, SIGTERM);
        (void)waitpid(child_one, nullptr, 0);
        (void)waitpid(child_two, nullptr, 0);
        msg_backend_destroy(parent);
        return failed("registro de ACK ou envio de mensagens falhou");
    }

    int status;
    for (size_t index = 0; index < 2; ++index) {
        if (wait(&status) == -1 || !WIFEXITED(status) || WEXITSTATUS(status) != 0) {
            msg_backend_destroy(parent);
            return failed("consumidor filho falhou ou evento nao foi publicado");
        }
    }

    if (msg_backend_poll_events(parent).status != MPMC_OK) {
        msg_backend_destroy(parent);
        return failed("msg_backend_poll_events falhou");
    }
    if (msg_backend_poll_events(parent).status != MPMC_OK) {
        msg_backend_destroy(parent);
        return failed("segunda drenagem falhou");
    }

    uint64_t count = UINT64_MAX;
    result = msg_backend_consumed_count(parent, 1, &count);
    if (result.status != MPMC_OK || count != 2) {
        msg_backend_destroy(parent);
        return failed("pai nao consolidou o total de dois consumidores distintos");
    }

    result = msg_backend_consumed_count(parent, 2, &count);
    if (result.status != MPMC_OK || count != 0) {
        msg_backend_destroy(parent);
        return failed("contador de estrutura independente falhou");
    }

    struct sigaction action = {.sa_handler = on_alarm};
    struct sigaction previous;
    sigemptyset(&action.sa_mask);
    if (sigaction(SIGALRM, &action, &previous) == -1) {
        msg_backend_destroy(parent);
        return failed("instalacao do timeout de ACK falhou");
    }
    alarm(2);
    result = msg_backend_ack_wait(parent, 2);
    alarm(0);
    bool confirmed = result.status == MPMC_OK;
    bool ignored = false;
    if (confirmed && msg_backend_ack_begin(parent, 1, 1).status == MPMC_OK) {
        alarm(1);
        result = msg_backend_ack_wait(parent, 1);
        alarm(0);
        ignored = result.status == MPMC_ERR_INTERRUPTED && result.system_errno == EINTR;
    }
    (void)sigaction(SIGALRM, &previous, nullptr);
    msg_backend_ack_end(parent, 2);
    msg_backend_ack_end(parent, 1);
    if (!confirmed || !ignored) {
        msg_backend_destroy(parent);
        return failed("ACK ausente ou emitido para operacao sem registro");
    }

    msg_backend_destroy(parent);
    check_no_orphan_queues();

    printf("%u verificações adicionais de filas, %u falhas\n", checks, failures);

    if (failures != 0) return 1;

    puts("Message backend event tests passed.");
    return 0;
}

/**
 * @brief Escreve o diagnóstico do cenário que falhou.
 * @param text Mensagem a apresentar em stderr.
 * @return Um, para indicar falha no processo de teste.
 * @details Também confere e remove filas que tenham sobrado no kernel, para que
 * uma falha antecipada em main() não deixe recursos System V.
 */
static int failed(const char *text) {
    fprintf(stderr, "%s\n", text);
    check_no_orphan_queues();
    return 1;
}

/**
 * @brief Permite interromper a espera por ACK no prazo do teste.
 * @param signal_number Sinal recebido; ignorado.
 */
static void on_alarm(int signal_number) {
    (void)signal_number;
}
