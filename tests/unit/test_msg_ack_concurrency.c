/**
 * @file test_msg_ack_concurrency.c
 * @brief Teste do backend MSG real: correlação de ACK por operação sob filas
 * finitas.
 * @ingroup tests
 * @details Usa filas System V reais e processos reais, com a capacidade
 * reduzida por msgctl(IPC_SET) para que a contrapressão ocorra de forma
 * determinística. Cobre:
 * - várias operações concorrentes, com contagens independentes e sem espera
 * circular;
 * - ACK que chega antes de ack_wait() e ACK de outra operação que não satisfaz
 * a espera;
 * - operação sem registro (ack=2): nenhum ACK é emitido e ack_wait() é
 * recusada;
 * - ACKs tardios de operação encerrada: sem msg_backend_ack_abandon() o
 * consumidor trava (controle negativo); com ele, o pai que atua como supervisor
 * o libera;
 * - argumentos, idempotência e seletividade de msg_backend_ack_abandon().
 * Todo bloqueio tem prazo: esperas de ACK usam alarm() e filhos são recolhidos
 * com prazo, sendo mortos por SIGKILL ao vencer. Cada cenário cria o próprio
 * backend e só o pai remove as filas, o que se confere por msgctl() no final.
 * Se o teste for interrompido (SIGINT, SIGTERM, SIGHUP) ou travar além do
 * prazo, o tratador de sinais instalado em main() mata os filhos e remove as
 * filas do backend em uso, para não deixar recursos System V órfãos (o pool de
 * mensagens do macOS é pequeno e compartilhado). Reaproveita o padrão de
 * test_msg_stress.c e os cenários mockados de test_msg_backend.c, sem
 * substituí-los.
 */
#include <errno.h>
#include <limits.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/msg.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

/* O teste precisa dos identificadores das filas, que são privados do backend.
 */
#include "../../src/ipc/msg_backend.c"

/** @brief Prazo de recolhimento de filhos em cenários que devem concluir, em
 * ms. */
#define CHILD_DEADLINE_MS 10000U
/** @brief Tempo em que um consumidor travado deve permanecer travado no
 * controle negativo. */
#define WEDGE_OBSERVE_MS 400U
/** @brief Prazo máximo de uma espera de ACK feita no processo do teste, em
 * segundos. */
#define WAIT_ALARM_SECONDS 5U
/** @brief Quantidade máxima de filhos simultâneos. */
#define MAX_KIDS 8U

static unsigned checks;
static unsigned failures;
static pid_t kids[MAX_KIDS];
/** @brief Backend em uso pelo cenário atual, para limpeza em caso de sinal
 * fatal. */
static msg_backend_t *volatile live_backend;
/** @brief PID do processo de teste; filhos herdam os tratadores e não removem
 * filas. */
static pid_t owner_pid;

/** @brief Registra uma propriedade sem depender de assert(). */
#define CHECK(condition)                                                                           \
    do {                                                                                           \
        ++checks;                                                                                  \
        if (!(condition)) {                                                                        \
            ++failures;                                                                            \
            fprintf(stderr, "FALHA %s:%d: %s\n", __FILE__, __LINE__, #condition);                  \
        }                                                                                          \
    } while (0)

/** @brief Tratador de SIGALRM: interrompe esperas do pai e mata filhos
 * próprios. */
static void on_alarm(int signal_number) {
    (void)signal_number;
    for (size_t index = 0; index < MAX_KIDS; ++index) {
        if (kids[index] > 0) (void)kill(kids[index], SIGKILL);
    }
}

/** @brief Remove as filas do backend em uso; só chama funções seguras em
 * tratador de sinal. */
static void remove_live_queues(void) {
    msg_backend_t *backend = live_backend;
    if (backend == nullptr) return;
    for (size_t index = 0; index < backend->producer_count; ++index) {
        (void)msgctl(backend->data_queues[index], IPC_RMID, nullptr);
    }
    (void)msgctl(backend->event_queue, IPC_RMID, nullptr);
    (void)msgctl(backend->ack_queue, IPC_RMID, nullptr);
}

/**
 * @brief Tratador de SIGINT/SIGTERM/SIGHUP: mata filhos, remove filas e
 * termina.
 * @details Filhos herdam este tratador; neles apenas terminam, sem remover
 * filas do pai.
 */
static void on_fatal(int signal_number) {
    if (getpid() == owner_pid) {
        for (size_t index = 0; index < MAX_KIDS; ++index) {
            if (kids[index] > 0) (void)kill(kids[index], SIGKILL);
        }
        remove_live_queues();
    }
    _exit(128 + signal_number);
}

/** @brief Reduz o limite de bytes de uma fila; reduzir não exige privilégio. */
static bool set_qbytes(int queue, unsigned long bytes) {
    struct msqid_ds info;
    if (msgctl(queue, IPC_STAT, &info) == -1) return false;
    info.msg_qbytes = bytes;
    return msgctl(queue, IPC_SET, &info) == 0;
}

/** @brief Quantidade de mensagens na fila; ULONG_MAX se a fila não existe mais.
 */
static unsigned long queued(int queue) {
    struct msqid_ds info;
    if (msgctl(queue, IPC_STAT, &info) == -1) return ULONG_MAX;
    return (unsigned long)info.msg_qnum;
}

/** @brief Pausa de 2 ms entre verificações de filhos. */
static void tick(void) {
    const struct timespec delay = {.tv_sec = 0, .tv_nsec = 2000000};
    (void)nanosleep(&delay, nullptr);
}

static unsigned long elapsed_ms(const struct timespec *start) {
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    return (unsigned long)((now.tv_sec - start->tv_sec) * 1000L
        + (now.tv_nsec - start->tv_nsec) / 1000000L);
}

/**
 * @brief Recolhe um filho com prazo; opcionalmente drena eventos como o
 * supervisor faz.
 * @return Código de saída (0..255); 128+sinal se morto por sinal; -1 se o prazo
 * venceu e o filho foi morto por SIGKILL.
 */
static int reap(size_t slot, unsigned deadline_ms, msg_backend_t *supervisor) {
    struct timespec start;
    clock_gettime(CLOCK_MONOTONIC, &start);
    for (;;) {
        if (supervisor != nullptr) (void)msg_backend_poll_events(supervisor);
        int status = 0;
        pid_t pid = waitpid(kids[slot], &status, WNOHANG);
        if (pid == kids[slot]) {
            kids[slot] = 0;
            return WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status);
        }
        if (pid == -1 && errno != EINTR) {
            kids[slot] = 0;
            return -1;
        }
        if (elapsed_ms(&start) > deadline_ms) {
            (void)kill(kids[slot], SIGKILL);
            (void)waitpid(kids[slot], &status, 0);
            kids[slot] = 0;
            return -1;
        }
        tick();
    }
}

/** @brief Verifica que todas as filas do backend sumiram do kernel após
 * destroy(). */
static void destroy_and_check(msg_backend_t *backend) {
    int ids[16];
    size_t count = 0;
    for (size_t index = 0; index < backend->producer_count && count < 13; ++index) {
        ids[count++] = backend->data_queues[index];
    }
    ids[count++] = backend->event_queue;
    ids[count++] = backend->ack_queue;
    live_backend = nullptr;
    msg_backend_destroy(backend);
    for (size_t index = 0; index < count; ++index) {
        CHECK(queued(ids[index]) == ULONG_MAX);
    }
}

/** @brief Três operações concorrentes, filas de 4 dados, 3 ACKs e 2 eventos. */
static void test_concurrent_operations(void) {
    msg_backend_t *backend = nullptr;
    CHECK(msg_backend_create(&backend, 1).status == MPMC_OK && backend != nullptr);
    if (backend == nullptr) return;
    live_backend = backend;
    CHECK(set_qbytes(backend->data_queues[0], 4 * sizeof(message_t)));
    CHECK(set_qbytes(backend->ack_queue, 3 * sizeof(ack_event_t)));
    CHECK(set_qbytes(backend->event_queue, 2 * sizeof(consumption_event_t)));

    const uint64_t operations[3] = {11, 22, 33};
    const unsigned counts[3] = {20, 7, 40};
    const unsigned per_consumer[2] = {34, 33};
    for (size_t slot = 0; slot < 3; ++slot) {
        kids[slot] = fork();
        if (kids[slot] == 0) {
            mpmc_result_t result = msg_backend_ack_begin(backend, operations[slot], counts[slot]);
            for (unsigned seq = 1; result.status == MPMC_OK && seq <= counts[slot]; ++seq) {
                message_t message = {
                    .sender_pid = getpid(), .sequence = seq, .operation_id = operations[slot]};
                result = msg_backend_send(backend, 1, &message);
            }
            if (result.status == MPMC_OK) {
                result = msg_backend_ack_wait(backend, operations[slot]);
            }
            mpmc_result_t ended = msg_backend_ack_end(backend, operations[slot]);
            _exit(result.status == MPMC_OK && ended.status == MPMC_OK ? 0 : 1);
        }
        CHECK(kids[slot] > 0);
    }
    for (size_t index = 0; index < 2; ++index) {
        size_t slot = 3 + index;
        kids[slot] = fork();
        if (kids[slot] == 0) {
            for (unsigned k = 0; k < per_consumer[index]; ++k) {
                message_t message;
                if (msg_backend_receive(backend, 1, &message, true).status != MPMC_OK) _exit(1);
            }
            _exit(0);
        }
        CHECK(kids[slot] > 0);
    }

    alarm(WAIT_ALARM_SECONDS * 4);
    for (size_t slot = 0; slot < 5; ++slot) {
        if (kids[slot] > 0) CHECK(reap(slot, CHILD_DEADLINE_MS, backend) == 0);
    }
    alarm(0);

    CHECK(msg_backend_poll_events(backend).status == MPMC_OK);
    uint64_t consumed = 0;
    CHECK(msg_backend_consumed_count(backend, 1, &consumed).status == MPMC_OK);
    CHECK(consumed == 67);
    CHECK(queued(backend->ack_queue) == 0);
    CHECK(queued(backend->data_queues[0]) == 0);
    destroy_and_check(backend);
}

/** @brief ACK antecipado, isolamento entre operações e ack=2 sem ACK. */
static void test_early_ack_and_unregistered(void) {
    msg_backend_t *backend = nullptr;
    CHECK(msg_backend_create(&backend, 1).status == MPMC_OK && backend != nullptr);
    if (backend == nullptr) return;
    live_backend = backend;

    CHECK(msg_backend_ack_begin(backend, 5, 3).status == MPMC_OK);
    CHECK(msg_backend_ack_begin(backend, 6, 1).status == MPMC_OK);
    for (unsigned seq = 1; seq <= 3; ++seq) {
        message_t message = {.sender_pid = getpid(), .sequence = seq, .operation_id = 5};
        CHECK(msg_backend_send(backend, 1, &message).status == MPMC_OK);
    }
    kids[0] = fork();
    if (kids[0] == 0) {
        for (unsigned k = 0; k < 3; ++k) {
            message_t message;
            if (msg_backend_receive(backend, 1, &message, true).status != MPMC_OK) _exit(1);
        }
        _exit(0);
    }
    CHECK(kids[0] > 0);
    CHECK(reap(0, CHILD_DEADLINE_MS, backend) == 0);

    /* Os três ACKs chegaram antes de qualquer ack_wait(): continuam na fila. */
    CHECK(queued(backend->ack_queue) == 3);

    /* A operação 6 não pode ser satisfeita pelos ACKs da 5. */
    alarm(1);
    mpmc_result_t other = msg_backend_ack_wait(backend, 6);
    alarm(0);
    CHECK(other.status == MPMC_ERR_INTERRUPTED && other.system_errno == EINTR);
    CHECK(queued(backend->ack_queue) == 3);

    alarm(WAIT_ALARM_SECONDS);
    mpmc_result_t waited = msg_backend_ack_wait(backend, 5);
    alarm(0);
    CHECK(waited.status == MPMC_OK);
    CHECK(queued(backend->ack_queue) == 0);
    CHECK(msg_backend_ack_end(backend, 5).status == MPMC_OK);
    CHECK(msg_backend_ack_end(backend, 6).status == MPMC_OK);

    /* ack=2: sem registro, a mensagem é de tipo 1, ninguém emite ACK e a espera é
     * recusada. */
    message_t plain = {.sender_pid = getpid(), .sequence = 1, .operation_id = 9};
    message_t out;
    CHECK(msg_backend_send(backend, 1, &plain).status == MPMC_OK);
    CHECK(msg_backend_receive(backend, 1, &out, false).status == MPMC_OK);
    CHECK(out.operation_id == 9);
    CHECK(queued(backend->ack_queue) == 0);
    CHECK(msg_backend_ack_wait(backend, 9).status == MPMC_ERR_STATE);

    destroy_and_check(backend);
}

/**
 * @brief A fila de eventos cheia não pode reter ACKs de mensagens já retiradas.
 * @details O consumidor processa duas mensagens, mas Q_evt comporta só um
 * evento. Ele fica bloqueado publicando o segundo evento; ainda assim, como
 * ACKs são publicados primeiro, o produtor pode concluir ack_wait(). Drenar
 * Q_evt então libera o consumidor e permite a limpeza normal.
 */
static void test_ack_before_full_event_queue(void) {
    msg_backend_t *backend = nullptr;
    CHECK(msg_backend_create(&backend, 1).status == MPMC_OK && backend != nullptr);
    if (backend == nullptr) return;
    live_backend = backend;
    CHECK(set_qbytes(backend->event_queue, sizeof(consumption_event_t)));
    CHECK(msg_backend_ack_begin(backend, 88, 2).status == MPMC_OK);

    for (unsigned seq = 1; seq <= 2; ++seq) {
        message_t message = {.sender_pid = getpid(), .sequence = seq, .operation_id = 88};
        CHECK(msg_backend_send(backend, 1, &message).status == MPMC_OK);
    }

    kids[0] = fork();
    if (kids[0] == 0) {
        for (unsigned index = 0; index < 2; ++index) {
            message_t message;
            if (msg_backend_receive(backend, 1, &message, true).status != MPMC_OK) _exit(1);
        }
        _exit(0);
    }
    CHECK(kids[0] > 0);
    if (kids[0] <= 0) {
        (void)msg_backend_ack_end(backend, 88);
        destroy_and_check(backend);
        return;
    }

    struct timespec start;
    clock_gettime(CLOCK_MONOTONIC, &start);
    while (queued(backend->ack_queue) < 2 && elapsed_ms(&start) < CHILD_DEADLINE_MS) {
        tick();
    }
    CHECK(queued(backend->ack_queue) == 2);
    CHECK(queued(backend->event_queue) == 1);

    alarm(WAIT_ALARM_SECONDS);
    mpmc_result_t waited = msg_backend_ack_wait(backend, 88);
    alarm(0);
    CHECK(waited.status == MPMC_OK);
    CHECK(waitpid(kids[0], nullptr, WNOHANG) == 0);

    CHECK(msg_backend_poll_events(backend).status == MPMC_OK);
    CHECK(reap(0, CHILD_DEADLINE_MS, backend) == 0);
    CHECK(msg_backend_ack_end(backend, 88).status == MPMC_OK);
    CHECK(msg_backend_poll_events(backend).status == MPMC_OK);
    uint64_t consumed = 0;
    CHECK(msg_backend_consumed_count(backend, 1, &consumed).status == MPMC_OK);
    CHECK(consumed == 2);
    destroy_and_check(backend);
}

/**
 * @brief Operação encerrada antes do consumo: ACKs tardios e fila de controle
 * de 2 posições.
 * @param abandon Se verdadeiro, o pai (supervisor) chama
 * msg_backend_ack_abandon().
 * @return Resultado de reap() do consumidor.
 */
static int run_late_ack(bool abandon) {
    msg_backend_t *backend = nullptr;
    CHECK(msg_backend_create(&backend, 1).status == MPMC_OK && backend != nullptr);
    if (backend == nullptr) return -2;
    live_backend = backend;
    CHECK(set_qbytes(backend->ack_queue, 2 * sizeof(ack_event_t)));

    /* Produtor ack=1 que desiste depois de enviar 4 mensagens (erro ou
     * cancelamento). */
    CHECK(msg_backend_ack_begin(backend, 77, 4).status == MPMC_OK);
    for (unsigned seq = 1; seq <= 4; ++seq) {
        message_t message = {.sender_pid = getpid(), .sequence = seq, .operation_id = 77};
        CHECK(msg_backend_send(backend, 1, &message).status == MPMC_OK);
    }
    CHECK(msg_backend_ack_end(backend, 77).status == MPMC_OK);
    if (abandon) CHECK(msg_backend_ack_abandon(backend, 77).status == MPMC_OK);

    kids[0] = fork();
    if (kids[0] == 0) {
        for (unsigned k = 0; k < 4; ++k) {
            message_t message;
            if (msg_backend_receive(backend, 1, &message, true).status != MPMC_OK) _exit(1);
        }
        _exit(0);
    }
    CHECK(kids[0] > 0);
    int rc = reap(0, abandon ? CHILD_DEADLINE_MS : WEDGE_OBSERVE_MS, backend);
    if (abandon) {
        CHECK(queued(backend->ack_queue) == 0);
        CHECK(queued(backend->data_queues[0]) == 0);
    }
    destroy_and_check(backend);
    return rc;
}

static void test_late_acks(void) {
    /* Controle negativo: sem abandon, o consumidor fica preso em msgsnd() da fila
     * de ACK. */
    CHECK(run_late_ack(false) == -1);
    /* Com abandon, poll_events() libera o consumidor e a fila de ACK termina
     * vazia. */
    CHECK(run_late_ack(true) == 0);
}

/** @brief Argumentos, idempotência e seletividade de msg_backend_ack_abandon().
 */
static void test_abandon_contract(void) {
    CHECK(msg_backend_ack_abandon(nullptr, 1).status == MPMC_ERR_INVALID_ARG);
    msg_backend_t *backend = nullptr;
    CHECK(msg_backend_create(&backend, 1).status == MPMC_OK && backend != nullptr);
    if (backend == nullptr) return;
    live_backend = backend;

    CHECK(msg_backend_ack_abandon(backend, 0).status == MPMC_ERR_INVALID_ARG);
    CHECK(msg_backend_ack_abandon(backend, (uint64_t)LONG_MAX + 1).status == MPMC_ERR_INVALID_ARG);
    CHECK(backend->abandoned_count == 0);

    CHECK(msg_backend_ack_abandon(backend, 40).status == MPMC_OK);
    CHECK(msg_backend_ack_abandon(backend, 40).status == MPMC_OK);
    CHECK(backend->abandoned_count == 1);

    /* ACKs injetados: o da operação abandonada some no poll; o de outra operação
     * fica. */
    ack_event_t late = {.operation_id = 40};
    ack_event_t live = {.operation_id = 41};
    CHECK(sysv_msg_send(backend->ack_queue, 40, &late, sizeof(late), true).status == MPMC_OK);
    CHECK(sysv_msg_send(backend->ack_queue, 41, &live, sizeof(live), true).status == MPMC_OK);
    CHECK(sysv_msg_send(backend->ack_queue, 40, &late, sizeof(late), true).status == MPMC_OK);
    CHECK(queued(backend->ack_queue) == 3);
    CHECK(msg_backend_poll_events(backend).status == MPMC_OK);
    CHECK(queued(backend->ack_queue) == 1);

    /* Um produtor vivo ainda recebe o próprio ACK depois da purga. */
    CHECK(msg_backend_ack_begin(backend, 41, 1).status == MPMC_OK);
    alarm(WAIT_ALARM_SECONDS);
    CHECK(msg_backend_ack_wait(backend, 41).status == MPMC_OK);
    alarm(0);
    CHECK(msg_backend_ack_end(backend, 41).status == MPMC_OK);
    destroy_and_check(backend);
}

/** @brief Instala SIGALRM (interrompe esperas, mata filhos) e os tratadores
 * fatais. */
static bool install_handlers(void) {
    struct sigaction alarm_action = {.sa_handler = on_alarm};
    struct sigaction fatal_action = {.sa_handler = on_fatal};
    sigemptyset(&alarm_action.sa_mask);
    sigemptyset(&fatal_action.sa_mask);
    return sigaction(SIGALRM, &alarm_action, nullptr) == 0
        && sigaction(SIGINT, &fatal_action, nullptr) == 0
        && sigaction(SIGTERM, &fatal_action, nullptr) == 0
        && sigaction(SIGHUP, &fatal_action, nullptr) == 0;
}

int main(void) {
    owner_pid = getpid();
    if (!install_handlers()) {
        perror("sigaction");
        return 1;
    }
    test_concurrent_operations();
    test_early_ack_and_unregistered();
    test_ack_before_full_event_queue();
    test_late_acks();
    test_abandon_contract();
    printf("%u verificações de ACK concorrente MSG, %u falhas\n", checks, failures);
    if (failures != 0) return 1;
    puts("Message backend ACK concurrency tests passed.");
    return 0;
}
