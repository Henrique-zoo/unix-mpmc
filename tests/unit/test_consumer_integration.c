/**
 * @file test_consumer_integration.c
 * @brief Testa consumer_run() sobre os backends SHM e MSG reais.
 * @ingroup tests
 * @details Cobre zero disponível, consumo parcial, n completo, ausência de
 * reinserção, interrupção do consumo bloqueante, liberação local no filho, ACK
 * feito pelo backend e consumo sem espera com a fila de eventos cheia (modo 2).
 * Os dois paradigmas executam a mesma sequência.
 */
#include <errno.h>
#include <poll.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "mpmc/workers/consumer.h"

#define WATCHDOG_SECONDS 30u

typedef struct {
    int status;
    int system_errno;
    unsigned consumed_count;
} child_report_t;

static int failures;
static int current_mode;

static void check(bool condition, const char *description, int line) {
    if (!condition) {
        fprintf(
            stderr,
            "consumer integration failed [mode %d, line %d]: %s\n",
            current_mode,
            line,
            description
        );
        ++failures;
    }
}
#define CHECK(condition, description) check((condition), (description), __LINE__)

/* Tratador vazio e sem SA_RESTART: faz o consumo bloqueante retornar EINTR. */
static void on_sigusr1(int signal_number) {
    (void)signal_number;
}

static bool send_messages(ipc_context_t *context, size_t index, unsigned count, uint64_t op) {
    for (unsigned n = 0; n < count; ++n) {
        message_t message = {.sender_pid = getpid(), .sequence = n + 1, .operation_id = op};
        if (clock_gettime(CLOCK_REALTIME, &message.sent_at) != 0
            || ipc_send(context, index, &message).status != MPMC_OK) {
            return false;
        }
    }
    return true;
}

static uint64_t backend_total(ipc_context_t *context, size_t index) {
    uint64_t total = UINT64_MAX;
    (void)ipc_poll_events(context);
    if (ipc_consumed_count(context, index, &total).status != MPMC_OK) {
        return UINT64_MAX;
    }
    return total;
}

static consumer_result_t run_consumer(
    ipc_context_t *context,
    size_t index,
    unsigned count,
    consumer_ack_mode_t mode,
    mpmc_result_t *result
) {
    consumer_job_t job = {
        .producer_id = index, .message_count = count, .ack_mode = mode, .operation_id = 900};
    consumer_result_t out = {.consumed_count = 99};
    *result = consumer_run(context, &job, &out);
    return out;
}

static void test_zero_available(ipc_context_t *context) {
    mpmc_result_t result;
    consumer_result_t out = run_consumer(context, 1, 3, CONSUMER_NONBLOCKING, &result);
    CHECK(
        result.status == MPMC_ERR_WOULD_BLOCK && out.error.status == result.status,
        "empty structure must report WOULD_BLOCK"
    );
    CHECK(out.consumed_count == 0, "zero available must consume zero");
    CHECK(
        result.system_errno == (current_mode == 1 ? EAGAIN : ENOMSG),
        "backend errno must be preserved"
    );
    CHECK(backend_total(context, 1) == 0, "backend total must stay zero");
}

static void test_partial_and_no_reinsert(ipc_context_t *context) {
    mpmc_result_t result;
    CHECK(send_messages(context, 1, 2, 10), "setup send");
    consumer_result_t out = run_consumer(context, 1, 5, CONSUMER_NONBLOCKING, &result);
    CHECK(
        result.status == MPMC_ERR_WOULD_BLOCK && out.consumed_count == 2,
        "partial result must report the 2 messages actually consumed"
    );
    CHECK(backend_total(context, 1) == 2, "backend total must equal consumed_count");
    out = run_consumer(context, 1, 1, CONSUMER_NONBLOCKING, &result);
    CHECK(
        result.status == MPMC_ERR_WOULD_BLOCK && out.consumed_count == 0,
        "consumed messages must not be reinserted"
    );
}

static void test_complete_leaves_remainder(ipc_context_t *context) {
    mpmc_result_t result;
    CHECK(send_messages(context, 2, 6, 11), "setup send");
    consumer_result_t out = run_consumer(context, 2, 4, CONSUMER_BLOCKING, &result);
    CHECK(result.status == MPMC_OK && out.consumed_count == 4, "blocking n completo");
    out = run_consumer(context, 2, 2, CONSUMER_NONBLOCKING, &result);
    CHECK(
        result.status == MPMC_OK && out.consumed_count == 2,
        "remainder must still be available after a complete consume"
    );
    CHECK(backend_total(context, 2) == 6, "backend total after complete consumes");
}

static void test_blocking_interrupted(ipc_context_t *context) {
    mpmc_result_t result;
    message_t message;
    CHECK(send_messages(context, 1, 5, 12), "setup send");
    run_consumer(context, 1, 2, CONSUMER_BLOCKING, &result);
    CHECK(
        ipc_receive(context, 1, &message, false).status == MPMC_OK && message.sequence == 3,
        "messages are consumed once and in order (next is #3)"
    );
    const uint64_t before = backend_total(context, 1); /* 2 + 2 + 1 */
    CHECK(before == 5, "total before the interrupted consume");

    int fds[2];
    if (pipe(fds) != 0) {
        CHECK(false, "pipe");
        return;
    }
    /* SIGUSR1 fica bloqueado até o filho instalar o tratador; sem isso, um sinal
     * precoce mataria o filho pela ação padrão. */
    sigset_t usr1_set;
    sigset_t old_set;
    sigemptyset(&usr1_set);
    sigaddset(&usr1_set, SIGUSR1);
    (void)sigprocmask(SIG_BLOCK, &usr1_set, &old_set);
    pid_t pid = fork();
    if (pid == 0) {
        struct sigaction action = {.sa_handler = on_sigusr1};
        sigemptyset(&action.sa_mask);
        (void)sigaction(SIGUSR1, &action, nullptr);
        (void)sigprocmask(SIG_SETMASK, &old_set, nullptr);
        mpmc_result_t child_result;
        /* 2 mensagens restantes; pede 4 e bloqueia esperando as demais. */
        consumer_result_t out = run_consumer(context, 1, 4, CONSUMER_BLOCKING, &child_result);
        child_report_t report = {
            child_result.status, child_result.system_errno, out.consumed_count};
        ssize_t written = write(fds[1], &report, sizeof report);
        ipc_worker_close(context); /* liberação local do filho */
        _exit(written == (ssize_t)sizeof report ? 0 : 1);
    }
    (void)sigprocmask(SIG_SETMASK, &old_set, nullptr);
    CHECK(pid > 0, "fork");
    /* O sinal pode chegar antes do bloqueio; reenviar até o filho responder. */
    child_report_t report = {};
    bool got_report = false;
    for (int attempt = 0; attempt < 200 && !got_report; ++attempt) {
        (void)kill(pid, SIGUSR1);
        struct pollfd pfd = {.fd = fds[0], .events = POLLIN};
        got_report = poll(&pfd, 1, 50) > 0;
    }
    if (got_report) {
        CHECK(read(fds[0], &report, sizeof report) == (ssize_t)sizeof report, "read report");
    }
    CHECK(got_report, "child must answer after SIGUSR1");
    if (!got_report) {
        (void)kill(pid, SIGKILL);
    }
    (void)waitpid(pid, nullptr, 0);
    close(fds[0]);
    close(fds[1]);
    CHECK(
        report.status == MPMC_ERR_INTERRUPTED && report.system_errno == EINTR,
        "interrupted blocking consume must return INTERRUPTED/EINTR"
    );
    CHECK(report.consumed_count == 2, "interruption keeps the partial count");
    CHECK(backend_total(context, 1) == before + 2, "backend total matches after interruption");
}

static void test_worker_close_keeps_supervisor(ipc_context_t *context) {
    message_t message;
    CHECK(send_messages(context, 2, 1, 13), "setup send");
    pid_t pid = fork();
    if (pid == 0) {
        ipc_worker_close(context);
        _exit(0);
    }
    CHECK(pid > 0, "fork");
    (void)waitpid(pid, nullptr, 0);
    CHECK(
        ipc_receive(context, 2, &message, false).status == MPMC_OK,
        "supervisor must still use the resources after a worker's "
        "ipc_worker_close()"
    );
}

static void test_backend_acks(ipc_context_t *context) {
    mpmc_result_t result;
    CHECK(ipc_ack_begin(context, 50, 3).status == MPMC_OK, "ack_begin");
    CHECK(send_messages(context, 1, 3, 50), "setup send");
    consumer_result_t out = run_consumer(context, 1, 3, CONSUMER_BLOCKING, &result);
    CHECK(result.status == MPMC_OK && out.consumed_count == 3, "consume the acked operation");
    CHECK(
        ipc_ack_wait(context, 50).status == MPMC_OK,
        "ACKs must arrive from the backend; the consumer worker publishes none"
    );
    (void)ipc_ack_end(context, 50);
}

/* Lote pequeno: cabe em qualquer msg_qbytes razoável sem exigir um consumidor
 * concorrente. */
#define EVENT_BACKLOG_BATCH 32u
#define EVENT_BACKLOG_ROUNDS 80u /* 2560 eventos: excede Q_evt (1024 no Linux, 128 no macOS). */
#define NONBLOCKING_CALL_LIMIT_MS 1000L

static long elapsed_ms(const struct timespec *start) {
    struct timespec now;
    (void)clock_gettime(CLOCK_MONOTONIC, &now);
    return (now.tv_sec - start->tv_sec) * 1000L + (now.tv_nsec - start->tv_nsec) / 1000000L;
}

/*
 * Consumo sem espera com Q_evt cheia (modo 2). O filho consome muito mais do
 * que cabe em Q_evt sem que ninguém drene a fila: nenhuma chamada pode ficar
 * presa, e nenhum consumo pode se perder. Só depois de o filho reportar o
 * supervisor passa a drenar Q_evt (o filho entrega os eventos acumulados ao
 * fechar).
 */
static void test_nonblocking_with_full_event_queue(ipc_context_t *context) {
    const uint64_t before = backend_total(context, 2);
    int fds[2];
    if (pipe(fds) != 0) {
        CHECK(false, "pipe");
        return;
    }
    pid_t pid = fork();
    if (pid == 0) {
        child_report_t report = {.status = MPMC_OK};
        for (unsigned round = 0; round < EVENT_BACKLOG_ROUNDS; ++round) {
            if (!send_messages(context, 2, EVENT_BACKLOG_BATCH, 20)) {
                report.status = -1;
                break;
            }
            struct timespec start;
            (void)clock_gettime(CLOCK_MONOTONIC, &start);
            mpmc_result_t result;
            consumer_result_t out =
                run_consumer(context, 2, EVENT_BACKLOG_BATCH, CONSUMER_NONBLOCKING, &result);
            report.consumed_count += out.consumed_count;
            if (result.status != MPMC_OK || elapsed_ms(&start) > NONBLOCKING_CALL_LIMIT_MS) {
                report.status = result.status != MPMC_OK ? (int)result.status : -2;
                break;
            }
        }
        ssize_t written = write(fds[1], &report, sizeof report);
        ipc_worker_close(context); /* entrega o acumulado; pode esperar o supervisor drenar */
        _exit(written == (ssize_t)sizeof report ? 0 : 1);
    }
    CHECK(pid > 0, "fork");

    child_report_t report = {.status = -3};
    struct pollfd pfd = {.fd = fds[0], .events = POLLIN};
    const bool got_report =
        poll(&pfd, 1, 15000) > 0 && read(fds[0], &report, sizeof report) == (ssize_t)sizeof report;
    CHECK(got_report, "nonblocking consumer must not hang on a full event queue");
    if (!got_report) {
        (void)kill(pid, SIGKILL);
    }
    CHECK(report.status == MPMC_OK, "every nonblocking call must succeed without waiting");
    CHECK(
        report.consumed_count == EVENT_BACKLOG_BATCH * EVENT_BACKLOG_ROUNDS,
        "child must consume every message it requested"
    );

    int wait_status = 0;
    pid_t reaped = 0;
    for (int attempt = 0; attempt < 1500 && reaped == 0; ++attempt) {
        (void)ipc_poll_events(context); /* o supervisor drena enquanto o filho encerra */
        reaped = waitpid(pid, &wait_status, WNOHANG);
        if (reaped == 0) {
            (void)poll(nullptr, 0, 10);
        }
    }
    CHECK(
        reaped == pid && WIFEXITED(wait_status) && WEXITSTATUS(wait_status) == 0,
        "child must exit once the supervisor drains the events"
    );
    close(fds[0]);
    close(fds[1]);
    CHECK(
        backend_total(context, 2) == before + EVENT_BACKLOG_BATCH * EVENT_BACKLOG_ROUNDS,
        "no consumption may be lost when Q_evt was full"
    );
}

int main(void) {
    (void)alarm(WATCHDOG_SECONDS);
    for (current_mode = 1; current_mode <= 2; ++current_mode) {
        ipc_context_t *context = nullptr;
        if (ipc_init(&context, 2, (ipc_mode_t)current_mode).status != MPMC_OK) {
            fprintf(stderr, "consumer integration: ipc_init failed (mode %d)\n", current_mode);
            return 1;
        }
        test_zero_available(context);
        test_partial_and_no_reinsert(context);
        test_complete_leaves_remainder(context);
        test_blocking_interrupted(context);
        test_worker_close_keeps_supervisor(context);
        /* ACK do modo 1 ainda é stub (issue #11): só o modo 2 pode verificá-lo. */
        if (current_mode == 2) {
            test_backend_acks(context);
            test_nonblocking_with_full_event_queue(context);
        }
        ipc_destroy(context);
    }
    if (failures != 0) {
        fprintf(stderr, "%d consumer integration failure(s)\n", failures);
        return 1;
    }
    puts("Consumer integration tests passed.");
    return 0;
}
