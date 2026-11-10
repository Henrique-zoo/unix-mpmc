/**
 * @file test_consumer_acceptance.c
 * @brief Critérios de aceite do consumo bloqueante e não bloqueante, nos
 * backends reais.
 * @ingroup tests
 * @details Cada caso corresponde a um critério de aceite, com processos
 * distintos: (1) o consumo bloqueante permanece aguardando e completa quando os
 * itens surgem; (2) o consumo não bloqueante retorna imediatamente e conta o
 * parcial corretamente, inclusive com a fila de confirmações (Q_ack) saturada
 * no modo 2; (3) cada mensagem é consumida no máximo uma vez (coberto em
 * test_consumer_concurrency); (4) consumidores concorrentes e consumo parcial
 * não alteram a correlação de ACK dos produtores (modo 2; o ACK do modo 1
 * pertence à issue #11). Todo o teste roda sob um alarme de segurança: uma
 * regressão vira falha, não travamento.
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
#include "mpmc/workers/producer.h"

#define WATCHDOG_SECONDS 90u
#define PROMPT_SECONDS 0.25
#define WAIT_MS 300

static int failures;
static int current_mode;

static void check(bool condition, const char *description, int line) {
    if (!condition) {
        fprintf(
            stderr,
            "consumer acceptance failed [mode %d, line %d]: %s\n",
            current_mode,
            line,
            description
        );
        ++failures;
    }
}
#define CHECK(condition, description) check((condition), (description), __LINE__)

typedef struct {
    unsigned consumed;
    int status;
} report_t;

static double monotonic_seconds(void) {
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    return (double)now.tv_sec + (double)now.tv_nsec / 1e9;
}

static void sleep_ms(long milliseconds) {
    struct timespec delay = {.tv_sec = 0, .tv_nsec = milliseconds * 1000000L};
    nanosleep(&delay, nullptr);
}

static bool send_messages(ipc_context_t *context, size_t index, unsigned count, uint64_t op) {
    for (unsigned n = 1; n <= count; ++n) {
        message_t message = {.sender_pid = getpid(), .sequence = n, .operation_id = op};
        if (ipc_send(context, index, &message).status != MPMC_OK) {
            return false;
        }
    }
    return true;
}

static bool still_running(pid_t pid) {
    int status = 0;
    return waitpid(pid, &status, WNOHANG) == 0;
}

static bool exited_ok(pid_t pid) {
    int status = 0;
    return waitpid(pid, &status, 0) == pid && WIFEXITED(status) && WEXITSTATUS(status) == 0;
}

/* Lê um relatório do filho em até timeout_ms; false se não chegou. */
static bool read_report(int fd, report_t *report, int timeout_ms) {
    struct pollfd watch = {.fd = fd, .events = POLLIN};
    if (poll(&watch, 1, timeout_ms) != 1) {
        return false;
    }
    return read(fd, report, sizeof(*report)) == (ssize_t)sizeof(*report);
}

/* Critério 1
 * ------------------------------------------------------------------------ */
static void test_blocking_waits_then_completes(ipc_context_t *context) {
    int fds[2];
    CHECK(pipe(fds) == 0, "pipe");
    pid_t pid = fork();
    if (pid == 0) {
        close(fds[0]);
        (void)alarm(20);
        consumer_job_t job = {
            .producer_id = 1,
            .message_count = 5,
            .ack_mode = CONSUMER_BLOCKING,
            .operation_id = 77,
        };
        consumer_result_t out;
        mpmc_result_t result = consumer_run(context, &job, &out);
        report_t report = {.consumed = out.consumed_count, .status = (int)result.status};
        _exit(write(fds[1], &report, sizeof(report)) == (ssize_t)sizeof(report) ? 0 : 1);
    }
    close(fds[1]);
    CHECK(pid > 0, "fork");

    sleep_ms(WAIT_MS);
    CHECK(still_running(pid), "empty structure: blocking consumer must keep waiting");
    CHECK(send_messages(context, 1, 3, 5), "send 3 of 5");
    sleep_ms(WAIT_MS);
    CHECK(still_running(pid), "3 of 5 available: must not return a partial result");
    CHECK(send_messages(context, 1, 2, 5), "send the last 2");

    report_t report = {.consumed = 999, .status = -1};
    CHECK(read_report(fds[0], &report, 3000), "must complete soon after the 5th item");
    CHECK(report.consumed == 5 && report.status == MPMC_OK, "5 consumed with MPMC_OK");
    CHECK(exited_ok(pid), "blocking consumer exits cleanly");
    close(fds[0]);
}

/* Critério 2
 * ------------------------------------------------------------------------ */
static void test_nonblocking_partial_count(ipc_context_t *context) {
    consumer_job_t job = {
        .producer_id = 1,
        .message_count = 5,
        .ack_mode = CONSUMER_NONBLOCKING,
        .operation_id = 78,
    };
    consumer_result_t out;
    uint64_t before = 0;
    (void)ipc_poll_events(context);
    CHECK(ipc_consumed_count(context, 1, &before).status == MPMC_OK, "total before");

    double started = monotonic_seconds();
    mpmc_result_t result = consumer_run(context, &job, &out);
    CHECK(monotonic_seconds() - started < PROMPT_SECONDS, "empty: must return immediately");
    CHECK(
        result.status == MPMC_ERR_WOULD_BLOCK && out.consumed_count == 0,
        "empty: WOULD_BLOCK with zero consumed"
    );

    CHECK(send_messages(context, 1, 3, 5), "send 3");
    started = monotonic_seconds();
    result = consumer_run(context, &job, &out);
    CHECK(monotonic_seconds() - started < PROMPT_SECONDS, "partial: must return immediately");
    CHECK(
        result.status == MPMC_ERR_WOULD_BLOCK && out.consumed_count == 3,
        "3 of 5: WOULD_BLOCK with consumed_count 3"
    );

    uint64_t after = 0;
    (void)ipc_poll_events(context);
    CHECK(ipc_consumed_count(context, 1, &after).status == MPMC_OK, "total after");
    CHECK(after == before + 3, "backend total grows by exactly the partial count");

    CHECK(send_messages(context, 1, 5, 5), "send 5");
    result = consumer_run(context, &job, &out);
    CHECK(result.status == MPMC_OK && out.consumed_count == 5, "5 of 5: complete");
}

/* Critério 4 (modo 2)
 * ----------------------------------------------------------------- */
static void test_ack_correlation_with_partial_and_concurrent(ipc_context_t *context) {
    enum { PER = 100, FIRST_OP = 1000 };
    pid_t producers[2];
    for (unsigned k = 0; k < 2; ++k) {
        producers[k] = fork();
        if (producers[k] == 0) {
            (void)alarm(30);
            producer_job_t job = {
                .producer_id = k + 1,
                .message_count = PER,
                .ack_mode = PRODUCER_ACK_WAIT,
                .operation_id = FIRST_OP + k * 1000,
            };
            _exit(producer_run(context, &job).status == MPMC_OK ? 0 : 1);
        }
    }
    sleep_ms(WAIT_MS);

    /* Consumo parcial; o operation_id do consumidor coincide com o de OUTRO
     * produtor. */
    for (size_t index = 1; index <= 2; ++index) {
        consumer_job_t job = {
            .producer_id = index,
            .message_count = 40,
            .ack_mode = CONSUMER_NONBLOCKING,
            .operation_id = index == 1 ? 2000 : 1000,
        };
        consumer_result_t out;
        mpmc_result_t result = consumer_run(context, &job, &out);
        CHECK(result.status == MPMC_OK && out.consumed_count == 40, "partial 40 of 100");
    }
    sleep_ms(WAIT_MS);
    CHECK(
        still_running(producers[0]) && still_running(producers[1]),
        "producers keep waiting for the 60 remaining ACKs"
    );

    pid_t consumers[4];
    unsigned spawned = 0;
    for (size_t index = 1; index <= 2; ++index) {
        for (unsigned h = 0; h < 2; ++h) {
            pid_t pid = fork();
            if (pid == 0) {
                (void)alarm(30);
                consumer_job_t job = {
                    .producer_id = index,
                    .message_count = 30,
                    .ack_mode = CONSUMER_BLOCKING,
                    .operation_id = FIRST_OP + h * 1000,
                };
                consumer_result_t out;
                mpmc_result_t result = consumer_run(context, &job, &out);
                _exit(result.status == MPMC_OK && out.consumed_count == 30 ? 0 : 1);
            }
            consumers[spawned++] = pid;
        }
    }
    unsigned consumers_ok = 0;
    for (unsigned c = 0; c < spawned; ++c) {
        consumers_ok += exited_ok(consumers[c]) ? 1 : 0;
    }
    CHECK(consumers_ok == 4, "concurrent blocking consumers complete their shares");
    CHECK(
        exited_ok(producers[0]) && exited_ok(producers[1]),
        "each producer receives exactly its own ACKs and finishes"
    );
}

/* Critério 2 sob saturação de Q_ack (modo 2)
 * ------------------------------------------ */
/*
 * O "produtor" é este processo: registra as operações mas só drena os ACKs no
 * fim. Cada rodada envia um lote pequeno, que um filho consome sem espera.
 * Depois de ~3000 ACKs Q_ack lota (mesmo nos limites padrão mais folgados); o
 * consumo sem espera deve seguir retornando na hora, e nenhum ACK pode se
 * perder.
 */
static void test_nonblocking_with_full_ack_queue(ipc_context_t *context) {
    enum { BATCH = 32, ROUNDS = 96, OPS = 3, ROUNDS_PER_OP = ROUNDS / OPS };
    for (uint64_t op = 1; op <= OPS; ++op) {
        CHECK(ipc_ack_begin(context, 500 + op, BATCH * ROUNDS_PER_OP).status == MPMC_OK, "begin");
    }
    (void)ipc_poll_events(context);
    uint64_t before = 0;
    CHECK(ipc_consumed_count(context, 1, &before).status == MPMC_OK, "total before");
    pid_t children[ROUNDS];
    unsigned spawned = 0;
    bool all_prompt = true;
    for (unsigned round = 0; round < ROUNDS; ++round) {
        const uint64_t op = 501 + round / ROUNDS_PER_OP;
        if (!send_messages(context, 1, BATCH, op)) {
            CHECK(false, "send batch");
            break;
        }
        int fds[2];
        if (pipe(fds) != 0) {
            CHECK(false, "pipe");
            break;
        }
        pid_t pid = fork();
        if (pid == 0) {
            close(fds[0]);
            (void)alarm(60);
            consumer_job_t job = {
                .producer_id = 1,
                .message_count = BATCH,
                .ack_mode = CONSUMER_NONBLOCKING,
                .operation_id = 900,
            };
            consumer_result_t out;
            mpmc_result_t result = consumer_run(context, &job, &out);
            report_t report = {.consumed = out.consumed_count, .status = (int)result.status};
            if (write(fds[1], &report, sizeof(report)) != (ssize_t)sizeof(report)) _exit(2);
            /* O fechamento entrega os ACKs devidos, aguardando se Q_ack ainda estiver
             * cheia. */
            ipc_worker_close(context);
            _exit(0);
        }
        close(fds[1]);
        report_t report = {};
        const bool answered = read_report(fds[0], &report, 3000);
        close(fds[0]);
        if (!answered) {
            CHECK(false, "non-blocking consumer must return even with Q_ack full");
            kill(pid, SIGKILL);
            (void)waitpid(pid, nullptr, 0);
            all_prompt = false;
            break;
        }
        CHECK(
            report.status == MPMC_OK && report.consumed == BATCH,
            "each batch is fully consumed with MPMC_OK"
        );
        children[spawned++] = pid;
    }
    if (!all_prompt) {
        for (unsigned c = 0; c < spawned; ++c) {
            kill(children[c], SIGKILL);
            (void)waitpid(children[c], nullptr, 0);
        }
        return;
    }

    /* Drenar como produtor dono: todo ACK devido tem de chegar (nenhum perdido).
     */
    for (uint64_t op = 1; op <= OPS; ++op) {
        CHECK(
            ipc_ack_wait(context, 500 + op).status == MPMC_OK,
            "every ACK, including the deferred ones, must reach the producer"
        );
        CHECK(ipc_ack_end(context, 500 + op).status == MPMC_OK, "ack_end");
    }
    /* No fechamento os filhos também entregam os eventos de consumo acumulados;
     * como supervisor, este processo drena Q_evt enquanto os recolhe. */
    unsigned closed_ok = 0;
    unsigned reaped = 0;
    const double deadline = monotonic_seconds() + 30.0;
    bool done[ROUNDS] = {false};
    while (reaped < spawned && monotonic_seconds() < deadline) {
        (void)ipc_poll_events(context);
        for (unsigned c = 0; c < spawned; ++c) {
            int status = 0;
            if (!done[c] && waitpid(children[c], &status, WNOHANG) == children[c]) {
                done[c] = true;
                ++reaped;
                closed_ok += WIFEXITED(status) && WEXITSTATUS(status) == 0 ? 1 : 0;
            }
        }
        sleep_ms(2);
    }
    CHECK(closed_ok == spawned, "consumers finish closing after the ACKs are drained");
    (void)ipc_poll_events(context);
    uint64_t total = 0;
    CHECK(ipc_consumed_count(context, 1, &total).status == MPMC_OK, "total");
    CHECK(total == before + (uint64_t)BATCH * ROUNDS, "backend total grows by everything consumed");
}

int main(void) {
    (void)alarm(WATCHDOG_SECONDS);
    for (current_mode = 1; current_mode <= 2; ++current_mode) {
        ipc_context_t *context = nullptr;
        if (ipc_init(&context, 2, (ipc_mode_t)current_mode).status != MPMC_OK) {
            fprintf(stderr, "consumer acceptance: ipc_init failed (mode %d)\n", current_mode);
            return 1;
        }
        test_blocking_waits_then_completes(context);
        test_nonblocking_partial_count(context);
        if (current_mode == 2) {
            test_ack_correlation_with_partial_and_concurrent(context);
            test_nonblocking_with_full_ack_queue(context);
        }
        ipc_destroy(context);
    }
    if (failures != 0) {
        fprintf(stderr, "%d consumer acceptance failure(s)\n", failures);
        return 1;
    }
    puts("Consumer acceptance tests passed.");
    return 0;
}
