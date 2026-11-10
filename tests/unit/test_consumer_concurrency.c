/**
 * @file test_consumer_concurrency.c
 * @brief Testa consumer_run() com consumidores e produtores concorrentes nos
 * backends reais.
 * @ingroup tests
 * @details Verifica, em processos distintos, que (1) cada mensagem é retirada
 * no máximo uma vez, (2) o consumo sem espera repartido entre vários
 * consumidores soma exatamente o que havia, e (3) no modo 2 os ACKs de cada
 * produtor continuam corretos mesmo com muitos consumidores e com consumidores
 * cujo operation_id coincide com o de um produtor. O ACK do modo 1 ainda é stub
 * (issue #11) e por isso só é exercitado no modo 2.
 */
#include <errno.h>
#include <limits.h>
#include <poll.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/wait.h>
#include <unistd.h>

#include "mpmc/workers/consumer.h"
#include "mpmc/workers/producer.h"

#define WATCHDOG_SECONDS 60u
#define PRODUCERS 4u
#define PER_PRODUCER 200u
#define CONSUMERS 6u
#define TOTAL (PRODUCERS * PER_PRODUCER)
#define FIRST_PRODUCER_OP 100u

typedef struct {
    int pid;
    unsigned sequence;
} record_t;

static int failures;
static int current_mode;

static void check(bool condition, const char *description, int line) {
    if (!condition) {
        fprintf(
            stderr,
            "consumer concurrency failed [mode %d, line %d]: %s\n",
            current_mode,
            line,
            description
        );
        ++failures;
    }
}
#define CHECK(condition, description) check((condition), (description), __LINE__)

static int compare_records(const void *left, const void *right) {
    const record_t *a = left;
    const record_t *b = right;
    if (a->pid != b->pid) return a->pid < b->pid ? -1 : 1;
    return (a->sequence > b->sequence) - (a->sequence < b->sequence);
}

static uint64_t backend_total(ipc_context_t *context, size_t index) {
    uint64_t total = UINT64_MAX;
    (void)ipc_poll_events(context);
    return ipc_consumed_count(context, index, &total).status == MPMC_OK ? total : UINT64_MAX;
}

static bool structure_is_empty(ipc_context_t *context, size_t index) {
    message_t message;
    return ipc_receive(context, index, &message, false).status == MPMC_ERR_WOULD_BLOCK;
}

static bool exited_ok(pid_t pid) {
    int status = 0;
    return waitpid(pid, &status, 0) == pid && WIFEXITED(status) && WEXITSTATUS(status) == 0;
}

/* Produtor real (producer_run); com ack=1 só existe no modo 2. */
static pid_t spawn_producer(ipc_context_t *context, unsigned index, bool ack) {
    pid_t pid = fork();
    if (pid == 0) {
        producer_job_t job = {
            .producer_id = 1,
            .message_count = PER_PRODUCER,
            .ack_mode = ack ? PRODUCER_ACK_WAIT : PRODUCER_ACK_NONE,
            .operation_id = FIRST_PRODUCER_OP + index,
        };
        _exit(producer_run(context, &job).status == MPMC_OK ? 0 : 1);
    }
    return pid;
}

/* Consumidor com consumer_run bloqueante; o operation_id do job coincide com o
 * de um produtor de propósito: ele não pode alterar nem consumir o ACK de
 * ninguém. */
static pid_t spawn_blocking_consumer(ipc_context_t *context, unsigned index, unsigned want) {
    pid_t pid = fork();
    if (pid == 0) {
        consumer_job_t job = {
            .producer_id = 1,
            .message_count = want,
            .ack_mode = CONSUMER_BLOCKING,
            .operation_id = FIRST_PRODUCER_OP + (index % PRODUCERS),
        };
        consumer_result_t out;
        mpmc_result_t result = consumer_run(context, &job, &out);
        _exit(result.status == MPMC_OK && out.consumed_count == want ? 0 : 1);
    }
    return pid;
}

static void test_blocking_consumers_and_producers(ipc_context_t *context) {
    const bool ack = current_mode == 2;
    pid_t consumers[CONSUMERS];
    pid_t producers[PRODUCERS];
    for (unsigned c = 0; c < CONSUMERS; ++c) {
        const unsigned want = TOTAL / CONSUMERS + (c < TOTAL % CONSUMERS ? 1 : 0);
        consumers[c] = spawn_blocking_consumer(context, c, want);
        CHECK(consumers[c] > 0, "fork consumer");
    }
    for (unsigned p = 0; p < PRODUCERS; ++p) {
        producers[p] = spawn_producer(context, p, ack);
        CHECK(producers[p] > 0, "fork producer");
    }
    for (unsigned p = 0; p < PRODUCERS; ++p) {
        CHECK(exited_ok(producers[p]), "each producer must finish (its own ACKs arrived)");
    }
    for (unsigned c = 0; c < CONSUMERS; ++c) {
        CHECK(exited_ok(consumers[c]), "each consumer must complete exactly its share");
    }
    CHECK(backend_total(context, 1) == TOTAL, "backend total must equal messages sent");
    CHECK(structure_is_empty(context, 1), "nothing may be left after every share completed");
}

/* Unicidade por conteúdo: ipc_receive devolve a mensagem, consumer_run só a
 * contagem. */
static void test_each_message_once(ipc_context_t *context) {
    int fds[2];
    CHECK(pipe(fds) == 0, "pipe");
    pid_t consumers[CONSUMERS];
    pid_t producers[PRODUCERS];
    const uint64_t before = backend_total(context, 1);
    for (unsigned c = 0; c < CONSUMERS; ++c) {
        const unsigned want = TOTAL / CONSUMERS + (c < TOTAL % CONSUMERS ? 1 : 0);
        consumers[c] = fork();
        if (consumers[c] == 0) {
            close(fds[0]);
            for (unsigned n = 0; n < want; ++n) {
                message_t message;
                if (ipc_receive(context, 1, &message, true).status != MPMC_OK) _exit(1);
                record_t record = {.pid = message.sender_pid, .sequence = message.sequence};
                if (write(fds[1], &record, sizeof record) != (ssize_t)sizeof record) _exit(2);
            }
            _exit(0);
        }
    }
    for (unsigned p = 0; p < PRODUCERS; ++p) {
        producers[p] = spawn_producer(context, p, false);
    }
    close(fds[1]);
    record_t *records = calloc(TOTAL * 2, sizeof *records);
    size_t count = 0;
    while (
        records != nullptr && count < TOTAL * 2
        && read(fds[0], &records[count], sizeof *records) == (ssize_t)sizeof *records
    ) {
        ++count;
    }
    close(fds[0]);
    for (unsigned p = 0; p < PRODUCERS; ++p) CHECK(exited_ok(producers[p]), "producer ok");
    for (unsigned c = 0; c < CONSUMERS; ++c) CHECK(exited_ok(consumers[c]), "consumer ok");
    CHECK(count == TOTAL, "every message must be received exactly once in total");
    if (records != nullptr) {
        qsort(records, count, sizeof *records, compare_records);
        unsigned duplicates = 0;
        for (size_t i = 1; i < count; ++i) {
            duplicates += compare_records(&records[i - 1], &records[i]) == 0 ? 1 : 0;
        }
        CHECK(duplicates == 0, "no message may be consumed twice");
    }
    free(records);
    CHECK(backend_total(context, 1) == before + TOTAL, "backend total after the race");
}

/* Vários consumidores sem espera disputam um estoque menor que a soma dos
 * pedidos. */
static void test_nonblocking_split(ipc_context_t *context) {
    enum { STOCK = 60, WORKERS = 5, ASK = 30 };
    for (unsigned n = 1; n <= STOCK; ++n) {
        message_t message = {.sender_pid = 7, .sequence = n, .operation_id = 1};
        CHECK(ipc_send(context, 1, &message).status == MPMC_OK, "stock send");
    }
    const uint64_t before = backend_total(context, 1);
    int fds[2];
    CHECK(pipe(fds) == 0, "pipe");
    pid_t workers[WORKERS];
    for (unsigned w = 0; w < WORKERS; ++w) {
        workers[w] = fork();
        if (workers[w] == 0) {
            consumer_job_t job = {.producer_id = 1,
                .message_count = ASK,
                .ack_mode = CONSUMER_NONBLOCKING,
                .operation_id = 300 + w};
            consumer_result_t out;
            mpmc_result_t result = consumer_run(context, &job, &out);
            const bool coherent = out.consumed_count == ASK ? result.status == MPMC_OK
                                                            : result.status == MPMC_ERR_WOULD_BLOCK;
            unsigned reported = coherent ? out.consumed_count : UINT_MAX;
            _exit(write(fds[1], &reported, sizeof reported) == (ssize_t)sizeof reported ? 0 : 1);
        }
    }
    close(fds[1]);
    unsigned sum = 0;
    unsigned reported;
    unsigned reports = 0;
    while (read(fds[0], &reported, sizeof reported) == (ssize_t)sizeof reported) {
        CHECK(reported != UINT_MAX, "status must match the partial count (OK only when complete)");
        sum += reported == UINT_MAX ? 0 : reported;
        ++reports;
    }
    close(fds[0]);
    for (unsigned w = 0; w < WORKERS; ++w) CHECK(exited_ok(workers[w]), "worker ok");
    CHECK(reports == WORKERS, "every worker must report");
    CHECK(sum == STOCK, "partial consumptions must add up to the stock, no more and no less");
    CHECK(backend_total(context, 1) == before + STOCK, "backend total equals the split");
    CHECK(structure_is_empty(context, 1), "stock fully consumed");
}

int main(void) {
    (void)alarm(WATCHDOG_SECONDS);
    for (current_mode = 1; current_mode <= 2; ++current_mode) {
        ipc_context_t *context = nullptr;
        if (ipc_init(&context, 1, (ipc_mode_t)current_mode).status != MPMC_OK) {
            fprintf(stderr, "consumer concurrency: ipc_init failed (mode %d)\n", current_mode);
            return 1;
        }
        test_blocking_consumers_and_producers(context);
        test_each_message_once(context);
        test_nonblocking_split(context);
        ipc_destroy(context);
    }
    if (failures != 0) {
        fprintf(stderr, "%d consumer concurrency failure(s)\n", failures);
        return 1;
    }
    puts("Consumer concurrency tests passed.");
    return 0;
}
