/**
 * @file test_consumer.c
 * @brief Testa o worker consumidor com respostas controladas da fachada IPC.
 * @ingroup tests
 * @details Controla ipc_receive(), observa ipc_worker_close() e fornece a consulta de
 * contagem do contexto de teste; não liga backends nem reproduz o transporte.
 */
#include <errno.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

#include "mpmc/workers/consumer.h"

#define MAX_RECEIVES MPMC_MAX_MESSAGES

struct ipc_context {
    size_t producer_count;
};

static unsigned producer_count_calls;
static unsigned receive_calls;
static unsigned close_calls;
static unsigned fail_receive_call;
static size_t observed_producer_ids[MAX_RECEIVES];
static bool observed_blocking[MAX_RECEIVES];
static ipc_context_t *observed_contexts[MAX_RECEIVES];
static mpmc_result_t receive_failure;

static int check(bool condition, const char *description);
static void reset_mocks(void);
static int test_complete_reads(void);
static int test_zero_available(void);
static int test_partial_nonblocking_result(void);
static int test_error_propagation(void);
static int test_validation(void);
static int test_maximum_count(void);

int main(void) {
    if (test_complete_reads() != 0 || test_zero_available() != 0
        || test_partial_nonblocking_result() != 0
        || test_error_propagation() != 0 || test_validation() != 0
        || test_maximum_count() != 0) {
        return 1;
    }
    if (check(close_calls == 0u, "consumer_run must leave worker cleanup to its caller")) {
        return 1;
    }
    puts("Consumer worker tests passed.");
    return 0;
}

/* Double da consulta pública: o contexto opaco real não é necessário neste teste unitário. */
size_t ipc_producer_count(const ipc_context_t *context) {
    ++producer_count_calls;
    return context == NULL ? 0u : context->producer_count;
}

/* Substituto privado da fachada para controlar sucesso, fila vazia e falhas. */
mpmc_result_t ipc_receive(
    ipc_context_t *context, size_t producer_id, message_t *out, bool blocking
) {
    unsigned call = ++receive_calls;
    if (call <= MAX_RECEIVES) {
        observed_contexts[call - 1u] = context;
        observed_producer_ids[call - 1u] = producer_id;
        observed_blocking[call - 1u] = blocking;
    }
    if (call == fail_receive_call) {
        return receive_failure;
    }
    *out = (message_t){.sequence = call};
    return (mpmc_result_t){.status = MPMC_OK};
}

/* Espião: o fechamento pertence ao ponto de entrada do filho, nunca ao worker. */
void ipc_worker_close(ipc_context_t *context) {
    (void)context;
    ++close_calls;
}

static int test_complete_reads(void) {
    struct ipc_context context = {.producer_count = 3u};
    const consumer_ack_mode_t modes[] = {CONSUMER_BLOCKING, CONSUMER_NONBLOCKING};

    for (size_t mode_index = 0u; mode_index < sizeof modes / sizeof modes[0]; ++mode_index) {
        consumer_job_t job = {
            .producer_id = 2u,
            .message_count = 4u,
            .ack_mode = modes[mode_index],
            .operation_id = 71u,
        };
        consumer_result_t out = {.consumed_count = 99u};
        reset_mocks();
        mpmc_result_t result = consumer_run(&context, &job, &out);
        const bool blocking = modes[mode_index] == CONSUMER_BLOCKING;

        if (check(result.status == MPMC_OK && result.system_errno == 0,
                  "complete receive sequence should succeed")
            || check(out.error.status == MPMC_OK && out.error.system_errno == 0
                         && out.consumed_count == job.message_count,
                     "success result should report every message")
            || check(producer_count_calls == 1u && receive_calls == job.message_count,
                     "query producer count once and receive exactly n times")) {
            return 1;
        }
        for (unsigned index = 0u; index < job.message_count; ++index) {
            if (check(observed_contexts[index] == &context
                          && observed_producer_ids[index] == job.producer_id
                          && observed_blocking[index] == blocking,
                      "each receive uses the supplied context, index, and wait policy")) {
                return 1;
            }
        }
        if (check(close_calls == 0u, "worker must not close the inherited context")) {
            return 1;
        }
    }
    return 0;
}

static int test_zero_available(void) {
    struct ipc_context context = {.producer_count = 2u};
    const consumer_ack_mode_t modes[] = {CONSUMER_BLOCKING, CONSUMER_NONBLOCKING};

    /* Falha já na primeira retirada: nada foi consumido, em qualquer política de espera. */
    for (size_t index = 0u; index < sizeof modes / sizeof modes[0]; ++index) {
        consumer_job_t job = {
            .producer_id = 2u,
            .message_count = 3u,
            .ack_mode = modes[index],
            .operation_id = 76u,
        };
        consumer_result_t out = {.consumed_count = 99u};
        reset_mocks();
        fail_receive_call = 1u;
        receive_failure = (mpmc_result_t){.status = MPMC_ERR_WOULD_BLOCK, .system_errno = EAGAIN};

        mpmc_result_t result = consumer_run(&context, &job, &out);
        if (check(result.status == MPMC_ERR_WOULD_BLOCK && result.system_errno == EAGAIN,
                  "empty structure must return the receive result unchanged")
            || check(out.error.status == result.status && out.error.system_errno == EAGAIN
                         && out.consumed_count == 0u,
                     "zero available must report consumed_count zero, not stale data")
            || check(receive_calls == 1u, "stop after the first empty receive")
            || check(close_calls == 0u, "worker must not close the inherited context")) {
            return 1;
        }
    }
    return 0;
}

static int test_partial_nonblocking_result(void) {
    struct ipc_context context = {.producer_count = 2u};
    consumer_job_t job = {
        .producer_id = 1u,
        .message_count = 5u,
        .ack_mode = CONSUMER_NONBLOCKING,
        .operation_id = 72u,
    };
    consumer_result_t out;
    reset_mocks();
    fail_receive_call = 3u;
    receive_failure = (mpmc_result_t){.status = MPMC_ERR_WOULD_BLOCK, .system_errno = ENOMSG};

    mpmc_result_t result = consumer_run(&context, &job, &out);
    if (check(result.status == MPMC_ERR_WOULD_BLOCK && result.system_errno == ENOMSG,
              "nonblocking empty result and errno must be propagated")
        || check(out.error.status == result.status && out.error.system_errno == ENOMSG
                     && out.consumed_count == 2u,
                 "partial result must preserve the successfully consumed count")
        || check(receive_calls == 3u, "stop on the first empty receive")) {
        return 1;
    }
    return 0;
}

static int test_error_propagation(void) {
    struct ipc_context context = {.producer_count = 4u};
    static const mpmc_result_t failures[] = {
        {.status = MPMC_ERR_INTERRUPTED, .system_errno = EINTR},
        {.status = MPMC_ERR_SYS, .system_errno = EIO},
        {.status = MPMC_ERR_STATE, .system_errno = 0},
        {.status = MPMC_ERR_WOULD_BLOCK, .system_errno = ENOMSG},
    };
    const consumer_ack_mode_t modes[] = {
        CONSUMER_BLOCKING,
        CONSUMER_BLOCKING,
        CONSUMER_NONBLOCKING,
        CONSUMER_BLOCKING,
    };

    for (size_t index = 0u; index < sizeof failures / sizeof failures[0]; ++index) {
        consumer_job_t job = {
            .producer_id = 4u,
            .message_count = 4u,
            .ack_mode = modes[index],
            .operation_id = 73u,
        };
        consumer_result_t out;
        reset_mocks();
        fail_receive_call = 3u;
        receive_failure = failures[index];
        mpmc_result_t result = consumer_run(&context, &job, &out);
        if (check(result.status == failures[index].status
                      && result.system_errno == failures[index].system_errno,
                  "receive error must be returned without translation")
            || check(out.error.status == failures[index].status
                         && out.error.system_errno == failures[index].system_errno
                         && out.consumed_count == 2u,
                     "receive failure must preserve its cause and partial count")
            || check(receive_calls == 3u, "first receive error stops the worker")) {
            return 1;
        }
    }
    return 0;
}

static int test_validation(void) {
    struct ipc_context context = {.producer_count = 2u};
    consumer_job_t job = {
        .producer_id = 1u,
        .message_count = 1u,
        .ack_mode = CONSUMER_BLOCKING,
        .operation_id = 74u,
    };
    consumer_result_t out;
    reset_mocks();

    if (check(consumer_run(NULL, &job, &out).status == MPMC_ERR_INVALID_ARG
                  && out.consumed_count == 0u && out.error.status == MPMC_ERR_INVALID_ARG,
              "null transport should fail and initialize result")
        || check(consumer_run(&context, NULL, &out).status == MPMC_ERR_INVALID_ARG
                     && out.consumed_count == 0u,
                 "null job should fail and initialize result")
        || check(consumer_run(&context, &job, NULL).status == MPMC_ERR_INVALID_ARG,
                 "null result should fail")) {
        return 1;
    }

    consumer_job_t invalid = job;
    invalid.message_count = 0u;
    if (check(consumer_run(&context, &invalid, &out).status == MPMC_ERR_INVALID_ARG,
              "zero count should be rejected")) {
        return 1;
    }
    invalid = job;
    invalid.message_count = MPMC_MAX_MESSAGES + 1u;
    if (check(consumer_run(&context, &invalid, &out).status == MPMC_ERR_INVALID_ARG,
              "count above protocol maximum should be rejected")) {
        return 1;
    }
    invalid = job;
    invalid.ack_mode = (consumer_ack_mode_t)3;
    if (check(consumer_run(&context, &invalid, &out).status == MPMC_ERR_INVALID_ARG,
              "unknown wait policy should be rejected")) {
        return 1;
    }
    invalid = job;
    invalid.producer_id = 0u;
    if (check(consumer_run(&context, &invalid, &out).status == MPMC_ERR_INVALID_ARG,
              "producer index zero should be rejected")) {
        return 1;
    }
    invalid.producer_id = 3u;
    if (check(consumer_run(&context, &invalid, &out).status == MPMC_ERR_INVALID_ARG,
              "producer index above count should be rejected")) {
        return 1;
    }
    struct ipc_context empty = {.producer_count = 0u};
    if (check(consumer_run(&empty, &job, &out).status == MPMC_ERR_INVALID_ARG,
              "empty transport should be rejected")) {
        return 1;
    }
    if (check(producer_count_calls == 3u && receive_calls == 0u,
              "invalid jobs must not receive; only valid-shaped contexts are queried")) {
        return 1;
    }
    return 0;
}

static int test_maximum_count(void) {
    struct ipc_context context = {.producer_count = 1u};
    consumer_job_t job = {
        .producer_id = 1u,
        .message_count = MPMC_MAX_MESSAGES,
        .ack_mode = CONSUMER_NONBLOCKING,
        .operation_id = 75u,
    };
    consumer_result_t out;
    reset_mocks();
    mpmc_result_t result = consumer_run(&context, &job, &out);
    if (check(result.status == MPMC_OK && out.error.status == MPMC_OK
                  && out.consumed_count == MPMC_MAX_MESSAGES
                  && receive_calls == MPMC_MAX_MESSAGES,
              "protocol maximum should be accepted and consumed exactly")) {
        return 1;
    }
    return 0;
}

static int check(bool condition, const char *description) {
    if (!condition) {
        fprintf(stderr, "consumer test failed: %s\n", description);
        return 1;
    }
    return 0;
}

static void reset_mocks(void) {
    producer_count_calls = 0u;
    receive_calls = 0u;
    fail_receive_call = 0u;
    receive_failure = (mpmc_result_t){.status = MPMC_ERR_SYS};
    memset(observed_producer_ids, 0, sizeof observed_producer_ids);
    memset(observed_blocking, 0, sizeof observed_blocking);
    memset(observed_contexts, 0, sizeof observed_contexts);
}
