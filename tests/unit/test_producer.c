/**
 * @file test_producer.c
 * @brief Testa o fluxo do produtor com uma fachada IPC controlada.
 * @ingroup tests
 * @details Substitui ipc_* por doubles locais para validar ordem, destinos,
 * metadados e propagação de erros sem depender da implementação dos backends.
 */
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "mpmc/workers/producer.h"

#define MAX_RECORDED_SENDS MPMC_MAX_MESSAGES
#define MAX_EVENTS (MPMC_MAX_MESSAGES + 4u)

struct ipc_context {
    size_t producer_count;
};

static size_t send_calls;
static size_t ack_begin_calls;
static size_t ack_wait_calls;
static size_t ack_end_calls;
static size_t fail_send_call;
static mpmc_result_t send_result;
static mpmc_result_t ack_begin_result;
static mpmc_result_t ack_wait_result;
static mpmc_result_t ack_end_result;
static size_t destinations[MAX_RECORDED_SENDS];
static message_t recorded_messages[MAX_RECORDED_SENDS];
static char events[MAX_EVENTS];
static size_t event_count;

static int check(bool condition, const char *description);
static void reset_mocks(size_t producer_count);
static int test_striped_ack_flow(void);
static int test_fixed_destination_without_ack(void);
static int test_cleanup_after_send_error(void);
static int test_ack_failures(void);
static int test_validation(void);

int main(void) {
    if (test_striped_ack_flow() != 0 || test_fixed_destination_without_ack() != 0
        || test_cleanup_after_send_error() != 0 || test_ack_failures() != 0
        || test_validation() != 0) {
        return 1;
    }
    puts("Producer worker tests passed.");
    return 0;
}

mpmc_result_t ipc_ack_begin(ipc_context_t *context, uint64_t operation_id, unsigned expected) {
    (void)context;
    (void)operation_id;
    (void)expected;
    ++ack_begin_calls;
    events[event_count++] = 'B';
    return ack_begin_result;
}

mpmc_result_t ipc_ack_wait(ipc_context_t *context, uint64_t operation_id) {
    (void)context;
    (void)operation_id;
    ++ack_wait_calls;
    events[event_count++] = 'W';
    return ack_wait_result;
}

mpmc_result_t ipc_ack_end(ipc_context_t *context, uint64_t operation_id) {
    (void)context;
    (void)operation_id;
    ++ack_end_calls;
    events[event_count++] = 'E';
    return ack_end_result;
}

size_t ipc_producer_count(const ipc_context_t *context) {
    return context == NULL ? 0u : context->producer_count;
}

mpmc_result_t ipc_send(ipc_context_t *context, size_t producer_id, const message_t *message) {
    (void)context;
    size_t index = send_calls++;
    events[event_count++] = 'S';
    if (index < MAX_RECORDED_SENDS) {
        destinations[index] = producer_id;
        recorded_messages[index] = *message;
    }
    if (fail_send_call != 0u && send_calls == fail_send_call) {
        return send_result;
    }
    return (mpmc_result_t){.status = MPMC_OK};
}

static int test_striped_ack_flow(void) {
    struct ipc_context context = {.producer_count = 3u};
    producer_job_t job = {
        .producer_id = 0u,
        .message_count = 5u,
        .ack_mode = PRODUCER_ACK_WAIT,
        .operation_id = 77u,
    };
    reset_mocks(context.producer_count);
    mpmc_result_t result = producer_run(&context, &job);

    if (check(result.status == MPMC_OK && result.system_errno == 0,
              "striped ACK job should succeed")
        || check(ack_begin_calls == 1u && ack_wait_calls == 1u && ack_end_calls == 1u,
                 "ACK begin/wait/end should each run once")
        || check(event_count == 8u && events[0] == 'B' && events[6] == 'W' && events[7] == 'E',
                 "ACK must begin before sends and end after wait")
        || check(send_calls == 5u, "all requested messages should be sent")) {
        return 1;
    }

    const size_t expected_destinations[] = {1u, 2u, 3u, 1u, 2u};
    for (size_t index = 0u; index < 5u; ++index) {
        const message_t *message = &recorded_messages[index];
        if (check(destinations[index] == expected_destinations[index], "striped destination")
            || check(message->sender_pid == getpid() && message->sequence == index + 1u
                         && message->operation_id == job.operation_id,
                     "message metadata should retain PID, sequence, and operation")
            || check(message->sent_at.tv_sec > 0 && message->sent_at.tv_nsec >= 0
                         && message->sent_at.tv_nsec < 1000000000L,
                     "message timestamp should be a valid realtime value")) {
            return 1;
        }
    }
    return 0;
}

static int test_fixed_destination_without_ack(void) {
    struct ipc_context context = {.producer_count = 4u};
    producer_job_t job = {
        .producer_id = 3u,
        .message_count = 3u,
        .ack_mode = PRODUCER_ACK_NONE,
        .operation_id = 91u,
    };
    reset_mocks(context.producer_count);
    mpmc_result_t result = producer_run(&context, &job);

    if (check(result.status == MPMC_OK, "no-ACK job should succeed")
        || check(ack_begin_calls == 0u && ack_wait_calls == 0u && ack_end_calls == 0u,
                 "no-ACK job must not touch ACK interface")
        || check(send_calls == 3u && event_count == 3u, "no-ACK job should only send")) {
        return 1;
    }
    for (size_t index = 0u; index < send_calls; ++index) {
        if (check(destinations[index] == 3u && recorded_messages[index].sequence == index + 1u,
                  "fixed destination and local sequence should be preserved")) {
            return 1;
        }
    }
    return 0;
}

static int test_cleanup_after_send_error(void) {
    struct ipc_context context = {.producer_count = 2u};
    producer_job_t job = {
        .producer_id = 1u,
        .message_count = 4u,
        .ack_mode = PRODUCER_ACK_WAIT,
        .operation_id = 101u,
    };
    reset_mocks(context.producer_count);
    fail_send_call = 2u;
    send_result = (mpmc_result_t){.status = MPMC_ERR_INTERRUPTED, .system_errno = 4};

    mpmc_result_t result = producer_run(&context, &job);
    if (check(result.status == MPMC_ERR_INTERRUPTED && result.system_errno == 4,
              "send error result should be propagated")
        || check(send_calls == 2u && ack_wait_calls == 0u && ack_end_calls == 1u,
                 "send failure should stop, skip wait, and end ACK state")
        || check(event_count == 4u && events[0] == 'B' && events[1] == 'S'
                     && events[2] == 'S' && events[3] == 'E',
                 "send failure cleanup order")) {
        return 1;
    }
    return 0;
}

static int test_ack_failures(void) {
    struct ipc_context context = {.producer_count = 1u};
    producer_job_t job = {
        .producer_id = 1u,
        .message_count = 1u,
        .ack_mode = PRODUCER_ACK_WAIT,
        .operation_id = 111u,
    };

    reset_mocks(context.producer_count);
    ack_begin_result = (mpmc_result_t){.status = MPMC_ERR_NO_SPACE};
    mpmc_result_t result = producer_run(&context, &job);
    if (check(result.status == MPMC_ERR_NO_SPACE && send_calls == 0u && ack_end_calls == 0u,
              "failed ACK begin should prevent sends and cleanup")) {
        return 1;
    }

    reset_mocks(context.producer_count);
    ack_wait_result = (mpmc_result_t){.status = MPMC_ERR_INTERRUPTED, .system_errno = 4};
    ack_end_result = (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = 5};
    result = producer_run(&context, &job);
    if (check(result.status == MPMC_ERR_INTERRUPTED && result.system_errno == 4,
              "ACK wait error should remain primary if cleanup also fails")
        || check(send_calls == 1u && ack_wait_calls == 1u && ack_end_calls == 1u,
                 "ACK wait failure should still end ACK state")) {
        return 1;
    }

    reset_mocks(context.producer_count);
    ack_end_result = (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = 5};
    result = producer_run(&context, &job);
    if (check(result.status == MPMC_ERR_SYS && result.system_errno == 5,
              "ACK end error should be reported after successful production")) {
        return 1;
    }
    return 0;
}

static int test_validation(void) {
    struct ipc_context context = {.producer_count = 2u};
    producer_job_t job = {
        .producer_id = 1u,
        .message_count = 1u,
        .ack_mode = PRODUCER_ACK_NONE,
        .operation_id = 121u,
    };

    reset_mocks(context.producer_count);
    if (check(producer_run(NULL, &job).status == MPMC_ERR_INVALID_ARG,
              "null transport should be rejected")
        || check(producer_run(&context, NULL).status == MPMC_ERR_INVALID_ARG,
                 "null job should be rejected")) {
        return 1;
    }

    producer_job_t invalid = job;
    invalid.message_count = 0u;
    if (check(producer_run(&context, &invalid).status == MPMC_ERR_INVALID_ARG,
              "zero message count should be rejected")) {
        return 1;
    }
    invalid = job;
    invalid.message_count = MPMC_MAX_MESSAGES + 1u;
    if (check(producer_run(&context, &invalid).status == MPMC_ERR_INVALID_ARG,
              "message count above protocol maximum should be rejected")) {
        return 1;
    }
    invalid = job;
    invalid.ack_mode = (producer_ack_mode_t)3;
    if (check(producer_run(&context, &invalid).status == MPMC_ERR_INVALID_ARG,
              "invalid ACK mode should be rejected")) {
        return 1;
    }
    invalid = job;
    invalid.operation_id = 0u;
    if (check(producer_run(&context, &invalid).status == MPMC_ERR_INVALID_ARG,
              "zero operation ID should be rejected")) {
        return 1;
    }
    invalid = job;
    invalid.producer_id = 3u;
    if (check(producer_run(&context, &invalid).status == MPMC_ERR_INVALID_INDEX,
              "destination above producer count should be rejected")) {
        return 1;
    }
    struct ipc_context empty_context = {.producer_count = 0u};
    if (check(producer_run(&empty_context, &job).status == MPMC_ERR_INVALID_ARG,
              "empty transport should be rejected")) {
        return 1;
    }
    if (check(send_calls == 0u && ack_begin_calls == 0u,
              "invalid jobs must not call send or ACK interfaces")) {
        return 1;
    }

    reset_mocks(context.producer_count);
    job.message_count = MPMC_MAX_MESSAGES;
    if (check(producer_run(&context, &job).status == MPMC_OK
                  && send_calls == MPMC_MAX_MESSAGES
                  && recorded_messages[MPMC_MAX_MESSAGES - 1u].sequence == MPMC_MAX_MESSAGES,
              "protocol maximum of 1024 messages should be accepted")) {
        return 1;
    }
    return 0;
}

static int check(bool condition, const char *description) {
    if (!condition) {
        fprintf(stderr, "producer test failed: %s\n", description);
        return 1;
    }
    return 0;
}

static void reset_mocks(size_t producer_count) {
    (void)producer_count;
    send_calls = 0u;
    ack_begin_calls = 0u;
    ack_wait_calls = 0u;
    ack_end_calls = 0u;
    fail_send_call = 0u;
    send_result = (mpmc_result_t){.status = MPMC_ERR_SYS};
    ack_begin_result = (mpmc_result_t){.status = MPMC_OK};
    ack_wait_result = (mpmc_result_t){.status = MPMC_OK};
    ack_end_result = (mpmc_result_t){.status = MPMC_OK};
    memset(destinations, 0, sizeof destinations);
    memset(recorded_messages, 0, sizeof recorded_messages);
    memset(events, 0, sizeof events);
    event_count = 0u;
}
