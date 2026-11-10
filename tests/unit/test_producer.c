/**
 * @file test_producer.c
 * @brief Testa o fluxo do produtor com uma fachada IPC controlada.
 * @ingroup tests
 * @details Substitui ipc_* por doubles locais para validar ordem, destinos,
 * metadados e propagação de erros sem depender da implementação dos backends.
 */
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
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

/* Argumentos observados nos doubles de ipc_*. */
static ipc_context_t *send_contexts[MAX_RECORDED_SENDS];
static ipc_context_t *begin_context;
static ipc_context_t *wait_context;
static ipc_context_t *end_context;
static uint64_t begin_operation;
static uint64_t wait_operation;
static uint64_t end_operation;
static unsigned begin_expected;
static size_t destroy_calls; /* acumulado: nunca zerado por reset_mocks() */

/* Relógio determinístico: cada chamada devolve CLOCK_REALTIME = 1000000 + n s. */
static unsigned clock_calls;
static unsigned fail_clock_call;
static bool clock_id_always_realtime;
static unsigned clock_calls_at_send[MAX_RECORDED_SENDS];

static int check(bool condition, const char *description);
static void reset_mocks(size_t producer_count);
static int test_striped_ack_flow(void);
static int test_fixed_destination_without_ack(void);
static int test_cleanup_after_send_error(void);
static int test_ack_failures(void);
static int test_validation(void);
static int test_ack_arguments_and_contexts(void);
static int test_minimum_message_count(void);
static int test_failure_points(void);
static int test_metadata_failure_points(void);
static int test_timestamp_taken_per_send(void);
static int test_reported_progress(void);

int main(void) {
    if (test_striped_ack_flow() != 0 || test_fixed_destination_without_ack() != 0
        || test_cleanup_after_send_error() != 0 || test_ack_failures() != 0
        || test_validation() != 0 || test_ack_arguments_and_contexts() != 0
        || test_minimum_message_count() != 0 || test_failure_points() != 0
        || test_metadata_failure_points() != 0 || test_timestamp_taken_per_send() != 0
        || test_reported_progress() != 0) {
        return 1;
    }
    if (check(destroy_calls == 0, "producer must never call ipc_destroy (supervisor owns IPC)")) {
        return 1;
    }
    puts("Producer worker tests passed.");
    return 0;
}

/* Substitui clock_gettime() em producer.c (ver Makefile: -Dclock_gettime=...). */
int mpmc_test_clock_gettime(clockid_t clock_id, struct timespec *out) {
    ++clock_calls;
    if (clock_id != CLOCK_REALTIME) {
        clock_id_always_realtime = false;
    }
    if (fail_clock_call != 0 && clock_calls == fail_clock_call) {
        errno = EINVAL;
        return -1;
    }
    out->tv_sec = 1000000 + (time_t)clock_calls;
    out->tv_nsec = 7;
    return 0;
}

/* O produtor jamais deve remover recursos globais: pertencem ao supervisor. */
void ipc_destroy(ipc_context_t *context) {
    (void)context;
    ++destroy_calls;
}

mpmc_result_t ipc_ack_begin(ipc_context_t *context, uint64_t operation_id, unsigned expected) {
    begin_context = context;
    begin_operation = operation_id;
    begin_expected = expected;
    ++ack_begin_calls;
    events[event_count++] = 'B';
    return ack_begin_result;
}

mpmc_result_t ipc_ack_wait(ipc_context_t *context, uint64_t operation_id) {
    wait_context = context;
    wait_operation = operation_id;
    ++ack_wait_calls;
    events[event_count++] = 'W';
    return ack_wait_result;
}

mpmc_result_t ipc_ack_end(ipc_context_t *context, uint64_t operation_id) {
    end_context = context;
    end_operation = operation_id;
    ++ack_end_calls;
    events[event_count++] = 'E';
    return ack_end_result;
}

size_t ipc_producer_count(const ipc_context_t *context) {
    return context == nullptr ? 0 : context->producer_count;
}

mpmc_result_t ipc_send(ipc_context_t *context, size_t producer_id, const message_t *message) {
    size_t index = send_calls++;
    events[event_count++] = 'S';
    if (index < MAX_RECORDED_SENDS) {
        send_contexts[index] = context;
        destinations[index] = producer_id;
        recorded_messages[index] = *message;
        clock_calls_at_send[index] = clock_calls;
    }
    if (fail_send_call != 0 && send_calls == fail_send_call) {
        return send_result;
    }
    return (mpmc_result_t){.status = MPMC_OK};
}

static int test_striped_ack_flow(void) {
    struct ipc_context context = {.producer_count = 3};
    producer_job_t job = {
        .producer_id = 0,
        .message_count = 5,
        .ack_mode = PRODUCER_ACK_WAIT,
        .operation_id = 77,
    };
    reset_mocks(context.producer_count);
    mpmc_result_t result = producer_run(&context, &job);

    if (check(
            result.status == MPMC_OK && result.system_errno == 0, "striped ACK job should succeed"
        )
        || check(
            ack_begin_calls == 1 && ack_wait_calls == 1 && ack_end_calls == 1,
            "ACK begin/wait/end should each run once"
        )
        || check(
            event_count == 8 && events[0] == 'B' && events[6] == 'W' && events[7] == 'E',
            "ACK must begin before sends and end after wait"
        )
        || check(send_calls == 5, "all requested messages should be sent")) {
        return 1;
    }

    const size_t expected_destinations[] = {1, 2, 3, 1, 2};
    for (size_t index = 0; index < 5; ++index) {
        const message_t *message = &recorded_messages[index];
        if (check(destinations[index] == expected_destinations[index], "striped destination")
            || check(
                message->sender_pid == getpid() && message->sequence == index + 1
                    && message->operation_id == job.operation_id,
                "message metadata should retain PID, sequence, and operation"
            )
            || check(
                message->sent_at.tv_sec > 0 && message->sent_at.tv_nsec >= 0
                    && message->sent_at.tv_nsec < 1000000000L,
                "message timestamp should be a valid realtime value"
            )) {
            return 1;
        }
    }
    return 0;
}

static int test_fixed_destination_without_ack(void) {
    struct ipc_context context = {.producer_count = 4};
    producer_job_t job = {
        .producer_id = 3,
        .message_count = 3,
        .ack_mode = PRODUCER_ACK_NONE,
        .operation_id = 91,
    };
    reset_mocks(context.producer_count);
    mpmc_result_t result = producer_run(&context, &job);

    if (check(result.status == MPMC_OK, "no-ACK job should succeed")
        || check(
            ack_begin_calls == 0 && ack_wait_calls == 0 && ack_end_calls == 0,
            "no-ACK job must not touch ACK interface"
        )
        || check(send_calls == 3 && event_count == 3, "no-ACK job should only send")) {
        return 1;
    }
    for (size_t index = 0; index < send_calls; ++index) {
        if (check(
                destinations[index] == 3 && recorded_messages[index].sequence == index + 1,
                "fixed destination and local sequence should be preserved"
            )) {
            return 1;
        }
    }
    return 0;
}

static int test_cleanup_after_send_error(void) {
    struct ipc_context context = {.producer_count = 2};
    producer_job_t job = {
        .producer_id = 1,
        .message_count = 4,
        .ack_mode = PRODUCER_ACK_WAIT,
        .operation_id = 101,
    };
    reset_mocks(context.producer_count);
    fail_send_call = 2;
    send_result = (mpmc_result_t){.status = MPMC_ERR_INTERRUPTED, .system_errno = 4};

    mpmc_result_t result = producer_run(&context, &job);
    if (check(
            result.status == MPMC_ERR_INTERRUPTED && result.system_errno == 4,
            "send error result should be propagated"
        )
        || check(
            send_calls == 2 && ack_wait_calls == 0 && ack_end_calls == 1,
            "send failure should stop, skip wait, and end ACK state"
        )
        || check(
            event_count == 4 && events[0] == 'B' && events[1] == 'S' && events[2] == 'S'
                && events[3] == 'E',
            "send failure cleanup order"
        )) {
        return 1;
    }
    return 0;
}

static int test_ack_failures(void) {
    struct ipc_context context = {.producer_count = 1};
    producer_job_t job = {
        .producer_id = 1,
        .message_count = 1,
        .ack_mode = PRODUCER_ACK_WAIT,
        .operation_id = 111,
    };

    reset_mocks(context.producer_count);
    ack_begin_result = (mpmc_result_t){.status = MPMC_ERR_NO_SPACE};
    mpmc_result_t result = producer_run(&context, &job);
    if (check(
            result.status == MPMC_ERR_NO_SPACE && send_calls == 0 && ack_end_calls == 0,
            "failed ACK begin should prevent sends and cleanup"
        )) {
        return 1;
    }

    reset_mocks(context.producer_count);
    ack_wait_result = (mpmc_result_t){.status = MPMC_ERR_INTERRUPTED, .system_errno = 4};
    ack_end_result = (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = 5};
    result = producer_run(&context, &job);
    if (check(
            result.status == MPMC_ERR_INTERRUPTED && result.system_errno == 4,
            "ACK wait error should remain primary if cleanup also fails"
        )
        || check(
            send_calls == 1 && ack_wait_calls == 1 && ack_end_calls == 1,
            "ACK wait failure should still end ACK state"
        )) {
        return 1;
    }

    reset_mocks(context.producer_count);
    ack_end_result = (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = 5};
    result = producer_run(&context, &job);
    if (check(
            result.status == MPMC_ERR_SYS && result.system_errno == 5,
            "ACK end error should be reported after successful production"
        )) {
        return 1;
    }
    return 0;
}

static int test_validation(void) {
    struct ipc_context context = {.producer_count = 2};
    producer_job_t job = {
        .producer_id = 1,
        .message_count = 1,
        .ack_mode = PRODUCER_ACK_NONE,
        .operation_id = 121,
    };

    reset_mocks(context.producer_count);
    if (check(
            producer_run(nullptr, &job).status == MPMC_ERR_INVALID_ARG,
            "null transport should be rejected"
        )
        || check(
            producer_run(&context, nullptr).status == MPMC_ERR_INVALID_ARG,
            "null job should be rejected"
        )) {
        return 1;
    }

    producer_job_t invalid = job;
    invalid.message_count = 0;
    if (check(
            producer_run(&context, &invalid).status == MPMC_ERR_INVALID_ARG,
            "zero message count should be rejected"
        )) {
        return 1;
    }
    invalid = job;
    invalid.message_count = MPMC_MAX_MESSAGES + 1;
    if (check(
            producer_run(&context, &invalid).status == MPMC_ERR_INVALID_ARG,
            "message count above protocol maximum should be rejected"
        )) {
        return 1;
    }
    invalid = job;
    invalid.ack_mode = (producer_ack_mode_t)3;
    if (check(
            producer_run(&context, &invalid).status == MPMC_ERR_INVALID_ARG,
            "invalid ACK mode should be rejected"
        )) {
        return 1;
    }
    invalid = job;
    invalid.operation_id = 0;
    if (check(
            producer_run(&context, &invalid).status == MPMC_ERR_INVALID_ARG,
            "zero operation ID should be rejected"
        )) {
        return 1;
    }
    invalid = job;
    invalid.producer_id = 3;
    if (check(
            producer_run(&context, &invalid).status == MPMC_ERR_INVALID_INDEX,
            "destination above producer count should be rejected"
        )) {
        return 1;
    }
    struct ipc_context empty_context = {.producer_count = 0};
    if (check(
            producer_run(&empty_context, &job).status == MPMC_ERR_INVALID_ARG,
            "empty transport should be rejected"
        )) {
        return 1;
    }
    if (check(
            send_calls == 0 && ack_begin_calls == 0 && clock_calls == 0,
            "invalid jobs must not call send, ACK interfaces or the clock"
        )) {
        return 1;
    }

    reset_mocks(context.producer_count);
    job.message_count = MPMC_MAX_MESSAGES;
    if (check(
            producer_run(&context, &job).status == MPMC_OK && send_calls == MPMC_MAX_MESSAGES
                && recorded_messages[MPMC_MAX_MESSAGES - 1].sequence == MPMC_MAX_MESSAGES,
            "protocol maximum of 1024 messages should be accepted"
        )) {
        return 1;
    }
    return 0;
}

/* ack=1: begin/wait/end recebem o contexto, o operation_id e o total n corretos.
 * Somente destino fixo (i > 0): o roteamento striped pertence à #18/#49. */
static int test_ack_arguments_and_contexts(void) {
    struct ipc_context context = {.producer_count = 3};
    producer_job_t job = {
        .producer_id = 2,
        .message_count = 7,
        .ack_mode = PRODUCER_ACK_WAIT,
        .operation_id = 77,
    };
    reset_mocks(context.producer_count);
    mpmc_result_t result = producer_run(&context, &job);
    if (check(result.status == MPMC_OK, "ACK job with n=7 should succeed")
        || check(
            begin_operation == 77 && begin_expected == 7,
            "ack_begin must receive operation_id and the total n"
        )
        || check(
            wait_operation == 77 && end_operation == 77,
            "ack_wait/ack_end must use the same operation_id as ack_begin"
        )
        || check(
            begin_context == &context && wait_context == &context && end_context == &context,
            "ACK calls must use the transport received by producer_run"
        )) {
        return 1;
    }
    for (size_t index = 0; index < send_calls; ++index) {
        if (check(
                send_contexts[index] == &context
                    && recorded_messages[index].operation_id == begin_operation,
                "every send must use the same transport and the ACK operation_id"
            )) {
            return 1;
        }
    }

    /* Máximo do protocolo com ACK, destino fixo igual a p (limite superior do índice). */
    struct ipc_context two = {.producer_count = 2};
    job.producer_id = 2;
    job.message_count = MPMC_MAX_MESSAGES;
    job.operation_id = 78;
    reset_mocks(two.producer_count);
    result = producer_run(&two, &job);
    if (check(
            result.status == MPMC_OK && send_calls == MPMC_MAX_MESSAGES,
            "n=1024 with ACK should send exactly 1024 messages"
        )
        || check(
            begin_expected == MPMC_MAX_MESSAGES && begin_operation == 78 && wait_operation == 78
                && end_operation == 78,
            "n=1024 ACK arguments"
        )
        || check(
            event_count == MPMC_MAX_MESSAGES + 3 && events[0] == 'B'
                && events[MPMC_MAX_MESSAGES + 1] == 'W' && events[MPMC_MAX_MESSAGES + 2] == 'E',
            "n=1024 ACK order"
        )
        || check(
            destinations[0] == 2 && destinations[MPMC_MAX_MESSAGES - 1] == 2
                && recorded_messages[MPMC_MAX_MESSAGES - 1].sequence == MPMC_MAX_MESSAGES,
            "producer_id == p should be a valid fixed destination"
        )) {
        return 1;
    }
    return 0;
}

/* n mínimo (1): exatamente uma mensagem, sequência 1, nos dois modos de ACK. */
static int test_minimum_message_count(void) {
    struct ipc_context context = {.producer_count = 3};
    producer_job_t job = {
        .producer_id = 1,
        .message_count = MPMC_MIN_MESSAGES,
        .ack_mode = PRODUCER_ACK_WAIT,
        .operation_id = 201,
    };
    reset_mocks(context.producer_count);
    mpmc_result_t result = producer_run(&context, &job);
    if (check(result.status == MPMC_OK && send_calls == 1, "n=1 with ACK should send once")
        || check(event_count == 4 && memcmp(events, "BSWE", 4) == 0, "n=1 ACK order is BSWE")
        || check(
            begin_expected == 1 && destinations[0] == 1 && recorded_messages[0].sequence == 1
                && recorded_messages[0].sender_pid == getpid(),
            "n=1 ACK metadata and expected count"
        )) {
        return 1;
    }

    job.ack_mode = PRODUCER_ACK_NONE;
    reset_mocks(context.producer_count);
    result = producer_run(&context, &job);
    if (check(
            result.status == MPMC_OK && send_calls == 1 && event_count == 1 && events[0] == 'S'
                && destinations[0] == 1 && recorded_messages[0].sequence == 1,
            "n=1 without ACK should send once to the requested destination"
        )) {
        return 1;
    }
    return 0;
}

/* Falha de envio na 1ª, em uma intermediária e na última mensagem. */
static int test_failure_points(void) {
    struct ipc_context context = {.producer_count = 2};
    const unsigned fail_at[] = {1, 2, 4};
    for (size_t k = 0; k < sizeof fail_at / sizeof fail_at[0]; ++k) {
        const unsigned failing = fail_at[k];
        producer_job_t job = {
            .producer_id = 2,
            .message_count = 4,
            .ack_mode = PRODUCER_ACK_WAIT,
            .operation_id = 300 + failing,
        };
        reset_mocks(context.producer_count);
        fail_send_call = failing;
        send_result = (mpmc_result_t){.status = MPMC_ERR_INTERRUPTED, .system_errno = 4};
        ack_end_result = (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = 5};
        mpmc_result_t result = producer_run(&context, &job);
        if (check(
                result.status == MPMC_ERR_INTERRUPTED && result.system_errno == 4,
                "send error must stay primary even if ack_end also fails"
            )
            || check(
                send_calls == failing && ack_wait_calls == 0 && ack_end_calls == 1,
                "failed send: stop at once, no wait, one ack_end"
            )
            || check(
                event_count == failing + 2 && events[0] == 'B' && events[event_count - 1] == 'E',
                "failed send: begin first, end last"
            )
            || check(
                recorded_messages[failing - 1].sequence == failing
                    && end_operation == job.operation_id,
                "failed send: no retry or skipped sequence; end uses operation_id"
            )) {
            return 1;
        }
    }

    /* ack=2: o erro do envio (inclusive de capacidade) é propagado sem tocar no ACK. */
    const mpmc_result_t errors[] = {
        {.status = MPMC_ERR_NO_SPACE, .system_errno = 0},
        {.status = MPMC_ERR_WOULD_BLOCK, .system_errno = 11},
        {.status = MPMC_ERR_SHUTDOWN, .system_errno = 0},
    };
    for (size_t k = 0; k < sizeof errors / sizeof errors[0]; ++k) {
        producer_job_t job = {
            .producer_id = 1,
            .message_count = 3,
            .ack_mode = PRODUCER_ACK_NONE,
            .operation_id = 400,
        };
        reset_mocks(context.producer_count);
        fail_send_call = 2;
        send_result = errors[k];
        mpmc_result_t result = producer_run(&context, &job);
        if (check(
                result.status == errors[k].status && result.system_errno == errors[k].system_errno,
                "ack=2: send error propagated unchanged"
            )
            || check(
                send_calls == 2 && event_count == 2 && ack_begin_calls == 0 && ack_wait_calls == 0
                    && ack_end_calls == 0,
                "ack=2: failed send stops and never touches ACK interface"
            )) {
            return 1;
        }
    }

    /* Falha em ack_begin preserva status e errno e não envia nem finaliza. */
    producer_job_t job = {
        .producer_id = 1,
        .message_count = 2,
        .ack_mode = PRODUCER_ACK_WAIT,
        .operation_id = 500,
    };
    reset_mocks(context.producer_count);
    ack_begin_result = (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = 12};
    mpmc_result_t result = producer_run(&context, &job);
    if (check(
            result.status == MPMC_ERR_SYS && result.system_errno == 12,
            "ack_begin error must be propagated with errno"
        )
        || check(
            event_count == 1 && events[0] == 'B' && clock_calls == 0,
            "ack_begin failure: nothing else runs"
        )) {
        return 1;
    }
    return 0;
}

/* Falha na construção dos metadados (clock_gettime) no meio da produção. */
static int test_metadata_failure_points(void) {
    struct ipc_context context = {.producer_count = 2};
    producer_job_t job = {
        .producer_id = 1,
        .message_count = 5,
        .ack_mode = PRODUCER_ACK_WAIT,
        .operation_id = 600,
    };
    reset_mocks(context.producer_count);
    fail_clock_call = 3;
    ack_end_result = (mpmc_result_t){.status = MPMC_ERR_STATE, .system_errno = 0};
    mpmc_result_t result = producer_run(&context, &job);
    if (check(
            result.status == MPMC_ERR_SYS && result.system_errno == EINVAL,
            "clock failure must keep status ERR_SYS and the original errno"
        )
        || check(
            send_calls == 2 && ack_wait_calls == 0 && ack_end_calls == 1 && event_count == 4
                && memcmp(events, "BSSE", 4) == 0,
            "clock failure: stop, skip wait, end ACK once"
        )) {
        return 1;
    }

    job.ack_mode = PRODUCER_ACK_NONE;
    reset_mocks(context.producer_count);
    fail_clock_call = 1;
    result = producer_run(&context, &job);
    if (check(
            result.status == MPMC_ERR_SYS && result.system_errno == EINVAL && event_count == 0
                && ack_end_calls == 0,
            "clock failure on first message without ACK sends nothing"
        )) {
        return 1;
    }
    return 0;
}

/* sent_at: CLOCK_REALTIME lido uma vez por mensagem, logo antes do respectivo envio. */
static int test_timestamp_taken_per_send(void) {
    struct ipc_context context = {.producer_count = 3};
    producer_job_t job = {
        .producer_id = 3,
        .message_count = 4,
        .ack_mode = PRODUCER_ACK_NONE,
        .operation_id = 700,
    };
    reset_mocks(context.producer_count);
    mpmc_result_t result = producer_run(&context, &job);
    if (check(
            result.status == MPMC_OK && clock_calls == 4 && clock_id_always_realtime,
            "one CLOCK_REALTIME read per message"
        )) {
        return 1;
    }
    for (unsigned index = 0; index < 4; ++index) {
        if (check(
                clock_calls_at_send[index] == index + 1,
                "timestamp of message k is read after send k-1 and before send k"
            )
            || check(
                recorded_messages[index].sent_at.tv_sec == 1000000 + (time_t)(index + 1)
                    && recorded_messages[index].sent_at.tv_nsec == 7,
                "sent_at must carry the clock value read for that message"
            )) {
            return 1;
        }
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
    send_calls = 0;
    ack_begin_calls = 0;
    ack_wait_calls = 0;
    ack_end_calls = 0;
    fail_send_call = 0;
    send_result = (mpmc_result_t){.status = MPMC_ERR_SYS};
    ack_begin_result = (mpmc_result_t){.status = MPMC_OK};
    ack_wait_result = (mpmc_result_t){.status = MPMC_OK};
    ack_end_result = (mpmc_result_t){.status = MPMC_OK};
    memset(destinations, 0, sizeof destinations);
    memset(recorded_messages, 0, sizeof recorded_messages);
    memset(events, 0, sizeof events);
    event_count = 0;
    memset(send_contexts, 0, sizeof send_contexts);
    memset(clock_calls_at_send, 0, sizeof clock_calls_at_send);
    begin_context = wait_context = end_context = nullptr;
    begin_operation = wait_operation = end_operation = 0;
    begin_expected = 0;
    clock_calls = 0;
    fail_clock_call = 0;
    clock_id_always_realtime = true;
}

/** @brief Preserva o progresso de produção mesmo quando o trabalho não conclui. */
static int test_reported_progress(void) {
    struct ipc_context context = {.producer_count = 1};
    producer_job_t job = {
        .producer_id = 1, .message_count = 5, .ack_mode = PRODUCER_ACK_WAIT, .operation_id = 400};
    producer_result_t progress;
    reset_mocks(1);
    fail_send_call = 3;
    send_result = (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = EIO};
    mpmc_result_t result = producer_run_with_result(&context, &job, &progress);
    if (check(
            result.status == MPMC_ERR_SYS && progress.produced_count == 2,
            "failed third send reports two successful sends"
        )
        || check(
            progress.error.status == result.status && progress.error.system_errno == EIO,
            "progress preserves primary cause"
        ))
        return 1;
    reset_mocks(1);
    ack_wait_result = (mpmc_result_t){.status = MPMC_ERR_INTERRUPTED, .system_errno = EINTR};
    result = producer_run_with_result(&context, &job, &progress);
    return check(
        result.status == MPMC_ERR_INTERRUPTED && progress.produced_count == 5,
        "full production does not imply successful ACK completion"
    );
}
