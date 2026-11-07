#include "mpmc/workers/producer.h"

#include <stdio.h>
#include <string.h>
#include <unistd.h>

int main(void) {
    producer_sequence_t sequence = {0};
    producer_job_t job = {
        .producer_id = 0u,
        .message_count = 3u,
        .ack_mode = PRODUCER_ACK_WAIT,
        .operation_id = 42u,
    };
    message_t messages[3];

    for (uint32_t index = 0u; index < 3u; ++index) {
        if (producer_message_next(&messages[index], &sequence, &job) != MPMC_OK
            || messages[index].sender_pid != getpid()
            || messages[index].sequence != index + 1u
            || messages[index].operation_id != job.operation_id
            || !messages[index].ack_required || messages[index].sent_at.tv_sec <= 0
            || messages[index].sent_at.tv_nsec < 0 || messages[index].sent_at.tv_nsec >= 1000000000L) {
            fputs("message metadata test failed\n", stderr);
            return 1;
        }
    }

    producer_job_t no_ack = job;
    no_ack.ack_mode = PRODUCER_ACK_NONE;
    message_t message;
    if (producer_message_next(&message, &sequence, &no_ack) != MPMC_OK
        || message.sequence != 4u || message.ack_required) {
        fputs("ack metadata test failed\n", stderr);
        return 1;
    }

    message_t unchanged;
    memset(&unchanged, 0xA5, sizeof unchanged);
    message_t before = unchanged;
    producer_job_t invalid = job;
    invalid.operation_id = 0u;
    if (producer_message_next(&unchanged, &sequence, &invalid) != MPMC_ERR_INVALID_ARG
        || memcmp(&unchanged, &before, sizeof unchanged) != 0) {
        fputs("invalid operation_id test failed\n", stderr);
        return 1;
    }

    puts("Message metadata tests passed.");
    return 0;
}
