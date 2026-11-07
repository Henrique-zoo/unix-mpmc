#include "mpmc/workers/producer.h"

#include <time.h>
#include <unistd.h>

mpmc_status_t
producer_message_next(message_t *out, producer_sequence_t *sequence, const producer_job_t *job) {
    if (out == NULL || sequence == NULL || job == NULL || job->operation_id == 0u
        || (job->ack_mode != PRODUCER_ACK_WAIT && job->ack_mode != PRODUCER_ACK_NONE)) {
        return MPMC_ERR_INVALID_ARG;
    }
    if (sequence->last_sequence == UINT32_MAX) {
        return MPMC_ERR_STATE;
    }

    struct timespec sent_at;
    if (clock_gettime(CLOCK_REALTIME, &sent_at) == -1) {
        return MPMC_ERR_SYS;
    }

    uint32_t next = sequence->last_sequence + 1u;
    *out = (message_t){
        .sender_pid = getpid(),
        .sequence = next,
        .sent_at = sent_at,
        .operation_id = job->operation_id,
        .ack_required = job->ack_mode == PRODUCER_ACK_WAIT,
    };
    sequence->last_sequence = next;
    return MPMC_OK;
}
