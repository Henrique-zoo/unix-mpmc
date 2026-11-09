/**
 * @file producer.c
 * @brief Construção dos metadados de mensagens no processo produtor.
 * @ingroup workers
 * @details Os contratos públicos estão em producer.h. O worker depende apenas
 * da fachada ipc_* e não conhece detalhes de nenhum backend.
 */
#include "mpmc/workers/producer.h"

#include <errno.h>
#include <time.h>
#include <unistd.h>

#include "producer_routing.h"

mpmc_status_t producer_message_next(
    message_t *out,
    producer_sequence_t *sequence,
    const producer_job_t *job
) {
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
    };
    sequence->last_sequence = next;
    return MPMC_OK;
}

mpmc_result_t producer_run_with_result(
    ipc_context_t *transport,
    const producer_job_t *job,
    producer_result_t *out
) {
    if (out == nullptr) return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};

    *out = (producer_result_t){.error = {.status = MPMC_ERR_INVALID_ARG}};
    if (transport == NULL || job == NULL || job->operation_id == 0u
        || job->message_count < MPMC_MIN_MESSAGES || job->message_count > MPMC_MAX_MESSAGES
        || (job->ack_mode != PRODUCER_ACK_WAIT && job->ack_mode != PRODUCER_ACK_NONE)) {
        return out->error;
    }

    size_t producer_count = ipc_producer_count(transport);
    if (producer_count == 0u) {
        return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};
    }
    if (job->producer_id > producer_count) {
        out->error = (mpmc_result_t){.status = MPMC_ERR_INVALID_INDEX};
        return out->error;
    }

    bool ack_started = false;
    if (job->ack_mode == PRODUCER_ACK_WAIT) {
        mpmc_result_t result = ipc_ack_begin(transport, job->operation_id, job->message_count);
        if (result.status != MPMC_OK) {
            out->error = result;
            return result;
        }
        ack_started = true;
    }

    producer_sequence_t sequence = {0};
    mpmc_result_t result = {.status = MPMC_OK};
    for (unsigned sent = 0u; sent < job->message_count; ++sent) {
        message_t message;
        mpmc_status_t status = producer_message_next(&message, &sequence, job);
        if (status != MPMC_OK) {
            result = (mpmc_result_t){
                .status = status,
                .system_errno = status == MPMC_ERR_SYS ? errno : 0,
            };
            break;
        }

        size_t destination = job->producer_id;
        if (destination == 0u) {
            status = producer_route_destination(message.sequence, producer_count, &destination);
            if (status != MPMC_OK) {
                result = (mpmc_result_t){.status = status};
                break;
            }
        }
        result = ipc_send(transport, destination, &message);
        if (result.status != MPMC_OK) {
            break;
        }
        ++out->produced_count;
    }

    if (result.status == MPMC_OK && ack_started) {
        result = ipc_ack_wait(transport, job->operation_id);
    }

    if (ack_started) {
        mpmc_result_t end_result = ipc_ack_end(transport, job->operation_id);
        if (result.status == MPMC_OK) {
            result = end_result;
        }
    }
    out->error = result;
    return result;
}

/** @details Mantém a interface simples para chamadores que não consultam progresso. */
mpmc_result_t producer_run(ipc_context_t *transport, const producer_job_t *job) {
    producer_result_t result;
    return producer_run_with_result(transport, job, &result);
}
