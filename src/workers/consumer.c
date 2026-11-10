/**
 * @file consumer.c
 * @brief Execução de processos consumidores pela fachada IPC.
 * @ingroup workers
 */
#include "mpmc/workers/consumer.h"

#include "mpmc/diagnostic.h"

mpmc_result_t consumer_run(
    ipc_context_t *transport,
    const consumer_job_t *job,
    consumer_result_t *out
) {
    mpmc_diagnostic_scope_t diagnostic_scope;
    mpmc_diagnostic_begin(&diagnostic_scope, __func__, false, false);

    if (out == nullptr) {
        return mpmc_diagnostic_end(
            &diagnostic_scope, (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG}
        );
    }

    *out = (consumer_result_t){.error = {.status = MPMC_ERR_INVALID_ARG}};
    if (transport == nullptr || job == nullptr || job->message_count < MPMC_MIN_MESSAGES
        || job->message_count > MPMC_MAX_MESSAGES
        || (job->ack_mode != CONSUMER_BLOCKING && job->ack_mode != CONSUMER_NONBLOCKING)) {
        return mpmc_diagnostic_end(&diagnostic_scope, out->error);
    }

    size_t producer_count = ipc_producer_count(transport);
    if (producer_count == 0 || job->producer_id == 0 || job->producer_id > producer_count) {
        return mpmc_diagnostic_end(&diagnostic_scope, out->error);
    }

    const bool blocking = job->ack_mode == CONSUMER_BLOCKING;
    out->error = (mpmc_result_t){.status = MPMC_OK};
    while (out->consumed_count < job->message_count) {
        message_t message;
        mpmc_result_t result = ipc_receive(transport, job->producer_id, &message, blocking);
        if (result.status != MPMC_OK) {
            out->error = result;
            return mpmc_diagnostic_end(&diagnostic_scope, result);
        }
        ++out->consumed_count;
    }

    return mpmc_diagnostic_end(&diagnostic_scope, out->error);
}
