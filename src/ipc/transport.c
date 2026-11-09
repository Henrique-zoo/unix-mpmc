#include "mpmc/ipc/transport.h"

#include <errno.h>
#include <limits.h>
#include <stdlib.h>

#include "mpmc/ipc/msg_backend.h"
#include "mpmc/ipc/shm_backend.h"

struct ipc_context {
    ipc_mode_t mode;
    size_t producer_count;
    msg_backend_t *message_backend;
    shm_backend_t *shared_backend;
};

/**
 * @brief Resultado de falha local, sem causa de sistema associada.
 * @param status Código de erro do projeto.
 * @return mpmc_result_t com system_errno zero.
 */
static mpmc_result_t local_error(mpmc_status_t status) {
    return (mpmc_result_t){.status = status};
}

/**
 * @brief Valida o índice lógico antes de despachar ao backend.
 * @details Os dois backends rejeitam índices fora de 1..p, mas com status
 * diferentes (SHM: MPMC_ERR_INVALID_INDEX; MSG: MPMC_ERR_INVALID_ARG). A fachada
 * decide antes do despacho para que o chamador veja o mesmo erro nos dois modos.
 */
static bool logical_index_valid(const ipc_context_t *context, size_t producer_id) {
    return producer_id >= 1u && producer_id <= context->producer_count;
}

/**
 * @brief Identificador de operação aceito por ambos os paradigmas.
 * @details No modo 2 o ID vira o mtype (long) do ACK; o mesmo limite vale no modo 1,
 * para que um trabalho não seja aceito num paradigma e recusado no outro.
 */
static bool operation_id_valid(uint64_t operation_id) {
    return operation_id != 0u && operation_id <= (uint64_t)LONG_MAX;
}

mpmc_result_t ipc_init(ipc_context_t **out, size_t producer_count, ipc_mode_t mode) {
    if (out == NULL || producer_count == 0u
        || (mode != IPC_SHARED_MEMORY && mode != IPC_MESSAGE_QUEUE)) {
        return local_error(MPMC_ERR_INVALID_ARG);
    }
    *out = NULL;
    ipc_context_t *context = calloc(1u, sizeof(*context));
    if (context == NULL) {
        return (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = errno};
    }
    context->mode = mode;
    context->producer_count = producer_count;

    /* Cada backend desfaz o que já criou antes de devolver erro; aqui só resta a
     * alocação da própria fachada. */
    mpmc_result_t result;
    if (mode == IPC_MESSAGE_QUEUE) {
        result = msg_backend_create(&context->message_backend, producer_count);
    } else {
        result = shm_backend_create(&context->shared_backend, producer_count, NULL);
    }
    if (result.status != MPMC_OK) {
        free(context);
        return result;
    }
    *out = context;
    return (mpmc_result_t){.status = MPMC_OK};
}

size_t ipc_producer_count(const ipc_context_t *context) {
    return context == NULL ? 0u : context->producer_count;
}

mpmc_result_t ipc_send(ipc_context_t *context, size_t producer_id, const message_t *message) {
    if (context == NULL || message == NULL) return local_error(MPMC_ERR_INVALID_ARG);
    if (!logical_index_valid(context, producer_id)) return local_error(MPMC_ERR_INVALID_INDEX);
    return context->mode == IPC_MESSAGE_QUEUE
        ? msg_backend_send(context->message_backend, producer_id, message)
        : shm_backend_send(context->shared_backend, producer_id, message);
}

mpmc_result_t ipc_receive(
    ipc_context_t *context,
    size_t producer_id,
    message_t *out,
    bool blocking
) {
    if (context == NULL || out == NULL) return local_error(MPMC_ERR_INVALID_ARG);
    if (!logical_index_valid(context, producer_id)) return local_error(MPMC_ERR_INVALID_INDEX);
    return context->mode == IPC_MESSAGE_QUEUE
        ? msg_backend_receive(context->message_backend, producer_id, out, blocking)
        : shm_backend_receive(context->shared_backend, producer_id, out, blocking);
}

mpmc_result_t ipc_ack_begin(ipc_context_t *context, uint64_t operation_id, unsigned expected) {
    if (context == NULL || !operation_id_valid(operation_id) || expected < MPMC_MIN_MESSAGES
        || expected > MPMC_MAX_MESSAGES) {
        return local_error(MPMC_ERR_INVALID_ARG);
    }
    return context->mode == IPC_MESSAGE_QUEUE
        ? msg_backend_ack_begin(context->message_backend, operation_id, expected)
        : shm_backend_ack_begin(context->shared_backend, operation_id, expected);
}

mpmc_result_t ipc_ack_wait(ipc_context_t *context, uint64_t operation_id) {
    if (context == NULL || !operation_id_valid(operation_id)) {
        return local_error(MPMC_ERR_INVALID_ARG);
    }
    return context->mode == IPC_MESSAGE_QUEUE
        ? msg_backend_ack_wait(context->message_backend, operation_id)
        : shm_backend_ack_wait(context->shared_backend, operation_id);
}

mpmc_result_t ipc_ack_end(ipc_context_t *context, uint64_t operation_id) {
    if (context == NULL || !operation_id_valid(operation_id)) {
        return local_error(MPMC_ERR_INVALID_ARG);
    }
    return context->mode == IPC_MESSAGE_QUEUE
        ? msg_backend_ack_end(context->message_backend, operation_id)
        : shm_backend_ack_end(context->shared_backend, operation_id);
}

mpmc_result_t ipc_ack_abandon(ipc_context_t *context, uint64_t operation_id) {
    if (context == NULL || !operation_id_valid(operation_id)) {
        return local_error(MPMC_ERR_INVALID_ARG);
    }
    if (context->mode == IPC_MESSAGE_QUEUE) {
        return msg_backend_ack_abandon(context->message_backend, operation_id);
    }
    return (mpmc_result_t){.status = MPMC_OK};
}

mpmc_result_t ipc_poll_events(ipc_context_t *context) {
    if (context == NULL) return local_error(MPMC_ERR_INVALID_ARG);
    if (context->mode == IPC_MESSAGE_QUEUE) {
        return msg_backend_poll_events(context->message_backend);
    }
    return (mpmc_result_t){.status = MPMC_OK};
}

mpmc_result_t ipc_consumed_count(ipc_context_t *context, size_t producer_id, uint64_t *out_count) {
    if (context == NULL || out_count == NULL) return local_error(MPMC_ERR_INVALID_ARG);
    if (!logical_index_valid(context, producer_id)) return local_error(MPMC_ERR_INVALID_INDEX);
    if (context->mode == IPC_MESSAGE_QUEUE) {
        return msg_backend_consumed_count(context->message_backend, producer_id, out_count);
    }
    return shm_backend_consumed_count(context->shared_backend, producer_id, out_count);
}

void ipc_worker_close(ipc_context_t *context) {
    if (context == NULL) return;
    if (context->mode == IPC_MESSAGE_QUEUE) {
        msg_backend_worker_close(context->message_backend);
    } else {
        (void)shm_backend_worker_close(context->shared_backend, NULL);
    }
    free(context);
}

void ipc_destroy(ipc_context_t *context) {
    if (context == NULL) return;
    if (context->mode == IPC_MESSAGE_QUEUE) {
        msg_backend_destroy(context->message_backend);
    } else {
        (void)shm_backend_destroy(context->shared_backend, NULL);
    }
    free(context);
}
