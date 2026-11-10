/**
 * @file msg.c
 * @brief Implementação dos wrappers sobre filas de mensagens System V.
 * @ingroup sysv_primitives
 *
 * @details
 * Traduz msgget(), msgsnd(), msgrcv() e msgctl() em mpmc_result_t, conforme os
 * contratos de msg.h. O envelope exigido pelo kernel, `{long mtype; corpo}`, é
 * montado aqui e o `msgsz` passado ao kernel exclui o campo `mtype`. EINTR não é
 * repetido: é devolvido ao chamador como MPMC_ERR_INTERRUPTED.
 */
#include "mpmc/ipc/msg.h"

#include <errno.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ipc.h>
#include <sys/msg.h>

#include "mpmc/diagnostic.h"

/** @brief Envelope de msgsnd()/msgrcv(): tipo seguido do corpo. */
typedef struct {
    long mtype;            /**< Tipo exigido pelo kernel, excluído de msgsz. */
    unsigned char mtext[]; /**< Corpo binário contíguo ao campo mtype. */
} msg_envelope_t;

static_assert(offsetof(msg_envelope_t, mtext) == sizeof(long), "mtext deve seguir mtype");

static msg_envelope_t *envelope_alloc(size_t body_size);
static mpmc_result_t msg_error_result(int error);

mpmc_result_t sysv_msg_create(int *out_msqid) {
    mpmc_diagnostic_scope_t diagnostic_scope;
    mpmc_diagnostic_begin(&diagnostic_scope, __func__, false, true);

    if (out_msqid == nullptr)
        return mpmc_diagnostic_end(
            &diagnostic_scope, (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG}
        );

    int msqid = msgget(IPC_PRIVATE, IPC_CREAT | 0600);
    if (msqid == -1) {
        return mpmc_diagnostic_end(
            &diagnostic_scope,
            (mpmc_result_t){
                .status = MPMC_ERR_SYS,
                .system_errno = errno,
            }
        );
    }

    *out_msqid = msqid;
    return mpmc_diagnostic_end(&diagnostic_scope, (mpmc_result_t){.status = MPMC_OK});
}

mpmc_result_t sysv_msg_send(int msqid, long type, const void *payload, size_t size, bool nowait) {
    mpmc_diagnostic_scope_t diagnostic_scope;
    mpmc_diagnostic_begin(&diagnostic_scope, __func__, false, true);
    diagnostic_scope.resource_id = msqid;

    if ((size > 0 && payload == nullptr) || size > SIZE_MAX - offsetof(msg_envelope_t, mtext)) {
        return mpmc_diagnostic_end(
            &diagnostic_scope, (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG}
        );
    }

    msg_envelope_t *envelope = envelope_alloc(size);
    if (envelope == nullptr) {
        return mpmc_diagnostic_end(
            &diagnostic_scope,
            (mpmc_result_t){
                .status = MPMC_ERR_SYS,
                .system_errno = errno,
            }
        );
    }

    envelope->mtype = type;
    if (size > 0) {
        memcpy(envelope->mtext, payload, size);
    }

    int rc = msgsnd(msqid, envelope, size, nowait ? IPC_NOWAIT : 0);
    int err = errno;

    free(envelope);

    if (rc == 0) return mpmc_diagnostic_end(&diagnostic_scope, (mpmc_result_t){.status = MPMC_OK});

    return mpmc_diagnostic_end(&diagnostic_scope, msg_error_result(err));
}

mpmc_result_t sysv_msg_receive(
    int msqid,
    long desired_type,
    void *out_payload,
    size_t capacity,
    bool nowait,
    ssize_t *out_size
) {
    mpmc_diagnostic_scope_t diagnostic_scope;
    mpmc_diagnostic_begin(&diagnostic_scope, __func__, false, true);
    diagnostic_scope.resource_id = msqid;

    return mpmc_diagnostic_end(
        &diagnostic_scope,
        sysv_msg_receive_typed(
            msqid, desired_type, out_payload, capacity, nowait, out_size, nullptr
        )
    );
}

mpmc_result_t sysv_msg_receive_typed(
    int msqid,
    long desired_type,
    void *out_payload,
    size_t capacity,
    bool nowait,
    ssize_t *out_size,
    long *out_type
) {
    mpmc_diagnostic_scope_t diagnostic_scope;
    mpmc_diagnostic_begin(&diagnostic_scope, __func__, false, true);
    diagnostic_scope.resource_id = msqid;

    if (out_size == nullptr || (capacity > 0 && out_payload == nullptr)
        || capacity > SIZE_MAX - offsetof(msg_envelope_t, mtext)) {
        return mpmc_diagnostic_end(
            &diagnostic_scope, (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG}
        );
    }

    msg_envelope_t *envelope = envelope_alloc(capacity);
    if (envelope == nullptr) {
        return mpmc_diagnostic_end(
            &diagnostic_scope,
            (mpmc_result_t){
                .status = MPMC_ERR_SYS,
                .system_errno = errno,
            }
        );
    }

    /* Sem MSG_NOERROR: mensagem maior que `capacity` falha com E2BIG e fica na fila. */
    ssize_t received = msgrcv(msqid, envelope, capacity, desired_type, nowait ? IPC_NOWAIT : 0);
    int err = errno;

    if (received >= 0) {
        long received_type = envelope->mtype;
        if (received > 0) {
            memcpy(out_payload, envelope->mtext, (size_t)received);
        }
        free(envelope);
        *out_size = received;
        if (out_type != nullptr) {
            *out_type = received_type;
        }
        return mpmc_diagnostic_end(&diagnostic_scope, (mpmc_result_t){.status = MPMC_OK});
    }

    free(envelope);
    return mpmc_diagnostic_end(&diagnostic_scope, msg_error_result(err));
}

mpmc_result_t sysv_msg_remove(int msqid) {
    mpmc_diagnostic_scope_t diagnostic_scope;
    mpmc_diagnostic_begin(&diagnostic_scope, __func__, false, true);
    diagnostic_scope.resource_id = msqid;

    if (msgctl(msqid, IPC_RMID, nullptr) == -1)
        return mpmc_diagnostic_end(&diagnostic_scope, msg_error_result(errno));

    return mpmc_diagnostic_end(&diagnostic_scope, (mpmc_result_t){.status = MPMC_OK});
}

/**
 * @brief Aloca espaço contíguo para o tipo System V e o corpo binário.
 * @param body_size Quantidade de bytes reservados para o corpo.
 * @return Envelope alocado por malloc(), ou nullptr se a alocação falhar.
 * @pre body_size deve permitir a soma com offsetof(msg_envelope_t, mtext) sem overflow.
 * @note O conteúdo não é inicializado; o chamador preenche o tipo e o corpo e libera
 * o envelope com free(). Em falha, errno é o informado por malloc().
 */
static msg_envelope_t *envelope_alloc(size_t body_size) {
    return malloc(offsetof(msg_envelope_t, mtext) + body_size);
}

/**
 * @brief Traduz uma falha de operação de fila depois da liberação do envelope.
 * @param error errno capturado antes de qualquer chamada de limpeza.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_ERR_INVALID_ARG: error igual a EINVAL ou EFAULT.
 * - MPMC_ERR_WOULD_BLOCK: error igual a EAGAIN ou ENOMSG.
 * - MPMC_ERR_INTERRUPTED: error igual a EINTR.
 * - MPMC_ERR_SHUTDOWN: error igual a EIDRM.
 * - MPMC_ERR_SYS: outra causa de sistema, inclusive E2BIG.
 * Em todos os resultados, system_errno recebe error.
 */
static mpmc_result_t msg_error_result(int error) {
    mpmc_status_t status;

    switch (error) {
        case EINVAL:
        case EFAULT:
            status = MPMC_ERR_INVALID_ARG;
            break;
        case EAGAIN:
        case ENOMSG:
            status = MPMC_ERR_WOULD_BLOCK;
            break;
        case EINTR:
            status = MPMC_ERR_INTERRUPTED;
            break;
        case EIDRM:
            status = MPMC_ERR_SHUTDOWN;
            break;
        default:
            status = MPMC_ERR_SYS;
            break;
    }

    return (mpmc_result_t){
        .status = status,
        .system_errno = error,
    };
}
