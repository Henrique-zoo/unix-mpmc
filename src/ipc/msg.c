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
#include <stdlib.h>
#include <string.h>
#include <sys/ipc.h>
#include <sys/msg.h>

/** @brief Envelope de msgsnd()/msgrcv(): tipo seguido do corpo. */
typedef struct {
    long mtype;            /**< Tipo exigido pelo kernel, excluído de msgsz. */
    unsigned char mtext[]; /**< Corpo binário contíguo ao campo mtype. */
} msg_envelope_t;

static_assert(offsetof(msg_envelope_t, mtext) == sizeof(long), "mtext deve seguir mtype");

/**
 * @brief Aloca espaço contíguo para o tipo System V e o corpo binário.
 * @param body_size Quantidade de bytes reservados para o corpo.
 * @return Envelope alocado por malloc(), ou NULL se a alocação falhar.
 * @pre body_size deve permitir a soma com offsetof(msg_envelope_t, mtext) sem overflow.
 * @note O conteúdo não é inicializado; o chamador preenche o tipo e o corpo e libera
 * o envelope com free(). Em falha, errno é o informado por malloc().
 */
static msg_envelope_t *envelope_alloc(size_t body_size) {
    return malloc(offsetof(msg_envelope_t, mtext) + body_size);
}

mpmc_result_t sysv_msg_create(int *out_msqid) {
    if (out_msqid == NULL) return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};

    int msqid = msgget(IPC_PRIVATE, IPC_CREAT | 0600);
    if (msqid == -1) {
        return (mpmc_result_t){
            .status = MPMC_ERR_SYS,
            .system_errno = errno,
        };
    }

    *out_msqid = msqid;
    return (mpmc_result_t){.status = MPMC_OK};
}

mpmc_result_t sysv_msg_send(int msqid, long type, const void *payload, size_t size, bool nowait) {
    msg_envelope_t *envelope = envelope_alloc(size);
    if (envelope == NULL) {
        return (mpmc_result_t){
            .status = MPMC_ERR_SYS,
            .system_errno = errno,
        };
    }

    envelope->mtype = type;
    if (size > 0) {
        memcpy(envelope->mtext, payload, size);
    }

    int rc = msgsnd(msqid, envelope, size, nowait ? IPC_NOWAIT : 0);
    int err = errno;

    free(envelope);

    if (rc == 0) return (mpmc_result_t){.status = MPMC_OK};

    if (err == EINTR) {
        return (mpmc_result_t){
            .status = MPMC_ERR_INTERRUPTED,
            .system_errno = err,
        };
    }
    if (nowait && err == EAGAIN) {
        return (mpmc_result_t){
            .status = MPMC_ERR_WOULD_BLOCK,
            .system_errno = err,
        };
    }
    return (mpmc_result_t){
        .status = MPMC_ERR_SYS,
        .system_errno = err,
    };
}

mpmc_result_t sysv_msg_receive(
    int msqid, long desired_type, void *out_payload, size_t capacity, bool nowait, ssize_t *out_size
) {
    msg_envelope_t *envelope = envelope_alloc(capacity);
    if (envelope == NULL) {
        return (mpmc_result_t){
            .status = MPMC_ERR_SYS,
            .system_errno = errno,
        };
    }

    /* Sem MSG_NOERROR: mensagem maior que `capacity` falha com E2BIG e fica na fila. */
    ssize_t received = msgrcv(msqid, envelope, capacity, desired_type, nowait ? IPC_NOWAIT : 0);
    int err = errno;

    if (received >= 0) {
        if (received > 0) {
            memcpy(out_payload, envelope->mtext, (size_t)received);
        }
        free(envelope);
        *out_size = received;
        return (mpmc_result_t){.status = MPMC_OK};
    }

    free(envelope);
    if (err == EINTR) {
        return (mpmc_result_t){
            .status = MPMC_ERR_INTERRUPTED,
            .system_errno = err,
        };
    }
    if (nowait && err == ENOMSG) {
        return (mpmc_result_t){
            .status = MPMC_ERR_WOULD_BLOCK,
            .system_errno = err,
        };
    }
    return (mpmc_result_t){
        .status = MPMC_ERR_SYS,
        .system_errno = err,
    };
}

mpmc_result_t sysv_msg_remove(int msqid) {
    if (msgctl(msqid, IPC_RMID, NULL) == -1) {
        return (mpmc_result_t){
            .status = MPMC_ERR_SYS,
            .system_errno = errno,
        };
    }

    return (mpmc_result_t){.status = MPMC_OK};
}
