/**
 * @file msg.c
 * @brief Implementação dos wrappers sobre filas de mensagens System V.
 * @ingroup sysv_primitives
 *
 * @details
 * Traduz msgget(), msgsnd(), msgrcv() e msgctl() em mpmc_status_t, conforme os
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
    long mtype;
    unsigned char mtext[];
} msg_envelope_t;

static_assert(offsetof(msg_envelope_t, mtext) == sizeof(long), "mtext deve seguir mtype");

static msg_envelope_t *envelope_alloc(size_t body_size) {
    return malloc(offsetof(msg_envelope_t, mtext) + body_size);
}

mpmc_status_t sysv_msg_create(int *out_msqid) {
    if (out_msqid == NULL) {
        return MPMC_ERR_INVALID_ARG;
    }

    int msqid = msgget(IPC_PRIVATE, IPC_CREAT | 0600);
    if (msqid == -1) {
        return MPMC_ERR_SYS;
    }

    *out_msqid = msqid;
    return MPMC_OK;
}

mpmc_status_t sysv_msg_send(int msqid, long type, const void *payload, size_t size, bool nowait) {
    msg_envelope_t *envelope = envelope_alloc(size);
    if (envelope == NULL) {
        return MPMC_ERR_SYS;
    }

    envelope->mtype = type;
    if (size > 0) {
        memcpy(envelope->mtext, payload, size);
    }

    int rc = msgsnd(msqid, envelope, size, nowait ? IPC_NOWAIT : 0);
    int err = errno;

    free(envelope);

    if (rc == 0) {
        return MPMC_OK;
    }

    errno = err;
    if (err == EINTR) {
        return MPMC_ERR_INTERRUPTED;
    }
    if (nowait && err == EAGAIN) {
        return MPMC_ERR_WOULD_BLOCK;
    }
    return MPMC_ERR_SYS;
}

mpmc_status_t sysv_msg_receive(
    int msqid, long desired_type, void *out_payload, size_t capacity, bool nowait, ssize_t *out_size
) {
    msg_envelope_t *envelope = envelope_alloc(capacity);
    if (envelope == NULL) {
        return MPMC_ERR_SYS;
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
        return MPMC_OK;
    }

    free(envelope);
    errno = err;
    if (err == EINTR) {
        return MPMC_ERR_INTERRUPTED;
    }
    if (nowait && err == ENOMSG) {
        return MPMC_ERR_WOULD_BLOCK;
    }
    return MPMC_ERR_SYS;
}

mpmc_status_t sysv_msg_remove(int msqid) {
    if (msgctl(msqid, IPC_RMID, NULL) == -1) {
        return MPMC_ERR_SYS;
    }

    return MPMC_OK;
}
