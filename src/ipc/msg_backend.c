#include "mpmc/ipc/msg_backend.h"

#include <errno.h>
#include <limits.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <time.h>

#include "mpmc/ipc/msg.h"

typedef struct {
    uint64_t operation_id;
    unsigned expected;
    unsigned received;
} ack_registration_t;

struct msg_backend {
    int *data_queues;
    size_t producer_count;
    int event_queue;
    int ack_queue;
    uint64_t *consumed_counts;
    ack_registration_t *registrations;
    size_t registration_count;
    size_t registration_capacity;
    uint64_t *abandoned;
    size_t abandoned_count;
    size_t abandoned_capacity;
};

typedef struct {
    size_t producer_id;
    uint64_t count;
} consumption_event_t;

typedef struct {
    uint64_t operation_id;
} ack_event_t;

static ack_registration_t *msg_backend_find_registration(
    msg_backend_t *backend,
    uint64_t operation_id
) {
    for (size_t index = 0u; index < backend->registration_count; ++index) {
        if (backend->registrations[index].operation_id == operation_id) {
            return &backend->registrations[index];
        }
    }
    return NULL;
}

static mpmc_result_t msg_backend_ack_receive(
    msg_backend_t *backend,
    ack_registration_t *registration,
    bool nowait,
    bool account,
    bool *out_received
) {
    ack_event_t ack;
    ssize_t received = 0;
    mpmc_result_t result = sysv_msg_receive(
        backend->ack_queue,
        (long)registration->operation_id,
        &ack,
        sizeof(ack),
        nowait,
        &received
    );
    if (result.status != MPMC_OK) {
        return result;
    }
    if ((size_t)received != sizeof(ack) || ack.operation_id != registration->operation_id
        || (account && registration->received >= registration->expected)) {
        return (mpmc_result_t){.status = MPMC_ERR_STATE};
    }
    if (account) {
        ++registration->received;
    }
    *out_received = true;
    return (mpmc_result_t){.status = MPMC_OK};
}

static mpmc_result_t msg_backend_ack_drain(
    msg_backend_t *backend,
    ack_registration_t *registration,
    bool account
) {
    for (;;) {
        bool received = false;
        mpmc_result_t result =
            msg_backend_ack_receive(backend, registration, true, account, &received);
        if (result.status == MPMC_ERR_WOULD_BLOCK) {
            return (mpmc_result_t){.status = MPMC_OK};
        }
        if (result.status != MPMC_OK) {
            return result;
        }
    }
}

/**
 * @brief Descarta sem bloquear todos os ACKs de uma operação encerrada.
 * @details Usada pelo supervisor: ACKs de operações sem dono só ocupam a fila de controle.
 */
static mpmc_result_t msg_backend_purge_operation(msg_backend_t *backend, uint64_t operation_id) {
    for (;;) {
        ack_event_t ack;
        ssize_t received = 0;
        mpmc_result_t result = sysv_msg_receive(
            backend->ack_queue, (long)operation_id, &ack, sizeof(ack), true, &received
        );
        if (result.status == MPMC_ERR_INTERRUPTED) {
            continue;
        }
        if (result.status == MPMC_ERR_WOULD_BLOCK) {
            return (mpmc_result_t){.status = MPMC_OK};
        }
        if (result.status != MPMC_OK) {
            return result;
        }
    }
}

static mpmc_result_t msg_backend_ack_backoff(void) {
    const struct timespec delay = {.tv_sec = 0, .tv_nsec = 1000000};
    if (nanosleep(&delay, NULL) == 0) {
        return (mpmc_result_t){.status = MPMC_OK};
    }
    int sleep_errno = errno;
    return (mpmc_result_t){
        .status = sleep_errno == EINTR ? MPMC_ERR_INTERRUPTED : MPMC_ERR_SYS,
        .system_errno = sleep_errno,
    };
}

static mpmc_result_t msg_backend_send_retry(
    int msqid,
    long type,
    const void *payload,
    size_t size,
    bool nowait
) {
    for (;;) {
        mpmc_result_t result = sysv_msg_send(msqid, type, payload, size, nowait);
        if (result.status != MPMC_ERR_INTERRUPTED) {
            return result;
        }
    }
}

static mpmc_result_t msg_backend_remove_created(msg_backend_t *backend) {
    for (size_t index = 0u; index < backend->producer_count; ++index) {
        if (backend->data_queues[index] != -1) {
            (void)sysv_msg_remove(backend->data_queues[index]);
        }
    }
    if (backend->event_queue != -1) {
        (void)sysv_msg_remove(backend->event_queue);
    }
    if (backend->ack_queue != -1) {
        (void)sysv_msg_remove(backend->ack_queue);
    }
    return (mpmc_result_t){.status = MPMC_OK};
}

mpmc_result_t msg_backend_create(msg_backend_t **out, size_t producer_count) {
    if (out == NULL) {
        return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};
    }
    *out = NULL;
    if (producer_count == 0u || producer_count > SIZE_MAX / sizeof(int)
        || producer_count > SIZE_MAX / sizeof(uint64_t)) {
        return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};
    }

    msg_backend_t *backend = calloc(1u, sizeof(*backend));
    if (backend == NULL) {
        return (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = errno};
    }
    *backend = (msg_backend_t){.event_queue = -1, .ack_queue = -1};

    backend->data_queues = malloc(producer_count * sizeof(*backend->data_queues));
    backend->consumed_counts = calloc(producer_count, sizeof(*backend->consumed_counts));
    if (backend->data_queues == NULL || backend->consumed_counts == NULL) {
        int allocation_errno = errno;
        free(backend->data_queues);
        free(backend->consumed_counts);
        free(backend);
        return (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = allocation_errno};
    }
    for (size_t index = 0u; index < producer_count; ++index) {
        backend->data_queues[index] = -1;
    }
    backend->producer_count = producer_count;

    for (size_t index = 0u; index < producer_count; ++index) {
        mpmc_result_t result = sysv_msg_create(&backend->data_queues[index]);
        if (result.status != MPMC_OK) {
            msg_backend_remove_created(backend);
            free(backend->consumed_counts);
            free(backend->data_queues);
            free(backend);
            return result;
        }
    }
    mpmc_result_t result = sysv_msg_create(&backend->event_queue);
    if (result.status != MPMC_OK) {
        msg_backend_remove_created(backend);
        free(backend->consumed_counts);
        free(backend->data_queues);
        free(backend);
        return result;
    }
    result = sysv_msg_create(&backend->ack_queue);
    if (result.status != MPMC_OK) {
        msg_backend_remove_created(backend);
        free(backend->consumed_counts);
        free(backend->data_queues);
        free(backend);
        return result;
    }
    *out = backend;
    return (mpmc_result_t){.status = MPMC_OK};
}

mpmc_result_t msg_backend_send(
    msg_backend_t *backend,
    size_t producer_id,
    const message_t *message
) {
    if (backend == NULL || message == NULL || producer_id == 0u
        || producer_id > backend->producer_count) {
        return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};
    }
    ack_registration_t *registration =
        message->operation_id == 0u ? NULL
                                    : msg_backend_find_registration(backend, message->operation_id);
    if (registration == NULL) {
        return sysv_msg_send(
            backend->data_queues[producer_id - 1u], 1, message, sizeof(*message), false
        );
    }

    if (registration->received >= registration->expected) {
        return (mpmc_result_t){.status = MPMC_ERR_STATE};
    }
    for (;;) {
        mpmc_result_t result = sysv_msg_send(
            backend->data_queues[producer_id - 1u],
            2,
            message,
            sizeof(*message),
            true
        );
        if (result.status == MPMC_OK) {
            return result;
        }
        if (result.status != MPMC_ERR_WOULD_BLOCK) {
            return result;
        }

        result = msg_backend_ack_drain(backend, registration, true);
        if (result.status != MPMC_OK) {
            return result;
        }
        result = msg_backend_ack_backoff();
        if (result.status != MPMC_OK) {
            return result;
        }
    }
}

mpmc_result_t msg_backend_receive(
    msg_backend_t *backend,
    size_t producer_id,
    message_t *out,
    bool blocking
) {
    if (backend == NULL || out == NULL || producer_id == 0u
        || producer_id > backend->producer_count) {
        return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};
    }

    ssize_t received = 0;
    long type = 0;
    mpmc_result_t result = sysv_msg_receive_typed(
        backend->data_queues[producer_id - 1u], 0, out, sizeof(*out), !blocking, &received, &type
    );
    if (result.status != MPMC_OK) {
        return result;
    }
    if ((size_t)received != sizeof(*out)) {
        return (mpmc_result_t){.status = MPMC_ERR_STATE};
    }
    if (type != 1 && type != 2) {
        return (mpmc_result_t){.status = MPMC_ERR_STATE};
    }

    consumption_event_t event = {.producer_id = producer_id, .count = 1u};
    result = msg_backend_send_retry(backend->event_queue, 1, &event, sizeof(event), false);
    if (result.status != MPMC_OK) {
        return result;
    }
    if (type == 2) {
        ack_event_t ack = {.operation_id = out->operation_id};
        result = msg_backend_send_retry(
            backend->ack_queue, (long)out->operation_id, &ack, sizeof(ack), false
        );
        if (result.status != MPMC_OK) {
            return result;
        }
    }
    return (mpmc_result_t){.status = MPMC_OK};
}

mpmc_result_t msg_backend_ack_begin(
    msg_backend_t *backend,
    uint64_t operation_id,
    unsigned expected
) {
    if (backend == NULL || operation_id == 0u || operation_id > (uint64_t)LONG_MAX
        || expected == 0u || expected > 1024u) {
        return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};
    }
    if (msg_backend_find_registration(backend, operation_id) != NULL) {
        return (mpmc_result_t){.status = MPMC_ERR_STATE};
    }
    if (backend->registration_count == backend->registration_capacity) {
        size_t capacity =
            backend->registration_capacity == 0u ? 8u : backend->registration_capacity * 2u;
        if (capacity < backend->registration_capacity
            || capacity > SIZE_MAX / sizeof(*backend->registrations)) {
            return (mpmc_result_t){.status = MPMC_ERR_SYS};
        }
        ack_registration_t *registrations =
            realloc(backend->registrations, capacity * sizeof(*registrations));
        if (registrations == NULL) {
            return (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = errno};
        }
        backend->registrations = registrations;
        backend->registration_capacity = capacity;
    }
    backend->registrations[backend->registration_count++] = (ack_registration_t){
        .operation_id = operation_id,
        .expected = expected,
        .received = 0u,
    };
    return (mpmc_result_t){.status = MPMC_OK};
}

mpmc_result_t msg_backend_ack_wait(msg_backend_t *backend, uint64_t operation_id) {
    if (backend == NULL || operation_id == 0u) {
        return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};
    }
    ack_registration_t *registration = msg_backend_find_registration(backend, operation_id);
    if (registration == NULL) {
        return (mpmc_result_t){.status = MPMC_ERR_STATE};
    }
    while (registration->received < registration->expected) {
        bool received = false;
        mpmc_result_t result =
            msg_backend_ack_receive(backend, registration, false, true, &received);
        if (result.status != MPMC_OK) {
            return result;
        }
    }
    return (mpmc_result_t){.status = MPMC_OK};
}

mpmc_result_t msg_backend_ack_end(msg_backend_t *backend, uint64_t operation_id) {
    if (backend == NULL || operation_id == 0u) {
        return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};
    }
    ack_registration_t *registration = msg_backend_find_registration(backend, operation_id);
    if (registration == NULL) {
        return (mpmc_result_t){.status = MPMC_OK};
    }
    mpmc_result_t result = msg_backend_ack_drain(backend, registration, false);
    for (size_t index = 0u; index < backend->registration_count; ++index) {
        if (backend->registrations[index].operation_id == operation_id) {
            backend->registrations[index] = backend->registrations[--backend->registration_count];
            break;
        }
    }
    return result;
}

mpmc_result_t msg_backend_poll_events(msg_backend_t *backend) {
    if (backend == NULL) {
        return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};
    }
    for (;;) {
        consumption_event_t event;
        ssize_t received = 0;
        mpmc_result_t result =
            sysv_msg_receive(backend->event_queue, 0, &event, sizeof(event), true, &received);
        if (result.status == MPMC_ERR_INTERRUPTED) {
            continue;
        }
        if (result.status == MPMC_ERR_WOULD_BLOCK) {
            break;
        }
        if (result.status != MPMC_OK) {
            return result;
        }
        if ((size_t)received != sizeof(event) || event.producer_id == 0u
            || event.producer_id > backend->producer_count
            || UINT64_MAX - backend->consumed_counts[event.producer_id - 1u] < event.count) {
            return (mpmc_result_t){.status = MPMC_ERR_STATE};
        }
        backend->consumed_counts[event.producer_id - 1u] += event.count;
    }

    /* ACKs tardios de operações abandonadas não podem lotar Q_ack e travar consumidores. */
    mpmc_result_t first = {.status = MPMC_OK};
    for (size_t index = 0u; index < backend->abandoned_count; ++index) {
        mpmc_result_t purged = msg_backend_purge_operation(backend, backend->abandoned[index]);
        if (first.status == MPMC_OK) {
            first = purged;
        }
    }
    return first;
}

mpmc_result_t msg_backend_ack_abandon(msg_backend_t *backend, uint64_t operation_id) {
    if (backend == NULL || operation_id == 0u || operation_id > (uint64_t)LONG_MAX) {
        return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};
    }
    bool known = false;
    for (size_t index = 0u; index < backend->abandoned_count; ++index) {
        known = known || backend->abandoned[index] == operation_id;
    }
    if (!known) {
        if (backend->abandoned_count == backend->abandoned_capacity) {
            size_t capacity =
                backend->abandoned_capacity == 0u ? 8u : backend->abandoned_capacity * 2u;
            if (capacity < backend->abandoned_capacity
                || capacity > SIZE_MAX / sizeof(*backend->abandoned)) {
                return (mpmc_result_t){.status = MPMC_ERR_SYS};
            }
            uint64_t *grown = realloc(backend->abandoned, capacity * sizeof(*grown));
            if (grown == NULL) {
                return (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = errno};
            }
            backend->abandoned = grown;
            backend->abandoned_capacity = capacity;
        }
        backend->abandoned[backend->abandoned_count++] = operation_id;
    }
    return msg_backend_purge_operation(backend, operation_id);
}

mpmc_result_t msg_backend_consumed_count(
    msg_backend_t *backend,
    size_t producer_id,
    uint64_t *out_count
) {
    if (backend == NULL || out_count == NULL || producer_id == 0u
        || producer_id > backend->producer_count) {
        return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};
    }
    *out_count = backend->consumed_counts[producer_id - 1u];
    return (mpmc_result_t){.status = MPMC_OK};
}

void msg_backend_worker_close(msg_backend_t *backend) {
    if (backend == NULL) {
        return;
    }
    free(backend->data_queues);
    free(backend->consumed_counts);
    free(backend->registrations);
    free(backend->abandoned);
    free(backend);
}

void msg_backend_destroy(msg_backend_t *backend) {
    if (backend == NULL) {
        return;
    }
    msg_backend_remove_created(backend);
    free(backend->data_queues);
    free(backend->consumed_counts);
    free(backend->registrations);
    free(backend->abandoned);
    free(backend);
}
