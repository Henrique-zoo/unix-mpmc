#include "mpmc/ipc/shm_backend.h"

#include <stdlib.h>

struct shm_backend {
    size_t producer_count;
    uint64_t *consumed_counts;
};

mpmc_result_t shm_backend_create(shm_backend_t **out, size_t producer_count) {
    if (out == NULL || producer_count == 0u) {
        return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};
    }
    *out = NULL;
    shm_backend_t *backend = calloc(1u, sizeof(*backend));
    if (backend == NULL) {
        return (mpmc_result_t){.status = MPMC_ERR_SYS};
    }
    backend->producer_count = producer_count;
    backend->consumed_counts = calloc(producer_count, sizeof(*backend->consumed_counts));
    if (backend->consumed_counts == NULL) {
        free(backend);
        return (mpmc_result_t){.status = MPMC_ERR_SYS};
    }
    *out = backend;
    return (mpmc_result_t){.status = MPMC_OK};
}

mpmc_result_t
shm_backend_send(shm_backend_t *backend, size_t producer_id, const message_t *message) {
    (void)backend; (void)producer_id; (void)message;
    return (mpmc_result_t){.status = MPMC_ERR_STATE};
}

mpmc_result_t
shm_backend_receive(shm_backend_t *backend, size_t producer_id, message_t *out, bool blocking) {
    (void)backend; (void)producer_id; (void)out; (void)blocking;
    return (mpmc_result_t){.status = MPMC_ERR_STATE};
}

mpmc_result_t
shm_backend_ack_begin(shm_backend_t *backend, uint64_t operation_id, unsigned expected) {
    (void)backend; (void)operation_id; (void)expected;
    return (mpmc_result_t){.status = MPMC_ERR_STATE};
}

mpmc_result_t shm_backend_ack_wait(shm_backend_t *backend, uint64_t operation_id) {
    (void)backend; (void)operation_id;
    return (mpmc_result_t){.status = MPMC_ERR_STATE};
}

mpmc_result_t shm_backend_ack_end(shm_backend_t *backend, uint64_t operation_id) {
    (void)backend; (void)operation_id;
    return (mpmc_result_t){.status = MPMC_ERR_STATE};
}

mpmc_result_t
shm_backend_consumed_count(shm_backend_t *backend, size_t producer_id, uint64_t *out_count) {
    if (backend == NULL || out_count == NULL || producer_id == 0u
        || producer_id > backend->producer_count) {
        return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};
    }
    *out_count = backend->consumed_counts[producer_id - 1u];
    return (mpmc_result_t){.status = MPMC_OK};
}

void shm_backend_worker_close(shm_backend_t *backend) {
    if (backend == NULL) return;
    free(backend->consumed_counts);
    free(backend);
}

void shm_backend_destroy(shm_backend_t *backend) {
    shm_backend_worker_close(backend);
}