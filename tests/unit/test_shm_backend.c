/**
 * @file test_shm_backend.c
 * @brief Testes do ciclo de vida e dos diagnósticos de cleanup do backend shm.
 * @ingroup tests
 * @details Substitui as alocações e os wrappers System V apenas no objeto do
 * backend. Injeta falhas em cada etapa sem criar recursos IPC reais e verifica
 * a continuidade da limpeza, a propriedade dos segmentos e os erros preservados.
 */

#include <errno.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "mpmc/ipc/shm_backend.h"

#define MAX_SEGMENTS 8
#define PRODUCER_COUNT 3

static unsigned checks;
static unsigned failures;

#define CHECK(condition)                                                                           \
    do {                                                                                           \
        ++checks;                                                                                  \
        if (!(condition)) {                                                                        \
            ++failures;                                                                            \
            fprintf(stderr, "FALHA %s:%d: %s\n", __FILE__, __LINE__, #condition);                  \
        }                                                                                          \
    } while (0)

typedef struct {
    bool created;
    bool attached;
    unsigned char data[MPMC_SHM_SEGMENT_BYTES];
} fake_segment_t;

typedef struct {
    shm_cleanup_error_t errors[2 * MAX_SEGMENTS];
    size_t count;
} error_log_t;

static fake_segment_t segments[MAX_SEGMENTS];
static void *allocations[MAX_SEGMENTS];
static unsigned live_allocations;
static unsigned allocation_calls;
static unsigned free_calls;
static unsigned create_calls;
static unsigned attach_calls;
static unsigned detach_calls;
static unsigned remove_calls;
static unsigned fail_allocation;
static unsigned fail_create;
static unsigned fail_attach;
static int detach_errors[MAX_SEGMENTS];
static int remove_errors[MAX_SEGMENTS];

/* Pontos de injeção usados somente por shm_backend_for_test.o. */
void *mpmc_test_malloc(size_t size);
void *mpmc_test_calloc(size_t count, size_t size);
void mpmc_test_free(void *pointer);
mpmc_result_t mpmc_test_shm_create(size_t size, int *out_shmid);
mpmc_result_t mpmc_test_shm_attach(int shmid, void **out_address);
mpmc_result_t mpmc_test_shm_detach(const void *address);
mpmc_result_t mpmc_test_shm_remove(int shmid);

static void track_allocation(void *pointer) {
    if (pointer == NULL) return;
    for (size_t i = 0; i < MAX_SEGMENTS; ++i) {
        if (allocations[i] == NULL) {
            allocations[i] = pointer;
            ++live_allocations;
            return;
        }
    }
    CHECK(false);
}

void *mpmc_test_malloc(size_t size) {
    if (++allocation_calls == fail_allocation) {
        errno = ENOMEM;
        return NULL;
    }
    void *pointer = malloc(size);
    track_allocation(pointer);
    return pointer;
}

void *mpmc_test_calloc(size_t count, size_t size) {
    if (++allocation_calls == fail_allocation) {
        errno = ENOMEM;
        return NULL;
    }
    void *pointer = calloc(count, size);
    track_allocation(pointer);
    return pointer;
}

void mpmc_test_free(void *pointer) {
    if (pointer == NULL) return;
    ++free_calls;
    for (size_t i = 0; i < MAX_SEGMENTS; ++i) {
        if (allocations[i] == pointer) {
            allocations[i] = NULL;
            --live_allocations;
            free(pointer);
            errno = EIO;
            return;
        }
    }
    CHECK(false); /* Detecta liberação duplicada ou de alocação desconhecida. */
}

mpmc_result_t mpmc_test_shm_create(size_t size, int *out_shmid) {
    CHECK(size == MPMC_SHM_SEGMENT_BYTES);
    CHECK(out_shmid != NULL);
    if (++create_calls == fail_create) {
        return (mpmc_result_t){
            .status = MPMC_ERR_SYS,
            .system_errno = ENOSPC,
        };
    }
    size_t index = create_calls - 1;
    CHECK(index < MAX_SEGMENTS);
    if (index >= MAX_SEGMENTS) return (mpmc_result_t){.status = MPMC_ERR_STATE};
    segments[index].created = true;
    *out_shmid = (int)index; /* O primeiro segmento tem ID zero. */
    return (mpmc_result_t){.status = MPMC_OK};
}

mpmc_result_t mpmc_test_shm_attach(int shmid, void **out_address) {
    CHECK(shmid >= 0 && shmid < MAX_SEGMENTS);
    CHECK(out_address != NULL);
    if (shmid < 0 || shmid >= MAX_SEGMENTS) return (mpmc_result_t){.status = MPMC_ERR_STATE};
    CHECK(segments[shmid].created);
    if (++attach_calls == fail_attach) {
        return (mpmc_result_t){
            .status = MPMC_ERR_SYS,
            .system_errno = EACCES,
        };
    }
    segments[shmid].attached = true;
    *out_address = segments[shmid].data;
    return (mpmc_result_t){.status = MPMC_OK};
}

mpmc_result_t mpmc_test_shm_detach(const void *address) {
    ++detach_calls;
    for (size_t i = 0; i < MAX_SEGMENTS; ++i) {
        if (address == segments[i].data) {
            CHECK(segments[i].created && segments[i].attached);
            if (detach_errors[i] != 0) {
                return (mpmc_result_t){
                    .status = MPMC_ERR_SYS,
                    .system_errno = detach_errors[i],
                };
            }
            segments[i].attached = false;
            return (mpmc_result_t){.status = MPMC_OK};
        }
    }
    CHECK(false);
    return (mpmc_result_t){.status = MPMC_ERR_STATE};
}

mpmc_result_t mpmc_test_shm_remove(int shmid) {
    ++remove_calls;
    CHECK(shmid >= 0 && shmid < MAX_SEGMENTS);
    if (shmid < 0 || shmid >= MAX_SEGMENTS) return (mpmc_result_t){.status = MPMC_ERR_STATE};
    CHECK(segments[shmid].created);
    if (remove_errors[shmid] != 0) {
        return (mpmc_result_t){
            .status = MPMC_ERR_SYS,
            .system_errno = remove_errors[shmid],
        };
    }
    segments[shmid].created = false;
    return (mpmc_result_t){.status = MPMC_OK};
}

static void collect_error(const shm_cleanup_error_t *error, void *user_data) {
    error_log_t *log = user_data;
    CHECK(log->count < sizeof log->errors / sizeof log->errors[0]);
    if (log->count < sizeof log->errors / sizeof log->errors[0]) {
        log->errors[log->count++] = *error;
    }
    errno = ERANGE; /* O diagnóstico não pode substituir a causa já capturada. */
}

static void reset_case(void) {
    CHECK(live_allocations == 0);
    for (size_t i = 0; i < MAX_SEGMENTS; ++i) {
        free(allocations[i]);
    }
    memset(allocations, 0, sizeof allocations);
    memset(segments, 0, sizeof segments);
    memset(detach_errors, 0, sizeof detach_errors);
    memset(remove_errors, 0, sizeof remove_errors);
    live_allocations = allocation_calls = free_calls = 0;
    create_calls = attach_calls = detach_calls = remove_calls = 0;
    fail_allocation = fail_create = fail_attach = 0;
}

static void check_result(mpmc_result_t result, mpmc_status_t status, int system_errno) {
    errno = ERANGE;
    CHECK(result.status == status);
    CHECK(result.system_errno == system_errno);
}

static void check_error(
    const error_log_t *log,
    size_t index,
    shm_cleanup_operation_t operation,
    size_t producer_id,
    int system_errno
) {
    CHECK(index < log->count);
    if (index >= log->count) return;
    const shm_cleanup_error_t *error = &log->errors[index];
    CHECK(error->operation == operation);
    CHECK(error->producer_id == producer_id);
    CHECK(error->shmid == (int)producer_id - 1);
    check_result(error->result, MPMC_ERR_SYS, system_errno);
}

static shm_backend_t *new_backend(void) {
    shm_backend_t *backend = NULL;
    check_result(shm_backend_create(&backend, PRODUCER_COUNT, NULL), MPMC_OK, 0);
    CHECK(backend != NULL);
    return backend;
}

static void test_null_and_invalid_arguments(void) {
    reset_case();
    error_log_t log = {0};
    const shm_cleanup_reporter_t reporter = {.on_error = collect_error, .user_data = &log};
    check_result(shm_backend_destroy(NULL, &reporter), MPMC_OK, 0);
    check_result(shm_backend_worker_close(NULL, &reporter), MPMC_OK, 0);
    check_result(shm_backend_destroy(NULL, NULL), MPMC_OK, 0);
    check_result(shm_backend_worker_close(NULL, NULL), MPMC_OK, 0);
    check_result(shm_backend_create(NULL, PRODUCER_COUNT, &reporter), MPMC_ERR_INVALID_ARG, 0);
    char sentinel;
    shm_backend_t *backend = (shm_backend_t *)(void *)&sentinel;
    check_result(shm_backend_create(&backend, 0, &reporter), MPMC_ERR_INVALID_ARG, 0);
    CHECK(backend == NULL);
    CHECK(allocation_calls == 0 && log.count == 0);
}

static void test_success(void) {
    reset_case();
    error_log_t log = {0};
    const shm_cleanup_reporter_t reporter = {.on_error = collect_error, .user_data = &log};
    shm_backend_t *backend = NULL;
    check_result(shm_backend_create(&backend, PRODUCER_COUNT, &reporter), MPMC_OK, 0);
    CHECK(backend != NULL);
    CHECK(create_calls == PRODUCER_COUNT && attach_calls == PRODUCER_COUNT);
    for (size_t i = 0; i < PRODUCER_COUNT; ++i) {
        CHECK(segments[i].created && segments[i].attached);
    }
    check_result(shm_backend_destroy(backend, &reporter), MPMC_OK, 0);
    CHECK(detach_calls == PRODUCER_COUNT && remove_calls == PRODUCER_COUNT);
    CHECK(allocation_calls == 2 && free_calls == 2 && live_allocations == 0);
    CHECK(log.count == 0);
    for (size_t i = 0; i < PRODUCER_COUNT; ++i) {
        CHECK(!segments[i].created && !segments[i].attached);
    }
}

static void test_allocation_failures(void) {
    for (unsigned position = 1; position <= 2; ++position) {
        reset_case();
        error_log_t log = {0};
        const shm_cleanup_reporter_t reporter = {.on_error = collect_error, .user_data = &log};
        fail_allocation = position;
        char sentinel;
        shm_backend_t *backend = (shm_backend_t *)(void *)&sentinel;
        check_result(shm_backend_create(&backend, PRODUCER_COUNT, &reporter), MPMC_ERR_SYS, ENOMEM);
        CHECK(backend == NULL && live_allocations == 0);
        CHECK(allocation_calls == position && free_calls == position - 1);
        CHECK(create_calls == 0 && detach_calls == 0 && remove_calls == 0);
        CHECK(log.count == 0);
    }
}

static void test_rollback_at_each_position(void) {
    for (unsigned operation = 0; operation < 2; ++operation) {
        for (unsigned position = 1; position <= PRODUCER_COUNT; ++position) {
            reset_case();
            error_log_t log = {0};
            const shm_cleanup_reporter_t reporter = {.on_error = collect_error, .user_data = &log};
            if (operation == 0) {
                fail_create = position;
            } else {
                fail_attach = position;
            }
            shm_backend_t *backend = NULL;
            check_result(
                shm_backend_create(&backend, PRODUCER_COUNT, &reporter),
                MPMC_ERR_SYS,
                operation == 0 ? ENOSPC : EACCES
            );
            CHECK(backend == NULL && live_allocations == 0);
            CHECK(create_calls == position);
            CHECK(attach_calls == position - (operation == 0 ? 1 : 0));
            CHECK(detach_calls == position - 1);
            CHECK(remove_calls == position - (operation == 0 ? 1 : 0));
            CHECK(allocation_calls == 2 && free_calls == 2 && log.count == 0);
            for (size_t i = 0; i < PRODUCER_COUNT; ++i) {
                CHECK(!segments[i].created && !segments[i].attached);
            }
        }
    }
}

static void test_multiple_destroy_errors(void) {
    reset_case();
    shm_backend_t *backend = new_backend();
    error_log_t log = {0};
    const shm_cleanup_reporter_t reporter = {.on_error = collect_error, .user_data = &log};
    detach_errors[0] = EINVAL;
    remove_errors[1] = EPERM;
    detach_errors[2] = EACCES;
    remove_errors[2] = EIDRM;
    check_result(shm_backend_destroy(backend, &reporter), MPMC_ERR_SYS, EINVAL);
    CHECK(detach_calls == PRODUCER_COUNT && remove_calls == PRODUCER_COUNT);
    CHECK(allocation_calls == 2 && free_calls == 2 && live_allocations == 0);
    CHECK(log.count == 4);
    check_error(&log, 0, SHM_CLEANUP_DETACH, 1, EINVAL);
    check_error(&log, 1, SHM_CLEANUP_REMOVE, 2, EPERM);
    check_error(&log, 2, SHM_CLEANUP_DETACH, 3, EACCES);
    check_error(&log, 3, SHM_CLEANUP_REMOVE, 3, EIDRM);
}

static void test_worker_close(void) {
    for (unsigned inject_errors = 0; inject_errors < 2; ++inject_errors) {
        reset_case();
        shm_backend_t *backend = new_backend();
        error_log_t log = {0};
        const shm_cleanup_reporter_t reporter = {.on_error = collect_error, .user_data = &log};
        if (inject_errors != 0) {
            detach_errors[0] = EINVAL;
            detach_errors[1] = EACCES;
            detach_errors[2] = EIO;
        }
        check_result(
            shm_backend_worker_close(backend, &reporter),
            inject_errors != 0 ? MPMC_ERR_SYS : MPMC_OK,
            inject_errors != 0 ? EINVAL : 0
        );
        CHECK(detach_calls == PRODUCER_COUNT && remove_calls == 0);
        CHECK(allocation_calls == 2 && free_calls == 2 && live_allocations == 0);
        CHECK(log.count == (inject_errors != 0 ? PRODUCER_COUNT : 0));
        for (size_t i = 0; i < PRODUCER_COUNT; ++i) {
            CHECK(segments[i].created);
            CHECK(segments[i].attached == (inject_errors != 0));
            if (inject_errors != 0) {
                check_error(&log, i, SHM_CLEANUP_DETACH, i + 1, detach_errors[i]);
            }
        }
    }
}

static void test_rollback_diagnostics_preserve_original_error(void) {
    for (unsigned operation = 0; operation < 2; ++operation) {
        reset_case();
        error_log_t log = {0};
        const shm_cleanup_reporter_t reporter = {.on_error = collect_error, .user_data = &log};
        if (operation == 0) {
            fail_create = PRODUCER_COUNT;
        } else {
            fail_attach = PRODUCER_COUNT;
        }
        detach_errors[0] = EINVAL;
        remove_errors[1] = EPERM;
        remove_errors[2] = EIDRM;
        shm_backend_t *backend = NULL;
        check_result(
            shm_backend_create(&backend, PRODUCER_COUNT, &reporter),
            MPMC_ERR_SYS,
            operation == 0 ? ENOSPC : EACCES
        );
        CHECK(backend == NULL && live_allocations == 0);
        CHECK(detach_calls == PRODUCER_COUNT - 1);
        CHECK(remove_calls == PRODUCER_COUNT - (operation == 0 ? 1 : 0));
        CHECK(log.count == (operation == 0 ? 2u : 3u));
        CHECK(allocation_calls == 2 && free_calls == 2);
        check_error(&log, 0, SHM_CLEANUP_DETACH, 1, EINVAL);
        check_error(&log, 1, SHM_CLEANUP_REMOVE, 2, EPERM);
        if (operation != 0) {
            check_error(&log, 2, SHM_CLEANUP_REMOVE, 3, EIDRM);
        }
    }
}

static void test_optional_reporter(void) {
    for (unsigned use_reporter = 0; use_reporter < 2; ++use_reporter) {
        reset_case();
        shm_backend_t *backend = new_backend();
        error_log_t log = {0};
        const shm_cleanup_reporter_t reporter = {.on_error = NULL, .user_data = &log};
        detach_errors[0] = EINVAL;
        remove_errors[1] = EPERM;
        check_result(
            shm_backend_destroy(backend, use_reporter != 0 ? &reporter : NULL), MPMC_ERR_SYS, EINVAL
        );
        CHECK(detach_calls == PRODUCER_COUNT && remove_calls == PRODUCER_COUNT);
        CHECK(log.count == 0 && live_allocations == 0);
    }
}

int main(void) {
    test_null_and_invalid_arguments();
    test_success();
    test_allocation_failures();
    test_rollback_at_each_position();
    test_multiple_destroy_errors();
    test_worker_close();
    test_rollback_diagnostics_preserve_original_error();
    test_optional_reporter();
    reset_case();
    printf("%u verificações do backend shm, %u falhas\n", checks, failures);
    return failures == 0 ? 0 : 1;
}
