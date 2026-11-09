/**
 * @file test_shm_backend_ack_create.c
 * @brief Testes de criação e rollback dos recursos compartilhados ACK.
 */

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "mpmc/ipc/shm.h"
#include "mpmc/ipc/sem.h"
#include "shm_backend_internal.h"

#define CHECK(condition) do { \
    ++checks; \
    if (!(condition)) { \
        ++failures; \
        fprintf(stderr, "FALHA %s:%d: %s\n", __FILE__, __LINE__, #condition); \
    } \
} while (0)

#define SHM_COUNT (SHM_ACK_BITMAP_SEGMENT_COUNT + 1U)

static unsigned checks;
static unsigned failures;

static unsigned create_calls;
static unsigned attach_calls;
static unsigned detach_calls;
static unsigned shm_remove_calls;
static unsigned sem_create_calls;
static unsigned sem_set_calls;
static unsigned sem_remove_calls;

static unsigned fail_create;
static unsigned fail_attach;
static unsigned fail_sem_create;
static unsigned fail_sem_set;

static bool shm_created[SHM_COUNT];
static bool shm_attached[SHM_COUNT];
static unsigned short sem_values[SHM_ACK_SEM_COUNT];
static bool sem_created;

static unsigned char shm_memory[SHM_COUNT][MPMC_SHM_SEGMENT_BYTES];

static mpmc_result_t simulated_error(mpmc_status_t status, int error) {
    return (mpmc_result_t){.status = status, .system_errno = error};
}

static void reset_case(void) {
    create_calls = attach_calls = detach_calls = 0;
    shm_remove_calls = sem_create_calls = sem_set_calls = 0;
    sem_remove_calls = 0;

    fail_create = fail_attach = fail_sem_create = fail_sem_set = 0;

    memset(shm_created, 0, sizeof(shm_created));
    memset(shm_attached, 0, sizeof(shm_attached));
    memset(sem_values, 0, sizeof(sem_values));
    memset(shm_memory, 0, sizeof(shm_memory));

    sem_created = false;
}

static void init_backend(shm_backend_t *backend) {
    memset(backend, 0, sizeof(*backend));

    backend->ack_control_shmid = -1;
    backend->ack_semid = -1;

    for (size_t index = 0; index < SHM_ACK_BITMAP_SEGMENT_COUNT; ++index) {
        backend->ack_bitmap_shmids[index] = -1;
    }
}

mpmc_result_t mpmc_test_shm_create(size_t size, int *out_shmid) {
    unsigned position = ++create_calls;

    CHECK(size == MPMC_SHM_SEGMENT_BYTES);
    CHECK(out_shmid != NULL);

    if (position == fail_create) {
        return simulated_error(MPMC_ERR_SYS, 28);
    }

    unsigned index = position - 1U;
    CHECK(index < SHM_COUNT);

    if (index >= SHM_COUNT) {
        return simulated_error(MPMC_ERR_STATE, 0);
    }

    shm_created[index] = true;
    *out_shmid = 100 + (int)index;

    return (mpmc_result_t){.status = MPMC_OK};
}

mpmc_result_t mpmc_test_shm_attach(int shmid, void **out_address) {
    unsigned position = ++attach_calls;

    if (position == fail_attach) {
        return simulated_error(MPMC_ERR_SYS, 13);
    }

    int index = shmid - 100;
    CHECK(index >= 0 && index < (int)SHM_COUNT);
    CHECK(out_address != NULL);

    if (index < 0 || index >= (int)SHM_COUNT || out_address == NULL) {
        return simulated_error(MPMC_ERR_STATE, 0);
    }

    CHECK(shm_created[index]);
    shm_attached[index] = true;
    *out_address = shm_memory[index];

    return (mpmc_result_t){.status = MPMC_OK};
}

mpmc_result_t mpmc_test_shm_detach(const void *address) {
    ++detach_calls;

    for (size_t index = 0; index < SHM_COUNT; ++index) {
        if (address == shm_memory[index] && shm_attached[index]) {
            shm_attached[index] = false;
            return (mpmc_result_t){.status = MPMC_OK};
        }
    }

    CHECK(false);
    return simulated_error(MPMC_ERR_STATE, 0);
}

mpmc_result_t mpmc_test_shm_remove(int shmid) {
    ++shm_remove_calls;

    int index = shmid - 100;
    CHECK(index >= 0 && index < (int)SHM_COUNT);

    if (index < 0 || index >= (int)SHM_COUNT) {
        return simulated_error(MPMC_ERR_STATE, 0);
    }

    CHECK(shm_created[index]);
    CHECK(!shm_attached[index]);

    shm_created[index] = false;
    return (mpmc_result_t){.status = MPMC_OK};
}

mpmc_result_t mpmc_test_sem_create(unsigned count, int *out_semid) {
    ++sem_create_calls;

    CHECK(count == SHM_ACK_SEM_COUNT);
    CHECK(out_semid != NULL);

    if (sem_create_calls == fail_sem_create) {
        return simulated_error(MPMC_ERR_SYS, 28);
    }

    sem_created = true;
    *out_semid = 700;

    return (mpmc_result_t){.status = MPMC_OK};
}

mpmc_result_t mpmc_test_sem_set(int semid, unsigned index, unsigned value) {
    unsigned position = ++sem_set_calls;

    CHECK(semid == 700);
    CHECK(index < SHM_ACK_SEM_COUNT);

    if (position == fail_sem_set) {
        return simulated_error(MPMC_ERR_INVALID_ARG, 34);
    }

    if (index >= SHM_ACK_SEM_COUNT) {
        return simulated_error(MPMC_ERR_STATE, 0);
    }

    sem_values[index] = (unsigned short)value;
    return (mpmc_result_t){.status = MPMC_OK};
}

mpmc_result_t mpmc_test_sem_remove(int semid) {
    ++sem_remove_calls;

    CHECK(semid == 700);
    CHECK(sem_created);

    sem_created = false;
    return (mpmc_result_t){.status = MPMC_OK};
}

mpmc_result_t mpmc_test_sem_change(int semid, unsigned index, short delta, bool nowait) {
    (void)semid;
    (void)index;
    (void)delta;
    (void)nowait;
    return simulated_error(MPMC_ERR_STATE, 0);
}

mpmc_result_t mpmc_test_sem_apply(int semid, struct sembuf *operations, size_t count) {
    (void)semid;
    (void)operations;
    (void)count;
    return simulated_error(MPMC_ERR_STATE, 0);
}

static void check_all_resources_removed(void) {
    for (size_t index = 0; index < SHM_COUNT; ++index) {
        CHECK(!shm_created[index]);
        CHECK(!shm_attached[index]);
    }

    CHECK(!sem_created);
}

static void test_shm_create_failures(void) {
    for (unsigned position = 1; position <= SHM_COUNT; ++position) {
        reset_case();

        shm_backend_t backend;
        init_backend(&backend);
        fail_create = position;

        mpmc_result_t result = shm_backend_ack_resources_create(&backend);

        CHECK(result.status == MPMC_ERR_SYS);
        CHECK(result.system_errno == 28);
        CHECK(create_calls == position);
        CHECK(attach_calls == position - 1U);

        result = shm_backend_ack_resources_cleanup(&backend, true);
        CHECK(result.status == MPMC_OK);

        CHECK(detach_calls == position - 1U);
        CHECK(shm_remove_calls == position - 1U);
        CHECK(sem_remove_calls == 0);

        check_all_resources_removed();
    }
}

static void test_shm_attach_failures(void) {
    for (unsigned position = 1; position <= SHM_COUNT; ++position) {
        reset_case();

        shm_backend_t backend;
        init_backend(&backend);
        fail_attach = position;

        mpmc_result_t result = shm_backend_ack_resources_create(&backend);

        CHECK(result.status == MPMC_ERR_SYS);
        CHECK(result.system_errno == 13);
        CHECK(create_calls == position);
        CHECK(attach_calls == position);

        result = shm_backend_ack_resources_cleanup(&backend, true);
        CHECK(result.status == MPMC_OK);

        CHECK(detach_calls == position - 1U);
        CHECK(shm_remove_calls == position);
        CHECK(sem_remove_calls == 0);

        check_all_resources_removed();
    }
}

static void test_sem_create_failure(void) {
    reset_case();

    shm_backend_t backend;
    init_backend(&backend);
    fail_sem_create = 1;

    mpmc_result_t result = shm_backend_ack_resources_create(&backend);

    CHECK(result.status == MPMC_ERR_SYS);
    CHECK(result.system_errno == 28);
    CHECK(create_calls == SHM_COUNT);
    CHECK(attach_calls == SHM_COUNT);
    CHECK(sem_create_calls == 1);

    result = shm_backend_ack_resources_cleanup(&backend, true);
    CHECK(result.status == MPMC_OK);

    CHECK(detach_calls == SHM_COUNT);
    CHECK(shm_remove_calls == SHM_COUNT);
    CHECK(sem_remove_calls == 0);

    check_all_resources_removed();
}

static void test_sem_set_failures(void) {
    for (unsigned position = 1; position <= SHM_ACK_SEM_COUNT; ++position) {
        reset_case();

        shm_backend_t backend;
        init_backend(&backend);
        fail_sem_set = position;

        mpmc_result_t result = shm_backend_ack_resources_create(&backend);

        CHECK(result.status == MPMC_ERR_INVALID_ARG);
        CHECK(result.system_errno == 34);
        CHECK(sem_set_calls == position);

        result = shm_backend_ack_resources_cleanup(&backend, true);
        CHECK(result.status == MPMC_OK);

        CHECK(detach_calls == SHM_COUNT);
        CHECK(shm_remove_calls == SHM_COUNT);
        CHECK(sem_remove_calls == 1);

        check_all_resources_removed();
    }
}

static void test_successful_creation(void) {
    reset_case();

    shm_backend_t backend;
    init_backend(&backend);

    mpmc_result_t result = shm_backend_ack_resources_create(&backend);

    CHECK(result.status == MPMC_OK);
    CHECK(create_calls == SHM_COUNT);
    CHECK(attach_calls == SHM_COUNT);
    CHECK(sem_create_calls == 1);
    CHECK(sem_set_calls == SHM_ACK_SEM_COUNT);

    CHECK(backend.ack_control_shmid == 100);
    CHECK(backend.ack_control != NULL);
    CHECK(backend.ack_control->magic == UINT32_C(0x53484D41));
    CHECK(backend.ack_control->version == 2U);
    CHECK(backend.ack_semid == 700);

    CHECK(sem_values[SHM_ACK_SEM_MUTEX] == 1U);

    for (unsigned index = 0; index < SHM_ACK_OPERATION_CAPACITY; ++index) {
        CHECK(sem_values[SHM_ACK_SEM_NOTIFICATION_BASE + index] == 0U);
    }

    for (size_t index = 0; index < SHM_ACK_BITMAP_SEGMENT_COUNT; ++index) {
        CHECK(backend.ack_bitmap_segments[index] != NULL);
        for (size_t byte = 0; byte < MPMC_SHM_SEGMENT_BYTES; ++byte) {
            CHECK(((unsigned char *)backend.ack_bitmap_segments[index])[byte] == 0);
        }
    }

    result = shm_backend_ack_resources_cleanup(&backend, true);
    CHECK(result.status == MPMC_OK);
    CHECK(detach_calls == SHM_COUNT);
    CHECK(shm_remove_calls == SHM_COUNT);
    CHECK(sem_remove_calls == 1);

    check_all_resources_removed();
}

int main(void) {
    test_shm_create_failures();
    test_shm_attach_failures();
    test_sem_create_failure();
    test_sem_set_failures();
    test_successful_creation();

    printf("%u verificações de criação ACK, %u falhas\n", checks, failures);
    return failures == 0 ? 0 : 1;
}
