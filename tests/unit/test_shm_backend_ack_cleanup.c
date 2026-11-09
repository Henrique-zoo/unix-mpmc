/**
 * @file test_shm_backend_ack_cleanup.c
 * @brief Testes isolados de falhas e novas tentativas de cleanup dos recursos ACK.
 */

#include <stdbool.h>
#include <stdio.h>
#include <string.h>

#include "mpmc/error.h"
#include "mpmc/ipc/sem.h"
#include "mpmc/ipc/shm.h"
#include "shm_backend_internal.h"

#define CHECK(condition)                                                                           \
    do {                                                                                           \
        ++checks;                                                                                  \
        if (!(condition)) {                                                                        \
            ++failures;                                                                            \
            fprintf(stderr, "FALHA %s:%d: %s\n", __FILE__, __LINE__, #condition);                  \
        }                                                                                          \
    } while (0)

static unsigned checks;
static unsigned failures;

static mpmc_result_t detach_results[1];
static mpmc_result_t shm_remove_results[1];
static mpmc_result_t sem_remove_result;

static unsigned detach_calls;
static unsigned shm_remove_calls;
static unsigned sem_remove_calls;

mpmc_result_t mpmc_test_shm_detach(const void *address) {
    CHECK(address != NULL);
    unsigned index = detach_calls++;
    if (index >= 1) {
        return (mpmc_result_t){.status = MPMC_ERR_STATE};
    }
    return detach_results[index];
}

mpmc_result_t mpmc_test_shm_remove(int shmid) {
    CHECK(shmid >= 0);
    unsigned index = shm_remove_calls++;
    if (index >= 1) {
        return (mpmc_result_t){.status = MPMC_ERR_STATE};
    }
    return shm_remove_results[index];
}

mpmc_result_t mpmc_test_sem_remove(int semid) {
    CHECK(semid >= 0);
    ++sem_remove_calls;
    return sem_remove_result;
}

static void reset_backend(shm_backend_t *backend) {
    memset(backend, 0, sizeof(*backend));

    backend->ack_control_shmid = 10;
    backend->ack_control = (shm_ack_control_t *)0x1000;
    backend->ack_semid = 20;

    memset(detach_results, 0, sizeof(detach_results));
    memset(shm_remove_results, 0, sizeof(shm_remove_results));
    sem_remove_result = (mpmc_result_t){.status = MPMC_OK};

    detach_calls = 0;
    shm_remove_calls = 0;
    sem_remove_calls = 0;
}

static void test_failed_detach_preserves_mapping(void) {
    shm_backend_t backend;
    reset_backend(&backend);

    detach_results[0] = (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = 5};

    mpmc_result_t result = shm_backend_ack_resources_cleanup(&backend, false);

    CHECK(result.status == MPMC_ERR_SYS);
    CHECK(result.system_errno == 5);
    CHECK(backend.ack_control != NULL);
    CHECK(detach_calls == 1);

    detach_results[0] = (mpmc_result_t){.status = MPMC_OK};

    detach_calls = 0;
    result = shm_backend_ack_resources_cleanup(&backend, false);

    CHECK(result.status == MPMC_OK);
    CHECK(backend.ack_control == NULL);
}

static void test_failed_global_removals_preserve_ids(void) {
    shm_backend_t backend;
    reset_backend(&backend);

    sem_remove_result = (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = 13};
    shm_remove_results[0] = (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = 1};

    mpmc_result_t result = shm_backend_ack_resources_cleanup(&backend, true);

    CHECK(result.status == MPMC_ERR_SYS);
    CHECK(result.system_errno == 13);
    CHECK(sem_remove_calls == 1);
    CHECK(shm_remove_calls == 1);
    CHECK(backend.ack_semid == 20);
    CHECK(backend.ack_control_shmid == 10);

    sem_remove_result = (mpmc_result_t){.status = MPMC_OK};
    memset(shm_remove_results, 0, sizeof(shm_remove_results));
    shm_remove_calls = 0;

    result = shm_backend_ack_resources_cleanup(&backend, true);

    CHECK(result.status == MPMC_OK);
    CHECK(backend.ack_semid == -1);
    CHECK(backend.ack_control_shmid == -1);
}

static void test_cleanup_preserves_first_error_and_continues(void) {
    shm_backend_t backend;
    reset_backend(&backend);

    detach_results[0] = (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = 5};
    sem_remove_result = (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = 13};
    shm_remove_results[0] = (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = 1};

    mpmc_result_t result = shm_backend_ack_resources_cleanup(&backend, true);

    CHECK(result.status == MPMC_ERR_SYS);
    CHECK(result.system_errno == 5);
    CHECK(detach_calls == 1);
    CHECK(sem_remove_calls == 1);
    CHECK(shm_remove_calls == 1);
    CHECK(backend.ack_control != NULL);
    CHECK(backend.ack_semid == 20);
    CHECK(backend.ack_control_shmid == 10);
}

int main(void) {
    test_failed_detach_preserves_mapping();
    test_failed_global_removals_preserve_ids();
    test_cleanup_preserves_first_error_and_continues();

    printf("%u verificações de cleanup ACK, %u falhas\n", checks, failures);
    return failures == 0 ? 0 : 1;
}
