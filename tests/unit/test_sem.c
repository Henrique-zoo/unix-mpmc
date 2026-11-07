
/**
 * @file test_sem.c
 * @brief Testa semáforos reais e injeta falhas de semop() no objeto de teste.
 */
#include <errno.h>
#include <limits.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/sem.h>
#include <unistd.h>

#include "mpmc/ipc/sem.h"

static unsigned checks;
static unsigned failures;
static int owned_semid = -1;
static int injected_semop_errno;

#define CHECK(condition)                                                                           \
    do {                                                                                           \
        ++checks;                                                                                  \
        if (!(condition)) {                                                                        \
            ++failures;                                                                            \
            fprintf(stderr, "FALHA %s:%d: %s\n", __FILE__, __LINE__, #condition);                  \
        }                                                                                          \
    } while (0)

#define CHECK_RESULT(call, expected_status, expected_errno)                                        \
    do {                                                                                           \
        mpmc_result_t result_ = (call);                                                            \
        CHECK(result_.status == (expected_status));                                                \
        CHECK(result_.system_errno == (expected_errno));                                           \
    } while (0)

/** @brief Substitui semop() somente no objeto de teste de sem.c. */
int mpmc_test_semop(int semid, struct sembuf *operations, size_t count) {
    if (injected_semop_errno != 0) {
        errno = injected_semop_errno;
        return -1;
    }
    return semop(semid, operations, count);
}

/** @brief Remove exclusivamente o conjunto criado por este teste. */
static void cleanup(void) {
    if (owned_semid >= 0) (void)semctl(owned_semid, 0, IPC_RMID);
}

static void on_alarm(int signal_number) {
    (void)signal_number;
}

static void test_arguments(void) {
    int sentinel = -123;

    CHECK_RESULT(sysv_sem_create(0, &sentinel), MPMC_ERR_INVALID_ARG, 0);
    CHECK(sentinel == -123);

    CHECK_RESULT(sysv_sem_create(UINT_MAX, &sentinel), MPMC_ERR_INVALID_ARG, 0);
    CHECK(sentinel == -123);

    CHECK_RESULT(sysv_sem_create(1, NULL), MPMC_ERR_INVALID_ARG, 0);
    CHECK_RESULT(sysv_sem_set(-1, 0, 0), MPMC_ERR_INVALID_ARG, 0);
    CHECK_RESULT(sysv_sem_change(-1, 0, -1, true), MPMC_ERR_INVALID_ARG, 0);
    CHECK_RESULT(sysv_sem_remove(-1), MPMC_ERR_INVALID_ARG, 0);
}

static void test_values_and_indices(void) {
    CHECK_RESULT(sysv_sem_set(owned_semid, 0, 1), MPMC_OK, 0);
    CHECK_RESULT(sysv_sem_set(owned_semid, 1, 0), MPMC_OK, 0);

    CHECK(semctl(owned_semid, 0, GETVAL) == 1);
    CHECK(semctl(owned_semid, 1, GETVAL) == 0);

    CHECK_RESULT(sysv_sem_set(owned_semid, 2, 1), MPMC_ERR_INVALID_ARG, EINVAL);
    CHECK_RESULT(sysv_sem_set(owned_semid, UINT_MAX, 1), MPMC_ERR_INVALID_INDEX, 0);
    CHECK_RESULT(sysv_sem_set(owned_semid, 0, UINT_MAX), MPMC_ERR_INVALID_ARG, 0);
    CHECK_RESULT(sysv_sem_set(owned_semid, 0, INT_MAX), MPMC_ERR_INVALID_ARG, ERANGE);

    CHECK_RESULT(sysv_sem_change(owned_semid, 2, -1, true), MPMC_ERR_INVALID_INDEX, EFBIG);
    CHECK_RESULT(sysv_sem_change(owned_semid, UINT_MAX, -1, true), MPMC_ERR_INVALID_INDEX, 0);

    errno = EACCES;
    CHECK_RESULT(sysv_sem_change(owned_semid, 0, -1, true), MPMC_OK, 0);

    CHECK_RESULT(sysv_sem_change(owned_semid, 0, -1, true), MPMC_ERR_WOULD_BLOCK, EAGAIN);
    CHECK_RESULT(sysv_sem_change(owned_semid, 1, 0, true), MPMC_OK, 0);
    CHECK_RESULT(sysv_sem_change(owned_semid, 1, 1, true), MPMC_OK, 0);
    CHECK_RESULT(sysv_sem_change(owned_semid, 1, 0, true), MPMC_ERR_WOULD_BLOCK, EAGAIN);

    CHECK(semctl(owned_semid, 0, GETVAL) == 0);
    CHECK(semctl(owned_semid, 1, GETVAL) == 1);
}

/** @brief P/V e SETVAL exigem alteração, sem leitura adicional por IPC_STAT. */
static void test_alter_only_permission(void) {
    struct semid_ds info;

    union {
        int val;
        struct semid_ds *buf;
        unsigned short *array;
    } argument = {.buf = &info};

    int rc = semctl(owned_semid, 0, IPC_STAT, argument);
    CHECK(rc == 0);
    if (rc == -1) return;

    info.sem_perm.mode = 0200;
    rc = semctl(owned_semid, 0, IPC_SET, argument);
    CHECK(rc == 0);
    if (rc == -1) return;

    CHECK_RESULT(sysv_sem_set(owned_semid, 0, 1), MPMC_OK, 0);
    CHECK_RESULT(sysv_sem_change(owned_semid, 0, -1, true), MPMC_OK, 0);
    CHECK_RESULT(sysv_sem_change(owned_semid, 0, 1, true), MPMC_OK, 0);

    info.sem_perm.mode = 0600;
    CHECK(semctl(owned_semid, 0, IPC_SET, argument) == 0);
}

/** @brief Exerce EINTR real, sem repetição automática pelo wrapper. */
static void test_interruption(void) {
    struct sigaction action = {.sa_handler = on_alarm};
    struct sigaction previous;

    sigemptyset(&action.sa_mask);

    int rc = sigaction(SIGALRM, &action, &previous);
    CHECK(rc == 0);
    if (rc == -1) return;

    CHECK_RESULT(sysv_sem_set(owned_semid, 0, 0), MPMC_OK, 0);

    alarm(1);
    mpmc_result_t result = sysv_sem_change(owned_semid, 0, -1, false);
    alarm(0);

    CHECK(result.status == MPMC_ERR_INTERRUPTED);
    CHECK(result.system_errno == EINTR);
    CHECK(sigaction(SIGALRM, &previous, NULL) == 0);
}

/** @brief Verifica erros sem depender da temporização entre processos. */
static void test_injected_errors(void) {
    const struct {
        int error;
        mpmc_status_t status;
    } cases[] = {
        {EIDRM, MPMC_ERR_SHUTDOWN},
        {EINVAL, MPMC_ERR_INVALID_ARG},
        {EACCES, MPMC_ERR_SYS},
    };

    for (size_t i = 0; i < sizeof cases / sizeof cases[0]; ++i) {
        injected_semop_errno = cases[i].error;
        mpmc_result_t result = sysv_sem_change(owned_semid, 0, -1, true);
        injected_semop_errno = 0;

        errno = 0;
        CHECK(result.status == cases[i].status);
        CHECK(result.system_errno == cases[i].error);
    }
}

int main(void) {
    if (atexit(cleanup) != 0) return EXIT_FAILURE;

    test_arguments();

    mpmc_result_t result = sysv_sem_create(2, &owned_semid);
    CHECK(result.status == MPMC_OK);
    CHECK(result.system_errno == 0);
    if (result.status != MPMC_OK) return EXIT_FAILURE;

    test_values_and_indices();
    test_alter_only_permission();
    test_interruption();
    test_injected_errors();

    int removed = owned_semid;
    result = sysv_sem_remove(owned_semid);
    CHECK(result.status == MPMC_OK);
    CHECK(result.system_errno == 0);
    if (result.status == MPMC_OK) owned_semid = -1;

    CHECK_RESULT(sysv_sem_remove(removed), MPMC_ERR_INVALID_ARG, EINVAL);

    printf("%u verificações de semáforos, %u falhas\n", checks, failures);
    return failures == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
