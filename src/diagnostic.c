/** @file diagnostic.c
 * @brief Escopos e observador local; nenhuma apresentação pertence às bibliotecas.
 * @ingroup core
 */
#include "mpmc/diagnostic.h"

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

static thread_local mpmc_reporter_t reporter;
static thread_local mpmc_diagnostic_scope_t *active;
static thread_local uint64_t groups;
static thread_local uint64_t reports;
static thread_local uint64_t worker_operation;
static thread_local size_t worker_producer;
static thread_local bool delivering;

static const char *system_operation(const char *function);

void mpmc_diagnostic_set_reporter(const mpmc_reporter_t *destination) {
    reporter = destination != nullptr ? *destination : (mpmc_reporter_t){};
}

void mpmc_diagnostic_after_fork(uint64_t operation_id, size_t producer_id) {
    active = nullptr;
    groups = reports = 0;
    delivering = false;
    worker_operation = operation_id;
    worker_producer = producer_id;
}

void mpmc_diagnostic_begin(
    mpmc_diagnostic_scope_t *scope,
    const char *function,
    bool cleanup,
    bool primitive
) {
    *scope = (mpmc_diagnostic_scope_t){
        .function = function,
        .parent = active,
        .first = {.status = MPMC_OK},
        .group = active != nullptr ? active->group : ++groups,
        .cleanup = cleanup || (active != nullptr && active->cleanup),
        .primitive = primitive,
        .fatal = active != nullptr && active->fatal,
        .resource_id = -1,
    };
    active = scope;
}

mpmc_result_t mpmc_diagnostic_end(mpmc_diagnostic_scope_t *scope, mpmc_result_t result) {
    if (scope->first.status == MPMC_OK && result.status != MPMC_OK
        && result.status != MPMC_ERR_WOULD_BLOCK
        && !(scope->primitive && result.status == MPMC_ERR_INTERRUPTED)) {
        mpmc_diagnostic_report(system_operation(scope->function), result);
    }
    mpmc_result_t outcome =
        scope->cleanup && scope->first.status != MPMC_OK ? scope->first : result;
    active = scope->parent;
    return outcome;
}

void mpmc_diagnostic_deliver(const mpmc_diagnostic_t *error) {
    int saved_errno = errno;
    ++reports;
    if (reporter.on_error != nullptr && !delivering) {
        delivering = true;
        reporter.on_error(error, reporter.user_data);
        delivering = false;
    }
    errno = saved_errno;
}

void mpmc_diagnostic_report(const char *operation, mpmc_result_t result) {
    if (result.status == MPMC_OK || result.status == MPMC_ERR_WOULD_BLOCK) return;
    int saved_errno = errno;
    mpmc_diagnostic_scope_t *root = active;
    for (mpmc_diagnostic_scope_t *scope = active; scope != nullptr; scope = scope->parent) {
        if (scope->first.status == MPMC_OK) scope->first = result;
        root = scope;
    }
    mpmc_diagnostic_t error = {
        .result = result,
        .phase = active != nullptr && active->cleanup ? MPMC_DIAGNOSTIC_CLEANUP
                                                      : MPMC_DIAGNOSTIC_OPERATION,
        .process_pid = getpid(),
        .operation_id = worker_operation,
        .group = root != nullptr ? root->group : ++groups,
        .producer_id = worker_producer,
        .resource_id = active != nullptr ? active->resource_id : -1,
        .fatal = active != nullptr && active->fatal,
    };
    (void)snprintf(
        error.function, sizeof error.function, "%s", root != nullptr ? root->function : operation
    );
    (void)snprintf(error.operation, sizeof error.operation, "%s", operation);
    mpmc_diagnostic_deliver(&error);
    errno = saved_errno;
}

void mpmc_diagnostic_cause(const char *operation, mpmc_result_t result) {
    if (active == nullptr || active->first.status == MPMC_OK) {
        mpmc_diagnostic_report(operation, result);
    }
}

bool mpmc_diagnostic_enabled(void) {
    return reporter.on_error != nullptr;
}

void mpmc_diagnostic_observe(const char *operation, mpmc_result_t result, uint64_t before) {
    if (reports == before) mpmc_diagnostic_report(operation, result);
}

uint64_t mpmc_diagnostic_count(void) {
    return reports;
}

/** @brief Nomes de syscalls para os wrappers; os demais nomes já identificam a operação. */
static const char *system_operation(const char *function) {
    static const struct {
        const char *function;
        const char *call;
    } names[] = {
        {"sysv_sem_create", "semget()"},
        {"sysv_sem_set", "semctl(SETVAL)"},
        {"sysv_sem_apply", "semop()"},
        {"sysv_sem_remove", "semctl(IPC_RMID)"},
        {"sysv_shm_create", "shmget()"},
        {"sysv_shm_attach", "shmat()"},
        {"sysv_shm_detach", "shmdt()"},
        {"sysv_shm_remove", "shmctl(IPC_RMID)"},
        {"sysv_shm_stat", "shmctl(IPC_STAT)"},
        {"sysv_msg_create", "msgget()"},
        {"sysv_msg_send", "msgsnd()"},
        {"sysv_msg_receive_typed", "msgrcv()"},
        {"sysv_msg_remove", "msgctl(IPC_RMID)"},
    };
    for (size_t index = 0; index < sizeof names / sizeof names[0]; ++index) {
        if (strcmp(function, names[index].function) == 0) return names[index].call;
    }
    return function;
}
