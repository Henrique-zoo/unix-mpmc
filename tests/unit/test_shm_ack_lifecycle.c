/** @file test_shm_ack_lifecycle.c
 * @brief Consumo real, conclusão de lote, interrupção e reutilização de ACK SHM. */
#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/sem.h>
#include <sys/shm.h>
#include <sys/wait.h>
#include <unistd.h>

#include "mpmc/ipc/sem.h"
#include "shm_backend_internal.h"
static pid_t parent;
static int ready[2];
static shm_backend_t *backend;
static shm_backend_t *spectator_backend;
static unsigned checks;
static pid_t children[16];
#define CHECK(condition)                                                                           \
    do {                                                                                           \
        ++checks;                                                                                  \
        if (!(condition)) {                                                                        \
            fprintf(stderr, "FALHA %s:%d: %s\n", __FILE__, __LINE__, #condition);                  \
            exit(EXIT_FAILURE);                                                                    \
        }                                                                                          \
    } while (0)
#define OK(call) CHECK((call).status == MPMC_OK)
static void cleanup(void) {
    if (getpid() == parent) {
        for (size_t index = 0; index < 16; ++index) {
            if (children[index] > 0) {
                (void)kill(children[index], SIGKILL);
                while (waitpid(children[index], nullptr, 0) < 0 && errno == EINTR) {
                }
            }
        }
    }
    if (getpid() == parent && spectator_backend != nullptr) {
        (void)shm_backend_destroy(spectator_backend, nullptr);
        spectator_backend = nullptr;
    }
    if (getpid() == parent && backend != nullptr) {
        (void)shm_backend_destroy(backend, nullptr);
    }
}
/** @brief Registra somente filhos pertencentes a este teste. */
static pid_t start_child(void) {
    pid_t child = fork();
    if (child > 0) {
        for (size_t index = 0; index < 16; ++index) {
            if (children[index] == 0) {
                children[index] = child;
                return child;
            }
        }
        (void)kill(child, SIGKILL);
        (void)waitpid(child, nullptr, 0);
        return -1;
    }
    return child;
}

mpmc_result_t mpmc_test_sem_change(int id, unsigned index, short delta, bool nowait) {
    if (getpid() != parent && index >= SHM_ACK_SEM_NOTIFICATION_BASE && delta == -1 && !nowait) {
        char byte = 1;
        if (write(ready[1], &byte, 1) != 1) _exit(90);
    }
    return sysv_sem_change(id, index, delta, nowait);
}
static message_t message(uint64_t operation, uint32_t sequence) {
    return (message_t){.sender_pid = parent, .operation_id = operation, .sequence = sequence};
}
static void receive_from(size_t index) {
    message_t out;
    OK(shm_backend_receive(backend, index, &out, true));
}
static void collect(pid_t child) {
    int status;
    CHECK(waitpid(child, &status, 0) == child);
    CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 0);
    for (size_t index = 0; index < 16; ++index) {
        if (children[index] == child) children[index] = 0;
    }
}
static void on_alarm(int signal_number) {
    (void)signal_number;
}
/** @brief Confirma lotes reais concorrentes e striped sem misturar identidades. */
static void test_concurrent_producers(void) {
    enum { PRODUCERS = 4, MESSAGES = 120 };
    pid_t producers[PRODUCERS];
    bool seen[PRODUCERS][MESSAGES] = {};
    OK(shm_backend_create(&backend, 3, nullptr));
    for (size_t index = 0; index < PRODUCERS; ++index) {
        producers[index] = start_child();
        CHECK(producers[index] >= 0);
        if (producers[index] == 0) {
            if (shm_backend_ack_begin(backend, 100 + index, MESSAGES).status != MPMC_OK) _exit(10);
            for (uint32_t sequence = 1; sequence <= MESSAGES; ++sequence) {
                message_t item = {
                    .sender_pid = getpid(), .operation_id = 100 + index, .sequence = sequence};
                if (shm_backend_send(backend, 1 + (sequence - 1) % 3, &item).status != MPMC_OK)
                    _exit(11);
            }
            mpmc_result_t result = shm_backend_ack_wait(backend, 100 + index);
            mpmc_result_t ended = shm_backend_ack_end(backend, 100 + index);
            mpmc_result_t closed = shm_backend_worker_close(backend, nullptr);
            _exit(
                result.status == MPMC_OK && ended.status == MPMC_OK && closed.status == MPMC_OK ? 0
                                                                                                : 12
            );
        }
    }
    for (unsigned item_index = 0; item_index < PRODUCERS * MESSAGES; ++item_index) {
        size_t origin = 1 + item_index % 3;
        message_t item;
        OK(shm_backend_receive(backend, origin, &item, true));
        CHECK(item.operation_id >= 100 && item.operation_id < 100 + PRODUCERS);
        size_t producer = (size_t)(item.operation_id - 100);
        CHECK(item.sender_pid == producers[producer]);
        CHECK(item.sequence >= 1 && item.sequence <= MESSAGES);
        CHECK(!seen[producer][item.sequence - 1]);
        CHECK(origin == 1 + (item.sequence - 1) % 3);
        seen[producer][item.sequence - 1] = true;
    }
    for (size_t index = 0; index < PRODUCERS; ++index) collect(producers[index]);
    for (size_t index = 0; index < 3; ++index) {
        uint64_t consumed;
        OK(shm_backend_consumed_count(backend, index + 1, &consumed));
        CHECK(consumed == PRODUCERS * MESSAGES / 3);
    }
    OK(shm_backend_destroy(backend, nullptr));
    backend = nullptr;
}

/** @brief Encerra um produtor bloqueado em ACK e preserva outro backend. */
static void test_shutdown_waiter(void) {
    OK(shm_backend_create(&spectator_backend, 1, nullptr));
    OK(shm_backend_create(&backend, 1, nullptr));
    int old_control = backend->ack_control_shmid;
    int old_semaphore = backend->ack_semid;
    int channel[2];
    CHECK(pipe(channel) == 0);
    pid_t child = start_child();
    CHECK(child >= 0);
    if (child == 0) {
        ready[1] = channel[1];
        if (shm_backend_ack_begin(backend, 500, 1).status != MPMC_OK) _exit(20);
        message_t item = {.sender_pid = getpid(), .operation_id = 500, .sequence = 1};
        if (shm_backend_send(backend, 1, &item).status != MPMC_OK) _exit(21);
        (void)shm_backend_ack_wait(backend, 500);
        _exit(22);
    }
    char byte;
    CHECK(read(channel[0], &byte, 1) == 1);
    CHECK(kill(child, SIGTERM) == 0);
    int status;
    CHECK(waitpid(child, &status, 0) == child);
    CHECK(WIFSIGNALED(status) && WTERMSIG(status) == SIGTERM);
    for (size_t index = 0; index < 16; ++index)
        if (children[index] == child) children[index] = 0;
    OK(shm_backend_destroy(backend, nullptr));
    backend = nullptr;
    struct shmid_ds info;
    CHECK(shmctl(old_control, IPC_STAT, &info) == -1);
    CHECK(semctl(old_semaphore, 0, GETVAL) == -1);
    message_t item = message(999, 1), out;
    OK(shm_backend_send(spectator_backend, 1, &item));
    OK(shm_backend_receive(spectator_backend, 1, &out, true));
    OK(shm_backend_destroy(spectator_backend, nullptr));
    spectator_backend = nullptr;
    close(channel[0]);
    close(channel[1]);
}

int main(void) {
    parent = getpid();
    struct sigaction action = {.sa_handler = on_alarm};
    sigemptyset(&action.sa_mask);
    CHECK(sigaction(SIGALRM, &action, nullptr) == 0);
    sigset_t mask;
    sigemptyset(&mask);
    sigaddset(&mask, SIGALRM);
    CHECK(sigprocmask(SIG_UNBLOCK, &mask, nullptr) == 0);
    alarm(20);
    CHECK(atexit(cleanup) == 0);
    OK(shm_backend_create(&backend, 2, nullptr));
    OK(shm_backend_ack_begin(backend, 10, 2));
    for (uint32_t sequence = 1; sequence <= 2; ++sequence) {
        message_t item = message(10, sequence);
        OK(shm_backend_send(backend, sequence, &item));
        receive_from(sequence);
    }
    CHECK(backend->ack_control->records[0].remaining == 0);
    CHECK(semctl(backend->ack_semid, SHM_ACK_SEM_NOTIFICATION_BASE, GETVAL) == 1);
    OK(shm_backend_ack_wait(backend, 10));
    OK(shm_backend_ack_end(backend, 10));
    OK(shm_backend_ack_begin(backend, 20, 2));
    CHECK(semctl(backend->ack_semid, SHM_ACK_SEM_NOTIFICATION_BASE, GETVAL) == 0);
    CHECK(pipe(ready) == 0);
    pid_t child = start_child();
    CHECK(child >= 0);
    if (child == 0) _exit(shm_backend_ack_wait(backend, 20).status == MPMC_OK ? 0 : 91);
    char byte;
    CHECK(read(ready[0], &byte, 1) == 1);
    CHECK(shm_backend_ack_end(backend, 20).status == MPMC_ERR_STATE);
    message_t item = message(20, 1);
    OK(shm_backend_send(backend, 1, &item));
    receive_from(1);
    CHECK(backend->ack_control->records[0].remaining == 1);
    CHECK(waitpid(child, nullptr, WNOHANG) == 0);
    item = message(20, 2);
    OK(shm_backend_send(backend, 2, &item));
    receive_from(2);
    collect(child);
    CHECK(!backend->ack_control->records[0].waiting);
    OK(shm_backend_ack_end(backend, 20));
    OK(shm_backend_ack_begin(backend, 30, 1));
    child = start_child();
    CHECK(child >= 0);
    if (child == 0) {
        alarm(1);
        mpmc_result_t result = shm_backend_ack_wait(backend, 30);
        _exit(result.status == MPMC_ERR_INTERRUPTED && result.system_errno == EINTR ? 0 : 94);
    }
    CHECK(read(ready[0], &byte, 1) == 1);
    collect(child);
    CHECK(!backend->ack_control->records[0].waiting);
    OK(shm_backend_ack_end(backend, 30));
    item = message(999, 1);
    OK(shm_backend_send(backend, 1, &item));
    receive_from(1);
    uint64_t total;
    OK(shm_backend_consumed_count(backend, 1, &total));
    CHECK(total == 3);
    CHECK(sizeof(shm_ack_record_t) == 16);
    CHECK(sizeof(shm_ack_control_t) == 1040);
    CHECK(shm_backend_destroy(backend, nullptr).status == MPMC_OK);
    backend = nullptr;
    test_concurrent_producers();
    test_shutdown_waiter();
    close(ready[0]);
    close(ready[1]);
    alarm(0);
    printf("%u verificacoes basicas de integracao, 0 falhas\n", checks);
}
