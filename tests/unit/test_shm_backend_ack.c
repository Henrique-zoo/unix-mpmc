/**
 * @file test_shm_backend_ack.c
 * @brief Testes de registro, correlação e espera de ACK do backend SHM.
 * @ingroup tests
 */

#include <errno.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "mpmc/diagnostic.h"
#include "mpmc/ipc/sem.h"
#include "mpmc/ipc/shm_backend.h"
#include "shm_backend_internal.h"

#define CHILD_CAPACITY 8

typedef struct {
    size_t producer_id;
    message_t message;
} notification_t;

static void capture_ack_failure(const mpmc_diagnostic_t *error, void *user_data);

static unsigned checks;
static unsigned failures;
static volatile sig_atomic_t alarm_seen;
static shm_backend_t *owned_backend;
static pid_t children[CHILD_CAPACITY];

typedef struct {
    bool enabled;
    int semid;
    unsigned index;
    short delta;
    unsigned skip_matches;
    mpmc_result_t failure;
} sem_failure_t;

static sem_failure_t sem_failure;

static void inject_sem_failure(
    int semid,
    unsigned index,
    short delta,
    unsigned skip_matches,
    mpmc_result_t failure
) {
    sem_failure = (sem_failure_t){
        .enabled = true,
        .semid = semid,
        .index = index,
        .delta = delta,
        .skip_matches = skip_matches,
        .failure = failure,
    };
}

mpmc_result_t mpmc_test_sem_change(int semid, unsigned index, short delta, bool nowait) {
    if (sem_failure.enabled && sem_failure.semid == semid && sem_failure.index == index
        && sem_failure.delta == delta) {
        if (sem_failure.skip_matches > 0) {
            sem_failure.skip_matches--;
        } else {
            mpmc_result_t failure = sem_failure.failure;
            sem_failure.enabled = false;
            return failure;
        }
    }

    return sysv_sem_change(semid, index, delta, nowait);
}

#define CHECK(condition)                                                                           \
    do {                                                                                           \
        ++checks;                                                                                  \
        if (!(condition)) {                                                                        \
            ++failures;                                                                            \
            fprintf(stderr, "FALHA %s:%d: %s\n", __FILE__, __LINE__, #condition);                  \
        }                                                                                          \
    } while (0)

static void check_result(mpmc_result_t result, mpmc_status_t expected_status, int expected_errno) {
    CHECK(result.status == expected_status);
    CHECK(result.system_errno == expected_errno);
}

static void on_alarm(int signal_number) {
    (void)signal_number;
    alarm_seen = 1;
}

static message_t make_message(uint64_t operation_id, uint32_t sequence, pid_t sender_pid) {
    return (message_t){
        .sender_pid = sender_pid,
        .sequence = sequence,
        .operation_id = operation_id,
    };
}

static notification_t make_notification(
    uint64_t operation_id,
    uint32_t sequence,
    pid_t sender_pid,
    size_t producer_id
) {
    return (notification_t){
        .producer_id = producer_id,
        .message = make_message(operation_id, sequence, sender_pid),
    };
}

static void notify_direct(const notification_t *notification) {
    CHECK(notification != nullptr);
    if (notification == nullptr) return;

    check_result(
        shm_backend_ack_notify_consumed(
            owned_backend, notification->producer_id, &notification->message
        ),
        MPMC_OK,
        0
    );
}

static bool track_child(pid_t child) {
    if (child <= 0) return false;

    for (size_t index = 0; index < CHILD_CAPACITY; index++) {
        if (children[index] == 0) {
            children[index] = child;
            return true;
        }
    }

    (void)kill(child, SIGKILL);
    (void)waitpid(child, nullptr, 0);
    CHECK(false);
    return false;
}

/**
 * @brief Cria um consumidor auxiliar que publica confirmações em outro processo.
 * @details O PID da mensagem permanece o do produtor que registrou a operação.
 */
static pid_t spawn_notifications(const notification_t *notifications, size_t count) {
    pid_t child = fork();

    if (child < 0) {
        CHECK(false);
        return -1;
    }

    if (child == 0) {
        for (size_t index = 0; index < count; index++) {
            mpmc_result_t result = shm_backend_ack_notify_consumed(
                owned_backend, notifications[index].producer_id, &notifications[index].message
            );

            if (result.status != MPMC_OK) _exit(10);
        }

        if (shm_backend_worker_close(owned_backend, nullptr).status != MPMC_OK) {
            _exit(11);
        }

        _exit(0);
    }

    (void)track_child(child);
    return child;
}

static void reap_child(pid_t child) {
    CHECK(child > 0);
    if (child <= 0) return;

    alarm_seen = 0;
    alarm(10);

    int status = 0;
    pid_t waited;

    do {
        waited = waitpid(child, &status, 0);
    } while (waited < 0 && errno == EINTR && !alarm_seen);

    alarm(0);

    bool timed_out = alarm_seen != 0;

    if (timed_out) {
        (void)kill(child, SIGKILL);

        do {
            waited = waitpid(child, &status, 0);
        } while (waited < 0 && errno == EINTR);
    }

    bool reaped = waited == child;

    if (reaped || (waited < 0 && errno == ECHILD)) {
        for (size_t index = 0; index < CHILD_CAPACITY; index++) {
            if (children[index] == child) {
                children[index] = 0;
                break;
            }
        }
    }

    CHECK(!timed_out);
    CHECK(reaped || (waited < 0 && errno == ECHILD));

    if (reaped && !timed_out) {
        CHECK(WIFEXITED(status));
        if (WIFEXITED(status)) {
            CHECK(WEXITSTATUS(status) == 0);
        }
    }
}

static void cleanup(void) {
    alarm(0);

    for (size_t index = 0; index < CHILD_CAPACITY; index++) {
        if (children[index] > 0) {
            (void)kill(children[index], SIGKILL);
        }
    }

    for (size_t index = 0; index < CHILD_CAPACITY; index++) {
        if (children[index] > 0) {
            while (waitpid(children[index], nullptr, 0) < 0 && errno == EINTR) {
            }
            children[index] = 0;
        }
    }

    if (owned_backend != nullptr) {
        shm_backend_t *backend = owned_backend;
        owned_backend = nullptr;
        (void)shm_backend_destroy(backend, nullptr);
    }
}

/** @brief Valida argumentos, estados inexistentes e índices lógicos. */
static void test_argument_validation(void) {
    check_result(shm_backend_ack_begin(nullptr, 1, 1), MPMC_ERR_INVALID_ARG, 0);
    check_result(shm_backend_ack_begin(owned_backend, 0, 1), MPMC_ERR_INVALID_ARG, 0);
    check_result(shm_backend_ack_begin(owned_backend, 1, 0), MPMC_ERR_INVALID_ARG, 0);
    check_result(
        shm_backend_ack_begin(owned_backend, 1, MPMC_MAX_MESSAGES + 1), MPMC_ERR_INVALID_ARG, 0
    );

    check_result(shm_backend_ack_wait(owned_backend, 99999), MPMC_ERR_STATE, 0);
    check_result(shm_backend_ack_end(owned_backend, 99999), MPMC_OK, 0);

    message_t message = make_message(0, 1, getpid());

    check_result(shm_backend_ack_notify_consumed(owned_backend, 1, &message), MPMC_OK, 0);
    check_result(
        shm_backend_ack_notify_consumed(owned_backend, 0, &message), MPMC_ERR_INVALID_INDEX, 0
    );
    check_result(
        shm_backend_ack_notify_consumed(owned_backend, 4, &message), MPMC_ERR_INVALID_INDEX, 0
    );
    check_result(
        shm_backend_ack_notify_consumed(owned_backend, 1, nullptr), MPMC_ERR_INVALID_ARG, 0
    );
}

/**
 * @brief Confirma mensagens em estruturas distintas antes da espera do produtor.
 * @details Demonstra que notificações antecipadas permanecem contabilizadas.
 */
static void test_early_notifications_across_structures(void) {
    const uint64_t operation_id = 101;
    const pid_t producer_pid = getpid();

    check_result(shm_backend_ack_begin(owned_backend, operation_id, 3), MPMC_OK, 0);

    const notification_t notifications[] = {
        make_notification(operation_id, 1, producer_pid, 1),
        make_notification(operation_id, 2, producer_pid, 2),
        make_notification(operation_id, 3, producer_pid, 3),
    };

    pid_t child =
        spawn_notifications(notifications, sizeof notifications / sizeof notifications[0]);
    reap_child(child);

    check_result(shm_backend_ack_wait(owned_backend, operation_id), MPMC_OK, 0);
    check_result(shm_backend_ack_end(owned_backend, operation_id), MPMC_OK, 0);
}

/**
 * @brief Verifica que mensagens inválidas ou duplicadas não completam a operação.
 */
/**
 * @brief Verifica a limpeza da notificação final e do contador ao reutilizar um slot.
 * @details A segunda operação recebe apenas um ACK e não pode herdar os ACKs
 *          antecipados da primeira operação.
 */
static void test_reuse_slot_after_early_notifications(void) {
    const uint64_t first_id = 120;
    const uint64_t second_id = 121;
    const pid_t producer_pid = getpid();

    check_result(shm_backend_ack_begin(owned_backend, first_id, 2), MPMC_OK, 0);

    notification_t first_notifications[] = {
        make_notification(first_id, 1, producer_pid, 1),
        make_notification(first_id, 2, producer_pid, 2),
    };

    pid_t child = spawn_notifications(first_notifications, 2);
    reap_child(child);

    check_result(shm_backend_ack_wait(owned_backend, first_id), MPMC_OK, 0);
    check_result(shm_backend_ack_end(owned_backend, first_id), MPMC_OK, 0);

    check_result(shm_backend_ack_begin(owned_backend, second_id, 2), MPMC_OK, 0);

    notification_t second_first = make_notification(second_id, 1, producer_pid, 1);
    notify_direct(&second_first);

    alarm_seen = 0;
    alarm(1);

    mpmc_result_t result = shm_backend_ack_wait(owned_backend, second_id);

    alarm(0);

    CHECK(alarm_seen);
    check_result(result, MPMC_ERR_INTERRUPTED, EINTR);

    notification_t second_final = make_notification(second_id, 2, producer_pid, 2);
    notify_direct(&second_final);

    check_result(shm_backend_ack_wait(owned_backend, second_id), MPMC_OK, 0);
    check_result(shm_backend_ack_end(owned_backend, second_id), MPMC_OK, 0);
}

static void test_foreign_messages_and_final_notification(void) {
    const uint64_t operation_id = 102;
    check_result(shm_backend_ack_begin(owned_backend, operation_id, 2), MPMC_OK, 0);
    notification_t wrong = make_notification(operation_id, 1, getpid() + 10000, 1);
    notify_direct(&wrong);
    CHECK(owned_backend->ack_control->records[0].remaining == 2);
    notification_t first = make_notification(operation_id, 1, getpid(), 1);
    notification_t last = make_notification(operation_id, 2, getpid(), 2);
    notify_direct(&first);
    CHECK(owned_backend->ack_control->records[0].remaining == 1);
    CHECK(semctl(owned_backend->ack_semid, SHM_ACK_SEM_NOTIFICATION_BASE, GETVAL) == 0);
    notify_direct(&last);
    CHECK(semctl(owned_backend->ack_semid, SHM_ACK_SEM_NOTIFICATION_BASE, GETVAL) == 1);
    check_result(shm_backend_ack_wait(owned_backend, operation_id), MPMC_OK, 0);
    check_result(shm_backend_ack_end(owned_backend, operation_id), MPMC_OK, 0);
}

/**
 * @brief Cria um processo que publica uma notificação após um pequeno atraso.
 * @details O atraso permite que o processo chamador entre na espera bloqueante.
 */
static pid_t spawn_delayed_notification(const notification_t *notification) {
    pid_t child = fork();

    if (child < 0) {
        CHECK(false);
        return -1;
    }

    if (child == 0) {
        struct timespec delay = {
            .tv_sec = 0,
            .tv_nsec = 100000000L,
        };

        while (nanosleep(&delay, &delay) < 0) {
            if (errno != EINTR) {
                _exit(12);
            }
        }

        mpmc_result_t result = shm_backend_ack_notify_consumed(
            owned_backend, notification->producer_id, &notification->message
        );

        if (result.status != MPMC_OK) {
            _exit(13);
        }

        if (shm_backend_worker_close(owned_backend, nullptr).status != MPMC_OK) {
            _exit(14);
        }

        _exit(0);
    }

    (void)track_child(child);
    return child;
}

/**
 * @brief Verifica recuperação quando a liberação inicial do mutex falha.
 */
static void test_wait_recovers_initial_unlock_failure(void) {
    const uint64_t operation_id = 106;

    check_result(shm_backend_ack_begin(owned_backend, operation_id, 1), MPMC_OK, 0);

    inject_sem_failure(
        owned_backend->ack_semid,
        SHM_ACK_SEM_MUTEX,
        1,
        0,
        (mpmc_result_t){.status = MPMC_ERR_INTERRUPTED, .system_errno = EINTR}
    );

    check_result(shm_backend_ack_wait(owned_backend, operation_id), MPMC_ERR_INTERRUPTED, EINTR);

    check_result(shm_backend_ack_end(owned_backend, operation_id), MPMC_OK, 0);
}

/**
 * @brief Verifica recuperação quando a aquisição do mutex no loop falha.
 */

/**
 * @brief Verifica recuperação quando a liberação do mutex no loop falha.
 */

/**
 * @brief Verifica recuperação na liberação do mutex após concluir a espera.
 */
static void test_wait_recovers_completed_unlock_failure(void) {
    const uint64_t operation_id = 109;

    check_result(shm_backend_ack_begin(owned_backend, operation_id, 1), MPMC_OK, 0);

    notification_t notification = make_notification(operation_id, 1, getpid(), 1);
    notify_direct(&notification);

    /*
     * Ignora a liberação inicial e injeta a falha na liberação
     * posterior à verificação de que todos os ACKs foram recebidos.
     */
    inject_sem_failure(
        owned_backend->ack_semid,
        SHM_ACK_SEM_MUTEX,
        1,
        0,
        (mpmc_result_t){.status = MPMC_ERR_INTERRUPTED, .system_errno = EINTR}
    );

    check_result(shm_backend_ack_wait(owned_backend, operation_id), MPMC_ERR_INTERRUPTED, EINTR);

    check_result(shm_backend_ack_end(owned_backend, operation_id), MPMC_OK, 0);
}

/**
 * @brief Verifica que uma notificação acorda o produtor bloqueado em ack_wait().
 */
static void test_wait_unblocks_on_notification(void) {
    const uint64_t operation_id = 105;

    check_result(shm_backend_ack_begin(owned_backend, operation_id, 1), MPMC_OK, 0);

    notification_t notification = make_notification(operation_id, 1, getpid(), 2);

    pid_t child = spawn_delayed_notification(&notification);

    CHECK(child > 0);
    if (child <= 0) {
        (void)shm_backend_ack_end(owned_backend, operation_id);
        return;
    }

    check_result(shm_backend_ack_wait(owned_backend, operation_id), MPMC_OK, 0);

    reap_child(child);
    check_result(shm_backend_ack_end(owned_backend, operation_id), MPMC_OK, 0);
}

/**
 * @brief Aguarda até que uma operação tenha um waiter ativo.
 * @details A leitura do registro é protegida pelo mutex global de ACK.
 */
static bool wait_for_active_waiter(uint64_t operation_id) {
    for (unsigned attempt = 0; attempt < 1000; attempt++) {
        mpmc_result_t result =
            sysv_sem_change(owned_backend->ack_semid, SHM_ACK_SEM_MUTEX, -1, false);

        if (result.status != MPMC_OK) {
            check_result(result, MPMC_OK, 0);
            return false;
        }

        bool active = false;

        for (size_t index = 0; index < SHM_ACK_OPERATION_CAPACITY; index++) {
            shm_ack_record_t *record = &owned_backend->ack_control->records[index];

            if (record->operation_id == operation_id) {
                active = record->waiting;
                break;
            }
        }

        result = sysv_sem_change(owned_backend->ack_semid, SHM_ACK_SEM_MUTEX, 1, false);
        if (result.status != MPMC_OK) {
            check_result(result, MPMC_OK, 0);
            return false;
        }

        if (active) {
            return true;
        }

        struct timespec delay = {
            .tv_sec = 0,
            .tv_nsec = 10000000L,
        };

        while (nanosleep(&delay, &delay) < 0) {
            if (errno != EINTR) {
                CHECK(false);
                return false;
            }
        }
    }

    CHECK(false);
    return false;
}

/**
 * @brief Impede que ack_end() libere um registro com uma espera ativa.
 * @details A confirmação é publicada somente depois de verificar que o
 * encerramento foi rejeitado.
 */
static void test_end_rejected_while_waiting(void) {
    const uint64_t operation_id = 106;

    check_result(shm_backend_ack_begin(owned_backend, operation_id, 1), MPMC_OK, 0);

    pid_t child = fork();

    if (child < 0) {
        CHECK(false);
        check_result(shm_backend_ack_end(owned_backend, operation_id), MPMC_OK, 0);
        return;
    }

    if (child == 0) {
        mpmc_result_t result = shm_backend_ack_wait(owned_backend, operation_id);

        if (result.status != MPMC_OK || result.system_errno != 0) {
            _exit(20);
        }

        if (shm_backend_worker_close(owned_backend, nullptr).status != MPMC_OK) {
            _exit(21);
        }

        _exit(0);
    }

    if (!track_child(child)) {
        check_result(shm_backend_ack_end(owned_backend, operation_id), MPMC_OK, 0);
        return;
    }

    bool waiter_active = wait_for_active_waiter(operation_id);
    CHECK(waiter_active);

    if (!waiter_active) {
        (void)kill(child, SIGKILL);
        reap_child(child);
        check_result(shm_backend_ack_end(owned_backend, operation_id), MPMC_OK, 0);
        return;
    }

    check_result(shm_backend_ack_end(owned_backend, operation_id), MPMC_ERR_STATE, 0);

    notification_t notification = make_notification(operation_id, 1, getpid(), 1);
    notify_direct(&notification);

    reap_child(child);

    check_result(shm_backend_ack_end(owned_backend, operation_id), MPMC_OK, 0);

    /* O mesmo slot deve estar disponível para uma nova operação. */
    check_result(shm_backend_ack_begin(owned_backend, operation_id, 1), MPMC_OK, 0);
    check_result(shm_backend_ack_end(owned_backend, operation_id), MPMC_OK, 0);
}

/**
 * @brief Duas operações diferentes recebem confirmações de processos concorrentes.
 */
/**
 * @brief Verifica que um processo filho continua bloqueado após um ACK alheio.
 * @details Usa WNOHANG para não bloquear o processo de teste.
 */
static void check_child_still_waiting(pid_t child) {
    int status = 0;
    pid_t result = waitpid(child, &status, WNOHANG);

    CHECK(result == 0);
    if (result == child) {
        for (size_t index = 0; index < CHILD_CAPACITY; index++) {
            if (children[index] == child) {
                children[index] = 0;
                break;
            }
        }
    } else if (result < 0 && errno != EINTR) {
        CHECK(false);
    }
}

/**
 * @brief Garante que uma notificação não libera o waiter de outra operação.
 */
static void test_notifications_do_not_cross_operations(void) {
    const uint64_t first_id = 110;
    const uint64_t second_id = 111;

    check_result(shm_backend_ack_begin(owned_backend, first_id, 1), MPMC_OK, 0);
    check_result(shm_backend_ack_begin(owned_backend, second_id, 1), MPMC_OK, 0);

    pid_t first_child = fork();

    if (first_child < 0) {
        CHECK(false);
        (void)shm_backend_ack_end(owned_backend, first_id);
        (void)shm_backend_ack_end(owned_backend, second_id);
        return;
    }

    if (first_child == 0) {
        mpmc_result_t result = shm_backend_ack_wait(owned_backend, first_id);
        _exit(result.status == MPMC_OK && result.system_errno == 0 ? 0 : 30);
    }

    if (!track_child(first_child)) {
        (void)shm_backend_ack_end(owned_backend, first_id);
        (void)shm_backend_ack_end(owned_backend, second_id);
        return;
    }

    pid_t second_child = fork();

    if (second_child < 0) {
        CHECK(false);
        (void)kill(first_child, SIGKILL);
        reap_child(first_child);
        (void)shm_backend_ack_end(owned_backend, first_id);
        (void)shm_backend_ack_end(owned_backend, second_id);
        return;
    }

    if (second_child == 0) {
        mpmc_result_t result = shm_backend_ack_wait(owned_backend, second_id);
        _exit(result.status == MPMC_OK && result.system_errno == 0 ? 0 : 31);
    }

    if (!track_child(second_child)) {
        (void)kill(first_child, SIGKILL);
        reap_child(first_child);
        (void)shm_backend_ack_end(owned_backend, first_id);
        (void)shm_backend_ack_end(owned_backend, second_id);
        return;
    }

    bool first_waiting = wait_for_active_waiter(first_id);
    bool second_waiting = wait_for_active_waiter(second_id);
    CHECK(first_waiting);
    CHECK(second_waiting);

    if (!first_waiting || !second_waiting) {
        (void)kill(first_child, SIGKILL);
        (void)kill(second_child, SIGKILL);
        reap_child(first_child);
        reap_child(second_child);
        (void)shm_backend_ack_end(owned_backend, first_id);
        (void)shm_backend_ack_end(owned_backend, second_id);
        return;
    }

    notification_t first_notification = make_notification(first_id, 1, getpid(), 1);
    notify_direct(&first_notification);

    reap_child(first_child);
    check_child_still_waiting(second_child);

    notification_t second_notification = make_notification(second_id, 1, getpid(), 2);
    notify_direct(&second_notification);

    reap_child(second_child);

    check_result(shm_backend_ack_end(owned_backend, first_id), MPMC_OK, 0);
    check_result(shm_backend_ack_end(owned_backend, second_id), MPMC_OK, 0);
}

static void test_simultaneous_operations(void) {
    const uint64_t first_id = 103;
    const uint64_t second_id = 104;
    const unsigned message_count = 16;
    const pid_t producer_pid = getpid();

    notification_t first[message_count];
    notification_t second[message_count];

    check_result(shm_backend_ack_begin(owned_backend, first_id, message_count), MPMC_OK, 0);
    check_result(shm_backend_ack_begin(owned_backend, second_id, message_count), MPMC_OK, 0);

    for (unsigned index = 0; index < message_count; index++) {
        first[index] = make_notification(first_id, index + 1, producer_pid, (index % 3) + 1);
        second[index] =
            make_notification(second_id, index + 1, producer_pid, ((index + 1) % 3) + 1);
    }

    pid_t first_child = spawn_notifications(first, message_count);
    pid_t second_child = spawn_notifications(second, message_count);

    reap_child(first_child);
    reap_child(second_child);

    check_result(shm_backend_ack_wait(owned_backend, first_id), MPMC_OK, 0);
    check_result(shm_backend_ack_wait(owned_backend, second_id), MPMC_OK, 0);

    check_result(shm_backend_ack_end(owned_backend, first_id), MPMC_OK, 0);
    check_result(shm_backend_ack_end(owned_backend, second_id), MPMC_OK, 0);
}

/**
 * @brief Exercita a capacidade máxima e 1024 consumos no último registro.
 */
static void test_capacity_counter_and_reuse(void) {
    enum {
        OPERATION_COUNT = 64,
        FIRST_OPERATION_ID = 200,
        LAST_EXPECTED = MPMC_MAX_MESSAGES,
    };

    for (unsigned index = 0; index < OPERATION_COUNT; index++) {
        unsigned expected = index == OPERATION_COUNT - 1 ? LAST_EXPECTED : 1;

        check_result(
            shm_backend_ack_begin(owned_backend, FIRST_OPERATION_ID + index, expected), MPMC_OK, 0
        );
    }

    CHECK(semctl(owned_backend->ack_semid, SHM_ACK_SEM_AVAILABLE, GETVAL) == 0);

    for (uint32_t sequence = 1; sequence <= LAST_EXPECTED; sequence++) {
        notification_t notification = make_notification(
            FIRST_OPERATION_ID + OPERATION_COUNT - 1, sequence, getpid(), (sequence % 3) + 1
        );
        notify_direct(&notification);
    }

    check_result(
        shm_backend_ack_wait(owned_backend, FIRST_OPERATION_ID + OPERATION_COUNT - 1), MPMC_OK, 0
    );

    for (unsigned index = 0; index < OPERATION_COUNT; index++) {
        check_result(shm_backend_ack_end(owned_backend, FIRST_OPERATION_ID + index), MPMC_OK, 0);
    }

    const uint64_t reused_id = 900;

    check_result(shm_backend_ack_begin(owned_backend, reused_id, 1), MPMC_OK, 0);

    notification_t notification = make_notification(reused_id, 1, getpid(), 1);
    notify_direct(&notification);

    check_result(shm_backend_ack_wait(owned_backend, reused_id), MPMC_OK, 0);
    check_result(shm_backend_ack_end(owned_backend, reused_id), MPMC_OK, 0);
    check_result(shm_backend_ack_end(owned_backend, reused_id), MPMC_OK, 0);
}

/**
 * @brief Verifica nova tentativa de unlock em caminhos que preservam erros.
 */
static void test_preserving_path_recovers_unlock_failure(void) {
    const uint64_t operation_id = 112;

    check_result(shm_backend_ack_begin(owned_backend, operation_id, 1), MPMC_OK, 0);

    inject_sem_failure(
        owned_backend->ack_semid,
        SHM_ACK_SEM_MUTEX,
        1,
        0,
        (mpmc_result_t){.status = MPMC_ERR_INTERRUPTED, .system_errno = EINTR}
    );

    mpmc_diagnostic_t captured[2] = {};
    mpmc_reporter_t reporter = {.on_error = capture_ack_failure, .user_data = captured};
    mpmc_diagnostic_set_reporter(&reporter);

    /*
     * O segundo begin é rejeitado por duplicidade. A falha injetada ocorre
     * ao liberar o mutex; a tentativa seguinte deve liberá-lo de verdade.
     */
    check_result(shm_backend_ack_begin(owned_backend, operation_id, 1), MPMC_ERR_STATE, 0);

    mpmc_diagnostic_set_reporter(nullptr);
    CHECK(captured[0].result.status == MPMC_ERR_STATE);
    CHECK(captured[0].phase == MPMC_DIAGNOSTIC_OPERATION);
    CHECK(captured[1].result.status == MPMC_ERR_INTERRUPTED);
    CHECK(captured[1].phase == MPMC_DIAGNOSTIC_CLEANUP);
    CHECK(!sem_failure.enabled);
    check_result(shm_backend_ack_end(owned_backend, operation_id), MPMC_OK, 0);
}

/**
 * @brief Verifica que ack_end libera o registro após falha transitória no unlock.
 */
static void test_end_releases_slot_after_unlock_failure(void) {
    const uint64_t operation_id = 110;
    const uint64_t reused_operation_id = 111;

    check_result(shm_backend_ack_begin(owned_backend, operation_id, 1), MPMC_OK, 0);

    size_t slot = SHM_ACK_OPERATION_CAPACITY;
    for (size_t index = 0; index < SHM_ACK_OPERATION_CAPACITY; index++) {
        if (owned_backend->ack_control->records[index].operation_id == operation_id) {
            slot = index;
            break;
        }
    }

    CHECK(slot < SHM_ACK_OPERATION_CAPACITY);
    if (slot == SHM_ACK_OPERATION_CAPACITY) return;

    notification_t notification = make_notification(operation_id, 1, getpid(), 1);
    notify_direct(&notification);

    inject_sem_failure(
        owned_backend->ack_semid,
        SHM_ACK_SEM_MUTEX,
        1,
        0,
        (mpmc_result_t){.status = MPMC_ERR_INTERRUPTED, .system_errno = EINTR}
    );

    check_result(shm_backend_ack_end(owned_backend, operation_id), MPMC_ERR_INTERRUPTED, EINTR);

    CHECK(!sem_failure.enabled);
    CHECK(owned_backend->ack_control->records[slot].operation_id == 0);

    check_result(shm_backend_ack_begin(owned_backend, reused_operation_id, 1), MPMC_OK, 0);
    check_result(shm_backend_ack_end(owned_backend, reused_operation_id), MPMC_OK, 0);
}

/**
 * @brief Verifica a limpeza do waiter após uma interrupção adicional do mutex.
 */
static void test_waiter_release_retries_interrupted_lock(void) {
    const uint64_t operation_id = 113;

    check_result(shm_backend_ack_begin(owned_backend, operation_id, 1), MPMC_OK, 0);

    /*
     * As duas primeiras aquisições registram o waiter e consultam a operação.
     * A terceira ocorre durante a limpeza do waiter.
     */
    inject_sem_failure(
        owned_backend->ack_semid,
        SHM_ACK_SEM_MUTEX,
        -1,
        1,
        (mpmc_result_t){.status = MPMC_ERR_INTERRUPTED, .system_errno = EINTR}
    );

    alarm_seen = 0;
    alarm(1);

    mpmc_result_t result = shm_backend_ack_wait(owned_backend, operation_id);

    alarm(0);

    CHECK(alarm_seen);
    check_result(result, MPMC_ERR_INTERRUPTED, EINTR);
    CHECK(!sem_failure.enabled);

    /* A espera terminou e o registro deve poder ser encerrado normalmente. */
    check_result(shm_backend_ack_end(owned_backend, operation_id), MPMC_OK, 0);
}

/**
 * @brief Verifica a propagação de uma falha fatal na espera por ACK.
 * @details Executado por último: após EIDRM, o contexto não é reutilizado.
 */
static void test_wait_fatal_sem_failure(void) {
    const uint64_t operation_id = 999;
    const mpmc_result_t fatal = {
        .status = MPMC_ERR_SHUTDOWN,
        .system_errno = EIDRM,
    };

    check_result(shm_backend_ack_begin(owned_backend, operation_id, 1), MPMC_OK, 0);

    size_t slot = SHM_ACK_OPERATION_CAPACITY;

    for (size_t index = 0; index < SHM_ACK_OPERATION_CAPACITY; index++) {
        if (owned_backend->ack_control->records[index].operation_id == operation_id) {
            slot = index;
            break;
        }
    }

    CHECK(slot < SHM_ACK_OPERATION_CAPACITY);
    if (slot == SHM_ACK_OPERATION_CAPACITY) return;

    inject_sem_failure(
        owned_backend->ack_semid, SHM_ACK_SEM_NOTIFICATION_BASE + (unsigned)slot, -1, 0, fatal
    );

    check_result(shm_backend_ack_wait(owned_backend, operation_id), MPMC_ERR_SHUTDOWN, EIDRM);

    CHECK(!sem_failure.enabled);

    /* Não chamar ack_end nem reutilizar o contexto após a falha fatal. */
}

int main(void) {
    struct sigaction action = {
        .sa_handler = on_alarm,
        .sa_flags = 0,
    };
    sigemptyset(&action.sa_mask);

    if (sigaction(SIGALRM, &action, nullptr) != 0) {
        perror("sigaction");
        return EXIT_FAILURE;
    }

    if (atexit(cleanup) != 0) {
        return EXIT_FAILURE;
    }

    mpmc_result_t result = shm_backend_create(&owned_backend, 3, nullptr);
    CHECK(result.status == MPMC_OK);
    CHECK(result.system_errno == 0);
    CHECK(owned_backend != nullptr);

    if (owned_backend != nullptr) {
        test_argument_validation();
        test_early_notifications_across_structures();
        test_reuse_slot_after_early_notifications();
        test_foreign_messages_and_final_notification();
        test_wait_recovers_initial_unlock_failure();
        test_wait_recovers_completed_unlock_failure();
        test_wait_unblocks_on_notification();
        test_end_rejected_while_waiting();
        test_simultaneous_operations();
        test_notifications_do_not_cross_operations();
        test_capacity_counter_and_reuse();
        test_preserving_path_recovers_unlock_failure();
        test_end_releases_slot_after_unlock_failure();
        test_waiter_release_retries_interrupted_lock();
        test_wait_fatal_sem_failure();
    }

    cleanup();

    printf("%u verificações de ACK SHM, %u falhas\n", checks, failures);
    return failures == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}

/** @brief Conserva separadamente a causa e a falha de unlock na fixture de duplicidade. */
static void capture_ack_failure(const mpmc_diagnostic_t *error, void *user_data) {
    mpmc_diagnostic_t *captured = user_data;
    size_t index = captured[0].result.status == MPMC_OK ? 0 : 1;
    CHECK(captured[index].result.status == MPMC_OK);
    captured[index] = *error;
}
