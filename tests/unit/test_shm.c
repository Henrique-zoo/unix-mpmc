/**
 * @file test_shm.c
 * @brief Testes dos wrappers de memória compartilhada System V.
 * @ingroup tests
 * @details Verifica tamanho e permissões dos segmentos, validação de argumentos,
 * preservação das causas de sistema e ciclo de vida entre pai e filho.
 * O cleanup de segurança remove somente segmentos criados pelo processo de teste.
 */

#include <errno.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/shm.h>
#include <sys/wait.h>
#include <unistd.h>

#include "mpmc/ipc/shm.h"
#include "mpmc/message.h"

static unsigned checks;
static unsigned failures;
static pid_t owner_pid;
static int owned_segments[8];
static size_t owned_count;

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
        errno = EACCES;                                                                            \
        mpmc_result_t result_ = (call);                                                            \
        errno = EIO;                                                                               \
        CHECK(result_.status == (expected_status));                                                \
        CHECK(result_.system_errno == (expected_errno));                                           \
    } while (0)

/** @brief Fallback de cleanup, inclusive quando uma verificação falha. */
static void cleanup_segments(void) {
    if (getpid() != owner_pid) return;

    for (size_t index = 0; index < owned_count; ++index) {
        if (owned_segments[index] >= 0) {
            (void)shmctl(owned_segments[index], IPC_RMID, NULL);
        }
    }
}

/** @brief Registra um segmento próprio para remoção de segurança na saída. */
static void track_segment(int shmid) {
    const size_t capacity = sizeof owned_segments / sizeof owned_segments[0];
    CHECK(owned_count < capacity);
    if (owned_count < capacity) {
        owned_segments[owned_count++] = shmid;
    } else {
        (void)shmctl(shmid, IPC_RMID, NULL);
    }
}

/** @brief Cria e registra um segmento de dados de 2048 bytes. */
static int new_segment(void) {
    int shmid = -1;
    errno = EACCES;
    mpmc_result_t result = sysv_shm_create(MPMC_SHM_SEGMENT_BYTES, &shmid);
    CHECK(result.status == MPMC_OK);
    CHECK(result.system_errno == 0);
    if (result.status != MPMC_OK) return -1;

    CHECK(shmid >= 0);
    track_segment(shmid);
    return shmid;
}

/** @brief Exercita a remoção pelo wrapper e dispensa o fallback em sucesso. */
static void remove_segment(int shmid) {
    errno = EACCES;
    mpmc_result_t result = sysv_shm_remove(shmid);
    CHECK(result.status == MPMC_OK);
    CHECK(result.system_errno == 0);
    if (result.status == MPMC_OK) {
        for (size_t index = 0; index < owned_count; ++index) {
            if (owned_segments[index] == shmid) {
                owned_segments[index] = -1;
            }
        }
    }
}

/** @brief Confere no kernel que um segmento sem associações foi destruído. */
static void check_removed(int shmid) {
    struct shmid_ds info;
    int rc = shmctl(shmid, IPC_STAT, &info);
    int system_errno = errno;
    CHECK(rc == -1);
    CHECK(system_errno == EINVAL || system_errno == EIDRM);
}

/** @brief Handler vazio: só existe para que SIGCHLD deixe de ser descartado. */
static void noop_sigchld_handler(int signal_number) {
    (void)signal_number;
}

/**
 * @brief Recolhe o filho com timeout de três segundos definido por alarm().
 * @details Bloqueia SIGCHLD e SIGALRM antes de verificar o filho e espera as
 * notificações com sigwait(), sem consultas periódicas. Este teste reserva
 * SIGALRM para essa espera; o alarme é cancelado e a máscara anterior restaurada
 * antes do retorno. Em falha, o filho é terminado e recolhido.
 */
static bool wait_for_child(pid_t child, int *status) {
    sigset_t wait_signals;
    sigset_t previous_mask;
    struct sigaction chld_action = {0};
    struct sigaction old_chld_action;
    bool action_saved = false;
    bool mask_saved = false;
    bool collected = false;
    bool have_child = true;

    sigemptyset(&wait_signals);
    sigaddset(&wait_signals, SIGCHLD);
    sigaddset(&wait_signals, SIGALRM);
    int rc = sigprocmask(SIG_BLOCK, &wait_signals, &previous_mask);

    CHECK(rc == 0);

    if (rc == -1) {
        goto cleanup;
    }

    mask_saved = true;

    chld_action.sa_handler = noop_sigchld_handler;
    sigemptyset(&chld_action.sa_mask);
    rc = sigaction(SIGCHLD, &chld_action, &old_chld_action);
    CHECK(rc == 0);
    if (rc == 0) {
        action_saved = true;
    }

    (void)alarm(3);

    for (;;) {
        pid_t waited = waitpid(child, status, WNOHANG);
        if (waited == child) {
            collected = true;
            break;
        }
        if (waited == -1) {
            if (errno == EINTR) {
                continue;
            }

            have_child = errno != ECHILD;
            CHECK(false);
            break;
        }

        int signal_number;
        rc = sigwait(&wait_signals, &signal_number);
        CHECK(rc == 0);
        if (rc != 0) {
            break;
        }

        if (signal_number == SIGALRM) {
            fputs("Filho de teste não concluiu dentro do prazo.\n", stderr);
            CHECK(false);
            break;
        }
    }

cleanup:
    if (mask_saved) {
        (void)alarm(0);
        /* O alarme pode ter vencido junto com o término do filho. */
        sigset_t pending;
        rc = sigpending(&pending);
        CHECK(rc == 0);
        if (rc == 0 && sigismember(&pending, SIGALRM) == 1) {
            sigdelset(&wait_signals, SIGCHLD);
            int signal_number;
            CHECK(sigwait(&wait_signals, &signal_number) == 0);
        }
    }

    if (!collected && have_child) {
        rc = kill(child, SIGKILL);
        CHECK(rc == 0 || errno == ESRCH);
        pid_t waited;
        do {
            waited = waitpid(child, status, 0);
        } while (waited == -1 && errno == EINTR);
        CHECK(waited == child || (waited == -1 && errno == ECHILD));
    }
    if (action_saved) {
        CHECK(sigaction(SIGCHLD, &old_chld_action, NULL) == 0);
    }
    if (mask_saved) {
        CHECK(sigprocmask(SIG_SETMASK, &previous_mask, NULL) == 0);
    }

    return collected;
}

static void test_invalid_arguments(void) {
    int shmid = -1;
    CHECK_RESULT(sysv_shm_create(0, &shmid), MPMC_ERR_INVALID_ARG, 0);
    CHECK(shmid == -1);
    CHECK_RESULT(sysv_shm_create(MPMC_SHM_SEGMENT_BYTES, NULL), MPMC_ERR_INVALID_ARG, 0);

    int sentinel;
    void *address = &sentinel;
    CHECK_RESULT(sysv_shm_attach(-1, &address), MPMC_ERR_INVALID_ARG, 0);
    CHECK(address == &sentinel);
    CHECK_RESULT(sysv_shm_attach(0, NULL), MPMC_ERR_INVALID_ARG, 0);
    CHECK_RESULT(sysv_shm_detach(NULL), MPMC_ERR_INVALID_ARG, 0);
    CHECK_RESULT(sysv_shm_remove(-1), MPMC_ERR_INVALID_ARG, 0);
}

static void test_create_system_error(void) {
    int shmid = -1;
    mpmc_result_t result = sysv_shm_create(SIZE_MAX, &shmid);

    if (result.status == MPMC_OK) {
        track_segment(shmid);
    }

    errno = EACCES;
    CHECK(result.status == MPMC_ERR_SYS);
    CHECK(result.system_errno != 0);
    CHECK(shmid == -1);
}

static void test_segments_are_private_and_sized(void) {
    int first = new_segment();
    if (first < 0) return;

    int second = new_segment();
    if (second < 0) return;

    CHECK(first != second);

    const int segments[] = {first, second};
    for (size_t index = 0; index < sizeof segments / sizeof segments[0]; ++index) {
        struct shmid_ds info;
        int rc = shmctl(segments[index], IPC_STAT, &info);
        CHECK(rc == 0);
        if (rc == 0) {
            CHECK(info.shm_segsz == 2048);
            CHECK((info.shm_perm.mode & 0777) == 0600);
            CHECK(info.shm_nattch == 0);
        }
    }

    void *address = NULL;
    errno = EACCES;
    mpmc_result_t result = sysv_shm_attach(first, &address);
    CHECK(result.status == MPMC_OK && result.system_errno == 0);
    if (result.status == MPMC_OK) {
        CHECK_RESULT(sysv_shm_detach(address), MPMC_OK, 0);
        /* A segunda desassociação chega ao kernel, sem dereferenciar address. */
        CHECK_RESULT(sysv_shm_detach(address), MPMC_ERR_SYS, EINVAL);
        struct shmid_ds info;
        int rc = shmctl(first, IPC_STAT, &info);
        CHECK(rc == 0);
        if (rc == 0) {
            CHECK(info.shm_nattch == 0);
        }
    }

    remove_segment(first);
    check_removed(first);

    int sentinel;
    address = &sentinel;
    result = sysv_shm_attach(first, &address);
    errno = EACCES;
    CHECK(result.status == MPMC_ERR_SYS);
    CHECK(result.system_errno == EINVAL || result.system_errno == EIDRM);
    CHECK(address == &sentinel);

    result = sysv_shm_remove(first);
    errno = EACCES;
    CHECK(result.status == MPMC_ERR_SYS);
    CHECK(result.system_errno == EINVAL || result.system_errno == EIDRM);

    remove_segment(second);
    check_removed(second);
}

static void test_parent_and_child_lifecycle(void) {
    int shmid = new_segment();
    if (shmid < 0) return;

    void *address = NULL;
    mpmc_result_t result = sysv_shm_attach(shmid, &address);
    CHECK(result.status == MPMC_OK && result.system_errno == 0);
    if (result.status != MPMC_OK) return;

    unsigned char *bytes = address;
    bytes[0] = 0;

    pid_t child = fork();
    CHECK(child >= 0);
    if (child == -1) return;

    if (child == 0) {
        bytes[0] = 42;
        result = sysv_shm_detach(address);
        _exit(result.status == MPMC_OK && result.system_errno == 0 ? 0 : 1);
    }

    int status;
    if (!wait_for_child(child, &status)) return;

    CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 0);
    CHECK(bytes[0] == 42);
    struct shmid_ds info;
    int rc = shmctl(shmid, IPC_STAT, &info);
    CHECK(rc == 0);
    if (rc == 0) {
        CHECK(info.shm_nattch == 1);
    }

    /* Só o pai solicita IPC_RMID; seu mapeamento continua válido até shmdt(). */
    remove_segment(shmid);
    CHECK(bytes[0] == 42);
    bytes[MPMC_SHM_SEGMENT_BYTES - 1] = 7;
    CHECK_RESULT(sysv_shm_detach(address), MPMC_OK, 0);
    check_removed(shmid);
}

int main(void) {
    owner_pid = getpid();
    if (atexit(cleanup_segments) != 0) {
        fputs("Não foi possível registrar cleanup dos segmentos de teste.\n", stderr);
        return 1;
    }

    test_invalid_arguments();
    test_create_system_error();
    test_segments_are_private_and_sized();
    test_parent_and_child_lifecycle();

    printf("%u verificações de memória compartilhada, %u falhas\n", checks, failures);
    return failures == 0 ? 0 : 1;
}
