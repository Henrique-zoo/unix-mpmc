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

/**
 * @brief Quantidade de verificações executadas nesta suíte.
 */
static unsigned checks;
/**
 * @brief Quantidade de verificações que registraram falha nesta suíte.
 */
static unsigned failures;
/**
 * @brief PID do dono dos segmentos; impede que o fallback remova recursos no filho.
 */
static pid_t owner_pid;
/**
 * @brief Identificadores dos segmentos próprios; entradas removidas recebem -1.
 */
static int owned_segments[8];
/**
 * @brief Quantidade de posições registradas no vetor de segmentos próprios.
 */
static size_t owned_count;

/**
 * @brief Registra uma verificação e informa a falha sem interromper a suíte.
 * @param condition Expressão lógica avaliada uma única vez.
 * @details Incrementa o contador de verificações; se a condição for falsa,
 * incrementa o contador de falhas e escreve o diagnóstico em stderr.
 */
#define CHECK(condition)                                                                           \
    do {                                                                                           \
        ++checks;                                                                                  \
        if (!(condition)) {                                                                        \
            ++failures;                                                                            \
            fprintf(stderr, "FALHA %s:%d: %s\n", __FILE__, __LINE__, #condition);                  \
        }                                                                                          \
    } while (0)

/**
 * @brief Executa uma operação uma vez e confere status e system_errno.
 * @param call Expressão que retorna mpmc_result_t.
 * @param expected_status Categoria de resultado esperada.
 * @param expected_errno Valor esperado para o errno armazenado no resultado.
 * @details Altera errno antes e depois da operação para conferir que o resultado
 * não depende do valor global corrente.
 */
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
            (void)shmctl(owned_segments[index], IPC_RMID, nullptr);
        }
    }
}

/**
 * @brief Registra um segmento próprio para remoção de segurança na saída.
 * @param shmid Identificador do segmento criado pelo teste.
 * @details Se o vetor estiver cheio, registra falha e tenta remover imediatamente
 * o segmento adicional, sem incluí-lo no vetor de fallback.
 */
static void track_segment(int shmid) {
    const size_t capacity = sizeof owned_segments / sizeof owned_segments[0];
    CHECK(owned_count < capacity);
    if (owned_count < capacity) {
        owned_segments[owned_count++] = shmid;
    } else {
        (void)shmctl(shmid, IPC_RMID, nullptr);
    }
}

/**
 * @brief Cria e registra um segmento de dados de 2048 bytes.
 * @return Identificador criado, inclusive zero; -1 se sysv_shm_create() falhar.
 * @details Confere status e system_errno e registra o segmento para cleanup na saída.
 * Falhas são registradas pelos contadores da suíte.
 */
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

/**
 * @brief Exercita a remoção pelo wrapper e dispensa o fallback em sucesso.
 * @param shmid Identificador do segmento próprio a marcar para remoção.
 * @details Confere status e system_errno. Somente em sucesso substitui os registros
 * correspondentes no vetor de fallback por -1.
 */
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

/**
 * @brief Confere no kernel que um segmento sem associações foi destruído.
 * @param shmid Identificador previamente removido e sem vínculos restantes.
 * @details Espera falha de shmctl(IPC_STAT) com EINVAL ou EIDRM e registra
 * qualquer resultado diferente nos contadores da suíte.
 */
static void check_removed(int shmid) {
    struct shmid_ds info;
    int rc = shmctl(shmid, IPC_STAT, &info);
    int system_errno = errno;
    CHECK(rc == -1);
    CHECK(system_errno == EINVAL || system_errno == EIDRM);
}

/**
 * @brief Mantém uma ação capturável de SIGCHLD durante a espera síncrona do teste.
 * @param signal_number Número do sinal recebido; ignorado.
 * @details O tratador vazio evita depender do descarte da ação padrão de SIGCHLD
 * em plataformas como macOS. Não executa coleta nem operações em contexto de sinal.
 */
static void noop_sigchld_handler(int signal_number) {
    (void)signal_number;
}

/**
 * @brief Recolhe o filho com timeout de três segundos definido por alarm().
 * @details Bloqueia SIGCHLD e SIGALRM antes de verificar o filho e espera as
 * notificações com sigwait(), sem consultas periódicas. Este teste reserva
 * SIGALRM para essa espera; o alarme é cancelado e a máscara anterior restaurada
 * antes do retorno. Em falha, o filho é terminado e recolhido.
 * Instala temporariamente noop_sigchld_handler() e restaura a ação anterior
 * antes de restaurar a máscara de sinais.
 * @param child PID positivo do filho pertencente ao processo de teste.
 * @param[out] status Recebe o estado de waitpid() quando o filho é recolhido.
 * @return true se a coleta normal ocorreu antes do timeout; false em erro ou timeout,
 * mesmo quando a coleta de segurança posterior conseguir recolher o filho.
 */
static bool wait_for_child(pid_t child, int *status) {
    sigset_t wait_signals;
    sigset_t previous_mask;
    struct sigaction chld_action = {};
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
        CHECK(sigaction(SIGCHLD, &old_chld_action, nullptr) == 0);
    }
    if (mask_saved) {
        CHECK(sigprocmask(SIG_SETMASK, &previous_mask, nullptr) == 0);
    }

    return collected;
}

/**
 * @brief Confere a rejeição de argumentos inválidos sem alterar os parâmetros de saída.
 */
static void test_invalid_arguments(void) {
    int shmid = -1;
    CHECK_RESULT(sysv_shm_create(0, &shmid), MPMC_ERR_INVALID_ARG, EINVAL);
    CHECK(shmid == -1);
    CHECK_RESULT(sysv_shm_create(MPMC_SHM_SEGMENT_BYTES, nullptr), MPMC_ERR_INVALID_ARG, 0);

    int sentinel;
    void *address = &sentinel;
    CHECK_RESULT(sysv_shm_attach(-1, &address), MPMC_ERR_INVALID_ARG, EINVAL);
    CHECK(address == &sentinel);
    CHECK_RESULT(sysv_shm_attach(0, nullptr), MPMC_ERR_INVALID_ARG, 0);
    CHECK_RESULT(sysv_shm_detach(nullptr), MPMC_ERR_INVALID_ARG, EINVAL);
    CHECK_RESULT(sysv_shm_remove(-1), MPMC_ERR_INVALID_ARG, EINVAL);

    struct shmid_ds info = {.shm_segsz = 123};
    CHECK_RESULT(sysv_shm_stat(-1, &info), MPMC_ERR_INVALID_ARG, EINVAL);
    CHECK(info.shm_segsz == 123);
}

/**
 * @brief Confere a preservação de errno quando o kernel rejeita o tamanho do segmento.
 */
static void test_create_system_error(void) {
    int shmid = -1;
    mpmc_result_t result = sysv_shm_create(SIZE_MAX, &shmid);

    if (result.status == MPMC_OK) {
        track_segment(shmid);
    }

    errno = EACCES;
    CHECK(result.status == MPMC_ERR_INVALID_ARG);
    CHECK(result.system_errno == EINVAL);
    CHECK(shmid == -1);
}

/**
 * @brief Confere segmentos independentes, tamanho, permissões e falhas de associação/liberação.
 */
static void test_segments_are_private_and_sized(void) {
    int first = new_segment();
    if (first < 0) return;

    int second = new_segment();
    if (second < 0) return;

    CHECK(first != second);

    const int segments[] = {first, second};
    for (size_t index = 0; index < sizeof segments / sizeof segments[0]; ++index) {
        struct shmid_ds info;
        mpmc_result_t stat_result = sysv_shm_stat(segments[index], &info);
        CHECK(stat_result.status == MPMC_OK && stat_result.system_errno == 0);
        if (stat_result.status == MPMC_OK) {
            CHECK(info.shm_segsz == 2048);
            CHECK((info.shm_perm.mode & 0777) == 0600);
            CHECK(info.shm_nattch == 0);
        }
    }

    CHECK_RESULT(sysv_shm_stat(first, nullptr), MPMC_ERR_INVALID_ARG, EFAULT);

    void *address = nullptr;
    errno = EACCES;
    mpmc_result_t result = sysv_shm_attach(first, &address);
    CHECK(result.status == MPMC_OK && result.system_errno == 0);
    if (result.status == MPMC_OK) {
        CHECK_RESULT(sysv_shm_detach(address), MPMC_OK, 0);
        /* A segunda desassociação chega ao kernel, sem dereferenciar address. */
        CHECK_RESULT(sysv_shm_detach(address), MPMC_ERR_INVALID_ARG, EINVAL);
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
    CHECK(result.system_errno == EINVAL || result.system_errno == EIDRM);
    CHECK(
        result.status == (result.system_errno == EIDRM ? MPMC_ERR_SHUTDOWN : MPMC_ERR_INVALID_ARG)
    );
    CHECK(address == &sentinel);

    result = sysv_shm_remove(first);
    errno = EACCES;
    CHECK(result.system_errno == EINVAL || result.system_errno == EIDRM);
    CHECK(
        result.status == (result.system_errno == EIDRM ? MPMC_ERR_SHUTDOWN : MPMC_ERR_INVALID_ARG)
    );

    remove_segment(second);
    check_removed(second);
}

/**
 * @brief Confere escrita compartilhada, detach local do filho e remoção global pelo pai.
 */
static void test_parent_and_child_lifecycle(void) {
    int shmid = new_segment();
    if (shmid < 0) return;

    void *address = nullptr;
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

/**
 * @brief Executa os casos da suíte e apresenta o resultado das verificações.
 * @return Zero se nenhuma verificação falhar; um se houver falha ou erro de preparação.
 */
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
