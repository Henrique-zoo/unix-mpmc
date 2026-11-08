
/**
 * @file test_sem.c
 * @brief Testa semáforos reais e injeta falhas de semop() no objeto de teste.
 * @ingroup tests
 */
#include <errno.h>
#include <limits.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/sem.h>
#include <unistd.h>

#include "mpmc/ipc/sem.h"

/**
 * @brief Quantidade de verificações executadas nesta suíte.
 */
static unsigned checks;
/**
 * @brief Quantidade de verificações que registraram falha nesta suíte.
 */
static unsigned failures;
/**
 * @brief Conjunto criado pela suíte; -1 indica ausência de recurso a remover.
 */
static int owned_semid = -1;
/**
 * @brief Erro simulado de semop(); zero mantém a chamada real ao kernel.
 */
static int injected_semop_errno;

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
 */
#define CHECK_RESULT(call, expected_status, expected_errno)                                        \
    do {                                                                                           \
        mpmc_result_t result_ = (call);                                                            \
        CHECK(result_.status == (expected_status));                                                \
        CHECK(result_.system_errno == (expected_errno));                                           \
    } while (0)

/**
 * @brief Intercepta semop() somente no objeto de teste das primitivas.
 * @param semid Identificador repassado à chamada real quando não há injeção.
 * @param operations Vetor de operações repassado à chamada real.
 * @param count Quantidade de operações no vetor.
 * @return Zero em sucesso da chamada real; -1 em falha real ou injetada.
 * @details Se injected_semop_errno for não zero, não acessa IPC: atribui esse
 * código a errno e retorna -1. Caso contrário, preserva o comportamento de semop().
 */
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

/**
 * @brief Permite interromper semop() pela entrega de SIGALRM.
 * @param signal_number Número do sinal recebido; ignorado.
 * @details Não realiza operações em contexto de sinal.
 */
static void on_alarm(int signal_number) {
    (void)signal_number;
}

/**
 * @brief Confere argumentos inválidos e a preservação da saída na criação rejeitada.
 */
static void test_arguments(void) {
    int sentinel = -123;

    CHECK_RESULT(sysv_sem_create(0, &sentinel), MPMC_ERR_INVALID_ARG, EINVAL);
    CHECK(sentinel == -123);

    CHECK_RESULT(sysv_sem_create(UINT_MAX, &sentinel), MPMC_ERR_INVALID_ARG, 0);
    CHECK(sentinel == -123);

    CHECK_RESULT(sysv_sem_create(1, NULL), MPMC_ERR_INVALID_ARG, 0);
    CHECK_RESULT(sysv_sem_set(-1, 0, 0), MPMC_ERR_INVALID_ARG, EINVAL);
    CHECK_RESULT(sysv_sem_change(-1, 0, -1, true), MPMC_ERR_INVALID_ARG, EINVAL);
    CHECK_RESULT(sysv_sem_remove(-1), MPMC_ERR_INVALID_ARG, EINVAL);
}

/** @brief Delega argumentos ao kernel e verifica aplicação ou rejeição atômica do vetor. */
static void test_atomic_apply(void) {
    CHECK_RESULT(sysv_sem_set(owned_semid, 0, 1), MPMC_OK, 0);
    CHECK_RESULT(sysv_sem_set(owned_semid, 1, 0), MPMC_OK, 0);
    struct sembuf operations[] = {
        {.sem_num = 0, .sem_op = -1, .sem_flg = IPC_NOWAIT},
        {.sem_num = 1, .sem_op = -1, .sem_flg = IPC_NOWAIT},
    };

    CHECK_RESULT(sysv_sem_apply(owned_semid, operations, 0), MPMC_ERR_INVALID_ARG, EINVAL);
    CHECK_RESULT(sysv_sem_apply(owned_semid, NULL, 1), MPMC_ERR_INVALID_ARG, EFAULT);
    CHECK_RESULT(sysv_sem_apply(owned_semid, operations, 2), MPMC_ERR_WOULD_BLOCK, EAGAIN);
    CHECK(semctl(owned_semid, 0, GETVAL) == 1);
    CHECK(semctl(owned_semid, 1, GETVAL) == 0);

    operations[1].sem_num = 2;
    CHECK_RESULT(sysv_sem_apply(owned_semid, operations, 2), MPMC_ERR_INVALID_INDEX, EFBIG);
    CHECK(semctl(owned_semid, 0, GETVAL) == 1);

    operations[1].sem_num = 1;
    operations[1].sem_op = 1;
    CHECK_RESULT(sysv_sem_apply(owned_semid, operations, 2), MPMC_OK, 0);
    CHECK(semctl(owned_semid, 0, GETVAL) == 0);
    CHECK(semctl(owned_semid, 1, GETVAL) == 1);
}

/**
 * @brief Confere valores, índices, limites de conversão, P/V e espera não bloqueante por zero.
 */
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

    /** @brief Variantes do argumento de semctl(); buf é usada em IPC_STAT e IPC_SET. */
    union {
        int val;               /**< Variante de valor para SETVAL. */
        struct semid_ds *buf;  /**< Metadados consultados e atualizados no teste de permissões. */
        unsigned short *array; /**< Variante de vetor para GETALL ou SETALL. */
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
    /** @brief Correspondência esperada entre o errno simulado e a categoria do resultado. */
    const struct {
        int error;            /**< Código que o substituto de semop() atribui a errno. */
        mpmc_status_t status; /**< Categoria esperada do wrapper para esse código. */
    } cases[] = {
        {EIDRM, MPMC_ERR_SHUTDOWN},
        {EINVAL, MPMC_ERR_INVALID_ARG},
        {E2BIG, MPMC_ERR_INVALID_ARG},
        {EFAULT, MPMC_ERR_INVALID_ARG},
        {ERANGE, MPMC_ERR_INVALID_ARG},
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

    /* A própria chamada decide a precedência entre múltiplos argumentos inválidos. */
    injected_semop_errno = EACCES;
    CHECK_RESULT(sysv_sem_apply(-1, NULL, 0), MPMC_ERR_SYS, EACCES);
    injected_semop_errno = 0;
}

/**
 * @brief Executa os casos da suíte e apresenta o resultado das verificações.
 * @return Zero se nenhuma verificação falhar; um se houver falha ou erro de preparação.
 */
int main(void) {
    if (atexit(cleanup) != 0) return EXIT_FAILURE;

    test_arguments();

    mpmc_result_t result = sysv_sem_create(2, &owned_semid);
    CHECK(result.status == MPMC_OK);
    CHECK(result.system_errno == 0);
    if (result.status != MPMC_OK) return EXIT_FAILURE;

    test_values_and_indices();
    test_atomic_apply();
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
