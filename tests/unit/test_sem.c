
/**
 * @file test_sem.c
 * @brief Verifica as operações de semáforos System V.
 */

#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "mpmc/ipc/sem.h"

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

/**
 * @brief Tratador utilizado para interromper uma operação bloqueante.
 */
static void tratar_alarme(int sinal) {
    (void)sinal;
}

int main(void) {
    int semid = -1;
    mpmc_result_t resultado;

    /* Validação dos argumentos de criação. */
    resultado = sysv_sem_create(0, &semid);
    CHECK(resultado.status == MPMC_ERR_INVALID_ARG);
    CHECK(resultado.system_errno == 0);

    resultado = sysv_sem_create(2, NULL);
    CHECK(resultado.status == MPMC_ERR_INVALID_ARG);

    /* Criação do conjunto. */
    resultado = sysv_sem_create(2, &semid);
    CHECK(resultado.status == MPMC_OK);
    CHECK(resultado.system_errno == 0);

    if (resultado.status != MPMC_OK) {
        printf("%u verificações, %u falhas\n", checks, failures + 1);
        return 1;
    }

    /* Inicialização dos semáforos. */
    resultado = sysv_sem_set(semid, 0, 1);
    CHECK(resultado.status == MPMC_OK);

    resultado = sysv_sem_set(semid, 1, 0);
    CHECK(resultado.status == MPMC_OK);

    /* Validação de índice inexistente. */
    resultado = sysv_sem_set(semid, 2, 1);
    CHECK(resultado.status == MPMC_ERR_INVALID_INDEX);

    /* Operação P com valor disponível. */
    resultado = sysv_sem_change(semid, 0, -1, false);
    CHECK(resultado.status == MPMC_OK);

    /* Operação P sem valor disponível e sem bloqueio. */
    resultado = sysv_sem_change(semid, 0, -1, true);
    CHECK(resultado.status == MPMC_ERR_WOULD_BLOCK);
    CHECK(resultado.system_errno == EAGAIN);

    /* Espera pelo valor zero. */
    resultado = sysv_sem_change(semid, 1, 0, true);
    CHECK(resultado.status == MPMC_OK);

    /* Operação V. */
    resultado = sysv_sem_change(semid, 1, 1, false);
    CHECK(resultado.status == MPMC_OK);

    /* Espera pelo valor zero quando o semáforo vale um. */
    resultado = sysv_sem_change(semid, 1, 0, true);
    CHECK(resultado.status == MPMC_ERR_WOULD_BLOCK);
    CHECK(resultado.system_errno == EAGAIN);

    /* Interrupção de uma operação bloqueante. */
    struct sigaction acao = {0};
    struct sigaction acao_anterior = {0};

    acao.sa_handler = tratar_alarme;
    sigemptyset(&acao.sa_mask);
    acao.sa_flags = 0;

    if (sigaction(SIGALRM, &acao, &acao_anterior) == -1) {
        CHECK(false);
    } else {
        alarm(1);

        resultado = sysv_sem_change(semid, 0, -1, false);

        alarm(0);

        CHECK(resultado.status == MPMC_ERR_INTERRUPTED);
        CHECK(resultado.system_errno == EINTR);

        CHECK(sigaction(SIGALRM, &acao_anterior, NULL) == 0);
    }

    /* Remoção do conjunto. */
    resultado = sysv_sem_remove(semid);
    CHECK(resultado.status == MPMC_OK);
    CHECK(resultado.system_errno == 0);

    printf("%u verificações de semáforos, %u falhas\n", checks, failures);

    return failures == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
