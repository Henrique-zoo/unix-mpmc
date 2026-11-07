/**
 * @file sem.c
 * @brief Implementação das primitivas de semáforos System V.
 * @ingroup sysv_primitives
 * @see sem.h para os contratos públicos.
 */
#include "mpmc/ipc/sem.h"

#include <errno.h>
#include <limits.h>
#include <sys/ipc.h>
#include <sys/sem.h>

/** @brief Argumentos utilizados por semctl(). */
union mpmc_semun {
    int val;               /**< Valor utilizado na operação SETVAL. */
    struct semid_ds *buf;  /**< Variante de argumento para consultas como IPC_STAT. */
    unsigned short *array; /**< Variante de argumento para GETALL ou SETALL. */
};

/**
 * @brief Traduz um errno já capturado, sem consultar a variável global errno.
 * @param error Código capturado na chamada de sistema que falhou.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_ERR_WOULD_BLOCK: error igual a EAGAIN.
 * - MPMC_ERR_INTERRUPTED: error igual a EINTR.
 * - MPMC_ERR_SHUTDOWN: error igual a EIDRM.
 * - MPMC_ERR_INVALID_INDEX: error igual a EFBIG.
 * - MPMC_ERR_INVALID_ARG: error igual a EINVAL.
 * - MPMC_ERR_SYS: outro código de erro.
 * Em todos os resultados, system_errno recebe error.
 */
static mpmc_result_t sem_error_result(int error) {
    mpmc_status_t status;

    switch (error) {
        case EAGAIN:
            status = MPMC_ERR_WOULD_BLOCK;
            break;
        case EINTR:
            status = MPMC_ERR_INTERRUPTED;
            break;
        case EIDRM:
            status = MPMC_ERR_SHUTDOWN;
            break;
        case EFBIG:
            status = MPMC_ERR_INVALID_INDEX;
            break;
        case EINVAL:
            status = MPMC_ERR_INVALID_ARG;
            break;
        default:
            status = MPMC_ERR_SYS;
            break;
    }

    mpmc_result_t resultado = {
        .status = status,
        .system_errno = error,
    };

    return resultado;
}

mpmc_result_t sysv_sem_create(unsigned count, int *out_semid) {
    if (count == 0 || count > (unsigned)INT_MAX || out_semid == NULL) {
        return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};
    }

    int identificador_sem = semget(IPC_PRIVATE, (int)count, 0600);
    if (identificador_sem == -1) return sem_error_result(errno);

    *out_semid = identificador_sem;

    return (mpmc_result_t){.status = MPMC_OK};
}

/**
 * @details O índice só é convertido após verificar sua representação em int.
 * EINVAL de SETVAL é ambíguo; não é adicionada uma consulta IPC_STAT para
 * distingui-lo, preservando as permissões exigidas pela operação original.
 */
mpmc_result_t sysv_sem_set(int semid, unsigned index, unsigned value) {
    if (semid < 0 || value > (unsigned)INT_MAX) {
        return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};
    }

    if (index > (unsigned)INT_MAX) return (mpmc_result_t){.status = MPMC_ERR_INVALID_INDEX};

    union mpmc_semun argumento = {.val = (int)value};

    if (semctl(semid, (int)index, SETVAL, argumento) == -1) {
        int erro = errno;

        if (erro == ERANGE) {
            return (mpmc_result_t){
                .status = MPMC_ERR_INVALID_ARG,
                .system_errno = erro,
            };
        }

        return sem_error_result(erro);
    }

    return (mpmc_result_t){.status = MPMC_OK};
}

/** @details Valida o estreitamento do índice antes de montar struct sembuf. */
mpmc_result_t sysv_sem_change(int semid, unsigned index, short delta, bool nowait) {
    if (semid < 0) return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};
    if (index > USHRT_MAX) return (mpmc_result_t){.status = MPMC_ERR_INVALID_INDEX};

    struct sembuf operacao = {
        .sem_num = (unsigned short)index,
        .sem_op = delta,
        .sem_flg = nowait ? IPC_NOWAIT : 0,
    };

    if (semop(semid, &operacao, 1) == -1) return sem_error_result(errno);

    return (mpmc_result_t){.status = MPMC_OK};
}

mpmc_result_t sysv_sem_remove(int semid) {
    if (semid < 0) return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};

    if (semctl(semid, 0, IPC_RMID) == -1) return sem_error_result(errno);

    return (mpmc_result_t){.status = MPMC_OK};
}
