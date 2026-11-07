/**
 * @file sem.c
 * @brief Implementação das operações de semáforos System V.
 * @ingroup sysv_primitives
 */

#include "mpmc/ipc/sem.h"

#include <errno.h>
#include <limits.h>
#include <sys/ipc.h>
#include <sys/sem.h>

/**
 * @brief Argumentos utilizados nas operações semctl().
 */
union mpmc_semun {
    int val;
    struct semid_ds *buf;
    unsigned short *array;
};

/**
 * @brief Retorna código de erro correspondente ao errno informado, definido no switch.
 * @param numero_erro Codigo do errno.
 * @return Status e o errno original
 */
static mpmc_result_t sem_error_result(int numero_erro) {
    mpmc_status_t estado = MPMC_ERR_SYS;

    switch (numero_erro) {
        case EAGAIN:
            estado = MPMC_ERR_WOULD_BLOCK;
            break;

        case EINTR:
            estado = MPMC_ERR_INTERRUPTED;
            break;

        case EIDRM:
            estado = MPMC_ERR_SHUTDOWN;
            break;

        case EFBIG:
            estado = MPMC_ERR_INVALID_INDEX;
            break;

        case EINVAL:
            estado = MPMC_ERR_INVALID_ARG;
            break;

        default:
            estado = MPMC_ERR_SYS;
            break;
    }

    mpmc_result_t resultado = {
        .status = estado,
        .system_errno = numero_erro,
    };

    return resultado;
}

/**
 * @brief Confere se o indice pertence ao conjunto de semáforos (IPC_STAT).
 * @param semid Identificador do conjunto.
 * @param index Índice do semáforo dentro do conjunto começando em 0.
 * @return MPMC_OK em caso de sucesso, MPMC_ERR_INVALID_ARG para argumentos
 * inválidos ou o erro correspondente.
 */
static mpmc_result_t sem_validate_index(int semid, unsigned index) {
    struct semid_ds informacoes;
    union mpmc_semun argumento;
    int resultado_consulta;
    int numero_erro;

    if (semid < 0) {
        return (mpmc_result_t){
            .status = MPMC_ERR_INVALID_ARG,
        };
    }

    if (index > USHRT_MAX) {
        return (mpmc_result_t){
            .status = MPMC_ERR_INVALID_INDEX,
        };
    }

    argumento.buf = &informacoes;
    resultado_consulta = semctl(semid, 0, IPC_STAT, argumento);

    if (resultado_consulta == -1) {
        numero_erro = errno;
        return sem_error_result(numero_erro);
    }

    if ((unsigned long)index >= informacoes.sem_nsems) {
        return (mpmc_result_t){
            .status = MPMC_ERR_INVALID_INDEX,
        };
    }

    return (mpmc_result_t){
        .status = MPMC_OK,
    };
}

mpmc_result_t sysv_sem_create(unsigned count, int *out_semid) {
    int identificador_sem;
    int numero_erro;

    if (count == 0 || count > (unsigned)INT_MAX || out_semid == NULL) {
        return (mpmc_result_t){
            .status = MPMC_ERR_INVALID_ARG,
        };
    }

    identificador_sem = semget(IPC_PRIVATE, (int)count, IPC_CREAT | 0600);

    if (identificador_sem == -1) {
        numero_erro = errno;
        return sem_error_result(numero_erro);
    }

    *out_semid = identificador_sem;

    return (mpmc_result_t){
        .status = MPMC_OK,
    };
}

mpmc_result_t sysv_sem_set(int semid, unsigned index, unsigned value) {
    mpmc_result_t resultado_validacao;
    union mpmc_semun argumento;
    int numero_erro;
    int resultado_atribuicao;

    if (value > (unsigned)INT_MAX) {
        return (mpmc_result_t){
            .status = MPMC_ERR_INVALID_ARG,
        };
    }

    resultado_validacao = sem_validate_index(semid, index);

    if (resultado_validacao.status != MPMC_OK) {
        return resultado_validacao;
    }

    argumento.val = (int)value;

    resultado_atribuicao = semctl(semid, (int)index, SETVAL, argumento);

    if (resultado_atribuicao == -1) {
        numero_erro = errno;

        if (numero_erro == ERANGE) {
            return (mpmc_result_t){
                .status = MPMC_ERR_INVALID_ARG,
                .system_errno = numero_erro,
            };
        }

        return sem_error_result(numero_erro);
    }

    return (mpmc_result_t){
        .status = MPMC_OK,
    };
}

mpmc_result_t sysv_sem_change(int semid, unsigned index, short delta, bool nowait) {
    mpmc_result_t resultado_validacao;
    struct sembuf operacao;
    int resultado_operacao;
    int numero_erro;

    operacao.sem_num = (unsigned short)index;
    operacao.sem_op = delta;

    if (nowait) {
        operacao.sem_flg = IPC_NOWAIT;
    } else {
        operacao.sem_flg = 0;
    }

    resultado_validacao = sem_validate_index(semid, index);

    if (resultado_validacao.status != MPMC_OK) {
        return resultado_validacao;
    }

    resultado_operacao = semop(semid, &operacao, 1);

    if (resultado_operacao == -1) {
        numero_erro = errno;
        return sem_error_result(numero_erro);
    }

    return (mpmc_result_t){
        .status = MPMC_OK,
    };
}

mpmc_result_t sysv_sem_remove(int semid) {
    int resultado_remocao;
    int numero_erro;

    if (semid < 0) {
        return (mpmc_result_t){
            .status = MPMC_ERR_INVALID_ARG,
        };
    }

    resultado_remocao = semctl(semid, 0, IPC_RMID);

    if (resultado_remocao == -1) {
        numero_erro = errno;
        return sem_error_result(numero_erro);
    }

    return (mpmc_result_t){
        .status = MPMC_OK,
    };
}
