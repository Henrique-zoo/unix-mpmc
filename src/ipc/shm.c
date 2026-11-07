/**
 * @file shm.c
 * @brief Implementação das primitivas de memória compartilhada System V.
 * @ingroup sysv_primitives
 *
 * @see shm.h para os contratos públicos das funções.
 */

#include "mpmc/ipc/shm.h"

#include <errno.h>
#include <sys/ipc.h>
#include <sys/shm.h>

/**
 * @details shmget() sinaliza falha exclusivamente com -1; o identificador zero
 * também é aceito como resultado válido.
 */
mpmc_result_t sysv_shm_create(size_t size, int *out_shmid) {
    if (size == 0 || out_shmid == NULL) return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};
    int shmid = shmget(IPC_PRIVATE, size, 0600);

    if (shmid == -1) {
        return (mpmc_result_t){
            .status = MPMC_ERR_SYS,
            .system_errno = errno,
        };
    }

    *out_shmid = shmid;

    return (mpmc_result_t){.status = MPMC_OK};
}

/**
 * @details O teste de falha de shmat() compara o endereço com `(void *) -1`;
 * NULL não é o sentinela de erro dessa chamada.
 */
mpmc_result_t sysv_shm_attach(int shmid, void **out_addr) {
    if (shmid < 0 || out_addr == NULL) return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};
    void *address = shmat(shmid, NULL, 0);

    if (address == (void *)-1) {
        return (mpmc_result_t){
            .status = MPMC_ERR_SYS,
            .system_errno = errno,
        };
    }

    *out_addr = address;

    return (mpmc_result_t){.status = MPMC_OK};
}

/**
 * @details shmdt() é executada uma única vez. Em falha, errno é lido diretamente
 * no inicializador do resultado, sem chamadas intermediárias.
 */
mpmc_result_t sysv_shm_detach(const void *address) {
    if (address == NULL) return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};

    if (shmdt(address) == -1) {
        return (mpmc_result_t){
            .status = MPMC_ERR_SYS,
            .system_errno = errno,
        };
    }

    return (mpmc_result_t){.status = MPMC_OK};
}

/**
 * @details O terceiro argumento de shmctl() é NULL porque IPC_RMID dispensa um
 * buffer struct shmid_ds. Em falha, errno é copiado diretamente para o resultado.
 */
mpmc_result_t sysv_shm_remove(int shmid) {
    if (shmid < 0) return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};

    if (shmctl(shmid, IPC_RMID, NULL) == -1) {
        return (mpmc_result_t){
            .status = MPMC_ERR_SYS,
            .system_errno = errno,
        };
    }

    return (mpmc_result_t){.status = MPMC_OK};
}

/**
 * @details Passa o buffer do chamador diretamente a shmctl(IPC_STAT), sem
 * alocação de memória temporária. O errno é copiado antes de qualquer outra chamada.
 */
mpmc_result_t sysv_shm_stat(int shmid, struct shmid_ds *out_info) {
    if (shmid < 0 || out_info == NULL) return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};

    if (shmctl(shmid, IPC_STAT, out_info) == -1) {
        return (mpmc_result_t){
            .status = MPMC_ERR_SYS,
            .system_errno = errno,
        };
    }

    return (mpmc_result_t){.status = MPMC_OK};
}
