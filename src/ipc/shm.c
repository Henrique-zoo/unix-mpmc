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

#include "mpmc/diagnostic.h"

static mpmc_result_t shm_error_result(int error);

/**
 * @details shmget() sinaliza falha exclusivamente com -1; o identificador zero
 * também é aceito como resultado válido.
 */
mpmc_result_t sysv_shm_create(size_t size, int *out_shmid) {
    mpmc_diagnostic_scope_t diagnostic_scope;
    mpmc_diagnostic_begin(&diagnostic_scope, __func__, false, true);

    if (out_shmid == nullptr)
        return mpmc_diagnostic_end(
            &diagnostic_scope, (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG}
        );
    int shmid = shmget(IPC_PRIVATE, size, 0600);

    if (shmid == -1) return mpmc_diagnostic_end(&diagnostic_scope, shm_error_result(errno));

    *out_shmid = shmid;

    return mpmc_diagnostic_end(&diagnostic_scope, (mpmc_result_t){.status = MPMC_OK});
}

/**
 * @details O teste de falha de shmat() compara o endereço com `(void *) -1`;
 * nullptr não é o sentinela de erro dessa chamada.
 */
mpmc_result_t sysv_shm_attach(int shmid, void **out_addr) {
    mpmc_diagnostic_scope_t diagnostic_scope;
    mpmc_diagnostic_begin(&diagnostic_scope, __func__, false, true);
    diagnostic_scope.resource_id = shmid;

    if (out_addr == nullptr)
        return mpmc_diagnostic_end(
            &diagnostic_scope, (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG}
        );
    void *address = shmat(shmid, nullptr, 0);

    if (address == (void *)-1)
        return mpmc_diagnostic_end(&diagnostic_scope, shm_error_result(errno));

    *out_addr = address;

    return mpmc_diagnostic_end(&diagnostic_scope, (mpmc_result_t){.status = MPMC_OK});
}

/**
 * @details O endereço é passado diretamente a shmdt(), sem desreferenciá-lo.
 * A chamada é executada uma única vez e seu errno é capturado antes da tradução.
 */
mpmc_result_t sysv_shm_detach(const void *address) {
    mpmc_diagnostic_scope_t diagnostic_scope;
    mpmc_diagnostic_begin(&diagnostic_scope, __func__, false, true);

    if (shmdt(address) == -1)
        return mpmc_diagnostic_end(&diagnostic_scope, shm_error_result(errno));

    return mpmc_diagnostic_end(&diagnostic_scope, (mpmc_result_t){.status = MPMC_OK});
}

/**
 * @details O terceiro argumento de shmctl() é nullptr porque IPC_RMID dispensa um
 * buffer struct shmid_ds. O identificador é validado pela própria chamada.
 */
mpmc_result_t sysv_shm_remove(int shmid) {
    mpmc_diagnostic_scope_t diagnostic_scope;
    mpmc_diagnostic_begin(&diagnostic_scope, __func__, false, true);
    diagnostic_scope.resource_id = shmid;

    if (shmctl(shmid, IPC_RMID, nullptr) == -1)
        return mpmc_diagnostic_end(&diagnostic_scope, shm_error_result(errno));

    return mpmc_diagnostic_end(&diagnostic_scope, (mpmc_result_t){.status = MPMC_OK});
}

/**
 * @details Passa o buffer do chamador diretamente a shmctl(IPC_STAT), sem
 * alocação de memória temporária. O errno é copiado antes de qualquer outra chamada.
 */
mpmc_result_t sysv_shm_stat(int shmid, struct shmid_ds *out_info) {
    mpmc_diagnostic_scope_t diagnostic_scope;
    mpmc_diagnostic_begin(&diagnostic_scope, __func__, false, true);
    diagnostic_scope.resource_id = shmid;

    if (shmctl(shmid, IPC_STAT, out_info) == -1)
        return mpmc_diagnostic_end(&diagnostic_scope, shm_error_result(errno));

    return mpmc_diagnostic_end(&diagnostic_scope, (mpmc_result_t){.status = MPMC_OK});
}

/**
 * @brief Classifica uma falha das primitivas SHM sem perder sua causa original.
 * @param error errno capturado imediatamente após a chamada que falhou.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_ERR_INVALID_ARG: error igual a EINVAL ou EFAULT.
 * - MPMC_ERR_SHUTDOWN: error igual a EIDRM.
 * - MPMC_ERR_SYS: outra causa de sistema.
 * Em todos os resultados, system_errno recebe error.
 */
static mpmc_result_t shm_error_result(int error) {
    mpmc_status_t status;

    switch (error) {
        case EINVAL:
        case EFAULT:
            status = MPMC_ERR_INVALID_ARG;
            break;
        case EIDRM:
            status = MPMC_ERR_SHUTDOWN;
            break;
        default:
            status = MPMC_ERR_SYS;
            break;
    }

    return (mpmc_result_t){
        .status = status,
        .system_errno = error,
    };
}
