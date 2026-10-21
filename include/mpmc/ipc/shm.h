/**
 * @file shm.h
 * @brief Wrappers mínimos sobre memória compartilhada System V.
 * @ingroup sysv_primitives
 */
#ifndef MPMC_IPC_SHM_H
#define MPMC_IPC_SHM_H

#include <stddef.h>
#include "mpmc/status.h"

/**
 * @defgroup sysv_primitives Primitivas System V
 * @ingroup ipc
 * @brief Operações de baixo nível e tradução de erros de chamadas de sistema.
 * @{ */

/**
 * @brief Cria segmento System V privado com permissões de usuário.
 * @param size Número exato de bytes solicitados em shmget().
 * @param out_shmid Recebe ID retornado pelo kernel em sucesso.
 * @retval MPMC_OK Segmento criado.
 * @retval MPMC_ERR_INVALID_ARG Tamanho nulo ou ponteiro inválido.
 * @retval MPMC_ERR_SYS Falha de shmget(); consultar errno.
 * @note O backend da atividade deve pedir 2048 bytes por segmento de dados.
 * @warning Quem cria deve providenciar remoção via sysv_shm_remove().
 */
mpmc_status_t sysv_shm_create(size_t size, int *out_shmid);

/**
 * @brief Anexa um segmento ao espaço virtual do processo atual.
 * @param shmid Identificador obtido com shmget().
 * @param out_addr Recebe endereço retornado por shmat() em sucesso.
 * @retval MPMC_OK Associação estabelecida.
 * @retval MPMC_ERR_SYS Falha em shmat(); valor `(void *) -1` é erro.
 * @note Cada processo pode ter endereço virtual diferente para o mesmo ID.
 */
mpmc_status_t sysv_shm_attach(int shmid, void **out_addr);

/**
 * @brief Desanexa mapeamento do processo corrente.
 * @param address Endereço devolvido por shmat().
 * @retval MPMC_OK Desanexado com sucesso.
 * @retval MPMC_ERR_SYS Falha de shmdt().
 * @note Desanexar não equivale a remover o segmento do sistema.
 */
mpmc_status_t sysv_shm_detach(const void *address);

/**
 * @brief Marca segmento global para remoção usando shmctl(IPC_RMID).
 * @param shmid Identificador de segmento que pertence ao supervisor.
 * @retval MPMC_OK Remoção solicitada.
 * @retval MPMC_ERR_SYS Falha de shmctl().
 * @warning Somente o supervisor deve solicitar remoção do objeto global.
 */
mpmc_status_t sysv_shm_remove(int shmid);

/** @} */
#endif
