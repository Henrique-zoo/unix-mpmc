/**
 * @file sem.h
 * @brief Wrappers mínimos sobre conjuntos de semáforos System V.
 * @ingroup sysv_primitives
 */
#ifndef MPMC_IPC_SEM_H
#define MPMC_IPC_SEM_H

#include <stdbool.h>

#include "mpmc/status.h"

/** @ingroup sysv_primitives
 * @{ */

/**
 * @brief Cria um conjunto privado de semáforos System V.
 * @param count Número positivo de semáforos no conjunto.
 * @param out_semid Recebe o identificador retornado por semget().
 * @retval MPMC_OK Conjunto criado.
 * @retval MPMC_ERR_INVALID_ARG count nulo ou ponteiro inválido.
 * @retval MPMC_ERR_SYS Falha de semget().
 * @note Valores iniciais devem ser explicitamente estabelecidos com semctl().
 */
mpmc_status_t sysv_sem_create(unsigned count, int *out_semid);

/**
 * @brief Atribui valor inicial a um semáforo de um conjunto.
 * @param semid Identificador do conjunto.
 * @param index Índice do semáforo dentro do conjunto, começando em 0.
 * @param value Valor inicial a passar para semctl(SETVAL).
 * @retval MPMC_OK Valor alterado.
 * @retval MPMC_ERR_SYS Falha de semctl() (inclusive faixa inválida).
 * @note Variantes de união `semun` dependem de plataforma; wrapper as encapsula.
 */
mpmc_status_t sysv_sem_set(int semid, unsigned index, unsigned value);

/**
 * @brief Efetua operação P/V sobre um semáforo usando semop().
 * @param semid Identificador do conjunto de semáforos.
 * @param index Índice no conjunto.
 * @param delta Negativo para P/reserva, positivo para V/liberação e zero
 * para esperar valor zero; não confundir com atribuição direta.
 * @param nowait Se true, usa IPC_NOWAIT para não bloquear.
 * @retval MPMC_OK Operação aplicada.
 * @retval MPMC_ERR_WOULD_BLOCK Operação não poderia prosseguir com nowait=true.
 * @retval MPMC_ERR_INTERRUPTED semop() interrompida por sinal (EINTR).
 * @retval MPMC_ERR_SYS Outro erro do kernel.
 * @note Não repete EINTR automaticamente: o chamador decide se está encerrando.
 * @warning Não aplicar SEM_UNDO indiscriminadamente em semáforos de contagem
 * dos itens/vagas de um produtor-consumidor; a recuperação exige protocolo.
 */
mpmc_status_t sysv_sem_change(int semid, unsigned index, short delta, bool nowait);

/**
 * @brief Remove um conjunto de semáforos com semctl(IPC_RMID).
 * @param semid Identificador do conjunto pertencente ao supervisor.
 * @retval MPMC_OK Conjunto removido.
 * @retval MPMC_ERR_SYS Falha do kernel.
 * @note Uma remoção pode interromper processos bloqueados em semop().
 */
mpmc_status_t sysv_sem_remove(int semid);

/** @} */
#endif
