/**
 * @file sem.h
 * @brief Wrappers mínimos sobre conjuntos de semáforos System V.
 * @ingroup sysv_primitives
 */
#ifndef MPMC_IPC_SEM_H
#define MPMC_IPC_SEM_H

#include <stdbool.h>

#include "mpmc/error.h"

/** @ingroup sysv_primitives
 * @{ */

/**
 * @brief Cria um conjunto privado de semáforos System V.
 * @param count Número positivo de semáforos no conjunto.
 * @param out_semid Recebe o identificador retornado por semget().
 * @return MPMC_OK em caso de sucesso, MPMC_ERR_INVALID_ARG para argumentos
 * inválidos ou o erro correspondente de semget().
 * @note Valores iniciais devem ser explicitamente estabelecidos com semctl().
 */
mpmc_result_t sysv_sem_create(unsigned count, int *out_semid);

/**
 * @brief Atribui valor inicial a um semáforo de um conjunto.
 * @param semid Identificador do conjunto.
 * @param index Índice do semáforo dentro do conjunto, começando em 0.
 * @param value Valor inicial a passar para semctl(SETVAL).
 * @return MPMC_OK em caso de sucesso, MPMC_ERR_INVALID_ARG para argumentos
 * inválidos, MPMC_ERR_INVALID_INDEX para índice inexistente ou o erro correspondente.
 * @note Variantes de união `semun` dependem de plataforma; wrapper as encapsula.
 */
mpmc_result_t sysv_sem_set(int semid, unsigned index, unsigned value);

/**
 * @brief Efetua operação P/V sobre um semáforo usando semop().
 * @param semid Identificador do conjunto de semáforos.
 * @param index Índice no conjunto.
 * @param delta Negativo para P/reserva, positivo para V/liberação e zero
 * para esperar valor zero; não confundir com atribuição direta.
 * @param nowait Se true, usa IPC_NOWAIT para não bloquear.
 * @return MPMC_OK em caso de sucesso, MPMC_ERR_INVALID_ARG para identificador
 * inválido, MPMC_ERR_INVALID_INDEX para índice inexistente,
 * MPMC_ERR_WOULD_BLOCK quando a operação não pode prosseguir sem bloqueio,
 * MPMC_ERR_INTERRUPTED quando interrompida por sinal, MPMC_ERR_SHUTDOWN
 * quando o conjunto é removido ou MPMC_ERR_SYS para outras falhas.
 * @note Não repete EINTR automaticamente: o chamador decide se está encerrando.
 * @warning Não aplicar SEM_UNDO indiscriminadamente em semáforos de contagem
 * dos itens/vagas de um produtor-consumidor; a recuperação exige protocolo.
 */
mpmc_result_t sysv_sem_change(int semid, unsigned index, short delta, bool nowait);

/**
 * @brief Remove um conjunto de semáforos com semctl(IPC_RMID).
 * @param semid Identificador do conjunto pertencente ao supervisor.
 * @return MPMC_OK em caso de sucesso, MPMC_ERR_INVALID_ARG para identificador
 * inválido, MPMC_ERR_SHUTDOWN se o conjunto já foi removido (quando indicado
 * pelo sistema) ou MPMC_ERR_SYS para outras falhas de semctl().
 * @note Uma remoção pode interromper processos bloqueados em semop().
 */
mpmc_result_t sysv_sem_remove(int semid);

/** @} */
#endif
