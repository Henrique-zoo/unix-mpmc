/**
 * @file consumer.h
 * @brief Lógica de execução de processos consumidores.
 * @ingroup workers
 */
#ifndef MPMC_WORKERS_CONSUMER_H
#define MPMC_WORKERS_CONSUMER_H

#include <stddef.h>
#include <stdint.h>
#include "mpmc/ipc/transport.h"
#include "mpmc/status.h"

/** @ingroup workers
 * @{ */

/** @brief Política de espera pelo consumo solicitado. */
typedef enum {
    CONSUMER_BLOCKING = 1, /**< Aguarda até conseguir as n mensagens. */
    CONSUMER_NONBLOCKING = 2 /**< Não aguarda mensagens ausentes. */
} consumer_ack_mode_t;

/** @brief Configuração imutável de um processo consumidor. */
typedef struct {
    size_t producer_id;        /**< Estrutura de onde retirar, 1..p. */
    unsigned message_count;   /**< Quantidade solicitada, 1..1024. */
    consumer_ack_mode_t ack_mode; /**< Aguarda ou retorna com consumo parcial. */
    uint64_t operation_id;    /**< Identificador atribuído pelo supervisor. */
} consumer_job_t;

/** @brief Resultado observado em uma operação de consumo. */
typedef struct {
    unsigned consumed_count;  /**< Mensagens realmente retiradas; nunca mais que n. */
    mpmc_status_t status;     /**< MPMC_OK se concluiu n, ou erro/insuficiência. */
} consumer_result_t;

/**
 * @brief Consome até a quantidade solicitada de uma estrutura lógica.
 * @param transport Backend IPC inicializado e herdado no filho.
 * @param job Configuração validada da operação de consumo.
 * @param out Resultado preenchido inclusive em consumo parcial.
 * @retval MPMC_OK Exatamente job->message_count mensagens foram consumidas.
 * @retval MPMC_ERR_WOULD_BLOCK O consumidor não bloqueante encontrou
 *   menos mensagens que as solicitadas; out->consumed_count informa o total.
 * @retval MPMC_ERR_INTERRUPTED Cancelamento antes de completar a operação.
 * @retval MPMC_ERR_SYS Falha na retirada ou contabilização IPC.
 * @details Cada retirada chama ipc_receive() com blocking=true (ack=1)
 * ou false (ack=2). Confirmação ao produtor e atualização dos totais são
 * responsabilidade do backend, não devem ser duplicadas aqui.
 * @pre Chamada apenas pelo filho; job->producer_id pertence a 1..p.
 * @post Se algumas mensagens forem retiradas e ocorrer erro, não são
 * devolvidas artificialmente à estrutura.
 */
mpmc_status_t consumer_run(
    ipc_context_t *transport,
    const consumer_job_t *job,
    consumer_result_t *out
);

/** @} */
#endif
