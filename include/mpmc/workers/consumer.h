/**
 * @file consumer.h
 * @brief Lógica de execução de processos consumidores.
 * @ingroup workers
 * @note O worker usa exclusivamente a fachada ipc_* e não gerencia o ciclo de vida
 * do contexto herdado.
 */
#ifndef MPMC_WORKERS_CONSUMER_H
#define MPMC_WORKERS_CONSUMER_H

#include <stddef.h>
#include <stdint.h>

#include "mpmc/error.h"
#include "mpmc/ipc/transport.h"

/** @ingroup workers
 * @{ */

/** @brief Política de espera pelo consumo solicitado. */
typedef enum {
    CONSUMER_BLOCKING = 1,   /**< Aguarda até conseguir as n mensagens. */
    CONSUMER_NONBLOCKING = 2 /**< Não aguarda mensagens ausentes. */
} consumer_ack_mode_t;

/** @brief Configuração imutável de um processo consumidor. */
typedef struct {
    size_t producer_id;     /**< Índice lógico da estrutura de onde retirar, 1..p; não é PID. */
    unsigned message_count; /**< Quantidade solicitada, 1..1024. */
    consumer_ack_mode_t ack_mode; /**< Aguarda ou retorna com consumo parcial. */
    uint64_t
        operation_id; /**< ID deste trabalho consumidor; não altera o operation_id das mensagens. */
} consumer_job_t;

/** @brief Resultado observado em uma operação de consumo. */
typedef struct {
    unsigned consumed_count; /**< Mensagens realmente retiradas; nunca mais que n. */
    mpmc_result_t error;     /**< Resultado completo, inclusive causa de sistema se houver. */
} consumer_result_t;

/**
 * @brief Consome até a quantidade solicitada de uma estrutura lógica.
 * @param transport Backend IPC inicializado e herdado no filho.
 * @param job Configuração validada da operação de consumo.
 * @param out Resultado preenchido inclusive em consumo parcial.
 * @return Resultado igual a out->error.
 * O valor retornado é igual a out->error, e out->consumed_count nunca excede `message_count`:
 * - MPMC_OK: as `message_count` mensagens foram retiradas.
 * - MPMC_ERR_INVALID_ARG: transport, job ou out nulo, ou job inválido. Com out não nulo,
 *   ele é preenchido com consumed_count zero; system_errno é zero.
 * - MPMC_ERR_WOULD_BLOCK: somente com CONSUMER_NONBLOCKING; faltaram mensagens e
 *   consumed_count (menor que `message_count`) informa quantas foram retiradas. O
 *   system_errno é o devolvido por ipc_receive().
 * - Qualquer outro status de ipc_receive(), inclusive MPMC_ERR_INTERRUPTED, com o
 *   system_errno original (ver transport.h para os valores de cada modo).
 * @details Cada retirada chama ipc_receive() com blocking=true (ack=1) ou false (ack=2) e
 * só entra em consumed_count se retornar MPMC_OK. Uma retirada que falhou depois de a
 * mensagem sair da estrutura (MPMC_ERR_STATE ou MPMC_ERR_SYS) não é contada. Em
 * CONSUMER_BLOCKING, o laço continua até completar as `message_count` mensagens ou
 * ocorrer erro; em CONSUMER_NONBLOCKING, para na primeira estrutura vazia. O primeiro erro
 * encerra o consumo.
 * @par Quem registra consumo e ACK
 * O backend, dentro de ipc_receive(), conta o consumo e confirma ao produtor quando a
 * operação produtora pediu ACK. consumer_run() não confirma, não conta por conta própria
 * e não consulta a solicitação de ACK, que não existe em message_t. O operation_id do
 * job identifica este trabalho consumidor e nunca altera o das mensagens retiradas.
 * @par Resultado para o supervisor
 * out vive no heap do filho. O supervisor sabe o desfecho pelo estado de término do filho
 * (código zero apenas com MPMC_OK) e o total por estrutura por ipc_poll_events() e
 * ipc_consumed_count(); a quantidade por trabalho é apresentada pelo próprio filho
 * (ver transport.h).
 * @pre Chamada apenas pelo filho; job->producer_id pertence a 1..p.
 * @post Se algumas mensagens forem retiradas e ocorrer erro, não são
 * devolvidas artificialmente à estrutura. consumer_run() não chama ipc_worker_close()
 * nem ipc_destroy(); o ponto de entrada do filho chama ipc_worker_close() uma única vez,
 * qualquer que seja o resultado, e então encerra o processo.
 */
mpmc_result_t consumer_run(
    ipc_context_t *transport,
    const consumer_job_t *job,
    consumer_result_t *out
);

/** @} */
#endif
