/**
 * @file producer.h
 * @brief Lógica de execução de processos produtores.
 * @ingroup workers
 * @note producer_message_next() está implementado. producer_run() ainda não possui
 * implementação: sua documentação é o contrato previsto (pendente: #17).
 */
#ifndef MPMC_WORKERS_PRODUCER_H
#define MPMC_WORKERS_PRODUCER_H

#include <stddef.h>
#include <stdint.h>

#include "mpmc/error.h"
#include "mpmc/ipc/transport.h"

/**
 * @defgroup workers Processos produtores e consumidores
 * @brief Implementação dos trabalhos executados pelos processos filhos.
 * @details Não conhecem o paradigma de IPC e não gerenciam recursos globais.
 * @{
 */

/** @brief Comportamento da confirmação solicitado para o produtor. */
typedef enum {
    PRODUCER_ACK_WAIT = 1, /**< Aguarda todas as mensagens serem consumidas. */
    PRODUCER_ACK_NONE = 2  /**< Não aguarda consumo após enviar os dados. */
} producer_ack_mode_t;

/** @brief Dados imutáveis de uma operação de produção. */
typedef struct {
    size_t producer_id;     /**< Índice lógico da fila destino (1..p); 0 distribui em striped. */
    unsigned message_count; /**< Quantidade de mensagens, 1..1024. */
    producer_ack_mode_t ack_mode; /**< Com ou sem espera por confirmações. */
    uint64_t operation_id;        /**< Identificador único gerado no supervisor. */
} producer_job_t;

/** @brief Estado sequencial local a um único processo produtor. */
typedef struct {
    uint32_t last_sequence; /**< Última sequência emitida; inicia em zero. */
} producer_sequence_t;

/**
 * @brief Inicializa a próxima mensagem lógica do trabalho produtor.
 * @param[out] out Mensagem preenchida somente em sucesso.
 * @param[in,out] sequence Estado sequencial privado do processo produtor.
 * @param job Trabalho recebido do supervisor, com operation_id não nulo.
 * @retval MPMC_OK Metadados preenchidos com PID real, sequência e CLOCK_REALTIME.
 * @retval MPMC_ERR_INVALID_ARG out, sequence ou job nulo, operation_id zero
 * ou ack_mode diferente de PRODUCER_ACK_WAIT/PRODUCER_ACK_NONE.
 * @retval MPMC_ERR_STATE Sequência esgotada.
 * @retval MPMC_ERR_SYS Falha em clock_gettime().
 * @details Só avança a sequência se conseguir obter o timestamp. Cada processo
 * produtor deve manter seu próprio estado, inicializado em zero; assim, a
 * primeira mensagem é 1 e mensagens striped continuam na mesma sequência.
 * O modo de ACK é validado como parte do trabalho, mas não é gravado na mensagem.
 * Não envia a mensagem nem depende de um backend IPC.
 * @post Em falha, a mensagem e o estado sequencial permanecem inalterados.
 * @note Retorna diretamente mpmc_status_t. No resultado MPMC_ERR_SYS,
 * clock_gettime() deixou a causa em errno; este retorno não transporta system_errno.
 */
mpmc_status_t
producer_message_next(message_t *out, producer_sequence_t *sequence, const producer_job_t *job);

/**
 * @brief Produz as mensagens de um trabalho a partir do processo filho.
 * @param transport Contexto IPC herdado via fork(), pronto para envio.
 * @param job Descrição validada e imutável da operação.
 * @return Contrato previsto; producer_run() ainda não possui implementação (pendente: #17).
 * O resultado é mpmc_result_t e deve ter status MPMC_OK somente quando as `message_count`
 * mensagens foram enviadas e, com PRODUCER_ACK_WAIT, todas foram confirmadas. Nos demais
 * casos, é o primeiro erro da sequência abaixo, sem alteração:
 * - MPMC_ERR_INVALID_ARG: transport ou job nulo ou inválido; system_errno é zero.
 * - Qualquer status de ipc_send(), ipc_ack_begin(), ipc_ack_wait() ou ipc_ack_end(), com
 *   o system_errno original (ver transport.h para os valores de cada modo).
 * - O resultado de producer_message_next(), convertido a mpmc_result_t: em
 *   MPMC_ERR_SYS, producer_run() deve capturar errno imediatamente, pois aquele retorno
 *   não o transporta.
 * @details Para cada mensagem, chama producer_message_next() imediatamente antes de
 * ipc_send(), de modo que sender_pid=getpid(), a sequência (iniciando em 1) e sent_at
 * (CLOCK_REALTIME) descrevam o envio. Para producer_id=0, o destino da mensagem k é
 * `1 + ((k-1) % p)`, com p = ipc_producer_count(). Ordem das operações:
 * - PRODUCER_ACK_WAIT: ipc_ack_begin(operation_id, message_count) antes do primeiro
 *   envio; ipc_send() de cada mensagem; ipc_ack_wait() depois do último envio;
 *   ipc_ack_end() ao final, em sucesso, erro ou cancelamento, se ipc_ack_begin()
 *   tiver retornado MPMC_OK. Se ipc_ack_begin() falhar, nada é enviado.
 * - PRODUCER_ACK_NONE: somente os envios; nenhuma operação de ACK é chamada.
 * O primeiro erro interrompe o trabalho e não é substituído por falhas posteriores
 * de ipc_ack_end(); se o trabalho tiver êxito e ipc_ack_end() falhar, esse é o resultado.
 * MPMC_ERR_INTERRUPTED e MPMC_ERR_SHUTDOWN encerram o trabalho do mesmo modo. As
 * mensagens já inseridas não são retiradas.
 * @pre Deve ser chamada em um filho do supervisor, não no processo principal.
 * @post O resultado não é entregue por heap: o supervisor o conhece pelo estado de
 * término do filho (ver transport.h). producer_run() não chama ipc_worker_close() nem
 * ipc_destroy(); o ponto de entrada do filho chama ipc_worker_close() uma única vez,
 * qualquer que seja o resultado, e então encerra o processo.
 * @warning Envio pode bloquear por esgotamento de capacidade mesmo com ack=2.
 */
mpmc_result_t producer_run(ipc_context_t *transport, const producer_job_t *job);

/** @} */
#endif
