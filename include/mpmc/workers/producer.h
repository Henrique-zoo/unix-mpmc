/**
 * @file producer.h
 * @brief Lógica de execução de processos produtores.
 * @ingroup workers
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
 * @param out Mensagem a preencher.
 * @param sequence Estado sequencial privado do processo produtor.
 * @param job Trabalho recebido do supervisor, com operation_id não nulo.
 * @retval MPMC_OK Metadados preenchidos com PID real, sequência e CLOCK_REALTIME.
 * @retval MPMC_ERR_INVALID_ARG Entrada inválida ou operation_id nulo.
 * @retval MPMC_ERR_STATE Sequência esgotada.
 * @retval MPMC_ERR_SYS Falha em clock_gettime().
 * @details Só avança a sequência se conseguir obter o timestamp. Cada processo
 * produtor deve manter seu próprio estado, inicializado em zero; assim, a
 * primeira mensagem é 1 e mensagens striped continuam na mesma sequência.
 * Não envia a mensagem nem depende de um backend IPC.
 */
mpmc_status_t
producer_message_next(message_t *out, producer_sequence_t *sequence, const producer_job_t *job);

/**
 * @brief Produz as mensagens de um trabalho a partir do processo filho.
 * @param transport Contexto IPC herdado via fork(), pronto para envio.
 * @param job Descrição validada e imutável da operação.
 * @return Estrutura mpmc_result_t. A função ainda não possui implementação;
 * os valores possíveis de status e system_errno não podem ser verificados.
 * @details Gera sender_pid=getpid(), sequência iniciando em 1 e sent_at
 * do CLOCK_REALTIME. Usa ipc_send() para o destino selecionado; para
 * producer_id=0, alterna destinos `1 + ((seq-1) % p)`. Na opção ack=1,
 * executa begin/wait/end; na opção ack=2, não solicita confirmação.
 * @pre Deve ser chamada em um filho do supervisor, não no processo principal.
 * @post O worker deve liberar recursos locais usando ipc_worker_close() antes
 * de finalizar o processo; o supervisor mantém posse dos recursos globais.
 * @warning Envio pode bloquear por esgotamento de capacidade mesmo com ack=2.
 */
mpmc_result_t producer_run(ipc_context_t *transport, const producer_job_t *job);

/** @} */
#endif
