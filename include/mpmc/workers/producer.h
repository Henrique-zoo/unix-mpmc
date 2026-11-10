/**
 * @file producer.h
 * @brief Lógica de execução de processos produtores.
 * @ingroup workers
 * @details Recebe um trabalho do ponto de entrada do filho no supervisor,
 * constrói metadados e roteia envios pela fachada transport.h. Gerencia o
 * registro/espera/finalização de ACK do trabalho quando solicitado. Devolve
 * progresso e resultado ao chamador, que fecha o contexto local e os envia ao pai.
 * @see @ref architecture para o papel do worker e os canais de retorno.
 * @see @ref ipc_protocol para identidades, striped e confirmação de consumo.
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
mpmc_status_t producer_message_next(
    message_t *out,
    producer_sequence_t *sequence,
    const producer_job_t *job
);

/** @brief Progresso e resultado de um trabalho produtor. */
typedef struct {
    unsigned produced_count; /**< Envios concluídos antes do retorno. */
    mpmc_result_t error;     /**< Resultado completo, inclusive da espera por ACK. */
} producer_result_t;

/**
 * @brief Executa produção e informa progresso inclusive em erro ou cancelamento.
 * @param transport Contexto IPC herdado e inicializado.
 * @param job Trabalho validado, com identidade única.
 * @param out Resultado obrigatório; preenchido inclusive em falha.
 * @return Estrutura mpmc_result_t com os mesmos estados de producer_run().
 * - MPMC_ERR_INVALID_ARG: out nulo, com system_errno zero.
 * @details produced_count aumenta somente após ipc_send() retornar sucesso.
 * O total pode estar completo mesmo se a espera de consumo falhar. O resultado
 * não transforma esse caso em conclusão bem-sucedida.
 */
mpmc_result_t producer_run_with_result(
    ipc_context_t *transport,
    const producer_job_t *job,
    producer_result_t *out
);

/**
 * @brief Produz as mensagens de um trabalho a partir do processo filho.
 * @param transport Contexto IPC herdado via fork(), pronto para envio.
 * @param job Descrição validada e imutável da operação.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: todas as mensagens enviadas e, se solicitado, consumidas;
 *   system_errno é zero.
 * - MPMC_ERR_INVALID_ARG: transporte/job inválido, quantidade fora de 1..1024,
 *   identidade ou modo de ACK inválido, com system_errno zero; argumentos
 *   rejeitados pelo kernel nas operações IPC preservam o errno original.
 * - MPMC_ERR_INVALID_INDEX: destino acima da quantidade de estruturas, com
 *   system_errno zero, ou semop() retornou EFBIG, preservado em system_errno.
 * - MPMC_ERR_STATE: registro/ACK inconsistente propagado pelo transporte;
 *   system_errno é zero.
 * - MPMC_ERR_INTERRUPTED: cancelamento local, com system_errno zero, ou espera
 *   interrompida em semop(), msgsnd(), msgrcv() ou nanosleep(), com EINTR capturado.
 * - MPMC_ERR_SHUTDOWN: contexto quiescente, com system_errno zero, ou recurso
 *   removido durante semop(), msgsnd() ou msgrcv(), com EIDRM capturado.
 * - MPMC_ERR_SYS: falha em clock_gettime(), alocação ou outra falha em semop(),
 *   msgsnd(), msgrcv() ou nanosleep(); system_errno preserva o errno capturado.
 *   Overflow no dimensionamento do registro ACK MSG usa system_errno zero.
 * @details Gera sender_pid=getpid(), sequência iniciando em 1 e sent_at
 * do CLOCK_REALTIME. Usa ipc_send() para o destino selecionado; para
 * producer_id=0, alterna destinos `1 + ((seq-1) % p)`. Na opção ack=1,
 * executa begin antes dos envios, wait após todos os envios e end em qualquer
 * saída posterior a begin; na opção ack=2, não solicita confirmação.
 * @pre Deve ser chamada em um filho do supervisor, não no processo principal.
 * @post Não fecha o contexto nem remove objetos globais. O ponto de entrada do
 * filho usa producer_run_with_result() para obter também o progresso, chama
 * ipc_worker_close() uma única vez e envia o resultado estruturado ao pai pelo pipe.
 * O supervisor recolhe separadamente o estado de término com waitpid().
 * @warning Envio pode bloquear por esgotamento de capacidade mesmo com ack=2.
 */
mpmc_result_t producer_run(ipc_context_t *transport, const producer_job_t *job);

/** @} */
#endif
