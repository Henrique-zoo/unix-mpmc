/**
 * @file transport.h
 * @brief Fachada independente de paradigma para comunicação entre workers.
 * @ingroup ipc
 */
#ifndef MPMC_IPC_TRANSPORT_H
#define MPMC_IPC_TRANSPORT_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include "mpmc/message.h"
#include "mpmc/status.h"

/**
 * @defgroup ipc Comunicação entre processos (IPC)
 * @brief Fachada, backends e wrappers de primitivas System V.
 * @details O supervisor possui os recursos IPC; os workers herdam seus
 * identificadores após fork() e operam exclusivamente pela fachada.
 * @{
 */

/** @brief Paradigmas definidos pelo enunciado. */
typedef enum {
    IPC_SHARED_MEMORY = 1, /**< Memória compartilhada System V com semáforos. */
    IPC_MESSAGE_QUEUE = 2  /**< Filas de mensagens System V. */
} ipc_mode_t;

/**
 * @brief Estado opaco do transporte usado pela aplicação.
 * @details Armazena o backend escolhido e suas estruturas privadas.
 * A instância é criada no pai e herdada pelo processo filho por fork().
 */
typedef struct ipc_context ipc_context_t;

/**
 * @brief Cria os recursos globais do backend selecionado.
 * @param out Recebe o ponteiro ao contexto, ou NULL em falha.
 * @param producer_count Número de estruturas lógicas; deve ser positivo.
 * @param mode Paradigma 1 ou 2.
 * @retval MPMC_OK Inicialização completa.
 * @retval MPMC_ERR_INVALID_ARG Entrada inválida.
 * @retval MPMC_ERR_SYS Falha do kernel ou alocação; a função limpa recursos
 *   criados parcialmente.
 * @par Propriedade
 * O supervisor é o único proprietário; ipc_destroy() deve ser chamado uma vez.
 */
mpmc_status_t ipc_init(ipc_context_t **out, size_t producer_count, ipc_mode_t mode);

/**
 * @brief Informa a quantidade de estruturas lógicas disponíveis.
 * @param context Contexto inicializado.
 * @return Valor `p` informado a ipc_init(); retorna 0 se context for NULL.
 */
size_t ipc_producer_count(const ipc_context_t *context);

/**
 * @brief Envia uma mensagem a uma estrutura lógica.
 * @param context Transporte inicializado e válido no processo atual.
 * @param producer_id Índice de destino, no intervalo 1..p.
 * @param message Mensagem lógica; a implementação não altera o objeto passado.
 * @retval MPMC_OK Mensagem inserida no backend.
 * @retval MPMC_ERR_INVALID_INDEX Índice de destino fora do intervalo.
 * @retval MPMC_ERR_INTERRUPTED Envio interrompido por cancelamento.
 * @retval MPMC_ERR_SYS Falha em semop(), msgsnd() ou operações associadas.
 * @details Pode bloquear aguardando espaço. Confirmações, quando solicitadas,
 * são independentes da conclusão desta função: envio != consumo.
 * @pre message->ack_required exige um ipc_ack_begin() válido para operation_id.
 */
mpmc_status_t ipc_send(
    ipc_context_t *context,
    size_t producer_id,
    const message_t *message
);

/**
 * @brief Retira uma única mensagem de uma estrutura lógica.
 * @param context Transporte inicializado.
 * @param producer_id Índice de origem, no intervalo 1..p.
 * @param out Recebe a mensagem removida se retornar MPMC_OK.
 * @param blocking Se true, aguarda um item; se false, não espera.
 * @retval MPMC_OK Mensagem efetivamente consumida.
 * @retval MPMC_ERR_WOULD_BLOCK Sem item para retirada imediata.
 * @retval MPMC_ERR_INVALID_INDEX Índice não existente.
 * @retval MPMC_ERR_INTERRUPTED Interrompido por shutdown/sinal.
 * @retval MPMC_ERR_SYS Falha no IPC ou no protocolo de contabilização.
 * @details O backend registra exatamente um consumo e sinaliza ao produtor
 * quando a mensagem possuir ack_required=true. Não emitir ACK duplicado.
 * @warning A contagem e a publicação do ACK exigem desenho à prova de falhas
 * para evitar consumo contado mas não confirmado.
 */
mpmc_status_t ipc_receive(
    ipc_context_t *context,
    size_t producer_id,
    message_t *out,
    bool blocking
);

/**
 * @brief Prepara o contexto de ACK de uma operação produtora.
 * @param context Transporte no processo produtor.
 * @param operation_id ID único e não zero atribuído pelo supervisor.
 * @param expected Número total de mensagens a confirmar, de 1 a 1024.
 * @retval MPMC_OK Canal/estado de confirmação pronto.
 * @retval MPMC_ERR_INVALID_ARG ID ou quantidade inválida.
 * @retval MPMC_ERR_SYS Falha de alocação de recurso de confirmação.
 * @pre Deve ser chamada ANTES do primeiro envio marcado com ack_required.
 */
mpmc_status_t ipc_ack_begin(
    ipc_context_t *context,
    uint64_t operation_id,
    unsigned expected
);

/**
 * @brief Espera o consumo efetivo de todas as mensagens de uma operação.
 * @param context Contexto do processo produtor.
 * @param operation_id Operação previamente registrada com ipc_ack_begin().
 * @retval MPMC_OK Todas as confirmações chegaram.
 * @retval MPMC_ERR_INTERRUPTED Espera cancelada ou interrompida por shutdown.
 * @retval MPMC_ERR_SYS Falha no canal de controle.
 * @pre A operação já enviou as `expected` mensagens correspondentes.
 * @note ACKs recebidos antes desta chamada devem permanecer contabilizados.
 * @warning O protocolo de transporte deve evitar deadlock por saturação de
 * filas/semáforos de confirmação enquanto o envio ainda está em andamento.
 */
mpmc_status_t ipc_ack_wait(ipc_context_t *context, uint64_t operation_id);

/**
 * @brief Libera os recursos de ACK de uma operação específica.
 * @param context Contexto do produtor.
 * @param operation_id Identificador preparado em ipc_ack_begin().
 * @retval MPMC_OK Recursos de confirmação liberados.
 * @retval MPMC_ERR_SYS Erro ao liberar recurso IPC de confirmação.
 * @note Chamar ao final do trabalho, mesmo quando o envio/espera falhar.
 */
mpmc_status_t ipc_ack_end(ipc_context_t *context, uint64_t operation_id);

/**
 * @brief Consolida eventos de controle e estatísticas no supervisor.
 * @param context Contexto pertencente ao processo supervisor.
 * @retval MPMC_OK Eventos disponíveis processados sem bloquear.
 * @retval MPMC_ERR_SYS Erro ao obter/publicar eventos de controle.
 * @details No modo 2, drena filas de eventos de consumo e atualiza os
 * contadores locais no supervisor. No modo 1, pode ser uma operação vazia:
 * os totais já estão armazenados em memória compartilhada.
 * @warning Não depende de o pai enxergar escritas no heap privado dos filhos.
 */
mpmc_status_t ipc_poll_events(ipc_context_t *context);

/**
 * @brief Obtém o total de mensagens efetivamente consumidas de um índice.
 * @param context Contexto do supervisor.
 * @param producer_id Índice de origem, 1..p.
 * @param out_count Recebe contador acumulado desde ipc_init().
 * @retval MPMC_OK Contador consultado.
 * @retval MPMC_ERR_INVALID_INDEX Índice inválido.
 * @retval MPMC_ERR_SYS Falha ao ler/obter contador.
 * @pre No modo 2, chamar ipc_poll_events() antes de obter números finais.
 */
mpmc_status_t ipc_consumed_count(
    ipc_context_t *context,
    size_t producer_id,
    uint64_t *out_count
);

/**
 * @brief Libera vínculos e estruturas locais do contexto em um worker.
 * @param context Cópia de contexto herdada pelo processo filho.
 * @details Desanexa páginas com shmdt() e libera alocações *locais* quando
 * aplicável. Nunca executa IPC_RMID em recursos globais, pois eles continuam
 * pertencendo ao supervisor e sendo usados por outros workers.
 * @pre Chamada no processo filho após ele concluir ou falhar.
 */
void ipc_worker_close(ipc_context_t *context);

/**
 * @brief Remove definitivamente os recursos IPC e libera o contexto do pai.
 * @param context Contexto pertencente ao supervisor; NULL é permitido.
 * @details Executa IPC_RMID das estruturas globais criadas pela aplicação,
 * fecha recursos de controle, libera alocações e torna o ponteiro inválido.
 * @pre Todos os workers foram encerrados e recolhidos, sem novos envios.
 * @warning Nunca chamar no filho após fork().
 */
void ipc_destroy(ipc_context_t *context);

/** @} */
#endif
