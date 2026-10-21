/**
 * @file msg_backend.h
 * @brief Backend de filas de mensagens e eventos System V.
 * @ingroup backends
 */
#ifndef MPMC_IPC_MSG_BACKEND_H
#define MPMC_IPC_MSG_BACKEND_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "mpmc/message.h"
#include "mpmc/status.h"

/** @ingroup backends
 * @{ */

/**
 * @brief Estado opaco das filas System V e dos canais auxiliares de controle.
 * @details Gerencia uma fila de dados por índice lógico, protocolo de ACK e
 * publicação de eventos de consumo para totalização pelo supervisor.
 */
typedef struct msg_backend msg_backend_t;

/**
 * @brief Cria filas de mensagens para os índices 1..p.
 * @param out Recebe a instância criada; NULL em falha.
 * @param producer_count Quantidade positiva de filas de dados.
 * @retval MPMC_OK Filas inicializadas.
 * @retval MPMC_ERR_INVALID_ARG Valor de p inválido.
 * @retval MPMC_ERR_SYS Falha de msgget() ou inicialização parcial.
 * @post Recursos já criados são removidos em caso de erro.
 * @note Filas adicionais para ACKs/eventos são recursos de controle, não
 * substituem as filas exigidas para troca de dados.
 */
mpmc_status_t msg_backend_create(msg_backend_t **out, size_t producer_count);

/**
 * @brief Insere uma mensagem na fila de dados de um índice lógico.
 * @param backend Instância herdada no processo produtor.
 * @param producer_id Índice de destino, 1..p.
 * @param message Mensagem lógica a copiar para o envelope System V.
 * @retval MPMC_OK Chamada msgsnd() concluída.
 * @retval MPMC_ERR_INTERRUPTED Envio bloqueante cancelado.
 * @retval MPMC_ERR_SYS Falha de fila/protocolo.
 * @details Prepara estrutura cujo primeiro campo é `long mtype` positivo.
 * O tamanho enviado a msgsnd() exclui esse campo. O envelope não armazena
 * ponteiros de memória privados do processo produtor.
 * @warning Um ACK por mensagem em fila auxiliar limitada pode causar
 * deadlock por contrapressão. Ver docs/protocol.md para mitigação exigida.
 */
mpmc_status_t
msg_backend_send(msg_backend_t *backend, size_t producer_id, const message_t *message);

/**
 * @brief Retira uma mensagem, contabiliza e encaminha seu ACK se necessário.
 * @param backend Instância do processo consumidor.
 * @param producer_id Índice lógico de origem, 1..p.
 * @param out Destino da mensagem recebida.
 * @param blocking Habilita espera por mensagem; false usa IPC_NOWAIT.
 * @retval MPMC_OK Remoção válida e evento de consumo publicado.
 * @retval MPMC_ERR_WOULD_BLOCK Nenhuma mensagem pronta com blocking=false.
 * @retval MPMC_ERR_INTERRUPTED Operação cancelada por sinal.
 * @retval MPMC_ERR_SYS Falha de msgrcv() ou do protocolo de controle.
 * @details A remoção de dados e a publicação de eventos/ACKs não são uma
 * transação atômica de kernel; especificar recuperação antes de implementar.
 */
mpmc_status_t
msg_backend_receive(msg_backend_t *backend, size_t producer_id, message_t *out, bool blocking);

/**
 * @brief Prepara rastreamento de ACK por operação produtora.
 * @param backend Instância no processo produtor.
 * @param operation_id ID exclusivo do trabalho.
 * @param expected Quantidade de confirmações a aguardar (1..1024).
 * @retval MPMC_OK Canal de ACK preparado.
 * @retval MPMC_ERR_SYS Falha em msgget() ou registro de controle.
 * @note O canal deve distinguir a operação, e não apenas o índice da fila.
 */
mpmc_status_t
msg_backend_ack_begin(msg_backend_t *backend, uint64_t operation_id, unsigned expected);

/**
 * @brief Aguarda ACKs de todas as mensagens da operação indicada.
 * @param backend Instância do filho produtor.
 * @param operation_id ID exclusivo preparado antes dos envios.
 * @retval MPMC_OK Quantidade esperada de consumos confirmada.
 * @retval MPMC_ERR_INTERRUPTED Cancelamento durante a espera.
 * @retval MPMC_ERR_SYS Falha do canal de ACK.
 * @warning O algoritmo deve progredir sem bloquear circularmente quando
 * filas de dados e canais de controle estiverem sujeitos a capacidade máxima.
 */
mpmc_status_t msg_backend_ack_wait(msg_backend_t *backend, uint64_t operation_id);

/**
 * @brief Finaliza registro/canal de ACK da operação.
 * @param backend Backend do processo produtor.
 * @param operation_id Trabalho previamente registrado.
 * @retval MPMC_OK Recurso de ACK liberado.
 * @retval MPMC_ERR_SYS Erro ao liberar recurso temporário.
 */
mpmc_status_t msg_backend_ack_end(msg_backend_t *backend, uint64_t operation_id);

/**
 * @brief Drena eventos de consumo e acumula estatísticas no processo pai.
 * @param backend Backend pertencente ao supervisor.
 * @retval MPMC_OK Eventos disponíveis drenados sem bloqueio.
 * @retval MPMC_ERR_SYS Falha de recebimento de evento de controle.
 * @details Filhos enviam registros IPC; o pai atualiza suas próprias
 * contagens locais após msgrcv(). É obrigatório drenar antes de imprimir os
 * números finais; os eventos não podem ser perdidos ou contados em duplicidade.
 */
mpmc_status_t msg_backend_poll_events(msg_backend_t *backend);

/**
 * @brief Lê contador de consumos consolidado para um índice lógico.
 * @param backend Backend no supervisor, atualizado por poll_events().
 * @param producer_id Índice lógico 1..p.
 * @param out_count Destino do total acumulado.
 * @retval MPMC_OK Total obtido.
 * @retval MPMC_ERR_SYS Erro ao consultar estado interno.
 * @pre Chamar msg_backend_poll_events() antes do relatório final.
 */
mpmc_status_t
msg_backend_consumed_count(msg_backend_t *backend, size_t producer_id, uint64_t *out_count);

/**
 * @brief Libera somente o estado local do processo filho.
 * @param backend Backend herdado pelo fork().
 * @details Nunca remove filas de dados/controle globais via IPC_RMID.
 */
void msg_backend_worker_close(msg_backend_t *backend);

/**
 * @brief Remove todas as filas globais de dados e controle do supervisor.
 * @param backend Backend pertencente ao pai; NULL é aceito.
 * @pre Todos os processos filhos foram recolhidos e eventos já drenados.
 * @details Chama msgctl(IPC_RMID) para todos os identificadores válidos
 * criados na inicialização e libera memória alocada para a instância.
 */
void msg_backend_destroy(msg_backend_t *backend);

/** @} */
#endif
