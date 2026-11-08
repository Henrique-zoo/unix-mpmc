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

#include "mpmc/error.h"
#include "mpmc/message.h"

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
 * @param out Recebe a instância criada; NULL em qualquer falha.
 * @param producer_count Quantidade positiva de filas de dados.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: p filas de dados, uma fila de eventos e uma fila de ACK criadas;
 *   system_errno é zero.
 * - MPMC_ERR_INVALID_ARG: out nulo, producer_count zero ou grande demais para
 *   dimensionar os vetores internos; system_errno é zero.
 * - MPMC_ERR_SYS: falha de calloc()/malloc() ou de msgget(); system_errno preserva
 *   o errno capturado na operação que falhou.
 * @post Em erro, as filas já criadas são removidas (IPC_RMID) e a memória local é
 * liberada. O erro retornado é o da criação, e não o de uma eventual falha de remoção.
 * @note Filas adicionais para ACKs/eventos são recursos de controle, não
 * substituem as filas exigidas para troca de dados.
 */
mpmc_result_t msg_backend_create(msg_backend_t **out, size_t producer_count);

/**
 * @brief Insere uma mensagem na fila de dados de um índice lógico.
 * @param backend Instância herdada no processo produtor.
 * @param producer_id Índice de destino, 1..p.
 * @param message Mensagem lógica a copiar para o envelope System V.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: mensagem inserida na fila; system_errno é zero.
 * - MPMC_ERR_INVALID_ARG: backend ou message nulo, ou producer_id fora de 1..p;
 *   system_errno é zero e nenhuma chamada ao kernel é feita.
 * - MPMC_ERR_INTERRUPTED: msgsnd() interrompida por sinal; system_errno contém EINTR.
 * - MPMC_ERR_SYS: falha em malloc() no wrapper ou em msgsnd(); system_errno
 *   preserva o errno capturado na operação que falhou.
 * @details Prepara estrutura cujo primeiro campo é `long mtype` positivo.
 * Usa mtype=2 se a operação possui contexto local de ACK ativo; caso contrário,
 * usa mtype=1. A política não acrescenta campos ao corpo message_t.
 * O tamanho enviado a msgsnd() exclui esse campo. O envelope não armazena
 * ponteiros de memória privados do processo produtor.
 * O envio é bloqueante: com a fila cheia (limite msg_qbytes), msgsnd() espera
 * vaga (backpressure) e prossegue quando um consumidor retira mensagens.
 * @warning Um ACK por mensagem em fila auxiliar limitada pode causar
 * deadlock por contrapressão. Ver docs/protocol.md para mitigação exigida.
 */
mpmc_result_t
msg_backend_send(msg_backend_t *backend, size_t producer_id, const message_t *message);

/**
 * @brief Retira uma mensagem, contabiliza e encaminha seu ACK se necessário.
 * @param backend Instância do processo consumidor.
 * @param producer_id Índice lógico de origem, 1..p.
 * @param out Destino da mensagem recebida.
 * @param blocking Habilita espera por mensagem; false usa IPC_NOWAIT.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: mensagem retirada, evento publicado e ACK enviado se solicitado;
 *   system_errno é zero.
 * - MPMC_ERR_INVALID_ARG: backend ou out nulo, ou producer_id fora de 1..p;
 *   system_errno é zero.
 * - MPMC_ERR_WOULD_BLOCK: fila vazia com blocking=false; system_errno contém ENOMSG
 *   capturado em msgrcv(). Nenhum evento é publicado.
 * - MPMC_ERR_INTERRUPTED: msgrcv() interrompida por sinal enquanto espera a
 *   mensagem; system_errno contém EINTR.
 * - MPMC_ERR_STATE: violação de contrato do corpo retirado (tamanho diferente de
 *   sizeof(message_t) ou tipo diferente de 1/2); system_errno é zero. A mensagem
 *   já foi retirada da fila e não é contada.
 * - MPMC_ERR_SYS: falha em malloc(), msgrcv() (inclusive E2BIG) ou msgsnd() de
 *   evento/ACK, com errno capturado em system_errno.
 * @details A remoção de dados e a publicação de eventos/ACKs não são uma
 * transação atômica de kernel; a recuperação de interrupções entre etapas é pendente.
 * Recebe com tipo zero para manter FIFO entre os dois tipos de dados e usa
 * o mtype recebido para decidir se deve emitir ACK.
 * @note Usa sysv_msg_receive_typed() para obter o tipo sem adicioná-lo a message_t.
 * @note Depois da retirada, o envio do evento e do ACK repete EINTR (não devolve
 * MPMC_ERR_INTERRUPTED), para reduzir a janela em que uma mensagem é retirada
 * sem ser contada. Esse envio pode bloquear se a fila de controle estiver cheia;
 * o protocolo que evita esse bloqueio pertence à #14.
 */
mpmc_result_t
msg_backend_receive(msg_backend_t *backend, size_t producer_id, message_t *out, bool blocking);

/**
 * @brief Prepara rastreamento de ACK por operação produtora.
 * @param backend Instância no processo produtor.
 * @param operation_id ID exclusivo do trabalho.
 * @param expected Quantidade de confirmações a aguardar (1..1024).
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: operação registrada localmente; system_errno é zero.
 * - MPMC_ERR_INVALID_ARG: backend nulo, operation_id zero ou expected fora de 1..1024.
 * - MPMC_ERR_STATE: operation_id já registrado.
 * - MPMC_ERR_SYS: falha ao ampliar a tabela de registros; system_errno contém o
 *   errno de realloc(), ou zero em overflow de capacidade.
 * @note O canal distingue a operação, e não apenas o índice da fila.
 */
mpmc_result_t
msg_backend_ack_begin(msg_backend_t *backend, uint64_t operation_id, unsigned expected);

/**
 * @brief Aguarda ACKs de todas as mensagens da operação indicada.
 * @param backend Instância do filho produtor.
 * @param operation_id ID exclusivo preparado antes dos envios.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: todas as confirmações esperadas foram recebidas; system_errno é zero.
 * - MPMC_ERR_INVALID_ARG: backend nulo ou operation_id zero.
 * - MPMC_ERR_STATE: operação não registrada, ou ACK com corpo inválido.
 * - MPMC_ERR_INTERRUPTED: msgrcv() interrompida por sinal; system_errno contém EINTR.
 * - MPMC_ERR_SYS: falha em msgrcv() ou malloc(), com errno capturado.
 * @warning O algoritmo deve progredir sem bloquear circularmente quando
 * filas de dados e canais de controle estiverem sujeitos a capacidade máxima (#14).
 */
mpmc_result_t msg_backend_ack_wait(msg_backend_t *backend, uint64_t operation_id);

/**
 * @brief Finaliza registro/canal de ACK da operação.
 * @param backend Backend do processo produtor.
 * @param operation_id Trabalho previamente registrado.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: registro removido, ou operação inexistente (idempotente).
 * - MPMC_ERR_INVALID_ARG: backend nulo ou operation_id zero.
 */
mpmc_result_t msg_backend_ack_end(msg_backend_t *backend, uint64_t operation_id);

/**
 * @brief Drena eventos de consumo e acumula estatísticas no processo pai.
 * @param backend Backend pertencente ao supervisor.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: fila de eventos esvaziada; system_errno é zero.
 * - MPMC_ERR_INVALID_ARG: backend nulo.
 * - MPMC_ERR_STATE: evento com tamanho ou índice inválido, ou overflow do contador.
 * - MPMC_ERR_SYS: falha em msgrcv() ou malloc(), com errno capturado.
 * @details Filhos enviam registros IPC; o pai atualiza suas próprias
 * contagens locais após msgrcv(). É obrigatório drenar antes de imprimir os
 * números finais; os eventos não podem ser perdidos ou contados em duplicidade.
 * EINTR durante a drenagem é repetido.
 */
mpmc_result_t msg_backend_poll_events(msg_backend_t *backend);

/**
 * @brief Lê contador de consumos consolidado para um índice lógico.
 * @param backend Backend no supervisor, atualizado por poll_events().
 * @param producer_id Índice lógico 1..p.
 * @param out_count Destino do total acumulado.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: total escrito em out_count; system_errno é zero.
 * - MPMC_ERR_INVALID_ARG: backend ou out_count nulo, ou producer_id fora de 1..p.
 * @pre Chamar msg_backend_poll_events() antes do relatório final.
 */
mpmc_result_t
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
 * Tenta remover todas as filas mesmo que uma remoção falhe. Falhas de remoção
 * ainda não são reportadas (pendente, alinhar com shm_backend na #16).
 */
void msg_backend_destroy(msg_backend_t *backend);

/** @} */
#endif
