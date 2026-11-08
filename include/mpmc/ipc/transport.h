/**
 * @file transport.h
 * @brief Fachada independente de paradigma para comunicação entre workers.
 * @ingroup ipc
 *
 * @par Estado de implementação
 * A fachada existe (src/ipc/transport.c) e apenas despacha para o backend selecionado.
 * - Modo 2 (filas): todas as operações abaixo possuem implementação; os retornos
 *   documentados correspondem ao que msg_backend.c produz hoje.
 * - Modo 1 (memória compartilhada): somente ipc_init(), ipc_producer_count(),
 *   ipc_poll_events(), ipc_worker_close() e ipc_destroy() são reais. ipc_send(),
 *   ipc_receive() e as operações de ACK ainda são stubs que retornam MPMC_ERR_STATE
 *   (pendentes: #10 e #11), e ipc_consumed_count() devolve zero incondicionalmente
 *   (pendente: #10). Esses retornos não são contratos: serão substituídos.
 * Itens marcados como "pendente" não podem ser verificados nesta revisão.
 *
 * @par Convenção de retorno
 * As operações que podem falhar retornam mpmc_result_t. `system_errno` carrega o errno
 * da chamada de sistema que falhou, capturado no ponto da falha, e é zero para erros
 * que não vêm do sistema (argumento inválido, violação de estado). Os resultados do
 * backend são propagados sem alteração. O índice lógico fora de 1..p é
 * MPMC_ERR_INVALID_ARG nesta fachada (MPMC_ERR_INVALID_INDEX pertence à validação de
 * comandos). Nenhuma operação desta fachada retorna MPMC_ERR_SHUTDOWN ou MPMC_ERR_NO_SPACE;
 * ambos estão reservados para #20 e #11.
 *
 * @par Propriedade dos recursos
 * - O supervisor cria o contexto com ipc_init() e o remove uma única vez com
 *   ipc_destroy(), depois de recolher todos os filhos.
 * - Cada filho herda uma cópia do contexto por fork() e a libera uma única vez com
 *   ipc_worker_close(). Essa liberação é local: não remove filas, segmentos ou
 *   semáforos, que continuam em uso pelo supervisor e por outros workers.
 * - producer_run() e consumer_run() não chamam ipc_worker_close() nem ipc_destroy(); quem
 *   chamou a função do worker (o ponto de entrada do filho) fecha o contexto, e só ele.
 * - O contexto não deve ser usado depois de ipc_worker_close() ou ipc_destroy().
 *
 * @par Erro primário versus liberação
 * O primeiro erro de uma operação é o resultado do trabalho e não é substituído por
 * falhas ocorridas ao liberar recursos. ipc_ack_end() é chamada em sucesso, falha ou
 * cancelamento sempre que ipc_ack_begin() tiver retornado MPMC_OK; se o trabalho
 * terminou sem erro e ipc_ack_end() falhar, esse resultado passa a ser o do trabalho.
 * ipc_worker_close() e ipc_destroy() não têm canal de erro: o backend de filas ignora
 * falhas de msgctl(IPC_RMID), e o de memória compartilhada as entrega só a um
 * shm_cleanup_reporter_t que a fachada ainda não repassa (pendente: #16).
 *
 * @par Interrupção
 * Esperas bloqueantes não repetem EINTR; retornam MPMC_ERR_INTERRUPTED com system_errno
 * igual a EINTR. Nesse caso a operação não surtiu efeito: nenhuma mensagem foi inserida
 * ou retirada, e ACKs já contados permanecem contados. O worker decide encerrar e deve
 * então executar ipc_ack_end() (se houver) e retornar. Os filhos criados pelo supervisor
 * mantêm as ações padrão de SIGTERM e SIGINT; portanto, hoje, o cancelamento por
 * shutdown mata o filho por sinal e MPMC_ERR_INTERRUPTED não chega a ser observado.
 * Única exceção: depois que ipc_receive() retira uma mensagem, a publicação do evento e
 * do ACK repete EINTR para não perder a contagem, e ipc_poll_events() também o repete.
 *
 * @par Consumo e ACK: quem registra
 * O backend, dentro de ipc_receive(), é o único responsável por contar o consumo e por
 * confirmá-lo ao produtor. O worker consumidor não chama operação de ACK nem de
 * contagem, e message_t não tem campo de solicitação de ACK: ela pertence ao contexto
 * criado por ipc_ack_begin() no produtor (no modo 2, o produtor a codifica no mtype do
 * envelope; no modo 1, o consumidor consulta a tabela de operações). Ordem de uso do
 * produtor com ack=1: ipc_ack_begin() antes do primeiro envio, ipc_send() para cada
 * mensagem, ipc_ack_wait() depois do último envio e ipc_ack_end() ao final. Com ack=2,
 * nenhuma operação de ACK é chamada.
 *
 * @par Como o supervisor recebe resultados
 * Nada depende do heap do filho.
 * - Erro e cancelamento: pelo estado de término coletado com waitpid() (código de saída
 *   ou sinal, em child_report_t). Código zero significa trabalho concluído por completo;
 *   código diferente de zero, falha ou consumo parcial; sinal, cancelamento ou morte.
 *   O mapeamento fino de mpmc_status_t para o código de saída é pendente (#16/#22); hoje
 *   o supervisor só distingue zero, diferente de zero e sinal.
 * - Quantidade efetiva consumida: por estrutura lógica, pelo backend. No modo 2, cada
 *   retirada publica um evento que ipc_poll_events() consolida no supervisor; no modo 1,
 *   o total fica em memória compartilhada (pendente: #10). O supervisor lê o resultado
 *   com ipc_consumed_count(). O consumer_result_t de um trabalho existe só no filho, e o
 *   código de saída (8 bits) não comporta 1024; a quantidade por trabalho é apresentada
 *   pelo próprio filho. Levá-la ao supervisor exigiria um canal novo, a acordar em #16.
 */
#ifndef MPMC_IPC_TRANSPORT_H
#define MPMC_IPC_TRANSPORT_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "mpmc/error.h"
#include "mpmc/message.h"

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
 * @param out Recebe o ponteiro ao contexto em caso de sucesso.
 * @param producer_count Número de estruturas lógicas; deve ser positivo.
 * @param mode Paradigma 1 ou 2.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: contexto criado e escrito em out; system_errno é zero.
 * - MPMC_ERR_INVALID_ARG: out nulo, producer_count zero ou mode diferente de 1/2;
 *   system_errno é zero.
 * - MPMC_ERR_SYS: falha de alocação ou de chamada de sistema (msgget(), shmget(),
 *   shmat()), com o errno capturado em system_errno; o estouro de tamanho do vetor de
 *   filas também retorna este estado, com system_errno zero.
 * @post Em qualquer falha exceto MPMC_ERR_INVALID_ARG, *out recebe NULL; em
 * MPMC_ERR_INVALID_ARG, *out não é modificado.
 * @par Rollback
 * Em falha parcial, os recursos já criados são removidos antes do retorno: o backend de
 * filas remove as filas criadas e o de memória compartilhada executa seu destroy; a
 * fachada libera a própria alocação. O chamador não deve chamar ipc_destroy() após falha.
 * @par Propriedade
 * O supervisor é o único proprietário; ipc_destroy() deve ser chamado uma vez.
 */
mpmc_result_t ipc_init(ipc_context_t **out, size_t producer_count, ipc_mode_t mode);

/**
 * @brief Informa a quantidade de estruturas lógicas disponíveis.
 * @param context Contexto inicializado.
 * @return Valor `p` informado a ipc_init(); retorna 0 se context for NULL.
 */
size_t ipc_producer_count(const ipc_context_t *context);

/**
 * @brief Envia uma mensagem a uma estrutura lógica.
 * @param context Transporte inicializado e válido no processo atual.
 * @param producer_id Índice lógico da fila de destino, no intervalo 1..p; não é PID.
 * @param message Mensagem lógica; a implementação não altera o objeto passado.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: mensagem inserida; system_errno é zero.
 * - MPMC_ERR_INVALID_ARG: context ou message nulo, ou producer_id fora de 1..p;
 *   system_errno é zero.
 * - MPMC_ERR_INTERRUPTED: espera interrompida por sinal, nada foi inserido;
 *   system_errno é EINTR (modo 2).
 * - MPMC_ERR_SYS: falha de alocação ou de msgsnd(); system_errno preserva o errno (modo 2).
 * - MPMC_ERR_STATE: stub do modo 1, pendente em #10.
 * @details Pode bloquear aguardando espaço, inclusive quando o produtor não pediu ACK.
 * Envio != consumo: confirmações são independentes da conclusão desta função. Uma
 * mensagem já inserida não pode ser retirada pelo produtor se um envio posterior falhar.
 * @pre Para solicitar confirmação, chamar ipc_ack_begin() para o
 * message->operation_id antes do primeiro envio. O backend consulta esse contexto
 * para determinar a política de ACK e preserva todos os campos da mensagem.
 * @warning No modo 2, o envio com ACK ativo ainda usa msgsnd() bloqueante simples, sem o
 * laço de drenagem descrito em docs/protocol.md (pendente: #14).
 */
mpmc_result_t ipc_send(ipc_context_t *context, size_t producer_id, const message_t *message);

/**
 * @brief Retira uma única mensagem de uma estrutura lógica.
 * @param context Transporte inicializado.
 * @param producer_id Índice lógico da fila de origem, no intervalo 1..p; não é PID do emissor.
 * @param out Recebe a mensagem removida se retornar MPMC_OK.
 * @param blocking Se true, aguarda um item; se false, não espera.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: mensagem retirada, contada e confirmada se a operação produtora pediu ACK;
 *   system_errno é zero.
 * - MPMC_ERR_INVALID_ARG: context ou out nulo, ou producer_id fora de 1..p;
 *   system_errno é zero.
 * - MPMC_ERR_WOULD_BLOCK: blocking=false e estrutura vazia; system_errno é ENOMSG (modo 2).
 * - MPMC_ERR_INTERRUPTED: espera interrompida por sinal, nada foi retirado; system_errno
 *   é EINTR (modo 2).
 * - MPMC_ERR_STATE: tipo de envelope inesperado (modo 2) ou stub do modo 1 (pendente: #10).
 * - MPMC_ERR_SYS: falha de alocação, de msgrcv() ou da publicação do evento/ACK, com o
 *   errno em system_errno; corpo de tamanho incompatível também retorna este estado, com
 *   system_errno zero.
 * @details O backend registra exatamente um consumo e sinaliza ao produtor quando a
 * operação pediu ACK. Não há ACK duplicado nem ACK feito pelo worker.
 * @warning MPMC_ERR_STATE e MPMC_ERR_SYS podem ocorrer depois de a mensagem sair da
 * fila: ela não é devolvida, out pode estar preenchido e o consumo pode não ter sido
 * contado nem confirmado. O chamador não deve contá-la como consumida nem repetir o ACK.
 * A recuperação dessa janela é pendente (ver docs/protocol.md).
 */
mpmc_result_t
ipc_receive(ipc_context_t *context, size_t producer_id, message_t *out, bool blocking);

/**
 * @brief Prepara o contexto de ACK de uma operação produtora.
 * @param context Transporte no processo produtor.
 * @param operation_id ID único e não zero do trabalho produtor, atribuído pelo supervisor.
 * @param expected Número total de mensagens a confirmar, de 1 a 1024; em produz 0, é o
 * total do trabalho, e não o de uma estrutura.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: operação registrada; system_errno é zero.
 * - MPMC_ERR_INVALID_ARG: context nulo, operation_id zero ou expected fora de 1..1024;
 *   system_errno é zero.
 * - MPMC_ERR_STATE: operation_id já registrada (modo 2) ou stub do modo 1 (pendente: #11).
 * - MPMC_ERR_SYS: falha de alocação do registro; system_errno preserva o errno de
 *   realloc(); o estouro do tamanho do registro retorna este estado com system_errno zero.
 * @pre Deve ser chamada ANTES do primeiro envio da operação que espera ACKs. Se retornar
 * erro, nada foi registrado e ipc_ack_end() não deve ser chamada.
 * @note Pendentes: recusa de operation_id maior que LONG_MAX (modo 2, #14) e
 * MPMC_ERR_NO_SPACE quando a tabela de 64 operações do modo 1 estiver cheia (#11).
 */
mpmc_result_t ipc_ack_begin(ipc_context_t *context, uint64_t operation_id, unsigned expected);

/**
 * @brief Espera o consumo efetivo de todas as mensagens de uma operação.
 * @param context Contexto do processo produtor.
 * @param operation_id Operação previamente registrada com ipc_ack_begin().
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: as `expected` confirmações da operação chegaram; system_errno é zero.
 * - MPMC_ERR_INVALID_ARG: context nulo ou operation_id zero; system_errno é zero.
 * - MPMC_ERR_STATE: operação não registrada, ou ACK inconsistente (modo 2), ou stub do
 *   modo 1 (pendente: #11).
 * - MPMC_ERR_INTERRUPTED: espera interrompida por sinal; system_errno é EINTR; as
 *   confirmações já contadas permanecem e ipc_ack_end() continua obrigatória (modo 2).
 * - MPMC_ERR_SYS: falha de msgrcv(); system_errno preserva o errno (modo 2).
 * @pre A operação já enviou as `expected` mensagens correspondentes.
 * @note ACKs recebidos antes desta chamada permanecem contabilizados.
 * @warning A garantia de ausência de deadlock por saturação do canal de confirmações
 * depende do laço de drenagem no envio e é pendente (#14).
 */
mpmc_result_t ipc_ack_wait(ipc_context_t *context, uint64_t operation_id);

/**
 * @brief Libera os recursos de ACK de uma operação específica.
 * @param context Contexto do produtor.
 * @param operation_id Identificador preparado em ipc_ack_begin().
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: estado local liberado; no modo 2, também quando a operação é desconhecida
 *   ou já foi encerrada (a chamada é idempotente); system_errno é zero.
 * - MPMC_ERR_INVALID_ARG: context nulo ou operation_id zero; system_errno é zero.
 * - MPMC_ERR_STATE: stub do modo 1 (pendente: #11).
 * @note Chamar ao final do trabalho, mesmo quando o envio/espera falhar, desde que
 * ipc_ack_begin() tenha retornado MPMC_OK. A drenagem de ACKs restantes da operação
 * descrita em docs/protocol.md é pendente (#14).
 */
mpmc_result_t ipc_ack_end(ipc_context_t *context, uint64_t operation_id);

/**
 * @brief Consolida eventos de controle e estatísticas no supervisor.
 * @param context Contexto pertencente ao processo supervisor.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: eventos pendentes consolidados; no modo 1 nada precisa ser feito;
 *   system_errno é zero.
 * - MPMC_ERR_INVALID_ARG: context nulo; system_errno é zero.
 * - MPMC_ERR_STATE: evento malformado ou de estrutura inexistente (modo 2).
 * - MPMC_ERR_SYS: falha de msgrcv(); system_errno preserva o errno (modo 2). EINTR é
 *   repetido internamente e nunca retorna MPMC_ERR_INTERRUPTED.
 * @details No modo 2, drena sem bloquear a fila de eventos de consumo e atualiza os
 * contadores locais do supervisor. No modo 1, é uma operação vazia: os totais ficam em
 * memória compartilhada. Apenas o supervisor a chama.
 * @warning Não depende de o pai enxergar escritas no heap privado dos filhos.
 */
mpmc_result_t ipc_poll_events(ipc_context_t *context);

/**
 * @brief Obtém o total de mensagens efetivamente consumidas de um índice.
 * @param context Contexto do supervisor.
 * @param producer_id Índice de origem, 1..p.
 * @param out_count Recebe contador acumulado desde ipc_init(); só é escrito em sucesso.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: total escrito em out_count; system_errno é zero.
 * - MPMC_ERR_INVALID_ARG: context ou out_count nulo, ou producer_id fora de 1..p;
 *   system_errno é zero.
 * @pre No modo 2, chamar ipc_poll_events() antes de obter números finais.
 * @warning No modo 1, o valor devolvido é sempre zero enquanto a contagem real não for
 * implementada (pendente: #10); não usá-lo como relatório correto.
 */
mpmc_result_t ipc_consumed_count(ipc_context_t *context, size_t producer_id, uint64_t *out_count);

/**
 * @brief Libera vínculos e estruturas locais do contexto em um worker.
 * @param context Cópia de contexto herdada pelo processo filho; NULL é permitido.
 * @details Desanexa páginas com shmdt() e libera alocações *locais* quando
 * aplicável. Nunca executa IPC_RMID em recursos globais, pois eles continuam
 * pertencendo ao supervisor e sendo usados por outros workers. Não retorna erros; ver
 * "Erro primário versus liberação" no topo do arquivo.
 * @pre Chamada uma única vez, no processo filho, depois de ele concluir ou falhar e de
 * executar ipc_ack_end() das operações iniciadas.
 * @post O ponteiro context é inválido neste processo. A cópia do supervisor não é afetada.
 */
void ipc_worker_close(ipc_context_t *context);

/**
 * @brief Remove definitivamente os recursos IPC e libera o contexto do pai.
 * @param context Contexto pertencente ao supervisor; NULL é permitido.
 * @details Executa IPC_RMID das estruturas globais criadas pela aplicação,
 * fecha recursos de controle, libera alocações e torna o ponteiro inválido.
 * Não retorna erros; ver "Erro primário versus liberação" no topo do arquivo.
 * @pre Todos os workers foram encerrados e recolhidos, sem novos envios, e a drenagem
 * final com ipc_poll_events() e a leitura de ipc_consumed_count() já ocorreram: os
 * eventos ainda na fila são descartados junto com os recursos.
 * @warning Nunca chamar no filho após fork().
 */
void ipc_destroy(ipc_context_t *context);

/** @} */
#endif
