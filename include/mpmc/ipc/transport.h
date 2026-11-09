/**
 * @file transport.h
 * @brief Fachada independente de paradigma para comunicação entre workers.
 * @ingroup ipc
 *
 * @par Estado de implementação
 * A fachada existe (src/ipc/transport.c) e apenas despacha para o backend selecionado.
 * - Modo 2 (filas): todas as operações abaixo possuem implementação; os retornos
 *   documentados correspondem ao que msg_backend.c produz hoje.
 * - Modo 1 (memória compartilhada): ciclo de vida, envio, recebimento e contagens
 *   sincronizadas são reais. Somente as operações de ACK permanecem como stubs
 *   que retornam MPMC_ERR_STATE (pendente: #11).
 * Itens marcados como "pendente" não podem ser verificados nesta revisão.
 *
 * @par Convenção de retorno
 * As operações que podem falhar retornam mpmc_result_t. `system_errno` carrega o errno
 * da chamada de sistema que falhou, capturado no ponto da falha, e é zero para erros
 * que não vêm do sistema (argumento inválido, violação de estado). Os resultados do
 * backend são propagados sem alteração. As rejeições locais, porém, são decididas pela
 * própria fachada antes do despacho e são idênticas nos dois modos: ponteiro nulo,
 * operation_id zero ou maior que LONG_MAX e expected fora de 1..1024 retornam
 * MPMC_ERR_INVALID_ARG; índice lógico fora de 1..p retorna MPMC_ERR_INVALID_INDEX
 * (system_errno zero em todos). O código do chamador nunca depende do paradigma para
 * interpretar um erro de argumento. O system_errno das falhas de sistema continua sendo
 * o da syscall de cada backend (por exemplo, EAGAIN no modo 1 e ENOMSG no modo 2 para
 * "estrutura vazia"); é diagnóstico e não deve ser usado para decidir o fluxo.
 * Falhas de argumentos rejeitados pelo kernel preservam errno, mesmo quando
 * classificadas como MPMC_ERR_INVALID_ARG; EIDRM resulta em MPMC_ERR_SHUTDOWN.
 * MPMC_ERR_NO_SPACE permanece reservado para a tabela de ACK da issue #11.
 *
 * @par Falhas fatais no modo 1
 * ipc_send(), ipc_receive() e ipc_consumed_count() seguem a política de
 * @ref shm_operation_failures. Rejeições locais com system_errno zero, ausência
 * de itens e interrupção da aquisição não invalidam o contexto por si mesmas.
 * As demais falhas de semop() exigem suspender o uso de dados e estatísticas do
 * contexto e encaminhar o encerramento coordenado ao supervisor. A fachada
 * apenas propaga o resultado: não marca o contexto como inválido nem avisa outros
 * processos. A integração do encerramento por falha fatal pertence à issue #20.
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
 *   o total fica em memória compartilhada. O supervisor lê o resultado
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
 *   shmat(), semget() ou semctl(SETVAL)), com o errno capturado em system_errno;
 *   o estouro de tamanho do vetor de
 *   filas também retorna este estado, com system_errno zero.
 * - MPMC_ERR_SHUTDOWN: criação/associação SHM encontrou EIDRM em shmat() ou
 *   semctl(SETVAL); system_errno preserva EIDRM.
 * @note Uma rejeição de argumentos pelo kernel durante a inicialização SHM também
 * retorna MPMC_ERR_INVALID_ARG, preservando o errno em system_errno.
 * @post A validação local da fachada não modifica *out em erro. Depois dela,
 * qualquer falha deixa *out igual a NULL, inclusive erro de argumento vindo do kernel.
 * @par Rollback
 * Em falha parcial, os recursos já criados são removidos antes do retorno: o backend de
 * filas remove as filas criadas e o de memória compartilhada limpa as entradas visitadas; a
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
 * - MPMC_ERR_INVALID_ARG: context/message nulo, com system_errno zero; ou argumento
 *   rejeitado por semop()/msgsnd(), com errno preservado.
 * - MPMC_ERR_INVALID_INDEX: índice lógico fora de 1..p (decidido pela fachada, nos dois
 *   modos), com system_errno zero; ou semop() retornou EFBIG, preservado em system_errno.
 * - MPMC_ERR_INTERRUPTED: espera interrompida por sinal, nada foi inserido;
 *   system_errno é EINTR.
 * - MPMC_ERR_SHUTDOWN: semop()/msgsnd() retornou EIDRM, preservado em system_errno.
 * - MPMC_ERR_SYS: falha de alocação ou outra falha em semop()/msgsnd();
 *   system_errno preserva o errno capturado.
 * @details Pode bloquear aguardando espaço, inclusive quando o produtor não pediu ACK.
 * Envio != consumo: confirmações são independentes da conclusão desta função. Uma
 * mensagem já inserida não pode ser retirada pelo produtor se um envio posterior falhar.
 * @warning No modo 1, falha da publicação final pode ocorrer após a inserção e
 * torna o contexto inutilizável. Não repetir o envio; aplicar a política de
 * @ref shm_operation_failures.
 * @pre Para solicitar confirmação, chamar ipc_ack_begin() para o
 * message->operation_id antes do primeiro envio. O backend consulta esse contexto
 * para determinar a política de ACK e preserva todos os campos da mensagem.
 * @note No modo 2, o envio com ACK ativo usa msgsnd() com IPC_NOWAIT: em fila cheia, drena
 * os ACKs da própria operação, espera até 1 ms e repete, de modo que o produtor nunca
 * bloqueia no kernel sem consumir ACKs (docs/protocol.md). Sem ACK ativo, o envio é
 * bloqueante.
 */
mpmc_result_t ipc_send(ipc_context_t *context, size_t producer_id, const message_t *message);

/**
 * @brief Retira uma única mensagem de uma estrutura lógica.
 * @param context Transporte inicializado.
 * @param producer_id Índice lógico da fila de origem, no intervalo 1..p; não é PID do emissor.
 * @param out Recebe a mensagem removida se retornar MPMC_OK.
 * @param blocking Se true, aguarda um item; se false, não espera.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: mensagem retirada e contada; no modo 2, confirmação publicada se
 *   solicitada. system_errno é zero. ACKs do modo 1 ainda são pendentes.
 * - MPMC_ERR_INVALID_ARG: context/out nulo, com system_errno zero; ou argumento
 *   rejeitado por semop()/msgrcv()/msgsnd(), com errno preservado.
 * - MPMC_ERR_INVALID_INDEX: índice lógico fora de 1..p (decidido pela fachada, nos dois
 *   modos), com system_errno zero; ou semop() retornou EFBIG, preservado em system_errno.
 * - MPMC_ERR_WOULD_BLOCK: blocking=false e estrutura vazia; system_errno é EAGAIN
 *   no modo 1 e ENOMSG no modo 2.
 * - MPMC_ERR_INTERRUPTED: espera interrompida por sinal, nada foi retirado; system_errno
 *   é EINTR.
 * - MPMC_ERR_SHUTDOWN: semop()/msgrcv()/msgsnd() retornou EIDRM, preservado em system_errno.
 * - MPMC_ERR_STATE: tipo de envelope inesperado (modo 2).
 * - MPMC_ERR_SYS: falha de alocação, de semop(), msgrcv() ou da publicação do evento/ACK, com o
 *   errno em system_errno; corpo de tamanho incompatível também retorna este estado, com
 *   system_errno zero.
 * @details A contagem pertence ao backend. No modo 2, ele também sinaliza o produtor
 * quando a operação pediu ACK; o worker não duplica a confirmação. Com blocking=false,
 * nem a contagem nem o ACK esperam por fila de controle cheia: o que não couber fica
 * devido no processo e é entregue no próximo ipc_receive() ou em ipc_worker_close(). No modo 1,
 * blocking=false ainda pode aguardar o mutex; a retirada não emite ACK nesta etapa.
 * @warning No modo 1, falha de publicação ocorre depois da retirada e da contagem,
 * podendo deixar out preenchido; a operação não é desfeita e o contexto torna-se
 * inutilizável. Não repetir o recebimento; aplicar @ref shm_operation_failures.
 * @warning No modo 2, MPMC_ERR_STATE e MPMC_ERR_SYS podem ocorrer depois de a
 * mensagem sair da fila: ela não é devolvida, out pode estar preenchido e o consumo
 * pode não ter sido contado nem confirmado. O chamador não deve contá-la como
 * consumida nem repetir o ACK.
 * A recuperação dessa janela é pendente (ver docs/protocol.md).
 */
mpmc_result_t ipc_receive(
    ipc_context_t *context,
    size_t producer_id,
    message_t *out,
    bool blocking
);

/**
 * @brief Prepara o contexto de ACK de uma operação produtora.
 * @param context Transporte no processo produtor.
 * @param operation_id ID único e não zero do trabalho produtor, atribuído pelo supervisor.
 * @param expected Número total de mensagens a confirmar, de 1 a 1024; em produz 0, é o
 * total do trabalho, e não o de uma estrutura.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: operação registrada; system_errno é zero.
 * - MPMC_ERR_INVALID_ARG: context nulo, operation_id zero ou maior que LONG_MAX, ou
 *   expected fora de 1..1024 (decidido pela fachada, nos dois modos); system_errno é zero.
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
 * - MPMC_ERR_INVALID_ARG: context nulo ou operation_id zero ou maior que LONG_MAX, com
 *   system_errno zero;
 *   ou msgrcv() retornou EINVAL/EFAULT, preservado em system_errno (modo 2).
 * - MPMC_ERR_STATE: operação não registrada, ou ACK inconsistente (modo 2), ou stub do
 *   modo 1 (pendente: #11).
 * - MPMC_ERR_INTERRUPTED: espera interrompida por sinal; system_errno é EINTR; as
 *   confirmações já contadas permanecem e ipc_ack_end() continua obrigatória (modo 2).
 * - MPMC_ERR_SHUTDOWN: msgrcv() retornou EIDRM, preservado em system_errno (modo 2).
 * - MPMC_ERR_SYS: outra falha de msgrcv(); system_errno preserva o errno (modo 2).
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
 * - MPMC_ERR_INVALID_ARG: context nulo ou operation_id zero ou maior que LONG_MAX;
 *   system_errno é zero.
 * - MPMC_ERR_STATE: stub do modo 1 (pendente: #11).
 * @note Chamar ao final do trabalho, mesmo quando o envio/espera falhar, desde que
 * ipc_ack_begin() tenha retornado MPMC_OK. No modo 2, drena sem bloquear os ACKs da
 * operação que já estão na fila; os que chegarem depois (consumo posterior ao fim do
 * produtor) são descartados pelo supervisor via ipc_ack_abandon()/ipc_poll_events().
 */
mpmc_result_t ipc_ack_end(ipc_context_t *context, uint64_t operation_id);

/**
 * @brief Declara sem dono uma operação cujo produtor terminou sem concluir a espera de ACK.
 * @param context Contexto pertencente ao processo supervisor.
 * @param operation_id ID da operação do filho produtor recolhido.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: no modo 2, operação registrada como abandonada e ACKs pendentes descartados;
 *   no modo 1, nada a fazer; system_errno é zero.
 * - MPMC_ERR_INVALID_ARG: context nulo, ou operation_id zero ou maior que LONG_MAX
 *   (decidido pela fachada, nos dois modos), com system_errno zero.
 * - MPMC_ERR_SYS: falha de alocação ou de msgrcv(), com errno preservado (modo 2).
 * - MPMC_ERR_SHUTDOWN: msgrcv() retornou EIDRM, preservado em system_errno (modo 2).
 * @details O supervisor a chama ao recolher um produtor que não terminou com sucesso.
 * Consumidores podem continuar retirando mensagens desse trabalho; sem o descarte, os ACKs
 * sem dono lotariam a fila de ACK e bloqueariam os consumidores. A partir daí,
 * ipc_poll_events() descarta os ACKs tardios. Chamar para trabalhos sem ACK é inofensivo.
 * Apenas o supervisor a chama.
 */
mpmc_result_t ipc_ack_abandon(ipc_context_t *context, uint64_t operation_id);

/**
 * @brief Consolida eventos de controle e estatísticas no supervisor.
 * @param context Contexto pertencente ao processo supervisor.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: eventos pendentes consolidados; no modo 1 nada precisa ser feito;
 *   system_errno é zero.
 * - MPMC_ERR_INVALID_ARG: context nulo, com system_errno zero; ou msgrcv() retornou
 *   EINVAL/EFAULT, preservado em system_errno (modo 2).
 * - MPMC_ERR_STATE: evento malformado ou de estrutura inexistente (modo 2).
 * - MPMC_ERR_SHUTDOWN: msgrcv() retornou EIDRM, preservado em system_errno (modo 2).
 * - MPMC_ERR_SYS: outra falha de msgrcv(); system_errno preserva o errno (modo 2). EINTR é
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
 * - MPMC_ERR_INVALID_ARG: context/out_count nulo, com system_errno zero; ou semop()
 *   rejeitou um argumento, com errno preservado (modo 1).
 * - MPMC_ERR_INVALID_INDEX: índice lógico fora de 1..p (decidido pela fachada, nos dois
 *   modos), com system_errno zero; ou semop() retornou EFBIG, preservado em system_errno.
 * - MPMC_ERR_INTERRUPTED: aquisição do mutex interrompida em semop(); system_errno
 *   contém EINTR (modo 1).
 * - MPMC_ERR_SHUTDOWN: semop() retornou EIDRM, preservado em system_errno (modo 1).
 * - MPMC_ERR_SYS: outra falha de semop(); system_errno preserva o errno (modo 1).
 * @pre No modo 2, chamar ipc_poll_events() antes de obter números finais.
 * @note No modo 1, o total é lido sob mutex e a saída só é preenchida após liberá-lo
 * com sucesso. Inclui retiradas executadas mesmo se a publicação da vaga falhou.
 * @warning No modo 1, a falha na liberação do mutex torna o contexto inutilizável,
 * embora out_count permaneça inalterado. Aplicar @ref shm_operation_failures;
 * não consultar estatísticas de um contexto que já sofreu uma falha fatal.
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
