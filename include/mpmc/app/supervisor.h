/**
 * @file supervisor.h
 * @brief Gerenciamento de IPC global, filhos, métricas e encerramento.
 * @ingroup application
 */
#ifndef MPMC_APP_SUPERVISOR_H
#define MPMC_APP_SUPERVISOR_H

#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <stdint.h>
#include <sys/types.h>

#include "mpmc/app/command.h"
#include "mpmc/ipc/transport.h"
#include "mpmc/status.h"

/** @ingroup application
 * @{ */

/**
 * @brief Estado opaco do supervisor; existe somente no processo principal.
 * @details Mantém contexto IPC, lista de filhos, identificador incremental de
 * operação, horário inicial monotônico e estatísticas já consolidadas.
 */
typedef struct supervisor supervisor_t;

/** @brief Forma como um filho supervisionado terminou. */
typedef enum {
    CHILD_END_EXITED,   /**< Saiu com exit(); `value` é o código de saída. */
    CHILD_END_SIGNALED, /**< Morto por sinal; `value` é o número do sinal. */
    CHILD_END_LOST      /**< Estado indisponível (ECHILD/inesperado); `value` é zero. */
} child_end_t;

/**
 * @brief Relatório imutável de um filho já recolhido.
 * @details Permite apresentar depois a duração e o status de cada processo.
 * A duração vai do instante imediatamente anterior ao fork() até o recolhimento
 * por waitpid(), medida com CLOCK_MONOTONIC; portanto inclui a latência (curta)
 * entre o término real e o recolhimento.
 */
typedef struct {
    pid_t pid;             /**< PID do filho. */
    command_kind_t kind;   /**< Tipo de trabalho executado. */
    uint64_t operation_id; /**< Identidade da operação atribuída no despacho. */
    uint64_t duration_ns;  /**< Duração em nanossegundos (nunca negativa). */
    child_end_t end;       /**< Forma de término. */
    int value;             /**< Código de saída ou sinal, conforme `end`. */
} child_report_t;

/**
 * @brief Cria o estado local de supervisão dos processos.
 * @param out Recebe a instância alocada; em falha recebe NULL.
 * @param producer_count Quantidade de estruturas lógicas, estritamente positiva.
 * @param mode Paradigma IPC_SHARED_MEMORY (1) ou IPC_MESSAGE_QUEUE (2).
 * @retval MPMC_OK Supervisor criado.
 * @retval MPMC_ERR_INVALID_ARG Parâmetros inválidos.
 * @retval MPMC_ERR_SYS Falha de alocação ou de chamada de sistema.
 * @details Também cria o canal de eventos (self-pipe) não bloqueante usado para
 * acordar o loop de controle; veja supervisor_event_fd().
 * @note A inicialização dos backends IPC ainda não está conectada nesta
 * implementação.
 * @par Propriedade
 * O chamador deve chamar supervisor_shutdown() e supervisor_destroy().
 */
mpmc_status_t supervisor_create(supervisor_t **out, size_t producer_count, ipc_mode_t mode);

/**
 * @brief Despacha um comando validado e retorna sem esperar um worker concluir.
 * @param supervisor Instância inicializada.
 * @param command Comando validado para esta instância.
 * @retval MPMC_OK Comando aceito; processo iniciado ou shutdown iniciado.
 * @retval MPMC_ERR_INVALID_ARG Comando não válido.
 * @retval MPMC_ERR_SYS Falha de fork ou outra operação do sistema.
 * @retval MPMC_ERR_SHUTDOWN Instância já está encerrando.
 * @details Cada comando de trabalho recebe um operation_id não nulo antes de
 * fork(); o pai registra PID, tipo e identidade da operação sem executar o
 * trabalho. Até a implementação dos módulos workers/IPC, o filho reporta que
 * a operação não está disponível e termina com erro.
 * @note COMMAND_SHUTDOWN delega a supervisor_shutdown().
 */
mpmc_status_t supervisor_dispatch(supervisor_t *supervisor, const command_t *command);

/**
 * @brief Recolhe filhos encerrados sem bloquear, quando solicitado.
 * @param supervisor Instância viva do supervisor.
 * @param blocking Se false, usa waitpid(..., WNOHANG) repetidamente; se true,
 * espera até não haver mais filhos registrados (utilizar após cancelamento).
 * @retval MPMC_OK Recolhimento realizado ou nenhum filho pendente.
 * @retval MPMC_ERR_SYS Erro irrecuperável em waitpid() ou registro de filho
 * cujo estado já foi coletado por outro processo. Um filho que termina por sinal
 * (por exemplo SIGKILL externo) NÃO é erro: é informado em stderr e o REPL e os
 * demais jobs seguem normalmente.
 * @details EINTR é repetido; término natural, falha e sinal são associados ao
 * PID, tipo de trabalho e operation_id registrados, e cada filho recolhido
 * gera um child_report_t consultável por supervisor_report_get().
 * @note Tratadores SIGCHLD só sinalizam a necessidade de coleta; waitpid()
 * é executado fora do contexto de sinal.
 */
mpmc_status_t supervisor_reap(supervisor_t *supervisor, bool blocking);

/**
 * @brief Impede novos comandos, termina filhos e os recolhe.
 * @param supervisor Supervisor que ainda detém os recursos globais.
 * @retval MPMC_OK Encerramento executado sem falhas relevantes.
 * @retval MPMC_ERR_SYS Algum filho ou recurso não pôde ser encerrado/removido.
 * @details Impede novos comandos, envia SIGTERM aos filhos, executa waitpid()
 * e processa o ponto de integração de eventos. A remoção de recursos IPC e o
 * relatório de métricas dependem dos backends ainda não implementados.
 * @post Nenhum filho registrado permanece ativo.
 * @note Deve ser segura contra chamadas repetidas (idempotência de shutdown).
 */
mpmc_status_t supervisor_shutdown(supervisor_t *supervisor);

/**
 * @brief Retorna quantos filhos já foram recolhidos e possuem relatório.
 * @param supervisor Instância do supervisor; NULL devolve zero.
 * @return Número de relatórios, em ordem de recolhimento.
 */
size_t supervisor_report_count(const supervisor_t *supervisor);

/**
 * @brief Copia o relatório de duração e status de um filho recolhido.
 * @param supervisor Instância do supervisor.
 * @param index Posição em [0, supervisor_report_count()).
 * @param[out] out Recebe o relatório; não é alterado em erro.
 * @retval MPMC_OK Relatório copiado.
 * @retval MPMC_ERR_INVALID_ARG Ponteiro nulo ou índice fora do intervalo.
 * @note Os relatórios permanecem válidos após supervisor_shutdown(), até
 * supervisor_destroy().
 */
mpmc_status_t
supervisor_report_get(const supervisor_t *supervisor, size_t index, child_report_t *out);

/**
 * @brief Imprime o relatório final: duração e status de cada filho recolhido e o
 * tempo total de operação do mpmc.
 * @param supervisor Instância do supervisor.
 * @param out Stream de saída (normalmente stdout).
 * @retval MPMC_OK Relatório escrito e descarregado.
 * @retval MPMC_ERR_INVALID_ARG Ponteiro nulo.
 * @retval MPMC_ERR_SYS Falha ao escrever em `out`.
 * @details Uma linha por filho, na ordem de recolhimento: PID, operação, tipo,
 * término (`código N (sucesso|falha)` ou `sinal N`) e duração em `S.mmm s`. O tempo
 * total vai da criação do supervisor até a chamada, em CLOCK_MONOTONIC. Deve ser
 * chamada após supervisor_shutdown(); filhos ainda ativos são apenas contados.
 */
mpmc_status_t supervisor_print_report(const supervisor_t *supervisor, FILE *out);

/**
 * @brief Informa quantos avisos o supervisor imprimiu desde a última consulta.
 * @param supervisor Instância viva do supervisor; NULL devolve zero.
 * @return Número de mensagens de término de filho escritas em stderr por
 * supervisor_reap(); o contador é zerado pela chamada.
 * @details Permite ao REPL redesenhar o prompt depois de uma saída assíncrona,
 * sem precisar que o usuário pressione Enter.
 */
size_t supervisor_take_notices(supervisor_t *supervisor);

/**
 * @brief Retorna a quantidade de estruturas configuradas no supervisor.
 * @param supervisor Instância inicializada.
 * @return Quantidade configurada, ou zero para NULL.
 */
size_t supervisor_producer_count(const supervisor_t *supervisor);

/**
 * @brief Processa eventos de controle sem bloquear.
 * @param supervisor Instância viva do supervisor.
 * @retval MPMC_OK Eventos drenados ou nenhum evento disponível.
 * @retval MPMC_ERR_SYS Falha ao processar o canal de controle.
 * @details Drena por completo o self-pipe (veja supervisor_event_fd()),
 * descartando os bytes de despertar acumulados por supervisor_notify_signal().
 * Deve ser chamada antes de waitpid(): um sinal que chegue depois da drenagem
 * deixa um novo byte pendente e provoca outra iteração, sem perder o evento.
 * Também é o ponto em que o backend de IPC pode conectar a drenagem de suas
 * filas de eventos.
 */
mpmc_status_t supervisor_poll_events(supervisor_t *supervisor);

/**
 * @brief Retorna o descritor de leitura do canal de eventos (self-pipe).
 * @param supervisor Instância viva do supervisor.
 * @return Descritor não bloqueante para poll()/select() com POLLIN, ou -1 para
 * NULL.
 * @details Fica legível quando supervisor_notify_signal() foi chamada desde a
 * última supervisor_poll_events(). O descritor pertence ao supervisor: o
 * chamador não deve fechá-lo nem lê-lo diretamente.
 */
int supervisor_event_fd(const supervisor_t *supervisor);

/**
 * @brief Acorda o loop de controle a partir de um tratador de sinal.
 * @details Escreve um byte no self-pipe da instância ativa (a criada mais
 * recentemente). É async-signal-safe: usa apenas write(2), não bloqueia e
 * preserva errno. Se o pipe estiver cheio já há despertar pendente, e o byte é
 * descartado. Sem instância ativa não faz nada. Pode ser chamada diretamente
 * do tratador de SIGCHLD.
 * @note O processo filho fecha sua cópia do pipe após fork(), portanto sinais
 * recebidos em workers não acordam o supervisor.
 */
void supervisor_notify_signal(void);

/**
 * @brief Libera memória local do supervisor após o encerramento.
 * @param supervisor Instância encerrada; NULL é aceito (sem efeito).
 * @pre Se não for NULL, supervisor_shutdown() já foi chamado.
 * @note Não substitui supervisor_shutdown() e não pode deixar IPC global órfão.
 */
void supervisor_destroy(supervisor_t *supervisor);

/** @} */
#endif
