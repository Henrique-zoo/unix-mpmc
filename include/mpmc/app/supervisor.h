/**
 * @file supervisor.h
 * @brief Gerenciamento de IPC global, filhos, métricas e encerramento.
 * @ingroup application
 */
#ifndef MPMC_APP_SUPERVISOR_H
#define MPMC_APP_SUPERVISOR_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/types.h>

#include "mpmc/app/command.h"
#include "mpmc/error.h"
#include "mpmc/ipc/transport.h"

/** @ingroup application
 * @{ */

/**
 * @brief Estado opaco do supervisor; existe somente no processo principal.
 * @details Mantém a lista de filhos, os relatórios de término, o identificador
 * incremental de operação, o horário inicial monotônico e o self-pipe.
 * Possui o contexto IPC criado no pai e herdado pelos filhos.
 */
typedef struct supervisor supervisor_t;

/** @brief Operação que originou um diagnóstico durante o shutdown. */
typedef enum {
    SUPERVISOR_SEND_SIGNAL,         /**< Falha em kill(). */
    SUPERVISOR_WAIT_CHILD,          /**< Falha em waitpid() ou estado inesperado. */
    SUPERVISOR_RECORD_REPORT,       /**< Falha ao registrar o relatório do filho. */
    SUPERVISOR_READ_CLOCK,          /**< Falha em clock_gettime(). */
    SUPERVISOR_WAIT_EVENTS,         /**< Falha em poll() ou eventos de erro no pipe. */
    SUPERVISOR_DRAIN_SELF_PIPE,     /**< Falha em read() ou EOF inesperado. */
    SUPERVISOR_POLL_IPC_EVENTS,     /**< Falha ao processar eventos do backend IPC. */
    SUPERVISOR_INSTALL_SIGCHLD,     /**< Falha ao instalar o tratador temporário. */
    SUPERVISOR_UNBLOCK_SIGCHLD,     /**< Falha ao desbloquear SIGCHLD. */
    SUPERVISOR_RESTORE_SIGCHLD,     /**< Falha ao restaurar a ação de SIGCHLD. */
    SUPERVISOR_RESTORE_SIGNAL_MASK, /**< Falha ao restaurar a máscara de sinais. */
    SUPERVISOR_READ_WORKER_RESULT,  /**< Falha ao receber o resultado estruturado do filho. */
    SUPERVISOR_ABANDON_ACK,         /**< Falha ao descartar ACKs de um produtor que não concluiu. */
} supervisor_shutdown_operation_t;

/**
 * @brief Diagnóstico individual de uma falha na coordenação do encerramento.
 * @details operation discrimina os membros da união anônima:
 * - SUPERVISOR_SEND_SIGNAL: send_signal, com PID e sinal enviados a kill().
 * - SUPERVISOR_WAIT_CHILD, SUPERVISOR_RECORD_REPORT e SUPERVISOR_ABANDON_ACK: child_pid.
 * - SUPERVISOR_WAIT_EVENTS: poll_revents; zero se poll() retornou -1, com
 *   errno em result.system_errno, ou os eventos de erro do descritor, com
 *   result.system_errno zero.
 * As demais operações não possuem dados específicos na união. Falhas internas
 * sem errno associado usam result.system_errno zero.
 */
typedef struct {
    supervisor_shutdown_operation_t operation; /**< Operação que falhou. */
    mpmc_result_t result;                      /**< Resultado com status e errno capturados. */

    union {
        struct {
            pid_t child_pid;   /**< PID do filho envolvido na operação que falhou. */
            int signal_number; /**< Sinal enviado em SEND_SIGNAL. */
        } send_signal;

        pid_t child_pid;    /**< Filho cuja coleta ou registro de relatório falhou. */
        short poll_revents; /**< Eventos observados em WAIT_EVENTS. */
    }; /**< Dados específicos da operação. */
} supervisor_shutdown_error_t;

/**
 * @brief Recebe um diagnóstico individual de falha durante o shutdown.
 * @param error Diagnóstico válido somente durante esta chamada.
 * @param user_data Dados fornecidos pelo chamador, sem interpretação pelo
 * supervisor; pode ser NULL se a callback não precisar de estado adicional.
 * @details A callback é executada sincronamente, no fluxo normal do shutdown,
 * fora dos tratadores de sinais, uma vez para cada falha reportada.
 *
 * O campo operation determina qual membro da união anônima pode ser acessado.
 * Para armazenar o diagnóstico, copie o conteúdo de *error, não seu ponteiro.
 * A cópia contém apenas valores e não exige liberação adicional.
 *
 * A callback deve retornar para permitir a continuidade do encerramento.
 * Seu retorno void não substitui o primeiro erro preservado como resultado
 * do shutdown nem solicita a interrupção das demais tentativas de limpeza.
 * @note O supervisor não adquire a propriedade de user_data. Esses dados devem
 * permanecer válidos durante toda a operação que utiliza a callback.
 * @warning A callback não deve reentrar nas operações de coordenação nem destruir
 * a instância do supervisor em uso.
 */
typedef void (*supervisor_shutdown_error_fn)(
    const supervisor_shutdown_error_t *error,
    void *user_data
);

/**
 * @brief Destino opcional dos diagnósticos individuais de coordenação.
 * @details O supervisor não retém este objeto nem adquire propriedade de
 * user_data. Ambos devem permanecer válidos durante a chamada que os recebe.
 * Reporter nulo ou on_error nulo desabilita a callback, preservando o resultado.
 * Cada falha é reportada no ponto de origem; sua propagação entre funções não
 * gera outra callback. O primeiro erro cronológico é preservado para o retorno.
 */
typedef struct {
    supervisor_shutdown_error_fn on_error; /**< Função chamada para cada falha reportável. */
    void *user_data; /**< Dados pertencentes ao chamador, repassados sem interpretação. */
} supervisor_shutdown_reporter_t;

/** @brief Forma como um filho supervisionado terminou. */
typedef enum {
    CHILD_END_EXITED,   /**< Saiu com exit(); `value` é o código de saída. */
    CHILD_END_SIGNALED, /**< Morto por sinal; `value` é o número do sinal. */
    CHILD_END_LOST      /**< Estado indisponível (ECHILD/inesperado); `value` é zero. */
} child_end_t;

/**
 * @brief Relatório imutável de um filho já recolhido.
 * @details Permite apresentar depois a duração e o status de cada processo.
 * operation_id identifica o trabalho do filho, enquanto pid identifica o
 * processo físico. Em relatórios de produtores, esses valores correspondem,
 * respectivamente, ao operation_id e sender_pid das mensagens emitidas. Um
 * relatório de consumidor descreve o consumidor e não reescreve a identidade
 * das mensagens que ele retirou.
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
 * @brief Cria o estado de supervisão, o self-pipe e o contexto IPC global.
 * @param out Recebe a instância alocada; em falha recebe NULL.
 * @param producer_count Quantidade de estruturas lógicas, estritamente positiva.
 * @param mode Paradigma IPC_SHARED_MEMORY (1) ou IPC_MESSAGE_QUEUE (2).
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: estado local, self-pipe e contexto IPC criados; instância escrita
 *   em out e system_errno zero.
 * - MPMC_ERR_INVALID_ARG: out nulo, producer_count zero ou paradigma diferente de 1/2;
 *   system_errno é zero. Também propaga argumentos rejeitados por ipc_init(),
 *   preservando system_errno conforme o contrato dessa função.
 * - MPMC_ERR_SYS: falha em calloc(), clock_gettime(), pipe() ou fcntl(); system_errno contém o
 *   errno capturado na operação que falhou, preservado durante a limpeza;
 *   também propaga falhas de alocação e de sistema de ipc_init().
 * - MPMC_ERR_SHUTDOWN: inicialização IPC encontrou recurso removido;
 *   system_errno preserva o errno retornado por ipc_init().
 * @details Também cria o canal de eventos (self-pipe) não bloqueante usado para
 * acordar o loop de controle; veja supervisor_event_fd().
 * @post Em falha, *out permanece NULL quando out é válido e os recursos locais
 * parciais são liberados. O rollback de IPC cabe a ipc_init(); o supervisor não
 * destrói um contexto que não chegou a adquirir.
 * @par Propriedade
 * O chamador deve chamar supervisor_shutdown() e supervisor_destroy().
 */
mpmc_result_t supervisor_create(supervisor_t **out, size_t producer_count, ipc_mode_t mode);

/**
 * @brief Despacha um comando validado e retorna sem esperar um worker concluir.
 * @param supervisor Instância inicializada.
 * @param command Comando validado para esta instância.
 * @param reporter Destino dos diagnósticos do comando shutdown; pode ser NULL.
 * Nos comandos de trabalho, não é utilizado.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: filho criado e registrado, ou shutdown concluído; system_errno é zero. O sucesso do
 *   despacho não implica sucesso do trabalho do filho.
 * - MPMC_ERR_INVALID_ARG: supervisor ou command nulo, ou tipo de comando desconhecido,
 *   com system_errno zero; no comando shutdown, também propaga rejeições da
 *   drenagem de eventos descritas em supervisor_shutdown().
 * - MPMC_ERR_SHUTDOWN: instância já marcada como encerrando, com system_errno zero;
 *   ou falha de remoção da fila de eventos propagada pelo shutdown, com errno preservado.
 * - MPMC_ERR_STATE: identificadores de operação esgotados ou evento inconsistente
 *   propagado pelo shutdown; system_errno é zero.
 * - MPMC_ERR_SYS: falha em realloc(), clock_gettime(), sigprocmask() ou fork(); no comando
 *   shutdown, propaga as falhas descritas em supervisor_shutdown(). system_errno
 *   contém o errno capturado na operação que falhou. Em overflow de capacidade ou
 *   falha interna propagada pelo shutdown, esse campo é zero.
 * @details Cada comando de trabalho recebe um operation_id não nulo antes de
 * fork(); o pai registra PID, tipo e identidade da operação sem executar o
 * trabalho. O filho executa producer_run_with_result() ou consumer_run(),
 * fecha seu contexto local e transmite o resultado por um pipe próprio.
 * @note COMMAND_SHUTDOWN delega a supervisor_shutdown(), inclusive em chamadas
 * repetidas. Novos trabalhos continuam recusados após início do encerramento.
 */
mpmc_result_t supervisor_dispatch(
    supervisor_t *supervisor,
    const command_t *command,
    const supervisor_shutdown_reporter_t *reporter
);

/**
 * @brief Recolhe filhos encerrados sem bloquear, quando solicitado.
 * @param supervisor Instância viva do supervisor.
 * @param blocking Se false, usa waitpid(..., WNOHANG) repetidamente; se true,
 * espera até não haver mais filhos registrados (utilizar após cancelamento).
 * @param reporter Destino opcional das falhas de coleta, registro e eventos;
 * pode ser NULL, inclusive nas coletas normais fora do shutdown.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: verificação/coleta concluída ou nenhum filho pronto para coleta; system_errno é
 *   zero.
 * - MPMC_ERR_INVALID_ARG: supervisor nulo, com system_errno zero; ou msgrcv()
 *   rejeitou argumentos na drenagem de eventos, com errno em system_errno.
 * - MPMC_ERR_STATE: evento de consumo inconsistente; system_errno é zero.
 * - MPMC_ERR_SHUTDOWN: msgrcv() encontrou a fila de eventos removida;
 *   system_errno contém EIDRM capturado nessa chamada.
 * - MPMC_ERR_SYS: falha em waitpid(), read(), malloc() ou msgrcv() na coleta ou
 *   drenagem de eventos, com o errno capturado
 *   em system_errno. O campo é zero para EOF inesperado no pipe, estado de término inesperado ou
 *   falta de capacidade para registrar o relatório.
 *
 * ECHILD é preservado em system_errno quando o estado de um filho registrado não está mais
 * disponível. EINTR em waitpid() ou read() é repetido. O término do filho por código de falha ou
 * sinal, por si só, não altera o status da coleta para erro.
 * @details EINTR é repetido; término natural, falha e sinal são associados ao
 * PID, tipo de trabalho e operation_id registrados, e cada filho recolhido
 * gera um child_report_t consultável por supervisor_report_get().
 * @note Tratadores SIGCHLD só sinalizam a necessidade de coleta; waitpid()
 * é executado fora do contexto de sinal.
 * @note O retorno preserva o primeiro erro, inclusive se a drenagem de eventos
 * falhar antes da coleta. Cada falha individual também chega ao reporter.
 * Falha na medição da duração mantém o fallback de duração zero; não é tratada
 * como erro de coleta nem como diagnóstico de shutdown.
 */
mpmc_result_t supervisor_reap(
    supervisor_t *supervisor,
    bool blocking,
    const supervisor_shutdown_reporter_t *reporter
);

/**
 * @brief Impede novos comandos, termina filhos e os recolhe.
 * @param supervisor Supervisor que ainda detém os recursos globais.
 * @param reporter Destino opcional de todas as falhas individuais; pode ser NULL.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: encerramento e coleta concluídos sem falhas reportadas, inclusive em chamadas
 *   repetidas e após SIGKILL por vencimento do prazo; system_errno é zero.
 * - MPMC_ERR_INVALID_ARG: supervisor nulo, com system_errno zero; ou msgrcv()
 *   rejeitou argumentos ao drenar eventos, com errno preservado em system_errno.
 * - MPMC_ERR_STATE: evento de consumo inconsistente no backend de filas;
 *   system_errno é zero.
 * - MPMC_ERR_SHUTDOWN: msgrcv() encontrou a fila de eventos removida;
 *   system_errno contém EIDRM capturado nessa chamada.
 * - MPMC_ERR_SYS: falha em clock_gettime(), sigaction(), sigprocmask(), poll(),
 *   kill(), waitpid(), read(), malloc() ou msgrcv(), com o errno capturado em
 *   system_errno. ESRCH de kill() é ignorado. Falhas internas da coleta e eventos
 *   de erro/EOF no self-pipe usam system_errno zero.
 * @details Impede novos comandos, envia SIGTERM aos filhos e aguarda um prazo
 * comum de dois segundos em CLOCK_MONOTONIC, usando poll() no self-pipe e coleta
 * não bloqueante. Despertares e EINTR não reiniciam o prazo. Ao vencer o prazo,
 * envia SIGKILL aos restantes e espera sua coleta. Os dois segundos limitam a
 * tolerância ao SIGTERM, não a duração da coleta após o encerramento forçado.
 * A espera configura temporariamente SIGCHLD e restaura a ação e a máscara do
 * chamador, sem depender dos tratadores do REPL. Falhas antecipam o encerramento
 * forçado e o primeiro erro prevalece. O contexto IPC permanece pertencendo ao
 * supervisor; sua destruição ocorre em supervisor_destroy(), após a coleta.
 * @post Em sucesso, todos os filhos registrados foram recolhidos.
 * @note Deve ser segura contra chamadas repetidas (idempotência de shutdown).
 * @note O reporter não modifica o resultado da operação. Sinalizar um PID já
 * inexistente (ESRCH), EINTR repetido e vencimento do prazo não geram callbacks.
 * @warning Se SIGKILL falhar para um filho, a coleta final não bloqueia e podem
 * restar filhos registrados. O chamador deve resolver a falha e repetir o shutdown
 * antes de destruir a instância ou seus recursos globais.
 */
mpmc_result_t supervisor_shutdown(
    supervisor_t *supervisor,
    const supervisor_shutdown_reporter_t *reporter
);

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
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: relatório selecionado copiado para out.
 * - MPMC_ERR_INVALID_ARG: supervisor ou out nulo, ou index fora dos relatórios disponíveis.
 *
 * system_errno é zero em todos os resultados.
 * @note Os relatórios permanecem válidos após supervisor_shutdown(), até
 * supervisor_destroy().
 */
mpmc_result_t supervisor_report_get(
    const supervisor_t *supervisor,
    size_t index,
    child_report_t *out
);

/**
 * @brief Imprime o relatório final: duração e status de cada filho recolhido e o
 * tempo total de operação do mpmc.
 * @param supervisor Instância do supervisor.
 * @param out Stream de saída (normalmente stdout).
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: relatório escrito e stream descarregado; system_errno é zero.
 * - MPMC_ERR_INVALID_ARG: supervisor ou out nulo; system_errno é zero.
 * - MPMC_ERR_SYS: falha em fputs(), fprintf() ou fflush(), com o errno capturado em
 *   system_errno. Se apenas ferror() indicar erro no stream após o descarregamento, system_errno
 *   é zero.
 *
 * Falha em clock_gettime() faz o tempo total informado ser zero; essa falha não é retornada como
 * MPMC_ERR_SYS.
 * @details Uma linha por filho, na ordem de recolhimento: PID, operação, tipo,
 * término (`código N (sucesso|falha)` ou `sinal N`) e duração em `S.mmm s`. O tempo
 * total vai da criação do supervisor até a chamada, em CLOCK_MONOTONIC. Deve ser
 * chamada após supervisor_shutdown(); filhos ainda ativos são apenas contados.
 */
mpmc_result_t supervisor_print_report(const supervisor_t *supervisor, FILE *out);

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
 * @param reporter Destino opcional dos erros de leitura do self-pipe e da
 * drenagem de eventos IPC; pode ser NULL.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: self-pipe e backend IPC drenados; EAGAIN/EWOULDBLOCK encerra somente
 *   a leitura do pipe, e system_errno é zero.
 * - MPMC_ERR_INVALID_ARG: supervisor nulo, com system_errno zero; ou msgrcv()
 *   rejeitou argumentos na fila de eventos, com errno em system_errno.
 * - MPMC_ERR_STATE: evento de consumo inconsistente; system_errno é zero.
 * - MPMC_ERR_SHUTDOWN: msgrcv() encontrou a fila de eventos removida;
 *   system_errno contém EIDRM capturado nessa chamada.
 * - MPMC_ERR_SYS: falha em read(), malloc() ou msgrcv(), com o errno capturado em
 *   system_errno, ou EOF inesperado no self-pipe, com system_errno zero.
 *
 * EINTR em read() é repetido e não é retornado como MPMC_ERR_INTERRUPTED.
 * @details Drena por completo o self-pipe (veja supervisor_event_fd()),
 * descartando os bytes de despertar acumulados por supervisor_notify_signal().
 * Deve ser chamada antes de waitpid(): um sinal que chegue depois da drenagem
 * deixa um novo byte pendente e provoca outra iteração, sem perder o evento.
 * Ao esgotar o pipe, chama ipc_poll_events() para consolidar eventos do backend.
 */
mpmc_result_t supervisor_poll_events(
    supervisor_t *supervisor,
    const supervisor_shutdown_reporter_t *reporter
);

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
