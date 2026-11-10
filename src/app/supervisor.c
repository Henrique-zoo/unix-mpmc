/**
 * @file supervisor.c
 * @brief Implementação do ciclo de vida de filhos e canal de despertar.
 * @ingroup application
 *
 * @details Mantém os processos criados pelo REPL, recolhe filhos encerrados,
 * inicia shutdown e fornece um self-pipe não bloqueante para acordar o loop de
 * controle em resposta a SIGCHLD. A integração dos backends IPC permanece
 * como ponto de extensão em supervisor_poll_events().
 */
#include "mpmc/app/supervisor.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <poll.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "job_outcome.h"
#include "mpmc/diagnostic.h"
#include "mpmc/workers/consumer.h"
#include "mpmc/workers/producer.h"
#include "worker_event.h"

/** @brief Capacidade da fila de eventos de resultado pendentes de apresentação. */
static constexpr size_t SUPERVISOR_MAX_PENDING_OUTCOMES = 32;

/** @brief Registro de identidade e estado de um processo filho supervisionado. */
typedef struct {
    pid_t pid;               /**< PID retornado por fork(). */
    command_kind_t kind;     /**< Tipo de trabalho executado pelo filho. */
    uint64_t operation_id;   /**< Identidade única atribuída antes de fork(). */
    struct timespec started; /**< Instante CLOCK_MONOTONIC capturado antes de fork(). */
    int result_fd;           /**< Leitura não bloqueante do resultado deste filho. */
    size_t result_bytes;     /**< Bytes recebidos do registro atômico. */
    worker_event_t event;    /**< Frame atualmente recebido. */
    job_outcome_t outcome;   /**< Resultado transmitido pelo filho. */
    bool reporting_failure;  /**< Está comunicando falhas e fechando seu contexto. */
    bool outcome_received;   /**< Resultado validado e apresentado. */
} child_record_t;

/** @brief Estado privado mantido exclusivamente pelo processo supervisor. */
struct supervisor {
    size_t producer_count; /**< Quantidade de estruturas configuradas. */
    ipc_context_t *ipc;    /**< Recursos IPC globais pertencentes ao supervisor. */

    child_record_t *children; /**< Vetor dinâmico de filhos ativos. */
    size_t child_count;       /**< Número de registros ocupados. */
    size_t child_capacity;    /**< Capacidade alocada do vetor. */

    struct timespec created_at; /**< Instante CLOCK_MONOTONIC da criação do supervisor. */
    child_report_t *reports;    /**< Filhos já recolhidos, na ordem de recolhimento. */
    size_t report_count;        /**< Número de relatórios armazenados. */
    size_t report_capacity;     /**< Capacidade alocada do vetor de relatórios. */

    uint64_t next_operation_id; /**< Próximo identificador; zero indica esgotamento. */
    bool compromised;           /**< Falha fatal observada em um worker. */
    bool shutting_down;         /**< Impede novos despachos após shutdown. */

    int event_pipe[2];   /**< Self-pipe: leitura em [0], escrita em [1]. */
    size_t notice_count; /**< Avisos impressos em stderr desde a última consulta. */

    job_outcome_t pending_outcomes[SUPERVISOR_MAX_PENDING_OUTCOMES]; /**< Eventos a apresentar. */
    size_t pending_outcome_count; /**< Eventos ocupados em pending_outcomes. */
};

/** @brief Descritor usado pelo tratador para acordar o loop ativo. */
static volatile sig_atomic_t wake_write_fd = -1;

/** @brief Prazo comum, em segundos, para os filhos atenderem SIGTERM. */
static constexpr time_t SHUTDOWN_GRACE_SECONDS = 2;

static mpmc_result_t signal_children(
    supervisor_t *supervisor,
    int signal_number,
    const supervisor_shutdown_reporter_t *reporter
);

static void notify_shutdown_child(int signal_number);

static mpmc_result_t wait_children_until(
    supervisor_t *supervisor,
    const struct timespec *deadline,
    const supervisor_shutdown_reporter_t *reporter
);

static void record_shutdown_error(
    mpmc_result_t *first,
    const supervisor_shutdown_error_t *error,
    const supervisor_shutdown_reporter_t *reporter
);

static mpmc_result_t open_result_pipe(int descriptors[2]);
static void close_descriptor(int fd);
static void cancel_worker(int signal_number);
static void run_worker(
    supervisor_t *supervisor,
    const command_t *command,
    uint64_t operation_id,
    int result_fd
);
static mpmc_result_t drain_worker_results(
    supervisor_t *supervisor,
    const supervisor_shutdown_reporter_t *reporter
);

/**
 * @brief Define um descritor como não bloqueante e close-on-exec.
 * @param fd Descritor válido a configurar.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: flags O_NONBLOCK e FD_CLOEXEC aplicadas; system_errno é zero.
 * - MPMC_ERR_SYS: falha em fcntl() ao consultar ou alterar flags; system_errno contém o errno
 *   capturado nessa falha.
 */
static mpmc_result_t set_nonblocking_cloexec(int fd) {
    int flags = fcntl(fd, F_GETFL);

    if (flags == -1 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        return (mpmc_result_t){
            .status = MPMC_ERR_SYS,
            .system_errno = errno,
        };
    }

    flags = fcntl(fd, F_GETFD);

    if (flags == -1 || fcntl(fd, F_SETFD, flags | FD_CLOEXEC) == -1) {
        return (mpmc_result_t){
            .status = MPMC_ERR_SYS,
            .system_errno = errno,
        };
    }

    return (mpmc_result_t){.status = MPMC_OK};
}

/**
 * @brief Move um descritor para fora da faixa 0..2 (stdin/stdout/stderr).
 * @param fd Descritor a mover; atualizado em caso de sucesso.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: descritor já acima de 2 ou duplicado e atualizado em fd; system_errno é zero.
 * - MPMC_ERR_SYS: falha em fcntl(F_DUPFD_CLOEXEC); system_errno contém o errno capturado nessa
 *   falha.
 * @details Se o processo iniciou com stdin fechado, pipe() devolveria o fd 0 e o
 * REPL passaria a ler do próprio self-pipe, sem nunca observar EOF.
 */
static mpmc_result_t move_above_stdio(int *fd) {
    if (*fd > STDERR_FILENO) return (mpmc_result_t){.status = MPMC_OK};

    int moved = fcntl(*fd, F_DUPFD_CLOEXEC, STDERR_FILENO + 1);

    if (moved == -1) {
        return (mpmc_result_t){
            .status = MPMC_ERR_SYS,
            .system_errno = errno,
        };
    }

    close_descriptor(*fd);
    *fd = moved;

    return (mpmc_result_t){.status = MPMC_OK};
}

/**
 * @brief Fecha as extremidades do self-pipe e invalida o descritor global.
 * @param supervisor Estado cujo canal será fechado; não pode ser nulo.
 * @details Só invalida wake_write_fd quando ele corresponde à extremidade
 * de escrita desta instância.
 */
static void close_event_pipe(supervisor_t *supervisor) {
    if (wake_write_fd == supervisor->event_pipe[1]) {
        wake_write_fd = -1;
    }

    for (size_t end = 0; end < 2; ++end) {
        if (supervisor->event_pipe[end] != -1) {
            close_descriptor(supervisor->event_pipe[end]);
            supervisor->event_pipe[end] = -1;
        }
    }
}

/**
 * @brief Cria e configura o canal não bloqueante de despertar.
 * @param supervisor Instância a receber os descritores.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: pipe criado, descritores configurados e canal de despertar registrado; system_errno
 *   é zero.
 * - MPMC_ERR_SYS: falha em pipe() ou fcntl(); system_errno contém o errno capturado na operação
 *   que falhou e é preservado ao fechar os descritores parciais.
 * @post Em falha, os descritores parcialmente criados são fechados.
 */
static mpmc_result_t open_event_pipe(supervisor_t *supervisor) {
    supervisor->event_pipe[0] = -1;
    supervisor->event_pipe[1] = -1;

    if (pipe(supervisor->event_pipe) == -1) {
        int pipe_errno = errno;

        supervisor->event_pipe[0] = -1;
        supervisor->event_pipe[1] = -1;

        return (mpmc_result_t){
            .status = MPMC_ERR_SYS,
            .system_errno = pipe_errno,
        };
    }

    for (size_t end = 0; end < 2; ++end) {
        mpmc_result_t error = move_above_stdio(&supervisor->event_pipe[end]);

        if (error.status != MPMC_OK) {
            close_event_pipe(supervisor);

            return error;
        }
    }

    for (size_t end = 0; end < 2; ++end) {
        mpmc_result_t error = set_nonblocking_cloexec(supervisor->event_pipe[end]);

        if (error.status != MPMC_OK) {
            close_event_pipe(supervisor);

            return error;
        }
    }

    wake_write_fd = supervisor->event_pipe[1];

    return (mpmc_result_t){.status = MPMC_OK};
}

/**
 * @brief Garante espaço para registrar mais um filho.
 * @param supervisor Instância cujo vetor pode crescer.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: vetor de filhos já comporta outro registro ou foi ampliado; system_errno é zero.
 * - MPMC_ERR_SYS: overflow no cálculo da capacidade, com system_errno zero, ou falha em
 *   realloc(), com o errno capturado nessa falha em system_errno.
 */
static mpmc_result_t ensure_child_capacity(supervisor_t *supervisor) {
    if (supervisor->child_count < supervisor->child_capacity) {
        return (mpmc_result_t){.status = MPMC_OK};
    }

    size_t capacity = supervisor->child_capacity == 0 ? 8 : supervisor->child_capacity * 2;

    if (capacity < supervisor->child_capacity
        || capacity > SIZE_MAX / sizeof(*supervisor->children)) {
        return (mpmc_result_t){.status = MPMC_ERR_SYS};
    }

    child_record_t *children = realloc(supervisor->children, capacity * sizeof(*children));

    if (children == nullptr) {
        return (mpmc_result_t){
            .status = MPMC_ERR_SYS,
            .system_errno = errno,
        };
    }

    supervisor->children = children;
    supervisor->child_capacity = capacity;

    return (mpmc_result_t){.status = MPMC_OK};
}

/**
 * @brief Garante espaço para os relatórios de todos os filhos ativos e de mais um.
 * @param supervisor Instância cujo vetor de relatórios pode crescer.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: vetor de relatórios comporta os relatórios existentes, os filhos ativos e outro
 *   filho; system_errno é zero.
 * - MPMC_ERR_SYS: overflow no cálculo da quantidade/capacidade, com system_errno zero, ou falha
 *   em realloc(), com o errno capturado nessa falha em system_errno.
 * @details Chamada antes de fork(): assim, o recolhimento de qualquer filho
 * registrado nunca precisa alocar memória e não perde o estado final.
 */
static mpmc_result_t ensure_report_capacity(supervisor_t *supervisor) {
    if (supervisor->report_count > SIZE_MAX - supervisor->child_count - 1) {
        return (mpmc_result_t){.status = MPMC_ERR_SYS};
    }

    const size_t needed = supervisor->report_count + supervisor->child_count + 1;

    if (needed <= supervisor->report_capacity) return (mpmc_result_t){.status = MPMC_OK};

    size_t capacity = supervisor->report_capacity == 0 ? 8 : supervisor->report_capacity;

    while (capacity < needed) {
        if (capacity > SIZE_MAX / 2) return (mpmc_result_t){.status = MPMC_ERR_SYS};

        capacity *= 2;
    }

    if (capacity > SIZE_MAX / sizeof(*supervisor->reports)) {
        return (mpmc_result_t){.status = MPMC_ERR_SYS};
    }

    child_report_t *reports = realloc(supervisor->reports, capacity * sizeof(*reports));

    if (reports == nullptr) {
        return (mpmc_result_t){
            .status = MPMC_ERR_SYS,
            .system_errno = errno,
        };
    }

    supervisor->reports = reports;
    supervisor->report_capacity = capacity;

    return (mpmc_result_t){.status = MPMC_OK};
}

/** @details Prepara a lista local de filhos e o self-pipe antes de inicializar
 * o contexto IPC. Em falha de ipc_init(), libera o pipe e o estado local;
 * o rollback dos objetos globais pertence à inicialização do backend.
 */
mpmc_result_t supervisor_create(supervisor_t **out, size_t producer_count, ipc_mode_t mode) {
    mpmc_diagnostic_scope_t diagnostic_scope;
    mpmc_diagnostic_begin(&diagnostic_scope, __func__, false, false);

    if (out == nullptr)
        return mpmc_diagnostic_end(
            &diagnostic_scope, (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG}
        );

    *out = nullptr;

    if (producer_count == 0 || (mode != IPC_SHARED_MEMORY && mode != IPC_MESSAGE_QUEUE)) {
        return mpmc_diagnostic_end(
            &diagnostic_scope, (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG}
        );
    }

    supervisor_t *supervisor = calloc(1, sizeof(*supervisor));

    if (supervisor == nullptr) {
        return mpmc_diagnostic_end(
            &diagnostic_scope,
            (mpmc_result_t){
                .status = MPMC_ERR_SYS,
                .system_errno = errno,
            }
        );
    }

    if (clock_gettime(CLOCK_MONOTONIC, &supervisor->created_at) == -1) {
        int clock_errno = errno;

        free(supervisor);

        return mpmc_diagnostic_end(
            &diagnostic_scope,
            (mpmc_result_t){
                .status = MPMC_ERR_SYS,
                .system_errno = clock_errno,
            }
        );
    }

    supervisor->producer_count = producer_count;
    supervisor->next_operation_id = 1;
    mpmc_result_t error = open_event_pipe(supervisor);

    if (error.status != MPMC_OK) {
        free(supervisor);

        return mpmc_diagnostic_end(&diagnostic_scope, error);
    }

    error = ipc_init(&supervisor->ipc, producer_count, mode);

    if (error.status != MPMC_OK) {
        close_event_pipe(supervisor);
        free(supervisor);

        return mpmc_diagnostic_end(&diagnostic_scope, error);
    }
    *out = supervisor;

    return mpmc_diagnostic_end(&diagnostic_scope, (mpmc_result_t){.status = MPMC_OK});
}

/** @details Copia o evento para a fila interna; a impressão ocorre em supervisor_reap(). */
mpmc_result_t supervisor_queue_outcome(supervisor_t *supervisor, const job_outcome_t *outcome) {
    mpmc_diagnostic_scope_t diagnostic_scope;
    mpmc_diagnostic_begin(&diagnostic_scope, __func__, false, false);

    if (supervisor == nullptr || outcome == nullptr) {
        return mpmc_diagnostic_end(
            &diagnostic_scope, (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG}
        );
    }
    if (job_outcome_format(outcome, nullptr, 0) < 0) {
        return mpmc_diagnostic_end(
            &diagnostic_scope, (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG}
        );
    }
    if (supervisor->pending_outcome_count >= SUPERVISOR_MAX_PENDING_OUTCOMES) {
        return mpmc_diagnostic_end(&diagnostic_scope, (mpmc_result_t){.status = MPMC_ERR_NO_SPACE});
    }
    supervisor->pending_outcomes[supervisor->pending_outcome_count++] = *outcome;
    return mpmc_diagnostic_end(&diagnostic_scope, (mpmc_result_t){.status = MPMC_OK});
}

/**
 * @brief Imprime em stderr os eventos de resultado pendentes e esvazia a fila.
 * @param supervisor Instância ativa.
 * @details Cada evento vira uma única linha, escrita de uma só vez, para que
 * linhas de eventos concorrentes não se misturem. Cada linha conta como aviso, o
 * que faz o REPL redesenhar o prompt em terminais.
 */
static void flush_pending_outcomes(supervisor_t *supervisor) {
    for (size_t index = 0; index < supervisor->pending_outcome_count; ++index) {
        char text[512];
        int length = job_outcome_format(&supervisor->pending_outcomes[index], text, sizeof text);
        if (length < 0) {
            continue;
        }
        char line[sizeof text + 16];
        (void)snprintf(line, sizeof line, "mpmc: %s\n", text);
        (void)fputs(line, stderr);
        ++supervisor->notice_count;
    }
    supervisor->pending_outcome_count = 0;
}

/** @details Devolve e zera o contador incrementado por supervisor_reap() a cada
 * mensagem de término de filho impressa em stderr.
 */
size_t supervisor_take_notices(supervisor_t *supervisor) {
    if (supervisor == nullptr) return 0;

    size_t count = supervisor->notice_count;

    supervisor->notice_count = 0;

    return count;
}

/** @details Consulta simples do valor armazenado na criação da instância. */
size_t supervisor_producer_count(const supervisor_t *supervisor) {
    return supervisor == nullptr ? 0 : supervisor->producer_count;
}

/** @details Expõe o descritor para que o REPL o inclua em poll(). */
int supervisor_event_fd(const supervisor_t *supervisor) {
    return supervisor == nullptr ? -1 : supervisor->event_pipe[0];
}

/** @details Pode ser chamada em tratador de sinal: usa write() no descritor
 * não bloqueante, preserva errno e descarta falha por pipe cheio, pois nesse
 * caso já existe um despertar pendente. O contrato público está em supervisor.h.
 */
void supervisor_notify_signal(void) {
    int saved_errno = errno;
    int fd = wake_write_fd;

    if (fd != -1) {
        const unsigned char wake = 1;
        ssize_t written;

        do {
            written = write(fd, &wake, sizeof(wake));
        } while (written == -1 && errno == EINTR);
    }

    errno = saved_errno;
}

/** @details Os bytes são notificações agregadas sem payload. Uma rejeição na
 * leitura e uma falha no backend recebem operações de diagnóstico distintas;
 * o resultado já reportado é apenas propagado aos chamadores.
 */
mpmc_result_t supervisor_poll_events(
    supervisor_t *supervisor,
    const supervisor_shutdown_reporter_t *reporter
) {
    mpmc_diagnostic_scope_t diagnostic_scope;
    mpmc_diagnostic_begin(&diagnostic_scope, __func__, false, false);

    if (supervisor == nullptr)
        return mpmc_diagnostic_end(
            &diagnostic_scope, (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG}
        );

    mpmc_result_t result = {.status = MPMC_OK};
    unsigned char sink[64];

    for (;;) {
        ssize_t count = read(supervisor->event_pipe[0], sink, sizeof(sink));

        if (count > 0) {
            continue;
        }

        int read_errno = count == 0 ? 0 : errno;

        if (read_errno == EINTR) {
            continue;
        }

        if (read_errno == EAGAIN || read_errno == EWOULDBLOCK) {
            result = drain_worker_results(supervisor, reporter);
            const supervisor_shutdown_error_t error = {
                .operation = SUPERVISOR_POLL_IPC_EVENTS,
                .result = ipc_poll_events(supervisor->ipc),
            };

            record_shutdown_error(&result, &error, reporter);

            return mpmc_diagnostic_end(&diagnostic_scope, result);
        }

        const supervisor_shutdown_error_t error = {
            .operation = SUPERVISOR_DRAIN_SELF_PIPE,
            .result = {.status = MPMC_ERR_SYS, .system_errno = read_errno},
        };

        record_shutdown_error(&result, &error, reporter);

        return mpmc_diagnostic_end(&diagnostic_scope, result);
    }
}

/**
 * @brief Restaura ações padrão de sinais no processo filho.
 * @return true se SIGCHLD, SIGINT e SIGTERM foram configurados com SIG_DFL;
 * false na primeira falha de sigaction().
 * @details Remove os tratadores herdados do REPL para que sinais de término,
 * em particular SIGTERM, tenham o comportamento esperado no worker.
 * @note Não altera a máscara de sinais. O chamador a restaura separadamente.
 * Em falha, ações anteriores já restauradas não são revertidas e errno é o da
 * sigaction() que falhou.
 */
static bool reset_child_signals(void) {
    static const int signals[] = {SIGCHLD, SIGINT, SIGTERM};
    struct sigaction action = {};

    action.sa_handler = SIG_DFL;
    sigemptyset(&action.sa_mask);

    for (size_t index = 0; index < sizeof(signals) / sizeof(signals[0]); ++index) {
        if (sigaction(signals[index], &action, nullptr) == -1) return false;
    }

    return true;
}

/** @details O pai registra PID, tipo e identidade da operação sem aguardar; o
 * filho recebe uma cópia imutável do comando, fecha o self-pipe e restaura os sinais.
 * Os sinais de término ficam bloqueados durante o fork() e só são liberados no
 * filho depois de restaurar as ações padrão, evitando perder um SIGTERM de
 * shutdown enviado imediatamente após o despacho.
 */
mpmc_result_t supervisor_dispatch(
    supervisor_t *supervisor,
    const command_t *command,
    const supervisor_shutdown_reporter_t *reporter
) {
    mpmc_diagnostic_scope_t diagnostic_scope;
    mpmc_diagnostic_begin(&diagnostic_scope, __func__, false, false);

    if (supervisor == nullptr || command == nullptr) {
        return mpmc_diagnostic_end(
            &diagnostic_scope, (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG}
        );
    }

    if (command->kind == COMMAND_SHUTDOWN)
        return mpmc_diagnostic_end(&diagnostic_scope, supervisor_shutdown(supervisor, reporter));

    if (supervisor->shutting_down)
        return mpmc_diagnostic_end(&diagnostic_scope, (mpmc_result_t){.status = MPMC_ERR_SHUTDOWN});

    if (command->kind != COMMAND_PRODUCE && command->kind != COMMAND_CONSUME) {
        return mpmc_diagnostic_end(
            &diagnostic_scope, (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG}
        );
    }

    if (supervisor->next_operation_id == 0)
        return mpmc_diagnostic_end(&diagnostic_scope, (mpmc_result_t){.status = MPMC_ERR_STATE});

    mpmc_result_t error = ensure_child_capacity(supervisor);

    if (error.status != MPMC_OK) return mpmc_diagnostic_end(&diagnostic_scope, error);

    error = ensure_report_capacity(supervisor);

    if (error.status != MPMC_OK) return mpmc_diagnostic_end(&diagnostic_scope, error);

    struct timespec started;

    if (clock_gettime(CLOCK_MONOTONIC, &started) == -1) {
        return mpmc_diagnostic_end(
            &diagnostic_scope,
            (mpmc_result_t){
                .status = MPMC_ERR_SYS,
                .system_errno = errno,
            }
        );
    }

    int result_pipe[2];
    error = open_result_pipe(result_pipe);
    if (error.status != MPMC_OK) return mpmc_diagnostic_end(&diagnostic_scope, error);

    sigset_t block_set;
    sigset_t old_set;

    sigemptyset(&block_set);
    sigaddset(&block_set, SIGCHLD);
    sigaddset(&block_set, SIGINT);
    sigaddset(&block_set, SIGTERM);

    if (sigprocmask(SIG_BLOCK, &block_set, &old_set) == -1) {
        int saved_errno = errno;
        mpmc_diagnostic_cause(
            "sigprocmask()", (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = saved_errno}
        );
        close_descriptor(result_pipe[0]);
        close_descriptor(result_pipe[1]);
        return mpmc_diagnostic_end(
            &diagnostic_scope, (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = saved_errno}
        );
    }

    const uint64_t operation_id = supervisor->next_operation_id;
    pid_t child = fork();

    if (child == -1) {
        int fork_error = errno;
        mpmc_diagnostic_cause(
            "fork()", (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = fork_error}
        );
        close_descriptor(result_pipe[0]);
        close_descriptor(result_pipe[1]);

        (void)sigprocmask(SIG_SETMASK, &old_set, nullptr);

        return mpmc_diagnostic_end(
            &diagnostic_scope,
            (mpmc_result_t){
                .status = MPMC_ERR_SYS,
                .system_errno = fork_error,
            }
        );
    }

    if (child == 0) {
        close_descriptor(result_pipe[0]);
        close_descriptor(supervisor->event_pipe[0]);
        for (size_t index = 0; index < supervisor->child_count; ++index) {
            if (supervisor->children[index].result_fd >= 0) {
                close_descriptor(supervisor->children[index].result_fd);
            }
        }

        if (!reset_child_signals() || sigprocmask(SIG_SETMASK, &old_set, nullptr) == -1) {
            _exit(EXIT_FAILURE);
        }

#ifdef DEBUG

        if (getenv("MPMC_TEST_WORKER_HANG") != nullptr) {
            for (;;) {
                (void)pause();
            }
        }
#endif

        run_worker(supervisor, command, operation_id, result_pipe[1]);
    }

    close_descriptor(result_pipe[1]);

    supervisor->children[supervisor->child_count++] = (child_record_t){
        .pid = child,
        .kind = command->kind,
        .operation_id = operation_id,
        .started = started,
        .result_fd = result_pipe[0],
    };

    supervisor->next_operation_id = operation_id == UINT64_MAX ? 0 : operation_id + 1;

    if (sigprocmask(SIG_SETMASK, &old_set, nullptr) == -1) {
        return mpmc_diagnostic_end(
            &diagnostic_scope,
            (mpmc_result_t){
                .status = MPMC_ERR_SYS,
                .system_errno = errno,
            }
        );
    }

    return mpmc_diagnostic_end(&diagnostic_scope, (mpmc_result_t){.status = MPMC_OK});
}

/**
 * @brief Remove um registro do vetor compactando-o com o último elemento.
 * @param supervisor Instância proprietária do vetor.
 * @param index Índice válido do registro a remover.
 */
static void remove_child(supervisor_t *supervisor, size_t index) {
    if (supervisor->children[index].result_fd >= 0) {
        close_descriptor(supervisor->children[index].result_fd);
    }
    supervisor->children[index] = supervisor->children[supervisor->child_count - 1];
    --supervisor->child_count;
}

/**
 * @brief Retorna a descrição curta do tipo registrado para um filho.
 * @param kind Tipo do trabalho armazenado no registro.
 * @return Literal estático "produz" para COMMAND_PRODUCE; "consome" nos demais casos.
 * @pre Os registros de filhos devem conter COMMAND_PRODUCE ou COMMAND_CONSUME.
 * @note O texto retornado não deve ser modificado nem liberado.
 */
static const char *child_kind_name(command_kind_t kind) {
    return kind == COMMAND_PRODUCE ? "produz" : "consome";
}

/**
 * @brief Calcula o tempo decorrido entre dois instantes monotônicos.
 * @param start Instante inicial.
 * @param end Instante final.
 * @return Duração em nanossegundos; zero se end for anterior a start.
 */
static uint64_t elapsed_ns(const struct timespec *start, const struct timespec *end) {
    if (end->tv_sec < start->tv_sec
        || (end->tv_sec == start->tv_sec && end->tv_nsec < start->tv_nsec)) {
        return 0;
    }

    uint64_t seconds = (uint64_t)(end->tv_sec - start->tv_sec);
    int64_t nanos = (int64_t)end->tv_nsec - (int64_t)start->tv_nsec;

    if (nanos < 0) {
        --seconds;
        nanos += 1000000000;
    }

    return seconds * 1000000000 + (uint64_t)nanos;
}

/**
 * @brief Acrescenta o relatório final de um filho recolhido.
 * @param supervisor Instância proprietária do vetor de relatórios.
 * @param finished Registro do filho no momento em que foi removido.
 * @param end Forma de término observada.
 * @param value Código de saída ou número do sinal, conforme end.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: relatório acrescentado ao vetor; system_errno é zero.
 * - MPMC_ERR_SYS: vetor de relatórios sem capacidade para outro registro; system_errno é zero,
 *   pois a falha é de estado interno, sem erro de chamada de sistema associado.
 * @details A duração usa CLOCK_MONOTONIC no instante do recolhimento; se o relógio
 * falhar, vale zero.
 */
static mpmc_result_t record_report(
    supervisor_t *supervisor,
    const child_record_t *finished,
    child_end_t end,
    int value
) {
    if (supervisor->report_count >= supervisor->report_capacity) {
        return (mpmc_result_t){.status = MPMC_ERR_SYS};
    }

    struct timespec now;
    uint64_t duration = 0;

    if (clock_gettime(CLOCK_MONOTONIC, &now) == 0) {
        duration = elapsed_ns(&finished->started, &now);
    }

    supervisor->reports[supervisor->report_count++] = (child_report_t){
        .pid = finished->pid,
        .kind = finished->kind,
        .operation_id = finished->operation_id,
        .duration_ns = duration,
        .end = end,
        .value = value,
    };

    return (mpmc_result_t){.status = MPMC_OK};
}

/**
 * @brief Descarta os ACKs sem dono de um produtor que terminou sem sucesso.
 * @param supervisor Instância proprietária do contexto IPC.
 * @param finished Registro do filho recolhido.
 * @param succeeded Verdadeiro se o filho saiu com EXIT_SUCCESS.
 * @param[in,out] result Resultado agregado; recebe o primeiro erro.
 * @param reporter Destino opcional dos diagnósticos; pode ser nullptr.
 * @details Só trabalhos produtores que não saíram com sucesso podem deixar ACKs pendentes:
 * um produtor ack=1 bem-sucedido já recebeu todos. Consumidores podem retirar depois do fim
 * do produtor mensagens dele e publicar ACKs que ninguém lerá, o que lotaria a fila de ACK
 * e bloquearia os consumidores. ipc_ack_abandon() descarta os que já existem e
 * ipc_poll_events() descarta os que chegarem depois.
 */
static void abandon_producer_acks(
    supervisor_t *supervisor,
    const child_record_t *finished,
    bool succeeded,
    mpmc_result_t *result,
    const supervisor_shutdown_reporter_t *reporter
) {
    if (finished->kind != COMMAND_PRODUCE || succeeded) return;

    const supervisor_shutdown_error_t error = {
        .operation = SUPERVISOR_ABANDON_ACK,
        .result = ipc_ack_abandon(supervisor->ipc, finished->operation_id),
        .child_pid = finished->pid,
    };

    record_shutdown_error(result, &error, reporter);
}

/** @details EINTR em waitpid() é repetido; cada estado coletado é correlacionado
 * ao registro original antes da remoção. ECHILD indica perda externa da
 * propriedade do filho, é reportado como erro e também limpa o registro obsoleto.
 * Término por sinal não é erro: é informado e armazenado como relatório. Todo
 * filho recolhido gera um relatório (início → recolhimento, forma de término).
 * Eventos do canal são drenados antes da coleta, fora do contexto de sinal.
 */
mpmc_result_t supervisor_reap(
    supervisor_t *supervisor,
    bool blocking,
    const supervisor_shutdown_reporter_t *reporter
) {
    mpmc_diagnostic_scope_t diagnostic_scope;
    mpmc_diagnostic_begin(&diagnostic_scope, __func__, false, false);

    if (supervisor == nullptr)
        return mpmc_diagnostic_end(
            &diagnostic_scope, (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG}
        );

    mpmc_result_t result = supervisor_poll_events(supervisor, reporter);

    flush_pending_outcomes(supervisor);

    size_t index = 0;

    while (index < supervisor->child_count) {
        int wait_options = blocking ? 0 : WNOHANG;
        int wait_status;
        pid_t waited;

        do {
            waited = waitpid(supervisor->children[index].pid, &wait_status, wait_options);
        } while (waited == -1 && errno == EINTR);

        if (waited == 0) {
            ++index;
            continue;
        }

        if (waited == -1) {
            int wait_errno = errno;

            const supervisor_shutdown_error_t error = {
                .operation = SUPERVISOR_WAIT_CHILD,
                .result = {.status = MPMC_ERR_SYS, .system_errno = wait_errno},
                .child_pid = supervisor->children[index].pid,
            };

            record_shutdown_error(&result, &error, reporter);

            if (wait_errno == ECHILD) {
                child_record_t lost = supervisor->children[index];

                ++supervisor->notice_count;
                fprintf(
                    stderr,
                    "mpmc: estado do filho %ld (%s, operação %" PRIu64
                    ") não está mais disponível (ECHILD).\n",
                    (long)lost.pid,
                    child_kind_name(lost.kind),
                    lost.operation_id
                );
                const supervisor_shutdown_error_t report_error = {
                    .operation = SUPERVISOR_RECORD_REPORT,
                    .result = record_report(supervisor, &lost, CHILD_END_LOST, 0),
                    .child_pid = lost.pid,
                };

                record_shutdown_error(&result, &report_error, reporter);
                abandon_producer_acks(supervisor, &lost, false, &result, reporter);
                remove_child(supervisor, index);
                continue;
            }

            ++index;
            continue;
        }

        mpmc_result_t worker_error = drain_worker_results(supervisor, reporter);
        if (result.status == MPMC_OK) result = worker_error;
        child_record_t finished = supervisor->children[index];
        child_end_t end = CHILD_END_LOST;
        int end_value = 0;

        if (WIFEXITED(wait_status)) {
            end = CHILD_END_EXITED;
            end_value = WEXITSTATUS(wait_status);
            ++supervisor->notice_count;
            fprintf(
                stderr,
                "mpmc: filho %ld (%s, operação %" PRIu64 ") terminou com código %d%s.\n",
                (long)waited,
                child_kind_name(finished.kind),
                finished.operation_id,
                end_value,
                end_value == EXIT_SUCCESS ? " (sucesso)" : " (falha)"
            );
        } else if (WIFSIGNALED(wait_status)) {
            end = CHILD_END_SIGNALED;
            end_value = WTERMSIG(wait_status);

            if (!supervisor->shutting_down || end_value != SIGTERM) {
                ++supervisor->notice_count;
                fprintf(
                    stderr,
                    "mpmc: filho %ld (%s, operação %" PRIu64 ") terminou pelo sinal %d.\n",
                    (long)waited,
                    child_kind_name(finished.kind),
                    finished.operation_id,
                    end_value
                );
            }
        } else {
            ++supervisor->notice_count;
            fprintf(
                stderr,
                "mpmc: estado inesperado para o filho %ld (%s, operação %" PRIu64 ").\n",
                (long)waited,
                child_kind_name(finished.kind),
                finished.operation_id
            );
            const supervisor_shutdown_error_t error = {
                .operation = SUPERVISOR_WAIT_CHILD,
                .result = {.status = MPMC_ERR_SYS},
                .child_pid = waited,
            };

            record_shutdown_error(&result, &error, reporter);
        }

        const supervisor_shutdown_error_t report_error = {
            .operation = SUPERVISOR_RECORD_REPORT,
            .result = record_report(supervisor, &finished, end, end_value),
            .child_pid = finished.pid,
        };

        record_shutdown_error(&result, &report_error, reporter);
        abandon_producer_acks(
            supervisor,
            &finished,
            end == CHILD_END_EXITED && end_value == EXIT_SUCCESS,
            &result,
            reporter
        );
        remove_child(supervisor, index);
    }

    return mpmc_diagnostic_end(&diagnostic_scope, result);
}

/** @details Usa um único prazo monotônico para todos os filhos. Erros na
 * preparação ou espera antecipam o envio de SIGKILL, preservando o primeiro
 * erro. Falha ao enviar SIGKILL permite somente coleta não bloqueante, para
 * não esperar indefinidamente por um filho que não foi possível encerrar.
 */
mpmc_result_t supervisor_shutdown(
    supervisor_t *supervisor,
    const supervisor_shutdown_reporter_t *reporter
) {
    mpmc_diagnostic_scope_t diagnostic_scope;
    mpmc_diagnostic_begin(&diagnostic_scope, __func__, false, false);

    if (supervisor == nullptr)
        return mpmc_diagnostic_end(
            &diagnostic_scope, (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG}
        );

    supervisor->shutting_down = true;

    mpmc_result_t result = supervisor_reap(supervisor, false, reporter);

    if (supervisor->child_count == 0) {
        ipc_quiesce(supervisor->ipc);
        return mpmc_diagnostic_end(&diagnostic_scope, result);
    }

    struct timespec deadline = {};

    if (result.status == MPMC_OK && clock_gettime(CLOCK_MONOTONIC, &deadline) == -1) {
        const supervisor_shutdown_error_t error = {
            .operation = SUPERVISOR_READ_CLOCK,
            .result = {.status = MPMC_ERR_SYS, .system_errno = errno},
        };

        record_shutdown_error(&result, &error, reporter);
    }

    deadline.tv_sec += SHUTDOWN_GRACE_SECONDS;

    mpmc_result_t signal_error = signal_children(supervisor, SIGTERM, reporter);

    if (result.status == MPMC_OK) result = signal_error;

    if (result.status == MPMC_OK) result = wait_children_until(supervisor, &deadline, reporter);

    mpmc_result_t reap_error = supervisor_reap(supervisor, false, reporter);

    if (result.status == MPMC_OK) result = reap_error;

    if (supervisor->child_count != 0) {
        signal_error = signal_children(supervisor, SIGKILL, reporter);

        if (result.status == MPMC_OK) result = signal_error;
        reap_error = supervisor_reap(supervisor, signal_error.status == MPMC_OK, reporter);

        if (result.status == MPMC_OK) result = reap_error;
    }

    if (supervisor->child_count == 0) ipc_quiesce(supervisor->ipc);

    return mpmc_diagnostic_end(&diagnostic_scope, result);
}

/**
 * @brief Formata uma duração como `S.mmm s` (milissegundos, truncados).
 * @param buffer Destino.
 * @param size Tamanho de buffer.
 * @param duration_ns Duração em nanossegundos.
 */
static void format_duration(char *buffer, size_t size, uint64_t duration_ns) {
    const uint64_t milliseconds = duration_ns / 1000000;

    (void)snprintf(
        buffer, size, "%" PRIu64 ".%03" PRIu64 " s", milliseconds / 1000, milliseconds % 1000
    );
}

/**
 * @brief Descreve como o filho terminou, em português.
 * @param report Relatório do filho.
 * @param buffer Destino.
 * @param size Tamanho de buffer.
 */
static void format_end(const child_report_t *report, char *buffer, size_t size) {
    switch (report->end) {
        case CHILD_END_EXITED:
            (void)snprintf(
                buffer,
                size,
                "código %d (%s)",
                report->value,
                report->value == EXIT_SUCCESS ? "sucesso" : "falha"
            );
            break;

        case CHILD_END_SIGNALED:
            (void)snprintf(buffer, size, "sinal %d", report->value);
            break;

        default:
            (void)snprintf(buffer, size, "indisponível");
            break;
    }
}

/** @details Layout fixo; o cabeçalho tem acentos (2 bytes em UTF-8), por isso seus
 * espaços são escritos à mão para alinhar com as linhas de dados. A saída nunca
 * contém ">>", para não ser confundida com o prompt.
 */
mpmc_result_t supervisor_print_report(const supervisor_t *supervisor, FILE *out) {
    mpmc_diagnostic_scope_t diagnostic_scope;
    mpmc_diagnostic_begin(&diagnostic_scope, __func__, false, false);

    if (supervisor == nullptr || out == nullptr) {
        return mpmc_diagnostic_end(
            &diagnostic_scope, (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG}
        );
    }

    mpmc_result_t event_error = ipc_poll_events((ipc_context_t *)supervisor->ipc);

    if (event_error.status != MPMC_OK) {
        return mpmc_diagnostic_end(&diagnostic_scope, event_error);
    }

    struct timespec now;
    uint64_t total_ns = 0;

    if (clock_gettime(CLOCK_MONOTONIC, &now) == 0) {
        total_ns = elapsed_ns(&supervisor->created_at, &now);
    }

    if (fputs("\nRelatório de execução\n----------------------\n", out) == EOF) {
        return mpmc_diagnostic_end(
            &diagnostic_scope,
            (mpmc_result_t){
                .status = MPMC_ERR_SYS,
                .system_errno = errno,
            }
        );
    }

    if (supervisor->report_count == 0) {
        if (fputs("Nenhum processo filho foi executado.\n", out) == EOF) {
            return mpmc_diagnostic_end(
                &diagnostic_scope,
                (mpmc_result_t){
                    .status = MPMC_ERR_SYS,
                    .system_errno = errno,
                }
            );
        }
    } else {
        if (fputs("PID      OP     TIPO     TÉRMINO                DURAÇÃO\n", out) == EOF) {
            return mpmc_diagnostic_end(
                &diagnostic_scope,
                (mpmc_result_t){
                    .status = MPMC_ERR_SYS,
                    .system_errno = errno,
                }
            );
        }

        for (size_t index = 0; index < supervisor->report_count; ++index) {
            const child_report_t *report = &supervisor->reports[index];
            char end_text[48];
            char duration_text[32];

            format_end(report, end_text, sizeof end_text);
            format_duration(duration_text, sizeof duration_text, report->duration_ns);

            if (fprintf(
                    out,
                    "%-8ld %-6" PRIu64 " %-8s %-20s %9s\n",
                    (long)report->pid,
                    report->operation_id,
                    child_kind_name(report->kind),
                    end_text,
                    duration_text
                )
                < 0) {
                return mpmc_diagnostic_end(
                    &diagnostic_scope,
                    (mpmc_result_t){
                        .status = MPMC_ERR_SYS,
                        .system_errno = errno,
                    }
                );
            }
        }
    }

    if (fprintf(out, "Filhos recolhidos: %zu\n", supervisor->report_count) < 0) {
        return mpmc_diagnostic_end(
            &diagnostic_scope,
            (mpmc_result_t){
                .status = MPMC_ERR_SYS,
                .system_errno = errno,
            }
        );
    }

    if (supervisor->child_count > 0) {
        if (fprintf(out, "Filhos ainda ativos: %zu\n", supervisor->child_count) < 0) {
            return mpmc_diagnostic_end(
                &diagnostic_scope,
                (mpmc_result_t){
                    .status = MPMC_ERR_SYS,
                    .system_errno = errno,
                }
            );
        }
    }

    for (size_t producer_id = 1; producer_id <= supervisor->producer_count; ++producer_id) {
        uint64_t consumed = 0;
        mpmc_result_t count_error = ipc_consumed_count(supervisor->ipc, producer_id, &consumed);

        if (count_error.status != MPMC_OK) {
            return mpmc_diagnostic_end(&diagnostic_scope, count_error);
        }

        if (fprintf(out, "Consumidos pela estrutura %zu: %" PRIu64 "\n", producer_id, consumed)
            < 0) {
            return mpmc_diagnostic_end(
                &diagnostic_scope, (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = errno}
            );
        }
    }

    char total_text[32];

    format_duration(total_text, sizeof total_text, total_ns);

    if (fprintf(out, "Tempo total de operação do mpmc: %s\n", total_text) < 0) {
        return mpmc_diagnostic_end(
            &diagnostic_scope,
            (mpmc_result_t){
                .status = MPMC_ERR_SYS,
                .system_errno = errno,
            }
        );
    }

    if (fflush(out) != 0) {
        return mpmc_diagnostic_end(
            &diagnostic_scope,
            (mpmc_result_t){
                .status = MPMC_ERR_SYS,
                .system_errno = errno,
            }
        );
    }

    if (ferror(out))
        return mpmc_diagnostic_end(&diagnostic_scope, (mpmc_result_t){.status = MPMC_ERR_SYS});

    return mpmc_diagnostic_end(&diagnostic_scope, (mpmc_result_t){.status = MPMC_OK});
}

/** @details Consulta simples do número de relatórios acumulados. */
size_t supervisor_report_count(const supervisor_t *supervisor) {
    return supervisor == nullptr ? 0 : supervisor->report_count;
}

/** @details Copia o relatório; a ordem é a de recolhimento. */
mpmc_result_t supervisor_report_get(
    const supervisor_t *supervisor,
    size_t index,
    child_report_t *out
) {
    mpmc_diagnostic_scope_t diagnostic_scope;
    mpmc_diagnostic_begin(&diagnostic_scope, __func__, false, false);

    if (supervisor == nullptr || out == nullptr || index >= supervisor->report_count) {
        return mpmc_diagnostic_end(
            &diagnostic_scope, (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG}
        );
    }
    *out = supervisor->reports[index];

    return mpmc_diagnostic_end(&diagnostic_scope, (mpmc_result_t){.status = MPMC_OK});
}

/** @details Libera os vetores de filhos e relatórios e fecha o self-pipe. */
void supervisor_destroy(supervisor_t *supervisor) {
    mpmc_diagnostic_scope_t diagnostic_scope;
    mpmc_diagnostic_begin(&diagnostic_scope, __func__, true, false);

    if (supervisor == nullptr) {
        (void)mpmc_diagnostic_end(&diagnostic_scope, (mpmc_result_t){.status = MPMC_OK});
        return;
    }

    close_event_pipe(supervisor);
    ipc_destroy(supervisor->ipc);

    free(supervisor->children);
    free(supervisor->reports);
    free(supervisor);
    (void)mpmc_diagnostic_end(&diagnostic_scope, (mpmc_result_t){.status = MPMC_OK});
}

/**
 * @brief Envia um sinal a todos os filhos ainda registrados.
 * @param supervisor Instância válida, proprietária dos PIDs registrados.
 * @param signal_number Sinal de término a enviar.
 * @param reporter Destino opcional das falhas individuais de kill().
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: sinais enviados ou filhos já inexistentes; system_errno é zero.
 * - MPMC_ERR_SYS: primeira falha de kill() diferente de ESRCH; system_errno
 *   contém o errno capturado nessa chamada.
 * @details Tenta todos os PIDs mesmo após uma falha. ESRCH não substitui a coleta
 * com waitpid(), que ainda pode encontrar o estado final do filho.
 */
static mpmc_result_t signal_children(
    supervisor_t *supervisor,
    int signal_number,
    const supervisor_shutdown_reporter_t *reporter
) {
    mpmc_result_t result = {.status = MPMC_OK};

    for (size_t i = 0; i < supervisor->child_count; i++) {
        // O emissor de diagnósticos tem o prazo comum para concluir seu cleanup.
        // SIGKILL continua abrangendo todos os filhos ao vencer o prazo.
        if (signal_number == SIGTERM && supervisor->children[i].reporting_failure
            && !supervisor->children[i].outcome_received)
            continue;
        pid_t child_pid = supervisor->children[i].pid;

        if (kill(child_pid, signal_number) == -1) {
            if (errno == ESRCH) continue;

            supervisor_shutdown_error_t error = {
                .operation = SUPERVISOR_SEND_SIGNAL,
                .result =
                    {
                        .status = MPMC_ERR_SYS,
                        .system_errno = errno,
                    },
                .send_signal = {.child_pid = child_pid, .signal_number = signal_number},
            };

            record_shutdown_error(&result, &error, reporter);
        }
    }

    return result;
}

/**
 * @brief Encaminha SIGCHLD ao self-pipe durante a espera do shutdown.
 * @param signal_number Número do sinal recebido, não utilizado.
 * @details Reutiliza a notificação async-signal-safe que preserva errno.
 */
static void notify_shutdown_child(int signal_number) {
    (void)signal_number;
    supervisor_notify_signal();
}

/**
 * @brief Recolhe filhos até todos terminarem ou o prazo monotônico vencer.
 * @param supervisor Instância válida com self-pipe aberto.
 * @param deadline Instante limite obtido de CLOCK_MONOTONIC.
 * @param reporter Destino opcional das falhas da espera e restauração.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: todos os filhos recolhidos ou prazo vencido; system_errno é zero.
 *   child_count distingue o término completo dos filhos ainda pendentes.
 * - MPMC_ERR_SYS: falha em sigaction(), sigprocmask(), clock_gettime(), poll(),
 *   waitpid() ou read(), com o errno capturado em system_errno; eventos de erro
 *   no pipe e falhas internas da coleta usam system_errno zero.
 *   Também propaga falhas de malloc()/msgrcv() da drenagem de eventos.
 * - MPMC_ERR_INVALID_ARG: msgrcv() rejeitou argumentos na drenagem de eventos;
 *   system_errno contém o errno capturado nessa chamada.
 * - MPMC_ERR_STATE: evento de consumo inconsistente; system_errno é zero.
 * - MPMC_ERR_SHUTDOWN: msgrcv() encontrou a fila de eventos removida;
 *   system_errno contém EIDRM capturado nessa chamada.
 * @details Instala um tratador temporário de SIGCHLD e desbloqueia esse sinal,
 * permitindo uso fora do REPL. Restaura ação, máscara e descritor de notificação
 * ao sair, preservando o primeiro erro. Cada despertar ou EINTR recalcula o
 * tempo restante sem reiniciar o prazo. A coleta drena o pipe antes de waitpid(),
 * evitando perder uma notificação entre coleta e poll().
 */
static mpmc_result_t wait_children_until(
    supervisor_t *supervisor,
    const struct timespec *deadline,
    const supervisor_shutdown_reporter_t *reporter
) {
    mpmc_result_t result = {.status = MPMC_OK};
    struct sigaction action = {};
    struct sigaction previous_action;

    action.sa_handler = notify_shutdown_child;
    action.sa_flags = SA_NOCLDSTOP;

    sigemptyset(&action.sa_mask);

    if (sigaction(SIGCHLD, &action, &previous_action) == -1) {
        const supervisor_shutdown_error_t error = {
            .operation = SUPERVISOR_INSTALL_SIGCHLD,
            .result = {.status = MPMC_ERR_SYS, .system_errno = errno},
        };

        record_shutdown_error(&result, &error, reporter);

        return result;
    }

    int previous_fd = wake_write_fd;

    // Registra que a extremidade de escrita do pipe é o waker para informar ao tratador de SIGCHLD
    // onde ele deve escrever para acordar o pai
    wake_write_fd = supervisor->event_pipe[1];

    sigset_t child_signal;
    sigset_t previous_mask;

    sigemptyset(&child_signal);
    sigaddset(&child_signal, SIGCHLD);

    // Desbloqueia SIGCHLD durante a espera para que o tratador notifique o self-pipe.

    if (sigprocmask(SIG_UNBLOCK, &child_signal, &previous_mask) == -1) {
        const supervisor_shutdown_error_t error = {
            .operation = SUPERVISOR_UNBLOCK_SIGCHLD,
            .result = {.status = MPMC_ERR_SYS, .system_errno = errno},
        };

        record_shutdown_error(&result, &error, reporter);

        wake_write_fd = previous_fd;

        if (sigaction(SIGCHLD, &previous_action, nullptr) == -1) {
            supervisor_shutdown_error_t restore_error = {
                .operation = SUPERVISOR_RESTORE_SIGCHLD,
                .result =
                    {
                        .status = MPMC_ERR_SYS,
                        .system_errno = errno,
                    },
            };

            record_shutdown_error(&result, &restore_error, reporter);
        }

        return result;
    }

    while (supervisor->child_count != 0) {
        result = supervisor_reap(supervisor, false, reporter);

        if (result.status != MPMC_OK || supervisor->child_count == 0) break;

        struct timespec now;

        if (clock_gettime(CLOCK_MONOTONIC, &now) == -1) {
            supervisor_shutdown_error_t error = {
                .operation = SUPERVISOR_READ_CLOCK,
                .result =
                    {
                        .status = MPMC_ERR_SYS,
                        .system_errno = errno,
                    },
            };

            record_shutdown_error(&result, &error, reporter);
            break;
        }

        int64_t remaining_ns = (int64_t)(deadline->tv_sec - now.tv_sec) * 1000000000LL
            + (deadline->tv_nsec - now.tv_nsec);

        if (remaining_ns <= 0) break;

        int timeout_ms = (int)((remaining_ns + 999999LL) / 1000000LL);

        struct pollfd wake = {
            .fd = supervisor->event_pipe[0],
            .events = POLLIN,
        };

        if (poll(&wake, 1, timeout_ms) == -1) {
            if (errno == EINTR) continue;

            supervisor_shutdown_error_t error = {
                .operation = SUPERVISOR_WAIT_EVENTS,
                .result =
                    {
                        .status = MPMC_ERR_SYS,
                        .system_errno = errno,
                    },
                .poll_revents = 0,
            };

            record_shutdown_error(&result, &error, reporter);
            break;
        }

        if ((wake.revents & (POLLERR | POLLHUP | POLLNVAL)) != 0) {
            supervisor_shutdown_error_t error = {
                .operation = SUPERVISOR_WAIT_EVENTS,
                .result = {.status = MPMC_ERR_SYS},
                .poll_revents = wake.revents,
            };

            record_shutdown_error(&result, &error, reporter);
            break;
        }
    }

    if (sigaction(SIGCHLD, &previous_action, nullptr) == -1) {
        const supervisor_shutdown_error_t error = {
            .operation = SUPERVISOR_RESTORE_SIGCHLD,
            .result = {.status = MPMC_ERR_SYS, .system_errno = errno},
        };

        record_shutdown_error(&result, &error, reporter);
    }

    wake_write_fd = previous_fd;

    if (sigprocmask(SIG_SETMASK, &previous_mask, nullptr) == -1) {
        const supervisor_shutdown_error_t error = {
            .operation = SUPERVISOR_RESTORE_SIGNAL_MASK,
            .result = {.status = MPMC_ERR_SYS, .system_errno = errno},
        };

        record_shutdown_error(&result, &error, reporter);
    }

    return result;
}

/**
 * @brief Preserva o primeiro erro e comunica um diagnóstico individual.
 * @param[in,out] first Resultado agregado, inicialmente MPMC_OK.
 * @param error Diagnóstico completo com errno capturado antes desta chamada.
 * @param reporter Destino opcional dos diagnósticos; pode ser nullptr.
 * @pre first e error apontam para objetos válidos durante a chamada.
 * @details Resultados MPMC_OK são ignorados. Cada falha é comunicada mesmo
 * quando first já contém um erro. A callback recebe um registro temporário,
 * pode alterar errno e não modifica o resultado agregado. Chamadores que
 * propagam um resultado já diagnosticado não devem registrá-lo novamente.
 */
static void record_shutdown_error(
    mpmc_result_t *first,
    const supervisor_shutdown_error_t *error,
    const supervisor_shutdown_reporter_t *reporter
) {
    mpmc_result_t current = error->result;

    if (current.status == MPMC_OK) return;

    if (first->status == MPMC_OK) {
        *first = current;
    }

    static const char *const names[] = {
        "kill()",
        "waitpid()",
        "registro do relatório",
        "clock_gettime()",
        "poll()",
        "read(self-pipe)",
        "eventos IPC",
        "sigaction(SIGCHLD)",
        "sigprocmask(SIG_UNBLOCK)",
        "restauração de SIGCHLD",
        "restauração da máscara de sinais",
        "read(resultado do worker)",
        "abandono de ACK",
    };
    if (error->operation != SUPERVISOR_POLL_IPC_EVENTS
        && error->operation != SUPERVISOR_ABANDON_ACK)
        mpmc_diagnostic_report(names[error->operation], current);
    else
        mpmc_diagnostic_cause(names[error->operation], current);

    if (reporter != nullptr && reporter->on_error != nullptr) {
        reporter->on_error(error, reporter->user_data);
    }
}

/** @brief Prepara o canal de um filho; só a leitura é não bloqueante. */
static mpmc_result_t open_result_pipe(int descriptors[2]) {
    if (pipe(descriptors) == -1) {
        return (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = errno};
    }
    mpmc_result_t result = set_nonblocking_cloexec(descriptors[0]);
    if (result.status == MPMC_OK && fcntl(descriptors[1], F_SETFD, FD_CLOEXEC) == -1) {
        result = (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = errno};
    }
    if (result.status != MPMC_OK) {
        mpmc_diagnostic_cause("fcntl()", result);
        close_descriptor(descriptors[0]);
        close_descriptor(descriptors[1]);
    }
    return result;
}

/** @brief Estado do emissor do filho; o pai drena o pipe enquanto o filho vive. */
typedef struct {
    int result_fd; /**< Canal de frames. */
    int wake_fd;   /**< Self-pipe do pai. */
    bool failed;   /**< Não foi possível entregar um frame. */
} worker_sink_t;

static bool send_worker_event(worker_sink_t *sink, const worker_event_t *event);
static void send_worker_diagnostic(const mpmc_diagnostic_t *error, void *user_data);

/** @brief Executa o trabalho e entrega todos os diagnósticos antes do resultado. */
static void run_worker(
    supervisor_t *supervisor,
    const command_t *command,
    uint64_t operation_id,
    int result_fd
) {
    mpmc_diagnostic_after_fork(operation_id, command->producer_id);
    worker_sink_t sink = {.result_fd = result_fd, .wake_fd = supervisor->event_pipe[1]};
    mpmc_reporter_t reporter = {.on_error = send_worker_diagnostic, .user_data = &sink};
    mpmc_diagnostic_set_reporter(&reporter);
    mpmc_diagnostic_scope_t scope;
    mpmc_diagnostic_begin(
        &scope, command->kind == COMMAND_PRODUCE ? "producer_run" : "consumer_run", false, false
    );
    job_outcome_t outcome = {
        .command = command->kind,
        .operation_id = operation_id,
        .producer_id = command->producer_id,
        .requested = command->message_count,
    };
    struct sigaction cancellation = {.sa_handler = cancel_worker};
    sigemptyset(&cancellation.sa_mask);
    if (sigaction(SIGTERM, &cancellation, nullptr) == -1
        || sigaction(SIGINT, &cancellation, nullptr) == -1) {
        outcome.error = (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = errno};
        mpmc_diagnostic_cause("sigaction(worker)", outcome.error);
        goto cleanup;
    }
    if (command->kind == COMMAND_PRODUCE) {
        const producer_job_t job = {
            .producer_id = command->producer_id,
            .message_count = command->message_count,
            .ack_mode = (producer_ack_mode_t)command->ack_mode,
            .operation_id = operation_id,
        };
        producer_result_t progress;
        outcome.error = producer_run_with_result(supervisor->ipc, &job, &progress);
        outcome.effective = progress.produced_count;
    } else {
        const consumer_job_t job = {
            .producer_id = command->producer_id,
            .message_count = command->message_count,
            .ack_mode = (consumer_ack_mode_t)command->ack_mode,
            .operation_id = operation_id,
        };
        consumer_result_t progress;
        outcome.error = consumer_run(supervisor->ipc, &job, &progress);
        outcome.effective = progress.consumed_count;
    }

cleanup:
    mpmc_diagnostic_cause("execução do worker", outcome.error);
    ipc_worker_close(supervisor->ipc);
    if (scope.first.status != MPMC_OK) outcome.error = scope.first;
    outcome.error = mpmc_diagnostic_end(&scope, outcome.error);
    outcome.kind = outcome.error.status == MPMC_OK     ? JOB_OUTCOME_COMPLETED
        : outcome.error.status == MPMC_ERR_WOULD_BLOCK ? JOB_OUTCOME_PARTIAL
        : outcome.error.status == MPMC_ERR_INTERRUPTED || outcome.error.status == MPMC_ERR_SHUTDOWN
        ? JOB_OUTCOME_CANCELLED
        : JOB_OUTCOME_KERNEL_ERROR;
    worker_event_t event = {.kind = WORKER_EVENT_OUTCOME, .outcome = outcome};
    (void)send_worker_event(&sink, &event);
    _exit(outcome.error.status == MPMC_OK && !sink.failed ? EXIT_SUCCESS : EXIT_FAILURE);
}

/** @brief Um frame cabe atomicamente no mínimo POSIX; EINTR anterior à escrita é repetido. */
static bool send_worker_event(worker_sink_t *sink, const worker_event_t *event) {
    ssize_t sent;
    do {
        sent = write(sink->result_fd, event, sizeof *event);
    } while (sent == -1 && errno == EINTR);
    if (sent != sizeof *event) sink->failed = true;
    unsigned char wake = 1;
    (void)write(sink->wake_fd, &wake, 1);
    return !sink->failed;
}

/** @brief Serializa cada erro imediatamente; não há uma fila local que possa descartar erros. */
static void send_worker_diagnostic(const mpmc_diagnostic_t *error, void *user_data) {
    worker_event_t event = {.kind = WORKER_EVENT_DIAGNOSTIC, .diagnostic = *error};
    (void)send_worker_event(user_data, &event);
}

/** @brief Drena canais próprios e apresenta resultados sem aguardar o fim dos filhos. */
static mpmc_result_t drain_worker_results(
    supervisor_t *supervisor,
    const supervisor_shutdown_reporter_t *reporter
) {
    mpmc_result_t result = {.status = MPMC_OK};
    for (size_t index = 0; index < supervisor->child_count; ++index) {
        child_record_t *child = &supervisor->children[index];
        if (child->result_fd < 0 || child->outcome_received) continue;
        while (!child->outcome_received) {
            unsigned char *destination = (unsigned char *)&child->event;
            ssize_t count = read(
                child->result_fd,
                destination + child->result_bytes,
                sizeof child->event - child->result_bytes
            );
            if (count > 0) {
                child->result_bytes += (size_t)count;
                if (child->result_bytes != sizeof child->event) continue;
                child->result_bytes = 0;
                if (child->event.kind == WORKER_EVENT_DIAGNOSTIC) {
                    mpmc_diagnostic_t *error = &child->event.diagnostic;
                    if (error->process_pid != child->pid
                        || error->operation_id != child->operation_id) {
                        result = (mpmc_result_t){.status = MPMC_ERR_STATE};
                        break;
                    }
                    error->function[sizeof error->function - 1] = '\0';
                    error->operation[sizeof error->operation - 1] = '\0';
                    child->reporting_failure = true;
                    mpmc_diagnostic_deliver(error);
                    ++supervisor->notice_count;
                    if (error->fatal || error->result.status == MPMC_ERR_STATE
                        || (error->result.system_errno != 0
                            && error->result.status != MPMC_ERR_INTERRUPTED))
                        supervisor->compromised = true;
                    continue;
                }
                if (child->event.kind != WORKER_EVENT_OUTCOME) {
                    result = (mpmc_result_t){.status = MPMC_ERR_STATE};
                    break;
                }
                child->outcome = child->event.outcome;
                child->outcome_received = true;
                break;
            }
            if (count < 0 && errno == EINTR) continue;
            if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;
            if (count < 0 || child->result_bytes != 0) {
                supervisor_shutdown_error_t error = {
                    .operation = SUPERVISOR_READ_WORKER_RESULT,
                    .result = {.status = MPMC_ERR_SYS, .system_errno = count < 0 ? errno : 0},
                    .child_pid = child->pid,
                };
                record_shutdown_error(&result, &error, reporter);
            }
            close_descriptor(child->result_fd);
            child->result_fd = -1;
            break;
        }
        if (child->outcome_received) {
            if (child->outcome.operation_id != child->operation_id
                || child->outcome.command != child->kind) {
                result = (mpmc_result_t){.status = MPMC_ERR_STATE};
                continue;
            }
            char text[512];
            if (job_outcome_format(&child->outcome, text, sizeof text) >= 0) {
                fprintf(stderr, "mpmc: %s\n", text);
                ++supervisor->notice_count;
            }
        }
    }
    if (supervisor->compromised && !supervisor->shutting_down && result.status == MPMC_OK)
        result = (mpmc_result_t){.status = MPMC_ERR_SHUTDOWN};
    return result;
}

/** @brief Fecha uma vez, reportando erro sem repetir close() sobre um FD potencialmente
 * reutilizado. */
static void close_descriptor(int fd) {
    mpmc_diagnostic_scope_t scope;
    mpmc_diagnostic_begin(&scope, "close", true, false);
    scope.resource_id = fd;
    mpmc_result_t result = {.status = MPMC_OK};
    if (close(fd) == -1) result = (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = errno};
    (void)mpmc_diagnostic_end(&scope, result);
}

/** @brief Apenas marca cancelamento; publicação, callbacks e cleanup ocorrem fora do sinal. */
static void cancel_worker(int signal_number) {
    (void)signal_number;
    ipc_worker_cancel();
}
