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
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

/** @brief Registro de identidade e estado de um processo filho supervisionado. */
typedef struct {
    pid_t pid;                 /**< PID retornado por fork(). */
    command_kind_t kind;       /**< Tipo de trabalho executado pelo filho. */
    uint64_t operation_id;     /**< Identidade única atribuída antes de fork(). */
    struct timespec started;   /**< Instante CLOCK_MONOTONIC capturado antes de fork(). */
} child_record_t;

/** @brief Estado privado mantido exclusivamente pelo processo supervisor. */
struct supervisor {
    size_t producer_count;    /**< Quantidade de estruturas configuradas. */
    child_record_t *children; /**< Vetor dinâmico de filhos ativos. */
    size_t child_count;       /**< Número de registros ocupados. */
    size_t child_capacity;    /**< Capacidade alocada do vetor. */
    child_report_t *reports;  /**< Filhos já recolhidos, na ordem de recolhimento. */
    size_t report_count;      /**< Número de relatórios armazenados. */
    size_t report_capacity;   /**< Capacidade alocada do vetor de relatórios. */
    uint64_t next_operation_id; /**< Próximo identificador; zero indica esgotamento. */
    bool shutting_down;       /**< Impede novos despachos após shutdown. */
    int event_pipe[2];        /**< Self-pipe: leitura em [0], escrita em [1]. */
    size_t notice_count;      /**< Avisos impressos em stderr desde a última consulta. */
};

/** @brief Descritor usado pelo tratador para acordar o loop ativo. */
static volatile sig_atomic_t wake_write_fd = -1;

/**
 * @brief Define um descritor como não bloqueante e close-on-exec.
 * @param fd Descritor válido a configurar.
 * @retval MPMC_OK Flags aplicadas.
 * @retval MPMC_ERR_SYS Falha em fcntl().
 */
static mpmc_status_t set_nonblocking_cloexec(int fd) {
    int flags = fcntl(fd, F_GETFL);
    if (flags == -1 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        return MPMC_ERR_SYS;
    }
    flags = fcntl(fd, F_GETFD);
    if (flags == -1 || fcntl(fd, F_SETFD, flags | FD_CLOEXEC) == -1) {
        return MPMC_ERR_SYS;
    }
    return MPMC_OK;
}

/**
 * @brief Move um descritor para fora da faixa 0..2 (stdin/stdout/stderr).
 * @param fd Descritor a mover; atualizado em caso de sucesso.
 * @retval MPMC_OK Descritor já era > 2 ou foi duplicado com sucesso.
 * @retval MPMC_ERR_SYS Falha em fcntl().
 * @details Se o processo iniciou com stdin fechado, pipe() devolveria o fd 0 e o
 * REPL passaria a ler do próprio self-pipe, sem nunca observar EOF.
 */
static mpmc_status_t move_above_stdio(int *fd) {
    if (*fd > STDERR_FILENO) {
        return MPMC_OK;
    }
    int moved = fcntl(*fd, F_DUPFD_CLOEXEC, STDERR_FILENO + 1);
    if (moved == -1) {
        return MPMC_ERR_SYS;
    }
    (void)close(*fd);
    *fd = moved;
    return MPMC_OK;
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
    for (size_t end = 0u; end < 2u; ++end) {
        if (supervisor->event_pipe[end] != -1) {
            (void)close(supervisor->event_pipe[end]);
            supervisor->event_pipe[end] = -1;
        }
    }
}

/**
 * @brief Cria e configura o canal não bloqueante de despertar.
 * @param supervisor Instância a receber os descritores.
 * @retval MPMC_OK Pipe criado e registrado como canal ativo.
 * @retval MPMC_ERR_SYS Falha ao criar o pipe ou configurar suas flags.
 * @post Em falha, os descritores parcialmente criados são fechados.
 */
static mpmc_status_t open_event_pipe(supervisor_t *supervisor) {
    supervisor->event_pipe[0] = -1;
    supervisor->event_pipe[1] = -1;
    if (pipe(supervisor->event_pipe) == -1) {
        supervisor->event_pipe[0] = -1;
        supervisor->event_pipe[1] = -1;
        return MPMC_ERR_SYS;
    }

    for (size_t end = 0u; end < 2u; ++end) {
        if (move_above_stdio(&supervisor->event_pipe[end]) != MPMC_OK) {
            close_event_pipe(supervisor);
            return MPMC_ERR_SYS;
        }
    }

    for (size_t end = 0u; end < 2u; ++end) {
        if (set_nonblocking_cloexec(supervisor->event_pipe[end]) != MPMC_OK) {
            close_event_pipe(supervisor);
            return MPMC_ERR_SYS;
        }
    }
    wake_write_fd = supervisor->event_pipe[1];
    return MPMC_OK;
}

/**
 * @brief Garante espaço para registrar mais um filho.
 * @param supervisor Instância cujo vetor pode crescer.
 * @retval MPMC_OK Já havia espaço ou a realocação foi concluída.
 * @retval MPMC_ERR_SYS Overflow de tamanho ou falha de alocação.
 */
static mpmc_status_t ensure_child_capacity(supervisor_t *supervisor) {
    if (supervisor->child_count < supervisor->child_capacity) {
        return MPMC_OK;
    }

    size_t capacity = supervisor->child_capacity == 0u ? 8u : supervisor->child_capacity * 2u;
    if (capacity < supervisor->child_capacity
        || capacity > SIZE_MAX / sizeof(*supervisor->children)) {
        return MPMC_ERR_SYS;
    }
    child_record_t *children = realloc(supervisor->children, capacity * sizeof(*children));
    if (children == NULL) {
        return MPMC_ERR_SYS;
    }
    supervisor->children = children;
    supervisor->child_capacity = capacity;
    return MPMC_OK;
}

/**
 * @brief Garante espaço para os relatórios de todos os filhos ativos e de mais um.
 * @param supervisor Instância cujo vetor de relatórios pode crescer.
 * @retval MPMC_OK Capacidade suficiente para report_count + child_count + 1.
 * @retval MPMC_ERR_SYS Overflow de tamanho ou falha de alocação.
 * @details Chamada antes de fork(): assim, o recolhimento de qualquer filho
 * registrado nunca precisa alocar memória e não perde o estado final.
 */
static mpmc_status_t ensure_report_capacity(supervisor_t *supervisor) {
    if (supervisor->report_count > SIZE_MAX - supervisor->child_count - 1u) {
        return MPMC_ERR_SYS;
    }
    const size_t needed = supervisor->report_count + supervisor->child_count + 1u;
    if (needed <= supervisor->report_capacity) {
        return MPMC_OK;
    }

    size_t capacity = supervisor->report_capacity == 0u ? 8u : supervisor->report_capacity;
    while (capacity < needed) {
        if (capacity > SIZE_MAX / 2u) {
            return MPMC_ERR_SYS;
        }
        capacity *= 2u;
    }
    if (capacity > SIZE_MAX / sizeof(*supervisor->reports)) {
        return MPMC_ERR_SYS;
    }
    child_report_t *reports = realloc(supervisor->reports, capacity * sizeof(*reports));
    if (reports == NULL) {
        return MPMC_ERR_SYS;
    }
    supervisor->reports = reports;
    supervisor->report_capacity = capacity;
    return MPMC_OK;
}

/** @details Prepara a lista local de filhos e o self-pipe. A inicialização dos
 * backends IPC ainda não está conectada; o contrato público está em supervisor.h.
 */
mpmc_status_t supervisor_create(supervisor_t **out, size_t producer_count, ipc_mode_t mode) {
    if (out == NULL) {
        return MPMC_ERR_INVALID_ARG;
    }
    *out = NULL;
    if (producer_count == 0u || (mode != IPC_SHARED_MEMORY && mode != IPC_MESSAGE_QUEUE)) {
        return MPMC_ERR_INVALID_ARG;
    }

    supervisor_t *supervisor = calloc(1u, sizeof(*supervisor));
    if (supervisor == NULL) {
        return MPMC_ERR_SYS;
    }

    supervisor->producer_count = producer_count;
    supervisor->next_operation_id = 1u;
    if (open_event_pipe(supervisor) != MPMC_OK) {
        free(supervisor);
        return MPMC_ERR_SYS;
    }
    *out = supervisor;
    return MPMC_OK;
}

/** @details Devolve e zera o contador incrementado por supervisor_reap() a cada
 * mensagem de término de filho impressa em stderr.
 */
size_t supervisor_take_notices(supervisor_t *supervisor) {
    if (supervisor == NULL) {
        return 0u;
    }
    size_t count = supervisor->notice_count;
    supervisor->notice_count = 0u;
    return count;
}

/** @details Consulta simples do valor armazenado na criação da instância. */
size_t supervisor_producer_count(const supervisor_t *supervisor) {
    return supervisor == NULL ? 0u : supervisor->producer_count;
}

/** @details Expõe o descritor para que o REPL o inclua em poll(). */
int supervisor_event_fd(const supervisor_t *supervisor) {
    return supervisor == NULL ? -1 : supervisor->event_pipe[0];
}

/** @details Pode ser chamada em tratador de sinal: usa write() no descritor
 * não bloqueante, preserva errno e descarta falha por pipe cheio, pois nesse
 * caso já existe um despertar pendente. O contrato público está em supervisor.h.
 */
void supervisor_notify_signal(void) {
    int saved_errno = errno;
    int fd = wake_write_fd;
    if (fd != -1) {
        const unsigned char wake = 1u;
        ssize_t written;
        do {
            written = write(fd, &wake, sizeof(wake));
        } while (written == -1 && errno == EINTR);
    }
    errno = saved_errno;
}

/** @details Os bytes são notificações agregadas sem payload. A integração da
 * drenagem dos backends IPC deve ocorrer neste ponto quando estiver disponível.
 */
mpmc_status_t supervisor_poll_events(supervisor_t *supervisor) {
    if (supervisor == NULL) {
        return MPMC_ERR_INVALID_ARG;
    }

    unsigned char sink[64];
    for (;;) {
        ssize_t count = read(supervisor->event_pipe[0], sink, sizeof(sink));
        if (count > 0) {
            continue;
        }
        if (count == 0) {
            return MPMC_ERR_SYS;
        }
        if (errno == EINTR) {
            continue;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return MPMC_OK;
        }
        return MPMC_ERR_SYS;
    }
}

/**
 * @brief Restaura ações padrão de sinais no processo filho.
 * @details Remove os tratadores herdados do REPL para que sinais de término,
 * em particular SIGTERM, tenham o comportamento esperado no worker.
 */
static bool reset_child_signals(void) {
    static const int signals[] = {SIGCHLD, SIGINT, SIGTERM};
    struct sigaction action = {0};
    action.sa_handler = SIG_DFL;
    sigemptyset(&action.sa_mask);
    for (size_t index = 0u; index < sizeof(signals) / sizeof(signals[0]); ++index) {
        if (sigaction(signals[index], &action, NULL) == -1) {
            return false;
        }
    }
    return true;
}

/** @details O pai registra PID, tipo e identidade da operação sem aguardar; o
 * filho recebe uma cópia imutável do comando, fecha o self-pipe e restaura os sinais.
 * Os sinais de término ficam bloqueados durante o fork() e só são liberados no
 * filho depois de restaurar as ações padrão, evitando perder um SIGTERM de
 * shutdown enviado imediatamente após o despacho.
 */
mpmc_status_t supervisor_dispatch(supervisor_t *supervisor, const command_t *command) {
    if (supervisor == NULL || command == NULL) {
        return MPMC_ERR_INVALID_ARG;
    }
    if (supervisor->shutting_down) {
        return MPMC_ERR_SHUTDOWN;
    }
    if (command->kind == COMMAND_SHUTDOWN) {
        return supervisor_shutdown(supervisor);
    }
    if (command->kind != COMMAND_PRODUCE && command->kind != COMMAND_CONSUME) {
        return MPMC_ERR_INVALID_ARG;
    }
    if (supervisor->next_operation_id == 0u) {
        return MPMC_ERR_STATE;
    }

    mpmc_status_t status = ensure_child_capacity(supervisor);
    if (status != MPMC_OK) {
        return status;
    }
    status = ensure_report_capacity(supervisor);
    if (status != MPMC_OK) {
        return status;
    }

    struct timespec started;
    if (clock_gettime(CLOCK_MONOTONIC, &started) == -1) {
        return MPMC_ERR_SYS;
    }

    sigset_t block_set;
    sigset_t old_set;
    sigemptyset(&block_set);
    sigaddset(&block_set, SIGCHLD);
    sigaddset(&block_set, SIGINT);
    sigaddset(&block_set, SIGTERM);
    if (sigprocmask(SIG_BLOCK, &block_set, &old_set) == -1) {
        return MPMC_ERR_SYS;
    }

    const uint64_t operation_id = supervisor->next_operation_id;
    pid_t child = fork();
    if (child == -1) {
        int fork_error = errno;
        (void)sigprocmask(SIG_SETMASK, &old_set, NULL);
        errno = fork_error;
        return MPMC_ERR_SYS;
    }
    if (child == 0) {
        close_event_pipe(supervisor);
        if (!reset_child_signals() || sigprocmask(SIG_SETMASK, &old_set, NULL) == -1) {
            _exit(EXIT_FAILURE);
        }
#ifdef DEBUG
        if (getenv("MPMC_TEST_WORKER_HANG") != NULL) {
            for (;;) {
                (void)pause();
            }
        }
#endif
        (void)dprintf(
            STDERR_FILENO,
            "mpmc: operação %" PRIu64 " (%s) recebida; execução de workers/IPC "
            "ainda não está implementada.\n",
            operation_id,
            command->kind == COMMAND_PRODUCE ? "produz" : "consome"
        );
        _exit(EXIT_FAILURE);
    }

    supervisor->children[supervisor->child_count++] = (child_record_t){
        .pid = child,
        .kind = command->kind,
        .operation_id = operation_id,
        .started = started,
    };
    supervisor->next_operation_id = operation_id == UINT64_MAX ? 0u : operation_id + 1u;
    if (sigprocmask(SIG_SETMASK, &old_set, NULL) == -1) {
        return MPMC_ERR_SYS;
    }
    return MPMC_OK;
}

/**
 * @brief Remove um registro do vetor compactando-o com o último elemento.
 * @param supervisor Instância proprietária do vetor.
 * @param index Índice válido do registro a remover.
 */
static void remove_child(supervisor_t *supervisor, size_t index) {
    supervisor->children[index] = supervisor->children[supervisor->child_count - 1u];
    --supervisor->child_count;
}

/** @brief Retorna a forma textual curta do tipo registrado para um filho. */
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
        return 0u;
    }
    uint64_t seconds = (uint64_t)(end->tv_sec - start->tv_sec);
    int64_t nanos = (int64_t)end->tv_nsec - (int64_t)start->tv_nsec;
    if (nanos < 0) {
        --seconds;
        nanos += 1000000000;
    }
    return seconds * 1000000000u + (uint64_t)nanos;
}

/**
 * @brief Acrescenta o relatório final de um filho recolhido.
 * @param supervisor Instância proprietária do vetor de relatórios.
 * @param finished Registro do filho no momento em que foi removido.
 * @param end Forma de término observada.
 * @param value Código de saída ou número do sinal, conforme end.
 * @retval MPMC_OK Relatório armazenado.
 * @retval MPMC_ERR_SYS Sem espaço (não esperado: a capacidade é reservada em dispatch).
 * @details A duração usa CLOCK_MONOTONIC no instante do recolhimento; se o relógio
 * falhar, vale zero.
 */
static mpmc_status_t record_report(
    supervisor_t *supervisor, const child_record_t *finished, child_end_t end, int value
) {
    if (supervisor->report_count >= supervisor->report_capacity) {
        return MPMC_ERR_SYS;
    }
    struct timespec now;
    uint64_t duration = 0u;
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
    return MPMC_OK;
}

/** @details EINTR em waitpid() é repetido; cada estado coletado é correlacionado
 * ao registro original antes da remoção. ECHILD indica perda externa da
 * propriedade do filho, é reportado como erro e também limpa o registro obsoleto.
 * Término por sinal não é erro: é informado e armazenado como relatório. Todo
 * filho recolhido gera um relatório (início → recolhimento, forma de término).
 * Eventos do canal são drenados antes da coleta, fora do contexto de sinal.
 */
mpmc_status_t supervisor_reap(supervisor_t *supervisor, bool blocking) {
    if (supervisor == NULL) {
        return MPMC_ERR_INVALID_ARG;
    }

    mpmc_status_t event_status = supervisor_poll_events(supervisor);
    mpmc_status_t reap_status = MPMC_OK;

    size_t index = 0u;
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
            if (errno == ECHILD) {
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
                reap_status = MPMC_ERR_SYS;
                (void)record_report(supervisor, &lost, CHILD_END_LOST, 0);
                remove_child(supervisor, index);
                continue;
            }
            reap_status = MPMC_ERR_SYS;
            ++index;
            continue;
        }

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
            reap_status = MPMC_ERR_SYS;
        }
        if (record_report(supervisor, &finished, end, end_value) != MPMC_OK) {
            reap_status = MPMC_ERR_SYS;
        }
        remove_child(supervisor, index);
    }
    return reap_status != MPMC_OK ? reap_status : event_status;
}

/** @details Marca a instância como encerrando, envia SIGTERM aos filhos e
 * espera sua saída. Recursos globais IPC ainda dependem dos backends pendentes.
 */
mpmc_status_t supervisor_shutdown(supervisor_t *supervisor) {
    if (supervisor == NULL) {
        return MPMC_ERR_INVALID_ARG;
    }
    if (supervisor->shutting_down) {
        return supervisor_reap(supervisor, true);
    }
    supervisor->shutting_down = true;

    mpmc_status_t signal_status = MPMC_OK;
    for (size_t index = 0u; index < supervisor->child_count; ++index) {
        if (kill(supervisor->children[index].pid, SIGTERM) == -1 && errno != ESRCH) {
            signal_status = MPMC_ERR_SYS;
        }
    }

    mpmc_status_t reap_status = supervisor_reap(supervisor, true);
    mpmc_status_t event_status = supervisor_poll_events(supervisor);
    if (signal_status != MPMC_OK) {
        return signal_status;
    }
    return reap_status != MPMC_OK ? reap_status : event_status;
}

/** @details Consulta simples do número de relatórios acumulados. */
size_t supervisor_report_count(const supervisor_t *supervisor) {
    return supervisor == NULL ? 0u : supervisor->report_count;
}

/** @details Copia o relatório; a ordem é a de recolhimento. */
mpmc_status_t
supervisor_report_get(const supervisor_t *supervisor, size_t index, child_report_t *out) {
    if (supervisor == NULL || out == NULL || index >= supervisor->report_count) {
        return MPMC_ERR_INVALID_ARG;
    }
    *out = supervisor->reports[index];
    return MPMC_OK;
}

/** @details Libera os vetores de filhos e relatórios e fecha o self-pipe. */
void supervisor_destroy(supervisor_t *supervisor) {
    if (supervisor == NULL) {
        return;
    }
    close_event_pipe(supervisor);
    free(supervisor->children);
    free(supervisor->reports);
    free(supervisor);
}
