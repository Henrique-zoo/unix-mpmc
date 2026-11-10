/**
 * @file repl.c
 * @brief Implementação do terminal interativo e despacho de comandos.
 * @ingroup application
 *
 * @details Lê linhas de stdin sem bloquear o ciclo de supervisão, integra o
 * descritor de despertar do supervisor com poll(), recupera-se de entradas
 * inválidas e encaminha EOF e sinais de término pelo shutdown ordenado.
 */
#include "mpmc/app/repl.h"

#include <errno.h>
#include <poll.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <sys/types.h>
#include <unistd.h>

#include "job_outcome.h"
#include "mpmc/app/command.h"
#include "mpmc/diagnostic.h"

/** @brief Comprimento máximo aceito para uma linha de comando. */
static constexpr size_t REPL_MAX_LINE = 4096;
/** @brief Intervalo máximo de espera para manutenção periódica (ms). */
#define REPL_POLL_INTERVAL_MS 100

/** @brief Indica que SIGINT ou SIGTERM solicitou o encerramento do REPL. */
static volatile sig_atomic_t termination_requested;

static void restore_signal_handler(int signal_number, const struct sigaction *previous);

/**
 * @brief Acorda o supervisor quando um processo filho altera seu estado.
 * @param signal_number Número do sinal recebido; não utilizado.
 * @details Apenas escreve no canal async-signal-safe do supervisor. A coleta
 * dos filhos é feita posteriormente fora do contexto do tratador.
 */
static void handle_child_signal(int signal_number) {
    (void)signal_number;
    supervisor_notify_signal();
}

/**
 * @brief Marca o pedido de encerramento vindo do processo ou terminal.
 * @param signal_number Número do sinal recebido; não utilizado.
 * @details Não executa limpeza nem operações não seguras em contexto de sinal;
 * o loop principal observa a flag e inicia o shutdown ordenado.
 */
static void handle_termination_signal(int signal_number) {
    (void)signal_number;
    termination_requested = 1;
}

/**
 * @brief Instala um tratador de sinal e preserva a ação anterior.
 * @param signal_number Sinal cuja ação será alterada.
 * @param handler Função tratadora a instalar.
 * @param previous Destino da ação anterior, restaurável por sigaction().
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: tratador instalado e ação anterior preservada em previous; system_errno é zero.
 * - MPMC_ERR_SYS: falha em sigaction(); system_errno contém o errno capturado nessa falha.
 */
static mpmc_result_t install_signal_handler(
    int signal_number,
    void (*handler)(int),
    struct sigaction *previous
) {
    struct sigaction action = {};
    action.sa_handler = handler;
    sigemptyset(&action.sa_mask);
    if (sigaction(signal_number, &action, previous) == -1) {
        return (mpmc_result_t){
            .status = MPMC_ERR_SYS,
            .system_errno = errno,
        };
    }
    return (mpmc_result_t){.status = MPMC_OK};
}

/**
 * @brief Evita tentativa de leitura de terminal por um job em background.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: stdin não é identificado como terminal ou o grupo do processo está em primeiro
 *   plano; system_errno é zero.
 * - MPMC_ERR_SYS: falha em tcgetpgrp(), com o errno capturado em system_errno, ou processo em
 *   background com stdin ligado ao terminal, com system_errno zero.
 * @details Entrada redirecionada por pipe ou arquivo continua permitida. A
 * verificação evita que a leitura de stdin suspenda o programa com SIGTTIN.
 */
static mpmc_result_t check_foreground_stdin(void) {
    if (!isatty(STDIN_FILENO)) return (mpmc_result_t){.status = MPMC_OK};

    pid_t foreground_group = tcgetpgrp(STDIN_FILENO);
    if (foreground_group == -1) {
        return (mpmc_result_t){
            .status = MPMC_ERR_SYS,
            .system_errno = errno,
        };
    }
    if (foreground_group != getpgrp()) {
        fprintf(
            stderr,
            "mpmc: stdin pertence a um terminal, mas este processo está em background; "
            "use-o em primeiro plano ou redirecione stdin.\n"
        );
        return (mpmc_result_t){.status = MPMC_ERR_SYS};
    }
    return (mpmc_result_t){.status = MPMC_OK};
}

/**
 * @brief Executa uma rodada não bloqueante de manutenção do supervisor.
 * @param supervisor Instância ativa do supervisor.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: manutenção não bloqueante concluída ou nenhum filho pronto para coleta;
 *   system_errno é zero.
 * - MPMC_ERR_INVALID_ARG: supervisor nulo; system_errno é zero.
 * - MPMC_ERR_SYS: falha em waitpid() ou read(), com o errno capturado em system_errno, ou falha
 *   interna da coleta/drenagem, com system_errno zero. O resultado de supervisor_reap() é
 *   propagado integralmente.
 * @details A chamada também drena o canal de despertar e serve de ponto para
 * processar eventos de controle associados ao supervisor.
 */
static mpmc_result_t service_supervisor(supervisor_t *supervisor) {
    return supervisor_reap(supervisor, false, nullptr);
}

/**
 * @brief Indica se a linha só contém espaços, tabulações ou CR.
 * @param line Linha terminada em NUL.
 * @return true se não há nenhum caractere de comando na linha.
 */
static bool is_blank_line(const char *line) {
    for (; *line != '\0'; ++line) {
        if (*line != ' ' && *line != '\t' && *line != '\r') return false;
    }
    return true;
}

/**
 * @brief Analisa, valida e despacha uma linha completa do REPL.
 * @param supervisor Supervisor responsável por validar configuração e executar.
 * @param line Linha terminada em NUL, sem newline final.
 * @param[out] shutdown Recebe true quando o comando aceito solicita shutdown.
 * @details Linhas em branco são ignoradas sem erro. Erros sintáticos,
 * semânticos ou de despacho são informados em stderr; a linha inválida não
 * encerra o loop nem modifica o estado de saída.
 */
static void dispatch_line(supervisor_t *supervisor, char *line, bool *shutdown) {
    if (is_blank_line(line)) return;

    uint64_t before = mpmc_diagnostic_count();
    command_t command;
    bool dispatch_attempted = false;
    mpmc_result_t error = command_parse(line, &command);
    if (error.status == MPMC_OK) {
        error = command_validate(&command, supervisor_producer_count(supervisor));
    }
    if (error.status == MPMC_OK) {
        dispatch_attempted = true;
        error = supervisor_dispatch(supervisor, &command, nullptr);
    }

    if (error.status != MPMC_OK) {
        char error_text[512];
        if (dispatch_attempted && error.status == MPMC_ERR_SYS
            && command.kind != COMMAND_SHUTDOWN) {
            /* O trabalho não começou: o diagnóstico informa o comando, o índice e a causa. */
            job_outcome_t outcome = {
                .kind = JOB_OUTCOME_SPAWN_FAILED,
                .command = command.kind,
                .producer_id = command.producer_id,
                .requested = command.message_count,
                .error = error,
            };
            (void)job_outcome_format(&outcome, error_text, sizeof error_text);
        } else {
            (void)mpmc_error_format(error, error_text, sizeof error_text);
        }
        if (!mpmc_diagnostic_enabled() || before == mpmc_diagnostic_count())
            fprintf(stderr, "mpmc: %s\n", error_text);
        return;
    }

    *shutdown = command.kind == COMMAND_SHUTDOWN;
}

/**
 * @details A implementação monitora stdin e o descritor de eventos do
 * supervisor com poll(), acumula linhas limitadas a REPL_MAX_LINE e trata EOF
 * como pedido de encerramento ordenado. O contrato público está documentado
 * na declaração de repl_run() em repl.h.
 */
mpmc_result_t repl_run(supervisor_t *supervisor) {
    struct sigaction previous_chld;
    struct sigaction previous_int;
    struct sigaction previous_term;
    char line[REPL_MAX_LINE + 1];
    size_t line_length = 0;
    bool line_too_long = false;
    bool need_prompt = true;
    const bool tty_output = isatty(STDOUT_FILENO) == 1;
    bool shutdown = false;
    mpmc_result_t result;

    if (supervisor == nullptr) return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};

    result = check_foreground_stdin();
    if (result.status != MPMC_OK) return result;

    termination_requested = 0;
    mpmc_diagnostic_scope_t setup;
    mpmc_diagnostic_begin(&setup, "repl_run", false, false);
    result = install_signal_handler(SIGCHLD, handle_child_signal, &previous_chld);
    if (result.status != MPMC_OK) return mpmc_diagnostic_end(&setup, result);
    result = install_signal_handler(SIGINT, handle_termination_signal, &previous_int);
    if (result.status != MPMC_OK) {
        mpmc_diagnostic_cause("sigaction()", result);
        restore_signal_handler(SIGCHLD, &previous_chld);
        return mpmc_diagnostic_end(&setup, result);
    }
    result = install_signal_handler(SIGTERM, handle_termination_signal, &previous_term);
    if (result.status != MPMC_OK) {
        mpmc_diagnostic_cause("sigaction()", result);
        restore_signal_handler(SIGINT, &previous_int);
        restore_signal_handler(SIGCHLD, &previous_chld);
        return mpmc_diagnostic_end(&setup, result);
    }

    (void)mpmc_diagnostic_end(&setup, (mpmc_result_t){.status = MPMC_OK});
    result = (mpmc_result_t){.status = MPMC_OK};
    while (!shutdown && !termination_requested) {
        if (need_prompt) {
            fputs(">> ", stdout);
            fflush(stdout);
            need_prompt = false;
        }

        struct pollfd fds[2] = {
            {.fd = STDIN_FILENO, .events = POLLIN | POLLHUP, .revents = 0},
            {.fd = supervisor_event_fd(supervisor), .events = POLLIN, .revents = 0},
        };
        struct pollfd *input = &fds[0];
        const struct pollfd *wake = &fds[1];
        int ready = poll(fds, 2, REPL_POLL_INTERVAL_MS);
        if (ready == -1) {
            int poll_errno = errno;
            if (poll_errno == EINTR) {
                continue;
            }
            result = (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = poll_errno};
            break;
        }
        if (ready > 0 && (wake->revents & (POLLERR | POLLNVAL)) != 0) {
            result = (mpmc_result_t){.status = MPMC_ERR_SYS};
            break;
        }
        if (ready > 0 && (input->revents & POLLNVAL) != 0 && (input->revents & POLLERR) == 0) {
            input->revents = POLLIN;
        }
        if (ready > 0 && (input->revents & POLLERR) != 0) {
            result = (mpmc_result_t){.status = MPMC_ERR_SYS};
            break;
        }

        /* O supervisor é servido a cada iteração, mesmo com stdin sempre pronto: nesse caso
         * poll() nunca estoura o tempo e um filho bloqueado em msgsnd() numa fila de eventos
         * cheia ficaria sem ser drenado. A chamada não bloqueia. */
        uint64_t before_service = mpmc_diagnostic_count();
        mpmc_result_t service_error = service_supervisor(supervisor);
        if (service_error.status != MPMC_OK) {
            char error_text[256];
            (void)mpmc_error_format(service_error, error_text, sizeof error_text);
            if (!mpmc_diagnostic_enabled() || before_service == mpmc_diagnostic_count())
                fprintf(
                    stderr,
                    "mpmc: falha ao processar eventos dos processos filhos: %s\n",
                    error_text
                );
            result = service_error;
            break;
        }
        if (supervisor_take_notices(supervisor) > 0 && tty_output) {
            need_prompt = true;
        }
        if (ready == 0 || input->revents == 0) {
            continue;
        }

        char buffer[512];
        ssize_t bytes_read = read(STDIN_FILENO, buffer, sizeof(buffer));
        if (bytes_read == -1) {
            int read_errno = errno;
            if (read_errno == EINTR) {
                continue;
            }
            if (read_errno == EAGAIN || read_errno == EWOULDBLOCK) {
                continue;
            }
            result = (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = read_errno};
            break;
        }
        if (bytes_read == 0) {
            if (line_length != 0 || line_too_long) {
                if (line_too_long) {
                    fprintf(
                        stderr,
                        "mpmc: linha excede o limite de %u caracteres.\n",
                        (unsigned)REPL_MAX_LINE
                    );
                } else {
                    line[line_length] = '\0';
                    dispatch_line(supervisor, line, &shutdown);
                }
            }
            break;
        }

        for (ssize_t index = 0; index < bytes_read && !shutdown; ++index) {
            char character = buffer[index];
            if (character == '\n') {
                if (line_too_long) {
                    fprintf(
                        stderr,
                        "mpmc: linha excede o limite de %u caracteres.\n",
                        (unsigned)REPL_MAX_LINE
                    );
                } else {
                    line[line_length] = '\0';
                    dispatch_line(supervisor, line, &shutdown);
                }
                line_length = 0;
                line_too_long = false;
                need_prompt = !shutdown;
                if (need_prompt) {
                    fputs(">> ", stdout);
                    fflush(stdout);
                    need_prompt = false;
                }
                continue;
            }

            if (!line_too_long) {
                if (line_length < REPL_MAX_LINE) {
                    line[line_length++] = character;
                } else {
                    line_too_long = true;
                    line_length = 0;
                }
            }
        }
    }

    mpmc_diagnostic_scope_t cleanup;
    mpmc_diagnostic_begin(&cleanup, "repl_run", true, false);
    restore_signal_handler(SIGTERM, &previous_term);
    restore_signal_handler(SIGINT, &previous_int);
    restore_signal_handler(SIGCHLD, &previous_chld);

    mpmc_result_t shutdown_error = supervisor_shutdown(supervisor, nullptr);
    if (result.status == MPMC_OK && shutdown_error.status != MPMC_OK) {
        result = shutdown_error;
    }
    mpmc_result_t finalization = mpmc_diagnostic_end(&cleanup, shutdown_error);
    if (result.status == MPMC_OK) result = finalization;
    return result;
}

/** @brief Restaura uma ação de sinal, comunicando falhas sem omitir outras restaurações. */
static void restore_signal_handler(int signal_number, const struct sigaction *previous) {
    mpmc_diagnostic_scope_t scope;
    mpmc_diagnostic_begin(&scope, "restauração de sinais do REPL", true, false);
    mpmc_result_t result = {.status = MPMC_OK};
    if (sigaction(signal_number, previous, nullptr) == -1)
        result = (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = errno};
    (void)mpmc_diagnostic_end(&scope, result);
}
