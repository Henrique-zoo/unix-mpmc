/**
 * @file test_outcome_repl.c
 * @brief Resultados controlados de trabalhos no REPL e no supervisor (issue #54).
 * @ingroup tests
 *
 * @details
 * Usa o supervisor.c e o repl.c reais, com fork() substituído por
 * mpmc_test_fork() para injetar falhas, e eventos de resultado enfileirados pelo
 * próprio teste, sem depender dos workers. Confere:
 *
 * - o texto de cada evento em stderr, com operação, índice e quantidade efetiva;
 * - consumo parcial e cancelamento nunca como conclusão;
 * - falha ao criar o filho com contexto, e o REPL continua aceitando comandos;
 * - o prompt `>> ` permanece só em stdout, sem texto de eventos;
 * - cada evento ocupa uma linha inteira, sem depender de ordem global;
 * - a contagem de avisos usada para redesenhar o prompt, e a consolidação de
 *   eventos pendentes no shutdown.
 *
 * Retorna 0 se todos os casos passarem; 1 caso contrário.
 */

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

#include "job_outcome.h"
#include "mpmc/app/command.h"
#include "mpmc/app/repl.h"
#include "mpmc/app/supervisor.h"
#include "mpmc/error.h"
#include "mpmc/ipc/transport.h"

static unsigned g_checks;
static unsigned g_failures;

#define CHECK(condition, ...)                                                                      \
    do {                                                                                           \
        ++g_checks;                                                                                \
        if (!(condition)) {                                                                        \
            ++g_failures;                                                                          \
            fprintf(stderr, "FALHA %s:%d: ", __FILE__, __LINE__);                                  \
            fprintf(stderr, __VA_ARGS__);                                                          \
            fputc('\n', stderr);                                                                   \
        }                                                                                          \
    } while (0)

/* ------------------------------------------------------------------ */
/* fork() substituível                                                */
/* ------------------------------------------------------------------ */

static int g_fork_failures_left;

pid_t mpmc_test_fork(void);

pid_t mpmc_test_fork(void) {
    if (g_fork_failures_left > 0) {
        --g_fork_failures_left;
        errno = EAGAIN;
        return -1;
    }
    return fork();
}

/* ------------------------------------------------------------------ */
/* Captura de stdout/stderr e entrada por pipe                        */
/* ------------------------------------------------------------------ */

#define CAPTURE_MAX 16384

typedef struct {
    char out[CAPTURE_MAX];
    char err[CAPTURE_MAX];
} capture_t;

static void read_file(FILE *file, char *buffer, size_t capacity) {
    rewind(file);
    size_t length = fread(buffer, 1u, capacity - 1u, file);

    buffer[length] = '\0';
}

typedef struct {
    int saved_stdin;
    int saved_stdout;
    int saved_stderr;
    FILE *out_file;
    FILE *err_file;
} redirect_t;

/* Redireciona stdin (para `input`), stdout e stderr para arquivos temporários. */
static bool redirect_begin(redirect_t *state, const char *input) {
    fflush(stdout);
    fflush(stderr);
    state->out_file = tmpfile();
    state->err_file = tmpfile();
    if (state->out_file == NULL || state->err_file == NULL) {
        return false;
    }

    int pipe_fds[2];

    if (pipe(pipe_fds) == -1) {
        return false;
    }
    if (input != NULL) {
        size_t length = strlen(input);

        if (write(pipe_fds[1], input, length) != (ssize_t)length) {
            return false;
        }
    }
    (void)close(pipe_fds[1]);

    state->saved_stdin = dup(STDIN_FILENO);
    state->saved_stdout = dup(STDOUT_FILENO);
    state->saved_stderr = dup(STDERR_FILENO);
    (void)dup2(pipe_fds[0], STDIN_FILENO);
    (void)dup2(fileno(state->out_file), STDOUT_FILENO);
    (void)dup2(fileno(state->err_file), STDERR_FILENO);
    (void)close(pipe_fds[0]);
    return true;
}

static void redirect_end(redirect_t *state, capture_t *capture) {
    fflush(stdout);
    fflush(stderr);
    (void)dup2(state->saved_stdin, STDIN_FILENO);
    (void)dup2(state->saved_stdout, STDOUT_FILENO);
    (void)dup2(state->saved_stderr, STDERR_FILENO);
    (void)close(state->saved_stdin);
    (void)close(state->saved_stdout);
    (void)close(state->saved_stderr);
    read_file(state->out_file, capture->out, sizeof capture->out);
    read_file(state->err_file, capture->err, sizeof capture->err);
    fclose(state->out_file);
    fclose(state->err_file);
}

/* Executa o REPL com a entrada dada e captura o que ele escreve. */
static mpmc_result_t run_repl(supervisor_t *supervisor, const char *input, capture_t *capture) {
    redirect_t state;

    if (!redirect_begin(&state, input)) {
        return (mpmc_result_t){.status = MPMC_ERR_SYS};
    }
    mpmc_result_t result = repl_run(supervisor);

    redirect_end(&state, capture);
    return result;
}

/* Executa uma rodada do supervisor e captura stderr. */
static mpmc_result_t run_reap(supervisor_t *supervisor, capture_t *capture) {
    redirect_t state;

    if (!redirect_begin(&state, NULL)) {
        return (mpmc_result_t){.status = MPMC_ERR_SYS};
    }
    mpmc_result_t result = supervisor_reap(supervisor, false, NULL);

    redirect_end(&state, capture);
    return result;
}

static supervisor_t *new_supervisor(void) {
    supervisor_t *supervisor = NULL;

    if (supervisor_create(&supervisor, 2u, IPC_MESSAGE_QUEUE).status != MPMC_OK) {
        fprintf(stderr, "supervisor_create falhou; System V IPC indisponível?\n");
        _exit(2);
    }
    return supervisor;
}

static void finish(supervisor_t *supervisor) {
    CHECK(supervisor_shutdown(supervisor, NULL).status == MPMC_OK, "shutdown do supervisor");
    supervisor_destroy(supervisor);
}

/* ------------------------------------------------------------------ */
/* Utilitários de texto                                               */
/* ------------------------------------------------------------------ */

static size_t count_substring(const char *text, const char *needle) {
    size_t count = 0u;

    for (const char *at = strstr(text, needle); at != NULL; at = strstr(at + 1, needle)) {
        ++count;
    }
    return count;
}

/* Toda linha de stderr deve estar inteira: começa com "mpmc: " e não tem outro "mpmc: ". */
static void check_whole_lines(const char *text, size_t *lines_out) {
    size_t lines = 0u;
    const char *cursor = text;

    while (*cursor != '\0') {
        const char *end = strchr(cursor, '\n');

        CHECK(end != NULL, "última linha sem quebra: \"%s\"", cursor);
        if (end == NULL) {
            break;
        }
        CHECK(
            strncmp(cursor, "mpmc: ", 6) == 0,
            "linha sem prefixo: \"%.*s\"",
            (int)(end - cursor),
            cursor
        );
        char line[1024];
        size_t length = (size_t)(end - cursor);

        if (length >= sizeof line) {
            length = sizeof line - 1u;
        }
        memcpy(line, cursor, length);
        line[length] = '\0';
        CHECK(strstr(line + 6, "mpmc: ") == NULL, "linhas misturadas: \"%s\"", line);
        ++lines;
        cursor = end + 1;
    }
    *lines_out = lines;
}

static job_outcome_t sample(
    job_outcome_kind_t kind,
    command_kind_t command,
    uint64_t operation_id,
    size_t producer_id,
    uint64_t requested,
    uint64_t effective,
    mpmc_result_t error
) {
    return (job_outcome_t){
        .kind = kind,
        .command = command,
        .operation_id = operation_id,
        .producer_id = producer_id,
        .requested = requested,
        .effective = effective,
        .error = error,
    };
}

/* ------------------------------------------------------------------ */
/* Casos                                                              */
/* ------------------------------------------------------------------ */

static void test_outcomes_are_presented_and_repl_continues(void) {
    static capture_t capture;
    supervisor_t *supervisor = new_supervisor();
    const mpmc_result_t io_error = {.status = MPMC_ERR_SYS, .system_errno = EIO};
    const job_outcome_t events[] = {
        sample(JOB_OUTCOME_KERNEL_ERROR, COMMAND_PRODUCE, 3, 1, 10, 4, io_error),
        sample(
            JOB_OUTCOME_PARTIAL, COMMAND_CONSUME, 4, 2, 5, 3, (mpmc_result_t){.status = MPMC_OK}
        ),
        sample(
            JOB_OUTCOME_CANCELLED, COMMAND_PRODUCE, 5, 0, 8, 2, (mpmc_result_t){.status = MPMC_OK}
        ),
    };

    for (size_t i = 0; i < sizeof events / sizeof events[0]; ++i) {
        CHECK(
            supervisor_queue_outcome(supervisor, &events[i]).status == MPMC_OK, "enfileirar %zu", i
        );
    }

    /* Um comando inválido no meio é um erro recuperável: o REPL segue até o shutdown. */
    mpmc_result_t result = run_repl(supervisor, "invalido\nshutdown\n", &capture);

    CHECK(result.status == MPMC_OK, "o REPL deveria terminar normalmente: %d", result.status);

    size_t lines = 0u;

    check_whole_lines(capture.err, &lines);
    CHECK(lines == 4u, "esperadas 3 linhas de eventos e 1 de erro, obtidas %zu", lines);

    CHECK(
        strstr(
            capture.err,
            "operação 3 (produz, estrutura 1) falhou após 4 de 10 mensagens "
            "produzidas"
        ) != NULL,
        "erro de kernel: %s",
        capture.err
    );
    CHECK(
        strstr(capture.err, strerror(EIO)) != NULL, "errno válido deve aparecer: %s", capture.err
    );
    CHECK(
        strstr(
            capture.err,
            "operação 4 (consome, estrutura 2) não foi concluída: apenas 3 de 5 "
            "mensagens consumidas"
        ) != NULL,
        "consumo parcial: %s",
        capture.err
    );
    CHECK(
        strstr(
            capture.err,
            "operação 5 (produz, todas as estruturas) cancelada antes de "
            "terminar: 2 de 8"
        ) != NULL,
        "cancelamento: %s",
        capture.err
    );

    /* Nenhum evento incompleto é apresentado como conclusão. */
    CHECK(
        strstr(capture.err, ") concluída") == NULL,
        "evento incompleto como sucesso: %s",
        capture.err
    );

    /* O erro do comando inválido continua sendo apresentado. */
    CHECK(count_substring(capture.err, "mpmc: ") == 4u, "linhas com prefixo");

    /* Eventos vão para stderr; stdout só tem o prompt. */
    CHECK(
        strstr(capture.out, "operação") == NULL, "eventos não podem ir para stdout: %s", capture.out
    );

    finish(supervisor);
}

static void test_spawn_failure_is_contextual_and_recoverable(void) {
    static capture_t capture;
    supervisor_t *supervisor = new_supervisor();

    g_fork_failures_left = 1;
    mpmc_result_t result = run_repl(supervisor, "produz 1 3 1\nproduz 1 3 1\nshutdown\n", &capture);
    g_fork_failures_left = 0;

    CHECK(
        result.status == MPMC_OK, "o REPL deveria sobreviver à falha de criação: %d", result.status
    );
    CHECK(
        strstr(
            capture.err,
            "mpmc: comando (produz, estrutura 1): não foi possível criar o "
            "processo filho: "
        ) != NULL,
        "diagnóstico da falha de criação: %s",
        capture.err
    );
    CHECK(strstr(capture.err, strerror(EAGAIN)) != NULL, "errno da falha de fork: %s", capture.err);

    /* O segundo comando foi despachado: o filho é recolhido e o relatório o registra. */
    static capture_t waiting;

    redirect_t state;
    CHECK(redirect_begin(&state, NULL), "redirecionar");
    CHECK(supervisor_reap(supervisor, true, NULL).status == MPMC_OK, "recolher o filho");
    redirect_end(&state, &waiting);

    FILE *report = tmpfile();
    char report_text[CAPTURE_MAX];

    CHECK(report != NULL, "arquivo do relatório");
    if (report != NULL) {
        CHECK(supervisor_print_report(supervisor, report).status == MPMC_OK, "relatório");
        read_file(report, report_text, sizeof report_text);
        fclose(report);
        CHECK(
            strstr(report_text, "Filhos recolhidos: 1") != NULL,
            "exatamente um filho deveria ter sido criado: %s",
            report_text
        );
        CHECK(strstr(report_text, "produz") != NULL, "o filho é do tipo produz: %s", report_text);
    }

    size_t lines = 0u;

    check_whole_lines(capture.err, &lines);
    finish(supervisor);
}

static void test_spawn_failure_texts_for_each_command_form(void) {
    static capture_t capture;
    supervisor_t *supervisor = new_supervisor();

    g_fork_failures_left = 2;
    mpmc_result_t result =
        run_repl(supervisor, "produz 0 4 1\nconsome 2 3 2 &\nshutdown\n", &capture);
    g_fork_failures_left = 0;

    CHECK(result.status == MPMC_OK, "REPL: %d", result.status);
    CHECK(
        strstr(capture.err, "comando (produz, todas as estruturas): não foi possível criar")
            != NULL,
        "produz 0: %s",
        capture.err
    );
    CHECK(
        strstr(capture.err, "comando (consome, estrutura 2): não foi possível criar") != NULL,
        "consome 2: %s",
        capture.err
    );
    finish(supervisor);
}

static void test_prompt_is_preserved(void) {
    static capture_t capture;
    supervisor_t *supervisor = new_supervisor();
    const job_outcome_t event = sample(
        JOB_OUTCOME_KERNEL_ERROR,
        COMMAND_CONSUME,
        6,
        1,
        4,
        1,
        (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = EIO}
    );

    CHECK(supervisor_queue_outcome(supervisor, &event).status == MPMC_OK, "enfileirar");
    g_fork_failures_left = 1;
    mpmc_result_t result = run_repl(supervisor, "invalido\nproduz 1 3 1\nshutdown\n", &capture);
    g_fork_failures_left = 0;

    CHECK(result.status == MPMC_OK, "REPL: %d", result.status);

    /* stdout contém apenas prompts `>> `: um inicial e um por linha processada, exceto o shutdown.
     */
    size_t prompts = count_substring(capture.out, ">> ");

    CHECK(
        strspn(capture.out, "> \n") == strlen(capture.out),
        "stdout só deve ter prompts: \"%s\"",
        capture.out
    );
    CHECK(prompts == 3u, "esperados 3 prompts (inicial, invalido e produz), obtidos %zu", prompts);
    CHECK(strstr(capture.err, ">>") == NULL, "o prompt não pode ir para stderr");
    finish(supervisor);
}

static void test_concurrent_events_are_whole_lines_without_global_order(void) {
    static capture_t capture;
    supervisor_t *supervisor = new_supervisor();
    const job_outcome_t first = sample(
        JOB_OUTCOME_PARTIAL, COMMAND_CONSUME, 11, 1, 5, 2, (mpmc_result_t){.status = MPMC_OK}
    );
    const job_outcome_t second = sample(
        JOB_OUTCOME_CANCELLED, COMMAND_PRODUCE, 12, 2, 6, 1, (mpmc_result_t){.status = MPMC_OK}
    );

    CHECK(supervisor_queue_outcome(supervisor, &first).status == MPMC_OK, "enfileirar 1");
    CHECK(supervisor_queue_outcome(supervisor, &second).status == MPMC_OK, "enfileirar 2");

    /* Um filho real termina durante a mesma sessão: seu aviso concorre com os eventos. */
    mpmc_result_t result = run_repl(supervisor, "produz 1 2 1\nshutdown\n", &capture);

    CHECK(result.status == MPMC_OK, "REPL: %d", result.status);

    size_t lines = 0u;

    check_whole_lines(capture.err, &lines);
    /* Cada evento aparece exatamente uma vez, em qualquer ordem entre si ou com o filho. */
    CHECK(
        count_substring(capture.err, "operação 11 (consome, estrutura 1) não foi concluída") == 1u,
        "evento 11: %s",
        capture.err
    );
    CHECK(
        count_substring(capture.err, "operação 12 (produz, estrutura 2) cancelada") == 1u,
        "evento 12: %s",
        capture.err
    );
    finish(supervisor);
}

static void test_notice_count_drives_prompt_redraw(void) {
    static capture_t capture;
    supervisor_t *supervisor = new_supervisor();
    const job_outcome_t event = sample(
        JOB_OUTCOME_PARTIAL, COMMAND_CONSUME, 1, 1, 5, 2, (mpmc_result_t){.status = MPMC_OK}
    );

    CHECK(supervisor_take_notices(supervisor) == 0u, "sem avisos iniciais");
    CHECK(supervisor_queue_outcome(supervisor, &event).status == MPMC_OK, "enfileirar");
    CHECK(supervisor_take_notices(supervisor) == 0u, "enfileirar não imprime nem avisa");

    CHECK(run_reap(supervisor, &capture).status == MPMC_OK, "reap");
    CHECK(count_substring(capture.err, "não foi concluída") == 1u, "linha impressa uma vez");
    CHECK(supervisor_take_notices(supervisor) == 1u, "cada linha conta como um aviso");
    CHECK(supervisor_take_notices(supervisor) == 0u, "o contador é zerado ao ser consultado");

    /* Uma nova rodada não repete o que já foi apresentado. */
    CHECK(run_reap(supervisor, &capture).status == MPMC_OK, "reap");
    CHECK(capture.err[0] == '\0', "evento repetido: %s", capture.err);
    CHECK(supervisor_take_notices(supervisor) == 0u, "sem novos avisos");
    finish(supervisor);
}

static void test_queue_contract(void) {
    static capture_t capture;
    supervisor_t *supervisor = new_supervisor();
    const job_outcome_t event = sample(
        JOB_OUTCOME_CANCELLED, COMMAND_PRODUCE, 1, 1, 5, 2, (mpmc_result_t){.status = MPMC_OK}
    );
    const job_outcome_t unknown = sample(
        (job_outcome_kind_t)99, COMMAND_PRODUCE, 1, 1, 5, 2, (mpmc_result_t){.status = MPMC_OK}
    );

    CHECK(supervisor_queue_outcome(NULL, &event).status == MPMC_ERR_INVALID_ARG, "supervisor nulo");
    CHECK(supervisor_queue_outcome(supervisor, NULL).status == MPMC_ERR_INVALID_ARG, "evento nulo");
    CHECK(
        supervisor_queue_outcome(supervisor, &unknown).status == MPMC_ERR_INVALID_ARG,
        "tipo desconhecido"
    );

    size_t accepted = 0u;

    while (supervisor_queue_outcome(supervisor, &event).status == MPMC_OK && accepted < 1000u) {
        ++accepted;
    }
    CHECK(accepted > 0u && accepted < 1000u, "a fila deve ter capacidade finita: %zu", accepted);
    CHECK(supervisor_queue_outcome(supervisor, &event).status == MPMC_ERR_NO_SPACE, "fila cheia");

    /* Esvaziar a fila libera espaço, e nenhum evento é perdido ou repetido. */
    CHECK(run_reap(supervisor, &capture).status == MPMC_OK, "reap");

    size_t lines = 0u;

    check_whole_lines(capture.err, &lines);
    CHECK(lines == accepted, "linhas %zu, eventos aceitos %zu", lines, accepted);
    CHECK(supervisor_queue_outcome(supervisor, &event).status == MPMC_OK, "fila liberada");
    finish(supervisor);
}

static void test_pending_outcomes_are_consolidated_at_shutdown(void) {
    static capture_t capture;
    supervisor_t *supervisor = new_supervisor();
    const job_outcome_t event = sample(
        JOB_OUTCOME_CANCELLED, COMMAND_CONSUME, 9, 1, 7, 3, (mpmc_result_t){.status = MPMC_OK}
    );
    redirect_t state;

    CHECK(supervisor_queue_outcome(supervisor, &event).status == MPMC_OK, "enfileirar");
    CHECK(redirect_begin(&state, NULL), "redirecionar");
    mpmc_result_t shutdown = supervisor_shutdown(supervisor, NULL);

    redirect_end(&state, &capture);
    CHECK(shutdown.status == MPMC_OK, "shutdown");
    CHECK(
        count_substring(capture.err, "operação 9 (consome, estrutura 1) cancelada") == 1u,
        "evento pendente deve ser apresentado no shutdown: %s",
        capture.err
    );
    supervisor_destroy(supervisor);
}

int main(void) {
    test_outcomes_are_presented_and_repl_continues();
    test_spawn_failure_is_contextual_and_recoverable();
    test_spawn_failure_texts_for_each_command_form();
    test_prompt_is_preserved();
    test_concurrent_events_are_whole_lines_without_global_order();
    test_notice_count_drives_prompt_redraw();
    test_queue_contract();
    test_pending_outcomes_are_consolidated_at_shutdown();

    printf("%u verificações, %u falhas\n", g_checks, g_failures);

    return g_failures == 0 ? 0 : 1;
}
