/**
 * @file test_supervisor.c
 * @brief Testes unitários do ciclo de vida de filhos em supervisor.c.
 * @ingroup tests
 *
 * @details
 * Cobre os critérios de aceite do supervisor:
 *
 * - término natural gera relatório (PID, tipo, operação, duração, código);
 * - filho morto por sinal fora do shutdown é recolhido sem que supervisor_reap()
 *   devolva erro (o REPL não deve encerrar) e os demais jobs continuam vivos;
 * - a duração é monotônica e coerente com o tempo real decorrido;
 * - shutdown recolhe todos os filhos e preserva os relatórios (idempotente);
 * - fork() malsucedido não registra filho, não consome operation_id e restaura a
 *   máscara de sinais.
 *
 * O supervisor.c deste teste é compilado com `-Dfork=mpmc_test_fork`, o que
 * permite injetar falhas de fork() de forma portável. Os cenários com worker
 * bloqueado usam MPMC_TEST_WORKER_HANG e exigem build com -DDEBUG e o utilitário
 * pgrep; sem eles, são ignorados com a mensagem SKIP.
 *
 * Retorna 0 se todos os casos passarem; 1 caso contrário.
 */

#include "mpmc/app/command.h"
#include "mpmc/app/supervisor.h"
#include "mpmc/error.h"

#include <errno.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

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
/* Injeção de falha em fork()                                         */
/* ------------------------------------------------------------------ */

static int g_fork_failures_left;

/** Substitui fork() em supervisor.c (compilado com -Dfork=mpmc_test_fork). */
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
/* Utilitários                                                        */
/* ------------------------------------------------------------------ */

static void sleep_ms(long milliseconds) {
    struct timespec delay = {.tv_sec = milliseconds / 1000, .tv_nsec = (milliseconds % 1000) * 1000000L};
    while (nanosleep(&delay, &delay) == -1 && errno == EINTR) {
    }
}

static command_t make_command(command_kind_t kind) {
    command_t command;
    memset(&command, 0, sizeof command);
    command.kind = kind;
    command.producer_id = 1u;
    command.message_count = 1u;
    command.ack_mode = 2u;
    return command;
}

/** Primeiro erro diferente de OK devolvido por supervisor_reap() em wait_for_reports(). */
static mpmc_result_t g_reap_error = (mpmc_result_t){.status = MPMC_OK};

/** Faz reap não bloqueante até haver `wanted` relatórios ou estourar o prazo. */
static bool wait_for_reports(supervisor_t *supervisor, size_t wanted, int timeout_ms) {
    for (int waited = 0; waited <= timeout_ms; waited += 10) {
        mpmc_result_t error = supervisor_reap(supervisor, false);
        if (error.status != MPMC_OK && g_reap_error.status == MPMC_OK) {
            g_reap_error = error;
        }
        if (supervisor_report_count(supervisor) >= wanted) {
            return true;
        }
        sleep_ms(10);
    }
    return false;
}

/**
 * @brief Executa supervisor_print_report() em um tmpfile e devolve o texto em `buffer`.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: supervisor_print_report() concluiu e a leitura do arquivo foi tentada; system_errno
 *   é zero.
 * - MPMC_ERR_INVALID_ARG: supervisor nulo detectado por supervisor_print_report(); system_errno
 *   é zero.
 * - MPMC_ERR_SYS: falha em tmpfile(), fputs(), fprintf() ou fflush(), com o errno capturado em
 *   system_errno, ou erro de stream indicado por ferror(), com system_errno zero.
 *
 * O retorno de supervisor_print_report() é preservado durante a leitura e o fechamento do
 * arquivo. Falhas de rewind(), fread() e fclose() não são incorporadas ao resultado deste
 * auxiliar.
 */
static mpmc_result_t capture_report(const supervisor_t *supervisor, char *buffer, size_t size) {
    FILE *file = tmpfile();
    int file_errno = errno;
    CHECK(file != NULL, "tmpfile");
    if (file == NULL) {
        buffer[0] = '\0';
        return (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = file_errno};
    }
    mpmc_result_t error = supervisor_print_report(supervisor, file);
    rewind(file);
    size_t length = fread(buffer, 1u, size - 1u, file);
    buffer[length] = '\0';
    (void)fclose(file);
    return error;
}

/** Conta as linhas de `text` que contêm `needle`. */
static size_t count_lines_with(const char *text, const char *needle) {
    size_t count = 0u;
    for (const char *line = text; *line != '\0';) {
        const char *end = strchr(line, '\n');
        size_t length = end == NULL ? strlen(line) : (size_t)(end - line);
        char copy[256];
        if (length >= sizeof copy) {
            length = sizeof copy - 1u;
        }
        memcpy(copy, line, length);
        copy[length] = '\0';
        if (strstr(copy, needle) != NULL) {
            ++count;
        }
        line = end == NULL ? line + strlen(line) : end + 1;
    }
    return count;
}

static supervisor_t *new_supervisor(void) {
    supervisor_t *supervisor = NULL;
    mpmc_result_t error = supervisor_create(&supervisor, 2u, IPC_SHARED_MEMORY);
    CHECK(error.status == MPMC_OK && supervisor != NULL, "supervisor_create falhou");
    CHECK(error.system_errno == 0, "sucesso não deve carregar errno anterior");
    return supervisor;
}

#ifdef DEBUG
/**
 * Lista os PIDs filhos do processo de teste via pgrep. Retorna -1 se falhar.
 * O shell criado por popen() também é filho: ele imprime o próprio PID primeiro
 * (`echo $$`) para ser descartado da contagem.
 */
static int list_children(pid_t *out, int max) {
    char command_line[96];
    (void)snprintf(
        command_line, sizeof command_line, "echo $$; pgrep -P %ld 2>/dev/null", (long)getpid()
    );
    FILE *pipe = popen(command_line, "r");
    if (pipe == NULL) {
        return -1;
    }
    long shell_pid = -1;
    if (fscanf(pipe, "%ld", &shell_pid) != 1) {
        (void)pclose(pipe);
        return -1;
    }
    int count = 0;
    long value;
    while (fscanf(pipe, "%ld", &value) == 1) {
        if (value != shell_pid && count < max) {
            out[count++] = (pid_t)value;
        }
    }
    (void)pclose(pipe);
    return count;
}

static bool have_pgrep(void) {
    return system("command -v pgrep >/dev/null 2>&1") == 0;
}

/** Espera até que existam exatamente `wanted` filhos vivos (pgrep). */
static int wait_for_children(pid_t *out, int wanted) {
    int count = 0;
    for (int attempt = 0; attempt < 100; ++attempt) {
        count = list_children(out, 16);
        if (count == wanted) {
            return count;
        }
        sleep_ms(20);
    }
    return count;
}
#endif

/* ------------------------------------------------------------------ */
/* Casos                                                              */
/* ------------------------------------------------------------------ */

static void test_natural_exit_report(void) {
    supervisor_t *supervisor = new_supervisor();
    command_t produce = make_command(COMMAND_PRODUCE);
    command_t consume = make_command(COMMAND_CONSUME);
    unsetenv("MPMC_TEST_WORKER_HANG");

    CHECK(supervisor_report_count(supervisor) == 0u, "sem filhos, sem relatórios");
    CHECK(supervisor_dispatch(supervisor, &produce).status == MPMC_OK, "dispatch produz");
    CHECK(supervisor_dispatch(supervisor, &consume).status == MPMC_OK, "dispatch consome");
    CHECK(wait_for_reports(supervisor, 2u, 5000), "filhos naturais não foram recolhidos");
    CHECK(supervisor_take_notices(supervisor) == 2u, "um aviso por filho recolhido");

    bool seen_produce = false;
    bool seen_consume = false;
    for (size_t index = 0u; index < supervisor_report_count(supervisor); ++index) {
        child_report_t report;
        CHECK(supervisor_report_get(supervisor, index, &report).status == MPMC_OK, "report_get");
        CHECK(report.pid > 0, "pid positivo");
        CHECK(report.end == CHILD_END_EXITED, "término natural: end=EXITED");
        CHECK(report.value == EXIT_FAILURE, "worker sem backend sai com EXIT_FAILURE");
        CHECK(report.duration_ns < 10ull * 1000000000ull, "duração coerente (< 10 s)");
        CHECK(report.operation_id == 1u || report.operation_id == 2u, "operation_id 1 ou 2");
        if (report.kind == COMMAND_PRODUCE) {
            seen_produce = report.operation_id == 1u;
        } else if (report.kind == COMMAND_CONSUME) {
            seen_consume = report.operation_id == 2u;
        }
    }
    CHECK(seen_produce && seen_consume, "tipo e operation_id associados ao PID correto");

    char text[4096];
    CHECK(capture_report(supervisor, text, sizeof text).status == MPMC_OK, "print_report");
    CHECK(strstr(text, "Relatório de execução") != NULL, "título do relatório");
    CHECK(strstr(text, "DURAÇÃO") != NULL, "cabeçalho da tabela");
    CHECK(count_lines_with(text, "produz") == 1u, "uma linha para o produz");
    CHECK(count_lines_with(text, "consome") == 1u, "uma linha para o consome");
    CHECK(count_lines_with(text, "código 1 (falha)") == 2u, "término com código e rótulo");
    CHECK(count_lines_with(text, ".") >= 3u && strstr(text, " s\n") != NULL,
          "durações no formato S.mmm s (2 filhos + total)");
    CHECK(strstr(text, "Filhos recolhidos: 2\n") != NULL, "contagem de filhos recolhidos");
    CHECK(strstr(text, "Filhos ainda ativos") == NULL, "sem filhos ativos");
    CHECK(strstr(text, "Tempo total de operação do mpmc: ") != NULL, "tempo total do mpmc");
    CHECK(strstr(text, ">>") == NULL, "relatório não contém o marcador de prompt");

    CHECK(supervisor_shutdown(supervisor).status == MPMC_OK, "shutdown sem filhos");
    CHECK(supervisor_report_count(supervisor) == 2u, "relatórios preservados após shutdown");
    supervisor_destroy(supervisor);
}

static void test_print_report_empty_and_guards(void) {
    supervisor_t *supervisor = new_supervisor();
    char text[1024];

    CHECK(capture_report(supervisor, text, sizeof text).status == MPMC_OK, "relatório vazio");
    CHECK(strstr(text, "Nenhum processo filho foi executado.") != NULL, "mensagem de vazio");
    CHECK(strstr(text, "Filhos recolhidos: 0\n") != NULL, "zero filhos recolhidos");
    CHECK(strstr(text, "Tempo total de operação do mpmc: 0.") != NULL,
          "tempo total presente (< 1 s) mesmo sem filhos");
    CHECK(supervisor_print_report(NULL, stdout).status == MPMC_ERR_INVALID_ARG, "supervisor NULL");
    CHECK(supervisor_print_report(supervisor, NULL).status == MPMC_ERR_INVALID_ARG, "stream NULL");

    FILE *read_only = fopen("/dev/null", "r");
    CHECK(read_only != NULL, "abrir stream somente para leitura");
    if (read_only != NULL) {
        mpmc_result_t error = supervisor_print_report(supervisor, read_only);
        errno = EINVAL;
        CHECK(error.status == MPMC_ERR_SYS, "falha de escrita deve retornar ERR_SYS");
        CHECK(error.system_errno == EBADF, "relatório deve preservar causa da falha de escrita");
        (void)fclose(read_only);
    }

    (void)supervisor_shutdown(supervisor);
    supervisor_destroy(supervisor);
}

static void test_report_get_guards(void) {
    supervisor_t *supervisor = new_supervisor();
    child_report_t report;
    memset(&report, 0x5A, sizeof report);
    child_report_t untouched = report;

    CHECK(supervisor_report_count(NULL) == 0u, "report_count(NULL) == 0");
    CHECK(supervisor_report_get(NULL, 0u, &report).status == MPMC_ERR_INVALID_ARG, "supervisor NULL");
    CHECK(supervisor_report_get(supervisor, 0u, &report).status == MPMC_ERR_INVALID_ARG, "índice vazio");
    CHECK(supervisor_report_get(supervisor, 0u, NULL).status == MPMC_ERR_INVALID_ARG, "out NULL");
    CHECK(memcmp(&report, &untouched, sizeof report) == 0, "out intacto em erro");

    (void)supervisor_shutdown(supervisor);
    supervisor_destroy(supervisor);
}

static void test_fork_failure(void) {
    supervisor_t *supervisor = new_supervisor();
    command_t produce = make_command(COMMAND_PRODUCE);
    unsetenv("MPMC_TEST_WORKER_HANG");

    g_fork_failures_left = 2;
    mpmc_result_t error = supervisor_dispatch(supervisor, &produce);
    errno = EINVAL;
    CHECK(error.status == MPMC_ERR_SYS, "fork falho -> ERR_SYS");
    CHECK(error.system_errno == EAGAIN, "erro de fork deve preservar EAGAIN após restaurar sinais");
    CHECK(supervisor_dispatch(supervisor, &produce).status == MPMC_ERR_SYS, "segunda falha -> ERR_SYS");
    CHECK(g_fork_failures_left == 0, "as duas falhas injetadas foram consumidas");

    /* Nenhum registro inválido: reap não tem o que recolher e nada vira relatório. */
    CHECK(supervisor_reap(supervisor, false).status == MPMC_OK, "reap sem filhos após fork falho");
    CHECK(supervisor_report_count(supervisor) == 0u, "fork falho não gera relatório");
    CHECK(supervisor_take_notices(supervisor) == 0u, "fork falho não gera aviso de filho");

    /* A máscara de sinais foi restaurada. */
    sigset_t current;
    CHECK(sigprocmask(SIG_BLOCK, NULL, &current) == 0, "sigprocmask");
    CHECK(!sigismember(&current, SIGCHLD) && !sigismember(&current, SIGINT)
              && !sigismember(&current, SIGTERM),
          "sinais devem continuar desbloqueados após fork falho");

    /* O supervisor segue utilizável e o id da operação não foi consumido. */
    CHECK(supervisor_dispatch(supervisor, &produce).status == MPMC_OK, "dispatch após falhas");
    CHECK(wait_for_reports(supervisor, 1u, 5000), "filho posterior recolhido");
    child_report_t report;
    CHECK(supervisor_report_get(supervisor, 0u, &report).status == MPMC_OK, "report_get");
    CHECK(report.operation_id == 1u, "operation_id não é consumido por fork falho (veio %llu)",
          (unsigned long long)report.operation_id);

    CHECK(supervisor_shutdown(supervisor).status == MPMC_OK, "shutdown");
    supervisor_destroy(supervisor);
}

#ifdef DEBUG
static void test_signal_kill_is_not_fatal(void) {
    supervisor_t *supervisor = new_supervisor();
    command_t produce = make_command(COMMAND_PRODUCE);
    command_t consume = make_command(COMMAND_CONSUME);
    pid_t children[16];

    setenv("MPMC_TEST_WORKER_HANG", "1", 1);
    CHECK(supervisor_dispatch(supervisor, &produce).status == MPMC_OK, "dispatch 1");
    CHECK(supervisor_dispatch(supervisor, &consume).status == MPMC_OK, "dispatch 2");
    unsetenv("MPMC_TEST_WORKER_HANG");

    int count = wait_for_children(children, 2);
    CHECK(count == 2, "dois jobs devem coexistir (pgrep viu %d)", count);
    if (count != 2) {
        (void)supervisor_shutdown(supervisor);
        supervisor_destroy(supervisor);
        return;
    }

    sleep_ms(120);
    pid_t victim = children[0];
    g_reap_error = (mpmc_result_t){.status = MPMC_OK};
    CHECK(kill(victim, SIGKILL) == 0, "kill SIGKILL");
    CHECK(wait_for_reports(supervisor, 1u, 5000), "filho morto por sinal não foi recolhido");

    /* Regressão: o reap que recolhe um filho morto por sinal (fora do shutdown) não
     * pode devolver erro; caso contrário o REPL encerraria e derrubaria os demais jobs. */
    CHECK(g_reap_error.status == MPMC_OK, "reap devolveu erro (%d) ao recolher filho morto por sinal",
          (int)g_reap_error.status);
    CHECK(supervisor_take_notices(supervisor) >= 1u, "término por sinal gera aviso");

    child_report_t report;
    CHECK(supervisor_report_get(supervisor, 0u, &report).status == MPMC_OK, "report_get");
    CHECK(report.pid == victim, "relatório do PID morto");
    CHECK(report.end == CHILD_END_SIGNALED && report.value == SIGKILL, "end=SIGNALED, sinal 9");
    CHECK(report.duration_ns >= 100ull * 1000000ull,
          "duração deve cobrir o tempo vivo (>=100 ms; veio %llu ns)",
          (unsigned long long)report.duration_ns);
    CHECK(report.duration_ns < 10ull * 1000000000ull, "duração coerente (< 10 s)");

    /* O outro job segue vivo e o supervisor continua operante. */
    CHECK(list_children(children, 16) == 1, "o outro job continua vivo");
    CHECK(supervisor_dispatch(supervisor, &produce).status == MPMC_OK, "dispatch após filho morto");

    CHECK(supervisor_shutdown(supervisor).status == MPMC_OK, "shutdown");
    CHECK(supervisor_report_count(supervisor) == 3u, "três relatórios ao final");

    char text[4096];
    CHECK(capture_report(supervisor, text, sizeof text).status == MPMC_OK, "print_report");
    CHECK(count_lines_with(text, "sinal 9") == 1u, "uma linha com o SIGKILL");
    CHECK(count_lines_with(text, "sinal 15") == 2u, "duas linhas com o SIGTERM do shutdown");
    CHECK(strstr(text, "Filhos recolhidos: 3\n") != NULL, "três filhos recolhidos");
    supervisor_destroy(supervisor);
}

static void test_shutdown_reports(void) {
    supervisor_t *supervisor = new_supervisor();
    command_t produce = make_command(COMMAND_PRODUCE);
    pid_t children[16];

    setenv("MPMC_TEST_WORKER_HANG", "1", 1);
    for (int job = 0; job < 3; ++job) {
        CHECK(supervisor_dispatch(supervisor, &produce).status == MPMC_OK, "dispatch %d", job);
    }
    unsetenv("MPMC_TEST_WORKER_HANG");
    CHECK(wait_for_children(children, 3) == 3, "três jobs simultâneos");

    CHECK(supervisor_shutdown(supervisor).status == MPMC_OK, "shutdown com jobs bloqueados");
    CHECK(supervisor_report_count(supervisor) == 3u, "um relatório por filho");
    for (size_t index = 0u; index < supervisor_report_count(supervisor); ++index) {
        child_report_t report;
        CHECK(supervisor_report_get(supervisor, index, &report).status == MPMC_OK, "report_get");
        CHECK(report.end == CHILD_END_SIGNALED && report.value == SIGTERM,
              "encerrado por SIGTERM do shutdown");
        CHECK(report.duration_ns < 10ull * 1000000000ull, "duração coerente");
    }
    CHECK(list_children(children, 16) == 0, "nenhum filho (nem zumbi) após shutdown");

    CHECK(supervisor_shutdown(supervisor).status == MPMC_OK, "shutdown idempotente");
    CHECK(supervisor_report_count(supervisor) == 3u, "relatórios não duplicam");
    CHECK(supervisor_dispatch(supervisor, &produce).status == MPMC_ERR_SHUTDOWN, "dispatch após shutdown");
    supervisor_destroy(supervisor);
}

static void test_fork_failure_with_active_job(void) {
    supervisor_t *supervisor = new_supervisor();
    command_t produce = make_command(COMMAND_PRODUCE);
    pid_t children[16];

    setenv("MPMC_TEST_WORKER_HANG", "1", 1);
    CHECK(supervisor_dispatch(supervisor, &produce).status == MPMC_OK, "dispatch ativo");
    g_fork_failures_left = 1;
    CHECK(supervisor_dispatch(supervisor, &produce).status == MPMC_ERR_SYS, "fork falho com job ativo");
    unsetenv("MPMC_TEST_WORKER_HANG");

    CHECK(wait_for_children(children, 1) == 1, "job ativo não é afetado pela falha");
    CHECK(supervisor_shutdown(supervisor).status == MPMC_OK, "shutdown");
    CHECK(supervisor_report_count(supervisor) == 1u, "somente o job realmente criado");
    supervisor_destroy(supervisor);
}
#endif

/* ------------------------------------------------------------------ */
/* Execução                                                           */
/* ------------------------------------------------------------------ */

int main(void) {
    test_natural_exit_report();
    test_report_get_guards();
    test_print_report_empty_and_guards();
    test_fork_failure();

#ifdef DEBUG
    if (have_pgrep()) {
        test_signal_kill_is_not_fatal();
        test_shutdown_reports();
        test_fork_failure_with_active_job();
    } else {
        puts("SKIP: cenários com worker bloqueado exigem pgrep.");
    }
#else
    puts("SKIP: cenários com worker bloqueado exigem build com -DDEBUG.");
#endif

    printf("%u verificações, %u falhas\n", g_checks, g_failures);
    return g_failures == 0 ? 0 : 1;
}
