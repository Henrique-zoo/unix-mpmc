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

#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include "mpmc/app/command.h"
#include "mpmc/app/supervisor.h"
#include "mpmc/error.h"

/**
 * @brief Quantidade de verificações executadas nesta suíte.
 */
static unsigned g_checks;
/**
 * @brief Quantidade de verificações que registraram falha nesta suíte.
 */
static unsigned g_failures;

/**
 * @brief Registra uma verificação e informa a falha sem interromper a suíte.
 * @param condition Expressão lógica avaliada uma única vez.
 * @param ... Formato e argumentos de fprintf() para o diagnóstico em caso de falha.
 * @details Incrementa o contador de verificações; se a condição for falsa,
 * incrementa o contador de falhas e escreve o diagnóstico em stderr.
 */
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

/**
 * @brief Número de próximas chamadas a fork() que devem falhar com EAGAIN.
 */
static int g_fork_failures_left;

/** Substitui fork() em supervisor.c (compilado com -Dfork=mpmc_test_fork). */
pid_t mpmc_test_fork(void);
/**
 * @brief Substitui fork() somente no objeto do supervisor usado nos testes.
 * @return PID positivo no pai ou zero no filho quando fork() funcionar; -1 em
 * falha real ou quando houver uma falha programada em g_fork_failures_left.
 * @details Na falha programada, decrementa o contador e define errno=EAGAIN.
 * Fora desse caso, devolve diretamente o resultado de fork().
 */
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

/**
 * @brief Suspende o teste aproximadamente pelo intervalo solicitado.
 * @param milliseconds Intervalo não negativo, em milissegundos.
 * @details Repete nanosleep() com o tempo restante após EINTR. Outra falha encerra
 * a espera sem propagação de erro; este auxiliar não serve como relógio de precisão.
 */
static void sleep_ms(long milliseconds) {
    struct timespec delay = {
        .tv_sec = milliseconds / 1000,
        .tv_nsec = (milliseconds % 1000) * 1000000L,
    };
    while (nanosleep(&delay, &delay) == -1 && errno == EINTR);
}

/**
 * @brief Constrói um comando de trabalho com parâmetros válidos para os cenários.
 * @param kind Tipo de trabalho a atribuir ao comando.
 * @return Comando com índice 1, uma mensagem e ack_mode igual a 2.
 * @pre kind deve identificar o cenário que o teste pretende despachar.
 */
static command_t make_command(command_kind_t kind) {
    command_t command;
    memset(&command, 0, sizeof command);
    command.kind = kind;
    command.producer_id = 1u;
    command.message_count = 1u;
    command.ack_mode = 2u;
    return command;
}

/** @brief Primeiro erro de coleta observado por wait_for_reports(), até o teste reinicializá-lo. */
static mpmc_result_t g_reap_error = (mpmc_result_t){.status = MPMC_OK};

/**
 * @brief Faz coletas não bloqueantes até obter a quantidade de relatórios desejada.
 * @param supervisor Instância viva a consultar.
 * @param wanted Quantidade mínima de relatórios que satisfaz a espera.
 * @param timeout_ms Limite aproximado, não negativo, em milissegundos.
 * @return true se a quantidade for alcançada; false após esgotar as tentativas.
 * @details Consulta a cada 10 ms e preserva o primeiro erro de supervisor_reap()
 * em g_reap_error. Um erro de coleta não encerra sozinho as tentativas.
 */
static bool wait_for_reports(supervisor_t *supervisor, size_t wanted, int timeout_ms) {
    for (int waited = 0; waited <= timeout_ms; waited += 10) {
        mpmc_result_t error = supervisor_reap(supervisor, false);
        if (error.status != MPMC_OK && g_reap_error.status == MPMC_OK) {
            g_reap_error = error;
        }
        if (supervisor_report_count(supervisor) >= wanted) return true;
        sleep_ms(10);
    }
    return false;
}

/**
 * @brief Executa supervisor_print_report() em um tmpfile e devolve o texto em `buffer`.
 * @param supervisor Instância a apresentar; pode ser NULL para exercitar a validação.
 * @param[out] buffer Destino válido do texto, sempre terminado em NUL.
 * @param size Capacidade positiva do destino, incluindo o terminador.
 * @pre buffer deve possuir pelo menos size bytes e size deve ser maior que zero.
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
        return (mpmc_result_t){
            .status = MPMC_ERR_SYS,
            .system_errno = file_errno,
        };
    }
    mpmc_result_t error = supervisor_print_report(supervisor, file);
    rewind(file);
    size_t length = fread(buffer, 1u, size - 1u, file);
    buffer[length] = '\0';
    (void)fclose(file);
    return error;
}

/**
 * @brief Conta linhas cuja porção inicial contém a substring procurada.
 * @param text Texto válido e terminado em NUL, com linhas separadas por LF.
 * @param needle Substring válida e terminada em NUL a procurar em cada linha.
 * @return Quantidade de linhas cuja cópia inicial, limitada a 255 bytes, contém needle.
 * @note A última linha também é examinada quando não possui LF final.
 */
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

/**
 * @brief Cria um supervisor com duas estruturas no modo de memória compartilhada.
 * @return Instância criada, ou NULL se supervisor_create() falhar.
 * @details Registra falha na preparação quando o resultado ou a instância não forem
 * os esperados. O chamador continua responsável pelo shutdown e pela destruição.
 */
static supervisor_t *new_supervisor(void) {
    supervisor_t *supervisor = NULL;
    mpmc_result_t error = supervisor_create(&supervisor, 2u, IPC_SHARED_MEMORY);
    CHECK(error.status == MPMC_OK && supervisor != NULL, "supervisor_create falhou");
    CHECK(error.system_errno == 0, "sucesso não deve carregar errno anterior");
    return supervisor;
}

#ifdef DEBUG
/**
 * @brief Lista filhos do processo de teste consultando pgrep por meio de popen().
 * @param[out] out Vetor que recebe os PIDs encontrados, excluído o shell auxiliar.
 * @param max Capacidade não negativa do vetor.
 * @return Quantidade armazenada, limitada a max; -1 se popen() falhar ou se não
 * for possível ler o PID do shell auxiliar.
 * @details O shell imprime seu próprio PID antes de executar pgrep, permitindo
 * excluí-lo. O estado retornado por pclose() não é propagado.
 */
static int list_children(pid_t *out, int max) {
    char command_line[96];
    (void)snprintf(
        command_line, sizeof command_line, "echo $$; pgrep -P %ld 2>/dev/null", (long)getpid()
    );
    FILE *pipe = popen(command_line, "r");
    if (pipe == NULL) return -1;
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

/**
 * @brief Verifica se pgrep pode ser encontrado pelo shell.
 * @return true se a consulta executada por system() retornar zero; false caso contrário.
 */
static bool have_pgrep(void) {
    return system("command -v pgrep >/dev/null 2>&1") == 0;
}

/**
 * @brief Aguarda aproximadamente dois segundos pela quantidade exata de filhos.
 * @param[out] out Vetor válido com capacidade para 16 PIDs.
 * @param wanted Quantidade exata que encerra antecipadamente a espera.
 * @return Última contagem de list_children(), inclusive -1 em falha de consulta.
 * @details Realiza até cem consultas, separadas por esperas de 20 ms.
 */
static int wait_for_children(pid_t *out, int wanted) {
    int count = 0;
    for (int attempt = 0; attempt < 100; ++attempt) {
        count = list_children(out, 16);
        if (count == wanted) return count;
        sleep_ms(20);
    }
    return count;
}
#endif

/* ------------------------------------------------------------------ */
/* Casos                                                              */
/* ------------------------------------------------------------------ */

/**
 * @brief Confere coleta natural, identidade dos trabalhos e conteúdo do relatório final.
 */
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
    CHECK(
        count_lines_with(text, ".") >= 3u && strstr(text, " s\n") != NULL,
        "durações no formato S.mmm s (2 filhos + total)"
    );
    CHECK(strstr(text, "Filhos recolhidos: 2\n") != NULL, "contagem de filhos recolhidos");
    CHECK(strstr(text, "Filhos ainda ativos") == NULL, "sem filhos ativos");
    CHECK(strstr(text, "Tempo total de operação do mpmc: ") != NULL, "tempo total do mpmc");
    CHECK(strstr(text, ">>") == NULL, "relatório não contém o marcador de prompt");

    CHECK(supervisor_shutdown(supervisor).status == MPMC_OK, "shutdown sem filhos");
    CHECK(supervisor_report_count(supervisor) == 2u, "relatórios preservados após shutdown");
    supervisor_destroy(supervisor);
}

/**
 * @brief Confere relatório vazio, argumentos nulos e preservação do erro de escrita.
 */
static void test_print_report_empty_and_guards(void) {
    supervisor_t *supervisor = new_supervisor();
    char text[1024];

    CHECK(capture_report(supervisor, text, sizeof text).status == MPMC_OK, "relatório vazio");
    CHECK(strstr(text, "Nenhum processo filho foi executado.") != NULL, "mensagem de vazio");
    CHECK(strstr(text, "Filhos recolhidos: 0\n") != NULL, "zero filhos recolhidos");
    CHECK(
        strstr(text, "Tempo total de operação do mpmc: 0.") != NULL,
        "tempo total presente (< 1 s) mesmo sem filhos"
    );
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

/**
 * @brief Confere consultas inválidas sem alterar a saída de supervisor_report_get().
 */
static void test_report_get_guards(void) {
    supervisor_t *supervisor = new_supervisor();
    child_report_t report;
    memset(&report, 0x5A, sizeof report);
    child_report_t untouched = report;

    CHECK(supervisor_report_count(NULL) == 0u, "report_count(NULL) == 0");
    CHECK(
        supervisor_report_get(NULL, 0u, &report).status == MPMC_ERR_INVALID_ARG, "supervisor NULL"
    );
    CHECK(
        supervisor_report_get(supervisor, 0u, &report).status == MPMC_ERR_INVALID_ARG,
        "índice vazio"
    );
    CHECK(supervisor_report_get(supervisor, 0u, NULL).status == MPMC_ERR_INVALID_ARG, "out NULL");
    CHECK(memcmp(&report, &untouched, sizeof report) == 0, "out intacto em erro");

    (void)supervisor_shutdown(supervisor);
    supervisor_destroy(supervisor);
}

/**
 * @brief Confere falhas de fork(), restauração de sinais e preservação do próximo operation_id.
 */
static void test_fork_failure(void) {
    supervisor_t *supervisor = new_supervisor();
    command_t produce = make_command(COMMAND_PRODUCE);
    unsetenv("MPMC_TEST_WORKER_HANG");

    g_fork_failures_left = 2;
    mpmc_result_t error = supervisor_dispatch(supervisor, &produce);
    errno = EINVAL;
    CHECK(error.status == MPMC_ERR_SYS, "fork falho -> ERR_SYS");
    CHECK(error.system_errno == EAGAIN, "erro de fork deve preservar EAGAIN após restaurar sinais");
    CHECK(
        supervisor_dispatch(supervisor, &produce).status == MPMC_ERR_SYS, "segunda falha -> ERR_SYS"
    );
    CHECK(g_fork_failures_left == 0, "as duas falhas injetadas foram consumidas");

    /* Nenhum registro inválido: reap não tem o que recolher e nada vira relatório. */
    CHECK(supervisor_reap(supervisor, false).status == MPMC_OK, "reap sem filhos após fork falho");
    CHECK(supervisor_report_count(supervisor) == 0u, "fork falho não gera relatório");
    CHECK(supervisor_take_notices(supervisor) == 0u, "fork falho não gera aviso de filho");

    /* A máscara de sinais foi restaurada. */
    sigset_t current;
    CHECK(sigprocmask(SIG_BLOCK, NULL, &current) == 0, "sigprocmask");
    CHECK(
        !sigismember(&current, SIGCHLD) && !sigismember(&current, SIGINT)
            && !sigismember(&current, SIGTERM),
        "sinais devem continuar desbloqueados após fork falho"
    );

    /* O supervisor segue utilizável e o id da operação não foi consumido. */
    CHECK(supervisor_dispatch(supervisor, &produce).status == MPMC_OK, "dispatch após falhas");
    CHECK(wait_for_reports(supervisor, 1u, 5000), "filho posterior recolhido");
    child_report_t report;
    CHECK(supervisor_report_get(supervisor, 0u, &report).status == MPMC_OK, "report_get");
    CHECK(
        report.operation_id == 1u,
        "operation_id não é consumido por fork falho (veio %llu)",
        (unsigned long long)report.operation_id
    );

    CHECK(supervisor_shutdown(supervisor).status == MPMC_OK, "shutdown");
    supervisor_destroy(supervisor);
}

#ifdef DEBUG
/**
 * @brief Confere que a morte de um job por sinal preserva os demais e fica no relatório.
 */
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
    CHECK(
        g_reap_error.status == MPMC_OK,
        "reap devolveu erro (%d) ao recolher filho morto por sinal",
        (int)g_reap_error.status
    );
    CHECK(supervisor_take_notices(supervisor) >= 1u, "término por sinal gera aviso");

    child_report_t report;
    CHECK(supervisor_report_get(supervisor, 0u, &report).status == MPMC_OK, "report_get");
    CHECK(report.pid == victim, "relatório do PID morto");
    CHECK(report.end == CHILD_END_SIGNALED && report.value == SIGKILL, "end=SIGNALED, sinal 9");
    CHECK(
        report.duration_ns >= 100ull * 1000000ull,
        "duração deve cobrir o tempo vivo (>=100 ms; veio %llu ns)",
        (unsigned long long)report.duration_ns
    );
    CHECK(report.duration_ns < 10ull * 1000000000ull, "duração coerente (< 10 s)");

    /* O outro job segue vivo e o supervisor continua operante. */
    CHECK(list_children(children, 16) == 1, "o outro job continua vivo");
    /* O terceiro filho também precisa ficar bloqueado; do contrário ele termina sozinho
     * (worker ainda não implementado) antes do SIGTERM do shutdown e o teste fica instável. */
    setenv("MPMC_TEST_WORKER_HANG", "1", 1);
    CHECK(supervisor_dispatch(supervisor, &produce).status == MPMC_OK, "dispatch após filho morto");
    unsetenv("MPMC_TEST_WORKER_HANG");

    CHECK(supervisor_shutdown(supervisor).status == MPMC_OK, "shutdown");
    CHECK(supervisor_report_count(supervisor) == 3u, "três relatórios ao final");

    char text[4096];
    CHECK(capture_report(supervisor, text, sizeof text).status == MPMC_OK, "print_report");
    CHECK(count_lines_with(text, "sinal 9") == 1u, "uma linha com o SIGKILL");
    CHECK(count_lines_with(text, "sinal 15") == 2u, "duas linhas com o SIGTERM do shutdown");
    CHECK(strstr(text, "Filhos recolhidos: 3\n") != NULL, "três filhos recolhidos");
    supervisor_destroy(supervisor);
}

/**
 * @brief Confere a coleta de jobs bloqueados e a preservação dos relatórios no shutdown repetido.
 */
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
        CHECK(
            report.end == CHILD_END_SIGNALED && report.value == SIGTERM,
            "encerrado por SIGTERM do shutdown"
        );
        CHECK(report.duration_ns < 10ull * 1000000000ull, "duração coerente");
    }
    CHECK(list_children(children, 16) == 0, "nenhum filho (nem zumbi) após shutdown");

    CHECK(supervisor_shutdown(supervisor).status == MPMC_OK, "shutdown idempotente");
    CHECK(supervisor_report_count(supervisor) == 3u, "relatórios não duplicam");
    CHECK(
        supervisor_dispatch(supervisor, &produce).status == MPMC_ERR_SHUTDOWN,
        "dispatch após shutdown"
    );
    supervisor_destroy(supervisor);
}

/**
 * @brief Confere que uma falha de fork() não remove nem altera um job já ativo.
 */
static void test_fork_failure_with_active_job(void) {
    supervisor_t *supervisor = new_supervisor();
    command_t produce = make_command(COMMAND_PRODUCE);
    pid_t children[16];

    setenv("MPMC_TEST_WORKER_HANG", "1", 1);
    CHECK(supervisor_dispatch(supervisor, &produce).status == MPMC_OK, "dispatch ativo");
    g_fork_failures_left = 1;
    CHECK(
        supervisor_dispatch(supervisor, &produce).status == MPMC_ERR_SYS, "fork falho com job ativo"
    );
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

/**
 * @brief Executa os casos da suíte e apresenta o resultado das verificações.
 * @return Zero se nenhuma verificação falhar; um se houver falha ou erro de preparação.
 */
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
