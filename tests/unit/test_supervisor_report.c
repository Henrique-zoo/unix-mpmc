/**
 * @file test_supervisor_report.c
 * @brief Valida a apresentação dos relatórios do supervisor com dados controlados.
 * @ingroup tests
 *
 * @details
 * Fixture isolada para a issue #53. Substitui fork(), waitpid(), clock_gettime()
 * e as operações IPC usadas pelo supervisor somente no objeto deste teste.
 *
 * Verifica:
 * - associação entre PID, operation_id, tipo e estado de término;
 * - saída normal, código de falha e interrupção por sinal;
 * - duração positiva entre segundos, duração zero por regressão do relógio e
 *   duração zero quando clock_gettime() falha;
 * - tempo total do processo principal, inclusive quando o relógio falha;
 * - contagens controladas para todas as estruturas lógicas 1..p;
 * - apresentação de cada linha sem pressupor a ordem de coleta dos filhos.
 *
 * Nenhum processo filho real nem recurso IPC System V é criado. Os estados que
 * seriam entregues por waitpid() são fornecidos pela fixture no processo pai.
 */

#include <errno.h>
#include <inttypes.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>

#include "mpmc/app/command.h"
#include "mpmc/app/supervisor.h"
#include "mpmc/error.h"
#include "mpmc/ipc/transport.h"

#define FIXTURE_CHILDREN 3u
#define FIXTURE_CLOCKS 9u
#define FIXTURE_PRODUCERS 3u

struct ipc_context {
    unsigned marker;
};

typedef struct {
    pid_t pid;
    int wait_status;
    bool ready;
    bool collected;
} fixture_child_t;

static struct ipc_context g_ipc_context = {.marker = 53u};

static struct {
    bool active;
    size_t next_fork;
    fixture_child_t children[FIXTURE_CHILDREN];
    struct timespec clocks[FIXTURE_CLOCKS];
    bool fail_clock[FIXTURE_CLOCKS];
    size_t clock_index;
    size_t fork_calls;
    size_t waitpid_calls;
    size_t init_calls;
    size_t destroy_calls;
    size_t poll_calls;
    size_t count_calls;
    size_t count_ids[FIXTURE_CLOCKS];
    uint64_t consumed[FIXTURE_PRODUCERS];
    bool bad_clock_id;
    bool clock_overrun;
    bool bad_wait_options;
    bool bad_ipc_context;
} g_fixture;

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
/* Substitutos ligados somente ao objeto supervisor_report_for_test.o */
/* ------------------------------------------------------------------ */

pid_t mpmc_test_fork(void) {
    ++g_fixture.fork_calls;
    if (!g_fixture.active || g_fixture.next_fork >= FIXTURE_CHILDREN) {
        errno = EAGAIN;
        return -1;
    }
    return g_fixture.children[g_fixture.next_fork++].pid;
}

pid_t mpmc_test_waitpid(pid_t pid, int *status, int options) {
    ++g_fixture.waitpid_calls;
    if (!g_fixture.active || status == NULL) {
        errno = EINVAL;
        return -1;
    }
    if (options != WNOHANG) {
        g_fixture.bad_wait_options = true;
        errno = EINVAL;
        return -1;
    }

    for (size_t index = 0u; index < FIXTURE_CHILDREN; ++index) {
        fixture_child_t *child = &g_fixture.children[index];
        if (child->pid != pid) continue;
        if (!child->ready || child->collected) return 0;

        child->collected = true;
        *status = child->wait_status;
        return pid;
    }

    errno = ECHILD;
    return -1;
}

int mpmc_test_clock_gettime(clockid_t clock_id, struct timespec *out) {
    if (!g_fixture.active || out == NULL) {
        errno = EINVAL;
        return -1;
    }
    if (clock_id != CLOCK_MONOTONIC) {
        g_fixture.bad_clock_id = true;
        errno = EINVAL;
        return -1;
    }

    size_t call = g_fixture.clock_index++;
    if (call >= FIXTURE_CLOCKS) {
        g_fixture.clock_overrun = true;
        errno = EOVERFLOW;
        return -1;
    }
    if (g_fixture.fail_clock[call]) {
        errno = EIO;
        return -1;
    }

    *out = g_fixture.clocks[call];
    return 0;
}

mpmc_result_t mpmc_test_ipc_init(
    ipc_context_t **out, size_t producer_count, ipc_mode_t mode
) {
    if (!g_fixture.active || out == NULL) {
        return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};
    }
    *out = NULL;
    if (producer_count != FIXTURE_PRODUCERS || mode != IPC_SHARED_MEMORY) {
        return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};
    }

    ++g_fixture.init_calls;
    *out = &g_ipc_context;
    return (mpmc_result_t){.status = MPMC_OK};
}

void mpmc_test_ipc_destroy(ipc_context_t *context) {
    ++g_fixture.destroy_calls;
    if (context != &g_ipc_context) {
        g_fixture.bad_ipc_context = true;
    }
}

mpmc_result_t mpmc_test_ipc_poll_events(ipc_context_t *context) {
    if (!g_fixture.active || context != &g_ipc_context) {
        g_fixture.bad_ipc_context = true;
        return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};
    }

    ++g_fixture.poll_calls;
    return (mpmc_result_t){.status = MPMC_OK};
}

mpmc_result_t mpmc_test_ipc_consumed_count(
    ipc_context_t *context, size_t producer_id, uint64_t *out_count
) {
    if (!g_fixture.active || context != &g_ipc_context || out_count == NULL
        || producer_id == 0u || producer_id > FIXTURE_PRODUCERS) {
        g_fixture.bad_ipc_context = true;
        return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};
    }

    if (g_fixture.count_calls < FIXTURE_CLOCKS) {
        g_fixture.count_ids[g_fixture.count_calls] = producer_id;
    }
    ++g_fixture.count_calls;
    *out_count = g_fixture.consumed[producer_id - 1u];
    return (mpmc_result_t){.status = MPMC_OK};
}

/* ------------------------------------------------------------------ */
/* Auxiliares                                                         */
/* ------------------------------------------------------------------ */

static command_t make_command(command_kind_t kind) {
    return (command_t){
        .kind = kind,
        .producer_id = 1u,
        .message_count = 1u,
        .ack_mode = 2u,
    };
}

static void reset_fixture(void) {
    memset(&g_fixture, 0, sizeof g_fixture);
    g_fixture.active = true;

    g_fixture.children[0] = (fixture_child_t){
        .pid = 4101,
        .wait_status = EXIT_SUCCESS << 8,
        .ready = false,
    };
    g_fixture.children[1] = (fixture_child_t){
        .pid = 4102,
        .wait_status = SIGTERM,
        .ready = true,
    };
    g_fixture.children[2] = (fixture_child_t){
        .pid = 4103,
        .wait_status = 7 << 8,
        .ready = true,
    };

    /*
     * Ordem das chamadas:
     * 1: criação do supervisor;
     * 2..4: início dos três filhos;
     * 5..7: recolhimento de 4102, 4103 e 4101;
     * 8: primeira impressão, com falha do relógio total;
     * 9: segunda impressão, com relógio disponível.
     */
    const struct timespec clocks[FIXTURE_CLOCKS] = {
        {.tv_sec = 100, .tv_nsec = 900000000},
        {.tv_sec = 101, .tv_nsec = 900000000},
        {.tv_sec = 102, .tv_nsec = 200000000},
        {.tv_sec = 103, .tv_nsec = 300000000},
        {.tv_sec = 102, .tv_nsec = 100000000},
        {.tv_sec = 103, .tv_nsec = 200000000},
        {.tv_sec = 102, .tv_nsec = 100000000},
        {.tv_sec = 104, .tv_nsec = 900000000},
        {.tv_sec = 104, .tv_nsec = 900000000},
    };
    memcpy(g_fixture.clocks, clocks, sizeof clocks);

    /* Falha na duração do filho 4102 e na primeira medição do total. */
    g_fixture.fail_clock[4] = true;
    g_fixture.fail_clock[7] = true;

    g_fixture.consumed[0] = 17u;
    g_fixture.consumed[1] = 29u;
    g_fixture.consumed[2] = 41u;
}

static bool report_for_pid(
    const supervisor_t *supervisor, pid_t pid, child_report_t *out
) {
    for (size_t index = 0u; index < supervisor_report_count(supervisor); ++index) {
        child_report_t candidate;
        if (supervisor_report_get(supervisor, index, &candidate).status != MPMC_OK) {
            return false;
        }
        if (candidate.pid == pid) {
            *out = candidate;
            return true;
        }
    }
    return false;
}

static mpmc_result_t capture_report(
    const supervisor_t *supervisor, char *buffer, size_t size
) {
    if (buffer == NULL || size < 2u) {
        return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};
    }

    FILE *file = tmpfile();
    if (file == NULL) {
        return (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = errno};
    }

    mpmc_result_t result = supervisor_print_report(supervisor, file);
    if (fseek(file, 0L, SEEK_SET) != 0) {
        int saved_errno = errno;
        (void)fclose(file);
        return (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = saved_errno};
    }

    size_t length = fread(buffer, 1u, size - 1u, file);
    bool read_failed = ferror(file) != 0;
    buffer[length] = '\0';
    (void)fclose(file);

    if (read_failed) {
        return (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = errno};
    }
    return result;
}

static void check_report_line(
    const char *output,
    pid_t pid,
    uint64_t operation_id,
    const char *kind,
    const char *end,
    const char *duration
) {
    char expected[256];
    (void)snprintf(
        expected,
        sizeof expected,
        "%-8ld %-6" PRIu64 " %-8s %-20s %9s",
        (long)pid,
        operation_id,
        kind,
        end,
        duration
    );
    CHECK(strstr(output, expected) != NULL, "linha esperada ausente: %s", expected);
}

/* ------------------------------------------------------------------ */
/* Cenário principal                                                  */
/* ------------------------------------------------------------------ */

static void test_controlled_report_presentation(void) {
    reset_fixture();

    supervisor_t *supervisor = NULL;
    mpmc_result_t result = supervisor_create(&supervisor, FIXTURE_PRODUCERS, IPC_SHARED_MEMORY);
    CHECK(
        result.status == MPMC_OK && supervisor != NULL,
        "supervisor_create deve aceitar a fixture"
    );
    CHECK(result.system_errno == 0, "criação bem-sucedida deve zerar system_errno");
    CHECK(g_fixture.init_calls == 1u, "IPC controlado inicializado uma vez");
    CHECK(
        supervisor_producer_count(supervisor) == FIXTURE_PRODUCERS,
        "quantidade lógica configurada"
    );

    if (supervisor == NULL) {
        g_fixture.active = false;
        return;
    }

    command_t produce = make_command(COMMAND_PRODUCE);
    command_t consume = make_command(COMMAND_CONSUME);

    CHECK(supervisor_dispatch(supervisor, &produce).status == MPMC_OK, "despacho do PID 4101");
    CHECK(supervisor_dispatch(supervisor, &consume).status == MPMC_OK, "despacho do PID 4102");
    CHECK(supervisor_dispatch(supervisor, &produce).status == MPMC_OK, "despacho do PID 4103");
    CHECK(g_fixture.fork_calls == FIXTURE_CHILDREN, "três PIDs simulados");

    /*
     * 4101 ainda não está pronto. 4102 e 4103 terminam primeiro.
     * A compactação interna do vetor não deve afetar a identidade dos relatórios.
     */
    CHECK(
        supervisor_reap(supervisor, false).status == MPMC_OK,
        "primeira coleta não bloqueante"
    );
    CHECK(supervisor_report_count(supervisor) == 2u, "dois relatórios na primeira coleta");

    g_fixture.children[0].ready = true;
    CHECK(
        supervisor_reap(supervisor, false).status == MPMC_OK,
        "segunda coleta não bloqueante"
    );
    CHECK(supervisor_report_count(supervisor) == FIXTURE_CHILDREN, "todos os filhos recolhidos");

    child_report_t report;

    CHECK(report_for_pid(supervisor, 4101, &report), "relatório do PID 4101 encontrado");
    if (report_for_pid(supervisor, 4101, &report)) {
        CHECK(
            report.operation_id == 1u && report.kind == COMMAND_PRODUCE,
            "PID 4101 associado à operação 1 de produção"
        );
        CHECK(
            report.end == CHILD_END_EXITED && report.value == EXIT_SUCCESS,
            "PID 4101 terminou com sucesso"
        );
        CHECK(
            report.duration_ns == 200000000u,
            "PID 4101 atravessou a mudança de segundo com 200 ms"
        );
        CHECK(report.duration_ns < UINT64_C(1000000000), "duração positiva sem underflow");
    }

    CHECK(report_for_pid(supervisor, 4102, &report), "relatório do PID 4102 encontrado");
    if (report_for_pid(supervisor, 4102, &report)) {
        CHECK(
            report.operation_id == 2u && report.kind == COMMAND_CONSUME,
            "PID 4102 associado à operação 2 de consumo"
        );
        CHECK(
            report.end == CHILD_END_SIGNALED && report.value == SIGTERM,
            "PID 4102 terminou por SIGTERM"
        );
        CHECK(
            report.duration_ns == 0u,
            "falha de CLOCK_MONOTONIC na duração individual resulta em zero"
        );
    }

    CHECK(report_for_pid(supervisor, 4103, &report), "relatório do PID 4103 encontrado");
    if (report_for_pid(supervisor, 4103, &report)) {
        CHECK(
            report.operation_id == 3u && report.kind == COMMAND_PRODUCE,
            "PID 4103 associado à operação 3 de produção"
        );
        CHECK(
            report.end == CHILD_END_EXITED && report.value == 7,
            "PID 4103 terminou com código de falha"
        );
        CHECK(
            report.duration_ns == 0u,
            "regressão do relógio não produz duração negativa nem underflow"
        );
    }

    CHECK(g_fixture.waitpid_calls == 4u, "somente os quatro estados esperados foram consultados");
    CHECK(!g_fixture.bad_wait_options, "reap utilizou WNOHANG nos cenários não bloqueantes");

    /* O relatório final deve ser impresso após o encerramento do supervisor. */
    CHECK(supervisor_shutdown(supervisor).status == MPMC_OK, "shutdown sem filhos ativos");
    CHECK(supervisor_report_count(supervisor) == FIXTURE_CHILDREN, "relatórios preservados");

    /* Relatório após a primeira falha do relógio total. */
    char output[4096];
    result = capture_report(supervisor, output, sizeof output);
    CHECK(result.status == MPMC_OK, "falha do relógio total não impede a apresentação");
    CHECK(
        strstr(output, "Tempo total de operação do mpmc: 0.000 s") != NULL,
        "falha do relógio total apresenta duração zero"
    );
    CHECK(strstr(output, "Filhos recolhidos: 3\n") != NULL, "total de filhos recolhidos");

    check_report_line(
        output, 4101, 1u, "produz", "código 0 (sucesso)", "0.200 s"
    );
    check_report_line(
        output, 4102, 2u, "consome", "sinal 15", "0.000 s"
    );
    check_report_line(
        output, 4103, 3u, "produz", "código 7 (falha)", "0.000 s"
    );

    CHECK(
        strstr(output, "Consumidos pela estrutura 1: 17\n") != NULL
            && strstr(output, "Consumidos pela estrutura 2: 29\n") != NULL
            && strstr(output, "Consumidos pela estrutura 3: 41\n") != NULL,
        "relatório apresenta as três contagens lógicas controladas"
    );

    /* Segunda impressão: o relógio funciona e calcula o total desde a criação. */
    result = capture_report(supervisor, output, sizeof output);
    CHECK(result.status == MPMC_OK, "relatório com relógio disponível");
    CHECK(
        strstr(output, "Tempo total de operação do mpmc: 4.000 s") != NULL,
        "tempo total calculado desde a criação do supervisor"
    );

    const size_t expected_count_ids[] = {1u, 2u, 3u, 1u, 2u, 3u};
    CHECK(g_fixture.count_calls == 6u, "cada impressão consultou as três estruturas");
    for (size_t index = 0u; index < 6u; ++index) {
        CHECK(
            g_fixture.count_ids[index] == expected_count_ids[index],
            "consulta de contagem %zu usa a estrutura lógica esperada",
            index + 1u
        );
    }

    CHECK(!g_fixture.bad_clock_id, "todas as medições usaram CLOCK_MONOTONIC");
    CHECK(!g_fixture.clock_overrun, "quantidade de leituras do relógio dentro da fixture");
    CHECK(g_fixture.clock_index == FIXTURE_CLOCKS, "nove chamadas de relógio consumidas");
    CHECK(g_fixture.poll_calls == 6u, "eventos IPC consultados nos pontos esperados");
    CHECK(!g_fixture.bad_ipc_context, "contexto IPC fictício preservado");

    /* Todos os filhos simulados foram recolhidos; nenhum sinal é enviado a PIDs fictícios. */
    supervisor_destroy(supervisor);
    CHECK(g_fixture.destroy_calls == 1u, "contexto IPC destruído exatamente uma vez");

    for (size_t index = 0u; index < FIXTURE_CHILDREN; ++index) {
        CHECK(
            g_fixture.children[index].collected,
            "PID %ld foi recolhido pela fixture",
            (long)g_fixture.children[index].pid
        );
    }

    g_fixture.active = false;
}

int main(void) {
    test_controlled_report_presentation();

    printf("%u verificações, %u falhas\n", g_checks, g_failures);
    return g_failures == 0u ? EXIT_SUCCESS : EXIT_FAILURE;
}
