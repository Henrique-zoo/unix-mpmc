/**
 * @file test_isolation.c
 * @brief Comandos inválidos não criam processos nem objetos IPC.
 * @ingroup tests
 *
 * @details
 * Critério de aceite da issue #23: command_parse() e command_validate() devem
 * rejeitar comandos inválidos sem qualquer efeito no sistema. O teste fornece,
 * no próprio executável, versões espiãs de fork(), msgget(), shmget() e
 * semget(). Como command.c é ligado a este executável, qualquer chamada dessas
 * funções a partir do parsing incrementa um contador, e o teste falha.
 *
 * O teste só é compilado com command.c e error.c, sem nenhum módulo de IPC ou
 * de supervisão, portanto não depende de objetos IPC reais.
 *
 * Retorna 0 se todos os casos passarem; 1 caso contrário.
 */

#include <errno.h>
#include <stdio.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <sys/sem.h>
#include <sys/shm.h>
#include <sys/types.h>
#include <unistd.h>

#include "mpmc/app/command.h"
#include "mpmc/error.h"

/* Único ponto que depende da representação do resultado das operações. */
#define STATUS_IS_OK(call) ((call).status == MPMC_OK)

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
/* Espiões                                                            */
/* ------------------------------------------------------------------ */

static unsigned g_fork_calls;
static unsigned g_ipc_calls;

pid_t fork(void) {
    ++g_fork_calls;
    errno = EPERM;
    return -1;
}

int msgget(key_t key, int flags) {
    (void)key;
    (void)flags;
    ++g_ipc_calls;
    errno = EPERM;
    return -1;
}

int shmget(key_t key, size_t size, int flags) {
    (void)key;
    (void)size;
    (void)flags;
    ++g_ipc_calls;
    errno = EPERM;
    return -1;
}

int semget(key_t key, int count, int flags) {
    (void)key;
    (void)count;
    (void)flags;
    ++g_ipc_calls;
    errno = EPERM;
    return -1;
}

static void reset_spies(void) {
    g_fork_calls = 0;
    g_ipc_calls = 0;
}

/* Garante que os espiões funcionam, para o teste principal não passar em vão. */
static void test_spies_work(void) {
    reset_spies();
    CHECK(fork() == -1 && g_fork_calls == 1, "espião de fork não registrou a chamada");
    CHECK(msgget(0, 0) == -1 && g_ipc_calls == 1, "espião de msgget não registrou a chamada");
    CHECK(shmget(0, 0, 0) == -1 && g_ipc_calls == 2, "espião de shmget não registrou a chamada");
    CHECK(semget(0, 0, 0) == -1 && g_ipc_calls == 3, "espião de semget não registrou a chamada");
    reset_spies();
}

/* ------------------------------------------------------------------ */
/* Casos                                                              */
/* ------------------------------------------------------------------ */

#define PRODUCERS 2u

/* Retorna true se command_parse() ou command_validate() rejeita a linha. */
static bool is_rejected(const char *line) {
    command_t command = {0};

    if (!STATUS_IS_OK(command_parse(line, &command))) {
        return true;
    }
    return !STATUS_IS_OK(command_validate(&command, PRODUCERS));
}

static void test_invalid_commands_have_no_side_effects(void) {
    static const char *const invalid[] = {
        /* sintaxe */
        "",
        "   ",
        "invalido",
        "PRODUZ 1 1 1",
        "produz",
        "produz 1 5",
        "produz 1 5 abc",
        "produz 1 5 -1",
        /* tokens excedentes */
        "produz 1 5 1 9",
        "consome 1 5 1 9",
        "shutdown 1",
        "produz 1 5 1 & extra",
        /* overflow */
        "produz 18446744073709551616 1 1",
        "produz 1 18446744073709551616 1",
        "produz 1 5 18446744073709551616",
        "produz 1 999999999999999999999999 1",
        /* faixas de n: 0 e 1025 */
        "produz 1 0 1",
        "produz 1 1025 1",
        "consome 1 0 1",
        "consome 1 1025 1",
        /* valores de ack */
        "produz 1 5 0",
        "produz 1 5 3",
        "consome 1 5 0",
        "consome 1 5 3",
        /* índices inexistentes e consome 0 */
        "produz 3 1 1",
        "consome 3 1 1",
        "consome 0 1 1",
    };

    reset_spies();
    for (size_t i = 0; i < sizeof invalid / sizeof invalid[0]; ++i) {
        CHECK(is_rejected(invalid[i]), "\"%s\" deveria ser rejeitado", invalid[i]);
    }
    CHECK(g_fork_calls == 0, "comandos inválidos chamaram fork() %u vez(es)", g_fork_calls);
    CHECK(
        g_ipc_calls == 0, "comandos inválidos chamaram msgget/shmget/semget %u vez(es)", g_ipc_calls
    );
}

/* O parsing de comandos válidos também é puro: criar processos é papel do supervisor. */
static void test_valid_commands_have_no_side_effects(void) {
    static const char *const valid[] = {
        "produz 0 1 1",
        "produz 1 1024 2",
        "consome 2 1 1 &",
        "shutdown",
    };

    reset_spies();
    for (size_t i = 0; i < sizeof valid / sizeof valid[0]; ++i) {
        CHECK(!is_rejected(valid[i]), "\"%s\" deveria ser aceito", valid[i]);
    }
    CHECK(g_fork_calls == 0, "parsing chamou fork() %u vez(es)", g_fork_calls);
    CHECK(g_ipc_calls == 0, "parsing chamou msgget/shmget/semget %u vez(es)", g_ipc_calls);
}

int main(void) {
    test_spies_work();
    test_invalid_commands_have_no_side_effects();
    test_valid_commands_have_no_side_effects();

    printf("%u verificações, %u falhas\n", g_checks, g_failures);

    return g_failures == 0 ? 0 : 1;
}
