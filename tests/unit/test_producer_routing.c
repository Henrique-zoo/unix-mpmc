/**
 * @file test_producer_routing.c
 * @brief Testes do cálculo de destino cíclico de `produz 0` (issue #49).
 * @ingroup tests
 *
 * @details
 * Cobre producer_route_destination() sem aleatoriedade, IPC, fork ou ACK:
 *
 * - sequências exatas para p = 1, n < p, n divisível e não divisível por p;
 * - n = 1024 para vários p, com destinos em 1..p e soma das quantidades igual a n;
 * - varredura determinística de p = 1..64 e n = 1..1024;
 * - extremos de size_t, sem overflow;
 * - pré-condições: p > 0 e ordinal com base 1, saída intocada em falha;
 * - ausência de processos e de objetos IPC (funções espiãs) e sequência do
 *   produtor preservada, usando producer_message_next() de verdade.
 *
 * Retorna 0 se todos os casos passarem; 1 caso contrário.
 */

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <sys/sem.h>
#include <sys/shm.h>
#include <sys/types.h>
#include <unistd.h>

#include "mpmc/error.h"
#include "mpmc/workers/producer.h"
#include "producer_routing.h"

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
/* Espiões: o roteamento não pode criar processos nem objetos IPC.     */
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

/* Garante que os espiões funcionam, para a verificação principal não passar em vão. */
static void test_spies_work(void) {
    reset_spies();
    CHECK(fork() == -1 && g_fork_calls == 1, "espião de fork não registrou a chamada");
    CHECK(msgget(0, 0) == -1 && g_ipc_calls == 1, "espião de msgget não registrou a chamada");
    CHECK(shmget(0, 0, 0) == -1 && g_ipc_calls == 2, "espião de shmget não registrou a chamada");
    CHECK(semget(0, 0, 0) == -1 && g_ipc_calls == 3, "espião de semget não registrou a chamada");
    reset_spies();
}

/* ------------------------------------------------------------------ */
/* Utilitários                                                        */
/* ------------------------------------------------------------------ */

#define SENTINEL ((size_t)0xDEADBEEFu)

/* Destino de k com p estruturas; devolve 0 (inválido) se a função falhar. */
static size_t route(size_t ordinal, size_t producer_count) {
    size_t destination = SENTINEL;

    if (producer_route_destination(ordinal, producer_count, &destination) != MPMC_OK) {
        return 0u;
    }
    return destination;
}

/* Confere a sequência exata dos destinos de k = 1..n. */
static void check_sequence(size_t producer_count, const size_t *expected, size_t count) {
    for (size_t k = 1u; k <= count; ++k) {
        size_t got = route(k, producer_count);

        CHECK(
            got == expected[k - 1u],
            "p=%zu k=%zu: destino %zu, esperado %zu",
            producer_count,
            k,
            got,
            expected[k - 1u]
        );
    }
}

/*
 * Confere, para n mensagens e p estruturas, que todo destino está em 1..p, que a
 * soma das quantidades é n e que a estrutura d recebe n/p mais 1 se d <= n%p.
 * `counts` precisa ter pelo menos p + 1 posições.
 */
static void check_distribution(size_t producer_count, size_t message_count, size_t *counts) {
    size_t total = 0u;

    memset(counts, 0, (producer_count + 1u) * sizeof *counts);
    for (size_t k = 1u; k <= message_count; ++k) {
        size_t destination = route(k, producer_count);

        CHECK(
            destination >= 1u && destination <= producer_count,
            "p=%zu k=%zu: destino %zu fora de 1..p",
            producer_count,
            k,
            destination
        );
        if (destination >= 1u && destination <= producer_count) {
            ++counts[destination];
        }
    }
    for (size_t d = 1u; d <= producer_count; ++d) {
        size_t expected = (message_count / producer_count) + (d <= message_count % producer_count);

        total += counts[d];
        CHECK(
            counts[d] == expected,
            "p=%zu n=%zu: estrutura %zu recebeu %zu, esperado %zu",
            producer_count,
            message_count,
            d,
            counts[d],
            expected
        );
    }
    CHECK(
        total == message_count,
        "p=%zu n=%zu: soma das quantidades é %zu",
        producer_count,
        message_count,
        total
    );
}

/* ------------------------------------------------------------------ */
/* Casos                                                              */
/* ------------------------------------------------------------------ */

static void test_exact_sequences(void) {
    /* p = 1: tudo vai para a estrutura 1. */
    static const size_t single[] = {1, 1, 1, 1, 1};
    check_sequence(1u, single, 5u);

    /* n < p: só as primeiras estruturas recebem. */
    static const size_t fewer_than_p[] = {1, 2, 3};
    check_sequence(5u, fewer_than_p, 3u);

    /* n divisível por p: 12 mensagens, 4 estruturas. */
    static const size_t divisible[] = {1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4};
    check_sequence(4u, divisible, 12u);

    /* n não divisível por p: 10 mensagens, 4 estruturas. */
    static const size_t not_divisible[] = {1, 2, 3, 4, 1, 2, 3, 4, 1, 2};
    check_sequence(4u, not_divisible, 10u);

    /* n não divisível por p: 7 mensagens, 3 estruturas. */
    static const size_t seven_by_three[] = {1, 2, 3, 1, 2, 3, 1};
    check_sequence(3u, seven_by_three, 7u);

    /* n = p e n = p + 1. */
    static const size_t equal_to_p[] = {1, 2, 3, 4, 5};
    check_sequence(5u, equal_to_p, 5u);
    static const size_t one_more_than_p[] = {1, 2, 3, 4, 5, 1};
    check_sequence(5u, one_more_than_p, 6u);
}

static void test_distribution_for_boundary_counts(void) {
    static size_t counts[2050];
    static const size_t producers[] = {1, 2, 3, 4, 7, 8, 64, 1000, 1023, 1024, 1025, 2048};

    /* n = 1024, o máximo aceito pelo comando, para vários p. */
    for (size_t i = 0u; i < sizeof producers / sizeof producers[0]; ++i) {
        check_distribution(producers[i], 1024u, counts);
    }

    /* n pequeno: n = 1 e n < p. */
    check_distribution(1u, 1u, counts);
    check_distribution(5u, 1u, counts);
    check_distribution(5u, 3u, counts);
    check_distribution(1024u, 1u, counts);
}

static void test_exhaustive_small_grid(void) {
    static size_t counts[65];

    for (size_t producer_count = 1u; producer_count <= 64u; ++producer_count) {
        for (size_t message_count = 1u; message_count <= 1024u; message_count += 1u) {
            /* A distribuição completa só é recontada em n que são fronteiras. */
            if (message_count <= 3u * producer_count || message_count == 1024u) {
                check_distribution(producer_count, message_count, counts);
            }
        }
        /* O padrão é periódico: o destino de k é o de k + p. */
        for (size_t k = 1u; k + producer_count <= 1024u; ++k) {
            if (route(k, producer_count) != route(k + producer_count, producer_count)) {
                CHECK(
                    0,
                    "p=%zu: destino de %zu difere do de %zu",
                    producer_count,
                    k,
                    k + producer_count
                );
                break;
            }
        }
    }
}

static void test_size_max_extremes(void) {
    const size_t max = SIZE_MAX;

    CHECK(route(1u, max) == 1u, "p=SIZE_MAX k=1");
    CHECK(route(2u, max) == 2u, "p=SIZE_MAX k=2");
    CHECK(route(max, max) == max, "p=SIZE_MAX k=SIZE_MAX deve dar SIZE_MAX");
    CHECK(route(max - 1u, max) == max - 1u, "p=SIZE_MAX k=SIZE_MAX-1");
    CHECK(route(max, max - 1u) == 1u, "p=SIZE_MAX-1 k=SIZE_MAX deve dar a volta para 1");
    CHECK(route(max - 1u, max - 1u) == max - 1u, "p=SIZE_MAX-1 k=p");
    CHECK(route(max, 1u) == 1u, "p=1 k=SIZE_MAX");
    CHECK(route(max, 2u) == 1u + ((max - 1u) % 2u), "p=2 k=SIZE_MAX");
}

static void test_preconditions(void) {
    size_t out = SENTINEL;

    /* A base do ordinal é 1: k = 1 é o primeiro destino, k = 0 é inválido. */
    CHECK(producer_route_destination(1u, 4u, &out) == MPMC_OK && out == 1u, "k=1 deve dar 1");

    out = SENTINEL;
    CHECK(
        producer_route_destination(0u, 4u, &out) == MPMC_ERR_INVALID_ARG, "k=0 deve ser inválido"
    );
    CHECK(out == SENTINEL, "saída alterada com k=0");

    /* p precisa ser positivo. */
    CHECK(
        producer_route_destination(1u, 0u, &out) == MPMC_ERR_INVALID_ARG, "p=0 deve ser inválido"
    );
    CHECK(out == SENTINEL, "saída alterada com p=0");
    CHECK(producer_route_destination(7u, 0u, &out) == MPMC_ERR_INVALID_ARG, "p=0, k=7 inválido");
    CHECK(out == SENTINEL, "saída alterada com p=0 e k=7");

    /* Ambos inválidos. */
    CHECK(producer_route_destination(0u, 0u, &out) == MPMC_ERR_INVALID_ARG, "k=0 e p=0 inválido");
    CHECK(out == SENTINEL, "saída alterada com k=0 e p=0");

    /* Saída nula não pode ser desreferenciada, mesmo com k e p válidos. */
    CHECK(producer_route_destination(1u, 4u, NULL) == MPMC_ERR_INVALID_ARG, "saída nula");
    CHECK(producer_route_destination(0u, 0u, NULL) == MPMC_ERR_INVALID_ARG, "tudo inválido");
}

/*
 * A função não cria processos nem IPC e não mexe no estado do produtor: usa
 * producer_message_next() de verdade e roteia cada mensagem pelo seu ordinal.
 */
static void test_no_side_effects_and_sequence_is_preserved(void) {
    const size_t producer_count = 4u;
    producer_sequence_t sequence = {0};
    const producer_job_t job = {
        .producer_id = 0u,
        .message_count = 10u,
        .ack_mode = PRODUCER_ACK_NONE,
        .operation_id = 7u,
    };

    reset_spies();
    for (uint32_t k = 1u; k <= job.message_count; ++k) {
        message_t message;
        message_t before;
        size_t destination = SENTINEL;

        CHECK(producer_message_next(&message, &sequence, &job) == MPMC_OK, "mensagem %u", k);
        memcpy(&before, &message, sizeof message);

        CHECK(
            producer_route_destination(message.sequence, producer_count, &destination) == MPMC_OK,
            "roteamento da mensagem %u",
            k
        );
        CHECK(
            destination == 1u + ((k - 1u) % producer_count),
            "destino da mensagem %u: %zu",
            k,
            destination
        );
        CHECK(
            memcmp(&before, &message, sizeof message) == 0, "metadados alterados na mensagem %u", k
        );
        CHECK(
            sequence.last_sequence == k,
            "sequência após a mensagem %u é %u",
            k,
            sequence.last_sequence
        );
    }

    /* Uma chamada inválida também não toca a sequência nem reinicia a contagem. */
    size_t untouched = SENTINEL;

    CHECK(
        producer_route_destination(0u, producer_count, &untouched) == MPMC_ERR_INVALID_ARG,
        "chamada inválida"
    );
    CHECK(
        sequence.last_sequence == job.message_count,
        "sequência reiniciada: %u",
        sequence.last_sequence
    );

    /* A numeração continua de onde parou, e o destino acompanha o ordinal. */
    message_t next;
    size_t destination = SENTINEL;

    CHECK(producer_message_next(&next, &sequence, &job) == MPMC_OK, "mensagem seguinte");
    CHECK(next.sequence == job.message_count + 1u, "a sequência continuou em %u", next.sequence);
    CHECK(
        producer_route_destination(next.sequence, producer_count, &destination) == MPMC_OK
            && destination == 1u + (job.message_count % producer_count),
        "destino da mensagem seguinte: %zu",
        destination
    );

    CHECK(g_fork_calls == 0, "o roteamento chamou fork() %u vez(es)", g_fork_calls);
    CHECK(g_ipc_calls == 0, "o roteamento chamou msgget/shmget/semget %u vez(es)", g_ipc_calls);
}

int main(void) {
    test_spies_work();
    test_exact_sequences();
    test_distribution_for_boundary_counts();
    test_exhaustive_small_grid();
    test_size_max_extremes();
    test_preconditions();
    test_no_side_effects_and_sequence_is_preserved();

    printf("%u verificações, %u falhas\n", g_checks, g_failures);

    return g_failures == 0 ? 0 : 1;
}
