/**
 * @file test_protocol_properties.c
 * @brief Propriedades determinísticas do protocolo (issue 24).
 * @ingroup tests
 *
 * @details Verifica, sem depender de aleatoriedade nem de escalonamento:
 * - conversão de status em mensagens legíveis;
 * - composição dos metadados e unicidade da chave (operation_id, sender_pid, sequence);
 * - sequência crescente por processo emissor (processos reais, sincronizados por pipes);
 * - distribuição striped `1 + ((k-1) % p)` para vários valores de `p` e `n`;
 * - capacidade, alinhamento e limites do segmento de 2048 bytes.
 *
 * Garantias do próprio teste: nenhum filho fica zumbi (todos são recolhidos e o fim é
 * conferido com waitpid(-1, WNOHANG)); nenhuma espera é indefinida (os filhos só
 * esperam um pipe que o pai fecha incondicionalmente, e alarm() limita a execução).
 */
#include <errno.h>
#include <stdalign.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

#include "mpmc/error.h"
#include "mpmc/message.h"
#include "mpmc/workers/producer.h"
#include "producer_routing.h"
#include "shm_ring.h"

static int checks = 0;
static int failures = 0;

#define CHECK(cond, ...)                                                                           \
    do {                                                                                           \
        ++checks;                                                                                  \
        if (!(cond)) {                                                                             \
            ++failures;                                                                            \
            fprintf(stderr, "FALHA %s:%d: ", __FILE__, __LINE__);                                  \
            fprintf(stderr, __VA_ARGS__);                                                          \
            fputc('\n', stderr);                                                                   \
        }                                                                                          \
    } while (0)

/* producer.c referencia a fachada ipc_*; estes substitutos mantêm o teste isolado dos
 * backends. producer_run() não é exercido aqui. */
size_t ipc_producer_count(const ipc_context_t *context) {
    (void)context;
    return 0u;
}

mpmc_result_t ipc_send(ipc_context_t *context, size_t producer_id, const message_t *message) {
    (void)context;
    (void)producer_id;
    (void)message;
    return (mpmc_result_t){.status = MPMC_ERR_STATE};
}

mpmc_result_t ipc_ack_begin(ipc_context_t *context, uint64_t operation_id, unsigned expected) {
    (void)context;
    (void)operation_id;
    (void)expected;
    return (mpmc_result_t){.status = MPMC_ERR_STATE};
}

mpmc_result_t ipc_ack_wait(ipc_context_t *context, uint64_t operation_id) {
    (void)context;
    (void)operation_id;
    return (mpmc_result_t){.status = MPMC_ERR_STATE};
}

mpmc_result_t ipc_ack_end(ipc_context_t *context, uint64_t operation_id) {
    (void)context;
    (void)operation_id;
    return (mpmc_result_t){.status = MPMC_ERR_STATE};
}

/* ------------------------------------------------------------------------- */
/* 1. Conversão de status                                                    */
/* ------------------------------------------------------------------------- */

/** @brief Cada status conhecido tem texto não vazio e distinto; desconhecidos compartilham um texto
 * próprio. */
static void test_status_conversion(void) {
    const mpmc_status_t all[] = {
        MPMC_OK,
        MPMC_ERR_INVALID_ARG,
        MPMC_ERR_INVALID_INDEX,
        MPMC_ERR_WOULD_BLOCK,
        MPMC_ERR_NO_SPACE,
        MPMC_ERR_INTERRUPTED,
        MPMC_ERR_SHUTDOWN,
        MPMC_ERR_SYS,
        MPMC_ERR_STATE,
    };
    const size_t count = sizeof all / sizeof all[0];
    const char *unknown = mpmc_status_str((mpmc_status_t)99);

    CHECK(unknown != NULL && unknown[0] != '\0', "texto de status desconhecido vazio");
    CHECK(
        strcmp(unknown, mpmc_status_str((mpmc_status_t)1000)) == 0,
        "status desconhecidos devem ter o mesmo texto"
    );

    for (size_t i = 0; i < count; ++i) {
        const char *text = mpmc_status_str(all[i]);
        CHECK(text != NULL && text[0] != '\0', "status %zu sem texto", i);
        CHECK(strcmp(text, unknown) != 0, "status %zu confundido com desconhecido", i);
        for (size_t j = i + 1u; j < count; ++j) {
            CHECK(strcmp(text, mpmc_status_str(all[j])) != 0, "status %zu e %zu iguais", i, j);
        }
    }
    CHECK(all[0] == 0, "MPMC_OK deve valer zero");

    /* Formatação completa. */
    char big[256];
    for (size_t i = 0; i < count; ++i) {
        const char *text = mpmc_status_str(all[i]);
        int n = mpmc_error_format((mpmc_result_t){.status = all[i]}, big, sizeof big);
        CHECK(n == (int)strlen(text) && strcmp(big, text) == 0, "format sem errno, status %zu", i);

        n = mpmc_error_format(
            (mpmc_result_t){.status = all[i], .system_errno = ENOENT}, big, sizeof big
        );
        CHECK(n == (int)strlen(big), "retorno de format com errno, status %zu", i);
        CHECK(strstr(big, strerror(ENOENT)) != NULL, "errno ausente na mensagem, status %zu", i);
        CHECK(big[strlen(big) - 1u] == '.', "mensagem sem ponto final, status %zu", i);
        CHECK(strstr(big, "..") == NULL, "ponto duplicado na mensagem, status %zu", i);
    }

    /* Causas diferentes geram mensagens diferentes para a mesma categoria. */
    char other[256];
    mpmc_error_format(
        (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = ENOENT}, big, sizeof big
    );
    mpmc_error_format(
        (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = EACCES}, other, sizeof other
    );
    CHECK(strcmp(big, other) != 0, "errno diferentes devem gerar textos diferentes");

    /* Truncamento: sempre terminado em NUL, retorno é o tamanho completo. */
    mpmc_result_t sys = {.status = MPMC_ERR_SYS, .system_errno = ENOENT};
    int full = mpmc_error_format(sys, big, sizeof big);
    char small[8];
    memset(small, 'x', sizeof small);
    int n = mpmc_error_format(sys, small, sizeof small);
    CHECK(n == full, "retorno deve ser o tamanho completo (%d vs %d)", n, full);
    CHECK(memchr(small, '\0', sizeof small) != NULL, "buffer truncado sem NUL");
    CHECK(strncmp(small, big, sizeof small - 1u) == 0, "prefixo truncado difere");
    CHECK(mpmc_error_format(sys, NULL, 0u) == full, "medição com capacidade zero");
    CHECK(mpmc_error_format(sys, NULL, 4u) == -1, "buffer nulo com capacidade deve falhar");
}

/* ------------------------------------------------------------------------- */
/* 2. Metadados, operation_id e sequência                                    */
/* ------------------------------------------------------------------------- */

typedef struct {
    uint64_t operation_id;
    uint32_t sequence;
    pid_t pid;
} key_t3;

static int key_equal(key_t3 a, key_t3 b) {
    return a.operation_id == b.operation_id && a.sequence == b.sequence && a.pid == b.pid;
}

/** @brief Composição dos campos e rejeições sem efeito colateral. */
static void test_metadata_composition(void) {
    producer_sequence_t sequence = {0};
    producer_job_t job = {
        .producer_id = 3u,
        .message_count = 1u,
        .ack_mode = PRODUCER_ACK_WAIT,
        .operation_id = UINT64_MAX,
    };
    message_t m;

    CHECK(producer_message_next(&m, &sequence, &job) == MPMC_OK, "geração básica");
    CHECK(m.sender_pid == getpid(), "sender_pid deve ser getpid()");
    CHECK(m.sequence == 1u && sequence.last_sequence == 1u, "primeira sequência deve ser 1");
    CHECK(m.operation_id == UINT64_MAX, "operation_id de 64 bits não pode ser truncado");
    CHECK(
        m.sent_at.tv_sec > 0 && m.sent_at.tv_nsec >= 0 && m.sent_at.tv_nsec < 1000000000L, "sent_at"
    );

    /* producer_id é destino lógico: não altera os metadados da mensagem. */
    producer_job_t other_dest = job;
    other_dest.producer_id = 0u;
    producer_sequence_t seq_b = {0};
    message_t mb;
    CHECK(producer_message_next(&mb, &seq_b, &other_dest) == MPMC_OK, "geração com destino 0");
    CHECK(
        mb.sequence == m.sequence && mb.operation_id == m.operation_id
            && mb.sender_pid == m.sender_pid,
        "producer_id não pode influenciar sender_pid/sequence/operation_id"
    );

    /* Modo de ACK não altera os metadados. */
    producer_job_t none = job;
    none.ack_mode = PRODUCER_ACK_NONE;
    producer_sequence_t seq_c = {0};
    message_t mc;
    CHECK(producer_message_next(&mc, &seq_c, &none) == MPMC_OK, "geração sem ACK");
    CHECK(mc.sequence == 1u && mc.operation_id == job.operation_id, "ACK não altera metadados");

    /* Rejeições: saída e contador intactos. */
    message_t guard;
    memset(&guard, 0xA5, sizeof guard);
    const message_t before = guard;
    producer_sequence_t keep = {.last_sequence = 7u};

    producer_job_t bad = job;
    bad.operation_id = 0u;
    CHECK(producer_message_next(&guard, &keep, &bad) == MPMC_ERR_INVALID_ARG, "operation_id zero");
    bad = job;
    bad.ack_mode = (producer_ack_mode_t)0;
    CHECK(producer_message_next(&guard, &keep, &bad) == MPMC_ERR_INVALID_ARG, "ack_mode 0");
    bad.ack_mode = (producer_ack_mode_t)3;
    CHECK(producer_message_next(&guard, &keep, &bad) == MPMC_ERR_INVALID_ARG, "ack_mode 3");
    CHECK(producer_message_next(NULL, &keep, &job) == MPMC_ERR_INVALID_ARG, "saída nula");
    CHECK(producer_message_next(&guard, NULL, &job) == MPMC_ERR_INVALID_ARG, "sequência nula");
    CHECK(producer_message_next(&guard, &keep, NULL) == MPMC_ERR_INVALID_ARG, "job nulo");
    CHECK(keep.last_sequence == 7u, "sequência não pode avançar em rejeição");
    CHECK(memcmp(&guard, &before, sizeof guard) == 0, "saída não pode mudar em rejeição");

    /* Esgotamento: erro de estado, sem voltar a zero. */
    producer_sequence_t full = {.last_sequence = UINT32_MAX};
    CHECK(producer_message_next(&guard, &full, &job) == MPMC_ERR_STATE, "sequência esgotada");
    CHECK(full.last_sequence == UINT32_MAX, "sequência esgotada não pode dar a volta");
    CHECK(memcmp(&guard, &before, sizeof guard) == 0, "saída intacta no esgotamento");

    producer_sequence_t edge = {.last_sequence = UINT32_MAX - 1u};
    CHECK(
        producer_message_next(&guard, &edge, &job) == MPMC_OK && guard.sequence == UINT32_MAX,
        "última sequência válida"
    );
}

/** @brief Operações intercaladas no mesmo processo: sequência única, contadores separados. */
static void test_interleaved_operations(void) {
    enum { OPS = 5, ROUNDS = 40 };
    producer_sequence_t sequence = {0};
    producer_job_t jobs[OPS];
    unsigned per_op[OPS] = {0};
    key_t3 keys[OPS * ROUNDS];
    size_t total = 0u;

    for (unsigned i = 0; i < OPS; ++i) {
        jobs[i] = (producer_job_t){
            .producer_id = i + 1u,
            .message_count = ROUNDS,
            .ack_mode = (i % 2u == 0u) ? PRODUCER_ACK_WAIT : PRODUCER_ACK_NONE,
            .operation_id = 100u + 7u * i,
        };
    }

    /* Ordem fixa e determinística: round-robin com saltos (i*3+r) % OPS. */
    for (unsigned r = 0; r < OPS * ROUNDS; ++r) {
        unsigned op = (r * 3u + r / OPS) % OPS;
        if (per_op[op] == ROUNDS) {
            for (unsigned k = 0; k < OPS; ++k) {
                if (per_op[k] < ROUNDS) {
                    op = k;
                    break;
                }
            }
        }
        message_t m;
        CHECK(producer_message_next(&m, &sequence, &jobs[op]) == MPMC_OK, "geração intercalada");
        CHECK(m.operation_id == jobs[op].operation_id, "operation_id da operação %u", op);
        CHECK(m.sequence == (uint32_t)total + 1u, "sequência única por processo (%u)", m.sequence);
        keys[total++] = (key_t3){m.operation_id, m.sequence, m.sender_pid};
        per_op[op]++;
    }

    CHECK(total == OPS * ROUNDS && sequence.last_sequence == OPS * ROUNDS, "total emitido");
    for (unsigned i = 0; i < OPS; ++i) {
        CHECK(per_op[i] == ROUNDS, "contagem da operação %u = %u", i, per_op[i]);
        unsigned seen = 0u;
        for (size_t k = 0; k < total; ++k) {
            seen += keys[k].operation_id == jobs[i].operation_id;
        }
        CHECK(seen == ROUNDS, "mensagens com operation_id da operação %u = %u", i, seen);
    }
    for (size_t a = 0; a < total; ++a) {
        for (size_t b = a + 1u; b < total; ++b) {
            if (key_equal(keys[a], keys[b])) {
                CHECK(0, "chave duplicada nas posições %zu e %zu", a, b);
                return;
            }
        }
    }
    CHECK(1, "todas as chaves (operation_id, pid, sequence) distintas");
}

typedef struct {
    pid_t pid;
    uint64_t operation_id;
    uint32_t first_sequence;
    uint32_t last_sequence;
    uint32_t emitted;
    int ok; /* 1 se todas as sequências 1..emitted vieram em ordem */
} child_report_t;

static int read_full(int fd, void *buffer, size_t size) {
    unsigned char *p = buffer;
    while (size > 0u) {
        ssize_t n = read(fd, p, size);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return -1;
        p += n;
        size -= (size_t)n;
    }
    return 0;
}

static int write_full(int fd, const void *buffer, size_t size) {
    const unsigned char *p = buffer;
    while (size > 0u) {
        ssize_t n = write(fd, p, size);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return -1;
        p += n;
        size -= (size_t)n;
    }
    return 0;
}

/** @brief Cada processo real inicia a própria sequência em 1 e usa seu PID. */
static void test_sequence_per_process(void) {
    enum { CHILDREN = 4, EMITS = 6 };
    int reports[CHILDREN][2];
    int release[2];
    pid_t pids[CHILDREN] = {0};
    int started = 0;

    /* A sequência do pai avança antes dos forks: filhos não podem herdá-la. */
    producer_sequence_t parent_sequence = {0};
    producer_job_t parent_job = {
        .producer_id = 1u,
        .message_count = 1u,
        .ack_mode = PRODUCER_ACK_NONE,
        .operation_id = 9u,
    };
    message_t scratch;
    for (int i = 0; i < 3; ++i) {
        (void)producer_message_next(&scratch, &parent_sequence, &parent_job);
    }

    if (pipe(release) != 0) {
        CHECK(0, "pipe(release) falhou");
        return;
    }

    for (int c = 0; c < CHILDREN; ++c) {
        if (pipe(reports[c]) != 0) {
            CHECK(0, "pipe(reports) falhou");
            break;
        }
        pid_t pid = fork();
        if (pid < 0) {
            close(reports[c][0]);
            close(reports[c][1]);
            CHECK(0, "fork falhou");
            break;
        }
        if (pid == 0) {
            close(release[1]);
            close(reports[c][0]);
            producer_sequence_t seq = {0}; /* nova sequência: um processo, um contador */
            producer_job_t job = {
                .producer_id = (size_t)c + 1u,
                .message_count = EMITS,
                .ack_mode = PRODUCER_ACK_WAIT,
                .operation_id = 1000u + (uint64_t)c,
            };
            child_report_t report = {.pid = getpid(), .operation_id = job.operation_id, .ok = 1};
            for (uint32_t i = 1; i <= EMITS; ++i) {
                message_t m;
                if (producer_message_next(&m, &seq, &job) != MPMC_OK || m.sequence != i
                    || m.sender_pid != report.pid || m.operation_id != job.operation_id) {
                    report.ok = 0;
                }
                if (i == 1u) report.first_sequence = m.sequence;
                report.last_sequence = m.sequence;
                report.emitted = i;
            }
            int failed = write_full(reports[c][1], &report, sizeof report) != 0;
            close(reports[c][1]);
            /* Espera o pai fechar 'release' (EOF); nunca bloqueia para sempre. */
            char byte;
            (void)read(release[0], &byte, 1);
            _exit(failed ? 2 : 0);
        }
        close(reports[c][1]);
        pids[c] = pid;
        ++started;
    }
    close(release[0]);

    child_report_t got[CHILDREN];
    int have[CHILDREN] = {0};
    for (int c = 0; c < started; ++c) {
        have[c] = read_full(reports[c][0], &got[c], sizeof got[c]) == 0;
        close(reports[c][0]);
    }

    /* Todos os filhos estão vivos aqui: PIDs distintos por construção. */
    close(release[1]);

    for (int c = 0; c < started; ++c) {
        int status = 0;
        pid_t r;
        do {
            r = waitpid(pids[c], &status, 0);
        } while (r < 0 && errno == EINTR);
        CHECK(
            r == pids[c] && WIFEXITED(status) && WEXITSTATUS(status) == 0, "filho %d recolhido", c
        );
    }
    CHECK(started == CHILDREN, "todos os filhos criados");

    for (int c = 0; c < started; ++c) {
        CHECK(have[c], "relatório do filho %d", c);
        if (!have[c]) continue;
        CHECK(got[c].ok, "filho %d: sequência 1..%d em ordem", c, EMITS);
        CHECK(got[c].first_sequence == 1u && got[c].last_sequence == EMITS, "filho %d: faixa", c);
        CHECK(got[c].pid == pids[c] && got[c].pid != getpid(), "filho %d: PID próprio", c);
        CHECK(got[c].operation_id == 1000u + (uint64_t)c, "filho %d: operation_id", c);
        for (int d = c + 1; d < started; ++d) {
            if (have[d]) {
                CHECK(got[c].pid != got[d].pid, "PIDs %d e %d distintos", c, d);
                CHECK(got[c].operation_id != got[d].operation_id, "operation_id %d e %d", c, d);
            }
        }
    }

    CHECK(parent_sequence.last_sequence == 3u, "sequência do pai não é alterada pelos filhos");
    errno = 0;
    CHECK(waitpid(-1, NULL, WNOHANG) == -1 && errno == ECHILD, "não restam filhos (zumbis)");
}

/* ------------------------------------------------------------------------- */
/* 3. Distribuição striped                                                   */
/* ------------------------------------------------------------------------- */

/** @brief Confere uma combinação (p, n) completa; devolve 0 se tudo certo. */
static int check_striped(size_t p, size_t n) {
    static size_t counts[1026];
    memset(counts, 0, sizeof counts);
    int ok = 1;

    for (size_t k = 1; k <= n; ++k) {
        size_t dest = 0u;
        size_t expected = 1u + ((k - 1u) % p);
        if (producer_route_destination(k, p, &dest) != MPMC_OK || dest != expected || dest < 1u
            || dest > p) {
            fprintf(
                stderr, "p=%zu n=%zu k=%zu: obtido %zu esperado %zu\n", p, n, k, dest, expected
            );
            ok = 0;
            continue;
        }
        if (k <= p && dest != k) ok = 0; /* a primeira volta é 1, 2, ..., p */
        if (k > p) {
            size_t previous = 0u;
            (void)producer_route_destination(k - p, p, &previous);
            if (previous != dest) ok = 0; /* período exato p */
        }
        counts[dest]++;
    }

    size_t sum = 0u;
    const size_t base = n / p;
    const size_t extra = n % p;
    for (size_t d = 1; d <= p && d < 1026u; ++d) {
        size_t want = base + (d <= extra ? 1u : 0u); /* distribuição o mais uniforme possível */
        if (counts[d] != want) {
            fprintf(
                stderr,
                "p=%zu n=%zu destino %zu: %zu mensagens, esperado %zu\n",
                p,
                n,
                d,
                counts[d],
                want
            );
            ok = 0;
        }
        sum += counts[d];
    }
    if (sum != n || counts[0] != 0u) ok = 0; /* nada se perde e o destino 0 nunca é usado */
    return ok;
}

static void test_striped_distribution(void) {
    int all_ok = 1;
    size_t cases = 0u;

    /* Exaustivo em faixa pequena. */
    for (size_t p = 1; p <= 40; ++p) {
        for (size_t n = 1; n <= 130; ++n) {
            all_ok &= check_striped(p, n);
            ++cases;
        }
    }
    /* Valores relevantes de p e limites do protocolo. */
    const size_t ps[] = {1, 2, 3, 7, 10, 63, 64, 100, 511, 512, 1000, 1023, 1024};
    for (size_t i = 0; i < sizeof ps / sizeof ps[0]; ++i) {
        const size_t p = ps[i];
        const size_t ns[] = {1, 2, p - 1u, p, p + 1u, 2u * p, 1023, 1024};
        for (size_t j = 0; j < sizeof ns / sizeof ns[0]; ++j) {
            if (ns[j] >= MPMC_MIN_MESSAGES && ns[j] <= MPMC_MAX_MESSAGES) {
                all_ok &= check_striped(p, ns[j]);
                ++cases;
            }
        }
    }
    CHECK(all_ok, "distribuição striped divergente em algum caso (%zu casos)", cases);

    /* Extremos sem overflow. */
    size_t dest = 0u;
    CHECK(
        producer_route_destination(SIZE_MAX, SIZE_MAX, &dest) == MPMC_OK && dest == SIZE_MAX,
        "extremo p=n=SIZE_MAX"
    );
    CHECK(
        producer_route_destination(1u, SIZE_MAX, &dest) == MPMC_OK && dest == 1u, "k=1, p=SIZE_MAX"
    );
    CHECK(
        producer_route_destination(SIZE_MAX, 1u, &dest) == MPMC_OK && dest == 1u,
        "p=1 sempre destino 1"
    );

    /* Entradas inválidas: erro e saída intacta. */
    dest = 12345u;
    CHECK(
        producer_route_destination(0u, 4u, &dest) == MPMC_ERR_INVALID_ARG && dest == 12345u,
        "ordinal 0"
    );
    CHECK(
        producer_route_destination(1u, 0u, &dest) == MPMC_ERR_INVALID_ARG && dest == 12345u, "p = 0"
    );
    CHECK(producer_route_destination(1u, 4u, NULL) == MPMC_ERR_INVALID_ARG, "saída nula");
}

/** @brief Roteamento + metadados: contadores por destino e chaves de mensagem não se misturam. */
static void test_striped_with_metadata(void) {
    const size_t cases[][2] = {{1, 5}, {3, 10}, {4, 4}, {7, 3}, {5, 1024}};
    for (size_t c = 0; c < sizeof cases / sizeof cases[0]; ++c) {
        const size_t p = cases[c][0];
        const size_t n = cases[c][1];
        producer_sequence_t sequence = {0};
        producer_job_t job = {
            .producer_id = 0u,
            .message_count = (unsigned)n,
            .ack_mode = PRODUCER_ACK_WAIT,
            .operation_id = 500u + c,
        };
        static size_t per_dest[1025];
        memset(per_dest, 0, sizeof per_dest);
        int ok = 1;
        for (size_t k = 1; k <= n; ++k) {
            message_t m;
            size_t dest = 0u;
            if (producer_message_next(&m, &sequence, &job) != MPMC_OK
                || producer_route_destination(k, p, &dest) != MPMC_OK) {
                ok = 0;
                break;
            }
            /* A sequência é por envio, não por fila destino. */
            if (m.sequence != k || m.operation_id != job.operation_id) ok = 0;
            per_dest[dest]++;
        }
        size_t sum = 0u;
        for (size_t d = 1; d <= p; ++d) sum += per_dest[d];
        CHECK(
            ok && sum == n && sequence.last_sequence == n, "striped + metadados p=%zu n=%zu", p, n
        );
    }
}

/* ------------------------------------------------------------------------- */
/* 4. Capacidade e alinhamento do segmento                                   */
/* ------------------------------------------------------------------------- */

static_assert(MPMC_SHM_SEGMENT_BYTES == 2048u, "segmento de 2048 bytes");
static_assert(SHM_RING_CAPACITY == 63u, "capacidade esperada: 63 mensagens");
static_assert((2048u - 16u) / 32u == 63u, "63 = floor((2048 - 16) / 32)");

#define GUARD 64u

typedef struct {
    alignas(16) unsigned char bytes[MPMC_SHM_SEGMENT_BYTES + GUARD];
} segment_t;

static void
ring_invariants(const shm_ring_t *ring, size_t pushed, size_t popped, const char *where) {
    CHECK(ring->capacity == SHM_RING_CAPACITY, "%s: capacity", where);
    CHECK(
        ring->head < ring->capacity && ring->tail < ring->capacity,
        "%s: índices fora de 0..cap-1",
        where
    );
    CHECK(ring->count <= ring->capacity, "%s: count > capacity", where);
    CHECK(
        ring->count == pushed - popped,
        "%s: count=%u esperado %zu",
        where,
        ring->count,
        pushed - popped
    );
    CHECK(
        (ring->head + ring->count) % ring->capacity == ring->tail, "%s: head+count != tail", where
    );
    CHECK(ring->consumed_total == popped, "%s: consumed_total", where);
}

static message_t make_message(uint32_t n) {
    return (message_t){
        .sender_pid = (pid_t)(1000 + n % 13u),
        .sequence = n,
        .sent_at = {.tv_sec = 1700000000 + (time_t)n, .tv_nsec = (long)(n % 1000u) * 1000L},
        .operation_id = 0x0100000000ULL + n,
    };
}

static void test_segment_capacity(void) {
    /* Layout. */
    CHECK(
        sizeof(message_t) == 32u && alignof(message_t) == 8u, "message_t 32 bytes, alinhamento 8"
    );
    CHECK(
        sizeof(shm_ring_t) == 16u && offsetof(shm_ring_t, messages) == 16u, "cabeçalho de 16 bytes"
    );
    CHECK(offsetof(shm_ring_t, messages) % alignof(message_t) == 0u, "messages alinhado");
    CHECK(SHM_RING_CAPACITY == 63u, "SHM_RING_CAPACITY == 63");
    CHECK(16u + SHM_RING_CAPACITY * sizeof(message_t) == 2032u, "63 mensagens terminam em 2032");
    CHECK(
        MPMC_SHM_SEGMENT_BYTES - 2032u == 16u && 16u < sizeof(message_t),
        "sobra de 16 bytes não comporta a 64ª mensagem"
    );
    CHECK(
        16u + (SHM_RING_CAPACITY + 1u) * sizeof(message_t) > MPMC_SHM_SEGMENT_BYTES,
        "64 mensagens não cabem"
    );
    CHECK(SHM_RING_CAPACITY <= UINT8_MAX, "capacidade cabe em uint8_t");

    /* Segmento simulado, pré-preenchido com lixo e com guarda após os 2048 bytes. */
    static segment_t segment;
    memset(segment.bytes, 0xFF, sizeof segment.bytes);
    CHECK((uintptr_t)segment.bytes % 16u == 0u, "segmento alinhado a 16");
    shm_ring_t *ring = (shm_ring_t *)(void *)segment.bytes;
    shm_ring_init(ring);

    CHECK(
        ring->head == 0 && ring->tail == 0 && ring->count == 0 && ring->consumed_total == 0u,
        "init zera o estado"
    );
    ring_invariants(ring, 0u, 0u, "init");
    CHECK((uintptr_t)&ring->messages[0] % alignof(message_t) == 0u, "mensagem 0 alinhada");
    CHECK(
        (uintptr_t)&ring->messages[SHM_RING_CAPACITY - 1u] % alignof(message_t) == 0u,
        "última mensagem alinhada"
    );
    CHECK(
        (const unsigned char *)(&ring->messages[SHM_RING_CAPACITY - 1u] + 1)
            <= segment.bytes + MPMC_SHM_SEGMENT_BYTES,
        "última mensagem termina dentro do segmento"
    );

    /* Encher até a capacidade exata. */
    size_t pushed = 0u;
    size_t popped = 0u;
    for (uint32_t i = 1; i <= SHM_RING_CAPACITY; ++i) {
        shm_ring_push(ring, make_message(i));
        ++pushed;
    }
    ring_invariants(ring, pushed, popped, "cheio");
    CHECK(
        ring->count == ring->capacity && ring->head == 0 && ring->tail == 0,
        "cheio: tail dá a volta"
    );

    /* Nada fora da área de mensagens foi tocado: sobra [2032, 2048) e guarda. */
    int slack_intact = 1;
    for (size_t b = 2032u; b < sizeof segment.bytes; ++b) {
        if (segment.bytes[b] != 0xFF) slack_intact = 0;
    }
    CHECK(slack_intact, "sobra e guarda após o segmento permanecem intactas");

    /* Retirar tudo preservando FIFO e conteúdo. */
    for (uint32_t i = 1; i <= SHM_RING_CAPACITY; ++i) {
        message_t m = shm_ring_pop(ring);
        message_t want = make_message(i);
        ++popped;
        CHECK(
            m.sequence == want.sequence && m.sender_pid == want.sender_pid
                && m.operation_id == want.operation_id && m.sent_at.tv_sec == want.sent_at.tv_sec
                && m.sent_at.tv_nsec == want.sent_at.tv_nsec,
            "FIFO/conteúdo na posição %u",
            i
        );
    }
    ring_invariants(ring, pushed, popped, "vazio");

    /* Várias voltas com ocupação variável e padrão fixo, conferindo invariantes a cada passo. */
    uint32_t next_in = 1000u;
    uint32_t next_out = 1000u;
    int fifo_ok = 1;
    for (unsigned round = 0; round < 200u; ++round) {
        unsigned burst = (round * 5u) % (SHM_RING_CAPACITY + 1u);
        for (unsigned i = 0; i < burst && ring->count < ring->capacity; ++i) {
            shm_ring_push(ring, make_message(next_in++));
            ++pushed;
        }
        unsigned drain = (round * 3u) % (SHM_RING_CAPACITY + 1u);
        for (unsigned i = 0; i < drain && ring->count > 0; ++i) {
            message_t m = shm_ring_pop(ring);
            ++popped;
            if (m.sequence != next_out++) fifo_ok = 0;
        }
        if (ring->head >= ring->capacity || ring->tail >= ring->capacity
            || ring->count > ring->capacity || ring->count != pushed - popped
            || ring->consumed_total != popped) {
            fifo_ok = 0;
        }
    }
    CHECK(fifo_ok, "invariantes e FIFO durante várias voltas");
    while (ring->count > 0) {
        message_t m = shm_ring_pop(ring);
        ++popped;
        if (m.sequence != next_out++) fifo_ok = 0;
    }
    CHECK(fifo_ok && next_in == next_out, "FIFO após drenagem completa");
    ring_invariants(ring, pushed, popped, "final");
    CHECK(
        popped > 2u * SHM_RING_CAPACITY && ring->consumed_total > UINT8_MAX,
        "consumed_total acumula além de uint8_t e da capacidade"
    );

    slack_intact = 1;
    for (size_t b = 2032u; b < sizeof segment.bytes; ++b) {
        if (segment.bytes[b] != 0xFF) slack_intact = 0;
    }
    CHECK(slack_intact, "sobra e guarda intactas após todas as voltas");
}

int main(void) {
    alarm(60); /* rede de segurança: qualquer travamento vira falha em vez de bloquear o CI */

    test_status_conversion();
    test_metadata_composition();
    test_interleaved_operations();
    test_sequence_per_process();
    test_striped_distribution();
    test_striped_with_metadata();
    test_segment_capacity();

    printf("%d verificações de propriedades do protocolo, %d falhas\n", checks, failures);
    return failures == 0 ? 0 : 1;
}
