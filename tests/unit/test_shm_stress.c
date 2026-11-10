/**
 * @file test_shm_stress.c
 * @brief Estresse de dados do backend SHM com filas cheias e vários processos (issue 56).
 * @ingroup tests
 * @details Usa o backend real, sem ACK, supervisor, workers finais ou REPL. Em cada
 * rodada o pai cria um backend, enche todas as estruturas, dispara produtores (que
 * bloqueiam em estruturas cheias) e depois dois consumidores por estrutura. Cada
 * mensagem tem identidade única (operation_id, sequence). O pai confere, sem supor
 * ordem global entre produtores: nenhuma perda, nenhuma duplicação, destino correto,
 * remetente correto, FIFO por (consumidor, produtor) e totais consumidos exatos.
 * O volume é fixo e determinístico (sem aleatoriedade, sem sleep()). Todo prazo usa
 * alarm(); os filhos morrem por SIGALRM se travarem e o pai os encerra no tratador.
 * Cada filho fecha apenas seu estado local; só o pai remove seus próprios recursos.
 * @note Limitações: não comprova o protocolo de ACK nem o shutdown durante esperas
 * reais (continuam na issue 26).
 */
#include <errno.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/sem.h>
#include <sys/shm.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "shm_backend_internal.h"

/** @brief Quantidade de produtores por rodada. */
#define PRODUCERS 6U
/** @brief Mensagens enviadas por produtor em cada rodada. */
#define MESSAGES_PER_PRODUCER 150U
/** @brief Capacidade de cada estrutura (mensagens). */
#define CAP ((unsigned)SHM_RING_CAPACITY)
/** @brief Maior quantidade de estruturas entre as rodadas. */
#define MAX_STRUCTURES 4U
/** @brief Maior quantidade de consumidores (dois por estrutura). */
#define MAX_CONSUMERS (2U * MAX_STRUCTURES)
/** @brief Maior sequência esperada (pré-enchimento do pai ou produtor). */
#define MAX_SEQ                                                                                    \
    (MAX_STRUCTURES * CAP > MESSAGES_PER_PRODUCER ? MAX_STRUCTURES * CAP : MESSAGES_PER_PRODUCER)
/** @brief Prazo do pai por rodada e dos filhos, em segundos. */
#define ROUND_TIMEOUT 30U
#define CHILD_TIMEOUT 20U

/** @brief Quantidade de estruturas de cada rodada (cria/destrói repetidamente). */
static const size_t structures_per_round[] = {1, 2, 3, 4};

/** @brief Contadores de verificações e falhas. */
static unsigned checks, failures;
/** @brief Filhos próprios ainda não recolhidos; zero indica posição livre. */
static pid_t children[PRODUCERS + MAX_CONSUMERS];
/** @brief Indica expiração do prazo do pai. */
static volatile sig_atomic_t alarm_seen;
/** @brief Alguma rodada estourou o prazo; as demais são abandonadas. */
static bool timed_out;

/** @brief Registra uma propriedade sem depender de assert(). */
#define CHECK(condition)                                                                           \
    do {                                                                                           \
        ++checks;                                                                                  \
        if (!(condition)) {                                                                        \
            ++failures;                                                                            \
            fprintf(stderr, "FALHA %s:%d: %s\n", __FILE__, __LINE__, #condition);                  \
        }                                                                                          \
    } while (0)

/** @brief Observação enviada por um consumidor ao pai, menor que PIPE_BUF (escrita atômica). */
typedef struct {
    unsigned consumer;  /**< Índice do consumidor. */
    unsigned structure; /**< Estrutura lógica 1..p da qual saiu a mensagem. */
    message_t message;  /**< Mensagem retirada. */
} record_t;

/** @brief Tratador de SIGALRM do pai: encerra só os filhos próprios (async-signal-safe). */
static void on_alarm(int signal_number) {
    (void)signal_number;
    alarm_seen = 1;
    for (size_t i = 0; i < sizeof children / sizeof children[0]; ++i) {
        pid_t pid = children[i];
        if (pid > 0) (void)kill(pid, SIGKILL);
    }
}

/** @brief Estrutura de destino da mensagem i do produtor k (0-based), em 1..p. */
static unsigned destination(unsigned k, unsigned i, size_t p) {
    return 1 + (unsigned)((k + i) % p);
}

/** @brief Cria uma mensagem com identidade (operation_id, sequence). */
static message_t make_message(uint64_t operation_id, uint32_t sequence) {
    message_t message = {
        .sender_pid = getpid(), .sequence = sequence, .operation_id = operation_id};
    (void)clock_gettime(CLOCK_REALTIME, &message.sent_at);
    return message;
}

/** @brief Recolhe os filhos próprios; devolve true se todos saíram com status zero. */
static bool reap_children(void) {
    bool all_ok = true;
    for (size_t i = 0; i < sizeof children / sizeof children[0]; ++i) {
        if (children[i] <= 0) continue;
        int status = 0;
        pid_t pid;
        while ((pid = waitpid(children[i], &status, 0)) < 0 && errno == EINTR) {
        }
        if (pid != children[i] || !WIFEXITED(status) || WEXITSTATUS(status) != 0) all_ok = false;
        children[i] = 0;
    }
    return all_ok;
}

/** @brief Encerra à força e recolhe filhos próprios (usado em caminhos de falha). */
static void kill_children(void) {
    for (size_t i = 0; i < sizeof children / sizeof children[0]; ++i) {
        if (children[i] > 0) (void)kill(children[i], SIGKILL);
    }
    (void)reap_children();
}

/** @brief Corpo do produtor k: envia suas mensagens, bloqueando em estruturas cheias. */
static void producer_child(
    shm_backend_t *backend,
    unsigned k,
    uint64_t base,
    size_t p,
    int pipe_read,
    int pipe_write
) {
    (void)close(pipe_read);
    (void)close(pipe_write);
    (void)signal(SIGALRM, SIG_DFL);
    (void)alarm(CHILD_TIMEOUT);
    for (unsigned i = 0; i < MESSAGES_PER_PRODUCER; ++i) {
        message_t message = make_message(base + k + 1, i + 1);
        if (shm_backend_send(backend, destination(k, i, p), &message).status != MPMC_OK) _exit(2);
    }
    _exit(shm_backend_worker_close(backend, nullptr).status == MPMC_OK ? 0 : 3);
}

/** @brief Corpo do consumidor c: retira exatamente sua cota e reporta cada mensagem. */
static void consumer_child(
    shm_backend_t *backend,
    unsigned c,
    unsigned structure,
    unsigned quota,
    int pipe_read,
    int pipe_write
) {
    (void)close(pipe_read);
    (void)signal(SIGALRM, SIG_DFL);
    (void)alarm(CHILD_TIMEOUT);
    for (unsigned n = 0; n < quota; ++n) {
        record_t record = {.consumer = c, .structure = structure};
        if (shm_backend_receive(backend, structure, &record.message, true).status != MPMC_OK) {
            _exit(2);
        }
        if (write(pipe_write, &record, sizeof record) != (ssize_t)sizeof record) _exit(4);
    }
    (void)close(pipe_write);
    _exit(shm_backend_worker_close(backend, nullptr).status == MPMC_OK ? 0 : 3);
}

/** @brief Lê exatamente um registro; false no EOF, em erro ou se o prazo expirou. */
static bool read_record(int fd, record_t *record) {
    size_t done = 0;
    while (done < sizeof *record) {
        ssize_t n = read(fd, (char *)record + done, sizeof *record - done);
        if (n > 0) {
            done += (size_t)n;
        } else if (n < 0 && errno == EINTR && !alarm_seen) {
            continue;
        } else {
            return false;
        }
    }
    return true;
}

/** @brief Executa uma rodada completa de estresse com p estruturas. */
static void run_round(unsigned round, size_t p) {
    const uint64_t base = (uint64_t)round * 100;
    const unsigned consumers = (unsigned)(2 * p);
    static unsigned seen[PRODUCERS + 1][MAX_SEQ + 1];
    static uint32_t last[MAX_CONSUMERS][PRODUCERS + 1];
    memset(seen, 0, sizeof seen);
    memset(last, 0, sizeof last);
    alarm_seen = 0;
    (void)alarm(ROUND_TIMEOUT);

    shm_backend_t *backend = nullptr;
    mpmc_result_t created = shm_backend_create(&backend, p, nullptr);
    CHECK(created.status == MPMC_OK && backend != nullptr);
    if (created.status != MPMC_OK || backend == nullptr) return;

    int shmids[MAX_STRUCTURES], semids[MAX_STRUCTURES];
    for (size_t j = 0; j < p; ++j) {
        shmids[j] = backend->segments[j].shmid;
        semids[j] = backend->segments[j].semid;
    }

    /* O pai enche todas as estruturas antes de criar filhos: a primeira operação de cada
     * produtor encontra a estrutura cheia, sem depender de tempo. */
    for (unsigned j = 1; j <= p; ++j) {
        for (unsigned n = 0; n < CAP; ++n) {
            message_t message = make_message(base, (j - 1) * CAP + n + 1);
            mpmc_result_t sent = shm_backend_send(backend, j, &message);
            CHECK(sent.status == MPMC_OK);
            if (sent.status != MPMC_OK) {
                timed_out = true; /* prazo ou falha no pré-enchimento: nada mais é tentado */
                (void)shm_backend_destroy(backend, nullptr);
                (void)alarm(0);
                return;
            }
        }
        CHECK(semctl(semids[j - 1], SHM_SEM_EMPTY, GETVAL) == 0);
        CHECK(semctl(semids[j - 1], SHM_SEM_FULL, GETVAL) == (int)CAP);
    }

    unsigned total[MAX_STRUCTURES + 1];
    for (unsigned j = 1; j <= p; ++j) total[j] = CAP;
    for (unsigned k = 0; k < PRODUCERS; ++k) {
        for (unsigned i = 0; i < MESSAGES_PER_PRODUCER; ++i) ++total[destination(k, i, p)];
    }

    int fds[2];
    CHECK(pipe(fds) == 0);
    size_t slot = 0;
    bool spawned = true;
    for (unsigned k = 0; k < PRODUCERS && spawned; ++k) {
        pid_t pid = fork();
        if (pid == 0) producer_child(backend, k, base, p, fds[0], fds[1]);
        if (pid < 0)
            spawned = false;
        else
            children[slot++] = pid;
    }
    for (unsigned c = 0; c < consumers && spawned; ++c) {
        unsigned structure = 1 + c % (unsigned)p;
        unsigned quota = total[structure] / 2 + (c < p ? total[structure] % 2 : 0);
        pid_t pid = fork();
        if (pid == 0) consumer_child(backend, c, structure, quota, fds[0], fds[1]);
        if (pid < 0)
            spawned = false;
        else
            children[slot++] = pid;
    }
    (void)close(fds[1]);
    CHECK(spawned);

    pid_t producer_pid[PRODUCERS + 1] = {getpid()};
    for (unsigned k = 0; k < PRODUCERS; ++k) producer_pid[k + 1] = children[k];

    unsigned received = 0, expected_total = 0;
    for (unsigned j = 1; j <= p; ++j) expected_total += total[j];
    record_t record;
    while (read_record(fds[0], &record)) {
        ++received;
        uint64_t op = record.message.operation_id - base;
        uint32_t seq = record.message.sequence;
        bool valid = record.message.operation_id >= base && op <= PRODUCERS && seq >= 1
            && seq <= MAX_SEQ && record.consumer < consumers;
        CHECK(valid);
        if (!valid) continue;
        unsigned dest = op == 0 ? (seq - 1) / CAP + 1 : destination((unsigned)op - 1, seq - 1, p);
        CHECK(op == 0 ? seq <= p * CAP : seq <= MESSAGES_PER_PRODUCER);
        CHECK(dest == record.structure);
        CHECK(record.message.sender_pid == producer_pid[op]);
        CHECK(seen[op][seq] == 0); /* sem consumo duplicado */
        ++seen[op][seq];
        CHECK(last[record.consumer][op] < seq); /* FIFO por consumidor e produtor */
        last[record.consumer][op] = seq;
    }
    (void)close(fds[0]);

    CHECK(!alarm_seen);
    CHECK(received == expected_total);
    for (unsigned op = 0; op <= PRODUCERS; ++op) {
        unsigned limit = op == 0 ? (unsigned)p * CAP : MESSAGES_PER_PRODUCER;
        for (unsigned seq = 1; seq <= limit; ++seq) CHECK(seen[op][seq] == 1); /* nada perdido */
    }
    if (alarm_seen || !reap_children()) {
        /* Prazo expirado ou filho com falha: filhos mortos por SIGKILL podem ter deixado o
         * mutex adquirido, então nenhuma operação sincronizada é tentada; só a remoção. */
        CHECK(false);
        kill_children();
        timed_out = true;
        (void)shm_backend_destroy(backend, nullptr);
        (void)alarm(0);
        return;
    }
    errno = 0;
    CHECK(
        waitpid(-1, nullptr, WNOHANG) < 0 && errno == ECHILD
    ); /* sem zumbis ou filhos restantes */

    for (unsigned j = 1; j <= p; ++j) {
        uint64_t consumed = 0;
        CHECK(shm_backend_consumed_count(backend, j, &consumed).status == MPMC_OK);
        CHECK(consumed == total[j]);
        message_t out;
        CHECK(shm_backend_receive(backend, j, &out, false).status == MPMC_ERR_WOULD_BLOCK);
        CHECK(semctl(semids[j - 1], SHM_SEM_MUTEX, GETVAL) == 1);
        CHECK(semctl(semids[j - 1], SHM_SEM_EMPTY, GETVAL) == (int)CAP);
        CHECK(semctl(semids[j - 1], SHM_SEM_FULL, GETVAL) == 0);
    }

    CHECK(shm_backend_destroy(backend, nullptr).status == MPMC_OK);
    for (size_t j = 0; j < p; ++j) {
        CHECK(shmctl(shmids[j], IPC_STAT, &(struct shmid_ds){}) < 0);
        CHECK(semctl(semids[j], 0, GETVAL) < 0);
    }
    (void)alarm(0);
}

/** @brief Programa principal: um backend espectador vive durante todas as rodadas. */
int main(void) {
    struct sigaction action = {.sa_handler = on_alarm, .sa_flags = 0};
    sigemptyset(&action.sa_mask);
    if (sigaction(SIGALRM, &action, nullptr) != 0) return EXIT_FAILURE;

    shm_backend_t *bystander = nullptr;
    CHECK(shm_backend_create(&bystander, 1, nullptr).status == MPMC_OK && bystander != nullptr);
    if (bystander == nullptr) return EXIT_FAILURE;
    int by_shm = bystander->segments[0].shmid, by_sem = bystander->segments[0].semid;
    message_t keep = make_message(7777, 1);
    (void)alarm(ROUND_TIMEOUT);
    mpmc_result_t kept = shm_backend_send(bystander, 1, &keep);
    CHECK(kept.status == MPMC_OK);
    if (kept.status != MPMC_OK) {
        (void)shm_backend_destroy(bystander, nullptr);
        printf("%u verificações de estresse SHM, %u falhas\n", checks, failures);
        return EXIT_FAILURE;
    }
    (void)alarm(0);

    for (
        unsigned round = 0; round < sizeof structures_per_round / sizeof structures_per_round[0];
        ++round
    ) {
        run_round(round + 1, structures_per_round[round]);
        kill_children();
        if (timed_out) break;
    }

    /* Recursos de outra instância (o espectador) não foram removidos nem alterados. */
    message_t out;
    (void)alarm(ROUND_TIMEOUT);
    CHECK(shmctl(by_shm, IPC_STAT, &(struct shmid_ds){}) == 0);
    CHECK(semctl(by_sem, SHM_SEM_FULL, GETVAL) == 1);
    CHECK(shm_backend_receive(bystander, 1, &out, false).status == MPMC_OK);
    CHECK(out.operation_id == keep.operation_id && out.sequence == keep.sequence);
    CHECK(shm_backend_destroy(bystander, nullptr).status == MPMC_OK);
    (void)alarm(0);

    printf("%u verificações de estresse SHM, %u falhas\n", checks, failures);
    return failures == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
