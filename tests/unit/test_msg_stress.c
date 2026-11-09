/**
 * @file test_msg_stress.c
 * @brief Teste do backend MSG real com vários processos, filas cheias e sem ACK (issue 57).
 * @ingroup tests
 * @details Usa o backend real, sem ACK, supervisor, workers finais ou REPL. Em cada rodada o
 * pai cria um backend, enche cada fila de dados até o kernel recusar (IPC_NOWAIT), dispara
 * produtores (que bloqueiam em filas cheias) e depois dois consumidores por estrutura.
 * Cada mensagem tem identidade única (operation_id, sequence) e metadados determinísticos
 * em sent_at. O pai confere, sem supor ordem global entre produtores: nenhuma perda,
 * nenhuma duplicação, destino, remetente e metadados corretos, FIFO por (consumidor,
 * produtor) e totais consumidos exatos. Enquanto espera os consumidores, o pai drena a fila
 * de eventos a cada despertar de poll(), como o supervisor faz no REPL, e assim os
 * consumidores nunca bloqueiam no canal de estatísticas. O volume é fixo (sem aleatoriedade
 * e sem sleep()). Todo prazo usa alarm(); os filhos morrem por SIGALRM se travarem e o pai
 * encerra os próprios filhos no tratador. Cada filho fecha só seu estado local; só o pai
 * remove os recursos que criou, o que se confere por msgctl().
 * @note test_timeout_detection() força dois travamentos reais para validar os prazos.
 * Limitações: não cobre ACK, consumo parcial de workers, striped nem shutdown
 * durante esperas reais (continuam na issue 25).
 */
#include <errno.h>
#include <poll.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/msg.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

/* O teste precisa dos identificadores das filas, que são privados do backend. */
#include "../../src/ipc/msg_backend.c"

/** @brief Quantidade de produtores por rodada. */
#define PRODUCERS 4U
/** @brief Mensagens enviadas por produtor em cada rodada. */
#define MESSAGES_PER_PRODUCER 100U
/** @brief Maior quantidade de estruturas entre as rodadas. */
#define MAX_STRUCTURES 3U
/** @brief Maior quantidade de consumidores (dois por estrutura). */
#define MAX_CONSUMERS (2U * MAX_STRUCTURES)
/** @brief Maior sequência esperada, incluindo o pré-enchimento do pai. */
#define MAX_SEQ 8192U
/** @brief Prazo do pai por rodada e dos filhos, em segundos. */
#define ROUND_TIMEOUT 30U
#define CHILD_TIMEOUT 20U
/** @brief Espera máxima de poll() entre duas drenagens da fila de eventos, em ms. */
#define DRAIN_INTERVAL_MS 20

/** @brief Quantidade de estruturas de cada rodada. */
static const size_t structures_per_round[] = {1, 2, 3};

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
    return 1U + (unsigned)((k + i) % p);
}

/** @brief Cria uma mensagem com identidade (operation_id, sequence) e metadados fixos. */
static message_t make_message(uint64_t operation_id, uint32_t sequence) {
    message_t message = {
        .sender_pid = getpid(),
        .sequence = sequence,
        .sent_at = {.tv_sec = (time_t)(1000U + operation_id), .tv_nsec = (long)sequence},
        .operation_id = operation_id,
    };
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

/** @brief Corpo do produtor k: envia suas mensagens, bloqueando em filas cheias. */
static void producer_child(
    msg_backend_t *backend,
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
        message_t message = make_message(base + k + 1U, i + 1U);
        if (msg_backend_send(backend, destination(k, i, p), &message).status != MPMC_OK) _exit(2);
    }
    msg_backend_worker_close(backend);
    _exit(0);
}

/** @brief Corpo do consumidor c: retira exatamente sua cota e reporta cada mensagem. */
static void consumer_child(
    msg_backend_t *backend,
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
        if (msg_backend_receive(backend, structure, &record.message, true).status != MPMC_OK) {
            _exit(2);
        }
        if (write(pipe_write, &record, sizeof record) != (ssize_t)sizeof record) _exit(4);
    }
    (void)close(pipe_write);
    msg_backend_worker_close(backend);
    _exit(0);
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

/** @brief Remove os recursos do pai após uma falha de rodada e abandona as seguintes. */
static void abandon_round(msg_backend_t *backend) {
    timed_out = true;
    msg_backend_destroy(backend);
    (void)alarm(0);
}

/** @brief Verifica que o backend rejeita, sem inventar retornos, índices e ponteiros inválidos. */
static void test_invalid_arguments(void) {
    msg_backend_t *backend = (msg_backend_t *)&backend;
    CHECK(msg_backend_create(NULL, 1U).status == MPMC_ERR_INVALID_ARG);
    CHECK(msg_backend_create(&backend, 0U).status == MPMC_ERR_INVALID_ARG);
    CHECK(backend == NULL);

    CHECK(msg_backend_create(&backend, 1U).status == MPMC_OK && backend != NULL);
    if (backend == NULL) return;
    message_t message = make_message(1U, 1U);
    message_t out;
    uint64_t count = 77U;
    CHECK(msg_backend_send(backend, 0U, &message).status == MPMC_ERR_INVALID_ARG);
    CHECK(msg_backend_send(backend, 2U, &message).status == MPMC_ERR_INVALID_ARG);
    CHECK(msg_backend_send(backend, 1U, NULL).status == MPMC_ERR_INVALID_ARG);
    CHECK(msg_backend_receive(backend, 0U, &out, false).status == MPMC_ERR_INVALID_ARG);
    CHECK(msg_backend_receive(backend, 2U, &out, false).status == MPMC_ERR_INVALID_ARG);
    CHECK(msg_backend_receive(backend, 1U, NULL, false).status == MPMC_ERR_INVALID_ARG);
    CHECK(msg_backend_consumed_count(backend, 0U, &count).status == MPMC_ERR_INVALID_ARG);
    CHECK(msg_backend_consumed_count(backend, 2U, &count).status == MPMC_ERR_INVALID_ARG);
    CHECK(msg_backend_consumed_count(backend, 1U, NULL).status == MPMC_ERR_INVALID_ARG);
    CHECK(count == 77U);
    /* Recepção não bloqueante em estrutura vazia responde sem esperar itens futuros. */
    CHECK(msg_backend_receive(backend, 1U, &out, false).status == MPMC_ERR_WOULD_BLOCK);
    msg_backend_destroy(backend);
}

/** @brief Recolhe um filho de teste e devolve o sinal que o matou (0 se saiu normalmente). */
static int wait_for_signal(pid_t pid) {
    int status = 0;
    pid_t done;
    while ((done = waitpid(pid, &status, 0)) < 0 && errno == EINTR) {
    }
    if (done != pid) return -1;
    return WIFSIGNALED(status) ? WTERMSIG(status) : 0;
}

/**
 * @brief Força travamentos reais para provar que os prazos os detectam e limpam.
 * @details Um filho bloqueia em fila vazia com seu próprio alarm(): deve morrer por SIGALRM.
 * Outro bloqueia sem alarme útil (apenas um limite de reserva longo): o alarm() do pai aciona
 * on_alarm(), que o mata por SIGKILL. Em ambos, o pai recolhe o filho e remove somente a fila
 * que criou.
 */
static void test_timeout_detection(void) {
    msg_backend_t *backend = NULL;
    CHECK(msg_backend_create(&backend, 1U).status == MPMC_OK && backend != NULL);
    if (backend == NULL) return;
    int queue_id = backend->data_queues[0];
    struct msqid_ds info;

    /* Prazo do próprio filho. */
    pid_t pid = fork();
    if (pid == 0) {
        message_t out;
        (void)signal(SIGALRM, SIG_DFL);
        (void)alarm(1U);
        (void)msg_backend_receive(backend, 1U, &out, true);
        _exit(0); /* só alcançado se o bloqueio não ocorrer */
    }
    CHECK(pid > 0);
    children[0] = pid;
    (void)alarm(ROUND_TIMEOUT);
    CHECK(wait_for_signal(pid) == SIGALRM);
    children[0] = 0;

    /* Prazo do pai: o tratador encerra o filho bloqueado. */
    alarm_seen = 0;
    pid = fork();
    if (pid == 0) {
        message_t out;
        (void)signal(SIGALRM, SIG_DFL);
        (void)alarm(CHILD_TIMEOUT); /* reserva: se o tratador do pai falhar, o resultado difere */
        (void)msg_backend_receive(backend, 1U, &out, true);
        _exit(0);
    }
    CHECK(pid > 0);
    children[0] = pid;
    (void)alarm(1U);
    CHECK(wait_for_signal(pid) == SIGKILL);
    CHECK(alarm_seen);
    children[0] = 0;
    (void)alarm(0);

    errno = 0;
    CHECK(waitpid(-1, NULL, WNOHANG) < 0 && errno == ECHILD); /* sem zumbis */
    CHECK(msgctl(queue_id, IPC_STAT, &info) == 0 && info.msg_qnum == 0U);
    msg_backend_destroy(backend);
    CHECK(msgctl(queue_id, IPC_STAT, &info) < 0); /* a fila do teste foi removida */
    alarm_seen = 0;
}

/** @brief Executa uma rodada completa com p estruturas. */
static void run_round(unsigned round, size_t p) {
    const uint64_t base = (uint64_t)round * 100U;
    const unsigned consumers = (unsigned)(2U * p);
    static unsigned seen[PRODUCERS + 1U][MAX_SEQ + 1U];
    static uint32_t last[MAX_CONSUMERS][PRODUCERS + 1U];
    static unsigned prefill_structure[MAX_SEQ + 1U];
    memset(seen, 0, sizeof seen);
    memset(last, 0, sizeof last);
    memset(prefill_structure, 0, sizeof prefill_structure);
    alarm_seen = 0;
    (void)alarm(ROUND_TIMEOUT);

    msg_backend_t *backend = NULL;
    mpmc_result_t created = msg_backend_create(&backend, p);
    CHECK(created.status == MPMC_OK && backend != NULL);
    if (created.status != MPMC_OK || backend == NULL) return;

    int queue_ids[MAX_STRUCTURES];
    for (size_t j = 0; j < p; ++j) queue_ids[j] = backend->data_queues[j];
    int event_queue = backend->event_queue;

    /* O pai enche cada fila antes de criar filhos, de forma que a primeira operação de cada
     * produtor encontra a fila cheia, sem depender de tempo. O limite vem do kernel. */
    unsigned prefilled[MAX_STRUCTURES + 1U] = {0};
    unsigned next_seq = 1U;
    for (unsigned j = 1; j <= p; ++j) {
        for (;;) {
            if (next_seq > MAX_SEQ) {
                CHECK(false);
                abandon_round(backend);
                return;
            }
            message_t message = make_message(base, next_seq);
            mpmc_result_t sent =
                sysv_msg_send(queue_ids[j - 1U], 1, &message, sizeof message, true);
            if (sent.status == MPMC_ERR_WOULD_BLOCK) break;
            if (sent.status != MPMC_OK) {
                CHECK(false);
                abandon_round(backend);
                return;
            }
            prefill_structure[next_seq++] = j;
            ++prefilled[j];
        }
        struct msqid_ds info;
        CHECK(prefilled[j] > 0U);
        CHECK(msgctl(queue_ids[j - 1U], IPC_STAT, &info) == 0);
        CHECK(info.msg_qnum == prefilled[j]);
    }

    unsigned total[MAX_STRUCTURES + 1U];
    for (unsigned j = 1; j <= p; ++j) total[j] = prefilled[j];
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
        unsigned structure = 1U + c % (unsigned)p;
        unsigned quota = total[structure] / 2U + (c < p ? total[structure] % 2U : 0U);
        pid_t pid = fork();
        if (pid == 0) consumer_child(backend, c, structure, quota, fds[0], fds[1]);
        if (pid < 0)
            spawned = false;
        else
            children[slot++] = pid;
    }
    (void)close(fds[1]);
    CHECK(spawned);

    pid_t producer_pid[PRODUCERS + 1U] = {getpid()};
    for (unsigned k = 0; k < PRODUCERS; ++k) producer_pid[k + 1U] = children[k];

    unsigned received = 0, expected_total = 0;
    for (unsigned j = 1; j <= p; ++j) expected_total += total[j];

    struct pollfd watched = {.fd = fds[0], .events = POLLIN};
    for (;;) {
        int ready = poll(&watched, 1, DRAIN_INTERVAL_MS);
        if (alarm_seen) break;
        if (ready < 0 && errno == EINTR) continue;
        CHECK(ready >= 0);
        if (ready < 0) break;
        /* Eventos são drenados a cada despertar, independentemente de haver registros. */
        CHECK(msg_backend_poll_events(backend).status == MPMC_OK);
        if (ready == 0) continue;
        record_t record;
        if (!read_record(fds[0], &record)) break;
        ++received;
        uint64_t op = record.message.operation_id - base;
        uint32_t seq = record.message.sequence;
        bool valid = record.message.operation_id >= base && op <= PRODUCERS && seq >= 1U
            && seq <= MAX_SEQ && record.consumer < consumers;
        CHECK(valid);
        if (!valid) continue;
        unsigned dest =
            op == 0 ? prefill_structure[seq] : destination((unsigned)op - 1U, seq - 1U, p);
        CHECK(op == 0 ? (seq < next_seq) : (seq <= MESSAGES_PER_PRODUCER));
        CHECK(dest == record.structure);
        CHECK(record.message.sender_pid == producer_pid[op]);
        CHECK(record.message.sent_at.tv_sec == (time_t)(1000U + record.message.operation_id));
        CHECK(record.message.sent_at.tv_nsec == (long)seq); /* metadados preservados */
        CHECK(seen[op][seq] == 0U);                         /* sem consumo duplicado */
        ++seen[op][seq];
        CHECK(last[record.consumer][op] < seq); /* FIFO por consumidor e produtor */
        last[record.consumer][op] = seq;
    }
    (void)close(fds[0]);

    CHECK(!alarm_seen);
    CHECK(received == expected_total);
    for (unsigned op = 0; op <= PRODUCERS; ++op) {
        unsigned limit = op == 0 ? next_seq - 1U : MESSAGES_PER_PRODUCER;
        for (unsigned seq = 1; seq <= limit; ++seq) CHECK(seen[op][seq] == 1U); /* nada perdido */
    }
    if (alarm_seen || !reap_children()) {
        CHECK(false);
        kill_children();
        abandon_round(backend);
        return;
    }
    errno = 0;
    CHECK(waitpid(-1, NULL, WNOHANG) < 0 && errno == ECHILD); /* sem zumbis ou filhos restantes */

    /* A consolidação final obtém os totais exatos, sem depender do heap dos filhos. */
    CHECK(msg_backend_poll_events(backend).status == MPMC_OK);
    struct msqid_ds info;
    CHECK(msgctl(event_queue, IPC_STAT, &info) == 0 && info.msg_qnum == 0U);
    for (unsigned j = 1; j <= p; ++j) {
        uint64_t consumed = 0;
        message_t out;
        CHECK(msg_backend_consumed_count(backend, j, &consumed).status == MPMC_OK);
        CHECK(consumed == total[j]);
        CHECK(msg_backend_receive(backend, j, &out, false).status == MPMC_ERR_WOULD_BLOCK);
        CHECK(msgctl(queue_ids[j - 1U], IPC_STAT, &info) == 0 && info.msg_qnum == 0U);
    }

    msg_backend_destroy(backend);
    for (size_t j = 0; j < p; ++j) CHECK(msgctl(queue_ids[j], IPC_STAT, &info) < 0);
    CHECK(msgctl(event_queue, IPC_STAT, &info) < 0);
    (void)alarm(0);
}

/** @brief Programa principal: um backend espectador vive durante todas as rodadas. */
int main(void) {
    struct sigaction action = {.sa_handler = on_alarm, .sa_flags = 0};
    sigemptyset(&action.sa_mask);
    if (sigaction(SIGALRM, &action, NULL) != 0) return EXIT_FAILURE;

    (void)alarm(ROUND_TIMEOUT);
    test_invalid_arguments();
    (void)alarm(0);
    test_timeout_detection();

    msg_backend_t *bystander = NULL;
    CHECK(msg_backend_create(&bystander, 1U).status == MPMC_OK && bystander != NULL);
    if (bystander == NULL) return EXIT_FAILURE;
    int by_queue = bystander->data_queues[0];
    message_t keep = make_message(7777U, 1U);
    (void)alarm(ROUND_TIMEOUT);
    mpmc_result_t kept = msg_backend_send(bystander, 1U, &keep);
    CHECK(kept.status == MPMC_OK);
    if (kept.status != MPMC_OK) {
        msg_backend_destroy(bystander);
        printf("%u verificações de estresse MSG, %u falhas\n", checks, failures);
        return EXIT_FAILURE;
    }
    (void)alarm(0);

    for (
        unsigned round = 0; round < sizeof structures_per_round / sizeof structures_per_round[0];
        ++round
    ) {
        run_round(round + 1U, structures_per_round[round]);
        kill_children();
        if (timed_out) break;
    }

    /* Recursos de outra instância (o espectador) não foram removidos nem alterados. */
    message_t out;
    struct msqid_ds info;
    (void)alarm(ROUND_TIMEOUT);
    CHECK(msgctl(by_queue, IPC_STAT, &info) == 0 && info.msg_qnum == 1U);
    CHECK(msg_backend_receive(bystander, 1U, &out, false).status == MPMC_OK);
    CHECK(out.operation_id == keep.operation_id && out.sequence == keep.sequence);
    CHECK(out.sent_at.tv_sec == keep.sent_at.tv_sec && out.sent_at.tv_nsec == keep.sent_at.tv_nsec);
    msg_backend_destroy(bystander);
    (void)alarm(0);

    printf("%u verificações de estresse MSG, %u falhas\n", checks, failures);
    return failures == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
