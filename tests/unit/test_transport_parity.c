/**
 * @file test_transport_parity.c
 * @brief Critérios de aceite da integração dos dois paradigmas pela fachada.
 * @ingroup tests
 * @details Usa os backends reais (System V) e processos reais. Cobre:
 * - C1: o código dos workers não ramifica por paradigma (varredura do código-fonte)
 *   e uma única função de cenário, que só usa ipc_*, producer_run() e consumer_run(),
 *   produz o mesmo resultado observável nos dois modos;
 * - C2: a mesma tabela de chamadas inválidas dá o mesmo status nos dois modos;
 * - C3: uma falha parcial real de ipc_init() (limite do kernel esgotado, sem
 *   substitutos) devolve erro, deixa *out nulo e não deixa objetos IPC criados;
 * - C4: ipc_worker_close() num filho não remove recurso global; ipc_destroy() remove.
 * Todo bloqueio tem prazo: um alarm() mata os filhos e remove o contexto em uso.
 */
#include <errno.h>
#include <limits.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <sys/sem.h>
#include <sys/shm.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

/* Os identificadores dos recursos são privados dos backends. */
#include "../../src/ipc/msg_backend.c"
#include "../../src/ipc/shm_backend/shm_backend_internal.h"
#include "../../src/ipc/transport.c"
#include "mpmc/workers/consumer.h"
#include "mpmc/workers/producer.h"

#define DEADLINE_MS 15000U
#define MAX_KIDS 8U
#define SOURCE_WORKERS 3U

static unsigned checks;
static unsigned failures;
static pid_t kids[MAX_KIDS];
static ipc_context_t *volatile live;

#define CHECK(condition)                                                                           \
    do {                                                                                           \
        ++checks;                                                                                  \
        if (!(condition)) {                                                                        \
            ++failures;                                                                            \
            fprintf(stderr, "FALHA %s:%d: %s\n", __FILE__, __LINE__, #condition);                  \
        }                                                                                          \
    } while (0)

static const char *mode_name(ipc_mode_t mode) {
    return mode == IPC_SHARED_MEMORY ? "shm" : "msg";
}

/** @brief Último recurso contra travamento: mata filhos e remove o contexto em uso. */
static void on_alarm(int signal_number) {
    (void)signal_number;
    for (size_t index = 0u; index < MAX_KIDS; ++index) {
        if (kids[index] > 0) (void)kill(kids[index], SIGKILL);
    }
    ipc_context_t *context = live;
    if (context != NULL) ipc_destroy(context);
    static const char message[] = "ALARME: teste travou; filhos mortos\n";
    (void)!write(STDERR_FILENO, message, sizeof(message) - 1u);
    _exit(3);
}

static void tick(void) {
    const struct timespec delay = {.tv_sec = 0, .tv_nsec = 2000000};
    (void)nanosleep(&delay, NULL);
}

static unsigned long elapsed_ms(const struct timespec *start) {
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    return (unsigned long)((now.tv_sec - start->tv_sec) * 1000L
        + (now.tv_nsec - start->tv_nsec) / 1000000L);
}

static int exit_code(mpmc_result_t result) {
    return result.status == MPMC_OK ? 0 : 100 + (int)result.status;
}

/** @brief Recolhe um filho com prazo e drena eventos, como faz o supervisor. */
static int reap(size_t slot, ipc_context_t *context) {
    struct timespec start;
    clock_gettime(CLOCK_MONOTONIC, &start);
    for (;;) {
        (void)ipc_poll_events(context);
        int status = 0;
        pid_t pid = waitpid(kids[slot], &status, WNOHANG);
        if (pid == kids[slot]) {
            kids[slot] = 0;
            return WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status);
        }
        if (pid == -1 && errno != EINTR) {
            kids[slot] = 0;
            return -1;
        }
        if (elapsed_ms(&start) > DEADLINE_MS) {
            (void)kill(kids[slot], SIGKILL);
            (void)waitpid(kids[slot], &status, 0);
            kids[slot] = 0;
            return -1;
        }
        tick();
    }
}

/* ------------------------------------------------------------------------- */
/* C1: sem ramificação por paradigma nos workers                             */
/* ------------------------------------------------------------------------- */

/** @brief Verdadeiro se o arquivo existe e não cita nenhum símbolo de paradigma. */
static bool source_is_mode_agnostic(const char *path) {
    static const char *const forbidden[] = {
        "IPC_SHARED_MEMORY",
        "IPC_MESSAGE_QUEUE",
        "ipc_mode_t",
        "shm_backend",
        "msg_backend",
        "sysv_msg",
        "semop",
        "msgsnd",
        "msgrcv",
    };
    FILE *file = fopen(path, "r");
    if (file == NULL) {
        fprintf(stderr, "não foi possível abrir %s (execute a partir da raiz)\n", path);
        return false;
    }
    char line[512];
    bool clean = true;
    while (fgets(line, sizeof(line), file) != NULL) {
        for (size_t index = 0u; index < sizeof(forbidden) / sizeof(forbidden[0]); ++index) {
            if (strstr(line, forbidden[index]) != NULL) {
                fprintf(stderr, "%s cita %s\n", path, forbidden[index]);
                clean = false;
            }
        }
    }
    fclose(file);
    return clean;
}

static void test_workers_have_no_paradigm_branches(void) {
    static const char *const sources[SOURCE_WORKERS] = {
        "src/workers/producer.c",
        "src/workers/consumer.c",
        "src/workers/producer_routing.c",
    };
    for (size_t index = 0u; index < SOURCE_WORKERS; ++index) {
        CHECK(source_is_mode_agnostic(sources[index]));
    }
}

/*
 * Volumes do cenário. O total em trânsito antes do primeiro consumo é kDirected +
 * kStriped = 20 mensagens: os produtores usam envio bloqueante e este teste roda
 * também no macOS, onde o kernel limita as mensagens em trânsito no sistema todo
 * (da ordem de 40) e as filas a 2 KiB. Volumes maiores travariam os produtores.
 */
enum {
    kDirected = 8,  /* produtor A: mensagens à fila 2 */
    kStriped = 12,  /* produtor B: em rodízio nas filas 1..3 */
    kPerQueueB = 4, /* kStriped / 3 */
    kHead = 3,      /* itens conferidos diretamente pela fachada */
};

typedef struct {
    int codes[8];
    size_t code_count;
    uint64_t consumed[3];
    bool metadata_ok;
} outcome_t;

static pid_t spawn_producer(
    ipc_context_t *context,
    size_t queue,
    unsigned count,
    producer_ack_mode_t ack,
    uint64_t operation
) {
    pid_t pid = fork();
    if (pid == 0) {
        (void)signal(SIGALRM, SIG_DFL);
        producer_job_t job = {
            .producer_id = queue,
            .message_count = count,
            .ack_mode = ack,
            .operation_id = operation,
        };
        _exit(exit_code(producer_run(context, &job)));
    }
    return pid;
}

static pid_t spawn_consumer(
    ipc_context_t *context,
    size_t queue,
    unsigned count,
    consumer_ack_mode_t mode
) {
    pid_t pid = fork();
    if (pid == 0) {
        (void)signal(SIGALRM, SIG_DFL);
        consumer_job_t job = {
            .producer_id = queue,
            .message_count = count,
            .ack_mode = mode,
            .operation_id = 900u + count,
        };
        consumer_result_t result;
        _exit(exit_code(consumer_run(context, &job, &result)));
    }
    return pid;
}

/**
 * @brief Verifica, pela fachada, os primeiros itens de uma fila.
 * @details FIFO por emissor, PID real do filho, operation_id do trabalho e sent_at
 * preenchido. Não conhece o paradigma: só chama ipc_receive().
 */
static bool check_head(
    ipc_context_t *context,
    size_t queue,
    pid_t sender,
    uint64_t operation,
    const uint32_t *expected_sequences,
    size_t count
) {
    bool ok = true;
    for (size_t index = 0u; index < count; ++index) {
        message_t message;
        mpmc_result_t result = ipc_receive(context, queue, &message, false);
        ok = ok && result.status == MPMC_OK && message.sender_pid == sender
            && message.operation_id == operation && message.sequence == expected_sequences[index]
            && message.sent_at.tv_sec != 0;
    }
    return ok;
}

/**
 * @brief Cenário único, idêntico para os dois paradigmas.
 * @details O modo só aparece em ipc_init(). Daí em diante o código usa apenas a
 * fachada e os workers: dois produtores (um dirigido, um *striped*), consumidores
 * bloqueantes e um não bloqueante sobre fila vazia.
 */
static outcome_t run_scenario(ipc_mode_t mode) {
    outcome_t outcome = {0};
    ipc_context_t *context = NULL;
    if (ipc_init(&context, 3u, mode).status != MPMC_OK) return outcome;
    live = context;

    /* Produtor A: 20 mensagens à fila 2. Produtor B: 30 mensagens em rodízio 1..3. */
    kids[0] = spawn_producer(context, 2u, kDirected, PRODUCER_ACK_NONE, 1u);
    pid_t producer_a = kids[0];
    outcome.codes[outcome.code_count++] = reap(0u, context);
    kids[0] = spawn_producer(context, 0u, kStriped, PRODUCER_ACK_NONE, 2u);
    pid_t producer_b = kids[0];
    outcome.codes[outcome.code_count++] = reap(0u, context);

    /* Fila 1 só tem as mensagens 1, 4, 7... do striped; a fila 2 tem as de A antes. */
    const uint32_t striped_first[3] = {1u, 4u, 7u};
    const uint32_t directed_first[3] = {1u, 2u, 3u};
    outcome.metadata_ok = check_head(context, 1u, producer_b, 2u, striped_first, 3u)
        && check_head(context, 2u, producer_a, 1u, directed_first, 3u);

    /* O restante é consumido por workers: 7 + 27 + 10. */
    kids[0] = spawn_consumer(context, 1u, kPerQueueB - kHead, CONSUMER_BLOCKING);
    kids[1] = spawn_consumer(context, 2u, kDirected + kPerQueueB - kHead, CONSUMER_BLOCKING);
    kids[2] = spawn_consumer(context, 3u, kPerQueueB, CONSUMER_BLOCKING);
    for (size_t slot = 0u; slot < 3u; ++slot) {
        outcome.codes[outcome.code_count++] = reap(slot, context);
    }

    /* Tudo foi retirado: um consumo não bloqueante agora termina por falta de item. */
    kids[0] = spawn_consumer(context, 3u, 1u, CONSUMER_NONBLOCKING);
    outcome.codes[outcome.code_count++] = reap(0u, context);

    (void)ipc_poll_events(context);
    for (size_t queue = 1u; queue <= 3u; ++queue) {
        uint64_t count = 0u;
        if (ipc_consumed_count(context, queue, &count).status == MPMC_OK) {
            outcome.consumed[queue - 1u] = count;
        }
    }
    live = NULL;
    ipc_destroy(context);
    return outcome;
}

static void print_outcome(const char *name, const outcome_t *outcome) {
    fprintf(stderr, "  [%s] metadata_ok=%d códigos=[", name, (int)outcome->metadata_ok);
    for (size_t index = 0u; index < outcome->code_count; ++index) {
        fprintf(stderr, "%s%d", index == 0u ? "" : " ", outcome->codes[index]);
    }
    fprintf(
        stderr,
        "] consumidos=[%llu %llu %llu]\n",
        (unsigned long long)outcome->consumed[0],
        (unsigned long long)outcome->consumed[1],
        (unsigned long long)outcome->consumed[2]
    );
}

static void test_same_worker_code_same_outcome(void) {
    const unsigned before = failures;
    outcome_t shm = run_scenario(IPC_SHARED_MEMORY);
    outcome_t msg = run_scenario(IPC_MESSAGE_QUEUE);
    const int expected_codes[6] = {0, 0, 0, 0, 0, 100 + (int)MPMC_ERR_WOULD_BLOCK};
    const uint64_t expected_consumed[3] = {kPerQueueB, kDirected + kPerQueueB, kPerQueueB};

    for (size_t mode_index = 0u; mode_index < 2u; ++mode_index) {
        const outcome_t *outcome = mode_index == 0u ? &shm : &msg;
        CHECK(outcome->code_count == 6u);
        CHECK(memcmp(outcome->codes, expected_codes, sizeof(expected_codes)) == 0);
        CHECK(memcmp(outcome->consumed, expected_consumed, sizeof(expected_consumed)) == 0);
        CHECK(outcome->metadata_ok);
    }
    /* Paridade: o resultado observável é o mesmo, byte a byte. */
    CHECK(shm.code_count == msg.code_count);
    CHECK(memcmp(shm.codes, msg.codes, sizeof(shm.codes)) == 0);
    CHECK(memcmp(shm.consumed, msg.consumed, sizeof(shm.consumed)) == 0);
    if (failures != before) {
        print_outcome("shm", &shm);
        print_outcome("msg", &msg);
    }
}

/* ------------------------------------------------------------------------- */
/* C2: erros convertidos de modo consistente                                 */
/* ------------------------------------------------------------------------- */

typedef struct {
    const char *name;
    mpmc_status_t status;
    int system_errno;
} call_result_t;

#define ERROR_CASES 22u

/** @brief Executa a mesma tabela de chamadas inválidas e devolve o resultado de cada uma. */
static void collect_errors(ipc_mode_t mode, call_result_t out[ERROR_CASES]) {
    ipc_context_t *context = NULL;
    CHECK(ipc_init(&context, 2u, mode).status == MPMC_OK);
    if (context == NULL) return;
    message_t message = {.sender_pid = getpid(), .sequence = 1u, .operation_id = 7u};
    message_t received;
    uint64_t count = 0u;
    size_t n = 0u;
#define RECORD(label, expression)                                                                  \
    do {                                                                                           \
        mpmc_result_t r_ = (expression);                                                           \
        out[n++] = (call_result_t){label, r_.status, r_.system_errno};                             \
    } while (0)
    RECORD("send índice 0", ipc_send(context, 0u, &message));
    RECORD("send índice p+1", ipc_send(context, 3u, &message));
    RECORD("send mensagem nula", ipc_send(context, 1u, NULL));
    RECORD("receive índice 0", ipc_receive(context, 0u, &received, false));
    RECORD("receive índice p+1", ipc_receive(context, 3u, &received, false));
    RECORD("receive saída nula", ipc_receive(context, 1u, NULL, false));
    RECORD("consumed índice 0", ipc_consumed_count(context, 0u, &count));
    RECORD("consumed índice p+1", ipc_consumed_count(context, 3u, &count));
    RECORD("consumed saída nula", ipc_consumed_count(context, 1u, NULL));
    RECORD("ack_begin op 0", ipc_ack_begin(context, 0u, 1u));
    RECORD("ack_begin op > LONG_MAX", ipc_ack_begin(context, (uint64_t)LONG_MAX + 1u, 1u));
    RECORD("ack_begin n 0", ipc_ack_begin(context, 9u, 0u));
    RECORD("ack_begin n 1025", ipc_ack_begin(context, 9u, 1025u));
    RECORD("ack_wait op 0", ipc_ack_wait(context, 0u));
    RECORD("ack_end op 0", ipc_ack_end(context, 0u));
    RECORD("ack_abandon op 0", ipc_ack_abandon(context, 0u));
    RECORD("ack_abandon op > LONG_MAX", ipc_ack_abandon(context, (uint64_t)LONG_MAX + 1u));
    RECORD("contexto nulo send", ipc_send(NULL, 1u, &message));
    RECORD("contexto nulo receive", ipc_receive(NULL, 1u, &received, false));
    RECORD("contexto nulo poll", ipc_poll_events(NULL));
    RECORD("contexto nulo ack_begin", ipc_ack_begin(NULL, 9u, 1u));
    /* Fila vazia sem bloquear: mesmo status; o errno é o da syscall de cada paradigma. */
    RECORD("receive vazia", ipc_receive(context, 1u, &received, false));
#undef RECORD
    ipc_destroy(context);
}

static void test_error_parity(void) {
    call_result_t shm[ERROR_CASES + 1u] = {0};
    call_result_t msg[ERROR_CASES + 1u] = {0};
    collect_errors(IPC_SHARED_MEMORY, shm);
    collect_errors(IPC_MESSAGE_QUEUE, msg);
    static const mpmc_status_t expected[ERROR_CASES + 1u] = {
        MPMC_ERR_INVALID_INDEX,
        MPMC_ERR_INVALID_INDEX,
        MPMC_ERR_INVALID_ARG,
        MPMC_ERR_INVALID_INDEX,
        MPMC_ERR_INVALID_INDEX,
        MPMC_ERR_INVALID_ARG,
        MPMC_ERR_INVALID_INDEX,
        MPMC_ERR_INVALID_INDEX,
        MPMC_ERR_INVALID_ARG,
        MPMC_ERR_INVALID_ARG,
        MPMC_ERR_INVALID_ARG,
        MPMC_ERR_INVALID_ARG,
        MPMC_ERR_INVALID_ARG,
        MPMC_ERR_INVALID_ARG,
        MPMC_ERR_INVALID_ARG,
        MPMC_ERR_INVALID_ARG,
        MPMC_ERR_INVALID_ARG,
        MPMC_ERR_INVALID_ARG,
        MPMC_ERR_INVALID_ARG,
        MPMC_ERR_INVALID_ARG,
        MPMC_ERR_INVALID_ARG,
        MPMC_ERR_WOULD_BLOCK,
    };
    for (size_t index = 0u; index < ERROR_CASES; ++index) {
        if (shm[index].name == NULL || msg[index].name == NULL) {
            CHECK(false);
            continue;
        }
        if (shm[index].status != expected[index] || msg[index].status != expected[index]) {
            fprintf(
                stderr,
                "caso \"%s\": shm=%d msg=%d esperado=%d\n",
                shm[index].name,
                (int)shm[index].status,
                (int)msg[index].status,
                (int)expected[index]
            );
        }
        CHECK(shm[index].status == expected[index]);
        CHECK(msg[index].status == expected[index]);
        CHECK(shm[index].status == msg[index].status);
        if (expected[index] != MPMC_ERR_WOULD_BLOCK) {
            /* Rejeições locais não têm causa de sistema. */
            CHECK(shm[index].system_errno == 0 && msg[index].system_errno == 0);
        }
    }
    /* O errno de "vazio" é diagnóstico da syscall de cada paradigma. */
    CHECK(shm[ERROR_CASES - 1u].system_errno == EAGAIN);
    CHECK(msg[ERROR_CASES - 1u].system_errno == ENOMSG);
}

/* ------------------------------------------------------------------------- */
/* C4: worker_close local, destroy remove                                    */
/* ------------------------------------------------------------------------- */

typedef struct {
    int msq[8];
    int shm[8];
    int sem[8];
    size_t msq_count;
    size_t segment_count;
} resource_ids_t;

static resource_ids_t capture_ids(ipc_context_t *context) {
    resource_ids_t ids = {0};
    if (context->mode == IPC_MESSAGE_QUEUE) {
        msg_backend_t *backend = context->message_backend;
        for (size_t index = 0u; index < backend->producer_count && ids.msq_count < 6u; ++index) {
            ids.msq[ids.msq_count++] = backend->data_queues[index];
        }
        ids.msq[ids.msq_count++] = backend->event_queue;
        ids.msq[ids.msq_count++] = backend->ack_queue;
    } else {
        struct shm_backend *backend = context->shared_backend;
        for (
            size_t index = 0u; index < backend->producer_count && ids.segment_count < 8u; ++index
        ) {
            ids.shm[ids.segment_count] = backend->segments[index].shmid;
            ids.sem[ids.segment_count] = backend->segments[index].semid;
            ++ids.segment_count;
        }
    }
    return ids;
}

static bool msq_exists(int id) {
    struct msqid_ds info;
    return msgctl(id, IPC_STAT, &info) == 0;
}

static bool shm_exists(int id) {
    struct shmid_ds info;
    return shmctl(id, IPC_STAT, &info) == 0;
}

static bool sem_exists(int id) {
    return semctl(id, 0, GETVAL) >= 0;
}

static size_t count_existing(const resource_ids_t *ids) {
    size_t existing = 0u;
    for (size_t index = 0u; index < ids->msq_count; ++index)
        existing += msq_exists(ids->msq[index]);
    for (size_t index = 0u; index < ids->segment_count; ++index) {
        existing += shm_exists(ids->shm[index]);
        existing += sem_exists(ids->sem[index]);
    }
    return existing;
}

static size_t total_resources(const resource_ids_t *ids) {
    return ids->msq_count + 2u * ids->segment_count;
}

static void test_close_is_local_destroy_removes(ipc_mode_t mode) {
    ipc_context_t *context = NULL;
    CHECK(ipc_init(&context, 2u, mode).status == MPMC_OK);
    if (context == NULL) return;
    live = context;
    const resource_ids_t ids = capture_ids(context);
    const size_t total = total_resources(&ids);
    CHECK(total > 0u && count_existing(&ids) == total);

    /* Dois filhos, em sequência: o fechamento do primeiro não pode afetar o segundo. */
    for (unsigned round = 0u; round < 2u; ++round) {
        pid_t child = fork();
        if (child == 0) {
            (void)signal(SIGALRM, SIG_DFL);
            message_t message = {
                .sender_pid = getpid(), .sequence = round + 1u, .operation_id = 5u};
            mpmc_result_t sent = ipc_send(context, 1u, &message);
            ipc_worker_close(context);
            _exit(sent.status == MPMC_OK ? 0 : 1);
        }
        CHECK(child > 0);
        kids[0] = child;
        CHECK(reap(0u, context) == 0);
        /* Depois do worker_close, nenhum recurso global desapareceu e o dono ainda opera. */
        CHECK(count_existing(&ids) == total);
    }
    message_t first;
    message_t second;
    CHECK(ipc_receive(context, 1u, &first, false).status == MPMC_OK && first.sequence == 1u);
    CHECK(ipc_receive(context, 1u, &second, false).status == MPMC_OK && second.sequence == 2u);
    CHECK(count_existing(&ids) == total);

    live = NULL;
    ipc_destroy(context);
    CHECK(count_existing(&ids) == 0u);
}

/* ------------------------------------------------------------------------- */
/* C3: rollback de falha parcial real (limite do kernel esgotado)            */
/* ------------------------------------------------------------------------- */

/** @brief Lê um inteiro de um arquivo do /proc; -1 se indisponível. */
static long read_long(const char *path) {
    FILE *file = fopen(path, "r");
    if (file == NULL) return -1;
    long value = -1;
    if (fscanf(file, "%ld", &value) != 1) value = -1;
    fclose(file);
    return value;
}

/**
 * @brief Conta os objetos System V de um tipo existentes no sistema.
 * @param kind 'm' (memória compartilhada), 's' (semáforos) ou 'q' (filas).
 * @return Quantidade, ou -1 se não foi possível contar.
 * @details No Linux lê /proc/sysvipc/{shm,sem,msg}. Nos demais sistemas (macOS) conta
 * as linhas de `ipcs`, que listam cada objeto com o tipo (m, s ou q) na primeira coluna.
 */
static long count_objects(char kind) {
    const char *table = kind == 'm' ? "/proc/sysvipc/shm"
        : kind == 's'               ? "/proc/sysvipc/sem"
                                    : "/proc/sysvipc/msg";
    FILE *file = fopen(table, "r");
    if (file != NULL) {
        long lines = -1; /* a primeira linha é o cabeçalho */
        int ch;
        while ((ch = fgetc(file)) != EOF) {
            if (ch == '\n') ++lines;
        }
        fclose(file);
        return lines;
    }
    char command[32];
    snprintf(command, sizeof(command), "ipcs -%c 2>/dev/null", kind);
    file = popen(command, "r");
    if (file == NULL) return -1;
    long objects = 0;
    char line[256];
    bool any_output = false;
    while (fgets(line, sizeof(line), file) != NULL) {
        any_output = true;
        if (line[0] == kind && line[1] == ' ') ++objects;
    }
    return pclose(file) == 0 && any_output ? objects : -1;
}

/**
 * @brief Força uma falha parcial real de ipc_init() e confere o rollback.
 * @details Pede mais estruturas do que o kernel permite, de modo que a criação falha
 * depois de vários recursos já criados. O limite é lido do /proc quando existe; sem ele,
 * escala-se o pedido (128, 1024, 8192, 40000) até o kernel recusar, destruindo o que for
 * aceito no caminho. Os valores baixos bastam no macOS (shmmni 32, msgmni 40).
 */
static void test_partial_init_rolls_back(ipc_mode_t mode) {
    const bool shm = mode == IPC_SHARED_MEMORY;
    const long limit = read_long(shm ? "/proc/sys/kernel/shmmni" : "/proc/sys/kernel/msgmni");
    const size_t ladder[] = {128u, 1024u, 8192u, 40000u};
    const size_t known[] = {limit > 0 ? (size_t)limit + 8u : 0u};
    const size_t *candidates = limit > 0 ? known : ladder;
    const size_t candidate_count = limit > 0 ? 1u : sizeof(ladder) / sizeof(ladder[0]);

    for (size_t index = 0u; index < candidate_count; ++index) {
        const size_t producers = candidates[index];
        if (producers > 60000u) break;
        const long shm_before = count_objects('m');
        const long sem_before = count_objects('s');
        const long msq_before = count_objects('q');

        ipc_context_t *context = (ipc_context_t *)0x1;
        mpmc_result_t result = ipc_init(&context, producers, mode);
        if (result.status == MPMC_OK) {
            ipc_destroy(context); /* o kernel aceitou; tenta um pedido maior */
            continue;
        }
        /* A falha acontece depois de muitos recursos já criados. */
        CHECK(result.status == MPMC_ERR_SYS || result.status == MPMC_ERR_SHUTDOWN);
        CHECK(result.system_errno != 0);
        CHECK(context == NULL);
        if (shm_before >= 0 && sem_before >= 0 && msq_before >= 0) {
            CHECK(count_objects('m') == shm_before);
            CHECK(count_objects('s') == sem_before);
            CHECK(count_objects('q') == msq_before);
        } else {
            printf(
                "  [%s] não foi possível contar objetos IPC; remanescentes não verificados\n",
                mode_name(mode)
            );
        }
        return;
    }
    printf("  [%s] o kernel aceitou todos os pedidos; rollback real dispensado\n", mode_name(mode));
}

int main(void) {
    struct sigaction action = {.sa_handler = on_alarm};
    sigemptyset(&action.sa_mask);
    if (sigaction(SIGALRM, &action, NULL) == -1) {
        perror("sigaction");
        return 1;
    }
    alarm(120u);

    test_workers_have_no_paradigm_branches();
    test_same_worker_code_same_outcome();
    test_error_parity();
    test_close_is_local_destroy_removes(IPC_SHARED_MEMORY);
    test_close_is_local_destroy_removes(IPC_MESSAGE_QUEUE);
    test_partial_init_rolls_back(IPC_SHARED_MEMORY);
    test_partial_init_rolls_back(IPC_MESSAGE_QUEUE);

    alarm(0u);
    printf("%u verificações de paridade de transporte, %u falhas\n", checks, failures);
    if (failures != 0u) return 1;
    puts("Transport parity tests passed.");
    return 0;
}
