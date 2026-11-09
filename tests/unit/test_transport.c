/**
 * @file test_transport.c
 * @brief Audita a fachada transport.c com backends substitutos.
 * @ingroup tests
 * @details Subtarefa #46 de #16. O executável é ligado a transport.c e a implementações
 * substitutas de msg_backend_*() e shm_backend_*() definidas neste arquivo: nenhum backend
 * real, nenhuma primitiva System V funcional e nenhum processo filho participam do teste.
 * Os substitutos registram cada chamada (backend, índice, ponteiros, flags e identificadores)
 * e devolvem o resultado configurado pelo teste.
 *
 * Verifica, para os dois modos:
 * - despacho: cada operação chega ao backend do modo, com os argumentos exatos, e nunca ao
 *   outro backend;
 * - propagação: status e system_errno dos backends, e os dados de saída, atravessam a
 *   fachada sem alteração;
 * - argumentos inválidos: contexto nulo, modo inválido, saída nula e contagem zero, conforme
 *   documentado em transport.h;
 * - falha parcial de ipc_init(): alocação e criação do backend com falha injetada, *out
 *   nulo, sem destruir um backend cuja criação falhou e sem vazar a alocação da fachada;
 * - ipc_worker_close() e ipc_destroy(): rotina correta do backend, chamada antes de liberar o
 *   contexto, sem que o fechamento do filho remova recursos globais.
 *
 * transport.c é compilado com calloc() e free() redirecionados (-Dcalloc=mpmc_test_calloc
 * -Dfree=mpmc_test_free) para injetar falhas de alocação e detectar vazamento. Espiões de
 * msgget(), msgsnd(), msgrcv(), shmget(), semget() e semop() garantem que a fachada não usa
 * System V diretamente.
 *
 * Complementa test_transport_contract.c, que exercita os backends reais. Retorna 0 se todas
 * as verificações passarem; 1 caso contrário.
 */
#include <errno.h>
#include <limits.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <sys/sem.h>
#include <sys/shm.h>
#include <sys/types.h>

#include "mpmc/ipc/msg_backend.h"
#include "mpmc/ipc/shm_backend.h"
#include "mpmc/ipc/transport.h"

/** @brief Quantidade de verificações executadas nesta suíte. */
static unsigned g_checks;
/** @brief Quantidade de verificações que registraram falha nesta suíte. */
static unsigned g_failures;

/**
 * @brief Registra uma verificação e informa a falha sem interromper a suíte.
 * @param condition Expressão lógica avaliada uma única vez.
 * @param ... Formato e argumentos da mensagem exibida na falha.
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
/* Instrumentação: alocação, System V e registro de chamadas          */
/* ------------------------------------------------------------------ */

/** @brief Chamadas a calloc() feitas por transport.c. */
static unsigned g_calloc_calls;
/** @brief Chamadas a free() com ponteiro não nulo feitas por transport.c. */
static unsigned g_free_calls;
/** @brief Alocações de transport.c ainda não liberadas. */
static int g_live_allocations;
/** @brief Se diferente de zero, o próximo calloc() falha com este errno. */
static int g_calloc_failure_errno;
/** @brief Chamadas diretas a primitivas System V feitas por qualquer código ligado ao teste. */
static unsigned g_sysv_calls;

void *mpmc_test_calloc(size_t count, size_t size);
void mpmc_test_free(void *pointer);

/**
 * @brief Substitui calloc() em transport.c, com falha injetável.
 * @param count Quantidade de elementos.
 * @param size Tamanho de cada elemento.
 * @return Bloco zerado, ou NULL com errno igual a g_calloc_failure_errno.
 */
void *mpmc_test_calloc(size_t count, size_t size) {
    ++g_calloc_calls;
    if (g_calloc_failure_errno != 0) {
        errno = g_calloc_failure_errno;
        return NULL;
    }
    void *block = calloc(count, size);
    if (block != NULL) {
        ++g_live_allocations;
    }
    return block;
}

/**
 * @brief Substitui free() em transport.c, contando as liberações.
 * @param pointer Bloco a liberar; NULL é aceito e não é contado.
 */
void mpmc_test_free(void *pointer) {
    if (pointer != NULL) {
        ++g_free_calls;
        --g_live_allocations;
    }
    free(pointer);
}

int msgget(key_t key, int flags) {
    (void)key;
    (void)flags;
    ++g_sysv_calls;
    errno = EPERM;
    return -1;
}

int msgsnd(int queue, const void *message, size_t size, int flags) {
    (void)queue;
    (void)message;
    (void)size;
    (void)flags;
    ++g_sysv_calls;
    errno = EPERM;
    return -1;
}

ssize_t msgrcv(int queue, void *message, size_t size, long type, int flags) {
    (void)queue;
    (void)message;
    (void)size;
    (void)type;
    (void)flags;
    ++g_sysv_calls;
    errno = EPERM;
    return -1;
}

int shmget(key_t key, size_t size, int flags) {
    (void)key;
    (void)size;
    (void)flags;
    ++g_sysv_calls;
    errno = EPERM;
    return -1;
}

int semget(key_t key, int count, int flags) {
    (void)key;
    (void)count;
    (void)flags;
    ++g_sysv_calls;
    errno = EPERM;
    return -1;
}

int semop(int set, struct sembuf *operations, size_t count) {
    (void)set;
    (void)operations;
    (void)count;
    ++g_sysv_calls;
    errno = EPERM;
    return -1;
}

/** @brief Operações de backend que os substitutos registram. */
typedef enum {
    CALL_NONE = 0,
    CALL_MSG_CREATE,
    CALL_MSG_SEND,
    CALL_MSG_RECEIVE,
    CALL_MSG_ACK_BEGIN,
    CALL_MSG_ACK_WAIT,
    CALL_MSG_ACK_END,
    CALL_MSG_ACK_ABANDON,
    CALL_MSG_POLL,
    CALL_MSG_COUNT,
    CALL_MSG_CLOSE,
    CALL_MSG_DESTROY,
    CALL_SHM_CREATE,
    CALL_SHM_SEND,
    CALL_SHM_RECEIVE,
    CALL_SHM_ACK_BEGIN,
    CALL_SHM_ACK_WAIT,
    CALL_SHM_ACK_END,
    CALL_SHM_COUNT,
    CALL_SHM_CLOSE,
    CALL_SHM_DESTROY
} call_kind_t;

/** @brief Uma chamada recebida por um backend substituto. */
typedef struct {
    call_kind_t kind;      /**< Rotina chamada. */
    const void *backend;   /**< Backend recebido; NULL em create. */
    size_t index;          /**< producer_id, ou producer_count em create. */
    const void *message;   /**< Mensagem recebida por send. */
    const void *out;       /**< Ponteiro de saída recebido. */
    bool blocking;         /**< Flag recebida por receive. */
    uint64_t operation_id; /**< Operação de ACK. */
    unsigned expected;     /**< Total esperado em ack_begin. */
    const void *reporter;  /**< Reporter recebido pelo backend shm. */
    unsigned frees_before; /**< Liberações de contexto já feitas quando a rotina foi chamada. */
} call_t;

/** @brief Capacidade do registro de chamadas. */
#define MAX_CALLS 32u

static call_t g_calls[MAX_CALLS];
static unsigned g_call_count;

/** @brief Resultado devolvido pelos backends substitutos. */
static mpmc_result_t g_result;
/** @brief Se verdadeiro, create deixa um ponteiro não nulo em *out mesmo ao falhar. */
static bool g_dirty_out_on_failure;

/** @brief Backend de filas substituto; o endereço identifica a instância. */
struct msg_backend {
    int tag;
};

/** @brief Backend de memória compartilhada substituto; o endereço identifica a instância. */
struct shm_backend {
    int tag;
};

static struct msg_backend g_msg_object;
static struct shm_backend g_shm_object;

/** @brief Mensagem que o teste envia; a fachada não deve alterá-la nem copiá-la. */
static message_t g_message_in = {.sender_pid = 1234,
    .sequence = 9u,
    .sent_at = {.tv_sec = 5, .tv_nsec = 6},
    .operation_id = 77u};
/** @brief Mensagem que os backends escrevem em receive ao terem sucesso. */
static const message_t g_receive_payload = {.sender_pid = 4321,
    .sequence = 11u,
    .sent_at = {.tv_sec = 7, .tv_nsec = 8},
    .operation_id = 88u};
/** @brief Destino de ipc_receive(). */
static message_t g_message_out;
/** @brief Total que os backends escrevem em consumed_count ao terem sucesso. */
#define COUNT_PAYLOAD UINT64_C(0x1122334455667788)
/** @brief Destino de ipc_consumed_count(). */
static uint64_t g_count_out;

/**
 * @brief Anota uma chamada de backend.
 * @param kind Rotina chamada.
 * @param backend Backend recebido.
 * @return Registro a ser completado pelo substituto.
 */
static call_t *record(call_kind_t kind, const void *backend) {
    if (g_call_count >= MAX_CALLS) {
        fputs("test_transport: registro de chamadas esgotado\n", stderr);
        abort();
    }
    call_t *call = &g_calls[g_call_count++];
    *call = (call_t){.kind = kind, .backend = backend, .frees_before = g_free_calls};
    return call;
}

/* ------------------------------------------------------------------ */
/* Backend de filas substituto                                         */
/* ------------------------------------------------------------------ */

mpmc_result_t msg_backend_create(msg_backend_t **out, size_t producer_count) {
    call_t *call = record(CALL_MSG_CREATE, NULL);
    call->index = producer_count;
    call->out = out;
    if (g_result.status == MPMC_OK || g_dirty_out_on_failure) {
        *out = &g_msg_object;
    }
    return g_result;
}

mpmc_result_t msg_backend_send(
    msg_backend_t *backend,
    size_t producer_id,
    const message_t *message
) {
    call_t *call = record(CALL_MSG_SEND, backend);
    call->index = producer_id;
    call->message = message;
    return g_result;
}

mpmc_result_t msg_backend_receive(
    msg_backend_t *backend,
    size_t producer_id,
    message_t *out,
    bool blocking
) {
    call_t *call = record(CALL_MSG_RECEIVE, backend);
    call->index = producer_id;
    call->out = out;
    call->blocking = blocking;
    if (g_result.status == MPMC_OK) {
        *out = g_receive_payload;
    }
    return g_result;
}

mpmc_result_t msg_backend_ack_begin(
    msg_backend_t *backend,
    uint64_t operation_id,
    unsigned expected
) {
    call_t *call = record(CALL_MSG_ACK_BEGIN, backend);
    call->operation_id = operation_id;
    call->expected = expected;
    return g_result;
}

mpmc_result_t msg_backend_ack_wait(msg_backend_t *backend, uint64_t operation_id) {
    call_t *call = record(CALL_MSG_ACK_WAIT, backend);
    call->operation_id = operation_id;
    return g_result;
}

mpmc_result_t msg_backend_ack_end(msg_backend_t *backend, uint64_t operation_id) {
    call_t *call = record(CALL_MSG_ACK_END, backend);
    call->operation_id = operation_id;
    return g_result;
}

mpmc_result_t msg_backend_ack_abandon(msg_backend_t *backend, uint64_t operation_id) {
    call_t *call = record(CALL_MSG_ACK_ABANDON, backend);
    call->operation_id = operation_id;
    return g_result;
}

mpmc_result_t msg_backend_poll_events(msg_backend_t *backend) {
    (void)record(CALL_MSG_POLL, backend);
    return g_result;
}

mpmc_result_t msg_backend_consumed_count(
    msg_backend_t *backend,
    size_t producer_id,
    uint64_t *out_count
) {
    call_t *call = record(CALL_MSG_COUNT, backend);
    call->index = producer_id;
    call->out = out_count;
    if (g_result.status == MPMC_OK) {
        *out_count = COUNT_PAYLOAD;
    }
    return g_result;
}

void msg_backend_worker_close(msg_backend_t *backend) {
    (void)record(CALL_MSG_CLOSE, backend);
}

void msg_backend_destroy(msg_backend_t *backend) {
    (void)record(CALL_MSG_DESTROY, backend);
}

/* ------------------------------------------------------------------ */
/* Backend de memória compartilhada substituto                         */
/* ------------------------------------------------------------------ */

mpmc_result_t shm_backend_create(
    shm_backend_t **out,
    size_t producer_count,
    const shm_cleanup_reporter_t *reporter
) {
    call_t *call = record(CALL_SHM_CREATE, NULL);
    call->index = producer_count;
    call->out = out;
    call->reporter = reporter;
    if (g_result.status == MPMC_OK || g_dirty_out_on_failure) {
        *out = &g_shm_object;
    }
    return g_result;
}

mpmc_result_t shm_backend_send(
    shm_backend_t *backend,
    size_t producer_id,
    const message_t *message
) {
    call_t *call = record(CALL_SHM_SEND, backend);
    call->index = producer_id;
    call->message = message;
    return g_result;
}

mpmc_result_t shm_backend_receive(
    shm_backend_t *backend,
    size_t producer_id,
    message_t *out,
    bool blocking
) {
    call_t *call = record(CALL_SHM_RECEIVE, backend);
    call->index = producer_id;
    call->out = out;
    call->blocking = blocking;
    if (g_result.status == MPMC_OK) {
        *out = g_receive_payload;
    }
    return g_result;
}

mpmc_result_t shm_backend_ack_begin(
    shm_backend_t *backend,
    uint64_t operation_id,
    unsigned expected
) {
    call_t *call = record(CALL_SHM_ACK_BEGIN, backend);
    call->operation_id = operation_id;
    call->expected = expected;
    return g_result;
}

mpmc_result_t shm_backend_ack_wait(shm_backend_t *backend, uint64_t operation_id) {
    call_t *call = record(CALL_SHM_ACK_WAIT, backend);
    call->operation_id = operation_id;
    return g_result;
}

mpmc_result_t shm_backend_ack_end(shm_backend_t *backend, uint64_t operation_id) {
    call_t *call = record(CALL_SHM_ACK_END, backend);
    call->operation_id = operation_id;
    return g_result;
}

mpmc_result_t shm_backend_consumed_count(
    shm_backend_t *backend,
    size_t producer_id,
    uint64_t *out_count
) {
    call_t *call = record(CALL_SHM_COUNT, backend);
    call->index = producer_id;
    call->out = out_count;
    if (g_result.status == MPMC_OK) {
        *out_count = COUNT_PAYLOAD;
    }
    return g_result;
}

mpmc_result_t shm_backend_worker_close(
    shm_backend_t *backend,
    const shm_cleanup_reporter_t *reporter
) {
    call_t *call = record(CALL_SHM_CLOSE, backend);
    call->reporter = reporter;
    return g_result;
}

mpmc_result_t shm_backend_destroy(shm_backend_t *backend, const shm_cleanup_reporter_t *reporter) {
    call_t *call = record(CALL_SHM_DESTROY, backend);
    call->reporter = reporter;
    return g_result;
}

/* ------------------------------------------------------------------ */
/* Utilitários                                                        */
/* ------------------------------------------------------------------ */

/** @brief Byte cujo endereço marca um *out que a fachada não deve tocar (o contexto é opaco). */
static char g_marker_byte;
/** @brief Ponteiro de contexto que nunca é devolvido por ipc_init(); detecta escrita em *out. */
#define MARKER ((ipc_context_t *)&g_marker_byte)

#define RESULT_OK ((mpmc_result_t){.status = MPMC_OK, .system_errno = 0})

/** @brief Resultados de backend usados para verificar a propagação sem alteração. */
static const mpmc_result_t k_results[] = {
    {MPMC_OK, 0},
    {MPMC_ERR_INVALID_ARG, 0},
    {MPMC_ERR_INVALID_INDEX, 0},
    {MPMC_ERR_WOULD_BLOCK, ENOMSG},
    {MPMC_ERR_NO_SPACE, ENOSPC},
    {MPMC_ERR_INTERRUPTED, EINTR},
    {MPMC_ERR_SHUTDOWN, 0},
    {MPMC_ERR_SYS, EIDRM},
    {MPMC_ERR_SYS, E2BIG},
    {MPMC_ERR_SYS, 0},
    {MPMC_ERR_STATE, 0},
};

#define RESULT_COUNT (sizeof k_results / sizeof k_results[0])

/** @brief Modos exercitados por cada caso. */
static const ipc_mode_t k_modes[] = {IPC_SHARED_MEMORY, IPC_MESSAGE_QUEUE};

/**
 * @brief Nome legível do modo.
 * @param mode Paradigma.
 * @return Texto fixo para as mensagens de falha.
 */
static const char *mode_name(ipc_mode_t mode) {
    return mode == IPC_MESSAGE_QUEUE ? "modo 2 (filas)" : "modo 1 (memória compartilhada)";
}

/**
 * @brief Backend substituto esperado para um modo.
 * @param mode Paradigma.
 * @return Endereço da instância substituta correspondente.
 */
static const void *backend_of(ipc_mode_t mode) {
    return mode == IPC_MESSAGE_QUEUE ? (const void *)&g_msg_object : (const void *)&g_shm_object;
}

/**
 * @brief Informa se uma rotina pertence ao backend de filas.
 * @param kind Rotina registrada.
 * @return true para as rotinas CALL_MSG_*.
 */
static bool is_msg_call(call_kind_t kind) {
    return kind >= CALL_MSG_CREATE && kind <= CALL_MSG_DESTROY;
}

/**
 * @brief Informa se uma rotina pertence ao backend de memória compartilhada.
 * @param kind Rotina registrada.
 * @return true para as rotinas CALL_SHM_*.
 */
static bool is_shm_call(call_kind_t kind) {
    return kind >= CALL_SHM_CREATE && kind <= CALL_SHM_DESTROY;
}

/** @brief Restaura o estado dos substitutos e dos contadores antes de cada caso. */
static void reset_state(void) {
    memset(g_calls, 0, sizeof g_calls);
    g_call_count = 0u;
    g_result = RESULT_OK;
    g_dirty_out_on_failure = false;
    g_calloc_calls = 0u;
    g_free_calls = 0u;
    g_live_allocations = 0;
    g_calloc_failure_errno = 0;
    g_sysv_calls = 0u;
    memset(&g_message_out, 0, sizeof g_message_out);
    g_count_out = 0u;
}

/**
 * @brief Compara dois resultados campo a campo.
 * @param actual Resultado obtido.
 * @param expected Resultado esperado.
 * @return true se status e system_errno coincidirem.
 */
static bool same_result(mpmc_result_t actual, mpmc_result_t expected) {
    return actual.status == expected.status && actual.system_errno == expected.system_errno;
}

/**
 * @brief Cria um contexto com backend substituto bem-sucedido e limpa o registro.
 * @param mode Paradigma.
 * @param producers Quantidade de estruturas lógicas.
 * @return Contexto válido; aborta o teste se a criação falhar.
 */
static ipc_context_t *make_context(ipc_mode_t mode, size_t producers) {
    ipc_context_t *context = NULL;
    g_result = RESULT_OK;
    mpmc_result_t result = ipc_init(&context, producers, mode);
    if (!same_result(result, RESULT_OK) || context == NULL) {
        fprintf(stderr, "test_transport: ipc_init() preparatório falhou\n");
        abort();
    }
    g_call_count = 0u;
    return context;
}

/**
 * @brief Verifica que nada vazou nem tocou System V ao final de um caso.
 * @param where Descrição do caso.
 */
static void expect_clean(const char *where) {
    CHECK(
        g_live_allocations == 0,
        "%s: %d alocação(ões) da fachada vazada(s)",
        where,
        g_live_allocations
    );
    CHECK(
        g_sysv_calls == 0u,
        "%s: a fachada chamou %u primitiva(s) System V diretamente",
        where,
        g_sysv_calls
    );
}

/* ------------------------------------------------------------------ */
/* Operações da fachada, de forma uniforme                            */
/* ------------------------------------------------------------------ */

typedef enum {
    OP_SEND,
    OP_RECEIVE_BLOCKING,
    OP_RECEIVE_NONBLOCKING,
    OP_ACK_BEGIN,
    OP_ACK_WAIT,
    OP_ACK_END,
    OP_ACK_ABANDON,
    OP_POLL,
    OP_CONSUMED,
    OP_COUNT
} op_t;

static const char *const k_op_names[OP_COUNT] = {
    "ipc_send",
    "ipc_receive(blocking)",
    "ipc_receive(nonblocking)",
    "ipc_ack_begin",
    "ipc_ack_wait",
    "ipc_ack_end",
    "ipc_ack_abandon",
    "ipc_poll_events",
    "ipc_consumed_count",
};

#define SEND_INDEX 2u
#define RECEIVE_INDEX 3u
#define COUNT_INDEX 1u
#define ACK_OPERATION UINT64_C(77)
#define ACK_EXPECTED 5u

/**
 * @brief Chama a operação da fachada com argumentos fixos e reconhecíveis.
 * @param op Operação.
 * @param context Contexto, possivelmente NULL.
 * @return Resultado devolvido pela fachada.
 */
static mpmc_result_t invoke(op_t op, ipc_context_t *context) {
    switch (op) {
        case OP_SEND:
            return ipc_send(context, SEND_INDEX, &g_message_in);
        case OP_RECEIVE_BLOCKING:
            return ipc_receive(context, RECEIVE_INDEX, &g_message_out, true);
        case OP_RECEIVE_NONBLOCKING:
            return ipc_receive(context, RECEIVE_INDEX, &g_message_out, false);
        case OP_ACK_BEGIN:
            return ipc_ack_begin(context, ACK_OPERATION, ACK_EXPECTED);
        case OP_ACK_WAIT:
            return ipc_ack_wait(context, ACK_OPERATION);
        case OP_ACK_END:
            return ipc_ack_end(context, ACK_OPERATION);
        case OP_ACK_ABANDON:
            return ipc_ack_abandon(context, ACK_OPERATION);
        case OP_POLL:
            return ipc_poll_events(context);
        case OP_CONSUMED:
            return ipc_consumed_count(context, COUNT_INDEX, &g_count_out);
        case OP_COUNT:
            break;
    }
    return (mpmc_result_t){.status = MPMC_ERR_STATE};
}

/**
 * @brief Rotina de backend esperada para uma operação da fachada.
 * @param op Operação da fachada.
 * @param mode Paradigma.
 * @return Rotina esperada, ou CALL_NONE quando a fachada não delega ao backend.
 */
static call_kind_t expected_kind(op_t op, ipc_mode_t mode) {
    const bool queues = mode == IPC_MESSAGE_QUEUE;
    switch (op) {
        case OP_SEND:
            return queues ? CALL_MSG_SEND : CALL_SHM_SEND;
        case OP_RECEIVE_BLOCKING:
        case OP_RECEIVE_NONBLOCKING:
            return queues ? CALL_MSG_RECEIVE : CALL_SHM_RECEIVE;
        case OP_ACK_BEGIN:
            return queues ? CALL_MSG_ACK_BEGIN : CALL_SHM_ACK_BEGIN;
        case OP_ACK_WAIT:
            return queues ? CALL_MSG_ACK_WAIT : CALL_SHM_ACK_WAIT;
        case OP_ACK_END:
            return queues ? CALL_MSG_ACK_END : CALL_SHM_ACK_END;
        case OP_ACK_ABANDON:
            return queues ? CALL_MSG_ACK_ABANDON : CALL_NONE;
        case OP_POLL:
            return queues ? CALL_MSG_POLL : CALL_NONE;
        case OP_CONSUMED:
            return queues ? CALL_MSG_COUNT : CALL_SHM_COUNT;
        case OP_COUNT:
            break;
    }
    return CALL_NONE;
}

/**
 * @brief Confere que o backend recebeu exatamente os argumentos que a fachada deveria repassar.
 * @param op Operação da fachada.
 * @param mode Paradigma.
 * @param call Chamada registrada.
 */
static void check_forwarded_arguments(op_t op, ipc_mode_t mode, const call_t *call) {
    const char *name = k_op_names[op];
    CHECK(
        call->backend == backend_of(mode),
        "%s, %s: backend repassado diferente do criado",
        name,
        mode_name(mode)
    );
    switch (op) {
        case OP_SEND:
            CHECK(
                call->index == SEND_INDEX,
                "%s: índice %zu repassado em vez de %u",
                name,
                call->index,
                SEND_INDEX
            );
            CHECK(
                call->message == &g_message_in, "%s: a mensagem foi copiada ou substituída", name
            );
            break;
        case OP_RECEIVE_BLOCKING:
        case OP_RECEIVE_NONBLOCKING:
            CHECK(
                call->index == RECEIVE_INDEX,
                "%s: índice %zu repassado em vez de %u",
                name,
                call->index,
                RECEIVE_INDEX
            );
            CHECK(call->out == &g_message_out, "%s: o destino da mensagem foi substituído", name);
            CHECK(
                call->blocking == (op == OP_RECEIVE_BLOCKING),
                "%s: flag blocking repassada com valor trocado",
                name
            );
            break;
        case OP_ACK_BEGIN:
            CHECK(call->operation_id == ACK_OPERATION, "%s: operation_id repassado errado", name);
            CHECK(call->expected == ACK_EXPECTED, "%s: expected repassado errado", name);
            break;
        case OP_ACK_WAIT:
        case OP_ACK_END:
        case OP_ACK_ABANDON:
            CHECK(call->operation_id == ACK_OPERATION, "%s: operation_id repassado errado", name);
            break;
        case OP_POLL:
            break;
        case OP_CONSUMED:
            CHECK(
                call->index == COUNT_INDEX,
                "%s: índice %zu repassado em vez de %u",
                name,
                call->index,
                COUNT_INDEX
            );
            CHECK(call->out == &g_count_out, "%s: o destino do total foi substituído", name);
            break;
        case OP_COUNT:
            break;
    }
}

/* ------------------------------------------------------------------ */
/* Casos                                                              */
/* ------------------------------------------------------------------ */

/** @brief Os espiões de System V e de alocação funcionam; sem isto os demais passariam em vão. */
static void test_instrumentation_works(void) {
    reset_state();
    CHECK(msgget(0, 0) == -1 && g_sysv_calls == 1u, "espião de msgget não registrou a chamada");
    CHECK(
        msgsnd(0, NULL, 0, 0) == -1 && g_sysv_calls == 2u,
        "espião de msgsnd não registrou a chamada"
    );
    CHECK(
        msgrcv(0, NULL, 0, 0, 0) == -1 && g_sysv_calls == 3u,
        "espião de msgrcv não registrou a chamada"
    );
    CHECK(shmget(0, 0, 0) == -1 && g_sysv_calls == 4u, "espião de shmget não registrou a chamada");
    CHECK(semget(0, 0, 0) == -1 && g_sysv_calls == 5u, "espião de semget não registrou a chamada");
    CHECK(semop(0, NULL, 0) == -1 && g_sysv_calls == 6u, "espião de semop não registrou a chamada");

    reset_state();
    void *block = mpmc_test_calloc(1u, 8u);
    CHECK(
        block != NULL && g_live_allocations == 1 && g_calloc_calls == 1u,
        "calloc de teste não contou a alocação"
    );
    mpmc_test_free(block);
    CHECK(g_live_allocations == 0 && g_free_calls == 1u, "free de teste não contou a liberação");
    mpmc_test_free(NULL);
    CHECK(g_free_calls == 1u, "free(NULL) não deve ser contado");
    g_calloc_failure_errno = ENOMEM;
    errno = 0;
    CHECK(
        mpmc_test_calloc(1u, 8u) == NULL && errno == ENOMEM,
        "falha injetada de calloc não foi aplicada"
    );
    reset_state();
}

/** @brief Cada operação chega ao backend do modo, com os argumentos exatos, e a nenhum outro. */
static void test_dispatch_goes_to_selected_backend(void) {
    for (size_t m = 0; m < sizeof k_modes / sizeof k_modes[0]; ++m) {
        const ipc_mode_t mode = k_modes[m];
        reset_state();
        ipc_context_t *context = make_context(mode, 3u);

        for (op_t op = 0; op < OP_COUNT; ++op) {
            g_call_count = 0u;
            g_result = RESULT_OK;
            const mpmc_result_t result = invoke(op, context);
            const call_kind_t want = expected_kind(op, mode);

            CHECK(
                same_result(result, RESULT_OK),
                "%s, %s: resultado %d/%d em vez de OK",
                k_op_names[op],
                mode_name(mode),
                (int)result.status,
                result.system_errno
            );
            if (want == CALL_NONE) {
                CHECK(
                    g_call_count == 0u,
                    "%s, %s: a fachada não deveria chamar nenhum backend (%u chamada(s))",
                    k_op_names[op],
                    mode_name(mode),
                    g_call_count
                );
                continue;
            }
            CHECK(
                g_call_count == 1u,
                "%s, %s: %u chamada(s) ao backend em vez de uma",
                k_op_names[op],
                mode_name(mode),
                g_call_count
            );
            if (g_call_count != 1u) {
                continue;
            }
            CHECK(
                g_calls[0].kind == want,
                "%s, %s: rotina de backend %d em vez de %d",
                k_op_names[op],
                mode_name(mode),
                (int)g_calls[0].kind,
                (int)want
            );
            CHECK(
                mode == IPC_MESSAGE_QUEUE ? !is_shm_call(g_calls[0].kind)
                                          : !is_msg_call(g_calls[0].kind),
                "%s, %s: a chamada foi para o backend do outro paradigma",
                k_op_names[op],
                mode_name(mode)
            );
            check_forwarded_arguments(op, mode, &g_calls[0]);
        }

        ipc_destroy(context);
        expect_clean(mode_name(mode));
    }
}

/** @brief ipc_poll_events() no modo 1 não consulta nenhum backend, mesmo que eles falhem. */
static void test_poll_events_is_noop_in_shared_memory_mode(void) {
    reset_state();
    ipc_context_t *context = make_context(IPC_SHARED_MEMORY, 2u);
    g_result = (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = EIO};
    const mpmc_result_t result = ipc_poll_events(context);
    CHECK(same_result(result, RESULT_OK), "modo 1: ipc_poll_events() deveria devolver OK");
    CHECK(
        g_call_count == 0u, "modo 1: ipc_poll_events() chamou um backend (%u vez(es))", g_call_count
    );
    ipc_destroy(context);
    expect_clean("poll modo 1");
}

/** @brief Status, errno e dados de saída atravessam a fachada sem alteração. */
static void test_results_and_outputs_are_forwarded(void) {
    for (size_t m = 0; m < sizeof k_modes / sizeof k_modes[0]; ++m) {
        const ipc_mode_t mode = k_modes[m];
        reset_state();
        ipc_context_t *context = make_context(mode, 3u);

        for (op_t op = 0; op < OP_COUNT; ++op) {
            if (expected_kind(op, mode) == CALL_NONE) {
                continue;
            }
            for (size_t r = 0; r < RESULT_COUNT; ++r) {
                const mpmc_result_t want = k_results[r];
                const message_t sentinel_message = {
                    .sender_pid = -1, .sequence = 0xFFFFFFFFu, .operation_id = 5u};
                g_message_out = sentinel_message;
                g_count_out = UINT64_C(0xDEADBEEF);
                g_call_count = 0u;
                g_result = want;

                const mpmc_result_t got = invoke(op, context);

                CHECK(
                    same_result(got, want),
                    "%s, %s: backend devolveu %d/%d, a fachada devolveu %d/%d",
                    k_op_names[op],
                    mode_name(mode),
                    (int)want.status,
                    want.system_errno,
                    (int)got.status,
                    got.system_errno
                );
                CHECK(
                    g_call_count == 1u,
                    "%s, %s: %u chamada(s) ao backend com resultado %d",
                    k_op_names[op],
                    mode_name(mode),
                    g_call_count,
                    (int)want.status
                );

                if (op == OP_RECEIVE_BLOCKING || op == OP_RECEIVE_NONBLOCKING) {
                    if (want.status == MPMC_OK) {
                        CHECK(
                            memcmp(&g_message_out, &g_receive_payload, sizeof g_message_out) == 0,
                            "%s, %s: a mensagem recebida foi alterada pela fachada",
                            k_op_names[op],
                            mode_name(mode)
                        );
                    } else {
                        CHECK(
                            memcmp(&g_message_out, &sentinel_message, sizeof g_message_out) == 0,
                            "%s, %s: a fachada escreveu na saída apesar do erro %d",
                            k_op_names[op],
                            mode_name(mode),
                            (int)want.status
                        );
                    }
                }
                if (op == OP_CONSUMED) {
                    const uint64_t expected_count =
                        want.status == MPMC_OK ? COUNT_PAYLOAD : UINT64_C(0xDEADBEEF);
                    CHECK(
                        g_count_out == expected_count,
                        "%s, %s: total %llu em vez de %llu",
                        k_op_names[op],
                        mode_name(mode),
                        (unsigned long long)g_count_out,
                        (unsigned long long)expected_count
                    );
                }
            }
        }

        g_result = RESULT_OK;
        ipc_destroy(context);
        expect_clean(mode_name(mode));
    }
}

/** @brief Contexto nulo é recusado em toda operação, sem chamar backend. */
static void test_null_context_is_rejected(void) {
    for (op_t op = 0; op < OP_COUNT; ++op) {
        reset_state();
        g_result = (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = EIO};
        const mpmc_result_t result = invoke(op, NULL);
        CHECK(
            result.status == MPMC_ERR_INVALID_ARG && result.system_errno == 0,
            "%s(NULL): resultado %d/%d em vez de INVALID_ARG/0",
            k_op_names[op],
            (int)result.status,
            result.system_errno
        );
        CHECK(
            g_call_count == 0u,
            "%s(NULL): chamou backend (%u vez(es))",
            k_op_names[op],
            g_call_count
        );
    }

    reset_state();
    CHECK(ipc_producer_count(NULL) == 0u, "ipc_producer_count(NULL) deveria ser 0");
    ipc_worker_close(NULL);
    ipc_destroy(NULL);
    CHECK(g_call_count == 0u, "ipc_worker_close/ipc_destroy(NULL) chamaram backend");
    CHECK(g_free_calls == 0u, "ipc_worker_close/ipc_destroy(NULL) liberaram memória");
    expect_clean("contexto nulo");
}

/** @brief Argumentos inválidos de ipc_init() seguem o contrato: erro, *out intacto, sem efeitos. */
static void test_init_rejects_invalid_arguments(void) {
    static const ipc_mode_t invalid_modes[] = {(ipc_mode_t)0,
        (ipc_mode_t)3,
        (ipc_mode_t)255,
        (ipc_mode_t)-1,
        (ipc_mode_t)INT_MAX,
        (ipc_mode_t)INT_MIN};
    ipc_context_t *const marker = MARKER;

    for (size_t i = 0; i < sizeof invalid_modes / sizeof invalid_modes[0]; ++i) {
        reset_state();
        ipc_context_t *out = marker;
        const mpmc_result_t result = ipc_init(&out, 2u, invalid_modes[i]);
        CHECK(
            result.status == MPMC_ERR_INVALID_ARG && result.system_errno == 0,
            "modo inválido %d: resultado %d/%d",
            (int)invalid_modes[i],
            (int)result.status,
            result.system_errno
        );
        CHECK(out == marker, "modo inválido %d: *out foi modificado", (int)invalid_modes[i]);
        CHECK(
            g_call_count == 0u && g_calloc_calls == 0u,
            "modo inválido %d: houve alocação ou chamada de backend",
            (int)invalid_modes[i]
        );
    }

    for (size_t m = 0; m < sizeof k_modes / sizeof k_modes[0]; ++m) {
        const ipc_mode_t mode = k_modes[m];
        reset_state();
        ipc_context_t *out = marker;
        mpmc_result_t result = ipc_init(&out, 0u, mode);
        CHECK(
            result.status == MPMC_ERR_INVALID_ARG && result.system_errno == 0,
            "%s, p=0: resultado %d/%d",
            mode_name(mode),
            (int)result.status,
            result.system_errno
        );
        CHECK(out == marker, "%s, p=0: *out foi modificado", mode_name(mode));
        CHECK(
            g_call_count == 0u && g_calloc_calls == 0u,
            "%s, p=0: houve alocação ou chamada de backend",
            mode_name(mode)
        );

        result = ipc_init(NULL, 2u, mode);
        CHECK(
            result.status == MPMC_ERR_INVALID_ARG && result.system_errno == 0,
            "%s, out nulo: resultado %d/%d",
            mode_name(mode),
            (int)result.status,
            result.system_errno
        );
        CHECK(
            g_call_count == 0u && g_calloc_calls == 0u,
            "%s, out nulo: houve alocação ou chamada de backend",
            mode_name(mode)
        );
    }
    reset_state();
    mpmc_result_t result = ipc_init(NULL, 0u, (ipc_mode_t)7);
    CHECK(
        result.status == MPMC_ERR_INVALID_ARG && result.system_errno == 0,
        "todos os argumentos inválidos: resultado %d/%d",
        (int)result.status,
        result.system_errno
    );
    expect_clean("init inválido");
}

/** @brief ipc_init() bem-sucedido cria o backend do modo, uma única vez, e guarda p. */
static void test_init_success_selects_backend(void) {
    static const size_t counts[] = {1u, 3u, 1024u, SIZE_MAX};

    for (size_t m = 0; m < sizeof k_modes / sizeof k_modes[0]; ++m) {
        const ipc_mode_t mode = k_modes[m];
        for (size_t i = 0; i < sizeof counts / sizeof counts[0]; ++i) {
            reset_state();
            ipc_context_t *context = MARKER;
            const mpmc_result_t result = ipc_init(&context, counts[i], mode);

            CHECK(
                same_result(result, RESULT_OK),
                "%s, p=%zu: resultado %d/%d",
                mode_name(mode),
                counts[i],
                (int)result.status,
                result.system_errno
            );
            CHECK(
                context != NULL && context != MARKER,
                "%s, p=%zu: *out não recebeu o novo contexto",
                mode_name(mode),
                counts[i]
            );
            CHECK(
                g_call_count == 1u,
                "%s, p=%zu: %u chamada(s) ao backend em vez de uma",
                mode_name(mode),
                counts[i],
                g_call_count
            );
            if (g_call_count == 1u) {
                CHECK(
                    g_calls[0].kind
                        == (mode == IPC_MESSAGE_QUEUE ? CALL_MSG_CREATE : CALL_SHM_CREATE),
                    "%s, p=%zu: backend criado é o do outro paradigma",
                    mode_name(mode),
                    counts[i]
                );
                CHECK(
                    g_calls[0].index == counts[i],
                    "%s: backend recebeu p=%zu em vez de %zu",
                    mode_name(mode),
                    g_calls[0].index,
                    counts[i]
                );
                CHECK(
                    g_calls[0].reporter == NULL,
                    "%s: o reporter do backend shm deveria ser NULL (a fachada ainda não o "
                    "repassa)",
                    mode_name(mode)
                );
            }
            CHECK(
                g_calloc_calls == 1u && g_live_allocations == 1,
                "%s, p=%zu: a fachada deveria ter uma alocação (calloc=%u, vivas=%d)",
                mode_name(mode),
                counts[i],
                g_calloc_calls,
                g_live_allocations
            );
            if (context != NULL && context != MARKER) {
                CHECK(
                    ipc_producer_count(context) == counts[i],
                    "%s: ipc_producer_count() devolveu %zu em vez de %zu",
                    mode_name(mode),
                    ipc_producer_count(context),
                    counts[i]
                );
                ipc_destroy(context);
            }
            expect_clean(mode_name(mode));
        }
    }
}

/** @brief Falha de alocação em ipc_init(): erro preservado, *out nulo, backend não criado. */
static void test_init_allocation_failure(void) {
    static const int errors[] = {ENOMEM, EAGAIN};

    for (size_t m = 0; m < sizeof k_modes / sizeof k_modes[0]; ++m) {
        for (size_t e = 0; e < sizeof errors / sizeof errors[0]; ++e) {
            const ipc_mode_t mode = k_modes[m];
            reset_state();
            g_calloc_failure_errno = errors[e];
            ipc_context_t *context = MARKER;
            const mpmc_result_t result = ipc_init(&context, 4u, mode);

            CHECK(
                result.status == MPMC_ERR_SYS && result.system_errno == errors[e],
                "%s, calloc falha com errno %d: resultado %d/%d",
                mode_name(mode),
                errors[e],
                (int)result.status,
                result.system_errno
            );
            CHECK(
                context == NULL, "%s: *out deveria ser NULL após falha de alocação", mode_name(mode)
            );
            CHECK(
                g_call_count == 0u,
                "%s: o backend foi criado apesar da falha de alocação",
                mode_name(mode)
            );
            expect_clean(mode_name(mode));
        }
    }
}

/**
 * @brief Falha de criação do backend: resultado repassado sem alteração, *out nulo,
 * alocação da fachada liberada e nenhuma rotina de remoção chamada (o rollback pertence
 * ao backend, conforme transport.h).
 */
static void test_init_backend_failure_rolls_back(void) {

    for (size_t m = 0; m < sizeof k_modes / sizeof k_modes[0]; ++m) {
        const ipc_mode_t mode = k_modes[m];
        for (size_t r = 0; r < RESULT_COUNT; ++r) {
            if (k_results[r].status == MPMC_OK) {
                continue;
            }
            for (int dirty = 0; dirty <= 1; ++dirty) {
                reset_state();
                g_result = k_results[r];
                g_dirty_out_on_failure = dirty != 0;
                ipc_context_t *context = MARKER;
                const mpmc_result_t result = ipc_init(&context, 5u, mode);

                CHECK(
                    same_result(result, k_results[r]),
                    "%s: backend falhou com %d/%d, ipc_init() devolveu %d/%d",
                    mode_name(mode),
                    (int)k_results[r].status,
                    k_results[r].system_errno,
                    (int)result.status,
                    result.system_errno
                );
                CHECK(
                    context == NULL,
                    "%s, falha %d%s: *out deveria ser NULL",
                    mode_name(mode),
                    (int)k_results[r].status,
                    dirty ? " (backend deixou *out sujo)" : ""
                );
                CHECK(
                    g_call_count == 1u,
                    "%s, falha %d: %u chamada(s) ao backend; nenhuma remoção deve ser chamada após "
                    "falha de criação",
                    mode_name(mode),
                    (int)k_results[r].status,
                    g_call_count
                );
                if (g_call_count >= 1u) {
                    CHECK(
                        g_calls[0].kind
                            == (mode == IPC_MESSAGE_QUEUE ? CALL_MSG_CREATE : CALL_SHM_CREATE),
                        "%s: a primeira chamada não foi a criação do backend do modo",
                        mode_name(mode)
                    );
                }
                CHECK(
                    g_calloc_calls == 1u && g_free_calls == 1u,
                    "%s, falha %d: calloc=%u free=%u (a fachada deve liberar a própria alocação "
                    "uma vez)",
                    mode_name(mode),
                    (int)k_results[r].status,
                    g_calloc_calls,
                    g_free_calls
                );
                expect_clean(mode_name(mode));
            }
        }
    }
}

/**
 * @brief ipc_worker_close() chama só a rotina de fechamento local do backend, antes de
 * liberar o contexto, e nunca a de remoção de recursos globais.
 */
static void test_worker_close_is_local_only(void) {
    for (size_t m = 0; m < sizeof k_modes / sizeof k_modes[0]; ++m) {
        const ipc_mode_t mode = k_modes[m];
        for (int failing = 0; failing <= 1; ++failing) {
            reset_state();
            ipc_context_t *context = make_context(mode, 2u);
            g_result =
                failing ? (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = EIO} : RESULT_OK;

            ipc_worker_close(context);

            CHECK(
                g_call_count == 1u,
                "%s: ipc_worker_close() fez %u chamada(s) ao backend",
                mode_name(mode),
                g_call_count
            );
            if (g_call_count >= 1u) {
                CHECK(
                    g_calls[0].kind
                        == (mode == IPC_MESSAGE_QUEUE ? CALL_MSG_CLOSE : CALL_SHM_CLOSE),
                    "%s: ipc_worker_close() não chamou o fechamento local do backend (rotina %d)",
                    mode_name(mode),
                    (int)g_calls[0].kind
                );
                CHECK(
                    g_calls[0].backend == backend_of(mode),
                    "%s: fechamento recebeu outro backend",
                    mode_name(mode)
                );
                CHECK(
                    g_calls[0].reporter == NULL, "%s: o reporter deveria ser NULL", mode_name(mode)
                );
                CHECK(
                    g_calls[0].frees_before == 0u,
                    "%s: o contexto foi liberado antes de fechar o backend",
                    mode_name(mode)
                );
            }
            for (unsigned i = 0; i < g_call_count; ++i) {
                CHECK(
                    g_calls[i].kind != CALL_MSG_DESTROY && g_calls[i].kind != CALL_SHM_DESTROY,
                    "%s: o fechamento do filho chamou a remoção de recursos globais",
                    mode_name(mode)
                );
            }
            CHECK(
                g_free_calls == 1u,
                "%s: o contexto deveria ser liberado uma vez (free=%u)%s",
                mode_name(mode),
                g_free_calls,
                failing ? " mesmo com erro do backend" : ""
            );
            expect_clean(mode_name(mode));
        }
    }
}

/**
 * @brief ipc_destroy() chama só a rotina de remoção do backend, antes de liberar o
 * contexto, e nunca o fechamento local do filho.
 */
static void test_destroy_removes_global_resources(void) {
    for (size_t m = 0; m < sizeof k_modes / sizeof k_modes[0]; ++m) {
        const ipc_mode_t mode = k_modes[m];
        for (int failing = 0; failing <= 1; ++failing) {
            reset_state();
            ipc_context_t *context = make_context(mode, 2u);
            g_result =
                failing ? (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = EIO} : RESULT_OK;

            ipc_destroy(context);

            CHECK(
                g_call_count == 1u,
                "%s: ipc_destroy() fez %u chamada(s) ao backend",
                mode_name(mode),
                g_call_count
            );
            if (g_call_count >= 1u) {
                CHECK(
                    g_calls[0].kind
                        == (mode == IPC_MESSAGE_QUEUE ? CALL_MSG_DESTROY : CALL_SHM_DESTROY),
                    "%s: ipc_destroy() não chamou a remoção do backend (rotina %d)",
                    mode_name(mode),
                    (int)g_calls[0].kind
                );
                CHECK(
                    g_calls[0].backend == backend_of(mode),
                    "%s: remoção recebeu outro backend",
                    mode_name(mode)
                );
                CHECK(
                    g_calls[0].reporter == NULL, "%s: o reporter deveria ser NULL", mode_name(mode)
                );
                CHECK(
                    g_calls[0].frees_before == 0u,
                    "%s: o contexto foi liberado antes de remover o backend",
                    mode_name(mode)
                );
            }
            for (unsigned i = 0; i < g_call_count; ++i) {
                CHECK(
                    g_calls[i].kind != CALL_MSG_CLOSE && g_calls[i].kind != CALL_SHM_CLOSE,
                    "%s: ipc_destroy() chamou o fechamento local do filho",
                    mode_name(mode)
                );
            }
            CHECK(
                g_free_calls == 1u,
                "%s: o contexto deveria ser liberado uma vez (free=%u)%s",
                mode_name(mode),
                g_free_calls,
                failing ? " mesmo com erro do backend" : ""
            );
            expect_clean(mode_name(mode));
        }
    }
}

/** @brief Dois contextos de modos diferentes convivem sem estado global compartilhado. */
static void test_contexts_of_both_modes_coexist(void) {
    reset_state();
    ipc_context_t *shared = make_context(IPC_SHARED_MEMORY, 2u);
    ipc_context_t *queues = make_context(IPC_MESSAGE_QUEUE, 7u);

    CHECK(
        ipc_producer_count(shared) == 2u && ipc_producer_count(queues) == 7u,
        "cada contexto deve guardar o seu p"
    );

    g_call_count = 0u;
    CHECK(
        same_result(ipc_send(queues, 1u, &g_message_in), RESULT_OK),
        "envio pelo contexto de filas falhou"
    );
    CHECK(
        g_call_count == 1u && g_calls[0].kind == CALL_MSG_SEND,
        "o contexto de filas não chamou o backend de filas"
    );
    g_call_count = 0u;
    CHECK(
        same_result(ipc_send(shared, 1u, &g_message_in), RESULT_OK),
        "envio pelo contexto de memória compartilhada falhou"
    );
    CHECK(
        g_call_count == 1u && g_calls[0].kind == CALL_SHM_SEND,
        "o contexto de memória compartilhada não chamou o seu backend"
    );

    g_call_count = 0u;
    ipc_worker_close(queues);
    CHECK(
        g_call_count == 1u && g_calls[0].kind == CALL_MSG_CLOSE,
        "o fechamento do contexto de filas afetou o outro backend"
    );
    g_call_count = 0u;
    ipc_destroy(shared);
    CHECK(
        g_call_count == 1u && g_calls[0].kind == CALL_SHM_DESTROY,
        "a remoção do contexto de memória compartilhada afetou o outro backend"
    );
    expect_clean("coexistência");
}

int main(void) {
    test_instrumentation_works();
    test_dispatch_goes_to_selected_backend();
    test_poll_events_is_noop_in_shared_memory_mode();
    test_results_and_outputs_are_forwarded();
    test_null_context_is_rejected();
    test_init_rejects_invalid_arguments();
    test_init_success_selects_backend();
    test_init_allocation_failure();
    test_init_backend_failure_rolls_back();
    test_worker_close_is_local_only();
    test_destroy_removes_global_resources();
    test_contexts_of_both_modes_coexist();

    printf(
        "%u verificações do transporte com backends substitutos, %u falhas\n", g_checks, g_failures
    );
    return g_failures == 0u ? 0 : 1;
}
