/**
 * @file test_shm_ring.c
 * @brief Testes de fronteira e invariantes do buffer circular compartilhado.
 * @ingroup tests
 * @details Usa memória alocada localmente, com sentinelas antes e depois dos
 * 2048 bytes do segmento. Não cria IPC nem exercita sincronização entre processos.
 */
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "shm_ring.h"

/** @brief Capacidade prevista pelo layout documentado, independente do cálculo do módulo. */
#define EXPECTED_CAPACITY 63U
/** @brief Bytes de guarda em cada extremidade; preservam o alinhamento do buffer. */
#define GUARD_BYTES 32U
/** @brief Padrão que identifica alterações nos bytes de guarda e na reserva final. */
#define GUARD_PATTERN 0xA5U

static_assert(GUARD_BYTES % alignof(shm_ring_t) == 0, "a guarda deve preservar o alinhamento");

/** @brief Quantidade acumulada de verificações executadas. */
static unsigned checks;
/** @brief Quantidade acumulada de verificações que falharam. */
static unsigned failures;

/**
 * @brief Registra uma verificação sem interromper os demais cenários.
 * @param condition Expressão avaliada uma única vez.
 */
#define CHECK(condition)                                                                           \
    do {                                                                                           \
        ++checks;                                                                                  \
        if (!(condition)) {                                                                        \
            ++failures;                                                                            \
            fprintf(stderr, "FALHA %s:%d: %s\n", __FILE__, __LINE__, #condition);                  \
        }                                                                                          \
    } while (0)

static void test_initialization(void);
static void test_full_and_empty(void);
static void test_wrap_with_pending_messages(void);
static void test_many_wraps(void);
static void test_independent_buffers(void);
static shm_ring_t *new_ring(void);
static void free_ring(shm_ring_t *ring);
static message_t make_message(uint32_t sequence);
static void check_message(message_t message, uint32_t sequence);
static void check_state(const shm_ring_t *ring, uint32_t produced, uint32_t consumed);
static void check_guards(const shm_ring_t *ring);

/**
 * @brief Executa os cenários de fronteira e apresenta o total de verificações.
 * @return Zero se todos os cenários passarem; um se houver falha ou erro de alocação.
 */
int main(void) {
    test_initialization();
    test_full_and_empty();
    test_wrap_with_pending_messages();
    test_many_wraps();
    test_independent_buffers();
    printf("%u verificações do buffer circular, %u falhas\n", checks, failures);
    return failures == 0 ? 0 : 1;
}

/** @brief Confere estado vazio, alinhamento e ocupação máxima do segmento. */
static void test_initialization(void) {
    shm_ring_t *ring = new_ring();
    if (ring == NULL) return;

    check_state(ring, 0, 0);
    CHECK((uintptr_t)ring % alignof(shm_ring_t) == 0);
    CHECK((uintptr_t)ring->messages % alignof(message_t) == 0);
    size_t used = offsetof(shm_ring_t, messages) + ring->capacity * sizeof(message_t);
    CHECK(used == 2032);
    CHECK(MPMC_SHM_SEGMENT_BYTES - used == 16);
    CHECK(used + sizeof(message_t) > MPMC_SHM_SEGMENT_BYTES);
    free_ring(ring);
}

/** @brief Preenche todas as posições, esvazia e reutiliza o buffer sem perder mensagens. */
static void test_full_and_empty(void) {
    shm_ring_t *ring = new_ring();
    if (ring == NULL) return;

    for (uint32_t sequence = 1; sequence <= EXPECTED_CAPACITY; ++sequence) {
        shm_ring_push(ring, make_message(sequence));
        check_state(ring, sequence, 0);
    }
    CHECK(ring->count == ring->capacity && ring->head == ring->tail);
    check_guards(ring);

    for (uint32_t sequence = 1; sequence <= EXPECTED_CAPACITY; ++sequence) {
        check_message(shm_ring_pop(ring), sequence);
        check_state(ring, EXPECTED_CAPACITY, sequence);
    }
    CHECK(ring->count == 0 && ring->head == ring->tail);

    shm_ring_push(ring, make_message(EXPECTED_CAPACITY + 1));
    check_state(ring, EXPECTED_CAPACITY + 1, EXPECTED_CAPACITY);
    check_message(shm_ring_pop(ring), EXPECTED_CAPACITY + 1);
    check_state(ring, EXPECTED_CAPACITY + 1, EXPECTED_CAPACITY + 1);
    free_ring(ring);
}

/** @brief Confere FIFO quando a escrita volta ao início com mensagens ainda pendentes. */
static void test_wrap_with_pending_messages(void) {
    shm_ring_t *ring = new_ring();
    if (ring == NULL) return;

    const uint32_t removed = 17;
    for (uint32_t sequence = 1; sequence <= EXPECTED_CAPACITY; ++sequence) {
        shm_ring_push(ring, make_message(sequence));
    }
    for (uint32_t sequence = 1; sequence <= removed; ++sequence) {
        check_message(shm_ring_pop(ring), sequence);
        check_state(ring, EXPECTED_CAPACITY, sequence);
    }
    for (
        uint32_t sequence = EXPECTED_CAPACITY + 1; sequence <= EXPECTED_CAPACITY + removed;
        ++sequence
    ) {
        shm_ring_push(ring, make_message(sequence));
        check_state(ring, sequence, removed);
    }
    CHECK(ring->count == ring->capacity && ring->head == removed && ring->tail == removed);

    for (uint32_t sequence = removed + 1; sequence <= EXPECTED_CAPACITY + removed; ++sequence) {
        check_message(shm_ring_pop(ring), sequence);
        check_state(ring, EXPECTED_CAPACITY + removed, sequence);
    }
    free_ring(ring);
}

/**
 * @brief Alterna inserções e retiradas por várias voltas e além de 1024 consumos.
 * @details O modelo esperado usa sequências crescentes e contagens independentes
 * dos metadados do buffer para decidir cada operação e conferir a ordem FIFO.
 */
static void test_many_wraps(void) {
    shm_ring_t *ring = new_ring();
    if (ring == NULL) return;

    uint32_t produced = 0;
    uint32_t consumed = 0;
    for (uint32_t step = 0; step < 4096; ++step) {
        uint32_t pending = produced - consumed;
        if (pending == 0 || (pending < EXPECTED_CAPACITY && step % 7 < 4)) {
            shm_ring_push(ring, make_message(++produced));
        } else {
            check_message(shm_ring_pop(ring), ++consumed);
        }
        check_state(ring, produced, consumed);
    }
    while (consumed < produced) {
        check_message(shm_ring_pop(ring), ++consumed);
        check_state(ring, produced, consumed);
    }
    CHECK(consumed > MPMC_MAX_MESSAGES);
    free_ring(ring);
}

/** @brief Garante que operar um buffer não altera os metadados e mensagens de outro. */
static void test_independent_buffers(void) {
    shm_ring_t *first = new_ring();
    shm_ring_t *second = new_ring();
    if (first == NULL || second == NULL) {
        free_ring(first);
        free_ring(second);
        return;
    }

    shm_ring_push(first, make_message(1));
    shm_ring_push(second, make_message(42));
    check_message(shm_ring_pop(first), 1);
    check_state(first, 1, 1);
    check_state(second, 1, 0);
    check_message(shm_ring_pop(second), 42);
    check_state(second, 1, 1);
    free_ring(first);
    free_ring(second);
}

/**
 * @brief Aloca e inicializa um segmento local entre duas áreas de guarda.
 * @return Buffer inicializado, ou NULL se a alocação falhar ou a capacidade divergir.
 * @details malloc() fornece o alinhamento necessário; GUARD_BYTES é múltiplo
 * de alignof(shm_ring_t), preservando-o no endereço interior usado pelo buffer.
 * O preenchimento prévio não nulo impede que a inicialização dependa de memória zerada.
 */
static shm_ring_t *new_ring(void) {
    unsigned char *memory = malloc(GUARD_BYTES + MPMC_SHM_SEGMENT_BYTES + GUARD_BYTES);
    CHECK(memory != NULL);
    if (memory == NULL) return NULL;

    memset(memory, GUARD_PATTERN, GUARD_BYTES + MPMC_SHM_SEGMENT_BYTES + GUARD_BYTES);
    shm_ring_t *ring = (void *)(memory + GUARD_BYTES);
    shm_ring_init(ring);
    CHECK(ring->capacity == EXPECTED_CAPACITY);
    if (ring->capacity != EXPECTED_CAPACITY) {
        free(memory);
        return NULL;
    }
    return ring;
}

/**
 * @brief Confere as sentinelas e libera a alocação de new_ring().
 * @param ring Buffer ainda pertencente ao teste; NULL é aceito.
 */
static void free_ring(shm_ring_t *ring) {
    if (ring == NULL) return;
    check_guards(ring);
    free((unsigned char *)ring - GUARD_BYTES);
}

/**
 * @brief Produz uma mensagem com todos os campos variando de forma determinística.
 * @param sequence Sequência positiva utilizada como identidade esperada pelo teste.
 * @return Mensagem sintética com PID, timestamp e operação derivados da sequência.
 */
static message_t make_message(uint32_t sequence) {
    return (message_t){
        .sender_pid = (pid_t)(1000U + sequence % 97U),
        .sequence = sequence,
        .sent_at =
            {
                .tv_sec = (time_t)(1700000000U + sequence),
                .tv_nsec = (long)(sequence * 1021U % 1000000000U),
            },
        .operation_id = UINT64_C(0x100000000) + sequence,
    };
}

/**
 * @brief Confere todos os campos da mensagem retirada, sem comparar padding.
 * @param message Mensagem efetivamente retornada pelo buffer.
 * @param sequence Sequência da mensagem esperada pelo modelo FIFO.
 */
static void check_message(message_t message, uint32_t sequence) {
    message_t expected = make_message(sequence);
    CHECK(message.sender_pid == expected.sender_pid);
    CHECK(message.sequence == expected.sequence);
    CHECK(message.sent_at.tv_sec == expected.sent_at.tv_sec);
    CHECK(message.sent_at.tv_nsec == expected.sent_at.tv_nsec);
    CHECK(message.operation_id == expected.operation_id);
}

/**
 * @brief Confronta os invariantes com totais de produção e consumo independentes.
 * @param ring Buffer inicializado a conferir.
 * @param produced Quantidade de inserções realizadas neste buffer.
 * @param consumed Quantidade de retiradas realizadas neste buffer.
 */
static void check_state(const shm_ring_t *ring, uint32_t produced, uint32_t consumed) {
    CHECK(ring->capacity == EXPECTED_CAPACITY);
    CHECK(ring->head < ring->capacity && ring->tail < ring->capacity);
    CHECK(ring->count <= ring->capacity);
    CHECK(ring->count == produced - consumed);
    CHECK(ring->head == consumed % EXPECTED_CAPACITY);
    CHECK(ring->tail == produced % EXPECTED_CAPACITY);
    CHECK(ring->consumed_total == consumed);
}

/**
 * @brief Detecta escritas fora do segmento e na reserva após a última mensagem.
 * @param ring Buffer alocado por new_ring() e ainda não liberado.
 */
static void check_guards(const shm_ring_t *ring) {
    const unsigned char *bytes = (const void *)ring;
    const unsigned char *memory = bytes - GUARD_BYTES;
    for (size_t i = 0; i < GUARD_BYTES; ++i) {
        CHECK(memory[i] == GUARD_PATTERN);
        CHECK(bytes[MPMC_SHM_SEGMENT_BYTES + i] == GUARD_PATTERN);
    }
    for (size_t i = 2032; i < MPMC_SHM_SEGMENT_BYTES; ++i) {
        CHECK(bytes[i] == GUARD_PATTERN);
    }
}
