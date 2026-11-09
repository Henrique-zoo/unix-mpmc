/**
 * @file shm_ring.h
 * @brief Layout e operações internas do buffer circular compartilhado.
 * @ingroup backends
 * @details Interface privada de shm_backend. As operações não executam
 * sincronização, reservas de semáforos nem chamadas de IPC.
 * @warning As pré-condições são obrigatórias; sua violação não é tratada como
 * erro recuperável.
 */

#ifndef SHM_RING_H
#define SHM_RING_H

#include <assert.h>
#include <stddef.h>
#include <stdint.h>

#include "mpmc/message.h"

/**
 * @brief Cabeçalho e área flexível de mensagens de um segmento compartilhado.
 * @details O vetor messages ocupa o espaço disponível após o cabeçalho no
 * segmento de MPMC_SHM_SEGMENT_BYTES bytes. A quantidade de posições utilizáveis
 * é calculada por shm_ring_init() e armazenada em capacity.
 */
typedef struct {
    uint8_t head;     /**< Índice da próxima retirada, no intervalo 0..capacity-1. */
    uint8_t tail;     /**< Índice da próxima inserção, no intervalo 0..capacity-1. */
    uint8_t count;    /**< Quantidade de mensagens armazenadas, no intervalo 0..capacity. */
    uint8_t capacity; /**< Quantidade de posições de mensagem disponíveis no segmento. */

    uint64_t consumed_total; /**< Total acumulado de retiradas desde a inicialização. */

    message_t messages[]; /**< Área de mensagens com capacity posições utilizáveis. */
} shm_ring_t;

/** @brief Quantidade de mensagens inteiras que cabe após o cabeçalho alinhado. */
#define SHM_RING_CAPACITY                                                                          \
    ((MPMC_SHM_SEGMENT_BYTES - offsetof(shm_ring_t, messages)) / sizeof(message_t))

static_assert(sizeof(shm_ring_t) == 16, "cabecalho de shm_ring_t deve ter 16 bytes");
static_assert(alignof(shm_ring_t) == 8, "shm_ring_t deve ter alinhamento 8");
static_assert(offsetof(shm_ring_t, head) == 0, "offset de head");
static_assert(offsetof(shm_ring_t, tail) == 1, "offset de tail");
static_assert(offsetof(shm_ring_t, count) == 2, "offset de count");
static_assert(offsetof(shm_ring_t, capacity) == 3, "offset de capacity");
static_assert(offsetof(shm_ring_t, consumed_total) == 8, "offset de consumed_total");
static_assert(offsetof(shm_ring_t, messages) == 16, "offset de messages");
static_assert(
    offsetof(shm_ring_t, messages) % alignof(message_t) == 0,
    "messages deve estar alinhado"
);
static_assert(
    offsetof(shm_ring_t, messages) < MPMC_SHM_SEGMENT_BYTES,
    "cabecalho deve caber no segmento"
);
static_assert(SHM_RING_CAPACITY > 0, "o segmento deve comportar ao menos uma mensagem");
static_assert(SHM_RING_CAPACITY <= UINT8_MAX, "a capacidade deve ser representavel em uint8_t");
static_assert(
    offsetof(shm_ring_t, messages) + SHM_RING_CAPACITY * sizeof(message_t)
        <= MPMC_SHM_SEGMENT_BYTES,
    "as mensagens devem caber nos 2048 bytes do segmento"
);

/**
 * @brief Inicializa o buffer circular em um segmento compartilhado.
 * @param[out] ring Buffer cujo cabeçalho será inicializado.
 * @pre ring aponta para uma região gravável de MPMC_SHM_SEGMENT_BYTES bytes,
 * corretamente alinhada para shm_ring_t.
 * @post head, tail, count e consumed_total são zero; capacity contém a quantidade
 * de mensagens que cabe no espaço disponível após o cabeçalho.
 * @note Deve ser chamada apenas durante a inicialização do segmento,
 * antes de qualquer acesso concorrente.
 */
void shm_ring_init(shm_ring_t *ring);

/**
 * @brief Insere uma mensagem no final do buffer circular.
 * @param[in,out] ring Buffer circular que receberá a mensagem.
 * @param[in] message Mensagem a ser inserida, copiada integralmente para o buffer.
 * @pre ring aponta para um buffer circular previamente inicializado.
 * @pre ring->count < ring->capacity.
 * @pre O chamador reservou uma posição para escrita e garantiu acesso exclusivo
 * ao buffer.
 * @post A mensagem ocupa a antiga posição tail.
 * @post tail avança uma posição circularmente e count é incrementado.
 * @post head e consumed_total permanecem inalterados.
 */
void shm_ring_push(shm_ring_t *ring, message_t message);

/**
 * @brief Remove e retorna a mensagem mais antiga do buffer circular.
 * @param[in,out] ring Buffer circular do qual a mensagem será retirada.
 * @return Cópia da mensagem mais antiga armazenada no buffer.
 * @pre ring aponta para um buffer circular previamente inicializado.
 * @pre ring->count > 0.
 * @pre O chamador reservou uma mensagem para leitura e garantiu acesso exclusivo
 * ao buffer.
 * @post head avança uma posição circularmente e count é decrementado.
 * @post consumed_total é incrementado em uma unidade.
 * @post tail permanece inalterado.
 */
message_t shm_ring_pop(shm_ring_t *ring);

#endif
