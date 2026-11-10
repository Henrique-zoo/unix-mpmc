/**
 * @file shm_ring.c
 * @brief Implementação das operações internas do buffer circular compartilhado.
 * @ingroup backends
 * @details As verificações com assert() auxiliam o diagnóstico de violações das
 * pré-condições e são desabilitadas quando NDEBUG está definido.
 * @see shm_ring.h para os contratos das operações e a descrição do layout.
 */
#include "shm_ring.h"

#include <assert.h>
#include <stddef.h>

/**
 * @details Calcula a capacidade usando offsetof(shm_ring_t, messages) e
 * sizeof(message_t), considerando o alinhamento definido pelo compilador.
 * A atribuição do literal composto inicializa somente a parte fixa da estrutura;
 * não percorre o vetor flexível nem apaga as posições de mensagens.
 */
void shm_ring_init(shm_ring_t *ring) {
    assert(ring != nullptr);

    *ring = (shm_ring_t){
        .head = 0,
        .tail = 0,
        .count = 0,
        .capacity = SHM_RING_CAPACITY,
        .consumed_total = 0,
    };
}

/**
 * @details As asserções verificam o ponteiro e a existência de espaço livre.
 * A cópia usa atribuição de message_t; o acesso à posição utiliza o valor de
 * tail anterior ao pós-incremento, seguido da redução módulo capacity.
 */
void shm_ring_push(shm_ring_t *ring, message_t message) {
    assert(ring != nullptr);
    assert(ring->count < ring->capacity);

    ring->messages[ring->tail++] = message;
    ring->tail %= ring->capacity;
    ring->count++;
}

/**
 * @details As asserções verificam o ponteiro e a existência de mensagens.
 * A mensagem é copiada para uma variável local antes da atualização dos contadores.
 * Os bytes da posição consumida não são apagados; sua validade é determinada
 * pelos metadados do buffer, não pelo conteúdo armazenado.
 */
message_t shm_ring_pop(shm_ring_t *ring) {
    assert(ring != nullptr);
    assert(ring->count > 0);

    message_t message = ring->messages[ring->head++];
    ring->head %= ring->capacity;
    ring->count--;
    ring->consumed_total++;

    return message;
}
