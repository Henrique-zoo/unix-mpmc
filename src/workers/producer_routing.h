/**
 * @file producer_routing.h
 * @brief Cálculo do destino lógico de `produz 0` (distribuição cíclica).
 * @ingroup workers
 *
 * @details
 * Cabeçalho interno de src/workers/, sem equivalente em include/. Contém apenas
 * a função pura de roteamento: ela não usa IPC, não cria processos, não espera
 * ACK e não conhece metadados de mensagens nem o estado sequencial do produtor.
 * A ligação ao laço de envio de producer_run() e o ACK agregado pertencem à
 * issue #18. Testes de propriedades (issue #24) podem usar esta função
 * compilando com `-Isrc/workers`.
 */
#ifndef MPMC_WORKERS_PRODUCER_ROUTING_H
#define MPMC_WORKERS_PRODUCER_ROUTING_H

#include <stddef.h>

#include "mpmc/error.h"

/**
 * @brief Calcula o destino da mensagem de ordinal `k` em `produz 0`.
 * @param ordinal Ordinal `k` da mensagem no processo emissor, com base 1: a
 * primeira mensagem é 1, como em producer_sequence_t. Deve ser maior que zero.
 * @param producer_count Quantidade `p` de estruturas destinatárias. Deve ser
 * maior que zero.
 * @param[out] out_destination Recebe o destino `1 + ((k - 1) % p)`, em `1..p`,
 * somente em sucesso.
 * @retval MPMC_OK Destino calculado.
 * @retval MPMC_ERR_INVALID_ARG `out_destination` nulo, `producer_count == 0` ou
 * `ordinal == 0`.
 * @details O destino de `k` é o de `k - p` somado a `p`: as mensagens percorrem
 * `1, 2, ..., p, 1, 2, ...`. Para `n` mensagens com `k = 1..n`, a estrutura `d`
 * recebe `n / p` mensagens, mais uma se `d <= n % p`; a soma é `n`. A conta nunca
 * excede `p`, portanto não há overflow para nenhum `size_t` válido.
 * @post Em falha, `*out_destination` permanece inalterado.
 * @note Função pura e reentrante: sem estado, sem efeitos colaterais e sem
 * chamadas de sistema. Não altera nem reinicia a sequência do produtor.
 */
mpmc_status_t producer_route_destination(
    size_t ordinal,
    size_t producer_count,
    size_t *out_destination
);

#endif
