/**
 * @file producer_routing.c
 * @brief Implementação do roteamento cíclico de `produz 0`.
 * @ingroup workers
 * @see producer_routing.h para o contrato e as pré-condições.
 */
#include "producer_routing.h"

mpmc_status_t
producer_route_destination(size_t ordinal, size_t producer_count, size_t *out_destination) {
    if (out_destination == NULL || producer_count == 0u || ordinal == 0u) {
        return MPMC_ERR_INVALID_ARG;
    }

    *out_destination = 1u + ((ordinal - 1u) % producer_count);
    return MPMC_OK;
}
