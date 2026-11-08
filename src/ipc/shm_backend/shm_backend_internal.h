/**
 * @file shm_backend_internal.h
 * @brief Representação privada do contexto do backend de memória compartilhada.
 * @ingroup backends
 * @details Reúne os tipos usados pelos arquivos de implementação do backend.
 * O cabeçalho público shm_backend.h mantém shm_backend_t como tipo opaco para
 * a fachada de transporte; este arquivo descreve seu estado local completo.
 */
#ifndef SHM_BACKEND_INTERNAL_H
#define SHM_BACKEND_INTERNAL_H

#include <stddef.h>

#include "mpmc/ipc/shm_backend.h"
#include "shm_ring.h"

/** @brief Identificador global e associação local de um segmento System V. */
typedef struct {
    int shmid;        /**< Identificador do segmento; -1 antes da criação. Zero é válido. */
    shm_ring_t *ring; /**< Endereço associado neste processo; NULL antes da associação. */
} shm_segment_t;

/**
 * @brief Representação privada dos segmentos pertencentes ao backend.
 * @details A estrutura e seu vetor são alocados no heap local do processo;
 * o conteúdo compartilhado está nas regiões System V apontadas por segments[i].ring.
 */
struct shm_backend {
    size_t producer_count;   /**< Quantidade de estruturas lógicas e registros em segments. */
    shm_segment_t *segments; /**< Vetor local com um registro por estrutura lógica. */
};

#endif
