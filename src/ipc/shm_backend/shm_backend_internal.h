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

/**
 * @brief Recursos de uma estrutura lógica: segmento, conjunto de semáforos e associação local.
 * @details shmid e semid identificam objetos globais pertencentes ao supervisor.
 * O ponteiro ring é válido somente no espaço de endereçamento do processo atual.
 * Cada entrada é preparada por lifecycle.c antes de qualquer criação de recurso.
 */
typedef struct {
    int shmid; /**< Identificador do segmento; -1 antes da criação. Zero é válido. */
    int semid; /**< Identificador do conjunto de SHM_SEM_COUNT semáforos; -1 antes da criação. */
    shm_ring_t *ring; /**< Endereço associado neste processo; NULL antes da associação. */
} shm_segment_t;

/**
 * @brief Índices privados do conjunto de semáforos de cada buffer circular.
 * @details Os índices são os mesmos em todas as estruturas lógicas. Os vetores
 * de operations.c combinam a reserva de EMPTY/FULL com a aquisição de MUTEX.
 */
enum {
    SHM_SEM_MUTEX, /**< Exclusão mútua para dados e contadores; valor inicial 1. */
    SHM_SEM_EMPTY, /**< Posições livres para inserção; valor inicial igual à capacidade. */
    SHM_SEM_FULL,  /**< Mensagens disponíveis para retirada; valor inicial zero. */
    SHM_SEM_COUNT, /**< Quantidade de semáforos do conjunto; não é um índice utilizável. */
};

/**
 * @brief Representação privada dos segmentos pertencentes ao backend.
 * @details A estrutura e seu vetor são alocados no heap local do processo;
 * o conteúdo compartilhado está nas regiões System V apontadas por segments[i].ring,
 * e os valores dos semáforos são mantidos pelo kernel, identificados por segments[i].semid.
 */
struct shm_backend {
    size_t producer_count;   /**< Quantidade de estruturas lógicas e registros em segments. */
    shm_segment_t *segments; /**< Vetor local com um registro por estrutura lógica. */
};

#endif
