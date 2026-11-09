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

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

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

/** @brief Quantidade máxima de operações ACK simultaneamente registradas. */
#define SHM_ACK_OPERATION_CAPACITY 64

/** @brief Índices do conjunto global de semáforos de ACK. */
enum {
    SHM_ACK_SEM_MUTEX,
    SHM_ACK_SEM_NOTIFICATION_BASE,
    SHM_ACK_SEM_COUNT = SHM_ACK_SEM_NOTIFICATION_BASE + SHM_ACK_OPERATION_CAPACITY,
};

/**
 * @brief Estado compartilhado de uma operação que aguarda consumo.
 * @details operation_id zero indica posição livre. Uma operação registrada
 * está concluída quando remaining chega a zero. Todos os campos são
 * acessados sob o mutex global; waiting impede reutilização durante a espera.
 */
typedef struct {
    uint64_t operation_id; /**< Identificador único; zero se livre. */
    pid_t producer_pid;    /**< PID do produtor proprietário. */
    uint16_t remaining;    /**< Consumos ainda não confirmados. */
    bool waiting;          /**< Existe uma espera ativa nesta posição. */
} shm_ack_record_t;

/** @brief Tabela global de operações ACK, protegida pelo mutex. */
typedef struct {
    uint32_t magic;    /**< Identificador do layout. */
    uint64_t reserved; /**< Reservado para evolução do formato. */
    shm_ack_record_t records[SHM_ACK_OPERATION_CAPACITY];
} shm_ack_control_t;

static_assert(
    sizeof(shm_ack_control_t) <= MPMC_SHM_SEGMENT_BYTES,
    "a tabela de ACK deve caber em um segmento de 2048 bytes"
);

/** @brief Recursos locais e identificadores globais do backend SHM. */
struct shm_backend {
    size_t producer_count;          /**< Quantidade de estruturas lógicas. */
    shm_segment_t *segments;        /**< Recursos de cada buffer. */
    int ack_control_shmid;          /**< ID do segmento da tabela ACK. */
    shm_ack_control_t *ack_control; /**< Associação local da tabela. */
    int ack_semid;                  /**< Mutex e notificações de conclusão. */
};

/** @brief Cria e inicializa os recursos globais de controle ACK. */
mpmc_result_t shm_backend_ack_resources_create(shm_backend_t *backend);

/**
 * @brief Desassocia e, quando solicitado, remove os recursos globais de ACK.
 * @param backend Instância do backend.
 * @param remove_global Se true, também solicita IPC_RMID dos recursos.
 */
mpmc_result_t shm_backend_ack_resources_cleanup(shm_backend_t *backend, bool remove_global);

/**
 * @brief Confirma uma retirada efetiva para a operação produtora.
 * @param backend Backend compartilhado.
 * @param producer_id Estrutura lógica de origem, no intervalo 1..p.
 * @param message Mensagem efetivamente retirada.
 * @pre Cada retirada efetiva chama esta função exatamente uma vez.
 * @details Função privada, sem deduplicação por sequência. Operações
 * desconhecidas são ignoradas. Somente o último consumo publica notificação.
 */
mpmc_result_t shm_backend_ack_notify_consumed(
    shm_backend_t *backend,
    size_t producer_id,
    const message_t *message
);

#endif
