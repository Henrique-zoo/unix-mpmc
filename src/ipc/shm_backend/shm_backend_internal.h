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

/**
 * @brief Representação privada dos segmentos pertencentes ao backend.
 * @details A estrutura e seu vetor são alocados no heap local do processo;
 * o conteúdo compartilhado está nas regiões System V apontadas por segments[i].ring,
 * e os valores dos semáforos são mantidos pelo kernel, identificados por segments[i].semid.
 */
/**
 * @brief Limites e índices dos recursos compartilhados de confirmação.
 *
 * Cada operação pode confirmar até MPMC_MAX_MESSAGES sequências distintas.
 * O bitmap de cada operação ocupa 128 bytes. Dezesseis bitmaps cabem
 * exatamente em um segmento de controle de 2048 bytes.
 */
#define SHM_ACK_OPERATION_CAPACITY 64U
#define SHM_ACK_BITMAP_BYTES ((MPMC_MAX_MESSAGES + 7U) / 8U)
#define SHM_ACK_BITMAPS_PER_SEGMENT (MPMC_SHM_SEGMENT_BYTES / SHM_ACK_BITMAP_BYTES)
#define SHM_ACK_BITMAP_SEGMENT_COUNT (SHM_ACK_OPERATION_CAPACITY / SHM_ACK_BITMAPS_PER_SEGMENT)

/** @brief Índices do conjunto global de semáforos de ACK. */
enum {
    SHM_ACK_SEM_MUTEX,
    SHM_ACK_SEM_NOTIFICATION_BASE,
    SHM_ACK_SEM_COUNT = SHM_ACK_SEM_NOTIFICATION_BASE + SHM_ACK_OPERATION_CAPACITY,
};

/** @brief Registro compartilhado de uma operação produtora. */
typedef struct {
    uint64_t operation_id; /**< Zero indica registro livre. */
    pid_t producer_pid;    /**< Processo que registrou a operação. */
    uint16_t expected;     /**< Quantidade esperada, de 1 a 1024. */
    uint16_t received;     /**< Quantidade de sequências distintas confirmadas. */
    uint32_t waiters;      /**< 1 enquanto existe uma espera ativa; caso contrário, 0. */
} shm_ack_record_t;

/**
 * @brief Tabela global de metadados das operações ACK.
 * @details operation_id zero representa uma posição livre. Os mapas de
 * sequências ficam nos segmentos shm_ack_bitmap_segment_t correspondentes.
 */
typedef struct {
    uint32_t magic;    /**< Identifica uma tabela inicializada. */
    uint32_t version;  /**< Versão do layout compartilhado. */
    uint64_t reserved; /**< Reservado para evolução do formato. */
    shm_ack_record_t records[SHM_ACK_OPERATION_CAPACITY];
} shm_ack_control_t;

/**
 * @brief Mapas de sequências confirmadas para dezesseis operações.
 * @details Cada mapa possui um bit por sequência, indexado a partir de zero.
 * O segmento inteiro ocupa exatamente MPMC_SHM_SEGMENT_BYTES.
 */
typedef struct {
    uint8_t bits[SHM_ACK_BITMAPS_PER_SEGMENT][SHM_ACK_BITMAP_BYTES];
} shm_ack_bitmap_segment_t;

static_assert(
    SHM_ACK_BITMAP_BYTES * SHM_ACK_BITMAPS_PER_SEGMENT == MPMC_SHM_SEGMENT_BYTES,
    "cada segmento de bitmap deve ocupar exatamente 2048 bytes"
);
static_assert(
    SHM_ACK_BITMAP_SEGMENT_COUNT * SHM_ACK_BITMAPS_PER_SEGMENT == SHM_ACK_OPERATION_CAPACITY,
    "os segmentos de bitmap devem cobrir todos os registros"
);
static_assert(
    sizeof(shm_ack_bitmap_segment_t) == MPMC_SHM_SEGMENT_BYTES,
    "segmento de bitmap deve ter exatamente 2048 bytes"
);
static_assert(
    sizeof(shm_ack_control_t) <= MPMC_SHM_SEGMENT_BYTES,
    "tabela de metadados deve caber em um segmento de 2048 bytes"
);

struct shm_backend {
    size_t producer_count;   /**< Quantidade de estruturas lógicas e registros em segments. */
    shm_segment_t *segments; /**< Vetor local com um registro por estrutura lógica. */

    int ack_control_shmid;          /**< ID do segmento global de metadados ACK. */
    shm_ack_control_t *ack_control; /**< Mapeamento local da tabela de operações. */

    int ack_bitmap_shmids[SHM_ACK_BITMAP_SEGMENT_COUNT];
    /**< IDs dos segmentos que armazenam os mapas de sequências. */
    shm_ack_bitmap_segment_t *ack_bitmap_segments[SHM_ACK_BITMAP_SEGMENT_COUNT];
    /**< Mapeamentos locais dos segmentos de bitmap. */

    int ack_semid; /**< ID do conjunto global de semáforos de ACK. */
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
 * @brief Registra uma confirmação de consumo controlada para uma mensagem.
 * @param backend Backend compartilhado.
 * @param producer_id Estrutura lógica de origem, no intervalo 1..p.
 * @param message Mensagem efetivamente consumida.
 * @details Função privada. A integração com operations.c será feita na issue #11.
 */
mpmc_result_t shm_backend_ack_notify_consumed(
    shm_backend_t *backend,
    size_t producer_id,
    const message_t *message
);

#endif
