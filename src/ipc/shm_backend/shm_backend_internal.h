/**
 * @file shm_backend_internal.h
 * @brief Representação privada do contexto do backend de memória compartilhada.
 * @ingroup backends
 * @details Reúne os tipos usados pelos arquivos de implementação do backend.
 * O cabeçalho público shm_backend.h mantém shm_backend_t como tipo opaco para
 * a fachada de transporte; este arquivo descreve seu estado local completo.
 * lifecycle.c, operations.c e ack.c usam estes tipos para compartilhar a mesma
 * representação; shm_ring.h mantém o layout do buffer sem depender do contexto.
 * @see @ref architecture para a divisão das responsabilidades internas.
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
    shm_ring_t *ring; /**< Endereço associado neste processo; nullptr antes da associação. */
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

/** @brief Registros ativos de ACK; comandos adicionais aguardam uma vaga. */
#define SHM_ACK_OPERATION_CAPACITY 64

/** @brief Índices do conjunto global de semáforos de ACK. */
enum {
    SHM_ACK_SEM_MUTEX,
    SHM_ACK_SEM_AVAILABLE, /**< Registros livres; produtores aguardam uma vaga sem manter o mutex.
                            */
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

/**
 * @brief Cria e inicializa os recursos globais de controle ACK.
 * @param backend Estado local com IDs -1 e associação nula antes da criação.
 * @return Estrutura mpmc_result_t com um dos estados:
 * - MPMC_OK: segmento, mutex, vagas e conclusões inicializados; system_errno zero.
 * - MPMC_ERR_INVALID_ARG: backend nulo, com errno zero, ou argumentos rejeitados
 *   por shmget(), shmat(), semget() ou semctl(SETVAL), com errno preservado.
 * - MPMC_ERR_STATE: o backend já contém recursos de ACK; system_errno zero.
 * - MPMC_ERR_SHUTDOWN: recurso removido durante inicialização; errno EIDRM preservado.
 * - MPMC_ERR_SYS: outra falha dos wrappers de SHM/SEM; errno da syscall preservado.
 * @post IDs parciais continuam disponíveis para o rollback de lifecycle.c.
 */
mpmc_result_t shm_backend_ack_resources_create(shm_backend_t *backend);

/**
 * @brief Desassocia e, quando solicitado, remove os recursos globais de ACK.
 * @param backend Instância do backend.
 * @param remove_global Se true, também solicita IPC_RMID dos recursos.
 * @return Estrutura mpmc_result_t com um dos estados:
 * - MPMC_OK: liberações solicitadas concluídas; system_errno zero.
 * - MPMC_ERR_INVALID_ARG: backend nulo, com errno zero, ou EINVAL de shmdt(),
 *   semctl(IPC_RMID) ou shmctl(IPC_RMID), com errno preservado.
 * - MPMC_ERR_SHUTDOWN: IPC_RMID encontrou recurso removido; errno EIDRM preservado.
 * - MPMC_ERR_SYS: outra falha de liberação; errno da primeira falha preservado.
 * @details Visita todos os recursos. O observador comum recebe cada falha;
 * IDs/mapeamento só são invalidados quando sua liberação conclui.
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
 * @return Estrutura mpmc_result_t com um dos estados:
 * - MPMC_OK: consumo registrado ou mensagem sem lote ativo; system_errno zero.
 * - MPMC_ERR_INVALID_ARG: backend/mensagem nulos, com errno zero, ou argumento
 *   rejeitado por semop(), com errno preservado.
 * - MPMC_ERR_INVALID_INDEX: índice lógico fora de 1..p; system_errno zero.
 * - MPMC_ERR_STATE: controle ACK não inicializado; system_errno zero.
 * - MPMC_ERR_INTERRUPTED: semop() interrompida; errno EINTR preservado.
 * - MPMC_ERR_SHUTDOWN: conjunto removido; errno EIDRM preservado.
 * - MPMC_ERR_SYS: outra falha em semop(); errno capturado é preservado.
 * @warning A mensagem já saiu do ring; em falha, não repetir o recebimento.
 */
mpmc_result_t shm_backend_ack_notify_consumed(
    shm_backend_t *backend,
    size_t producer_id,
    const message_t *message
);

#endif
