/**
 * @file operations.c
 * @brief Operações sincronizadas de dados e estatísticas do backend SHM.
 * @ingroup backends
 * @details Usa os conjuntos de semáforos associados a cada buffer para proteger
 * as operações locais de shm_ring.c. Os vetores de aquisição e publicação são
 * atômicos individualmente; a alteração do buffer ocorre entre essas chamadas.
 * @see shm_backend.h para os contratos públicos e os efeitos de falhas.
 */
#include <assert.h>
#include <sys/sem.h>

#include "mpmc/error.h"
#include "mpmc/ipc/sem.h"
#include "mpmc/ipc/shm_backend.h"
#include "shm_backend_internal.h"
#include "shm_ring.h"

/**
 * @details O vetor inicial decrementa EMPTY e MUTEX na mesma semop(). Se qualquer
 * condição exigir espera ou a aquisição falhar, não há reserva parcial de EMPTY
 * mantida pelo vetor. Os asserts conferem os vínculos preparados por lifecycle.c.
 *
 * shm_ring_push() altera o buffer sob exclusão mútua. O vetor final incrementa
 * MUTEX e FULL atomicamente. Uma falha nessa segunda chamada é propagada sem
 * desfazer a inserção nem executar uma compensação adicional.
 */
mpmc_result_t shm_backend_send(
    shm_backend_t *backend,
    size_t producer_id,
    const message_t *message
) {
    if (backend == NULL || message == NULL) {
        return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};
    }

    if (producer_id == 0 || producer_id > backend->producer_count) {
        return (mpmc_result_t){.status = MPMC_ERR_INVALID_INDEX};
    }

    shm_segment_t *segment = &backend->segments[producer_id - 1];

    assert(segment->ring != NULL);
    assert(segment->semid >= 0);

    struct sembuf acquire[] = {
        {
            .sem_num = SHM_SEM_EMPTY,
            .sem_op = -1,
            .sem_flg = 0,
        },
        {
            .sem_num = SHM_SEM_MUTEX,
            .sem_op = -1,
            .sem_flg = 0,
        },
    };

    mpmc_result_t result = sysv_sem_apply(segment->semid, acquire, 2);
    if (result.status != MPMC_OK) return result;

    shm_ring_push(segment->ring, *message);

    struct sembuf release[] = {
        {
            .sem_num = SHM_SEM_MUTEX,
            .sem_op = 1,
            .sem_flg = 0,
        },
        {
            .sem_num = SHM_SEM_FULL,
            .sem_op = 1,
            .sem_flg = 0,
        },
    };

    return sysv_sem_apply(segment->semid, release, 2);
}

/**
 * @details O vetor inicial decrementa FULL e MUTEX atomicamente. IPC_NOWAIT é
 * aplicado somente a FULL quando blocking=false; MUTEX continua com flags zero.
 * O kernel desfaz as alterações tentativas do vetor se a aquisição não concluir.
 *
 * shm_ring_pop() copia a mensagem e incrementa consumed_total na seção crítica.
 * O vetor final libera MUTEX e incrementa EMPTY. Se a publicação falhar, out e o
 * buffer já foram alterados; a função não reinsere a mensagem nem emite ACK.
 */
mpmc_result_t shm_backend_receive(
    shm_backend_t *backend,
    size_t producer_id,
    message_t *out,
    bool blocking
) {
    if (backend == NULL || out == NULL) {
        return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};
    }

    if (producer_id == 0 || producer_id > backend->producer_count) {
        return (mpmc_result_t){.status = MPMC_ERR_INVALID_INDEX};
    }

    shm_segment_t *segment = &backend->segments[producer_id - 1];

    assert(segment->ring != NULL);
    assert(segment->semid >= 0);

    short flags = blocking ? 0 : IPC_NOWAIT;

    struct sembuf acquire[] = {
        {
            .sem_num = SHM_SEM_FULL, // Reserva um item no mesmo vetor que adquire MUTEX.
            .sem_op = -1,
            .sem_flg = flags,
        },
        {
            .sem_num = SHM_SEM_MUTEX,
            .sem_op = -1,
            .sem_flg = 0,
        },
    };

    mpmc_result_t result = sysv_sem_apply(segment->semid, acquire, 2);
    if (result.status != MPMC_OK) return result;

    *out = shm_ring_pop(segment->ring);

    struct sembuf release[] = {
        {
            .sem_num = SHM_SEM_MUTEX,
            .sem_op = 1,
            .sem_flg = 0,
        },
        {
            .sem_num =
                SHM_SEM_EMPTY, // Publica a vaga após a retirada, junto da liberação de MUTEX.
            .sem_op = 1,
            .sem_flg = 0,
        },
    };

    result = sysv_sem_apply(segment->semid, release, 2);
    if (result.status != MPMC_OK) return result;

    return shm_backend_ack_notify_consumed(backend, producer_id, out);
}

/**
 * @details Adquire apenas MUTEX para copiar consumed_total a uma variável local.
 * O parâmetro out_count só é escrito depois que a liberação do mutex retorna
 * sucesso; falhas em qualquer uma das duas operações preservam a saída.
 */
mpmc_result_t shm_backend_consumed_count(
    shm_backend_t *backend,
    size_t producer_id,
    uint64_t *out_count
) {
    if (backend == NULL || out_count == NULL) {
        return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};
    }

    if (producer_id == 0 || producer_id > backend->producer_count) {
        return (mpmc_result_t){.status = MPMC_ERR_INVALID_INDEX};
    }

    shm_segment_t *segment = &backend->segments[producer_id - 1];

    assert(segment->ring != NULL);
    assert(segment->semid >= 0);

    mpmc_result_t result = sysv_sem_change(segment->semid, SHM_SEM_MUTEX, -1, false);
    if (result.status != MPMC_OK) return result;

    uint64_t count = segment->ring->consumed_total;

    result = sysv_sem_change(segment->semid, SHM_SEM_MUTEX, 1, false);
    if (result.status != MPMC_OK) return result;

    *out_count = count;
    return (mpmc_result_t){.status = MPMC_OK};
}

/** @details A propriedade exclusiva do pai após waitpid() substitui o mutex. */
mpmc_result_t shm_backend_final_count(
    shm_backend_t *backend,
    size_t producer_id,
    uint64_t *out_count
) {
    if (backend == nullptr || out_count == nullptr)
        return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};
    if (producer_id == 0 || producer_id > backend->producer_count)
        return (mpmc_result_t){.status = MPMC_ERR_INVALID_INDEX};
    *out_count = backend->segments[producer_id - 1].ring->consumed_total;
    return (mpmc_result_t){.status = MPMC_OK};
}
