/**
 * @file ack.c
 * @brief Registro compartilhado e sincronização das confirmações do backend SHM.
 * @ingroup backends
 *
 * @details A tabela e os bitmaps ficam em segmentos System V próprios, separados
 * dos buffers circulares. Um mutex global protege os registros e os bitmaps;
 * cada operação possui um semáforo contador para conservar notificações anteriores
 * à espera do produtor.
 */

#include <string.h>
#include <sys/sem.h>
#include <unistd.h>

#include "mpmc/ipc/sem.h"
#include "mpmc/ipc/shm.h"
#include "shm_backend_internal.h"

/** @brief Identificador do layout privado da tabela de ACK. */
#define SHM_ACK_CONTROL_MAGIC UINT32_C(0x53484D41)

/** @brief Versão do layout privado da tabela de ACK. */
#define SHM_ACK_CONTROL_VERSION 2U

/**
 * @brief Preserva o primeiro erro de uma sequência de operações.
 * @param first Resultado agregado, atualizado somente no primeiro erro.
 * @param current Resultado da operação atual.
 */
static void ack_keep_first(mpmc_result_t *first, mpmc_result_t current) {
    if (first->status == MPMC_OK && current.status != MPMC_OK) {
        *first = current;
    }
}

/**
 * @brief Valida os mapeamentos e o conjunto de semáforos do backend.
 * @param backend Instância a validar.
 * @return MPMC_OK quando todos os recursos estão associados e inicializados.
 */
static mpmc_result_t ack_validate_backend(const shm_backend_t *backend) {
    if (backend == NULL) {
        return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};
    }

    if (backend->ack_control == NULL || backend->ack_semid < 0
        || backend->ack_control->magic != SHM_ACK_CONTROL_MAGIC
        || backend->ack_control->version != SHM_ACK_CONTROL_VERSION) {
        return (mpmc_result_t){.status = MPMC_ERR_STATE};
    }

    for (size_t index = 0; index < SHM_ACK_BITMAP_SEGMENT_COUNT; index++) {
        if (backend->ack_bitmap_segments[index] == NULL) {
            return (mpmc_result_t){.status = MPMC_ERR_STATE};
        }
    }

    return (mpmc_result_t){.status = MPMC_OK};
}

/**
 * @brief Adquire o mutex global da tabela ACK.
 * @param backend Backend validado.
 */
static mpmc_result_t ack_lock(shm_backend_t *backend) {
    return sysv_sem_change(backend->ack_semid, SHM_ACK_SEM_MUTEX, -1, false);
}

/**
 * @brief Libera o mutex global da tabela ACK.
 * @param backend Backend validado.
 */
static mpmc_result_t ack_unlock(shm_backend_t *backend) {
    return sysv_sem_change(backend->ack_semid, SHM_ACK_SEM_MUTEX, 1, false);
}

/**
 * @brief Libera o mutex e prioriza falhas nessa operação.
 * @param backend Backend validado.
 * @param first Resultado da operação anterior.
 * @return Erro de liberação, se houver; caso contrário, o resultado anterior.
 */
static mpmc_result_t ack_unlock_preserving(shm_backend_t *backend, mpmc_result_t first) {
    mpmc_result_t unlock_result = ack_unlock(backend);
    if (unlock_result.status != MPMC_OK) {
        /*
         * semop() não aplica operações parcialmente quando falha.
         * Uma segunda tentativa recupera falhas transitórias de liberação.
         */
        mpmc_result_t retry_result = ack_unlock(backend);
        return retry_result.status != MPMC_OK ? retry_result : unlock_result;
    }

    return first;
}

/**
 * @brief Localiza o registro associado a um operation_id.
 * @param backend Backend com o mutex adquirido.
 * @param operation_id Identificador procurado.
 * @return Índice do registro ou SHM_ACK_OPERATION_CAPACITY se não encontrado.
 */
static size_t ack_find_operation(const shm_backend_t *backend, uint64_t operation_id) {
    for (size_t index = 0; index < SHM_ACK_OPERATION_CAPACITY; index++) {
        if (backend->ack_control->records[index].operation_id == operation_id) {
            return index;
        }
    }

    return SHM_ACK_OPERATION_CAPACITY;
}

/**
 * @brief Obtém o bitmap de uma posição da tabela.
 * @param backend Backend associado.
 * @param slot Índice do registro.
 * @return Ponteiro para os bytes do bitmap ou NULL se o índice for inválido.
 */
static uint8_t *ack_bitmap_for(shm_backend_t *backend, size_t slot) {
    if (slot >= SHM_ACK_OPERATION_CAPACITY) {
        return NULL;
    }

    size_t segment_index = slot / SHM_ACK_BITMAPS_PER_SEGMENT;
    size_t bitmap_index = slot % SHM_ACK_BITMAPS_PER_SEGMENT;

    if (backend->ack_bitmap_segments[segment_index] == NULL) {
        return NULL;
    }

    return backend->ack_bitmap_segments[segment_index]->bits[bitmap_index];
}

/**
 * @brief Descarta notificações pendentes de uma posição.
 * @details Deve ser chamada com o mutex global adquirido. A operação não bloqueia:
 * encerra quando o semáforo contador estiver vazio.
 */
static mpmc_result_t ack_drain_notifications(shm_backend_t *backend, size_t slot) {
    unsigned semaphore_index = SHM_ACK_SEM_NOTIFICATION_BASE + (unsigned)slot;

    for (;;) {
        mpmc_result_t result = sysv_sem_change(backend->ack_semid, semaphore_index, -1, true);

        if (result.status == MPMC_ERR_WOULD_BLOCK) {
            return (mpmc_result_t){.status = MPMC_OK};
        }

        if (result.status != MPMC_OK) {
            return result;
        }
    }
}

/**
 * @brief Cria e inicializa os recursos globais de controle ACK.
 * @details Cada segmento de controle e bitmap possui exatamente 2048 bytes.
 * O conjunto de semáforos contém um mutex e um contador por registro.
 *
 * @note Em caso de falha, os identificadores e associações já obtidos permanecem
 * registrados em backend para que shm_backend_ack_resources_cleanup() execute
 * o rollback de todos os recursos parcialmente criados.
 */
mpmc_result_t shm_backend_ack_resources_create(shm_backend_t *backend) {
    if (backend == NULL) {
        return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};
    }

    if (backend->ack_control_shmid >= 0 || backend->ack_control != NULL
        || backend->ack_semid >= 0) {
        return (mpmc_result_t){.status = MPMC_ERR_STATE};
    }

    for (size_t index = 0; index < SHM_ACK_BITMAP_SEGMENT_COUNT; index++) {
        if (backend->ack_bitmap_shmids[index] >= 0 || backend->ack_bitmap_segments[index] != NULL) {
            return (mpmc_result_t){.status = MPMC_ERR_STATE};
        }
    }

    mpmc_result_t result = sysv_shm_create(MPMC_SHM_SEGMENT_BYTES, &backend->ack_control_shmid);
    if (result.status != MPMC_OK) {
        return result;
    }

    void *address = NULL;
    result = sysv_shm_attach(backend->ack_control_shmid, &address);
    if (result.status != MPMC_OK) {
        return result;
    }

    backend->ack_control = address;
    memset(backend->ack_control, 0, MPMC_SHM_SEGMENT_BYTES);
    backend->ack_control->magic = SHM_ACK_CONTROL_MAGIC;
    backend->ack_control->version = SHM_ACK_CONTROL_VERSION;

    for (size_t index = 0; index < SHM_ACK_BITMAP_SEGMENT_COUNT; index++) {
        result = sysv_shm_create(MPMC_SHM_SEGMENT_BYTES, &backend->ack_bitmap_shmids[index]);
        if (result.status != MPMC_OK) {
            return result;
        }

        address = NULL;
        result = sysv_shm_attach(backend->ack_bitmap_shmids[index], &address);
        if (result.status != MPMC_OK) {
            return result;
        }

        backend->ack_bitmap_segments[index] = address;
        memset(address, 0, MPMC_SHM_SEGMENT_BYTES);
    }

    result = sysv_sem_create(SHM_ACK_SEM_COUNT, &backend->ack_semid);
    if (result.status != MPMC_OK) {
        return result;
    }

    result = sysv_sem_set(backend->ack_semid, SHM_ACK_SEM_MUTEX, 1);
    if (result.status != MPMC_OK) {
        return result;
    }

    for (unsigned index = 0; index < SHM_ACK_OPERATION_CAPACITY; index++) {
        result = sysv_sem_set(backend->ack_semid, SHM_ACK_SEM_NOTIFICATION_BASE + index, 0);
        if (result.status != MPMC_OK) {
            return result;
        }
    }

    return (mpmc_result_t){.status = MPMC_OK};
}

/**
 * @brief Desassocia e, quando solicitado, remove os recursos globais de ACK.
 * @details Tenta todas as operações, mesmo após uma falha, e retorna a primeira.
 * Recursos parcialmente inicializados são aceitos.
 */
mpmc_result_t shm_backend_ack_resources_cleanup(shm_backend_t *backend, bool remove_global) {
    if (backend == NULL) {
        return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};
    }

    mpmc_result_t result = {.status = MPMC_OK};

    if (backend->ack_control != NULL) {
        mpmc_result_t current = sysv_shm_detach(backend->ack_control);
        ack_keep_first(&result, current);
        if (current.status == MPMC_OK) {
            backend->ack_control = NULL;
        }
    }

    for (size_t index = 0; index < SHM_ACK_BITMAP_SEGMENT_COUNT; index++) {
        if (backend->ack_bitmap_segments[index] != NULL) {
            mpmc_result_t current = sysv_shm_detach(backend->ack_bitmap_segments[index]);
            ack_keep_first(&result, current);
            if (current.status == MPMC_OK) {
                backend->ack_bitmap_segments[index] = NULL;
            }
        }
    }

    if (remove_global && backend->ack_semid >= 0) {
        mpmc_result_t current = sysv_sem_remove(backend->ack_semid);
        ack_keep_first(&result, current);
        if (current.status == MPMC_OK) {
            backend->ack_semid = -1;
        }
    }

    if (remove_global && backend->ack_control_shmid >= 0) {
        mpmc_result_t current = sysv_shm_remove(backend->ack_control_shmid);
        ack_keep_first(&result, current);
        if (current.status == MPMC_OK) {
            backend->ack_control_shmid = -1;
        }
    }

    if (remove_global) {
        for (size_t index = 0; index < SHM_ACK_BITMAP_SEGMENT_COUNT; index++) {
            if (backend->ack_bitmap_shmids[index] >= 0) {
                mpmc_result_t current = sysv_shm_remove(backend->ack_bitmap_shmids[index]);
                ack_keep_first(&result, current);
                if (current.status == MPMC_OK) {
                    backend->ack_bitmap_shmids[index] = -1;
                }
            }
        }
    }

    return result;
}

/**
 * @brief Registra uma operação produtora na tabela compartilhada.
 * @details O slot só é publicado depois de limpar o bitmap e o contador de
 * notificações que possam ter permanecido de uma utilização anterior.
 */
mpmc_result_t shm_backend_ack_begin(
    shm_backend_t *backend,
    uint64_t operation_id,
    unsigned expected
) {
    if (backend == NULL || operation_id == 0 || expected == 0 || expected > MPMC_MAX_MESSAGES) {
        return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};
    }

    mpmc_result_t result = ack_validate_backend(backend);
    if (result.status != MPMC_OK) {
        return result;
    }

    result = ack_lock(backend);
    if (result.status != MPMC_OK) {
        return result;
    }

    size_t free_slot = SHM_ACK_OPERATION_CAPACITY;

    for (size_t index = 0; index < SHM_ACK_OPERATION_CAPACITY; index++) {
        uint64_t registered_id = backend->ack_control->records[index].operation_id;

        if (registered_id == operation_id) {
            return ack_unlock_preserving(backend, (mpmc_result_t){.status = MPMC_ERR_STATE});
        }

        if (registered_id == 0 && free_slot == SHM_ACK_OPERATION_CAPACITY) {
            free_slot = index;
        }
    }

    if (free_slot == SHM_ACK_OPERATION_CAPACITY) {
        return ack_unlock_preserving(backend, (mpmc_result_t){.status = MPMC_ERR_NO_SPACE});
    }

    result = ack_drain_notifications(backend, free_slot);
    if (result.status != MPMC_OK) {
        return ack_unlock_preserving(backend, result);
    }

    uint8_t *bitmap = ack_bitmap_for(backend, free_slot);
    if (bitmap == NULL) {
        return ack_unlock_preserving(backend, (mpmc_result_t){.status = MPMC_ERR_STATE});
    }

    memset(bitmap, 0, SHM_ACK_BITMAP_BYTES);
    backend->ack_control->records[free_slot] = (shm_ack_record_t){
        .operation_id = operation_id,
        .producer_pid = getpid(),
        .expected = (uint16_t)expected,
        .received = 0,
        .waiters = 0,
    };

    result = ack_unlock(backend);
    if (result.status != MPMC_OK) {
        backend->ack_control->records[free_slot] = (shm_ack_record_t){0};
        memset(bitmap, 0, SHM_ACK_BITMAP_BYTES);
        return ack_unlock_preserving(backend, result);
    }

    return (mpmc_result_t){.status = MPMC_OK};
}

/**
 * @brief Libera o registro de waiter de uma operação.
 * @details Deve ser chamada quando a espera termina com erro, depois de
 * recuperar o mutex global.
 */
static mpmc_result_t ack_waiter_release(
    shm_backend_t *backend,
    uint64_t operation_id,
    mpmc_result_t first
) {
    mpmc_result_t result = ack_lock(backend);
    if (result.status != MPMC_OK) {
        return result;
    }

    size_t slot = ack_find_operation(backend, operation_id);
    if (slot == SHM_ACK_OPERATION_CAPACITY) {
        return ack_unlock_preserving(backend, (mpmc_result_t){.status = MPMC_ERR_STATE});
    }

    shm_ack_record_t *record = &backend->ack_control->records[slot];
    if (record->waiters != 1U) {
        return ack_unlock_preserving(backend, (mpmc_result_t){.status = MPMC_ERR_STATE});
    }

    record->waiters = 0;

    return ack_unlock_preserving(backend, first);
}

/**
 * @brief Aguarda as confirmações de uma operação registrada.
 * @details Apenas um processo pode aguardar cada operação. O registro
 * permanece protegido contra reutilização durante toda a espera.
 */
mpmc_result_t shm_backend_ack_wait(shm_backend_t *backend, uint64_t operation_id) {
    if (backend == NULL || operation_id == 0) {
        return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};
    }

    mpmc_result_t result = ack_validate_backend(backend);
    if (result.status != MPMC_OK) {
        return result;
    }

    result = ack_lock(backend);
    if (result.status != MPMC_OK) {
        return result;
    }

    size_t slot = ack_find_operation(backend, operation_id);
    if (slot == SHM_ACK_OPERATION_CAPACITY) {
        return ack_unlock_preserving(backend, (mpmc_result_t){.status = MPMC_ERR_STATE});
    }

    shm_ack_record_t *record = &backend->ack_control->records[slot];

    if (record->waiters != 0U) {
        return ack_unlock_preserving(backend, (mpmc_result_t){.status = MPMC_ERR_STATE});
    }

    record->waiters = 1U;

    result = ack_unlock(backend);
    if (result.status != MPMC_OK) {
        /*
         * semop() é atômico: como o incremento falhou, ainda possuímos
         * o mutex e podemos desfazer o registro do waiter com segurança.
         */
        record->waiters = 0U;

        mpmc_result_t retry_result = ack_unlock(backend);
        return retry_result.status != MPMC_OK ? retry_result : result;
    }

    unsigned notification_index = SHM_ACK_SEM_NOTIFICATION_BASE + (unsigned)slot;

    for (;;) {
        result = ack_lock(backend);
        if (result.status != MPMC_OK) {
            return ack_waiter_release(backend, operation_id, result);
        }

        slot = ack_find_operation(backend, operation_id);
        if (slot == SHM_ACK_OPERATION_CAPACITY) {
            return ack_unlock_preserving(backend, (mpmc_result_t){.status = MPMC_ERR_STATE});
        }

        record = &backend->ack_control->records[slot];

        if (record->received >= record->expected) {
            record->waiters = 0U;

            result = ack_unlock(backend);
            if (result.status != MPMC_OK) {
                mpmc_result_t retry_result = ack_unlock(backend);
                return retry_result.status != MPMC_OK ? retry_result : result;
            }

            return result;
        }

        result = ack_unlock(backend);
        if (result.status != MPMC_OK) {
            /*
             * A liberação falhou e o mutex continua adquirido.
             * Retiramos o waiter antes de tentar liberar novamente.
             */
            record->waiters = 0U;

            mpmc_result_t retry_result = ack_unlock(backend);
            return retry_result.status != MPMC_OK ? retry_result : result;
        }

        result = sysv_sem_change(backend->ack_semid, notification_index, -1, false);
        if (result.status != MPMC_OK) {
            return ack_waiter_release(backend, operation_id, result);
        }
    }
}

/**
 * @brief Encerra uma operação e libera seu registro para reutilização.
 * @details Operações desconhecidas já estão encerradas e retornam sucesso.
 */
mpmc_result_t shm_backend_ack_end(shm_backend_t *backend, uint64_t operation_id) {
    if (backend == NULL || operation_id == 0) {
        return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};
    }

    mpmc_result_t result = ack_validate_backend(backend);
    if (result.status != MPMC_OK) {
        return result;
    }

    result = ack_lock(backend);
    if (result.status != MPMC_OK) {
        return result;
    }

    size_t slot = ack_find_operation(backend, operation_id);
    if (slot == SHM_ACK_OPERATION_CAPACITY) {
        return ack_unlock_preserving(backend, (mpmc_result_t){.status = MPMC_OK});
    }

    if (backend->ack_control->records[slot].waiters != 0U) {
        return ack_unlock_preserving(backend, (mpmc_result_t){.status = MPMC_ERR_STATE});
    }

    uint8_t *bitmap = ack_bitmap_for(backend, slot);
    if (bitmap == NULL) {
        return ack_unlock_preserving(backend, (mpmc_result_t){.status = MPMC_ERR_STATE});
    }

    memset(bitmap, 0, SHM_ACK_BITMAP_BYTES);
    backend->ack_control->records[slot] = (shm_ack_record_t){0};

    result = ack_unlock(backend);
    if (result.status != MPMC_OK) {
        /*
         * A falha de semop() não aplicou a liberação do mutex. O registro
         * permanece encerrado; tentamos liberar o mutex mais uma vez para
         * evitar deixar a posição ocupada após uma falha transitória.
         */
        mpmc_result_t retry_result = ack_unlock(backend);
        return retry_result.status != MPMC_OK ? retry_result : result;
    }

    return result;
}

/**
 * @brief Conta uma confirmação válida de mensagem efetivamente consumida.
 * @param backend Backend compartilhado.
 * @param producer_id Estrutura lógica na qual a mensagem foi consumida.
 * @param message Mensagem retirada do buffer.
 *
 * @details Confirmações desconhecidas, duplicadas ou incompatíveis com o PID e
 * intervalo de sequências da operação são ignoradas. A notificação e a liberação
 * do mutex são publicadas no mesmo vetor atômico de semop().
 */
mpmc_result_t shm_backend_ack_notify_consumed(
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

    if (message->operation_id == 0) {
        return (mpmc_result_t){.status = MPMC_OK};
    }

    mpmc_result_t result = ack_validate_backend(backend);
    if (result.status != MPMC_OK) {
        return result;
    }

    result = ack_lock(backend);
    if (result.status != MPMC_OK) {
        return result;
    }

    size_t slot = ack_find_operation(backend, message->operation_id);
    if (slot == SHM_ACK_OPERATION_CAPACITY) {
        return ack_unlock_preserving(backend, (mpmc_result_t){.status = MPMC_OK});
    }

    shm_ack_record_t *record = &backend->ack_control->records[slot];

    if (message->sender_pid != record->producer_pid || message->sequence == 0
        || message->sequence > record->expected || record->received >= record->expected) {
        return ack_unlock_preserving(backend, (mpmc_result_t){.status = MPMC_OK});
    }

    uint8_t *bitmap = ack_bitmap_for(backend, slot);
    if (bitmap == NULL) {
        return ack_unlock_preserving(backend, (mpmc_result_t){.status = MPMC_ERR_STATE});
    }

    uint32_t bit_index = message->sequence - 1U;
    size_t byte_index = bit_index / 8U;
    uint8_t bit_mask = (uint8_t)(1U << (bit_index % 8U));

    if ((bitmap[byte_index] & bit_mask) != 0) {
        return ack_unlock_preserving(backend, (mpmc_result_t){.status = MPMC_OK});
    }

    bitmap[byte_index] |= bit_mask;
    record->received++;

    struct sembuf release[] = {
        {
            .sem_num = (unsigned short)(SHM_ACK_SEM_NOTIFICATION_BASE + slot),
            .sem_op = 1,
            .sem_flg = 0,
        },
        {
            .sem_num = SHM_ACK_SEM_MUTEX,
            .sem_op = 1,
            .sem_flg = 0,
        },
    };

    result = sysv_sem_apply(backend->ack_semid, release, 2);
    if (result.status != MPMC_OK) {
        bitmap[byte_index] &= (uint8_t)~bit_mask;
        record->received--;

        return ack_unlock_preserving(backend, result);
    }

    return (mpmc_result_t){.status = MPMC_OK};
}
