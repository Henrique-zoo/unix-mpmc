/**
 * @file ack.c
 * @brief Conclusão de lotes por contador compartilhado e notificação final.
 * @ingroup backends
 * @details Um mutex protege a tabela. Cada posição tem um semáforo de
 * conclusão, incrementado somente quando remaining passa de um para zero.
 * @see shm_backend.h para os contratos públicos.
 */
#include <string.h>
#include <sys/sem.h>
#include <unistd.h>

#include "mpmc/diagnostic.h"
#include "mpmc/ipc/sem.h"
#include "mpmc/ipc/shm.h"
#include "shm_backend_internal.h"

static constexpr uint32_t SHM_ACK_CONTROL_MAGIC = UINT32_C(0x53484D41);

static void ack_keep_first(mpmc_result_t *first, mpmc_result_t current);
static mpmc_result_t ack_validate_backend(const shm_backend_t *backend);
static mpmc_result_t ack_lock(shm_backend_t *backend);
static mpmc_result_t ack_unlock(shm_backend_t *backend);
static mpmc_result_t ack_unlock_preserving(shm_backend_t *backend, mpmc_result_t first);
static size_t ack_find_operation(const shm_backend_t *backend, uint64_t operation_id);
static mpmc_result_t ack_drain_notification(shm_backend_t *backend, size_t slot);
static mpmc_result_t ack_finish_wait(
    shm_backend_t *backend,
    uint64_t operation_id,
    mpmc_result_t first
);

/**
 * @details Identificadores parciais permanecem no backend para o rollback
 * de lifecycle.c. A tabela usa um segmento; não existem segmentos de bitmap.
 */
mpmc_result_t shm_backend_ack_resources_create(shm_backend_t *backend) {
    mpmc_diagnostic_scope_t diagnostic_scope;
    mpmc_diagnostic_begin(&diagnostic_scope, __func__, false, false);

    if (backend == nullptr)
        return mpmc_diagnostic_end(
            &diagnostic_scope, (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG}
        );

    if (backend->ack_control_shmid >= 0 || backend->ack_control != nullptr
        || backend->ack_semid >= 0) {
        return mpmc_diagnostic_end(&diagnostic_scope, (mpmc_result_t){.status = MPMC_ERR_STATE});
    }

    mpmc_result_t result = sysv_shm_create(MPMC_SHM_SEGMENT_BYTES, &backend->ack_control_shmid);
    if (result.status != MPMC_OK) return mpmc_diagnostic_end(&diagnostic_scope, result);

    void *address = nullptr;

    result = sysv_shm_attach(backend->ack_control_shmid, &address);
    if (result.status != MPMC_OK) return mpmc_diagnostic_end(&diagnostic_scope, result);

    backend->ack_control = address;
    memset(address, 0, MPMC_SHM_SEGMENT_BYTES);
    backend->ack_control->magic = SHM_ACK_CONTROL_MAGIC;

    result = sysv_sem_create(SHM_ACK_SEM_COUNT, &backend->ack_semid);
    if (result.status != MPMC_OK) return mpmc_diagnostic_end(&diagnostic_scope, result);

    result = sysv_sem_set(backend->ack_semid, SHM_ACK_SEM_MUTEX, 1);
    if (result.status != MPMC_OK) return mpmc_diagnostic_end(&diagnostic_scope, result);

    for (unsigned index = 0; index < SHM_ACK_OPERATION_CAPACITY; ++index) {
        result = sysv_sem_set(backend->ack_semid, SHM_ACK_SEM_NOTIFICATION_BASE + index, 0);
        if (result.status != MPMC_OK) return mpmc_diagnostic_end(&diagnostic_scope, result);
    }

    return mpmc_diagnostic_end(&diagnostic_scope, (mpmc_result_t){.status = MPMC_OK});
}

/** @details Visita todos os recursos adquiridos e preserva o primeiro erro. */
mpmc_result_t shm_backend_ack_resources_cleanup(shm_backend_t *backend, bool remove_global) {
    mpmc_diagnostic_scope_t diagnostic_scope;
    mpmc_diagnostic_begin(&diagnostic_scope, __func__, true, false);

    if (backend == nullptr)
        return mpmc_diagnostic_end(
            &diagnostic_scope, (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG}
        );

    mpmc_result_t result = {.status = MPMC_OK};

    if (backend->ack_control != nullptr) {
        uint64_t before = mpmc_diagnostic_count();
        mpmc_result_t current = sysv_shm_detach(backend->ack_control);
        mpmc_diagnostic_observe("sysv_shm_detach", current, before);

        ack_keep_first(&result, current);

        if (current.status == MPMC_OK) backend->ack_control = nullptr;
    }

    if (remove_global && backend->ack_semid >= 0) {
        uint64_t before = mpmc_diagnostic_count();
        mpmc_result_t current = sysv_sem_remove(backend->ack_semid);
        mpmc_diagnostic_observe("sysv_sem_remove", current, before);

        ack_keep_first(&result, current);

        if (current.status == MPMC_OK) backend->ack_semid = -1;
    }

    if (remove_global && backend->ack_control_shmid >= 0) {
        uint64_t before = mpmc_diagnostic_count();
        mpmc_result_t current = sysv_shm_remove(backend->ack_control_shmid);
        mpmc_diagnostic_observe("sysv_shm_remove", current, before);

        ack_keep_first(&result, current);

        if (current.status == MPMC_OK) backend->ack_control_shmid = -1;
    }

    return mpmc_diagnostic_end(&diagnostic_scope, result);
}

/**
 * @details Drena a eventual notificação final anterior antes de publicar
 * a nova identidade. Uma posição livre não possui waiter nem notificador ativo.
 */
mpmc_result_t shm_backend_ack_begin(
    shm_backend_t *backend,
    uint64_t operation_id,
    unsigned expected
) {
    mpmc_diagnostic_scope_t diagnostic_scope;
    mpmc_diagnostic_begin(&diagnostic_scope, __func__, false, false);

    if (backend == nullptr || operation_id == 0 || expected == 0 || expected > MPMC_MAX_MESSAGES) {
        return mpmc_diagnostic_end(
            &diagnostic_scope, (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG}
        );
    }

    mpmc_result_t result = ack_validate_backend(backend);
    if (result.status != MPMC_OK) return mpmc_diagnostic_end(&diagnostic_scope, result);

    result = ack_lock(backend);
    if (result.status != MPMC_OK) return mpmc_diagnostic_end(&diagnostic_scope, result);

    size_t free_slot = SHM_ACK_OPERATION_CAPACITY;

    for (size_t index = 0; index < SHM_ACK_OPERATION_CAPACITY; ++index) {
        uint64_t registered = backend->ack_control->records[index].operation_id;

        if (registered == operation_id) {
            return mpmc_diagnostic_end(
                &diagnostic_scope,
                ack_unlock_preserving(backend, (mpmc_result_t){.status = MPMC_ERR_STATE})
            );
        }

        if (registered == 0 && free_slot == SHM_ACK_OPERATION_CAPACITY) free_slot = index;
    }

    if (free_slot == SHM_ACK_OPERATION_CAPACITY) {
        return mpmc_diagnostic_end(
            &diagnostic_scope,
            ack_unlock_preserving(backend, (mpmc_result_t){.status = MPMC_ERR_NO_SPACE})
        );
    }

    result = ack_drain_notification(backend, free_slot);
    if (result.status != MPMC_OK)
        return mpmc_diagnostic_end(&diagnostic_scope, ack_unlock_preserving(backend, result));

    shm_ack_record_t *record = &backend->ack_control->records[free_slot];

    *record = (shm_ack_record_t){
        .operation_id = operation_id,
        .producer_pid = getpid(),
        .remaining = (uint16_t)expected,
    };

    result = ack_unlock(backend);

    if (result.status != MPMC_OK) {
        /* A liberação falhou atomicamente: o mutex continua adquirido. */
        *record = (shm_ack_record_t){};
        return mpmc_diagnostic_end(&diagnostic_scope, ack_unlock_preserving(backend, result));
    }

    return mpmc_diagnostic_end(&diagnostic_scope, result);
}

/**
 * @details Registra o waiter antes de liberar o mutex. O token final permanece
 * no kernel se o último consumo ocorrer antes da chamada bloqueante.
 */
mpmc_result_t shm_backend_ack_wait(shm_backend_t *backend, uint64_t operation_id) {
    mpmc_diagnostic_scope_t diagnostic_scope;
    mpmc_diagnostic_begin(&diagnostic_scope, __func__, false, false);

    if (backend == nullptr || operation_id == 0) {
        return mpmc_diagnostic_end(
            &diagnostic_scope, (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG}
        );
    }

    mpmc_result_t result = ack_validate_backend(backend);
    if (result.status != MPMC_OK) return mpmc_diagnostic_end(&diagnostic_scope, result);

    result = ack_lock(backend);
    if (result.status != MPMC_OK) return mpmc_diagnostic_end(&diagnostic_scope, result);

    size_t slot = ack_find_operation(backend, operation_id);

    if (slot == SHM_ACK_OPERATION_CAPACITY) {
        return mpmc_diagnostic_end(
            &diagnostic_scope,
            ack_unlock_preserving(backend, (mpmc_result_t){.status = MPMC_ERR_STATE})
        );
    }

    shm_ack_record_t *record = &backend->ack_control->records[slot];

    if (record->waiting) {
        return mpmc_diagnostic_end(
            &diagnostic_scope,
            ack_unlock_preserving(backend, (mpmc_result_t){.status = MPMC_ERR_STATE})
        );
    }

    if (record->remaining == 0) {
        return mpmc_diagnostic_end(
            &diagnostic_scope, ack_unlock_preserving(backend, (mpmc_result_t){.status = MPMC_OK})
        );
    }

    record->waiting = true;
    result = ack_unlock(backend);

    if (result.status != MPMC_OK) {
        record->waiting = false;
        return mpmc_diagnostic_end(&diagnostic_scope, ack_unlock_preserving(backend, result));
    }

    result = sysv_sem_change(
        backend->ack_semid, SHM_ACK_SEM_NOTIFICATION_BASE + (unsigned)slot, -1, false
    );

    return mpmc_diagnostic_end(&diagnostic_scope, ack_finish_wait(backend, operation_id, result));
}

/** @details A posição só pode ser liberada quando não existe waiter ativo. */
mpmc_result_t shm_backend_ack_end(shm_backend_t *backend, uint64_t operation_id) {
    mpmc_diagnostic_scope_t diagnostic_scope;
    mpmc_diagnostic_begin(&diagnostic_scope, __func__, true, false);

    if (backend == nullptr || operation_id == 0) {
        return mpmc_diagnostic_end(
            &diagnostic_scope, (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG}
        );
    }

    mpmc_result_t result = ack_validate_backend(backend);
    if (result.status != MPMC_OK) return mpmc_diagnostic_end(&diagnostic_scope, result);

    result = ack_lock(backend);
    if (result.status != MPMC_OK) return mpmc_diagnostic_end(&diagnostic_scope, result);

    size_t slot = ack_find_operation(backend, operation_id);

    if (slot == SHM_ACK_OPERATION_CAPACITY) {
        return mpmc_diagnostic_end(
            &diagnostic_scope, ack_unlock_preserving(backend, (mpmc_result_t){.status = MPMC_OK})
        );
    }

    shm_ack_record_t *record = &backend->ack_control->records[slot];

    if (record->waiting) {
        return mpmc_diagnostic_end(
            &diagnostic_scope,
            ack_unlock_preserving(backend, (mpmc_result_t){.status = MPMC_ERR_STATE})
        );
    }

    *record = (shm_ack_record_t){};

    return mpmc_diagnostic_end(
        &diagnostic_scope, ack_unlock_preserving(backend, (mpmc_result_t){.status = MPMC_OK})
    );
}

/**
 * @details A função é chamada uma vez por retirada efetiva. Decrementa o lote
 * sob mutex; apenas a transição para zero publica uma notificação. Se semop()
 * falhar, restaura o contador antes de tentar liberar o mutex ainda adquirido.
 */
mpmc_result_t shm_backend_ack_notify_consumed(
    shm_backend_t *backend,
    size_t producer_id,
    const message_t *message
) {
    mpmc_diagnostic_scope_t diagnostic_scope;
    mpmc_diagnostic_begin(&diagnostic_scope, __func__, false, false);

    if (backend == nullptr || message == nullptr) {
        return mpmc_diagnostic_end(
            &diagnostic_scope, (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG}
        );
    }

    if (producer_id == 0 || producer_id > backend->producer_count) {
        return mpmc_diagnostic_end(
            &diagnostic_scope, (mpmc_result_t){.status = MPMC_ERR_INVALID_INDEX}
        );
    }

    if (message->operation_id == 0)
        return mpmc_diagnostic_end(&diagnostic_scope, (mpmc_result_t){.status = MPMC_OK});

    mpmc_result_t result = ack_validate_backend(backend);
    if (result.status != MPMC_OK) return mpmc_diagnostic_end(&diagnostic_scope, result);

    result = ack_lock(backend);
    if (result.status != MPMC_OK) return mpmc_diagnostic_end(&diagnostic_scope, result);

    size_t slot = ack_find_operation(backend, message->operation_id);

    if (slot == SHM_ACK_OPERATION_CAPACITY) {
        return mpmc_diagnostic_end(
            &diagnostic_scope, ack_unlock_preserving(backend, (mpmc_result_t){.status = MPMC_OK})
        );
    }

    shm_ack_record_t *record = &backend->ack_control->records[slot];

    if (record->producer_pid != message->sender_pid || record->remaining == 0) {
        return mpmc_diagnostic_end(
            &diagnostic_scope, ack_unlock_preserving(backend, (mpmc_result_t){.status = MPMC_OK})
        );
    }

    --record->remaining;

    if (record->remaining != 0) {
        result = ack_unlock(backend);
    } else {
        struct sembuf release[] = {
            {
                .sem_num = (unsigned short)(SHM_ACK_SEM_NOTIFICATION_BASE + slot),
                .sem_op = 1,
            },
            {
                .sem_num = SHM_ACK_SEM_MUTEX,
                .sem_op = 1,
            },
        };

        result = sysv_sem_apply(backend->ack_semid, release, 2);
    }

    if (result.status != MPMC_OK) {
        ++record->remaining;
        return mpmc_diagnostic_end(&diagnostic_scope, ack_unlock_preserving(backend, result));
    }

    return mpmc_diagnostic_end(&diagnostic_scope, result);
}

/** @brief Preserva o primeiro erro durante a visita aos recursos de cleanup. */
static void ack_keep_first(mpmc_result_t *first, mpmc_result_t current) {
    if (first->status == MPMC_OK) *first = current;
}

/** @brief Confere o mapeamento local e a versão do layout compartilhado. */
static mpmc_result_t ack_validate_backend(const shm_backend_t *backend) {
    if (backend->ack_control == nullptr || backend->ack_semid < 0
        || backend->ack_control->magic != SHM_ACK_CONTROL_MAGIC) {
        return (mpmc_result_t){.status = MPMC_ERR_STATE};
    }

    return (mpmc_result_t){.status = MPMC_OK};
}

/** @brief Adquire o mutex global; não repete interrupções da operação normal. */
static mpmc_result_t ack_lock(shm_backend_t *backend) {
    return sysv_sem_change(backend->ack_semid, SHM_ACK_SEM_MUTEX, -1, false);
}

/** @brief Libera o mutex global em uma operação atômica do kernel. */
static mpmc_result_t ack_unlock(shm_backend_t *backend) {
    return sysv_sem_change(backend->ack_semid, SHM_ACK_SEM_MUTEX, 1, false);
}

/**
 * @brief Libera o mutex, preservando a primeira falha e comunicando as seguintes.
 * @details Uma falha não aplica o incremento. Tenta novamente uma vez;
 * a primeira falha permanece no resultado, mesmo se a repetição concluir.
 */
static mpmc_result_t ack_unlock_preserving(shm_backend_t *backend, mpmc_result_t first) {
    mpmc_diagnostic_scope_t diagnostic_scope;
    mpmc_diagnostic_begin(&diagnostic_scope, __func__, true, false);

    mpmc_diagnostic_cause("operação ACK", first);
    uint64_t before = mpmc_diagnostic_count();
    mpmc_result_t result = ack_unlock(backend);
    mpmc_diagnostic_observe("liberação do mutex ACK", result, before);
    if (result.status != MPMC_OK) {
        mpmc_diagnostic_cause("liberação do mutex ACK", result);
        before = mpmc_diagnostic_count();
        mpmc_result_t retry = ack_unlock(backend);
        mpmc_diagnostic_observe("segunda liberação do mutex ACK", retry, before);
        if (retry.status != MPMC_OK) {
            // Os wrappers reais comunicam cada falha individual.
            mpmc_diagnostic_cause("segunda liberação do mutex ACK", retry);
        }
        if (first.status == MPMC_OK) first = result;
    }
    return mpmc_diagnostic_end(&diagnostic_scope, first);
}

/** @brief Procura uma identidade com o mutex adquirido; capacidade indica ausência. */
static size_t ack_find_operation(const shm_backend_t *backend, uint64_t operation_id) {
    for (size_t index = 0; index < SHM_ACK_OPERATION_CAPACITY; ++index) {
        if (backend->ack_control->records[index].operation_id == operation_id) return index;
    }

    return SHM_ACK_OPERATION_CAPACITY;
}

/**
 * @brief Descarta o eventual token final de uma utilização anterior.
 * @pre Mutex adquirido e posição livre, sem waiter ativo.
 * @details Pelo protocolo existe no máximo um token; ausência é sucesso.
 */
static mpmc_result_t ack_drain_notification(shm_backend_t *backend, size_t slot) {
    mpmc_result_t result = sysv_sem_change(
        backend->ack_semid, SHM_ACK_SEM_NOTIFICATION_BASE + (unsigned)slot, -1, true
    );

    if (result.status == MPMC_ERR_WOULD_BLOCK) return (mpmc_result_t){.status = MPMC_OK};

    return result;
}

/**
 * @brief Finaliza a indicação de espera e confere a conclusão do lote.
 * @details Repete EINTR somente para adquirir o mutex necessário à limpeza.
 * Falhas de recurso nessa recuperação são propagadas sem acessar o registro.
 */
static mpmc_result_t ack_finish_wait(
    shm_backend_t *backend,
    uint64_t operation_id,
    mpmc_result_t first
) {
    mpmc_result_t result;

    do {
        result = ack_lock(backend);
    } while (result.status == MPMC_ERR_INTERRUPTED);

    if (result.status != MPMC_OK) return result;

    size_t slot = ack_find_operation(backend, operation_id);

    if (slot == SHM_ACK_OPERATION_CAPACITY) {
        return ack_unlock_preserving(backend, (mpmc_result_t){.status = MPMC_ERR_STATE});
    }

    shm_ack_record_t *record = &backend->ack_control->records[slot];

    if (!record->waiting) {
        return ack_unlock_preserving(backend, (mpmc_result_t){.status = MPMC_ERR_STATE});
    }

    record->waiting = false;

    if (first.status == MPMC_OK && record->remaining != 0) {
        first = (mpmc_result_t){.status = MPMC_ERR_STATE};
    }

    return ack_unlock_preserving(backend, first);
}
