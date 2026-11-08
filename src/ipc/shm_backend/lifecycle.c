/**
 * @file lifecycle.c
 * @brief Implementação do ciclo de vida dos segmentos de memória compartilhada.
 * @ingroup backends
 *
 * @details Mantém os identificadores e associações locais dos segmentos System V.
 * A limpeza percorre todos os recursos e comunica cada falha à callback fornecida
 * pelo chamador, sem manter uma lista de diagnósticos nem imprimir mensagens.
 * @see shm_backend.h para os contratos públicos das funções.
 */

#include <errno.h>
#include <stddef.h>
#include <stdlib.h>

#include "mpmc/ipc/shm.h"
#include "shm_backend_internal.h"

static mpmc_result_t shm_backend_cleanup(
    shm_backend_t *backend, bool remove_global, const shm_cleanup_reporter_t *reporter
);

/**
 * @details Aloca o estado com malloc() e o vetor de segmentos com calloc().
 * Inicializa todos os identificadores com -1 antes de criar os segmentos, para
 * distinguir entradas ainda não criadas do identificador válido zero.
 *
 * Cada entrada recebe um segmento de 2048 bytes por sysv_shm_create() e uma
 * associação por sysv_shm_attach(). Após cada associação, shm_ring_init() inicializa
 * o cabeçalho no mapeamento. O backend só é publicado em out depois que todas as
 * entradas forem criadas, associadas e inicializadas.
 *
 * Em falha, delega o rollback a shm_backend_destroy(), que aceita o vetor ainda
 * nulo quando calloc() falha. O resultado original, incluindo system_errno, é
 * preservado; falhas secundárias são comunicadas por reporter.
 */
mpmc_result_t shm_backend_create(
    shm_backend_t **out, size_t producer_count, const shm_cleanup_reporter_t *reporter
) {
    if (out == NULL) return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};
    *out = NULL;

    if (producer_count == 0) return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};

    shm_backend_t *backend = malloc(sizeof(*backend));

    if (backend == NULL) {
        return (mpmc_result_t){
            .status = MPMC_ERR_SYS,
            .system_errno = errno,
        };
    }

    backend->producer_count = producer_count;
    backend->segments = calloc(producer_count, sizeof(*backend->segments));
    mpmc_result_t result;

    if (backend->segments == NULL) {
        result = (mpmc_result_t){
            .status = MPMC_ERR_SYS,
            .system_errno = errno,
        };
        goto cleanup;
    }

    for (size_t i = 0; i < producer_count; i++) {
        backend->segments[i] = (shm_segment_t){
            .shmid = -1,
            .ring = NULL,
        };
    }

    for (size_t i = 0; i < producer_count; i++) {
        shm_segment_t *segment = &backend->segments[i];

        result = sysv_shm_create(MPMC_SHM_SEGMENT_BYTES, &segment->shmid);

        if (result.status != MPMC_OK) goto cleanup;

        void *address;
        result = sysv_shm_attach(segment->shmid, &address);

        if (result.status != MPMC_OK) goto cleanup;

        segment->ring = address;
        shm_ring_init(segment->ring);
    }

    *out = backend;
    return (mpmc_result_t){.status = MPMC_OK};

cleanup:
    (void)shm_backend_destroy(backend, reporter);
    return result;
}

mpmc_result_t
shm_backend_send(shm_backend_t *backend, size_t producer_id, const message_t *message) {
    (void)backend;
    (void)producer_id;
    (void)message;
    return (mpmc_result_t){.status = MPMC_ERR_STATE};
}

mpmc_result_t
shm_backend_receive(shm_backend_t *backend, size_t producer_id, message_t *out, bool blocking) {
    (void)backend;
    (void)producer_id;
    (void)out;
    (void)blocking;
    return (mpmc_result_t){.status = MPMC_ERR_STATE};
}

mpmc_result_t
shm_backend_ack_begin(shm_backend_t *backend, uint64_t operation_id, unsigned expected) {
    (void)backend;
    (void)operation_id;
    (void)expected;
    return (mpmc_result_t){.status = MPMC_ERR_STATE};
}

mpmc_result_t shm_backend_ack_wait(shm_backend_t *backend, uint64_t operation_id) {
    (void)backend;
    (void)operation_id;
    return (mpmc_result_t){.status = MPMC_ERR_STATE};
}

mpmc_result_t shm_backend_ack_end(shm_backend_t *backend, uint64_t operation_id) {
    (void)backend;
    (void)operation_id;
    return (mpmc_result_t){.status = MPMC_ERR_STATE};
}

mpmc_result_t
shm_backend_consumed_count(shm_backend_t *backend, size_t producer_id, uint64_t *out_count) {
    if (backend == NULL || out_count == NULL || producer_id == 0u
        || producer_id > backend->producer_count) {
        return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};
    }
    *out_count = 0u;
    return (mpmc_result_t){.status = MPMC_OK};
}

/**
 * @details Compartilha shm_backend_cleanup() com shm_backend_destroy(), usando
 * remove_global=false para preservar os segmentos globais do supervisor.
 */
mpmc_result_t
shm_backend_worker_close(shm_backend_t *backend, const shm_cleanup_reporter_t *reporter) {
    return shm_backend_cleanup(backend, false, reporter);
}

/**
 * @details Delega a shm_backend_cleanup() com remove_global=true, inclusive no
 * rollback de shm_backend_create(). A liberação do estado local ocorre nessa
 * rotina comum, uma única vez.
 */
mpmc_result_t shm_backend_destroy(shm_backend_t *backend, const shm_cleanup_reporter_t *reporter) {
    return shm_backend_cleanup(backend, true, reporter);
}

/**
 * @brief Preserva o primeiro erro de cleanup e comunica a falha individual.
 * @param[in,out] first Resultado agregado, atualizado somente no primeiro erro.
 * @param current Resultado da operação de liberação recém-executada.
 * @param reporter Destino opcional dos diagnósticos.
 * @param operation Operação de liberação executada.
 * @param producer_id Índice lógico da estrutura, no intervalo 1..p.
 * @param shmid Identificador do segmento envolvido.
 * @details Ignora resultados MPMC_OK. Para cada falha, preserva o primeiro erro
 * agregado antes de chamar a callback. A expressão on_error(&error, user_data)
 * executa a função fornecida pelo chamador com um registro temporário na pilha.
 *
 * Os campos são copiados antes dessa chamada, que pode alterar errno. A callback
 * pode copiar o registro para armazenamento; seu retorno não altera o resultado
 * agregado nem interrompe a visita aos demais recursos.
 */
static void record_cleanup_error(
    mpmc_result_t *first,
    mpmc_result_t current,
    const shm_cleanup_reporter_t *reporter,
    shm_cleanup_operation_t operation,
    size_t producer_id,
    int shmid
) {
    if (current.status == MPMC_OK) return;
    if (first->status == MPMC_OK) {
        *first = current;
    }
    if (reporter != NULL && reporter->on_error != NULL) {
        const shm_cleanup_error_t error = {
            .operation = operation,
            .producer_id = producer_id,
            .shmid = shmid,
            .result = current,
        };
        reporter->on_error(&error, reporter->user_data);
    }
}

/**
 * @brief Libera o estado local e, opcionalmente, solicita a remoção global.
 * @param backend Instância completa, parcialmente inicializada ou nula.
 * @param remove_global Se true, também solicita IPC_RMID para cada segmento criado.
 * @param reporter Destino opcional de todas as falhas individuais.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: nenhuma operação de liberação falhou, inclusive para backend nulo;
 *   system_errno é zero.
 * - MPMC_ERR_SYS: falha em shmdt() ou shmctl(IPC_RMID); system_errno preserva o errno
 *   da primeira falha. As demais falhas também são comunicadas por reporter.
 * @details Visita as estruturas em ordem, tentando detach antes de IPC_RMID.
 * Cada falha gera uma chamada de record_cleanup_error(), que preserva o primeiro
 * erro para retorno e comunica todos os diagnósticos ao reporter habilitado.
 *
 * O vetor nulo indica falha de alocação anterior à criação de segmentos. A rotina
 * sempre libera o vetor e a instância, sem alocar memória para os diagnósticos.
 * Backend nulo retorna sucesso sem executar operações ou chamar a callback.
 */
static mpmc_result_t shm_backend_cleanup(
    shm_backend_t *backend, bool remove_global, const shm_cleanup_reporter_t *reporter
) {
    if (backend == NULL) return (mpmc_result_t){.status = MPMC_OK};
    mpmc_result_t result = {.status = MPMC_OK};

    if (backend->segments != NULL) {
        for (size_t i = 0; i < backend->producer_count; i++) {
            shm_segment_t *segment = &backend->segments[i];

            if (segment->ring != NULL) {
                mpmc_result_t current = sysv_shm_detach(segment->ring);
                record_cleanup_error(
                    &result, current, reporter, SHM_CLEANUP_DETACH, i + 1, segment->shmid
                );
            }

            if (remove_global && segment->shmid >= 0) {
                mpmc_result_t current = sysv_shm_remove(segment->shmid);
                record_cleanup_error(
                    &result, current, reporter, SHM_CLEANUP_REMOVE, i + 1, segment->shmid
                );
            }
        }
    }

    free(backend->segments);
    free(backend);

    return result;
}
