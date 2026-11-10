/**
 * @file lifecycle.c
 * @brief Ciclo de vida dos segmentos e conjuntos de semáforos do backend SHM.
 * @ingroup backends
 *
 * @details Mantém os identificadores dos objetos System V e as associações locais.
 * A limpeza percorre todos os recursos e comunica cada falha à callback fornecida
 * pelo chamador, sem manter uma lista de diagnósticos nem imprimir mensagens.
 * @see shm_backend.h para os contratos públicos das funções.
 */
#include <errno.h>
#include <stddef.h>
#include <stdlib.h>

#include "mpmc/diagnostic.h"
#include "mpmc/ipc/sem.h"
#include "mpmc/ipc/shm.h"
#include "shm_backend_internal.h"

static mpmc_result_t shm_backend_cleanup(
    shm_backend_t *backend,
    size_t segment_count,
    bool remove_global,
    const shm_cleanup_reporter_t *reporter
);

/**
 * @details Aloca o estado com malloc() e o vetor de segmentos com calloc().
 * Prepara cada entrada visitada com identificadores -1 e mapeamento NULL antes
 * de criar seus recursos, distinguindo ausência de recurso do identificador zero.
 *
 * Cada entrada recebe um segmento de 2048 bytes por sysv_shm_create() e uma
 * associação por sysv_shm_attach(). Após cada associação, shm_ring_init() inicializa
 * o cabeçalho no mapeamento. Em seguida, cria o conjunto de três semáforos e
 * inicializa MUTEX=1, EMPTY=capacity e FULL=0. O backend só é publicado em out
 * depois que todas as entradas forem preparadas.
 *
 * Se calloc() falhar, captura errno antes de liberar a instância. Uma falha no
 * laço delega o rollback diretamente a shm_backend_cleanup(), com i+1 entradas:
 * inclui a entrada parcial atual e exclui as posições ainda não visitadas.
 * O resultado original é preservado; falhas secundárias são comunicadas por reporter.
 */
mpmc_result_t shm_backend_create(
    shm_backend_t **out,
    size_t producer_count,
    const shm_cleanup_reporter_t *reporter
) {
    mpmc_diagnostic_scope_t diagnostic_scope;
    mpmc_diagnostic_begin(&diagnostic_scope, __func__, false, false);

    if (out == NULL)
        return mpmc_diagnostic_end(
            &diagnostic_scope, (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG}
        );
    *out = NULL;

    if (producer_count == 0)
        return mpmc_diagnostic_end(
            &diagnostic_scope, (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG}
        );

    shm_backend_t *backend = malloc(sizeof(*backend));

    if (backend == NULL) {
        return mpmc_diagnostic_end(
            &diagnostic_scope,
            (mpmc_result_t){
                .status = MPMC_ERR_SYS,
                .system_errno = errno,
            }
        );
    }

    *backend = (shm_backend_t){
        .producer_count = producer_count,
        .ack_control_shmid = -1,
        .ack_control = NULL,
        .ack_semid = -1,
    };

    backend->segments = calloc(producer_count, sizeof(*backend->segments));

    if (backend->segments == NULL) {
        int saved_errno = errno;
        free(backend);

        return mpmc_diagnostic_end(
            &diagnostic_scope,
            (mpmc_result_t){
                .status = MPMC_ERR_SYS,
                .system_errno = saved_errno,
            }
        );
    }

    mpmc_result_t result;
    size_t i;

    for (i = 0; i < producer_count; i++) {
        shm_segment_t *segment = &backend->segments[i];

        *segment = (shm_segment_t){
            .shmid = -1,
            .semid = -1,
            .ring = NULL,
        };

        result = sysv_shm_create(MPMC_SHM_SEGMENT_BYTES, &segment->shmid);
        if (result.status != MPMC_OK) goto cleanup;

        void *address;
        result = sysv_shm_attach(segment->shmid, &address);
        if (result.status != MPMC_OK) goto cleanup;

        segment->ring = address;
        shm_ring_init(segment->ring);

        result = sysv_sem_create(SHM_SEM_COUNT, &segment->semid);
        if (result.status != MPMC_OK) goto cleanup;

        result = sysv_sem_set(segment->semid, SHM_SEM_MUTEX, 1);
        if (result.status != MPMC_OK) goto cleanup;

        result = sysv_sem_set(segment->semid, SHM_SEM_EMPTY, segment->ring->capacity);
        if (result.status != MPMC_OK) goto cleanup;

        result = sysv_sem_set(segment->semid, SHM_SEM_FULL, 0);
        if (result.status != MPMC_OK) goto cleanup;
    }

    result = shm_backend_ack_resources_create(backend);
    if (result.status != MPMC_OK) {
        mpmc_diagnostic_cause("criação do controle ACK", result);
        (void)shm_backend_cleanup(backend, producer_count, true, reporter);
        return mpmc_diagnostic_end(&diagnostic_scope, result);
    }

    *out = backend;
    return mpmc_diagnostic_end(&diagnostic_scope, (mpmc_result_t){.status = MPMC_OK});

cleanup:
    mpmc_diagnostic_cause("criação dos recursos SHM", result);
    (void)shm_backend_cleanup(backend, i + 1, true, reporter);
    return mpmc_diagnostic_end(&diagnostic_scope, result);
}

/**
 * @details Compartilha shm_backend_cleanup() com shm_backend_destroy(), usando
 * remove_global=false para preservar segmentos e conjuntos globais do supervisor.
 * Trata backend nulo antes de consultar producer_count ou chamar a auxiliar.
 */
mpmc_result_t shm_backend_worker_close(
    shm_backend_t *backend,
    const shm_cleanup_reporter_t *reporter
) {
    mpmc_diagnostic_scope_t diagnostic_scope;
    mpmc_diagnostic_begin(&diagnostic_scope, __func__, true, false);

    if (backend == NULL)
        return mpmc_diagnostic_end(&diagnostic_scope, (mpmc_result_t){.status = MPMC_OK});

    return mpmc_diagnostic_end(
        &diagnostic_scope, shm_backend_cleanup(backend, backend->producer_count, false, reporter)
    );
}

/**
 * @details Delega a shm_backend_cleanup() com remove_global=true e producer_count
 * entradas. O rollback de create() chama a auxiliar diretamente com o prefixo
 * visitado. Trata backend nulo antes de consultar producer_count.
 */
mpmc_result_t shm_backend_destroy(shm_backend_t *backend, const shm_cleanup_reporter_t *reporter) {
    mpmc_diagnostic_scope_t diagnostic_scope;
    mpmc_diagnostic_begin(&diagnostic_scope, __func__, true, false);

    if (backend == NULL)
        return mpmc_diagnostic_end(&diagnostic_scope, (mpmc_result_t){.status = MPMC_OK});

    return mpmc_diagnostic_end(
        &diagnostic_scope, shm_backend_cleanup(backend, backend->producer_count, true, reporter)
    );
}

/**
 * @brief Preserva o primeiro erro de cleanup e comunica a falha individual.
 * @param[in,out] first Resultado agregado, atualizado somente no primeiro erro.
 * @param current Resultado da operação de liberação recém-executada.
 * @param reporter Destino opcional dos diagnósticos.
 * @param operation Operação de liberação executada.
 * @param producer_id Índice lógico da estrutura, no intervalo 1..p.
 * @param resource_id Identificador passado à operação de liberação; shmid para
 * detach/remoção de segmento ou semid para remoção de conjunto.
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
    int resource_id
) {
    if (current.status == MPMC_OK) return;
    if (first->status == MPMC_OK) {
        *first = current;
    }

    if (reporter != NULL && reporter->on_error != NULL) {
        const shm_cleanup_error_t error = {
            .operation = operation,
            .producer_id = producer_id,
            .resource_id = resource_id,
            .result = current,
        };
        reporter->on_error(&error, reporter->user_data);
    }
}

/**
 * @brief Libera o estado local e, opcionalmente, solicita a remoção global.
 * @param backend Instância não nula, completa ou parcialmente inicializada.
 * @param segment_count Quantidade de entradas preparadas do início do vetor a visitar.
 * @param remove_global Se true, também tenta remover os conjuntos e segmentos globais.
 * @param reporter Destino opcional de todas as falhas individuais.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: nenhuma operação de liberação falhou; system_errno é zero.
 * - MPMC_ERR_INVALID_ARG: primeira falha de shmdt(), semctl(IPC_RMID) ou
 *   shmctl(IPC_RMID) retornou EINVAL, preservado em system_errno.
 * - MPMC_ERR_SHUTDOWN: primeira falha de semctl(IPC_RMID) ou shmctl(IPC_RMID)
 *   retornou EIDRM, preservado em system_errno.
 * - MPMC_ERR_SYS: primeira falha de shmdt(), semctl(IPC_RMID) ou shmctl(IPC_RMID)
 *   tem outra causa; system_errno preserva o errno capturado nessa chamada.
 * Em erro, system_errno preserva o errno da primeira falha. Todas as falhas
 * também são comunicadas por reporter, com as categorias traduzidas pelos wrappers.
 * @pre backend é não nulo e aponta para uma instância válida.
 * @pre Se o vetor existir, segment_count não excede a quantidade de entradas preparadas.
 * @details Visita o prefixo informado em ordem, tentando detach, remoção do conjunto
 * e remoção do segmento. Cada remoção global verifica o identificador do respectivo
 * recurso, para ignorar os objetos ainda não criados na entrada parcial.
 * Cada falha gera uma chamada de record_cleanup_error(), que preserva o primeiro
 * erro para retorno e comunica todos os diagnósticos ao reporter habilitado.
 *
 * Sempre libera o vetor e a instância, sem alocar memória para os diagnósticos.
 * A auxiliar aceita vetor nulo. Os chamadores garantem que backend é não nulo:
 * os pontos públicos tratam NULL antecipadamente, e create() só delega após alocá-lo.
 */
static mpmc_result_t shm_backend_cleanup(
    shm_backend_t *backend,
    size_t segment_count,
    bool remove_global,
    const shm_cleanup_reporter_t *reporter
) {
    mpmc_diagnostic_scope_t diagnostic_scope;
    mpmc_diagnostic_begin(&diagnostic_scope, __func__, true, false);

    mpmc_result_t result = {.status = MPMC_OK};

    /*
     * Os recursos ACK são globais e não pertencem a uma estrutura lógica
     * individual. Por isso, seu erro entra no resultado agregado, sem gerar
     * shm_cleanup_error_t com producer_id artificial.
     */
    mpmc_result_t ack_result = shm_backend_ack_resources_cleanup(backend, remove_global);
    if (ack_result.status != MPMC_OK) {
        result = ack_result;
    }

    if (backend->segments != NULL) {
        for (size_t i = 0; i < segment_count; i++) {
            shm_segment_t *segment = &backend->segments[i];

            if (segment->ring != NULL) {
                uint64_t before = mpmc_diagnostic_count();
                mpmc_result_t current = sysv_shm_detach(segment->ring);
                mpmc_diagnostic_observe("sysv_shm_detach", current, before);

                record_cleanup_error(
                    &result, current, reporter, SHM_CLEANUP_DETACH, i + 1, segment->shmid
                );
            }

            if (remove_global && segment->semid >= 0) {
                uint64_t before = mpmc_diagnostic_count();
                mpmc_result_t current = sysv_sem_remove(segment->semid);
                mpmc_diagnostic_observe("sysv_sem_remove", current, before);

                record_cleanup_error(
                    &result, current, reporter, SHM_CLEANUP_SEM_REMOVE, i + 1, segment->semid
                );
            }

            if (remove_global && segment->shmid >= 0) {
                uint64_t before = mpmc_diagnostic_count();
                mpmc_result_t current = sysv_shm_remove(segment->shmid);
                mpmc_diagnostic_observe("sysv_shm_remove", current, before);

                record_cleanup_error(
                    &result, current, reporter, SHM_CLEANUP_REMOVE, i + 1, segment->shmid
                );
            }
        }
    }

    free(backend->segments);
    free(backend);

    return mpmc_diagnostic_end(&diagnostic_scope, result);
}
