/**
 * @file test_shm_backend.c
 * @brief Testes do ciclo de vida e dos diagnósticos de cleanup do backend shm.
 * @ingroup tests
 * @details Substitui as alocações e os wrappers System V apenas no objeto do
 * backend. Injeta falhas em cada etapa sem criar recursos IPC reais e verifica
 * a continuidade da limpeza, a propriedade dos segmentos e os erros preservados.
 */

#include <errno.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "mpmc/ipc/shm_backend.h"
#include "shm_backend_internal.h"
#include "shm_ring.h"

/**
 * @brief Capacidade dos vetores de segmentos e de rastreamento de alocações.
 */
#define MAX_SEGMENTS 8
/**
 * @brief Quantidade de estruturas lógicas criada nos cenários usuais.
 */
#define PRODUCER_COUNT 3

/**
 * @brief Quantidade de verificações executadas nesta suíte.
 */
static unsigned checks;
/**
 * @brief Quantidade de verificações que registraram falha nesta suíte.
 */
static unsigned failures;

/**
 * @brief Registra uma verificação e informa a falha sem interromper a suíte.
 * @param condition Expressão lógica avaliada uma única vez.
 * @details Incrementa o contador de verificações; se a condição for falsa,
 * incrementa o contador de falhas e escreve o diagnóstico em stderr.
 */
#define CHECK(condition)                                                                           \
    do {                                                                                           \
        ++checks;                                                                                  \
        if (!(condition)) {                                                                        \
            ++failures;                                                                            \
            fprintf(stderr, "FALHA %s:%d: %s\n", __FILE__, __LINE__, #condition);                  \
        }                                                                                          \
    } while (0)

/**
 * @brief Estado simplificado de um segmento, sem criar IPC no kernel.
 * @details O índice no vetor segments é o identificador simulado; a posição zero
 * permite verificar que o backend aceita esse identificador como válido.
 */
typedef struct {
    bool created;  /**< Criação simulada ainda não seguida de remoção global bem-sucedida. */
    bool attached; /**< Associação simulada ainda não seguida de detach bem-sucedido. */
    /** @brief Área de dados alinhada para o buffer compartilhado. */
    alignas(shm_ring_t) unsigned char data[MPMC_SHM_SEGMENT_BYTES];
} fake_segment_t;

/** @brief Conjunto simulado, com ID independente do segmento e valores iniciais observáveis. */
typedef struct {
    bool created;                   /**< Conjunto ainda não removido. */
    int id;                         /**< ID atribuído pela criação simulada. */
    unsigned values[SHM_SEM_COUNT]; /**< Valores configurados por SETVAL. */
} fake_semaphore_set_t;

static_assert(
    offsetof(fake_segment_t, data) % alignof(shm_ring_t) == 0,
    "os dados de cada segmento simulado devem estar alinhados para shm_ring_t"
);

/** @brief Armazena cópias dos diagnósticos recebidos pela callback de cleanup. */
typedef struct {
    shm_cleanup_error_t
        errors[3 * MAX_SEGMENTS]; /**< Detach e remoções de conjunto/segmento por entrada. */
    size_t count;                 /**< Quantidade de registros efetivamente copiados para errors. */
} error_log_t;

/**
 * @brief Segmentos simulados, indexados pelo identificador devolvido ao backend.
 */
static fake_segment_t segments[MAX_SEGMENTS];
/** @brief Conjuntos de semáforos simulados, na ordem de criação. */
static fake_semaphore_set_t semaphore_sets[MAX_SEGMENTS];
/** @brief Chamadas às primitivas simuladas de criação, SETVAL e remoção de conjuntos. */
static unsigned sem_create_calls, sem_set_calls, sem_remove_calls;
/** @brief Chamadas escolhidas para falhar; zero desabilita a injeção. */
static unsigned fail_sem_create, fail_sem_set;
/** @brief Falha de remoção injetada por conjunto, preservada por valor. */
static mpmc_result_t sem_remove_errors[MAX_SEGMENTS];
/**
 * @brief Ponteiros de alocações reais ainda pertencentes ao backend sob teste.
 */
static void *allocations[MAX_SEGMENTS];
/**
 * @brief Quantidade de alocações rastreadas ainda não liberadas pelo backend.
 */
static unsigned live_allocations;
/**
 * @brief Chamadas acumuladas aos substitutos de malloc() e calloc(), inclusive falhas.
 */
static unsigned allocation_calls;
/**
 * @brief Chamadas ao substituto de free() com ponteiro não nulo.
 */
static unsigned free_calls;
/**
 * @brief Tentativas de criação simulada, incluindo a tentativa que falhar.
 */
static unsigned create_calls;
/**
 * @brief Tentativas de associação de identificadores simulados válidos.
 */
static unsigned attach_calls;
/**
 * @brief Tentativas de desassociação simulada.
 */
static unsigned detach_calls;
/**
 * @brief Tentativas de remoção global simulada.
 */
static unsigned remove_calls;
/**
 * @brief Posição baseada em um da alocação que falhará; zero desabilita a injeção.
 */
static unsigned fail_allocation;
/**
 * @brief Posição baseada em um da criação que falhará; zero desabilita a injeção.
 */
static unsigned fail_create;
/**
 * @brief Posição baseada em um da associação que falhará; zero desabilita a injeção.
 */
static unsigned fail_attach;
/**
 * @brief Resultado simulado por segmento no detach; MPMC_OK representa sucesso.
 */
static mpmc_result_t detach_errors[MAX_SEGMENTS];
/**
 * @brief Resultado simulado por segmento na remoção; MPMC_OK representa sucesso.
 */
static mpmc_result_t remove_errors[MAX_SEGMENTS];

/* Pontos de injeção usados somente por shm_backend_for_test.o. */
void *mpmc_test_shm_malloc(size_t size);
void *mpmc_test_shm_calloc(size_t count, size_t size);
void mpmc_test_shm_free(void *pointer);
mpmc_result_t mpmc_test_shm_create(size_t size, int *out_shmid);
mpmc_result_t mpmc_test_shm_attach(int shmid, void **out_address);
mpmc_result_t mpmc_test_shm_detach(const void *address);
mpmc_result_t mpmc_test_shm_remove(int shmid);
mpmc_result_t mpmc_test_sem_create(unsigned count, int *out_semid);
mpmc_result_t mpmc_test_sem_set(int semid, unsigned index, unsigned value);
mpmc_result_t mpmc_test_sem_remove(int semid);

/**
 * @brief Cria um conjunto simulado com três semáforos e ID zero ou distinto do shmid.
 * @param count Quantidade esperada de semáforos.
 * @param[out] out_semid Recebe o identificador somente em sucesso.
 * @return Estrutura mpmc_result_t com status MPMC_OK, ou MPMC_ERR_SYS/ENOSPC
 * na chamada escolhida por fail_sem_create. system_errno é zero em sucesso.
 */
mpmc_result_t mpmc_test_sem_create(unsigned count, int *out_semid) {
    CHECK(count == SHM_SEM_COUNT && out_semid != NULL);
    unsigned position = sem_create_calls++;
    if (sem_create_calls == fail_sem_create) {
        return (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = ENOSPC};
    }
    CHECK(position < MAX_SEGMENTS);
    semaphore_sets[position].created = true;
    semaphore_sets[position].id = position == 0 ? 0 : 100 + (int)position;
    *out_semid = semaphore_sets[position].id;
    return (mpmc_result_t){.status = MPMC_OK};
}

/**
 * @brief Registra SETVAL em um conjunto simulado.
 * @param semid ID de conjunto criado pelo substituto.
 * @param index Índice do semáforo, menor que SHM_SEM_COUNT.
 * @param value Valor inicial a registrar.
 * @return Estrutura mpmc_result_t com status MPMC_OK e system_errno zero, ou
 * MPMC_ERR_INVALID_ARG/ERANGE na chamada escolhida por fail_sem_set.
 */
mpmc_result_t mpmc_test_sem_set(int semid, unsigned index, unsigned value) {
    if (++sem_set_calls == fail_sem_set) {
        return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG, .system_errno = ERANGE};
    }
    for (size_t i = 0; i < MAX_SEGMENTS; ++i) {
        if (semaphore_sets[i].created && semaphore_sets[i].id == semid) {
            CHECK(index < SHM_SEM_COUNT);
            if (index < SHM_SEM_COUNT) semaphore_sets[i].values[index] = value;
            return (mpmc_result_t){.status = MPMC_OK};
        }
    }
    CHECK(false);
    return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG, .system_errno = EINVAL};
}

/**
 * @brief Remove um conjunto simulado e detecta IDs inexistentes ou remoções duplicadas.
 * @param semid ID que identifica exclusivamente o conjunto.
 * @return Estrutura mpmc_result_t com status MPMC_OK em sucesso; uma falha configurada
 * em sem_remove_errors é propagada integralmente. ID desconhecido retorna
 * MPMC_ERR_INVALID_ARG/EINVAL e registra falha da suíte.
 */
mpmc_result_t mpmc_test_sem_remove(int semid) {
    ++sem_remove_calls;
    for (size_t i = 0; i < MAX_SEGMENTS; ++i) {
        if (semaphore_sets[i].created && semaphore_sets[i].id == semid) {
            if (sem_remove_errors[i].status != MPMC_OK) return sem_remove_errors[i];
            semaphore_sets[i].created = false;
            return (mpmc_result_t){.status = MPMC_OK};
        }
    }
    CHECK(false);
    return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG, .system_errno = EINVAL};
}

/**
 * @brief Registra um ponteiro alocado para detectar vazamentos e liberações duplicadas.
 * @param pointer Ponteiro obtido de malloc()/calloc(); NULL é ignorado.
 * @details Ocupa a primeira posição livre e incrementa live_allocations. Registra
 * falha se não houver espaço para rastrear um ponteiro não nulo.
 */
static void track_allocation(void *pointer) {
    if (pointer == NULL) return;
    for (size_t i = 0; i < MAX_SEGMENTS; ++i) {
        if (allocations[i] == NULL) {
            allocations[i] = pointer;
            ++live_allocations;
            return;
        }
    }
    CHECK(false);
}

/**
 * @brief Substitui malloc() no objeto do backend e rastreia a alocação real.
 * @param size Quantidade de bytes solicitada pelo backend.
 * @return Ponteiro devolvido por malloc(), ou NULL em falha real ou programada.
 * @details Incrementa allocation_calls. Na posição fail_allocation, define errno=ENOMEM
 * e não aloca; fora dela, registra o ponteiro para verificar sua liberação posterior.
 */
void *mpmc_test_shm_malloc(size_t size) {
    if (++allocation_calls == fail_allocation) {
        errno = ENOMEM;
        return NULL;
    }
    void *pointer = malloc(size);
    track_allocation(pointer);
    return pointer;
}

/**
 * @brief Substitui calloc() no objeto do backend e rastreia a alocação real.
 * @param count Quantidade de elementos solicitada pelo backend.
 * @param size Tamanho de cada elemento, em bytes.
 * @return Ponteiro devolvido por calloc(), ou NULL em falha real ou programada.
 * @details Compartilha allocation_calls e fail_allocation com mpmc_test_malloc().
 * Na falha programada, define errno=ENOMEM; fora dela, rastreia o bloco zerado.
 */
void *mpmc_test_shm_calloc(size_t count, size_t size) {
    if (++allocation_calls == fail_allocation) {
        errno = ENOMEM;
        return NULL;
    }
    void *pointer = calloc(count, size);
    track_allocation(pointer);
    return pointer;
}

/**
 * @brief Libera um ponteiro conhecido e detecta liberações indevidas do backend.
 * @param pointer Ponteiro rastreado; NULL é ignorado.
 * @details Conta chamadas não nulas. Se encontrar o ponteiro, remove o registro,
 * decrementa live_allocations, executa free() e define errno=EIO para testar a
 * preservação de erros anteriores. Ponteiro desconhecido registra falha e não é liberado.
 */
void mpmc_test_shm_free(void *pointer) {
    if (pointer == NULL) return;
    ++free_calls;
    for (size_t i = 0; i < MAX_SEGMENTS; ++i) {
        if (allocations[i] == pointer) {
            allocations[i] = NULL;
            --live_allocations;
            free(pointer);
            errno = EIO;
            return;
        }
    }
    CHECK(false); /* Detecta liberação duplicada ou de alocação desconhecida. */
}

/**
 * @brief Simula a criação de um segmento de 2048 bytes sem acessar IPC real.
 * @param size Tamanho que deve coincidir com MPMC_SHM_SEGMENT_BYTES.
 * @param[out] out_shmid Recebe o índice do segmento simulado somente em sucesso.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: segmento simulado marcado como criado; system_errno é zero.
 * - MPMC_ERR_SYS: tentativa escolhida por fail_create; system_errno simula ENOSPC.
 * - MPMC_ERR_STATE: tentativas excederam o vetor de segmentos; system_errno é zero.
 * @pre out_shmid deve apontar para um inteiro válido.
 * @details Confere o tamanho com CHECK(). A primeira criação devolve o identificador zero;
 * os códigos de falha são construídos diretamente, sem chamada de sistema.
 */
mpmc_result_t mpmc_test_shm_create(size_t size, int *out_shmid) {
    CHECK(size == MPMC_SHM_SEGMENT_BYTES);
    CHECK(out_shmid != NULL);
    if (++create_calls == fail_create) {
        return (mpmc_result_t){
            .status = MPMC_ERR_SYS,
            .system_errno = ENOSPC,
        };
    }
    size_t index = create_calls - 1;
    CHECK(index < MAX_SEGMENTS);
    if (index >= MAX_SEGMENTS) return (mpmc_result_t){.status = MPMC_ERR_STATE};
    segments[index].created = true;
    *out_shmid = (int)index; /* O primeiro segmento tem ID zero. */
    return (mpmc_result_t){.status = MPMC_OK};
}

/**
 * @brief Simula a associação do segmento e devolve seu vetor de dados local.
 * @param shmid Índice do segmento simulado.
 * @param[out] out_address Recebe o início de segments[shmid].data somente em sucesso.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: associação simulada registrada; system_errno é zero.
 * - MPMC_ERR_SYS: tentativa escolhida por fail_attach; system_errno simula EACCES.
 * - MPMC_ERR_STATE: identificador fora do vetor simulado; system_errno é zero.
 * @pre out_address deve apontar para um ponteiro válido.
 * @details A existência da criação é conferida com CHECK(); não executa shmat().
 */
mpmc_result_t mpmc_test_shm_attach(int shmid, void **out_address) {
    CHECK(shmid >= 0 && shmid < MAX_SEGMENTS);
    CHECK(out_address != NULL);
    if (shmid < 0 || shmid >= MAX_SEGMENTS) return (mpmc_result_t){.status = MPMC_ERR_STATE};
    CHECK(segments[shmid].created);
    if (++attach_calls == fail_attach) {
        return (mpmc_result_t){
            .status = MPMC_ERR_SYS,
            .system_errno = EACCES,
        };
    }
    segments[shmid].attached = true;
    *out_address = segments[shmid].data;
    return (mpmc_result_t){.status = MPMC_OK};
}

/**
 * @brief Simula o detach do segmento identificado pelo endereço de seus dados.
 * @param address Endereço a procurar nas áreas de dados dos segmentos simulados.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: associação simulada desfeita; system_errno é zero.
 * - MPMC_ERR_INVALID_ARG: EINVAL configurado em detach_errors.
 * - MPMC_ERR_SYS: outra falha configurada em detach_errors.
 * - MPMC_ERR_STATE: endereço não pertence aos segmentos simulados; system_errno é zero.
 * Nas falhas configuradas, system_errno preserva o código fornecido pelo cenário.
 * @details Incrementa detach_calls e confere as flags de criação/associação com CHECK().
 * Uma falha injetada preserva a flag attached; não executa shmdt().
 */
mpmc_result_t mpmc_test_shm_detach(const void *address) {
    ++detach_calls;
    for (size_t i = 0; i < MAX_SEGMENTS; ++i) {
        if (address == segments[i].data) {
            CHECK(segments[i].created && segments[i].attached);
            if (detach_errors[i].status != MPMC_OK) return detach_errors[i];
            segments[i].attached = false;
            return (mpmc_result_t){.status = MPMC_OK};
        }
    }
    CHECK(false);
    return (mpmc_result_t){.status = MPMC_ERR_STATE};
}

/**
 * @brief Simula a remoção global de um segmento sem acessar o kernel.
 * @param shmid Índice do segmento simulado a remover.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: flag de criação removida; system_errno é zero.
 * - MPMC_ERR_SHUTDOWN: EIDRM configurado em remove_errors.
 * - MPMC_ERR_SYS: outra falha configurada em remove_errors.
 * - MPMC_ERR_STATE: identificador fora do vetor simulado; system_errno é zero.
 * Nas falhas configuradas, system_errno preserva o código fornecido pelo cenário.
 * @details Incrementa remove_calls e confere a flag de criação com CHECK(). Em falha
 * injetada preserva essa flag; não executa shmctl(IPC_RMID).
 */
mpmc_result_t mpmc_test_shm_remove(int shmid) {
    ++remove_calls;
    CHECK(shmid >= 0 && shmid < MAX_SEGMENTS);
    if (shmid < 0 || shmid >= MAX_SEGMENTS) return (mpmc_result_t){.status = MPMC_ERR_STATE};
    CHECK(segments[shmid].created);
    if (remove_errors[shmid].status != MPMC_OK) return remove_errors[shmid];
    segments[shmid].created = false;
    return (mpmc_result_t){.status = MPMC_OK};
}

/**
 * @brief Copia o diagnóstico temporário da callback para o relatório pertencente ao teste.
 * @param error Diagnóstico válido durante a chamada, fornecido pelo backend.
 * @param user_data Ponteiro válido para error_log_t.
 * @details Registra falha se o relatório estiver cheio e só copia quando houver espaço.
 * Define errno=ERANGE para conferir que a callback não substitui causas já capturadas.
 */
static void collect_error(const shm_cleanup_error_t *error, void *user_data) {
    error_log_t *log = user_data;
    CHECK(log->count < sizeof log->errors / sizeof log->errors[0]);
    if (log->count < sizeof log->errors / sizeof log->errors[0]) {
        log->errors[log->count++] = *error;
    }
    errno = ERANGE; /* O diagnóstico não pode substituir a causa já capturada. */
}

/**
 * @brief Libera resíduos locais e reinicializa o estado da simulação.
 * @details Antes de limpar, confere que live_allocations é zero para detectar vazamentos.
 * Libera ponteiros restantes diretamente, zera vetores, contadores e pontos de falha.
 * Não remove IPC real, pois os segmentos desta suíte são simulados.
 */
static void reset_case(void) {
    CHECK(live_allocations == 0);
    for (size_t i = 0; i < MAX_SEGMENTS; ++i) {
        free(allocations[i]);
    }
    memset(allocations, 0, sizeof allocations);
    memset(segments, 0, sizeof segments);
    memset(semaphore_sets, 0, sizeof semaphore_sets);
    memset(sem_remove_errors, 0, sizeof sem_remove_errors);
    memset(detach_errors, 0, sizeof detach_errors);
    memset(remove_errors, 0, sizeof remove_errors);
    live_allocations = allocation_calls = free_calls = 0;
    create_calls = attach_calls = detach_calls = remove_calls = 0;
    fail_allocation = fail_create = fail_attach = 0;
    sem_create_calls = sem_set_calls = sem_remove_calls = 0;
    fail_sem_create = fail_sem_set = 0;
}

/**
 * @brief Confere um resultado já capturado independentemente de errno corrente.
 * @param result Resultado devolvido pela operação exercitada.
 * @param status Categoria esperada.
 * @param system_errno Causa de sistema esperada no resultado.
 * @details Define errno=ERANGE antes de verificar os dois campos, registrando diferenças.
 */
static void check_result(mpmc_result_t result, mpmc_status_t status, int system_errno) {
    errno = ERANGE;
    CHECK(result.status == status);
    CHECK(result.system_errno == system_errno);
}

/**
 * @brief Confere uma cópia de diagnóstico e sua identidade no relatório de cleanup.
 * @param log Relatório válido pertencente ao teste.
 * @param index Posição baseada em zero do diagnóstico esperado.
 * @param operation Operação de liberação que deve ter falhado.
 * @param producer_id Índice lógico esperado, baseado em um.
 * @param expected Categoria e causa de sistema esperadas para a falha simulada.
 * @details Confere o ID da classe indicada por operation; segmentos e conjuntos
 * têm numerações independentes. Se index não existir, não acessa o vetor.
 */
static void check_error(
    const error_log_t *log,
    size_t index,
    shm_cleanup_operation_t operation,
    size_t producer_id,
    mpmc_result_t expected
) {
    CHECK(index < log->count);
    if (index >= log->count) return;
    const shm_cleanup_error_t *error = &log->errors[index];
    CHECK(error->operation == operation);
    CHECK(error->producer_id == producer_id);
    int expected_id = operation == SHM_CLEANUP_SEM_REMOVE ? semaphore_sets[producer_id - 1].id
                                                          : (int)producer_id - 1;
    CHECK(error->resource_id == expected_id);
    check_result(error->result, expected.status, expected.system_errno);
}

/**
 * @brief Cria um backend com PRODUCER_COUNT segmentos simulados e sem reporter.
 * @return Backend criado, ou NULL se a criação falhar.
 * @details Registra falhas no resultado e na instância; o cenário deve fechar ou destruir
 * o backend recebido para verificar a liberação das alocações rastreadas.
 */
static shm_backend_t *new_backend(void) {
    shm_backend_t *backend = NULL;
    check_result(shm_backend_create(&backend, PRODUCER_COUNT, NULL), MPMC_OK, 0);
    CHECK(backend != NULL);
    return backend;
}

/**
 * @brief Confere fechamento nulo, argumentos inválidos e ausência de alocações nesses casos.
 */
static void test_null_and_invalid_arguments(void) {
    reset_case();
    error_log_t log = {0};
    const shm_cleanup_reporter_t reporter = {.on_error = collect_error, .user_data = &log};
    check_result(shm_backend_destroy(NULL, &reporter), MPMC_OK, 0);
    check_result(shm_backend_worker_close(NULL, &reporter), MPMC_OK, 0);
    check_result(shm_backend_destroy(NULL, NULL), MPMC_OK, 0);
    check_result(shm_backend_worker_close(NULL, NULL), MPMC_OK, 0);
    check_result(shm_backend_create(NULL, PRODUCER_COUNT, &reporter), MPMC_ERR_INVALID_ARG, 0);
    char sentinel;
    shm_backend_t *backend = (shm_backend_t *)(void *)&sentinel;
    check_result(shm_backend_create(&backend, 0, &reporter), MPMC_ERR_INVALID_ARG, 0);
    CHECK(backend == NULL);
    CHECK(allocation_calls == 0 && log.count == 0);
    CHECK(sem_create_calls == 0 && sem_remove_calls == 0 && detach_calls == 0);
}

/**
 * @brief Confere criação completa, IDs válidos, ausência de diagnósticos e liberação de todos os
 * recursos.
 */
static void test_success(void) {
    reset_case();
    for (size_t i = 0; i < PRODUCER_COUNT; ++i) {
        memset(segments[i].data, 0xA5, sizeof segments[i].data);
    }
    error_log_t log = {0};
    const shm_cleanup_reporter_t reporter = {.on_error = collect_error, .user_data = &log};
    shm_backend_t *backend = NULL;
    check_result(shm_backend_create(&backend, PRODUCER_COUNT, &reporter), MPMC_OK, 0);
    CHECK(backend != NULL);
    CHECK(create_calls == PRODUCER_COUNT && attach_calls == PRODUCER_COUNT);
    CHECK(sem_create_calls == PRODUCER_COUNT && sem_set_calls == 3 * PRODUCER_COUNT);
    for (size_t i = 0; i < PRODUCER_COUNT; ++i) {
        CHECK(segments[i].created && segments[i].attached);
        const shm_ring_t *ring = (const void *)segments[i].data;
        CHECK(ring->head == 0 && ring->tail == 0 && ring->count == 0);
        CHECK(ring->capacity == 63);
        CHECK(ring->consumed_total == 0);
        CHECK(semaphore_sets[i].created);
        CHECK(semaphore_sets[i].values[SHM_SEM_MUTEX] == 1);
        CHECK(semaphore_sets[i].values[SHM_SEM_EMPTY] == ring->capacity);
        CHECK(semaphore_sets[i].values[SHM_SEM_FULL] == 0);
    }
    check_result(shm_backend_destroy(backend, &reporter), MPMC_OK, 0);
    CHECK(detach_calls == PRODUCER_COUNT && remove_calls == PRODUCER_COUNT);
    CHECK(sem_remove_calls == PRODUCER_COUNT);
    CHECK(allocation_calls == 2 && free_calls == 2 && live_allocations == 0);
    CHECK(log.count == 0);
    for (size_t i = 0; i < PRODUCER_COUNT; ++i) {
        CHECK(!segments[i].created && !segments[i].attached);
        CHECK(!semaphore_sets[i].created);
    }
}

/**
 * @brief Injeta falha em cada alocação e confere saída nula sem recursos parciais restantes.
 */
static void test_allocation_failures(void) {
    for (unsigned position = 1; position <= 2; ++position) {
        reset_case();
        error_log_t log = {0};
        const shm_cleanup_reporter_t reporter = {.on_error = collect_error, .user_data = &log};
        fail_allocation = position;
        char sentinel;
        shm_backend_t *backend = (shm_backend_t *)(void *)&sentinel;
        check_result(shm_backend_create(&backend, PRODUCER_COUNT, &reporter), MPMC_ERR_SYS, ENOMEM);
        CHECK(backend == NULL && live_allocations == 0);
        CHECK(allocation_calls == position && free_calls == position - 1);
        CHECK(create_calls == 0 && detach_calls == 0 && remove_calls == 0);
        CHECK(log.count == 0);
    }
}

/**
 * @brief Injeta falhas de criação e associação em cada posição e confere o rollback completo.
 */
static void test_rollback_at_each_position(void) {
    for (unsigned operation = 0; operation < 2; ++operation) {
        for (unsigned position = 1; position <= PRODUCER_COUNT; ++position) {
            reset_case();
            error_log_t log = {0};
            const shm_cleanup_reporter_t reporter = {.on_error = collect_error, .user_data = &log};
            if (operation == 0) {
                fail_create = position;
            } else {
                fail_attach = position;
            }
            shm_backend_t *backend = NULL;
            check_result(
                shm_backend_create(&backend, PRODUCER_COUNT, &reporter),
                MPMC_ERR_SYS,
                operation == 0 ? ENOSPC : EACCES
            );
            CHECK(backend == NULL && live_allocations == 0);
            CHECK(create_calls == position);
            CHECK(attach_calls == position - (operation == 0 ? 1 : 0));
            CHECK(detach_calls == position - 1);
            CHECK(remove_calls == position - (operation == 0 ? 1 : 0));
            CHECK(sem_create_calls == position - 1 && sem_remove_calls == position - 1);
            CHECK(allocation_calls == 2 && free_calls == 2 && log.count == 0);
            for (size_t i = 0; i < PRODUCER_COUNT; ++i) {
                CHECK(!segments[i].created && !segments[i].attached);
                CHECK(!semaphore_sets[i].created);
            }
        }
    }
}

/** @brief Injeta falha de criação ou de cada SETVAL em todas as entradas do backend. */
static void test_semaphore_initialization_failures(void) {
    for (unsigned stage = 0; stage <= SHM_SEM_COUNT; ++stage) {
        for (unsigned position = 1; position <= PRODUCER_COUNT; ++position) {
            reset_case();
            if (stage == 0)
                fail_sem_create = position;
            else
                fail_sem_set = (position - 1) * SHM_SEM_COUNT + stage;
            error_log_t log = {0};
            const shm_cleanup_reporter_t reporter = {.on_error = collect_error, .user_data = &log};
            shm_backend_t *backend = NULL;
            check_result(
                shm_backend_create(&backend, PRODUCER_COUNT, &reporter),
                stage == 0 ? MPMC_ERR_SYS : MPMC_ERR_INVALID_ARG,
                stage == 0 ? ENOSPC : ERANGE
            );
            CHECK(backend == NULL && live_allocations == 0);
            CHECK(create_calls == position && attach_calls == position);
            CHECK(detach_calls == position && remove_calls == position);
            CHECK(sem_remove_calls == position - (stage == 0 ? 1 : 0));
            CHECK(log.count == 0);
            for (size_t i = 0; i < PRODUCER_COUNT; ++i) {
                CHECK(!segments[i].created && !segments[i].attached);
                CHECK(!semaphore_sets[i].created);
            }
        }
    }
}

/** @brief Mistura erros de segmento/conjunto e mantém ordem, identidade e erro original. */
static void test_semaphore_cleanup_errors(void) {
    reset_case();
    shm_backend_t *backend = new_backend();
    error_log_t log = {0};
    const shm_cleanup_reporter_t reporter = {.on_error = collect_error, .user_data = &log};
    sem_remove_errors[0] = (mpmc_result_t){.status = MPMC_ERR_SHUTDOWN, .system_errno = EIDRM};
    remove_errors[0] = (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = EPERM};
    sem_remove_errors[2] = (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = EACCES};
    check_result(shm_backend_destroy(backend, &reporter), MPMC_ERR_SHUTDOWN, EIDRM);
    CHECK(sem_remove_calls == PRODUCER_COUNT && remove_calls == PRODUCER_COUNT);
    CHECK(detach_calls == PRODUCER_COUNT && live_allocations == 0 && log.count == 3);
    check_error(&log, 0, SHM_CLEANUP_SEM_REMOVE, 1, sem_remove_errors[0]);
    check_error(&log, 1, SHM_CLEANUP_REMOVE, 1, remove_errors[0]);
    check_error(&log, 2, SHM_CLEANUP_SEM_REMOVE, 3, sem_remove_errors[2]);
    CHECK(!semaphore_sets[1].created && !segments[2].created);

    reset_case();
    fail_sem_set = 4;
    sem_remove_errors[0] = (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = EACCES};
    log = (error_log_t){0};
    backend = NULL;
    check_result(
        shm_backend_create(&backend, PRODUCER_COUNT, &reporter), MPMC_ERR_INVALID_ARG, ERANGE
    );
    CHECK(backend == NULL && live_allocations == 0 && log.count == 1);
    CHECK(sem_remove_calls == 2 && remove_calls == 2);
    check_error(&log, 0, SHM_CLEANUP_SEM_REMOVE, 1, sem_remove_errors[0]);
}

/**
 * @brief Confere continuidade do destroy, primeiro erro retornado e todos os diagnósticos copiados.
 */
static void test_multiple_destroy_errors(void) {
    reset_case();
    shm_backend_t *backend = new_backend();
    error_log_t log = {0};
    const shm_cleanup_reporter_t reporter = {.on_error = collect_error, .user_data = &log};
    detach_errors[0] = (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG, .system_errno = EINVAL};
    remove_errors[1] = (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = EPERM};
    detach_errors[2] = (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = EACCES};
    remove_errors[2] = (mpmc_result_t){.status = MPMC_ERR_SHUTDOWN, .system_errno = EIDRM};
    check_result(shm_backend_destroy(backend, &reporter), MPMC_ERR_INVALID_ARG, EINVAL);
    CHECK(detach_calls == PRODUCER_COUNT && remove_calls == PRODUCER_COUNT);
    CHECK(allocation_calls == 2 && free_calls == 2 && live_allocations == 0);
    CHECK(log.count == 4);
    check_error(&log, 0, SHM_CLEANUP_DETACH, 1, detach_errors[0]);
    check_error(&log, 1, SHM_CLEANUP_REMOVE, 2, remove_errors[1]);
    check_error(&log, 2, SHM_CLEANUP_DETACH, 3, detach_errors[2]);
    check_error(&log, 3, SHM_CLEANUP_REMOVE, 3, remove_errors[2]);
}

/** @brief Preserva shutdown como primeira falha e continua a limpeza dos demais segmentos. */
static void test_removed_resource_result(void) {
    reset_case();
    shm_backend_t *backend = new_backend();
    error_log_t log = {0};
    const shm_cleanup_reporter_t reporter = {.on_error = collect_error, .user_data = &log};
    remove_errors[0] = (mpmc_result_t){.status = MPMC_ERR_SHUTDOWN, .system_errno = EIDRM};

    check_result(shm_backend_destroy(backend, &reporter), MPMC_ERR_SHUTDOWN, EIDRM);
    CHECK(detach_calls == PRODUCER_COUNT && remove_calls == PRODUCER_COUNT);
    CHECK(live_allocations == 0 && log.count == 1);
    check_error(&log, 0, SHM_CLEANUP_REMOVE, 1, remove_errors[0]);
}

/**
 * @brief Confere liberação local com e sem erros, preservando os segmentos globais simulados.
 */
static void test_worker_close(void) {
    for (unsigned inject_errors = 0; inject_errors < 2; ++inject_errors) {
        reset_case();
        shm_backend_t *backend = new_backend();
        error_log_t log = {0};
        const shm_cleanup_reporter_t reporter = {.on_error = collect_error, .user_data = &log};
        if (inject_errors != 0) {
            detach_errors[0] =
                (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG, .system_errno = EINVAL};
            detach_errors[1] = (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = EACCES};
            detach_errors[2] = (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = EIO};
        }
        check_result(
            shm_backend_worker_close(backend, &reporter),
            inject_errors != 0 ? MPMC_ERR_INVALID_ARG : MPMC_OK,
            inject_errors != 0 ? EINVAL : 0
        );
        CHECK(detach_calls == PRODUCER_COUNT && remove_calls == 0 && sem_remove_calls == 0);
        CHECK(allocation_calls == 2 && free_calls == 2 && live_allocations == 0);
        CHECK(log.count == (inject_errors != 0 ? PRODUCER_COUNT : 0));
        for (size_t i = 0; i < PRODUCER_COUNT; ++i) {
            CHECK(segments[i].created);
            CHECK(semaphore_sets[i].created);
            CHECK(segments[i].attached == (inject_errors != 0));
            if (inject_errors != 0) {
                check_error(&log, i, SHM_CLEANUP_DETACH, i + 1, detach_errors[i]);
            }
        }
    }
}

/**
 * @brief Confere que falhas no rollback não substituem a causa original de criação ou associação.
 */
static void test_rollback_diagnostics_preserve_original_error(void) {
    for (unsigned operation = 0; operation < 2; ++operation) {
        reset_case();
        error_log_t log = {0};
        const shm_cleanup_reporter_t reporter = {.on_error = collect_error, .user_data = &log};
        if (operation == 0) {
            fail_create = PRODUCER_COUNT;
        } else {
            fail_attach = PRODUCER_COUNT;
        }
        detach_errors[0] = (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG, .system_errno = EINVAL};
        remove_errors[1] = (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = EPERM};
        remove_errors[2] = (mpmc_result_t){.status = MPMC_ERR_SHUTDOWN, .system_errno = EIDRM};
        shm_backend_t *backend = NULL;
        check_result(
            shm_backend_create(&backend, PRODUCER_COUNT, &reporter),
            MPMC_ERR_SYS,
            operation == 0 ? ENOSPC : EACCES
        );
        CHECK(backend == NULL && live_allocations == 0);
        CHECK(detach_calls == PRODUCER_COUNT - 1);
        CHECK(remove_calls == PRODUCER_COUNT - (operation == 0 ? 1 : 0));
        CHECK(log.count == (operation == 0 ? 2u : 3u));
        CHECK(allocation_calls == 2 && free_calls == 2);
        check_error(&log, 0, SHM_CLEANUP_DETACH, 1, detach_errors[0]);
        check_error(&log, 1, SHM_CLEANUP_REMOVE, 2, remove_errors[1]);
        if (operation != 0) {
            check_error(&log, 2, SHM_CLEANUP_REMOVE, 3, remove_errors[2]);
        }
    }
}

/**
 * @brief Confere o resultado e a limpeza com reporter nulo ou callback desabilitada.
 */
static void test_optional_reporter(void) {
    for (unsigned use_reporter = 0; use_reporter < 2; ++use_reporter) {
        reset_case();
        shm_backend_t *backend = new_backend();
        error_log_t log = {0};
        const shm_cleanup_reporter_t reporter = {.on_error = NULL, .user_data = &log};
        detach_errors[0] = (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG, .system_errno = EINVAL};
        remove_errors[1] = (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = EPERM};
        check_result(
            shm_backend_destroy(backend, use_reporter != 0 ? &reporter : NULL),
            MPMC_ERR_INVALID_ARG,
            EINVAL
        );
        CHECK(detach_calls == PRODUCER_COUNT && remove_calls == PRODUCER_COUNT);
        CHECK(log.count == 0 && live_allocations == 0);
    }
}

/**
 * @brief Executa os casos da suíte e apresenta o resultado das verificações.
 * @return Zero se nenhuma verificação falhar; um se houver falha ou erro de preparação.
 */
int main(void) {
    test_null_and_invalid_arguments();
    test_success();
    test_allocation_failures();
    test_rollback_at_each_position();
    test_semaphore_initialization_failures();
    test_semaphore_cleanup_errors();
    test_multiple_destroy_errors();
    test_removed_resource_result();
    test_worker_close();
    test_rollback_diagnostics_preserve_original_error();
    test_optional_reporter();
    reset_case();
    printf("%u verificações do backend shm, %u falhas\n", checks, failures);
    return failures == 0 ? 0 : 1;
}
