/**
 * @file diagnostic.h
 * @brief Diagnósticos síncronos de operações e cleanup, sem alocação dinâmica.
 * @ingroup core
 * @details Contrato transversal para observar todas as falhas de um fluxo,
 * complementando o único mpmc_result_t retornado. Camadas inferiores emitem
 * registros; a aplicação instala a apresentação no pai ou o emissor de pipe no
 * filho. Este módulo não conhece terminal, REPL nem protocolo de dados IPC.
 * @see @ref flow_diagnostics para escopos, propriedade e transporte dos registros.
 */
#ifndef MPMC_DIAGNOSTIC_H
#define MPMC_DIAGNOSTIC_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

#include "mpmc/error.h"

/** @brief Etapa que originou uma falha individual. */
typedef enum {
    MPMC_DIAGNOSTIC_OPERATION, /**< Execução da operação solicitada. */
    MPMC_DIAGNOSTIC_CLEANUP,   /**< Finalização, rollback ou liberação. */
} mpmc_diagnostic_phase_t;

/**
 * @brief Registro copiável de uma falha; não contém ponteiros emprestados.
 * @details group identifica uma chamada raiz dentro de process_pid.
 * operation_id e producer_id identificam o trabalho, quando disponíveis.
 * resource_id igual a -1 significa ausência de identificador específico.
 */
typedef struct {
    char function[48];             /**< Função raiz que falhou. */
    char operation[64];            /**< Operação interna que originou a falha. */
    mpmc_result_t result;          /**< Categoria e errno capturados. */
    mpmc_diagnostic_phase_t phase; /**< Execução ou cleanup. */
    pid_t process_pid;             /**< Processo que observou o erro. */
    uint64_t operation_id;         /**< Trabalho; zero fora de um worker. */
    uint64_t group;                /**< Identidade da chamada raiz. */
    size_t producer_id;            /**< Estrutura lógica; zero em striped ou sem contexto. */
    int resource_id;               /**< ID System V ou PID; -1 se não aplicável. */
    bool fatal;                    /**< Contexto não pode continuar após esta falha. */
} mpmc_diagnostic_t;

/**
 * @brief Recebe uma falha individual no fluxo normal, fora de tratadores de sinais.
 * @param error Registro válido durante a chamada; pode ser copiado por valor.
 * @param user_data Estado do observador, pertencente ao chamador; pode ser nullptr.
 * @warning Não reentrar na operação observada nem destruir seus recursos.
 */
typedef void (*mpmc_diagnostic_fn)(const mpmc_diagnostic_t *error, void *user_data);

/** @brief Destino opcional dos diagnósticos desta thread/processo. */
typedef struct {
    mpmc_diagnostic_fn on_error; /**< Callback; nullptr desabilita a apresentação. */
    void *user_data;             /**< Estado válido até substituir o observador. */
} mpmc_reporter_t;

/**
 * @brief Instala uma cópia do observador; nullptr desabilita a callback.
 * @param reporter Configuração copiada; o estado apontado continua pertencendo ao chamador.
 * @details A configuração é local à thread e é herdada por fork(). O filho
 * pode substituí-la por um emissor para seu pipe, sem afetar o processo pai.
 */
void mpmc_diagnostic_set_reporter(const mpmc_reporter_t *reporter);

/**
 * @brief Desvincula escopos herdados do pai, antes de iniciar o fluxo do filho.
 * @param operation_id Identidade do trabalho executado pelo filho.
 * @param producer_id Estrutura lógica, ou zero para striped.
 */
void mpmc_diagnostic_after_fork(uint64_t operation_id, size_t producer_id);

/**
 * @brief Escopo de uma chamada; mantém o primeiro erro e a hierarquia de cleanup.
 * @details Objeto automático, inicializado por begin e finalizado exatamente uma vez.
 */
typedef struct mpmc_diagnostic_scope {
    const char *function;                 /**< Nome estático da função. */
    struct mpmc_diagnostic_scope *parent; /**< Escopo chamador, se houver. */
    mpmc_result_t first;                  /**< Primeiro erro não tratado como controle de fluxo. */
    uint64_t group;                       /**< Grupo da chamada raiz. */
    bool cleanup;                         /**< Cleanup explícito ou herdado. */
    bool fatal;                           /**< Operação já alterou estado compartilhado. */
    bool primitive;                       /**< Primitiva que pode propagar EINTR para repetição. */
    int resource_id;                      /**< Recurso específico ou -1. */
} mpmc_diagnostic_scope_t;

/**
 * @brief Inicia um escopo; cleanup é herdado dos chamadores.
 * @param scope Objeto automático a finalizar exatamente uma vez por end().
 * @param function Nome estático da chamada, válido até finalizar o escopo.
 * @param cleanup Marca finalização, rollback ou liberação.
 * @param primitive Indica wrapper cujo EINTR pode ser repetido pelo chamador.
 */
void mpmc_diagnostic_begin(
    mpmc_diagnostic_scope_t *scope,
    const char *function,
    bool cleanup,
    bool primitive
);

/**
 * @brief Finaliza o escopo, preservando o primeiro erro e evitando propagação duplicada.
 * @param scope Escopo ativo, inicializado por begin.
 * @param result Resultado produzido pela função.
 * @return Estrutura mpmc_result_t com result; em escopos de cleanup, preserva
 * o primeiro erro observado caso alguma liberação tenha falhado.
 * @details WOULD_BLOCK é controle de fluxo. EINTR de primitivas não gera
 * diagnóstico até que uma camada decida propagá-lo em vez de repetir a chamada.
 */
mpmc_result_t mpmc_diagnostic_end(mpmc_diagnostic_scope_t *scope, mpmc_result_t result);

/**
 * @brief Registra uma falha individual na etapa do escopo atual; preserva errno.
 * @param operation Operação que originou o resultado, copiada para o registro.
 * @param result Resultado capturado; OK e WOULD_BLOCK não emitem diagnóstico.
 */
void mpmc_diagnostic_report(const char *operation, mpmc_result_t result);

/**
 * @brief Registra a causa antes do rollback, se ela ainda não foi comunicada.
 * @param operation Nome da operação que falhou.
 * @param result Resultado original, com sua causa de sistema quando aplicável.
 */
void mpmc_diagnostic_cause(const char *operation, mpmc_result_t result);

/**
 * @brief Entrega um diagnóstico recebido de outro processo, sem alterar o fluxo do pai.
 * @param error Registro recebido e validado; copiado por quem precisar retê-lo.
 */
void mpmc_diagnostic_deliver(const mpmc_diagnostic_t *error);

/**
 * @brief Indica se existe observador instalado nesta thread.
 * @return true quando a configuração contém callback não nula.
 */
bool mpmc_diagnostic_enabled(void);

/**
 * @brief Comunica resultado ainda não diagnosticado por uma primitiva substituta.
 * @param operation Operação executada.
 * @param result Resultado capturado, preservado por valor.
 * @param before Contador lido imediatamente antes da operação.
 * @details Evita duplicar o diagnóstico quando o wrapper já o emitiu.
 */
void mpmc_diagnostic_observe(const char *operation, mpmc_result_t result, uint64_t before);

/**
 * @brief Quantidade de diagnósticos emitidos ou recebidos nesta thread.
 * @return Contador desde o início ou a última chamada de after_fork().
 */
uint64_t mpmc_diagnostic_count(void);

#endif
