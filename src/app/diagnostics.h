/** @file diagnostics.h
 * @brief Apresentação dos diagnósticos da aplicação.
 * @ingroup application
 * @details Observador instalado por main() para erros locais do pai e registros
 * entregues pelo supervisor a partir dos pipes. Agrupa a apresentação por chamada
 * e etapa, incluindo cleanup; o emissor no filho pertence ao supervisor.
 * @see @ref flow_diagnostics para o percurso do registro e as regras de cor.
 */
#ifndef MPMC_APP_DIAGNOSTICS_H
#define MPMC_APP_DIAGNOSTICS_H

#include <stdio.h>

#include "mpmc/diagnostic.h"

/** @brief Estado da apresentação; não retém registros nem usa buffers limitados de erros. */
typedef struct {
    FILE *output;                  /**< Destino pertencente ao chamador. */
    pid_t process_pid;             /**< Último processo apresentado. */
    uint64_t group;                /**< Última chamada apresentada. */
    mpmc_diagnostic_phase_t phase; /**< Última etapa apresentada. */
    bool started;                  /**< Já foi apresentado algum erro. */
} app_diagnostics_t;

/**
 * @brief Callback que imprime a causa de cada falha sob seu diagnóstico principal.
 * @param error Registro válido recebido pelo observador.
 * @param user_data app_diagnostics_t válido, com output aberto para escrita.
 * @details Usa vermelho somente em terminal, salvo NO_COLOR definido ou TERM=dumb.
 * A configuração do observador pertence ao chamador, instalado em main().
 */
void app_diagnostic_print(const mpmc_diagnostic_t *error, void *user_data);

#endif
