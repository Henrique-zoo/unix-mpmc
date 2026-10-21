/**
 * @file repl.h
 * @brief Ciclo de leitura, validação e despacho de comandos interativos.
 * @ingroup application
 */
#ifndef MPMC_APP_REPL_H
#define MPMC_APP_REPL_H

#include "mpmc/app/supervisor.h"
#include "mpmc/status.h"

/** @ingroup application
 * @{ */

/**
 * @brief Executa o loop de leitura e despacho até `shutdown` ou EOF.
 * @details Mostra `>>`, lê a linha completa, chama command_parse(),
 * command_validate() e supervisor_dispatch(). O REPL deve continuar a receber
 * novos comandos enquanto existem workers vivos e precisa permitir que o
 * supervisor recolha filhos terminados periodicamente, sem criar zumbis.
 * No modo de filas, não pode interromper o escoamento de eventos de consumo
 * enquanto aguarda stdin; considerar notificação por sinal + pselect/self-pipe.
 *
 * `EOF` sem `shutdown` deve iniciar o mesmo caminho de encerramento ordenado.
 * Leituras interrompidas por sinal exigem tratamento explícito de `EINTR`.
 *
 * @param supervisor Supervisor inicializado e pertencente ao processo principal.
 * @retval MPMC_OK REPL encerrado normalmente, com shutdown solicitado.
 * @retval MPMC_ERR_SYS Falha irrecuperável ao ler ou tratar a entrada.
 * @pre supervisor não é NULL nem foi encerrado/destruído.
 * @post Não ficam comandos pendentes de processamento.
 * @warning Leitura de stdin em programa posto em background pelo shell pode
 * causar suspensão por SIGTTIN com job control ativo.
 */
mpmc_status_t repl_run(supervisor_t *supervisor);

/** @} */
#endif
