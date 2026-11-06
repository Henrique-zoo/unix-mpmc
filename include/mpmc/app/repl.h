/**
 * @file repl.h
 * @brief Ciclo de leitura, validação e despacho de comandos interativos.
 * @ingroup application
 */
#ifndef MPMC_APP_REPL_H
#define MPMC_APP_REPL_H

#include "mpmc/app/supervisor.h"
#include "mpmc/error.h"

/** @ingroup application
 * @{ */

/**
 * @brief Executa o loop de leitura e despacho até `shutdown` ou EOF.
 * @details Mostra `>>`, lê a linha completa, chama command_parse(),
 * command_validate() e supervisor_dispatch(). O REPL deve continuar a receber
 * novos comandos enquanto existem workers vivos e precisa permitir que o
 * supervisor recolha filhos terminados periodicamente, sem criar zumbis.
 * No modo de filas, não pode interromper o escoamento de eventos de consumo
 * enquanto aguarda stdin.
 *
 * O loop espera com poll() em `stdin` e no self-pipe do supervisor
 * (supervisor_event_fd()). O tratador de `SIGCHLD` apenas chama
 * supervisor_notify_signal(), que acorda o loop imediatamente; a coleta com
 * waitpid() e a drenagem de eventos ocorrem fora do contexto de sinal, via
 * supervisor_reap() e supervisor_poll_events(). Um tempo limite curto no poll()
 * serve como rede de segurança para eventos que não geram sinal.
 *
 * Linhas em branco são ignoradas. Quando supervisor_take_notices() indica que o
 * supervisor imprimiu um aviso e a saída padrão é um terminal, o prompt é
 * redesenhado sem esperar nova entrada.
 *
 * `EOF` sem `shutdown` deve iniciar o mesmo caminho de encerramento ordenado.
 * Leituras interrompidas por sinal exigem tratamento explícito de `EINTR`.
 *
 * @param supervisor Supervisor inicializado e pertencente ao processo principal.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: loop encerrado por shutdown, EOF, SIGINT ou SIGTERM, sem falha reportada no
 *   encerramento; system_errno é zero.
 * - MPMC_ERR_INVALID_ARG: supervisor nulo; system_errno é zero.
 * - MPMC_ERR_SYS: falha em tcgetpgrp(), sigaction(), poll(), read() ou, por propagação do
 *   supervisor, kill() ou waitpid(); system_errno contém o errno capturado na operação que
 *   falhou. O campo é zero para execução em background com stdin no terminal, eventos
 *   POLLERR/POLLNVAL ou falhas internas propagadas pelo supervisor.
 *
 * Erros recuperáveis de parsing, validação e despacho são apresentados e não se tornam o retorno
 * do REPL. Leituras e poll() interrompidos por EINTR são repetidos.
 * @pre supervisor não é NULL nem foi encerrado/destruído.
 * @post Não ficam comandos pendentes de processamento.
 * @warning Leitura de stdin em programa posto em background pelo shell pode
 * causar suspensão por SIGTTIN com job control ativo.
 */
mpmc_result_t repl_run(supervisor_t *supervisor);

/** @} */
#endif
