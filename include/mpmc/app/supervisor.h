/**
 * @file supervisor.h
 * @brief Gerenciamento de IPC global, filhos, métricas e encerramento.
 * @ingroup application
 */
#ifndef MPMC_APP_SUPERVISOR_H
#define MPMC_APP_SUPERVISOR_H

#include <stdbool.h>
#include <stddef.h>

#include "mpmc/app/command.h"
#include "mpmc/ipc/transport.h"
#include "mpmc/status.h"

/** @ingroup application
 * @{ */

/**
 * @brief Estado opaco do supervisor; existe somente no processo principal.
 * @details Mantém contexto IPC, lista de filhos, identificador incremental de
 * operação, horário inicial monotônico e estatísticas já consolidadas.
 */
typedef struct supervisor supervisor_t;

/**
 * @brief Cria o estado local de supervisão dos processos.
 * @param out Recebe a instância alocada; em falha recebe NULL.
 * @param producer_count Quantidade de estruturas lógicas, estritamente positiva.
 * @param mode Paradigma IPC_SHARED_MEMORY (1) ou IPC_MESSAGE_QUEUE (2).
 * @retval MPMC_OK Supervisor criado.
 * @retval MPMC_ERR_INVALID_ARG Parâmetros inválidos.
 * @retval MPMC_ERR_SYS Falha de alocação ou de chamada de sistema.
 * @note A inicialização dos backends IPC ainda não está conectada nesta
 * implementação.
 * @par Propriedade
 * O chamador deve chamar supervisor_shutdown() e supervisor_destroy().
 */
mpmc_status_t supervisor_create(supervisor_t **out, size_t producer_count, ipc_mode_t mode);

/**
 * @brief Despacha um comando validado e retorna sem esperar um worker concluir.
 * @param supervisor Instância inicializada.
 * @param command Comando validado para esta instância.
 * @retval MPMC_OK Comando aceito; processo iniciado ou shutdown iniciado.
 * @retval MPMC_ERR_INVALID_ARG Comando não válido.
 * @retval MPMC_ERR_SYS Falha de fork ou outra operação do sistema.
 * @retval MPMC_ERR_SHUTDOWN Instância já está encerrando.
 * @details Comandos de trabalho são encaminhados por fork() sem bloquear o
 * pai. Até a implementação dos módulos workers/IPC, o filho reporta que a
 * operação não está disponível e termina com erro.
 * @note COMMAND_SHUTDOWN delega a supervisor_shutdown().
 */
mpmc_status_t supervisor_dispatch(supervisor_t *supervisor, const command_t *command);

/**
 * @brief Recolhe filhos encerrados sem bloquear, quando solicitado.
 * @param supervisor Instância viva do supervisor.
 * @param blocking Se false, usa waitpid(..., WNOHANG) repetidamente; se true,
 * espera até não haver mais filhos registrados (utilizar após cancelamento).
 * @retval MPMC_OK Recolhimento realizado ou nenhum filho pendente.
 * @retval MPMC_ERR_SYS Erro irrecuperável em waitpid().
 * @note Tratadores SIGCHLD só sinalizam a necessidade de coleta; waitpid()
 * é executado fora do contexto de sinal.
 */
mpmc_status_t supervisor_reap(supervisor_t *supervisor, bool blocking);

/**
 * @brief Impede novos comandos, termina filhos e os recolhe.
 * @param supervisor Supervisor que ainda detém os recursos globais.
 * @retval MPMC_OK Encerramento executado sem falhas relevantes.
 * @retval MPMC_ERR_SYS Algum filho ou recurso não pôde ser encerrado/removido.
 * @details Impede novos comandos, envia SIGTERM aos filhos, executa waitpid()
 * e processa o ponto de integração de eventos. A remoção de recursos IPC e o
 * relatório de métricas dependem dos backends ainda não implementados.
 * @post Nenhum filho registrado permanece ativo.
 * @note Deve ser segura contra chamadas repetidas (idempotência de shutdown).
 */
mpmc_status_t supervisor_shutdown(supervisor_t *supervisor);

/**
 * @brief Retorna a quantidade de estruturas configuradas no supervisor.
 * @param supervisor Instância inicializada.
 * @return Quantidade configurada, ou zero para NULL.
 */
size_t supervisor_producer_count(const supervisor_t *supervisor);

/**
 * @brief Processa eventos de controle sem bloquear.
 * @param supervisor Instância viva do supervisor.
 * @retval MPMC_OK Eventos drenados ou nenhum evento disponível.
 * @retval MPMC_ERR_SYS Falha ao processar o canal de controle.
 * @details Este ponto deve ser chamado periodicamente mesmo sem comandos
 * novos. A implementação atual mantém o ponto de integração; o backend de
 * IPC pode conectar aqui a drenagem de suas filas de eventos.
 */
mpmc_status_t supervisor_poll_events(supervisor_t *supervisor);

/**
 * @brief Libera memória local do supervisor após o encerramento.
 * @param supervisor Instância encerrada; NULL é aceito (sem efeito).
 * @pre Se não for NULL, supervisor_shutdown() já foi chamado.
 * @note Não substitui supervisor_shutdown() e não pode deixar IPC global órfão.
 */
void supervisor_destroy(supervisor_t *supervisor);

/** @} */
#endif
