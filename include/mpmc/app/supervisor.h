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
 * @brief Cria o supervisor e inicializa o paradigma de IPC solicitado.
 * @param out Recebe a instância alocada; em falha recebe NULL.
 * @param producer_count Quantidade de estruturas lógicas, estritamente positiva.
 * @param mode Paradigma IPC_SHARED_MEMORY (1) ou IPC_MESSAGE_QUEUE (2).
 * @retval MPMC_OK Recursos criados e disponíveis.
 * @retval MPMC_ERR_INVALID_ARG Parâmetros inválidos.
 * @retval MPMC_ERR_SYS Falha de alocação ou de chamada de sistema.
 * @post Em falha, libera/ remove todos os objetos IPC parcialmente criados.
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
 * @details Para produção/consumo, atribui um operation_id, registra tempo
 * monotônico, executa fork() e inicia producer_run()/consumer_run() no filho.
 * O processo pai não bloqueia aguardando a conclusão do worker.
 * @note COMMAND_SHUTDOWN delega a supervisor_shutdown().
 */
mpmc_status_t supervisor_dispatch(supervisor_t *supervisor, const command_t *command);

/**
 * @brief Recolhe filhos encerrados e registra resultados e durações.
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
 * @brief Inicia encerramento ordenado e imprime as métricas obrigatórias.
 * @param supervisor Supervisor que ainda detém os recursos globais.
 * @retval MPMC_OK Encerramento executado sem falhas relevantes.
 * @retval MPMC_ERR_SYS Algum filho ou recurso não pôde ser encerrado/removido.
 * @details Impede novos comandos, finaliza workers vivos (mesmo bloqueados),
 * executa waitpid(), drena eventos do backend, registra duração de cada filho,
 * total consumido por estrutura e duração global do mpmc, e remove objetos IPC.
 * @post Os objetos IPC globais são removidos e nenhum filho permanece ativo.
 * @note Deve ser segura contra chamadas repetidas (idempotência de shutdown).
 */
mpmc_status_t supervisor_shutdown(supervisor_t *supervisor);

/**
 * @brief Libera memória local do supervisor após o encerramento.
 * @param supervisor Instância encerrada; NULL é aceito (sem efeito).
 * @pre Se não for NULL, supervisor_shutdown() já foi chamado.
 * @note Não substitui supervisor_shutdown() e não pode deixar IPC global órfão.
 */
void supervisor_destroy(supervisor_t *supervisor);

/** @} */
#endif
