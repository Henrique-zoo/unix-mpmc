/**
 * @file shm_backend.h
 * @brief Backend de buffer circular em memória compartilhada com semáforos.
 * @ingroup backends
 * @note Estão implementados a criação e a inicialização dos buffers, o fechamento
 * local e a destruição dos segmentos. Envio, recebimento, ACKs e contagens abaixo
 * são contratos propostos.
 */
#ifndef MPMC_IPC_SHM_BACKEND_H
#define MPMC_IPC_SHM_BACKEND_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "mpmc/error.h"
#include "mpmc/message.h"

/**
 * @defgroup backends Backends dos paradigmas
 * @ingroup ipc
 * @brief Implementações concretas da API de transporte, selecionadas pela fachada.
 * @{ */

/**
 * @brief Estado opaco do backend por memória compartilhada.
 * @details Mantém os segmentos System V de 2048 bytes e suas associações locais.
 * Apenas `ipc/transport.c` deve acessar diretamente esta interface.
 */
typedef struct shm_backend shm_backend_t;

/** @brief Operação de liberação que originou um diagnóstico de cleanup. */
typedef enum {
    SHM_CLEANUP_DETACH, /**< Falha ao desassociar um mapeamento com shmdt(). */
    SHM_CLEANUP_REMOVE  /**< Falha ao remover um segmento com shmctl(IPC_RMID). */
} shm_cleanup_operation_t;

/**
 * @brief Diagnóstico de uma falha individual na liberação de um segmento.
 * @details Os campos são copiados por valor e não dependem do estado do backend
 * após sua liberação. O registro recebido pela callback só é válido durante a
 * chamada; copie o conteúdo de *error, não o ponteiro error, para armazenar o
 * diagnóstico. A cópia pertence ao chamador e não exige liberação adicional.
 * @see @ref shm_cleanup_diagnostics para o fluxo e um exemplo de coleta.
 */
typedef struct {
    shm_cleanup_operation_t operation; /**< Operação de liberação que falhou. */
    size_t producer_id;                /**< Índice lógico da estrutura, no intervalo 1..p. */
    int shmid;                         /**< Identificador do segmento envolvido, inclusive zero. */
    mpmc_result_t result;              /**< Falha individual, com o errno original preservado. */
} shm_cleanup_error_t;

/**
 * @brief Recebe uma falha individual durante a limpeza do backend.
 * @param error Diagnóstico válido somente durante a chamada.
 * @param user_data Ponteiro fornecido pelo chamador, sem interpretação pelo backend;
 * pode ser NULL se a callback não precisar de estado adicional.
 * @details Este tipo declara um ponteiro para função. O chamador atribui sua
 * função ao campo on_error de shm_cleanup_reporter_t. A cada falha de liberação,
 * o backend executa on_error(error, user_data), no mesmo fluxo da chamada que
 * está realizando a limpeza.
 *
 * A callback não é chamada para operações bem-sucedidas. Em rollback, recebe
 * somente as falhas de liberação; a falha que impediu a criação é retornada por
 * shm_backend_create(). O retorno da callback é void: ela registra ou apresenta
 * o diagnóstico e retorna para que a limpeza prossiga.
 * @note A callback não deve destruir o backend que já está sendo limpo, nem
 * invalidar o reporter ou seus dados antes que a chamada de limpeza termine.
 * @see @ref shm_cleanup_diagnostics para um exemplo de implementação.
 */
typedef void (*shm_cleanup_error_fn)(const shm_cleanup_error_t *error, void *user_data);

/**
 * @brief Destino opcional dos diagnósticos individuais de cleanup.
 * @details O backend não retém este objeto nem adquire a propriedade de user_data.
 * Ambos devem permanecer válidos durante a operação que recebe o reporter.
 * O campo on_error informa qual função executar; user_data informa onde essa
 * função encontra o estado do chamador, como um buffer de diagnósticos.
 *
 * O reporter vale somente para a chamada que o recebe. Passá-lo a create()
 * habilita os diagnósticos de rollback dessa criação; passe-o novamente a
 * worker_close() ou destroy() para receber os diagnósticos dessas operações.
 *
 * Não há alocação de uma lista de erros pelo backend. Um reporter nulo ou uma
 * callback nula desabilita os diagnósticos, preservando o resultado de retorno.
 * @see @ref shm_cleanup_diagnostics para armazenamento, duração dos dados e resultados.
 */
typedef struct {
    shm_cleanup_error_fn on_error; /**< Callback chamada uma vez para cada falha de liberação. */
    void *user_data;               /**< Dados repassados à callback, pertencentes ao chamador. */
} shm_cleanup_reporter_t;

/**
 * @brief Cria e associa um segmento de 2048 bytes por estrutura lógica.
 * @param[out] out Recebe o backend criado; recebe NULL em falha, quando o ponteiro é válido.
 * @param producer_count Número positivo de estruturas independentes.
 * @param reporter Destino dos diagnósticos de rollback; pode ser NULL.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: backend alocado, segmentos criados e associados ao processo, com os
 *   cabeçalhos dos buffers inicializados; system_errno é zero.
 * - MPMC_ERR_INVALID_ARG: out nulo ou producer_count zero; system_errno é zero.
 * - MPMC_ERR_SYS: falha em malloc(), calloc(), shmget() ou shmat(); system_errno contém
 *   o errno capturado na operação que falhou, preservado durante a limpeza.
 * @post Em sucesso, *out contém o backend com producer_count segmentos associados,
 * cada um com buffer vazio, índices e total consumido zerados e capacidade calculada.
 * Em falha, *out permanece NULL quando out é válido, as alocações locais são liberadas
 * e a limpeza dos segmentos parcialmente criados é tentada.
 * @note Falhas de rollback são comunicadas por reporter e não substituem o erro
 * original de criação ou associação retornado pela função. Sem reporter habilitado,
 * essas falhas secundárias não ficam disponíveis após o retorno.
 * @note A criação de semáforos ainda não está implementada nesta função.
 */
mpmc_result_t shm_backend_create(
    shm_backend_t **out, size_t producer_count, const shm_cleanup_reporter_t *reporter
);

/**
 * @brief Insere mensagem no buffer circular do índice escolhido.
 * @param backend Instância do backend herdada no processo produtor.
 * @param producer_id Índice válido 1..p.
 * @param message Mensagem lógica, com o identificador da operação produtora.
 * @return Estrutura mpmc_result_t com o seguinte estado do campo status:
 * - MPMC_ERR_STATE: operação ainda não implementada; system_errno é zero.
 * @details Contrato previsto: reserva `empty`, adquire `mutex`, escreve e atualiza `tail`,
 * solta `mutex` e sinaliza `full`. Não mantém mutex enquanto espera espaço.
 */
mpmc_result_t
shm_backend_send(shm_backend_t *backend, size_t producer_id, const message_t *message);

/**
 * @brief Retira mensagem de um buffer circular.
 * @param backend Backend inicializado.
 * @param producer_id Índice de origem 1..p.
 * @param out Recebe item em sucesso.
 * @param blocking Habilita/desabilita espera por `full`.
 * @return Estrutura mpmc_result_t com o seguinte estado do campo status:
 * - MPMC_ERR_STATE: operação ainda não implementada; system_errno é zero.
 * @details Contrato previsto: reserva `full`, adquire mutex, retira e atualiza `head`,
 * contabiliza, solta mutex, sinaliza `empty`, confirma consumo se solicitado.
 * A confirmação não deve depender de mutex do buffer ainda adquirido.
 */
mpmc_result_t
shm_backend_receive(shm_backend_t *backend, size_t producer_id, message_t *out, bool blocking);

/**
 * @brief Reserva mecanismo de confirmação para uma operação produtora.
 * @param backend Backend do filho produtor.
 * @param operation_id ID único de trabalho, gerado pelo supervisor.
 * @param expected Quantidade total de confirmações (1..1024).
 * @return Estrutura mpmc_result_t com o seguinte estado do campo status:
 * - MPMC_ERR_STATE: operação ainda não implementada; system_errno é zero.
 * @note Consumidores podem precisar de um identificador de ACK no envelope
 * físico do buffer; a codificação é detalhe privado deste backend.
 */
mpmc_result_t
shm_backend_ack_begin(shm_backend_t *backend, uint64_t operation_id, unsigned expected);

/**
 * @brief Aguarda confirmação de todos os consumos de uma operação.
 * @param backend Backend do filho produtor.
 * @param operation_id Trabalho registrado anteriormente.
 * @return Estrutura mpmc_result_t com o seguinte estado do campo status:
 * - MPMC_ERR_STATE: operação ainda não implementada; system_errno é zero.
 * @note O progresso das confirmações deve sobreviver a mudanças de processo
 * e à chegada de ACKs antes da chamada de espera.
 */
mpmc_result_t shm_backend_ack_wait(shm_backend_t *backend, uint64_t operation_id);

/**
 * @brief Remove mecanismo temporário de ACK de um trabalho.
 * @param backend Backend do filho produtor.
 * @param operation_id ID da operação previamente registrada.
 * @return Estrutura mpmc_result_t com o seguinte estado do campo status:
 * - MPMC_ERR_STATE: operação ainda não implementada; system_errno é zero.
 */
mpmc_result_t shm_backend_ack_end(shm_backend_t *backend, uint64_t operation_id);

/**
 * @brief Obtém total de mensagens consumidas, com sincronização adequada.
 * @param backend Instância no supervisor.
 * @param producer_id Índice lógico 1..p.
 * @param out_count Destino do acumulador de consumo deste segmento.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: out_count recebe zero provisoriamente; system_errno é zero.
 * - MPMC_ERR_INVALID_ARG: backend ou out_count nulo, ou producer_id fora de 1..p;
 *   system_errno é zero.
 * @note A consulta sincronizada do contador compartilhado ainda depende da #10.
 */
mpmc_result_t
shm_backend_consumed_count(shm_backend_t *backend, size_t producer_id, uint64_t *out_count);

/**
 * @brief Desanexa mapeamentos herdados pelo processo filho.
 * @param backend Instância herdada após fork(); NULL é permitido.
 * @param reporter Destino dos diagnósticos de liberação; pode ser NULL.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: associações locais desfeitas e estado local liberado, ou backend nulo;
 *   system_errno é zero.
 * - MPMC_ERR_SYS: falha em shmdt(); system_errno contém o errno capturado na primeira
 *   falha. Cada falha individual é comunicada por reporter, quando habilitado.
 * @details Tenta todas as desassociações, mesmo após uma falha, e libera o estado
 * local. Os segmentos globais permanecem sob responsabilidade do supervisor.
 * @post O backend é liberado mesmo em erro e não pode ser reutilizado após o retorno.
 */
mpmc_result_t
shm_backend_worker_close(shm_backend_t *backend, const shm_cleanup_reporter_t *reporter);

/**
 * @brief Remove objetos IPC e libera estado pertencente ao processo pai.
 * @param backend Instância criada pelo supervisor; NULL é permitido.
 * @param reporter Destino dos diagnósticos de liberação; pode ser NULL.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: desassociações e solicitações de remoção concluídas, ou backend nulo;
 *   system_errno é zero.
 * - MPMC_ERR_SYS: falha em shmdt() ou shmctl(IPC_RMID); system_errno contém o errno
 *   capturado na primeira falha. Cada falha individual é comunicada por reporter,
 *   quando habilitado.
 * @pre Todos os trabalhadores foram interrompidos/recolhidos.
 * @details Aceita estado parcialmente inicializado. Tenta todas as desassociações
 * e remoções de segmentos, mesmo após falhas, e libera as alocações locais.
 * @post O backend é liberado mesmo em erro e não pode ser reutilizado após o retorno.
 */
mpmc_result_t shm_backend_destroy(shm_backend_t *backend, const shm_cleanup_reporter_t *reporter);

/** @} */
#endif
