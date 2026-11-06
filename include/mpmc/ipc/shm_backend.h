/**
 * @file shm_backend.h
 * @brief Backend de buffer circular em memória compartilhada com semáforos.
 * @ingroup backends
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
 * @details Contém IDs dos segmentos System V (2048 bytes cada), conjuntos de
 * semáforos, mapeamentos atuais e infraestrutura de ACK/estatísticas.
 * Apenas `ipc/transport.c` deve acessar diretamente esta interface.
 */
typedef struct shm_backend shm_backend_t;

/**
 * @brief Inicializa um segmento de 2048 bytes e semáforos por produtor lógico.
 * @param out Recebe o backend criado; NULL em falha.
 * @param producer_count Número positivo de estruturas independentes.
 * @return Estrutura mpmc_result_t. A função ainda não possui implementação;
 * os valores possíveis de status e system_errno não podem ser verificados.
 * @post Em erro, remove segmentos/semáforos criados parcialmente.
 * @details O layout deve caber EXATAMENTE no segmento exigido pelo trabalho;
 * usar sizeof/alinhamento real para calcular a capacidade do buffer.
 */
mpmc_result_t shm_backend_create(shm_backend_t **out, size_t producer_count);

/**
 * @brief Insere mensagem no buffer circular do índice escolhido.
 * @param backend Instância do backend herdada no processo produtor.
 * @param producer_id Índice válido 1..p.
 * @param message Mensagem lógica, incluindo metadados de ACK.
 * @return Estrutura mpmc_result_t. A função ainda não possui implementação;
 * os valores possíveis de status e system_errno não podem ser verificados.
 * @details Reserva `empty`, adquire `mutex`, escreve e atualiza `tail`,
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
 * @return Estrutura mpmc_result_t. A função ainda não possui implementação;
 * os valores possíveis de status e system_errno não podem ser verificados.
 * @details Reserva `full`, adquire mutex, retira e atualiza `head`,
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
 * @return Estrutura mpmc_result_t. A função ainda não possui implementação;
 * os valores possíveis de status e system_errno não podem ser verificados.
 * @note Consumidores podem precisar de um identificador de ACK no envelope
 * físico do buffer; a codificação é detalhe privado deste backend.
 */
mpmc_result_t
shm_backend_ack_begin(shm_backend_t *backend, uint64_t operation_id, unsigned expected);

/**
 * @brief Aguarda confirmação de todos os consumos de uma operação.
 * @param backend Backend do filho produtor.
 * @param operation_id Trabalho registrado anteriormente.
 * @return Estrutura mpmc_result_t. A função ainda não possui implementação;
 * os valores possíveis de status e system_errno não podem ser verificados.
 * @note O progresso das confirmações deve sobreviver a mudanças de processo
 * e à chegada de ACKs antes da chamada de espera.
 */
mpmc_result_t shm_backend_ack_wait(shm_backend_t *backend, uint64_t operation_id);

/**
 * @brief Remove mecanismo temporário de ACK de um trabalho.
 * @param backend Backend do filho produtor.
 * @param operation_id ID da operação previamente registrada.
 * @return Estrutura mpmc_result_t. A função ainda não possui implementação;
 * os valores possíveis de status e system_errno não podem ser verificados.
 */
mpmc_result_t shm_backend_ack_end(shm_backend_t *backend, uint64_t operation_id);

/**
 * @brief Obtém total de mensagens consumidas, com sincronização adequada.
 * @param backend Instância no supervisor.
 * @param producer_id Índice lógico 1..p.
 * @param out_count Destino do acumulador de consumo deste segmento.
 * @return Estrutura mpmc_result_t. A função ainda não possui implementação;
 * os valores possíveis de status e system_errno não podem ser verificados.
 */
mpmc_result_t
shm_backend_consumed_count(shm_backend_t *backend, size_t producer_id, uint64_t *out_count);

/**
 * @brief Desanexa mapeamentos herdados pelo processo filho.
 * @param backend Instância herdada após fork().
 * @details Chama shmdt() conforme necessário e libera estruturas apenas locais
 * a este processo. Não remove os segmentos nem semáforos globais.
 */
void shm_backend_worker_close(shm_backend_t *backend);

/**
 * @brief Remove objetos IPC e libera estado pertencente ao processo pai.
 * @param backend Instância criada pelo supervisor; NULL é permitido.
 * @pre Todos os trabalhadores foram interrompidos/recolhidos.
 * @details Remove segmentos shm e semáforos System V via IPC_RMID. Evita
 * remover identificadores que não foram realmente criados pela instância.
 */
void shm_backend_destroy(shm_backend_t *backend);

/** @} */
#endif
