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
#include "mpmc/message.h"
#include "mpmc/status.h"

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
 * @retval MPMC_OK Tudo inicializado.
 * @retval MPMC_ERR_INVALID_ARG Entrada inválida.
 * @retval MPMC_ERR_SYS Falha em shmget(), shmat(), semget() ou semctl().
 * @post Em erro, remove segmentos/semáforos criados parcialmente.
 * @details O layout deve caber EXATAMENTE no segmento exigido pelo trabalho;
 * usar sizeof/alinhamento real para calcular a capacidade do buffer.
 */
mpmc_status_t shm_backend_create(shm_backend_t **out, size_t producer_count);

/**
 * @brief Insere mensagem no buffer circular do índice escolhido.
 * @param backend Instância do backend herdada no processo produtor.
 * @param producer_id Índice válido 1..p.
 * @param message Mensagem lógica, incluindo metadados de ACK.
 * @retval MPMC_OK Inserida sem corrupção e anunciada como disponível.
 * @retval MPMC_ERR_INTERRUPTED Operação bloqueante cancelada.
 * @retval MPMC_ERR_SYS Falha de sincronização/mapeamento.
 * @details Reserva `empty`, adquire `mutex`, escreve e atualiza `tail`,
 * solta `mutex` e sinaliza `full`. Não mantém mutex enquanto espera espaço.
 */
mpmc_status_t shm_backend_send(
    shm_backend_t *backend,
    size_t producer_id,
    const message_t *message
);

/**
 * @brief Retira mensagem de um buffer circular.
 * @param backend Backend inicializado.
 * @param producer_id Índice de origem 1..p.
 * @param out Recebe item em sucesso.
 * @param blocking Habilita/desabilita espera por `full`.
 * @retval MPMC_OK Item removido, contabilizado e confirmado quando necessário.
 * @retval MPMC_ERR_WOULD_BLOCK Buffer vazio com blocking=false.
 * @retval MPMC_ERR_INTERRUPTED Espera cancelada.
 * @retval MPMC_ERR_SYS Falha no IPC.
 * @details Reserva `full`, adquire mutex, retira e atualiza `head`,
 * contabiliza, solta mutex, sinaliza `empty`, confirma consumo se solicitado.
 * A confirmação não deve depender de mutex do buffer ainda adquirido.
 */
mpmc_status_t shm_backend_receive(
    shm_backend_t *backend,
    size_t producer_id,
    message_t *out,
    bool blocking
);

/**
 * @brief Reserva mecanismo de confirmação para uma operação produtora.
 * @param backend Backend do filho produtor.
 * @param operation_id ID único de trabalho, gerado pelo supervisor.
 * @param expected Quantidade total de confirmações (1..1024).
 * @retval MPMC_OK Canal de confirmação pronto antes do primeiro envio.
 * @retval MPMC_ERR_SYS Falha na criação de semáforo/metadados de ACK.
 * @note Consumidores podem precisar de um identificador de ACK no envelope
 * físico do buffer; a codificação é detalhe privado deste backend.
 */
mpmc_status_t shm_backend_ack_begin(shm_backend_t *backend, uint64_t operation_id, unsigned expected);

/**
 * @brief Aguarda confirmação de todos os consumos de uma operação.
 * @param backend Backend do filho produtor.
 * @param operation_id Trabalho registrado anteriormente.
 * @retval MPMC_OK Recebeu a quantidade esperada de confirmações.
 * @retval MPMC_ERR_INTERRUPTED Espera cancelada por shutdown/sinal.
 * @retval MPMC_ERR_SYS Falha do mecanismo de ACK.
 * @note O progresso das confirmações deve sobreviver a mudanças de processo
 * e à chegada de ACKs antes da chamada de espera.
 */
mpmc_status_t shm_backend_ack_wait(shm_backend_t *backend, uint64_t operation_id);

/**
 * @brief Remove mecanismo temporário de ACK de um trabalho.
 * @param backend Backend do filho produtor.
 * @param operation_id ID da operação previamente registrada.
 * @retval MPMC_OK Recurso de confirmação liberado.
 * @retval MPMC_ERR_SYS Falha ao desalocar o recurso de confirmação.
 */
mpmc_status_t shm_backend_ack_end(shm_backend_t *backend, uint64_t operation_id);

/**
 * @brief Obtém total de mensagens consumidas, com sincronização adequada.
 * @param backend Instância no supervisor.
 * @param producer_id Índice lógico 1..p.
 * @param out_count Destino do acumulador de consumo deste segmento.
 * @retval MPMC_OK Valor lido de forma consistente.
 * @retval MPMC_ERR_SYS Falha ao obter sincronização.
 */
mpmc_status_t shm_backend_consumed_count(
    shm_backend_t *backend,
    size_t producer_id,
    uint64_t *out_count
);

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
