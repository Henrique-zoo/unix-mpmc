/**
 * @file message.h
 * @brief Mensagem lógica compartilhada entre os paradigmas de IPC.
 * @ingroup core
 */
#ifndef MPMC_MESSAGE_H
#define MPMC_MESSAGE_H

#include <stdbool.h>
#include <stdint.h>
#include <sys/types.h>
#include <time.h>

/** @ingroup core
 * @{ */

/** @brief Tamanho obrigatório de cada segmento de memória compartilhada. */
#define MPMC_SHM_SEGMENT_BYTES 2048U

/** @brief Menor quantidade válida de mensagens por comando. */
#define MPMC_MIN_MESSAGES 1U

/** @brief Maior quantidade válida de mensagens por comando. */
#define MPMC_MAX_MESSAGES 1024U

/**
 * @brief Dados lógicos de uma mensagem produzida.
 * @details O enunciado exige sender_pid, sequence e sent_at. Os campos
 * operation_id e ack_required são metadados propostos para correlacionar
 * confirmações. Um backend de filas System V adicionará separadamente
 * seu envelope `long mtype`; `mtype` não faz parte desta estrutura.
 *
 * `sequence` começa em 1 para cada processo produtor e avança por envio,
 * inclusive em comandos striped. `sent_at` usa CLOCK_REALTIME.
 */
typedef struct {
    pid_t sender_pid;         /**< PID real do processo produtor que enviou a mensagem. */
    uint32_t sequence;        /**< Sequência do processo produtor, iniciando em 1. */
    struct timespec sent_at;  /**< Instante do envio (relógio de calendário). */
    uint64_t operation_id;    /**< Identificador único do comando atribuído pelo supervisor. */
    bool ack_required;        /**< Se o consumo deve confirmar a entrega ao produtor. */
} message_t;

/** @} */
#endif
