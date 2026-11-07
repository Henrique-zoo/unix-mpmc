/**
 * @file message.h
 * @brief Mensagem lógica compartilhada entre os paradigmas de IPC.
 * @ingroup core
 */
#ifndef MPMC_MESSAGE_H
#define MPMC_MESSAGE_H

#include <assert.h>
#include <stdbool.h>
#include <stddef.h>
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
 * @details sender_pid, sequence e sent_at são campos mínimos obrigatórios do
 * enunciado e devem ser preservados por ambos os backends. operation_id e
 * ack_required são metadados internos adicionais para correlacionar confirmação;
 * não substituem os campos mínimos. Um backend de filas System V adiciona
 * separadamente o envelope `long mtype`, que não faz parte desta estrutura.
 *
 * `sender_pid` identifica o processo físico que envia (getpid()), enquanto
 * `producer_id` é o índice lógico da fila destino e não faz parte deste
 * registro. Em produção striped, um único PID envia para vários índices.
 * `sequence` começa em 1 para cada processo emissor e avança por envio,
 * inclusive em comandos striped. `sent_at` usa CLOCK_REALTIME.
 */
typedef struct {
    pid_t sender_pid;        /**< PID físico real do processo que enviou a mensagem (getpid()). */
    uint32_t sequence;       /**< Sequência crescente por processo emissor, iniciando em 1. */
    struct timespec sent_at; /**< Instante do envio (relógio de calendário). */
    uint64_t operation_id;   /**< Identificador único do comando atribuído pelo supervisor. */
    bool ack_required;       /**< Se o consumo deve confirmar a entrega ao produtor. */
} message_t;

static_assert(sizeof(message_t) == 40, "message_t deve ter 40 bytes (docs/protocol.md)");
static_assert(_Alignof(message_t) == 8, "message_t deve ter alinhamento 8");
static_assert(offsetof(message_t, sender_pid) == 0, "offset de sender_pid");
static_assert(offsetof(message_t, sequence) == 4, "offset de sequence");
static_assert(offsetof(message_t, sent_at) == 8, "offset de sent_at");
static_assert(offsetof(message_t, operation_id) == 24, "offset de operation_id");
static_assert(offsetof(message_t, ack_required) == 32, "offset de ack_required");

/** @} */
#endif
