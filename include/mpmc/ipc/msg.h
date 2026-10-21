/**
 * @file msg.h
 * @brief Wrappers mínimos sobre filas de mensagens System V.
 * @ingroup sysv_primitives
 */
#ifndef MPMC_IPC_MSG_H
#define MPMC_IPC_MSG_H

#include <stdbool.h>
#include <stddef.h>
#include <sys/types.h>
#include "mpmc/status.h"

/** @ingroup sysv_primitives
 * @{ */

/**
 * @brief Cria uma fila System V privada.
 * @param out_msqid Recebe o ID de fila criado por msgget().
 * @retval MPMC_OK Fila criada.
 * @retval MPMC_ERR_INVALID_ARG Ponteiro de saída inválido.
 * @retval MPMC_ERR_SYS Falha de msgget().
 * @warning A fila persiste no kernel até sua remoção por msgctl(IPC_RMID).
 */
mpmc_status_t sysv_msg_create(int *out_msqid);

/**
 * @brief Envia o corpo de uma mensagem com tipo explícito.
 * @param msqid Fila System V previamente criada.
 * @param type Tipo positivo (`mtype > 0`), conforme System V.
 * @param payload Endereço do corpo binário; pode ser NULL apenas se size==0.
 * @param size Quantidade de bytes do corpo (exclui o `long mtype`).
 * @param nowait Se true, passa IPC_NOWAIT a msgsnd().
 * @retval MPMC_OK Corpo inserido na fila.
 * @retval MPMC_ERR_WOULD_BLOCK Fila cheia e nowait=true.
 * @retval MPMC_ERR_INTERRUPTED Operação interrompida por sinal.
 * @retval MPMC_ERR_SYS Falha de msgsnd().
 * @details Wrapper monta internamente o envelope `{long mtype; unsigned char[]}`
 * necessário para a chamada de sistema. Não pode serializar ponteiros locais.
 */
mpmc_status_t sysv_msg_send(
    int msqid,
    long type,
    const void *payload,
    size_t size,
    bool nowait
);

/**
 * @brief Recebe o corpo de uma mensagem da fila.
 * @param msqid Identificador da fila.
 * @param desired_type Tipo desejado conforme regras de msgrcv(); `0` recebe
 * a primeira mensagem FIFO. Valores positivos filtram pelo tipo.
 * @param out_payload Destino do corpo, com capacidade `capacity`.
 * @param capacity Máximo de bytes que cabem no destino.
 * @param nowait Se true, usa IPC_NOWAIT para não esperar mensagem.
 * @param out_size Recebe bytes efetivamente copiados em sucesso.
 * @retval MPMC_OK Mensagem retirada com sucesso.
 * @retval MPMC_ERR_WOULD_BLOCK Fila vazia / sem tipo desejado em modo nowait.
 * @retval MPMC_ERR_INTERRUPTED Recebimento interrompido por sinal.
 * @retval MPMC_ERR_SYS Falha em msgrcv() (inclusive E2BIG quando não cabe).
 * @note Não usar MSG_NOERROR para truncar silenciosamente mensagens.
 * @note O valor retornado em out_size exclui `long mtype`.
 */
mpmc_status_t sysv_msg_receive(
    int msqid,
    long desired_type,
    void *out_payload,
    size_t capacity,
    bool nowait,
    ssize_t *out_size
);

/**
 * @brief Remove uma fila de mensagens pertencente ao supervisor.
 * @param msqid ID da fila System V.
 * @retval MPMC_OK Fila removida com msgctl(IPC_RMID).
 * @retval MPMC_ERR_SYS Falha de msgctl().
 * @warning Não remover em um worker: outros processos podem depender da fila.
 */
mpmc_status_t sysv_msg_remove(int msqid);

/** @} */
#endif
