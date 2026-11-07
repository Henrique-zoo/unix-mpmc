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

#include "mpmc/error.h"

/** @ingroup sysv_primitives
 * @{ */

/**
 * @brief Cria uma fila System V privada.
 * @param out_msqid Recebe o ID de fila criado por msgget().
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: fila privada criada e identificador escrito em out_msqid; system_errno é zero.
 * - MPMC_ERR_INVALID_ARG: out_msqid nulo; system_errno é zero.
 * - MPMC_ERR_SYS: falha em msgget(); system_errno contém o errno capturado nessa falha.
 * @warning A fila persiste no kernel até sua remoção por msgctl(IPC_RMID).
 */
mpmc_result_t sysv_msg_create(int *out_msqid);

/**
 * @brief Envia o corpo de uma mensagem com tipo explícito.
 * @param msqid Fila System V previamente criada.
 * @param type Tipo positivo (`mtype > 0`), conforme System V.
 * @param payload Endereço do corpo binário; pode ser NULL apenas se size==0.
 * @param size Quantidade de bytes do corpo (exclui o `long mtype`).
 * @param nowait Se true, passa IPC_NOWAIT a msgsnd().
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: corpo inserido na fila com o tipo solicitado; system_errno é zero.
 * - MPMC_ERR_WOULD_BLOCK: fila sem capacidade e nowait=true; system_errno contém EAGAIN
 *   retornado por msgsnd().
 * - MPMC_ERR_INTERRUPTED: msgsnd() interrompida por sinal; system_errno contém EINTR.
 * - MPMC_ERR_SYS: falha em malloc() ao alocar o envelope ou em msgsnd() por outra causa;
 *   system_errno contém o errno capturado na operação que falhou.
 * @details Wrapper monta internamente o envelope `{long mtype; unsigned char[]}`
 * necessário para a chamada de sistema. Não pode serializar ponteiros locais.
 * @pre Se size for positivo, payload deve apontar para pelo menos size bytes legíveis.
 * @pre size deve permitir a soma com o cabeçalho long sem overflow de size_t.
 */
mpmc_result_t sysv_msg_send(int msqid, long type, const void *payload, size_t size, bool nowait);

/**
 * @brief Recebe o corpo de uma mensagem da fila.
 * @param msqid Identificador da fila.
 * @param desired_type Tipo desejado conforme regras de msgrcv(); `0` recebe
 * a primeira mensagem FIFO. Valores positivos filtram pelo tipo.
 * @param out_payload Destino do corpo, com capacidade `capacity`.
 * @param capacity Máximo de bytes que cabem no destino.
 * @param nowait Se true, usa IPC_NOWAIT para não esperar mensagem.
 * @param out_size Recebe bytes efetivamente copiados em sucesso.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: mensagem retirada e corpo copiado; out_size recebe os bytes do corpo e system_errno
 *   é zero.
 * - MPMC_ERR_WOULD_BLOCK: nenhuma mensagem compatível com desired_type e nowait=true;
 *   system_errno contém ENOMSG retornado por msgrcv().
 * - MPMC_ERR_INTERRUPTED: msgrcv() interrompida por sinal; system_errno contém EINTR.
 * - MPMC_ERR_SYS: falha em malloc() ao alocar o envelope ou em msgrcv() por outra causa,
 *   inclusive E2BIG quando o corpo excede capacity; system_errno contém o errno capturado na
 *   operação que falhou.
 * @note Não usar MSG_NOERROR para truncar silenciosamente mensagens.
 * @note O valor retornado em out_size exclui `long mtype`.
 * @pre out_size deve apontar para um ssize_t válido. Se capacity for positiva,
 * out_payload deve possuir pelo menos capacity bytes disponíveis para escrita.
 * @pre capacity deve permitir a soma com o cabeçalho long sem overflow de size_t.
 * @post Em falha, out_size e o buffer de saída permanecem inalterados.
 */
mpmc_result_t sysv_msg_receive(
    int msqid, long desired_type, void *out_payload, size_t capacity, bool nowait, ssize_t *out_size
);

/**
 * @brief Remove uma fila de mensagens pertencente ao supervisor.
 * @param msqid ID da fila System V.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: fila removida com IPC_RMID; system_errno é zero.
 * - MPMC_ERR_SYS: falha em msgctl(IPC_RMID); system_errno contém o errno capturado nessa falha.
 * @warning Não remover em um worker: outros processos podem depender da fila.
 */
mpmc_result_t sysv_msg_remove(int msqid);

/** @} */
#endif
