/**
 * @file job_outcome.h
 * @brief Apresentação contextual do resultado de um trabalho (produz/consome).
 * @ingroup app
 *
 * @details
 * Cabeçalho interno de src/app/, sem equivalente em include/. Reúne o tipo que
 * descreve como um trabalho terminou e o formatador do texto mostrado ao usuário:
 * erro de kernel, consumo ou produção parcial, cancelamento e falha ao criar o
 * processo filho. O texto identifica a operação, o índice da estrutura e a
 * quantidade efetiva quando se aplicam, e nunca apresenta um resultado
 * incompleto como conclusão bem-sucedida.
 *
 * O módulo não imprime nada e não conhece o REPL: devolve texto em buffer do
 * chamador. A propagação dos resultados reais dos workers até aqui pertence às
 * issues #22 e #25; por ora os eventos são controlados pelos testes.
 */
#ifndef MPMC_APP_JOB_OUTCOME_H
#define MPMC_APP_JOB_OUTCOME_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "mpmc/app/command.h"
#include "mpmc/app/supervisor.h"
#include "mpmc/error.h"

/** @brief Como um trabalho terminou. */
typedef enum {
    JOB_OUTCOME_COMPLETED,    /**< Concluído: quantidade efetiva igual à solicitada. */
    JOB_OUTCOME_PARTIAL,      /**< Parcial: menos mensagens que o solicitado, sem erro. */
    JOB_OUTCOME_CANCELLED,    /**< Cancelado (encerramento ou sinal) antes de terminar. */
    JOB_OUTCOME_KERNEL_ERROR, /**< Falha de chamada de sistema ou do backend durante o trabalho. */
    JOB_OUTCOME_SPAWN_FAILED  /**< O processo filho não pôde ser criado. */
} job_outcome_kind_t;

/** @brief Evento de resultado de um trabalho, com o contexto para o diagnóstico. */
typedef struct {
    job_outcome_kind_t kind; /**< Como o trabalho terminou. */
    command_kind_t command;  /**< COMMAND_PRODUCE ou COMMAND_CONSUME. */
    uint64_t operation_id;   /**< Identificador da operação; zero se ainda não foi atribuído. */
    size_t producer_id;      /**< Índice da estrutura; zero em `produz 0` (distribuído). */
    uint64_t requested;      /**< Quantidade de mensagens solicitada. */
    uint64_t effective;      /**< Quantidade efetivamente processada; ignorada em SPAWN_FAILED. */
    mpmc_result_t error;     /**< Causa em KERNEL_ERROR e SPAWN_FAILED; ignorada nos demais. */
} job_outcome_t;

/**
 * @brief Maior errno considerado válido para interpretação (limite do Linux).
 */
#define JOB_OUTCOME_ERRNO_MAX 4095

/**
 * @brief Indica se o evento representa conclusão bem-sucedida.
 * @param outcome Evento a avaliar; NULL resulta em false.
 * @return true somente para JOB_OUTCOME_COMPLETED com `effective == requested`.
 * Parcial, cancelado, erro e falha de criação nunca são sucesso.
 */
bool job_outcome_is_success(const job_outcome_t *outcome);

/**
 * @brief Formata o diagnóstico de um evento, sem prefixo `mpmc: ` nem newline.
 * @param outcome Evento a apresentar.
 * @param[out] buffer Recebe o texto terminado em NUL; pode ser NULL se `capacity` for 0.
 * @param capacity Tamanho de `buffer` em bytes.
 * @return Comprimento que o texto completo teria, como snprintf(); -1 se `outcome`
 * for NULL, se `buffer` for NULL com `capacity` maior que zero ou se o `kind` for
 * desconhecido. Com `capacity` insuficiente, o texto é truncado e continua
 * terminado em NUL.
 * @details
 * - O texto identifica a operação (`operação N` ou `comando`, se ainda não há
 *   identificador), o verbo, o índice (`estrutura I` ou `todas as estruturas` em
 *   `produz 0`) e, quando se aplica, `efetivas de solicitadas`.
 * - Um evento COMPLETED com quantidade efetiva diferente da solicitada é
 *   apresentado como parcial, nunca como conclusão.
 * - `error.system_errno` só é interpretado (strerror) quando `error.status` não
 *   é MPMC_OK e o valor está em 1..JOB_OUTCOME_ERRNO_MAX; caso contrário é
 *   ignorado, sem texto como "Success" ou "Unknown error".
 * - Em KERNEL_ERROR com `error.status == MPMC_OK`, a causa é "não informada".
 * @note Função pura: não usa IPC, não imprime e não consulta errno.
 */
int job_outcome_format(const job_outcome_t *outcome, char *buffer, size_t capacity);

/**
 * @brief Enfileira um evento controlado para apresentação pelo supervisor.
 * @param supervisor Instância ativa.
 * @param outcome Evento a apresentar; copiado.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: evento enfileirado; system_errno é zero.
 * - MPMC_ERR_INVALID_ARG: argumento nulo ou `kind` desconhecido; system_errno é zero.
 * - MPMC_ERR_NO_SPACE: fila de eventos cheia; system_errno é zero.
 * @details O evento é impresso em stderr (uma linha, `mpmc: <diagnóstico>`) na
 * próxima chamada de supervisor_reap(), e conta como aviso para o redesenho do
 * prompt. Entre eventos concorrentes, e entre eles e os avisos de término de
 * filhos, não há ordem global determinística. Implementado em supervisor.c.
 */
mpmc_result_t supervisor_queue_outcome(supervisor_t *supervisor, const job_outcome_t *outcome);

#endif
