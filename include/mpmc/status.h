/**
 * @file status.h
 * @brief Códigos de retorno usados por toda a aplicação.
 * @ingroup core
 */
#ifndef MPMC_STATUS_H
#define MPMC_STATUS_H

/**
 * @defgroup core Modelo comum e erros
 * @brief Tipos compartilhados entre a aplicação, os workers e o IPC.
 * @details Os estados distinguem falhas de entrada, ausência temporária de
 * dados, falhas do kernel e cancelamento durante o encerramento.
 * @{
 */

/** @brief Resultado tipado das operações do projeto. */
typedef enum {
    MPMC_OK = 0,            /**< Operação concluída normalmente. */
    MPMC_ERR_INVALID_ARG,   /**< Argumento ou comando malformado. */
    MPMC_ERR_INVALID_INDEX, /**< Estrutura lógica inexistente. */
    MPMC_ERR_WOULD_BLOCK,   /**< Sem mensagem no recebimento não bloqueante. */
    MPMC_ERR_NO_SPACE,      /**< Falta de capacidade em operação não bloqueante. */
    MPMC_ERR_INTERRUPTED,   /**< Operação interrompida por sinal/cancelamento. */
    MPMC_ERR_SHUTDOWN,      /**< Aplicação está encerrando. */
    MPMC_ERR_SYS,           /**< Falha em chamada de sistema; consultar errno. */
    MPMC_ERR_STATE          /**< Violação de estado ou contrato interno. */
} mpmc_status_t;

/**
 * @brief Retorna descrição textual estática de um código de status.
 * @param status Código a descrever, inclusive valores inesperados.
 * @return Ponteiro para string constante, nunca NULL.
 * @par Concorrência
 * Pode ser chamada concorrentemente e não altera estado global.
 */
const char *mpmc_status_str(mpmc_status_t status);

/** @} */
#endif
