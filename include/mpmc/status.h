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
 *
 * @details
 * Os códigos de status representam o resultado das operações do projeto,
 * distinguindo falhas de entrada, indisponibilidade temporária de recursos,
 * falhas do sistema operacional e interrupções durante o encerramento.
 *
 * Os módulos devem utilizar esses códigos para comunicar os resultados
 * de suas operações, sem depender de mensagens textuais ou dos valores
 * numéricos específicos de errno.
 *
 * @par Tratamento de errno
 * As funções que realizam chamadas de sistema devem traduzir os erros
 * conhecidos para os códigos MPMC_ERR_* apropriados, conforme os
 * contratos definidos em suas respectivas interfaces.
 *
 * Quando não houver uma representação específica para a falha,
 * a função deve retornar MPMC_ERR_SYS.
 *
 * Quando o status resultar diretamente de uma falha de chamada de
 * sistema, as funções que garantirem a preservação de errno devem
 * manter seu valor original até o retorno, inclusive quando a falha
 * for traduzida para um código de status específico.
 *
 * O chamador deve consultar errno somente quando essa garantia estiver
 * prevista no contrato da função, capturando seu valor imediatamente
 * após a chamada e antes de executar operações que possam modificá-lo.
 *
 * O valor de errno não deve ser utilizado para determinar o sucesso
 * de uma operação, nem presumido válido para erros que não tenham
 * origem em chamadas de sistema.
 *
 * @par Responsabilidades
 * - Os módulos de IPC interpretam os erros das chamadas de sistema.
 * - Os demais módulos propagam ou tratam os códigos de status.
 * - A camada de apresentação decide como comunicar os erros ao usuário.
 *
 * @{
 */

/**
 * @brief Resultado tipado das operações do projeto.
 *
 * @details
 * Os códigos representam categorias de resultados e não substituem
 * os detalhes específicos de uma falha do sistema operacional.
 *
 * Quando o contrato da função permitir, errno poderá fornecer
 * informações adicionais sobre a causa de um erro.
 */
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
 * @brief Retorna a descrição textual de um código de status.
 *
 * @param status Código a descrever, inclusive valores inesperados.
 *
 * @return Ponteiro para uma string constante e estática, nunca NULL.
 *
 * @par Concorrência
 * Pode ser chamada concorrentemente e não modifica estado global.
 *
 * @post O ponteiro retornado permanece válido durante toda a execução
 * do programa e não deve ser liberado pelo chamador.
 *
 * @note Esta função não interpreta errno nem apresenta informações
 * específicas sobre falhas de chamadas de sistema.
 */
const char *mpmc_status_str(mpmc_status_t status);

/** @} */

#endif
