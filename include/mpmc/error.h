/**
 * @file error.h
 * @brief Tipos e funções para representação e descrição de resultados.
 * @ingroup core
 */
#ifndef MPMC_ERROR_H
#define MPMC_ERROR_H

#include <stddef.h>

/**
 * @defgroup core Modelo comum e erros
 * @brief Tipos compartilhados entre a aplicação, os workers e o IPC.
 *
 * @details
 * O projeto utiliza um modelo de resultados composto por duas informações:
 * - Uma categoria de resultado, representada por mpmc_status_t.
 * - Um código de erro do sistema operacional, quando aplicável.
 *
 * A estrutura mpmc_result_t reúne essas informações e constitui o tipo
 * de retorno padrão das operações que podem falhar.
 *
 * Essa separação permite que os módulos interpretem os resultados sem
 * depender dos códigos numéricos específicos do sistema operacional,
 * preservando simultaneamente os detalhes necessários ao diagnóstico.
 *
 * @par Tratamento de errno
 * As funções que realizam chamadas de sistema devem interpretar os
 * erros conhecidos e atribuir os códigos MPMC_ERR_* apropriados,
 * conforme os contratos definidos em suas respectivas interfaces.
 *
 * Quando não houver uma categoria específica para a falha, deve
 * ser utilizado MPMC_ERR_SYS.
 *
 * Quando uma chamada de sistema falhar, seu errno deve ser capturado
 * imediatamente e armazenado em mpmc_result_t::system_errno, inclusive
 * quando a falha for traduzida para uma categoria específica.
 *
 * Os resultados propagados entre os módulos devem preservar essa
 * informação, sem depender do valor corrente de errno.
 * Nas interfaces, @c return descreve a estrutura mpmc_result_t retornada
 * e lista os valores possíveis de seu campo status.
 *
 * O valor de errno não deve ser utilizado para determinar o sucesso
 * de uma operação, nem presumido válido para falhas que não tenham
 * origem em chamadas de sistema.
 *
 * @par Invariantes
 * - MPMC_OK implica system_errno igual a zero.
 * - system_errno igual a zero indica ausência de erro de sistema registrado.
 * - system_errno diferente de zero identifica uma causa de sistema
 *   operacional, independentemente da categoria de status.
 *
 * @par Responsabilidades
 * - Os módulos de IPC interpretam os erros das chamadas de sistema
 *   e preservam suas causas.
 * - Os wrappers delegam ao kernel a validação dos argumentos das chamadas.
 *   Somente restrições das operações em C, como saídas desreferenciadas,
 *   conversões sem perda e tamanhos de envelopes, são verificadas localmente.
 * - As camadas superiores validam o modelo da aplicação e seus estados,
 *   que não são conhecidos pelo kernel.
 * - Os demais módulos propagam ou tratam os resultados tipados.
 * - A camada de apresentação decide como contextualizar e comunicar
 *   os erros ao usuário.
 *
 * @{
 */

/**
 * @brief Categorias de resultado das operações do projeto.
 *
 * @details
 * Os códigos representam resultados e condições relevantes para
 * o fluxo de controle da aplicação.
 *
 * Não representam individualmente todos os erros que podem ser
 * produzidos pelo sistema operacional.
 *
 * Quando uma categoria resultar de uma falha de chamada de sistema,
 * o código original será disponibilizado em
 * mpmc_result_t::system_errno.
 */
typedef enum {
    MPMC_OK = 0,            /**< Operação concluída normalmente. */
    MPMC_ERR_INVALID_ARG,   /**< Argumento ou comando malformado. */
    MPMC_ERR_INVALID_INDEX, /**< Estrutura lógica inexistente. */
    MPMC_ERR_WOULD_BLOCK,   /**< Operação não bloqueante não pôde prosseguir. */
    MPMC_ERR_NO_SPACE,      /**< Falta de capacidade em operação não bloqueante. */
    MPMC_ERR_INTERRUPTED,   /**< Operação interrompida por sinal/cancelamento. */
    MPMC_ERR_SHUTDOWN,      /**< Aplicação está encerrando. */
    MPMC_ERR_SYS,           /**< Falha de sistema sem categoria mais específica. */
    MPMC_ERR_STATE          /**< Violação de estado ou contrato interno. */
} mpmc_status_t;

/**
 * @brief Resultado completo de uma operação que pode falhar.
 *
 * @details
 * Combina uma categoria de resultado com o código original de erro
 * do sistema operacional, quando existente.
 *
 * O campo status permite que o chamador determine como reagir ao
 * resultado. O campo system_errno preserva informações adicionais
 * para diagnóstico, apresentação ou propagação entre módulos.
 *
 * @par Invariantes
 * - Em caso de sucesso, status é MPMC_OK e system_errno é zero.
 * - Em erros internos da aplicação, system_errno é zero quando não
 *   houver uma causa de sistema operacional associada.
 * - Em falhas de chamadas de sistema, system_errno contém o errno
 *   original da operação que falhou.
 *
 * @note O campo system_errno pode ser diferente de zero mesmo quando
 * status não for MPMC_ERR_SYS. Isso ocorre, por exemplo, quando
 * EAGAIN é traduzido para MPMC_ERR_WOULD_BLOCK ou quando EINTR
 * é traduzido para MPMC_ERR_INTERRUPTED.
 *
 * @note A estrutura armazena valores, não ponteiros para mensagens,
 * e não exige alocação dinâmica nem liberação de recursos.
 */
typedef struct {
    mpmc_status_t status; /**< Categoria do resultado da operação. */
    int system_errno;     /**< errno original ou zero quando não aplicável. */
} mpmc_result_t;

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
 * @note Esta função descreve somente a categoria do resultado.
 * Não interpreta errno nem consulta mpmc_result_t::system_errno.
 *
 * @see mpmc_error_format
 */
const char *mpmc_status_str(mpmc_status_t status);

/**
 * @brief Formata a descrição completa de um resultado.
 *
 * @details
 * Produz uma mensagem textual a partir da categoria de status
 * e, quando disponível, do código original de erro do sistema
 * operacional.
 *
 * A descrição da categoria é obtida por mpmc_status_str().
 * Quando system_errno for diferente de zero, a descrição
 * correspondente ao erro do sistema operacional é acrescentada
 * à mensagem.
 *
 * A função utiliza exclusivamente o buffer fornecido pelo chamador
 * para armazenar o resultado, sem realizar alocação dinâmica.
 *
 * @param error Resultado a ser descrito.
 * @param[out] buffer Buffer que receberá a mensagem formatada.
 *                    Pode ser NULL somente quando capacity for zero.
 * @param capacity Capacidade total do buffer, em bytes, incluindo
 *                 o espaço necessário ao terminador nulo.
 *
 * @return Número de caracteres que seriam escritos, excluindo o
 * terminador nulo, em caso de sucesso. Retorna um valor negativo
 * se ocorrer uma falha de formatação.
 *
 * @pre buffer deve apontar para uma região válida de pelo menos
 * capacity bytes quando capacity for maior que zero.
 *
 * @post Se capacity for maior que zero e a formatação for
 * bem-sucedida, buffer conterá uma string terminada em '\0',
 * possivelmente truncada.
 *
 * @par Truncamento
 * A função segue a convenção de retorno de snprintf().
 * Se o valor retornado for maior ou igual a capacity, a mensagem
 * não coube integralmente no buffer.
 *
 * A capacidade necessária pode ser consultada chamando a função
 * com buffer igual a NULL e capacity igual a zero.
 *
 * @par Origem das informações
 * A função utiliza exclusivamente os campos de error para determinar
 * o resultado e sua causa. Não consulta o valor corrente de errno.
 *
 * @note A função não acrescenta informações sobre a operação que
 * produziu o erro. Esse contexto pertence à camada de apresentação.
 *
 * @see mpmc_status_str
 */
int mpmc_error_format(mpmc_result_t error, char *buffer, size_t capacity);

/** @} */

#endif
