/**
 * @file command.h
 * @brief Interpretação e validação dos comandos do REPL.
 * @ingroup application
 */
#ifndef MPMC_APP_COMMAND_H
#define MPMC_APP_COMMAND_H

#include <stdbool.h>
#include <stddef.h>

#include "mpmc/error.h"

/**
 * @defgroup application Aplicação e gerenciamento de processos
 * @brief Entrada interativa, despacho de comandos e supervisão dos filhos.
 * @details Esta camada não depende dos detalhes de semáforos, filas ou
 * disposição binária da memória compartilhada.
 * @{
 */

/** @brief Tipo sintático do comando interativo. */
typedef enum {
    COMMAND_PRODUCE, /**< `produz i n ack` — cria trabalho de produção. */
    COMMAND_CONSUME, /**< `consome i n ack` — cria trabalho de consumo. */
    COMMAND_SHUTDOWN /**< `shutdown` — termina a aplicação. */
} command_kind_t;

/**
 * @brief Forma normalizada de uma linha do REPL.
 * @details Para COMMAND_SHUTDOWN, os campos numéricos são ignorados.
 * O sufixo '&' é opcional e registrado apenas para fins de diagnóstico:
 * o supervisor sempre cria um processo filho para os dois tipos de trabalho.
 */
typedef struct {
    command_kind_t kind;    /**< Operação reconhecida. */
    size_t producer_id;     /**< Índice 1..p; 0 permitido apenas para COMMAND_PRODUCE. */
    unsigned message_count; /**< Quantidade de mensagens, 1..1024. */
    unsigned ack_mode;      /**< 1 ou 2; semântica depende do tipo do comando. */
    bool background_token;  /**< Indica presença de '&' ao fim da linha. */
} command_t;

/**
 * @brief Converte uma linha de entrada em um comando normalizado.
 *
 * @details
 * Etapa sintática do parsing. Reconhece o verbo, confere a aridade,
 * valida o formato dos campos numéricos, converte-os e descarta o
 * sufixo opcional '&'. Não conhece a quantidade de produtores e,
 * portanto, não verifica a existência do índice lógico: essa regra
 * pertence a command_validate().
 *
 * Nenhum limite de domínio é aplicado aqui. Valores como 5000 mensagens
 * ou ack igual a 3 são sintaticamente válidos e só são rejeitados na
 * etapa semântica, o que mantém o diagnóstico separado entre "não é um
 * número representável" e "número fora da regra".
 *
 * @param line Linha terminada em NUL, possivelmente com espaços e '\n'.
 * @param out Destino do comando; só é escrito quando retornar MPMC_OK.
 *
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: verbo, aridade e números reconhecidos; comando normalizado escrito em out.
 * - MPMC_ERR_INVALID_ARG: line ou out nulo, linha vazia, verbo desconhecido, aridade incorreta,
 *   token inesperado, número malformado ou não representável no campo de destino.
 *
 * system_errno é zero em todos os resultados; a conversão numérica não usa errno.
 *
 * @par Overflow
 * Cada campo é convertido com o limite do seu tipo de destino:
 * producer_id usa SIZE_MAX, message_count e ack_mode usam UINT_MAX.
 * Um valor acima desse limite, inclusive quando excede a capacidade
 * de uintmax_t, é rejeitado com MPMC_ERR_INVALID_ARG. A verificação
 * precede a multiplicação e nunca sofre wrap.
 *
 * Note a distinção: um valor que cabe no tipo do campo mas excede a
 * faixa normativa (p, 1024, ack 2) é aceito aqui e rejeitado por
 * command_validate().
 *
 * @pre line e out não podem ser NULL.
 * @post Em caso de erro, o conteúdo de *out permanece inalterado.
 *
 * @note A existência do índice lógico é verificada por command_validate().
 * @par Concorrência
 * Reentrante, sem estado global.
 */
mpmc_result_t command_parse(const char *line, command_t *out);

/**
 * @brief Valida as regras semânticas de um comando.
 *
 * @details
 * Etapa semântica do parsing. Aplica as faixas normativas de n_msgs e
 * ack, verifica a existência do índice lógico contra a quantidade de
 * estruturas inicializadas e trata a exceção de produção striped.
 *
 * A ordem das checagens é determinística: faixas numéricas antes do
 * índice. Um comando que viola as duas regras ao mesmo tempo retorna
 * MPMC_ERR_INVALID_ARG, não MPMC_ERR_INVALID_INDEX.
 *
 * @param command Comando retornado por command_parse().
 * @param producer_count Número de estruturas inicializadas (deve ser > 0).
 *
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: comando shutdown ou parâmetros válidos para a quantidade de estruturas informada.
 * - MPMC_ERR_INVALID_ARG: command nulo, producer_count zero, tipo de comando desconhecido,
 *   quantidade fora de 1..1024 ou ack diferente de 1/2.
 * - MPMC_ERR_INVALID_INDEX: índice acima de producer_count ou índice zero em consome.
 *
 * system_errno é zero em todos os resultados.
 *
 * @pre command não é NULL.
 * @note `consome 0` é inválido; `produz 0` é válido.
 * @par Concorrência
 * Reentrante, sem estado global.
 */
mpmc_result_t command_validate(const command_t *command, size_t producer_count);

/** @} */
#endif
