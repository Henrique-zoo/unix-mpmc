/**
 * @file command.h
 * @brief Interpretação e validação dos comandos do REPL.
 * @ingroup application
 */
#ifndef MPMC_APP_COMMAND_H
#define MPMC_APP_COMMAND_H

#include <stdbool.h>
#include <stddef.h>
#include "mpmc/status.h"

/**
 * @defgroup application Aplicação e gerenciamento de processos
 * @brief Entrada interativa, despacho de comandos e supervisão dos filhos.
 * @details Esta camada não depende dos detalhes de semáforos, filas ou
 * disposição binária da memória compartilhada.
 * @{
 */

/** @brief Tipo sintático do comando interativo. */
typedef enum {
    COMMAND_PRODUCE,  /**< `produz i n ack` — cria trabalho de produção. */
    COMMAND_CONSUME,  /**< `consome i n ack` — cria trabalho de consumo. */
    COMMAND_SHUTDOWN  /**< `shutdown` — termina a aplicação. */
} command_kind_t;

/**
 * @brief Forma normalizada de uma linha do REPL.
 * @details Para COMMAND_SHUTDOWN, os campos numéricos são ignorados.
 * O sufixo '&' é opcional e registrado apenas para fins de diagnóstico:
 * o supervisor sempre cria um processo filho para os dois tipos de trabalho.
 */
typedef struct {
    command_kind_t kind;   /**< Operação reconhecida. */
    size_t producer_id;    /**< Índice 1..p; 0 permitido apenas para COMMAND_PRODUCE. */
    unsigned message_count;/**< Quantidade de mensagens, 1..1024. */
    unsigned ack_mode;     /**< 1 ou 2; semântica depende do tipo do comando. */
    bool background_token; /**< Indica presença de '&' ao fim da linha. */
} command_t;

/**
 * @brief Converte uma linha de entrada em um comando normalizado.
 * @param line Linha terminada em NUL, possivelmente com espaços e '\n'.
 * @param out Destino do comando; só é válido quando retornar MPMC_OK.
 * @retval MPMC_OK Sintaxe reconhecida e tipos numéricos convertidos.
 * @retval MPMC_ERR_INVALID_ARG Comando desconhecido, tokens inesperados,
 *   números inválidos ou ausência de argumentos.
 * @pre line e out não podem ser NULL.
 * @note A existência do índice lógico é verificada por command_validate().
 * @par Concorrência
 * Reentrante, sem estado global.
 */
mpmc_status_t command_parse(const char *line, command_t *out);

/**
 * @brief Valida as regras semânticas de um comando.
 * @param command Comando retornado por command_parse().
 * @param producer_count Número de estruturas inicializadas (deve ser > 0).
 * @retval MPMC_OK Parâmetros válidos para esta instância.
 * @retval MPMC_ERR_INVALID_ARG Quantidade fora de 1..1024 ou ack diferente de 1/2.
 * @retval MPMC_ERR_INVALID_INDEX Índice fora de 1..p (exceto produção striped=0).
 * @pre command não é NULL.
 * @note `consome 0` é inválido; `produz 0` é válido.
 */
mpmc_status_t command_validate(const command_t *command, size_t producer_count);

/** @} */
#endif
