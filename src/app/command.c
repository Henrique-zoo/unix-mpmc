/**
 * @file command.c
 * @brief Interpretação e validação dos comandos do REPL.
 * @ingroup application
 *
 * @details
 * O parsing é dividido em duas responsabilidades distintas, conforme o
 * contrato de command.h:
 *
 * - command_parse()     — etapa sintática: tokeniza, reconhece o verbo,
 *                         confere aridade, converte os campos numéricos e
 *                         descarta o sufixo opcional '&'. Não conhece p.
 * - command_validate()  — etapa semântica: aplica as faixas de domínio
 *                         (1..1024, ack 1/2) e a existência do índice
 *                         lógico contra a quantidade de produtores.
 *
 * Nenhuma das duas funções cria processos, aloca memória, realiza I/O
 * ou mantém estado global.
 */

#include "mpmc/app/command.h"

#include <limits.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define CMD_MAX_TOKENS 5u
#define CMD_NMSGS_MIN 1u
#define CMD_NMSGS_MAX 1024u
#define CMD_ACK_MIN 1u
#define CMD_ACK_MAX 2u

/** @brief Fatia imutável da linha original. */
typedef struct {
    const char *text;
    size_t length;
} token_t;

/**
 * @brief Testa se o caractere separa tokens em uma linha do REPL. */
static bool is_blank(unsigned char character) {
    return character == (unsigned char)' ' || character == (unsigned char)'\t'
        || character == (unsigned char)'\r' || character == (unsigned char)'\n';
}

/**
 * @brief Avança o cursor até o próximo token.
 *
 * @param[in,out] cursor Posição corrente; avança até o fim do token lido.
 * @param[out]    token  Fatia preenchida em caso de sucesso.
 * @return true se um token foi lido; false no fim da linha.
 */
static bool next_token(const char **cursor, token_t *token) {
    const char *start;

    while (is_blank((unsigned char)**cursor)) {
        ++*cursor;
    }

    if (**cursor == '\0') {
        return false;
    }

    start = *cursor;
    while (**cursor != '\0' && !is_blank((unsigned char)**cursor)) {
        ++*cursor;
    }

    token->text = start;
    token->length = (size_t)(*cursor - start);
    return true;
}

/** @brief Compara um token com um literal, sem exigir terminador no token. */
static bool token_equals(token_t token, const char *text) {
    size_t index = 0;

    while (text[index] != '\0') {
        if (index >= token.length || token.text[index] != text[index]) {
            return false;
        }
        ++index;
    }

    return index == token.length;
}

/**
 * @brief Converte um token decimal sem sinal, com detecção de overflow.
 *
 * Aceita somente [0-9]+. Rejeita sinal, separador, notação hexadecimal e
 * qualquer caractere não numérico. Rejeita, ainda, valores que excedam
 * @p limit, inclusive quando excedem a capacidade de uintmax_t.
 *
 * A comparação precede a multiplicação e usa apenas divisão e resto, de
 * modo que nunca ocorre wrap, qualquer que seja @p limit.
 *
 * @param[in]  token Token de origem.
 * @param[in]  limit Maior valor aceito.
 * @param[out] value Valor convertido; escrito apenas em caso de sucesso.
 * @return true se o token é um decimal válido dentro de @p limit.
 */
static bool parse_unsigned(token_t token, uintmax_t limit, uintmax_t *value) {
    uintmax_t parsed = 0;

    if (token.length == 0u) {
        return false;
    }

    for (size_t index = 0; index < token.length; ++index) {
        unsigned char character = (unsigned char)token.text[index];
        uintmax_t digit;

        if (character < (unsigned char)'0' || character > (unsigned char)'9') {
            return false;
        }

        digit = (uintmax_t)(character - (unsigned char)'0');

        if (parsed > limit / 10u || (parsed == limit / 10u && digit > limit % 10u)) {
            return false;
        }

        parsed = parsed * 10u + digit;
    }

    *value = parsed;
    return true;
}

/**
 * @brief Remove um '&' terminal do token, encurtando-o.
 *
 * Cobre as duas formas previstas no enunciado: '&' como token isolado
 * (o token fica vazio e é descartado pelo chamador) e '&' colado ao
 * último argumento, como em "produz 1 5 1&".
 *
 * @param[in,out] token Token a encurtar.
 * @return true se um '&' foi removido.
 */
static bool strip_ampersand(token_t *token) {
    if (token->length > 0u && token->text[token->length - 1u] == '&') {
        --token->length;
        return true;
    }

    return false;
}

mpmc_status_t command_parse(const char *line, command_t *out) {
    token_t tokens[CMD_MAX_TOKENS];
    token_t extra;
    const char *cursor;
    command_t parsed = {0};
    size_t count = 0;
    uintmax_t index;
    uintmax_t message_count;
    uintmax_t ack;
    bool has_ampersand = false;

    if (line == NULL || out == NULL) {
        return MPMC_ERR_INVALID_ARG;
    }

    cursor = line;
    while (count < (size_t)CMD_MAX_TOKENS && next_token(&cursor, &tokens[count])) {
        ++count;
    }
    if (next_token(&cursor, &extra)) {
        return MPMC_ERR_INVALID_ARG;
    }

    if (count == 0u) {
        return MPMC_ERR_INVALID_ARG;
    }

    if (strip_ampersand(&tokens[count - 1u])) {
        has_ampersand = true;
        if (tokens[count - 1u].length == 0u) {
            --count;
        }
    }

    if (count == 0u) {
        return MPMC_ERR_INVALID_ARG;
    }

    if (token_equals(tokens[0], "shutdown")) {
        if (count != 1u) {
            return MPMC_ERR_INVALID_ARG;
        }

        parsed.kind = COMMAND_SHUTDOWN;
        parsed.producer_id = 0u;
        parsed.message_count = 0u;
        parsed.ack_mode = 0u;
        parsed.background_token = has_ampersand;
        *out = parsed;
        return MPMC_OK;
    }

    if (token_equals(tokens[0], "produz")) {
        parsed.kind = COMMAND_PRODUCE;
    } else if (token_equals(tokens[0], "consome")) {
        parsed.kind = COMMAND_CONSUME;
    } else {
        return MPMC_ERR_INVALID_ARG;
    }

    if (count != 4u) {
        return MPMC_ERR_INVALID_ARG;
    }

    if (!parse_unsigned(tokens[1], (uintmax_t)SIZE_MAX, &index)
        || !parse_unsigned(tokens[2], (uintmax_t)UINT_MAX, &message_count)
        || !parse_unsigned(tokens[3], (uintmax_t)UINT_MAX, &ack)) {
        return MPMC_ERR_INVALID_ARG;
    }

    parsed.producer_id = (size_t)index;
    parsed.message_count = (unsigned)message_count;
    parsed.ack_mode = (unsigned)ack;
    parsed.background_token = has_ampersand;
    *out = parsed;
    return MPMC_OK;
}

mpmc_status_t command_validate(const command_t *command, size_t producer_count) {
    if (command == NULL || producer_count == 0u) {
        return MPMC_ERR_INVALID_ARG;
    }

    if (command->kind == COMMAND_SHUTDOWN) {
        return MPMC_OK;
    }

    if (command->kind != COMMAND_PRODUCE && command->kind != COMMAND_CONSUME) {
        return MPMC_ERR_INVALID_ARG;
    }

    if (command->message_count < CMD_NMSGS_MIN || command->message_count > CMD_NMSGS_MAX) {
        return MPMC_ERR_INVALID_ARG;
    }

    if (command->ack_mode < CMD_ACK_MIN || command->ack_mode > CMD_ACK_MAX) {
        return MPMC_ERR_INVALID_ARG;
    }

    if (command->producer_id == 0u) {
        return command->kind == COMMAND_PRODUCE ? MPMC_OK : MPMC_ERR_INVALID_INDEX;
    }

    if (command->producer_id > producer_count) {
        return MPMC_ERR_INVALID_INDEX;
    }

    return MPMC_OK;
}
