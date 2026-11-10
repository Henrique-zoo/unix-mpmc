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

/**
 * @brief Quantidade máxima de tokens, incluindo o sufixo separado &.
 */
#define CMD_MAX_TOKENS 5u
/**
 * @brief Menor quantidade de mensagens aceita pela validação semântica.
 */
#define CMD_NMSGS_MIN 1u
/**
 * @brief Maior quantidade de mensagens aceita pela validação semântica.
 */
#define CMD_NMSGS_MAX 1024u
/**
 * @brief Menor valor válido para a política de confirmação.
 */
#define CMD_ACK_MIN 1u
/**
 * @brief Maior valor válido para a política de confirmação.
 */
#define CMD_ACK_MAX 2u

/** @brief Fatia imutável da linha original. */
typedef struct {
    const char *text; /**< Início da fatia emprestada da linha; não exige NUL no fim. */
    size_t length;    /**< Quantidade de bytes pertencentes ao token. */
} token_t;

/**
 * @brief Testa se um caractere separa tokens na gramática do REPL.
 * @param character Byte a examinar, sem interpretação dependente da localidade.
 * @return true para espaço, tabulação, CR ou LF; false para os demais bytes.
 */
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

    if (**cursor == '\0') return false;

    start = *cursor;
    while (**cursor != '\0' && !is_blank((unsigned char)**cursor)) {
        ++*cursor;
    }

    token->text = start;
    token->length = (size_t)(*cursor - start);
    return true;
}

/**
 * @brief Compara uma fatia com uma string terminada em NUL.
 * @param token Fatia cuja leitura é limitada por length.
 * @param text String de referência, válida e terminada em NUL.
 * @return true se tamanho e bytes coincidirem; false em qualquer diferença.
 * @note O token não precisa possuir um terminador após seus bytes.
 */
static bool token_equals(token_t token, const char *text) {
    size_t index = 0;

    while (text[index] != '\0') {
        if (index >= token.length || token.text[index] != text[index]) return false;
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

    if (token.length == 0) return false;

    for (size_t index = 0; index < token.length; ++index) {
        unsigned char character = (unsigned char)token.text[index];
        uintmax_t digit;

        if (character < (unsigned char)'0' || character > (unsigned char)'9') return false;

        digit = (uintmax_t)(character - (unsigned char)'0');

        if (parsed > limit / 10 || (parsed == limit / 10 && digit > limit % 10)) return false;

        parsed = parsed * 10 + digit;
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
    if (token->length > 0 && token->text[token->length - 1] == '&') {
        --token->length;
        return true;
    }

    return false;
}

mpmc_result_t command_parse(const char *line, command_t *out) {
    token_t tokens[CMD_MAX_TOKENS];
    token_t extra;
    const char *cursor;
    command_t parsed = {};
    size_t count = 0;
    uintmax_t index;
    uintmax_t message_count;
    uintmax_t ack;
    bool has_ampersand = false;

    if (line == nullptr || out == nullptr) return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};

    cursor = line;
    while (count < (size_t)CMD_MAX_TOKENS && next_token(&cursor, &tokens[count])) {
        ++count;
    }
    if (next_token(&cursor, &extra)) return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};

    if (count == 0) return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};

    if (strip_ampersand(&tokens[count - 1])) {
        has_ampersand = true;
        if (tokens[count - 1].length == 0) {
            --count;
        }
    }

    if (count == 0) return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};

    if (token_equals(tokens[0], "shutdown")) {
        if (count != 1) return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};

        parsed.kind = COMMAND_SHUTDOWN;
        parsed.producer_id = 0;
        parsed.message_count = 0;
        parsed.ack_mode = 0;
        parsed.background_token = has_ampersand;
        *out = parsed;
        return (mpmc_result_t){.status = MPMC_OK};
    }

    if (token_equals(tokens[0], "produz")) {
        parsed.kind = COMMAND_PRODUCE;
    } else if (token_equals(tokens[0], "consome")) {
        parsed.kind = COMMAND_CONSUME;
    } else {
        return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};
    }

    if (count != 4) return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};

    if (!parse_unsigned(tokens[1], (uintmax_t)SIZE_MAX, &index)
        || !parse_unsigned(tokens[2], (uintmax_t)UINT_MAX, &message_count)
        || !parse_unsigned(tokens[3], (uintmax_t)UINT_MAX, &ack)) {
        return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};
    }

    parsed.producer_id = (size_t)index;
    parsed.message_count = (unsigned)message_count;
    parsed.ack_mode = (unsigned)ack;
    parsed.background_token = has_ampersand;
    *out = parsed;
    return (mpmc_result_t){.status = MPMC_OK};
}

mpmc_result_t command_validate(const command_t *command, size_t producer_count) {
    if (command == nullptr || producer_count == 0) {
        return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};
    }

    if (command->kind == COMMAND_SHUTDOWN) return (mpmc_result_t){.status = MPMC_OK};

    if (command->kind != COMMAND_PRODUCE && command->kind != COMMAND_CONSUME) {
        return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};
    }

    if (command->message_count < CMD_NMSGS_MIN || command->message_count > CMD_NMSGS_MAX) {
        return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};
    }

    if (command->ack_mode < CMD_ACK_MIN || command->ack_mode > CMD_ACK_MAX) {
        return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};
    }

    if (command->producer_id == 0) {
        return (mpmc_result_t){
            .status = command->kind == COMMAND_PRODUCE ? MPMC_OK : MPMC_ERR_INVALID_INDEX};
    }

    if (command->producer_id > producer_count) {
        return (mpmc_result_t){.status = MPMC_ERR_INVALID_INDEX};
    }

    return (mpmc_result_t){.status = MPMC_OK};
}
