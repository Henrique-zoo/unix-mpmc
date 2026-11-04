/**
 * @file command.c
 * @brief Parsing e validação de comandos do REPL do mpmc.
 * @ingroup app
 */

#include "mpmc/app/command.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define CMD_MAX_TOKENS 5u
#define CMD_NMSGS_MIN  1u
#define CMD_NMSGS_MAX  1024u
#define CMD_ACK_MIN    1u
#define CMD_ACK_MAX    2u

/** @brief Fatia imutável da linha original. */
typedef struct {
	const char *text;
	size_t length;
} token_t;

/** @brief Testa se o caractere separa tokens em uma linha de REPL. */
static bool is_blank(unsigned char character)
{
	return character == (unsigned char)' '  ||
	       character == (unsigned char)'\t' ||
	       character == (unsigned char)'\r' ||
	       character == (unsigned char)'\n';
}

/**
 * @brief Avança o cursor até o próximo token.
 *
 * @param[in,out] cursor Posição corrente; avança até o fim do token lido.
 * @param[out]    token  Fatia preenchida em caso de sucesso.
 * @return true se um token foi lido; false no fim da linha.
 */
static bool next_token(const char **cursor, token_t *token)
{
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

/** @brief Compara um token com um literal, sem assumir terminador no token. */
static bool token_equals(token_t token, const char *text)
{
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
 * Aceita somente [+0-9]+ (nenhum sinal, nenhum separador, nenhum lixo
 * alfabético). Rejeita qualquer valor que exceda @p limit.
 *
 * @param[in]  token Token de origem.
 * @param[in]  limit Maior valor representável aceito.
 * @param[out] value Valor convertido; escrito apenas em caso de sucesso.
 * @return true se o token é um decimal válido dentro de @p limit.
 */
static bool parse_unsigned(token_t token, uintmax_t limit, uintmax_t *value)
{
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

		if (parsed > limit / 10u ||
		    (parsed == limit / 10u && digit > limit % 10u)) {
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
 * Cobre as duas formas aceitas pelo enunciado: "&" como token isolado
 * (aqui o token fica vazio e é descartado pelo chamador) e "&" colado ao
 * último argumento, como em "produz 1 5 1&".
 *
 * @param[in,out] token Token a encurtar.
 * @return true se um '&' foi removido.
 */
static bool strip_ampersand(token_t *token)
{
	if (token->length > 0u && token->text[token->length - 1u] == '&') {
		--token->length;
		return true;
	}

	return false;
}

/**
 * @brief Etapa 2: validação de domínio, dependente da quantidade de produtores.
 *
 * @param[in] kind          Comando já reconhecido.
 * @param[in] index         Índice lógico lido da linha.
 * @param[in] message_count Quantidade lida da linha.
 * @param[in] ack           Modo de confirmação lido da linha.
 * @param[in] producers     p da inicialização (>= 1).
 * @return MPMC_OK ou o código de erro específico da regra violada.
 */
static mpmc_status_t validate_semantics(uintmax_t index,
                                        uintmax_t message_count,
                                        uintmax_t ack,
                                        unsigned producers)
{
	if (index > (uintmax_t)producers) {
		return MPMC_ERR_INVALID_INDEX;
	}

	if (message_count < (uintmax_t)CMD_NMSGS_MIN ||
	    message_count > (uintmax_t)CMD_NMSGS_MAX) {
		return MPMC_ERR_INVALID_ARG;
	}

	if (ack < (uintmax_t)CMD_ACK_MIN || ack > (uintmax_t)CMD_ACK_MAX) {
		return MPMC_ERR_INVALID_ARG;
	}

	return MPMC_OK;
}

mpmc_status_t command_parse(const char *line, unsigned producers, command_t *out)
{
	token_t tokens[CMD_MAX_TOKENS];
	token_t extra;
	const char *cursor;
	command_t parsed = {0};
	mpmc_status_t status;
	size_t count = 0;
	uintmax_t index;
	uintmax_t message_count;
	uintmax_t ack;

	if (line == NULL || out == NULL || producers == 0u) {
		return MPMC_ERR_INVALID_ARG;
	}

	cursor = line;
	while (count < (size_t)CMD_MAX_TOKENS && next_token(&cursor, &tokens[count])) {
		++count;
	}
	if (next_token(&cursor, &extra)) {
		return MPMC_ERR_INVALID_ARG;
	}

	if (count > 0u && strip_ampersand(&tokens[count - 1u]) &&
	    tokens[count - 1u].length == 0u) {
		--count;
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
		parsed.background_token = false;
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

	if (!parse_unsigned(tokens[1], UINTMAX_MAX, &index) ||
	    !parse_unsigned(tokens[2], UINTMAX_MAX, &message_count) ||
	    !parse_unsigned(tokens[3], UINTMAX_MAX, &ack)) {
		return MPMC_ERR_INVALID_ARG;
	}

	if (index == 0u && parsed.kind != COMMAND_PRODUCE) {
		return MPMC_ERR_INVALID_INDEX;
	}

	status = validate_semantics(index, message_count, ack, producers);
	if (status != MPMC_OK) {
		return status;
	}

	parsed.producer_id = (size_t)index;
	parsed.message_count = (unsigned)message_count;
	parsed.ack_mode = (unsigned)ack;
	parsed.background_token = false;
	*out = parsed;
	return MPMC_OK;
}
