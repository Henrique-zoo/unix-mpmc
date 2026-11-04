#include "mpmc/app/command.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define CMD_MAX_TOKENS 5u
#define CMD_NMSGS_MIN  1u
#define CMD_NMSGS_MAX  1024u
#define CMD_ACK_MIN    1u
#define CMD_ACK_MAX    2u

typedef struct {
	const char *text;
	size_t length;
} token_t;

static bool is_blank(unsigned char character)
{
	return character == (unsigned char)' '  ||
	       character == (unsigned char)'\t' ||
	       character == (unsigned char)'\r' ||
	       character == (unsigned char)'\n';
}

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

static bool parse_unsigned(token_t token, uintmax_t limit, uintmax_t *value)
{
	uintmax_t parsed = 0;

	if (token.length == 0u) {
		return false;
	}

	for (size_t i = 0; i < token.length; ++i) {
		unsigned char character = (unsigned char)token.text[i];
		uintmax_t digit;

		if (character < (unsigned char)'0' || character > (unsigned char)'9') {
			return false;
		}

		digit = (uintmax_t)(character - (unsigned char)'0');
		if (parsed > limit / 10) {
			return false;
		}
		parsed = parsed * 10u + digit;

        if (parsed > limit) {
		    return false;
	    }
	}

    *value = parsed;
	return true;
}

static mpmc_status_t validate_semantics(cmd_kind_t kind, uintmax_t index,
                                        uintmax_t message_count, uintmax_t ack,
                                        unsigned producers)
{
	if (index == 0u) {
		if (kind != COMMAND_PRODUCE) {
			return MPMC_ERR_INDEX_INVALID;   /* 0 só é válido em produz */
		}
	} else if (index > (uintmax_t)producers) {
		return MPMC_ERR_INDEX_INVALID;
	}

	if (message_count < CMD_NMSGS_MIN || message_count > CMD_NMSGS_MAX) {
		return MPMC_ERR_ARG_RANGE;
	}

	if (ack < CMD_ACK_MIN || ack > CMD_ACK_MAX) {
		return MPMC_ERR_ARG_RANGE;
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
	while (count < CMD_MAX_TOKENS && next_token(&cursor, &tokens[count])) {
		++count;
	}
	if (next_token(&cursor, &extra)) {
		return MPMC_ERR_ARITY;
	}

	if (count > 0u && strip_ampersand(&tokens[count - 1u]) &&
	    tokens[count - 1u].length == 0u) {
		--count;
	}

	if (count == 0u) {
		return MPMC_ERR_EMPTY_COMMAND;
	}

	if (token_equals(tokens[0], "shutdown")) {
		if (count != 1u) {
			return MPMC_ERR_ARITY;
		}
		parsed.kind = COMMAND_SHUTDOWN;
		*out = parsed;
		return MPMC_OK;
	}

	if (token_equals(tokens[0], "produz")) {
		parsed.kind = COMMAND_PRODUCE;
	} else if (token_equals(tokens[0], "consome")) {
		parsed.kind = COMMAND_CONSUME;
	} else {
		return MPMC_ERR_UNKNOWN_COMMAND;
	}

	if (count != 4u) {
		return MPMC_ERR_ARITY;
	}

	if (!parse_unsigned(tokens[1], (uintmax_t)SIZE_MAX, &index) ||
	    !parse_unsigned(tokens[2], (uintmax_t)UINT_MAX, &message_count) ||
	    !parse_unsigned(tokens[3], (uintmax_t)UINT_MAX, &ack)) {
		return MPMC_ERR_INVALID_NUMBER;
	}

	status = validate_semantics(parsed.kind, index, message_count, ack, producers);
	if (status != MPMC_OK) {
		return status;
	}

	parsed.producer_id = (size_t)index;
	parsed.message_count = (unsigned)message_count;
	parsed.ack_mode = (unsigned)ack;
	*out = parsed;
	return MPMC_OK;
}
