/**
 * @file test_error.c
 * @brief Verifica diagnósticos independentes de errno e limites de formatação.
 * @ingroup tests
 */

#include <errno.h>
#include <stdio.h>
#include <string.h>

#include "mpmc/error.h"

/**
 * @brief Quantidade de verificações executadas nesta suíte.
 */
static unsigned checks;
/**
 * @brief Quantidade de verificações que registraram falha nesta suíte.
 */
static unsigned failures;

/**
 * @brief Registra uma verificação e informa a falha sem interromper a suíte.
 * @param condition Expressão lógica avaliada uma única vez.
 * @details Incrementa o contador de verificações; se a condição for falsa,
 * incrementa o contador de falhas e escreve o diagnóstico em stderr.
 */
#define CHECK(condition)                                                                           \
    do {                                                                                           \
        ++checks;                                                                                  \
        if (!(condition)) {                                                                        \
            ++failures;                                                                            \
            fprintf(stderr, "FALHA %s:%d: %s\n", __FILE__, __LINE__, #condition);                  \
        }                                                                                          \
    } while (0)

/**
 * @brief Executa os casos da suíte e apresenta o resultado das verificações.
 * @return Zero se nenhuma verificação falhar; um se houver falha ou erro de preparação.
 */
int main(void) {
    char description[256];
    mpmc_result_t error = {.status = MPMC_ERR_INVALID_ARG};

    errno = EACCES;
    int length = mpmc_error_format(error, description, sizeof description);
    CHECK(length >= 0);
    CHECK(strcmp(description, mpmc_status_str(MPMC_ERR_INVALID_ARG)) == 0);

    error = (mpmc_result_t){.status = MPMC_OK};
    CHECK(mpmc_error_format(error, description, sizeof description) >= 0);
    CHECK(strcmp(description, mpmc_status_str(MPMC_OK)) == 0);

    error = (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = EACCES};
    errno = EINVAL;
    length = mpmc_error_format(error, description, sizeof description);
    CHECK(length > 0 && (size_t)length == strlen(description));
    CHECK(strstr(description, strerror(EACCES)) != NULL);
    CHECK(error.status == MPMC_ERR_SYS && error.system_errno == EACCES);

    error = (mpmc_result_t){.status = MPMC_ERR_WOULD_BLOCK, .system_errno = EAGAIN};
    length = mpmc_error_format(error, description, sizeof description);
    CHECK(length > 0);
    CHECK(strstr(description, strerror(EAGAIN)) != NULL);
    CHECK(mpmc_error_format(error, NULL, 0) == length);

    char small[5] = {'?', '?', '?', '?', '!'};
    CHECK(mpmc_error_format(error, small, 4) == length);
    CHECK(small[3] == '\0' && small[4] == '!');
    CHECK(mpmc_error_format(error, NULL, 1) < 0);

    error = (mpmc_result_t){.status = (mpmc_status_t)-1};
    CHECK(mpmc_error_format(error, description, sizeof description) > 0);
    CHECK(strcmp(description, mpmc_status_str(error.status)) == 0);

    printf("%u verificações de diagnóstico, %u falhas\n", checks, failures);
    return failures == 0 ? 0 : 1;
}
