/**
 * @file main.c
 * @brief Ponto de entrada e validação dos argumentos de inicialização.
 *
 * @details
 * Responsável por interpretar os argumentos fornecidos na invocação
 * do executável, validar a configuração solicitada e iniciar a aplicação.
 *
 * A validação é realizada por funções privadas, independentes dos
 * mecanismos de comunicação entre processos.
 *
 * Este módulo não implementa os comandos interativos, a supervisão
 * dos processos ou as operações de IPC.
 */

#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>

#include "mpmc/config.h"
#include "mpmc/status.h"

static mpmc_status_t parse_args(int argc, char *argv[], mpmc_config_t *out);
static mpmc_status_t parse_positive_integer(const char *text, size_t *out);

/**
 * @brief Valida os argumentos de inicialização e coordena a execução.
 *
 * @param argc Quantidade de argumentos, incluindo o nome do executável.
 * @param argv Vetor de strings dos argumentos, terminado por NULL.
 *
 * @return EXIT_SUCCESS em caso de sucesso ou EXIT_FAILURE em caso de erro.
 *
 * @details
 * Espera exatamente dois argumentos:
 * - Número de produtores lógicos, inteiro estritamente positivo.
 * - Paradigma IPC: 1 para memória compartilhada ou 2 para filas de mensagens.
 *
 * Em caso de argumentos inválidos, apresenta uma mensagem de erro
 * e a sintaxe esperada para a execução.
 */
int main(int argc, char *argv[]) {
    mpmc_config_t config;
    mpmc_status_t status = parse_args(argc, argv, &config);

    if (status != MPMC_OK) {
        fprintf(stderr, "%s\n", mpmc_status_str(status));
        fprintf(stderr, "Uso: %s <produtores> <paradigma>\n", argv[0]);
        return EXIT_FAILURE;
    }

    return 0;
}

/**
 * @brief Interpreta e valida os argumentos de inicialização.
 *
 * @details
 * Converte os argumentos textuais em uma configuração tipada,
 * verificando a quantidade e a validade dos parâmetros.
 *
 * São exigidos exatamente dois argumentos:
 * - Número de produtores lógicos: inteiro estritamente positivo,
 *   representável em size_t.
 * - Paradigma IPC: IPC_SHARED_MEMORY ou IPC_MESSAGE_QUEUE.
 *
 * A conversão numérica é delegada a parse_positive_integer().
 *
 * @param argc Quantidade de argumentos, incluindo o executável.
 * @param argv Vetor de strings dos argumentos.
 * @param[out] out Configuração validada em caso de sucesso.
 *
 * @retval MPMC_OK Configuração validada.
 * @retval MPMC_ERR_INVALID_ARG Quantidade, formato ou valor dos
 *         argumentos inválido, ou ponteiro de saída nulo.
 *
 * @post Em caso de sucesso, *out contém a configuração validada.
 * Em caso de erro, *out permanece inalterado, quando válido.
 *
 * @note A função não inicializa recursos IPC nem cria processos.
 */
static mpmc_status_t parse_args(int argc, char *argv[], mpmc_config_t *out) {
    if (argc != 3 || argv == NULL || out == NULL) {
        return MPMC_ERR_INVALID_ARG;
    }

    size_t producer_count;
    size_t parsed_mode;

    if (parse_positive_integer(argv[1], &producer_count) != MPMC_OK) {
        return MPMC_ERR_INVALID_ARG;
    }

    if (parse_positive_integer(argv[2], &parsed_mode) != MPMC_OK) {
        return MPMC_ERR_INVALID_ARG;
    }

    if (parsed_mode != IPC_SHARED_MEMORY && parsed_mode != IPC_MESSAGE_QUEUE) {
        return MPMC_ERR_INVALID_ARG;
    }

    *out = (mpmc_config_t){
        .producer_count = producer_count,
        .mode = (ipc_mode_t)parsed_mode,
    };

    return MPMC_OK;
}

/**
 * @brief Converte uma string decimal em um inteiro estritamente positivo.
 *
 * @details
 * Aceita exclusivamente dígitos decimais, sem sinais ou espaços.
 * Zeros à esquerda são permitidos, desde que o valor resultante
 * seja diferente de zero.
 *
 * A conversão utiliza strtoumax() e rejeita valores que excedam
 * a faixa representável por size_t.
 *
 * @param text String terminada em NUL a ser convertida.
 * @param[out] out Valor convertido em caso de sucesso.
 *
 * @retval MPMC_OK Conversão realizada com sucesso.
 * @retval MPMC_ERR_INVALID_ARG String vazia, caracteres inválidos,
 *         valor nulo, estouro numérico ou ponteiro inválido.
 *
 * @post Em caso de sucesso, *out contém o valor convertido.
 * Em caso de erro, *out permanece inalterado, quando válido.
 *
 * @note A função pode modificar errno durante a conversão.
 * O chamador não deve depender de seu valor após o retorno.
 */
static mpmc_status_t parse_positive_integer(const char *text, size_t *out) {
    if (text == NULL || out == NULL || *text == '\0') {
        return MPMC_ERR_INVALID_ARG;
    }

    for (const char *ptr = text; *ptr != '\0'; ptr++) {
        if (*ptr < '0' || *ptr > '9') {
            return MPMC_ERR_INVALID_ARG;
        }
    }

    errno = 0;

    char *endptr;
    uintmax_t value = strtoumax(text, &endptr, 10);

    if (errno == ERANGE || *endptr != '\0' || value == 0 || value > SIZE_MAX) {
        return MPMC_ERR_INVALID_ARG;
    }

    *out = (size_t)value;
    return MPMC_OK;
}
