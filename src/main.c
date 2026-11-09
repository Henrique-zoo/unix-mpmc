/**
 * @file main.c
 * @brief Ponto de entrada e validação dos argumentos de inicialização.
 *
 * @details
 * Responsável por interpretar os argumentos, validar a configuração,
 * inicializar o supervisor e executar o terminal interativo.
 *
 * A validação é realizada por funções privadas, independentes dos
 * mecanismos de comunicação entre processos.
 *
 * O parsing de comandos e o ciclo de vida dos processos pertencem aos
 * módulos app/repl e app/supervisor; os backends de IPC são independentes.
 */

#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>

#include "mpmc/app/repl.h"
#include "mpmc/app/supervisor.h"
#include "mpmc/config.h"
#include "mpmc/error.h"

static mpmc_result_t parse_args(int argc, char *argv[], mpmc_config_t *out);
static mpmc_result_t parse_positive_integer(const char *text, size_t *out);

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
 * - Número de produtores lógicos, inteiro de 1 a MPMC_MAX_PRODUCERS.
 * - Paradigma IPC: 1 para memória compartilhada ou 2 para filas de mensagens.
 *
 * Em caso de argumentos inválidos, apresenta uma mensagem de erro
 * e a sintaxe esperada para a execução.
 */
int main(int argc, char *argv[]) {
    mpmc_config_t config;
    mpmc_result_t error = parse_args(argc, argv, &config);
    char error_text[256];

    if (error.status != MPMC_OK) {
        (void)mpmc_error_format(error, error_text, sizeof error_text);
        fprintf(stderr, "%s\n", error_text);
        fprintf(
            stderr, "Uso: %s <produtores: 1..%u> <paradigma: 1|2>\n", argv[0], MPMC_MAX_PRODUCERS
        );
        return EXIT_FAILURE;
    }

    supervisor_t *supervisor = NULL;
    error = supervisor_create(&supervisor, config.producer_count, config.mode);
    if (error.status != MPMC_OK) {
        (void)mpmc_error_format(error, error_text, sizeof error_text);
        fprintf(stderr, "mpmc: não foi possível inicializar o supervisor: %s\n", error_text);
        return EXIT_FAILURE;
    }

    error = repl_run(supervisor);
    if (error.status != MPMC_OK) {
        (void)mpmc_error_format(error, error_text, sizeof error_text);
        fprintf(stderr, "mpmc: REPL encerrado com erro: %s\n", error_text);
        (void)supervisor_shutdown(supervisor, NULL);
    }
    mpmc_result_t report_error = supervisor_print_report(supervisor, stdout);
    if (report_error.status != MPMC_OK) {
        (void)mpmc_error_format(report_error, error_text, sizeof error_text);
        fprintf(stderr, "mpmc: falha ao apresentar o relatório: %s\n", error_text);
        if (error.status == MPMC_OK) {
            error = report_error;
        }
    }
    supervisor_destroy(supervisor);
    return error.status == MPMC_OK ? EXIT_SUCCESS : EXIT_FAILURE;
}

/**
 * @brief Interpreta e valida os argumentos de inicialização.
 *
 * @details
 * Converte os argumentos textuais em uma configuração tipada,
 * verificando a quantidade e a validade dos parâmetros.
 *
 * São exigidos exatamente dois argumentos:
 * - Número de produtores lógicos: inteiro de 1 a MPMC_MAX_PRODUCERS.
 * - Paradigma IPC: IPC_SHARED_MEMORY ou IPC_MESSAGE_QUEUE.
 *
 * A conversão numérica é delegada a parse_positive_integer().
 *
 * @param argc Quantidade de argumentos, incluindo o executável.
 * @param argv Vetor de strings dos argumentos.
 * @param[out] out Configuração validada em caso de sucesso.
 *
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: quantidade de estruturas e paradigma validados e escritos em out; system_errno é
 *   zero.
 * - MPMC_ERR_INVALID_ARG: argc diferente de três, argv ou out nulo, formato/valor numérico
 *   inválido, quantidade acima de MPMC_MAX_PRODUCERS ou paradigma diferente de 1/2. Se
 *   strtoumax() falhar na conversão de um argumento, system_errno preserva o errno capturado,
 *   inclusive ERANGE; nas demais rejeições, system_errno é zero.
 *
 * @post Em caso de sucesso, *out contém a configuração validada.
 * Em caso de erro, *out permanece inalterado, quando válido.
 *
 * @note A função não inicializa recursos IPC nem cria processos.
 */
static mpmc_result_t parse_args(int argc, char *argv[], mpmc_config_t *out) {
    if (argc != 3 || argv == NULL || out == NULL) {
        return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};
    }

    size_t producer_count;
    size_t parsed_mode;

    mpmc_result_t error = parse_positive_integer(argv[1], &producer_count);
    if (error.status != MPMC_OK) return error;

    if (producer_count > MPMC_MAX_PRODUCERS) return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};

    error = parse_positive_integer(argv[2], &parsed_mode);
    if (error.status != MPMC_OK) return error;

    if (parsed_mode != IPC_SHARED_MEMORY && parsed_mode != IPC_MESSAGE_QUEUE) {
        return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};
    }

    *out = (mpmc_config_t){
        .producer_count = producer_count,
        .mode = (ipc_mode_t)parsed_mode,
    };

    return (mpmc_result_t){.status = MPMC_OK};
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
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: decimal positivo representável em size_t escrito em out; system_errno é zero.
 * - MPMC_ERR_INVALID_ARG: text ou out nulo, texto vazio, caractere não decimal, valor zero,
 *   valor não representável em size_t ou falha de strtoumax(). system_errno contém o errno
 *   capturado quando strtoumax() falha, inclusive ERANGE; nos demais casos, é zero.
 *
 * @post Em caso de sucesso, *out contém o valor convertido.
 * Em caso de erro, *out permanece inalterado, quando válido.
 *
 * @note Erros de strtoumax() são preservados em system_errno. Erros de
 * formato/faixa sem causa de sistema possuem system_errno igual a zero.
 */
static mpmc_result_t parse_positive_integer(const char *text, size_t *out) {
    if (text == NULL || out == NULL || *text == '\0') {
        return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};
    }

    for (const char *ptr = text; *ptr != '\0'; ptr++) {
        if (*ptr < '0' || *ptr > '9') return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};
    }

    errno = 0;

    char *endptr;
    uintmax_t value = strtoumax(text, &endptr, 10);
    int conversion_errno = errno;

    if (conversion_errno != 0) {
        return (mpmc_result_t){
            .status = MPMC_ERR_INVALID_ARG,
            .system_errno = conversion_errno,
        };
    }
    if (*endptr != '\0' || value == 0 || value > SIZE_MAX) {
        return (mpmc_result_t){.status = MPMC_ERR_INVALID_ARG};
    }

    *out = (size_t)value;
    return (mpmc_result_t){.status = MPMC_OK};
}
