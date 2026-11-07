/**
 * @defgroup tests Testes
 * @brief Testes unitários e de integração do projeto.
 */

/**
 * @file test_command.c
 * @brief Testes unitários de command_parse() e command_validate().
 * @ingroup tests
 *
 * @details
 * O parsing está dividido em duas etapas com contratos distintos:
 *
 * - command_parse(line, out)                — sintaxe
 * - command_validate(&cmd, producer_count)  — semântica
 *
 * Os testes de overflow distinguem três situações diferentes:
 *   1. valor que excede uintmax_t             → parse rejeita
 *   2. valor que cabe em uintmax_t mas não
 *      no tipo do campo (size_t/unsigned)     → parse rejeita
 *   3. valor que cabe no campo mas excede
 *      a faixa normativa (p, 1024, 2)         → validate rejeita
 *
 * Retorna 0 se todos os casos passarem; 1 caso contrário.
 */

#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "mpmc/app/command.h"
#include "mpmc/error.h"

/**
 * @brief Quantidade de verificações executadas nesta suíte.
 */
static unsigned g_checks;
/**
 * @brief Quantidade de verificações que registraram falha nesta suíte.
 */
static unsigned g_failures;

/* ------------------------------------------------------------------ */
/* Infraestrutura mínima                                              */
/* ------------------------------------------------------------------ */

/**
 * @brief Registra uma verificação e informa a falha sem interromper a suíte.
 * @param condition Expressão lógica avaliada uma única vez.
 * @param ... Formato e argumentos de fprintf() para o diagnóstico em caso de falha.
 * @details Incrementa o contador de verificações; se a condição for falsa,
 * incrementa o contador de falhas e escreve o diagnóstico em stderr.
 */
#define CHECK(condition, ...)                                                                      \
    do {                                                                                           \
        ++g_checks;                                                                                \
        if (!(condition)) {                                                                        \
            ++g_failures;                                                                          \
            fprintf(stderr, "FALHA %s:%d: ", __FILE__, __LINE__);                                  \
            fprintf(stderr, __VA_ARGS__);                                                          \
            fputc('\n', stderr);                                                                   \
        }                                                                                          \
    } while (0)

/* Sentinela para provar que *out não foi tocado quando o parse falha. */
/**
 * @brief Índice sentinela usado para detectar escrita indevida em uma saída.
 */
#define SENTINEL_INDEX ((size_t)0xDEADBEEF)
/**
 * @brief Contagem sentinela usada para detectar escrita indevida em uma saída.
 */
#define SENTINEL_COUNT ((unsigned)0xCAFE)
/**
 * @brief Política sentinela usada para detectar escrita indevida em uma saída.
 */
#define SENTINEL_ACK ((unsigned)0xBA5E)

/**
 * @brief Constrói um comando com valores sentinela para verificar preservação da saída.
 * @return Comando zerado inicialmente, com kind=COMMAND_SHUTDOWN e os campos
 * numéricos preenchidos pelas constantes SENTINEL_INDEX, SENTINEL_COUNT e SENTINEL_ACK.
 */
static command_t make_sentinel(void) {
    command_t c;

    memset(&c, 0, sizeof c);
    c.kind = COMMAND_SHUTDOWN; /* diferente dos valores esperados */
    c.producer_id = SENTINEL_INDEX;
    c.message_count = SENTINEL_COUNT;
    c.ack_mode = SENTINEL_ACK;
    return c;
}

/**
 * @brief Verifica os três campos numéricos sentinela de um comando.
 * @param c Comando válido a examinar.
 * @return Um se os três campos coincidirem com as sentinelas; zero caso contrário.
 * @note Não compara kind nem background_token.
 */
static int sentinel_intact(const command_t *c) {
    return c->producer_id == SENTINEL_INDEX && c->message_count == SENTINEL_COUNT
        && c->ack_mode == SENTINEL_ACK;
}

/* ------------------------------------------------------------------ */
/* Funções auxiliares                                                 */
/* ------------------------------------------------------------------ */

/**
 * @brief Confere parsing, campos normalizados e validação de uma linha válida.
 * @param line Linha de comando terminada em NUL.
 * @param producers Quantidade de estruturas usada na validação semântica.
 * @param kind Tipo de comando esperado.
 * @param index Índice lógico esperado.
 * @param n_msgs Quantidade de mensagens esperada.
 * @param ack Política de confirmação esperada.
 * @param background Valor esperado para a presença do sufixo &.
 * @details Registra falhas pelos contadores da suíte. Se o parsing falhar,
 * retorna antes de examinar os campos ou executar a validação semântica.
 */
static void expect_ok_bg(
    const char *line,
    unsigned producers,
    command_kind_t kind,
    size_t index,
    unsigned n_msgs,
    unsigned ack,
    bool background
) {
    command_t c = make_sentinel();
    mpmc_result_t error = command_parse(line, &c);
    CHECK(error.system_errno == 0, "parsing não deve carregar erro de sistema");

    ++g_checks;
    if (error.status != MPMC_OK) {
        ++g_failures;
        fprintf(
            stderr,
            "FALHA %s:%d: \"%s\": parse esperava MPMC_OK, "
            "obtido %d\n",
            __FILE__,
            __LINE__,
            line,
            (int)error.status
        );
        return;
    }

    CHECK(c.kind == kind, "\"%s\": kind esperado %d, obtido %d", line, (int)kind, (int)c.kind);
    CHECK(
        c.producer_id == index,
        "\"%s\": índice esperado %zu, obtido %zu",
        line,
        index,
        c.producer_id
    );
    CHECK(
        c.message_count == n_msgs,
        "\"%s\": n_msgs esperado %u, obtido %u",
        line,
        n_msgs,
        c.message_count
    );
    CHECK(c.ack_mode == ack, "\"%s\": ack esperado %u, obtido %u", line, ack, c.ack_mode);
    CHECK(
        c.background_token == background,
        "\"%s\": background_token esperado %d, obtido %d",
        line,
        (int)background,
        (int)c.background_token
    );

    error = command_validate(&c, (size_t)producers);
    CHECK(error.system_errno == 0, "validação não deve carregar erro de sistema");
    CHECK(
        error.status == MPMC_OK,
        "\"%s\" p=%u: validate esperava MPMC_OK, "
        "obtido %d",
        line,
        producers,
        (int)error.status
    );
}

/**
 * @brief Confere uma linha válida sem o sufixo de execução em background.
 * @param line Linha de comando terminada em NUL.
 * @param producers Quantidade de estruturas usada na validação semântica.
 * @param kind Tipo de comando esperado.
 * @param index Índice lógico esperado.
 * @param n_msgs Quantidade de mensagens esperada.
 * @param ack Política de confirmação esperada.
 * @details Delega a expect_ok_bg() com background=false.
 */
static void expect_ok(
    const char *line,
    unsigned producers,
    command_kind_t kind,
    size_t index,
    unsigned n_msgs,
    unsigned ack
) {
    expect_ok_bg(line, producers, kind, index, n_msgs, ack, false);
}

/**
 * @brief Confere rejeição sintática sem sobrescrever os campos numéricos da saída.
 * @param line Linha que deve ser rejeitada por command_parse().
 * @details Espera MPMC_ERR_INVALID_ARG com system_errno zero e verifica as sentinelas.
 */
static void expect_parse_error(const char *line) {
    command_t c = make_sentinel();
    mpmc_result_t error = command_parse(line, &c);

    CHECK(
        error.status == MPMC_ERR_INVALID_ARG,
        "\"%s\": parse esperava MPMC_ERR_INVALID_ARG, obtido %d",
        line,
        (int)error.status
    );
    CHECK(error.system_errno == 0, "erro de sintaxe deve ter system_errno zero");
    CHECK(sentinel_intact(&c), "\"%s\": *out foi modificado em falha de parsing", line);
}

/**
 * @brief Confere uma linha aceita pelo parser e rejeitada na validação semântica.
 * @param line Linha sintaticamente válida a examinar.
 * @param producers Quantidade de estruturas lógicas informada à validação.
 * @param expected Categoria de erro esperada de command_validate().
 * @details Se o parsing falhar, registra a inconsistência e não valida a saída.
 */
static void expect_validate_error(const char *line, unsigned producers, mpmc_status_t expected) {
    command_t c = make_sentinel();
    mpmc_result_t error = command_parse(line, &c);

    ++g_checks;
    if (error.status != MPMC_OK) {
        ++g_failures;
        fprintf(
            stderr,
            "FALHA %s:%d: \"%s\": parse deveria aceitar antes "
            "da validação semântica, obtido %d\n",
            __FILE__,
            __LINE__,
            line,
            (int)error.status
        );
        return;
    }

    error = command_validate(&c, (size_t)producers);
    CHECK(error.system_errno == 0, "erro de validação deve ter system_errno zero");
    CHECK(
        error.status == expected,
        "\"%s\" p=%u: validate esperava %d, obtido %d",
        line,
        producers,
        (int)expected,
        (int)error.status
    );
}

/* ------------------------------------------------------------------ */
/* U01 — comandos válidos                                             */
/* ------------------------------------------------------------------ */

/**
 * @brief Confere produção, consumo e shutdown com argumentos válidos.
 */
static void test_valid_commands(void) {
    expect_ok("produz 1 1 1", 4, COMMAND_PRODUCE, 1, 1, 1);
    expect_ok("produz 2 5 2", 4, COMMAND_PRODUCE, 2, 5, 2);
    expect_ok("produz 4 1024 1", 4, COMMAND_PRODUCE, 4, 1024, 1);
    expect_ok("consome 1 1 1", 4, COMMAND_CONSUME, 1, 1, 1);
    expect_ok("consome 3 7 2", 4, COMMAND_CONSUME, 3, 7, 2);
    expect_ok("consome 4 1024 2", 4, COMMAND_CONSUME, 4, 1024, 2);
    expect_ok("shutdown", 4, COMMAND_SHUTDOWN, 0, 0, 0);
    expect_ok("shutdown", 1, COMMAND_SHUTDOWN, 0, 0, 0);
}

/* ------------------------------------------------------------------ */
/* U02 — falhas de sintaxe (command_parse)                            */
/* ------------------------------------------------------------------ */

/**
 * @brief Confere rejeição de verbos, aridades e formatos numéricos inválidos.
 */
static void test_syntax_errors(void) {
    /* linha vazia ou só separadores */
    expect_parse_error("");
    expect_parse_error("   ");
    expect_parse_error("\t\n");

    /* verbo desconhecido — diferencia maiúsculas de minúsculas */
    expect_parse_error("xyz 1 2 3");
    expect_parse_error("PRODUZ 1 1 1");
    expect_parse_error("Produz 1 1 1");
    expect_parse_error("produzir 1 1 1");
    expect_parse_error("produz 1 1 1 produzem");

    /* aridade */
    expect_parse_error("produz");
    expect_parse_error("produz 1");
    expect_parse_error("produz 1 5");
    expect_parse_error("produz 1 5 1 9");
    expect_parse_error("consome 1 5");
    expect_parse_error("consome 1 5 1 9");
    expect_parse_error("shutdown 1");
    expect_parse_error("shutdown extra");

    /* formato numérico */
    expect_parse_error("produz abc 5 1");
    expect_parse_error("produz 1 abc 1");
    expect_parse_error("produz 1 5 abc");
    expect_parse_error("produz 1 5 1x");
    expect_parse_error("produz 1 5 x1");
    expect_parse_error("produz 1 5 -1");
    expect_parse_error("produz 1 -5 1");
    expect_parse_error("produz 1 5 +1");
    expect_parse_error("produz 1 5 1.0");
    expect_parse_error("produz 1 5 0x1");
    expect_parse_error("produz 1 5 1e3");
}

/* ------------------------------------------------------------------ */
/* U03 — limites de n_msgs (command_validate)                         */
/* ------------------------------------------------------------------ */

/**
 * @brief Confere as fronteiras de 1 a 1024 mensagens na validação semântica.
 */
static void test_message_count_bounds(void) {
    expect_validate_error("produz 1 0 1", 4, MPMC_ERR_INVALID_ARG);
    expect_ok("produz 1 1 1", 4, COMMAND_PRODUCE, 1, 1, 1);
    expect_ok("produz 1 1024 1", 4, COMMAND_PRODUCE, 1, 1024, 1);
    expect_validate_error("produz 1 1025 1", 4, MPMC_ERR_INVALID_ARG);
    expect_validate_error("produz 1 999999 1", 4, MPMC_ERR_INVALID_ARG);

    expect_validate_error("consome 1 0 1", 4, MPMC_ERR_INVALID_ARG);
    expect_ok("consome 1 1024 1", 4, COMMAND_CONSUME, 1, 1024, 1);
    expect_validate_error("consome 1 1025 1", 4, MPMC_ERR_INVALID_ARG);
}

/* ------------------------------------------------------------------ */
/* U04 — limites de ack (command_validate)                            */
/* ------------------------------------------------------------------ */

/**
 * @brief Confere que somente as políticas de confirmação 1 e 2 são aceitas.
 */
static void test_ack_bounds(void) {
    expect_validate_error("produz 1 5 0", 4, MPMC_ERR_INVALID_ARG);
    expect_ok("produz 1 5 1", 4, COMMAND_PRODUCE, 1, 5, 1);
    expect_ok("produz 1 5 2", 4, COMMAND_PRODUCE, 1, 5, 2);
    expect_validate_error("produz 1 5 3", 4, MPMC_ERR_INVALID_ARG);
    expect_validate_error("produz 1 5 99", 4, MPMC_ERR_INVALID_ARG);

    expect_validate_error("consome 1 5 0", 4, MPMC_ERR_INVALID_ARG);
    expect_ok("consome 1 5 1", 4, COMMAND_CONSUME, 1, 5, 1);
    expect_ok("consome 1 5 2", 4, COMMAND_CONSUME, 1, 5, 2);
    expect_validate_error("consome 1 5 3", 4, MPMC_ERR_INVALID_ARG);
}

/* ------------------------------------------------------------------ */
/* U05 — produz 0 válido, consome 0 rejeitado                         */
/* ------------------------------------------------------------------ */

/**
 * @brief Confere a aceitação de produz 0 e a rejeição de consome 0.
 */
static void test_index_zero(void) {
    expect_ok("produz 0 1 1", 4, COMMAND_PRODUCE, 0, 1, 1);
    expect_ok("produz 0 8 2", 4, COMMAND_PRODUCE, 0, 8, 2);
    expect_ok("produz 0 1024 1", 4, COMMAND_PRODUCE, 0, 1024, 1);
    expect_ok("produz 0 5 1", 1, COMMAND_PRODUCE, 0, 5, 1);

    expect_validate_error("consome 0 1 1", 4, MPMC_ERR_INVALID_INDEX);
    expect_validate_error("consome 0 5 2", 4, MPMC_ERR_INVALID_INDEX);
    expect_validate_error("consome 0 1024 1", 4, MPMC_ERR_INVALID_INDEX);
    expect_validate_error("consome 0 5 1", 1, MPMC_ERR_INVALID_INDEX);
}

/* ------------------------------------------------------------------ */
/* U06 — índice contra p (command_validate)                           */
/* ------------------------------------------------------------------ */

/**
 * @brief Confere a faixa de índices para diferentes quantidades de estruturas.
 */
static void test_index_bounds(void) {
    /* p = 4: 1..4 válidos, 5+ inválidos */
    expect_ok("produz 1 1 1", 4, COMMAND_PRODUCE, 1, 1, 1);
    expect_ok("produz 4 1 1", 4, COMMAND_PRODUCE, 4, 1, 1);
    expect_validate_error("produz 5 1 1", 4, MPMC_ERR_INVALID_INDEX);
    expect_validate_error("produz 100 1 1", 4, MPMC_ERR_INVALID_INDEX);

    expect_ok("consome 1 1 1", 4, COMMAND_CONSUME, 1, 1, 1);
    expect_ok("consome 4 1 1", 4, COMMAND_CONSUME, 4, 1, 1);
    expect_validate_error("consome 5 1 1", 4, MPMC_ERR_INVALID_INDEX);

    /* p = 1: apenas índice 1 */
    expect_ok("produz 1 1 1", 1, COMMAND_PRODUCE, 1, 1, 1);
    expect_validate_error("produz 2 1 1", 1, MPMC_ERR_INVALID_INDEX);
    expect_ok("consome 1 1 1", 1, COMMAND_CONSUME, 1, 1, 1);
    expect_validate_error("consome 2 1 1", 1, MPMC_ERR_INVALID_INDEX);

    /* p = 8: índice 8 ainda válido */
    expect_ok("produz 8 1 1", 8, COMMAND_PRODUCE, 8, 1, 1);
    expect_validate_error("produz 9 1 1", 8, MPMC_ERR_INVALID_INDEX);
}

/* ------------------------------------------------------------------ */
/* U07 — sufixo '&' (command_parse)                                   */
/* ------------------------------------------------------------------ */

/**
 * @brief Confere o sufixo & separado ou colado e rejeita posições inválidas.
 */
static void test_ampersand_suffix(void) {
    expect_ok_bg("produz 1 5 1 &", 4, COMMAND_PRODUCE, 1, 5, 1, true);
    expect_ok_bg("produz 1 5 1&", 4, COMMAND_PRODUCE, 1, 5, 1, true);
    expect_ok_bg("consome 1 5 2 &", 4, COMMAND_CONSUME, 1, 5, 2, true);
    expect_ok_bg("consome 1 5 2&", 4, COMMAND_CONSUME, 1, 5, 2, true);
    expect_ok_bg("produz 0 8 2 &", 4, COMMAND_PRODUCE, 0, 8, 2, true);
    expect_ok_bg("shutdown &", 4, COMMAND_SHUTDOWN, 0, 0, 0, true);
    expect_ok_bg("shutdown&", 4, COMMAND_SHUTDOWN, 0, 0, 0, true);

    /* sem '&' o flag permanece falso */
    expect_ok_bg("produz 1 5 1", 4, COMMAND_PRODUCE, 1, 5, 1, false);

    /* '&' fora da última posição ou duplicado não é sufixo válido */
    expect_parse_error("produz & 1 5 1");
    expect_parse_error("produz 1 & 5 1");
    expect_parse_error("produz 1 5 1 & extra");
    expect_parse_error("produz 1 5 1 &&");
    expect_parse_error("& produz 1 5 1");

    /* '&' não preenche argumento ausente: sem o quarto campo o erro é
     * de aridade, não de índice. */
    expect_parse_error("produz 1 5 &");
}

/* ------------------------------------------------------------------ */
/* U08 — overflow e limites de tipo (command_parse)                   */
/* ------------------------------------------------------------------ */

/**
 * @brief Confere overflow numérico, limites dos tipos e distinção das faixas de domínio.
 */
static void test_overflow(void) {
    /*
     * --- 1. Valores que excedem uintmax_t em qualquer plataforma ---
     * 2^64 = 18446744073709551616
     */
    expect_parse_error("produz 18446744073709551616 1 1");
    expect_parse_error("produz 1 18446744073709551616 1");
    expect_parse_error("produz 1 5 18446744073709551616");

    /* 2^64 + 1: regressão */
    expect_parse_error("produz 18446744073709551617 1 1");
    expect_parse_error("produz 1 18446744073709551617 1");
    expect_parse_error("produz 1 5 18446744073709551617");

    /* sequência longa de dígitos */
    expect_parse_error("produz 999999999999999999999999999999 1 1");
    expect_parse_error("produz 1 999999999999999999999999999999 1");
    expect_parse_error("produz 1 5 999999999999999999999999999999");

    /*
     * --- 2. Valores que cabem em uintmax_t mas não no tipo do campo ---
     * Guardas de pré-processador mantêm o teste portável.
     */
#if UINT_MAX == 4294967295u
    /* unsigned de 32 bits: 2^32 excede o campo */
    expect_parse_error("produz 1 4294967296 1");
    expect_parse_error("produz 1 5 4294967296");

    /* limite exato de unsigned: cabe no campo, excede a faixa */
    expect_validate_error("produz 1 4294967295 1", 4, MPMC_ERR_INVALID_ARG);
    expect_validate_error("produz 1 5 4294967295", 4, MPMC_ERR_INVALID_ARG);
#endif

#if SIZE_MAX == 18446744073709551615ULL
    /*
     * size_t de 64 bits: 2^64 - 1 é exatamente SIZE_MAX, logo cabe no
     * campo. O parse aceita e a validação rejeita por exceder p.
     */
    expect_validate_error("produz 18446744073709551615 1 1", 4, MPMC_ERR_INVALID_INDEX);
#endif

#if SIZE_MAX == 4294967295u
    /* size_t de 32 bits: 2^32 excede o campo, então o parse rejeita */
    expect_parse_error("produz 4294967296 1 1");
    expect_validate_error("produz 4294967295 1 1", 4, MPMC_ERR_INVALID_INDEX);
#endif

    /*
     * --- 3. Valores que cabem no campo mas excedem a faixa normativa ---
     * Ponto de controle da separação entre as duas etapas: o parse
     * aceita, a validação rejeita.
     */
    expect_validate_error("produz 1 65535 1", 4, MPMC_ERR_INVALID_ARG);
}

/* ------------------------------------------------------------------ */
/* U09 — separação de responsabilidades                               */
/* ------------------------------------------------------------------ */

/**
 * @brief Confere a separação entre reconhecimento sintático e validação semântica.
 */
static void test_stage_separation(void) {
    command_t c = make_sentinel();

    /*
     * command_parse não aplica faixas de domínio: aceita índices,
     * n_msgs e ack fora de faixa, porque a checagem pertence a
     * command_validate.
     */
    CHECK(
        command_parse("produz 99 5000 3", &c).status == MPMC_OK,
        "parse deveria aceitar valores fora de faixa (etapa sintática)"
    );
    CHECK(
        c.producer_id == 99 && c.message_count == 5000 && c.ack_mode == 3,
        "parse deveria preservar os valores lidos"
    );
    CHECK(
        command_validate(&c, 4).status != MPMC_OK, "validate deveria rejeitar valores fora de faixa"
    );

    /* producers nem existe mais na assinatura do parse */
    CHECK(command_parse("produz 1 5 1", &c).status == MPMC_OK, "parse não depende de producers");
    CHECK(
        command_validate(&c, 0).status == MPMC_ERR_INVALID_ARG,
        "validate deveria rejeitar producer_count 0"
    );

    /* shutdown é aceito por validate independentemente dos campos */
    CHECK(command_parse("shutdown", &c).status == MPMC_OK, "shutdown parse");
    c.producer_id = 999;
    c.message_count = 999;
    c.ack_mode = 999;
    CHECK(
        command_validate(&c, 4).status == MPMC_OK, "shutdown ignora campos numéricos em validate"
    );
}

/* ------------------------------------------------------------------ */
/* U10 — guards de argumento                                          */
/* ------------------------------------------------------------------ */

/**
 * @brief Confere a rejeição de argumentos nulos sem sobrescrever as sentinelas.
 */
static void test_argument_guards(void) {
    command_t c = make_sentinel();

    CHECK(
        command_parse(NULL, &c).status == MPMC_ERR_INVALID_ARG,
        "line NULL deveria retornar INVALID_ARG"
    );
    CHECK(sentinel_intact(&c), "*out tocado com line NULL");

    CHECK(
        command_parse("produz 1 1 1", NULL).status == MPMC_ERR_INVALID_ARG,
        "out NULL deveria retornar INVALID_ARG"
    );

    CHECK(
        command_validate(NULL, 4).status == MPMC_ERR_INVALID_ARG,
        "validate com command NULL deveria retornar INVALID_ARG"
    );
}

/* ------------------------------------------------------------------ */
/* U11 — validate sobre structs construídos à mão                     */
/* ------------------------------------------------------------------ */

/**
 * @brief Confere a validação de comandos construídos diretamente, sem passar pelo parser.
 */
static void test_validate_direct(void) {
    command_t c;

    /* discriminante fora do enum */
    memset(&c, 0, sizeof c);
    c.kind = (command_kind_t)99;
    c.producer_id = 1;
    c.message_count = 1;
    c.ack_mode = 1;
    CHECK(
        command_validate(&c, 4).status == MPMC_ERR_INVALID_ARG,
        "kind fora do enum deveria retornar INVALID_ARG"
    );

    /* ordem das checagens: faixas antes do índice */
    memset(&c, 0, sizeof c);
    c.kind = COMMAND_PRODUCE;
    c.producer_id = 99;  /* índice inválido */
    c.message_count = 0; /* faixa inválida */
    c.ack_mode = 1;
    CHECK(
        command_validate(&c, 4).status == MPMC_ERR_INVALID_ARG,
        "faixa deveria ser checada antes do índice"
    );

    /* ack inválido com índice inválido: ack tem precedência */
    memset(&c, 0, sizeof c);
    c.kind = COMMAND_CONSUME;
    c.producer_id = 99;
    c.message_count = 1;
    c.ack_mode = 3;
    CHECK(
        command_validate(&c, 4).status == MPMC_ERR_INVALID_ARG,
        "ack deveria ser checado antes do índice"
    );

    /* caso puramente de índice */
    memset(&c, 0, sizeof c);
    c.kind = COMMAND_CONSUME;
    c.producer_id = 99;
    c.message_count = 1;
    c.ack_mode = 1;
    CHECK(
        command_validate(&c, 4).status == MPMC_ERR_INVALID_INDEX,
        "índice fora de 1..p deveria retornar INVALID_INDEX"
    );
}

/* ------------------------------------------------------------------ */
/* U12 — espaçamento e normalização                                   */
/* ------------------------------------------------------------------ */

/**
 * @brief Confere espaços, tabulações e terminações de linha aceitos pelo parser.
 */
static void test_whitespace(void) {
    expect_ok("produz   1   5   1", 4, COMMAND_PRODUCE, 1, 5, 1);
    expect_ok("\tproduz\t1\t5\t1\t", 4, COMMAND_PRODUCE, 1, 5, 1);
    expect_ok("produz 1 5 1\n", 4, COMMAND_PRODUCE, 1, 5, 1);
    expect_ok("produz 1 5 1\r\n", 4, COMMAND_PRODUCE, 1, 5, 1);
    expect_ok("  produz 1 5 1  ", 4, COMMAND_PRODUCE, 1, 5, 1);
    expect_ok("shutdown\n", 4, COMMAND_SHUTDOWN, 0, 0, 0);
    expect_ok("shutdown \t \n", 4, COMMAND_SHUTDOWN, 0, 0, 0);
}

/* ------------------------------------------------------------------ */
/* U13 — pureza e reuso                                               */
/* ------------------------------------------------------------------ */

/**
 * @brief Confere a preservação da linha original e o reuso do objeto de saída.
 */
static void test_no_side_effects_on_input(void) {
    char line[] = "produz 1 5 1 &";
    char copy[sizeof line];
    command_t c;

    memcpy(copy, line, sizeof line);
    (void)command_parse(line, &c);

    CHECK(memcmp(line, copy, sizeof line) == 0, "command_parse modificou a linha do chamador");

    CHECK(command_parse("produz 2 3 1", &c).status == MPMC_OK, "reuso 1");
    CHECK(
        c.kind == COMMAND_PRODUCE && c.producer_id == 2 && c.message_count == 3 && c.ack_mode == 1,
        "reuso 1: valores incorretos"
    );

    CHECK(command_parse("consome 4 9 2", &c).status == MPMC_OK, "reuso 2");
    CHECK(
        c.kind == COMMAND_CONSUME && c.producer_id == 4 && c.message_count == 9 && c.ack_mode == 2,
        "reuso 2: valores incorretos"
    );
}

/* ------------------------------------------------------------------ */
/* Execução dos testes                                                */
/* ------------------------------------------------------------------ */

/**
 * @brief Executa os casos da suíte e apresenta o resultado das verificações.
 * @return Zero se nenhuma verificação falhar; um se houver falha ou erro de preparação.
 */
int main(void) {
    test_valid_commands();
    test_syntax_errors();
    test_message_count_bounds();
    test_ack_bounds();
    test_index_zero();
    test_index_bounds();
    test_ampersand_suffix();
    test_overflow();
    test_stage_separation();
    test_argument_guards();
    test_validate_direct();
    test_whitespace();
    test_no_side_effects_on_input();

    printf("%u verificações, %u falhas\n", g_checks, g_failures);

    return g_failures == 0 ? 0 : 1;
}
