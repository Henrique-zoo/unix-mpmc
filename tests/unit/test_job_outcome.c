/**
 * @file test_job_outcome.c
 * @brief Testes do texto de resultados de trabalhos (issue #54).
 * @ingroup tests
 *
 * @details
 * Exercita job_outcome_format() com eventos controlados, sem processos, IPC ou
 * backends: erro de kernel, consumo/produção parcial, cancelamento, falha ao criar
 * o filho e conclusão. Confere que o texto identifica operação, índice e
 * quantidade efetiva, que system_errno só é interpretado quando válido e que um
 * resultado incompleto nunca é apresentado como conclusão bem-sucedida.
 *
 * Retorna 0 se todos os casos passarem; 1 caso contrário.
 */

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>

#include "job_outcome.h"

static unsigned g_checks;
static unsigned g_failures;

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

#define CHECK_TEXT(outcome, expected)                                                              \
    do {                                                                                           \
        char text_[512];                                                                           \
        int length_ = job_outcome_format(&(outcome), text_, sizeof text_);                         \
        CHECK(                                                                                     \
            length_ >= 0 && strcmp(text_, (expected)) == 0,                                        \
            "texto: \"%s\" (esperado \"%s\")",                                                     \
            text_,                                                                                 \
            (expected)                                                                             \
        );                                                                                         \
        CHECK(                                                                                     \
            length_ == (int)strlen(text_),                                                         \
            "retorno %d difere do comprimento %zu",                                                \
            length_,                                                                               \
            strlen(text_)                                                                          \
        );                                                                                         \
    } while (0)

/* Causa esperada, montada com o formatador do módulo de erros. */
static void expected_cause(mpmc_result_t error, char *out, size_t capacity) {
    (void)mpmc_error_format(error, out, capacity);
}

static job_outcome_t make(
    job_outcome_kind_t kind,
    command_kind_t command,
    uint64_t operation_id,
    size_t producer_id,
    uint64_t requested,
    uint64_t effective,
    mpmc_result_t error
) {
    return (job_outcome_t){
        .kind = kind,
        .command = command,
        .operation_id = operation_id,
        .producer_id = producer_id,
        .requested = requested,
        .effective = effective,
        .error = error,
    };
}

static const mpmc_result_t NO_ERROR = {.status = MPMC_OK};

/* ------------------------------------------------------------------ */

static void test_exact_texts(void) {
    job_outcome_t completed = make(JOB_OUTCOME_COMPLETED, COMMAND_CONSUME, 7, 3, 5, 5, NO_ERROR);
    CHECK_TEXT(
        completed, "operação 7 (consome, estrutura 3) concluída: 5 de 5 mensagens consumidas."
    );

    job_outcome_t partial = make(JOB_OUTCOME_PARTIAL, COMMAND_CONSUME, 7, 3, 5, 3, NO_ERROR);
    CHECK_TEXT(
        partial,
        "operação 7 (consome, estrutura 3) não foi concluída: apenas 3 de 5 mensagens consumidas."
    );

    job_outcome_t cancelled = make(JOB_OUTCOME_CANCELLED, COMMAND_PRODUCE, 5, 1, 8, 2, NO_ERROR);
    CHECK_TEXT(
        cancelled,
        "operação 5 (produz, estrutura 1) cancelada antes de terminar: 2 de 8 mensagens produzidas."
    );

    job_outcome_t distributed =
        make(JOB_OUTCOME_COMPLETED, COMMAND_PRODUCE, 2, 0, 12, 12, NO_ERROR);
    CHECK_TEXT(
        distributed,
        "operação 2 (produz, todas as estruturas) concluída: 12 de 12 mensagens produzidas."
    );
}

static void test_kernel_error_and_spawn_failure(void) {
    char cause[256];
    char expected[512];

    mpmc_result_t io_error = {.status = MPMC_ERR_SYS, .system_errno = EIO};
    job_outcome_t kernel = make(JOB_OUTCOME_KERNEL_ERROR, COMMAND_PRODUCE, 9, 2, 10, 4, io_error);
    expected_cause(io_error, cause, sizeof cause);
    (void)snprintf(
        expected,
        sizeof expected,
        "operação 9 (produz, estrutura 2) falhou após 4 de 10 mensagens produzidas: %s",
        cause
    );
    CHECK_TEXT(kernel, expected);
    CHECK(strstr(expected, strerror(EIO)) != nullptr, "o errno válido deve ser interpretado");

    /* Falha ao criar o filho: nenhuma quantidade efetiva é mencionada. */
    mpmc_result_t again = {.status = MPMC_ERR_SYS, .system_errno = EAGAIN};
    job_outcome_t spawn = make(JOB_OUTCOME_SPAWN_FAILED, COMMAND_PRODUCE, 0, 1, 3, 0, again);
    expected_cause(again, cause, sizeof cause);
    (void)snprintf(
        expected,
        sizeof expected,
        "comando (produz, estrutura 1): não foi possível criar o processo filho: %s",
        cause
    );
    CHECK_TEXT(spawn, expected);
    CHECK(
        strstr(expected, " de 3 mensagens") == nullptr,
        "falha de criação não tem quantidade efetiva"
    );

    job_outcome_t spawn_with_id =
        make(JOB_OUTCOME_SPAWN_FAILED, COMMAND_CONSUME, 4, 2, 5, 0, again);
    char text[512];
    CHECK(job_outcome_format(&spawn_with_id, text, sizeof text) > 0, "formatação falhou");
    CHECK(
        strncmp(text, "operação 4 (consome, estrutura 2): não foi possível criar", 56) == 0,
        "texto: %s",
        text
    );
}

static void test_identification_of_operation_index_and_quantity(void) {
    job_outcome_t outcome = make(
        JOB_OUTCOME_KERNEL_ERROR,
        COMMAND_CONSUME,
        123456789012ull,
        42,
        1024,
        1000,
        (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = EINTR}
    );
    char text[512];

    CHECK(job_outcome_format(&outcome, text, sizeof text) > 0, "formatação falhou");
    CHECK(strstr(text, "operação 123456789012") != nullptr, "falta a operação: %s", text);
    CHECK(strstr(text, "consome") != nullptr, "falta o verbo: %s", text);
    CHECK(strstr(text, "estrutura 42") != nullptr, "falta o índice: %s", text);
    CHECK(strstr(text, "1000 de 1024") != nullptr, "falta a quantidade efetiva: %s", text);
}

static void test_incomplete_results_are_never_success(void) {
    const job_outcome_kind_t kinds[] = {
        JOB_OUTCOME_PARTIAL,
        JOB_OUTCOME_CANCELLED,
        JOB_OUTCOME_KERNEL_ERROR,
        JOB_OUTCOME_SPAWN_FAILED,
    };

    for (size_t i = 0; i < sizeof kinds / sizeof kinds[0]; ++i) {
        /* Mesmo com quantidade efetiva igual à solicitada, esses tipos não são sucesso. */
        job_outcome_t outcome = make(
            kinds[i],
            COMMAND_PRODUCE,
            3,
            1,
            5,
            5,
            (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = EIO}
        );
        char text[512];

        CHECK(!job_outcome_is_success(&outcome), "tipo %d não pode ser sucesso", (int)kinds[i]);
        CHECK(job_outcome_format(&outcome, text, sizeof text) > 0, "formatação falhou");
        CHECK(
            strstr(text, ") concluída") == nullptr,
            "tipo %d apresentado como conclusão: %s",
            (int)kinds[i],
            text
        );
    }

    /* Conclusão declarada com menos mensagens que o solicitado vira parcial. */
    job_outcome_t claimed = make(JOB_OUTCOME_COMPLETED, COMMAND_CONSUME, 8, 2, 5, 4, NO_ERROR);
    job_outcome_t partial = make(JOB_OUTCOME_PARTIAL, COMMAND_CONSUME, 8, 2, 5, 4, NO_ERROR);
    char claimed_text[512];
    char partial_text[512];

    CHECK(!job_outcome_is_success(&claimed), "4 de 5 não pode ser sucesso");
    CHECK(job_outcome_format(&claimed, claimed_text, sizeof claimed_text) > 0, "falhou");
    CHECK(job_outcome_format(&partial, partial_text, sizeof partial_text) > 0, "falhou");
    CHECK(strcmp(claimed_text, partial_text) == 0, "texto de conclusão falsa: %s", claimed_text);

    job_outcome_t exact = make(JOB_OUTCOME_COMPLETED, COMMAND_CONSUME, 8, 2, 5, 5, NO_ERROR);

    CHECK(job_outcome_is_success(&exact), "5 de 5 deve ser sucesso");
    CHECK(!job_outcome_is_success(nullptr), "NULL não é sucesso");
}

static void test_errno_is_interpreted_only_when_valid(void) {
    char text[512];
    const int invalid_errnos[] = {0, -1, INT_MIN, JOB_OUTCOME_ERRNO_MAX + 1, INT_MAX};

    for (size_t i = 0; i < sizeof invalid_errnos / sizeof invalid_errnos[0]; ++i) {
        mpmc_result_t error = {.status = MPMC_ERR_SYS, .system_errno = invalid_errnos[i]};
        job_outcome_t outcome = make(JOB_OUTCOME_KERNEL_ERROR, COMMAND_PRODUCE, 1, 1, 5, 2, error);

        CHECK(job_outcome_format(&outcome, text, sizeof text) > 0, "formatação falhou");
        CHECK(
            strstr(text, "Unknown error") == nullptr && strstr(text, "Success") == nullptr,
            "errno %d inválido foi interpretado: %s",
            invalid_errnos[i],
            text
        );
        CHECK(
            strstr(text, mpmc_status_str(MPMC_ERR_SYS)) != nullptr,
            "a categoria deve aparecer: %s",
            text
        );
    }

    /* Status OK não carrega causa: o errno é ignorado e a causa é "não informada". */
    mpmc_result_t ok_with_errno = {.status = MPMC_OK, .system_errno = EIO};
    job_outcome_t no_cause =
        make(JOB_OUTCOME_KERNEL_ERROR, COMMAND_PRODUCE, 1, 1, 5, 2, ok_with_errno);

    CHECK(job_outcome_format(&no_cause, text, sizeof text) > 0, "formatação falhou");
    CHECK(strstr(text, "causa não informada") != nullptr, "causa ausente: %s", text);
    CHECK(strstr(text, strerror(EIO)) == nullptr, "errno de status OK foi interpretado: %s", text);

    /* Status que não é de sistema também pode carregar um errno válido. */
    mpmc_result_t interrupted = {.status = MPMC_ERR_INTERRUPTED, .system_errno = EINTR};
    job_outcome_t with_errno =
        make(JOB_OUTCOME_KERNEL_ERROR, COMMAND_CONSUME, 1, 1, 5, 2, interrupted);

    CHECK(job_outcome_format(&with_errno, text, sizeof text) > 0, "formatação falhou");
    CHECK(strstr(text, strerror(EINTR)) != nullptr, "errno válido não interpretado: %s", text);

    /* Limites do intervalo válido. */
    mpmc_result_t first = {.status = MPMC_ERR_SYS, .system_errno = 1};
    job_outcome_t at_one = make(JOB_OUTCOME_SPAWN_FAILED, COMMAND_PRODUCE, 0, 1, 1, 0, first);

    CHECK(job_outcome_format(&at_one, text, sizeof text) > 0, "formatação falhou");
    CHECK(strstr(text, strerror(1)) != nullptr, "errno 1 deveria ser interpretado: %s", text);
}

static void test_lines_are_single_and_unprefixed(void) {
    const job_outcome_kind_t kinds[] = {
        JOB_OUTCOME_COMPLETED,
        JOB_OUTCOME_PARTIAL,
        JOB_OUTCOME_CANCELLED,
        JOB_OUTCOME_KERNEL_ERROR,
        JOB_OUTCOME_SPAWN_FAILED,
    };

    for (size_t i = 0; i < sizeof kinds / sizeof kinds[0]; ++i) {
        job_outcome_t outcome = make(
            kinds[i],
            COMMAND_PRODUCE,
            1,
            1,
            3,
            2,
            (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = EAGAIN}
        );
        char text[512];

        CHECK(job_outcome_format(&outcome, text, sizeof text) > 0, "formatação falhou");
        CHECK(strchr(text, '\n') == nullptr, "o texto não pode ter quebra de linha: %s", text);
        CHECK(strncmp(text, "mpmc:", 5) != 0, "o prefixo é do chamador: %s", text);
    }
}

static void test_buffer_contract(void) {
    job_outcome_t outcome = make(JOB_OUTCOME_PARTIAL, COMMAND_CONSUME, 7, 3, 5, 3, NO_ERROR);
    char full[512];
    int needed = job_outcome_format(&outcome, full, sizeof full);

    CHECK(needed > 0, "formatação falhou");
    CHECK(job_outcome_format(&outcome, nullptr, 0) == needed, "tamanho sem buffer difere");

    char small[16];
    memset(small, 'x', sizeof small);
    CHECK(job_outcome_format(&outcome, small, sizeof small) == needed, "retorno com truncamento");
    CHECK(
        small[sizeof small - 1] == '\0' && strlen(small) == sizeof small - 1,
        "texto truncado deve terminar em NUL"
    );
    CHECK(strncmp(small, full, sizeof small - 1) == 0, "o truncamento deve preservar o início");

    char one[1] = {'x'};
    CHECK(
        job_outcome_format(&outcome, one, sizeof one) == needed && one[0] == '\0',
        "capacidade 1 deve produzir texto vazio"
    );

    CHECK(job_outcome_format(nullptr, full, sizeof full) == -1, "evento nulo");
    CHECK(job_outcome_format(&outcome, nullptr, 8) == -1, "buffer nulo com capacidade");

    job_outcome_t unknown = make((job_outcome_kind_t)99, COMMAND_CONSUME, 1, 1, 1, 1, NO_ERROR);

    CHECK(job_outcome_format(&unknown, full, sizeof full) == -1, "tipo desconhecido");
}

int main(void) {
    test_exact_texts();
    test_kernel_error_and_spawn_failure();
    test_identification_of_operation_index_and_quantity();
    test_incomplete_results_are_never_success();
    test_errno_is_interpreted_only_when_valid();
    test_lines_are_single_and_unprefixed();
    test_buffer_contract();

    printf("%u verificações, %u falhas\n", g_checks, g_failures);

    return g_failures == 0 ? 0 : 1;
}
