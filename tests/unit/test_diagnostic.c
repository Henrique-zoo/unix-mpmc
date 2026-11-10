/**
 * @file test_diagnostic.c
 * @brief Verifica hierarquia, preservação de errno e apresentação de todos os erros.
 * @ingroup tests
 */
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "../../src/app/diagnostics.h"
#include "mpmc/diagnostic.h"

/** @brief Registros copiados pelo observador da fixture. */
static mpmc_diagnostic_t errors[16];
/** @brief Contadores da fixture. */
static unsigned count, checks, failures;
/** @brief Acumula verificações também em release. */
#define CHECK(test)                                                                                \
    do {                                                                                           \
        ++checks;                                                                                  \
        if (!(test)) {                                                                             \
            ++failures;                                                                            \
            fprintf(stderr, "FALHA %s:%d: %s\n", __FILE__, __LINE__, #test);                       \
        }                                                                                          \
    } while (0)

static void collect(const mpmc_diagnostic_t *error, void *user_data);
static void test_nested(void);
static void test_control_flow(void);
static void test_presentation(void);

int main(void) {
    test_nested();
    test_control_flow();
    test_presentation();
    mpmc_diagnostic_set_reporter(nullptr);
    CHECK(!mpmc_diagnostic_enabled());
    printf("%u verificações de observadores, %u falhas\n", checks, failures);
    return failures == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}

/** @brief Copia por valor e altera errno para verificar o isolamento da callback. */
static void collect(const mpmc_diagnostic_t *error, void *user_data) {
    CHECK(user_data == errors);
    CHECK(count < sizeof errors / sizeof errors[0]);
    if (count < sizeof errors / sizeof errors[0]) errors[count++] = *error;
    errno = ERANGE;
}

/** @brief Preserva a causa inicial, registra várias liberações e evita duplicar propagação. */
static void test_nested(void) {
    mpmc_reporter_t reporter = {.on_error = collect, .user_data = errors};
    mpmc_diagnostic_set_reporter(&reporter);
    mpmc_diagnostic_after_fork(77, 3);
    mpmc_diagnostic_scope_t root, primitive, cleanup;
    mpmc_diagnostic_begin(&root, "criar_contexto", false, false);
    mpmc_diagnostic_begin(&primitive, "sysv_sem_create", false, true);
    mpmc_result_t primary = {.status = MPMC_ERR_SYS, .system_errno = ENOSPC};
    errno = EIO;
    CHECK(mpmc_diagnostic_end(&primitive, primary).system_errno == ENOSPC);
    CHECK(errno == EIO);
    CHECK(count == 1);
    mpmc_diagnostic_begin(&cleanup, "rollback", true, false);
    mpmc_diagnostic_report(
        "shmdt()", (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = EACCES}
    );
    mpmc_diagnostic_report(
        "shmctl(IPC_RMID)", (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = EPERM}
    );
    CHECK(mpmc_diagnostic_end(&cleanup, (mpmc_result_t){.status = MPMC_OK}).system_errno == EACCES);
    CHECK(mpmc_diagnostic_end(&root, primary).system_errno == ENOSPC);
    CHECK(count == 3);
    CHECK(errors[0].phase == MPMC_DIAGNOSTIC_OPERATION);
    CHECK(errors[1].phase == MPMC_DIAGNOSTIC_CLEANUP && errors[2].phase == MPMC_DIAGNOSTIC_CLEANUP);
    CHECK(errors[0].group == errors[2].group);
    CHECK(strcmp(errors[0].function, "criar_contexto") == 0);
    CHECK(strcmp(errors[0].operation, "semget()") == 0);
    CHECK(errors[0].operation_id == 77 && errors[0].producer_id == 3);
    CHECK(errors[0].process_pid == getpid());
    // Replay do filho não transforma uma operação bem-sucedida do pai em falha.
    mpmc_diagnostic_begin(&root, "pai", false, false);
    mpmc_diagnostic_deliver(&errors[0]);
    CHECK(mpmc_diagnostic_end(&root, (mpmc_result_t){.status = MPMC_OK}).status == MPMC_OK);
}

/** @brief EAGAIN e EINTR repetível não geram diagnósticos; EINTR final em cleanup gera. */
static void test_control_flow(void) {
    unsigned before = count;
    mpmc_diagnostic_scope_t scope;
    mpmc_diagnostic_begin(&scope, "sysv_sem_apply", false, true);
    CHECK(
        mpmc_diagnostic_end(
            &scope, (mpmc_result_t){.status = MPMC_ERR_INTERRUPTED, .system_errno = EINTR}
        )
            .status
        == MPMC_ERR_INTERRUPTED
    );
    CHECK(count == before);
    mpmc_diagnostic_begin(&scope, "sysv_msg_receive", false, true);
    CHECK(
        mpmc_diagnostic_end(
            &scope, (mpmc_result_t){.status = MPMC_ERR_WOULD_BLOCK, .system_errno = ENOMSG}
        )
            .status
        == MPMC_ERR_WOULD_BLOCK
    );
    CHECK(count == before);
    mpmc_diagnostic_begin(&scope, "liberar", true, false);
    CHECK(
        mpmc_diagnostic_end(
            &scope, (mpmc_result_t){.status = MPMC_ERR_INTERRUPTED, .system_errno = EINTR}
        )
            .status
        == MPMC_ERR_INTERRUPTED
    );
    CHECK(count == before + 1);
}

/** @brief Redirecionamento não recebe ANSI e conserva as duas categorias de falha. */
static void test_presentation(void) {
    FILE *out = tmpfile();
    CHECK(out != nullptr);
    if (out == nullptr) return;
    app_diagnostics_t state = {.output = out};
    for (unsigned index = 0; index < 3; ++index) app_diagnostic_print(&errors[index], &state);
    rewind(out);
    char text[4096] = {};
    size_t size = fread(text, 1, sizeof text - 1, out);
    CHECK(size != 0);
    CHECK(strstr(text, "falha em criar_contexto") != nullptr);
    CHECK(strstr(text, "Ao finalizar/liberar recursos") != nullptr);
    CHECK(strstr(text, "semget()") != nullptr && strstr(text, "shmdt()") != nullptr);
    CHECK(strstr(text, "\033[") == nullptr);
    CHECK(fclose(out) == 0);
}
