/** @file diagnostics.c
 * @brief Apresentação incremental de operações e cleanup, sem descartar erros.
 * @ingroup application
 */
#include "diagnostics.h"

#include <inttypes.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

void app_diagnostic_print(const mpmc_diagnostic_t *error, void *user_data) {
    app_diagnostics_t *state = user_data;
    FILE *out = state->output;
    const char *term = getenv("TERM");
    bool color = isatty(fileno(out)) && getenv("NO_COLOR") == nullptr
        && (term == nullptr || strcmp(term, "dumb") != 0);
    const char *red = color ? "\033[31m" : "";
    const char *reset = color ? "\033[0m" : "";
    bool new_group =
        !state->started || state->group != error->group || state->process_pid != error->process_pid;
    if (new_group) {
        fprintf(out, "%smpmc: falha em %s", red, error->function);
        if (error->operation_id != 0) {
            fprintf(
                out,
                " (operação %" PRIu64 ", processo %ld, ",
                error->operation_id,
                (long)error->process_pid
            );
            if (error->producer_id == 0)
                fputs("todas as estruturas)", out);
            else
                fprintf(out, "estrutura %zu)", error->producer_id);
        }
        fprintf(out, ".%s\n", reset);
    }
    if (new_group || state->phase != error->phase) {
        fprintf(
            out,
            "%s  %s:%s\n",
            red,
            error->phase == MPMC_DIAGNOSTIC_CLEANUP
                ? "Ao finalizar/liberar recursos, ocorreram os seguintes erros"
                : "Ocorreram os seguintes erros na operação",
            reset
        );
    }
    char text[256];
    (void)mpmc_error_format(error->result, text, sizeof text);
    fprintf(out, "%s    - %s", red, error->operation);
    if (error->resource_id >= 0) fprintf(out, " [recurso %d]", error->resource_id);
    fprintf(out, ": %s%s\n", text, reset);
    state->process_pid = error->process_pid;
    state->group = error->group;
    state->phase = error->phase;
    state->started = true;
}
