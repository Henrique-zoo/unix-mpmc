/**
 * @file job_outcome.c
 * @brief Texto do diagnóstico contextual de resultados de trabalhos.
 * @ingroup application
 * @see job_outcome.h para o contrato.
 */
#include "job_outcome.h"

#include <inttypes.h>
#include <stdio.h>

bool job_outcome_is_success(const job_outcome_t *outcome) {
    return outcome != nullptr && outcome->kind == JOB_OUTCOME_COMPLETED
        && outcome->effective == outcome->requested;
}

static bool errno_is_valid(mpmc_result_t error) {
    return error.status != MPMC_OK && error.system_errno >= 1
        && error.system_errno <= JOB_OUTCOME_ERRNO_MAX;
}

/** @brief Causa em texto; o errno só é interpretado quando válido. */
static void format_cause(mpmc_result_t error, char *buffer, size_t capacity) {
    if (error.status == MPMC_OK) {
        (void)snprintf(buffer, capacity, "causa não informada");
        return;
    }
    if (!errno_is_valid(error)) {
        error.system_errno = 0;
    }
    (void)mpmc_error_format(error, buffer, capacity);
}

int job_outcome_format(const job_outcome_t *outcome, char *buffer, size_t capacity) {
    if (outcome == nullptr || (buffer == nullptr && capacity > 0)) {
        return -1;
    }

    job_outcome_kind_t kind = outcome->kind;
    if (kind == JOB_OUTCOME_COMPLETED && outcome->effective != outcome->requested) {
        kind = JOB_OUTCOME_PARTIAL;
    }

    const char *verb = outcome->command == COMMAND_PRODUCE ? "produz" : "consome";
    const char *done = outcome->command == COMMAND_PRODUCE ? "produzidas" : "consumidas";

    char structure[48];
    if (outcome->producer_id == 0) {
        (void)snprintf(structure, sizeof structure, "todas as estruturas");
    } else {
        (void)snprintf(structure, sizeof structure, "estrutura %zu", outcome->producer_id);
    }

    char subject[128];
    if (outcome->operation_id == 0) {
        (void)snprintf(subject, sizeof subject, "comando (%s, %s)", verb, structure);
    } else {
        (void)snprintf(
            subject,
            sizeof subject,
            "operação %" PRIu64 " (%s, %s)",
            outcome->operation_id,
            verb,
            structure
        );
    }

    char cause[256];

    switch (kind) {
        case JOB_OUTCOME_COMPLETED:
            return snprintf(
                buffer,
                capacity,
                "%s concluída: %" PRIu64 " de %" PRIu64 " mensagens %s.",
                subject,
                outcome->effective,
                outcome->requested,
                done
            );
        case JOB_OUTCOME_PARTIAL:
            return snprintf(
                buffer,
                capacity,
                "%s não foi concluída: apenas %" PRIu64 " de %" PRIu64 " mensagens %s.",
                subject,
                outcome->effective,
                outcome->requested,
                done
            );
        case JOB_OUTCOME_CANCELLED:
            return snprintf(
                buffer,
                capacity,
                "%s cancelada antes de terminar: %" PRIu64 " de %" PRIu64 " mensagens %s.",
                subject,
                outcome->effective,
                outcome->requested,
                done
            );
        case JOB_OUTCOME_KERNEL_ERROR:
            format_cause(outcome->error, cause, sizeof cause);
            return snprintf(
                buffer,
                capacity,
                "%s falhou após %" PRIu64 " de %" PRIu64 " mensagens %s: %s",
                subject,
                outcome->effective,
                outcome->requested,
                done,
                cause
            );
        case JOB_OUTCOME_SPAWN_FAILED:
            format_cause(outcome->error, cause, sizeof cause);
            return snprintf(
                buffer, capacity, "%s: não foi possível criar o processo filho: %s", subject, cause
            );
    }
    return -1;
}
