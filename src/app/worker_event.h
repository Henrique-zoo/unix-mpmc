/**
 * @file worker_event.h
 * @brief Registros privados transmitidos pelos pipes dos workers.
 * @ingroup application
 */
#ifndef MPMC_WORKER_EVENT_H
#define MPMC_WORKER_EVENT_H

#include "job_outcome.h"
#include "mpmc/diagnostic.h"

/** @brief Discriminante do registro, sempre seguido do membro correspondente. */
typedef enum {
    WORKER_EVENT_DIAGNOSTIC, /**< Falha individual, antes do resultado final. */
    WORKER_EVENT_OUTCOME,    /**< Resultado final; último registro do canal. */
} worker_event_kind_t;

/** @brief Frame sem ponteiros, escrito atomicamente por seu único produtor. */
typedef struct {
    worker_event_kind_t kind; /**< Discriminante da união. */
    union {
        mpmc_diagnostic_t diagnostic; /**< Erro copiável e contextualizado. */
        job_outcome_t outcome;        /**< Resultado consolidado. */
    }; /**< Conteúdo correspondente a kind. */
} worker_event_t;

static_assert(sizeof(worker_event_t) <= 512, "frame deve caber no PIPE_BUF mínimo POSIX");

#endif
