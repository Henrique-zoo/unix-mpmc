/**
 * @file test_message_metadata.c
 * @brief Testes da construção dos metadados de mensagens no produtor.
 * @ingroup tests
 */
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "mpmc/workers/producer.h"

/**
 * @brief Verifica PID, sequência, política de ACK e timestamp dos metadados.
 * @return Zero se todos os cenários passarem; um na primeira inconsistência,
 * após escrever o diagnóstico em stderr.
 * @details Exercita a sequência de um único emissor, rejeição de operation_id
 * nulo sem alterar a saída e captura do timestamp no momento de construir cada mensagem.
 * Não executa envio por transporte nem verifica o fluxo completo de um worker.
 */
int main(void) {
    producer_sequence_t sequence = {0};
    producer_job_t job = {
        .producer_id = 0u,
        .message_count = 3u,
        .ack_mode = PRODUCER_ACK_WAIT,
        .operation_id = 42u,
    };
    message_t messages[3];

    for (uint32_t index = 0u; index < 3u; ++index) {
        if (producer_message_next(&messages[index], &sequence, &job) != MPMC_OK
            || messages[index].sender_pid != getpid() || messages[index].sequence != index + 1u
            || messages[index].operation_id != job.operation_id || !messages[index].ack_required
            || messages[index].sent_at.tv_sec <= 0 || messages[index].sent_at.tv_nsec < 0
            || messages[index].sent_at.tv_nsec >= 1000000000L) {
            fputs("message metadata test failed\n", stderr);
            return 1;
        }
    }

    producer_job_t no_ack = job;
    no_ack.ack_mode = PRODUCER_ACK_NONE;
    message_t message;
    if (producer_message_next(&message, &sequence, &no_ack) != MPMC_OK || message.sequence != 4u
        || message.ack_required) {
        fputs("ack metadata test failed\n", stderr);
        return 1;
    }

    message_t unchanged;
    memset(&unchanged, 0xA5, sizeof unchanged);
    message_t before = unchanged;
    producer_job_t invalid = job;
    invalid.operation_id = 0u;
    if (producer_message_next(&unchanged, &sequence, &invalid) != MPMC_ERR_INVALID_ARG
        || memcmp(&unchanged, &before, sizeof unchanged) != 0) {
        fputs("invalid operation_id test failed\n", stderr);
        return 1;
    }

    {
        producer_sequence_t timed_sequence = {0};
        struct timespec created;
        struct timespec pause = {.tv_sec = 0, .tv_nsec = 20L * 1000L * 1000L};
        message_t first;
        message_t second;
        struct timespec window_start;
        struct timespec window_end;

        clock_gettime(CLOCK_REALTIME, &created);
        nanosleep(&pause, NULL);

        clock_gettime(CLOCK_REALTIME, &window_start);
        if (producer_message_next(&first, &timed_sequence, &job) != MPMC_OK) {
            fputs("sent_at timing test failed (primeira)\n", stderr);
            return 1;
        }
        nanosleep(&pause, NULL);
        if (producer_message_next(&second, &timed_sequence, &job) != MPMC_OK) {
            fputs("sent_at timing test failed (segunda)\n", stderr);
            return 1;
        }
        clock_gettime(CLOCK_REALTIME, &window_end);

        long long created_ns = (long long)created.tv_sec * 1000000000LL + created.tv_nsec;
        long long start_ns = (long long)window_start.tv_sec * 1000000000LL + window_start.tv_nsec;
        long long end_ns = (long long)window_end.tv_sec * 1000000000LL + window_end.tv_nsec;
        long long first_ns = (long long)first.sent_at.tv_sec * 1000000000LL + first.sent_at.tv_nsec;
        long long second_ns =
            (long long)second.sent_at.tv_sec * 1000000000LL + second.sent_at.tv_nsec;

        if (first_ns < start_ns || first_ns - created_ns < 20LL * 1000LL * 1000LL
            || second_ns > end_ns || second_ns - first_ns < 20LL * 1000LL * 1000LL) {
            fputs("sent_at não representa o instante do envio\n", stderr);
            return 1;
        }
    }

    puts("Message metadata tests passed.");
    return 0;
}
