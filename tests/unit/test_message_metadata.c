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

/* producer_run() is not exercised here; these doubles keep the metadata test
 * independent of the transport/backend implementations. */
size_t ipc_producer_count(const ipc_context_t *context) {
    (void)context;
    return 0;
}

mpmc_result_t ipc_send(ipc_context_t *context, size_t producer_id, const message_t *message) {
    (void)context;
    (void)producer_id;
    (void)message;
    return (mpmc_result_t){.status = MPMC_ERR_STATE};
}

mpmc_result_t ipc_ack_begin(ipc_context_t *context, uint64_t operation_id, unsigned expected) {
    (void)context;
    (void)operation_id;
    (void)expected;
    return (mpmc_result_t){.status = MPMC_ERR_STATE};
}

mpmc_result_t ipc_ack_wait(ipc_context_t *context, uint64_t operation_id) {
    (void)context;
    (void)operation_id;
    return (mpmc_result_t){.status = MPMC_ERR_STATE};
}

mpmc_result_t ipc_ack_end(ipc_context_t *context, uint64_t operation_id) {
    (void)context;
    (void)operation_id;
    return (mpmc_result_t){.status = MPMC_ERR_STATE};
}

/**
 * @brief Verifica PID, sequência, operação e timestamp dos metadados.
 * @return Zero se todos os cenários passarem; um na primeira inconsistência,
 * após escrever o diagnóstico em stderr.
 * @details Exercita a sequência de um único emissor nos dois modos de ACK,
 * preservando o operation_id também sem espera por confirmação. Verifica rejeição
 * de operation_id nulo sem alterar a saída e captura do timestamp por mensagem.
 * Não executa envio por transporte nem verifica o fluxo completo de um worker.
 */
int main(void) {
    producer_sequence_t sequence = {};
    producer_job_t job = {
        .producer_id = 0,
        .message_count = 3,
        .ack_mode = PRODUCER_ACK_WAIT,
        .operation_id = 42,
    };
    message_t messages[3];

    for (uint32_t index = 0; index < 3; ++index) {
        if (producer_message_next(&messages[index], &sequence, &job) != MPMC_OK
            || messages[index].sender_pid != getpid() || messages[index].sequence != index + 1
            || messages[index].operation_id != job.operation_id
            || messages[index].sent_at.tv_sec <= 0 || messages[index].sent_at.tv_nsec < 0
            || messages[index].sent_at.tv_nsec >= 1000000000L) {
            fputs("message metadata test failed\n", stderr);
            return 1;
        }
    }

    producer_job_t no_ack = job;
    no_ack.ack_mode = PRODUCER_ACK_NONE;
    no_ack.operation_id = 43;
    message_t message;
    if (producer_message_next(&message, &sequence, &no_ack) != MPMC_OK || message.sequence != 4
        || message.sender_pid != getpid() || message.operation_id != no_ack.operation_id) {
        fputs("metadata test failed without ACK wait\n", stderr);
        return 1;
    }

    message_t unchanged;
    memset(&unchanged, 0xA5, sizeof unchanged);
    message_t before = unchanged;
    producer_job_t invalid = job;
    invalid.operation_id = 0;
    if (producer_message_next(&unchanged, &sequence, &invalid) != MPMC_ERR_INVALID_ARG
        || memcmp(&unchanged, &before, sizeof unchanged) != 0) {
        fputs("invalid operation_id test failed\n", stderr);
        return 1;
    }

    {
        producer_sequence_t timed_sequence = {};
        struct timespec created;
        struct timespec pause = {.tv_sec = 0, .tv_nsec = 20L * 1000L * 1000L};
        message_t first;
        message_t second;
        struct timespec window_start;
        struct timespec window_end;

        clock_gettime(CLOCK_REALTIME, &created);
        nanosleep(&pause, nullptr);

        clock_gettime(CLOCK_REALTIME, &window_start);
        if (producer_message_next(&first, &timed_sequence, &job) != MPMC_OK) {
            fputs("sent_at timing test failed (primeira)\n", stderr);
            return 1;
        }
        nanosleep(&pause, nullptr);
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
