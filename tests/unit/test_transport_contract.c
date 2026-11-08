/**
 * @file test_transport_contract.c
 * @brief Verifica os retornos de transport.h que as implementações atuais honram.
 * @ingroup tests
 * @details Cobre a validação de argumentos, o modo 2 de ponta a ponta (dados, ACK e
 * eventos entre processos), além do ciclo de vida, dados e contagens do modo 1.
 * O protocolo de ACK do modo 1 continua pendente em #11.
 * Um alarm() limita a duração; ao expirar, o teste remove as filas e termina com falha.
 */
#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/wait.h>
#include <unistd.h>

#include "mpmc/ipc/transport.h"

/** @brief Quantidade de verificações executadas nesta suíte. */
static unsigned checks;
/** @brief Quantidade de verificações que registraram falha nesta suíte. */
static unsigned failures;
/** @brief Contexto do supervisor de teste, removido pelo tratador de alarme se houver travamento.
 */
static ipc_context_t *guarded_context;
/** @brief Filho de teste ainda vivo, encerrado pelo tratador de alarme. */
static volatile pid_t guarded_child;

/**
 * @brief Registra uma verificação e informa a falha sem interromper a suíte.
 * @param condition Expressão lógica avaliada uma única vez.
 */
#define CHECK(condition)                                                                           \
    do {                                                                                           \
        ++checks;                                                                                  \
        if (!(condition)) {                                                                        \
            ++failures;                                                                            \
            fprintf(stderr, "FALHA %s:%d: %s\n", __FILE__, __LINE__, #condition);                  \
        }                                                                                          \
    } while (0)

/**
 * @brief Verifica o status e a causa de sistema de um resultado.
 * @param result Resultado devolvido pela operação.
 * @param expected_status Status esperado.
 * @param expected_errno Causa de sistema esperada, ou zero.
 */
#define CHECK_RESULT(result, expected_status, expected_errno)                                      \
    do {                                                                                           \
        mpmc_result_t checked_result = (result);                                                   \
        CHECK(checked_result.status == (expected_status));                                         \
        CHECK(checked_result.system_errno == (expected_errno));                                    \
    } while (0)

/**
 * @brief Evita que um travamento deixe filas System V órfãs.
 * @param signal_number Número do sinal recebido.
 */
static void on_alarm(int signal_number) {
    (void)signal_number;
    if (guarded_child > 0) {
        (void)kill(guarded_child, SIGKILL);
    }
    ipc_destroy(guarded_context);
    _exit(2);
}

/** @brief Valida argumentos nulos e inválidos, comuns aos dois modos. */
static void test_argument_validation(void) {
    ipc_context_t *context = NULL;
    ipc_context_t *sentinel = (ipc_context_t *)&context;
    message_t message = {.sender_pid = 1, .sequence = 1u, .operation_id = 1u};
    uint64_t count = 77u;

    CHECK_RESULT(ipc_init(NULL, 1u, IPC_MESSAGE_QUEUE), MPMC_ERR_INVALID_ARG, 0);

    context = sentinel;
    CHECK_RESULT(ipc_init(&context, 0u, IPC_MESSAGE_QUEUE), MPMC_ERR_INVALID_ARG, 0);
    CHECK(context == sentinel);
    CHECK_RESULT(ipc_init(&context, 1u, (ipc_mode_t)0), MPMC_ERR_INVALID_ARG, 0);
    CHECK_RESULT(ipc_init(&context, 1u, (ipc_mode_t)3), MPMC_ERR_INVALID_ARG, 0);
    CHECK(context == sentinel);

    CHECK(ipc_producer_count(NULL) == 0u);
    CHECK_RESULT(ipc_send(NULL, 1u, &message), MPMC_ERR_INVALID_ARG, 0);
    CHECK_RESULT(ipc_receive(NULL, 1u, &message, false), MPMC_ERR_INVALID_ARG, 0);
    CHECK_RESULT(ipc_ack_begin(NULL, 1u, 1u), MPMC_ERR_INVALID_ARG, 0);
    CHECK_RESULT(ipc_ack_wait(NULL, 1u), MPMC_ERR_INVALID_ARG, 0);
    CHECK_RESULT(ipc_ack_end(NULL, 1u), MPMC_ERR_INVALID_ARG, 0);
    CHECK_RESULT(ipc_poll_events(NULL), MPMC_ERR_INVALID_ARG, 0);
    CHECK_RESULT(ipc_consumed_count(NULL, 1u, &count), MPMC_ERR_INVALID_ARG, 0);
    CHECK(count == 77u);
    ipc_worker_close(NULL);
    ipc_destroy(NULL);
}

/** @brief Valida, nos dois modos, os índices e o ciclo de vida que já são reais. */
static void test_lifecycle(ipc_mode_t mode) {
    ipc_context_t *context = NULL;
    CHECK_RESULT(ipc_init(&context, 2u, mode), MPMC_OK, 0);
    if (context == NULL) {
        return;
    }
    CHECK(ipc_producer_count(context) == 2u);
    CHECK_RESULT(ipc_poll_events(context), MPMC_OK, 0);

    uint64_t count = 77u;
    mpmc_status_t invalid_index =
        mode == IPC_SHARED_MEMORY ? MPMC_ERR_INVALID_INDEX : MPMC_ERR_INVALID_ARG;
    CHECK_RESULT(ipc_consumed_count(context, 0u, &count), invalid_index, 0);
    CHECK_RESULT(ipc_consumed_count(context, 3u, &count), invalid_index, 0);
    CHECK_RESULT(ipc_consumed_count(context, 1u, NULL), MPMC_ERR_INVALID_ARG, 0);
    CHECK(count == 77u);

    /* O fechamento local no filho não pode afetar o contexto do supervisor. */
    pid_t child = fork();
    if (child == 0) {
        ipc_worker_close(context);
        _exit(0);
    }
    CHECK(child > 0);
    guarded_child = child;
    int wait_status = 0;
    CHECK(child > 0 && waitpid(child, &wait_status, 0) == child);
    guarded_child = 0;
    CHECK(WIFEXITED(wait_status) && WEXITSTATUS(wait_status) == 0);
    CHECK_RESULT(ipc_poll_events(context), MPMC_OK, 0);
    CHECK(ipc_producer_count(context) == 2u);
    ipc_destroy(context);
}

/** @brief Valida argumentos e estados das operações de dados e de ACK no modo 2. */
static void test_queue_statuses(ipc_context_t *context) {
    message_t message = {.sender_pid = getpid(), .sequence = 1u, .operation_id = 5u};
    message_t received = {0};

    CHECK_RESULT(ipc_send(context, 0u, &message), MPMC_ERR_INVALID_ARG, 0);
    CHECK_RESULT(ipc_send(context, 3u, &message), MPMC_ERR_INVALID_ARG, 0);
    CHECK_RESULT(ipc_send(context, 1u, NULL), MPMC_ERR_INVALID_ARG, 0);
    CHECK_RESULT(ipc_receive(context, 0u, &received, false), MPMC_ERR_INVALID_ARG, 0);
    CHECK_RESULT(ipc_receive(context, 3u, &received, false), MPMC_ERR_INVALID_ARG, 0);
    CHECK_RESULT(ipc_receive(context, 1u, NULL, false), MPMC_ERR_INVALID_ARG, 0);
    CHECK_RESULT(ipc_receive(context, 1u, &received, false), MPMC_ERR_WOULD_BLOCK, ENOMSG);

    CHECK_RESULT(ipc_ack_begin(context, 0u, 1u), MPMC_ERR_INVALID_ARG, 0);
    CHECK_RESULT(ipc_ack_begin(context, 9u, 0u), MPMC_ERR_INVALID_ARG, 0);
    CHECK_RESULT(ipc_ack_begin(context, 9u, 1025u), MPMC_ERR_INVALID_ARG, 0);
    CHECK_RESULT(ipc_ack_wait(context, 0u), MPMC_ERR_INVALID_ARG, 0);
    CHECK_RESULT(ipc_ack_wait(context, 9u), MPMC_ERR_STATE, 0);
    CHECK_RESULT(ipc_ack_end(context, 0u), MPMC_ERR_INVALID_ARG, 0);
    CHECK_RESULT(ipc_ack_end(context, 9u), MPMC_OK, 0);

    CHECK_RESULT(ipc_ack_begin(context, 9u, 1u), MPMC_OK, 0);
    CHECK_RESULT(ipc_ack_begin(context, 9u, 1u), MPMC_ERR_STATE, 0);
    CHECK_RESULT(ipc_ack_end(context, 9u), MPMC_OK, 0);
    CHECK_RESULT(ipc_ack_end(context, 9u), MPMC_OK, 0);
}

/** @brief Valida dados, ACK por operação e contagem do supervisor sem bloqueio. */
static void test_queue_flow(ipc_context_t *context) {
    message_t sent = {
        .sender_pid = getpid(),
        .sequence = 4u,
        .sent_at = {.tv_sec = 12, .tv_nsec = 34},
        .operation_id = 7u,
    };
    message_t received = {0};

    /* Com ACK pedido: o ACK fica na fila e ack_wait não bloqueia. */
    CHECK_RESULT(ipc_ack_begin(context, 7u, 1u), MPMC_OK, 0);
    CHECK_RESULT(ipc_send(context, 2u, &sent), MPMC_OK, 0);
    CHECK_RESULT(ipc_receive(context, 1u, &received, false), MPMC_ERR_WOULD_BLOCK, ENOMSG);
    CHECK_RESULT(ipc_receive(context, 2u, &received, false), MPMC_OK, 0);
    CHECK(received.sender_pid == sent.sender_pid && received.sequence == sent.sequence);
    CHECK(received.sent_at.tv_sec == 12 && received.sent_at.tv_nsec == 34);
    CHECK(received.operation_id == 7u);
    CHECK_RESULT(ipc_ack_wait(context, 7u), MPMC_OK, 0);
    CHECK_RESULT(ipc_ack_end(context, 7u), MPMC_OK, 0);

    /* Sem ACK pedido: a retirada é contada, e nenhuma operação de ACK é necessária. */
    sent.operation_id = 8u;
    CHECK_RESULT(ipc_send(context, 1u, &sent), MPMC_OK, 0);
    CHECK_RESULT(ipc_receive(context, 1u, &received, true), MPMC_OK, 0);

    /* Um filho retira uma mensagem: o total chega por evento, e não pelo heap herdado. */
    sent.operation_id = 9u;
    CHECK_RESULT(ipc_send(context, 2u, &sent), MPMC_OK, 0);
    pid_t child = fork();
    if (child == 0) {
        message_t from_child;
        mpmc_result_t result = ipc_receive(context, 2u, &from_child, true);
        ipc_worker_close(context);
        _exit(result.status == MPMC_OK ? 0 : 1);
    }
    CHECK(child > 0);
    guarded_child = child;
    int wait_status = 0;
    CHECK(child > 0 && waitpid(child, &wait_status, 0) == child);
    guarded_child = 0;
    CHECK(WIFEXITED(wait_status) && WEXITSTATUS(wait_status) == 0);

    uint64_t first = 0u;
    uint64_t second = 0u;
    CHECK_RESULT(ipc_poll_events(context), MPMC_OK, 0);
    CHECK_RESULT(ipc_consumed_count(context, 1u, &first), MPMC_OK, 0);
    CHECK_RESULT(ipc_consumed_count(context, 2u, &second), MPMC_OK, 0);
    CHECK(first == 1u);
    CHECK(second == 2u);

    /* A drenagem repetida não conta de novo, e o contexto do pai continua utilizável. */
    CHECK_RESULT(ipc_poll_events(context), MPMC_OK, 0);
    CHECK_RESULT(ipc_consumed_count(context, 2u, &second), MPMC_OK, 0);
    CHECK(second == 2u);
    CHECK_RESULT(ipc_send(context, 1u, &sent), MPMC_OK, 0);
    CHECK_RESULT(ipc_receive(context, 1u, &received, false), MPMC_OK, 0);
}

/** @brief Exercita dados e totais reais de SHM pela fachada, sem depender dos ACKs pendentes. */
static void test_shared_memory_flow(ipc_context_t *context) {
    message_t sent = {.sender_pid = getpid(), .sequence = 7u, .operation_id = 41u};
    message_t received = {0};
    uint64_t count = 99;
    CHECK_RESULT(ipc_send(context, 0, &sent), MPMC_ERR_INVALID_INDEX, 0);
    CHECK_RESULT(ipc_receive(context, 3, &received, false), MPMC_ERR_INVALID_INDEX, 0);
    CHECK_RESULT(ipc_receive(context, 1, &received, false), MPMC_ERR_WOULD_BLOCK, EAGAIN);
    CHECK_RESULT(ipc_send(context, 2, &sent), MPMC_OK, 0);
    CHECK_RESULT(ipc_receive(context, 1, &received, false), MPMC_ERR_WOULD_BLOCK, EAGAIN);
    CHECK_RESULT(ipc_receive(context, 2, &received, true), MPMC_OK, 0);
    CHECK(received.sender_pid == sent.sender_pid && received.sequence == sent.sequence);
    CHECK(received.operation_id == sent.operation_id);
    CHECK_RESULT(ipc_consumed_count(context, 1, &count), MPMC_OK, 0);
    CHECK(count == 0);
    CHECK_RESULT(ipc_consumed_count(context, 2, &count), MPMC_OK, 0);
    CHECK(count == 1);
    CHECK_RESULT(ipc_ack_begin(context, sent.operation_id, 1), MPMC_ERR_STATE, 0);
}

/**
 * @brief Executa a suíte.
 * @return Zero se nenhuma verificação falhar; um se houver falha; dois se expirar o alarme.
 */
int main(void) {
    struct sigaction action = {0};
    action.sa_handler = on_alarm;
    sigemptyset(&action.sa_mask);
    if (sigaction(SIGALRM, &action, NULL) == -1) {
        perror("sigaction");
        return 1;
    }
    alarm(30u);

    test_argument_validation();
    test_lifecycle(IPC_MESSAGE_QUEUE);
    test_lifecycle(IPC_SHARED_MEMORY);

    mpmc_result_t result = ipc_init(&guarded_context, 2u, IPC_MESSAGE_QUEUE);
    CHECK_RESULT(result, MPMC_OK, 0);
    if (guarded_context != NULL) {
        test_queue_statuses(guarded_context);
        test_queue_flow(guarded_context);
        ipc_destroy(guarded_context);
        guarded_context = NULL;
    }
    CHECK_RESULT(ipc_init(&guarded_context, 2, IPC_SHARED_MEMORY), MPMC_OK, 0);
    if (guarded_context != NULL) {
        test_shared_memory_flow(guarded_context);
        ipc_destroy(guarded_context);
        guarded_context = NULL;
    }

    printf("%u verificações do contrato de transporte, %u falhas\n", checks, failures);
    return failures == 0u ? 0 : 1;
}
