/**
 * @file test_msg_backend.c
 * @brief Testa a consolidação de consumo, a política de ACK entre processos e as filas de dados.
 * @ingroup tests
 * @details Os consumidores são criados antes do registro de ACK no produtor,
 * verificando que a solicitação chega pelo envelope e não pelo heap herdado.
 * As esperas de confirmação usam alarm() como limite de tempo. Verifica o isolamento
 * entre filas, a ordem FIFO e o recebimento não bloqueante. Também testa a
 * consolidação de eventos e a política de ACK entre processos.
 */
#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <sys/wait.h>
#include <unistd.h>

#include "mpmc/ipc/msg_backend.h"

static int failed(const char *text);
static void on_alarm(int signal_number);
static void test_arguments(void);
static void test_queue_isolation(void);
static void test_fifo(void);
static void test_nonblocking_receive(void);
static void check_message(const message_t *received, const message_t *expected);

/**
 * @brief Quantidade de verificações executadas nesta suíte.
 */
static unsigned checks;

/**
 * @brief Quantidade de verificações que registraram falha nesta suíte.
 */
static unsigned failures;

/**
 * @brief Registra uma verificação sem interromper a suíte.
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
 * @brief Executa uma operação e confere status e system_errno.
 * @param call Expressão que retorna mpmc_result_t.
 * @param expected_status Categoria de resultado esperada.
 * @param expected_errno Valor esperado para o errno armazenado no resultado.
 */
#define CHECK_RESULT(call, expected_status, expected_errno)                                        \
    do {                                                                                           \
        mpmc_result_t result_ = (call);                                                            \
        CHECK(result_.status == (expected_status));                                                \
        CHECK(result_.system_errno == (expected_errno));                                           \
    } while (0)

/**
 * @brief Confere argumentos inválidos do backend.
 */
static void test_arguments(void) {
    msg_backend_t *backend = NULL;
    message_t message = {
        .sender_pid = getpid(),
        .sequence = 1u,
        .operation_id = 1u,
    };
    message_t received = {0};

    CHECK_RESULT(msg_backend_create(NULL, 2u), MPMC_ERR_INVALID_ARG, 0);
    CHECK_RESULT(msg_backend_create(&backend, 0u), MPMC_ERR_INVALID_ARG, 0);
    CHECK(backend == NULL);

    CHECK_RESULT(msg_backend_send(NULL, 1u, &message), MPMC_ERR_INVALID_ARG, 0);

    CHECK_RESULT(msg_backend_receive(NULL, 1u, &received, false), MPMC_ERR_INVALID_ARG, 0);

    CHECK_RESULT(msg_backend_receive(NULL, 1u, NULL, false), MPMC_ERR_INVALID_ARG, 0);
}

/**
 * @brief Verifica que cada índice lógico acessa somente sua própria fila.
 */
static void test_queue_isolation(void) {
    msg_backend_t *backend = NULL;
    mpmc_result_t result = msg_backend_create(&backend, 2u);

    CHECK(result.status == MPMC_OK);
    CHECK(result.system_errno == 0);
    CHECK(backend != NULL);

    if (result.status != MPMC_OK || backend == NULL) return;

    message_t first = {
        .sender_pid = getpid(),
        .sequence = 1u,
        .sent_at = {.tv_sec = 10, .tv_nsec = 100},
        .operation_id = 10u,
    };

    message_t second = {
        .sender_pid = getpid(),
        .sequence = 2u,
        .sent_at = {.tv_sec = 20, .tv_nsec = 200},
        .operation_id = 20u,
    };

    message_t received = {0};

    /* Os índices válidos devem estar no intervalo 1..producer_count. */
    CHECK_RESULT(msg_backend_send(backend, 0u, &first), MPMC_ERR_INVALID_ARG, 0);
    CHECK_RESULT(msg_backend_send(backend, 3u, &first), MPMC_ERR_INVALID_ARG, 0);

    CHECK_RESULT(msg_backend_receive(backend, 0u, &received, false), MPMC_ERR_INVALID_ARG, 0);
    CHECK_RESULT(msg_backend_receive(backend, 3u, &received, false), MPMC_ERR_INVALID_ARG, 0);

    /* Ponteiros nulos devem ser rejeitados. */
    CHECK_RESULT(msg_backend_send(backend, 1u, NULL), MPMC_ERR_INVALID_ARG, 0);
    CHECK_RESULT(msg_backend_receive(backend, 1u, NULL, false), MPMC_ERR_INVALID_ARG, 0);

    /* A mensagem enviada à fila 1 não deve aparecer na fila 2. */
    CHECK_RESULT(msg_backend_send(backend, 1u, &first), MPMC_OK, 0);

    CHECK_RESULT(msg_backend_receive(backend, 2u, &received, false), MPMC_ERR_WOULD_BLOCK, ENOMSG);

    /* A mensagem deve continuar disponível na fila 1. */
    result = msg_backend_receive(backend, 1u, &received, false);
    CHECK(result.status == MPMC_OK);
    CHECK(result.system_errno == 0);

    if (result.status == MPMC_OK) {
        check_message(&received, &first);
    }

    /* A mensagem enviada à fila 2 não deve aparecer na fila 1. */
    CHECK_RESULT(msg_backend_send(backend, 2u, &second), MPMC_OK, 0);

    CHECK_RESULT(msg_backend_receive(backend, 1u, &received, false), MPMC_ERR_WOULD_BLOCK, ENOMSG);

    /* A mensagem deve estar disponível na fila 2. */
    result = msg_backend_receive(backend, 2u, &received, false);
    CHECK(result.status == MPMC_OK);
    CHECK(result.system_errno == 0);

    if (result.status == MPMC_OK) {
        check_message(&received, &second);
    }

    msg_backend_destroy(backend);
}

/**
 * @brief Verifica que mensagens com diferentes tipos preservam a ordem FIFO.
 * @details A primeira mensagem utiliza mtype 1 e a segunda utiliza mtype 2,
 * pois somente a segunda operação possui registro de ACK.
 */
static void test_fifo(void) {
    msg_backend_t *backend = NULL;
    mpmc_result_t result = msg_backend_create(&backend, 1u);

    CHECK(result.status == MPMC_OK);
    CHECK(result.system_errno == 0);
    CHECK(backend != NULL);

    if (result.status != MPMC_OK || backend == NULL) return;

    message_t first = {
        .sender_pid = getpid(),
        .sequence = 1u,
        .sent_at = {.tv_sec = 30, .tv_nsec = 100},
        .operation_id = 30u,
    };

    message_t second = {
        .sender_pid = getpid(),
        .sequence = 2u,
        .sent_at = {.tv_sec = 31, .tv_nsec = 200},
        .operation_id = 31u,
    };

    message_t received = {0};

    /* Somente a segunda operação deve solicitar ACK. */
    result = msg_backend_ack_begin(backend, 31u, 1u);
    CHECK(result.status == MPMC_OK);
    CHECK(result.system_errno == 0);

    if (result.status != MPMC_OK) {
        msg_backend_destroy(backend);
        return;
    }

    result = msg_backend_send(backend, 1u, &first);
    CHECK(result.status == MPMC_OK);
    CHECK(result.system_errno == 0);

    if (result.status != MPMC_OK) {
        (void)msg_backend_ack_end(backend, 31u);
        msg_backend_destroy(backend);
        return;
    }

    result = msg_backend_send(backend, 1u, &second);
    CHECK(result.status == MPMC_OK);
    CHECK(result.system_errno == 0);

    if (result.status != MPMC_OK) {
        (void)msg_backend_ack_end(backend, 31u);
        msg_backend_destroy(backend);
        return;
    }

    /* A primeira mensagem deve ser recebida primeiro. */
    result = msg_backend_receive(backend, 1u, &received, false);
    CHECK(result.status == MPMC_OK);
    CHECK(result.system_errno == 0);

    if (result.status != MPMC_OK) {
        (void)msg_backend_ack_end(backend, 31u);
        msg_backend_destroy(backend);
        return;
    }

    check_message(&received, &first);

    /* A segunda mensagem deve ser recebida em seguida. */
    result = msg_backend_receive(backend, 1u, &received, false);
    CHECK(result.status == MPMC_OK);
    CHECK(result.system_errno == 0);

    if (result.status != MPMC_OK) {
        (void)msg_backend_ack_end(backend, 31u);
        msg_backend_destroy(backend);
        return;
    }

    /*
     * A espera por ACK só é executada quando a segunda mensagem foi
     * efetivamente recebida e corresponde à operação registrada.
     */
    check_message(&received, &second);

    struct sigaction action = {.sa_handler = on_alarm};
    struct sigaction previous;
    sigemptyset(&action.sa_mask);

    int rc = sigaction(SIGALRM, &action, &previous);
    CHECK(rc == 0);

    if (rc == 0) {
        alarm(2);
        result = msg_backend_ack_wait(backend, 31u);
        alarm(0);

        CHECK(result.status == MPMC_OK);
        CHECK(result.system_errno == 0);

        CHECK(sigaction(SIGALRM, &previous, NULL) == 0);
    }

    CHECK_RESULT(msg_backend_ack_end(backend, 31u), MPMC_OK, 0);

    msg_backend_destroy(backend);
}

/**
 * @brief Verifica que o recebimento não bloqueante não espera por mensagens.
 */
static void test_nonblocking_receive(void) {
    msg_backend_t *backend = NULL;
    mpmc_result_t result = msg_backend_create(&backend, 1u);

    CHECK(result.status == MPMC_OK);
    CHECK(result.system_errno == 0);
    CHECK(backend != NULL);

    if (result.status != MPMC_OK || backend == NULL) return;

    message_t received = {0};
    message_t message = {
        .sender_pid = getpid(),
        .sequence = 1u,
        .sent_at = {.tv_sec = 40, .tv_nsec = 100},
        .operation_id = 40u,
    };

    /* A fila vazia deve retornar WOULD_BLOCK imediatamente. */
    CHECK_RESULT(msg_backend_receive(backend, 1u, &received, false), MPMC_ERR_WOULD_BLOCK, ENOMSG);

    CHECK_RESULT(msg_backend_send(backend, 1u, &message), MPMC_OK, 0);

    result = msg_backend_receive(backend, 1u, &received, false);
    CHECK(result.status == MPMC_OK);
    CHECK(result.system_errno == 0);

    if (result.status == MPMC_OK) {
        check_message(&received, &message);
    }

    /* Depois do consumo, a fila deve estar vazia novamente. */
    CHECK_RESULT(msg_backend_receive(backend, 1u, &received, false), MPMC_ERR_WOULD_BLOCK, ENOMSG);

    msg_backend_destroy(backend);
}

/**
 * @brief Confere todos os campos de uma mensagem.
 * @param received Mensagem recebida.
 * @param expected Mensagem esperada.
 * @details Compara os campos individualmente, sem considerar padding.
 */
static void check_message(const message_t *received, const message_t *expected) {
    CHECK(received->sender_pid == expected->sender_pid);
    CHECK(received->sequence == expected->sequence);
    CHECK(received->sent_at.tv_sec == expected->sent_at.tv_sec);
    CHECK(received->sent_at.tv_nsec == expected->sent_at.tv_nsec);
    CHECK(received->operation_id == expected->operation_id);
}

/**
 * @brief Executa os testes das filas de dados, cenário de dois consumidores e confirmações
 * seletivas.
 * @return Zero se todos os cenários passarem; um se falha.
 */
int main(void) {
    test_arguments();
    test_queue_isolation();
    test_fifo();
    test_nonblocking_receive();

    msg_backend_t *parent = NULL;
    mpmc_result_t result = msg_backend_create(&parent, 2u);
    if (result.status != MPMC_OK || parent == NULL) {
        return failed("msg_backend_create falhou");
    }

    message_t first = {.sender_pid = getpid(), .sequence = 1u, .operation_id = 1u};
    message_t second = {.sender_pid = getpid(), .sequence = 2u, .operation_id = 2u};

    pid_t child_one = fork();
    if (child_one == -1) {
        msg_backend_destroy(parent);
        return failed("primeiro fork falhou");
    }
    if (child_one == 0) {
        message_t received;
        result = msg_backend_receive(parent, 1u, &received, true);
        msg_backend_worker_close(parent);
        _exit(result.status == MPMC_OK ? 0 : 1);
    }

    pid_t child_two = fork();
    if (child_two == -1) {
        (void)kill(child_one, SIGTERM);
        (void)waitpid(child_one, NULL, 0);
        msg_backend_destroy(parent);
        return failed("segundo fork falhou");
    }
    if (child_two == 0) {
        message_t received;
        result = msg_backend_receive(parent, 1u, &received, true);
        msg_backend_worker_close(parent);
        _exit(result.status == MPMC_OK ? 0 : 1);
    }

    /* Os consumidores ja existem quando o produtor registra sua operacao de ACK. */
    if (msg_backend_ack_begin(parent, 2u, 1u).status != MPMC_OK
        || msg_backend_send(parent, 1u, &first).status != MPMC_OK
        || msg_backend_send(parent, 1u, &second).status != MPMC_OK) {
        (void)kill(child_one, SIGTERM);
        (void)kill(child_two, SIGTERM);
        (void)waitpid(child_one, NULL, 0);
        (void)waitpid(child_two, NULL, 0);
        msg_backend_destroy(parent);
        return failed("registro de ACK ou envio de mensagens falhou");
    }

    int status;
    for (size_t index = 0u; index < 2u; ++index) {
        if (wait(&status) == -1 || !WIFEXITED(status) || WEXITSTATUS(status) != 0) {
            msg_backend_destroy(parent);
            return failed("consumidor filho falhou ou evento nao foi publicado");
        }
    }

    if (msg_backend_poll_events(parent).status != MPMC_OK) {
        msg_backend_destroy(parent);
        return failed("msg_backend_poll_events falhou");
    }
    if (msg_backend_poll_events(parent).status != MPMC_OK) {
        msg_backend_destroy(parent);
        return failed("segunda drenagem falhou");
    }

    uint64_t count = UINT64_MAX;
    result = msg_backend_consumed_count(parent, 1u, &count);
    if (result.status != MPMC_OK || count != 2u) {
        msg_backend_destroy(parent);
        return failed("pai nao consolidou o total de dois consumidores distintos");
    }

    result = msg_backend_consumed_count(parent, 2u, &count);
    if (result.status != MPMC_OK || count != 0u) {
        msg_backend_destroy(parent);
        return failed("contador de estrutura independente falhou");
    }

    struct sigaction action = {.sa_handler = on_alarm};
    struct sigaction previous;
    sigemptyset(&action.sa_mask);
    if (sigaction(SIGALRM, &action, &previous) == -1) {
        msg_backend_destroy(parent);
        return failed("instalacao do timeout de ACK falhou");
    }
    alarm(2);
    result = msg_backend_ack_wait(parent, 2u);
    alarm(0);
    bool confirmed = result.status == MPMC_OK;
    bool ignored = false;
    if (confirmed && msg_backend_ack_begin(parent, 1u, 1u).status == MPMC_OK) {
        alarm(1);
        result = msg_backend_ack_wait(parent, 1u);
        alarm(0);
        ignored = result.status == MPMC_ERR_INTERRUPTED && result.system_errno == EINTR;
    }
    (void)sigaction(SIGALRM, &previous, NULL);
    msg_backend_ack_end(parent, 2u);
    msg_backend_ack_end(parent, 1u);
    if (!confirmed || !ignored) {
        msg_backend_destroy(parent);
        return failed("ACK ausente ou emitido para operacao sem registro");
    }

    msg_backend_destroy(parent);
    printf("%u verificações adicionais de filas, %u falhas\n", checks, failures);
    puts("Message backend event tests passed.");
    return failures == 0u ? 0 : 1;
}

/**
 * @brief Escreve o diagnóstico do cenário que falhou.
 * @param text Mensagem a apresentar em stderr.
 * @return Um, para indicar falha no processo de teste.
 */
static int failed(const char *text) {
    fprintf(stderr, "%s\n", text);
    return 1;
}

/**
 * @brief Permite interromper a espera por ACK no prazo do teste.
 * @param signal_number Sinal recebido; ignorado.
 */
static void on_alarm(int signal_number) {
    (void)signal_number;
}
