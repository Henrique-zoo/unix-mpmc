/**
 * @file test_msg_backend.c
 * @brief Testa a consolidação de consumo e a política de ACK entre processos.
 * @ingroup tests
 * @details Os consumidores são criados antes do registro de ACK no produtor,
 * verificando que a solicitação chega pelo envelope e não pelo heap herdado.
 * As esperas de confirmação usam alarm() como limite de tempo.
 */
#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <sys/wait.h>
#include <unistd.h>

#include "mpmc/ipc/msg_backend.h"

static int failed(const char *text);
static void on_alarm(int signal_number);

/**
 * @brief Executa o cenário de dois consumidores e confirmações seletivas.
 * @return Zero se contagens e ACKs corresponderem ao esperado; um em falha.
 */
int main(void) {
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
    puts("Message backend event tests passed.");
    return 0;
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
