#include "mpmc/ipc/msg_backend.h"

#include <stdio.h>
#include <sys/wait.h>
#include <unistd.h>

static int failed(const char *text) {
    fprintf(stderr, "%s\n", text);
    return 1;
}

int main(void) {
    msg_backend_t *parent = NULL;
    mpmc_result_t result = msg_backend_create(&parent, 2u);
    if (result.status != MPMC_OK || parent == NULL) {
        return failed("msg_backend_create falhou");
    }

    message_t first = {.sender_pid = getpid(), .sequence = 1u, .operation_id = 1u};
    message_t second = {.sender_pid = getpid(), .sequence = 2u, .operation_id = 1u};
    if (msg_backend_send(parent, 1u, &first).status != MPMC_OK
        || msg_backend_send(parent, 1u, &second).status != MPMC_OK) {
        msg_backend_destroy(parent);
        return failed("envio de mensagens falhou");
    }

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

    msg_backend_destroy(parent);
    puts("Message backend event tests passed.");
    return 0;
}
