/**
 * @file test_repl.c
 * @brief Verifica o REPL com um pseudo-terminal como stdin e stdout.
 * @ingroup tests
 *
 * @details Com stdout em terminal, o REPL redesenha o prompt `>>` depois do aviso
 * assíncrono de término de um filho. `test_repl.sh` usa pipes e não consegue cobrir esse
 * caso. Este teste abre um pseudo-terminal, executa o mpmc como líder de sessão com esse
 * terminal de controle, despacha um `produz` e confirma que o prompt reaparece depois do
 * aviso. Sem MPMC_TEST_WORKER_HANG o filho termina sozinho, pois o worker ainda não está
 * implementado.
 */

#define _XOPEN_SOURCE 700

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

/** @brief Prazo total, em milissegundos, para cada etapa do teste. */
#define STEP_TIMEOUT_MS 5000L

/** @brief Capacidade do buffer que acumula a saída do terminal. */
#define OUTPUT_CAPACITY 8192u

/** @brief Saída acumulada do mpmc, sempre terminada em NUL. */
static char output[OUTPUT_CAPACITY];
static size_t output_length;

static long now_ms(void) {
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    return (long)now.tv_sec * 1000L + now.tv_nsec / 1000000L;
}

/**
 * @brief Lê do terminal até `needle` aparecer em `output[from..]` ou o prazo acabar.
 * @param master Lado mestre do pseudo-terminal.
 * @param from Posição a partir da qual procurar, para ignorar o que já foi consumido.
 * @param needle Texto esperado.
 * @return Posição em que `needle` foi encontrado, ou -1 em prazo esgotado, EOF ou erro.
 */
static long wait_for(int master, size_t from, const char *needle) {
    const long deadline = now_ms() + STEP_TIMEOUT_MS;

    for (;;) {
        const char *found = strstr(output + from, needle);
        if (found != NULL) {
            return (long)(found - output);
        }
        long remaining = deadline - now_ms();
        if (remaining <= 0 || output_length + 1u >= sizeof output) {
            return -1;
        }

        struct pollfd fd = {.fd = master, .events = POLLIN, .revents = 0};
        int ready = poll(&fd, 1u, (int)remaining);
        if (ready == -1 && errno == EINTR) {
            continue;
        }
        if (ready <= 0) {
            return -1;
        }

        ssize_t bytes = read(master, output + output_length, sizeof output - output_length - 1u);
        if (bytes <= 0) {
            /* No Linux, ler do mestre depois que o filho fecha o escravo retorna EIO. */
            return strstr(output + from, needle) != NULL
                ? (long)(strstr(output + from, needle) - output)
                : -1;
        }
        output_length += (size_t)bytes;
        output[output_length] = '\0';
    }
}

static void send_text(int master, const char *text) {
    size_t length = strlen(text);
    if (write(master, text, length) != (ssize_t)length) {
        perror("write");
    }
}

/** @brief Encerra o mpmc, mostra a saída capturada e sai com erro. */
static void fail(pid_t child, const char *message) {
    fprintf(stderr, "FALHA: %s\n--- saída ---\n%s\n", message, output);
    kill(child, SIGKILL);
    waitpid(child, NULL, 0);
    exit(EXIT_FAILURE);
}

/** @brief Executa o mpmc no terminal escravo, como líder de sessão. */
static void run_child(const char *binary, const char *slave_path) {
    if (setsid() == -1) {
        _exit(126);
    }
    int slave = open(slave_path, O_RDWR);
    if (slave == -1 || ioctl(slave, TIOCSCTTY, 0) == -1) {
        _exit(126);
    }
    if (dup2(slave, STDIN_FILENO) == -1 || dup2(slave, STDOUT_FILENO) == -1
        || dup2(slave, STDERR_FILENO) == -1) {
        _exit(126);
    }
    if (slave > STDERR_FILENO) {
        close(slave);
    }
    execl(binary, binary, "2", "1", (char *)NULL);
    _exit(127);
}

int main(int argc, char *argv[]) {
    const char *binary = argc > 1 ? argv[1] : "./mpmc";

    int master = posix_openpt(O_RDWR | O_NOCTTY);
    if (master == -1 || grantpt(master) == -1 || unlockpt(master) == -1) {
        perror("pseudo-terminal");
        return EXIT_FAILURE;
    }
    const char *slave_path = ptsname(master);
    if (slave_path == NULL) {
        perror("ptsname");
        return EXIT_FAILURE;
    }

    pid_t child = fork();
    if (child == -1) {
        perror("fork");
        return EXIT_FAILURE;
    }
    if (child == 0) {
        close(master);
        run_child(binary, slave_path);
    }

    if (wait_for(master, 0u, ">> ") < 0) {
        fail(child, "prompt inicial não apareceu");
    }

    send_text(master, "produz 1 1 2\n");
    long notice = wait_for(master, 0u, "terminou");
    if (notice < 0) {
        fail(child, "aviso de término do filho não apareceu");
    }

    /* O aviso termina na quebra de linha; o prompt só conta se vier depois dela. */
    long line_end = wait_for(master, (size_t)notice, "\n");
    if (line_end < 0) {
        fail(child, "o aviso de término não foi concluído");
    }
    size_t after_notice = (size_t)line_end + 1u;
    if (wait_for(master, after_notice, ">> ") < 0) {
        fail(child, "o prompt não foi redesenhado após o aviso assíncrono");
    }

    send_text(master, "shutdown\n");
    if (wait_for(master, 0u, "Tempo total") < 0) {
        fail(child, "relatório final não apareceu após shutdown");
    }

    int status = 0;
    const long deadline = now_ms() + STEP_TIMEOUT_MS;
    pid_t reaped;
    while ((reaped = waitpid(child, &status, WNOHANG)) == 0 && now_ms() < deadline) {
        nanosleep(&(struct timespec){.tv_nsec = 10L * 1000L * 1000L}, NULL);
    }
    if (reaped != child) {
        fail(child, "mpmc não terminou após shutdown");
    }
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        fprintf(stderr, "FALHA: mpmc terminou com status anormal (%d)\n%s\n", status, output);
        return EXIT_FAILURE;
    }

    close(master);
    puts("REPL pty tests passed.");
    return EXIT_SUCCESS;
}
