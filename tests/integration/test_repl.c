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

/** @brief Habilita as interfaces XSI usadas para criar o pseudo-terminal. */
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
/** @brief Quantidade de bytes acumulados em output, sem o terminador nulo. */
static size_t output_length;

/**
 * @brief Converte o relógio monotônico para milissegundos inteiros.
 * @return Instante de CLOCK_MONOTONIC, em milissegundos, truncando a fração restante.
 * @pre clock_gettime(CLOCK_MONOTONIC) deve concluir com sucesso neste ambiente de teste.
 * @note Este auxiliar não verifica nem propaga falhas da consulta ao relógio.
 */
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
 * @return Posição absoluta de needle no buffer, ou -1 em prazo esgotado,
 * falta de espaço no buffer, EOF ou falha de poll()/read().
 * @pre from deve pertencer ao intervalo de zero a output_length; needle deve ser
 * uma string válida terminada em NUL e master deve identificar o terminal aberto.
 * @details Acumula a saída em output, preservando o terminador NUL. Repete poll()
 * em EINTR; falhas de read() encerram a tentativa sem propagar errno separadamente.
 */
static long wait_for(int master, size_t from, const char *needle) {
    const long deadline = now_ms() + STEP_TIMEOUT_MS;

    for (;;) {
        const char *found = strstr(output + from, needle);
        if (found != nullptr) {
            return (long)(found - output);
        }
        long remaining = deadline - now_ms();
        if (remaining <= 0 || output_length + 1 >= sizeof output) {
            return -1;
        }

        struct pollfd fd = {.fd = master, .events = POLLIN, .revents = 0};
        int ready = poll(&fd, 1, (int)remaining);
        if (ready == -1 && errno == EINTR) {
            continue;
        }
        if (ready <= 0) {
            return -1;
        }

        ssize_t bytes = read(master, output + output_length, sizeof output - output_length - 1);
        if (bytes <= 0) {
            /* No Linux, ler do mestre depois que o filho fecha o escravo retorna EIO. */
            return strstr(output + from, needle) != nullptr
                ? (long)(strstr(output + from, needle) - output)
                : -1;
        }
        output_length += (size_t)bytes;
        output[output_length] = '\0';
    }
}

/**
 * @brief Tenta escrever o comando no lado mestre do pseudo-terminal.
 * @param master Descritor válido do lado mestre.
 * @param text Texto válido e terminado em NUL a enviar ao REPL.
 * @details Executa uma única write(). Em falha ou escrita parcial, chama perror(),
 * sem repetir a escrita nem devolver o erro; o cenário verifica o progresso por wait_for().
 */
static void send_text(int master, const char *text) {
    size_t length = strlen(text);
    if (write(master, text, length) != (ssize_t)length) {
        perror("write");
    }
}

/**
 * @brief Mostra o diagnóstico e a saída capturada, tenta encerrar o mpmc e termina o teste.
 * @param child PID positivo do mpmc criado por este teste.
 * @param message Mensagem de falha a apresentar em stderr.
 * @details Envia SIGKILL e tenta recolher o processo com waitpid(); os retornos dessas
 * chamadas não são verificados. Encerra com EXIT_FAILURE e não retorna ao chamador.
 */
static void fail(pid_t child, const char *message) {
    fprintf(stderr, "FALHA: %s\n--- saída ---\n%s\n", message, output);
    kill(child, SIGKILL);
    waitpid(child, nullptr, 0);
    exit(EXIT_FAILURE);
}

/**
 * @brief Executa o mpmc no terminal escravo, como líder de sessão.
 * @param binary Caminho do executável, repassado a execl() sem pesquisa em PATH.
 * @param slave_path Caminho válido do terminal escravo devolvido por ptsname().
 * @details Configura o terminal de controle e redireciona os três descritores padrão.
 * A execução usa duas estruturas no paradigma de memória compartilhada.
 * @note Não retorna: execl() substitui o processo em sucesso; falhas de preparação
 * terminam com _exit(126), e falhas de execução, com _exit(127).
 */
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
    execl(binary, binary, "2", "1", (char *)nullptr);
    _exit(127);
}

/**
 * @brief Confere o redesenho do prompt após um aviso assíncrono em um pseudo-terminal.
 * @param argc Quantidade de argumentos, incluindo o nome do teste.
 * @param argv Vetor de argumentos; argv[1], quando presente, informa o executável mpmc.
 * @return EXIT_SUCCESS quando prompt, aviso, relatório e término forem os esperados;
 * EXIT_FAILURE em falha de preparação ou inconsistência observada.
 * @details Usa ./mpmc quando não há caminho informado. Cada etapa tem o prazo
 * STEP_TIMEOUT_MS; a coleta final consulta waitpid(WNOHANG) com intervalos de 10 ms.
 * @pre MPMC_TEST_WORKER_HANG não deve estar habilitada, pois o cenário espera que
 * o filho produtor termine espontaneamente sem um backend implementado.
 */
int main(int argc, char *argv[]) {
    const char *binary = argc > 1 ? argv[1] : "./mpmc";

    int master = posix_openpt(O_RDWR | O_NOCTTY);
    if (master == -1 || grantpt(master) == -1 || unlockpt(master) == -1) {
        perror("pseudo-terminal");
        return EXIT_FAILURE;
    }
    const char *slave_path = ptsname(master);
    if (slave_path == nullptr) {
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

    if (wait_for(master, 0, ">> ") < 0) {
        fail(child, "prompt inicial não apareceu");
    }

    send_text(master, "produz 1 1 2\n");
    long notice = wait_for(master, 0, "terminou");
    if (notice < 0) {
        fail(child, "aviso de término do filho não apareceu");
    }

    /* O aviso termina na quebra de linha; o prompt só conta se vier depois dela. */
    long line_end = wait_for(master, (size_t)notice, "\n");
    if (line_end < 0) {
        fail(child, "o aviso de término não foi concluído");
    }
    size_t after_notice = (size_t)line_end + 1;
    if (wait_for(master, after_notice, ">> ") < 0) {
        fail(child, "o prompt não foi redesenhado após o aviso assíncrono");
    }

    send_text(master, "shutdown\n");
    if (wait_for(master, 0, "Tempo total") < 0) {
        fail(child, "relatório final não apareceu após shutdown");
    }

    int status = 0;
    const long deadline = now_ms() + STEP_TIMEOUT_MS;
    pid_t reaped;
    while ((reaped = waitpid(child, &status, WNOHANG)) == 0 && now_ms() < deadline) {
        nanosleep(&(struct timespec){.tv_nsec = 10L * 1000L * 1000L}, nullptr);
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
