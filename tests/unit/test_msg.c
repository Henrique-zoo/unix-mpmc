/**
 * @file test_msg.c
 * @brief Testes unitários dos wrappers de filas de mensagens System V.
 * @ingroup tests
 *
 * @details
 * Cobrem os critérios de aceite da issue #12 e os códigos de retorno
 * documentados em msg.h.
 *
 * Compilação e execução (a partir da raiz do repositório):
 *
 *     cc -std=c23 -Wall -Wextra -Iinclude -D_POSIX_C_SOURCE=200809L \
 *        tests/unit/test_msg.c src/ipc/msg.c src/error.c -o build/test_msg
 *     ./build/test_msg
 *
 * Retorna 0 se todos os casos passarem; 1 caso contrário.
 */

#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "mpmc/error.h"
#include "mpmc/ipc/msg.h"

static unsigned g_checks;
static unsigned g_failures;

#define CHECK(condition, ...)                                                                      \
    do {                                                                                           \
        ++g_checks;                                                                                \
        if (!(condition)) {                                                                        \
            ++g_failures;                                                                          \
            fprintf(stderr, "FALHA %s:%d: ", __FILE__, __LINE__);                                  \
            fprintf(stderr, __VA_ARGS__);                                                          \
            fputc('\n', stderr);                                                                   \
        }                                                                                          \
    } while (0)

#define CHECK_STATUS(call, expected)                                                               \
    do {                                                                                           \
        mpmc_result_t status_ = (call);                                                            \
        CHECK(                                                                                     \
            status_.status == (expected),                                                          \
            "%s retornou '%s', esperado '%s'",                                                     \
            #call,                                                                                 \
            mpmc_status_str(status_.status),                                                       \
            mpmc_status_str(expected)                                                              \
        );                                                                                         \
        CHECK(                                                                                     \
            status_.status != MPMC_OK || status_.system_errno == 0,                                \
            "%s: sucesso deve ter system_errno zero",                                              \
            #call                                                                                  \
        );                                                                                         \
    } while (0)

#define CHECK_ERROR(call, expected, expected_errno)                                                \
    do {                                                                                           \
        mpmc_result_t error_ = (call);                                                             \
        CHECK(error_.status == (expected), "%s: categoria inesperada", #call);                     \
        CHECK(                                                                                     \
            error_.system_errno == (expected_errno),                                               \
            "%s: system_errno=%d, esperado %d",                                                    \
            #call,                                                                                 \
            error_.system_errno,                                                                   \
            (expected_errno)                                                                       \
        );                                                                                         \
    } while (0)

static int new_queue(void) {
    int msqid = -1;

    mpmc_result_t error = sysv_msg_create(&msqid);
    if (error.status != MPMC_OK) {
        char description[256];
        (void)mpmc_error_format(error, description, sizeof description);
        fprintf(stderr, "sysv_msg_create: %s\n", description);
        _exit(2);
    }
    return msqid;
}

static void drop_queue(int msqid) {
    CHECK_STATUS(sysv_msg_remove(msqid), MPMC_OK);
}

static void on_alarm(int signo) {
    (void)signo;
}

/* Instala SIGALRM sem SA_RESTART e agenda um disparo em `millis` ms. */
static void arm_alarm(long millis) {
    struct sigaction action;
    struct itimerval timer;

    memset(&action, 0, sizeof action);
    action.sa_handler = on_alarm;
    sigemptyset(&action.sa_mask);
    sigaction(SIGALRM, &action, NULL);

    memset(&timer, 0, sizeof timer);
    timer.it_value.tv_sec = millis / 1000;
    timer.it_value.tv_usec = (millis % 1000) * 1000;
    setitimer(ITIMER_REAL, &timer, NULL);
}

/**
 * @brief Enche a fila com mensagens de 1 KiB, sem bloquear; retorna o resultado completo.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: mil envios concluídos sem atingir o limite da fila; system_errno é zero.
 * - MPMC_ERR_WOULD_BLOCK: fila ficou sem capacidade em um envio não bloqueante; system_errno
 *   contém EAGAIN.
 * - MPMC_ERR_INTERRUPTED: msgsnd() retornou EINTR, preservado em system_errno.
 * - MPMC_ERR_SYS: falha em malloc() ou msgsnd() por outra causa; system_errno contém o errno
 *   capturado na operação que falhou.
 *
 * O resultado do último sysv_msg_send() é propagado integralmente.
 */
static mpmc_result_t fill_queue(int msqid) {
    char block[1024];
    mpmc_result_t error = (mpmc_result_t){.status = MPMC_OK};

    memset(block, 0x5A, sizeof block);
    for (int i = 0; i < 1000 && error.status == MPMC_OK; ++i) {
        error = sysv_msg_send(msqid, 1, block, sizeof block, true);
    }
    return error;
}

static void test_create_remove(void) {
    int a = -1;
    int b = -1;

    CHECK_ERROR(sysv_msg_create(NULL), MPMC_ERR_INVALID_ARG, 0);
    CHECK_STATUS(sysv_msg_create(&a), MPMC_OK);
    CHECK_STATUS(sysv_msg_create(&b), MPMC_OK);
    CHECK(a >= 0 && b >= 0 && a != b, "IDs inválidos ou repetidos (%d, %d)", a, b);

    CHECK_STATUS(sysv_msg_remove(a), MPMC_OK);
    CHECK_STATUS(sysv_msg_remove(b), MPMC_OK);
    CHECK_STATUS(sysv_msg_remove(a), MPMC_ERR_SYS); /* já removida */
}

static void test_failures_are_propagated(void) {
    char byte = 'x';
    char out;
    ssize_t size = 0;

    CHECK_ERROR(sysv_msg_send(-1, 1, &byte, 1, true), MPMC_ERR_SYS, EINVAL);
    CHECK_ERROR(sysv_msg_receive(-1, 0, &out, 1, true, &size), MPMC_ERR_SYS, EINVAL);
    CHECK_ERROR(sysv_msg_remove(-1), MPMC_ERR_SYS, EINVAL);
}

static void test_roundtrip_preserves_payload(void) {
    int msqid = new_queue();
    unsigned char sent[256];
    unsigned char got[256];
    ssize_t size = -1;

    for (size_t i = 0; i < sizeof sent; ++i) {
        sent[i] = (unsigned char)i; /* inclui 0x00 */
    }
    memset(got, 0xEE, sizeof got);

    errno = EACCES;
    CHECK_STATUS(sysv_msg_send(msqid, 7, sent, sizeof sent, false), MPMC_OK);
    errno = EACCES;
    CHECK_STATUS(sysv_msg_receive(msqid, 7, got, sizeof got, false, &size), MPMC_OK);
    CHECK(size == (ssize_t)sizeof sent, "msgsz deve excluir mtype: recebido %zd", size);
    CHECK(memcmp(sent, got, sizeof sent) == 0, "payload alterado no trânsito");

    drop_queue(msqid);
}

static void test_zero_size_message(void) {
    int msqid = new_queue();
    ssize_t size = -1;

    CHECK_STATUS(sysv_msg_send(msqid, 3, NULL, 0, false), MPMC_OK);
    CHECK_STATUS(sysv_msg_receive(msqid, 3, NULL, 0, false, &size), MPMC_OK);
    CHECK(size == 0, "corpo vazio deveria retornar 0 bytes, obteve %zd", size);

    drop_queue(msqid);
}

static void test_fifo_order(void) {
    int msqid = new_queue();

    for (int i = 0; i < 10; ++i) {
        CHECK_STATUS(sysv_msg_send(msqid, 1, &i, sizeof i, false), MPMC_OK);
    }
    for (int i = 0; i < 10; ++i) {
        int got = -1;
        ssize_t size = 0;

        CHECK_STATUS(sysv_msg_receive(msqid, 0, &got, sizeof got, false, &size), MPMC_OK);
        CHECK(
            size == (ssize_t)sizeof got && got == i, "FIFO quebrado: esperado %d, obtido %d", i, got
        );
    }

    drop_queue(msqid);
}

static void test_type_filtering(void) {
    int msqid = new_queue();
    char got = 0;
    ssize_t size = 0;

    CHECK_STATUS(sysv_msg_send(msqid, 5, "a", 1, false), MPMC_OK);
    CHECK_STATUS(sysv_msg_send(msqid, 2, "b", 1, false), MPMC_OK);
    CHECK_STATUS(sysv_msg_send(msqid, 5, "c", 1, false), MPMC_OK);

    CHECK_STATUS(sysv_msg_receive(msqid, 2, &got, 1, true, &size), MPMC_OK);
    CHECK(got == 'b', "tipo 2 deveria entregar 'b', entregou '%c'", got);

    CHECK_STATUS(sysv_msg_receive(msqid, 9, &got, 1, true, &size), MPMC_ERR_WOULD_BLOCK);

    CHECK_STATUS(sysv_msg_receive(msqid, 0, &got, 1, true, &size), MPMC_OK);
    CHECK(got == 'a', "tipo 0 deveria entregar 'a', entregou '%c'", got);
    CHECK_STATUS(sysv_msg_receive(msqid, 0, &got, 1, true, &size), MPMC_OK);
    CHECK(got == 'c', "tipo 0 deveria entregar 'c', entregou '%c'", got);

    drop_queue(msqid);
}

static void test_receive_nowait_empty(void) {
    int msqid = new_queue();
    char got;
    ssize_t size = 0;

    mpmc_result_t error = sysv_msg_receive(msqid, 0, &got, 1, true, &size);
    errno = EACCES;
    CHECK(error.status == MPMC_ERR_WOULD_BLOCK, "fila vazia deve retornar WOULD_BLOCK");
    CHECK(error.system_errno == ENOMSG, "causa ENOMSG deve sobreviver à mudança de errno");

    drop_queue(msqid);
}

static void test_send_nowait_full(void) {
    int msqid = new_queue();
    unsigned char got[1024];
    ssize_t size = 0;

    CHECK_ERROR(fill_queue(msqid), MPMC_ERR_WOULD_BLOCK, EAGAIN);

    /* Após consumir uma mensagem, o envio volta a funcionar. */
    CHECK_STATUS(sysv_msg_receive(msqid, 0, got, sizeof got, true, &size), MPMC_OK);
    CHECK_STATUS(sysv_msg_send(msqid, 1, got, sizeof got, true), MPMC_OK);

    drop_queue(msqid);
}

static void test_message_too_big_is_not_truncated(void) {
    int msqid = new_queue();
    char got[16];
    ssize_t size = 77;

    CHECK_STATUS(sysv_msg_send(msqid, 1, "0123456789", 10, false), MPMC_OK);
    CHECK_ERROR(sysv_msg_receive(msqid, 1, got, 4, true, &size), MPMC_ERR_SYS, E2BIG);
    CHECK(size == 77, "out_size não deve ser alterado em falha");

    /* A mensagem continua na fila, íntegra. */
    CHECK_STATUS(sysv_msg_receive(msqid, 1, got, sizeof got, true, &size), MPMC_OK);
    CHECK(size == 10 && memcmp(got, "0123456789", 10) == 0, "mensagem perdida ou truncada");

    drop_queue(msqid);
}

static void test_blocking_receive_interrupted(void) {
    int msqid = new_queue();
    char got;
    ssize_t size = 0;

    arm_alarm(50);
    CHECK_ERROR(sysv_msg_receive(msqid, 0, &got, 1, false, &size), MPMC_ERR_INTERRUPTED, EINTR);

    drop_queue(msqid);
}

static void test_blocking_send_interrupted(void) {
    int msqid = new_queue();
    char block[1024] = {0};

    CHECK_STATUS(fill_queue(msqid), MPMC_ERR_WOULD_BLOCK);

    arm_alarm(50);
    CHECK_ERROR(sysv_msg_send(msqid, 1, block, sizeof block, false), MPMC_ERR_INTERRUPTED, EINTR);

    drop_queue(msqid);
}

/* O ID herdado por fork() permite que outro processo desbloqueie o receptor. */
static void test_blocking_receive_wakes_on_send_from_child(void) {
    int msqid = new_queue();
    char got[8] = {0};
    ssize_t size = 0;
    int wstatus = 0;
    pid_t child = fork();

    CHECK(child >= 0, "fork falhou");
    if (child == 0) {
        nanosleep(&(struct timespec){.tv_nsec = 100000000L}, NULL);
        _exit(sysv_msg_send(msqid, 4, "filho", 5, false).status == MPMC_OK ? 0 : 1);
    }

    CHECK_STATUS(sysv_msg_receive(msqid, 4, got, sizeof got, false, &size), MPMC_OK);
    CHECK(size == 5 && memcmp(got, "filho", 5) == 0, "payload do filho incorreto");

    waitpid(child, &wstatus, 0);
    CHECK(WIFEXITED(wstatus) && WEXITSTATUS(wstatus) == 0, "filho falhou ao enviar");

    drop_queue(msqid);
}

/* Lê um limite do kernel em /proc/sys/kernel; devolve `fallback` se indisponível. */
static size_t kernel_limit(const char *path, size_t fallback) {
    FILE *file = fopen(path, "r");
    unsigned long value = 0;

    if (file == NULL) return fallback;
    if (fscanf(file, "%lu", &value) != 1 || value == 0) {
        value = fallback;
    }
    fclose(file);
    return (size_t)value;
}

static void test_message_size_limit(void) {
    int msqid = new_queue();
    size_t msgmax = kernel_limit("/proc/sys/kernel/msgmax", 0);
    size_t msgmnb = kernel_limit("/proc/sys/kernel/msgmnb", 16384);

    if (msgmax == 0) {
        drop_queue(msqid);
        return;
    }

    unsigned char *sent = malloc(msgmax + 1);
    unsigned char *got = malloc(msgmax + 1);
    ssize_t size = 0;

    CHECK(sent != NULL && got != NULL, "malloc falhou");
    if (sent == NULL || got == NULL) {
        free(sent);
        free(got);
        drop_queue(msqid);
        return;
    }
    memset(sent, 0xA5, msgmax + 1);

    /* Acima de MSGMAX o kernel recusa, e o wrapper propaga a falha. */
    CHECK_STATUS(sysv_msg_send(msqid, 1, sent, msgmax + 1, true), MPMC_ERR_SYS);

    /* Exatamente MSGMAX é aceito, desde que a fila comporte (MSGMNB). */
    if (msgmax <= msgmnb) {
        CHECK_STATUS(sysv_msg_send(msqid, 1, sent, msgmax, true), MPMC_OK);
        CHECK_STATUS(sysv_msg_receive(msqid, 1, got, msgmax, true, &size), MPMC_OK);
        CHECK(
            size == (ssize_t)msgmax && memcmp(sent, got, msgmax) == 0,
            "mensagem de tamanho MSGMAX corrompida (size=%zd)",
            size
        );
    }

    free(sent);
    free(got);
    drop_queue(msqid);
}

/* Envio bloqueado numa fila cheia é liberado quando outro processo consome. */
static void test_blocked_send_wakes_on_receive_from_child(void) {
    int msqid = new_queue();
    char block[1024] = {0};
    struct timespec start;
    struct timespec end;
    int wstatus = 0;
    pid_t child;

    CHECK_STATUS(fill_queue(msqid), MPMC_ERR_WOULD_BLOCK);

    child = fork();
    CHECK(child >= 0, "fork falhou");
    if (child == 0) {
        char buffer[1024];
        ssize_t size = 0;

        nanosleep(&(struct timespec){.tv_nsec = 100000000L}, NULL);
        _exit(
            sysv_msg_receive(msqid, 0, buffer, sizeof buffer, false, &size).status == MPMC_OK ? 0
                                                                                              : 1
        );
    }

    clock_gettime(CLOCK_MONOTONIC, &start);
    CHECK_STATUS(sysv_msg_send(msqid, 1, block, sizeof block, false), MPMC_OK);
    clock_gettime(CLOCK_MONOTONIC, &end);

    /* O envio realmente esperou pelo filho (que dorme 100 ms antes de consumir). */
    long elapsed_ms =
        (end.tv_sec - start.tv_sec) * 1000L + (end.tv_nsec - start.tv_nsec) / 1000000L;

    CHECK(elapsed_ms >= 50, "envio não bloqueou: levou apenas %ld ms", elapsed_ms);

    waitpid(child, &wstatus, 0);
    CHECK(WIFEXITED(wstatus) && WEXITSTATUS(wstatus) == 0, "filho falhou ao consumir");

    drop_queue(msqid);
}

/* Confere, com msgrcv() direto, o tipo e o corpo que o wrapper realmente colocou na fila. */
static void test_send_stores_type_and_payload(void) {
    int msqid = new_queue();
    struct {
        long mtype;
        unsigned char mtext[32];
    } raw;
    const long types[] = {1, 42, 123456};
    const unsigned char body[] = {0x00, 0x11, 0xFF, 0x00, 0x7F};

    for (size_t i = 0; i < sizeof types / sizeof types[0]; ++i) {
        memset(&raw, 0xEE, sizeof raw);
        CHECK_STATUS(sysv_msg_send(msqid, types[i], body, sizeof body, false), MPMC_OK);

        ssize_t n = msgrcv(msqid, &raw, sizeof raw.mtext, 0, IPC_NOWAIT);

        CHECK(n == (ssize_t)sizeof body, "kernel recebeu %zd bytes, esperado %zu", n, sizeof body);
        CHECK(raw.mtype == types[i], "kernel recebeu tipo %ld, esperado %ld", raw.mtype, types[i]);
        CHECK(
            n == (ssize_t)sizeof body && memcmp(raw.mtext, body, sizeof body) == 0,
            "corpo gravado na fila difere do enviado"
        );
    }

    drop_queue(msqid);
}

/* Mensagem inserida direto pelo kernel é entregue pelo wrapper de recebimento sem alterações. */
static void test_receive_returns_kernel_payload(void) {
    int msqid = new_queue();
    struct {
        long mtype;
        unsigned char mtext[4];
    } raw = {.mtype = 9, .mtext = {0xDE, 0xAD, 0x00, 0xEF}};
    unsigned char got[8];
    ssize_t size = -1;

    CHECK(msgsnd(msqid, &raw, sizeof raw.mtext, 0) == 0, "msgsnd direto falhou");
    CHECK_STATUS(sysv_msg_receive(msqid, 9, got, sizeof got, true, &size), MPMC_OK);
    CHECK(size == 4 && memcmp(got, raw.mtext, 4) == 0, "corpo entregue difere do inserido");

    drop_queue(msqid);
}

int main(void) {
    test_create_remove();
    test_failures_are_propagated();
    test_roundtrip_preserves_payload();
    test_zero_size_message();
    test_send_stores_type_and_payload();
    test_receive_returns_kernel_payload();
    test_fifo_order();
    test_type_filtering();
    test_receive_nowait_empty();
    test_send_nowait_full();
    test_message_too_big_is_not_truncated();
    test_blocking_receive_interrupted();
    test_blocking_send_interrupted();
    test_blocking_receive_wakes_on_send_from_child();
    test_blocked_send_wakes_on_receive_from_child();
    test_message_size_limit();

    printf("%u verificações, %u falhas\n", g_checks, g_failures);

    return g_failures == 0 ? 0 : 1;
}
