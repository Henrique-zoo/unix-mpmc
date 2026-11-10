/**
 * @file probe_syscalls.c
 * @brief Adaptador de testes: observa recursos próprios e injeta falhas no executável de prova.
 * @ingroup tests
 * @details Ligado somente a mpmc_probe. O executável distribuído não lê estas variáveis.
 */
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <sys/sem.h>
#include <sys/shm.h>
#include <time.h>
#include <unistd.h>

#include "mpmc/diagnostic.h"
#include "mpmc/message.h"

/** @brief Número de tentativas de criação neste processo. */
static unsigned creates;
static bool fail_create(void);
static void trigger_cancellation(void);
static void record_wait(const char *kind, int id);
static void record_resource(const char *kind, int id);

/** @brief Intercepta semget(), conservando sua assinatura e seu resultado real. */
int mpmc_probe_semget(key_t key, int count, int flags) {
    if (fail_create()) return -1;
    int id = semget(key, count, flags);
    if (id >= 0) record_resource("sem", id);
    return id;
}

/** @brief Intercepta shmget(), conservando sua assinatura e seu resultado real. */
int mpmc_probe_shmget(key_t key, size_t size, int flags) {
    if (fail_create()) return -1;
    int id = shmget(key, size, flags);
    if (id >= 0) record_resource("shm", id);
    return id;
}

/** @brief Intercepta msgget(), conservando sua assinatura e seu resultado real. */
int mpmc_probe_msgget(key_t key, int flags) {
    if (fail_create()) return -1;
    int id = msgget(key, flags);
    if (id >= 0) record_resource("msg", id);
    return id;
}

/** @brief Falha somente o vetor de publicação de dados, depois da alteração do ring. */
int mpmc_probe_semop(int id, struct sembuf *operations, size_t count) {
    if (getenv("MPMC_PROBE_FAIL_RELEASE") != nullptr && count == 2 && operations[0].sem_op > 0) {
        errno = EIO;
        return -1;
    }
    for (size_t index = 0; index < count; ++index) {
        if (operations[index].sem_op < 0 && !(operations[index].sem_flg & IPC_NOWAIT)
            && semctl(id, operations[index].sem_num, GETVAL) < -operations[index].sem_op) {
            record_wait("sem", id);
            break;
        }
    }
    int result = semop(id, operations, count);
    if (result == 0 && count == 2 && operations[0].sem_num == 2 && operations[0].sem_op < 0
        && getenv("MPMC_PROBE_CANCEL_AFTER_RECEIVE") != nullptr)
        trigger_cancellation();
    if (result == 0 && count == 2 && operations[0].sem_num == 1 && operations[0].sem_op < 0
        && getenv("MPMC_PROBE_CANCEL_AFTER_SEND") != nullptr)
        trigger_cancellation();
    return result;
}

/** @brief Simula erro de detach; ao terminar o processo, o kernel desfaz suas associações. */
int mpmc_probe_shmdt(const void *address) {
    if (getenv("MPMC_PROBE_DIAGNOSTIC_FLOOD") != nullptr) {
        // Carga sintética do canal: cada callback deve atravessar o pipe sem descarte.
        for (unsigned index = 0; index < 200; ++index)
            mpmc_diagnostic_report(
                "detalhe de cleanup injetado",
                (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = EACCES}
            );
    }
    if (getenv("MPMC_PROBE_FAIL_DETACH") != nullptr) {
        errno = EACCES;
        return -1;
    }
    return shmdt(address);
}

/** @brief Simula falha de remoção de filas para conferir múltiplos erros no rollback. */
int mpmc_probe_msgctl(int id, int command, struct msqid_ds *buffer) {
    if (command == IPC_RMID && getenv("MPMC_PROBE_FAIL_REMOVE") != nullptr) {
        errno = EPERM;
        return -1;
    }
    return msgctl(id, command, buffer);
}

/** @brief Falha a tentativa ordinal solicitada pelo cenário; não afeta outras criações. */
static bool fail_create(void) {
    const char *at = getenv("MPMC_PROBE_FAIL_CREATE_AT");
    ++creates;
    if (at != nullptr && creates == strtoul(at, nullptr, 10)) {
        errno = ENOSPC;
        return true;
    }
    return false;
}

/** @brief Registra somente IDs retornados com sucesso; preserva errno do wrapper. */
static void record_resource(const char *kind, int id) {
    int saved_errno = errno;
    const char *path = getenv("MPMC_PROBE_RESOURCES");
    if (path != nullptr) {
        int fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0600);
        if (fd >= 0) {
            char text[128];
            int size = snprintf(text, sizeof text, "%s %d %ld\n", kind, id, (long)getpid());
            if (size > 0 && (size_t)size < sizeof text) (void)write(fd, text, (size_t)size);
            (void)close(fd);
        }
    }
    errno = saved_errno;
}

/** @brief Publica readiness ao teste antes de uma espera comprovadamente sem recurso disponível. */
static void record_wait(const char *kind, int id) {
    int saved_errno = errno;
    const char *descriptor = getenv("MPMC_PROBE_WAIT_FD");
    if (descriptor != nullptr) {
        int fd = (int)strtol(descriptor, nullptr, 10);
        char text[128];
        int size = snprintf(text, sizeof text, "%s %ld %d\n", kind, (long)getpid(), id);
        if (size > 0 && (size_t)size < sizeof text) (void)write(fd, text, (size_t)size);
    }
    errno = saved_errno;
}

/** @brief Observa uma recepção bloqueante sem itens, sem alterar a chamada real. */
ssize_t mpmc_probe_msgrcv(int id, void *message, size_t size, long type, int flags) {
    struct msqid_ds info;
    if (!(flags & IPC_NOWAIT) && msgctl(id, IPC_STAT, &info) == 0 && info.msg_qnum == 0)
        record_wait("receive", id);
    ssize_t received = msgrcv(id, message, size, type, flags);
    if (received == sizeof(message_t) && type == 0
        && getenv("MPMC_PROBE_CANCEL_AFTER_RECEIVE") != nullptr)
        trigger_cancellation();
    return received;
}

/** @brief Observa saturação de uma fila de mensagens de tamanho uniforme. */
int mpmc_probe_msgsnd(int id, const void *message, size_t size, int flags) {
    if ((flags & IPC_NOWAIT)
        && ((size == 16 && getenv("MPMC_PROBE_DEFER_EVENTS") != nullptr)
            || (size == 8 && getenv("MPMC_PROBE_DEFER_ACK") != nullptr))) {
        errno = EAGAIN;
        return -1;
    }
    struct msqid_ds info;
    if (!(flags & IPC_NOWAIT) && size != 0 && msgctl(id, IPC_STAT, &info) == 0
        && info.msg_qnum + 1 > info.msg_qbytes / size)
        record_wait("send", id);
    int result = msgsnd(id, message, size, flags);
    if (result == 0 && size == sizeof(message_t)
        && getenv("MPMC_PROBE_CANCEL_AFTER_SEND") != nullptr)
        trigger_cancellation();
    if (result == 0 && size != sizeof(message_t) && !(flags & IPC_NOWAIT)
        && getenv("MPMC_PROBE_SIGNAL_DURING_CLOSE") != nullptr)
        trigger_cancellation();
    return result;
}

/** @brief Controle negativo opcional que reproduz a ação padrão anterior ao cancelamento
 * cooperativo. */
static void trigger_cancellation(void) {
    if (getenv("MPMC_PROBE_RESTORE_SIGTERM_DEFAULT") != nullptr) {
        struct sigaction previous = {.sa_handler = SIG_DFL};
        sigemptyset(&previous.sa_mask);
        (void)sigaction(SIGTERM, &previous, nullptr);
    }
    (void)raise(SIGTERM);
}

/** @brief Falha de relógio ativada pelo teste apenas depois da conclusão dos workers. */
int mpmc_probe_clock_gettime(clockid_t clock_id, struct timespec *out) {
    const char *marker = getenv("MPMC_PROBE_FAIL_CLOCK_MARKER");
    if (clock_id == CLOCK_MONOTONIC && marker != nullptr && access(marker, F_OK) == 0) {
        errno = EIO;
        return -1;
    }
    return clock_gettime(clock_id, out);
}
