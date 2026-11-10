/**
 * @file probe_syscalls.c
 * @brief Adaptador de testes: observa recursos próprios e injeta falhas no executável de prova.
 * @ingroup tests
 * @details Ligado somente a mpmc_probe. O executável distribuído não lê estas variáveis.
 */
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <sys/sem.h>
#include <sys/shm.h>
#include <unistd.h>

/** @brief Número de tentativas de criação neste processo. */
static unsigned creates;
static bool fail_create(void);
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
    return semop(id, operations, count);
}

/** @brief Simula erro de detach; ao terminar o processo, o kernel desfaz suas associações. */
int mpmc_probe_shmdt(const void *address) {
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
