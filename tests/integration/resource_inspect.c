/**
 * @file resource_inspect.c
 * @brief Consulta ou remove um único recurso cujo ID foi registrado pela fixture.
 * @ingroup tests
 */
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/msg.h>
#include <sys/sem.h>
#include <sys/shm.h>

/** @brief Argumento portável exigido por semctl(IPC_STAT). */
union inspect_semun {
    struct semid_ds *buf;
};

/** @brief Retorna zero se o recurso existe/foi removido, um se ausente e dois em falha. */
int main(int argc, char *argv[]) {
    if (argc != 4) return 2;
    if (strcmp(argv[1], "create") == 0) {
        int created = -1;
        if (strcmp(argv[2], "sem") == 0) created = semget(IPC_PRIVATE, 1, 0600);
        if (strcmp(argv[2], "shm") == 0) created = shmget(IPC_PRIVATE, 2048, 0600);
        if (strcmp(argv[2], "msg") == 0) created = msgget(IPC_PRIVATE, 0600);
        if (created < 0) return 2;
        printf("%d\n", created);
        return 0;
    }
    bool remove = strcmp(argv[1], "remove") == 0;
    int command = remove ? IPC_RMID : IPC_STAT;
    int id = (int)strtol(argv[3], nullptr, 10);
    int result = -1;
    if (strcmp(argv[2], "sem") == 0) {
        struct semid_ds info;
        union inspect_semun argument = {.buf = &info};
        result = semctl(id, 0, command, argument);
    } else if (strcmp(argv[2], "shm") == 0) {
        struct shmid_ds info;
        result = shmctl(id, command, remove ? nullptr : &info);
    } else if (strcmp(argv[2], "msg") == 0) {
        struct msqid_ds info;
        result = msgctl(id, command, remove ? nullptr : &info);
    } else
        return 2;
    if (result >= 0) return 0;
    return errno == EINVAL || errno == EIDRM ? 1 : 2;
}
