/**
 * @file sem.h
 * @brief Wrappers mínimos sobre conjuntos de semáforos System V.
 * @ingroup sysv_primitives
 * @details Argumentos das chamadas de sistema são validados pelo kernel.
 * O wrapper verifica apenas saídas escritas em C e conversões que perderiam informação.
 */
#ifndef MPMC_IPC_SEM_H
#define MPMC_IPC_SEM_H

#include <stdbool.h>
#include <stddef.h>
#include <sys/sem.h>

#include "mpmc/error.h"

/** @ingroup sysv_primitives
 * @{ */

/**
 * @brief Cria um conjunto privado de semáforos com permissões 0600.
 * @param count Número positivo de semáforos, representável em int.
 * @param[out] out_semid Recebe o identificador, inclusive zero, somente em sucesso.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: conjunto criado; system_errno é zero.
 * - MPMC_ERR_INVALID_ARG: out_semid nulo ou count não representável em int,
 *   com system_errno zero; ou semget() rejeitou count, inclusive zero, com EINVAL,
 *   preservado em system_errno.
 * - MPMC_ERR_SYS: outra falha em semget(); system_errno contém o errno capturado.
 * @post Em erro, *out_semid permanece inalterado quando o ponteiro é válido.
 * @note Inicialize os valores com sysv_sem_set() antes de iniciar os workers.
 * @warning O dono deve remover o conjunto com sysv_sem_remove().
 */
mpmc_result_t sysv_sem_create(unsigned count, int *out_semid);

/**
 * @brief Atribui o valor inicial de um semáforo com semctl(SETVAL).
 * @param semid Identificador do conjunto.
 * @param index Índice baseado em zero, representável em int.
 * @param value Valor inicial não negativo, representável em int.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: valor atribuído; system_errno é zero.
 * - MPMC_ERR_INVALID_ARG: value não representável em int,
 *   com system_errno zero; ou semctl(SETVAL) retornou EINVAL ou ERANGE,
 *   preservado em system_errno.
 * - MPMC_ERR_INVALID_INDEX: index não representável em int; system_errno é zero.
 * - MPMC_ERR_SHUTDOWN: semctl(SETVAL) retornou EIDRM, preservado em system_errno.
 * - MPMC_ERR_SYS: outra falha em semctl(SETVAL); system_errno contém o errno capturado.
 * @note EINVAL não distingue um semid inválido de um índice inexistente no conjunto.
 * Não é executado IPC_STAT para tentar diferenciá-los, pois exigiria leitura adicional.
 * @note A união utilizada por semctl() é encapsulada na implementação.
 * @warning Use SETVAL na inicialização; não o utilize para implementar P/V nem
 * para compensar reservas enquanto houver processos usando o conjunto.
 */
mpmc_result_t sysv_sem_set(int semid, unsigned index, unsigned value);

/**
 * @brief Efetua P/V ou espera por zero, usando uma operação semop().
 * @param semid Identificador do conjunto.
 * @param index Índice baseado em zero, representável em unsigned short.
 * @param delta Negativo para P, positivo para V e zero para esperar valor zero.
 * @param nowait Se true, usa IPC_NOWAIT.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: operação concluída; system_errno é zero.
 * - MPMC_ERR_INVALID_ARG: semop() retornou EINVAL por identificador inválido ou
 *   ERANGE por exceder o valor máximo do semáforo; system_errno preserva esse errno.
 * - MPMC_ERR_INVALID_INDEX: index não representável em unsigned short, com
 *   system_errno zero; ou semop() retornou EFBIG, preservado em system_errno.
 * - MPMC_ERR_WOULD_BLOCK: semop() retornou EAGAIN com nowait=true;
 *   system_errno contém EAGAIN.
 * - MPMC_ERR_INTERRUPTED: semop() retornou EINTR, preservado em system_errno.
 * - MPMC_ERR_SHUTDOWN: semop() retornou EIDRM, preservado em system_errno.
 * - MPMC_ERR_SYS: outra falha em semop(); system_errno contém o errno capturado.
 * @note Não repete EINTR e não consulta IPC_STAT antes da operação.
 * @warning Não usa SEM_UNDO. Compensações pertencem ao protocolo do backend.
 */
mpmc_result_t sysv_sem_change(int semid, unsigned index, short delta, bool nowait);

/**
 * @brief Executa atomicamente múltiplas operações sobre semáforos System V.
 *
 * @details
 * Utiliza semop() para aplicar um vetor de operações sobre semáforos
 * pertencentes ao mesmo conjunto.
 *
 * Cada operação é descrita por uma estrutura sembuf, contendo:
 * - sem_num: índice do semáforo no conjunto.
 * - sem_op: valor negativo para P, positivo para V ou zero para esperar
 *   que o semáforo alcance zero.
 * - sem_flg: flags da operação, incluindo IPC_NOWAIT quando desejado.
 *
 * O kernel garante a atomicidade do vetor: todas as operações são
 * aplicadas ou nenhuma delas é executada. Caso alguma operação não
 * possa prosseguir, a chamada aguarda, salvo quando IPC_NOWAIT estiver
 * especificado para a operação que impediria o prosseguimento.
 *
 * @param semid Identificador do conjunto de semáforos.
 * @param[in] operations Vetor de operações a executar, na ordem informada.
 * @param count Quantidade de operações no vetor, obrigatoriamente positiva.
 *
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: todas as operações foram concluídas; system_errno é zero.
 * - MPMC_ERR_INVALID_ARG: semop() retornou EINVAL por identificador inválido ou
 *   count zero, E2BIG por excesso de operações, EFAULT por vetor inacessível ou
 *   ERANGE por exceder os limites de valor; system_errno preserva o errno capturado.
 * - MPMC_ERR_INVALID_INDEX: semop() retornou EFBIG, preservado
 *   em system_errno.
 * - MPMC_ERR_WOULD_BLOCK: semop() retornou EAGAIN, preservado
 *   em system_errno.
 * - MPMC_ERR_INTERRUPTED: semop() retornou EINTR, preservado
 *   em system_errno.
 * - MPMC_ERR_SHUTDOWN: semop() retornou EIDRM, preservado
 *   em system_errno.
 * - MPMC_ERR_SYS: outra falha em semop(); system_errno contém
 *   o errno capturado.
 *
 * @pre Se operations não for nulo, deve apontar para um vetor de pelo menos count elementos.
 * @note Identificador, quantidade, acesso ao vetor e índices são verificados por semop().
 * O wrapper não acessa operations nem consulta IPC_STAT antes da chamada.
 *
 * @post Em sucesso, todas as operações foram aplicadas atomicamente.
 * @post Em erro, nenhuma operação do vetor foi aplicada.
 *
 * @note Não repete automaticamente chamadas interrompidas por EINTR.
 * @note Todos os elementos do vetor se referem ao mesmo conjunto semid.
 *
 * @warning O protocolo do backend de memória compartilhada não utiliza
 * SEM_UNDO. A recuperação de reservas e o tratamento de cancelamentos
 * permanecem sob responsabilidade do backend.
 */
mpmc_result_t sysv_sem_apply(int semid, struct sembuf *operations, size_t count);

/**
 * @brief Remove um conjunto com semctl(IPC_RMID).
 * @param semid Identificador do conjunto pertencente ao supervisor.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: conjunto removido; system_errno é zero.
 * - MPMC_ERR_INVALID_ARG: semctl(IPC_RMID) retornou EINVAL por identificador inválido;
 *   system_errno preserva EINVAL.
 * - MPMC_ERR_SHUTDOWN: semctl(IPC_RMID) retornou EIDRM, preservado em system_errno.
 * - MPMC_ERR_SYS: outra falha em semctl(IPC_RMID); system_errno contém o errno capturado.
 * @note Um identificador já removido pode produzir EINVAL; a remoção não é idempotente.
 * @warning Somente o supervisor deve remover o conjunto global. Workers apenas
 * liberam seu estado local. A remoção acorda esperas em semop() com EIDRM.
 */
mpmc_result_t sysv_sem_remove(int semid);

/** @} */
#endif
