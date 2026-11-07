/**
 * @file shm.h
 * @brief Wrappers mínimos sobre memória compartilhada System V.
 * @ingroup sysv_primitives
 */
#ifndef MPMC_IPC_SHM_H
#define MPMC_IPC_SHM_H

#include <stddef.h>
#include <sys/shm.h>

#include "mpmc/error.h"

/**
 * @defgroup sysv_primitives Primitivas System V
 * @ingroup ipc
 * @brief Operações de baixo nível e tradução de erros de chamadas de sistema.
 * @{ */

/**
 * @brief Cria um segmento privado de memória compartilhada System V.
 *
 * @details
 * Utiliza shmget() com IPC_PRIVATE para solicitar a criação de um
 * novo segmento de memória compartilhada, com o tamanho informado
 * e permissões de leitura e escrita para o proprietário (0600).
 *
 * O identificador retornado pelo kernel é armazenado em out_shmid
 * somente quando a criação é concluída com sucesso.
 *
 * @param size Tamanho solicitado para o segmento, em bytes.
 * @param[out] out_shmid Ponteiro que recebe o identificador do segmento.
 *
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: segmento privado criado e identificador escrito em out_shmid; system_errno é zero.
 * - MPMC_ERR_INVALID_ARG: size zero ou out_shmid nulo; system_errno é zero.
 * - MPMC_ERR_SYS: falha em shmget(); system_errno contém o errno capturado nessa falha.
 *
 * @pre size deve ser maior que zero.
 * @pre out_shmid deve apontar para uma variável válida.
 *
 * @post Em caso de sucesso, *out_shmid contém o identificador do
 * segmento criado. Em caso de erro, *out_shmid permanece inalterado,
 * quando o ponteiro é válido.
 *
 * @par Tratamento de errno
 * Em caso de falha de shmget(), retorna status MPMC_ERR_SYS e armazena
 * em system_errno o código de erro fornecido pelo sistema operacional.
 *
 * Erros de validação dos argumentos possuem system_errno igual a zero.
 *
 * @warning O segmento criado deve ser posteriormente removido
 * por sysv_shm_remove() para evitar recursos IPC residuais.
 *
 * @note A função não associa o segmento ao espaço de endereçamento
 * do processo. Essa operação pertence a sysv_shm_attach().
 *
 * @note O tamanho de 2048 bytes exigido pelo trabalho é uma
 * responsabilidade do backend, não deste wrapper genérico.
 */
mpmc_result_t sysv_shm_create(size_t size, int *out_shmid);

/**
 * @brief Associa um segmento de memória compartilhada System V ao processo atual.
 *
 * @details
 * Utiliza shmat() para associar o segmento identificado por @p shmid
 * ao espaço de endereçamento virtual do processo, com permissões de
 * leitura e escrita.
 *
 * O endereço de associação é escolhido automaticamente pelo kernel,
 * pois shmaddr é informado como NULL.
 *
 * Em caso de sucesso, o endereço inicial do segmento é armazenado em
 * @p out_addr. Em caso de falha, o parâmetro de saída permanece inalterado.
 *
 * @param shmid Identificador do segmento de memória compartilhada.
 * @param[out] out_addr Ponteiro que recebe o endereço inicial do segmento.
 *
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: segmento associado com sucesso.
 * - MPMC_ERR_INVALID_ARG: shmid negativo ou ponteiro de saída nulo.
 * - MPMC_ERR_SYS: falha em shmat(), com o erro original armazenado em system_errno.
 *
 * Nos resultados MPMC_OK e MPMC_ERR_INVALID_ARG, system_errno é zero.
 *
 * @pre out_addr deve apontar para uma variável válida.
 * @pre shmid deve identificar um segmento existente, acessível ao processo.
 *
 * @post Em caso de sucesso, *out_addr contém o endereço inicial do
 * segmento associado. Em caso de erro, *out_addr permanece inalterado,
 * desde que out_addr seja válido.
 *
 * @par Tratamento de erros
 * Quando shmat() falha, o campo system_errno do resultado recebe o
 * valor de errno correspondente à falha. Nos demais casos, esse
 * campo é inicializado com zero.
 *
 * @note Processos distintos podem associar o mesmo segmento em
 * endereços virtuais diferentes.
 *
 * @warning A associação deve ser posteriormente desfeita por
 * sysv_shm_detach() quando não for mais necessária.
 */
mpmc_result_t sysv_shm_attach(int shmid, void **out_addr);

/**
 * @brief Desanexa mapeamento do processo corrente.
 * @param address Endereço inicial da associação, retornado por shmat() neste processo.
 *
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: associação desfeita com sucesso; system_errno é zero.
 * - MPMC_ERR_INVALID_ARG: address nulo; system_errno é zero.
 * - MPMC_ERR_SYS: falha em shmdt(); system_errno contém o errno capturado nessa falha.
 *
 * @post Em caso de sucesso, os ponteiros para a associação desfeita não devem mais ser usados.
 * @note Desanexar não equivale a remover o segmento do sistema.
 * @see sysv_shm_remove
 */
mpmc_result_t sysv_shm_detach(const void *address);

/**
 * @brief Marca segmento global para remoção usando shmctl(IPC_RMID).
 * @details Se ainda houver associações ativas, a destruição efetiva é adiada até
 * a última desassociação. Esta função não executa shmdt() no processo atual.
 *
 * @param shmid Identificador de segmento que pertence ao supervisor.
 *
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: segmento marcado para remoção com sucesso; system_errno é zero.
 * - MPMC_ERR_INVALID_ARG: shmid negativo; system_errno é zero.
 * - MPMC_ERR_SYS: falha em shmctl(IPC_RMID); system_errno contém o errno capturado nessa falha.
 *
 * @warning Somente o supervisor deve solicitar remoção do objeto global, pois outros
 * processos podem estar usando o segmento.
 * @see sysv_shm_detach
 */
mpmc_result_t sysv_shm_remove(int shmid);

/**
 * @brief Consulta as informações de um segmento de memória compartilhada System V.
 *
 * @details
 * Utiliza shmctl() com IPC_STAT para recuperar os metadados do segmento
 * identificado por @p shmid, incluindo tamanho, permissões, proprietário
 * e quantidade de associações existentes.
 *
 * As informações são armazenadas em @p out_info somente quando a
 * operação é concluída com sucesso.
 *
 * @param shmid Identificador do segmento de memória compartilhada.
 * @param[out] out_info Ponteiro que recebe os metadados do segmento.
 *
 * @return Estrutura mpmc_result_t com um dos seguintes estados:
 * - MPMC_OK: informações recuperadas com sucesso.
 * - MPMC_ERR_INVALID_ARG: identificador negativo ou ponteiro de saída nulo.
 * - MPMC_ERR_SYS: falha em shmctl(IPC_STAT); system_errno contém o errno
 *   capturado nessa falha.
 *
 * Nos resultados MPMC_OK e MPMC_ERR_INVALID_ARG, system_errno é zero.
 *
 * @pre out_info deve apontar para uma estrutura shmid_ds válida.
 *
 * @post Em caso de sucesso, *out_info contém os metadados do segmento.
 * Em caso de falha, *out_info permanece inalterado.
 *
 * @note O campo shm_segsz de shmid_ds informa o tamanho do segmento,
 * em bytes, permitindo verificar o requisito de 2048 bytes do projeto.
 *
 * @note A operação não modifica o conteúdo nem as propriedades
 * do segmento consultado.
 */
mpmc_result_t sysv_shm_stat(int shmid, struct shmid_ds *out_info);

/** @} */
#endif
