/**
 * @file config.h
 * @brief Configuração de inicialização da aplicação MPMC.
 * @ingroup core
 *
 * @details
 * Define a representação tipada dos parâmetros necessários à
 * inicialização do sistema de múltiplos produtores e consumidores.
 *
 * A configuração é construída a partir dos argumentos da linha
 * de comando, após validação de seus formatos e valores.
 *
 * Este módulo apenas representa a configuração, não sendo
 * responsável por interpretá-la, validá-la ou inicializar
 * recursos de comunicação entre processos.
 */

#ifndef MPMC_CONFIG_H
#define MPMC_CONFIG_H

#include <stddef.h>

#include "mpmc/ipc/transport.h"

/**
 * @addtogroup core
 * @{
 */

/**
 * @brief Maior quantidade válida de estruturas lógicas (parâmetro `p`).
 * @details Cada estrutura consome recursos System V do kernel: no paradigma 1, um segmento
 * de memória compartilhada e um conjunto de semáforos; no paradigma 2, uma fila de mensagens.
 * O limite evita pedir ao kernel uma quantidade absurda de recursos e fica abaixo dos
 * valores padrão usuais do Linux (por exemplo, `shmmni` = 4096), deixando folga para outros
 * processos. Não garante que o sistema comporte `p` estruturas: ipc_init() ainda precisa
 * tratar a falha e desfazer o que já criou.
 */
#define MPMC_MAX_PRODUCERS 1024U

/**
 * @brief Configuração de inicialização do sistema MPMC.
 *
 * @details
 * Armazena os parâmetros que determinam a quantidade de estruturas
 * lógicas de comunicação e o paradigma IPC utilizado pela aplicação.
 *
 * Uma configuração validada deve satisfazer as seguintes invariantes:
 * - 1 <= producer_count <= MPMC_MAX_PRODUCERS.
 * - mode == IPC_SHARED_MEMORY ou mode == IPC_MESSAGE_QUEUE.
 *
 * @note A existência desta estrutura não garante que seus campos
 * tenham sido validados. A responsabilidade pela validação pertence
 * ao código que constrói a configuração.
 *
 * @note A quantidade de estruturas IPC corresponde a producer_count,
 * independentemente do paradigma selecionado.
 */
typedef struct {
    size_t producer_count; /**< Quantidade positiva de estruturas lógicas de produtores. */
    ipc_mode_t mode;       /**< Paradigma IPC selecionado para a execução. */
} mpmc_config_t;

/** @} */

#endif
