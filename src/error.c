/**
 * @file error.c
 * @brief Implementação das funções de descrição e formatação de resultados.
 * @ingroup core
 *
 * @details
 * Implementa a conversão dos resultados da aplicação em mensagens
 * legíveis, independentes dos mecanismos específicos de comunicação
 * entre processos.
 *
 * O módulo oferece duas operações complementares:
 * - mpmc_status_str(): retorna a descrição textual de uma categoria
 *   de resultado.
 * - mpmc_error_format(): combina a descrição da categoria com a
 *   causa específica do sistema operacional, quando disponível.
 *
 * As descrições das categorias são armazenadas em literais de string
 * estáticos. A formatação de resultados completos utiliza um buffer
 * fornecido pelo chamador, dispensando alocação dinâmica e
 * gerenciamento adicional de memória.
 *
 * @par Tratamento de erros do sistema operacional
 * As causas de falhas do sistema operacional são obtidas a partir
 * de mpmc_result_t::system_errno e convertidas em texto com strerror().
 *
 * O módulo não consulta o valor corrente de errno para determinar
 * a causa de um erro e não realiza a tradução de códigos errno
 * para categorias mpmc_status_t.
 *
 * @par Responsabilidades
 * Este módulo apenas produz descrições textuais. A interpretação
 * dos erros de chamadas de sistema pertence aos módulos que as
 * executam, enquanto a apresentação e a contextualização das
 * mensagens pertencem à camada de aplicação.
 */

#include "mpmc/error.h"

#include <stdio.h>
#include <string.h>

const char *mpmc_status_str(mpmc_status_t status) {
    switch (status) {
        case MPMC_OK:
            return "Operação concluída com sucesso.";

        case MPMC_ERR_INVALID_ARG:
            return "Argumento inválido.";

        case MPMC_ERR_INVALID_INDEX:
            return "Índice de estrutura lógica inválido.";

        case MPMC_ERR_WOULD_BLOCK:
            return "Operação não bloqueante não pôde prosseguir.";

        case MPMC_ERR_NO_SPACE:
            return "Não há espaço disponível para a operação.";

        case MPMC_ERR_INTERRUPTED:
            return "Operação interrompida.";

        case MPMC_ERR_SHUTDOWN:
            return "Sistema em processo de encerramento.";

        case MPMC_ERR_SYS:
            return "Erro em uma chamada de sistema.";

        case MPMC_ERR_STATE:
            return "Estado interno inválido.";

        default:
            return "Código de status desconhecido.";
    }
}

int mpmc_error_format(mpmc_result_t error, char *buffer, size_t capacity) {
    if (buffer == NULL && capacity > 0) {
        return -1;
    }

    const char *description = mpmc_status_str(error.status);

    if (error.system_errno == 0) {
        return snprintf(buffer, capacity, "%s", description);
    }

    size_t length = strlen(description);

    if (length > 0 && description[length - 1] == '.') {
        length--;
    }

    return snprintf(
        buffer, capacity, "%.*s: %s.", (int)length, description, strerror(error.system_errno)
    );
}
