
/**
 * @file status.c
 * @brief Implementação das descrições textuais dos códigos de status.
 * @ingroup core
 *
 * @details
 * Implementa a conversão dos códigos de status da aplicação em
 * mensagens legíveis, independentes dos mecanismos específicos
 * de comunicação entre processos.
 *
 * As descrições são armazenadas em literais de string estáticos,
 * dispensando alocação dinâmica e gerenciamento de memória pelo
 * chamador.
 *
 * Este módulo não interpreta errno, realiza chamadas de sistema
 * ou apresenta mensagens diretamente ao usuário.
 */

#include "mpmc/status.h"

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
