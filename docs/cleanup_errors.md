# Diagnósticos de cleanup do backend shm {#shm_cleanup_diagnostics}

@tableofcontents

Esta página aprofunda o reporter específico de SHM. Comece pelo
[fluxo comum de diagnósticos](diagnostics.md); o [guia de leitura](reading_guide.md)
situa este contrato no ciclo de vida do backend.

O observador comum de @ref flow_diagnostics complementa os reporters específicos
abaixo e é utilizado pela aplicação completa. Ele cobre também ACK, filas,
transporte, workers e coordenação, incluindo chamadas com reporter específico nulo.

## O que é a callback

`shm_cleanup_error_fn` é um tipo de ponteiro para função em C. O chamador escreve
uma função com essa assinatura e fornece seu endereço no campo `on_error`.
O campo `user_data` leva o endereço de um objeto do chamador até essa função,
como um buffer onde os diagnósticos serão copiados.

| Elemento | Papel |
|---|---|
| `shm_cleanup_error_t` | Valores de uma falha: operação, índice lógico 1..p, `resource_id` e resultado com `errno` capturado. |
| `shm_cleanup_error_fn` | Assinatura da função que recebe uma falha individual. |
| `shm_cleanup_reporter_t` | Reúne o endereço dessa função (`on_error`) e o estado do chamador (`user_data`). |

Ao encontrar uma falha, o backend chama `reporter->on_error(&error, reporter->user_data)`.
Essa é uma chamada normal de função, executada imediatamente no fluxo do cleanup.
A callback retorna `void`: sua tarefa é registrar ou apresentar a falha. Depois
que ela retorna, o backend continua a limpeza dos demais recursos.

## Resultado da operação e diagnósticos individuais

O retorno `mpmc_result_t` orienta o fluxo de controle. A callback fornece os
detalhes de cada falha de liberação. São informações complementares:

| Função | Resultado de retorno | Diagnósticos enviados à callback |
|---|---|---|
| `shm_backend_create()` | Sucesso ou erro que impediu a inicialização. | Falhas de desassociação e remoção de conjuntos/segmentos durante o rollback. |
| `shm_backend_worker_close()` | Sucesso ou primeira falha de desassociação. | Todas as falhas de `detach`; segmentos e conjuntos globais são preservados. |
| `shm_backend_destroy()` | Sucesso ou primeira falha de liberação. | Todas as falhas de desassociação e remoção de conjuntos/segmentos. |

`operation` determina a classe de `resource_id`:

| Operação | Chamada | Identificador |
|---|---|---|
| `SHM_CLEANUP_DETACH` | `shmdt()` | `shmid` do segmento associado ao endereço |
| `SHM_CLEANUP_REMOVE` | `shmctl(IPC_RMID)` | `shmid` do segmento |
| `SHM_CLEANUP_SEM_REMOVE` | `semctl(IPC_RMID)` | `semid` do conjunto |

IDs de segmentos e conjuntos são independentes. Zero é válido em ambas as classes.

Por exemplo, se a associação do terceiro segmento falhar e a remoção do primeiro
segmento também falhar no rollback, `create()` retorna o erro da associação. A
callback recebe o erro da remoção com `producer_id == 1` e o `shmid` correspondente
em `resource_id`.
O erro original de criação não é repetido como um diagnóstico de cleanup.

As categorias traduzidas pelos wrappers são preservadas: por exemplo, um `EINVAL`
de `shmdt()` aparece como `MPMC_ERR_INVALID_ARG`, com `system_errno=EINVAL`.
Um `EIDRM` de `shmctl(IPC_RMID)` ou `semctl(IPC_RMID)` aparece como
`MPMC_ERR_SHUTDOWN`, com a causa original.
O cleanup não converte essas categorias novamente para `MPMC_ERR_SYS`.

Operações bem-sucedidas não geram chamadas da callback. Um `reporter == nullptr` ou
`reporter->on_error == nullptr` desabilita os diagnósticos individuais, mantendo o
resultado de retorno. Sem observador comum instalado, erros secundários do rollback não são guardados.

## Duração dos dados e propriedade

- O backend recebe o reporter emprestado somente durante a chamada. Ele não o armazena.
- `user_data` pertence ao chamador e pode ser nulo se a callback não precisar de estado.
- O ponteiro `error` aponta para um registro temporário. Para guardá-lo, copie
  `*error` para armazenamento próprio. A cópia contém apenas valores e não depende
  do backend depois que ele for liberado.
- Passe o reporter novamente a cada operação que deve produzir diagnósticos.
  Fornecê-lo a `create()` não o registra automaticamente para um futuro `destroy()`.
- A callback deve retornar normalmente e manter o reporter e seu estado válidos
  até o fim da chamada. Ela não deve tentar destruir novamente o backend em limpeza.

`destroy()` e `worker_close()` liberam o estado local mesmo quando retornam erro.
Ambas aceitam backend nulo como operação vazia: retornam `MPMC_OK`, com
`system_errno=0`, sem liberar recursos nem chamar a callback. Essa verificação
ocorre nos pontos públicos, antes de consultar o contexto; a auxiliar privada
de cleanup exige backend não nulo.
O chamador deve deixar de usar o backend e zerar sua variável. Se alguma operação
de liberação falhar, os diagnósticos identificam os recursos envolvidos; retornar
erro não preserva o backend para uma segunda tentativa de destruição.

## Exemplo: copiar os diagnósticos para um buffer do chamador

O exemplo reserva espaço antes da limpeza. Se o buffer encher, a callback conta
os registros omitidos e continua retornando normalmente. O backend continua
comunicando cada falha, sem alocar memória para manter uma lista.

```c
#include "mpmc/ipc/shm_backend.h"

#include <stdio.h>
#include <stdlib.h>

#define CLEANUP_LOG_CAPACITY 16

typedef struct {
    shm_cleanup_error_t items[CLEANUP_LOG_CAPACITY];
    size_t count;
    size_t omitted;
} cleanup_log_t;

static void collect_cleanup_error(const shm_cleanup_error_t *error, void *user_data) {
    cleanup_log_t *log = user_data;

    if (log->count < CLEANUP_LOG_CAPACITY) {
        log->items[log->count++] = *error;
    } else {
        ++log->omitted;
    }
}

int main(void) {
    cleanup_log_t log = {};
    const shm_cleanup_reporter_t reporter = {
        .on_error = collect_cleanup_error,
        .user_data = &log,
    };
    shm_backend_t *backend = nullptr;
    mpmc_result_t result = shm_backend_create(&backend, 2, &reporter);

    if (result.status == MPMC_OK) {
        /* Usar o backend antes de encerrar. */
        result = shm_backend_destroy(backend, &reporter);
        backend = nullptr;
    }

    /* O erro principal é independente dos diagnósticos de cleanup. */
    if (result.status != MPMC_OK) {
        char text[256];
        (void)mpmc_error_format(result, text, sizeof text);
        fprintf(stderr, "Resultado da operação: %s\n", text);
    }

    /* As cópias continuam disponíveis depois da liberação do backend. */
    for (size_t i = 0; i < log.count; ++i) {
        const shm_cleanup_error_t *error = &log.items[i];
        const char *operation;
        switch (error->operation) {
            case SHM_CLEANUP_DETACH:
                operation = "detach";
                break;
            case SHM_CLEANUP_REMOVE:
                operation = "remove segmento";
                break;
            case SHM_CLEANUP_SEM_REMOVE:
                operation = "remove conjunto";
                break;
            default:
                operation = "operação desconhecida";
                break;
        }
        char text[256];
        (void)mpmc_error_format(error->result, text, sizeof text);
        fprintf(
            stderr,
            "%s, estrutura %zu, recurso %d: %s\n",
            operation,
            error->producer_id,
            error->resource_id,
            text
        );
    }
    if (log.omitted != 0) {
        fprintf(stderr, "%zu diagnósticos não couberam no buffer.\n", log.omitted);
    }

    return result.status == MPMC_OK ? EXIT_SUCCESS : EXIT_FAILURE;
}
```

A atribuição `.on_error = collect_cleanup_error` fornece o endereço da função;
ela não executa a coleta naquele ponto. `.user_data = &log` permite que a callback
encontre esse buffer quando for chamada. O registro é armazenado pela atribuição
`log->items[log->count++] = *error`, que copia todos os campos.

Os contratos públicos permanecem em `include/mpmc/ipc/shm_backend.h`. O arquivo
`src/ipc/shm_backend/lifecycle.c` descreve a implementação e o contrato das auxiliares
privadas de criação e liberação dos recursos.
A representação do contexto e dos registros de segmentos fica em
`src/ipc/shm_backend/shm_backend_internal.h`.
