# Guia de leitura da documentação {#documentation_guide}

@tableofcontents

Comece pelo [README do projeto](../README.md) para compilar, executar os dois
paradigmas e experimentar os comandos. Este guia organiza a leitura para entender
como um comando percorre os módulos e como os processos devolvem seus resultados.

## Ordem recomendada

| Ordem | Documento | O que deve ficar claro ao terminar |
|---|---|---|
| 1 | [Arquitetura](architecture.md) | Responsabilidade de cada camada, dependências, fluxo completo de um comando e propriedade dos recursos no pai e nos filhos. |
| 2 | [Protocolo](protocol.md) | Identidades, FIFO, diferença entre espaço e ACK, layouts SHM/MSG, sincronização e cancelamento. |
| 3 | [Diagnósticos](diagnostics.md) | Relação entre resultado e callbacks, primeira causa, erros de cleanup, transporte dos diagnósticos ao pai e apresentação. |
| 4 | [Cleanup SHM](cleanup_errors.md) | Uso do reporter específico do backend, duração dos registros e diferença em relação ao observador comum. É um aprofundamento para quem vai alterar o ciclo de vida SHM. |
| 5 | [Testes](testing.md) | Quais propriedades cada suíte verifica, como executar e quais são os limites da validação. |
| 6 | [Rastreabilidade](traceability.md) | Onde cada requisito do enunciado foi implementado e quais testes fornecem evidência. |

Para conferir a entrega contra o enunciado, também é possível começar pela
rastreabilidade e seguir os módulos e testes indicados em cada linha. Para alterar
um componente, leia arquitetura e protocolo antes de seu contrato público.

## Ordem de leitura do código

Leia primeiro os cabeçalhos, que definem os contratos, e depois as implementações.
A sequência abaixo acompanha o fluxo de execução; os tipos comuns são consultados
ao longo dela. Cabeçalhos públicos ficam em `include/mpmc/`; os caminhos de
cabeçalhos privados são indicados explicitamente. No HTML gerado, os nomes dos
arquivos levam aos contratos correspondentes.

| Etapa | Contratos e arquivos | Pergunta que respondem |
|---|---|---|
| Modelo comum | `config.h`, `message.h`, `error.h` | O que configura o programa, identifica uma mensagem e representa um resultado? |
| Entrada | `src/main.c`, `app/repl.h`, `app/command.h` | Como a entrada textual vira um trabalho validado? |
| Coordenação | `app/supervisor.h`, `src/app/worker_event.h` | Quem cria os filhos e os recursos, recebe resultados, recolhe processos e encerra a execução? |
| Trabalho no filho | `workers/producer.h`, `workers/consumer.h`, `src/workers/producer_routing.h` | Como um trabalho envia ou consome mensagens usando apenas a fachada IPC? |
| Fachada | `ipc/transport.h` | Quais operações e regras de erro são comuns aos dois paradigmas? |
| Backends | `ipc/shm_backend.h`, `ipc/msg_backend.h` | Como cada paradigma implementa dados, ACKs, contagens e ciclo de vida? |
| Interior do SHM | `src/ipc/shm_backend/shm_backend_internal.h`, `src/ipc/shm_backend/shm_ring.h` | Quais tipos são compartilhados entre lifecycle, operações e ACK, e quem sincroniza o ring? |
| Primitivas | `ipc/shm.h`, `ipc/sem.h`, `ipc/msg.h` | Como chamadas System V são encapsuladas e seus erros traduzidos? |
| Diagnóstico e apresentação | `diagnostic.h`, `src/app/diagnostics.h`, `src/app/job_outcome.h` | Como cada falha é preservada, encaminhada ao pai e convertida em texto? |

No backend SHM, leia `lifecycle.c` para aquisição/rollback/liberação,
`operations.c` para reserva e publicação de dados, `shm_ring.c` para manipulação
local do buffer e `ack.c` para admissão e conclusão dos lotes. No backend MSG,
essas responsabilidades ficam reunidas em `msg_backend.c`. Os detalhes dos
algoritmos ficam nos comentários de implementação desses arquivos.

## Como navegar no Doxygen

Execute `make docs` e abra `build/docs/html/index.html`. A página inicial é o
README do projeto, que aponta para este guia. Os grupos **Aplicação**, **Workers**,
**IPC**, **Backends**, **Primitivas System V** e **Modelo comum** organizam os
contratos por responsabilidade. As páginas dos módulos remetem à arquitetura,
ao protocolo e aos tipos relacionados; a navegação por arquivos também inclui
os cabeçalhos privados e os fontes dos testes.

## Onde documentar cada informação

- `architecture.md`: responsabilidades, dependências e fluxo entre processos.
- `protocol.md`: invariantes, formatos compartilhados e regras de sincronização.
- Cabeçalhos: contrato das funções e tipos, incluindo propriedade, pré-condições,
  efeitos, resultados e `system_errno`.
- Fontes `.c`: algoritmo e decisões específicas da implementação, com referência
  ao contrato do cabeçalho, sem repeti-lo.
- `diagnostics.md` e `cleanup_errors.md`: observadores e preservação de múltiplas falhas.
- `testing.md` e `traceability.md`: evidências de comportamento e vínculo com o enunciado.

Uma mudança de fluxo deve atualizar tanto o contrato afetado quanto a visão de
arquitetura/protocolo e sua evidência de teste. Referências a issues explicam a
origem do trabalho; o comportamento atual deve ser descrito sem depender de sua
numeração nem apresentar uma integração já existente como tarefa futura.
