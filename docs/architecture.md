# Arquitetura e fronteiras entre módulos {#architecture}

@tableofcontents

## Camadas e dependências

O projeto adota uma arquitetura em camadas orientada a processos. As dependências apontam
sempre para baixo:

1. `main.c` inicia a aplicação e interpreta `argc`/`argv`.
2. `app/command` faz o parsing e a validação, sem side effects.
3. `app/repl` gerencia a entrada interativa.
4. `app/supervisor` realiza `fork`, `waitpid`, shutdown e métricas de processos.
5. `workers/producer` e `workers/consumer` executam operações concretas nos filhos.
6. `ipc/transport` oferece API estável para ambos os workers.
7. `ipc/shm_backend` / `ipc/msg_backend` implementam as políticas de cada paradigma.
8. `ipc/shm`, `ipc/sem` e `ipc/msg` encapsulam chamadas de sistema.

`message.h` e `error.h` são tipos transversais e não dependem de outros módulos da aplicação.

## Matriz de responsabilidades

| Módulo | Faz | Não deve fazer |
|---|---|---|
| `main` | Validar argumentos iniciais, configurar supervisor | Implementar IPC ou loop de trabalho |
| `command` | Análise sintática e validação semântica | Chamar `fork` ou `semop` |
| `repl` | Ler linhas e despachar comandos | Remover recursos IPC |
| `supervisor` | Criar/acompanhar filhos, coletar tempos, finalizar | Manipular os detalhes do buffer circular |
| `producer` | Construir mensagens e enviá-las, aguardar ACKs quando pedido | Criar segmentos/filas globais |
| `consumer` | Executar consumo bloqueante/não bloqueante | Modificar diretamente índices dos segmentos |
| `transport` | Despachar para o backend, encapsular ciclo de vida | Implementar parsing |
| `shm_backend` | Buffer circular, sincronização, ACKs e estatísticas compartilhadas | Imprimir prompt do REPL |
| `msg_backend` | Filas de dados/controle, ACKs, publicação de estatísticas | Assumir memória compartilhada entre pai e filho |
| `shm`, `sem`, `msg` | Wrappers finos com tratamento de `errno` | Conter regras de `produz`/`consome` |

## Modelo de processos e propriedade de recursos

- O processo `mpmc` é o **dono** das estruturas IPC globais.
- Para cada comando `produz`/`consome`, é criado um filho.
- O filho herda identificadores IPC e handles após `fork`, mas **não** compartilha
  automaticamente as variáveis normais do heap com o pai.
- `ipc_worker_close` desaloca o estado local do filho; **não** chama `IPC_RMID`. É chamada
  uma única vez pelo ponto de entrada do filho depois que `producer_run()`/`consumer_run()`
  retornam, qualquer que seja o resultado; essas funções não fecham nem destroem o contexto.
- `ipc_destroy` remove recursos persistentes e é executado só pelo supervisor, após
  os filhos terem terminado/recolhidos.
- Se a inicialização falhar na metade, `ipc_init` deve reverter as estruturas já criadas.
- Falhas ao liberar recursos não substituem o primeiro erro do trabalho. Os contratos
  completos, com os retornos de cada função, estão em `transport.h`, `producer.h` e `consumer.h`.

O supervisor registra `pid`, `operation_id`, tipo de comando e timestamps monotônicos no
fork e na coleta do término. Processos que saíram espontaneamente devem ser recolhidos
sem esperar pelo shutdown. No modo de filas, a implementação também precisa
receber eventos de estatísticas **mesmo quando não há novos comandos**: o REPL não
pode permanecer indefinidamente em uma leitura bloqueante de `stdin` enquanto uma
fila de controle fica cheia. O ponto essencial é continuar drenando eventos sem
depender de novas linhas do usuário.

A implementação usa o padrão *self-pipe*: o supervisor cria um pipe não bloqueante e o
tratador de `SIGCHLD` apenas escreve um byte nele (`supervisor_notify_signal`). O REPL espera
com `poll()` em `stdin` e na ponta de leitura do pipe (`supervisor_event_fd`), de modo que o
término de um filho acorda o loop imediatamente. `supervisor_poll_events` drena o pipe e, só
depois, `supervisor_reap` executa `waitpid(..., WNOHANG)` fora do tratador; essa ordem
impede perda de sinais que cheguem durante a coleta. O filho fecha sua cópia do pipe após o
`fork`. Não chamar `printf`, `malloc` ou rotinas de IPC não assíncronas dentro do tratador
de sinal.

Durante `fork()`, `supervisor_dispatch` bloqueia `SIGCHLD`, `SIGINT` e `SIGTERM`. O filho só
os libera depois de restaurar as ações padrão, e o pai os libera após registrar o PID. Sem isso,
um `SIGTERM` enviado por `shutdown` imediatamente após o despacho cairia no tratador herdado do
REPL (que apenas marca uma flag) e o filho o ignoraria, travando o `waitpid()` bloqueante. O
descritor de leitura/escrita do self-pipe é movido para fora de 0..2, para não colidir com
`stdin` quando o processo é iniciado com descritores padrão fechados.

## Resultados dos filhos para o supervisor

Nenhum resultado depende do heap do filho. Erro e cancelamento chegam pelo estado de término
coletado com `waitpid()` (código de saída ou sinal, em `child_report_t`): código zero significa
trabalho concluído por completo; código diferente de zero, falha ou consumo parcial; sinal,
cancelamento (`shutdown` envia `SIGTERM`) ou morte. A quantidade consumida chega por estrutura
lógica, pelo backend: eventos consolidados por `ipc_poll_events()` no modo 2 e contador em memória
compartilhada no modo 1, lidos com `ipc_consumed_count()`. A quantidade de um único trabalho
consumidor existe só em `consumer_result_t`, no filho, e é apresentada por ele; levá-la ao
supervisor exigiria um canal novo, a ser acordado em #16.

## Interface pública versus backends

Os workers usam somente `ipc/transport.h`; a fachada redireciona os métodos para o
backend selecionado. As funções `shm_backend_*` e `msg_backend_*` são internas à
implementação, mesmo quando seus protótipos são publicados em `include/` para
facilitar a documentação técnica.

A implementação de `shm_backend` fica no diretório `src/ipc/shm_backend/`.
O arquivo `lifecycle.c` contém a criação, a inicialização e a liberação dos
segmentos e conjuntos de semáforos. `operations.c` implementa envio, recebimento
e leitura sincronizada da contagem. `shm_ring.c` implementa as operações locais do buffer, cujo
layout e contratos privados ficam em `shm_ring.h`. A interface para a fachada
permanece em `include/mpmc/ipc/shm_backend.h`.

Tipos que precisam ser conhecidos por vários arquivos de implementação devem
ser definidos em um cabeçalho privado no mesmo diretório. `shm_ring_t` já segue
essa organização. `shm_backend_internal.h` define `shm_segment_t` e a representação
completa de `struct shm_backend`, incluindo o vetor local de segmentos, os
identificadores dos conjuntos e os índices MUTEX/EMPTY/FULL. `lifecycle.c` e
`operations.c` compartilham esses tipos. O layout e as operações do buffer continuam
em `shm_ring.h`, independentes dos tipos de contexto do backend.
Um arquivo `shm_backend.c` pode conter rotinas comuns se houver necessidade,
mas definir tipos nele não os torna visíveis aos demais arquivos `.c`.

O layout compartilhado do buffer circular é responsabilidade de `shm_backend`.
Seus metadados de endereçamento e ocupação usam os menores tipos inteiros sem sinal
de largura fixa compatíveis com os limites definidos em @ref ipc_wire_formats.
A implementação deve verificar esses limites, tamanhos e alinhamentos com
`static_assert`; os tipos do layout não impõem a largura dos índices ou ponteiros
usados nas APIs e no estado local dos processos.

Os wrappers `sysv_*` preservam os contratos das chamadas de sistema, verificam
resultados e retornam `mpmc_result_t`, com a categoria em `status` e a causa original em
`system_errno`. A causa é capturada antes de qualquer limpeza e propagada sem depender
do valor corrente de `errno`. Não devem silenciar permanentemente erros nem abstrair
semânticas diferentes de `IPC_NOWAIT`, `EINTR` e `IPC_RMID`.

Identificadores, existência e permissões dos recursos, tamanhos admitidos pelo kernel,
índices de semáforos e quantidades de operações são validados pela própria chamada
de sistema. Os wrappers não repetem essas verificações nem consultam `IPC_STAT`
para antecipar falhas. A tradução de um erro de argumento mantém o `errno` original:
por exemplo, `EINVAL` resulta em `MPMC_ERR_INVALID_ARG` com `system_errno=EINVAL`.
`EFAULT` também identifica argumento inválido; `EIDRM` identifica recurso removido
e resulta em `MPMC_ERR_SHUTDOWN`. As correspondências específicas ficam nos contratos
de cada primitiva, inclusive `EFBIG` de `semop()` como `MPMC_ERR_INVALID_INDEX`.

Permanecem locais somente as restrições introduzidas pelo wrapper: ponteiros que ele
desreferencia, overflow na alocação/cópia de envelopes e conversões de tipos que
perderiam informação. Essas rejeições não têm causa de sistema e usam `system_errno=0`.
Ponteiros passados diretamente ao kernel, como o vetor de `sysv_sem_apply()` e o buffer
de `sysv_shm_stat()`, são verificados pela chamada correspondente. O kernel não conhece
as restrições do modelo da aplicação: índices lógicos, limites dos buffers, estado do
contexto e protocolo de ACK continuam sob responsabilidade das camadas superiores.

O ciclo de vida de `shm_backend` aceita um `shm_cleanup_reporter_t` opcional em
`create`, `worker_close` e `destroy`. A callback recebe cada falha de liberação,
com a operação (detach, remoção de segmento ou remoção de conjunto), índice lógico
1..p, `resource_id` e resultado original. O identificador é um `shmid` ou `semid`,
conforme a operação; não se deve assumir que os dois números coincidam.
O chamador pode copiar os registros para um relatório ou apresentá-los na camada
de aplicação; os registros temporários só são válidos durante cada chamada da
callback. O backend não retém o reporter nem aloca uma lista de erros.

A limpeza tenta todos os recursos e libera o estado local mesmo quando há falhas.
`worker_close` mantém os objetos globais; `destroy` também solicita `IPC_RMID`.
O resultado dessas operações preserva o primeiro erro. No rollback, `create`
usa a auxiliar de cleanup com o prefixo de entradas visitadas, incluindo a parcial,
retorna a causa original da inicialização e comunica as falhas
secundárias pela callback. `mpmc_result_t` continua sendo um valor simples, sem
propriedade de ponteiros ou necessidade de liberação adicional.

As operações de dados reservam EMPTY/FULL e adquirem MUTEX em um único vetor de
`semop()`. Uma aquisição incompleta não deixa reserva parcial. Após alterar o buffer,
outro vetor libera MUTEX e publica o item ou a vaga. Essas duas chamadas não formam
uma transação com a alteração de memória: uma falha na publicação não desfaz a
inserção/retirada. A recepção com `blocking=false` não espera itens futuros, mas
ainda pode aguardar MUTEX. A leitura de `consumed_total` usa o mesmo mutex e só
preenche a saída após liberá-lo com sucesso.

O protocolo de ACK e a recuperação de término de processo entre aquisição e
publicação continuam pendentes. A implementação atual não mascara sinais nesse
intervalo. `worker_close` e `destroy` tratam backend nulo como operação vazia,
retornando sucesso antes de consultar o contexto. A auxiliar privada exige backend
não nulo; os chamadores garantem essa pré-condição, inclusive no rollback de `create`.

O fluxo, os papéis dos tipos e um exemplo completo estão em
[Diagnósticos de cleanup](cleanup_errors.md) (@ref shm_cleanup_diagnostics).

## Falhas e recuperação

1. Parsing inválido: imprimir mensagem e voltar ao prompt, sem criar filhos.
2. Índice lógico inválido: não criar processo nem acessar IPC.
3. Falha no `fork`: registrar erro e manter o REPL operacional.
4. Falha no IPC durante o filho: reportar diagnóstico, liberar recursos locais e sair.
5. Falha durante `ipc_init`: remover somente os recursos já criados.
6. `shutdown`: impedir novos jobs, encerrar filhos pendentes, coletar eventos e
   remover todos os objetos IPC por responsabilidade do pai.
7. `EINTR`: decidir explicitamente entre repetir operação (normal) e abortar por
   encerramento solicitado (shutdown), sem loops infinitos.

## Decisões a revisar quando houver implementação

- Layout binário e capacidade de cada segmento de 2048 bytes.
- Mecanismo de ACK por operação nos dois backends, incluindo ausência de deadlock.
- Volume máximo de jobs simultâneos e limites de IPC do kernel.
- Saída no terminal quando filhos e REPL imprimem simultaneamente.
- Relação entre tempo de operação da aplicação e tempos de execução dos filhos.

Consulte @ref ipc_protocol e @ref testing_plan.

## Pendências de integração

- #13 e #14: completar as garantias de progresso e recuperação do backend de filas.
  A integração atual usa sysv_msg_receive_typed() e os tipos de dados 1/2 para
  distinguir operações sem/com ACK, recebendo por tipo zero para preservar FIFO.
- #16: a fachada de transporte já despacha para os backends e seus contratos estão
  fixados em `transport.h` (#45). Falta propagar resultados e diagnósticos de cleanup
  em `ipc_worker_close`/`ipc_destroy` e completar a integração da aplicação. Os dados
  e as contagens do modo 1 já são exercitados com IPC real; seus ACKs continuam stubs (#11).
- #17: o núcleo de producer_run() e o roteamento já existem e possuem testes com
  transporte substituto. Falta ligar a chamada ao ponto de entrada do filho no supervisor
  e comprovar a execução integrada, incluindo os ACKs reais dos dois backends.
- #10 e #20: definir conjuntamente a espera cancelável e a recuperação de reservas.
  Encerramento global, que descarta o contexto, não equivale ao cancelamento de
  um trabalho com preservação do contexto para as operações restantes.
