# Estratégia de testes {#testing_plan}

@tableofcontents

## Objetivo

Testar o comportamento observável exigido pelo enunciado separadamente em memória
compartilhada (modo 1) e troca de mensagens (modo 2). Um teste de encerramento bem-sucedido
não é suficiente para provar correção concorrente.

## Testes unitários

| ID | Componente | Casos |
|---|---|---|
| U01 | `command` | `produz`, `consome`, `shutdown` válidos |
| U02 | `command` | Comandos vazios, desconhecidos, tokens adicionais, caracteres não numéricos |
| U03 | `command` | `n_msgs` igual a 0, 1, 1024, 1025 |
| U04 | `command` | `ack` igual a 0, 1, 2, 3 |
| U05 | `command` | `produz 0` aceito; `consome 0` rejeitado |
| U06 | `command` | `i` maior que `p` rejeitado |
| U07 | `producer` | Mapeamento *striped* com `n` divisível/não divisível por `p` (`test_producer_routing`: função pura `producer_route_destination`, `p=1`, `n<p`, `n=1024`, pré-condições `p>0` e ordinal de base 1) |
| U08 | `message` | Sequências crescentes por PID de produtor; timestamps preenchidos |
| U09 | `status` | Conversão de códigos de retorno a mensagens legíveis |
| U10 | `message`/ACK | Identidade `(operation_id, sender_pid, sequence)` preservada até o ACK; `sent_at` não é usado como chave |
| U11	| `sem` | Criação, inicialização, operações P/V, IPC_NOWAIT, índice inválido, interrupção por EINTR e remoção de semáforos |
| U12 | `shm_ring` | Inicialização, capacidade de 63 mensagens, vazio/cheio, FIFO com voltas e mensagens pendentes, contagem acumulada e limites do segmento |
| U13 | `shm_backend` | Inicialização de todos os cabeçalhos em áreas simuladas alinhadas, previamente preenchidas com bytes não nulos |
| U14 | `transport` | Fachada com backends substitutos (`test_transport`): despacho de cada operação ao backend do modo, com os argumentos exatos; propagação de `status`/`system_errno` e dos dados de saída; contexto nulo, modo inválido, `p` zero e saída nula; falha de alocação e de criação do backend em `ipc_init()` sem vazar nem destruir o backend; `ipc_worker_close()` sem remover recursos globais e `ipc_destroy()` sem chamar o fechamento do filho |
| U15 | `protocol` | Propriedades determinísticas (`test_protocol_properties`): textos de status distintos e formatação com `errno`/truncamento; composição de metadados, chave `(operation_id, sender_pid, sequence)` sem colisões (e, em `test_supervisor`, `operation_id` único e consecutivo para 20 trabalhos despachados) entre operações intercaladas; sequência por processo com filhos reais sincronizados por pipes (sem zumbis); distribuição *striped* `1 + ((k-1) % p)` exaustiva para `p` 1..40, `n` 1..130 e limites até 1024; capacidade de 63 mensagens, alinhamento e área de guarda do segmento de 2048 bytes |
| U16 | `supervisor`/relatório | Fixture controlada (`test_supervisor_report`): três filhos simulados com saída normal, término por sinal e falha; associação por PID, `operation_id` e tipo sem assumir ordem de coleta; durações monotônicas com virada de segundo, regressão e falha do relógio; duração total do processo principal com e sem relógio; contagens controladas para estruturas 1..p; substituição de `fork()`, `waitpid()`, relógio e fachada IPC, sem criar filhos reais ou recursos System V |
| U17 | `shm_backend` | Criação/SETVAL de cada conjunto, rollback por etapa, IDs independentes e falhas de remoção de semáforos |
| U18 | `shm_operations` | FIFO, metadados, isolamento, totais, interrupção de esperas reais, concorrência e falhas de aquisição/publicação |

## Testes de integração (repetir para modos 1 e 2)

| ID | Cenário | Propriedade esperada |
|---|---|---|
| I01 | Inicialização e `shutdown` | Recursos criados/removidos e saída normal |
| I02 | Produzir antes de consumir | FIFO e preservação das mensagens |
| I03 | Consumir antes de produzir com `ack=1` | Consumidor espera e depois conclui |
| I04 | Consumir sem mensagens com `ack=2` | Erro e total consumido `0`, sem bloqueio |
| I05 | Consumo parcial com `ack=2` | Erro e total efetivamente consumido |
| I06 | Produtor com `ack=1` | Só conclui após consumo das próprias mensagens |
| I07 | Produtor com `ack=2` | Não espera confirmação de consumo |
| I08 | Vários consumidores na mesma estrutura | Mensagem não é consumida duas vezes |
| I09 | Vários produtores na mesma estrutura | Não há perda, duplicidade ou corrupção |
| I10 | `produz 0 n ...` | Distribuição 1..p cíclica, com contagem correta |
| I11 | Saturação do buffer/fila | Backpressure e retomada após consumo |
| I12 | `shutdown` com consumidor bloqueado | Filhos recolhidos e IPC removido |
| I13 | `shutdown` com produtor aguardando ACK | Filhos recolhidos e IPC removido |
| I14 | `shutdown` com produtor bloqueado por buffer cheio | Encerramento sem travamento |
| I15 | Comando para índice inexistente | Erro, nenhuma estrutura extra criada |
| I16 | Repetir executar/desligar | Nenhum recurso IPC residual |
| I17 | `n_msgs=1024` | Limite superior funciona sem corrupção |
| I18 | Paralelismo e variação de escalonamento | Sem deadlock; totais reproduzíveis |
| I19 | Estatísticas finais | Soma por estrutura equivale aos consumos válidos |
| I20 | Multiprodutores com ACK concorrente | ACKs não se cruzam entre trabalhos |
| I21 | Tempos de vida dos filhos e do `mpmc` | Relatório apresenta durações não negativas e coerentes |
| I22 | Mensagem e relatório final | ID/PID do produtor correlacionam com suas mensagens; relatório de consumidor não sobrescreve os metadados do produtor |
| I23 | Inicialização com `p` no limite e acima dele | `p = MPMC_MAX_PRODUCERS` é aceito nos dois paradigmas; `p` maior, inclusive overflow de `size_t`, é recusado com erro e uso, sem iniciar o REPL |

## Propriedades a verificar

- O segmento de memória compartilhada tem 2048 bytes, cabeçalho alinhado de 16 bytes
  e 63 mensagens de 32 bytes a partir do deslocamento 16. Os tipos e deslocamentos
  do layout devem ser conferidos com `sizeof`, `offsetof` e `static_assert`.
- Os campos `head`, `tail`, `count` e `capacity` usam `uint8_t`, com capacidade
  representável até o estado cheio. Após inserções e retiradas que deem várias
  voltas no buffer, os índices permanecem em `0..capacity-1` e a ocupação em
  `0..capacity`, preservando a ordem FIFO. `consumed_total` continua acumulando
  além da capacidade do buffer e do limite de mensagens por comando.
- `consumidas(i) <= produzidas(i)` sempre, para cada estrutura `i`.
- O número de mensagens retiradas globalmente equivale à soma dos relatórios dos consumidores.
- Para cada mensagem produzida, há no máximo um consumidor correspondente.
- `sender_pid`, `sequence` e `sent_at` sobrevivem sem alteração ao transporte; a fila lógica é identificada separadamente pelo índice `producer_id`.
- Cada ACK corresponde a uma chave de mensagem distinta `(operation_id, sender_pid, sequence)`
  cuja operação solicitou confirmação; o ID do trabalho consumidor não é usado como
  ID de produção. Alternar mensagens com e sem ACK na mesma estrutura preserva FIFO
  e gera confirmações somente para as operações que as solicitaram. No modo 2,
  verificar o mtype recebido e a leitura por tipo zero, sem filtragem por política de ACK.
- O relatório do supervisor usa PID e `operation_id` do processo para identificar o trabalho; estatísticas de consumo continuam agrupadas pela fila lógica.
- Se um produtor conclui normalmente com `ack=1`, todas as mensagens daquele trabalho
  aparecem entre as efetivamente consumidas.
- Após `shutdown` completo, não há filhos zumbis do `mpmc` nem objetos System V
  pertencentes àquela execução.
- Uma operação bloqueada pode ser cancelada pelo encerramento.

## Ferramentas de diagnóstico

```bash
ipcs -m  # segmentos de memória compartilhada
ipcs -s  # conjuntos de semáforos
ipcs -q  # filas de mensagens

# Somente em ambiente controlado, para diagnosticar:
ps -o pid,ppid,stat,cmd -C mpmc
```

Compare o inventário **antes e depois** da execução. Não remova indiscriminadamente
recursos IPC de outros usuários/processos por `ipcrm`.

## Build e automação

```bash
make debug
make unit   # testes de command, supervisor, msg, sem, error, wrappers, backend, buffer shm e roteamento cíclico do produtor
make test   # unitários + smoke + integração do REPL
make docs
```

O `shutdown` imprime em stdout o relatório final (`supervisor_print_report()`): uma linha por
filho recolhido (PID, operação, tipo, término e duração `S.mmm s`), a contagem de filhos e o
tempo total de operação do `mpmc`. O formato é verificado nos testes unitário e de integração.

A fixture `tests/unit/test_supervisor_report.c` (U16) compila um objeto dedicado de
`supervisor.c`, substituindo `fork()`, `waitpid()`, `clock_gettime()` e as funções da
fachada IPC por implementações controladas. Ela verifica estados de término, associação
dos dados por processo, durações monotônicas não negativas, duração total e contagens
fixas para as estruturas de 1 a `p`, sem depender da ordem em que os filhos são recolhidos
ou de visibilidade do heap privado dos filhos. As contagens são valores injetados pela
fixture: isso não valida a consolidação real dos backends nem a drenagem de eventos, que
permanecem fora deste teste e no escopo da issue #21.

Os testes de resultados verificam `status` e `system_errno` separadamente: sucesso e
validação sem causa de sistema usam zero; falhas preservam a causa original mesmo após
limpeza ou mudança do `errno` corrente. São exercitados `EAGAIN`, `ENOMSG`, `EINTR`,
`E2BIG`, falha de `fork()` e falha de escrita do relatório. A formatação também cobre
consulta de capacidade, truncamento e diagnósticos com causa de sistema.

Os testes de `sem` verificam valores e índices, espera por zero, P/V, IPC_NOWAIT,
vetores atômicos, limites representáveis, preservação de errno e permissão de alteração
sem leitura (0200). A interrupção real usa alarm() e sigaction() sem SA_RESTART. Um objeto de
sem.c compilado com -Dsemop=mpmc_test_semop permite injetar erros, inclusive EIDRM,
E2BIG e EACCES, e comprovar que os argumentos chegam à chamada sem alterar o objeto
da aplicação. A remoção de um conjunto com filhos realmente
bloqueados ainda deve ser exercitada pelos testes de integração.

Os testes de `shm` exercitam os cinco wrappers, verificam segmentos independentes de
2048 bytes com permissões 0600 via `IPC_STAT` e conferem a preservação dos parâmetros
de saída em falhas. Um cenário com `fork()` verifica que o filho desassocia apenas
seu vínculo, que o mapeamento do pai continua válido após `IPC_RMID` e que a última
desassociação destrói o segmento. O cleanup de segurança remove somente segmentos
criados pelo teste; a coleta do filho usa `alarm(3)` e espera bloqueante por sinais
para detectar travamentos, sem consultas periódicas.

Nos wrappers, IDs negativos, tamanho zero de segmento, vetor de semáforos vazio/nulo
e buffer nulo de `IPC_STAT` são rejeitados pelo kernel. Os testes conferem a categoria
traduzida e o `errno` original, distinguindo essas falhas das rejeições locais de
ponteiros usados em C, overflow do envelope ou estreitamento de tipos. Filas também
exercitam tipos inválidos e o limite MSGMAX diretamente no kernel; uma recepção
rejeitada localmente não remove a mensagem pendente.

Os testes de `shm_backend` usam alocações e wrappers substituídos apenas no objeto
de teste para injetar falhas de `malloc()`, `calloc()`, criação e associação em
cada posição. Os cenários existentes cobrem tratamento do identificador zero,
liberação somente local pelo worker e continuidade após múltiplas
falhas de cleanup. Uma callback copia os diagnósticos e altera `errno` para verificar
que cada falha e o erro original da criação continuam preservados. Os testes também
conferem as categorias traduzidas de argumento inválido e recurso removido, que a
limpeza libera o estado local uma única vez e que não aloca diagnósticos.
As áreas de dados simuladas usam `alignas(shm_ring_t)` no membro do vetor de bytes,
garantindo o alinhamento também para cada elemento do vetor de segmentos. A criação
é exercitada após preencher essas áreas com bytes não nulos e deve inicializar
todos os cabeçalhos com índices, ocupação e total consumido zerados e capacidade 63.

Os conjuntos simulados têm identificadores independentes dos segmentos, incluindo
ID zero. A suíte verifica os valores iniciais MUTEX=1, EMPTY=capacity e FULL=0,
injeta falha na criação e em cada SETVAL de cada entrada e confere o rollback dos
recursos efetivamente criados. Os diagnósticos usam `resource_id`, distinguem
segmento/conjunto e preservam o primeiro erro mesmo quando o cleanup também falha.
Fechamento com NULL não aloca, não libera recursos e não chama o reporter.

`test_shm_operations` usa segmentos e semáforos reais para FIFO após volta dos índices,
metadados, isolamento entre estruturas e totais independentes. Interrompe com
`alarm(1)` as esperas por itens, vagas e mutex; verifica ausência de reservas parciais
e preservação das saídas. Dois produtores e dois consumidores exercitam a mesma
estrutura, com 128 identidades conferidas sem perda ou duplicação. Um pipe leva as
observações ao pai; sua ordem não é usada como evidência da ordem de retirada.

Somente o objeto de operações do teste substitui `sysv_sem_apply`/`sysv_sem_change`:
normalmente delega às primitivas reais, mas permite falhar uma aquisição ou publicação.
Os cenários distinguem buffer/saída inalterados de uma alteração já executada antes
de a publicação falhar, inclusive na leitura do contador. Após cada falha fatal
injetada, o teste destrói o contexto; não tenta repetir a operação nem comprova
recuperação dos contadores. Isso corresponde à obrigação de descarte descrita em
@ref shm_operation_failures. Os testes de interrupção da aquisição e recepção vazia
verificam que esses retornos não deixam reservas parciais nem alteram o buffer.
A comunicação de falhas fatais ao supervisor e o shutdown coordenado continuam
pendentes de integração na #20 e não são comprovados por esta suíte.
O cleanup de segurança encerra/recolhe apenas os filhos próprios e remove apenas
os recursos do contexto.
Esses testes não comprovam o protocolo de ACK, que continua pendente.

Para Linux e macOS, SIGALRM é instalado com `sigaction()` sem SA_RESTART e fica
desbloqueado durante os cenários. A coleta usa `alarm()` e `sigwait()` com
SIGCHLD/SIGALRM bloqueados, instalando antes do fork uma ação capturável de SIGCHLD.
Isso evita depender de sinais ignorados, que o macOS descarta antes de `sigwait()`;
veja a [documentação da Apple](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/sigwait.2.html).
A suíte cancela alarmes, drena um SIGALRM eventualmente pendente e restaura ações e
máscara originais. Não exige `sigtimedwait()`, indisponível no macOS.

Os testes de `shm_ring` usam alocações locais com 2048 bytes úteis, sentinelas antes
e depois do segmento e verificação dos 16 bytes reservados após a última mensagem.
Conferem a inicialização, ocupação máxima, transição para vazio, reutilização, FIFO
com retorno dos índices ao início ainda com mensagens pendentes, buffers independentes
e várias voltas com mais de 1024 consumos. Todos os campos das mensagens são
conferidos sem comparar bytes de padding. As operações respeitam suas pré-condições;
esses testes locais são complementados pela cobertura sincronizada de `shm_operations`.

Os testes de filas verificam a recepção do mtype junto ao corpo, FIFO entre tipos
intercalados e preservação das saídas em ENOMSG/E2BIG. O teste do backend de filas
cria dois consumidores antes do registro de ACK no produtor e confere a contagem
consolidada no pai e a confirmação somente da operação registrada. A ausência de
ACK para a outra operação é verificada por espera interrompida com alarm(), sem polling.

Para conferir acessos à memória e alinhamento com AddressSanitizer e
[UndefinedBehaviorSanitizer](https://clang.llvm.org/docs/UndefinedBehaviorSanitizer.html),
sem misturar objetos com o build normal:

```bash
make MODE=debug BUILD_DIR=build/sanitize \
  CFLAGS='-std=c23 -Wall -Wextra -Wpedantic -Wshadow -O1 -g -DDEBUG -fsanitize=address,undefined -fno-sanitize-recover=all' \
  build/sanitize/unit/test_shm_ring build/sanitize/unit/test_shm_backend
build/sanitize/unit/test_shm_ring
build/sanitize/unit/test_shm_backend
```

`tests/unit/test_transport.c` (U14) liga `transport.c`, compilado com `calloc()`/`free()`
redirecionados, a backends substitutos definidos no próprio teste, e a espiões de `msgget()`,
`msgsnd()`, `msgrcv()`, `shmget()`, `semget()` e `semop()`. Ele prova o despacho e os contratos
da fachada sem depender de nenhuma primitiva System V funcional; FIFO, ACK e limpeza reais dos
dois paradigmas continuam nos testes dos backends (`test_transport_contract` e integração).

O alvo `test` também executa `tests/integration/test_repl.c`, que abre um pseudo-terminal
para verificar o prompt inicial, seu redesenho após o aviso de término de um filho e o
relatório final. A entrada em terminal complementa os cenários por pipes e FIFOs do
script `test_repl.sh`, incluindo a validação do limite de estruturas na inicialização.

Os testes completos de integração de FIFO, ACKs e concorrência dos backends da matriz acima ainda
dependem das respectivas implementações. Os *smoke tests* validam somente inicialização
e shutdown; os cenários do REPL não substituem a verificação do transporte IPC completo.
Scripts automatizados devem capturar a saída e impor limites de tempo para detectar travamentos.
