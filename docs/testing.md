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
| U18 | `shm_operations` | FIFO, metadados, isolamento, totais, interrupção de esperas reais, concorrência e falhas de aquisição/publicação; envio em estrutura cheia que bloqueia só o remetente e retoma após consumo; recepção bloqueante retomada somente por dado da própria estrutura (processos filhos coordenados por eventos); backend com 1024 estruturas independentes e índice 1025 inválido |
| U19 | `shm_stress` | Estresse do backend SHM real (`test_shm_stress`): 4 rodadas criam/destroem backends com 1..4 estruturas cheias; 6 produtores e 2 consumidores por estrutura; 30375 verificações de identidade única, sem perda nem duplicação, destino/remetente corretos, FIFO por (consumidor, produtor) e totais exatos, sem filhos remanescentes; backend espectador preservado |
| U20 | `msg_stress` | Estresse do backend MSG real (`test_msg_stress`): argumentos e índices inválidos; 3 rodadas criam/destroem backends com 1..3 filas cheias (limite do kernel via `IPC_NOWAIT`); 4 produtores e 2 consumidores por fila; identidade única, sem perda nem duplicação, destino/remetente/metadados corretos, FIFO por (consumidor, produtor), totais exatos por `poll_events`, sem filhos remanescentes; backend espectador preservado |
| U20a | `msg_ack_concurrency` | Correlação de ACK no backend MSG real (`test_msg_ack_concurrency`): 3 operações concorrentes com filas de dados/ACK/eventos reduzidas por `IPC_SET` (contagens independentes, sem espera circular); ACK antecipado preservado e ACK de outra operação não satisfaz a espera; `ack=2` sem ACK e `ack_wait` recusada; ACKs tardios travam o consumidor sem `ack_abandon` (controle negativo) e são liberados com ele; contrato e seletividade de `ack_abandon`. Prazos por `alarm()` e `SIGKILL`, um backend por cenário, filas conferidas por `msgctl` após `destroy` |
| U20b | `msg_ack_lifecycle` | Critérios de aceite de ACK ponta a ponta no modo MSG (`test_msg_ack_lifecycle`), com `producer_run()`/`consumer_run()` em processos reais sobre `ipc_*`: **C1** 4 produtores `ack=1` (um *striped*) + 1 `ack=2`, 4 consumidores, sem troca de ACKs; **C2** produtor `ack=1` permanece vivo sem consumo e com consumo parcial, e só conclui após o consumo total; consumir mensagens de outro produtor não o libera; **C3** dados (3), ACK (2) e eventos (2) lotados sem deadlock; **C4** `SIGTERM` + `ack_abandon`, remoção da fila de ACK (libera `ack_wait` e consumidor preso em `msgsnd`) e da fila de dados (libera o laço de envio). Validado por mutação: `msgsnd` bloqueante sem drenagem e ACK sem correlação por `mtype` fazem o teste falhar |
| U20c | `consumer_integration` | `consumer_run()` sobre SHM e MSG reais (`test_consumer_integration`): zero disponível, consumo parcial sem reinserção, `n` completo deixando o restante, `EINTR` no bloqueante com parcial preservado, `ipc_worker_close()` sem afetar o supervisor e ACK emitido pelo backend (modo 2). No modo 2, consumo sem espera de 2560 mensagens com `Q_evt` cheia e ninguém drenando: nenhuma chamada passa de 1 s, o filho encerra quando o supervisor drena e o total do backend não perde nenhum consumo. Validado por mutação: `msgsnd` bloqueante do evento no recebimento sem espera faz o teste falhar |
| U20d | `consumer_concurrency` | Consumidores e produtores concorrentes em processos reais, SHM e MSG (`test_consumer_concurrency`): 4 produtores × 200 mensagens e 6 consumidores bloqueantes por `consumer_run()`, com `operation_id` do consumidor igual ao de um produtor; no modo 2 os produtores usam `ack=1` e todos concluem; unicidade por conteúdo (`sender_pid`, `sequence`) sem duplicata; 5 consumidores sem espera dividindo um estoque de 60 somam exatamente 60, com status coerente com o parcial. O ACK do modo 1 não é exercitado (issue #11) |
| U20c | supervisor e fachada | `ipc_ack_abandon` despachado só ao modo 2 (`test_transport`, `test_transport_contract`); `supervisor_reap` abandona somente produtor que não saiu com sucesso (`test_supervisor_report`) |
| U21 | `supervisor`/shutdown | Filhos reais sincronizados por pipes, saída após SIGTERM, filhos que ignoram SIGTERM, prazo comum e SIGKILL, preservação de sinais, EINTR sem reinício do prazo, erros de relógio/poll/sinalização e shutdown repetido |
| U22 | `job_outcome`/REPL | Apresentação de resultados de trabalhos com eventos controlados, sem workers (`test_job_outcome` e `test_outcome_repl`): texto de erro de kernel, consumo parcial, cancelamento e falha ao criar o filho com operação, índice (`estrutura I` ou `todas as estruturas`) e quantidade efetiva; `system_errno` interpretado só em 1..4095 e com status diferente de `MPMC_OK`; parcial, cancelado e conclusão com menos mensagens nunca como sucesso; REPL segue aceitando comandos após falha de `fork()` (identificador não consumido); prompt só em stdout; uma linha inteira por evento, sem ordem global; contagem de avisos para redesenhar o prompt, fila finita sem perda ou repetição e eventos pendentes consolidados no `shutdown` |
| U23 | `transport_parity` | Critérios de aceite da integração dos paradigmas pela fachada (`test_transport_parity`), com backends e processos reais: **C1** varredura prova que `producer.c`, `consumer.c` e `producer_routing.c` não citam paradigma nem syscall, e uma única função de cenário (só `ipc_*`, `producer_run()` e `consumer_run()`) produz resultado idêntico em SHM e MSG (códigos de saída, totais por fila, FIFO/PID/`operation_id`/`sent_at`); **C2** a mesma tabela de 22 chamadas inválidas dá o mesmo `status` nos dois modos (`INVALID_INDEX` para índice fora de 1..p, `INVALID_ARG` para ponteiro nulo/`operation_id`/`expected`), com `system_errno` zero nas rejeições locais; **C3** rollback real: `ipc_init()` com mais estruturas que o limite do kernel (`shmmni`/`msgmni`) falha com `*out` nulo e a contagem de objetos (`/proc/sysvipc/{shm,sem,msg}` no Linux, `ipcs` nos demais) volta ao valor anterior; sem `/proc` o pedido escala (128, 1024, 8192, 40000) até o kernel recusar; **C4** dois filhos chamam `ipc_worker_close()` sem remover nenhum recurso (conferido por `shmctl`/`semctl`/`msgctl`), o dono ainda opera, e `ipc_destroy()` remove todos. Validado por mutação (fachada sem validação, `msg_backend_create` sem rollback, `worker_close` destruindo, worker citando o paradigma) |
| U24 | `shm_backend_ack` | Registro e finalização de operações, IDs duplicados/desconhecidos, notificações antecipadas, validação do PID, notificação final única, operações simultâneas e isolamento entre waiters, mensagens em múltiplas estruturas, limite de 64 posições e 1024 consumos, reutilização de slot após ACKs antecipados, waiter único, rejeição de `ack_end` durante espera, interrupção por sinal e propagação de falha fatal `EIDRM`; recuperação de falhas transitórias de liberação do mutex nos caminhos de erro e em ack_end, com verificação de reutilização do slot |

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
| I23 | Inicialização com `p` no limite e acima dele | `p=1025` chega à inicialização IPC em fixture com falha de criação; overflow de size_t é recusado com uso, antes do REPL |

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
make unit   # testes de command, supervisor, msg, sem, error, wrappers, backend, buffer shm, roteamento cíclico do produtor e apresentação de resultados de trabalhos
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

`test_supervisor_shutdown` usa filhos reais e uma fachada IPC substituta, sem
criar objetos System V nem depender de pgrep ou DEBUG. Pipes confirmam a configuração
dos sinais antes de cada despacho voltar. Um cenário executa a espera real de dois
segundos para dois filhos que ignoram SIGTERM; outro avança um relógio controlado
e injeta EINTR, comprovando que a espera seguinte usa apenas os 800 ms restantes.
Também injeta falhas de relógio, poll(), eventos inválidos no pipe e kill(), verifica
restauração de SIGCHLD e de sua máscara, bloqueio de novos trabalhos e repetição
segura após falha no SIGKILL. Um filho externo aos registros do supervisor continua
vivo, e a limpeza final encerra e recolhe somente filhos próprios do teste.
Um watchdog com alarm() limita travamentos da suíte. Esses testes comprovam a
coordenação de processos da #52; as esperas reais de dados/espaço/ACK permanecem
na integração de #20/#26.

A mesma suíte copia os diagnósticos do reporter e altera errno na callback para
verificar preservação de causas. Cobre falhas simultâneas em vários filhos, erros
de espera seguidos de falhas na restauração de ação/máscara, callback nula,
user_data nulo e ausência de diagnósticos duplicados. Os contextos discriminados
são verificados para PID/sinal, PID de coleta e bits de poll(), distinguindo
falhas de read()/EOF no self-pipe e retornos do backend IPC. ESRCH, EINTR e timeout
não geram diagnósticos de falha.

Um filho que termina normalmente é observado por SIGCHLD bloqueado e sigwait(),
com ação capturável instalada antes do fork() para manter a compatibilidade com
macOS. O teste também cobre ECHILD após coleta externa, estado inesperado do filho,
falha de fork() com outro filho ativo e falhas de inicialização antes/depois da
criação do self-pipe. Confere que os descritores parciais foram fechados e que
nenhum contexto IPC não adquirido foi destruído. A fachada é substituída;
esses cenários não comprovam o rollback interno dos backends System V.

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
Fechamento com nullptr não aloca, não libera recursos e não chama o reporter.

`test_shm_operations` usa segmentos e semáforos reais para FIFO após volta dos índices,
metadados, isolamento entre estruturas e totais independentes. Interrompe com
`alarm(1)` as esperas por itens, vagas e mutex; verifica ausência de reservas parciais
e preservação das saídas. Dois produtores e dois consumidores exercitam a mesma
estrutura, com 128 identidades conferidas sem perda ou duplicação. Um pipe leva as
observações ao pai; sua ordem não é usada como evidência da ordem de retirada.

`test_shm_stress` (U19, issue 56) exercita o backend SHM real sem ACK, supervisor, workers finais
ou REPL. Em cada uma de quatro rodadas (1, 2, 3 e 4 estruturas) o pai cria o backend e enche todas
as estruturas (63 mensagens, `EMPTY == 0`) antes de criar os filhos; seis produtores enviam 150
mensagens cada, bloqueando em estruturas cheias, e dois consumidores por estrutura retiram
exatamente a cota de cada uma. A identidade é `(operation_id, sequence)`; o pai confere, sem supor
ordem global entre produtores, que cada identidade chega uma única vez, na estrutura e do remetente
corretos, que a ordem de cada consumidor respeita FIFO por produtor, que `consumed_count` iguala o
total enviado e que as estruturas terminam vazias. Volume e padrão de envio são fixos
(reproduzíveis, sem `sleep()`). Pai usa `alarm(30)` por rodada (cancela as demais ao expirar);
filhos usam `alarm(20)` e morrem por SIGALRM. Os filhos fecham só o estado local, o pai recolhe
todos (sem zumbis) e remove apenas seus recursos, o que se confere por `shmctl`/`semctl`; um
backend espectador criado antes das rodadas continua íntegro no fim.
**Limitações:** esta etapa não comprova o protocolo de ACK nem shutdown durante esperas reais;
ACK concorrente, interrupção durante dados/espaço/ACK, falhas parciais e shutdown completo
continuam na issue 26.

`test_msg_stress` (U20, issue 57) exercita o backend MSG real sem ACK, supervisor, workers finais
ou REPL, na mesma estrutura de `test_shm_stress`. Em cada uma de três rodadas (1, 2 e 3 filas) o
pai cria o backend e enche cada fila de dados com `IPC_NOWAIT` até o kernel recusar (a capacidade
vem do kernel, não de uma constante) antes de criar os filhos; quatro produtores enviam 100
mensagens cada, bloqueando em filas cheias, e dois consumidores por fila retiram exatamente a cota
de cada uma, o que comprova que o envio bloqueado retoma após o consumo. A identidade é
`(operation_id, sequence)` e `sent_at` carrega valores fixos para conferir os metadados. O pai
confere, sem supor ordem global entre produtores, que cada identidade chega uma única vez, na
fila e do remetente corretos, que a ordem de cada consumidor respeita FIFO por produtor, que
`consumed_count` iguala o total enviado e que as filas terminam vazias. Enquanto espera os
consumidores, o pai drena a fila de eventos a cada despertar de `poll()`, como o supervisor faz no
REPL, de modo que os consumidores nunca bloqueiam no canal de estatísticas e os eventos são
recebidos sem depender de entrada do usuário. Um teste de argumentos cobre índices 0 e p+1, ponteiros
nulos e `producer_count` zero, e a recepção não bloqueante em fila vazia retorna
`MPMC_ERR_WOULD_BLOCK` sem esperar. Um teste de timeout força dois travamentos reais (filho bloqueado em fila vazia): um morre pelo
`alarm(1)` do próprio filho (SIGALRM) e o outro pelo tratador do `alarm(1)` do pai (SIGKILL), e a
fila do teste é removida em seguida. Volume e padrão de envio são fixos (sem `sleep()`); o pai usa
`alarm(30)` por rodada e os filhos `alarm(20)`, morrendo por SIGALRM se travarem. Os filhos fecham
só o estado local, o pai recolhe todos (sem zumbis) e remove apenas as filas que criou, o que se
confere por `msgctl`; um backend espectador criado antes das rodadas continua íntegro no fim.
**Limitações:** esta etapa não cobre ACK, consumo parcial de workers, striped, matriz de comandos
pelo `stdin` nem shutdown durante esperas reais, que continuam integralmente na issue 25.

Dois cenários adicionais usam processos filhos de teste, sem ACK, workers finais, supervisor
ou REPL. Cada filho avisa o pai por pipe imediatamente antes de uma única operação bloqueante,
de modo que a coordenação depende de eventos, SIGCHLD e do `alarm(10)` de segurança, nunca de
`sleep()` ou de consultas periódicas. No primeiro, a estrutura 1 está cheia (63 mensagens,
`EMPTY == 0`) e um filho tenta enviar mais uma; enquanto ele espera, a estrutura 2 executa
um ciclo completo de envio e retirada, mostrando que a espera é só do remetente. Um consumo
na estrutura 1 libera uma vaga: o envio pendente conclui, a ordem FIFO e os metadados são
conferidos (inclusive a mensagem tardia, ao final), o contador acumulado vale 64 e os três
semáforos voltam aos valores de repouso. No segundo, um filho bloqueia ao receber da
estrutura 2 vazia; o pai envia antes à estrutura 1 e depois à 2, e o filho deve obter
exatamente a mensagem da 2, com todos os campos, enquanto a da 1 permanece. Recursos System V
são criados e removidos apenas pelo pai, no backend do próprio teste.

Um terceiro cenário cria o backend com 1024 estruturas, sem processos
filhos: segmentos e conjuntos de semáforos distintos por índice, uma mensagem própria enviada a
cada índice de 1 a 1024 e retirada do mesmo índice em ordem inversa, contador acumulado igual a
1 em todos, e índice 1025 inválido em `send`, `receive` e `consumed_count` com saídas
preservadas. 1024 é o volume deste cenário, não um limite de p. O parser aceita
p positivo representável em size_t; a criação depende de memória e limites reais
do sistema. Se o sistema esgotar seus limites System V (ENOSPC/ENOMEM/EMFILE), o cenário é dispensado
com aviso em stderr; qualquer outra falha conta como falha do teste.

Alcance (#55, subtarefa de #25): valida o backend SHM real com processos de teste e mensagens
sem confirmação. A matriz de comandos pelo stdin, os modos de ACK, o consumo parcial do worker
e o striped integrados continuam exigidos em #25.

Somente o objeto de operações do teste substitui `sysv_sem_apply`/`sysv_sem_change`:
normalmente delega às primitivas reais, mas permite falhar uma aquisição ou publicação.
Os cenários distinguem buffer/saída inalterados de uma alteração já executada antes
de a publicação falhar, inclusive na leitura do contador. Após cada falha fatal
injetada, o teste destrói o contexto; não tenta repetir a operação nem comprova
recuperação dos contadores. Isso corresponde à obrigação de descarte descrita em
@ref shm_operation_failures. Os testes de interrupção da aquisição e recepção vazia
verificam que esses retornos não deixam reservas parciais nem alteram o buffer.
A comunicação de falhas fatais ao supervisor e o shutdown coordenado continuam
comprovados pela matriz do executável de prova descrita ao final desta página.
O cleanup de segurança encerra/recolhe apenas os filhos próprios e remove apenas
os recursos do contexto.
O teste U24 (`test_shm_backend_ack`) verifica registro, conclusão antecipada,
proprietário, notificação final única, isolamento entre operações, 64 posições internas com espera por vaga e 1024 consumos, reutilização, waiter único, interrupção e falhas de mutex.
A deduplicação por sequência foi substituída pelo contrato de uma confirmação por
retirada efetiva. `test_shm_ack_lifecycle` passa pelo buffer real: confirmações
antecipadas, consumo parcial, espera bloqueada, quatro produtores concorrentes e
striped, metadados sem perda/duplicação, cancelamento de produtor em ACK e remoção
somente dos recursos do teste, preservando um backend espectador.

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

FIFO, ACKs e concorrência dos backends têm cobertura nas suítes nativas e de paridade. Os *smoke tests* validam somente inicialização
e shutdown; os cenários do REPL não substituem a verificação do transporte IPC completo.
Scripts automatizados devem capturar a saída e impor limites de tempo para detectar travamentos.

`tests/integration/test_application.py` executa o binário real com stdin e saída
conectados por pipes. Usa selectors e prazos monotônicos, sem sleeps: ACK striped
em três estruturas, produção sem espera seguida de consumo parcial, 1024 mensagens
com contrapressão e seis produtores concorrentes na mesma estrutura. Repete todos
os cenários em SHM e MSG, confere resultados, contagens e número de filhos, e encerra
somente o grupo de processos criado por cada cenário se alguma asserção falhar.

## Matriz final do executável e auditoria de shutdown

`test_application.py` executa o programa distribuído nos modos 1 e 2. Verifica
consumidor antes do produtor, produção sem espera antes de haver consumidor,
ACK somente após a última mensagem, striped divisível e não divisível por p,
consumo parcial/zero, índices inválidos sem novos filhos, n=1024 e vários
produtores/consumidores na mesma estrutura. As verificações usam propriedades,
contagens e identidade das operações; a prova de FIFO/metadados está nas suítes
nativas dos backends e de paridade, pois o CLI não imprime cada mensagem.

`test_shutdown_resources.py` usa `mpmc_probe`, ligado aos mesmos módulos da
aplicação com syscalls observadas somente nos wrappers de teste. Um pipe adicional
informa quando o worker vai aguardar um recurso indisponível: dados, espaço ou
ACK. Só então o teste envia shutdown. São três repetições por espera e paradigma,
mais EOF, SIGINT e SIGTERM: 24 cenários. Não usa sleeps para sincronização.

Cada criação bem-sucedida registra tipo, ID e PID em um manifest privado.
`resource_inspect` consulta esses IDs via IPC_STAT, verifica a ausência depois do
encerramento e permite cleanup de reserva restrito a eles. O teste também confere
que o PID do filho já não existe e conserva três recursos externos (fila,
segmento e conjunto) durante toda a matriz, verificando que continuam existentes.
Nunca executa remoção global com ipcrm. Prazos de subprocessos e grupos próprios
limitam falhas sem atingir outras execuções.

`test_diagnostics.py` cobre falhas parciais de inicialização, várias falhas de
rollback e liberação, propagação pelo pipe do worker e terminal/NO_COLOR. Os
adaptadores não são ligados ao executável distribuído. Python 3 é necessário
somente para essas suítes de integração; o programa continua sendo C23.

O estresse de dados MSG da #58 já está em `test_msg_stress`: mensagens com identidade
única, concorrência, saturação, metadados, ausência de perda/duplicação, repetição
de criação/destruição e alarmes. A cobertura de ACK/shutdown é complementar, nas
suítes de ACK e na matriz acima. Os testes C mantêm `alarm()` com tratadores e
máscaras adequados ao macOS; não foi realizada execução nativa em macOS nesta revisão.


## Regressões de aderência ao enunciado

- test_background.py: job em background com terminal suspende por SIGTTIN,
  retoma quando seu grupo recebe foreground/SIGCONT e aceita shutdown; dois modos.
- test_conformance.py: 65 produtores ACK antes do consumidor concluem nos dois
  modos; p=1025 alcança o backend; EIO no relógio final preserva contagens e
  apresenta indisponível com saída de erro.
- test_shm_ack_lifecycle: espera por vaga interrompida por alarm(), retorno de
  todas as reservas e liberação do mutex para o processo que encerra outro lote.
- test_supervisor_report: falha individual, regressão monotônica e falha total
  preservam dados, errno/PID e callbacks sem duplicação; zero real continua válido.

As suítes Python usam processos, pipes e prazos; os testes nativos conservam
alarm()/sigaction sem SA_RESTART e máscaras explícitas para compatibilidade macOS.
A execução desta revisão ocorreu em Linux; não foi feita validação nativa macOS.
