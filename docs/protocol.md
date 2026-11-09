# Protocolo IPC, concorrência e confirmações {#ipc_protocol}

@tableofcontents

## Objetivos de correção

Os dois paradigmas devem apresentar as mesmas semânticas observáveis:

- Cada mensagem enviada pertence a exatamente uma estrutura lógica `1..p`.
- Cada mensagem consumida foi previamente enviada e é retirada uma única vez.
- Dentro de uma estrutura, o consumo segue a ordem de inserção (FIFO).
- Consumos e inserções concorrentes não corrompem metadados compartilhados.
- Um produtor com `ack=1` só termina normalmente após confirmar o consumo das
  **próprias mensagens**; consumos de outros produtores não satisfazem seus ACKs.
- Um consumidor com `ack=1` só termina normalmente depois de `n` consumos, a menos
  que seja interrompido por shutdown/erro.
- Um consumidor com `ack=2` não espera indefinidamente pelo próximo item; se não
  conseguir `n`, encerra com erro e informa a contagem efetivamente removida.

## Envelope lógico da mensagem

`message_t` contém `sender_pid`, `sequence`, `sent_at` e `operation_id`.
`sender_pid`, `sequence` e `sent_at` são campos mínimos obrigatórios do enunciado:
devem existir e ser preservados em toda mensagem, independentemente do modo IPC.
`operation_id` é uma extensão interna; não substitui nem altera o significado dos
campos obrigatórios. A solicitação de ACK pertence à operação e é preparada por
`ipc_ack_begin()`, sem um campo adicional em cada mensagem. `producer_id` não
pertence à mensagem: é o índice lógico da fila/estrutura escolhida pelo comando e
passado separadamente a `ipc_send()`/`ipc_receive()`.

O supervisor atribui um `operation_id` não nulo e único por trabalho **antes de
`fork()`**. Para trabalho produtor, esse ID identifica a operação de produção e é
copiado sem alteração para todas as mensagens dela. O processo filho emissor define
`sender_pid=getpid()`; esse PID físico não é o índice lógico `producer_id`. Um único
emissor *striped* conserva o mesmo PID e sequência ao alternar entre destinos.
`sequence` começa em 1 e cresce uma vez por mensagem no processo emissor, não por
fila; não reinicia ao mudar de destino.

`sent_at` é capturado para cada mensagem com `CLOCK_REALTIME`, no ponto definido
como envio pelo worker. É metadado de tempo civil para observabilidade, não chave
de identidade nem garantia de ordem: a ordem por emissor é determinada por
`sequence`, pois o relógio de calendário pode ajustar-se. Uma serialização de fila
pode ter envelope de controle extra, mas deve preservar os três campos mínimos e
os metadados internos. A estrutura enviada a `msgsnd` exige `long mtype` inicial e
`msgsz` que **não inclui `mtype`**.

## Correlação entre produção, consumo e relatório

As identidades têm escopos distintos e não devem ser reaproveitadas umas pelas
outras:

| Identificador/campo | Identifica | Regra |
|---|---|---|
| `producer_id` | Fila lógica `1..p` | Seleciona o destino/origem IPC; não é PID e não é campo de `message_t`. `produz 0` é apenas a regra de distribuição *striped*. |
| `sender_pid` | Processo produtor físico | É `getpid()` no processo que emite a mensagem; preservado ao consumir. |
| `sequence` | Mensagem dentro do emissor | Inicia em 1 e avança por envio no processo, incluindo destinos distintos em *striped*. |
| `operation_id` da mensagem | Trabalho produtor | Mesmo ID em todas as mensagens desse trabalho; não é o ID do comando consumidor que as retira. |
| `operation_id` do consumidor | Trabalho consumidor | Identifica o processo/comando consumidor e seu relatório; nunca sobrescreve o ID gravado na mensagem. |

A chave de correlação de uma mensagem é `(operation_id, sender_pid, sequence)`.
O `operation_id` impede colisões entre trabalhos, inclusive quando o sistema
reutiliza um PID; `sender_pid` e `sequence` preservam a procedência exigida e a
ordem dentro do emissor. `sent_at` não integra essa chave.

Uma operação `produz ... 1` prepara seu contexto de ACK antes do primeiro envio;
`produz ... 2` não cria esse contexto. Na memória compartilhada, o consumidor
consulta o registro de operações com ACK pendente pelo `operation_id`. Nas filas,
o produtor codifica a solicitação no `mtype` obrigatório do envelope de dados e o
consumidor interpreta esse tipo, pois o estado local do produtor não é compartilhado.
Cada retirada efetiva com solicitação de ACK gera exatamente uma confirmação
associada à chave da mensagem; uma mensagem não retirada não pode ser confirmada.
O backend pode agregar confirmações para transporte, mas deve manter a contagem por
`operation_id` e não contar duas vezes a mesma chave. O produtor aguarda o número
esperado para o seu próprio `operation_id`; ACKs de outro trabalho ou do comando
consumidor não satisfazem essa espera. ACK confirma consumo, não apenas inserção.

O relatório de processos do supervisor identifica cada filho por seu PID,
`operation_id`, tipo, estado de término e duração. Para um filho produtor, seu PID
deve coincidir com `sender_pid` das mensagens que emitiu e seu `operation_id` com
o das mensagens. Para um filho consumidor, o PID e `operation_id` descrevem o
consumidor, enquanto as mensagens retiradas mantêm os dados de seus emissores e
os IDs dos trabalhos produtores. Portanto, o relatório de término de processo não
é um recibo individual de mensagem nem substitui as estatísticas de consumo por
fila; o estado de sucesso de um produtor `ack=1` pressupõe que todos os ACKs da
operação chegaram.

## Fluxo por comando

### `produz i n 2`

O supervisor valida o comando, cria o filho, e `producer_run` envia `n` mensagens
sucessivamente por `ipc_send`. O filho não aguarda consumo, mas **a inserção pode
bloquear** se o buffer/fila estiver cheio. `ack=2` elimina só a confirmação de consumo,
não o limite de capacidade do recurso IPC.

### `produz i n 1`

1. Criar contexto de confirmação com `ipc_ack_begin(operation_id, n)`.
2. Enviar as `n` mensagens com o mesmo `operation_id`, mantendo o contexto de ACK ativo.
3. Esperar em `ipc_ack_wait(operation_id)` até receber `n` confirmações de consumo.
4. Executar `ipc_ack_end(operation_id)` em sucesso, falha ou cancelamento.

O protocolo deve preservar confirmações emitidas **antes** de o produtor entrar em
`ipc_ack_wait` (ACKs não são eventos de borda que possam se perder). Cada ACK
corresponde a uma mensagem da operação, e a soma de confirmações distintas não
pode exceder o número de mensagens efetivamente consumidas.

### `produz 0 n ack`

Um único processo filho cria `n` mensagens e calcula o destino da mensagem de
índice `k` a partir de `1 + ((k - 1) % p)`. A sequência *do processo emissor*
é `1..n`. Para `ack=1`, uma operação espera o total agregado de `n` confirmações
independentemente da quantidade de estruturas destinatárias.

### `consome i n 1`

O filho remove mensagens sucessivas vezes de `i` usando `ipc_receive(..., blocking=true)`.
Entre uma mensagem e outra, poderá ficar bloqueado. Cada retirada válida aumenta a
contagem de `i` e gera confirmação **se o protocolo do backend indicar ACK para a
operação produtora**, independentemente do modo de espera do comando consumidor.

Uma retirada conta como consumo do trabalho somente quando `ipc_receive` retorna `MPMC_OK`.
O consumidor não confirma nem conta por conta própria: o backend faz as duas coisas dentro de
`ipc_receive`, e `message_t` não carrega a solicitação de ACK.

### `consome i n 2`

O filho executa até `n` tentativas de remoção sem esperar. Se `ipc_receive`
retornar `MPMC_ERR_WOULD_BLOCK`, finaliza com esse status (quantidade insuficiente) e
informa quantas mensagens conseguiu remover. As mensagens já consumidas **não**
são repostas na fila ou buffer.

## Formato físico e protocolo por backend {#ipc_wire_formats}

Esta seção fixa o que cada backend grava e troca, byte a byte e semáforo a semáforo.
Ela é o contrato que `shm_backend` e `msg_backend` devem implementar; o restante do
documento descreve as propriedades que esse contrato precisa garantir. Os tamanhos
abaixo foram obtidos com `sizeof`/`offsetof` em Linux LP64 (x86-64 e aarch64) e a
implementação deve confirmá-los com `static_assert`, recusando-se a compilar se divergirem.

### Representação comum de `message_t`

Os dois backends transportam a mesma estrutura, copiada com `memcpy` e sem ponteiros.
A cópia bruta é válida porque todos os processos executam o mesmo binário na mesma
máquina e, portanto, compartilham o mesmo ABI.

| Campo | Deslocamento | Bytes | Observação |
|---|---:|---:|---|
| `sender_pid` | 0 | 4 | `getpid()` do filho emissor |
| `sequence` | 4 | 4 | Começa em 1 por processo emissor |
| `sent_at` | 8 | 16 | `struct timespec`, `CLOCK_REALTIME` |
| `operation_id` | 24 | 8 | ID do trabalho **produtor**, atribuído pelo supervisor |
| **Total** | | **32** | Alinhamento 8 |

Regras comuns aos dois backends:

- `sent_at` é gravado pelo produtor imediatamente antes da chamada a `ipc_send()` daquela
  mensagem e nunca é reescrito por backend ou consumidor. É o instante em que o produtor
  inicia o envio, e não o instante em que o comando foi criado nem o da inserção: se o
  envio esperar vaga, a inserção ocorre depois do `sent_at`.
- O backend não altera `sender_pid`, `sequence`, `sent_at` nem `operation_id`.
  O consumidor recebe a mensagem exatamente como o produtor a enviou.
- O índice lógico `producer_id` nunca é gravado na mensagem; ele é o endereço (segmento
  ou fila) onde a mensagem fica.

### Backend 1: memória compartilhada e semáforos

**Recursos do backend, solicitado pelo supervisor em `ipc_init()`:**

| Recurso | Quantidade | Conteúdo |
|---|---:|---|
| Segmento `S_i` | `p` | Exatamente 2048 bytes, buffer circular da estrutura `i` |
| Conjunto de semáforos `M_i` | `p` | 3 semáforos da estrutura `i` |
| Segmento de controle `C` | 1 | Tabela de operações com ACK pendente |
| Conjunto de semáforos `K` | 1 | `1 + 64` semáforos de ACK |

`lifecycle.c` já cria e inicializa `S_i` e `M_i`. O segmento `C` e o conjunto `K`
pertencem ao desenho de ACK ainda pendente; `ack_begin`, `ack_wait` e `ack_end`
continuam retornando `MPMC_ERR_STATE`.

**Layout de `S_i` (2048 bytes):**

| Faixa | Conteúdo |
|---|---|
| 0 | `head` (`uint8_t`): índice da próxima mensagem a retirar, de 0 a 62 |
| 1 | `tail` (`uint8_t`): índice da próxima posição de inserção, de 0 a 62 |
| 2 | `count` (`uint8_t`): entradas ocupadas, de 0 a 63 |
| 3 | `capacity` (`uint8_t`): gravado em `ipc_init()`, vale 63 |
| 4–7 | Preenchimento para alinhamento de 8 bytes; não integra os metadados |
| 8–15 | `consumed_total` (`uint64_t`): mensagens retiradas desta estrutura |
| 16–2031 | 63 entradas de 32 bytes (`message_t`); a entrada `j` começa em `16 + 32·j` |
| 2032–2047 | Reservado, zerado |

Os metadados de endereçamento e ocupação usam o menor tipo inteiro sem sinal de
largura fixa que comporte seu domínio. `head` e `tail` são índices de mensagens,
não ponteiros nem deslocamentos em bytes; assim como `count` e `capacity`, cabem
em `uint8_t`. A capacidade deve ser menor ou igual a `UINT8_MAX`, considerando que
`count` precisa representar também o buffer cheio.

Se for necessário armazenar um deslocamento relativo em bytes no segmento, o tipo
é `uint16_t`, suficiente para as posições de 0 a 2047. O layout acima não armazena
esse campo: o deslocamento de cada mensagem é calculado a partir de seu índice,
com aritmética em `size_t`, e convertido em endereço somente no mapeamento local.
Ponteiros de processo não são gravados no segmento.

`consumed_total` permanece `uint64_t`: é acumulado entre comandos e não está
limitado às 63 posições nem às 1024 mensagens de um único comando. A escolha dos
tipos menores não elimina a exclusão mútua exigida pelo protocolo.

O cabeçalho alinhado ocupa 16 bytes. O preenchimento preserva o alinhamento natural
de `consumed_total` e `message_t`, sem estruturas compactadas. Na implementação,
`static_assert` deve verificar os tamanhos dos campos, os deslocamentos acima, o
alinhamento das mensagens, os 2048 bytes do segmento e a capacidade calculada dentro
do limite de `uint8_t`. Se um deslocamento em bytes passar a ser armazenado, seu
limite também deve ser verificado contra `UINT16_MAX`.

Essas verificações estão em `src/ipc/shm_backend/shm_ring.h`. O cálculo da
capacidade é compartilhado pela constante interna `SHM_RING_CAPACITY` e pela
inicialização, sem substituir a validação dos limites do layout.

O endereço base também deve respeitar `alignof(shm_ring_t)`. Na associação real,
`shmat(shmid, NULL, 0)` escolhe um endereço alinhado à página, suficiente para o
alinhamento de 8 bytes deste layout; veja
[shmat(2)](https://man7.org/linux/man-pages/man2/shmat.2.html). Para áreas locais,
`malloc()` fornece o alinhamento necessário. Um vetor de bytes embutido em outra
estrutura deve declarar `alignas(shm_ring_t)` no próprio membro, como fazem os
segmentos simulados dos testes. Converter um ponteiro não ajusta seu alinhamento.

A capacidade segue a fórmula da seção anterior: `(2048 − 16) / 32 = 63`. Uma estrutura
comporta no máximo 63 mensagens pendentes; um `produz i 1024 2` sem consumidores bloqueia
na 64ª, e isso é o comportamento previsto (backpressure), não um erro.

**Semáforos de `M_i`:** índice 0 = `mutex` (inicial 1); índice 1 = `empty` (inicial 63);
índice 2 = `full` (inicial 0). Os índices valem 0, 1 e 2 em todas as estruturas.

**Envio (`shm_backend_send`):**

1. Executa `[P(empty), P(mutex)]` em uma única `semop()` bloqueante.
2. Sob exclusão mútua, insere a cópia integral da mensagem em `tail`, avança o índice
   circularmente e incrementa `count`, usando `shm_ring_push()`.
3. Executa `[V(mutex), V(full)]` em outra `semop()` para publicar a mensagem.

A aquisição é atômica: o kernel não mantém uma reserva de `empty` quando não consegue
concluir o vetor, inclusive enquanto aguarda `mutex`. Não é necessária uma compensação
separada de `empty` nesse caminho. A publicação também é atômica individualmente.
`SEM_UNDO` não é usado; o envio não aguarda consumo nem registra ACK nesta etapa.

**Recebimento (`shm_backend_receive`):**

1. Executa `[P(full), P(mutex)]` em uma única `semop()`. Apenas a operação em `full`
   recebe `IPC_NOWAIT` se `blocking=false`; falta de item retorna
   `MPMC_ERR_WOULD_BLOCK` com `system_errno=EAGAIN`. O mutex continua podendo exigir espera.
2. Sob exclusão mútua, copia a entrada de `head` para a saída, avança o índice,
   decrementa `count` e incrementa `consumed_total`, usando `shm_ring_pop()`.
3. Executa `[V(mutex), V(empty)]` em outra `semop()` para publicar a vaga.
   A implementação atual ainda não consulta o registro de ACK nem confirma o consumo.

**Falhas e sinais.** Se o vetor de aquisição falhar, nenhuma reserva parcial
permanece e a operação não altera o buffer. Rejeições locais de argumentos
(`system_errno == 0`), `MPMC_ERR_WOULD_BLOCK` na recepção vazia e
`MPMC_ERR_INTERRUPTED` na aquisição não invalidam o contexto por si mesmas.

As demais falhas de `semop()` são **fatais ao contexto inteiro**, inclusive
argumentos rejeitados pelo kernel, perda de permissão e remoção do conjunto. Se o
vetor final falhar, a inserção ou retirada já ocorreu e não é desfeita; no
recebimento, a saída e a contagem também já foram alteradas. O vetor final falha
atomicamente, portanto pode deixar o mutex adquirido e o item ou a vaga sem
publicação. Não se deve repetir o envio/recebimento inteiro nem reparar os
contadores com outros workers ativos. `mpmc_result_t` não identifica a etapa da
falha; por isso a política de falha fatal também abrange os erros de recursos
anteriores à alteração do buffer. Ver @ref shm_operation_failures.

Cabe ao chamador suspender todas as operações de dados e estatísticas do contexto,
propagar a falha ao supervisor e encaminhar o encerramento coordenado. Não há
marca de inutilização, bloqueio automático de novas chamadas ou notificação de
outros processos pelo backend. A integração dessa reação pertence à #20: cada
filho fecha somente seu estado local, e o supervisor encerra e recolhe todos os
filhos antes da remoção dos recursos globais.

A implementação não mascara `SIGTERM`/`SIGINT` entre aquisição e publicação. A
atomicidade de cada vetor não protege todo esse intervalo contra término de
processo. No shutdown global, as reservas restantes são descartadas junto com o
contexto; não é necessário restaurá-las para reutilização. Término de um worker
nesse intervalo também exige descartar o contexto. Cancelamento isolado com
preservação do contexto exigiria outro protocolo e está fora do contrato da #10.
Simplesmente bloquear os sinais antes de um `semop()` bloqueante pode impedir que
SIGTERM cancele a espera.

**ACK por operação (`ack=1`, contrato previsto).** O segmento `C` deverá guardar 64 posições de 8 bytes,
cada uma com um `operation_id` (0 = livre). O conjunto `K` tem o semáforo 0 como
`ack_mutex` (inicial 1) e, nos índices 1 a 64, um contador de ACKs por posição (inicial 0).
Como os consumidores são criados antes de o produtor existir, este é o único ponto de
encontro que ambos já herdaram por `fork()`.

| Passo | Quem | Ação |
|---|---|---|
| `ack_begin(op, n)` | Produtor | `P(ack_mutex)`; recusa `op` já presente (`MPMC_ERR_STATE`); toma a primeira posição livre (sem posição livre: `MPMC_ERR_NO_SPACE`); grava `op`; zera o contador da posição; `V(ack_mutex)` |
| ACK | Consumidor | `P(ack_mutex)`; procura `op = message.operation_id`; se achar, `V(contador)` na posição; `V(ack_mutex)`. Se não achar, não emite ACK (a operação não pediu confirmação, já terminou ou foi cancelada) |
| `ack_wait(op)` | Produtor | `semop(contador, −n)`: bloqueia até o contador valer pelo menos `n` e subtrai `n` de uma vez |
| `ack_end(op)` | Produtor | `P(ack_mutex)`; grava 0 na posição; `V(ack_mutex)`. Chamado em sucesso, falha ou cancelamento |

Propriedades exigidas para a implementação desse desenho:

- ACKs que chegam antes de `ack_wait` não se perdem, porque ficam acumulados no contador.
- Um ACK nunca é creditado a outra operação: a busca é pelo `operation_id` da mensagem e
  o `V` ocorre com `ack_mutex` adquirido, então `ack_end` não pode liberar e reutilizar a
  posição entre a busca e o `V`.
- Em `produz 0 n 1`, as `n` mensagens, de qualquer estrutura de destino, incrementam o
  mesmo contador; o produtor espera `n` no total.
- Não há espera circular: o consumidor nunca bloqueia no ACK (o `V` não bloqueia e o
  `ack_mutex` só é mantido por trechos curtos sem espera), e o único bloqueio do canal é
  o do produtor em `ack_wait`.
- O limite é de 64 operações `produz ... 1` simultâneas; a seguinte falha com
  `MPMC_ERR_NO_SPACE` e o REPL reporta o erro. O valor máximo de um semáforo System V
  (32767 no Linux) comporta os 1024 ACKs do maior trabalho.

**Estatísticas.** `shm_backend_consumed_count()` lê `consumed_total` sob `mutex`.
Só preenche a saída após liberar o mutex com sucesso. O total contabiliza retiradas
executadas, inclusive se a publicação posterior da vaga falhar. Falha na liberação
do mutex é fatal ao contexto mesmo preservando a saída. Após uma falha fatal, não
se deve tentar ler os totais desse contexto. `ipc_poll_events()` não faz nada neste modo.

### Backend 2: filas de mensagens System V

**Recursos criados pelo supervisor em `ipc_init()`:**

| Fila | Quantidade | Tipo (`mtype`) | Corpo (`msgsz`, sem `mtype`) |
|---|---:|---|---|
| Dados `D_i` | `p` | 1 sem ACK; 2 com ACK | `message_t`, 32 bytes |
| ACK `Q_ack` | 1 | `operation_id` do produtor | `{ uint64_t operation_id; }`, 8 bytes |
| Eventos `Q_evt` | 1 | 1 | `{ size_t producer_id; uint64_t count; }`, 16 bytes |

Os dados do paradigma 2 trafegam só por filas, nunca por `shm`. As filas `Q_ack` e `Q_evt`
são recursos de controle: não substituem as `p` filas de dados exigidas.

**Capacidade.** O limite de cada fila é `msg_qbytes`. Por exemplo, 16384 bytes
comportam 512 corpos de dados de 32 bytes, 2048 corpos de ACK de 8 bytes ou 1024 corpos de
evento de 16 bytes. O limite
varia entre sistemas e deve ser consultado, não tratado como constante. Como no
modo 1, `msgsnd` bloqueante em fila cheia é backpressure esperado.

**Envio.** O backend consulta o contexto local criado por `ack_begin` para o
`operation_id` da mensagem. Sem contexto ativo, usa `msgsnd(D_i, {1, message_t})`
bloqueante. Com contexto ativo, usa `mtype=2` e o laço descrito em "ACK por operação"
abaixo. Em ambos os casos, `msgsz = sizeof(message_t)`: o `mtype` obrigatório não
integra o corpo e não acrescenta bytes a `message_t`.

**Recebimento.** `msgrcv(D_i, tipo 0, msgsz = sizeof(message_t))`, sem `MSG_NOERROR`;
com `blocking=false` acrescenta `IPC_NOWAIT` e `ENOMSG` vira `MPMC_ERR_WOULD_BLOCK`.
Um tipo recebido diferente de 1/2 ou corpo de tamanho diferente de `sizeof(message_t)`
é violação de contrato (`MPMC_ERR_STATE`). Depois de retirar a mensagem, o consumidor,
nesta ordem:

1. envia `{producer_id, count = 1}` a `Q_evt` (um evento por mensagem retirada, com ou sem ACK);
2. se o tipo recebido era 2, envia `{operation_id}` a `Q_ack` com `mtype = operation_id`.

O tipo zero seleciona a primeira mensagem da fila, preservando FIFO entre dados com
e sem ACK; filtrar apenas pelo tipo 1 ou 2 alteraria essa ordem. Os valores 1 e 2
classificam a solicitação de confirmação, não a prioridade das mensagens. Essa é a
semântica de [msgrcv(2)](https://man7.org/linux/man-pages/man2/msgop.2.html).
O wrapper `sysv_msg_receive_typed()` devolve também o tipo ao backend.
`sysv_msg_receive()` mantém sua interface anterior e descarta essa informação.

**ACK por operação (`ack=1`).** O `mtype` do ACK é o `operation_id` do produtor, de modo
que `msgrcv(Q_ack, tipo = operation_id)` entrega a cada produtor somente as próprias
confirmações, mesmo com vários `produz` no mesmo índice. Como `mtype` é `long`, um
`operation_id` maior que `LONG_MAX` é recusado em `ack_begin` (`MPMC_ERR_INVALID_ARG`);
na prática os IDs crescem a partir de 1.

| Passo | Ação |
|---|---|
| `ack_begin(op, n)` | Só estado local: guarda `op`, `n` e um contador em zero. A fila `Q_ack` já existe |
| Envio de cada mensagem | Laço: `msgsnd(D_i, IPC_NOWAIT)`; em sucesso, segue. Em `EAGAIN`, drena as confirmações próprias (`msgrcv(Q_ack, tipo = op, IPC_NOWAIT)` até `ENOMSG`), somando ao contador, espera no máximo 1 ms com `nanosleep()` e repete. `EINTR` no laço retorna `MPMC_ERR_INTERRUPTED` |
| `ack_wait(op)` | `msgrcv(Q_ack, tipo = op)` bloqueante até o contador chegar a `n` |
| `ack_end(op)` | Drena sem bloquear as confirmações restantes de `op` e libera o estado local da operação, mesmo se a drenagem falhar; não há recurso do kernel a liberar. Operação desconhecida é sucesso (idempotente) |
| `ack_abandon(op)` | Só o supervisor. Registra `op` como sem dono e descarta, sem bloquear, os ACKs de `op` já enfileirados; `poll_events` repete o descarte a cada chamada, para os ACKs que chegarem depois |

Por que isso não trava: um `msgsnd` bloqueante por ACK, sozinho, permitiria o ciclo em que
produtores esperam vaga em `D_i` enquanto consumidores esperam vaga em `Q_ack`. Aqui, um
produtor com `ack=1` nunca fica bloqueado no kernel sem drenar os próprios ACKs: ele só
espera em `ack_wait` (que consome ACKs) ou em `nanosleep` curto (depois de drenar). Logo,
se `Q_ack` encher, o produtor dono dos ACKs acaba drenando e libera os consumidores.

**Integração com envio e recepção, sem espera circular.** Há três atores e cada um só
espera por algo que outro ator sempre libera:

| Ator | Onde pode bloquear | Quem o libera |
|---|---|---|
| Produtor `ack=1` | `ack_wait` (consome os próprios ACKs) e `nanosleep` de 1 ms depois de drenar. Nunca em `msgsnd` bloqueante de `D_i` | Consumidores, ao retirar e confirmar |
| Produtor sem ACK | `msgsnd` bloqueante em `D_i` cheia (backpressure) | Consumidores, ao retirar |
| Consumidor | `msgrcv` em `D_i` vazia; `msgsnd` em `Q_evt` ou `Q_ack` cheias, depois da retirada | Supervisor drena `Q_evt` em `poll_events`; o produtor dono drena `Q_ack` (envio, `ack_wait`, `ack_end`); o supervisor descarta `Q_ack` sem dono (`ack_abandon`) |
| Supervisor | Nunca em fila: só `IPC_NOWAIT` e `waitpid(WNOHANG)` | — |

Como o produtor com ACK nunca bloqueia no kernel sem drenar os próprios ACKs, e como o
supervisor nunca bloqueia, o grafo de espera não tem ciclo: consumidor bloqueado em `Q_ack`
cheia depende de um produtor que está drenando ou de um supervisor que descarta. O envio
usa `mtype = 2` apenas quando o contexto local de ACK existe; sem `ack_begin` (`ack=2`),
a mensagem sai com `mtype = 1`, nenhum ACK é emitido e `ack_wait` é recusada com
`MPMC_ERR_STATE`.

**ACKs sem dono.** Se um produtor `ack=1` termina sem receber todos os ACKs (erro, sinal,
cancelamento), consumidores ainda podem retirar mensagens dele e publicar ACKs que
ninguém lerá. Esses ACKs ocupam `Q_ack` e acabariam bloqueando consumidores em `msgsnd`.
Por isso, ao recolher um filho **produtor que não saiu com `EXIT_SUCCESS`** (inclusive
morte por sinal e perda por `ECHILD`), `supervisor_reap` chama `ipc_ack_abandon()` com o
`operation_id` dele; a partir daí `ipc_poll_events()` descarta os ACKs tardios dessa
operação a cada ciclo. Um produtor que termina com sucesso já recebeu todos os ACKs e não
é abandonado. Limites: a lista de operações abandonadas cresce em uma entrada por produtor
que falhou e é percorrida a cada `poll_events` (uma chamada `msgrcv` não bloqueante por
entrada); o descarte é seletivo por `mtype`, então ACKs de operações vivas não são
afetados; `operation_id` não é reutilizado durante a vida das filas, ou um ACK tardio
poderia ser creditado a outra operação.

**Limpeza em falhas.** `ack_begin` só cria o registro local depois de ter memória para ele.
`producer_run` chama `ack_end` em sucesso, falha ou cancelamento, e `ack_end` remove o
registro local mesmo que a drenagem falhe. Os identificadores das filas pertencem ao
supervisor: o filho usa `worker_close` (libera só memória local, inclusive registros de ACK
e a lista de abandonadas herdada) e nunca remove filas; `msg_backend_destroy` remove todas
as filas de dados e de controle e libera a memória, mesmo que uma remoção falhe.

**Estatísticas.** O supervisor chama `ipc_poll_events()` a cada ciclo do REPL (no máximo
100 ms), ao receber `SIGCHLD` e durante o `shutdown`: lê `Q_evt` com `IPC_NOWAIT` até
`ENOMSG` e incrementa seu contador local de `producer_id`. No `shutdown`, depois de enviar
`SIGTERM`, o supervisor alterna `waitpid(WNOHANG)` e `ipc_poll_events()` até recolher todos
os filhos, e faz uma drenagem final antes de ler `ipc_consumed_count()`. Assim, um consumidor
bloqueado em `Q_evt` cheia é liberado pela drenagem e nenhum evento enfileirado é perdido.

**Limite conhecido, janela entre retirada e evento.** O kernel não oferece "receber e
publicar" atômico. Se um `SIGTERM` terminar o consumidor entre o retorno de `msgrcv` e o
envio do evento, essa mensagem foi retirada mas não contada. A janela é de poucas
instruções e só se abre durante o `shutdown` ou numa morte anormal; o relatório de
consumo por estrutura pode então ficar uma unidade abaixo do real nesse caso.

### Resultado de um trabalho

Um worker devolve `mpmc_result_t`; o primeiro erro do trabalho prevalece sobre falhas de
liberação, e `ipc_ack_end` roda sempre que `ipc_ack_begin` retornou `MPMC_OK`. O supervisor
conhece o desfecho pelo estado de término do filho (código zero só em sucesso completo) e
os totais por estrutura por `ipc_poll_events`/`ipc_consumed_count`. Um erro de
`ipc_receive` depois da retirada (`MPMC_ERR_STATE`, `MPMC_ERR_SYS`) não conta como consumo
do trabalho. Os retornos por função estão em `transport.h`.

### Comparação dos dois backends

| Aspecto | Memória compartilhada | Filas de mensagens |
|---|---|---|
| Unidade de dados | Entrada de 32 bytes em `S_i` | Mensagem System V com corpo de 32 bytes em `D_i` |
| Capacidade por estrutura | 63 (fixa pela fórmula de 2048 bytes) | Definida por `msg_qbytes` (512 se o limite for 16384 bytes) |
| Ordem | FIFO por `head`/`tail` | FIFO da fila com recebimento por tipo 0 |
| Exclusão mútua | `mutex` do conjunto `M_i` | Fornecida pelo kernel |
| Contagem de consumo | `consumed_total` sob `mutex`, junto da retirada | Evento em `Q_evt`, consolidado pelo supervisor |
| Canal de ACK | Tabela em `C` e contadores no conjunto `K` | `Q_ack`, com `mtype = operation_id` |
| Limite de ACK | 64 operações com ACK simultâneas | Capacidade de `Q_ack`, com drenagem pelo produtor |
| Espera pelo ACK | `semop(−n)` | `msgrcv` por tipo |
| Garantia contra deadlock de ACK | `V` nunca bloqueia | Produtor `ack=1` sempre drena antes de esperar |
| Janela retirada→contagem | Nenhuma (mesma seção crítica) | Poucas instruções, só em término forçado |

### Cancelamento e sinais

- Esperas bloqueantes (`semop`, `msgsnd`, `msgrcv`, `nanosleep`) não repetem `EINTR`
  por conta própria; devolvem `MPMC_ERR_INTERRUPTED`, e o worker decide encerrar.
- No modo 1, os vetores de aquisição/publicação são atômicos individualmente, mas
  SIGTERM/SIGINT não são mascarados no intervalo entre eles. Término de processo
  nesse intervalo exige descartar o contexto por encerramento coordenado; não há
  garantia de cancelamento isolado com preservação do contexto na #10.
- Falhas fatais de IPC exigem interromper o uso de dados e estatísticas do contexto.
  Sua comunicação ao supervisor e o encaminhamento do shutdown são integração da #20;
  a propagação do erro pelo backend não executa esse encerramento automaticamente.
- O supervisor remove os recursos (`IPC_RMID`) somente depois de recolher todos os filhos.

## Semáforos e invariantes da memória compartilhada

Para cada segmento, `0 <= count <= capacity`. A implementação de buffer circular deve
satisfazer `tail` apontando para a próxima posição de inserção e `head` para a
próxima retirada, ambas interpretadas módulo `capacity`.

Semáforos de cada estrutura (índices e valores iniciais em @ref ipc_wire_formats):

| Nome | Inicial | Semântica |
|---|---:|---|
| `mutex` | `1` | Acesso exclusivo aos metadados da estrutura |
| `empty` | `capacity` | Reservas livres para inserção |
| `full` | `0` | Itens disponíveis para remoção |

**Cuidado:** as contagens dos semáforos refletem também reservas e sinalizações ainda
não finalizadas durante transições; não é correto comparar ingenuamente `semval(empty)`
com `capacity - count` a qualquer instante sem considerar operações em voo.

Sequências normais: produtor `[P(empty), P(mutex)]`, grava/avança,
`[V(mutex), V(full)]`; consumidor `[P(full), P(mutex)]`, lê/avança,
`[V(mutex), V(empty)]`. Cada par entre colchetes é um único vetor atômico de `semop()`.
Operações com possibilidade de bloqueio devem ocorrer **fora da seção crítica**.
A aquisição interrompida ou rejeitada não deixa vagas/itens parcialmente reservados.
Uma falha fatal após aquisição ou término de processo nesse intervalo exige
descartar o contexto; não há garantia de compensação para reutilizá-lo.

A contagem total de mensagens consumidas deve ser persistida em memória visível aos
processos, atualizada atomicamente/sob exclusão mútua.

## Filas de mensagens e notificações

Cada estrutura lógica `i` é representada por uma fila de dados independente. Tipos,
corpos e o algoritmo de ACK escolhido estão em @ref ipc_wire_formats.
O backend pode criar filas **auxiliares de controle** para ACKs e eventos, mas dados
do paradigma 2 continuam trafegando por filas System V, nunca por um buffer `shm`.

Mensagens de controle precisam distinguir:

- Confirmação de consumo (`operation_id` produtor e identidade da mensagem; o
  destino é o contexto do produtor, não o PID lógico da fila);
- Evento de mensagem consumida (`logical_producer_id`, para estatísticas);
- Solicitação de encerramento/cancelamento, caso um canal explícito seja escolhido.

O evento de estatística identifica a fila lógica de onde a mensagem foi retirada;
não deve inferir essa fila a partir de `sender_pid`. A mensagem preservada continua
contendo a identidade de seu emissor original.

Apenas o supervisor pode consolidar um contador local baseado nos eventos recebidos,
por meio de `ipc_poll_events`; não basta incrementar uma variável em um filho, pois
o heap após `fork` não é compartilhado com o pai.

### Evitar deadlock no canal de confirmações

Se os consumidores publicarem um ACK por mensagem em uma fila de controle de capacidade
limitada, uma fila lotada pode bloquear consumidores; produtores podem simultaneamente
estar bloqueados tentando enviar mais mensagens à fila de dados. Isso cria possibilidade
real de espera circular. A implementação **deve** escolher um protocolo sem esse ciclo:
por exemplo, confirmação agregada, consumo intercalado de ACKs durante o envio não
bloqueante, ou canal e política de capacidade comprovadamente suficientes para o limite
de simultaneidade. **Não considerar um simples `msgsnd` bloqueante para cada ACK
automaticamente livre de deadlock.**

Também é necessário garantir que a publicação de eventos não atrase
indefinidamente um consumidor não bloqueante (`ack=2`), nem deixe totais
inconsistentes quando houver cancelamento entre retirada e publicação.

É necessário garantir que cada ACK seja correlacionado ao trabalho correto,
mesmo que vários comandos `produz` usem o mesmo índice lógico simultaneamente.

## Shutdown com operações bloqueadas

O supervisor deve parar de aceitar comandos, notificar/encerrar os filhos e recolhê-los
antes de remover recursos globais. Nenhum filho pode ficar bloqueado indefinidamente
em `semop`, `msgsnd`, `msgrcv` ou no ACK. A política exata de sinais e prazo para
encerramento forçado deve evitar zumbis e recursos System V abandonados.

A coordenação implementada concede dois segundos, comuns a todos os filhos, após
o início da sinalização por `SIGTERM`. `CLOCK_MONOTONIC` define o instante limite;
`poll()` espera notificações no self-pipe pelo tempo restante, e `waitpid()` com
`WNOHANG` recolhe os filhos disponíveis. Despertares e `EINTR` não reiniciam o prazo.
Ao vencer o prazo, o supervisor envia `SIGKILL` aos filhos restantes e os recolhe.
Esse prazo limita a fase cooperativa; a coleta após `SIGKILL` ainda espera o kernel.

A espera instala temporariamente o tratador de `SIGCHLD` e desbloqueia esse sinal,
restaurando a ação e a máscara anteriores ao terminar. Portanto, funciona também
quando chamada diretamente ou após o REPL restaurar seus tratadores. Falhas no
relógio, configuração ou espera antecipam a tentativa de encerramento forçado e
não substituem o primeiro erro. Se `SIGKILL` falhar, a coleta final não bloqueia:
o chamador deve resolver o erro e repetir o shutdown antes de destruir o contexto.

Um reporter opcional acompanha o fluxo de `supervisor_dispatch()` até a sinalização,
coleta e espera. Cada falha é comunicada somente onde se origina; a propagação do
resultado não duplica o diagnóstico. O retorno preserva o primeiro erro, inclusive
quando a drenagem de eventos falha antes de uma coleta ou quando a restauração de
sinais falha após outro erro. A callback recebe valores já capturados e pode alterar
errno sem modificar as causas registradas. O chamador pode copiar os diagnósticos;
o supervisor não retém o reporter nem seus dados.

A união do diagnóstico é discriminada pela operação: envio usa PID e sinal, coleta
e registro de relatório usam PID, e espera usa os bits de `poll.revents`. Erro do
próprio `poll()` usa revents zero e errno capturado; erro indicado no descritor usa
os bits originais e system_errno zero. Operações de configuração de sinais e de
cálculo do prazo não possuem dados específicos. O fallback de duração zero nos
relatórios continua sendo uma degradação permitida, sem diagnóstico de shutdown.

Essa coordenação é exercitada com filhos controlados. A validação de shutdown
durante todas as esperas reais de dados/espaço/ACK continua na integração de #20/#26.

A remoção de um segmento/filas não é substituto geral para supervisão dos filhos.
O `waitpid()` deve ser usado para recolher processos. `IPC_RMID` é responsabilidade
única do supervisor.

## Requisitos versus decisões propostas

A obrigatoriedade de `shm + sem`, `msg`, tamanho de 2048 bytes, comandos, flags e
informações da mensagem vem do enunciado. Buffer circular, identificador de operação,
formato do ACK, organização em backends e filas auxiliares de controle são **decisões
arquiteturais do projeto**, fixadas em @ref ipc_wire_formats e a revisar durante os testes
de concorrência. Os limites que elas introduzem (63 entradas por segmento, 64 operações
com ACK simultâneas no modo 1, ACKs órfãos e a janela de contagem no modo 2) estão
declarados ali e não vêm do enunciado.
