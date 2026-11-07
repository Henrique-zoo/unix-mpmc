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

`message_t` contém `sender_pid`, `sequence`, `sent_at`, `operation_id` e `ack_required`.
`sender_pid`, `sequence` e `sent_at` são campos mínimos obrigatórios do enunciado:
devem existir e ser preservados em toda mensagem, independentemente do modo IPC.
`operation_id` e `ack_required` são extensões internas; não substituem nem alteram
o significado dos campos obrigatórios. `producer_id` não pertence à mensagem: é
o índice lógico da fila/estrutura escolhida pelo comando e passado separadamente
a `ipc_send()`/`ipc_receive()`.

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

Quando `ack_required=false`, o consumo não gera ACK ao produtor. Quando é `true`,
cada retirada efetiva gera exatamente uma confirmação associada à chave da
mensagem; uma mensagem não retirada não pode ser confirmada. O backend pode
agregar confirmações para transporte, mas deve manter a contagem por
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
2. Enviar as `n` mensagens marcadas com `ack_required=true`.
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
contagem de `i` e gera confirmação **se a mensagem tiver `ack_required=true`**.

### `consome i n 2`

O filho executa até `n` tentativas de remoção sem esperar. Se `ipc_receive`
retornar `MPMC_ERR_WOULD_BLOCK`, finaliza com erro de quantidade insuficiente e
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
| `ack_required` | 32 | 1 | Seguido de 7 bytes de preenchimento |
| **Total** | | **40** | Alinhamento 8 |

Regras comuns aos dois backends:

- `sent_at` é gravado pelo produtor imediatamente antes da chamada a `ipc_send()` daquela
  mensagem e nunca é reescrito por backend ou consumidor. É o instante em que o produtor
  inicia o envio, e não o instante em que o comando foi criado nem o da inserção: se o
  envio esperar vaga, a inserção ocorre depois do `sent_at`.
- O backend não altera `sender_pid`, `sequence`, `sent_at`, `operation_id` nem
  `ack_required`. O consumidor recebe a mensagem exatamente como o produtor a enviou.
- O índice lógico `producer_id` nunca é gravado na mensagem; ele é o endereço (segmento
  ou fila) onde a mensagem fica.

### Backend 1: memória compartilhada e semáforos

**Recursos criados pelo supervisor em `ipc_init()`:**

| Recurso | Quantidade | Conteúdo |
|---|---:|---|
| Segmento `S_i` | `p` | Exatamente 2048 bytes, buffer circular da estrutura `i` |
| Conjunto de semáforos `M_i` | `p` | 3 semáforos da estrutura `i` |
| Segmento de controle `C` | 1 | Tabela de operações com ACK pendente |
| Conjunto de semáforos `K` | 1 | `1 + 64` semáforos de ACK |

**Layout de `S_i` (2048 bytes):**

| Faixa | Conteúdo |
|---|---|
| 0–3 | `head` (`uint32_t`): próxima posição de retirada |
| 4–7 | `tail` (`uint32_t`): próxima posição de inserção |
| 8–11 | `count` (`uint32_t`): entradas ocupadas |
| 12–15 | `capacity` (`uint32_t`): gravado em `ipc_init()`, vale 50 |
| 16–23 | `consumed_total` (`uint64_t`): mensagens retiradas desta estrutura |
| 24–2023 | 50 entradas de 40 bytes (`message_t`); a entrada `j` começa em `24 + 40·j` |
| 2024–2047 | Reservado, zerado |

A capacidade segue a fórmula da seção anterior: `(2048 − 24) / 40 = 50`. Uma estrutura
comporta no máximo 50 mensagens pendentes; um `produz i 1024 2` sem consumidores bloqueia
na 51ª, e isso é o comportamento previsto (backpressure), não um erro.

**Semáforos de `M_i`:** índice 0 = `mutex` (inicial 1); índice 1 = `empty` (inicial 50);
índice 2 = `full` (inicial 0). Os índices valem 0, 1 e 2 em todas as estruturas.

**Envio (`shm_backend_send`):**

1. `P(empty)` bloqueante. Se interrompido, nada foi reservado e retorna `MPMC_ERR_INTERRUPTED`.
2. Bloqueia `SIGTERM` e `SIGINT` com `sigprocmask()`.
3. `P(mutex)`; grava a entrada em `tail`; `tail = (tail + 1) % capacity`; `count++`; `V(mutex)`.
4. `V(full)`; restaura a máscara de sinais (um término pendente é entregue agora).

Se a obtenção de `mutex` falhar depois de `P(empty)`, o backend executa `V(empty)` para
devolver a vaga antes de retornar o erro. Nenhuma espera bloqueante ocorre com `mutex`
adquirido, e `SEM_UNDO` não é usado.

**Recebimento (`shm_backend_receive`):**

1. `P(full)` (com `IPC_NOWAIT` se `blocking=false`; `EAGAIN` vira `MPMC_ERR_WOULD_BLOCK`).
2. Bloqueia `SIGTERM` e `SIGINT`.
3. `P(mutex)`; copia a entrada de `head`; `head = (head + 1) % capacity`; `count--`;
   `consumed_total++`; `V(mutex)`. Remoção e contagem são uma única seção crítica,
   então não existe mensagem retirada e não contada.
4. `V(empty)`; se `ack_required`, executa o protocolo de ACK abaixo; restaura a máscara.

**Limite da proteção contra sinais.** O mascaramento acima protege somente o
intervalo posterior a sigprocmask(): ele não torna atômicos o retorno de P e o
bloqueio dos sinais. Um término nessa janela pode deixar uma reserva pendente.

No shutdown global, todos os filhos são encerrados e recolhidos antes da remoção
dos recursos, e essas reservas são descartadas junto com o contexto. Cancelar
somente um trabalho e continuar usando o contexto exige outro protocolo de
cancelamento, a definir nas issues #10 e #20. Simplesmente bloquear os sinais
antes de um semop() bloqueante pode impedir que SIGTERM cancele a espera.

**ACK por operação (`ack=1`).** O segmento de controle `C` guarda 64 posições de 8 bytes,
cada uma com um `operation_id` (0 = livre). O conjunto `K` tem o semáforo 0 como
`ack_mutex` (inicial 1) e, nos índices 1 a 64, um contador de ACKs por posição (inicial 0).
Como os consumidores são criados antes de o produtor existir, este é o único ponto de
encontro que ambos já herdaram por `fork()`.

| Passo | Quem | Ação |
|---|---|---|
| `ack_begin(op, n)` | Produtor | `P(ack_mutex)`; recusa `op` já presente (`MPMC_ERR_STATE`); toma a primeira posição livre (sem posição livre: `MPMC_ERR_NO_SPACE`); grava `op`; zera o contador da posição; `V(ack_mutex)` |
| ACK | Consumidor | `P(ack_mutex)`; procura `op = message.operation_id`; se achar, `V(contador)` na posição; `V(ack_mutex)`. Se não achar, o ACK é descartado (o produtor já terminou ou foi cancelado) |
| `ack_wait(op)` | Produtor | `semop(contador, −n)`: bloqueia até o contador valer pelo menos `n` e subtrai `n` de uma vez |
| `ack_end(op)` | Produtor | `P(ack_mutex)`; grava 0 na posição; `V(ack_mutex)`. Chamado em sucesso, falha ou cancelamento |

Propriedades que esse desenho garante:

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
`ipc_poll_events()` não faz nada neste modo.

### Backend 2: filas de mensagens System V

**Recursos criados pelo supervisor em `ipc_init()`:**

| Fila | Quantidade | Tipo (`mtype`) | Corpo (`msgsz`, sem `mtype`) |
|---|---:|---|---|
| Dados `D_i` | `p` | 1 | `message_t`, 40 bytes |
| ACK `Q_ack` | 1 | `operation_id` do produtor | `{ pid_t sender_pid; uint32_t sequence; }`, 8 bytes |
| Eventos `Q_evt` | 1 | 1 | `{ uint32_t producer_id; }`, 4 bytes |

Os dados do paradigma 2 trafegam só por filas, nunca por `shm`. As filas `Q_ack` e `Q_evt`
são recursos de controle: não substituem as `p` filas de dados exigidas.

**Capacidade.** O limite de cada fila é `msg_qbytes`. Neste ambiente de teste ele vale
16384 bytes e a fila de dados encheu com 409 mensagens de 40 bytes; a de ACK, com 2048
mensagens de 8 bytes. O valor varia entre sistemas e deve ser tratado como dado, não como
constante. Como no modo 1, `msgsnd` bloqueante em fila cheia é backpressure esperado.

**Envio.** `msgsnd(D_i, {1, message_t})` com `msgsz = sizeof(message_t)`. Se
`ack_required=false`, usa a forma bloqueante, porque nenhum ACK depende desse produtor.
Se `ack_required=true`, usa o laço descrito em "ACK por operação" abaixo.

**Recebimento.** `msgrcv(D_i, tipo 1, msgsz = sizeof(message_t))`, sem `MSG_NOERROR`;
com `blocking=false` acrescenta `IPC_NOWAIT` e `ENOMSG` vira `MPMC_ERR_WOULD_BLOCK`. Um
corpo de tamanho diferente de `sizeof(message_t)` é violação de contrato
(`MPMC_ERR_STATE`). Depois de retirar a mensagem, o consumidor, nesta ordem:

1. envia `{producer_id}` a `Q_evt` (um evento por mensagem retirada, com ou sem ACK);
2. se `ack_required`, envia `{sender_pid, sequence}` a `Q_ack` com `mtype = operation_id`.

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
| `ack_end(op)` | Drena sem bloquear as confirmações restantes de `op`; não há recurso do kernel a liberar |

Por que isso não trava: um `msgsnd` bloqueante por ACK, sozinho, permitiria o ciclo em que
produtores esperam vaga em `D_i` enquanto consumidores esperam vaga em `Q_ack`. Aqui, um
produtor com `ack=1` nunca fica bloqueado no kernel sem drenar os próprios ACKs: ele só
espera em `ack_wait` (que consome ACKs) ou em `nanosleep` curto (depois de drenar). Logo,
se `Q_ack` encher, o produtor dono dos ACKs acaba drenando e libera os consumidores.

**Limite conhecido, ACKs órfãos.** Se um produtor morre antes de receber seus ACKs, as
confirmações que chegarem depois ficam em `Q_ack` sem dono. Ao recolher um filho produtor,
o supervisor drena `Q_ack` no tipo do `operation_id` dele; ACKs que cheguem depois dessa
purga permanecem até o `shutdown`. Em operação normal isso não ocorre, porque o produtor
só termina depois de receber todos os ACKs.

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

### Comparação dos dois backends

| Aspecto | Memória compartilhada | Filas de mensagens |
|---|---|---|
| Unidade de dados | Entrada de 40 bytes em `S_i` | Mensagem System V com corpo de 40 bytes em `D_i` |
| Capacidade por estrutura | 50 (fixa pela fórmula de 2048 bytes) | Definida por `msg_qbytes` (409 neste ambiente) |
| Ordem | FIFO por `head`/`tail` | FIFO da fila para o tipo 1 |
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
- No modo 1, as seções críticas que contêm retirada e contagem rodam com `SIGTERM` e
  `SIGINT` bloqueados; um término recebido nesse intervalo é entregue ao final, sem
  deixar a estrutura em estado parcial.
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

Sequências normais: produtor `P(empty)`, `P(mutex)`, grava/avança, `V(mutex)`, `V(full)`;
consumidor `P(full)`, `P(mutex)`, lê/avança, `V(mutex)`, `V(empty)`.
Operações com possibilidade de bloqueio devem ocorrer **fora da seção crítica**.
Ao tratar interrupções e falhas, não deixar vagas/itens reservados sem compensação.

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
encerramento forçado é decisão de implementação, mas deve evitar zumbis e recursos
System V abandonados.

A remoção de um segmento/filas não é substituto geral para supervisão dos filhos.
O `waitpid()` deve ser usado para recolher processos. `IPC_RMID` é responsabilidade
única do supervisor.

## Requisitos versus decisões propostas

A obrigatoriedade de `shm + sem`, `msg`, tamanho de 2048 bytes, comandos, flags e
informações da mensagem vem do enunciado. Buffer circular, identificador de operação,
formato do ACK, organização em backends e filas auxiliares de controle são **decisões
arquiteturais do projeto**, fixadas em @ref ipc_wire_formats e a revisar durante os testes
de concorrência. Os limites que elas introduzem (50 entradas por segmento, 64 operações
com ACK simultâneas no modo 1, ACKs órfãos e a janela de contagem no modo 2) estão
declarados ali e não vêm do enunciado.
