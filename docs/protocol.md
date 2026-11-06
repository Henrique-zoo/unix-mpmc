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

## Semáforos e invariantes da memória compartilhada

Para cada segmento, `0 <= count <= capacity`. A implementação de buffer circular deve
satisfazer `tail` apontando para a próxima posição de inserção e `head` para a
próxima retirada, ambas interpretadas módulo `capacity`.

Semáforos sugeridos:

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

Cada estrutura lógica `i` é representada por uma fila de dados independente.
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
formato do ACK, organização em backends e fila auxiliar de controle são **decisões
arquiteturais sugeridas**, a revisar durante os testes de concorrência.
