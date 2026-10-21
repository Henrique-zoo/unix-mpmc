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
Os três primeiros campos atendem ao conteúdo mínimo do enunciado; os demais são
metadados internos necessários para correlação de confirmações.

O identificador `operation_id` é atribuído pelo supervisor **antes de `fork()`**, de
forma que operações concorrentes tenham IDs distintos. O PID representa o processo
emissor real; **não** é equivalente ao índice lógico de produtor. `sequence` deve
ser crescente no contexto de cada processo produtor, inclusive no caso *striped*.

O instante `sent_at` é capturado no momento de envio, usando `CLOCK_REALTIME`.
Uma cópia serializada no formato da fila pode envolver campos de controle extras;
a estrutura enviada a `msgsnd` exige o `long mtype` inicial e um `msgsz` que
**não inclui `mtype`**.

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
`ipc_ack_wait` (ACKs não são eventos de borda que possam se perder).

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

- Confirmação de consumo (`operation_id` e destinatário do ACK);
- Evento de mensagem consumida (`logical_producer_id`, para estatísticas);
- Solicitação de encerramento/cancelamento, caso um canal explícito seja escolhido.

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
