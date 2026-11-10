# Arquitetura e fronteiras entre módulos {#architecture}

@tableofcontents

O [guia de leitura](reading_guide.md) organiza a documentação e os contratos públicos.
Esta página descreve quem participa de cada etapa; os formatos e invariantes
estão no [protocolo](protocol.md), e os erros no [fluxo de diagnósticos](diagnostics.md).

## Camadas e dependências

O projeto adota uma arquitetura em camadas orientada a processos. As setas abaixo
representam chamadas e dependências; a posição de um módulo não determina em qual
processo ele executa:

```text
main -> repl -> command
  |       |
  +-----> supervisor -> producer / consumer
                |                |
                +--> transport <-+
                         |
                         +-- shm_backend -> shm / sem
                         |      |
                         |      +-- shm_ring
                         +-- msg_backend -> msg
```

`command` é um auxiliar da aplicação, não uma etapa obrigatória entre todas as
camadas. O supervisor usa o transporte para criar, consultar e destruir o contexto;
o ponto de entrada que ele executa no filho chama o worker. Workers usam apenas a
fachada de transporte, sem escolher paradigma ou chamar primitivas System V.
`shm_ring` manipula memória já associada; a sincronização pertence às operações do
backend. O backend MSG reúne dados, ACK e eventos em um único módulo.

`config.h`, `message.h`, `error.h` e `diagnostic.h` são contratos comuns usados
nas camadas pertinentes. Resultados e diagnósticos voltam aos chamadores e ao pai
por retornos, callbacks e pipes; esse fluxo de informação não introduz dependência
dos backends em `app/`.

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
| `shm_ring` | Manipular cabeçalho e posições de um buffer já associado | Adquirir semáforos ou criar objetos IPC |
| `error`, `diagnostic` | Representar resultados e comunicar falhas com contexto | Decidir a apresentação no terminal |
| `app/diagnostics`, `app/job_outcome` | Apresentar falhas e formatar o resultado do trabalho | Implementar o protocolo de dados ou ACK |

## Percurso de uma execução

### Inicialização no processo principal

1. `main()` valida `p` e o paradigma, instala o observador que apresenta erros e
   chama `supervisor_create()`.
2. O supervisor cria seu estado local e o self-pipe, e chama `ipc_init()`.
3. O transporte seleciona um backend. O backend adquire os objetos globais por
   `sysv_*` e, no SHM, inicializa o ring antes de disponibilizá-lo aos filhos.
   Em falha parcial, o backend executa rollback e preserva a causa inicial.
4. `main()` entrega o supervisor pronto a `repl_run()`.

### Comando e trabalho no processo filho

1. O REPL lê uma linha, chama `command_parse()` e `command_validate()` e entrega
   o comando normalizado a `supervisor_dispatch()`. Comando inválido não cria filho.
2. Para `produz`/`consome`, o supervisor prepara identidade e canal de resultados,
   faz `fork()` e registra o filho no pai. O REPL continua atendendo entrada e eventos.
3. No filho, o ponto de entrada instala o emissor de diagnósticos e o cancelamento
   cooperativo. Ele chama `producer_run_with_result()` ou `consumer_run()`.
4. O produtor constrói metadados e calcula o destino, incluindo striped, antes de
   chamar `ipc_send()`. Com ACK, usa a sequência begin/envios/wait/end. O consumidor
   chama `ipc_receive()` até completar a quantidade ou encontrar um erro.
5. O transporte valida as regras comuns e despacha para o backend. No SHM, as
   operações reservam semáforos, manipulam o ring e publicam item/vaga. No MSG,
   enviam ou recebem envelopes pelas filas. O próprio backend contabiliza a
   retirada e confirma o consumo ao produtor quando necessário.
6. O ponto de entrada fecha somente o contexto local com `ipc_worker_close()`,
   preserva as falhas do trabalho e do fechamento, envia o resultado final pelo
   pipe e encerra o filho. O worker não fecha o contexto por conta própria.

### Retorno ao pai e encerramento

Os canais têm funções distintas:

| Canal/estado | Informação | Quem usa |
|---|---|---|
| Dados e ACK do backend | Mensagens e confirmação de seu consumo | Workers, pela fachada `ipc_*` |
| Contador SHM ou eventos MSG | Total acumulado por estrutura | Supervisor, por `ipc_poll_events()` e `ipc_consumed_count()` |
| Pipe próprio de cada filho | Diagnósticos e resultado com progresso por trabalho | Filho publica; supervisor valida, drena e apresenta |
| Self-pipe | Notificação para acordar o loop, sem resultado no byte | Tratadores e emissores notificam; REPL/supervisor drenam |
| `waitpid()` | Término efetivo, código de saída ou sinal | Supervisor recolhe o filho e registra sua duração |

O resultado final pode chegar antes de `waitpid()` indicar o término. Receber um
resultado não dispensa a coleta do filho. Enquanto a entrada fica ociosa, o REPL
continua dando oportunidade de drenagem e coleta ao supervisor.

`shutdown`, EOF ou sinal de término encaminham o encerramento coordenado.
O supervisor impede novos trabalhos, solicita cancelamento, mantém a drenagem
durante o prazo e recolhe os filhos, com SIGKILL de reserva. Só depois de comprovar
que nenhum filho permanece ativo marca o contexto como quiescente. `main()` pede
o relatório e chama `supervisor_destroy()`, que delega a remoção global a
`ipc_destroy()`. Os detalhes do prazo e das falhas estão nas seções seguintes.

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
impede perda de sinais que cheguem durante a coleta. O filho fecha a leitura do self-pipe e conserva a escrita até terminar de
publicar diagnósticos e resultado; o pai drena esse canal antes dos pipes de resultados. Não chamar `printf`, `malloc` ou rotinas de IPC não assíncronas dentro do tratador
de sinal.

Durante `fork()`, `supervisor_dispatch` bloqueia `SIGCHLD`, `SIGINT` e `SIGTERM`. O filho só
os libera depois de configurar suas ações de sinais, e o pai os libera após registrar o PID. Sem isso,
um `SIGTERM` enviado por `shutdown` imediatamente após o despacho cairia no tratador herdado do
REPL (que apenas marca uma flag) e o filho o ignoraria, travando o `waitpid()` bloqueante. O
descritor de leitura/escrita do self-pipe é movido para fora de 0..2, para não colidir com
`stdin` quando o processo é iniciado com descritores padrão fechados.

O shutdown impede novos trabalhos, envia `SIGTERM` e concede um prazo comum de
dois segundos aos filhos. A espera usa `CLOCK_MONOTONIC` e `poll()` no self-pipe,
recolhendo filhos sem bloquear a cada despertar. Ao vencer o prazo, envia `SIGKILL`
aos restantes e os recolhe. Um tratador temporário de `SIGCHLD` permite executar
esse fluxo fora do REPL; ação e máscara de sinais são restauradas ao sair.
Erros antecipam a tentativa de encerramento forçado, preservando o primeiro erro.
Se o envio de `SIGKILL` falhar, a função retorna após coleta não bloqueante e o
chamador deve resolver a falha e repetir o shutdown antes de destruir os recursos.

As operações de despacho, shutdown, coleta e drenagem recebem um
`supervisor_shutdown_reporter_t` opcional. Cada falha reportável é comunicada uma
vez, no ponto de origem, enquanto o retorno preserva o primeiro erro cronológico.
Diagnósticos incluem a operação, o resultado com errno e, conforme o discriminante,
PID/sinal, PID de coleta/relatório ou os eventos do descritor. Reporter e callback
nulos desabilitam somente os diagnósticos; não alteram o resultado da operação.
A callback pode copiar o registro temporário e é executada fora dos tratadores.
ESRCH ao sinalizar, EINTR repetido e vencimento do prazo são condições esperadas.

## Resultados dos filhos para o supervisor

Cada despacho cria um pipe próprio antes de fork(). O filho fecha descritores de
leitura herdados, executa o worker real e fecha somente seu contexto IPC local.
O resultado contém identidade do trabalho, quantidade solicitada, progresso e causa
de erro. O registro cabe nos 512 bytes mínimos de PIPE_BUF e é escrito atomicamente.
A leitura no pai é não bloqueante e acumula leituras parciais. Identidade e tipo são
conferidos antes da apresentação; EOF sem resultado é permitido para filhos mortos
por sinal. O pipe é fechado ao recolher o filho. O self-pipe acorda o controle após
publicar um resultado, e a coleta drena novamente o resultado depois de waitpid().

O estado de término continua vindo de waitpid(), nunca do heap do filho. Tempos usam
CLOCK_MONOTONIC. Totais por estrutura vêm do backend: contador SHM ou eventos MSG
consolidados por ipc_poll_events(). O consumidor informa progresso mesmo em consumo
parcial; o produtor também conserva a quantidade enviada se a espera de ACK falhar.

## Interface pública versus backends

Os workers usam somente `ipc/transport.h`; a fachada redireciona os métodos para o
backend selecionado. As funções `shm_backend_*` e `msg_backend_*` são internas à
implementação, mesmo quando seus protótipos são publicados em `include/` para
facilitar a documentação técnica.

A implementação de `shm_backend` fica no diretório `src/ipc/shm_backend/`.
O arquivo `lifecycle.c` contém a criação, a inicialização e a liberação dos
segmentos e conjuntos de semáforos. `operations.c` implementa envio, recebimento
e leitura sincronizada da contagem. `ack.c` implementa o registro, a correlação,
a notificação auxiliar e a espera de ACK por operação. `shm_ring.c` implementa
as operações locais do buffer, cujo layout e contratos privados ficam em
`shm_ring.h`. A interface para a fachada permanece em
`include/mpmc/ipc/shm_backend.h`.

Tipos que precisam ser conhecidos por vários arquivos de implementação devem
ser definidos em um cabeçalho privado no mesmo diretório. `shm_ring_t` já segue
essa organização. `shm_backend_internal.h` define `shm_segment_t`, os tipos de
controle de ACK e a representação completa de `struct shm_backend`,
incluindo os vetores locais de segmentos, os identificadores dos conjuntos e
os índices MUTEX/EMPTY/FULL e ACK. `lifecycle.c`, `operations.c` e `ack.c`
compartilham esses tipos. O layout e as operações do buffer continuam em
`shm_ring.h`, independentes dos tipos de contexto do backend.
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
callback. O backend não retém o reporter nem aloca uma lista de erros. Os recursos globais de ACK não pertencem a um produtor específico; falhas ao limpá-los são incluídas no primeiro erro retornado pela operação, mas não são atribuídas artificialmente a um `producer_id` na callback.

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

Falhas locais de argumentos, ausência de itens e interrupção da aquisição não
invalidam o contexto por si mesmas. As demais falhas de `semop()` são fatais ao
contexto inteiro: a operação pode já ter modificado o buffer e mantido MUTEX
adquirido. Como o resultado não identifica a etapa da falha, essa política também
se aplica aos erros de recursos na aquisição. O chamador deve suspender operações
de dados e estatísticas, propagar o erro ao supervisor e encaminhar o encerramento
coordenado. A leitura de totais segue a mesma política se a liberação de MUTEX
falhar, embora preserve a saída. O contrato completo está em
@ref shm_operation_failures.

O backend propaga a falha e o diagnóstico de contexto comprometido. O supervisor
recebe o registro do worker e inicia o encerramento coordenado; o backend não
repara o estado para continuar novas operações. Cada worker fecha seu estado local; somente após
recolher todos os filhos o supervisor destrói os objetos globais. Não há tentativa
de reparar contadores ou continuar usando o contexto após uma falha fatal.

O módulo `ack.c` implementa o registro, a correlação por operação, a espera e a
finalização dos ACKs SHM. O recebimento confirma cada retirada e publica somente
a conclusão final; não há bitmap nem deduplicação por sequência. Workers reais
tratam SIGTERM/SIGINT como pedido de cancelamento e concluem as publicações já
iniciadas. Morte forçada entre aquisição e publicação exige descartar o contexto;
recuperá-lo para continuar operações não faz parte do contrato.
`worker_close` e `destroy` tratam backend nulo como operação vazia,
retornando sucesso antes de consultar o contexto. A auxiliar privada exige backend
não nulo; os chamadores garantem essa pré-condição, inclusive no rollback de `create`.

O fluxo, os papéis dos tipos e um exemplo completo estão em
[Diagnósticos de cleanup](cleanup_errors.md) (@ref shm_cleanup_diagnostics).

## Falhas e recuperação

1. Parsing inválido: imprimir mensagem e voltar ao prompt, sem criar filhos.
2. Índice lógico inválido: não criar processo nem acessar IPC.
3. Falha no `fork`: registrar erro e manter o REPL operacional.
4. Falha no IPC durante o filho: reportar diagnóstico, liberar recursos locais e sair.
   No modo 1, uma falha fatal exige também suspender o uso do contexto e encaminhar
   o encerramento coordenado pelo supervisor, conforme @ref shm_operation_failures.
5. Falha durante `ipc_init`: remover somente os recursos já criados.
6. `shutdown`: impedir novos jobs, encerrar filhos pendentes, coletar eventos e
   remover todos os objetos IPC por responsabilidade do pai. Se o contexto SHM
   sofreu uma falha fatal, não tentar ler seus totais sob um mutex comprometido.
7. `EINTR`: decidir explicitamente entre repetir operação (normal) e abortar por
   encerramento solicitado (shutdown), sem loops infinitos.

## Apresentação de resultados de trabalhos

Cada trabalho (`produz`/`consome`) pode terminar de cinco formas, descritas por
`job_outcome_t` no helper privado `src/app/job_outcome.{h,c}`: concluído, parcial,
cancelado, erro de kernel e falha ao criar o filho. O texto vai para `stderr` em uma
única linha `mpmc: <diagnóstico>`, o prompt `>> ` permanece em `stdout`, e o REPL
continua aceitando o próximo comando depois de qualquer erro recuperável.

| Resultado | Texto |
|---|---|
| Concluído | `operação N (consome, estrutura I) concluída: 5 de 5 mensagens consumidas.` |
| Parcial | `operação N (consome, estrutura I) não foi concluída: apenas 3 de 5 mensagens consumidas.` |
| Cancelado | `operação N (produz, estrutura I) cancelada antes de terminar: 2 de 8 mensagens produzidas.` |
| Erro de kernel | `operação N (produz, estrutura I) falhou após 4 de 10 mensagens produzidas: <causa>` |
| Falha ao criar o filho | `comando (produz, estrutura I): não foi possível criar o processo filho: <causa>` |

- O índice é `estrutura I` ou `todas as estruturas` em `produz 0`. Na falha ao criar o
  filho ainda não há identificador de operação (ele só é consumido quando o `fork()`
  funciona), por isso o texto usa `comando` em vez de `operação N`.
- `system_errno` só é interpretado quando `status != MPMC_OK` e o valor está em
  `1..4095`; fora disso o texto traz apenas a categoria, sem `Success` ou
  `Unknown error`.
- Parcial, cancelado, erro e falha de criação nunca são apresentados como conclusão. Um
  evento "concluído" com quantidade efetiva diferente da solicitada é tratado como
  parcial.
- Eventos concorrentes, e os avisos de término de filhos, não têm ordem global
  garantida; cada um ocupa uma linha inteira. Cada linha conta como aviso para o
  redesenho do prompt em terminais, e eventos pendentes são apresentados no `shutdown`.
- Resultados reais são lidos dos pipes de cada filho e apresentados pelo
  supervisor, inclusive antes do recolhimento. `supervisor_queue_outcome()` é
  uma interface privada adicional para eventos controlados nos testes, com fila
  finita; ela não armazena o fluxo de diagnósticos dos workers reais.

## Garantias e limites da integração

Os dois workers executam operações reais pela fachada. Diagnósticos e resultados
por trabalho chegam ao supervisor pelos pipes; contagens por estrutura vêm dos
backends. Falhas fatais observadas nesses canais comprometem o contexto e acionam
o encerramento coordenado, sem continuar aceitando trabalhos.

No modo MSG, dados, eventos e ACKs pertencem a filas diferentes. Publicações já
devidas são concluídas no cancelamento cooperativo e no fechamento local, mas
uma morte forçada entre retirada e publicação pode perder o evento/ACK. Não há
transação do kernel abrangendo as duas filas. No SHM, a aquisição e a publicação
de semáforos são atômicas individualmente; não formam uma transação com a alteração
de memória. A recuperação de um contexto após morte arbitrária não é oferecida.

Após recolher todos os filhos, o supervisor marca o contexto como quiescente. A
fachada recusa novos envios/recebimentos/esperas e lê os totais SHM sem mutex:
um filho encerrado pode ter deixado o mutex adquirido. A pré-condição é a ausência
comprovada de workers vivos; durante a execução a consulta continua sincronizada.


## Admissão e custo de memória

Não existe teto fixo de p: ele deve ser positivo e representável em size_t.
A inicialização tenta criar somente os recursos solicitados e desfaz tudo em
falha de alocação ou de limites do kernel. SHM reserva 2048 × (p + 1) bytes
lógicos: p buffers de dados e um controle ACK. Filas MSG usam p + 2 objetos.
Páginas, tabelas de processos, descritores e metadados do kernel têm custo adicional.

O controle ACK continua com 64 registros, 1040 bytes de conteúdo em um segmento
reservado de 2048 bytes. O conjunto possui 66 semáforos: mutex, vagas e 64 conclusões.
Uma tabela cheia suspende a admissão de um produtor antes de seu primeiro envio,
sem manter mutex e sem busy waiting; finalizar um lote devolve uma vaga. Isso
limita a memória do controle, sem rejeitar o 65º comando. A espera é cancelável.
O custo de processos filhos cresce com os trabalhos lançados, conforme o modelo
de processos exigido pelo enunciado.

Tempos usam CLOCK_MONOTONIC. Erros conservam errno e o contexto do filho;
relatórios com duration_error exibem indisponível e permanecem disponíveis.
O relatório final conserva contagens e estados mesmo se sua medição total falhar.
