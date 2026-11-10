# unix-mpmc

Aplicação em C23 para o Trabalho Prático 02/2026 de Sistemas Operacionais da
Universidade de Brasília: múltiplos produtores e consumidores com IPC System V.

Os dois paradigmas executam trabalhos reais em processos filhos, enquanto o
processo principal aceita comandos, acompanha os filhos e apresenta os resultados.
O modo 1 usa memória compartilhada e semáforos; o modo 2 usa filas de mensagens.

## Compilação e testes

Dependências da aplicação: Clang 23 com suporte a C23, GNU Make e um sistema Unix
com IPC System V. Os testes de integração também usam Python 3. Doxygen é
necessário para gerar a documentação; clang-format 23 verifica a formatação.
A CI executa em Linux, com debug e release. macOS não foi executado nesta revisão;
a disponibilidade e os limites de IPC precisam ser suficientes no sistema usado.

```sh
make                    # build/debug/mpmc e link de conveniência ./mpmc
make MODE=release       # build/release/mpmc e atualização do link
make test               # unitários e integração do executável debug
make MODE=release test  # mesmos testes para release
make docs               # build/docs/html/index.html
make clean
```

O build utiliza `-std=c23`, `-Wall -Wextra -Wpedantic -Wshadow -Werror`.
A documentação também reprova em caso de avisos Doxygen. Os fontes de testes
que injetam falhas são ligados apenas ao executável de prova, `mpmc_probe`.
As variáveis `MPMC_PROBE_*` não são interpretadas pelo programa distribuído.

## Inicialização e controle de jobs

```sh
./mpmc 3 1              # três estruturas lógicas, memória compartilhada
./mpmc 3 2              # três estruturas lógicas, filas de mensagens
```

`p` é um inteiro decimal positivo representável em `size_t`. Não há teto fixo de
1024 estruturas: esse limite do enunciado vale para a quantidade de mensagens de
um comando. O programa tenta criar somente os recursos solicitados e desfaz a
inicialização parcial se faltarem memória ou objetos IPC no kernel.

O shell pode iniciar o programa com `&`. Com `stdin` ligado ao terminal, o controle
de jobs pode suspender a leitura por `SIGTTIN`; use `fg` para dar o terminal ao job:

```sh
./mpmc 3 1 &
fg
```

Com entrada redirecionada por arquivo ou pipe, o job pode continuar em background.
EOF solicita o mesmo encerramento ordenado de `shutdown`. Por isso, um arquivo de
comandos que termina imediatamente após despachar workers pode cancelá-los antes
de completarem o trabalho. Para resultados específicos, sincronize pelo término
dos trabalhos, como fazem os testes de integração.

## Comandos do REPL

O programa apresenta `>>` e lê um comando por linha. O `&` ao final de cada comando
é opcional: `produz` e `consome` sempre criam um processo filho concorrente.

| Comando | Comportamento |
|---|---|
| `produz i n 1` | Envia n mensagens e aguarda o consumo de todas as mensagens desse trabalho. |
| `produz i n 2` | Envia n mensagens sem aguardar confirmação de consumo. |
| `produz 0 n ack` | Distribui as mensagens por 1, 2, ..., p, 1, ... com um único emissor. |
| `consome i n 1` | Aguarda até consumir n mensagens da estrutura i. |
| `consome i n 2` | Consome o que estiver disponível; se faltar, informa erro e quantidade efetiva, inclusive zero. |
| `shutdown` | Impede novos trabalhos, termina/recolhe filhos, imprime relatório e remove recursos próprios. |

`n` fica entre 1 e 1024, inclusive; `ack` deve ser 1 ou 2. Índices válidos ficam
em 1..p, com zero reservado ao striped de `produz`. Índices inválidos são rejeitados
antes de criar o filho. Produção pode aguardar espaço, inclusive com ack=2:
confirmação de consumo e capacidade do buffer são condições diferentes.

Exemplo interativo, repetível nos dois paradigmas:

```text
>> consome 1 3 1 &
>> consome 2 3 1 &
>> consome 3 3 1 &
>> produz 0 9 1 &
```

Aguarde os avisos de conclusão dos quatro trabalhos e então digite `shutdown`.
O relatório deve mostrar três mensagens consumidas em cada estrutura e quatro
filhos recolhidos. A ordem dos avisos entre processos concorrentes não é fixa.

## Estruturas, memória e ACK

Cada mensagem contém PID físico do emissor, sequência crescente dentro do trabalho,
instante de envio (`CLOCK_REALTIME`) e identificador interno da operação. O pedido
de confirmação pertence à operação; a mensagem não tem `ack_required`.

No modo SHM, cada estrutura lógica tem um segmento de **2048 bytes**, com buffer
circular de 63 mensagens de 32 bytes e cabeçalho alinhado de 16 bytes. Um conjunto
MUTEX/EMPTY/FULL protege cada buffer; os vetores de reserva/publicação usam semop.
Um segmento auxiliar de 2048 bytes mantém o controle de ACKs por lote.

A tabela mantém 64 registros ativos, com contador de consumos restantes e uma
notificação por conclusão de lote. Quando a tabela estiver cheia, um novo produtor
aguarda uma vaga **sem manter o mutex**. `ack_end` devolve a vaga. Assim, o 65º
comando é aceito sem ampliar a tabela nem retornar falta de espaço. Essa espera
também é cancelável. Há um mutex global de ACK, um semáforo de vagas e 64 de conclusão.

A reserva lógica de SHM é `2048 × (p + 1)` bytes. Para p=1024, são aproximadamente
2 MiB, mais metadados locais, páginas e objetos do kernel. O controle ACK continua
custando um segmento de 2 KiB independentemente dos trabalhos aguardando vaga.
Processos e descritores têm custos próprios; a memória total não é ilimitada.

No modo MSG, existem p filas de dados e duas auxiliares: ACK e eventos de consumo.
Os dados trafegam por `msg`, sem memória compartilhada ou semáforos desse backend.
Capacidades das filas dependem do kernel. Consumidores publicam ACK/eventos e o
supervisor consolida as contagens, inclusive quando o usuário não digita comandos.

Os recursos auxiliares são escolhas do protocolo. Todos os segmentos SHM possuem
2048 bytes, inclusive o segmento de controle. Não há prealocação para um p máximo.

## Encerramento, medições e diagnóstico

SIGINT/SIGTERM no processo principal solicitam shutdown. Workers reais usam
cancelamento cooperativo: publicações iniciadas, ACKs e eventos já devidos podem
concluir, e o contexto local é liberado antes de sair. O supervisor concede um
prazo monotônico comum de dois segundos; SIGKILL encerra quem não atender ao prazo.
Todos os filhos são recolhidos antes da remoção global normal dos recursos.

O relatório apresenta duração e término de cada filho, total consumido por
estrutura e duração do principal. Durações usam `CLOCK_MONOTONIC`, do despacho até
o recolhimento. Se uma medição falhar, aparece **indisponível**, o erro é preservado
e o callback o informa; uma falha não é representada como `0.000 s`. Os demais
estados e contagens continuam sendo apresentados.

As funções propagam `mpmc_result_t` e preservam errno capturado. Um observador
comum recebe cada falha, com função, operação, PID/trabalho e etapa de execução ou
cleanup. Diagnósticos dos filhos atravessam seus pipes; o pai faz a apresentação.
Vermelho é usado somente em terminal, respeitando `NO_COLOR` e `TERM=dumb`.

Falhas após alteração de estado compartilhado podem tornar o contexto inutilizável
e exigem shutdown; a operação não deve ser repetida como se nada tivesse ocorrido.
O protocolo não oferece recuperação transacional após morte arbitrária por SIGKILL.
Também não promete remover um objeto cuja remoção o kernel recuse: a falha é
comunicada e as demais liberações continuam sendo tentadas.

## Organização

```text
include/mpmc/           Contratos públicos, modelos e resultados
src/app/                Comandos, REPL, supervisor e apresentação
src/workers/            Produção, consumo e roteamento striped
src/ipc/transport.c     Fachada independente do paradigma
src/ipc/shm_backend/    Lifecycle, ring, operações e ACK privados
src/ipc/msg_backend.c   Dados, ACK e estatísticas por filas
src/ipc/{shm,sem,msg}.c Wrappers System V e tradução de errno
tests/unit/            Componentes, concorrência e injeção de falhas
tests/integration/     CLI, pseudo-terminal, shutdown e auditoria de recursos
docs/                  Arquitetura, protocolo, testes e rastreabilidade
```

Os contratos públicos ficam nos cabeçalhos; comentários de implementação explicam
algoritmos e invariantes. Os testes verificam metadados/FIFO, ACK antecipado e
concorrente, saturação, 65 trabalhos ACK, espera por vaga interrompida, p acima de
1024, controle de jobs, falhas de relógio e cleanup com múltiplos erros.

## Documentação e enunciado

- [Arquitetura](docs/architecture.md)
- [Protocolo e layouts](docs/protocol.md)
- [Diagnósticos e propriedade dos dados](docs/diagnostics.md)
- [Callbacks específicos de cleanup SHM](docs/cleanup_errors.md)
- [Estratégia e evidências de testes](docs/testing.md)
- [Rastreabilidade dos requisitos](docs/traceability.md)

O primeiro exemplo do enunciado contém `produz 3`, mas sua explicação menciona
memória 2. A implementação segue o argumento 3 e a descrição normativa. A numeração
3.2 repetida e ambiguidades sobre espera por ACK/espaço estão registradas na
rastreabilidade. Os exemplos são avaliados respeitando a concorrência e o momento
em que `shutdown` é emitido.
