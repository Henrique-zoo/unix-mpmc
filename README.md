# unix-mpmc

> Sistema de múltiplos produtores e múltiplos consumidores em C, utilizando mecanismos de comunicação entre processos (IPC) System V no Unix/Linux.

**Disciplina:** Sistemas Operacionais — Universidade de Brasília (UnB), 2026/2.
**Trabalho:** Trabalho Prático 02 — Múltiplos Produtores / Múltiplos Consumidores.
**Linguagem:** C23. **Ambiente-alvo:** Linux/Unix com IPC System V e compilação via GNU Make.

> [!IMPORTANT]
> **Estado deste material:** este repositório de documentação contém o desenho da arquitetura,
> as *interfaces propostas* (`.h`) documentadas com Doxygen, os documentos de projeto e o Makefile.
> **As implementações em `src/*.c` ainda não foram escritas.** Os exemplos de execução abaixo
> descrevem a interface especificada, não funcionalidades já implementadas. A compilação de `mpmc`
> passará a funcionar quando as implementações correspondentes forem adicionadas.

## Sumário

1. [Objetivo e escopo](#objetivo-e-escopo)
2. [Requisitos do trabalho](#requisitos-do-trabalho)
3. [Compilação e execução](#compilação-e-execução)
4. [Comandos interativos](#comandos-interativos)
5. [Arquitetura](#arquitetura)
6. [Modelos de IPC e sincronização](#modelos-de-ipc-e-sincronização)
7. [Processos, mensagens e confirmações](#processos-mensagens-e-confirmações)
8. [Ciclo de vida e encerramento](#ciclo-de-vida-e-encerramento)
9. [Tratamento de erros e observabilidade](#tratamento-de-erros-e-observabilidade)
10. [Testes](#testes)
11. [Documentação Doxygen](#documentação-doxygen)
12. [Decisões e pontos em aberto](#decisões-e-pontos-em-aberto)
13. [Referências](#referências)

## Objetivo e escopo

Implementar uma aplicação que permita criar **processos produtores e consumidores concorrentes** e
executar as mesmas operações por dois paradigmas de IPC:

- **Modo 1 — memória compartilhada:** segmentos System V (`shm`) e semáforos System V (`sem`).
- **Modo 2 — troca de mensagens:** filas de mensagens System V (`msg`).

O programa principal, `mpmc`, recebe comandos interativamente e não executa diretamente
as operações demoradas: cada operação `produz` ou `consome` cria um processo filho por `fork()`.
O processo principal permanece responsável por receber comandos, acompanhar os filhos, manter
estatísticas e executar um encerramento ordenado.

O projeto não usa threads como substitutas dos processos nem filas ou semáforos POSIX como
substitutos das primitivas System V exigidas pelo enunciado.

## Requisitos do trabalho

| Aspecto | Requisito | Origem |
|---|---|---|
| Inicialização | `mpmc p paradigma` | Enunciado, §2 |
| Paradigmas | `1`: `shm` + `sem`; `2`: `msg` | Enunciado, §2 |
| Estruturas de dados | Uma estrutura IPC por produtor lógico `1..p` | Enunciado, exemplos |
| Segmentos compartilhados | **2048 bytes por segmento**, no modo 1 | Enunciado, §2 |
| Mensagem | PID emissor, sequência por processo emissor e instante de envio | Enunciado, §2 |
| Produção | `produz p n_msgs ack` | Enunciado, §2 |
| Consumo | `consome p n_msgs ack` | Enunciado, §2 |
| Quantidade | `n_msgs` entre **1 e 1024**, inclusive | Enunciado, §2 |
| Distribuição | `produz 0 ...`: distribuição *striped* pelos índices `1..p` | Enunciado, §2 |
| Encerramento | `shutdown`: termina processos, reporta tempos/contagens, remove recursos | Enunciado, §2 |

O `&` final exibido nos exemplos do enunciado é opcional. No REPL do `mpmc`, ele é tratado
como sufixo sintático permitido: os trabalhos **sempre** rodam em processos filhos concorrentes.
O `&` informado ao iniciar o programa (`./mpmc ... &`) é um operador do *shell*, não um
argumento do `mpmc`. Um REPL em segundo plano pode ser suspenso pelo controle de tarefas do
terminal ao tentar ler de `stdin`; para uso interativo, inicie o programa em primeiro plano.

## Compilação e execução

**Dependências:** compilador C compatível com C23 (`gcc 15+` ou `clang 18+`), `make`, Linux com
IPC System V disponível. O Doxygen é opcional para compilar a aplicação.

```bash
make                  # build/debug/mpmc e link ./mpmc
make debug            # compilação para depuração
make release          # build/release/mpmc, otimizado
make test             # smoke tests (após implementar os módulos)
make clean            # remove binários e objetos
```

A compilação utiliza `-std=c23`, avisos rigorosos e arquivos de dependências `.d` gerados
automaticamente. Fontes novas em `src/` são descobertas sem edição manual do Makefile.

Exemplos de inicialização previstos:

```bash
./mpmc 4 1    # 4 estruturas IPC, usando memória compartilhada + semáforos
./mpmc 2 2    # 2 estruturas IPC, usando filas de mensagens
```

Os índices de produtores usados nos comandos vão de `1` até o valor `p` da inicialização.
O índice `0` é um caso especial **somente para `produz`**.

## Comandos interativos

Depois de inicializado, o programa exibe `>>` e recebe um comando por linha.

| Comando | Efeito esperado |
|---|---|
| `produz i n 1` | Produz `n` mensagens na estrutura `i` e só conclui quando todas forem consumidas. |
| `produz i n 2` | Produz `n` mensagens em `i`, sem aguardar confirmações de consumo. |
| `produz 0 n a` | Distribui `n` mensagens ciclicamente entre estruturas `1..p`. |
| `consome i n 1` | Consome `n` mensagens de `i`, aguardando quando faltarem mensagens. |
| `consome i n 2` | Consome imediatamente até `n` mensagens disponíveis; quando não conseguir `n`, informa erro e quantidade efetivamente consumida. |
| `shutdown` | Encerra filhos, apresenta métricas e remove os recursos IPC. |

Aqui, `i` é um índice lógico e `a` vale `1` ou `2`. Um comando como
`produz 0 8 2` em `mpmc 3 ...` encaminha mensagens na sequência
`1, 2, 3, 1, 2, 3, 1, 2`.

O significado de `ack` **depende do comando**:

- Em `produz`, `ack=1` solicita confirmação de que **todas as mensagens daquele trabalho**
  foram consumidas. `ack=2` dispensa a espera pelas confirmações.
- Em `consome`, `ack=1` significa **consumo bloqueante** até completar a quantidade solicitada.
  `ack=2` significa **consumo não bloqueante**, retornando erro se a quantidade disponível for
  insuficiente, juntamente com a contagem consumida.

### Exemplos de sessão (ilustrativos)

**Memória compartilhada:**

```text
$ ./mpmc 4 1
>> produz 3 2 1
>> consome 3 2 1
>> shutdown
```

**Filas de mensagens:**

```text
$ ./mpmc 2 2
>> consome 2 2 1
>> consome 2 2 1
>> produz 2 5 2
>> shutdown
```

A aplicação deve continuar aceitando comandos enquanto os trabalhos concorrentes executam.
A ordem real de execução, a ordem de término e a interposição das mensagens no terminal são
não determinísticas.

## Arquitetura

### Organização dos diretórios

```text
unix-mpmc/
├── Makefile                 # Compilação, testes e geração de documentação
├── Doxyfile                 # Configuração do Doxygen
├── README.md                # Visão geral e instruções de uso
├── .gitignore
├── include/mpmc/
│   ├── status.h             # Códigos de retorno e tratamento de erros
│   ├── message.h            # Estrutura lógica das mensagens
│   ├── app/
│   │   ├── command.h        # Parsing e validação de comandos
│   │   ├── repl.h           # Laço interativo
│   │   └── supervisor.h     # Criação, acompanhamento e término de filhos
│   ├── workers/
│   │   ├── producer.h       # Execução de produtores
│   │   └── consumer.h       # Execução de consumidores
│   └── ipc/
│       ├── transport.h      # Fachada comum aos dois paradigmas
│       ├── shm_backend.h    # Transporte por memória compartilhada
│       ├── msg_backend.h    # Transporte por filas de mensagens
│       ├── shm.h            # Wrappers de shmget/shmat/shmdt/shmctl
│       ├── sem.h            # Wrappers de semget/semop/semctl
│       └── msg.h            # Wrappers de msgget/msgsnd/msgrcv/msgctl
├── src/
│   ├── main.c
│   ├── status.c
│   ├── app/                  # Implementações de command/repl/supervisor
│   ├── workers/              # Implementações de producer/consumer
│   └── ipc/                  # Implementações de transporte e wrappers
├── docs/
│   ├── architecture.md      # Fronteiras e dependências
│   ├── protocol.md          # Contratos concorrentes e confirmações
│   └── testing.md           # Matriz de testes e propriedades esperadas
├── tests/
│   ├── unit/
│   └── integration/
└── build/                   # Gerado; não versionado
```

Os arquivos `.c` listados acima são **alvos de implementação**, não arquivos entregues
neste pacote de documentação.

### Caminho de execução

```text
                    main.c
                       |
             app/repl + command
                       |
                app/supervisor
                  |        |
                 fork()   fork()
                  |        |
           workers/     workers/
           producer     consumer
                  \       /
                 ipc/transport
                  /       \
        ipc/shm_backend  ipc/msg_backend
          |         |           |
       ipc/shm  ipc/sem      ipc/msg
             \     |          /
             Kernel System V IPC
```

**`app/`** cuida do ciclo de vida e do terminal. **`workers/`** modela os comportamentos
de produção e consumo. **`ipc/`** implementa as diferenças de armazenamento, sincronização,
contagem e confirmação. Os workers dependem **somente** da fachada `ipc/transport.h`,
sem chamar `shmat()`, `semop()`, `msgsnd()` ou `msgrcv()` diretamente.

### Contratos principais

- `command_parse()` transforma a linha recebida em um comando validado em duas etapas
  (sintaxe e valores dependentes da quantidade de produtores).
- `supervisor_dispatch()` cria **um filho por comando `produz`/`consome`** com `fork()`;
  o filho executa `producer_run()` ou `consumer_run()` e encerra.
- `ipc_init()` cria as estruturas do paradigma selecionado antes da criação dos filhos;
  os identificadores IPC são herdados pelos filhos via `fork()`.
- `ipc_send()` e `ipc_receive()` encapsulam operações de cada paradigma.
- `ipc_ack_begin()` / `ipc_ack_wait()` / `ipc_ack_end()` oferecem confirmação de
  **consumo efetivo** para o produtor `ack=1`.
- `ipc_poll_events()` consolida eventos/estatísticas recebidos por mensagens (modo 2),
  enquanto `ipc_consumed_count()` disponibiliza os totais para o supervisor.
- `ipc_worker_close()` libera apenas vínculos **locais ao filho**. `ipc_destroy()` remove
  os recursos globais e só pode ser chamado pelo processo supervisor.

Descrições mais detalhadas em [docs/architecture.md](docs/architecture.md) e
[docs/protocol.md](docs/protocol.md). As assinaturas completas e os contratos individuais
estão nos cabeçalhos da pasta `include/mpmc/`.

## Modelos de IPC e sincronização

### Paradigma 1: memória compartilhada + semáforos

Para cada índice `i` (`1..p`), o supervisor cria um segmento `shm` de **2048 bytes** e
os semáforos necessários ao protocolo. Um buffer circular armazena mensagens e metadados
compartilhados. O número efetivo de posições não deve ser fixado arbitrariamente:

```text
capacity = floor((2048 - tamanho_do_cabeçalho_alinhado) / tamanho_da_entrada_alinhada)
```

Os valores exatos dependem do layout em C e de seu alinhamento (`sizeof`, `alignof`).
O layout deve ser dimensionado/verificado antes de implementar o algoritmo.

Semáforos típicos por estrutura:

- `mutex = 1`: exclusão mútua na atualização dos índices e contadores do buffer;
- `empty = capacity`: quantidade de vagas disponíveis;
- `full = 0`: quantidade de entradas disponíveis.

Fluxo simplificado de produção: `P(empty) → P(mutex) → inserir → V(mutex) → V(full)`.
Fluxo simplificado de consumo: `P(full) → P(mutex) → retirar → atualizar contagem
→ V(mutex) → V(empty)`; a confirmação ao produtor é emitida conforme o protocolo do backend.
**Nenhuma operação bloqueante deve manter `mutex` adquirido.** Tratamento de erros deve
preservar os invariantes ao cancelar operações parcialmente iniciadas.

### Paradigma 2: filas de mensagens

Para cada índice `i`, o supervisor cria uma fila System V. Os workers utilizam
`msgsnd()` e `msgrcv()` encapsulados pelo backend. O recebimento não bloqueante usa
`IPC_NOWAIT`, e o bloqueante aguarda mensagens do índice solicitado. O envelope de uma
mensagem System V contém `long mtype`, mas o tamanho passado a `msgsnd()` e `msgrcv()`
**exclui esse campo**.

Confirmações e estatísticas precisam de canais de controle próprios ou de um protocolo
sobre as filas. **Não** se deve assumir que a memória privada de `fork()` seja compartilhada:
contadores atualizados por um filho não aparecem automaticamente no heap do supervisor.
O desenho previsto usa eventos de controle por filas de mensagens e consolidação
pelo supervisor. Consulte [docs/protocol.md](docs/protocol.md).

## Processos, mensagens e confirmações

Cada chamada a `produz` ou `consome` cria um processo filho distinto. O identificador do
produtor informado na linha é o **índice lógico da estrutura**, não o PID do filho. Em
`produz 0`, um **único filho produtor** envia mensagens alternadamente aos índices `1..p`.

Uma mensagem guarda:

- `sender_pid`: PID real do processo produtor (`getpid()`);
- `sequence`: número sequencial **do processo emissor** (1, 2, ...);
- `sent_at`: instante de envio (`CLOCK_REALTIME`), incluindo nanossegundos;
- `operation_id`: identificador único atribuído pelo supervisor, para correlacionar
  confirmações e facilitar observabilidade;
- `ack_required`: indica solicitação de confirmação de consumo (`produz ... 1`).

Uma operação de consumo bem-sucedida remove cada mensagem **exatamente uma vez**. O
protocolo de confirmação sinaliza que a mensagem **foi consumida**, e não apenas enviada ou
inserida. Aguardas e bloqueios devem ser interrompíveis pelo encerramento do sistema.

O desenho das confirmações e o tratamento de operações *striped* estão em
[docs/protocol.md](docs/protocol.md).

## Ciclo de vida e encerramento

1. Validar argumentos de `mpmc` (`p > 0`, paradigma `1` ou `2`).
2. Inicializar o backend e todas as estruturas IPC; desfazer as já criadas se houver erro.
3. Iniciar o REPL e criar filhos conforme os comandos recebidos.
4. Registrar PIDs, operações e horários de criação/término; recolher filhos com `waitpid()`
   para evitar zumbis (inclusive durante o REPL, não somente no `shutdown`).
5. Ao receber `shutdown`, recusar novos trabalhos e solicitar o término dos filhos vivos;
   tratar bloqueados em `semop()`, `msgsnd()`, `msgrcv()` ou na espera por confirmações.
6. Recolher todos os filhos, consolidar os eventos e imprimir durações e contagens.
7. Remover filas, segmentos e semáforos pertencentes à aplicação (`IPC_RMID`) **uma vez**;
   liberar as estruturas locais e encerrar o `mpmc`.

Os tempos de duração devem utilizar relógio monotônico (`CLOCK_MONOTONIC`), ao passo que
`sent_at` deve usar um relógio de calendário (`CLOCK_REALTIME`).

## Tratamento de erros e observabilidade

O projeto prevê erros distintos para: argumentos inválidos, índice lógico inexistente,
falta de mensagens em modo não bloqueante, interrupção por sinal, falha de chamada de sistema
e encerramento em andamento. O `errno` originado do kernel pode ser preservado pelo wrapper
para mensagens de diagnóstico (`perror`/`strerror`).

A saída de status deve incluir, no mínimo, os requisitos do enunciado: duração de cada
processo, total consumido por estrutura e tempo total de operação do `mpmc`. A formatação
textual exata dessas informações é uma decisão de implementação.

## Testes

As suítes devem abranger parsing, limites (`1..1024`), índices inválidos, distribuição
*striped*, FIFO por estrutura, competição entre consumidores, `ack=1` e `ack=2` para ambos
os comandos, bloqueios, `shutdown` com filhos pendentes e vazamento de recursos IPC.
Veja [docs/testing.md](docs/testing.md) para a matriz completa.

Depois da implementação, execute:

```bash
make test
```

O alvo atual `test` do Makefile contém apenas *smoke tests* (entrada `shutdown` nos dois
paradigmas); **não constitui comprovação de correção concorrente**.

## Documentação Doxygen

Todos os módulos possuem grupos Doxygen (`@defgroup` / `@ingroup`), contratos de funções,
pré-condições, códigos de retorno, aspectos de sincronização e responsabilidade pelo ciclo
de vida dos recursos.

```bash
# Ubuntu/Debian (para geração local da documentação)
sudo apt install doxygen

make docs                 # gera HTML em build/docs/html/
# ou:
doxygen Doxyfile
```

Abra `build/docs/html/index.html` no navegador. A página principal é este README;
os documentos de arquitetura, protocolo e testes aparecem nas páginas relacionadas.

Os arquivos `.h` especificam **interfaces propostas**, que podem evoluir durante a
implementação. Ao alterar um contrato, atualize a documentação e os testes associados.

## Decisões e pontos em aberto

O enunciado determina comportamento observável, mas não detalha tudo sobre implementação.
As seguintes escolhas são **propostas de projeto**, não exigências adicionais do professor:

1. `fork()` por comando, sem executáveis separados `produz`/`consome`.
2. Backend selecionado na inicialização e exposto por uma interface única.
3. Uso de buffers circulares no paradigma de memória compartilhada.
4. `&` final no comando interno aceito sem alterar a concorrência obrigatória.
5. Identificador de operação extra nas mensagens, para correlação dos ACKs.
6. Contadores no modo de fila atualizados por eventos IPC, não por memória privada de filhos.
7. Estatísticas de processo calculadas pelo supervisor usando os eventos de `fork()`/`waitpid()`.

**Ambiguidades/erros aparentes do documento:** o primeiro exemplo usa `produz 3 2 1`,
mas a explicação menciona a memória 2; aqui seguimos a **sintaxe do comando**, que indica
estrutura 3. A descrição de alguns exemplos de bloqueio também é imprecisa; a regra
normativa adotada é a explicação dos parâmetros `ack` na seção 2 do enunciado.

## Referências

- Enunciado fornecido: *Descrição do Trabalho Prático (02/2026) — Múltiplos Produtores /
  Múltiplos Consumidores*, UnB, disciplina Sistemas Operacionais.
- Linux man-pages: `shmget(2)`, `shmat(2)`, `semget(2)`, `semop(2)`, `msgget(2)`,
  `msgsnd(2)`, `msgrcv(2)`, `waitpid(2)`, `fork(2)`.
- Doxygen: https://www.doxygen.nl/manual/

---

**Nota:** a documentação constitui uma especificação de engenharia para orientar o
trabalho e não deve ser interpretada como garantia de que a implementação já atende aos
requisitos de concorrência ou foi testada.
