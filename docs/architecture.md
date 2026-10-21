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

`message.h` e `status.h` são tipos transversais e não dependem de outros módulos da aplicação.

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
- `ipc_worker_close` desaloca o estado local do filho; **não** chama `IPC_RMID`.
- `ipc_destroy` remove recursos persistentes e é executado só pelo supervisor, após
  os filhos terem terminado/recolhidos.
- Se a inicialização falhar na metade, `ipc_init` deve reverter as estruturas já criadas.

O supervisor registra `pid`, `operation_id`, tipo de comando e timestamps monotônicos no
fork e na coleta do término. Processos que saíram espontaneamente devem ser recolhidos
sem esperar pelo shutdown. No modo de filas, a implementação também precisa
receber eventos de estatísticas **mesmo quando não há novos comandos**: o REPL não
pode permanecer indefinidamente em uma leitura bloqueante de `stdin` enquanto uma
fila de controle fica cheia. Uma estratégia possível usa notificação por sinal e
`pselect`/self-pipe; outra usa um coletor dedicado. O ponto essencial é continuar
drenando eventos sem depender de novas linhas do usuário. `SIGCHLD` pode ser tratado com um *flag* `volatile sig_atomic_t`
e `waitpid(..., WNOHANG)` fora do tratador; não chamar `printf`, `malloc` ou rotinas de
IPC não assíncronas dentro do tratador de sinal.

## Interface pública versus backends

Os workers usam somente `ipc/transport.h`; a fachada redireciona os métodos para o
backend selecionado. As funções `shm_backend_*` e `msg_backend_*` são internas à
implementação, mesmo quando seus protótipos são publicados em `include/` para
facilitar a documentação técnica.

Os wrappers `sysv_*` preservam os contratos das chamadas de sistema, verificam
resultados e traduzem falhas em `mpmc_status_t`. Não devem silenciar permanentemente
erros nem abstrair semânticas diferentes de `IPC_NOWAIT`, `EINTR` e `IPC_RMID`.

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
