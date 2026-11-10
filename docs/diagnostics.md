# Diagnósticos de operações e liberação {#flow_diagnostics}

Leia a [arquitetura](architecture.md) e o [protocolo](protocol.md) antes de
acompanhar a propagação dos erros. O [guia de leitura](reading_guide.md) reúne a ordem
dos documentos e os cabeçalhos relevantes.

O resultado `mpmc_result_t` continua sendo o contrato de controle de fluxo. Um
observador adicional recebe **cada falha individual**, inclusive as secundárias
que um único retorno não consegue representar. A aplicação instala esse observador
uma vez em `main()` com `mpmc_diagnostic_set_reporter()`.

## Contrato e hierarquia

`mpmc_reporter_t` contém uma função e `user_data`. A configuração é copiada para
armazenamento local à thread; o estado do chamador continua emprestado. A callback
é síncrona, não é executada em tratadores de sinais, deve retornar normalmente e
não deve reentrar no fluxo observado. Para remover o observador, passe `nullptr`.

O registro `mpmc_diagnostic_t` contém somente valores: função raiz, operação que
falhou, status/errno, PID, operação do trabalho, índice, recurso quando conhecido,
grupo da chamada e etapa (`OPERATION` ou `CLEANUP`). O ponteiro recebido vale apenas
durante a callback; copiar `*error` permite conservá-lo depois. O observador comum
preserva `errno` mesmo se a callback o modificar.

As funções iniciam e finalizam escopos explícitos. O escopo guarda a primeira
falha para evitar repetir um diagnóstico a cada propagação. O retorno normal
continua sendo o resultado escolhido pela função. Escopos de cleanup agregam a
primeira falha e continuam tentando as demais liberações. Criação com rollback
registra a causa antes de liberar recursos e retorna a causa original. O worker
combina execução e fechamento local: se a execução foi bem-sucedida, uma falha no
fechamento passa a ser seu resultado; caso contrário, conserva a causa inicial.

`WOULD_BLOCK` é controle de fluxo, sem callback. `EINTR` de uma primitiva pode ser
repetido pelo chamador: só é diagnosticado quando uma camada decide propagá-lo.
Checkpoints em cleanup também observam resultados de primitivas substitutas nos
testes, sem duplicar as notificações emitidas pelos wrappers reais.

Os reporters específicos de SHM e shutdown continuam disponíveis para observadores
que precisam de seus dados discriminados. Passar `nullptr` nesses parâmetros só
desabilita o reporter específico; o observador comum instalado permanece ativo.
Os recursos ACK também são cobertos pelo observador comum, sem índice lógico
artificial. Evite instalar dois observadores que imprimam a mesma falha.

## Filhos e apresentação

Depois de `fork()`, o filho desvincula a pilha de escopos herdada e instala um
emissor para seu pipe. Cada diagnóstico é um frame sem ponteiros, menor que o
`PIPE_BUF` mínimo POSIX. O filho escreve o frame e acorda o pai pelo self-pipe.
O pai drena continuamente os canais, valida PID/operação e apresenta os erros;
o resultado final é o último frame. Não existe um vetor limitado que descarte
erros depois de certa quantidade. Se o pipe encher, o pai continua drenando-o.

A apresentação é feita apenas pelo processo principal, com a forma:

```text
mpmc: falha em producer_run (operação 7, processo 1234, estrutura 2).
  Ocorreram os seguintes erros na operação:
    - semop() [recurso 42]: ...
  Ao finalizar/liberar recursos, ocorreram os seguintes erros:
    - shmdt(): ...
```

Vermelho é ativado apenas se `stderr` for terminal. `NO_COLOR` definido ou
`TERM=dumb` o desabilita; pipes e arquivos não recebem escapes ANSI. Grupos
concorrentes podem intercalar-se; cada troca reapresenta o contexto, sem prometer
ordem global entre processos. Consumo parcial continua mostrando quantidade real.

Falhas fatais após alteração do buffer, ou falhas de recursos IPC do kernel,
impedem continuar usando o contexto: o pai entra em shutdown, interrompe os demais
workers, drena diagnósticos e recolhe todos antes da remoção global. Um filho já
comunicando falhas recebe o prazo comum para terminar seu cleanup; ao expirar,
SIGKILL também o alcança. A morte forçada pode impedir a entrega de registros ainda
não escritos e, no modo MSG, de eventos locais ainda não publicados. O relatório
não representa durabilidade transacional contra morte arbitrária de processos.

## Evidência

- `test_diagnostic`: hierarquia, primeira causa, errno, replay e saída redirecionada.
- `test_shm_backend_ack_cleanup`: três falhas de liberação na mesma chamada,
  todas observadas, com preservação da primeira.
- `test_supervisor_shutdown`: várias falhas de sinais/restauração, prazo e coleta.
- `test_application.py`: resultados reais de workers e ausência de ANSI em pipe.

- `test_diagnostics.py`: falhas de criação, rollback com três remoções falhando,
  falha após alteração do ring, seis detaches falhando entre filho/pai e cores
  em pseudo-terminal; verifica ausência dos IDs registrados pelo próprio cenário.

## Cancelamento normal

Workers reais instalam tratadores mínimos de SIGINT/SIGTERM, sem SA_RESTART.
O tratador apenas solicita `ipc_worker_cancel()`: novas operações de dados/espera
retornam INTERRUPTED, enquanto publicações já iniciadas, ack_end e fechamento local
continuam disponíveis. Assim, uma retirada anterior ao sinal ainda publica seu
ACK/evento e entra no total consumido. `test_cooperative_shutdown.py` injeta SIGTERM
exatamente nessa janela nos dois modos e verifica o consumo efetivo de uma mensagem.
O stub DEBUG de testes mantém SIG_DFL para os cenários de morte por sinal.

A auditoria das transições também cobre envio aceito antes do cancelamento,
ACK e eventos adiados até o fechamento local, sinais repetidos nesse fechamento
e 1200 diagnósticos sintéticos entregues integralmente sob backpressure do pipe.
O controle negativo da fixture pode restaurar SIG_DFL antes da injeção; a suíte
rejeitou essa reprodução do comportamento antigo, confirmando a regressão.
