# Rastreabilidade do enunciado {#traceability}

A referência é o Trabalho Prático 02/2026, Sistemas Operacionais, UnB. Os nomes dos
módulos/testes e o protocolo de controle são escolhas do projeto. A tabela abaixo
aponta implementações e evidências existentes, não funcionalidades planejadas.

| Requisito | Implementação | Evidência |
|---|---|---|
| C e chamadas Unix | Fontes C23; fork/waitpid e System V | Build debug/release com -Werror |
| `mpmc p paradigma` | main.c; p positivo representável em size_t, modos 1/2 | test_args.sh; test_conformance.py (p=1025 chega ao backend) |
| Lançamento com e sem & | Job control do shell, sem rejeitar background | test_background.py: SIGTTIN, foreground/SIGCONT e shutdown |
| Modo 1: shm + sem | shm_backend/{lifecycle,operations}.c | test_shm_operations; test_transport_parity |
| Segmentos de 2048 bytes | message.h e shmget, inclusive controle ACK | test_shm_backend_ack_create; test_shm_operations; inspeção IPC_STAT |
| Modo 2: msg | msg_backend.c | test_msg_backend; test_msg_stress; test_transport_parity |
| PID, sequência e instante de envio | message.h; producer_message_next | test_message_metadata; test_producer; test_transport_parity |
| Prompt >> | repl.c com poll e self-pipe | test_repl.c e test_repl.sh |
| Um processo concorrente por produz/consome | supervisor_dispatch e run_worker | test_supervisor; test_application.py |
| Mensagens uma a uma, n=1..1024 | producer_run/consumer_run | test_command; test_producer; test_consumer; n=1024 no CLI |
| `produz 0` striped | producer_routing.c e produtor único | test_producer_routing; test_protocol_properties; test_application.py |
| Produtor ack=1 espera consumo integral | begin/send/wait/end da mesma operação | test_shm_backend_ack; test_msg_ack_concurrency; test_application.py |
| Novas operações ACK com tabela cheia | Semáforo de vagas, espera sem mutex | test_shm_ack_lifecycle; 65 produtores em test_conformance.py |
| Produtor ack=2 não aguarda consumo | Envio sem registro ACK | test_producer; test_application.py |
| Consumidor ack=1 aguarda n itens | ipc_receive bloqueante | test_consumer_integration; test_shutdown_resources.py |
| Consumidor ack=2 informa erro/quantidade parcial | WOULD_BLOCK + consumed_count | test_consumer_acceptance; test_application.py (parcial e zero) |
| REPL segue disponível com workers bloqueados | Poll/self-pipe e filhos independentes | test_repl; test_application.py; test_shutdown_resources.py |
| Shutdown termina e recolhe filhos | Cancelamento, prazo comum e SIGKILL de reserva | test_supervisor_shutdown; 24 cenários reais de test_shutdown_resources.py |
| Publicações devidas sobrevivem ao cancelamento normal | Worker cancelável e fechamento local | test_cooperative_shutdown.py |
| Durações de cada filho e do principal | CLOCK_MONOTONIC; erro marcado como indisponível | test_supervisor_report; test_conformance.py (EIO no relatório) |
| Totais consumidos por estrutura | Contador SHM/eventos MSG | test_transport_parity; test_application.py; test_cooperative_shutdown.py |
| Remoção dos mecanismos IPC próprios | Destroy/rollback após recolher filhos | test_shutdown_resources.py; test_diagnostics.py; recursos externos preservados |

## Requisitos e escolhas de implementação

O limite 1024 do enunciado aplica-se a n_msgs. Não há teto fixo para p além da
representação em C e dos recursos que o sistema consegue disponibilizar.
SHM possui p segmentos de dados e um controle ACK, todos com 2048 bytes. MSG
possui p filas de dados e duas auxiliares. Objetos auxiliares suportam confirmação
e métricas; não substituem as estruturas de dados por produtor lógico.

A tabela de ACK comporta 64 registros ativos. Tabela cheia **aguarda vaga**, sem
rejeitar o novo comando e sem crescer a memória do controle. Backpressure por
espaço e espera por confirmação têm causas diferentes, inclusive quando ack=2.

Com stdin no terminal, & segue job control: uma leitura em background pode
suspender por SIGTTIN; fg retoma o job. Entrada redirecionada pode ser consumida
em background. Não há apropriação automática do terminal pertencente ao shell.

## Ambiguidades e alcance

1. O primeiro exemplo contém produz 3, mas a narrativa menciona memória 2. O
   argumento do comando e a descrição da operação são a referência adotada.
2. Os exemplos confundem em alguns trechos espera por confirmação e por espaço.
   O significado de ack é o definido na descrição: 1 aguarda, 2 não aguarda ACK.
3. A numeração 3.2 repetida é editorial.
4. Contagens narradas pressupõem que os consumos ocorreram antes de shutdown.
   Um lote de comandos imediatamente seguido de shutdown pode cancelar trabalhos.
5. Remoção recusada pelo kernel e morte arbitrária com SIGKILL não equivalem a um
   encerramento normal: diagnósticos e limites de recuperação estão em protocol.md.

A revisão e execução completas foram feitas em Linux. Compatibilidade com macOS
orienta o uso de sinais/temporizadores, mas não substitui uma execução nativa lá.
