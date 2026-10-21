# Rastreabilidade do enunciado {#traceability}

@tableofcontents

## Premissas de leitura

Este quadro utiliza o enunciado *Trabalho Prático 02 — Múltiplos Produtores /
Múltiplos Consumidores*, disciplina Sistemas Operacionais, UnB, 2026/2. Todos os
itens listados abaixo são **requisitos ou comportamentos descritos pelo enunciado**;
a arquitetura proposta e os nomes de teste são escolhas do projeto.

## Matriz de cobertura

| Requisito do enunciado | Módulos responsáveis | Testes planejados |
|---|---|---|
| `mpmc p paradigma` | `main`, `supervisor`, `transport` | I01 |
| Paradigma 1: `shm` + `sem` | `shm_backend`, `shm`, `sem` | I01, I02, I08, I11 |
| 2048 bytes por segmento compartilhado | `shm_backend`, `shm`, `message` | I01, I11, I17 |
| Paradigma 2: `msg` | `msg_backend`, `msg` | I01, I02, I08, I11 |
| Mensagem com PID, sequência e tempo de envio | `message`, `producer` | U08, I02 |
| Prompt e leitura interativa `>>` | `repl`, `command` | U01, I01 |
| `produz i n ack` | `command`, `supervisor`, `producer` | U01, I02, I06, I07 |
| `n_msgs` de 1 a 1024 | `command` | U03, I17 |
| `produz 0`: distribuição *striped* | `producer`, `transport` | U07, I10 |
| Produtor `ack=1`: espera consumo total | `producer`, `transport`, `shm_backend`, `msg_backend` | I06, I13, I20 |
| Produtor `ack=2`: não espera consumo | `producer`, `transport` | I07 |
| `consome i n ack` | `command`, `supervisor`, `consumer` | U01, I03, I04 |
| Consumidor `ack=1`: aguarda quantidade | `consumer`, `transport` | I03, I12 |
| Consumidor `ack=2`: erro e quantidade consumida | `consumer`, `transport` | I04, I05 |
| Processos filhos concorrem com o REPL | `supervisor`, `repl` | I03, I08, I09 |
| `shutdown` encerra produtores/consumidores | `supervisor`, `transport` | I12, I13, I14 |
| Apresenta duração de cada processo | `supervisor` | I21 |
| Apresenta total consumido por estrutura | `transport`, `shm_backend`, `msg_backend`, `supervisor` | I10, I19 |
| Apresenta duração total do `mpmc` | `supervisor` | I21 |
| Remove todos os objetos de IPC | `transport`, `shm_backend`, `msg_backend`, wrappers `sysv_*` | I01, I12, I16 |

## Divergências textuais identificadas

1. O primeiro exemplo apresenta `produz 3 2 1`, mas a explicação associa as
   mensagens à memória 2; para a implementação, o **argumento do comando** é a
   referência normativa, isto é, índice 3.
2. Alguns exemplos narrativos parecem confundir espera do produtor por confirmação
   com espera por espaço na fila. O significado adotado para `ack` é aquele definido
   expressamente na seção de descrição do enunciado.
3. A numeração `3.2` aparece duas vezes; isso não muda as exigências de projeto.

Os casos devem ser conferidos com o professor caso surja divergência na correção.
