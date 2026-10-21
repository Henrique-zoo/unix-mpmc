# Estratégia de testes {#testing_plan}

@tableofcontents

## Objetivo

Testar o comportamento observável exigido pelo enunciado separadamente em memória
compartilhada (modo 1) e troca de mensagens (modo 2). Um teste de encerramento bem-sucedido
não é suficiente para provar correção concorrente.

## Testes unitários

| ID | Componente | Casos |
|---|---|---|
| U01 | `command` | `produz`, `consome`, `shutdown` válidos |
| U02 | `command` | Comandos vazios, desconhecidos, tokens adicionais, caracteres não numéricos |
| U03 | `command` | `n_msgs` igual a 0, 1, 1024, 1025 |
| U04 | `command` | `ack` igual a 0, 1, 2, 3 |
| U05 | `command` | `produz 0` aceito; `consome 0` rejeitado |
| U06 | `command` | `i` maior que `p` rejeitado |
| U07 | `producer` | Mapeamento *striped* com `n` divisível/não divisível por `p` |
| U08 | `message` | Sequências crescentes por PID de produtor; timestamps preenchidos |
| U09 | `status` | Conversão de códigos de retorno a mensagens legíveis |

## Testes de integração (repetir para modos 1 e 2)

| ID | Cenário | Propriedade esperada |
|---|---|---|
| I01 | Inicialização e `shutdown` | Recursos criados/removidos e saída normal |
| I02 | Produzir antes de consumir | FIFO e preservação das mensagens |
| I03 | Consumir antes de produzir com `ack=1` | Consumidor espera e depois conclui |
| I04 | Consumir sem mensagens com `ack=2` | Erro e total consumido `0`, sem bloqueio |
| I05 | Consumo parcial com `ack=2` | Erro e total efetivamente consumido |
| I06 | Produtor com `ack=1` | Só conclui após consumo das próprias mensagens |
| I07 | Produtor com `ack=2` | Não espera confirmação de consumo |
| I08 | Vários consumidores na mesma estrutura | Mensagem não é consumida duas vezes |
| I09 | Vários produtores na mesma estrutura | Não há perda, duplicidade ou corrupção |
| I10 | `produz 0 n ...` | Distribuição 1..p cíclica, com contagem correta |
| I11 | Saturação do buffer/fila | Backpressure e retomada após consumo |
| I12 | `shutdown` com consumidor bloqueado | Filhos recolhidos e IPC removido |
| I13 | `shutdown` com produtor aguardando ACK | Filhos recolhidos e IPC removido |
| I14 | `shutdown` com produtor bloqueado por buffer cheio | Encerramento sem travamento |
| I15 | Comando para índice inexistente | Erro, nenhuma estrutura extra criada |
| I16 | Repetir executar/desligar | Nenhum recurso IPC residual |
| I17 | `n_msgs=1024` | Limite superior funciona sem corrupção |
| I18 | Paralelismo e variação de escalonamento | Sem deadlock; totais reproduzíveis |
| I19 | Estatísticas finais | Soma por estrutura equivale aos consumos válidos |
| I20 | Multiprodutores com ACK concorrente | ACKs não se cruzam entre trabalhos |
| I21 | Tempos de vida dos filhos e do `mpmc` | Relatório apresenta durações não negativas e coerentes |

## Propriedades a verificar

- `consumidas(i) <= produzidas(i)` sempre, para cada estrutura `i`.
- O número de mensagens retiradas globalmente equivale à soma dos relatórios dos consumidores.
- Para cada mensagem produzida, há no máximo um consumidor correspondente.
- Se um produtor conclui normalmente com `ack=1`, todas as mensagens daquele trabalho
  aparecem entre as efetivamente consumidas.
- Após `shutdown` completo, não há filhos zumbis do `mpmc` nem objetos System V
  pertencentes àquela execução.
- Uma operação bloqueada pode ser cancelada pelo encerramento.

## Ferramentas de diagnóstico

```bash
ipcs -m  # segmentos de memória compartilhada
ipcs -s  # conjuntos de semáforos
ipcs -q  # filas de mensagens

# Somente em ambiente controlado, para diagnosticar:
ps -o pid,ppid,stat,cmd -C mpmc
```

Compare o inventário **antes e depois** da execução. Não remova indiscriminadamente
recursos IPC de outros usuários/processos por `ipcrm`.

## Build e automação

```bash
make debug
make test
make docs
```

Os *smoke tests* do Makefile validam somente a capacidade de iniciar e executar `shutdown`.
Os testes de integração acima são casos a **implementar**. Para que scripts automatizados
dirijam o REPL, envie comandos por `stdin`, capture `stdout`/`stderr` e imponha limites
de tempo externos para detectar deadlocks de verdade.
