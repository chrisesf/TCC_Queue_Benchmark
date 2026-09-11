# tcc-bench

## Visao geral

Este repositorio compara diferentes mecanismos de fila sob alta concorrencia.

| Mecanismo | Status |
|---|---|
| Fila com `Mutex` (lock-based) | Implementado |
| Fila de Michael-Scott (lock-free) | A qualquer momento |
| Ring buffer (LMAX Disruptor) | A qualquer momento |
| RabbitMQ | A qualquer momento |
| Apache Kafka | A qualquer momento |

O ambiente de testes de medicao (`bench.rs`), a mensagem canonica, o contrato `Queue` e o
modulo de estatisticas ja estao prontos e serao reaproveitados nas demais
implementacoes.

## Como executar

1. Validar corretude:

```bash
cargo test --release
```

2. Rodar benchmark base (`Mutex`):

```bash
cargo run --release --bin mutex_bench -- --producers 4 --consumers 4
```

### Parametros da CLI

```text
--producers N     threads produtoras (padrao 4)
--consumers N     threads consumidoras (padrao 4)
--messages N      mensagens por produtor na fase medida (padrao 200000)
--payload N       bytes: 64 | 1024 | 65536 (padrao 64)
--capacity N      capacidade da fila (padrao 8192; 0 = ilimitada)
--repetitions N   execucoes independentes medidas (padrao 10)
--arrival MODE    saturated | burst | paced (padrao saturated)
--rate N          msgs/s por produtor, com --arrival paced
--batch N         tamanho da rajada, com --arrival burst
--pause-ns N      pausa entre rajadas, com --arrival burst
```

> Importante: sempre use `--release`. Em modo debug, a ausencia de otimizacoes
> distorce fortemente os resultados.

## Estrutura do projeto

```text
src/
  lib.rs              raiz da biblioteca
  message.rs          mensagem canonica, formato de fio, gerador de payload
  queue.rs            trait Queue<T> - contrato comum aos 5 mecanismos
  queues/
    mutex_queue.rs    fila lock-based (baseline)
  bench.rs            ambiente de testes: produtores, consumidores, coleta de metricas
  stats.rs            percentis, desvio padrao, IC 95%
  clock.rs            fonte de tempo e custo do proprio relogio
  resource.rs         CPU e pico de RSS via /proc
  bin/mutex_bench.rs  CLI
tests/
  mutex_queue.rs      8 testes de corretude
```

## Decisoes de projeto

### 1) Fila limitada por padrao

O ring buffer e limitado por construcao, e os brokers aplicam backpressure.
Uma fila ilimitada mediria outra semantica: o produtor nao bloqueia, a fila
cresce em memoria e a vazao pode ficar artificialmente inflada. A opcao
`--capacity 0` existe apenas como cenario de controle.

### 2) Duas variaveis de condicao

Produtores esperam em `not_full` e consumidores em `not_empty`. Com uma unica
condvar, notificacoes de espaco livre acordariam tambem consumidores, gerando
reavaliacoes desnecessarias do predicado (thundering herd).

### 3) `id` empacota produtor + sequencia

Formato: `(producer_id << 48) | seq`, mantendo 8 bytes por identificador.
Uma sequencia global unica exigiria um contador compartilhado, introduzindo um
ponto de contencao artificial.

### 4) Corretude embutida no benchmark

Cada consumidor valida que as sequencias por produtor sao estritamente
crescentes. Em conjunto com `recebidas == enviadas`, isso garante ausencia de
perda e duplicacao para fila FIFO linearizavel. Para brokers com semantica
*at-least-once*, sera necessario adicionar bitmap de IDs vistos.

### 5) Timestamp depende do padrao de chegada

Em `paced`, a mensagem e carimbada com o instante previsto de envio, nao o
instante real. Isso evita omissao coordenada quando o produtor atrasa por
saturacao da fila.

### 6) Custo do relogio e reportado

`now_ns()` custa cerca de 20-45 ns. Em cenarios muito rapidos (ex.: SPSC com
ring buffer), a latencia medida pode ficar na mesma ordem do custo de
instrumentacao; por isso esse numero precisa ser exposto.