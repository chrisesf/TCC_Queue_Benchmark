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

> Importante: sempre use `--release`.

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
