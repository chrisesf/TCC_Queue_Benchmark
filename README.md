# tcc-bench

## Visao geral

Este repositorio compara diferentes mecanismos de fila sob alta concorrencia.

| Mecanismo | Status | Binario |
|---|---|---|
| Fila com `Mutex` (lock-based) | Implementado | `mutex_bench` |
| Fila de Michael-Scott (lock-free) | Implementado | `michael_scott_bench` |
| Ring buffer (inspirado no LMAX Disruptor) | Implementado | `ring_buffer_bench` |
| RabbitMQ | A qualquer momento | |
| Apache Kafka | A qualquer momento | |

O ambiente de testes (`bench.rs`), a mensagem canonica, o contrato `Queue`, o modulo de
estatisticas e a CLI (`cli.rs`) sao compartilhados por todas as implementacoes.

## Como executar

As sondas de CPU e memoria leem `/proc` e `clock_gettime`, portanto rode no Linux
(WSL2 ou `docker compose up -d && docker compose exec bench bash`). Em outros sistemas
esses campos saem zerados.

1. Validar corretude:

```bash
cargo test --release
```

2. Rodar os benchmarks:

```bash
cargo run --release --bin mutex_bench         -- --producers 4 --consumers 4
cargo run --release --bin michael_scott_bench -- --producers 4 --consumers 4
cargo run --release --bin ring_buffer_bench   -- --producers 4 --consumers 4
```

3. Exportar para analise estatistica (uma linha por execucao medida):

```bash
cargo run --release --bin ring_buffer_bench -- --csv results/ring.csv
```

### Parametros da CLI

```text
--producers N        threads produtoras (padrao 4)
--consumers N        threads consumidoras (padrao 4)
--messages N         mensagens por produtor na fase medida (padrao 200000)
--warmup N           mensagens por produtor no warm-up de cada execucao
                     (padrao: messages/10, minimo 1000; 0 desativa)
--payload N          bytes: 64 | 1024 | 65536 (padrao 64)
--payload-kind K     heap (Vec<u8>) | inline ([u8; N])
                     (padrao: heap para mutex e michael-scott, inline para ring buffer)
--capacity N         capacidade da fila (padrao 8192; 0 = ilimitada)
--repetitions N      execucoes independentes medidas (padrao 30)
--arrival MODE       saturated | burst | paced (padrao saturated)
--rate N             msgs/s por produtor, com --arrival paced (padrao 100000)
--batch N            tamanho da rajada, com --arrival burst (padrao 1000)
--pause-ns N         pausa entre rajadas, com --arrival burst (padrao 100000)
--csv ARQUIVO        acrescenta os resultados de cada execucao ao CSV
```

> Importante: sempre use `--release`.

Restricoes por estrutura:

- **Ring buffer**: capacidade potencia de dois e >= 2; nao aceita `--capacity 0`. Todo o
  buffer e pre-alocado (`capacidade x tamanho da mensagem`): com `--payload 65536` e a
  capacidade padrao sao 512 MiB, entao prefira `--capacity 1024` nesse cenario.
- **Michael-Scott**: `--capacity 0` executa o algoritmo original (ilimitado). Com
  capacidade > 0 um contador atomico aplica backpressure (ver abaixo).

## Implementacoes

### Fila com `Mutex` (`queues/mutex_queue.rs`)

`VecDeque` protegido por `Mutex`, com duas `Condvar` (nao-vazia e nao-cheia). Threads
bloqueadas dormem no kernel. E a baseline.

### Fila de Michael-Scott (`queues/michael_scott.rs`)

- Lista encadeada com no sentinela; `enqueue` faz CAS no `next` do ultimo no e depois
  tenta avancar `tail`; `dequeue` faz CAS em `head`. Threads ajudam a avancar um `tail`
  atrasado, como no artigo.

### Ring buffer inspirado no Disruptor (`queues/ring_buffer.rs`)

- Buffer circular de tamanho fixo (potencia de dois, indice por mascara), alocado e com
  as paginas tocadas na construcao: nada e alocado no caminho quente.


## Estrutura do projeto

```text
src/
  lib.rs                       raiz da biblioteca
  message.rs                   Message (Vec<u8>), FixedMessage<N> ([u8; N]), trait BenchMessage
  queue.rs                     trait Queue<T> - contrato comum aos 5 mecanismos
  queues/
    mutex_queue.rs             fila lock-based (baseline)
    michael_scott.rs           fila lock-free de Michael-Scott
    ring_buffer.rs             ring buffer inspirado no LMAX Disruptor
  bench.rs                     ambiente de testes: produtores, consumidores, warm-up, metricas
  cli.rs                       parametros, relatorio e exportacao CSV
  stats.rs                     percentis, desvio padrao, IC 95% (t de Student)
  clock.rs                     relogio monotono e custo do proprio relogio
  resource.rs                  CPU (clock_gettime) e RSS via /proc
  bin/mutex_bench.rs           CLI da fila com Mutex
  bin/michael_scott_bench.rs   CLI da fila de Michael-Scott
  bin/ring_buffer_bench.rs     CLI do ring buffer
tests/
  queue_contract.rs            contrato Queue aplicado as 3 filas + testes especificos
  bench_metrics.rs             validacao das metricas do ambiente de testes
  message.rs                   formato binario, checksum e mensagens heap/inline
```
