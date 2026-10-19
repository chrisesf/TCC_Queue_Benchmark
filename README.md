# tcc-bench

## Visao geral

Este repositorio compara diferentes mecanismos de fila sob alta concorrencia.

| Mecanismo | Status | Binario |
|---|---|---|
| Fila com `Mutex` (lock-based) | Implementado | `mutex_bench` |
| Fila de Michael-Scott (lock-free) | Implementado | `michael_scott_bench` |
| Ring buffer (inspirado no LMAX Disruptor) | Implementado | `ring_buffer_bench` |
| RabbitMQ 4.3 (`lapin`) | Implementado | `rabbitmq_bench` (feature `rabbitmq`) |
| Apache Kafka 4.3 (`rdkafka`) | Implementado | `kafka_bench` (feature `kafka`) |

O ambiente de testes (`bench.rs`), a mensagem canonica, o modulo de estatisticas e a CLI
(`cli.rs`) sao compartilhados por todos os mecanismos. Filas em memoria e brokers passam
pela mesma abstracao (`transport.rs`), entao as metricas sao calculadas pelo mesmo codigo.

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

### Brokers

Os brokers rodam em containers na mesma maquina e so sobem com o perfil `brokers`, para
nao disputar CPU com os benchmarks em memoria. O cliente roda no container `bench`.

```bash
docker compose --profile brokers up -d          # tcc-rabbitmq e tcc-kafka
docker compose run --rm bench cargo test --release --features brokers
docker compose run --rm bench cargo run --release --features brokers --bin rabbitmq_bench -- --semantics at-least-once
docker compose run --rm bench cargo run --release --features brokers --bin kafka_bench -- --semantics exactly-once
docker compose --profile brokers down           # antes de medir as filas em memoria
```

O container `bench` monta `/var/run/docker.sock` para ler CPU e memoria dos containers
dos brokers. Isso da a ele acesso equivalente a root no host: use so em maquina de testes.

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

Opcoes adicionais dos brokers:

```text
--semantics S        at-most-once | at-least-once | exactly-once (so Kafka) (padrao at-least-once)
--idle-timeout-ms N  tempo sem mensagens para dar como perdidas as que faltam (padrao 5000)
--broker-container C container medido via API do Docker (padrao tcc-rabbitmq / tcc-kafka)
--amqp-url URL       RabbitMQ (padrao amqp://rabbitmq:5672/%2f)
--prefetch N         RabbitMQ: basic.qos por consumidor (padrao 1000)
--confirm-window N   RabbitMQ: publicacoes pendentes por produtor (padrao 1000)
--kafka-brokers B    Kafka: bootstrap.servers (padrao kafka:9092)
--linger-ms N        Kafka: linger.ms do produtor (padrao 5)
--tx-batch N         Kafka exactly-once: mensagens por transacao (padrao 1000)
```

Nos brokers o payload e sempre `heap` (a mensagem trafega serializada) e `--capacity` nao
se aplica.

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

### Brokers (`brokers/`)

Cada repeticao cria uma fila/topico novo e o remove ao final; cada produtor e consumidor
tem conexao propria, aberta antes da medicao. A mensagem usa a serializacao binaria fixa
de `Message::encode` (id + timestamp + payload). O tempo medido termina na ultima mensagem
medida consumida; se faltar alguma, os consumidores param apos `--idle-timeout-ms` sem
mensagens e a diferenca vira perda.

| Semantica | RabbitMQ | Kafka |
|---|---|---|
| at-most-once | mensagens transientes, sem publisher confirms, auto-ack | `acks=0`; offset armazenado ao receber |
| at-least-once | mensagens persistentes, publisher confirms, ack manual apos processar | `acks=all`; offset armazenado apos processar |
| exactly-once | nao suportado pelo AMQP 0-9-1 (o binario recusa) | produtor idempotente e transacional (commit a cada `--tx-batch`), consumidor `read_committed` |

- **RabbitMQ**: fila classica duravel na exchange padrao, consumidores concorrentes. O
  `lapin` so conclui uma publicacao ou ack depois que sua thread de I/O escreve o frame;
  por isso as operacoes ficam em pipeline (janela `--confirm-window`/`--prefetch`) em vez
  de aguardar uma a uma, o que dobrou a vazao e cortou 4x o CPU do cliente.
- **Kafka**: uma particao por consumidor, atribuida manualmente (sem rebalanceamento); o
  produtor `p` envia a sequencia `s` para a particao `(s + p) % particoes`. A ordem e
  verificada por particao.
- CPU e memoria do broker vem da API do Docker; os nucleos ocupados usam o intervalo real
  entre as amostras (relogio do daemon), que em execucoes curtas e maior que a fase medida.

Observado nos testes: com `acks=0`, logo apos criar o topico o broker responde
`NotLeaderOrFollower` e fecha a conexao, descartando sem aviso as requisicoes em voo
(~2% do warm-up). E a perda tipica de at-most-once; o warm-up a absorve. Em exactly-once a
vazao e a latencia dependem quase so de `--tx-batch` (cada commit custa ~100 ms aqui).


## Estrutura do projeto

```text
src/
  lib.rs                       raiz da biblioteca
  message.rs                   Message (Vec<u8>), FixedMessage<N> ([u8; N]), trait BenchMessage
  queue.rs                     trait Queue<T> - contrato das 3 filas em memoria
  queues/
    mutex_queue.rs             fila lock-based (baseline)
    michael_scott.rs           fila lock-free de Michael-Scott
    ring_buffer.rs             ring buffer inspirado no LMAX Disruptor
  transport.rs                 Transport/Producer/Consumer: filas em memoria e brokers
  brokers/
    mod.rs                     semanticas de entrega
    rabbitmq.rs                RabbitMQ via lapin
    kafka.rs                   Kafka via rdkafka
  bench.rs                     ambiente de testes: produtores, consumidores, warm-up, metricas
  cli.rs                       parametros, relatorio e exportacao CSV
  stats.rs                     percentis, desvio padrao, IC 95% (t de Student)
  clock.rs                     relogio monotono e custo do proprio relogio
  resource.rs                  CPU (clock_gettime) e RSS via /proc; CPU/memoria do broker
  bin/mutex_bench.rs           CLI da fila com Mutex
  bin/michael_scott_bench.rs   CLI da fila de Michael-Scott
  bin/ring_buffer_bench.rs     CLI do ring buffer
  bin/rabbitmq_bench.rs        CLI do RabbitMQ
  bin/kafka_bench.rs           CLI do Kafka
tests/
  queue_contract.rs            contrato Queue aplicado as 3 filas + testes especificos
  bench_metrics.rs             validacao das metricas (inclui broker simulado com perda/nack)
  message.rs                   formato binario, checksum e mensagens heap/inline
  brokers.rs                   integracao com RabbitMQ e Kafka reais (feature brokers)
```
