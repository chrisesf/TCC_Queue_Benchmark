use std::time::Duration;

use tcc_bench::brokers::kafka::{Kafka, KafkaConfig};
use tcc_bench::brokers::Semantics;
use tcc_bench::cli::{fail, run_transport_benchmark, Args, PayloadKind};

const EXTRA_USAGE: &str = "

Opcoes do Kafka:
  --semantics S        at-most-once | at-least-once | exactly-once (padrao at-least-once)
  --kafka-brokers B    bootstrap.servers (padrao kafka:9092)
  --linger-ms N        linger.ms do produtor (padrao 5, o do librdkafka)
  --tx-batch N         mensagens por transacao em exactly-once (padrao 1000)
  --idle-timeout-ms N  tempo sem mensagens para dar como perdidas as que faltam, no
                       warm-up e apos a producao (padrao 5000)
  --broker-container C container medido via API do Docker (padrao tcc-kafka; none desativa)

O topico tem uma particao por consumidor. A mensagem trafega serializada (id +
timestamp + payload), por isso o payload e sempre heap.";

const EXTRA_FLAGS: [&str; 6] = [
    "--semantics",
    "--kafka-brokers",
    "--linger-ms",
    "--tx-batch",
    "--idle-timeout-ms",
    "--broker-container",
];

fn main() {
    let mut args = Args::parse_with(PayloadKind::Heap, &EXTRA_FLAGS, EXTRA_USAGE);
    if args.payload_kind != PayloadKind::Heap {
        fail("brokers usam apenas --payload-kind heap");
    }
    // Capacidade nao se aplica: o limite e a retencao/disco do broker.
    args.capacity = None;

    let semantics = Semantics::parse(args.extra("--semantics").unwrap_or("at-least-once"))
        .unwrap_or_else(|| fail("--semantics invalido"));
    let cfg = KafkaConfig {
        brokers: args.extra("--kafka-brokers").unwrap_or("kafka:9092").to_owned(),
        semantics,
        linger_ms: args.extra_num("--linger-ms", 5) as u32,
        tx_batch: args.extra_num("--tx-batch", 1000),
        container: match args.extra("--broker-container").unwrap_or("tcc-kafka") {
            "none" => None,
            c => Some(c.to_owned()),
        },
    };
    let partitions = args.consumers;
    args.idle_timeout = Duration::from_millis(args.extra_num("--idle-timeout-ms", 5000));

    drop(Kafka::new(cfg.clone(), partitions).unwrap_or_else(|e| fail(&e)));

    let title = format!("Kafka ({semantics}, {partitions} particoes)");
    run_transport_benchmark::<_, tcc_bench::Message, _>(&title, &args, || {
        Kafka::new(cfg.clone(), partitions).unwrap_or_else(|e| panic!("{e}"))
    });
}
