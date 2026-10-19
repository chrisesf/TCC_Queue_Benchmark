//! Testes de integracao com os brokers reais. Exigem RabbitMQ e Kafka no ar:
//!
//!   docker compose --profile brokers up -d
//!   docker compose run --rm -T bench cargo test --release --features brokers --test brokers
//!
//! Enderecos: `TCC_AMQP_URL` (padrao amqp://rabbitmq:5672/%2f) e `TCC_KAFKA_BROKERS`
//! (padrao kafka:9092).

use std::time::Duration;

use tcc_bench::bench::{run, Arrival, RunConfig, RunOutcome};
use tcc_bench::brokers::kafka::{Kafka, KafkaConfig};
use tcc_bench::brokers::rabbitmq::{RabbitConfig, RabbitMq};
use tcc_bench::brokers::Semantics;

fn amqp_url() -> String {
    std::env::var("TCC_AMQP_URL").unwrap_or_else(|_| "amqp://rabbitmq:5672/%2f".into())
}

fn kafka_brokers() -> String {
    std::env::var("TCC_KAFKA_BROKERS").unwrap_or_else(|_| "kafka:9092".into())
}

fn docker_disponivel() -> bool {
    std::path::Path::new("/var/run/docker.sock").exists()
}

fn cfg(producers: usize, consumers: usize, payload_bytes: usize, messages: u64) -> RunConfig {
    RunConfig {
        producers,
        consumers,
        messages_per_producer: messages,
        warmup_per_producer: messages / 10,
        payload_bytes,
        arrival: Arrival::Saturated,
        idle_timeout: Duration::from_secs(10),
    }
}

fn rabbit(semantics: Semantics) -> RabbitConfig {
    RabbitConfig {
        url: amqp_url(),
        semantics,
        container: Some("tcc-rabbitmq".into()),
        ..Default::default()
    }
}

fn kafka(semantics: Semantics) -> KafkaConfig {
    KafkaConfig {
        brokers: kafka_brokers(),
        semantics,
        tx_batch: 200,
        container: Some("tcc-kafka".into()),
        ..Default::default()
    }
}

/// Sem falhas injetadas, todas as semanticas devem entregar tudo, uma vez, em ordem.
fn assert_entrega_completa(o: &RunOutcome, cfg: &RunConfig, contexto: &str) {
    let total = cfg.total_messages();
    assert_eq!(o.rejected, 0, "{contexto}: rejeitadas");
    assert_eq!(o.sent, total, "{contexto}: enviadas");
    assert_eq!(o.lost(), 0, "{contexto}: perdidas");
    assert_eq!(o.duplicated(), 0, "{contexto}: duplicadas");
    assert_eq!(o.out_of_order, 0, "{contexto}: fora de ordem");
    assert_eq!(o.corrupted, 0, "{contexto}: corrompidas");
    assert_eq!(o.received, total, "{contexto}: warm-up vazou para a fase medida");
    assert_eq!(o.latency.count as u64, total);
    assert!(o.latency.min <= o.latency.p50 && o.latency.p99 <= o.latency.max);
    assert!(o.throughput > 0.0 && o.elapsed_secs > 0.0);
    assert!(o.cpu_secs >= 0.0);
    if docker_disponivel() {
        assert!(o.broker_cpu_secs.is_some(), "{contexto}: CPU do broker nao medida");
        assert!(o.broker_peak_mem_kib.unwrap_or(0) > 0, "{contexto}: memoria do broker");
    }
}

#[test]
fn rabbitmq_entrega_completa_nas_semanticas_suportadas() {
    for semantics in [Semantics::AtMostOnce, Semantics::AtLeastOnce] {
        for (p, c) in [(1, 1), (3, 2)] {
            let c = cfg(p, c, 1024, 3_000);
            let t = RabbitMq::new(rabbit(semantics)).expect("RabbitMQ indisponivel");
            let o = run(&t, &c);
            assert_entrega_completa(&o, &c, &format!("rabbitmq {semantics} {}", c.topology()));
        }
    }
}

#[test]
fn rabbitmq_recusa_exactly_once() {
    let err = RabbitMq::new(rabbit(Semantics::ExactlyOnce))
        .err()
        .expect("exactly-once deveria ser recusado");
    assert!(err.contains("exactly-once"));
}

#[test]
fn rabbitmq_payload_grande_e_rajadas() {
    let mut c = cfg(2, 2, 65_536, 300);
    let t = RabbitMq::new(rabbit(Semantics::AtLeastOnce)).expect("RabbitMQ indisponivel");
    assert_entrega_completa(&run(&t, &c), &c, "rabbitmq 64KiB");

    c.payload_bytes = 64;
    c.messages_per_producer = 3_000;
    c.arrival = Arrival::Burst { batch: 500, pause_ns: 1_000_000 };
    let t = RabbitMq::new(rabbit(Semantics::AtLeastOnce)).expect("RabbitMQ indisponivel");
    assert_entrega_completa(&run(&t, &c), &c, "rabbitmq burst");
}

#[test]
fn kafka_entrega_completa_em_todas_as_semanticas() {
    for semantics in [Semantics::AtMostOnce, Semantics::AtLeastOnce, Semantics::ExactlyOnce] {
        for (p, c) in [(1, 1), (3, 2)] {
            let c = cfg(p, c, 1024, 3_000);
            let t = Kafka::new(kafka(semantics), c.consumers).expect("Kafka indisponivel");
            let o = run(&t, &c);
            assert_entrega_completa(&o, &c, &format!("kafka {semantics} {}", c.topology()));
        }
    }
}

#[test]
fn kafka_payload_grande_e_um_produtor_varios_consumidores() {
    let c = cfg(1, 4, 65_536, 400);
    let t = Kafka::new(kafka(Semantics::AtLeastOnce), c.consumers).expect("Kafka indisponivel");
    assert_entrega_completa(&run(&t, &c), &c, "kafka 64KiB SPMC");
}

#[test]
fn kafka_exactly_once_com_transacao_parcial_no_fim() {
    // 2_150 mensagens com lotes de 1_000: a ultima transacao so fecha no flush; se ela
    // nao fosse confirmada, read_committed esconderia as 150 finais.
    let c = cfg(2, 2, 64, 2_150);
    let mut k = kafka(Semantics::ExactlyOnce);
    k.tx_batch = 1_000;
    let t = Kafka::new(k, c.consumers).expect("Kafka indisponivel");
    assert_entrega_completa(&run(&t, &c), &c, "kafka exactly-once tx=1000");
}
