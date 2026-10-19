//! Apache Kafka via `rdkafka` (librdkafka).
//!
//! Cada repeticao cria um topico novo com uma particao por consumidor (fator de
//! replicacao 1, broker unico) e o remove ao final. O consumidor `i` le a particao `i`
//! por atribuicao manual (sem rebalanceamento durante a medicao); o produtor `p` envia a
//! mensagem de sequencia `s` para a particao `(s + p) % particoes`, distribuindo a carga
//! igualmente. A ordem e preservada por particao, como no log particionado do Kafka.
//!
//! Semanticas:
//! - at-most-once: `acks=0` (sem confirmacao do broker); o offset e armazenado ao
//!   receber, antes do processamento.
//! - at-least-once: `acks=all` com reenvio; o offset so e armazenado apos o processamento.
//! - exactly-once: produtor idempotente e transacional (commit a cada `tx_batch`
//!   mensagens) e consumidor com `isolation.level=read_committed`, que so enxerga
//!   transacoes confirmadas; offset armazenado apos o processamento.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer as _};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::producer::{BaseProducer, BaseRecord, DeliveryResult, Producer as _, ProducerContext};
use rdkafka::{ClientConfig, ClientContext, Message as _, Offset, TopicPartitionList};

use super::{unique_name, Semantics};
use crate::message::Message;
use crate::transport::{self, Recv, Transport};

const OP_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub struct KafkaConfig {
    pub brokers: String,
    pub semantics: Semantics,
    /// `linger.ms` do produtor: espera para formar lotes (padrao do librdkafka: 5).
    pub linger_ms: u32,
    /// Mensagens por transacao (exactly-once).
    pub tx_batch: u64,
    pub container: Option<String>,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            brokers: "kafka:9092".into(),
            semantics: Semantics::AtLeastOnce,
            linger_ms: 5,
            tx_batch: 1000,
            container: None,
        }
    }
}

pub struct Kafka {
    cfg: KafkaConfig,
    topic: String,
    partitions: i32,
    admin: AdminClient<DefaultClientContext>,
}

impl Kafka {
    /// `partitions` deve ser igual ao numero de consumidores.
    pub fn new(cfg: KafkaConfig, partitions: usize) -> Result<Self, String> {
        let partitions = partitions.max(1) as i32;
        let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", &cfg.brokers)
            .create()
            .map_err(|e| format!("Kafka em {}: {e}", cfg.brokers))?;
        let topic = unique_name("tcc-bench");
        let opts = AdminOptions::new().operation_timeout(Some(OP_TIMEOUT));
        let new_topic = NewTopic::new(&topic, partitions, TopicReplication::Fixed(1));
        let results = futures_executor::block_on(admin.create_topics([&new_topic], &opts))
            .map_err(|e| format!("Kafka em {}: falha ao criar topico: {e}", cfg.brokers))?;
        for r in results {
            r.map_err(|(t, code)| format!("falha ao criar topico {t}: {code}"))?;
        }
        wait_for_leaders(&admin, &topic, partitions)?;
        Ok(Self {
            cfg,
            topic,
            partitions,
            admin,
        })
    }
}

/// Aguarda todas as particoes terem lider, para que a primeira mensagem nao pague a
/// eleicao.
fn wait_for_leaders(
    admin: &AdminClient<DefaultClientContext>,
    topic: &str,
    partitions: i32,
) -> Result<(), String> {
    let deadline = Instant::now() + OP_TIMEOUT;
    loop {
        if let Ok(md) = admin.inner().fetch_metadata(Some(topic), Duration::from_secs(5)) {
            let ready = md.topics().iter().any(|t| {
                t.name() == topic
                    && t.error().is_none()
                    && t.partitions().len() == partitions as usize
                    && t.partitions().iter().all(|p| p.leader() >= 0 && p.error().is_none())
            });
            if ready {
                return Ok(());
            }
        }
        if Instant::now() >= deadline {
            return Err(format!("topico {topic} sem lider apos {OP_TIMEOUT:?}"));
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

impl Drop for Kafka {
    fn drop(&mut self) {
        let opts = AdminOptions::new().operation_timeout(Some(OP_TIMEOUT));
        let _ = futures_executor::block_on(self.admin.delete_topics(&[&self.topic], &opts));
    }
}

impl Transport<Message> for Kafka {
    type Producer = KafkaProducer;
    type Consumer = KafkaConsumer;

    fn producer(&self, index: usize) -> KafkaProducer {
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", &self.cfg.brokers)
            .set("client.id", format!("{}-p{index}", self.topic))
            .set("linger.ms", self.cfg.linger_ms.to_string());
        match self.cfg.semantics {
            Semantics::AtMostOnce => config.set("acks", "0").set("enable.idempotence", "false"),
            Semantics::AtLeastOnce => config.set("acks", "all").set("enable.idempotence", "false"),
            Semantics::ExactlyOnce => config
                .set("acks", "all")
                .set("enable.idempotence", "true")
                .set("transactional.id", format!("{}-p{index}", self.topic)),
        };
        let producer: BaseProducer<DeliveryCounter> = config
            .create_with_context(DeliveryCounter::default())
            .unwrap_or_else(|e| panic!("produtor {index}: falha ao criar cliente Kafka: {e}"));
        let transactional = self.cfg.semantics == Semantics::ExactlyOnce;
        if transactional {
            producer
                .init_transactions(OP_TIMEOUT)
                .and_then(|_| producer.begin_transaction())
                .unwrap_or_else(|e| panic!("produtor {index}: falha ao iniciar transacao: {e}"));
        }
        KafkaProducer {
            producer,
            topic: self.topic.clone(),
            partitions: self.partitions,
            transactional,
            tx_batch: self.cfg.tx_batch.max(1),
            in_tx: 0,
            tx_failed: 0,
            buf: Vec::new(),
        }
    }

    fn consumer(&self, index: usize) -> KafkaConsumer {
        let store_after_processing = self.cfg.semantics != Semantics::AtMostOnce;
        let isolation = if self.cfg.semantics == Semantics::ExactlyOnce {
            "read_committed"
        } else {
            "read_uncommitted"
        };
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &self.cfg.brokers)
            .set("group.id", format!("{}-g", self.topic))
            .set("client.id", format!("{}-c{index}", self.topic))
            .set("enable.auto.commit", "true")
            .set("enable.auto.offset.store", (!store_after_processing).to_string())
            .set("isolation.level", isolation)
            .set("enable.partition.eof", "false")
            .create()
            .unwrap_or_else(|e| panic!("consumidor {index}: falha ao criar cliente Kafka: {e}"));
        let partition = (index as i32) % self.partitions;
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(&self.topic, partition, Offset::Beginning)
            .and_then(|_| consumer.assign(&tpl))
            .unwrap_or_else(|e| panic!("consumidor {index}: falha ao atribuir particao: {e}"));
        KafkaConsumer {
            consumer,
            topic: self.topic.clone(),
            store_after_processing,
            pending_offset: None,
        }
    }

    fn name(&self) -> &'static str {
        "kafka"
    }

    fn semantics(&self) -> &'static str {
        self.cfg.semantics.label()
    }

    fn external_container(&self) -> Option<&str> {
        self.cfg.container.as_deref()
    }
}

/// Conta mensagens cuja entrega o broker recusou (ou expirou) apos `send`.
#[derive(Default)]
pub struct DeliveryCounter {
    failed: AtomicU64,
}

impl ClientContext for DeliveryCounter {}

impl ProducerContext for DeliveryCounter {
    type DeliveryOpaque = ();

    fn delivery(&self, result: &DeliveryResult<'_>, _: ()) {
        if result.is_err() {
            self.failed.fetch_add(1, Ordering::Relaxed);
        }
    }
}

pub struct KafkaProducer {
    producer: BaseProducer<DeliveryCounter>,
    topic: String,
    partitions: i32,
    transactional: bool,
    tx_batch: u64,
    in_tx: u64,
    /// Mensagens descartadas por transacoes abortadas.
    tx_failed: u64,
    buf: Vec<u8>,
}

impl KafkaProducer {
    fn commit(&mut self) {
        if self.in_tx == 0 {
            return;
        }
        if self.producer.commit_transaction(OP_TIMEOUT).is_err() {
            let _ = self.producer.abort_transaction(OP_TIMEOUT);
            self.tx_failed += self.in_tx;
        }
        self.in_tx = 0;
        self.producer
            .begin_transaction()
            .expect("falha ao iniciar nova transacao");
    }
}

impl transport::Producer<Message> for KafkaProducer {
    fn send(&mut self, msg: Message) -> Result<(), Message> {
        msg.encode_into(&mut self.buf);
        let partition = ((msg.sequence() + msg.producer_id() as u64) % self.partitions as u64) as i32;
        let mut record = BaseRecord::<(), [u8]>::to(&self.topic)
            .partition(partition)
            .payload(&self.buf[..]);
        loop {
            match self.producer.send(record) {
                Ok(()) => break,
                // Fila local do librdkafka cheia: contrapressao, aguarda entregas.
                Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), r)) => {
                    record = r;
                    self.producer.poll(Duration::from_millis(1));
                }
                Err(_) => return Err(msg),
            }
        }
        // Atende callbacks de entrega sem bloquear.
        self.producer.poll(Duration::ZERO);
        if self.transactional {
            self.in_tx += 1;
            if self.in_tx >= self.tx_batch {
                self.commit();
            }
        }
        Ok(())
    }

    fn flush(&mut self) -> u64 {
        if self.transactional {
            self.commit();
        }
        let _ = self.producer.flush(OP_TIMEOUT);
        self.producer.context().failed.swap(0, Ordering::Relaxed) + std::mem::take(&mut self.tx_failed)
    }
}

impl Drop for KafkaProducer {
    fn drop(&mut self) {
        if self.transactional {
            let _ = self.producer.abort_transaction(OP_TIMEOUT);
        }
    }
}

pub struct KafkaConsumer {
    consumer: BaseConsumer,
    topic: String,
    store_after_processing: bool,
    /// (particao, offset) da mensagem anterior, armazenado so depois de processada.
    pending_offset: Option<(i32, i64)>,
}

impl KafkaConsumer {
    fn store_previous(&mut self) {
        if let Some((partition, offset)) = self.pending_offset.take() {
            let _ = self.consumer.store_offset(&self.topic, partition, offset);
        }
    }
}

impl transport::Consumer<Message> for KafkaConsumer {
    fn recv(&mut self, timeout: Duration) -> Recv<Message> {
        self.store_previous();
        match self.consumer.poll(timeout) {
            Some(Ok(m)) => {
                let msg = m
                    .payload()
                    .and_then(Message::decode)
                    .expect("mensagem Kafka com formato binario invalido");
                if self.store_after_processing {
                    self.pending_offset = Some((m.partition(), m.offset()));
                }
                Recv::Message(msg)
            }
            Some(Err(e)) => {
                eprintln!("erro no consumo Kafka: {e}");
                Recv::Empty
            }
            None => Recv::Empty,
        }
    }
}

impl Drop for KafkaConsumer {
    fn drop(&mut self) {
        self.store_previous();
    }
}
