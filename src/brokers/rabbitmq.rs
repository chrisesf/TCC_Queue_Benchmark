//! RabbitMQ (AMQP 0-9-1) via `lapin`.
//!
//! Cada repeticao declara uma fila nova, alimentada pela exchange padrao (roteamento
//! direto pelo nome da fila) e consumida por consumidores concorrentes (cada mensagem vai
//! para um unico consumidor). Cada produtor e cada consumidor tem conexao e canal proprios.
//!
//! Semanticas:
//! - at-most-once: mensagens transientes, sem publisher confirms, consumo com auto-ack
//!   (o broker considera entregue ao enviar).
//! - at-least-once: mensagens persistentes, publisher confirms (janela de confirmacoes
//!   pendentes) e ack manual apos o processamento.
//! - exactly-once: nao oferecida pelo AMQP 0-9-1 (sem envio idempotente nem transacoes
//!   que cubram o consumo); recusada na criacao.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures_util::task::noop_waker_ref;
use futures_util::{FutureExt, StreamExt};
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, BasicQosOptions,
    ConfirmSelectOptions, QueueDeclareOptions, QueueDeleteOptions,
};
use lapin::types::{AMQPValue, FieldTable, ShortString};
use lapin::{Acker, BasicProperties, Channel, Confirmation, Connection, ConnectionProperties};
use tokio::runtime::Runtime;

use super::{unique_name, Semantics};
use crate::message::Message;
use crate::transport::{self, Recv, Transport};

#[derive(Debug, Clone)]
pub struct RabbitConfig {
    pub url: String,
    pub semantics: Semantics,
    /// `basic.qos`: mensagens entregues e ainda nao confirmadas por consumidor.
    pub prefetch: u16,
    /// Publicacoes pendentes por produtor: ainda nao escritas no socket ou, em
    /// at-least-once, ainda nao confirmadas pelo broker.
    pub confirm_window: usize,
    /// Container do broker para medir CPU/memoria.
    pub container: Option<String>,
}

impl Default for RabbitConfig {
    fn default() -> Self {
        Self {
            url: "amqp://rabbitmq:5672/%2f".into(),
            semantics: Semantics::AtLeastOnce,
            prefetch: 1000,
            confirm_window: 1000,
            container: None,
        }
    }
}

pub struct RabbitMq {
    cfg: RabbitConfig,
    queue: String,
    rt: Arc<Runtime>,
    admin: Connection,
    admin_channel: Channel,
}

impl RabbitMq {
    pub fn new(cfg: RabbitConfig) -> Result<Self, String> {
        if cfg.semantics == Semantics::ExactlyOnce {
            return Err("RabbitMQ (AMQP 0-9-1) nao oferece exactly-once: nao ha publicacao \
                        idempotente nem transacao que inclua o consumo"
                .into());
        }
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("amqp-io")
            .build()
            .map_err(|e| format!("falha ao criar runtime tokio: {e}"))?;
        let queue = unique_name("tcc-bench");

        let (admin, admin_channel) = rt
            .block_on(async {
                let conn = connect(&cfg.url).await?;
                let ch = conn.create_channel().await?;
                let mut args = FieldTable::default();
                args.insert("x-queue-type".into(), AMQPValue::LongString("classic".into()));
                // Duravel em ambas as semanticas: filas transientes nao exclusivas estao
                // depreciadas no RabbitMQ 4; a diferenca fica na persistencia das mensagens.
                ch.queue_declare(
                    queue.as_str().into(),
                    QueueDeclareOptions {
                        durable: true,
                        ..Default::default()
                    },
                    args,
                )
                .await?;
                Ok::<_, lapin::Error>((conn, ch))
            })
            .map_err(|e| format!("RabbitMQ em {}: {e}", cfg.url))?;

        Ok(Self {
            cfg,
            queue,
            rt: Arc::new(rt),
            admin,
            admin_channel,
        })
    }

    fn reliable(&self) -> bool {
        self.cfg.semantics == Semantics::AtLeastOnce
    }
}

async fn connect(url: &str) -> lapin::Result<Connection> {
    Connection::connect(url, ConnectionProperties::default()).await
}

type Pending<T> = Pin<Box<dyn Future<Output = lapin::Result<T>> + Send>>;

/// Operacoes AMQP em andamento. O `lapin` so conclui uma publicacao (ou ack) quando a
/// thread de I/O da conexao escreve o frame no socket; aguardar cada uma serializaria o
/// cliente com uma troca de thread por mensagem. Cada operacao e iniciada com um unico
/// poll, que enfileira os frames na ordem de envio, e so e aguardada quando ha mais de
/// `window` pendentes ou em `settle_all`.
struct Pipeline<T> {
    rt: Arc<Runtime>,
    pending: VecDeque<Pending<T>>,
    window: usize,
}

impl<T> Pipeline<T> {
    fn new(rt: Arc<Runtime>, window: usize) -> Self {
        Self {
            rt,
            pending: VecDeque::with_capacity(window + 1),
            window: window.max(1),
        }
    }

    fn start(&mut self, mut op: Pending<T>, mut on_done: impl FnMut(lapin::Result<T>)) {
        let polled = {
            let _ctx = self.rt.enter();
            op.as_mut().poll(&mut Context::from_waker(noop_waker_ref()))
        };
        match polled {
            Poll::Ready(r) => on_done(r),
            Poll::Pending => self.pending.push_back(op),
        }
        while self.pending.len() > self.window {
            if let Some(op) = self.pending.pop_front() {
                on_done(self.rt.block_on(op));
            }
        }
    }

    fn settle_all(&mut self, mut on_done: impl FnMut(lapin::Result<T>)) {
        while let Some(op) = self.pending.pop_front() {
            on_done(self.rt.block_on(op));
        }
    }
}

impl Drop for RabbitMq {
    fn drop(&mut self) {
        let _ = self.rt.block_on(async {
            let _ = self
                .admin_channel
                .queue_delete(self.queue.as_str().into(), QueueDeleteOptions::default())
                .await;
            self.admin.close(200, "OK".into()).await
        });
    }
}

impl Transport<Message> for RabbitMq {
    type Producer = RabbitProducer;
    type Consumer = RabbitConsumer;

    fn producer(&self, index: usize) -> RabbitProducer {
        let reliable = self.reliable();
        let (conn, channel) = self
            .rt
            .block_on(async {
                let conn = connect(&self.cfg.url).await?;
                let ch = conn.create_channel().await?;
                if reliable {
                    ch.confirm_select(ConfirmSelectOptions::default()).await?;
                }
                Ok::<_, lapin::Error>((conn, ch))
            })
            .unwrap_or_else(|e| panic!("produtor {index}: falha ao conectar ao RabbitMQ: {e}"));
        // delivery_mode 2 = persistente (gravada em disco), 1 = transiente.
        let properties = BasicProperties::default().with_delivery_mode(if reliable { 2 } else { 1 });
        RabbitProducer {
            rt: Arc::clone(&self.rt),
            conn,
            channel,
            queue: self.queue.as_str().into(),
            properties,
            in_flight: Pipeline::new(Arc::clone(&self.rt), self.cfg.confirm_window),
            rejected: 0,
        }
    }

    fn consumer(&self, index: usize) -> RabbitConsumer {
        let manual_ack = self.reliable();
        let prefetch = self.cfg.prefetch;
        let queue = self.queue.as_str();
        let (conn, channel, consumer) = self
            .rt
            .block_on(async {
                let conn = connect(&self.cfg.url).await?;
                let ch = conn.create_channel().await?;
                ch.basic_qos(prefetch, BasicQosOptions::default()).await?;
                let consumer = ch
                    .basic_consume(
                        queue.into(),
                        format!("consumidor-{index}").into(),
                        BasicConsumeOptions {
                            no_ack: !manual_ack,
                            ..Default::default()
                        },
                        FieldTable::default(),
                    )
                    .await?;
                Ok::<_, lapin::Error>((conn, ch, consumer))
            })
            .unwrap_or_else(|e| panic!("consumidor {index}: falha ao conectar ao RabbitMQ: {e}"));
        RabbitConsumer {
            rt: Arc::clone(&self.rt),
            conn,
            _channel: channel,
            consumer,
            manual_ack,
            unacked: None,
            acks: Pipeline::new(Arc::clone(&self.rt), prefetch as usize),
        }
    }

    fn name(&self) -> &'static str {
        "rabbitmq"
    }

    fn semantics(&self) -> &'static str {
        self.cfg.semantics.label()
    }

    fn external_container(&self) -> Option<&str> {
        self.cfg.container.as_deref()
    }
}

pub struct RabbitProducer {
    rt: Arc<Runtime>,
    conn: Connection,
    channel: Channel,
    queue: ShortString,
    properties: BasicProperties,
    /// Publicacoes ainda nao escritas ou, com confirms, ainda nao confirmadas.
    in_flight: Pipeline<Confirmation>,
    rejected: u64,
}

/// Erro de envio ou nack do broker. Sem confirms, a confirmacao e `NotRequested`.
fn publish_failed(result: lapin::Result<Confirmation>) -> bool {
    !matches!(result, Ok(ref c) if !c.is_nack())
}

impl transport::Producer<Message> for RabbitProducer {
    fn send(&mut self, msg: Message) -> Result<(), Message> {
        let channel = self.channel.clone();
        let queue = self.queue.clone();
        let properties = self.properties.clone();
        let payload = msg.encode();
        let publish: Pending<Confirmation> = Box::pin(async move {
            let confirm = channel
                .basic_publish(
                    ShortString::from(""),
                    queue,
                    BasicPublishOptions::default(),
                    &payload,
                    properties,
                )
                .await?;
            confirm.await
        });
        let rejected = &mut self.rejected;
        self.in_flight.start(publish, |r| {
            if publish_failed(r) {
                *rejected += 1;
            }
        });
        Ok(())
    }

    fn flush(&mut self) -> u64 {
        let rejected = &mut self.rejected;
        self.in_flight.settle_all(|r| {
            if publish_failed(r) {
                *rejected += 1;
            }
        });
        std::mem::take(&mut self.rejected)
    }
}

impl Drop for RabbitProducer {
    fn drop(&mut self) {
        let _ = self.rt.block_on(self.conn.close(200, "OK".into()));
    }
}

pub struct RabbitConsumer {
    rt: Arc<Runtime>,
    conn: Connection,
    _channel: Channel,
    consumer: lapin::Consumer,
    manual_ack: bool,
    /// Entrega anterior, confirmada so na chamada seguinte, depois de processada.
    unacked: Option<Acker>,
    acks: Pipeline<bool>,
}

impl RabbitConsumer {
    fn ack_previous(&mut self) {
        if let Some(acker) = self.unacked.take() {
            let ack: Pending<bool> =
                Box::pin(async move { acker.ack(BasicAckOptions::default()).await });
            self.acks.start(ack, |_| ());
        }
    }
}

impl transport::Consumer<Message> for RabbitConsumer {
    fn recv(&mut self, timeout: Duration) -> Recv<Message> {
        self.ack_previous();
        let consumer = &mut self.consumer;
        // Entregas ja recebidas pela thread de I/O saem sem armar temporizador.
        let ready = {
            let _ctx = self.rt.enter();
            consumer.next().now_or_never()
        };
        let next = match ready {
            Some(item) => Ok(item),
            None => self
                .rt
                .block_on(async { tokio::time::timeout(timeout, consumer.next()).await }),
        };
        match next {
            Ok(Some(Ok(delivery))) => {
                let msg = Message::decode(&delivery.data)
                    .expect("mensagem AMQP com formato binario invalido");
                if self.manual_ack {
                    self.unacked = Some(delivery.acker);
                }
                Recv::Message(msg)
            }
            Ok(Some(Err(e))) => {
                eprintln!("erro no consumo AMQP: {e}");
                Recv::Closed
            }
            Ok(None) => Recv::Closed,
            Err(_) => Recv::Empty,
        }
    }
}

impl Drop for RabbitConsumer {
    fn drop(&mut self) {
        self.ack_previous();
        self.acks.settle_all(|_| ());
        let _ = self.rt.block_on(self.conn.close(200, "OK".into()));
    }
}
