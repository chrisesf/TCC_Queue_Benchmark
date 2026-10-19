//! Transportes sobre message brokers (RabbitMQ via `lapin`, Kafka via `rdkafka`).
//!
//! As mensagens trafegam com a mesma serializacao binaria fixa (`Message::encode`), e
//! cada repeticao usa uma fila/topico novo, removido ao final.

#[cfg(feature = "kafka")]
pub mod kafka;
#[cfg(feature = "rabbitmq")]
pub mod rabbitmq;

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

/// Semantica de entrega (Kleppmann, 2017).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Semantics {
    /// Sem confirmacao do envio; a posicao e confirmada antes do processamento.
    AtMostOnce,
    /// Envio confirmado pelo broker; a posicao e confirmada apos o processamento.
    AtLeastOnce,
    /// Envio idempotente e transacional, leitura apenas do que foi confirmado.
    ExactlyOnce,
}

impl Semantics {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "at-most-once" => Some(Self::AtMostOnce),
            "at-least-once" => Some(Self::AtLeastOnce),
            "exactly-once" => Some(Self::ExactlyOnce),
            _ => None,
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::AtMostOnce => "at-most-once",
            Self::AtLeastOnce => "at-least-once",
            Self::ExactlyOnce => "exactly-once",
        }
    }
}

impl fmt::Display for Semantics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

/// Nome unico por execucao para filas/topicos, evitando reaproveitar estado anterior.
pub(crate) fn unique_name(prefix: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!(
        "{prefix}-{}-{nanos}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}
