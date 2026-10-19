//! Abstracao sobre o mecanismo de comunicacao, para que filas em memoria e brokers sejam
//! medidos pelo mesmo ambiente de testes (`bench::run`), com as mesmas metricas.
//!
//! Cada produtor e cada consumidor recebe seu proprio handle, criado na thread principal
//! antes da medicao: com brokers isso corresponde a uma conexao/canal por thread, e o
//! custo de abri-los fica fora do tempo medido.

use std::sync::Arc;
use std::time::Duration;

use crate::queue::Queue;

pub trait Transport<M>: Send + Sync + 'static {
    type Producer: Producer<M>;
    type Consumer: Consumer<M>;

    fn producer(&self, index: usize) -> Self::Producer;

    fn consumer(&self, index: usize) -> Self::Consumer;

    /// Os produtores terminaram. Filas em memoria fecham, e os consumidores recebem
    /// `Recv::Closed` depois de drena-las; brokers nao tem "fechamento", e o ambiente
    /// encerra os consumidores quando todas as mensagens medidas chegaram ou apos um
    /// periodo sem mensagens.
    fn finish(&self) {}

    fn name(&self) -> &'static str;

    /// Semantica de entrega configurada (`n/a` para filas em memoria).
    fn semantics(&self) -> &'static str {
        "n/a"
    }

    /// Container Docker do broker, cujo CPU e memoria sao medidos junto com o cliente.
    fn external_container(&self) -> Option<&str> {
        None
    }
}

pub trait Producer<M>: Send + 'static {
    /// Envia a mensagem; devolve-a se o transporte recusou (fila fechada, erro de envio).
    fn send(&mut self, msg: M) -> Result<(), M>;

    /// Bloqueia ate o transporte confirmar o que foi enviado. Devolve quantas mensagens
    /// foram rejeitadas depois de aceitas por `send` (ex.: nack, falha de entrega).
    fn flush(&mut self) -> u64 {
        0
    }
}

pub enum Recv<M> {
    Message(M),
    /// Nada chegou dentro do tempo limite.
    Empty,
    /// O transporte encerrou e nao entregara mais nada.
    Closed,
}

pub trait Consumer<M>: Send + 'static {
    /// Espera ate `timeout` por uma mensagem. Filas em memoria bloqueiam ate a proxima
    /// mensagem ou o fechamento, sem devolver `Empty`.
    fn recv(&mut self, timeout: Duration) -> Recv<M>;
}

/// Adaptador de uma fila em memoria: todos os handles compartilham a mesma fila.
pub struct InMemory<Q>(pub Arc<Q>);

pub struct QueueHandle<Q>(Arc<Q>);

impl<M: Send + 'static, Q: Queue<M> + 'static> Transport<M> for InMemory<Q> {
    type Producer = QueueHandle<Q>;
    type Consumer = QueueHandle<Q>;

    fn producer(&self, _index: usize) -> QueueHandle<Q> {
        QueueHandle(Arc::clone(&self.0))
    }

    fn consumer(&self, _index: usize) -> QueueHandle<Q> {
        QueueHandle(Arc::clone(&self.0))
    }

    fn finish(&self) {
        self.0.close();
    }

    fn name(&self) -> &'static str {
        self.0.name()
    }
}

impl<M: Send + 'static, Q: Queue<M> + 'static> Producer<M> for QueueHandle<Q> {
    #[inline]
    fn send(&mut self, msg: M) -> Result<(), M> {
        self.0.push(msg).map_err(|e| e.into_inner())
    }
}

impl<M: Send + 'static, Q: Queue<M> + 'static> Consumer<M> for QueueHandle<Q> {
    #[inline]
    fn recv(&mut self, _timeout: Duration) -> Recv<M> {
        match self.0.pop() {
            Some(m) => Recv::Message(m),
            None => Recv::Closed,
        }
    }
}
