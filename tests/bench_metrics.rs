//! Verifica as metricas produzidas pelo ambiente de testes (`run_once`).

use std::sync::Arc;

use tcc_bench::bench::{run_once, Arrival, RunConfig, RunOutcome};
use tcc_bench::message::{BenchMessage, FixedMessage, Message};
use tcc_bench::queue::{PushError, Queue};
use tcc_bench::queues::{MichaelScottQueue, MutexQueue, RingBuffer};

fn cfg(producers: usize, consumers: usize, arrival: Arrival) -> RunConfig {
    RunConfig {
        producers,
        consumers,
        messages_per_producer: 5_000,
        warmup_per_producer: 500,
        payload_bytes: 64,
        arrival,
    }
}

fn assert_consistente(o: &RunOutcome, cfg: &RunConfig) {
    let total = cfg.total_messages();
    assert_eq!(o.sent, total);
    assert_eq!(o.received, total, "warm-up vazou para a fase medida ou houve perda");
    assert_eq!(o.unique, total);
    assert_eq!(o.errors(), 0);
    assert_eq!(o.latency.count as u64, total);
    assert!(o.latency.min <= o.latency.p50);
    assert!(o.latency.p50 <= o.latency.p95);
    assert!(o.latency.p95 <= o.latency.p99);
    assert!(o.latency.p99 <= o.latency.max);
    assert!(o.throughput > 0.0 && o.elapsed_secs > 0.0);
}

#[test]
fn metricas_consistentes_para_todas_as_filas_e_topologias() {
    for (p, c) in [(1, 1), (4, 1), (1, 4), (4, 4)] {
        let cfg = cfg(p, c, Arrival::Saturated);
        assert_consistente(&run_once(Arc::new(MutexQueue::<Message>::with_capacity(256)), &cfg), &cfg);
        assert_consistente(
            &run_once(Arc::new(MichaelScottQueue::<Message>::with_capacity(256)), &cfg),
            &cfg,
        );
        assert_consistente(&run_once(Arc::new(MichaelScottQueue::<Message>::unbounded()), &cfg), &cfg);
        assert_consistente(
            &run_once(Arc::new(RingBuffer::<FixedMessage<64>>::with_capacity(256)), &cfg),
            &cfg,
        );
    }
}

#[test]
fn modos_de_chegada_burst_e_paced() {
    let burst = cfg(2, 2, Arrival::Burst { batch: 100, pause_ns: 10_000 });
    assert_consistente(&run_once(Arc::new(RingBuffer::<FixedMessage<64>>::with_capacity(64)), &burst), &burst);

    let paced = cfg(2, 2, Arrival::Paced { rate_per_producer: 200_000.0 });
    let o = run_once(Arc::new(MutexQueue::<Message>::with_capacity(64)), &paced);
    assert_consistente(&o, &paced);
    // 5000 mensagens a 200k msg/s levam ao menos 25 ms.
    assert!(o.elapsed_secs >= 0.024, "taxa nao respeitada: {}", o.elapsed_secs);
}

#[test]
fn sem_warmup() {
    let mut c = cfg(2, 2, Arrival::Saturated);
    c.warmup_per_producer = 0;
    assert_consistente(&run_once(Arc::new(MichaelScottQueue::<Message>::with_capacity(64)), &c), &c);
}

/// Fila defeituosa que duplica uma mensagem e descarta outra: o total recebido bate
/// com o enviado, mas as metricas precisam acusar ambos os erros.
struct FilaDefeituosa(MutexQueue<Message>);

impl Queue<Message> for FilaDefeituosa {
    fn push(&self, item: Message) -> Result<(), PushError<Message>> {
        match item.sequence() {
            1_000 => self.0.push(item.clone()).and_then(|_| self.0.push(item)),
            2_000 => Ok(()),
            _ => self.0.push(item),
        }
    }
    fn try_push(&self, item: Message) -> Result<(), PushError<Message>> {
        self.0.try_push(item)
    }
    fn pop(&self) -> Option<Message> {
        self.0.pop()
    }
    fn try_pop(&self) -> Option<Message> {
        self.0.try_pop()
    }
    fn close(&self) {
        self.0.close()
    }
    fn is_closed(&self) -> bool {
        self.0.is_closed()
    }
    fn len(&self) -> usize {
        self.0.len()
    }
    fn capacity(&self) -> Option<usize> {
        self.0.capacity()
    }
    fn name(&self) -> &'static str {
        "defeituosa"
    }
}

#[test]
fn perda_e_duplicacao_que_se_compensam_sao_detectadas() {
    let c = cfg(1, 2, Arrival::Saturated);
    let o = run_once(Arc::new(FilaDefeituosa(MutexQueue::with_capacity(64))), &c);
    assert_eq!(o.received, o.sent, "o defeito deveria manter o total igual");
    assert_eq!(o.lost(), 1);
    assert_eq!(o.duplicated(), 1);
}

/// Altera um byte do payload de uma mensagem: sem perda nem duplicacao, so a validacao
/// do conteudo feita pelo consumidor pode acusar.
struct FilaQueCorrompe(MutexQueue<Message>);

impl Queue<Message> for FilaQueCorrompe {
    fn push(&self, mut item: Message) -> Result<(), PushError<Message>> {
        if item.sequence() == 1_500 {
            item.payload[10] ^= 0xFF;
        }
        self.0.push(item)
    }
    fn try_push(&self, item: Message) -> Result<(), PushError<Message>> {
        self.0.try_push(item)
    }
    fn pop(&self) -> Option<Message> {
        self.0.pop()
    }
    fn try_pop(&self) -> Option<Message> {
        self.0.try_pop()
    }
    fn close(&self) {
        self.0.close()
    }
    fn is_closed(&self) -> bool {
        self.0.is_closed()
    }
    fn len(&self) -> usize {
        self.0.len()
    }
    fn capacity(&self) -> Option<usize> {
        self.0.capacity()
    }
    fn name(&self) -> &'static str {
        "corrompe"
    }
}

#[test]
fn payload_corrompido_e_detectado() {
    let c = cfg(2, 2, Arrival::Saturated);
    let o = run_once(Arc::new(FilaQueCorrompe(MutexQueue::with_capacity(64))), &c);
    assert_eq!(o.lost() + o.duplicated() + o.out_of_order, 0);
    assert_eq!(o.corrupted, 2, "uma mensagem corrompida por produtor");
}

#[test]
fn latencia_nunca_negativa_com_relogio_monotono() {
    let c = cfg(4, 4, Arrival::Saturated);
    let o = run_once(Arc::new(RingBuffer::<FixedMessage<64>>::with_capacity(128)), &c);
    assert_eq!(o.discarded_samples, 0);
    let m = <Message as BenchMessage>::build(0, 0, &[1; 8]);
    assert_eq!(m.payload_len(), 8);
}
