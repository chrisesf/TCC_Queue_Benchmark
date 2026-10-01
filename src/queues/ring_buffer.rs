//! Ring buffer MPMC inspirado no LMAX Disruptor (Thompson et al., 2011).
//!
//! Elementos do Disruptor mantidos:
//! - buffer circular de tamanho fixo (potencia de dois, indice por mascara),
//!   alocado e tocado na construcao, sem alocacao no caminho quente;
//! - cursores de produtor e consumidor como sequencias de 64 bits estritamente
//!   crescentes, cada um em sua propria linha de cache (cache line padding);
//! - uma sequencia por slot indicando quando ele foi publicado ou liberado,
//!   equivalente ao `availableBuffer` do `MultiProducerSequencer`;
//! - espera ativa com recuo para `yield` quando cheio/vazio, como a
//!   `YieldingWaitStrategy`.
//!
//! Diferenca: o Disruptor entrega cada evento a todos os consumidores (multicast);
//! aqui cada mensagem e consumida uma unica vez, semantica de fila exigida pela
//! comparacao. Consumidores disputam o cursor de leitura via CAS, como o
//! `WorkerPool` do Disruptor.
//!
//! O bit menos significativo do cursor do produtor marca a fila como fechada, de
//! modo que `close` e `push` disputam a mesma palavra e o fechamento e linearizavel.

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicU64, Ordering};

use crossbeam_utils::{Backoff, CachePadded};

use crate::queue::{PushError, Queue};

const CLOSED_BIT: u64 = 1;
const POS_STEP: u64 = 2;

struct Slot<T> {
    /// `pos`: livre para o produtor da volta `pos`;
    /// `pos + 1`: publicado, pronto para o consumidor de `pos`;
    /// `pos + capacidade`: liberado para a proxima volta.
    sequence: AtomicU64,
    value: UnsafeCell<MaybeUninit<T>>,
}

enum TryPush<T> {
    Done,
    Full(T),
    Closed(T),
}

enum TryPop<T> {
    Item(T),
    Empty,
    Closed,
}

pub struct RingBuffer<T> {
    /// Posicao do proximo produtor, deslocada 1 bit; o bit 0 e `CLOSED_BIT`.
    tail: CachePadded<AtomicU64>,
    /// Posicao do proximo consumidor.
    head: CachePadded<AtomicU64>,
    buffer: Box<[Slot<T>]>,
    mask: u64,
}

unsafe impl<T: Send> Send for RingBuffer<T> {}
unsafe impl<T: Send> Sync for RingBuffer<T> {}

impl<T> RingBuffer<T> {
    /// `capacity` deve ser potencia de dois (indice por mascara) e >= 2: com um unico
    /// slot, o estado "publicado" (`pos + 1`) coincide com "livre" da volta seguinte.
    pub fn with_capacity(capacity: usize) -> Self {
        assert!(
            capacity >= 2 && capacity.is_power_of_two(),
            "capacidade do ring buffer deve ser potencia de dois >= 2 (recebido {capacity})"
        );
        let mut buffer: Box<[Slot<T>]> = (0..capacity as u64)
            .map(|i| Slot {
                sequence: AtomicU64::new(i),
                value: UnsafeCell::new(MaybeUninit::uninit()),
            })
            .collect();
        // Toca todas as paginas para que a primeira volta nao pague page faults
        // durante a medicao.
        for slot in buffer.iter_mut() {
            unsafe { slot.value.get_mut().as_mut_ptr().write_bytes(0, 1) };
        }
        Self {
            tail: CachePadded::new(AtomicU64::new(0)),
            head: CachePadded::new(AtomicU64::new(0)),
            buffer,
            mask: capacity as u64 - 1,
        }
    }

    /// Bytes pre-alocados pelo buffer de slots.
    pub fn allocated_bytes(&self) -> usize {
        self.buffer.len() * std::mem::size_of::<Slot<T>>()
    }

    #[inline]
    fn slot(&self, pos: u64) -> &Slot<T> {
        &self.buffer[(pos & self.mask) as usize]
    }

    fn try_push_inner(&self, item: T) -> TryPush<T> {
        let mut tail = self.tail.load(Ordering::Relaxed);
        loop {
            if tail & CLOSED_BIT != 0 {
                return TryPush::Closed(item);
            }
            let pos = tail / POS_STEP;
            let slot = self.slot(pos);
            let seq = slot.sequence.load(Ordering::Acquire);
            let diff = seq.wrapping_sub(pos) as i64;

            if diff == 0 {
                match self.tail.compare_exchange_weak(
                    tail,
                    tail + POS_STEP,
                    Ordering::SeqCst,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        // O CAS deu posse exclusiva do slot ate a publicacao.
                        unsafe { (*slot.value.get()).write(item) };
                        slot.sequence.store(pos + 1, Ordering::Release);
                        return TryPush::Done;
                    }
                    Err(current) => tail = current,
                }
            } else if diff < 0 {
                // Slot ainda ocupado pela volta anterior; confirma que o cursor nao
                // mudou antes de declarar cheia.
                let current = self.tail.load(Ordering::Relaxed);
                if current == tail {
                    return TryPush::Full(item);
                }
                tail = current;
            } else {
                tail = self.tail.load(Ordering::Relaxed);
            }
        }
    }

    fn try_pop_inner(&self) -> TryPop<T> {
        let mut head = self.head.load(Ordering::Relaxed);
        loop {
            let slot = self.slot(head);
            let seq = slot.sequence.load(Ordering::Acquire);
            let diff = seq.wrapping_sub(head + 1) as i64;

            if diff == 0 {
                match self.head.compare_exchange_weak(
                    head,
                    head + 1,
                    Ordering::SeqCst,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        let item = unsafe { (*slot.value.get()).assume_init_read() };
                        slot.sequence
                            .store(head + self.buffer.len() as u64, Ordering::Release);
                        return TryPop::Item(item);
                    }
                    Err(current) => head = current,
                }
            } else if diff < 0 {
                let tail = self.tail.load(Ordering::SeqCst);
                if tail / POS_STEP == head {
                    return if tail & CLOSED_BIT != 0 {
                        TryPop::Closed
                    } else {
                        TryPop::Empty
                    };
                }
                // Um produtor reservou o slot mas ainda nao publicou.
                return TryPop::Empty;
            } else {
                head = self.head.load(Ordering::Relaxed);
            }
        }
    }
}

impl<T: Send> Queue<T> for RingBuffer<T> {
    fn push(&self, mut item: T) -> Result<(), PushError<T>> {
        let backoff = Backoff::new();
        loop {
            match self.try_push_inner(item) {
                TryPush::Done => return Ok(()),
                TryPush::Closed(v) => return Err(PushError::Closed(v)),
                TryPush::Full(v) => {
                    item = v;
                    backoff.snooze();
                }
            }
        }
    }

    fn try_push(&self, item: T) -> Result<(), PushError<T>> {
        match self.try_push_inner(item) {
            TryPush::Done => Ok(()),
            TryPush::Full(v) => Err(PushError::Full(v)),
            TryPush::Closed(v) => Err(PushError::Closed(v)),
        }
    }

    fn pop(&self) -> Option<T> {
        let backoff = Backoff::new();
        loop {
            match self.try_pop_inner() {
                TryPop::Item(item) => return Some(item),
                TryPop::Closed => return None,
                TryPop::Empty => backoff.snooze(),
            }
        }
    }

    fn try_pop(&self) -> Option<T> {
        match self.try_pop_inner() {
            TryPop::Item(item) => Some(item),
            TryPop::Empty | TryPop::Closed => None,
        }
    }

    fn close(&self) {
        self.tail.fetch_or(CLOSED_BIT, Ordering::SeqCst);
    }

    fn is_closed(&self) -> bool {
        self.tail.load(Ordering::Acquire) & CLOSED_BIT != 0
    }

    fn len(&self) -> usize {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire) / POS_STEP;
        tail.saturating_sub(head) as usize
    }

    fn capacity(&self) -> Option<usize> {
        Some(self.buffer.len())
    }

    fn name(&self) -> &'static str {
        "ring-buffer"
    }
}

impl<T> Drop for RingBuffer<T> {
    fn drop(&mut self) {
        let head = *self.head.get_mut();
        let tail = *self.tail.get_mut() / POS_STEP;
        for pos in head..tail {
            let slot = &mut self.buffer[(pos & self.mask) as usize];
            unsafe { slot.value.get_mut().assume_init_drop() };
        }
    }
}
