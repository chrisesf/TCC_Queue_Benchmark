//! Fila lock-free de Michael e Scott (PODC '96).
//!
//! Lista encadeada com no sentinela e dois ponteiros atomicos (`head`, `tail`)
//! atualizados por CAS. O artigo original evita ABA com ponteiros contados e uma
//! free list; aqui a reclamacao de memoria usa epocas (`crossbeam-epoch`), pois
//! Rust nao tem coletor de lixo e um no so pode ser liberado quando nenhuma
//! thread ainda puder le-lo.
//!
//! O fechamento e linearizavel: `close` troca o `next` nulo do ultimo no por um
//! nulo marcado (`CLOSED_TAG`), e o CAS de `enqueue` (que espera nulo sem marca)
//! passa a falhar. Assim nenhum item aceito apos o fechamento pode se perder.

use std::mem::MaybeUninit;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use crossbeam_epoch::{self as epoch, Atomic, Guard, Owned, Shared};
use crossbeam_utils::{Backoff, CachePadded};

use crate::queue::{PushError, Queue};

const CLOSED_TAG: usize = 1;

struct Node<T> {
    /// Nao inicializado no sentinela; o valor e movido para fora no dequeue e o no
    /// passa a ser o novo sentinela, entao nunca e dropado pelo no.
    data: MaybeUninit<T>,
    next: Atomic<Node<T>>,
}

impl<T> Node<T> {
    fn sentinel() -> Self {
        Self {
            data: MaybeUninit::uninit(),
            next: Atomic::null(),
        }
    }
}

enum Dequeue<T> {
    Item(T),
    Empty,
    Closed,
}

pub struct MichaelScottQueue<T> {
    head: CachePadded<Atomic<Node<T>>>,
    tail: CachePadded<Atomic<Node<T>>>,
    /// Ocupacao, mantida apenas na variante limitada (backpressure).
    len: CachePadded<AtomicUsize>,
    capacity: usize,
    bounded: bool,
    closed: AtomicBool,
}

unsafe impl<T: Send> Send for MichaelScottQueue<T> {}
unsafe impl<T: Send> Sync for MichaelScottQueue<T> {}

impl<T> MichaelScottQueue<T> {
    /// Algoritmo original, sem limite de capacidade.
    pub fn unbounded() -> Self {
        Self::new(usize::MAX, false)
    }

    /// Variante limitada: um contador atomico de ocupacao aplica backpressure para
    /// que a profundidade da fila seja comparavel as demais estruturas. O contador
    /// e uma extensao ao algoritmo e adiciona um ponto de contencao.
    pub fn with_capacity(capacity: usize) -> Self {
        assert!(capacity > 0, "capacidade deve ser maior que zero");
        Self::new(capacity, true)
    }

    fn new(capacity: usize, bounded: bool) -> Self {
        let queue = Self {
            head: CachePadded::new(Atomic::null()),
            tail: CachePadded::new(Atomic::null()),
            len: CachePadded::new(AtomicUsize::new(0)),
            capacity,
            bounded,
            closed: AtomicBool::new(false),
        };
        let sentinel = Owned::new(Node::sentinel());
        // Ainda nao compartilhada: nenhuma outra thread pode observar a fila.
        let guard = unsafe { epoch::unprotected() };
        let sentinel = sentinel.into_shared(guard);
        queue.head.store(sentinel, Ordering::Relaxed);
        queue.tail.store(sentinel, Ordering::Relaxed);
        queue
    }

    fn enqueue(&self, item: T, guard: &Guard) -> Result<(), T> {
        let mut new = Owned::new(Node {
            data: MaybeUninit::new(item),
            next: Atomic::null(),
        });
        loop {
            let tail = self.tail.load(Ordering::Acquire, guard);
            // `tail` nunca e nulo e o guard impede sua liberacao.
            let tail_ref = unsafe { tail.deref() };
            let next = tail_ref.next.load(Ordering::Acquire, guard);

            if next.tag() == CLOSED_TAG {
                let node = *new.into_box();
                return Err(unsafe { node.data.assume_init() });
            }
            if !next.is_null() {
                // Tail atrasado: ajuda a avanca-lo.
                let _ = self.tail.compare_exchange(
                    tail,
                    next,
                    Ordering::Release,
                    Ordering::Relaxed,
                    guard,
                );
                continue;
            }
            match tail_ref.next.compare_exchange(
                Shared::null(),
                new,
                Ordering::Release,
                Ordering::Relaxed,
                guard,
            ) {
                Ok(new_shared) => {
                    let _ = self.tail.compare_exchange(
                        tail,
                        new_shared,
                        Ordering::Release,
                        Ordering::Relaxed,
                        guard,
                    );
                    return Ok(());
                }
                Err(e) => new = e.new,
            }
        }
    }

    fn dequeue(&self, guard: &Guard) -> Dequeue<T> {
        loop {
            let head = self.head.load(Ordering::Acquire, guard);
            let head_ref = unsafe { head.deref() };
            let next = head_ref.next.load(Ordering::Acquire, guard);

            if next.is_null() {
                return if next.tag() == CLOSED_TAG {
                    Dequeue::Closed
                } else {
                    Dequeue::Empty
                };
            }

            let tail = self.tail.load(Ordering::Acquire, guard);
            if head == tail {
                let _ = self.tail.compare_exchange(
                    tail,
                    next,
                    Ordering::Release,
                    Ordering::Relaxed,
                    guard,
                );
                continue;
            }

            if self
                .head
                .compare_exchange(head, next, Ordering::Release, Ordering::Relaxed, guard)
                .is_ok()
            {
                // So o vencedor do CAS le o valor; `next` vira o novo sentinela.
                let item = unsafe { ptr::read(next.deref().data.as_ptr()) };
                unsafe { guard.defer_destroy(head) };
                return Dequeue::Item(item);
            }
        }
    }

    fn try_reserve(&self) -> bool {
        let mut cur = self.len.load(Ordering::Relaxed);
        loop {
            if cur >= self.capacity {
                return false;
            }
            match self.len.compare_exchange_weak(
                cur,
                cur + 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(actual) => cur = actual,
            }
        }
    }

    fn push_reserved(&self, item: T) -> Result<(), PushError<T>> {
        let guard = epoch::pin();
        self.enqueue(item, &guard).map_err(|item| {
            if self.bounded {
                self.len.fetch_sub(1, Ordering::AcqRel);
            }
            PushError::Closed(item)
        })
    }

    fn pop_once(&self) -> Dequeue<T> {
        let guard = epoch::pin();
        let result = self.dequeue(&guard);
        if self.bounded && matches!(result, Dequeue::Item(_)) {
            self.len.fetch_sub(1, Ordering::AcqRel);
        }
        result
    }
}

impl<T: Send> Queue<T> for MichaelScottQueue<T> {
    fn push(&self, item: T) -> Result<(), PushError<T>> {
        if self.bounded {
            let backoff = Backoff::new();
            while !self.try_reserve() {
                if self.is_closed() {
                    return Err(PushError::Closed(item));
                }
                backoff.snooze();
            }
        }
        self.push_reserved(item)
    }

    fn try_push(&self, item: T) -> Result<(), PushError<T>> {
        if self.is_closed() {
            return Err(PushError::Closed(item));
        }
        if self.bounded && !self.try_reserve() {
            return Err(PushError::Full(item));
        }
        self.push_reserved(item)
    }

    fn pop(&self) -> Option<T> {
        let backoff = Backoff::new();
        loop {
            match self.pop_once() {
                Dequeue::Item(item) => return Some(item),
                Dequeue::Closed => return None,
                Dequeue::Empty => backoff.snooze(),
            }
        }
    }

    fn try_pop(&self) -> Option<T> {
        match self.pop_once() {
            Dequeue::Item(item) => Some(item),
            Dequeue::Empty | Dequeue::Closed => None,
        }
    }

    fn close(&self) {
        let guard = epoch::pin();
        loop {
            let tail = self.tail.load(Ordering::Acquire, &guard);
            let tail_ref = unsafe { tail.deref() };
            let next = tail_ref.next.load(Ordering::Acquire, &guard);
            if next.tag() == CLOSED_TAG {
                break;
            }
            if !next.is_null() {
                let _ = self.tail.compare_exchange(
                    tail,
                    next,
                    Ordering::Release,
                    Ordering::Relaxed,
                    &guard,
                );
                continue;
            }
            if tail_ref
                .next
                .compare_exchange(
                    Shared::null(),
                    Shared::null().with_tag(CLOSED_TAG),
                    Ordering::Release,
                    Ordering::Relaxed,
                    &guard,
                )
                .is_ok()
            {
                break;
            }
        }
        self.closed.store(true, Ordering::Release);
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Na variante ilimitada percorre a lista: O(n) e apenas aproximado sob concorrencia.
    fn len(&self) -> usize {
        if self.bounded {
            return self.len.load(Ordering::Acquire);
        }
        let guard = epoch::pin();
        let mut count = 0;
        let mut node = self.head.load(Ordering::Acquire, &guard);
        loop {
            let next = unsafe { node.deref() }.next.load(Ordering::Acquire, &guard);
            if next.is_null() {
                return count;
            }
            count += 1;
            node = next;
        }
    }

    fn capacity(&self) -> Option<usize> {
        self.bounded.then_some(self.capacity)
    }

    fn name(&self) -> &'static str {
        "michael-scott"
    }
}

impl<T> Drop for MichaelScottQueue<T> {
    fn drop(&mut self) {
        // `&mut self`: acesso exclusivo, nenhuma thread concorrente.
        unsafe {
            let guard = epoch::unprotected();
            let sentinel = self.head.load(Ordering::Relaxed, guard);
            let mut next = sentinel.deref().next.load(Ordering::Relaxed, guard);
            drop(sentinel.into_owned());
            while !next.is_null() {
                let mut owned = next.into_owned();
                next = owned.next.load(Ordering::Relaxed, guard);
                ptr::drop_in_place(owned.data.as_mut_ptr());
            }
        }
    }
}
