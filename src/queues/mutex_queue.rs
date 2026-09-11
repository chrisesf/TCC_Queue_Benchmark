use std::collections::VecDeque;
use std::sync::{Condvar, Mutex};

use crate::queue::{PushError, Queue};

struct Inner<T> {
    buf: VecDeque<T>,
    closed: bool,
}

pub struct MutexQueue<T> {
    inner: Mutex<Inner<T>>,
    not_empty: Condvar,
    not_full: Condvar,
    capacity: usize,
    bounded: bool,
}

impl<T> MutexQueue<T> {
    pub fn with_capacity(capacity: usize) -> Self {
        assert!(capacity > 0, "capacidade deve ser maior que zero");
        Self {
            inner: Mutex::new(Inner {
                buf: VecDeque::with_capacity(capacity),
                closed: false,
            }),
            not_empty: Condvar::new(),
            not_full: Condvar::new(),
            capacity,
            bounded: true,
        }
    }

    /// Fila ilimitada (cenario de controle).
    pub fn unbounded() -> Self {
        Self {
            inner: Mutex::new(Inner {
                buf: VecDeque::new(),
                closed: false,
            }),
            not_empty: Condvar::new(),
            not_full: Condvar::new(),
            capacity: usize::MAX,
            bounded: false,
        }
    }
}

impl<T: Send> Queue<T> for MutexQueue<T> {
    fn push(&self, item: T) -> Result<(), PushError<T>> {
        let mut guard = self.inner.lock().expect("mutex corrompido");
        if self.bounded {
            while guard.buf.len() >= self.capacity && !guard.closed {
                guard = self.not_full.wait(guard).expect("mutex corrompido");
            }
        }
        if guard.closed {
            return Err(PushError::Closed(item));
        }
        guard.buf.push_back(item);
        drop(guard);
        self.not_empty.notify_one();
        Ok(())
    }

    fn try_push(&self, item: T) -> Result<(), PushError<T>> {
        let mut guard = self.inner.lock().expect("mutex corrompido");
        if guard.closed {
            return Err(PushError::Closed(item));
        }
        if self.bounded && guard.buf.len() >= self.capacity {
            return Err(PushError::Full(item));
        }
        guard.buf.push_back(item);
        drop(guard);
        self.not_empty.notify_one();
        Ok(())
    }

    fn pop(&self) -> Option<T> {
        let mut guard = self.inner.lock().expect("mutex corrompido");
        while guard.buf.is_empty() && !guard.closed {
            guard = self.not_empty.wait(guard).expect("mutex corrompido");
        }
        match guard.buf.pop_front() {
            Some(item) => {
                drop(guard);
                if self.bounded {
                    self.not_full.notify_one();
                }
                Some(item)
            }
            None => None,
        }
    }

    fn try_pop(&self) -> Option<T> {
        let mut guard = self.inner.lock().expect("mutex corrompido");
        match guard.buf.pop_front() {
            Some(item) => {
                drop(guard);
                if self.bounded {
                    self.not_full.notify_one();
                }
                Some(item)
            }
            None => None,
        }
    }

    fn close(&self) {
        let mut guard = self.inner.lock().expect("mutex corrompido");
        guard.closed = true;
        drop(guard);
        self.not_empty.notify_all();
        self.not_full.notify_all();
    }

    fn is_closed(&self) -> bool {
        self.inner.lock().expect("mutex corrompido").closed
    }

    fn len(&self) -> usize {
        self.inner.lock().expect("mutex corrompido").buf.len()
    }

    fn capacity(&self) -> Option<usize> {
        if self.bounded {
            Some(self.capacity)
        } else {
            None
        }
    }

    fn name(&self) -> &'static str {
        "mutex"
    }
}
