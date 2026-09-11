use std::fmt;

#[derive(Debug, PartialEq, Eq)]
pub enum PushError<T> {
    Closed(T),
    Full(T),
}

impl<T> PushError<T> {
    pub fn into_inner(self) -> T {
        match self {
            PushError::Closed(v) | PushError::Full(v) => v,
        }
    }
}

impl<T> fmt::Display for PushError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PushError::Closed(_) => write!(f, "fila fechada"),
            PushError::Full(_) => write!(f, "fila cheia"),
        }
    }
}

pub trait Queue<T>: Send + Sync {
    fn push(&self, item: T) -> Result<(), PushError<T>>;

    fn try_push(&self, item: T) -> Result<(), PushError<T>>;

    fn pop(&self) -> Option<T>;

    fn try_pop(&self) -> Option<T>;

    fn close(&self);

    fn is_closed(&self) -> bool;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn capacity(&self) -> Option<usize>;

    fn name(&self) -> &'static str;
}
