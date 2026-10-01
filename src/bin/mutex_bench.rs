//! Executa o benchmark da fila lock-based (`MutexQueue`).
//!
//! Uso:
//!   cargo run --release --bin mutex_bench -- [opcoes]   (--help lista as opcoes)

use tcc_bench::cli::{run_benchmark, Args, PayloadKind};
use tcc_bench::queues::MutexQueue;
use tcc_bench::with_message_type;

fn main() {
    let args = Args::parse(PayloadKind::Heap);
    with_message_type!(args, M => run_benchmark(
        "Fila lock-based (Mutex)",
        &args,
        || match args.capacity {
            Some(c) => MutexQueue::<M>::with_capacity(c),
            None => MutexQueue::<M>::unbounded(),
        },
    ));
}
