//! Executa o benchmark da fila lock-free de Michael-Scott (`MichaelScottQueue`).
//!
//! Uso:
//!   cargo run --release --bin michael_scott_bench -- [opcoes]   (--help lista as opcoes)
//!
//! Com `--capacity 0` roda o algoritmo original, sem limite; com capacidade > 0 um
//! contador atomico de ocupacao aplica backpressure.

use tcc_bench::cli::{run_benchmark, Args, PayloadKind};
use tcc_bench::queues::MichaelScottQueue;
use tcc_bench::with_message_type;

fn main() {
    let args = Args::parse(PayloadKind::Heap);
    with_message_type!(args, M => run_benchmark(
        "Fila lock-free (Michael-Scott)",
        &args,
        || match args.capacity {
            Some(c) => MichaelScottQueue::<M>::with_capacity(c),
            None => MichaelScottQueue::<M>::unbounded(),
        },
    ));
}
