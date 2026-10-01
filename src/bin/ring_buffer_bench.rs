//! Executa o benchmark do ring buffer inspirado no LMAX Disruptor (`RingBuffer`).
//!
//! Uso:
//!   cargo run --release --bin ring_buffer_bench -- [opcoes]   (--help lista as opcoes)
//!
//! O payload padrao e inline (`[u8; N]`, Secao 4.2); a capacidade deve ser potencia
//! de dois e todo o buffer (capacidade x tamanho da mensagem) e pre-alocado.

use tcc_bench::cli::{fail, run_benchmark, Args, PayloadKind};
use tcc_bench::queues::RingBuffer;
use tcc_bench::with_message_type;

fn main() {
    let args = Args::parse(PayloadKind::Inline);
    let capacity = match args.capacity {
        Some(c) if c >= 2 && c.is_power_of_two() => c,
        Some(c) => fail(&format!(
            "--capacity do ring buffer deve ser potencia de dois >= 2 (recebido {c})"
        )),
        None => fail("o ring buffer nao suporta capacidade ilimitada (--capacity 0)"),
    };
    with_message_type!(args, M => {
        let bytes = capacity * std::mem::size_of::<M>();
        println!("buffer pre-alocado: {:.1} MiB", bytes as f64 / (1024.0 * 1024.0));
        run_benchmark(
            "Ring buffer (inspirado no LMAX Disruptor)",
            &args,
            || RingBuffer::<M>::with_capacity(capacity),
        )
    });
}
