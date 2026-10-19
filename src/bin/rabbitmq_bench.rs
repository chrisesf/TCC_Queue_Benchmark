use std::time::Duration;

use tcc_bench::brokers::rabbitmq::{RabbitConfig, RabbitMq};
use tcc_bench::brokers::Semantics;
use tcc_bench::cli::{fail, run_transport_benchmark, Args, PayloadKind};

const EXTRA_USAGE: &str = "

Opcoes do RabbitMQ:
  --semantics S        at-most-once | at-least-once (padrao at-least-once)
  --amqp-url URL       padrao amqp://rabbitmq:5672/%2f
  --prefetch N         basic.qos por consumidor (padrao 1000)
  --confirm-window N   publisher confirms pendentes por produtor (padrao 1000)
  --idle-timeout-ms N  tempo sem mensagens para dar como perdidas as que faltam, no
                       warm-up e apos a producao (padrao 5000)
  --broker-container C container medido via API do Docker (padrao tcc-rabbitmq; none desativa)

A mensagem trafega serializada (id + timestamp + payload), por isso o payload e sempre heap.";

const EXTRA_FLAGS: [&str; 6] = [
    "--semantics",
    "--amqp-url",
    "--prefetch",
    "--confirm-window",
    "--idle-timeout-ms",
    "--broker-container",
];

fn main() {
    let mut args = Args::parse_with(PayloadKind::Heap, &EXTRA_FLAGS, EXTRA_USAGE);
    if args.payload_kind != PayloadKind::Heap {
        fail("brokers usam apenas --payload-kind heap");
    }
    // Capacidade nao se aplica: o limite e a memoria/disco do broker.
    args.capacity = None;

    let semantics = Semantics::parse(args.extra("--semantics").unwrap_or("at-least-once"))
        .unwrap_or_else(|| fail("--semantics invalido"));
    let prefetch = args.extra_num("--prefetch", 1000);
    if prefetch == 0 || prefetch > u16::MAX as u64 {
        fail("--prefetch deve estar entre 1 e 65535");
    }
    let cfg = RabbitConfig {
        url: args
            .extra("--amqp-url")
            .unwrap_or("amqp://rabbitmq:5672/%2f")
            .to_owned(),
        semantics,
        prefetch: prefetch as u16,
        confirm_window: args.extra_num("--confirm-window", 1000) as usize,
        container: match args.extra("--broker-container").unwrap_or("tcc-rabbitmq") {
            "none" => None,
            c => Some(c.to_owned()),
        },
    };
    args.idle_timeout = Duration::from_millis(args.extra_num("--idle-timeout-ms", 5000));

    // Valida conexao e semantica antes de iniciar as repeticoes.
    drop(RabbitMq::new(cfg.clone()).unwrap_or_else(|e| fail(&e)));

    let title = format!("RabbitMQ ({semantics})");
    run_transport_benchmark::<_, tcc_bench::Message, _>(&title, &args, || {
        RabbitMq::new(cfg.clone()).unwrap_or_else(|e| panic!("{e}"))
    });
}
