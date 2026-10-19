//! Compara execucoes independentes entre mecanismos (Secao 4.6).
//!
//! Le um ou mais CSV gerados pela CLI e, em cada cenario igual (topologia, threads,
//! payload, chegada e volume), testa a diferenca de vazao, p50 e p99. O teste e o t de
//! Welch quando as duas amostras parecem normais e Mann-Whitney caso contrario.

use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::process::exit;

use tcc_bench::stats::{compare_samples, fmt_ns, RunAggregate};

struct Row {
    mechanism: String,
    throughput: f64,
    p50: f64,
    p99: f64,
    errors: f64,
    sent: f64,
}

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.is_empty() || args.iter().any(|a| a == "--help" || a == "-h") {
        eprintln!("uso: compare_runs CSV...");
        exit(if args.is_empty() { 2 } else { 0 });
    }

    let mut groups: BTreeMap<String, Vec<Row>> = BTreeMap::new();
    for path in &args {
        let text = fs::read_to_string(path).unwrap_or_else(|e| {
            eprintln!("nao foi possivel ler {path}: {e}");
            exit(1);
        });
        merge_csv(&text, &mut groups);
    }
    if groups.is_empty() {
        eprintln!("nenhuma execucao nos CSV");
        exit(1);
    }

    let mut out = String::new();
    for (scenario, rows) in &groups {
        let mut by_mechanism: BTreeMap<&str, Vec<&Row>> = BTreeMap::new();
        for row in rows {
            by_mechanism.entry(&row.mechanism).or_default().push(row);
        }
        out.push_str(&format!("\n== {scenario} ==\n"));
        let mut ranking: Vec<(&str, RunAggregate, RunAggregate, RunAggregate, f64, f64)> = by_mechanism
            .iter()
            .map(|(name, samples)| {
                let thr = RunAggregate::from(&samples.iter().map(|r| r.throughput).collect::<Vec<_>>());
                let p50 = RunAggregate::from(&samples.iter().map(|r| r.p50).collect::<Vec<_>>());
                let p99 = RunAggregate::from(&samples.iter().map(|r| r.p99).collect::<Vec<_>>());
                let sent: f64 = samples.iter().map(|r| r.sent).sum();
                let errors: f64 = samples.iter().map(|r| r.errors).sum();
                (*name, thr, p50, p99, errors, sent)
            })
            .collect();
        ranking.sort_by(|a, b| b.1.mean.partial_cmp(&a.1.mean).unwrap_or(std::cmp::Ordering::Equal));
        for (name, thr, p50, p99, errors, sent) in &ranking {
            out.push_str(&format!(
                "{name:32} n={:<3} vazao {:>10.0} +/- {:>8.0}   p50 {}   p99 {}   erros {:.3e}\n",
                thr.n,
                thr.mean,
                thr.ci95_half_width,
                fmt_ns(p50.mean),
                fmt_ns(p99.mean),
                if *sent > 0.0 { errors / sent } else { 0.0 }
            ));
        }
        let names: Vec<&str> = ranking.iter().map(|r| r.0).collect();
        for i in 0..names.len() {
            for j in (i + 1)..names.len() {
                let metrics: [(&str, fn(&Row) -> f64); 3] = [
                    ("vazao", |r| r.throughput),
                    ("p50", |r| r.p50),
                    ("p99", |r| r.p99),
                ];
                for (label, pick) in metrics {
                    let a: Vec<f64> = by_mechanism[names[i]].iter().map(|r| pick(r)).collect();
                    let b: Vec<f64> = by_mechanism[names[j]].iter().map(|r| pick(r)).collect();
                    let Some(test) = compare_samples(&a, &b) else {
                        continue;
                    };
                    if !test.significant_95 {
                        continue;
                    }
                    let (higher, lower) = if test.statistic > 0.0 {
                        (names[i], names[j])
                    } else {
                        (names[j], names[i])
                    };
                    out.push_str(&format!(
                        "  {label}: {higher} > {lower} ({}, significativo a 95%)\n",
                        test.name
                    ));
                }
            }
        }
    }
    print!("{out}");
}

fn merge_csv(text: &str, groups: &mut BTreeMap<String, Vec<Row>>) {
    let mut lines = text.lines().filter(|l| !l.is_empty());
    let Some(header) = lines.next() else {
        return;
    };
    let columns: Vec<&str> = header.split(',').collect();
    let index = |name: &str| {
        columns.iter().position(|c| *c == name).unwrap_or_else(|| {
            eprintln!("coluna ausente: {name}");
            exit(1);
        })
    };
    let queue = index("queue");
    let semantics = index("semantics");
    let kind = index("payload_kind");
    let topology = index("topology");
    let producers = index("producers");
    let consumers = index("consumers");
    let payload = index("payload_bytes");
    let arrival = index("arrival");
    let messages = index("messages_per_producer");
    let throughput = index("throughput_msg_s");
    let p50 = index("lat_p50_ns");
    let p99 = index("lat_p99_ns");
    let sent = index("sent");
    let lost = index("lost");
    let duplicated = index("duplicated");
    let out_of_order = index("out_of_order");
    let corrupted = index("corrupted");

    for line in lines {
        let fields: Vec<&str> = line.split(',').collect();
        let num = |i: usize| fields[i].parse::<f64>().unwrap_or(0.0);
        let mechanism = if fields[semantics] == "n/a" {
            format!("{} / {}", fields[queue], fields[kind])
        } else {
            format!("{} / {}", fields[queue], fields[semantics])
        };
        let scenario = format!(
            "{} {}x{} payload={}B {} msgs/produtor={}",
            fields[topology], fields[producers], fields[consumers], fields[payload], fields[arrival], fields[messages]
        );
        groups.entry(scenario).or_default().push(Row {
            mechanism,
            throughput: num(throughput),
            p50: num(p50),
            p99: num(p99),
            sent: num(sent),
            errors: num(lost) + num(duplicated) + num(out_of_order) + num(corrupted),
        });
    }
}
