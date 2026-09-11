use std::collections::HashSet;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use tcc_bench::message::Message;
use tcc_bench::queue::{PushError, Queue};
use tcc_bench::queues::MutexQueue;

#[test]
fn fifo_em_cenario_sequencial() {
    let q: MutexQueue<u32> = MutexQueue::with_capacity(16);
    for i in 0..10 {
        q.push(i).unwrap();
    }
    for i in 0..10 {
        assert_eq!(q.pop(), Some(i));
    }
}

#[test]
fn try_push_falha_quando_cheia() {
    let q: MutexQueue<u32> = MutexQueue::with_capacity(2);
    q.try_push(1).unwrap();
    q.try_push(2).unwrap();
    match q.try_push(3) {
        Err(PushError::Full(v)) => assert_eq!(v, 3),
        other => panic!("esperado Full, obtido {:?}", other),
    }
    assert_eq!(q.len(), 2);
}

#[test]
fn pop_retorna_none_apenas_quando_fechada_e_vazia() {
    let q: MutexQueue<u32> = MutexQueue::with_capacity(8);
    q.push(42).unwrap();
    q.close();
    assert_eq!(q.pop(), Some(42));
    assert_eq!(q.pop(), None);
    assert!(q.is_closed());
}

#[test]
fn push_apos_fechamento_devolve_o_item() {
    let q: MutexQueue<String> = MutexQueue::with_capacity(4);
    q.close();
    match q.push("x".to_string()) {
        Err(PushError::Closed(v)) => assert_eq!(v, "x"),
        other => panic!("esperado Closed, obtido {:?}", other),
    }
}

#[test]
fn close_desbloqueia_consumidor_em_espera() {
    let q: Arc<MutexQueue<u32>> = Arc::new(MutexQueue::with_capacity(4));
    let q2 = Arc::clone(&q);
    let h = thread::spawn(move || q2.pop());
    thread::sleep(Duration::from_millis(50));
    q.close();
    assert_eq!(h.join().unwrap(), None, "consumidor nao foi acordado");
}

#[test]
fn close_desbloqueia_produtor_em_fila_cheia() {
    let q: Arc<MutexQueue<u32>> = Arc::new(MutexQueue::with_capacity(1));
    q.push(1).unwrap();
    let q2 = Arc::clone(&q);
    let h = thread::spawn(move || q2.push(2));
    thread::sleep(Duration::from_millis(50));
    q.close();
    assert!(h.join().unwrap().is_err(), "produtor nao foi acordado");
}

#[test]
fn mpmc_sem_perda_e_sem_duplicacao() {
    const PRODUTORES: u16 = 4;
    const CONSUMIDORES: usize = 4;
    const POR_PRODUTOR: u64 = 20_000;

    let q: Arc<MutexQueue<Message>> = Arc::new(MutexQueue::with_capacity(256));

    let consumidores: Vec<_> = (0..CONSUMIDORES)
        .map(|_| {
            let q = Arc::clone(&q);
            thread::spawn(move || {
                let mut vistos = Vec::new();
                let mut ultima = vec![None::<u64>; PRODUTORES as usize];
                let mut fora_de_ordem = 0u64;
                while let Some(m) = q.pop() {
                    let pid = m.producer_id() as usize;
                    let seq = m.sequence();
                    if let Some(prev) = ultima[pid] {
                        if seq <= prev {
                            fora_de_ordem += 1;
                        }
                    }
                    ultima[pid] = Some(seq);
                    vistos.push(m.id);
                }
                (vistos, fora_de_ordem)
            })
        })
        .collect();

    let produtores: Vec<_> = (0..PRODUTORES)
        .map(|pid| {
            let q = Arc::clone(&q);
            thread::spawn(move || {
                for seq in 0..POR_PRODUTOR {
                    q.push(Message::new(pid, seq, vec![0xAB; 32])).unwrap();
                }
            })
        })
        .collect();

    for p in produtores {
        p.join().unwrap();
    }
    q.close();

    let mut todos = Vec::new();
    let mut fora_de_ordem_total = 0u64;
    for c in consumidores {
        let (vistos, ooo) = c.join().unwrap();
        todos.extend(vistos);
        fora_de_ordem_total += ooo;
    }

    let esperado = PRODUTORES as u64 * POR_PRODUTOR;
    assert_eq!(todos.len() as u64, esperado, "houve perda ou duplicacao");

    let unicos: HashSet<u64> = todos.iter().copied().collect();
    assert_eq!(unicos.len() as u64, esperado, "ids duplicados detectados");

    assert_eq!(
        fora_de_ordem_total, 0,
        "ordem FIFO por produtor violada dentro de um consumidor"
    );
}

#[test]
fn codificacao_binaria_e_reversivel() {
    let m = Message::new(7, 123_456, vec![1, 2, 3, 4, 5]);
    let bytes = m.encode();
    assert_eq!(bytes.len(), 16 + 5);
    let d = Message::decode(&bytes).expect("falha ao decodificar");
    assert_eq!(d, m);
    assert_eq!(d.producer_id(), 7);
    assert_eq!(d.sequence(), 123_456);
}
