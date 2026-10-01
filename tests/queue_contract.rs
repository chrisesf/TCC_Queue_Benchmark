//! Contrato `Queue<T>` verificado igualmente para todas as filas.

use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use tcc_bench::message::Message;
use tcc_bench::queue::{PushError, Queue};
use tcc_bench::queues::{MichaelScottQueue, MutexQueue, RingBuffer};

pub fn fifo_em_cenario_sequencial<Q: Queue<u32>>(q: Q) {
    for i in 0..10 {
        q.push(i).unwrap();
    }
    assert_eq!(q.len(), 10);
    for i in 0..10 {
        assert_eq!(q.pop(), Some(i));
    }
    assert!(q.is_empty());
}

pub fn try_push_falha_quando_cheia<Q: Queue<u32>>(q: Q) {
    assert_eq!(q.capacity(), Some(2));
    q.try_push(1).unwrap();
    q.try_push(2).unwrap();
    match q.try_push(3) {
        Err(PushError::Full(v)) => assert_eq!(v, 3),
        other => panic!("esperado Full, obtido {:?}", other),
    }
    assert_eq!(q.len(), 2);
    assert_eq!(q.try_pop(), Some(1));
    q.try_push(3).unwrap();
}

pub fn try_pop_em_fila_vazia_nao_bloqueia<Q: Queue<u32>>(q: Q) {
    assert_eq!(q.try_pop(), None);
    assert!(!q.is_closed());
}

pub fn pop_retorna_none_apenas_quando_fechada_e_vazia<Q: Queue<u32>>(q: Q) {
    q.push(42).unwrap();
    q.close();
    assert!(q.is_closed());
    assert_eq!(q.pop(), Some(42));
    assert_eq!(q.pop(), None);
}

pub fn push_apos_fechamento_devolve_o_item<Q: Queue<String>>(q: Q) {
    q.close();
    match q.push("x".to_string()) {
        Err(PushError::Closed(v)) => assert_eq!(v, "x"),
        other => panic!("esperado Closed, obtido {:?}", other),
    }
    match q.try_push("y".to_string()) {
        Err(PushError::Closed(v)) => assert_eq!(v, "y"),
        other => panic!("esperado Closed, obtido {:?}", other),
    }
}

pub fn close_desbloqueia_consumidor_em_espera<Q: Queue<u32> + 'static>(q: Q) {
    let q = Arc::new(q);
    let q2 = Arc::clone(&q);
    let h = thread::spawn(move || q2.pop());
    thread::sleep(Duration::from_millis(50));
    q.close();
    assert_eq!(h.join().unwrap(), None, "consumidor nao foi acordado");
}

pub fn close_desbloqueia_produtor_em_fila_cheia<Q: Queue<u32> + 'static>(q: Q) {
    let q = Arc::new(q);
    while q.try_push(1).is_ok() {}
    let q2 = Arc::clone(&q);
    let h = thread::spawn(move || q2.push(2));
    thread::sleep(Duration::from_millis(50));
    q.close();
    assert!(h.join().unwrap().is_err(), "produtor nao foi acordado");
}

pub fn mpmc_sem_perda_e_sem_duplicacao<Q: Queue<Message> + 'static>(q: Q, por_produtor: u64) {
    const PRODUTORES: u16 = 4;
    const CONSUMIDORES: usize = 4;

    let q = Arc::new(q);

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
                for seq in 0..por_produtor {
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

    let esperado = PRODUTORES as u64 * por_produtor;
    assert_eq!(todos.len() as u64, esperado, "houve perda ou duplicacao");

    let unicos: HashSet<u64> = todos.iter().copied().collect();
    assert_eq!(unicos.len() as u64, esperado, "ids duplicados detectados");

    assert_eq!(
        fora_de_ordem_total, 0,
        "ordem FIFO por produtor violada dentro de um consumidor"
    );
}

/// Fechamento concorrente com produtores ativos: todo item aceito (`Ok`) tem de ser
/// entregue, e nada recusado pode aparecer.
pub fn close_concorrente_nao_perde_itens_aceitos<Q: Queue<u64> + 'static>(q: Q) {
    let q = Arc::new(q);
    let consumidores: Vec<_> = (0..2)
        .map(|_| {
            let q = Arc::clone(&q);
            thread::spawn(move || {
                let mut n = 0u64;
                while q.pop().is_some() {
                    n += 1;
                }
                n
            })
        })
        .collect();
    let produtores: Vec<_> = (0..4)
        .map(|_| {
            let q = Arc::clone(&q);
            thread::spawn(move || {
                let mut aceitos = 0u64;
                for i in 0.. {
                    if q.push(i).is_err() {
                        break;
                    }
                    aceitos += 1;
                }
                aceitos
            })
        })
        .collect();
    thread::sleep(Duration::from_millis(30));
    q.close();
    let aceitos: u64 = produtores.into_iter().map(|h| h.join().unwrap()).sum();
    let entregues: u64 = consumidores.into_iter().map(|h| h.join().unwrap()).sum();
    assert!(aceitos > 0);
    assert_eq!(aceitos, entregues, "itens aceitos apos o fechamento foram perdidos");
}

#[derive(Debug)]
pub struct ContaDrop(pub Arc<AtomicUsize>);

impl Drop for ContaDrop {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// Itens restantes na fila sao dropados exatamente uma vez quando ela e destruida.
pub fn drop_libera_itens_restantes<Q: Queue<ContaDrop>>(q: Q) {
    let drops = Arc::new(AtomicUsize::new(0));
    for _ in 0..5 {
        q.push(ContaDrop(Arc::clone(&drops))).unwrap();
    }
    drop(q.pop().unwrap());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    drop(q);
    assert_eq!(drops.load(Ordering::SeqCst), 5);
}

macro_rules! contrato {
    ($modulo:ident, $limitada:expr) => {
        mod $modulo {
            use super::*;

            #[test]
            fn fifo_em_cenario_sequencial() {
                super::fifo_em_cenario_sequencial($limitada(16));
            }

            #[test]
            fn try_push_falha_quando_cheia() {
                super::try_push_falha_quando_cheia($limitada(2));
            }

            #[test]
            fn try_pop_em_fila_vazia_nao_bloqueia() {
                super::try_pop_em_fila_vazia_nao_bloqueia($limitada(4));
            }

            #[test]
            fn pop_retorna_none_apenas_quando_fechada_e_vazia() {
                super::pop_retorna_none_apenas_quando_fechada_e_vazia($limitada(8));
            }

            #[test]
            fn push_apos_fechamento_devolve_o_item() {
                super::push_apos_fechamento_devolve_o_item($limitada(4));
            }

            #[test]
            fn close_desbloqueia_consumidor_em_espera() {
                super::close_desbloqueia_consumidor_em_espera($limitada(4));
            }

            #[test]
            fn close_desbloqueia_produtor_em_fila_cheia() {
                super::close_desbloqueia_produtor_em_fila_cheia($limitada(2));
            }

            #[test]
            fn mpmc_sem_perda_e_sem_duplicacao() {
                super::mpmc_sem_perda_e_sem_duplicacao($limitada(256), 20_000);
            }

            #[test]
            fn mpmc_com_capacidade_minima() {
                super::mpmc_sem_perda_e_sem_duplicacao($limitada(2), 5_000);
            }

            #[test]
            fn close_concorrente_nao_perde_itens_aceitos() {
                super::close_concorrente_nao_perde_itens_aceitos($limitada(64));
            }

            #[test]
            fn drop_libera_itens_restantes() {
                super::drop_libera_itens_restantes($limitada(8));
            }
        }
    };
}

contrato!(mutex, MutexQueue::with_capacity);
contrato!(michael_scott, MichaelScottQueue::with_capacity);
contrato!(ring_buffer, RingBuffer::with_capacity);

mod michael_scott_ilimitada {
    use super::*;

    #[test]
    fn fifo_em_cenario_sequencial() {
        super::fifo_em_cenario_sequencial(MichaelScottQueue::unbounded());
    }

    #[test]
    fn capacidade_e_none() {
        assert_eq!(Queue::<u32>::capacity(&MichaelScottQueue::<u32>::unbounded()), None);
    }

    #[test]
    fn mpmc_sem_perda_e_sem_duplicacao() {
        super::mpmc_sem_perda_e_sem_duplicacao(MichaelScottQueue::unbounded(), 50_000);
    }

    #[test]
    fn close_concorrente_nao_perde_itens_aceitos() {
        super::close_concorrente_nao_perde_itens_aceitos(MichaelScottQueue::unbounded());
    }

    #[test]
    fn drop_libera_itens_restantes() {
        super::drop_libera_itens_restantes(MichaelScottQueue::unbounded());
    }
}

mod ring_buffer_especifico {
    use super::*;

    #[test]
    #[should_panic(expected = "potencia de dois")]
    fn capacidade_precisa_ser_potencia_de_dois() {
        let _ = RingBuffer::<u32>::with_capacity(6);
    }

    #[test]
    #[should_panic(expected = ">= 2")]
    fn capacidade_minima_e_dois() {
        let _ = RingBuffer::<u32>::with_capacity(1);
    }

    /// Operacoes pseudoaleatorias comparadas com `VecDeque`, dando milhares de voltas
    /// num buffer pequeno.
    #[test]
    fn equivale_a_vecdeque_em_muitas_voltas() {
        use std::collections::VecDeque;
        use tcc_bench::message::XorShift64;

        let q: RingBuffer<u64> = RingBuffer::with_capacity(4);
        let mut modelo = VecDeque::new();
        let mut rng = XorShift64::new(42);
        let mut proximo = 0u64;
        for _ in 0..100_000 {
            if rng.next_u64().is_multiple_of(2) {
                match q.try_push(proximo) {
                    Ok(()) => modelo.push_back(proximo),
                    Err(PushError::Full(_)) => assert_eq!(modelo.len(), 4),
                    Err(e) => panic!("erro inesperado: {e}"),
                }
                proximo += 1;
            } else {
                assert_eq!(q.try_pop(), modelo.pop_front());
            }
            assert_eq!(q.len(), modelo.len());
        }
    }
}
