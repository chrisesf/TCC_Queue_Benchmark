use tcc_bench::message::{payload_checksum, BenchMessage, FixedMessage, Message, PayloadFactory};

#[test]
fn checksum_cobre_todos_os_bytes() {
    let base = PayloadFactory::new(1027, 9).template().to_vec();
    let ref_sum = payload_checksum(&base);
    for i in [0, 7, 8, 512, 1023, 1024, 1026] {
        let mut alterado = base.clone();
        alterado[i] ^= 0x01;
        assert_ne!(payload_checksum(&alterado), ref_sum, "byte {i} nao afeta o checksum");
    }
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

#[test]
fn payload_sem_bytes_zero() {
    let f = PayloadFactory::new(1024, 1);
    assert_eq!(f.size(), 1024);
    assert!(f.template().iter().all(|&b| b != 0));
}

#[test]
fn mensagem_inline_e_heap_tem_mesmo_id_e_payload() {
    let f = PayloadFactory::new(64, 3);
    let mut heap = <Message as BenchMessage>::build(5, 99, f.template());
    let mut inline = FixedMessage::<64>::build(5, 99, f.template());
    heap.set_timestamp(10);
    inline.set_timestamp(10);
    assert_eq!(BenchMessage::id(&heap), inline.id);
    assert_eq!(inline.producer_id(), 5);
    assert_eq!(inline.sequence(), 99);
    assert_eq!(&heap.payload[..], &inline.payload[..]);
    assert_eq!(inline.payload_len(), 64);
}
