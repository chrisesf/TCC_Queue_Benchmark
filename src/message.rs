use crate::clock::now_ns;

const PRODUCER_BITS: u32 = 16;
const SEQ_BITS: u32 = 64 - PRODUCER_BITS;
const SEQ_MASK: u64 = (1u64 << SEQ_BITS) - 1;

pub const MAX_PRODUCERS: u64 = 1 << PRODUCER_BITS;
pub const MAX_SEQ: u64 = SEQ_MASK;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub id: u64,
    pub timestamp_ns: u64,
    pub payload: Vec<u8>,
}

impl Message {
    #[inline]
    pub fn new(producer_id: u16, sequence: u64, payload: Vec<u8>) -> Self {
        debug_assert!(sequence <= MAX_SEQ, "sequencia excede 48 bits");
        Self {
            id: pack_id(producer_id, sequence),
            timestamp_ns: now_ns(),
            payload,
        }
    }

    #[inline]
    pub fn with_timestamp(producer_id: u16, sequence: u64, timestamp_ns: u64, payload: Vec<u8>) -> Self {
        Self {
            id: pack_id(producer_id, sequence),
            timestamp_ns,
            payload,
        }
    }

    #[inline]
    pub fn producer_id(&self) -> u16 {
        (self.id >> SEQ_BITS) as u16
    }

    #[inline]
    pub fn sequence(&self) -> u64 {
        self.id & SEQ_MASK
    }

    #[inline]
    pub fn latency_ns(&self) -> Option<u64> {
        now_ns().checked_sub(self.timestamp_ns)
    }

    pub fn wire_len(&self) -> usize {
        16 + self.payload.len()
    }

    pub fn encode_into(&self, buf: &mut Vec<u8>) {
        buf.clear();
        buf.reserve(self.wire_len());
        buf.extend_from_slice(&self.id.to_le_bytes());
        buf.extend_from_slice(&self.timestamp_ns.to_le_bytes());
        buf.extend_from_slice(&self.payload);
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.wire_len());
        self.encode_into(&mut buf);
        buf
    }

    /// Desserializa a partir do layout binario fixo.
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 16 {
            return None;
        }
        let id = u64::from_le_bytes(bytes[0..8].try_into().ok()?);
        let timestamp_ns = u64::from_le_bytes(bytes[8..16].try_into().ok()?);
        Some(Self {
            id,
            timestamp_ns,
            payload: bytes[16..].to_vec(),
        })
    }
}

#[inline]
pub fn pack_id(producer_id: u16, sequence: u64) -> u64 {
    ((producer_id as u64) << SEQ_BITS) | (sequence & SEQ_MASK)
}

pub struct PayloadFactory {
    template: Vec<u8>,
}

impl PayloadFactory {
    pub fn new(size_bytes: usize, seed: u64) -> Self {
        let mut rng = XorShift64::new(seed);
        let mut template = Vec::with_capacity(size_bytes);
        while template.len() < size_bytes {
            let word = rng.next_u64().to_le_bytes();
            let take = (size_bytes - template.len()).min(8);
            template.extend_from_slice(&word[..take]);
        }
        for b in template.iter_mut() {
            if *b == 0 {
                *b = 0x5A;
            }
        }
        Self { template }
    }

    #[inline]
    pub fn make(&self) -> Vec<u8> {
        self.template.clone()
    }

    pub fn size(&self) -> usize {
        self.template.len()
    }
}

pub struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    pub fn new(seed: u64) -> Self {
        Self {
            state: if seed == 0 { 0x2545F491_4F6CDD1D } else { seed },
        }
    }

    #[inline]
    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x.wrapping_mul(0x2545F491_4F6CDD1D)
    }
}
