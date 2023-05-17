use bincode::{Decode, Encode};
use bytes::BytesMut;

#[derive(Debug, Encode, Decode)]
pub struct Entry {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub timestamp: u128,
    pub deleted: bool,
}

impl Entry {
    pub fn serialize(&self) -> BytesMut {
        let mut s = BytesMut::new();
        s.extend_from_slice(&bincode::encode_to_vec(self, bincode::config::standard()).unwrap());
        s
    }

    pub fn deserialize(data: BytesMut) -> Self {
        let d: Self = bincode::decode_from_slice(&data, bincode::config::standard())
            .unwrap()
            .0;
        d
    }
}
