use base64::{engine::general_purpose as b64, Engine};
use indexmap::IndexMap;

use crate::{CursorValue, ShortByteString, SplitCommitLockValue, VarUInt};

pub trait Encode {
    fn encode(&self) -> Vec<u8>;

    fn encode_base64(&self) -> String {
        b64::URL_SAFE_NO_PAD.encode(self.encode())
    }
}

macro_rules! impl_uint_encode {
    ($type:ty) => {
        impl Encode for $type {
            fn encode(&self) -> Vec<u8> {
                Into::<VarUInt>::into(*self).encode()
            }
        }
    };
}

// no need to use for u8
impl_uint_encode!(usize);
impl_uint_encode!(u16);
impl_uint_encode!(u32);
impl_uint_encode!(u64);
impl_uint_encode!(u128);

impl Encode for bool {
    fn encode(&self) -> Vec<u8> {
        vec![*self as u8]
    }
}

impl<const N: usize> Encode for [u8; N] {
    fn encode(&self) -> Vec<u8> {
        self.to_vec()
    }
}

impl Encode for ShortByteString {
    fn encode(&self) -> Vec<u8> {
        [(self.0.len() as u8).encode(), self.0.clone()].concat()
    }
}

impl Encode for u8 {
    fn encode(&self) -> Vec<u8> {
        vec![*self]
    }
}

impl Encode for VarUInt {
    fn encode(&self) -> Vec<u8> {
        let bend = self.0.to_be_bytes();

        for idx in 0..16 {
            if bend[idx] != 0x00 {
                let size = 16 - idx;
                let mut out = Vec::with_capacity(1 + size);

                out.push(size.try_into().unwrap());
                out.extend_from_slice(&bend[idx..]);

                return out;
            }
        }

        vec![0]
    }
}

impl Encode for CursorValue {
    fn encode(&self) -> Vec<u8> {
        [
            self.height.encode(),
            self.hash.encode(),
            self.was_mempool.encode(),
            self.timestamp.encode(),
            self.mempool_info.encode(),
        ]
        .concat()
    }
}

impl Encode for SplitCommitLockValue {
    fn encode(&self) -> Vec<u8> {
        [
            self.safe_point.encode(),
            self.total_actions.encode(),
            self.mutable.encode(),
        ]
        .concat()
    }
}

impl<A: Encode> Encode for Vec<A> {
    fn encode(&self) -> Vec<u8> {
        [
            VarUInt::from(self.len()).encode(),
            self.iter().flat_map(|t| t.encode()).collect(),
        ]
        .concat()
    }
}

impl<A: Encode, B: Encode> Encode for (A, B) {
    fn encode(&self) -> Vec<u8> {
        [self.0.encode(), self.1.encode()].concat()
    }
}

impl<A: Encode, B: Encode, C: Encode> Encode for (A, B, C) {
    fn encode(&self) -> Vec<u8> {
        [self.0.encode(), self.1.encode(), self.2.encode()].concat()
    }
}

impl<T: Encode> Encode for &T {
    fn encode(&self) -> Vec<u8> {
        (*self).encode()
    }
}

impl<T: Encode> Encode for Option<T> {
    fn encode(&self) -> Vec<u8> {
        [
            self.is_some().encode(),
            self.as_ref().map(|t| t.encode()).unwrap_or_default(),
        ]
        .concat()
    }
}

impl<K: Encode, V: Encode> Encode for IndexMap<K, V> {
    fn encode(&self) -> Vec<u8> {
        [
            VarUInt::from(self.len()).encode(),
            self.iter()
                .flat_map(|(k, v)| [k.encode(), v.encode()].concat())
                .collect(),
        ]
        .concat()
    }
}

impl Encode for String {
    fn encode(&self) -> Vec<u8> {
        [self.len().encode(), self.as_bytes().to_vec()].concat()
    }
}
