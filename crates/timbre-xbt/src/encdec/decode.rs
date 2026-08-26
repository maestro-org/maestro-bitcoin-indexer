use std::hash::Hash;

use base64::{engine::general_purpose as b64, Engine};
use indexmap::IndexMap;

use crate::{
    reducers::{BlockHash, Height},
    CursorValue, Encode, ShortByteString, SplitCommitLockValue, TimbreError, VarUInt,
};

pub trait Decode
where
    Self: Sized,
{
    fn decode(bytes: &[u8]) -> Result<(Self, &[u8]), TimbreError>;

    fn decode_base64(s: &str) -> Result<(Self, String), TimbreError> {
        let binding = b64::URL_SAFE_NO_PAD.decode(s)?;
        let (decoded, remaining) = Self::decode(&binding)?;
        let remaining_str = std::str::from_utf8(remaining)?;

        Ok((decoded, remaining_str.to_string()))
    }
}

macro_rules! impl_uint_decode {
    ($t:ty) => {
        impl Decode for $t {
            fn decode(bytes: &[u8]) -> Result<(Self, &[u8]), TimbreError> {
                let (varuint, rem) = VarUInt::decode(bytes)?;

                let casted = Self::try_from(varuint)?;

                Ok((casted, rem))
            }
        }
    };
}

impl_uint_decode!(usize);
impl_uint_decode!(u16);
impl_uint_decode!(u32);
impl_uint_decode!(u64);
impl_uint_decode!(u128);

impl Decode for bool {
    fn decode(bytes: &[u8]) -> Result<(Self, &[u8]), TimbreError> {
        let (byte, rem) = u8::decode(bytes)?;

        match byte {
            0 => Ok((false, rem)),
            1 => Ok((true, rem)),
            x => Err(TimbreError::MalformedInput(format!(
                "bool byte not 0 or 1: {x}"
            ))),
        }
    }
}

impl Decode for u8 {
    fn decode(bytes: &[u8]) -> Result<(Self, &[u8]), TimbreError> {
        bytes
            .first()
            .map(|b| (*b, &bytes[1..]))
            .ok_or(TimbreError::MalformedInput(
                "Insufficient bytes for u8 decoding".to_string(),
            ))
    }
}

impl<const N: usize> Decode for [u8; N] {
    fn decode(bytes: &[u8]) -> Result<(Self, &[u8]), TimbreError> {
        bytes
            .get(..N)
            .map(|slice| {
                (
                    slice.try_into().expect("slice with incorrect length"),
                    &bytes[N..],
                )
            })
            .ok_or(TimbreError::MalformedInput(
                "Insufficient bytes for array decoding".to_string(),
            ))
    }
}

impl<A: Decode, B: Decode> Decode for (A, B) {
    fn decode(bytes: &[u8]) -> Result<(Self, &[u8]), TimbreError> {
        let (first, bytes) = A::decode(bytes)?;
        let (second, bytes) = B::decode(bytes)?;

        Ok(((first, second), bytes))
    }
}

impl<A: Decode, B: Decode, C: Decode> Decode for (A, B, C) {
    fn decode(bytes: &[u8]) -> Result<(Self, &[u8]), TimbreError> {
        let (first, bytes) = A::decode(bytes)?;
        let (second, bytes) = B::decode(bytes)?;
        let (third, bytes) = C::decode(bytes)?;

        Ok(((first, second, third), bytes))
    }
}

impl Decode for ShortByteString {
    fn decode(bytes: &[u8]) -> Result<(Self, &[u8]), TimbreError> {
        let len = bytes[0] as usize;
        let (data, bytes) = bytes[1..].split_at(len);

        Ok((ShortByteString(data.to_vec()), bytes))
    }
}

impl Decode for VarUInt {
    fn decode(bytes: &[u8]) -> Result<(Self, &[u8]), TimbreError> {
        let (&len_byte, rest) = bytes.split_first().ok_or(TimbreError::MalformedInput(
            "Empty input for VarUInt decode".into(),
        ))?;
        let len = len_byte as usize;
        // A VarUInt holds at most a u128 (16 bytes); a larger length prefix is
        // malformed and would underflow the `16 - len` pad below.
        if len > 16 {
            return Err(TimbreError::MalformedInput(
                "VarUInt length exceeds 16 bytes".into(),
            ));
        }
        let (data, bytes) = rest
            .split_at_checked(len)
            .ok_or(TimbreError::MalformedInput(
                "Insufficient bytes for VarUInt decode".into(),
            ))?;

        let be_128: [u8; 16] = [vec![0; 16 - len], data.to_vec()]
            .concat()
            .try_into()
            .unwrap();

        Ok((VarUInt(u128::from_be_bytes(be_128)), bytes))
    }
}

impl Decode for CursorValue {
    fn decode(bytes: &[u8]) -> Result<(Self, &[u8]), TimbreError> {
        let (height, bytes) = Height::decode(bytes)?;
        let (hash, bytes) = BlockHash::decode(bytes)?;
        let (was_mempool, bytes) = bool::decode(bytes)?;
        let (timestamp, bytes) = u64::decode(bytes)?;
        let (mempool_info, bytes) = <Option<((Height, BlockHash), u64)>>::decode(bytes)?;

        let out = CursorValue {
            height,
            hash,
            was_mempool,
            timestamp,
            mempool_info,
        };

        Ok((out, bytes))
    }
}

impl Decode for SplitCommitLockValue {
    fn decode(bytes: &[u8]) -> Result<(Self, &[u8]), TimbreError> {
        let (safe_point, bytes) = <Option<(Height, BlockHash)>>::decode(bytes)?;
        let (total_actions, bytes) = u64::decode(bytes)?;
        let (mutable, bytes) = bool::decode(bytes)?;

        let out = SplitCommitLockValue {
            safe_point,
            total_actions,
            mutable,
        };

        Ok((out, bytes))
    }
}

impl<T: Decode> Decode for Option<T> {
    fn decode(bytes: &[u8]) -> Result<(Self, &[u8]), TimbreError> {
        let (presence, bytes) = bool::decode(bytes)?;

        if !presence {
            return Ok((None, bytes));
        }

        let (value, bytes) = T::decode(bytes)?;
        Ok((Some(value), bytes))
    }
}

impl<A: Decode + Encode> Decode for Vec<A> {
    fn decode(bytes: &[u8]) -> Result<(Self, &[u8]), TimbreError> {
        let (len, mut bytes) = VarUInt::decode(bytes)?;
        let len = len.try_into()?;
        let mut vec = Vec::with_capacity(len);

        for _ in 0..len {
            let (item, rest) = A::decode(bytes)?;
            bytes = rest;
            vec.push(item);
        }

        Ok((vec.into(), bytes))
    }
}

impl<K, V> Decode for IndexMap<K, V>
where
    K: Decode + Encode + Eq + Hash,
    V: Decode + Encode + Eq + Hash,
{
    fn decode(bytes: &[u8]) -> Result<(Self, &[u8]), TimbreError> {
        let mut map = IndexMap::new();

        let (len, mut bytes) = VarUInt::decode(bytes)?;
        let len = len.inner();

        for _ in 0..len {
            let (key, rest) = K::decode(bytes)?;
            bytes = rest;
            let (value, rest) = V::decode(bytes)?;
            bytes = rest;
            map.insert(key, value);
        }

        Ok((map, bytes))
    }
}

impl Decode for String {
    fn decode(bytes: &[u8]) -> Result<(Self, &[u8]), TimbreError> {
        let (len, bytes) = u32::decode(&bytes[..4])?;
        let len = len as usize;
        let (str_bytes, bytes) = bytes.split_at(len);

        Ok((String::from_utf8(str_bytes.to_vec())?, bytes))
    }
}

#[cfg(test)]
mod hardening_tests {
    use crate::{Decode, VarUInt};

    // A length prefix greater than 16 previously underflowed `16 - len` and
    // requested a near-usize::MAX allocation (process abort). It must now be a
    // clean decode error.
    #[test]
    fn varuint_rejects_oversized_length_prefix() {
        let malformed = [0xffu8; 20]; // first byte 0xff = length 255
        assert!(VarUInt::decode(&malformed).is_err());
    }

    #[test]
    fn varuint_rejects_empty_input() {
        assert!(VarUInt::decode(&[]).is_err());
    }

    #[test]
    fn varuint_roundtrips_valid_value() {
        let (v, rest) = VarUInt::decode(&[0x02, 0x01, 0x00]).expect("valid varuint");
        assert_eq!(v.inner(), 256);
        assert!(rest.is_empty());
    }
}
