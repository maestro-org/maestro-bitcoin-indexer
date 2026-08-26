pub mod builder;
pub mod decode;
pub mod encode;
pub mod namespace;

use timbre_xbt_macros::Encode;

pub use self::{decode::Decode, encode::Encode, namespace::*};

pub fn enc() -> builder::EncodeBuilder {
    builder::EncodeBuilder::new()
}

#[cfg(test)]
mod tests {
    use std::u128;

    use indexmap::IndexMap;

    use super::*;
    use crate::*;

    #[test]
    fn test_namespace_roundtrip() {
        let original = Namespace {
            dataplane_id: 1,
            instance_id: 0xFFAA,
        };

        let encoded = original.encode();

        assert_eq!(
            encoded,
            [vec![1], u16::to_be_bytes(0xFFAA).to_vec(),].concat()
        );

        let (decoded, rem) = Namespace::decode(&encoded).unwrap();

        assert_eq!(rem, &[]);
        assert_eq!(original, decoded)
    }

    #[test]
    fn test_derive_encode_struct_roundtrip() {
        let original = reducers::utxos_by_rune_id::Key {
            rune_id: (32, 12),
            height: 3,
            utxo_hash: [0x52; 32],
            utxo_index: 0x25,
        };

        let encoded = original.encode();

        let (decoded, _) = reducers::utxos_by_rune_id::Key::decode(&encoded).unwrap();

        assert_eq!(original, decoded)
    }

    #[test]
    fn test_derive_encode_struct_rb_roundtrip() {
        let original = rollback::MetadataKey {
            height: 1337,
            hash: [10; 32],
            modified_key: vec![123; 56],
        };

        let encoded = original.encode();

        let (decoded, _) = rollback::MetadataKey::decode(&encoded).unwrap();

        assert_eq!(original, decoded)
    }

    #[test]
    fn test_index_map_roundtrip() {
        let original = vec![(1u8, 1u8), (2u8, 2u8)]
            .into_iter()
            .collect::<IndexMap<_, _>>();

        let encoded = original.encode();

        let (decoded, _) = IndexMap::decode(&encoded).unwrap();

        assert_eq!(original, decoded)
    }

    #[test]
    fn test_varint() {
        assert_eq!(VarUInt(0).encode(), vec![0]);
        assert_eq!(VarUInt(1).encode(), vec![1, 1]);
        assert_eq!(VarUInt(0xFF).encode(), vec![1, 255]);
        assert_eq!(VarUInt(0xFF + 1).encode(), vec![2, 1, 0]);
        assert_eq!(
            VarUInt(u128::MAX).encode(),
            vec![
                16, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255
            ]
        );

        // ---

        let mut x = 0;

        let original = VarUInt(x);
        let encoded = original.encode();
        let decoded = VarUInt::decode(&encoded).unwrap().0;

        assert_eq!(original, decoded);

        for _ in 0..16 {
            x = (x << 8) + 0xFF;

            let original = VarUInt(x);
            let encoded = original.encode();
            let decoded = VarUInt::decode(&encoded).unwrap().0;

            assert_eq!(original, decoded);
        }

        // ---

        assert!(VarUInt(0).encode() < VarUInt(0xFF).encode());
        assert!(VarUInt(0xFF).encode() < VarUInt(0xFF + 1).encode());
        assert!(VarUInt(0xFF).encode() < VarUInt(0xFFFF).encode());
        assert!(VarUInt(0xFF).encode() < VarUInt(0xFFFFFF).encode());
        assert!(VarUInt(0xFF).encode() < VarUInt(0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF).encode());
    }

    #[test]
    fn test_cursor_value_roundtrip() {
        let original = CursorValue {
            height: 1337,
            hash: [10; 32],
            was_mempool: true,
            timestamp: 123456789,
            mempool_info: None,
        };

        let encoded = original.encode();

        let (decoded, _) = CursorValue::decode(&encoded).unwrap();

        assert_eq!(original, decoded)
    }

    #[test]
    fn test_cursor_value_roundtrip_mempool() {
        let original = CursorValue {
            height: 1337,
            hash: [10; 32],
            was_mempool: true,
            timestamp: 123456789,
            mempool_info: Some(((456, [11; 32]), 789)),
        };

        let encoded = original.encode();

        let (decoded, _) = CursorValue::decode(&encoded).unwrap();

        assert_eq!(original, decoded)
    }

    #[test]
    fn test_cursor_value_back_compat() {
        let original = CursorValue {
            height: 1337,
            hash: [10; 32],
            was_mempool: true,
            timestamp: 123456789,
            mempool_info: None,
        };

        let encoded = original.encode();

        assert_eq!(
            (1337u64, [10u8; 32]),
            <(u64, [u8; 32])>::decode(&encoded).unwrap().0
        );
    }

    #[test]
    fn test_impl_try_from_varuint() {
        macro_rules! test_type {
            ($type:ty) => {
                assert_eq!(
                    <$type>::try_from(VarUInt(<$type>::MIN as u128)),
                    Ok(<$type>::MIN),
                );
                assert_eq!(
                    <$type>::try_from(VarUInt(<$type>::MAX as u128)),
                    Ok(<$type>::MAX),
                );
                assert_eq!(
                    <$type>::try_from(VarUInt(<$type>::MAX as u128 + 1u128)),
                    Err(TimbreError::VarUIntCasting(<$type>::MAX as u128 + 1u128)),
                );
            };
        }

        test_type!(usize);
        test_type!(u8);
        test_type!(u16);
        test_type!(u32);
        test_type!(u64);

        // u128 is tested separately because adding 1 to max val would overflow
        assert_eq!(<u128>::try_from(VarUInt(u128::MIN as u128)), Ok(u128::MIN));
        assert_eq!(<u128>::try_from(VarUInt(u128::MAX as u128)), Ok(u128::MAX));
    }

    #[test]
    fn test_vec() {
        let a = CursorValue {
            height: 1337,
            hash: [10; 32],
            was_mempool: true,
            timestamp: 123456789,
            mempool_info: None,
        };

        let b = CursorValue {
            height: 1337,
            hash: [10; 32],
            was_mempool: true,
            timestamp: 123456789,
            mempool_info: None,
        };

        let original = vec![a, b];

        let encoded = original.encode();

        let (decoded, rem) = <Vec<CursorValue>>::decode(&encoded).unwrap();

        assert_eq!(original, decoded);
        assert_eq!(rem.len(), 0)
    }

    #[test]
    fn test_u8() {
        let encoded = 255u8.encode();

        let (decoded, rem) = <u8>::decode(&encoded).unwrap();

        assert_eq!(255, decoded);
        assert_eq!(rem.len(), 0);
    }

    #[test]
    fn test_bool() {
        let encoded = true.encode();
        let (decoded, rem) = <bool>::decode(&encoded).unwrap();

        assert_eq!(true, decoded);
        assert_eq!(rem.len(), 0);

        let encoded = false.encode();
        let (decoded, rem) = <bool>::decode(&encoded).unwrap();

        assert_eq!(false, decoded);
        assert_eq!(rem.len(), 0);

        let encoded = vec![2];
        let res = <bool>::decode(&encoded);

        assert!(res.is_err());
    }

    #[test]
    fn test_option() {
        let encoded = Some(1u64).encode();
        let (decoded, rem) = <Option<u64>>::decode(&encoded).unwrap();

        assert_eq!(Some(1u64), decoded);
        assert_eq!(rem.len(), 0);

        let none: Option<u64> = None;
        let encoded = none.encode();
        let (decoded, rem) = <Option<u64>>::decode(&encoded).unwrap();

        assert_eq!(None, decoded);
        assert_eq!(rem.len(), 0);

        let encoded = vec![2];
        let res = <Option<u64>>::decode(&encoded);

        assert!(res.is_err());
    }

    #[test]
    fn test_struct_like_enum_variant_roundtrip() {
        #[derive(Debug, PartialEq, Encode, Decode)]
        enum TestEnum {
            StructLikeVariant {
                first_field: bool,
                second_field: u64,
            },
        }

        let enum_val = TestEnum::StructLikeVariant {
            first_field: true,
            second_field: 11u64,
        };

        let encoded_enum_val = enum_val.encode();

        let (decoded, _) = TestEnum::decode(&encoded_enum_val).unwrap();
        assert_eq!(enum_val, decoded)
    }
}
