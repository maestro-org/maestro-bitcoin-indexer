use std::fmt::Debug;

use crate::{enc, prefix_key_range, Decode, Encode, Namespace};

#[derive(Debug, Clone, Encode, Decode, PartialEq)]
pub struct MetadataKey {
    pub height: u64,
    pub hash: [u8; 32],
    pub modified_key: Vec<u8>,
}

pub fn rollback_metadata_key_range<A: Debug + Encode + Clone, B: Debug + Encode + Clone>(
    namespace: &Namespace,
    lower: Option<A>,
    upper: Option<B>,
) -> (Vec<u8>, Vec<u8>) {
    let prefix = enc().append(&namespace).rollback_tag().break_();

    let full_range = prefix_key_range(&prefix.clone().build());

    let start = lower.map(|v| prefix.clone().append(&v).build());
    let end = upper.map(|v| prefix.append(&v).build());

    (
        start.unwrap_or(full_range.start),
        end.unwrap_or(full_range.end),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        builder::{BREAK, BREAK_1, PREFIX_ROLLBACK},
        *,
    };

    #[test]
    fn test_rollback_key_encoding() {
        let key = MetadataKey {
            height: 3,
            hash: [25; 32],
            modified_key: vec![7, 8, 9],
        };

        let encoded = key.encode();

        assert_eq!(
            encoded,
            [vec![1u8, 3], vec![25u8; 32], vec![1u8, 3, 7, 8, 9]].concat()
        );

        let full_key_encoded = Prefix::new(1, 2).rollback(key);

        assert_eq!(
            full_key_encoded,
            [
                vec![1],
                vec![0, 2],
                vec![PREFIX_ROLLBACK, BREAK],
                vec![1u8, 3],
                vec![25u8; 32],
                vec![1u8, 3, 7, 8, 9],
            ]
            .concat()
        );
    }

    #[test]
    fn test_encode_rollback_range_cursor() {
        let (start, end) = rollback_metadata_key_range::<u64, _>(
            &Namespace::new(1, 2),
            Some(1),
            Some(MetadataKey {
                height: 3,
                hash: [25; 32],
                modified_key: vec![7, 8, 9],
            }),
        );

        assert!(start < end);
        assert_eq!(start, vec![1, 0, 2, PREFIX_ROLLBACK, BREAK, 1, 1]);
        assert_eq!(
            end,
            [
                vec![1],
                vec![0, 2],
                vec![PREFIX_ROLLBACK, BREAK],
                vec![1u8, 3],
                vec![25u8; 32],
                vec![1u8, 3, 7, 8, 9],
            ]
            .concat()
        );
    }

    #[test]
    fn test_encode_rollback_range_full() {
        let namespace = Namespace::new(1, 2);

        let (start, end) = rollback_metadata_key_range::<u64, u64>(&namespace, None, None);

        assert!(start < end);
        assert_eq!(
            start,
            [namespace.encode(), vec![PREFIX_ROLLBACK, BREAK]].concat()
        );
        assert_eq!(
            end,
            [namespace.encode(), vec![PREFIX_ROLLBACK, BREAK_1]].concat()
        );
    }

    #[test]
    fn test_encode_rollback_range_lower() {
        let namespace = Namespace::new(1, 2);

        let (start, end) = rollback_metadata_key_range::<u64, u64>(&namespace, Some(1), None);

        assert!(start < end);
        assert_eq!(
            start,
            [namespace.encode(), vec![PREFIX_ROLLBACK, BREAK, 1, 1]].concat()
        );
        assert_eq!(
            end,
            [namespace.encode(), vec![PREFIX_ROLLBACK, BREAK_1]].concat()
        );
    }

    #[test]
    fn test_encode_rollback_range_upper() {
        let namespace = Namespace::new(1, 2);

        let (start, end) = rollback_metadata_key_range::<u64, u64>(&namespace, None, Some(1));

        assert!(start < end);
        assert_eq!(
            start,
            [namespace.encode(), vec![PREFIX_ROLLBACK, BREAK]].concat()
        );
        assert_eq!(
            end,
            [namespace.encode(), vec![PREFIX_ROLLBACK, BREAK, 1, 1]].concat()
        );
    }

    #[test]
    fn test_encode_rollback_range_both() {
        let namespace = Namespace::new(1, 2);

        let (start, end) =
            rollback_metadata_key_range::<u64, u64>(&namespace, Some(0xFFFE), Some(0xFFFF));

        assert!(start < end);
        assert_eq!(
            start,
            [
                namespace.encode(),
                vec![PREFIX_ROLLBACK, BREAK, 2, 255, 254]
            ]
            .concat()
        );
        assert_eq!(
            end,
            [
                namespace.encode(),
                vec![PREFIX_ROLLBACK, BREAK, 2, 255, 255]
            ]
            .concat()
        );
    }
}
