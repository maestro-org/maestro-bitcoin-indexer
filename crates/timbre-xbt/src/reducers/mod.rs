use std::fmt::Debug;

use crate::{enc, prefix_key_range, Encode, Namespace, Reducer};

pub mod balances_by_brc20;
pub mod balances_by_rune_id;
pub mod block_by_tx_hash;
pub mod block_info;
pub mod brc20_balances_by_script_hash;
pub mod brc20_terms_by_ticker;
pub mod content_by_inscription_id;
pub mod etching_by_rune_id;
pub mod height_by_block_hash;
pub mod height_by_timestamp;
pub mod historical_sat_balance_by_script_hash;
pub mod inscription_activity_by_script_hash;
pub mod inscription_activity_by_tx;
pub mod inscription_activity_by_tx_v2;
pub mod inscription_utxos_by_script_hash;
pub mod mints_by_rune_id;
pub mod rune_balances_by_script_hash;
pub mod rune_id_by_rune_name;
pub mod rune_txs_by_script_hash;
pub mod rune_utxos_by_script_hash;
pub mod sat_balance_by_script_hash;
pub mod sat_txs_by_script_hash;
pub mod sats_per_vb_by_block;
pub mod script_by_script_hash;
pub mod script_hash_by_address_payload_hash;
pub mod spending_tx_by_txo;
pub mod total_inscriptions_by_script_hash;
pub mod total_outputs_by_script_hash;
pub mod total_sat_in_inputs_by_script_hash;
pub mod total_sat_in_outputs_by_script_hash;
pub mod total_txs_by_script_hash;
pub mod total_utxos_by_script_hash;
pub mod transfer_inscriptions_by_script_hash;
pub mod tx_first_seen_timestamp;
pub mod tx_info;
pub mod txs_by_block;
pub mod txs_by_inscription;
pub mod txs_by_rune_id;
pub mod txs_by_script_hash;
pub mod utxos_by_rune_id;
pub mod utxos_by_script_hash;

// type aliases to help with consistency across reducers

pub type TxHash = [u8; 32]; // transaction hash/id
pub type BlockHash = [u8; 32]; // block hash/id
pub type ScriptHash = [u8; 20]; // transaction hash/id

pub type Height = u64; // block height
pub type Timestamp = u32;
pub type TxIndex = u32; // index of transaction within block

pub type TxoIndex = u32; // index of output in transaction outputs

pub type SatoshiQuantity = u64; // satoshis in a single output

pub type InscriptionIndex = u32; // index of an inscription within an inscribing transaction
pub type SatoshiOffset = u64; // offset of an individual satoshi within an output

pub type InscriptionId = (TxHash, InscriptionIndex); // reveal tx and index of inscription in tx
pub type RuneId = (Height, TxIndex); // pointer to tx which created the rune

pub type AggregatedSatoshis = u128; // aggregation of multiple satoshi amounts (volume in block, ..)

pub type RuneQuantity = u128; // amount of runes
pub type Brc20Quantity = u128; // amount of brc20

/// Returns a (possible) pair of encoded keys representing the given range on the TiKV database.
///
/// Arguments:
///
/// - namespace: The namespace to use (containing dataplane and instance)
/// - reducer: The reducer to use
/// - params: The params for the reducer, like a rune_id for the utxo reducer
/// - range: The range of the values in the key we're interested in
pub fn reducer_maybe_key_range<
    A: Debug + Encode + Clone,
    B: Debug + Encode + Clone,
    C: Debug + Encode + Clone,
>(
    namespace: &Namespace,
    reducer: &Reducer,
    params: &Option<A>,
    lower: Option<B>,
    upper: Option<C>,
) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
    // create reducer data prefix
    let mut prefix = enc()
        .append(&namespace)
        .data_tag()
        .append_with_break(&reducer);

    // if params provided, extend the prefix with the params
    if let Some(p) = params {
        prefix = prefix.append(p);
    }

    // if a lower part provided, extend the prefix with the lower part for the start key
    let start = lower.map(|v| prefix.clone().append(&v).build());

    // if an upper part provided, extend the prefix with the upper part for the end key
    let end = upper.map(|v| prefix.append(&v).build());

    (start, end)
}

/// Same as `maybe_key_range`, but it returns keys for the bounds even if they are not present.
pub fn reducer_key_range<
    A: Debug + Encode + Clone,
    B: Debug + Encode + Clone,
    C: Debug + Encode + Clone,
>(
    namespace: &Namespace,
    reducer: &Reducer,
    params: &Option<A>,
    lower: Option<B>,
    upper: Option<C>,
) -> (Vec<u8>, Vec<u8>) {
    // create reducer data prefix
    let mut prefix = enc()
        .append(&namespace)
        .data_tag()
        .append_with_break(&reducer);

    // if params provided, extend the prefix with the params
    if let Some(p) = params {
        prefix = prefix.append(p);
    };

    let prefix = prefix.build();

    // build full key range for the reducer + params
    let full_range = prefix_key_range(&prefix);

    let (start, end) = reducer_maybe_key_range(namespace, reducer, params, lower, upper);

    // if no lower/upper bound provided, use the start/end of the reducer + params range
    (
        start.unwrap_or(full_range.start),
        end.unwrap_or(full_range.end),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::*;

    #[test]
    fn test_encode_key_pair() {
        let (start, end) = reducer_maybe_key_range(
            &Namespace::new(1, 2),
            &Reducer::UtxosByRuneId,
            &Some(1u8),
            Some(1u8),
            Some(3u8),
        );

        assert!(start.unwrap() < end.unwrap());
    }

    #[test]
    fn test_encode_key_pair_full() {
        let (start, end) = reducer_maybe_key_range::<usize, usize, usize>(
            &Namespace::new(1, 2),
            &Reducer::UtxosByRuneId,
            &Some(1),
            None,
            None,
        );

        assert!(start.is_none());
        assert!(end.is_none());
    }

    #[test]
    fn test_encode_key_pair_up_to() {
        let (start, end) = reducer_maybe_key_range::<usize, usize, usize>(
            &Namespace::new(1, 2),
            &Reducer::UtxosByRuneId,
            &Some(1),
            None,
            Some(3),
        );

        assert!(start.is_none());
        assert!(end.is_some());
    }

    #[test]
    fn test_encode_key_pair_starting_from() {
        let (start, end) = reducer_maybe_key_range::<usize, usize, usize>(
            &Namespace::new(1, 2),
            &Reducer::UtxosByRuneId,
            &Some(1),
            Some(3),
            None,
        );

        assert!(start.is_some());
        assert!(end.is_none());
    }

    #[test]
    fn test_encode_key_pair_strict() {
        let (start, end) = reducer_key_range::<u64, u64, u64>(
            &Namespace::new(1, 2),
            &Reducer::UtxosByRuneId,
            &Some(1),
            Some(1),
            Some(3),
        );

        assert!(start < end);
    }

    #[test]
    fn test_encode_key_pair_strict_cursor() {
        let (start, end) = reducer_key_range::<_, _, u64>(
            &Namespace::new(1, 2),
            &Reducer::UtxosByRuneId,
            &Some((32u64, 12u64)),
            Some(reducers::utxos_by_rune_id::Cursor {
                height: 3,
                utxo_hash: [0x52; 32],
                utxo_index: 0x24,
            }),
            Some(5u64),
        );

        let key1 = Prefix::new(1, 2).data(
            &Reducer::UtxosByRuneId,
            &reducers::utxos_by_rune_id::Key {
                rune_id: (32, 12),
                height: 3,
                utxo_hash: [0x52; 32],
                utxo_index: 0x25,
            },
        );

        let key2 = Prefix::new(1, 2).data(
            &Reducer::UtxosByRuneId,
            &reducers::utxos_by_rune_id::Key {
                rune_id: (32, 12),
                height: 7,
                utxo_hash: [0x52; 32],
                utxo_index: 0x25,
            },
        );

        assert!(start < key1);
        assert!(key1 < end);
        assert!(end < key2);
    }

    #[test]
    fn test_encode_key_pair_strict_full() {
        let (start, end) = reducer_key_range::<usize, usize, usize>(
            &Namespace::new(1, 2),
            &Reducer::UtxosByRuneId,
            &Some(1),
            None,
            None,
        );

        assert!(start < end);
    }

    #[test]
    fn test_encode_key_pair_strict_up_to() {
        let (start, end) = reducer_key_range::<u64, usize, u64>(
            &Namespace::new(1, 2),
            &Reducer::UtxosByRuneId,
            &Some(1),
            None,
            Some(3),
        );

        assert!(start < end);
    }

    #[test]
    fn test_encode_key_pair_strict_starting_from() {
        let (start, end) = reducer_key_range::<usize, usize, usize>(
            &Namespace::new(1, 2),
            &Reducer::UtxosByRuneId,
            &Some(1),
            Some(3),
            None,
        );

        assert!(start < end);
    }
}
