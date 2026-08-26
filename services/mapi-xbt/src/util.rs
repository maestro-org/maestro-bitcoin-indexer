use std::fmt::{self, Debug};
use std::str::FromStr;
use std::{collections::HashMap, ops::Range};

use axum::Extension;
use bitcoin::{hashes::Hash, Address, Script, Txid};
use chrono::{DateTime, TimeZone, Utc};
use ordinals::Rune;
use serde::Deserializer;
use timbre_xbt::reducers::{
    etching_by_rune_id,
    inscription_activity_by_script_hash::{
        ReceivedInscription, SelfTransferredInscription, SentInscription,
        Value as InscriptionActivityByScriptHashValue,
    },
    reducer_key_range,
    rune_txs_by_script_hash::Value as RuneTxsByScriptHashValue,
    sats_per_vb_by_block::{Key as SatsPerVbByBlockKey, Value as SatsPerVbByBlockValue},
    tx_info::{Key as TxInfoKey, Value as TxInfoValue},
    txs_by_block::{Key as TxsByBlockKey, Value as TxsByBlockValue},
    Height, Timestamp,
};
use timbre_xbt::{Decode, Encode, Prefix, Reducer};

use crate::{
    error::{Error, MapiResult},
    options::Mode,
    tikv::{adapter::TiKVAdapter, key_resolver::ReducerType, Scanner},
    types::{
        inscriptions::{
            FromInscriptionLocation, InscriptionActivity, InscriptionActivityByTx,
            ToInscriptionLocation,
        },
        runes::{
            EtchAndPremine, RuneActivity, RuneInfo, RuneInfoBrief, Terms, WalletRuneActivity,
            WalletRuneAndAmount,
        },
        BlockParam, BlockSatsPerVb, CountParam, CursorPaginationParams, EstimatedBlock,
        HeightPaginationParams, OrderParam, RuneAndAmount, Softfork,
    },
};

// MAX_TXS_BY_BLOCK is an upper bound to the number of inputs and outputs returned in the "Txs by Block" endpoint
// Update API doc if this value changes.
pub static TXS_BY_BLOCK_MAX_UTXOS: usize = 10;
pub static MAX_PAGE_COUNT: usize = 100;
pub static MAX_CONTENT_PREVIEW: usize = 100;
pub static DEFAULT_CONTENT_BODY_SIZE: u64 = 100;
pub static MAX_CONTENT_BODY_SIZE: u64 = 4096;

/// Checks if a script is an OP_RETURN script pubkey.
/// If it is, logs a warning and returns a `MalformedRequest` error.
///
/// OP_RETURN scripts are not supported by certain reducers like
/// `RuneTxsByScriptHash` and `SatTxsByScriptHash`.
pub fn check_op_return_script(script: &Script) -> MapiResult<()> {
    if script.is_op_return() {
        tracing::warn!(
            "OP_RETURN script pubkey detected: {}",
            hex::encode(script.as_bytes())
        );
        return Err(Error::MalformedRequest(
            "OP_RETURN script pubkeys are not supported by this endpoint".into(),
        ));
    }
    Ok(())
}

pub fn deserialize_softforks<'de, D>(deserializer: D) -> Result<HashMap<String, Softfork>, D::Error>
where
    D: Deserializer<'de>,
{
    struct SoftforksVisitor;

    impl<'de> serde::de::Visitor<'de> for SoftforksVisitor {
        type Value = HashMap<String, Softfork>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a hashmap or an array of softforks")
        }

        fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
        where
            M: serde::de::MapAccess<'de>,
        {
            let mut map = HashMap::new();
            while let Some((key, value)) = access.next_entry()? {
                map.insert(key, value);
            }
            Ok(map)
        }

        fn visit_seq<S>(self, mut access: S) -> Result<Self::Value, S::Error>
        where
            S: serde::de::SeqAccess<'de>,
        {
            let mut map = HashMap::new();
            let mut count = 0;
            while let Some(value) = access.next_element::<Softfork>()? {
                map.insert(count.to_string(), value);
                count += 1;
            }
            Ok(map)
        }
    }

    deserializer.deserialize_any(SoftforksVisitor)
}

pub struct ParsedHeightPaginationParams {
    count: usize,
    order: OrderParam,
    range: Range<Vec<u8>>,
}

impl ParsedHeightPaginationParams {
    pub fn parse<A: Encode + Decode + Debug + Clone, C: Encode + Decode + Debug>(
        params: HeightPaginationParams,
        encoder: &Prefix,
        reducer: &Reducer,
        key_params: Option<A>,
    ) -> MapiResult<Self> {
        return Self::parse_aux::<A, C, u64, u64>(
            params.count,
            params.order,
            params.cursor,
            params.from,
            params.to.map(|x| x.saturating_add(1)), // Upper bound is exclusive.
            encoder,
            reducer,
            key_params,
        );
    }

    pub fn parse_aux<
        A: Encode + Decode + Debug + Clone,
        C: Encode + Decode + Debug,
        D: Debug + Encode + Clone,
        E: Debug + Encode + Clone,
    >(
        params_count: Option<CountParam>,
        params_order: Option<OrderParam>,
        params_cursor: Option<String>,
        params_from: Option<D>,
        params_to: Option<E>,
        encoder: &Prefix,
        reducer: &Reducer,
        key_params: Option<A>,
    ) -> MapiResult<Self> {
        let count = params_count.map(|x| x.0).unwrap_or(MAX_PAGE_COUNT);

        if count > MAX_PAGE_COUNT || count == 0 {
            return Err(Error::MalformedRequest("Invalid page size".into()));
        }

        let order = params_order.unwrap_or(crate::types::OrderParam::Asc);

        // due to key format we need to add 1 to the height x if we want to find
        // keys with height x in the range
        let (mut range_lower, mut range_upper) = reducer_key_range(
            encoder.namespace(),
            reducer,
            &key_params,
            params_from,
            params_to,
        );

        if let Some(cursor) = &params_cursor {
            let (cursor, _) = C::decode_base64(cursor).map_err(|_| {
                Error::MalformedRequest("Malformed cursor: unable to decode".into())
            })?;

            let mut cursor_key = if let Some(p) = key_params {
                encoder.data(reducer, &(p, cursor))
            } else {
                encoder.data(reducer, &cursor)
            };

            if !(range_lower <= cursor_key && cursor_key <= range_upper) {
                return Err(Error::MalformedRequest(
                    "Malformed cursor: invalid for height range".into(),
                ));
            }

            // if ascending, increase cursor key by 1 lexicographically to avoid
            // including cursor kv in scanned keys
            if order == OrderParam::Asc {
                cursor_key.push(0x00);

                range_lower = cursor_key;
            } else {
                range_upper = cursor_key
            }
        }

        Ok(Self {
            count,
            order,
            range: range_lower..range_upper,
        })
    }

    pub fn count(&self) -> usize {
        self.count
    }

    pub fn order(&self) -> OrderParam {
        self.order
    }

    pub fn key_range(&self) -> Range<Vec<u8>> {
        self.range.clone()
    }
}

pub struct ParsedPaginationParams {
    count: usize,
    order: OrderParam,
    range: Range<Vec<u8>>,
}

impl ParsedPaginationParams {
    pub fn parse<A: Encode + Decode + Debug + Clone, C: Encode + Decode + Debug>(
        params: HeightPaginationParams,
        encoder: &Prefix,
        reducer: &Reducer,
        key_params: Option<A>,
    ) -> MapiResult<Self> {
        let count = params.count.map(|x| x.0).unwrap_or(MAX_PAGE_COUNT);

        if count > MAX_PAGE_COUNT || count == 0 {
            return Err(Error::MalformedRequest("Invalid page size".into()));
        }

        let order = params.order.unwrap_or(crate::types::OrderParam::Asc);

        // due to key format we need to add 1 to the height x if we want to find
        // keys with height x in the range
        let (mut range_lower, mut range_upper) = reducer_key_range(
            encoder.namespace(),
            reducer,
            &key_params,
            params.from,
            params.to.map(|x| x.saturating_add(1)),
        );

        if let Some(cursor) = &params.cursor {
            let (cursor, _) = C::decode_base64(cursor).map_err(|_| {
                Error::MalformedRequest("Malformed cursor: unable to decode".into())
            })?;

            let mut cursor_key = if let Some(p) = key_params {
                encoder.data(reducer, &(p, cursor))
            } else {
                encoder.data(reducer, &cursor)
            };

            if !(range_lower <= cursor_key && cursor_key <= range_upper) {
                return Err(Error::MalformedRequest(
                    "Malformed cursor: invalid for height range".into(),
                ));
            }

            // if ascending, increase cursor key by 1 lexicographically to avoid
            // including cursor kv in scanned keys
            if order == OrderParam::Asc {
                cursor_key.push(0x00);

                range_lower = cursor_key;
            } else {
                range_upper = cursor_key
            }
        }

        Ok(Self {
            count,
            order,
            range: range_lower..range_upper,
        })
    }

    pub fn parse_no_height<A: Encode + Decode + Debug + Clone, C: Encode + Decode + Debug>(
        params: CursorPaginationParams,
        encoder: &Prefix,
        reducer: &Reducer,
        key_params: Option<A>,
    ) -> MapiResult<Self> {
        let count = params.count.map(|x| x.0).unwrap_or(MAX_PAGE_COUNT);

        if count > MAX_PAGE_COUNT || count == 0 {
            return Err(Error::MalformedRequest("Invalid page size".into()));
        }

        let order = params.order.unwrap_or(crate::types::OrderParam::Asc);

        // due to key format we need to add 1 to the height x if we want to find
        // keys with height x in the range
        let (mut range_lower, mut range_upper) = reducer_key_range(
            encoder.namespace(),
            reducer,
            &key_params,
            None::<u64>,
            None::<u64>,
        );

        if let Some(cursor) = &params.cursor {
            let (cursor, _) = C::decode_base64(cursor).map_err(|_| {
                Error::MalformedRequest("Malformed cursor: unable to decode".into())
            })?;

            let mut cursor_key = if let Some(p) = key_params {
                encoder.data(reducer, &(p, cursor))
            } else {
                encoder.data(reducer, &cursor)
            };

            if !(range_lower <= cursor_key && cursor_key <= range_upper) {
                return Err(Error::MalformedRequest(
                    "Malformed cursor: invalid for height range".into(),
                ));
            }

            // if ascending, increase cursor key by 1 lexicographically to avoid
            // including cursor kv in scanned keys
            if order == OrderParam::Asc {
                cursor_key.push(0x00);

                range_lower = cursor_key;
            } else {
                range_upper = cursor_key
            }
        }

        Ok(Self {
            count,
            order,
            range: range_lower..range_upper,
        })
    }

    pub fn count(&self) -> usize {
        self.count
    }

    pub fn order(&self) -> OrderParam {
        self.order
    }

    pub fn key_range(&self) -> Range<Vec<u8>> {
        self.range.clone()
    }
}

pub enum RuneIdentifier {
    Id((u64, u32)),
    Name(u128),
}

impl RuneIdentifier {
    pub fn parse(string: String) -> MapiResult<Self> {
        if string.contains(':') {
            let parts: Vec<_> = string.split(':').collect();
            if parts.len() != 2 {
                return Err(Error::MalformedRequest(
                    "Rune ID must be etching block and transaction index in form '2519999:31'"
                        .into(),
                ));
            }
            let invalid =
                || Error::MalformedRequest("Rune ID must be in the form '2519999:31'".into());
            let block = parts[0].parse().map_err(|_| invalid())?;
            let tx = parts[1].parse().map_err(|_| invalid())?;

            Ok(Self::Id((block, tx)))
        } else {
            let without_spacers = string.replace("•", "");

            let rune = Rune::from_str(&without_spacers)
                .map_err(|_| Error::MalformedRequest("Unable to decode rune name".into()))?;

            Ok(Self::Name(rune.n()))
        }
    }
}

pub fn parse_inscription_id(input: &String) -> MapiResult<([u8; 32], u32)> {
    let mut parts = input.split('i');
    let hex_part = parts
        .next()
        .ok_or(Error::MalformedRequest("Empty inscription ID".into()))?;
    let int_part = parts
        .next()
        .ok_or(Error::MalformedRequest("Missing inscription index".into()))?;
    if parts.next().is_some() {
        return Err(Error::MalformedRequest("Wrong inscription ID".into()));
    }

    let mut bytes: [u8; 32] = hex::decode(hex_part)
        .map_err(|_| Error::InvalidHex(hex_part.into()))?
        .try_into()
        .map_err(|_| Error::MalformedRequest("Wrong tx hash in inscription ID".into()))?;

    bytes.reverse();

    let number = int_part
        .parse::<u32>()
        .map_err(|_| Error::MalformedRequest("Wrong index in inscription ID".into()))?;

    Ok((bytes, number))
}

pub fn parse_block_param(input: &String, is_timestamp: bool) -> MapiResult<BlockParam> {
    if is_timestamp {
        if let Ok(timestamp) = input.parse::<u32>() {
            return Ok(BlockParam::Timestamp(timestamp));
        } else {
            return Err(Error::MalformedRequest(
                "Unable to parse as timestamp".into(),
            ));
        }
    } else if let Ok(height) = input.parse::<u64>() {
        return Ok(BlockParam::Height(height));
    }

    let mut hash: [u8; 32] = hex::decode(input)
        .map_err(|_| Error::InvalidHex(input.into()))?
        .try_into()
        .map_err(|_| Error::MalformedRequest("Wrong block height or hash".into()))?;

    hash.reverse();

    Ok(BlockParam::Hash(hash))
}

pub fn parse_tx_hash(input: &String) -> MapiResult<[u8; 32]> {
    let mut hash: [u8; 32] = hex::decode(input)
        .map_err(|_| Error::InvalidHex(input.into()))?
        .try_into()
        .map_err(|_| Error::MalformedRequest("Wrong tx hash".into()))?;

    hash.reverse();

    Ok(hash)
}

// Group tags in by their length, iterate over each group, compute the set of subslices in
// `coinbase_tag` of each of these lengths, and try to match any of the tags in the group with one
// of the subslices in `coinbase_tag`.
pub fn get_miner_tag_maybe(coinbase_tag: &Vec<u8>, known_tags: Vec<Vec<u8>>) -> Option<Vec<u8>> {
    let mut length_groups: HashMap<usize, Vec<Vec<u8>>> = HashMap::new();
    for tag in known_tags {
        length_groups
            .entry(tag.len())
            .or_default()
            .push(tag.clone());
    }
    for (tags_length, tags) in length_groups.iter() {
        for candidate in coinbase_tag.clone().windows(*tags_length) {
            for tag in tags.iter() {
                if candidate == *tag {
                    return Some(tag.clone());
                }
            }
        }
    }
    None
}

pub fn decimal(num: u128, dec: usize) -> String {
    let mut bal_string = num.to_string();
    let bal_string_len = bal_string.len();

    if dec > 0 {
        if bal_string_len == dec {
            let mut new_string = String::from("0.");
            new_string.push_str(&bal_string);

            bal_string = new_string;
        } else if bal_string_len < dec {
            let mut new_string = String::from("0.");

            for _ in 0..(dec - bal_string_len) {
                new_string.push('0')
            }

            new_string.push_str(&bal_string);

            bal_string = new_string;
        } else {
            bal_string.insert(bal_string_len - dec, '.');
        }
    }

    bal_string
}

pub fn decimal_f64(num: f64, dec: usize) -> String {
    let divisor = 10f64.powi(dec as i32);
    let result = num / divisor;

    format!("{:.2}", result)
}

pub fn timestamp_to_string(ts: u64) -> String {
    let datetime: DateTime<Utc> = Utc.timestamp_opt(ts.try_into().unwrap(), 0).unwrap();
    datetime.format("%Y-%m-%d %H:%M:%S").to_string()
}

// Given a KV produced by the `InscriptionActivityByScriptHash` and related to a specific address
// and tx, process all self-transferred, sent and received inscriptions and build the resulting
// `InscriptionActivity`.
// `resolved_script_hashes` is passed as reference to avoid resolving script hashes involved in the
// processing of non-self-transferred inscriptions.
pub async fn build_inscription_activity(
    inscription_activity: InscriptionActivityByScriptHashValue,
    query_address: &Option<Address>,
    query_script_bytes: &Vec<u8>,
    current_tx_hash: [u8; 32],
    height: u64,
    resolved_script_hashes: &mut HashMap<[u8; 20], (Option<Address>, Vec<u8>)>,
    tikv: &mut Extension<TiKVAdapter>,
    mode: &Extension<Mode>,
) -> MapiResult<InscriptionActivity> {
    let query_address = query_address.as_ref().map(|x| x.to_string());
    let query_script_bytes = hex::encode(query_script_bytes.clone());

    // Populate list of self-transferred inscriptions.
    let mut self_transferred = vec![];
    for SelfTransferredInscription {
        inscription_id,
        input_index,
        input_sat_offset,
        output_index,
        output_sat_offset,
    } in inscription_activity.self_transfers
    {
        let from = Some(FromInscriptionLocation {
            address: query_address.clone(),
            script_pubkey: query_script_bytes.clone(),
            input_index,
            sat_offset: input_sat_offset,
        });

        let to = ToInscriptionLocation {
            address: query_address.clone(),
            script_pubkey: query_script_bytes.clone(),
            output_vout: output_index,
            sat_offset: output_sat_offset,
            output_txid: Txid::from_byte_array(current_tx_hash).to_string(),
        };

        self_transferred.push(InscriptionActivityByTx {
            inscription_id: format!(
                "{}i{}",
                Txid::from_byte_array(inscription_id.0),
                inscription_id.1,
            ),
            from,
            to,
        });
    }

    // Populate list of sent inscriptions.
    let mut sent = vec![];
    for SentInscription {
        inscription_id,
        input_index,
        input_sat_offset,
        output_index,
        output_sat_offset,
        output_script_hash,
    } in inscription_activity.sent
    {
        let from = Some(FromInscriptionLocation {
            address: query_address.clone(),
            script_pubkey: query_script_bytes.clone(),
            input_index,
            sat_offset: input_sat_offset,
        });

        let output_info =
            if let (Some(output_script_hash), Some(output_index), Some(output_sat_offset)) =
                (output_script_hash, output_index, output_sat_offset)
            {
                Some((output_script_hash, output_index, output_sat_offset))
            } else {
                None
            };

        let (output_script_hash, output_vout, sat_offset, output_txid) =
            get_inscription_coinbase_location(output_info, height, tikv, inscription_id).await?;

        // If `get_inscription_coinbase_location` returns something in `output_txid`, then it's the
        // coinbase tx hash. Otherwise, the inscription is in an output of the current tx.
        let output_txid = output_txid.unwrap_or(Txid::from_byte_array(current_tx_hash).to_string());

        let (output_address, output_script_bytes) =
            match resolved_script_hashes.get(&output_script_hash) {
                Some(res) => res.clone(),
                None => {
                    let (address, script_bytes) =
                        tikv.resolve_script_hash(mode.0, output_script_hash).await?;

                    resolved_script_hashes
                        .insert(output_script_hash, (address.clone(), script_bytes.clone()));

                    (address, script_bytes)
                }
            };

        let to = ToInscriptionLocation {
            address: output_address.as_ref().map(|x| x.to_string()),
            script_pubkey: hex::encode(&output_script_bytes),
            output_vout,
            sat_offset,
            output_txid,
        };

        sent.push(InscriptionActivityByTx {
            inscription_id: format!(
                "{}i{}",
                Txid::from_byte_array(inscription_id.0),
                inscription_id.1,
            ),
            from,
            to,
        });
    }

    // Populate list of received inscriptions.
    let mut received = vec![];
    for ReceivedInscription {
        inscription_id,
        input_index,
        input_sat_offset,
        input_script_hash,
        output_index,
        output_sat_offset,
    } in inscription_activity.received
    {
        // Inscription origin is optional.
        let from = if let (Some(input_index), Some(input_sat_offset), Some(input_script_hash)) =
            (input_index, input_sat_offset, input_script_hash)
        {
            let (input_address, input_script_bytes) =
                match resolved_script_hashes.get(&input_script_hash) {
                    Some(res) => res.clone(),
                    None => {
                        let (address, script_bytes) =
                            tikv.resolve_script_hash(mode.0, input_script_hash).await?;

                        resolved_script_hashes
                            .insert(input_script_hash, (address.clone(), script_bytes.clone()));

                        (address, script_bytes)
                    }
                };

            Some(FromInscriptionLocation {
                address: input_address.as_ref().map(|x| x.to_string()),
                script_pubkey: hex::encode(input_script_bytes.clone()),
                input_index,
                sat_offset: input_sat_offset,
            })
        } else {
            None
        };

        let to = ToInscriptionLocation {
            address: query_address.clone(),
            script_pubkey: query_script_bytes.clone(),
            output_vout: output_index,
            sat_offset: output_sat_offset,
            output_txid: Txid::from_byte_array(current_tx_hash).to_string(),
        };

        received.push(InscriptionActivityByTx {
            inscription_id: format!(
                "{}i{}",
                Txid::from_byte_array(inscription_id.0),
                inscription_id.1,
            ),
            from,
            to,
        });
    }

    // Sort activity in each activity kind to ensure same responses through different instances of
    // the reducer, which is non-deterministic.
    self_transferred.sort_by_key(|activity| activity.inscription_id.clone());
    sent.sort_by_key(|activity| activity.inscription_id.clone());
    received.sort_by_key(|activity| activity.inscription_id.clone());

    Ok(InscriptionActivity {
        self_transferred,
        sent,
        received,
    })
}

// Avoid re-fetching etching terms for runes that we have already processed.
// `decimals_and_minting` is a map from rune IDs to a pair of the decimals and the minting amount of
// the rune.
pub async fn get_decimals_and_minting(
    rune_id: (u64, u32),
    decimals_and_minting: &mut HashMap<(u64, u32), (usize, u128)>,
    tikv: &mut Extension<TiKVAdapter>,
) -> MapiResult<(usize, u128)> {
    match decimals_and_minting.get(&rune_id) {
        Some(info) => Ok(*info),
        None => {
            let etching_terms = tikv
                .get_reducer_key::<_, etching_by_rune_id::Value>(
                    (ReducerType::EtchingByRuneId, Reducer::EtchingByRuneId),
                    &etching_by_rune_id::Key { rune_id },
                )
                .await?;

            let dec = etching_terms.divisibility.unwrap_or(0) as usize;
            let minting = etching_terms.amount_per_mint.unwrap_or(0u128);

            decimals_and_minting.insert(rune_id, (dec, minting));

            Ok((dec, minting))
        }
    }
}

// Given a KV produced by the `RuneTxsByScriptHash` and related to a specific address and tx,
// process and collect activity into the resultin `RuneActivity`, including etched and minted runes,
// self-transferred runes, runes whose balance increased and runes whose balance decreased.
// `decimals_and_minting` (a map from rune IDs to a pair of the decimals and the minting amount of
// the rune) is passed as reference to avoid needing to re-fetch etching terms of any previously
// processed runes.
pub async fn build_rune_activity(
    rune_activity: RuneTxsByScriptHashValue,
    decimals_and_minting: &mut HashMap<(u64, u32), (usize, u128)>,
    tikv: &mut Extension<TiKVAdapter>,
) -> MapiResult<RuneActivity> {
    let etched_rune = match rune_activity.etched {
        Some((rune_id, premined_amount)) => {
            let (dec, _) = get_decimals_and_minting(rune_id, decimals_and_minting, tikv).await?;

            Some(EtchAndPremine {
                rune_id: format!("{}:{}", rune_id.0, rune_id.1),
                premined_amount: premined_amount.map(|x| decimal(x, dec)),
            })
        }
        None => None,
    };

    let minted = match rune_activity.minted {
        Some(rune_id) => {
            let (dec, minting) =
                get_decimals_and_minting(rune_id, decimals_and_minting, tikv).await?;

            Some(RuneAndAmount {
                rune_id: format!("{}:{}", rune_id.0, rune_id.1),
                amount: decimal(minting, dec),
            })
        }
        None => None,
    };

    let mut self_transfers: Vec<RuneAndAmount> = vec![];
    for (rune_id, amount) in rune_activity.self_transfers {
        let (dec, _) = get_decimals_and_minting(rune_id, decimals_and_minting, tikv).await?;

        self_transfers.push(RuneAndAmount {
            rune_id: format!("{}:{}", rune_id.0, rune_id.1),
            amount: decimal(amount, dec),
        });
    }

    let mut increased_balances: Vec<RuneAndAmount> = vec![];
    for (rune_id, amount) in rune_activity.increased_balances {
        let (dec, _) = get_decimals_and_minting(rune_id, decimals_and_minting, tikv).await?;

        increased_balances.push(RuneAndAmount {
            rune_id: format!("{}:{}", rune_id.0, rune_id.1),
            amount: decimal(amount, dec),
        });
    }

    let mut decreased_balances: Vec<RuneAndAmount> = vec![];
    for (rune_id, amount) in rune_activity.decreased_balances {
        let (dec, _) = get_decimals_and_minting(rune_id, decimals_and_minting, tikv).await?;

        decreased_balances.push(RuneAndAmount {
            rune_id: format!("{}:{}", rune_id.0, rune_id.1),
            amount: decimal(amount, dec),
        });
    }

    Ok(RuneActivity {
        etched_rune,
        minted,
        self_transfers,
        increased_balances,
        decreased_balances,
    })
}

// Computes the USD equivalent of a rune amount from previously fetched prices.
// Returns `Ok(None)` when no external price service is configured (`rune_prices` is `None`),
// and an internal error when a price service is configured but the price is missing.
fn rune_usd_amount(
    rune_prices: &Option<HashMap<Height, HashMap<String, f64>>>,
    activity_height: Height,
    rune_id: &str,
    amount: u128,
    dec: usize,
    context: &str,
) -> MapiResult<Option<String>> {
    match rune_prices {
        None => Ok(None),
        Some(prices) => match prices
            .get(&activity_height)
            .and_then(|rune_and_prices| rune_and_prices.get(rune_id))
        {
            Some(usd_price) => Ok(Some(decimal_f64(amount as f64 * usd_price, dec))),
            None => Err(Error::Internal(format!(
                "Cannot fetch USD price at {activity_height:?} for {rune_id:?} ({context})"
            ))),
        },
    }
}

// Similar to build_rune_activity but for Wallet API.
// `rune_prices` is `None` when no external price service is configured, in which case all USD
// amounts are null.
pub async fn build_wallet_rune_activity(
    rune_activity: RuneTxsByScriptHashValue,
    activity_height: Height,
    rune_prices: &Option<HashMap<Height, HashMap<String, f64>>>,
    decimals_and_minting: &mut HashMap<(u64, u32), (usize, u128)>,
    tikv: &mut Extension<TiKVAdapter>,
) -> MapiResult<WalletRuneActivity> {
    let etched_rune = match rune_activity.etched {
        Some((rune_id, premined_amount)) => {
            let (dec, _) = get_decimals_and_minting(rune_id, decimals_and_minting, tikv).await?;

            Some(EtchAndPremine {
                rune_id: format!("{}:{}", rune_id.0, rune_id.1),
                premined_amount: premined_amount.map(|x| decimal(x, dec)),
            })
        }
        None => None,
    };

    let minted = match rune_activity.minted {
        Some(rune_id) => {
            let (dec, minting) =
                get_decimals_and_minting(rune_id, decimals_and_minting, tikv).await?;

            let rune_id = format!("{}:{}", rune_id.0, rune_id.1);

            Some(WalletRuneAndAmount {
                rune_id: rune_id.clone(),
                amount: decimal(minting, dec),
                usd_amount: rune_usd_amount(
                    rune_prices,
                    activity_height,
                    &rune_id,
                    minting,
                    dec,
                    "minted",
                )?,
            })
        }
        None => None,
    };

    let mut self_transfers: Vec<WalletRuneAndAmount> = vec![];
    for (rune_id, amount) in rune_activity.self_transfers {
        let (dec, _) = get_decimals_and_minting(rune_id, decimals_and_minting, tikv).await?;

        let rune_id = format!("{}:{}", rune_id.0, rune_id.1);

        self_transfers.push(WalletRuneAndAmount {
            rune_id: rune_id.clone(),
            amount: decimal(amount, dec),
            usd_amount: rune_usd_amount(
                rune_prices,
                activity_height,
                &rune_id,
                amount,
                dec,
                "self-transfers",
            )?,
        });
    }

    let mut increased_balances: Vec<WalletRuneAndAmount> = vec![];
    for (rune_id, amount) in rune_activity.increased_balances {
        let (dec, _) = get_decimals_and_minting(rune_id, decimals_and_minting, tikv).await?;

        let rune_id = format!("{}:{}", rune_id.0, rune_id.1);

        increased_balances.push(WalletRuneAndAmount {
            rune_id: rune_id.clone(),
            amount: decimal(amount, dec),
            usd_amount: rune_usd_amount(
                rune_prices,
                activity_height,
                &rune_id,
                amount,
                dec,
                "increased_balances",
            )?,
        });
    }

    let mut decreased_balances: Vec<WalletRuneAndAmount> = vec![];
    for (rune_id, amount) in rune_activity.decreased_balances {
        let (dec, _) = get_decimals_and_minting(rune_id, decimals_and_minting, tikv).await?;

        let rune_id = format!("{}:{}", rune_id.0, rune_id.1);

        decreased_balances.push(WalletRuneAndAmount {
            rune_id: rune_id.clone(),
            amount: decimal(amount, dec),
            usd_amount: rune_usd_amount(
                rune_prices,
                activity_height,
                &rune_id,
                amount,
                dec,
                "decreased_balances",
            )?,
        });
    }

    Ok(WalletRuneActivity {
        etched_rune,
        minted,
        self_transfers,
        increased_balances,
        decreased_balances,
    })
}

// If data is provided in `output_info`, then we simply unfold and return it. This helps unify
// calling this function from the different scenarios in the different endpoints.
// Conversely, if `output_info` is null, then search for `inscription_id` in one of the outputs of
// the coinbase tx of the block at `height`.
pub async fn get_inscription_coinbase_location(
    output_info: Option<([u8; 20], u32, u64)>,
    height: u64,
    tikv: &mut Extension<TiKVAdapter>,
    inscription_id: ([u8; 32], u32),
) -> MapiResult<([u8; 20], u32, u64, Option<String>)> {
    if let Some((output_script_hash, output_vout, sat_offset)) = output_info {
        // Output info is known because inscription was not spent as fee.
        Ok((output_script_hash, output_vout, sat_offset, None))
    } else {
        // Inscription was spent as fee and must be found in the coinbase tx output controlled
        // by the miner.
        let txs_in_block = tikv
            .get_reducer_key_maybe::<TxsByBlockKey, TxsByBlockValue>(
                (ReducerType::TxsByBlock, Reducer::TxsByBlock),
                &TxsByBlockKey { height },
            )
            .await?
            .ok_or(Error::Internal(
                "Unexpected - inscription spent as fee: unable to fetch txs in block.".to_string(),
            ))?;

        let coinbase_tx_hash = txs_in_block.tx_hashes.get(0).ok_or(Error::Internal(
            "Unexpected - inscription spent as fee: empty list of txs in block.".to_string(),
        ))?;

        let coinbase_tx_info = tikv
            .get_reducer_key_maybe::<TxInfoKey, TxInfoValue>(
                (ReducerType::TxInfo, Reducer::TxInfo),
                &TxInfoKey {
                    tx_hash: *coinbase_tx_hash,
                },
            )
            .await?
            .ok_or(Error::Internal(
                "Unexpected - inscription spent as fee: unable to fetch coinbase tx info."
                    .to_string(),
            ))?;

        for (output_vout, output) in coinbase_tx_info.outputs.into_iter().enumerate() {
            for (sat_offset, id) in output.inscriptions {
                if inscription_id == id {
                    return Ok((
                        output.script_hash,
                        output_vout as u32,
                        sat_offset,
                        Some(Txid::from_byte_array(*coinbase_tx_hash).to_string()),
                    ));
                }
            }
        }

        return Err(Error::Internal(
            "Unexpected - inscription spent as fee: unable to find inscription in coinbase tx."
                .to_string(),
        ));
    }
}

pub async fn estimate_indexer_blocks(
    chain_tip_height: &u64,
    found_mempool_blocks: u64,
    tikv: &mut Extension<TiKVAdapter>,
) -> MapiResult<Vec<EstimatedBlock>> {
    let mut estimated_blocks = vec![];

    for i in 0..found_mempool_blocks as u64 {
        let estimated_block_height = *chain_tip_height + (i + 1);

        let sats_per_vb_vals = tikv
            .get_reducer_key::<_, SatsPerVbByBlockValue>(
                (ReducerType::SatsPerVbByBlock, Reducer::SatsPerVbByBlock),
                &SatsPerVbByBlockKey {
                    height: estimated_block_height,
                },
            )
            .await?;

        estimated_blocks.push(EstimatedBlock {
            block_height: estimated_block_height,
            sats_per_vb: BlockSatsPerVb {
                min: sats_per_vb_vals.min,
                median: sats_per_vb_vals.median,
                max: sats_per_vb_vals.max,
            },
        })
    }

    Ok(estimated_blocks)
}

pub async fn fetch_sat_prices(
    timestamps: Vec<Timestamp>,
    external_addr: &str,
) -> MapiResult<Vec<f64>> {
    let client = reqwest::Client::new();

    let response = client
        .post(external_addr)
        .json(&serde_json::json!({
            "timestamps": &timestamps
        }))
        .send()
        .await
        .map_err(|e| Error::Internal(format!("Unable to query endpoint ({:?})", e)))?;

    let json: serde_json::Value = response
        .json()
        .await
        .map_err(|_| Error::Internal(String::from("Unable to parse response as JSON value.")))?;

    match json["data"].as_array() {
        Some(array) => Ok(array
            .iter()
            .map(|item| item["price"].as_f64().unwrap_or(0.0f64))
            .collect()),
        None => Err(Error::Internal(
            "Unable to parse JSON response as array".into(),
        )),
    }
}

pub async fn fetch_rune_prices(
    timestamp_and_runes: Vec<(Timestamp, String)>,
    external_addr: &str,
) -> MapiResult<Vec<f64>> {
    let data = timestamp_and_runes
        .into_iter()
        .map(|(timestamp, rune_id)| {
            serde_json::json!({
                "timestamp": timestamp,
                "rune_id": rune_id
            })
        })
        .collect::<Vec<_>>();

    let client = reqwest::Client::new();

    let response = client
        .post(external_addr)
        .json(&serde_json::json!({
            "data": &data
        }))
        .send()
        .await
        .map_err(|e| Error::Internal(format!("Unable to query endpoint ({:?})", e)))?;

    let json: serde_json::Value = response
        .json()
        .await
        .map_err(|_| Error::Internal(String::from("Unable to parse response as JSON value.")))?;

    match json["data"].as_array() {
        Some(array) => Ok(array
            .iter()
            .map(|item| item["price_usd"].as_f64().unwrap_or(0.0f64))
            .collect()),
        None => Err(Error::Internal(
            "Unable to parse JSON response as array".into(),
        )),
    }
}

/// Fetches comprehensive rune information for a given rune ID.
/// Returns a `RuneInfo` struct containing etching details, supply metrics, and holder counts.
pub async fn fetch_rune_info(
    rune_id: (u64, u32),
    tikv: &mut Extension<TiKVAdapter>,
) -> MapiResult<RuneInfo> {
    use ordinals::{Rune, SpacedRune};
    use timbre_xbt::reducers::{
        balances_by_rune_id::Key as BalancesByRuneIdKey, etching_by_rune_id, mints_by_rune_id,
        reducer_key_range,
    };

    let balances_encoder = tikv.get_encoder(ReducerType::BalancesByRuneId)?;

    // Fetch etching information
    let info = tikv
        .get_reducer_key::<_, etching_by_rune_id::Value>(
            (ReducerType::EtchingByRuneId, Reducer::EtchingByRuneId),
            &etching_by_rune_id::Key { rune_id },
        )
        .await?;

    let dec = info.divisibility.unwrap_or_default() as usize;

    // Fetch total mints for this rune
    let total_mints = tikv
        .get_reducer_key_maybe::<_, u128>(
            (ReducerType::MintsByRuneId, Reducer::MintsByRuneId),
            &mints_by_rune_id::Key { rune_id },
        )
        .await?;

    // Fetch rune holders count and circulating supply
    let (rune_range_lower, rune_range_upper) = reducer_key_range(
        balances_encoder.namespace(),
        &Reducer::BalancesByRuneId,
        &Some(rune_id),
        None::<u64>,
        None::<u64>,
    );

    let rune_kvs = Scanner::new(rune_range_lower..rune_range_upper)
        .execute::<BalancesByRuneIdKey, u128>(tikv, ReducerType::BalancesByRuneId)
        .await?;

    let total_holders = rune_kvs.len();
    let circulating_supply: u128 = rune_kvs.into_iter().map(|(_, x)| x).sum();

    // Calculate max supply
    let max_supply = info.premine.unwrap_or(0).saturating_add(
        info.max_mint_txs
            .unwrap_or(0)
            .saturating_mul(info.amount_per_mint.unwrap_or(0)),
    );

    let rune = info
        .name
        .map(Rune)
        .unwrap_or(Rune::reserved(rune_id.0, rune_id.1));

    let spaced_rune = SpacedRune {
        rune,
        spacers: info.spacers.unwrap_or(0),
    };

    Ok(RuneInfo {
        id: format!("{}:{}", rune_id.0, rune_id.1),
        etching_cenotaph: info.cenotaph,
        etching_tx: Txid::from_byte_array(info.tx_hash).to_string(),
        etching_height: rune_id.0,
        name: rune.to_string(),
        spaced_name: spaced_rune.to_string(),
        symbol: info.symbol.map(|x| char::from_u32(x).unwrap_or(' ')),
        divisibility: info.divisibility.unwrap_or_default(),
        premine: info.premine.map(|x| decimal(x, dec)),
        terms: Terms {
            mint_txs_cap: info.max_mint_txs.map(|x| x.to_string()),
            amount_per_mint: info.amount_per_mint.map(|x| decimal(x, dec)),
            start_height: info.start_height.map(|x| x.to_string()),
            end_height: info.end_height.map(|x| x.to_string()),
            start_offset: info.start_offset.map(|x| x.to_string()),
            end_offset: info.end_offset.map(|x| x.to_string()),
        },
        max_supply: decimal(max_supply, dec),
        circulating_supply: decimal(circulating_supply, dec),
        mints: total_mints.unwrap_or(0) as u64,
        unique_holders: total_holders as u64,
    })
}

/// Fetches brief rune information for a given rune ID.
/// Returns only essential fields including etching_cenotaph, divisibility, etching_tx, etching_height, id, name, premine, spaced_name, symbol, and terms.
/// This is more efficient for endpoints that don't need holder counts or supply metrics.
pub async fn fetch_rune_info_brief(
    rune_id: (u64, u32),
    tikv: &mut Extension<TiKVAdapter>,
) -> MapiResult<RuneInfoBrief> {
    use ordinals::{Rune, SpacedRune};
    use timbre_xbt::reducers::etching_by_rune_id;

    // Fetch etching information
    let info = tikv
        .get_reducer_key::<_, etching_by_rune_id::Value>(
            (ReducerType::EtchingByRuneId, Reducer::EtchingByRuneId),
            &etching_by_rune_id::Key { rune_id },
        )
        .await?;

    let dec = info.divisibility.unwrap_or_default() as usize;

    let rune = info
        .name
        .map(Rune)
        .unwrap_or(Rune::reserved(rune_id.0, rune_id.1));

    let spaced_rune = SpacedRune {
        rune,
        spacers: info.spacers.unwrap_or(0),
    };

    Ok(RuneInfoBrief {
        id: format!("{}:{}", rune_id.0, rune_id.1),
        etching_cenotaph: info.cenotaph,
        etching_tx: Txid::from_byte_array(info.tx_hash).to_string(),
        etching_height: rune_id.0,
        name: rune.to_string(),
        spaced_name: spaced_rune.to_string(),
        symbol: info.symbol.map(|x| char::from_u32(x).unwrap_or(' ')),
        divisibility: info.divisibility.unwrap_or_default(),
        premine: info.premine.map(|x| decimal(x, dec)),
        terms: Terms {
            mint_txs_cap: info.max_mint_txs.map(|x| x.to_string()),
            amount_per_mint: info.amount_per_mint.map(|x| decimal(x, dec)),
            start_height: info.start_height.map(|x| x.to_string()),
            end_height: info.end_height.map(|x| x.to_string()),
            start_offset: info.start_offset.map(|x| x.to_string()),
            end_offset: info.end_offset.map(|x| x.to_string()),
        },
    })
}
