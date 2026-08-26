use axum::{
    extract::{Path, Query},
    response::IntoResponse,
    Extension, Json,
};
use bitcoin::{hashes::Hash, Address, Txid};
use reqwest::StatusCode;
use std::collections::HashMap;
use std::str::FromStr;
use timbre_xbt::{
    reducers::{
        height_by_block_hash::{Key as HeightByBlockHashKey, Value as HeightByBlockHashValue},
        inscription_activity_by_tx_v2::{
            Cursor as InscriptionActivityByTxV2Cursor, Key as InscriptionActivityByTxV2Key,
            Value as InscriptionActivityByTxV2Value,
        },
        reducer_key_range,
    },
    Decode, Encode, Reducer,
};

use crate::{
    error::Error,
    options::Mode,
    tikv::{adapter::TiKVAdapter, key_resolver::ReducerType, Scanner},
    types::{
        inscriptions::{
            FromInscriptionLocation, InscriptionActivityByBlock, ToInscriptionLocation,
        },
        BlockParam, CountParam, CursorPaginationParams, PaginatedResponse,
    },
    util::{get_inscription_coinbase_location, parse_block_param, MAX_PAGE_COUNT},
};

pub static REQUIRED_REDUCERS: &[ReducerType] = &[
    ReducerType::InscriptionActivityByTxV2,
    ReducerType::HeightByBlockHash,
    // resolving script hashes
    ReducerType::ScriptByScriptHash,
    // To fetch coinbase tx hash in case inscription was spent as fee.
    ReducerType::TxsByBlock,
    // To find inscription in output of coinbase tx.
    ReducerType::TxInfo,
];

#[utoipa::path(
    tag = "Blocks",
    get,
    path = "/blocks/{height_or_hash}/inscriptions/activity",
    params(
        ("height_or_hash" = String, Path, description = "Block height or block hash", example="00000000000000000000a7d0a1dac50a909601c93774d55bebdcd4000a2af5d1"),

        ("count" = inline(Option<CountParam>), Query, description = "The max number of transactions (with inscription activity) per page"),
        ("cursor" = inline(Option<String>), Query, description = "Pagination cursor string, use the cursor included in a page of results to fetch the next page"),
    ),
    responses(
        (
            status = 200,
            description = "Requested data",
            body = PaginatedInscriptionActivityByBlock,
            example = json!(serde_json::Value::from_str(EXAMPLE_RESPONSE).unwrap())
        ),
        (status = 400, description = "Malformed query parameters"),
        (status = 404, description = "Requested entity not found on-chain"),
        (status = 500, description = "Internal server error"),
    )
)]
#[tracing::instrument(
    name = "INSCRIPTION_ACTIVITY_BY_BLOCK",
    level = "info",
    skip(tikv, mode)
)]
/// Inscription Activity by Block
///
/// List of all inscription activity in the block, ordered by transaction index in the block and by output index in the transaction.
pub async fn inscription_activity_by_block(
    page_params: Query<CursorPaginationParams>,
    Path(height_or_hash): Path<String>,
    mut tikv: Extension<TiKVAdapter>,
    mode: Extension<Mode>,
) -> Result<impl IntoResponse, Error> {
    tikv.init_tip(REQUIRED_REDUCERS).await?;

    let activity_encoder = tikv.get_encoder(ReducerType::InscriptionActivityByTxV2)?;

    // --- initialise `next_cursor`

    let mut next_cursor = None;

    // --- parse params

    let height = match parse_block_param(&height_or_hash, false)? {
        BlockParam::Hash(block_hash) => {
            tikv.get_reducer_key_maybe::<HeightByBlockHashKey, HeightByBlockHashValue>(
                (ReducerType::HeightByBlockHash, Reducer::HeightByBlockHash),
                &HeightByBlockHashKey { block_hash },
            )
            .await?
            .ok_or_else(|| Error::NotFound)?
            .block_height
        }
        BlockParam::Height(height) => height,
        BlockParam::Timestamp(_) => {
            return Err(Error::Internal(format!(
                "Timestamps are not supported for this endpoint ({})",
                height_or_hash
            )))
        }
    };

    let count = match page_params.count {
        Some(CountParam(count)) => {
            if count > MAX_PAGE_COUNT || count == 0 {
                return Err(Error::MalformedRequest("Invalid page size".into()));
            }
            count
        }
        None => MAX_PAGE_COUNT,
    };

    let cursor = if let Some(cursor) = &page_params.cursor {
        match <InscriptionActivityByTxV2Cursor>::decode_base64(&cursor) {
            Ok((cursor, _)) => (cursor.tx_index, cursor.activity_index + 1),
            Err(_) => {
                return Err(Error::MalformedRequest(
                    "Error while decoding cursor".into(),
                ))
            }
        }
    } else {
        (0, 0)
    };

    // --- fetch data

    // scan from the tx index in the cursor, but we still need to account for activity index later
    let (tx_range_lower, tx_range_upper) = reducer_key_range(
        &activity_encoder.namespace(),
        &Reducer::InscriptionActivityByTxV2,
        &Some(height),
        Some(cursor.0),
        None::<u32>,
    );

    let range = tx_range_lower..tx_range_upper;

    let mut kvs = Scanner::new(range)
        .count(count + 1)
        .execute_with_map::<InscriptionActivityByTxV2Key, InscriptionActivityByTxV2Value, _, UnfoldedValue>(
            &mut tikv,
            ReducerType::InscriptionActivityByTxV2,
            unfold_fn,
        )
        .await?
        .into_iter()
        .skip(cursor.1 as usize)
        .enumerate();

    let mut resolved_script_hashes: HashMap<[u8; 20], (Option<Address>, Vec<u8>)> = HashMap::new();

    let mut activities: Vec<InscriptionActivityByBlock> = Vec::new();

    while let Some((i, (key, value))) = kvs.next() {
        // If this is the last result of the page, check if there is a subsequent
        // result (and therefore we need to return a cursor for next page)
        if i == (count - 1) && kvs.next().is_some() {
            next_cursor = Some(
                InscriptionActivityByTxV2Cursor {
                    tx_index: key.tx_index,
                    activity_index: value.activity_index,
                }
                .encode_base64(),
            );
        }

        // Input location.
        let from = if let Some((from_address, input_index, sat_offset)) = value.input_info {
            // Inscription existed.
            let (address, script_bytes) = match resolved_script_hashes.get(&from_address) {
                Some(res) => res.clone(),
                None => {
                    let (address, script_bytes) =
                        tikv.resolve_script_hash(mode.0, from_address).await?;

                    resolved_script_hashes
                        .insert(from_address, (address.clone(), script_bytes.clone()));

                    (address, script_bytes)
                }
            };

            Some(FromInscriptionLocation {
                address: address.map(|x| x.to_string()),
                script_pubkey: hex::encode(script_bytes),
                input_index,
                sat_offset,
            })
        } else {
            // New inscription.
            None
        };

        // Output location.
        let (output_script_hash, output_vout, sat_offset, output_txid) =
            get_inscription_coinbase_location(
                value.output_info,
                height,
                &mut tikv,
                value.inscription_id,
            )
            .await?;

        let (address, script_bytes) = match resolved_script_hashes.get(&output_script_hash) {
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
            address: address.map(|x| x.to_string()),
            script_pubkey: hex::encode(script_bytes),
            output_vout,
            sat_offset,
            output_txid: output_txid.unwrap_or(Txid::from_byte_array(value.tx_hash).to_string()),
        };

        activities.push(InscriptionActivityByBlock {
            tx_hash: Txid::from_byte_array(value.tx_hash).to_string(),
            inscription_id: format!(
                "{}i{}",
                Txid::from_byte_array(value.inscription_id.0),
                value.inscription_id.1,
            ),
            from,
            to,
        });

        if next_cursor.is_some() {
            break;
        }
    }

    let out = PaginatedResponse {
        data: activities,
        last_updated: tikv.get_snapshot_point()?,
        next_cursor,
    };

    Ok((StatusCode::OK, Json(out)))
}

pub struct UnfoldedValue {
    pub tx_hash: [u8; 32],
    pub activity_index: u32,
    pub inscription_id: ([u8; 32], u32),
    pub input_info: Option<([u8; 20], u32, u64)>,
    pub output_info: Option<([u8; 20], u32, u64)>,
}

fn unfold_fn(
    key: InscriptionActivityByTxV2Key,
    value: InscriptionActivityByTxV2Value,
) -> Result<Vec<(InscriptionActivityByTxV2Key, UnfoldedValue)>, Error> {
    let mut res = vec![];

    for (activity_idx, inscription_info) in value.inscriptions_activity.iter().enumerate() {
        let (inscription_id, (input_info, output_info)) = &inscription_info;
        res.push((
            key.clone(),
            UnfoldedValue {
                tx_hash: value.tx_hash,
                activity_index: activity_idx as u32,
                inscription_id: inscription_id.clone(),
                input_info: input_info.clone(),
                output_info: output_info.clone(),
            },
        ));
    }

    Ok(res)
}

static EXAMPLE_RESPONSE: &str = r##"{
    "data": [{
        "tx_hash": "12fe2ce2d98aa43bb6184d095e8a64b9362c2fe1fee52cb5ba43f9623ef4c72b",
        "inscription_id": "56f1eebc4cfc437cffc47caa5c08f2d4f5989ed8a8c5dd6cdec77996b0e055fdi0",
        "from": {
            "address": "bc1qprdf80adfz7aekh5nejjfrp3jksc8r929svpxk",
            "script_pubkey": "001408da93bfad48bddcdaf49e65248c3195a1838caa",
            "input_index": 0,
            "sat_offset": 100409840
        },
        "to": {
            "address": "bc1qprdf80adfz7aekh5nejjfrp3jksc8r929svpxk",
            "script_pubkey": "001408da93bfad48bddcdaf49e65248c3195a1838caa",
            "output_vout": 20,
            "sat_offset": 84345292
        }
    }, {
        "tx_hash": "12fe2ce2d98aa43bb6184d095e8a64b9362c2fe1fee52cb5ba43f9623ef4c72b",
        "inscription_id": "196378911c21ee6eaac767be609106ad2efce6d36af822d54d9583a2256cb9c2i0",
        "from": {
            "address": "bc1qprdf80adfz7aekh5nejjfrp3jksc8r929svpxk",
            "script_pubkey": "001408da93bfad48bddcdaf49e65248c3195a1838caa",
            "input_index": 0,
            "sat_offset": 100639840
        },
        "to": {
            "address": "bc1qprdf80adfz7aekh5nejjfrp3jksc8r929svpxk",
            "script_pubkey": "001408da93bfad48bddcdaf49e65248c3195a1838caa",
            "output_vout": 20,
            "sat_offset": 84575292
        }
    }],
    "last_updated": {
        "block_hash": "00000000000000000000380fb362a3ad3dc257cc28daadfc83fffd0c10eb3b82",
        "block_height": 811063
    },
    "next_cursor": "ARxgAQE"
}"##;
