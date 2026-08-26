use std::collections::HashMap;

use axum::{
    extract::{Path, Query},
    response::IntoResponse,
    Extension, Json,
};
use bitcoin::{hashes::Hash, Address, Txid};
use reqwest::StatusCode;
use std::str::FromStr;
use timbre_xbt::{
    reducers::{
        block_by_tx_hash::{Key as BlockByTxHashKey, Value as BlockByTxHashValue},
        inscription_activity_by_tx_v2::{
            Key as InscriptionActivityByTxV2Key, Value as InscriptionActivityByTxV2Value,
        },
        txs_by_block::{Key as TxsByBlockKey, Value as TxsByBlockValue},
    },
    Decode, Encode, Reducer,
};

use crate::{
    error::Error,
    options::Mode,
    tikv::{adapter::TiKVAdapter, key_resolver::ReducerType},
    types::{
        inscriptions::{FromInscriptionLocation, InscriptionActivityByTx, ToInscriptionLocation},
        CountParam, CursorPaginationParams, PaginatedResponse,
    },
    util::{get_inscription_coinbase_location, parse_tx_hash, MAX_PAGE_COUNT},
};

pub static REQUIRED_REDUCERS: &[ReducerType] = &[
    ReducerType::InscriptionActivityByTxV2,
    ReducerType::BlockByTxHash,
    // resolving script hashes
    ReducerType::ScriptByScriptHash,
    // To fetch coinbase tx hash in case inscription was spent as fee.
    ReducerType::TxsByBlock,
    // To find inscription in output of coinbase tx.
    ReducerType::TxInfo,
];

#[utoipa::path(
    tag = "Transactions",
    get,
    path = "/transactions/{tx_hash}/inscriptions/activity",
    params(
        ("tx_hash" = String, Path, description = "Transaction hash", example="06244b1eb209becb440c924c62e0290c210e749d0491ad6cf134c98d23082025"),

        ("count" = inline(Option<CountParam>), Query, description = "The max number of inscriptions per page"),
        ("cursor" = inline(Option<String>), Query, description = "Pagination cursor string, use the cursor included in a page of results to fetch the next page"),
    ),
    responses(
        (
            status = 200,
            description = "Requested data",
            body = PaginatedInscriptionActivityByTx,
            example = json!(serde_json::Value::from_str(EXAMPLE_RESPONSE).unwrap())
        ),
        (status = 400, description = "Malformed query parameters"),
        (status = 404, description = "Requested entity not found on-chain"),
        (status = 500, description = "Internal server error"),
    )
)]
#[tracing::instrument(name = "INSCRIPTION_ACTIVITY_BY_TX", level = "info", skip(tikv, mode))]
/// Inscription Activity by Transaction
///
/// List of all inscription activity in the transaction, including their satoshi-level positioning within transactions, ordered by transaction output index. The list of inscriptions is truncated to a maximum of 10,000 inscriptions.
pub async fn inscription_activity_by_tx(
    page_params: Query<CursorPaginationParams>,
    Path(tx_hash): Path<String>,
    mut tikv: Extension<TiKVAdapter>,
    mode: Extension<Mode>,
) -> Result<impl IntoResponse, Error> {
    tikv.init_tip(REQUIRED_REDUCERS).await?;

    // Initialise `next_cursor`.
    let mut next_cursor: Option<String> = None;

    // Parse count and cursor params.
    let (height, tx_index) = {
        let tx_hash = parse_tx_hash(&tx_hash)?;

        let height = tikv
            .get_reducer_key_maybe::<BlockByTxHashKey, BlockByTxHashValue>(
                (ReducerType::BlockByTxHash, Reducer::BlockByTxHash),
                &BlockByTxHashKey { tx_hash },
            )
            .await?
            .ok_or_else(|| Error::NotFound)?
            .height;

        let index = tikv
            .get_reducer_key::<TxsByBlockKey, TxsByBlockValue>(
                (ReducerType::TxsByBlock, Reducer::TxsByBlock),
                &TxsByBlockKey { height },
            )
            .await?
            .tx_hashes
            .iter()
            .position(|hash| *hash == tx_hash)
            .ok_or_else(|| Error::Internal("missing tx hash in block txs".into()))?;

        (height, index as u32)
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
        match <u64>::decode_base64(&cursor) {
            Ok((cursor, _)) => cursor as usize,
            Err(_) => {
                return Err(Error::MalformedRequest(
                    "Error while decoding cursor".into(),
                ))
            }
        }
    } else {
        0usize
    };

    // Fetch inscription activity in tx.
    let maybe_value = tikv
        .get_reducer_key_maybe::<InscriptionActivityByTxV2Key, InscriptionActivityByTxV2Value>(
            (
                ReducerType::InscriptionActivityByTxV2,
                Reducer::InscriptionActivityByTxV2,
            ),
            &InscriptionActivityByTxV2Key { height, tx_index },
        )
        .await?;

    let value = match maybe_value {
        Some(value) => value,
        None => {
            // No inscription activity for this transaction.
            let out = PaginatedResponse {
                data: vec![],
                last_updated: tikv.get_snapshot_point()?,
                next_cursor,
            };

            return Ok((StatusCode::OK, Json(out)));
        }
    };

    let tx_hash = value.tx_hash;

    let mut inscriptions = value.inscriptions_activity;

    // Filter by cursor.
    let mut inscriptions = inscriptions.split_off(cursor).into_iter().enumerate();

    let mut resolved_script_hashes: HashMap<[u8; 20], (Option<Address>, Vec<u8>)> = HashMap::new();

    let mut inscriptions_activity = Vec::new();

    while let Some((i, (inscription_id, (input_info, output_info)))) = inscriptions.next() {
        // If this is the last result of the page, check if there is a subsequent
        // result (and therefore we need to return a cursor for next page).
        if i == count - 1 && inscriptions.next().is_some() {
            next_cursor = Some(((cursor + i + 1) as u64).encode_base64());
        }

        // Input location.
        let from = if let Some((from_address, input_index, sat_offset)) = input_info {
            // Inscription already existed.
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
            get_inscription_coinbase_location(output_info, height, &mut tikv, inscription_id)
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
            output_txid: output_txid.unwrap_or(Txid::from_byte_array(tx_hash).to_string()),
        };

        inscriptions_activity.push(InscriptionActivityByTx {
            inscription_id: format!(
                "{}i{}",
                Txid::from_byte_array(inscription_id.0),
                inscription_id.1,
            ),
            from,
            to,
        });

        if next_cursor.is_some() {
            break;
        }
    }

    let out = PaginatedResponse {
        data: inscriptions_activity,
        last_updated: tikv.get_snapshot_point()?,
        next_cursor,
    };

    Ok((StatusCode::OK, Json(out)))
}

static EXAMPLE_RESPONSE: &str = r##"{
    "data": [{
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
    "next_cursor": "AAAAAAAAAAI"
}"##;
