use axum::{extract::Path, response::IntoResponse, Extension, Json};
use bitcoin::{hashes::Hash, Txid};
use reqwest::StatusCode;
use std::str::FromStr;
use timbre_xbt::{
    reducers::{
        content_by_inscription_id::{Key as InscriptionInfoKey, Value as InscriptionInfoValue},
        inscription_activity_by_tx_v2::{
            Key as InscriptionActivityByTxV2Key, Value as InscriptionActivityByTxV2Value,
        },
        reducer_key_range,
        txs_by_inscription::{
            Key as TxsByInscriptionKey, Value as TxsByInscriptionValue, PREFIX, PREFIX_LENGTH,
            SUFFIX,
        },
    },
    CollectionIngestor, Reducer,
};

use crate::{
    error::Error,
    options::Mode,
    tikv::{adapter::TiKVAdapter, key_resolver::ReducerType, Scanner},
    types::{
        collections::{CollectionMetadata, InscriptionId, TokenMetadata},
        inscriptions::{InscriptionInfo, InscriptionLocation},
        OrderParam, TimestampedResponse,
    },
    util::{get_inscription_coinbase_location, parse_inscription_id, MAX_CONTENT_PREVIEW},
};

pub static REQUIRED_REDUCERS: &[ReducerType] = &[
    ReducerType::ContentByInscriptionId,
    ReducerType::InscriptionActivityByTxV2,
    ReducerType::ScriptByScriptHash,
    ReducerType::TxsByInscription,
    // To fetch coinbase tx hash in case inscription was spent as fee.
    ReducerType::TxsByBlock,
    // To find inscription in output of coinbase tx.
    ReducerType::TxInfo,
];

#[utoipa::path(
    tag = "Inscriptions",
    get,
    path = "/assets/inscriptions/{inscription_id}",
    params(
        ("inscription_id" = String, Path, description = "Inscription ID", example="43cb5e2d66f5af8eb04391ef3d4048edc30a1f9edad594f83609fd7861a0f5e1i0"),
    ),
    responses(
        (
            status = 200,
            description = "Requested data",
            body = TimestampedInscriptionInfo,
            example = json!(serde_json::Value::from_str(EXAMPLE_RESPONSE).unwrap())
        ),
        (status = 400, description = "Malformed query parameters"),
        (status = 404, description = "Requested entity not found on-chain"),
        (status = 500, description = "Internal server error"),
    )
)]
#[tracing::instrument(name = "INSCRIPTION_INFO", level = "info", skip(tikv, mode))]
/// Inscription Info
///
/// Delivers information about a specific inscription, including its type, current location (UTXO and address), associated collection, and metadata like size and content preview (if text-based). Supports resolution of any inscription for wallet or explorer views.
/// A preview of the content body is given only if its type is `"text/plain"`. For the whole content, use the complementary endpoint, namely `/assets/inscriptions/{inscription_id}/content_body`.
pub async fn inscription_info(
    Path(inscription_id_str): Path<String>,
    mut tikv: Extension<TiKVAdapter>,
    mode: Extension<Mode>,
) -> Result<impl IntoResponse, Error> {
    tikv.init_tip(REQUIRED_REDUCERS).await?;

    let txs_encoder = tikv.get_encoder(ReducerType::TxsByInscription)?;

    tikv.with_collection_metadata().await?;

    // --- parse user params
    let (reveal_tx_hash, inscription_index) = parse_inscription_id(&inscription_id_str)?;
    let prefix: PREFIX = reveal_tx_hash[0..PREFIX_LENGTH].try_into().unwrap();
    let suffix: SUFFIX = reveal_tx_hash[PREFIX_LENGTH..].try_into().unwrap();

    // --- token metadata ID
    let inscription_id = InscriptionId {
        reveal_tx_hash,
        inscription_index,
    };

    let maybe_token_metadata = tikv
        .get_collection_key_maybe::<InscriptionId>(
            &CollectionIngestor::TokenMetadataByInscription,
            &inscription_id,
        )
        .await?;

    let (collection_symbol, inscription_number) = match maybe_token_metadata {
        Some(json_bytes) => {
            let data: TokenMetadata = serde_json::from_slice(&json_bytes)?;
            (
                Some(data.collection_symbol),
                Some(data.inscription_number as u64),
            )
        }
        // The token metadata record ('M') is a legacy record written by an earlier
        // metadata ingestor and nothing writes it now. Fall back to the collection
        // record ('C'), which is keyed by the same inscription id, so the symbol
        // still resolves for anything ingested since.
        //
        // Inscription number is deliberately left absent here: it exists nowhere but 'M'
        // -- the collection records carry only the min/max for the collection, and the
        // on-chain reducers have no notion of inscription numbering.
        None => {
            let maybe_collection_metadata = tikv
                .get_collection_key_maybe::<InscriptionId>(
                    &CollectionIngestor::MetadataByInscription,
                    &inscription_id,
                )
                .await?;

            match maybe_collection_metadata {
                Some(json_bytes) => {
                    let data: CollectionMetadata = serde_json::from_slice(&json_bytes)?;
                    (Some(data.symbol), None)
                }
                None => (None, None),
            }
        }
    };

    // --- fetch inscription info
    let inscription_info = tikv
        .get_reducer_key_maybe::<InscriptionInfoKey, InscriptionInfoValue>(
            (
                ReducerType::ContentByInscriptionId,
                Reducer::ContentByInscriptionId,
            ),
            &InscriptionInfoKey {
                inscription_id: (reveal_tx_hash, inscription_index),
            },
        )
        .await?
        .ok_or_else(|| Error::NotFound)?;

    // Attempt to find current location by fetching inscription latest activity. If the output
    // location in such activity is `None`, then it must be that the inscription was spent as fee in
    // the same block. In such a case, we need to fetch `TxInfo` KV for coinbase tx in that block,
    // find the inscription in one of its outputs (which is always expected to be found), and
    // finally build the location with that data.
    let current_location = {
        // Scan entire block range starting from block containing the inscribing tx.
        let (bucket_range_lower, bucket_range_upper) = reducer_key_range(
            &txs_encoder.namespace(),
            &Reducer::TxsByInscription,
            &Some(prefix),
            Some(inscription_info.created_at),
            None::<u64>,
        );

        // Unfold fn used by `execute_with_map`.
        let key_in_block = (suffix, inscription_index);
        let unfold_fn = |key: TxsByInscriptionKey, value: TxsByInscriptionValue| {
            if let Some(activity_in_block) = value.activity.get(&key_in_block) {
                let mut activity = vec![];

                for (tx_index, activity_index) in activity_in_block.iter() {
                    activity.push((*tx_index, *activity_index));
                }

                Ok(vec![(key, activity)])
            } else {
                Ok(vec![])
            }
        };

        // Fetch only one activity list entry (in descendant order last, which gives block where the
        // inscription had activity).
        let block: Vec<(TxsByInscriptionKey, Vec<(u32, u32)>)> =
            Scanner::new(bucket_range_lower..bucket_range_upper)
                .count(1)
                .order(OrderParam::Desc)
                .execute_with_map::<TxsByInscriptionKey, TxsByInscriptionValue, _, Vec<(u32, u32)>>(
                    &mut tikv,
                    ReducerType::TxsByInscription,
                    unfold_fn,
                )
                .await?;

        let (key, latest_activity) = block
            .get(0)
            .ok_or(Error::Internal("Unable to find bucket in block".into()))?;

        // Get tx index and activity index from last entry in the block.
        let (tx_index, activity_index) = latest_activity.last().ok_or(Error::Internal(
            "Unexpected: empty activity list for the queried inscription".into(),
        ))?;

        // Fetch inscription activity data using the tx index to fetch data from the store and the
        // activity index to find the last activity
        let tx_activity = tikv
            .get_reducer_key_maybe::<InscriptionActivityByTxV2Key, InscriptionActivityByTxV2Value>(
                (
                    ReducerType::InscriptionActivityByTxV2,
                    Reducer::InscriptionActivityByTxV2,
                ),
                &InscriptionActivityByTxV2Key {
                    height: key.height,
                    tx_index: *tx_index,
                },
            )
            .await?
            .ok_or_else(|| {
                Error::Internal("Unexpected: tx without inscription acitivity".into())
            })?;

        let &(id, (_, output_location)) = tx_activity
            .inscriptions_activity
            .get(*activity_index as usize)
            .ok_or(Error::Internal(
                "Unexpected: inscription activity index doesn't exist".into(),
            ))?;

        // Sanity check.
        if id != (reveal_tx_hash, inscription_index) {
            return Err(Error::Internal(format!("Unexpected: wrong inscription ID")));
        }

        if let Some((to_address, utxo_vout, utxo_sat_offset)) = output_location {
            // Inscription was not spent as fee. Build inscription current location.
            let (address, script_bytes) = tikv.resolve_script_hash(mode.0, to_address).await?;

            InscriptionLocation {
                address: address.map(|x| x.to_string()),
                script_pubkey: hex::encode(script_bytes),
                utxo_sat_offset,
                utxo_txid: Txid::from_byte_array(tx_activity.tx_hash).to_string(),
                utxo_vout,
            }
        } else {
            // Inscription was spent as fee and must be found in the coinbase tx output controlled
            // by the miner.
            let (output_script_hash, utxo_vout, utxo_sat_offset, utxo_txid) =
                get_inscription_coinbase_location(
                    None,
                    key.height,
                    &mut tikv,
                    (reveal_tx_hash, inscription_index),
                )
                .await?;

            // Resolve address and return location.
            let (address, script_bytes) =
                tikv.resolve_script_hash(mode.0, output_script_hash).await?;

            InscriptionLocation {
                address: address.map(|x| x.to_string()),
                script_pubkey: hex::encode(script_bytes),
                utxo_sat_offset,
                utxo_txid: utxo_txid.ok_or(Error::Internal(
                    "Unexpected: inscription should have been sent to coinbase tx.".into(),
                ))?,
                utxo_vout,
            }
        }
    };

    let (content_type, content_length, content_body_preview) =
        match String::from_utf8(inscription_info.content_type.clone()) {
            Ok(ct) => {
                match ct.split(";").next() {
                    // Type was correctly parsed and preview can be provided
                    Some("text/plain") | Some("application/json") => {
                        let mut content_body = inscription_info.content_body.clone();
                        let length = content_body.len();
                        // truncate content body preview
                        content_body.truncate(MAX_CONTENT_PREVIEW);
                        (
                            Some(ct),
                            length as u64,
                            Some(String::from_utf8_lossy(&content_body).to_string()),
                        )
                    }
                    // Unsupported type for providing content preview
                    Some(_) | None => (Some(ct), inscription_info.content_body.len() as u64, None),
                }
            }
            // Type wasn't correctly parsed
            _ => (None, inscription_info.content_body.len() as u64, None),
        };

    let out = TimestampedResponse {
        data: InscriptionInfo {
            inscription_id: inscription_id_str,
            inscription_number,
            created_at: inscription_info.created_at,
            current_location,
            content_type,
            content_body_preview,
            content_length,
            collection_symbol,
        },
        last_updated: tikv.get_snapshot_point()?,
    };

    Ok((StatusCode::OK, Json(out)))
}

static EXAMPLE_RESPONSE: &str = r##"{
    "data": {
        "inscription_id": "43cb5e2d66f5af8eb04391ef3d4048edc30a1f9edad594f83609fd7861a0f5e1i0",
        "inscription_number": 76800550,
        "created_at": 867795,
        "current_location": {
            "address": "bc1pyae5wn0hxwestrxg4vmwrw58stwzkhp64le0uulahj8xzp5ruz8s3m6xh7",
            "script_pubkey": "51202773474df733b3058cc8ab36e1ba8782dc2b5c3aaff2fe73fdbc8e610683e08f",
            "utxo_sat_offset": 0,
            "utxo_txid": "471852c0dff23a9d345a4550db8a5ff1a9cd0c1ff8e91f344762fba9a1bf000f",
            "utxo_vout": 1
        },
        "content_type": "text/html",
        "content_body_preview": null,
        "content_length": 934,
        "collection_symbol": "kikalepuppete"
    },
    "last_updated": {
        "block_hash": "000000000000000000018ebea33e7361bc4059eb6ff35d0a80836717accfc4a9",
        "block_height": 888637
    }
}"##;
