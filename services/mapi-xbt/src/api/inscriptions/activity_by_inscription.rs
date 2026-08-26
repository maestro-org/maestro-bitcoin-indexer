use std::collections::HashMap;

use axum::{
    extract::{Path, Query},
    response::IntoResponse,
    Extension, Json,
};
use bitcoin::{hashes::Hash, Address, Txid};
use reqwest::StatusCode;
use std::str::FromStr;
use tikv_client::KvPair;
use timbre_xbt::{
    reducers::{
        content_by_inscription_id::{
            Key as ContentByInscriptionIdKey, Value as ContentByInscriptionIdValue,
        },
        inscription_activity_by_tx_v2::{
            Key as InscriptionActivityByTxV2Key, Value as InscriptionActivityByTxV2Value,
        },
        reducer_key_range,
        txs_by_inscription::{
            Cursor as TxsByInscriptionCursor, Key as TxsByInscriptionKey,
            Value as TxsByInscriptionValue, PREFIX, PREFIX_LENGTH, SUFFIX,
        },
    },
    Decode, Encode, Reducer,
};

use crate::{
    error::Error,
    options::Mode,
    tikv::{adapter::TiKVAdapter, key_resolver::ReducerType, Scanner},
    timer::Timer,
    types::{
        inscriptions::{
            FromInscriptionLocation, InscriptionTxKind, ToInscriptionLocation, TxByInscription,
        },
        CountParam, CursorPaginationParams, OrderParam, PaginatedTxByInscription,
    },
    util::{get_inscription_coinbase_location, parse_inscription_id, MAX_PAGE_COUNT},
};

pub static REQUIRED_REDUCERS: &[ReducerType] = &[
    ReducerType::ContentByInscriptionId,
    ReducerType::InscriptionActivityByTxV2,
    ReducerType::TxsByInscription,
    // resolving script hashes
    ReducerType::ScriptByScriptHash,
    // To fetch coinbase tx hash in case inscription was spent as fee.
    ReducerType::TxsByBlock,
    // To find inscription in output of coinbase tx.
    ReducerType::TxInfo,
];

#[utoipa::path(
    tag = "Inscriptions",
    get,
    path = "/assets/inscriptions/{inscription_id}/activity",
    params(
        ("inscription_id" = String, Path, description = "Inscription ID", example="6fb976ab49dcec017f1e201e84395983204ae1a7c2abf7ced0a85d692e442799i0"),

        ("count" = inline(Option<CountParam>), Query, description = "The max number of transactions per page"),
        ("order" = inline(Option<OrderParam>), Query, description = "The order in which the results are sorted (by block height and tx index in the block)"),

        ("cursor" = inline(Option<String>), Query, description = "Pagination cursor string, use the cursor included in a page of results to fetch the next page"),
    ),
    responses(
        (
            status = 200,
            description = "Requested data",
            body = PaginatedTxByInscription,
            example = json!(serde_json::Value::from_str(EXAMPLE_RESPONSE).unwrap()),
        ),
        (status = 400, description = "Malformed query parameters"),
        (status = 404, description = "Requested entity not found on-chain"),
        (status = 500, description = "Internal server error"),
    )
)]
#[tracing::instrument(name = "ACTIVITY_BY_INSCRIPTION", level = "info", skip(tikv, mode))]
/// Activity by Inscription
///
/// Lists all transactions that have involved the given inscription, starting from its origin (reveal transaction) and including all transfers.
pub async fn activity_by_inscription(
    page_params: Query<CursorPaginationParams>,
    Path(inscription_id): Path<String>,
    mut tikv: Extension<TiKVAdapter>,
    mode: Extension<Mode>,
) -> Result<impl IntoResponse, Error> {
    let mut timer = Timer::new();

    // ---

    tikv.init_tip(REQUIRED_REDUCERS).await?;

    let txs_encoder = tikv.get_encoder(ReducerType::TxsByInscription)?;

    timer.checkpoint("initialise tikv adapter");

    // --- initialise `next_cursor`
    let mut next_cursor: Option<String> = None;

    // --- parse inscription ID
    let (reveal_tx_hash, inscription_index) = parse_inscription_id(&inscription_id)?;
    // --- reveal_tx_hash is guaranteed to be a 32-byte slice by `parse_inscription_id`,
    // --- we also know 0 < `PREFIX_LENGTH` < 32 from timbre_xbt
    let prefix: PREFIX = reveal_tx_hash[0..PREFIX_LENGTH].try_into().unwrap();
    let suffix: SUFFIX = reveal_tx_hash[PREFIX_LENGTH..].try_into().unwrap();

    // --- parse count param
    let count = match page_params.count {
        Some(CountParam(count)) => {
            if count > MAX_PAGE_COUNT || count == 0 {
                return Err(Error::MalformedRequest("Invalid page size".into()));
            }
            count
        }
        None => MAX_PAGE_COUNT,
    };

    // --- parse order param
    let order = page_params.order.unwrap_or(OrderParam::Asc);

    // --- fetch inscription info to get inscription height

    let inscription_info = tikv
        .get_reducer_key_maybe::<ContentByInscriptionIdKey, ContentByInscriptionIdValue>(
            (
                ReducerType::ContentByInscriptionId,
                Reducer::ContentByInscriptionId,
            ),
            &ContentByInscriptionIdKey {
                inscription_id: (reveal_tx_hash, inscription_index),
            },
        )
        .await;

    let inscription_height = match inscription_info {
        Ok(Some(inscription_info)) => inscription_info.created_at,
        Ok(None) => {
            // --- inscription doesn't exist, so it has no activity
            let out = PaginatedTxByInscription {
                data: vec![],
                last_updated: tikv.get_snapshot_point()?,
                next_cursor,
            };
            return Ok((StatusCode::OK, Json(out)));
        }
        Err(e) => return Err(e),
    };

    timer.checkpoint("fetch info");

    // --- parse cursor param
    let (cursor_height, cursor_tx_index) = if let Some(cursor) = &page_params.cursor {
        match <TxsByInscriptionCursor>::decode_base64(&cursor) {
            Ok((cursor, _)) => (cursor.height, cursor.tx_index),
            Err(_) => {
                return Err(Error::MalformedRequest(
                    "Error while decoding cursor".into(),
                ))
            }
        }
    } else {
        match order {
            OrderParam::Asc => (inscription_height, 0),
            OrderParam::Desc => (u64::MAX, 0),
        }
    };

    // --- items for response
    let mut inscription_activity: Vec<TxByInscription> = Vec::new();
    // --- response items counter
    let mut current_count = 0;

    // --- used to avoid resolving script hashes multiple times
    let mut resolved_script_hashes: HashMap<[u8; 20], (Option<Address>, Vec<u8>)> = HashMap::new();

    // --- range for activity in the bucket at any block height, starting at cursor

    let (lower_height, upper_height) = match order {
        OrderParam::Asc => (Some(cursor_height), None::<_>),
        OrderParam::Desc => (
            Some(inscription_height),
            Some(cursor_height.saturating_add(1)),
        ),
    };

    let (bucket_range_lower, bucket_range_upper) = reducer_key_range(
        &txs_encoder.namespace(),
        &Reducer::TxsByInscription,
        &Some(prefix),
        lower_height,
        upper_height,
    );

    // --- filter by activity of relevant inscription in block
    let key_in_block = (suffix, inscription_index);
    let filter_fn = |kv: &KvPair| {
        let (value, _) = TxsByInscriptionValue::decode(&kv.1).unwrap();
        value.activity.get(&key_in_block).is_some()
    };

    // --- blocks where the inscription has activity

    let mut blocks = Scanner::new(bucket_range_lower..bucket_range_upper)
        .count(count + 1)
        .order(order)
        .execute_with_filter::<TxsByInscriptionKey, TxsByInscriptionValue, _>(
            &mut tikv,
            ReducerType::TxsByInscription,
            Some(filter_fn),
        )
        .await?
        .into_iter();

    timer.checkpoint("fetch block kvs");

    'build_page: while let Some((key, value)) = blocks.next() {
        let height = key.height;

        let mut activity_in_block: Vec<_> = value
            .activity
            .get(&key_in_block)
            .ok_or(Error::Internal("Unexpected data inconsistency".into()))?
            .clone()
            .into();

        if order == OrderParam::Desc {
            activity_in_block.reverse()
        };

        let mut activity_in_block = activity_in_block.into_iter();

        while let Some((tx_index, activity_index)) = activity_in_block.next() {
            if height.clone() == cursor_height && tx_index < cursor_tx_index {
                // skip txs according to cursor
                continue;
            }

            if tx_index == 0 {
                // Skip coinbase txs.
                continue;
            }

            if current_count == count {
                next_cursor = Some((TxsByInscriptionCursor { height, tx_index }).encode_base64());
                break 'build_page;
            }

            let maybe_value = tikv
                .get_reducer_key_maybe::<InscriptionActivityByTxV2Key, InscriptionActivityByTxV2Value>(
                    (
                        ReducerType::InscriptionActivityByTxV2,
                        Reducer::InscriptionActivityByTxV2,
                    ),
                    &InscriptionActivityByTxV2Key { height, tx_index },
                )
                .await?;

            let tx_activity_value = match maybe_value {
                Some(value) => value,
                None => {
                    // there should be activity associated to this tx
                    return Err(Error::MissingData(
                        vec![height.encode(), tx_index.encode()].concat(),
                    ));
                }
            };

            timer.checkpoint("fetch activity kvs");

            // --- get specific activity related to the queried inscription
            let activity_in_tx: Vec<_> = tx_activity_value.inscriptions_activity;

            let (fetched_inscription_id, (input_info, output_info)) = activity_in_tx
                .get(activity_index as usize)
                // the inscription should be contained within the tx associated to it
                .ok_or(Error::MissingData(activity_index.encode()))?;

            if (reveal_tx_hash, inscription_index) != *fetched_inscription_id {
                // ID of queried inscription doesn't match inscription ID from fetched activity
                return Err(Error::Internal("Unexpected: wrong inscription ID".into()));
            }

            // Input location.
            let from = if let Some((from_address, input_vout, input_offset)) = input_info {
                // Inscription already existed.
                let (address, script_bytes) = match resolved_script_hashes.get(from_address) {
                    Some(res) => res.clone(),
                    None => {
                        let (address, script_bytes) =
                            tikv.resolve_script_hash(mode.0, *from_address).await?;

                        resolved_script_hashes
                            .insert(*from_address, (address.clone(), script_bytes.clone()));

                        (address, script_bytes)
                    }
                };
                Some(FromInscriptionLocation {
                    address: address.map(|x| x.to_string()),
                    script_pubkey: hex::encode(script_bytes),
                    input_index: *input_vout,
                    sat_offset: *input_offset,
                })
            } else {
                // New inscription.
                None
            };

            let (output_script_hash, output_vout, sat_offset, output_txid) =
                get_inscription_coinbase_location(
                    *output_info,
                    height,
                    &mut tikv,
                    (reveal_tx_hash, inscription_index),
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
                output_txid: output_txid
                    .unwrap_or(Txid::from_byte_array(tx_activity_value.tx_hash).to_string()),
            };

            timer.checkpoint("process activity");

            inscription_activity.push(TxByInscription {
                height,
                tx_index,
                tx_hash: Txid::from_byte_array(tx_activity_value.tx_hash).to_string(),
                r#type: if from.is_none() && output_info.is_none() {
                    // New inscription spent as fee.
                    InscriptionTxKind::InscribeAndSpentAsFee
                } else if from.is_none() {
                    // New inscription sent to output.
                    InscriptionTxKind::Inscribe
                } else if output_info.is_none() {
                    // Old inscription spent as fee.
                    InscriptionTxKind::SpentAsFee
                } else {
                    // Old inscription sent to output.
                    InscriptionTxKind::Transfer
                },
                from,
                to,
            });

            current_count += 1;
        }
    }

    timer.finish();

    // ---

    let out = PaginatedTxByInscription {
        data: inscription_activity,
        last_updated: tikv.get_snapshot_point()?,
        next_cursor,
    };

    Ok((StatusCode::OK, Json(out)))
}

static EXAMPLE_RESPONSE: &str = r##"{
  "data": [
    {
      "height": 839418,
      "tx_index": 1257,
      "tx_hash": "b1ef66c2d1a047cbaa6260b74daac43813924378fe08ef8545da4cb79e8fcf00",
      "type": "transfer",
      "from": {
        "address": "bc1pjzf5qmmzt57mtxgrgh42aazhnzwk7ge59e89dl6rde5zwg8he02q9u4xrr",
        "script_pubkey": "51209093406f625d3db5990345eaaef457989d6f23342e4e56ff436e682720f7cbd4",
        "input_index": 0,
        "sat_offset": 0
      },
      "to": {
        "address": "bc1p5u4y8vdhn46adxhfv5scfv4c8myykw6r5uyzlavm42k4wgjewktq7xqcyr",
        "script_pubkey": "5120a72a43b1b79d75d69ae9652184b2b83ec84b3b43a7082ff59baaad5722597596",
        "output_vout": 0,
        "sat_offset": 0
      }
    },
    {
      "height": 839876,
      "tx_index": 887,
      "tx_hash": "47c7260764af2ee17aa584d9c035f2e5429aefd96b8016cfe0e3f0bcf04869a3",
      "type": "transfer",
      "from": {
        "address": "bc1p5u4y8vdhn46adxhfv5scfv4c8myykw6r5uyzlavm42k4wgjewktq7xqcyr",
        "script_pubkey": "5120a72a43b1b79d75d69ae9652184b2b83ec84b3b43a7082ff59baaad5722597596",
        "input_index": 0,
        "sat_offset": 0
      },
      "to": {
        "address": "bc1ppth27qnr74qhusy9pmcyeaelgvsfky6qzquv9nf56gqmte59vfhqwkqguh",
        "script_pubkey": "51200aeeaf0263f5417e40850ef04cf73f43209b13401038c2cd34d201b5e685626e",
        "output_vout": 0,
        "sat_offset": 0
      }
    }
  ],
  "last_updated": {
    "block_hash": "000000000000000000001d3fb5743dcb30e12af7b0cd9c8d95c8b1a1fdd5c8d8",
    "block_height": 839908
  },
  "next_cursor": null
}"##;
