use axum::{
    extract::{Path, Query},
    response::IntoResponse,
    Extension, Json,
};
use bitcoin::{hashes::Hash, Script, Txid};
use reqwest::StatusCode;
use serde::Deserialize;
use std::collections::HashMap;
use std::str::FromStr;
use tikv_client::KvPair;
use timbre_xbt::{
    reducers::{
        etching_by_rune_id, reducer_key_range,
        rune_utxos_by_script_hash::{
            Cursor as RuneUtxosByScriptHashCursor, Key as RuneUtxosByScriptHashKey,
            Value as RuneUtxosByScriptHashValue,
        },
    },
    Decode, Encode, Reducer,
};

use crate::{
    error::Error,
    tikv::{adapter::TiKVAdapter, key_resolver::ReducerType, Scanner},
    types::{
        runes::RuneUtxoByAddress, CountParam, HeightPaginationParams, MempoolLastUpdated,
        MempoolPaginatedResponse, OrderBy, OrderParam, RuneAndAmount,
    },
    util::{decimal, estimate_indexer_blocks, timestamp_to_string, RuneIdentifier, MAX_PAGE_COUNT},
};

#[derive(Debug, Deserialize)]
pub struct RuneKindParam {
    pub rune: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct OrderByParam {
    pub order_by: Option<OrderBy>,
}

pub static REQUIRED_REDUCERS: &[ReducerType] = &[
    ReducerType::RuneUtxosByScriptHash,
    ReducerType::RuneIdByRuneName,
    ReducerType::EtchingByRuneId,
    ReducerType::ScriptByScriptHash,
    ReducerType::ScriptHashByAddressPayloadHash,
    // Estimated block fees.
    ReducerType::SatsPerVbByBlock,
];

#[utoipa::path(
    tag = "Addresses",
    get,
    path = "/mempool/addresses/{address}/runes/utxos",
    params(
        ("address" = String, Path, description = "Bitcoin address or hex encoded script pubkey", example=""),

        // Filter by rune.
        ("rune" = Option<String>, Query, description = "Return only UTxOs containing a specific Rune, specified either by the Rune ID (etching block number and transaction index) or name (spaced or un-spaced)", example="840000:3"),

        // Sort by amount order.
        ("order_by" = Option<OrderBy>, Query, description = "The property by which response items should be sorted. Supported values: height (height of block which produced the UTxO - default), amount (amount of runes in UTxO)"),

        // Sort by storage order.
        ("order" = inline(Option<OrderParam>), Query, description = "The order in which the results are sorted. Supported values: asc, desc"),

        // Pagination params applicable regardless of the sorting order.
        ("count" = inline(Option<CountParam>), Query, description = "The max number of results per page"),
        ("cursor" = inline(Option<String>), Query, description = "Pagination cursor string, use the cursor included in a page of results to fetch the next page."),
        ("from" = inline(Option<u64>), Query, description = "Return only UTxOs created on or after a specific height"),
        ("to" = inline(Option<u64>), Query, description = "Return only UTxOs created on or before a specific height"),
    ),
    responses(
        (
            status = 200,
            description = "Requested data",
            body = MempoolPaginatedRuneUtxoByAddress,
            example = json!(serde_json::Value::from_str(EXAMPLE_RESPONSE).unwrap())
        ),
        (status = 400, description = "Malformed query parameters"),
        (status = 404, description = "Requested entity not found on-chain"),
        (status = 500, description = "Internal server error"),
    )
)]
#[tracing::instrument(name = "MEMPOOL_RUNE_UTXOS_BY_ADDRESS", level = "info", skip(tikv))]
/// Rune UTxOs by Address (mempool-aware)
///
/// Lists all UTXOs at the address or script pubkey that contains Rune tokens, with optional refinement based on Rune type or metadata. Helpful for spend analysis or wallet state audits.
///
/// In addition to confirmed transactions, mempool endpoints return data which reflects pending transactions in some number of "estimated" blocks - predicted blocks containing transactions which have been propagated around the network but not yet included in a mined block, with transactions with a higher sat/vB value being prioritised. The response details how many of these estimated blocks were considered when fetching the data.
pub async fn mempool_rune_utxos_by_address(
    Path(addr_or_pk): Path<String>,
    rune_kind: Query<RuneKindParam>,
    order_by_param: Query<OrderByParam>,
    page_params: Query<HeightPaginationParams>,
    mut tikv: Extension<TiKVAdapter>,
) -> Result<impl IntoResponse, Error> {
    tikv.init_mempool(REQUIRED_REDUCERS, None).await?;

    let snapshot_chain_tip = tikv.get_snapshot_point()?;
    let snapshot_mempool_view = tikv.get_snapshot_mempool_info()?;
    let found_mempool_blocks = snapshot_mempool_view.map(|x| x.mempool_blocks).unwrap_or(0);

    // Initialise `next_cursor`.
    let mut next_cursor = None;

    let rune_utxos_encoder = tikv.get_encoder(ReducerType::RuneUtxosByScriptHash)?;

    // Parse and try decode user params.
    let script_bytes = match tikv.parse_address_or_script_bytes(&addr_or_pk).await {
        Ok((_, script_bytes)) => script_bytes,
        Err(Error::NotFound) => {
            // User param is a Bitcoin address, but the corresponding script pub key could not be
            // found in store.
            let out = MempoolPaginatedResponse {
                data: vec![],
                indexer_info: MempoolLastUpdated {
                    chain_tip: snapshot_chain_tip.clone(),
                    mempool_timestamp: snapshot_mempool_view
                        .map(|x| timestamp_to_string(x.mempool_view_ts)),
                    estimated_blocks: estimate_indexer_blocks(
                        &snapshot_chain_tip.block_height,
                        found_mempool_blocks as u64,
                        &mut tikv,
                    )
                    .await?,
                },
                next_cursor,
            };

            return Ok((StatusCode::OK, Json(out)));
        }
        Err(e) => return Err(e),
    };
    let script = Script::from_bytes(&script_bytes);
    let script_hash = script.script_hash().to_byte_array();

    let rune_filter = if let Some(rune_str) = &rune_kind.rune {
        match RuneIdentifier::parse(rune_str.clone())? {
            RuneIdentifier::Id(rune_id) => Some(rune_id),
            RuneIdentifier::Name(n) => Some(tikv.resolve_rune_name(n).await?.unwrap_or_default()),
        }
    } else {
        None
    };

    let from_param = page_params.0.from;
    let to_param = page_params.0.to;
    let count = page_params.count.map(|x| x.0).unwrap_or(MAX_PAGE_COUNT);
    let order = page_params.order.unwrap_or(crate::types::OrderParam::Asc);
    let cursor_param = page_params.0.cursor;

    if count > MAX_PAGE_COUNT || count == 0 {
        return Err(Error::MalformedRequest("Invalid page size".into()));
    }

    let cursor = if let Some(encoded_cursor) = cursor_param {
        Some(
            RuneUtxosByScriptHashCursor::decode_base64(&encoded_cursor)
                .map_err(|_| Error::MalformedRequest("Malformed cursor: unable to decode".into()))?
                .0,
        )
    } else {
        None
    };

    let (mut range_lower, mut range_upper) = reducer_key_range(
        &rune_utxos_encoder.namespace(),
        &Reducer::RuneUtxosByScriptHash,
        &Some(script_hash),
        from_param,
        to_param.map(|x| x.saturating_add(1)),
    );

    // -----

    let page_kvs: Vec<_> = match order_by_param.order_by {
        None | Some(OrderBy::Height) => {
            // restrict key range using cursor
            match cursor {
                Some(RuneUtxosByScriptHashCursor::ByHeight {
                    height,
                    utxo_hash,
                    utxo_index,
                }) => {
                    let cursor_key = RuneUtxosByScriptHashKey {
                        script_hash,
                        height,
                        utxo_hash,
                        utxo_index,
                    };

                    let mut cursor_key =
                        rune_utxos_encoder.data(&Reducer::RuneUtxosByScriptHash, &cursor_key);

                    if !(range_lower <= cursor_key && cursor_key <= range_upper) {
                        return Err(Error::MalformedRequest(
                            "Malformed cursor: invalid for height range".into(),
                        ));
                    }

                    // if ascending, increase cursor key by 1 lexicographically to avoid including
                    // cursor kv in scanned keys (upper bound is exclusive)
                    if order == OrderParam::Asc {
                        cursor_key.push(0x00);

                        range_lower = cursor_key;
                    } else {
                        range_upper = cursor_key
                    }
                }
                None => (),
                Some(RuneUtxosByScriptHashCursor::ByAmount { .. }) => {
                    return Err(Error::MalformedRequest(
                        "Error while decoding cursor: wrong cursor type.".into(),
                    ))
                }
            }

            // create filter function to ignore utxos which don't contain rune filter, if one was
            // provided
            let filter = if let Some(rune) = rune_filter {
                Some(move |kv: &KvPair| {
                    RuneUtxosByScriptHashValue::decode(&kv.1)
                        .unwrap()
                        .0
                        .runes
                        .iter()
                        .map(|(x, _)| x)
                        .position(|x| *x == rune)
                        .is_some()
                })
            } else {
                None
            };

            let kvs = Scanner::new(range_lower..range_upper)
                .count(count + 1)
                .order(order)
                .execute_with_filter::<RuneUtxosByScriptHashKey, RuneUtxosByScriptHashValue, _>(
                    &mut tikv,
                    ReducerType::RuneUtxosByScriptHash,
                    filter,
                )
                .await?;

            kvs.into_iter().map(|(k, v)| (k, v, None)).collect()
        }
        Some(OrderBy::Amount) => {
            let Some(rune_id) = rune_filter else {
                return Err(Error::MalformedRequest(
                    "Order by amount only supported if rune is specified.".into(),
                ));
            };

            let cursor = match cursor {
                Some(RuneUtxosByScriptHashCursor::ByAmount {
                    amount,
                    utxo_hash,
                    utxo_index,
                }) => Some((amount, utxo_hash, utxo_index)),
                None => None,
                Some(RuneUtxosByScriptHashCursor::ByHeight { .. }) => {
                    return Err(Error::MalformedRequest(
                        "Error while decoding cursor: wrong cursor type.".into(),
                    ))
                }
            };

            let mut lexico_kvs = Scanner::new(range_lower..range_upper)
                .execute_with_map::<RuneUtxosByScriptHashKey, RuneUtxosByScriptHashValue, _, (u128, RuneUtxosByScriptHashValue)>(
                    &mut tikv,
                    ReducerType::RuneUtxosByScriptHash,
                    by_amount_unfold_fn(rune_id, cursor.map(|x| (x, order))),
                )
                .await?;

            // lexico_kvs is kvs filtered by cursor+order, we just need to sort by amount/utxo then
            // reflect ordering direction

            lexico_kvs.sort_by_key(|(k, (amount, _))| (*amount, k.utxo_hash, k.utxo_index));

            if order == OrderParam::Desc {
                lexico_kvs.reverse();
            }

            lexico_kvs
                .into_iter()
                .map(|(k, (amount, v))| (k, v, Some(amount)))
                .take(count + 1)
                .collect()
        }
    };

    let mut kvs = page_kvs.into_iter().enumerate();

    // `rune_decimals` is used to avoid re-fetching etching terms for rune kinds that
    // were already seen.
    let mut rune_decimals: HashMap<(u64, u32), usize> = HashMap::new();

    let mut utxos: Vec<RuneUtxoByAddress> = vec![];

    // Process fetched kvs.
    while let Some((i, (key, value, amount))) = kvs.next() {
        // If this is the last result of the page, check if there is a subsequent
        // result (and therefore we need to return a cursor for next page)
        // Note that this cursor is only consistent with subsequent queries when combined with the
        // same sorting order (either by-amount or by-height).
        if i == (count - 1) && kvs.next().is_some() {
            next_cursor = if let Some(x) = amount {
                Some(
                    RuneUtxosByScriptHashCursor::ByAmount {
                        amount: x,
                        utxo_hash: key.utxo_hash,
                        utxo_index: key.utxo_index,
                    }
                    .encode_base64(),
                )
            } else {
                Some(
                    RuneUtxosByScriptHashCursor::ByHeight {
                        height: key.height,
                        utxo_hash: key.utxo_hash,
                        utxo_index: key.utxo_index,
                    }
                    .encode_base64(),
                )
            };
        }

        let mut runes = vec![];

        // Get rune divisibility.
        for (rid, amount) in value.runes.into_iter() {
            let dec = match rune_decimals.get(&rid) {
                Some(dec) => *dec,
                None => {
                    let dec = tikv
                        .get_reducer_key::<_, etching_by_rune_id::Value>(
                            (ReducerType::EtchingByRuneId, Reducer::EtchingByRuneId),
                            &etching_by_rune_id::Key { rune_id: rid },
                        )
                        .await?
                        .divisibility
                        .unwrap_or(0) as usize;

                    rune_decimals.insert(rid, dec);

                    dec
                }
            };

            runes.push(RuneAndAmount {
                rune_id: format!("{}:{}", rid.0, rid.1),
                amount: decimal(amount, dec),
            });
        }

        let height = key.height;

        utxos.push(RuneUtxoByAddress {
            txid: Txid::from_byte_array(key.utxo_hash).to_string(),
            vout: key.utxo_index,
            satoshis: value.satoshis.to_string(),
            confirmations: (snapshot_chain_tip.block_height + 1).saturating_sub(height),
            height,
            runes,
        });
    }

    let out = MempoolPaginatedResponse {
        data: utxos,
        indexer_info: MempoolLastUpdated {
            chain_tip: snapshot_chain_tip.clone(),
            mempool_timestamp: snapshot_mempool_view
                .map(|x| timestamp_to_string(x.mempool_view_ts)),
            estimated_blocks: estimate_indexer_blocks(
                &snapshot_chain_tip.block_height,
                found_mempool_blocks as u64,
                &mut tikv,
            )
            .await?,
        },
        next_cursor,
    };

    Ok((StatusCode::OK, Json(out)))
}

fn by_amount_unfold_fn(
    rune_id: (u64, u32),
    cursor_and_order: Option<((u128, [u8; 32], u32), OrderParam)>,
) -> impl Fn(
    RuneUtxosByScriptHashKey,
    RuneUtxosByScriptHashValue,
) -> Result<Vec<(RuneUtxosByScriptHashKey, (u128, RuneUtxosByScriptHashValue))>, Error> {
    move |key, value| {
        for (rid, amount) in value.runes.clone().into_iter() {
            if rid == rune_id {
                if let Some((cursor, order)) = &cursor_and_order {
                    match order {
                        OrderParam::Asc => {
                            if (amount, key.utxo_hash, key.utxo_index) > *cursor {
                                return Ok(vec![(key, (amount, value))]);
                            }
                        }
                        OrderParam::Desc => {
                            if (amount, key.utxo_hash, key.utxo_index) < *cursor {
                                return Ok(vec![(key, (amount, value))]);
                            }
                        }
                    }
                } else {
                    return Ok(vec![(key, (amount, value))]);
                }
            }
        }

        Ok(vec![])
    }
}

static EXAMPLE_RESPONSE: &str = r##"{
    "data": [{
        "txid": "67715c2b42eaa053fc174c68ca4f393446986567ed0509fa78eb3aa9d0b8db0b",
        "vout": 3,
        "satoshis": "546",
        "confirmations": 20674,
        "height": 876954,
        "runes": [{
            "rune_id": "876947:7",
            "amount": "2500000"
        }]
    }, {
        "txid": "67715c2b42eaa053fc174c68ca4f393446986567ed0509fa78eb3aa9d0b8db0b",
        "vout": 4,
        "satoshis": "546",
        "confirmations": 20674,
        "height": 876954,
        "runes": [{
            "rune_id": "876947:7",
            "amount": "2500000"
        }]
    }, {
        "txid": "67715c2b42eaa053fc174c68ca4f393446986567ed0509fa78eb3aa9d0b8db0b",
        "vout": 5,
        "satoshis": "546",
        "confirmations": 20674,
        "height": 876954,
        "runes": [{
            "rune_id": "876947:7",
            "amount": "2500000"
        }]
    }],
    "indexer_info": {
        "chain_tip": {
            "block_hash": "0000000000000000000119bd8dffd7d8285a69744011aa98f0d9091b0555ca46",
            "block_height": 897627
        },
        "mempool_timestamp": "2025-05-21 00:47:53",
        "estimated_blocks": [{
            "block_height": 897628,
            "sats_per_vb": {
                "min": 1,
                "median": 5,
                "max": 202
            }
        }]
    },
    "next_cursor": null
}"##;
