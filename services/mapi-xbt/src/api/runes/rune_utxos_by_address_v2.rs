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
        runes::RuneUtxoByAddress, CountParam, HeightPaginationParams, OrderBy, OrderParam,
        PaginatedResponse, RuneAndAmount,
    },
    util::{decimal, RuneIdentifier, MAX_PAGE_COUNT},
};

pub static REQUIRED_REDUCERS: &[ReducerType] = &[
    ReducerType::RuneUtxosByScriptHash,
    ReducerType::EtchingByRuneId,
    ReducerType::RuneIdByRuneName,
    ReducerType::ScriptByScriptHash,
    ReducerType::ScriptHashByAddressPayloadHash,
];

#[derive(Debug, Deserialize)]
pub struct RuneKindParam {
    pub rune: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct OrderByParam {
    pub order_by: Option<OrderBy>,
}

#[utoipa::path(
    tag = "Addresses",
    get,
    path = "/addresses/{address}/runes/utxos",
    params(
        ("address" = String, Path, description = "Bitcoin address or hex encoded script pubkey", example="bc1p27j3fa2mr3d50m3uaavr0ntyzr0v2a27n48lc9gxpkzd4xye6dgs2tzx6p"),

        // Filter by rune.
        ("rune" = Option<String>, Query, description = "Return only UTxOs containing a specific Rune, specified either by the Rune ID (etching block number and transaction index) or name (spaced or un-spaced)", example="840000:3"),

        // Property by which to sort. Order by amount is only supported if a rune is specified.
        ("order_by" = Option<OrderBy>, Query, description = "The property by which response items should be sorted. Supported values: height (height of block which produced the UTxO - default), amount (amount of runes in UTxO)"),

        // Sort by ascending or descending order. Support both for order by height and order by amount, independently of whether a rune filter was provided.
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
            body = PaginatedRuneUtxoByAddress,
            example = json!(serde_json::Value::from_str(EXAMPLE_RESPONSE).unwrap())
        ),
        (status = 400, description = "Malformed query parameters"),
        (status = 404, description = "Requested entity not found on-chain"),
        (status = 500, description = "Internal server error"),
    )
)]
#[tracing::instrument(name = "RUNE_UTXOS_BY_ADDRESS_V2", level = "info", skip(tikv))]
/// Rune UTxOs by Address
///
/// Lists all UTXOs at the address or script pubkey that contains Rune tokens, with optional refinement based on Rune type or metadata. Helpful for spend analysis or wallet state audits.
pub async fn rune_utxos_by_address_v2(
    Path(addr_or_pk): Path<String>,
    rune_kind: Query<RuneKindParam>,
    order_by_param: Query<OrderByParam>,
    page_params: Query<HeightPaginationParams>,
    mut tikv: Extension<TiKVAdapter>,
) -> Result<impl IntoResponse, Error> {
    tikv.init_tip(REQUIRED_REDUCERS).await?;

    let rune_utxos_encoder = tikv.get_encoder(ReducerType::RuneUtxosByScriptHash)?;

    let last_updated = tikv.get_snapshot_point()?;

    // Initialise `next_cursor`.
    let mut next_cursor = None;

    // Parse and try decode user params.
    let script_bytes = match tikv.parse_address_or_script_bytes(&addr_or_pk).await {
        Ok((_, script_bytes)) => script_bytes,
        Err(Error::NotFound) => {
            // User param is a Bitcoin address, but the corresponding script pub key could not be
            // found in store.
            let out = PaginatedResponse {
                data: vec![],
                last_updated,
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
            confirmations: (last_updated.block_height + 1).saturating_sub(height),
            height,
            runes,
        });
    }

    let out = PaginatedResponse {
        data: utxos,
        last_updated,
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
    "data": [
        {
            "txid": "67715c2b42eaa053fc174c68ca4f393446986567ed0509fa78eb3aa9d0b8db0b",
            "vout": 3,
            "satoshis": "546",
            "confirmations": 13636,
            "height": 876954,
            "runes": [
                {
                    "rune_id": "876947:7",
                    "amount": "2500000"
                }
            ]
        },
        {
            "txid": "67715c2b42eaa053fc174c68ca4f393446986567ed0509fa78eb3aa9d0b8db0b",
            "vout": 4,
            "satoshis": "546",
            "confirmations": 13636,
            "height": 876954,
            "runes": [
                {
                    "rune_id": "876947:7",
                    "amount": "2500000"
                }
            ]
        },
        {
            "txid": "67715c2b42eaa053fc174c68ca4f393446986567ed0509fa78eb3aa9d0b8db0b",
            "vout": 5,
            "satoshis": "546",
            "confirmations": 13636,
            "height": 876954,
            "runes": [
                {
                    "rune_id": "876947:7",
                    "amount": "2500000"
                }
            ]
        },
        {
            "txid": "67715c2b42eaa053fc174c68ca4f393446986567ed0509fa78eb3aa9d0b8db0b",
            "vout": 6,
            "satoshis": "546",
            "confirmations": 13636,
            "height": 876954,
            "runes": [
                {
                    "rune_id": "876947:7",
                    "amount": "2500000"
                }
            ]
        },
        {
            "txid": "67715c2b42eaa053fc174c68ca4f393446986567ed0509fa78eb3aa9d0b8db0b",
            "vout": 7,
            "satoshis": "546",
            "confirmations": 13636,
            "height": 876954,
            "runes": [
                {
                    "rune_id": "876947:7",
                    "amount": "2500000"
                }
            ]
        },
        {
            "txid": "67715c2b42eaa053fc174c68ca4f393446986567ed0509fa78eb3aa9d0b8db0b",
            "vout": 8,
            "satoshis": "546",
            "confirmations": 13636,
            "height": 876954,
            "runes": [
                {
                    "rune_id": "876947:7",
                    "amount": "2500000"
                }
            ]
        },
        {
            "txid": "67715c2b42eaa053fc174c68ca4f393446986567ed0509fa78eb3aa9d0b8db0b",
            "vout": 9,
            "satoshis": "546",
            "confirmations": 13636,
            "height": 876954,
            "runes": [
                {
                    "rune_id": "876947:7",
                    "amount": "2500000"
                }
            ]
        }
    ],
    "last_updated": {
        "block_hash": "00000000000000000000e0b35ac5973cb61e9994c78b83d20f75f4b8f8d54fff",
        "block_height": 890589
    },
    "next_cursor": null
}"##;
