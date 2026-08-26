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
        runes::DeprecatedRuneUtxoByAddress, CountParam, HeightPaginationParams, OrderBy,
        OrderParam, PaginatedResponse,
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
pub struct OrderByParam {
    pub order_by: Option<OrderBy>,
}

#[deprecated]
#[utoipa::path(
    tag = "Addresses",
    get,
    path = "/addresses/{address}/runes/{rune}",
    params(
        ("address" = String, Path, description = "Bitcoin address or hex encoded script pubkey", example="bc1p27j3fa2mr3d50m3uaavr0ntyzr0v2a27n48lc9gxpkzd4xye6dgs2tzx6p"),
        ("rune" = String, Path, description = "Rune, specified either by the Rune ID (etching block number and transaction index) or name (spaced or un-spaced)", example="876947:7"),

        ("count" = inline(Option<CountParam>), Query, description = "The max number of results per page"),

        ("order" = inline(Option<OrderParam>), Query, description = "The order in which the results are sorted (by height at which UTxO was produced)"),
        ("from" = inline(Option<u64>), Query, description = "Return only UTxOs created on or after a specific height"),
        ("to" = inline(Option<u64>), Query, description = "Return only UTxOs created on or before a specific height"),

        ("cursor" = inline(Option<String>), Query, description = "Pagination cursor string, use the cursor included in a page of results to fetch the next page"),
    ),
    responses(
        (
            status = 200,
            description = "Requested data",
            body = PaginatedDeprecatedRuneUtxoByAddress,
            example = json!(serde_json::Value::from_str(EXAMPLE_RESPONSE).unwrap())
        ),
        (status = 400, description = "Malformed query parameters"),
        (status = 404, description = "Requested entity not found on-chain"),
        (status = 500, description = "Internal server error"),
    )
)]
#[tracing::instrument(name = "RUNE_UTXOS_BY_ADDRESS", level = "info", skip(tikv))]
/// (deprecated) Rune UTxOs by Address and Rune
///
/// Return all UTxOs controlled by the specified address or script pubkey which contain runes, with the option to filter by a specific rune kind.
pub async fn rune_utxos_by_address(
    Path((addr_or_pk, rune_id)): Path<(String, String)>,
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

    let rune_id = match RuneIdentifier::parse(rune_id)? {
        RuneIdentifier::Id(id) => id,
        RuneIdentifier::Name(n) => tikv.resolve_rune_name(n).await?.unwrap_or_default(), // return empty vec instead of 404
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
            // Restrict key range using cursor and extract cursor field values for the unfold fn.
            let cursor = match cursor {
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

                    Some((height, utxo_hash, utxo_index))
                }
                None => None,
                Some(RuneUtxosByScriptHashCursor::ByAmount { .. }) => {
                    return Err(Error::MalformedRequest(
                        "Error while decoding cursor: wrong cursor type.".into(),
                    ));
                }
            };

            Scanner::new(range_lower..range_upper)
                .count(count + 1)
                .order(order)
                .execute_with_map::<RuneUtxosByScriptHashKey, RuneUtxosByScriptHashValue, _, (u128, RuneUtxosByScriptHashValue)>(
                    &mut tikv,
                    ReducerType::RuneUtxosByScriptHash,
                    by_height_unfold_fn(rune_id, cursor.map(|x| (x, order))),
                )
                .await?
        }
        Some(OrderBy::Amount) => {
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

            // `lexico_kvs` is KVs filtered by cursor+order, we just need to sort by amount / UTxO,
            // then reflect ordering direction and truncate response to `count` pagination param.
            lexico_kvs.sort_by_key(|(k, (amount, _))| (*amount, k.utxo_hash, k.utxo_index));

            if order == OrderParam::Desc {
                lexico_kvs.reverse();
            }

            lexico_kvs.into_iter().take(count + 1).collect()
        }
    };

    let mut kvs = page_kvs.into_iter().enumerate();

    // `rune_decimals` is used to avoid re-fetching etching terms for rune kinds that
    // were already seen.
    let mut rune_decimals: HashMap<(u64, u32), usize> = HashMap::new();

    let mut utxos: Vec<DeprecatedRuneUtxoByAddress> = vec![];

    // Process fetched kvs.
    while let Some((i, (key, (amount, value)))) = kvs.next() {
        // If this is the last result of the page, check if there is a subsequent
        // result (and therefore we need to return a cursor for next page)
        // Note that this cursor is only consistent with subsequent queries when combined with the
        // same sorting order (either by-amount or by-height).
        if i == (count - 1) && kvs.next().is_some() {
            next_cursor = match order_by_param.order_by {
                None | Some(OrderBy::Height) => Some(
                    RuneUtxosByScriptHashCursor::ByHeight {
                        height: key.height,
                        utxo_hash: key.utxo_hash,
                        utxo_index: key.utxo_index,
                    }
                    .encode_base64(),
                ),
                Some(OrderBy::Amount) => Some(
                    RuneUtxosByScriptHashCursor::ByAmount {
                        amount: amount,
                        utxo_hash: key.utxo_hash,
                        utxo_index: key.utxo_index,
                    }
                    .encode_base64(),
                ),
            };
        }

        // Get rune divisibility.
        let dec = match rune_decimals.get(&rune_id) {
            Some(dec) => *dec,
            None => {
                let dec = tikv
                    .get_reducer_key::<_, etching_by_rune_id::Value>(
                        (ReducerType::EtchingByRuneId, Reducer::EtchingByRuneId),
                        &etching_by_rune_id::Key { rune_id },
                    )
                    .await?
                    .divisibility
                    .unwrap_or(0) as usize;

                rune_decimals.insert(rune_id, dec);

                dec
            }
        };

        let height = key.height;

        utxos.push(DeprecatedRuneUtxoByAddress {
            txid: Txid::from_byte_array(key.utxo_hash).to_string(),
            vout: key.utxo_index,
            satoshis: value.satoshis.to_string(),
            confirmations: (last_updated.block_height + 1).saturating_sub(height),
            height,
            rune_amount: decimal(amount, dec),
        });
    }

    let out = PaginatedResponse {
        data: utxos,
        last_updated,
        next_cursor,
    };

    Ok((StatusCode::OK, Json(out)))
}

fn by_height_unfold_fn(
    rune_id: (u64, u32),
    cursor_and_order: Option<((u64, [u8; 32], u32), OrderParam)>, // height, utxo_hash, utxo_index
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
                            if (key.height, key.utxo_hash, key.utxo_index) > *cursor {
                                return Ok(vec![(key, (amount, value))]);
                            }
                        }
                        OrderParam::Desc => {
                            if (key.height, key.utxo_hash, key.utxo_index) < *cursor {
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

fn by_amount_unfold_fn(
    rune_id: (u64, u32),
    cursor_and_order: Option<((u128, [u8; 32], u32), OrderParam)>, // amount, utxo_hash, utxo_index
) -> impl Fn(
    RuneUtxosByScriptHashKey,
    RuneUtxosByScriptHashValue,
) -> Result<Vec<(RuneUtxosByScriptHashKey, (u128, RuneUtxosByScriptHashValue))>, Error> {
    move |key, value| {
        for (rid, amount) in value.runes.clone().into_iter() {
            if rid == (rune_id.0.into(), rune_id.1.into()) {
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
            "confirmations": 13634,
            "height": 876954,
            "rune_amount": "2500000"
        },
        {
            "txid": "67715c2b42eaa053fc174c68ca4f393446986567ed0509fa78eb3aa9d0b8db0b",
            "vout": 4,
            "satoshis": "546",
            "confirmations": 13634,
            "height": 876954,
            "rune_amount": "2500000"
        },
        {
            "txid": "67715c2b42eaa053fc174c68ca4f393446986567ed0509fa78eb3aa9d0b8db0b",
            "vout": 5,
            "satoshis": "546",
            "confirmations": 13634,
            "height": 876954,
            "rune_amount": "2500000"
        },
        {
            "txid": "67715c2b42eaa053fc174c68ca4f393446986567ed0509fa78eb3aa9d0b8db0b",
            "vout": 6,
            "satoshis": "546",
            "confirmations": 13634,
            "height": 876954,
            "rune_amount": "2500000"
        },
        {
            "txid": "67715c2b42eaa053fc174c68ca4f393446986567ed0509fa78eb3aa9d0b8db0b",
            "vout": 7,
            "satoshis": "546",
            "confirmations": 13634,
            "height": 876954,
            "rune_amount": "2500000"
        },
        {
            "txid": "67715c2b42eaa053fc174c68ca4f393446986567ed0509fa78eb3aa9d0b8db0b",
            "vout": 8,
            "satoshis": "546",
            "confirmations": 13634,
            "height": 876954,
            "rune_amount": "2500000"
        },
        {
            "txid": "67715c2b42eaa053fc174c68ca4f393446986567ed0509fa78eb3aa9d0b8db0b",
            "vout": 9,
            "satoshis": "546",
            "confirmations": 13634,
            "height": 876954,
            "rune_amount": "2500000"
        }
    ],
    "last_updated": {
        "block_hash": "0000000000000000000210d44d102ab68eaa052ce03fbe216d313294725bdfaf",
        "block_height": 890587
    },
    "next_cursor": null
}"##;
