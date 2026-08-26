use crate::{
    error::MapiResult,
    tikv::{adapter::TiKVAdapter, key_resolver::ReducerType, Scanner},
    types::{
        CommonPaginatedResponse, CountParam, HeightPaginationParams, HistoricalSatBalanceByAddress,
        OrderParam,
    },
    util::{fetch_sat_prices, timestamp_to_string, ParsedHeightPaginationParams, MAX_PAGE_COUNT},
};
use axum::{
    extract::{Path, Query},
    response::IntoResponse,
    Extension, Json,
};
use bitcoin::{hashes::Hash, Script};
use reqwest::StatusCode;
use serde::Deserialize;
use std::cmp::{min, Ordering};
use std::str::FromStr;
use timbre_xbt::{
    reducers::{
        block_info::{Key as BlockInfoKey, Value as BlockInfoValue},
        height_by_timestamp::{Key as HeightByTimestampKey, Value as HeightByTimestampValue},
        historical_sat_balance_by_script_hash::{Cursor, Key, Value},
        reducer_key_range, Height, SatoshiQuantity, Timestamp,
    },
    Decode, Encode, Prefix, Reducer,
};

use crate::{error::Error, options::arranger::Arranger};

#[derive(Debug, Deserialize)]
pub struct HeightOrTimestampParam {
    pub height_params: Option<bool>,
}

pub static REQUIRED_REDUCERS: &[ReducerType] = &[
    ReducerType::BlockInfo,
    ReducerType::HeightByTimestamp,
    ReducerType::HistoricalSatBalanceByScriptHash,
    // parsing address parameter
    ReducerType::ScriptByScriptHash,
    ReducerType::ScriptHashByAddressPayloadHash,
];

#[utoipa::path(
    tag = "Addresses",
    get,
    path = "/addresses/{address}/balance/historical",
    params(
        ("address" = String, Path, description = "Bitcoin address or hex encoded script pubkey", example="bc1qcx7ys0ahvtfqcc63sfn6axls0qrhkadnslpd94"),

        // Pagination params applicable regardless of the sorting order and property.
        ("order" = inline(Option<OrderParam>), Query, description = "The order in which the results are sorted. Supported values: asc, desc"),
        ("count" = inline(Option<CountParam>), Query, description = "The max number of results per page"),
        ("from" = inline(Option<u64>), Query, description = "Return only blocks included on or after a specific height or timestamps. If this parameter is not provided, the starting point will be the first block where the address has seen its balance increase or decrease."),
        ("to" = inline(Option<u64>), Query, description = "Return only blocks included on or before a specific height or timestamp"),
        ("cursor" = inline(Option<String>), Query, description = "Pagination cursor string, use the cursor included in a page of results to fetch the next page"),

        // How to read from and to query params.
        ("height_params" = inline(Option<bool>), Query, description = "Whether the from and to integer query params should be read as timestamps or as block heights. True (the default) means from and to params should be read as block heights."),
    ),
    responses(
        (
            status = 200,
            description = "Requested data",
            body = PaginatedHistoricalSatBalanceByAddress,
            example = json!(serde_json::Value::from_str(EXAMPLE_RESPONSE).unwrap())
        ),
        (status = 400, description = "Malformed query parameters"),
        (status = 404, description = "Requested entity not found on-chain"),
        (status = 500, description = "Internal server error"),
    )
)]
#[tracing::instrument(
    name = "HISTORICAL_SATOSHI_BALANCE_BY_ADDRESS",
    level = "info",
    skip(tikv, arranger)
)]
/// Historical Satoshi Balance by Address
///
/// Returns the historical satoshi balances, itemized by block and including USD price.
pub async fn historical_satoshi_balance_by_address(
    Path(addr_or_pk): Path<String>,
    mut page_params: Query<HeightPaginationParams>,
    param_type: Query<HeightOrTimestampParam>,
    mut tikv: Extension<TiKVAdapter>,
    Extension(arranger): Extension<Arranger>,
) -> Result<impl IntoResponse, Error> {
    tikv.init_tip(REQUIRED_REDUCERS).await?;

    let last_updated = tikv.get_snapshot_point()?;

    // Initialize `next_cursor`.
    let mut next_cursor: Option<String> = None;

    let height_by_timestamp_encoder = tikv.get_encoder(ReducerType::HeightByTimestamp)?;
    let sat_balance_encoder = tikv.get_encoder(ReducerType::HistoricalSatBalanceByScriptHash)?;

    let script_bytes = match tikv.parse_address_or_script_bytes(&addr_or_pk).await {
        Ok((_, script_bytes)) => script_bytes,
        Err(Error::NotFound) => {
            // User param is a Bitcoin address, but the corresponding script pub key could not be
            // found in store.
            let out = CommonPaginatedResponse {
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

    // Extract from query param or fetch lowest KV as starting point for the page.
    let mut from = if let Some(from) = page_params.from {
        from
    } else {
        match fetch_lowest_height(
            &sat_balance_encoder,
            &height_by_timestamp_encoder,
            &script_hash,
            param_type.height_params,
            page_params.to,
            &mut tikv,
        )
        .await?
        {
            Some(from) => {
                // Height of lowest block where the balance was updated.
                page_params.from = Some(from);

                from
            }
            None => {
                // No activity was found between max(genesis, from) and to.
                let out = CommonPaginatedResponse {
                    data: vec![],
                    last_updated,
                    next_cursor,
                };

                return Ok((StatusCode::OK, Json(out)));
            }
        }
    };

    // Parse pagination params, starting with from and to.
    let (from, to) = if !param_type.height_params.unwrap_or(true) {
        if page_params.from.is_none() && page_params.to.is_none() {
            return Err(Error::MalformedRequest(
                "From and/or to params are provided as timestamps, but they are both absent".into(),
            ));
        }

        // from and/or to are timestamps. Fetch `HeightByTimestamp` KV for each and extract block
        // heights. This can be done by fetching the highest KV whose timestamp is less than from
        // (resp. to).
        from = {
            let (range_lower, range_upper) = reducer_key_range(
                height_by_timestamp_encoder.namespace(),
                &Reducer::HeightByTimestamp,
                &None::<u64>,
                Some(from),
                None::<u64>,
            );

            let previous_block = Scanner::new(range_lower..range_upper)
                .count(1)
                .order(OrderParam::Asc)
                .execute::<HeightByTimestampKey, HeightByTimestampValue>(
                    &mut tikv,
                    ReducerType::HeightByTimestamp,
                )
                .await?;

            match previous_block.get(0) {
                Some((_, value)) => {
                    page_params.from = Some(value.height);

                    value.height
                }
                None => {
                    // Unable to resolve from timestamp.
                    return Err(Error::Internal("Unable to resolve from timestamp.".into()));
                }
            }
        };

        let mut to = if let Some(to) = page_params.to {
            let (range_lower, range_upper) = reducer_key_range(
                height_by_timestamp_encoder.namespace(),
                &Reducer::HeightByTimestamp,
                &None::<u64>,
                Some(0u64),
                Some(to + 1),
            );

            let previous_block = Scanner::new(range_lower..range_upper)
                .count(1)
                .order(OrderParam::Desc)
                .execute::<HeightByTimestampKey, HeightByTimestampValue>(
                    &mut tikv,
                    ReducerType::HeightByTimestamp,
                )
                .await?;

            page_params.to = previous_block.get(0).map(|(_, value)| value.height);

            page_params.to
        } else {
            None
        };

        if let Some(to) = to {
            if to <= from {
                return Err(Error::MalformedRequest(
                    "from and to params are incompatible.".into(),
                ));
            }
        }

        if let Some(cursor_param) = page_params.cursor.clone() {
            let cursor: Height = Cursor::decode_base64(&cursor_param)
                .map_err(|_| Error::MalformedRequest("Malormed cursor: unable to decode.".into()))?
                .0;

            if page_params.order == Some(OrderParam::Desc) {
                // Order explicitly set as descending.
                if let Some(to) = to {
                    if to < cursor {
                        // to and cursor params are provided, and order is descending.
                        // cursor lies after to.
                        return Err(Error::MalformedRequest(
                            "Malformed cursor: invalid for to and order param.".into(),
                        ));
                    }
                }

                // Override to.
                to = Some(cursor);

                page_params.to = to;
            } else {
                // Order set as ascending, explicitly or by default.
                if from > cursor {
                    // from and cursor parameters are provided and order is ascending.
                    // cursor lies before from.
                    return Err(Error::MalformedRequest(
                        "Malformed cursor: invalid for from and order param.".into(),
                    ));
                }

                // Override from.
                from = cursor;

                page_params.from = Some(from);
            }
        }

        (from, to)
    } else {
        // Query params are already block heights, but we need to adjust wrt the cursor.
        let mut to = page_params.to;

        if let Some(cursor_param) = page_params.cursor.clone() {
            let cursor: Height = Cursor::decode_base64(&cursor_param)
                .map_err(|_| Error::MalformedRequest("Malormed cursor: unable to decode.".into()))?
                .0;

            if page_params.order == Some(OrderParam::Desc) {
                // Order explicitly set as descending.
                if let Some(to) = to {
                    if to < cursor {
                        // to and cursor params are provided, and order is descending.
                        // cursor lies after to.
                        return Err(Error::MalformedRequest(
                            "Malformed cursor: invalid for to and order param.".into(),
                        ));
                    }
                }

                // Override to.
                to = Some(cursor);

                page_params.to = to;
            } else {
                // Order set as ascending, explicitly or by default.
                if from > cursor {
                    // from and cursor parameters are provided and order is ascending.
                    // cursor lies before from.
                    return Err(Error::MalformedRequest(
                        "Malformed cursor: invalid for from and order param.".into(),
                    ));
                }

                // Override from.
                from = cursor;

                page_params.from = Some(from);
            }
        }

        (from, to)
    };

    let parsed_page_params = ParsedHeightPaginationParams::parse::<_, Cursor>(
        page_params.0,
        &sat_balance_encoder,
        &Reducer::HistoricalSatBalanceByScriptHash,
        Some(script_hash),
    )?;

    let mut balances: Vec<HistoricalSatBalanceByAddress> = vec![];

    let kvs = Scanner::new(parsed_page_params.key_range())
        // Fetch at most count elements. We'll check manually whether next_cursor should be updated.
        .count(parsed_page_params.count() + 1)
        .order(parsed_page_params.order())
        .execute::<Key, Value>(&mut tikv, ReducerType::HistoricalSatBalanceByScriptHash)
        .await?;

    // To complete the block range, we need to fill the gaps at the beginning and end of the
    // queried range, as well as in between KVs.
    let kvs = complete_range(
        kvs,
        &script_hash,
        from,
        to,
        last_updated.block_height,
        parsed_page_params.order(),
        parsed_page_params.count() as u64 + 1,
        &mut tikv,
        &sat_balance_encoder,
    )
    .await?;

    if kvs.len() == 0 {
        // This means there were fetched KVs in the query range, and moreover the address has never
        // had any activity whatsoever (up to to, if it was provided). Return empty response.
        let out = CommonPaginatedResponse {
            data: vec![],
            last_updated,
            next_cursor,
        };

        return Ok((StatusCode::OK, Json(out)));
    }

    // Fetch timestamps for each block.
    let kvs = fetch_timestamps(kvs, parsed_page_params.order(), &mut tikv).await?;

    // Fetch USD prices for each block (None if no price service is configured).
    let mut kvs = fetch_sat_prices_from_arranger(kvs, arranger.get_sat_prices_path().as_deref())
        .await?
        .enumerate();

    // Process fetched kvs.
    while let Some((i, ((height, balance, unix_timestamp), price))) = kvs.next() {
        // If this is the last result of the page, check if there is a subsequent block and the
        // upper bound height has not been reached yet(and therefore we need to return a cursor
        // for next page).
        if i == (parsed_page_params.count() - 1) && kvs.next().is_some() {
            // Separate treatment of cursor relative to pagination order.
            match parsed_page_params.order() {
                OrderParam::Asc => {
                    // Check there are subsequent blocks.
                    if last_updated.block_height > height {
                        // Check if upper bound has not been reached. Upper bound is exclusive.
                        if to.is_none() || to > Some(height) {
                            next_cursor = Some((height + 1).encode_base64());
                        }
                    }
                }
                OrderParam::Desc => {
                    // Check there are previous balance update KVs.
                    if height > from {
                        next_cursor = Some((height - 1).encode_base64());
                    }
                }
            }
        }

        balances.push(HistoricalSatBalanceByAddress {
            height,
            confirmations: (last_updated.block_height + 1).saturating_sub(height),
            unix_timestamp,
            timestamp: timestamp_to_string(unix_timestamp as u64),
            sat_balance: balance.to_string(),
            // USD price means BTC to USD exchange rate.
            usd_balance: price
                .map(|price| format!("{:.2}", (balance as f64 * price) / 100000000.0)),
        })
    }

    let out = CommonPaginatedResponse {
        data: balances,
        last_updated,
        next_cursor,
    };

    Ok((StatusCode::OK, Json(out)))
}

// NOTE: both from and to are guaranteed to represent blocks, not timestamps.
// Example:
//      Given address A, suppose from == 2, to == 6, that kvs is  [(3, 100), (4, 50)], that block
//      tip is at height 7, that order == OrderParam::Asc, that count == MAX_PAGE_COUNT and that A
//      has also had activity (1, 33). Then we expect complete_range to return the following:
//          [(2, 33), (3, 100), (4, 50), (5, 50)]
//      To achieve this, we first complete the range from from to lowest fetched KV (i.e., from
//      2 to 3) with balance in previous block (i.e., 33), then fill gaps between KVs (i.e., add
//      (3, 100) to fill gap between height 3 and 4), and finally complete range by adding blocks
//      from highest fetched KV (i.e., 4) to to (i.e, 6) with highest fetched balance (i.e., add
//      (4, 50) and (5, 50)).
async fn complete_range(
    kvs: Vec<(Key, Value)>,
    script_hash: &[u8; 20],
    from: u64,
    to: Option<u64>,
    last_block_height: u64,
    order: OrderParam,
    count: u64,
    tikv: &mut Extension<TiKVAdapter>,
    sat_balance_encoder: &Prefix,
) -> MapiResult<Vec<(Height, SatoshiQuantity)>> {
    match order {
        OrderParam::Asc => {
            complete_range_asc(
                kvs,
                script_hash,
                from,
                to,
                last_block_height,
                count,
                tikv,
                sat_balance_encoder,
            )
            .await
        }
        OrderParam::Desc => {
            complete_range_desc(
                kvs,
                script_hash,
                from,
                to,
                last_block_height,
                count,
                tikv,
                sat_balance_encoder,
            )
            .await
        }
    }
}

async fn complete_range_asc(
    kvs: Vec<(Key, Value)>,
    script_hash: &[u8; 20],
    from: u64,
    to: Option<u64>,
    last_block_height: u64,
    count: u64,
    tikv: &mut Extension<TiKVAdapter>,
    sat_balance_encoder: &Prefix,
) -> MapiResult<Vec<(Height, SatoshiQuantity)>> {
    let mut res = vec![];

    // Fill the gap up to the lowest KV.
    let (mut prev_height, mut prev_balance) = if let Some((lowest_key, lowest_fetched_balance)) =
        kvs.get(0)
    {
        let lowest_fetched_height = lowest_key.height;

        // We may need to fill the gap between lower bound and the first KV.
        match lowest_fetched_height.cmp(&from) {
            Ordering::Greater => {
                // Fill the gap between from and the lowest fetched KV.
                let prev_balance =
                    fetch_previous_balance(sat_balance_encoder, script_hash, from, tikv).await?;

                // Add items to result to fill the gap.
                for height in from..lowest_fetched_height {
                    if (res.len() as u64) < count {
                        res.push((height, prev_balance));
                    } else {
                        return Ok(res);
                    }
                }
            }
            Ordering::Equal => (), // No gap to fill.
            Ordering::Less => {
                // This should never happen.
                return Err(Error::Internal("Inconsistent order between KVs".into()));
            }
        }

        (lowest_fetched_height, *lowest_fetched_balance)
    } else {
        // This means balance hasn't changed during [from, to), so no keys where found. Try
        // and scan a previous block, filling the entire block range with that balance.
        // Note that if no KVs were ever written before from, this means balance is 0, which
        // is correctly handled by fetch_previous_balance.
        let prev_balance =
            fetch_previous_balance(sat_balance_encoder, script_hash, from, tikv).await?;

        let last_height = if let Some(to) = to {
            from + min(to.saturating_sub(from), count)
        } else {
            from + count
        };

        // Add items to result to fill the gap.
        for height in from..=min(last_height, last_block_height) {
            if (res.len() as u64) < count {
                res.push((height, prev_balance))
            } else {
                return Ok(res);
            }
        }

        return Ok(res);
    };

    // Remove first item before entering the loop, as we have already covered it above.
    let mut kvs = kvs[1..].iter();

    while let Some((key, sat_balance)) = kvs.next() {
        for height in prev_height..key.height {
            if (res.len() as u64) < count {
                res.push((height, prev_balance));
            } else {
                return Ok(res);
            }
        }

        prev_height = key.height;
        prev_balance = *sat_balance;
    }

    // Continue adding items after highest fetched KV to complete page.
    let last_height = if let Some(to) = to {
        // Either add blocks to complete page or to reach upper bound.
        prev_height + min(to - prev_height, count)
    } else {
        // No upper bound provided - add blocks to complete page.
        prev_height + count
    };

    for height in prev_height..=min(last_height, last_block_height) {
        if (res.len() as u64) < count {
            res.push((height, prev_balance));
        } else {
            return Ok(res);
        }
    }

    Ok(res)
}

async fn complete_range_desc(
    kvs: Vec<(Key, Value)>,
    script_hash: &[u8; 20],
    from: u64,
    to: Option<u64>,
    last_block_height: u64,
    count: u64,
    tikv: &mut Extension<TiKVAdapter>,
    sat_balance_encoder: &Prefix,
) -> MapiResult<Vec<(Height, SatoshiQuantity)>> {
    let mut res = vec![];

    // Fill the gap from to the highest KV up to upper bound, if provided.
    let mut prev_height = if let Some((highest_key, highest_fetched_balance)) = kvs.get(0) {
        let highest_fetched_height = highest_key.height;

        if let Some(to) = to {
            // If an upper bound was provided, then we may need to fill the gap between it and the
            // first KV.
            match highest_fetched_height.cmp(&to) {
                Ordering::Less => {
                    // Fill the gap between highest fetched KV and to.
                    // Iterate from block prior to to, down to heighest_fetched_height + 1.
                    for height in (highest_fetched_height..=to).rev() {
                        if (res.len() as u64) < count {
                            res.push((height, *highest_fetched_balance));
                        } else {
                            return Ok(res);
                        }
                    }
                }
                Ordering::Equal => (), // No gap to fill.
                Ordering::Greater => {
                    // This should never happen.
                    return Err(Error::Internal("Inconsistent order between KVs".into()));
                }
            }
        } else {
            // We need to fill the gap between the last confirmed block and highest_fetched_height.
            // Iterate from to down to heighest_fetched_height + 1, both inclusive.
            for height in (highest_fetched_height..=last_block_height).rev() {
                if (res.len() as u64) < count {
                    res.push((height, *highest_fetched_balance));
                } else {
                    return Ok(res);
                }
            }
        }

        highest_fetched_height
    } else {
        // This means balance hasn't changed from from up to to or last_block_height. Try and scan
        // a previous block, filling the entire block range with that balance.
        // Note that if no KVs were ever written before from, this means balance is 0, which is
        // correctly handled by fetch_previous_balance.
        let prev_balance =
            fetch_previous_balance(sat_balance_encoder, script_hash, from, tikv).await?;

        let last_height = if let Some(to) = to {
            from + min(to - from, count)
        } else {
            from + count
        };

        // Add items to result to fill the gap.
        for height in (from..=last_height).rev() {
            if (res.len() as u64) < count {
                res.push((height, prev_balance))
            } else {
                return Ok(res);
            }
        }

        return Ok(res);
    };

    // Remove first item before entering the loop, as we have already covered it above.
    let mut kvs = kvs[1..].iter();

    while let Some((key, sat_balance)) = kvs.next() {
        for height in (key.height..prev_height).rev() {
            if (res.len() as u64) < count {
                res.push((height, *sat_balance));
            } else {
                return Ok(res);
            }
        }

        prev_height = key.height;
    }

    // Continue adding items before lowest fetched KV to complete page.
    // Either add blocks to complete page or to reach lower bound.
    // +1 because last fetched KV was not pushed to res yet.
    let remaining_blocks = min(prev_height - from, count - (res.len() as u64));

    if remaining_blocks > 0 {
        // Fetch previous block with balance update and extract balance.
        let prev_balance =
            fetch_previous_balance(sat_balance_encoder, script_hash, prev_height, tikv).await?;

        // Fill the gap between from and the lowest fetched KV.
        for diff_with_prev_height in 1..=remaining_blocks {
            if (res.len() as u64) < count {
                res.push((
                    prev_height.saturating_sub(diff_with_prev_height),
                    prev_balance,
                ));
            } else {
                return Ok(res);
            }
        }
    }

    Ok(res)
}

// Fetch lowest KV with balance update and return its height.
async fn fetch_lowest_height(
    sat_balance_encoder: &Prefix,
    height_by_timestamp_encoder: &Prefix,
    script_hash: &[u8; 20],
    is_height: Option<bool>,
    to: Option<u64>,
    tikv: &mut Extension<TiKVAdapter>,
) -> MapiResult<Option<u64>> {
    let to = if is_height.unwrap_or(true) {
        to
    } else {
        match to {
            Some(to_timestamp) => {
                let (range_lower, range_upper) = reducer_key_range(
                    height_by_timestamp_encoder.namespace(),
                    &Reducer::HeightByTimestamp,
                    &None::<u64>,
                    Some(0u64),
                    Some(to_timestamp + 1),
                );

                let previous_block = Scanner::new(range_lower..range_upper)
                    .count(1)
                    .order(OrderParam::Desc)
                    .execute::<HeightByTimestampKey, HeightByTimestampValue>(
                        tikv,
                        ReducerType::HeightByTimestamp,
                    )
                    .await?;

                previous_block.get(0).map(|(_, value)| value.height)
            }
            None => {
                // We know at this point from is None, and so is to. Then is_height should not
                // have been be Some(false).
                return Err(Error::MalformedRequest(
                    "From and/or to params are provided as timestamps, but they are both absent"
                        .into(),
                ));
            }
        }
    };

    let (range_lower, range_upper) = reducer_key_range(
        sat_balance_encoder.namespace(),
        &Reducer::HistoricalSatBalanceByScriptHash,
        &Some(script_hash),
        Some(0u64),
        to,
    );

    let previous_block = Scanner::new(range_lower..range_upper)
        .count(1)
        .order(OrderParam::Asc)
        .execute::<Key, Value>(tikv, ReducerType::HistoricalSatBalanceByScriptHash)
        .await?;

    Ok(previous_block.get(0).map(|(key, _)| key.height))
}

// Fetch previous block with balance update and extract balance, or return balance 0.
async fn fetch_previous_balance(
    sat_balance_encoder: &Prefix,
    script_hash: &[u8; 20],
    from: u64,
    tikv: &mut Extension<TiKVAdapter>,
) -> MapiResult<u64> {
    let (range_lower, range_upper) = reducer_key_range(
        sat_balance_encoder.namespace(),
        &Reducer::HistoricalSatBalanceByScriptHash,
        &Some(script_hash),
        Some(0u64),
        Some(from),
    );

    let previous_block = Scanner::new(range_lower..range_upper)
        .count(1)
        .order(OrderParam::Desc)
        .execute::<Key, Value>(tikv, ReducerType::HistoricalSatBalanceByScriptHash)
        .await?;

    Ok(previous_block
        .get(0)
        .map(|(_, sat_balance)| *sat_balance)
        .unwrap_or(0u64))
}

async fn fetch_timestamps(
    kvs: Vec<(Height, SatoshiQuantity)>,
    order: OrderParam,
    tikv: &mut Extension<TiKVAdapter>,
) -> MapiResult<Vec<(Height, SatoshiQuantity, Timestamp)>> {
    let (from_height, to_height) = match order {
        OrderParam::Asc => {
            let from_height = match kvs.get(0) {
                Some((from_height, _)) => *from_height,
                None => {
                    return Err(Error::Internal(
                        "Could not find from in ascending order.".into(),
                    ))
                }
            };

            let to_height = match kvs.last() {
                Some((to_height, _)) => *to_height,
                None => {
                    return Err(Error::Internal(
                        "Could not find to in ascending order.".into(),
                    ))
                }
            };

            (from_height, to_height)
        }
        OrderParam::Desc => {
            let from_height = match kvs.last() {
                Some((from_height, _)) => *from_height,
                None => {
                    return Err(Error::Internal(
                        "Could not find from in descending order.".into(),
                    ))
                }
            };

            let to_height = match kvs.get(0) {
                Some((to_height, _)) => *to_height,
                None => {
                    return Err(Error::Internal(
                        "Could not find to in descending order.".into(),
                    ))
                }
            };

            (from_height, to_height)
        }
    };

    let block_info_encoder = tikv.get_encoder(ReducerType::BlockInfo)?;

    let (range_lower, range_upper) = reducer_key_range(
        block_info_encoder.namespace(),
        &Reducer::BlockInfo,
        &None::<u64>,
        Some(from_height),
        Some(to_height + 1u64),
    );

    if kvs.len() > MAX_PAGE_COUNT + 1 {
        // Unexpected - KVs vector should have at most MAX_PAGE_COUNT + 1 elements.
        return Err(Error::Internal(format!(
            "Trying to fetch timestamps for too many blocks ({:?})",
            kvs.len()
        )));
    }

    let timestamps = Scanner::new(range_lower..range_upper)
        .count(kvs.len())
        .order(order)
        .execute::<BlockInfoKey, BlockInfoValue>(tikv, ReducerType::BlockInfo)
        .await?;

    let mut res = vec![];

    for ((height, balance), (_, block_info_value)) in kvs.into_iter().zip(timestamps.into_iter()) {
        if let Some(timestamp) = block_info_value.timestamp {
            res.push((height, balance, timestamp));
        } else {
            return Err(Error::Internal("Unable to find block timestamp".into()));
        }
    }

    Ok(res)
}

// Zips each entry with its USD price. When no external price service is configured (`arranger`
// is `None`), each entry is zipped with `None` instead.
async fn fetch_sat_prices_from_arranger(
    kvs: Vec<(Height, SatoshiQuantity, Timestamp)>,
    arranger: Option<&str>,
) -> MapiResult<impl Iterator<Item = ((Height, SatoshiQuantity, Timestamp), Option<f64>)>> {
    let prices: Vec<Option<f64>> = if let Some(arranger) = arranger {
        let timestamps = kvs
            .iter()
            .map(|(_, _, timestamp)| *timestamp)
            .collect::<Vec<_>>();

        let prices = fetch_sat_prices(timestamps, arranger).await?;

        if kvs.len() != prices.len() {
            return Err(Error::Internal(
                "Unexpected number of prices from upstream".into(),
            ));
        }

        prices.into_iter().map(Some).collect()
    } else {
        vec![None; kvs.len()]
    };

    Ok(kvs.into_iter().zip(prices.into_iter()))
}

pub static EXAMPLE_RESPONSE: &str = r##"{
    "data": [{
        "height": 899903,
        "confirmations": 932,
        "unix_timestamp": 1749126863,
        "timestamp": "2025-06-05 12:34:23",
        "sat_balance": "155191",
        "usd_balance": "163.59"
    }, {
        "height": 899904,
        "confirmations": 931,
        "unix_timestamp": 1749126892,
        "timestamp": "2025-06-05 12:34:52",
        "sat_balance": "155191",
        "usd_balance": "163.59"
    }, {
        "height": 899905,
        "confirmations": 930,
        "unix_timestamp": 1749126924,
        "timestamp": "2025-06-05 12:35:24",
        "sat_balance": "155191",
        "usd_balance": "163.56"
    }],
    "last_updated": {
        "block_hash": "000000000000000000014ba9b2d30d9c737423c753c5b6a27989815ed50afe04",
        "block_height": 900834
    },
    "next_cursor": null
}"##;
