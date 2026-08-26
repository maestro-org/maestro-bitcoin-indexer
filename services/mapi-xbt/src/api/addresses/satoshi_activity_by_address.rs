use axum::{
    extract::{Path, Query},
    response::IntoResponse,
    Extension, Json,
};
use bitcoin::{hashes::Hash, Script, Txid};
use reqwest::StatusCode;
use serde::Deserialize;
use std::str::FromStr;
use tikv_client::KvPair;
use timbre_xbt::{
    reducers::sat_txs_by_script_hash::{
        Cursor as SatTxsByScriptHashCursor, Key as SatTxsByScriptHashKey, SatActivityType,
        Value as SatTxsByScriptHashValue,
    },
    Decode, Encode, Reducer,
};

use crate::{
    error::Error,
    tikv::{adapter::TiKVAdapter, key_resolver::ReducerType, Scanner},
    types::{
        ActivityByAddress, ActivityKindByAddress, CommonPaginatedResponse, CountParam,
        HeightPaginationParams, OrderParam, SatActivity,
    },
    util::ParsedHeightPaginationParams,
};

#[derive(Debug, Deserialize)]
pub struct FilterParams {
    pub activity_kind: Option<ActivityKindByAddress>,
    pub exclude_self_transfers: Option<bool>,
}

pub static REQUIRED_REDUCERS: &[ReducerType] = &[
    ReducerType::SatTxsByScriptHash,
    // resolving script hashes
    ReducerType::ScriptByScriptHash,
    ReducerType::ScriptHashByAddressPayloadHash,
];

#[utoipa::path(
    tag = "Addresses",
    get,
    path = "/addresses/{address}/activity",
    params(
        ("address" = String, Path, description = "Bitcoin address or hex encoded script pubkey", example="bc1qcx7ys0ahvtfqcc63sfn6axls0qrhkadnslpd94"),

        // Pagination params applicable regardless of the sorting order and property.
        ("order" = inline(Option<OrderParam>), Query, description = "The order in which the results are sorted. Supported values: asc, desc"),
        ("count" = inline(Option<CountParam>), Query, description = "The max number of results per page"),
        ("from" = inline(Option<u64>), Query, description = "Return only transactions included on or after a specific height"),
        ("to" = inline(Option<u64>), Query, description = "Return only transactions included on or before a specific height"),
        ("cursor" = inline(Option<String>), Query, description = "Pagination cursor string, use the cursor included in a page of results to fetch the next page"),

        // Filter by activity kind.
        ("activity_kind" = Option<ActivityKindByAddress>, Query, description = "Only return transactions of a specific activity kind. Supported values: \"increase\" for transactions where satoshi balance increases, \"decrease\" for decrease, and \"self_transfer\" for transactions where satoshi balance remained the same."),

         // Filter self-transfers out.
        ("exclude_self_transfers" = Option<bool>, Query, description = "Do not return self-transfer transactions - transactions in which satoshi balance did not increase or decrease."),
    ),
    responses(
        (
            status = 200,
            description = "Requested data",
            body = PaginatedActivityByAddress,
            example = json!(serde_json::Value::from_str(EXAMPLE_RESPONSE).unwrap())
        ),
        (status = 400, description = "Malformed query parameters"),
        (status = 404, description = "Requested entity not found on-chain"),
        (status = 500, description = "Internal server error"),
    )
)]
#[tracing::instrument(name = "SATOSHI_ACTIVITY_BY_ADDRESS", level = "info", skip(tikv))]
/// Satoshi Activity by Address
///
/// Returns all transactions for a given address or script pubkey, allowing insight into when the balance increased, decreased, or remained the same. This endpoint supports customization to narrow results by time, transaction type, or ordering, enabling tailored historical views.
pub async fn satoshi_activity_by_address(
    Path(addr_or_pk): Path<String>,
    page_params: Query<HeightPaginationParams>,
    filter_params: Query<FilterParams>,
    mut tikv: Extension<TiKVAdapter>,
) -> Result<impl IntoResponse, Error> {
    tikv.init_tip(REQUIRED_REDUCERS).await?;

    let last_updated = tikv.get_snapshot_point()?;

    // Initialize `next_cursor`.
    let mut next_cursor: Option<String> = None;

    let sat_txs_encoder = tikv.get_encoder(ReducerType::SatTxsByScriptHash)?;

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

    // Parse pagination params.
    let page_params = ParsedHeightPaginationParams::parse::<_, SatTxsByScriptHashCursor>(
        page_params.0,
        &sat_txs_encoder,
        &Reducer::SatTxsByScriptHash,
        Some(script_hash),
    )?;

    // Parse `exclude_self_transfers` filter param.
    let excl_self_transfers: bool = filter_params.exclude_self_transfers.unwrap_or(false);

    // Rule out the ill-formed combination of `excl_self_transfers` on and `activity_kind` set to self-transfers.
    if excl_self_transfers
        && filter_params.activity_kind == Some(ActivityKindByAddress::SelfTransfer)
    {
        return Err(Error::MalformedRequest(
            "Ill-formed combination of excl_self_transfers and activity_kind.".into(),
        ));
    }

    let mut kvs = if let Some(activity_kind) = &filter_params.activity_kind {
        let filter_fn = Some(|kv: &KvPair| {
            // NOTE: we don't need to check `excl_self_transfers` because we have already ruled
            // out the ill-formed case.
            let value = SatTxsByScriptHashValue::decode(&kv.1).unwrap().0;

            match activity_kind {
                ActivityKindByAddress::SelfTransfer => {
                    value.activity_type == SatActivityType::SelfTransferred
                }
                ActivityKindByAddress::Increase => {
                    value.activity_type == SatActivityType::Increased
                }
                ActivityKindByAddress::Decrease => {
                    value.activity_type == SatActivityType::Decreased
                }
            }
        });

        Scanner::new(page_params.key_range())
            .count(page_params.count() + 1)
            .order(page_params.order())
            .execute_with_filter::<SatTxsByScriptHashKey, SatTxsByScriptHashValue, _>(
                &mut tikv,
                ReducerType::SatTxsByScriptHash,
                filter_fn,
            )
            .await?
    } else if excl_self_transfers {
        let filter_fn = Some(|kv: &KvPair| {
            let value = SatTxsByScriptHashValue::decode(&kv.1).unwrap().0;
            value.activity_type != SatActivityType::SelfTransferred
        });

        Scanner::new(page_params.key_range())
            .count(page_params.count() + 1)
            .order(page_params.order())
            .execute_with_filter::<SatTxsByScriptHashKey, SatTxsByScriptHashValue, _>(
                &mut tikv,
                ReducerType::SatTxsByScriptHash,
                filter_fn,
            )
            .await?
    } else {
        Scanner::new(page_params.key_range())
            .count(page_params.count() + 1)
            .order(page_params.order())
            .execute::<SatTxsByScriptHashKey, SatTxsByScriptHashValue>(
                &mut tikv,
                ReducerType::SatTxsByScriptHash,
            )
            .await?
    }
    .into_iter()
    .enumerate();

    let mut activity: Vec<ActivityByAddress> = vec![];

    // Process fetched kvs.
    while let Some((i, (key, value))) = kvs.next() {
        // If this is the last result of the page, check if there is a subsequent result (and
        // therefore we need to return a cursor for next page).
        if i == (page_params.count() - 1) && kvs.next().is_some() {
            next_cursor = Some(
                SatTxsByScriptHashCursor {
                    height: key.height,
                    activity_tx_index: key.activity_tx_index,
                    tx_hash: key.tx_hash,
                }
                .encode_base64(),
            );
        }

        activity.push(ActivityByAddress {
            height: key.height,
            confirmations: (last_updated.block_height + 1).saturating_sub(key.height),
            tx_hash: Txid::from_byte_array(key.tx_hash).to_string(),
            sat_activity: SatActivity {
                kind: match value.activity_type {
                    SatActivityType::SelfTransferred => ActivityKindByAddress::SelfTransfer,
                    SatActivityType::Increased => ActivityKindByAddress::Increase,
                    SatActivityType::Decreased => ActivityKindByAddress::Decrease,
                },
                amount: value.amount.to_string(),
            },
        });
    }

    let out = CommonPaginatedResponse {
        data: activity,
        last_updated,
        next_cursor,
    };

    Ok((StatusCode::OK, Json(out)))
}

pub static EXAMPLE_RESPONSE: &str = r##"{
    "data": [{
        "height": 892671,
        "confirmations": 48,
        "tx_hash": "9f2d17f672ebb9962940aba94daef140cc876f32edba5e9325c18ca17534120e",
        "kind": "increase",
        "amount": "4456732"
    }, {
        "height": 892676,
        "confirmations": 43,
        "tx_hash": "2c34dbe0d8dcbf4faba291e50cda1d9b6f70cccdbb2fcd23a9341d2791845998",
        "kind": "decrease",
        "amount": "4456732"
    }],
    "last_updated": {
        "block_hash": "00000000000000000000c7f19aca70cdeab9d7f04a79d1c8283b66dce78796e8",
        "block_height": 892719
    },
    "next_cursor": null
}"##;
