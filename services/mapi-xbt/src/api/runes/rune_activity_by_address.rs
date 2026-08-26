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
    reducers::rune_txs_by_script_hash::{
        Cursor as RuneTxsByScriptHashCursor, Key as RuneTxsByScriptHashKey,
        Value as RuneTxsByScriptHashValue,
    },
    Decode, Encode, Reducer,
};

use crate::{
    error::Error,
    tikv::{adapter::TiKVAdapter, key_resolver::ReducerType, Scanner},
    types::{
        runes::{RuneActivityByAddress, RuneActivityKindByAddress},
        CommonPaginatedResponse, CountParam, HeightPaginationParams, OrderParam,
    },
    util::{
        build_rune_activity, check_op_return_script, ParsedHeightPaginationParams, RuneIdentifier,
    },
};

#[derive(Debug, Deserialize)]
pub struct FilterParams {
    pub rune: Option<String>,
    pub exclude_self_transfers: Option<bool>,
    pub activity_kind: Option<RuneActivityKindByAddress>,
}

pub static REQUIRED_REDUCERS: &[ReducerType] = &[
    ReducerType::RuneIdByRuneName,
    ReducerType::EtchingByRuneId,
    ReducerType::RuneTxsByScriptHash,
    // parsing address parameter
    ReducerType::ScriptByScriptHash,
    ReducerType::ScriptHashByAddressPayloadHash,
];

#[utoipa::path(
    tag = "Addresses",
    get,
    path = "/addresses/{address}/runes/activity",
    params(
        ("address" = String, Path, description = "Bitcoin address or hex encoded script pubkey", example="bc1p27j3fa2mr3d50m3uaavr0ntyzr0v2a27n48lc9gxpkzd4xye6dgs2tzx6p"),

        // Pagination params applicable regardless of the sorting order and property.
        ("order" = inline(Option<OrderParam>), Query, description = "The order in which the results are sorted. Supported values: asc, desc"),
        ("count" = inline(Option<CountParam>), Query, description = "The max number of results per page"),
        ("from" = inline(Option<u64>), Query, description = "Return only transactions created on or after a specific height"),
        ("to" = inline(Option<u64>), Query, description = "Return only transactions created on or before a specific height"),
        ("cursor" = inline(Option<String>), Query, description = "Pagination cursor string, use the cursor included in a page of results to fetch the next page"),

        // Filter by rune.
        ("rune" = Option<String>, Query, description = "Return only transactions containing a specific rune, specified either by the rune ID (etching block number and transaction index) or name (spaced or un-spaced). In presence of activity_kind, it relates to this specific rune. In presence of exclude_self_transfers, it is this specific rune that the queried address should see increase or decrease in balance in the tx, not just being self-transferred.", example="840000:3"),

        // Filter by activity kind.
        ("activity_kind" = Option<RuneActivityKindByAddress>, Query, description = "Filter txs by presence specific activity kind. Supported values: increased, decreased, self_transfer. In presence of rune filter, the activity kind relates to that specific rune. In presence of exclude_self_transfers, this activity kind cannot be self_transfer."),

        // Filter self-transfers out.
        ("exclude_self_transfers" = Option<bool>, Query, description = "Exclude txs only containing runes self-transfers. In presence of activity_kind, it cannot be self_transfer. In presence of rune filter, that specific rune should be sent or received, not self-transferred."),
    ),
    responses(
        (
            status = 200,
            description = "Requested data",
            body = PaginatedRuneActivityByAddress,
            example = json!(serde_json::Value::from_str(EXAMPLE_RESPONSE).unwrap())
        ),
        (status = 400, description = "Malformed query parameters"),
        (status = 404, description = "Requested entity not found on-chain"),
        (status = 500, description = "Internal server error"),
    )
)]
#[tracing::instrument(name = "RUNE_ACTIVITY_BY_ADDRESS", level = "info", skip(tikv))]
/// Rune Activity by Address
///
/// Return all transactions where the specified address has rune activity, with the option to filter by a specific rune kind.
pub async fn rune_activity_by_address(
    Path(addr_or_pk): Path<String>,
    page_params: Query<HeightPaginationParams>,
    filter_params: Query<FilterParams>,
    mut tikv: Extension<TiKVAdapter>,
) -> Result<impl IntoResponse, Error> {
    tikv.init_tip(REQUIRED_REDUCERS).await?;

    let last_updated = tikv.get_snapshot_point()?;

    // Initialize `next_cursor`.
    let mut next_cursor: Option<String> = None;

    let rune_txs_encoder = tikv.get_encoder(ReducerType::RuneTxsByScriptHash)?;

    // Parse and try decode user params.
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

    // Check if the script is an OP_RETURN script pubkey and reject if so.
    check_op_return_script(&script)?;

    let script_hash = script.script_hash().to_byte_array();

    // Parse pagination params.
    let page_params = ParsedHeightPaginationParams::parse::<_, RuneTxsByScriptHashCursor>(
        page_params.0,
        &rune_txs_encoder,
        &Reducer::RuneTxsByScriptHash,
        Some(script_hash),
    )?;

    // Parse rune filter.
    let rune_filter = if let Some(rune_str) = &filter_params.rune {
        match RuneIdentifier::parse(rune_str.clone())? {
            RuneIdentifier::Id(rune_id) => Some(rune_id),
            RuneIdentifier::Name(n) => Some(tikv.resolve_rune_name(n).await?.unwrap_or_default()),
        }
    } else {
        None
    };

    // Parse `exclude_self_transfers` filter param.
    let excl_self_transfers: bool = filter_params.exclude_self_transfers.unwrap_or(false);

    // Rule out the ill-formed combination of `excl_self_transfers` on and `activity_kind` set to self-transfers.
    if excl_self_transfers
        && filter_params.activity_kind == Some(RuneActivityKindByAddress::SelfTransfer)
    {
        return Err(Error::MalformedRequest(
            "Ill-formed combination of excl_self_transfers and activity_kind.".into(),
        ));
    }

    let kvs = if let Some(activity_kind) = &filter_params.activity_kind {
        // There's an activity kind filter. There may also be a rune filter and the exclude
        // self-transfers flag may be on, although we don't need to check the latter because we've
        // already ruled out the ill-formed case.
        let filter_fn = Some(|kv: &KvPair| {
            let value = RuneTxsByScriptHashValue::decode(&kv.1).unwrap().0;

            if let Some(id) = rune_filter {
                // If both a rune filter and an activity kind filter is provided, they are
                // combined.
                match activity_kind {
                    RuneActivityKindByAddress::SelfTransfer => value
                        .self_transfers
                        .into_iter()
                        .any(|(rune_id, _)| rune_id == id),
                    RuneActivityKindByAddress::Increase => value
                        .increased_balances
                        .into_iter()
                        .any(|(rune_id, _)| rune_id == id),
                    RuneActivityKindByAddress::Decrease => value
                        .decreased_balances
                        .into_iter()
                        .any(|(rune_id, _)| rune_id == id),
                }
            } else {
                // If an activity kind filter is provided but there's no rune filter, we check for
                // non-emptiness of the specific kind of activity independently of any particular
                // rune.
                match activity_kind {
                    RuneActivityKindByAddress::SelfTransfer => !value.self_transfers.is_empty(),
                    RuneActivityKindByAddress::Increase => !value.increased_balances.is_empty(),
                    RuneActivityKindByAddress::Decrease => !value.decreased_balances.is_empty(),
                }
            }
        });

        Scanner::new(page_params.key_range())
            .count(page_params.count() + 1)
            .order(page_params.order())
            .execute_with_filter::<RuneTxsByScriptHashKey, RuneTxsByScriptHashValue, _>(
                &mut tikv,
                ReducerType::RuneTxsByScriptHash,
                filter_fn,
            )
            .await?
    } else if excl_self_transfers {
        // There's no activity kind filter, but there may still be a rune filter.
        let filter_fn = Some(|kv: &KvPair| {
            let value = RuneTxsByScriptHashValue::decode(&kv.1).unwrap().0;

            // Exclude activity that only involves self-transfers.
            if let Some(id) = rune_filter {
                // Rune must be in either increased or decreased balances.
                value
                    .increased_balances
                    .into_iter()
                    .any(|(rune_id, _)| rune_id == id)
                    || value
                        .decreased_balances
                        .into_iter()
                        .any(|(rune_id, _)| rune_id == id)
            } else {
                // If no rune filter is provided, we exclude txs whose activity consists only of self-transfers.
                value.self_transfers.is_empty()
                    || !value.increased_balances.is_empty()
                    || !value.decreased_balances.is_empty()
            }
        });

        Scanner::new(page_params.key_range())
            .count(page_params.count() + 1)
            .order(page_params.order())
            .execute_with_filter::<RuneTxsByScriptHashKey, RuneTxsByScriptHashValue, _>(
                &mut tikv,
                ReducerType::RuneTxsByScriptHash,
                filter_fn,
            )
            .await?
    } else if let Some(id) = rune_filter {
        // Rune must be part of the activity of the address, either in self-transfers, increased
        // balances or decreased balances.
        let filter_fn = Some(|kv: &KvPair| {
            let value = RuneTxsByScriptHashValue::decode(&kv.1).unwrap().0;

            value
                .self_transfers
                .into_iter()
                .any(|(rune_id, _)| rune_id == id)
                || value
                    .increased_balances
                    .into_iter()
                    .any(|(rune_id, _)| rune_id == id)
                || value
                    .decreased_balances
                    .into_iter()
                    .any(|(rune_id, _)| rune_id == id)
        });

        Scanner::new(page_params.key_range())
            .count(page_params.count() + 1)
            .order(page_params.order())
            .execute_with_filter::<RuneTxsByScriptHashKey, RuneTxsByScriptHashValue, _>(
                &mut tikv,
                ReducerType::RuneTxsByScriptHash,
                filter_fn,
            )
            .await?
    } else {
        Scanner::new(page_params.key_range())
            .count(page_params.count() + 1)
            .order(page_params.order())
            .execute::<RuneTxsByScriptHashKey, RuneTxsByScriptHashValue>(
                &mut tikv,
                ReducerType::RuneTxsByScriptHash,
            )
            .await?
    };

    let mut kvs = kvs.into_iter().enumerate();

    // `decimals_and_minting` is used to avoid re-fetching etching terms for rune kinds that we have already processed.
    let mut decimals_and_minting: HashMap<(u64, u32), (usize, u128)> = HashMap::new();

    let mut activity: Vec<RuneActivityByAddress> = vec![];

    // Process fetched kvs.
    while let Some((i, (key, value))) = kvs.next() {
        // If this is the last result of the page, check if there is a subsequent result (and
        // therefore we need to return a cursor for next page).
        if i == (page_params.count() - 1) && kvs.next().is_some() {
            next_cursor = Some(
                RuneTxsByScriptHashCursor {
                    height: key.height,
                    activity_tx_index: key.activity_tx_index,
                    tx_hash: key.tx_hash,
                }
                .encode_base64(),
            );
        }

        // Build rune activity.
        let rune_activity =
            build_rune_activity(value, &mut decimals_and_minting, &mut tikv).await?;

        activity.push(RuneActivityByAddress {
            height: key.height,
            confirmations: (last_updated.block_height + 1).saturating_sub(key.height),
            tx_hash: Txid::from_byte_array(key.tx_hash).to_string(),
            rune_activity,
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
        "height": 875944,
        "confirmations": 21684,
        "tx_hash": "6a4237d43719e0766a8333eb8531b3bae9b20e7484e6962a731773b883315da1",
        "etched_rune": null,
        "minted": null,
        "self_transfers": [],
        "increased_balances": [{
            "rune_id": "840000:28",
            "amount": "1453573"
        }],
        "decreased_balances": []
    }, {
        "height": 876070,
        "confirmations": 21558,
        "tx_hash": "1c2c45980432108fda7b4eb2a390bd1cae7aac62b9309f5ef3911960e50b8501",
        "etched_rune": null,
        "minted": null,
        "self_transfers": [{
            "rune_id": "840000:28",
            "amount": "1453573"
        }],
        "increased_balances": [],
        "decreased_balances": []
    }, {
        "height": 876103,
        "confirmations": 21525,
        "tx_hash": "cbcbac069d142c303b062dda88635e4939d69120f66d89a9da17ebd0ff806a1f",
        "etched_rune": null,
        "minted": null,
        "self_transfers": [],
        "increased_balances": [],
        "decreased_balances": [{
            "rune_id": "840000:28",
            "amount": "207654"
        }]
    }],
    "last_updated": {
        "block_hash": "0000000000000000000119bd8dffd7d8285a69744011aa98f0d9091b0555ca46",
        "block_height": 897627
    },
    "next_cursor": null
}"##;
