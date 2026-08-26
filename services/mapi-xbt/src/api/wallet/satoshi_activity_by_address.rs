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
    api::wallet::common::zip_with_exchange_rates,
    error::Error,
    options::arranger::Arranger,
    tikv::{adapter::TiKVAdapter, key_resolver::ReducerType, Scanner},
    types::{
        ActivityKindByAddress, CountParam, HeightPaginationParams, MempoolLastUpdated,
        MempoolWalletPaginatedResponse, OrderParam, WalletActivityByAddress, WalletSatActivity,
    },
    util::{
        check_op_return_script, estimate_indexer_blocks, timestamp_to_string,
        ParsedHeightPaginationParams,
    },
};

#[derive(Debug, Deserialize)]
pub struct QueryParams {
    pub activity_kind: Option<ActivityKindByAddress>,
    pub exclude_self_transfers: Option<bool>,
    pub mempool: Option<bool>,
}

pub static REQUIRED_REDUCERS: &[ReducerType] = &[
    ReducerType::SatTxsByScriptHash,
    // resolving script hashes
    ReducerType::ScriptByScriptHash,
    ReducerType::ScriptHashByAddressPayloadHash,
    // Required to fetch chain tip timestamp and query Arranger for USD-BTC exchange rate with.
    ReducerType::BlockInfo,
    // estimated block fees
    ReducerType::SatsPerVbByBlock,
];

#[utoipa::path(
    tag = "Addresses",
    get,
    path = "/wallet/addresses/{address}/activity",
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

        // Include mempool data.
        ("mempool" = Option<bool>, Query, description = "Include mempool data. Default: true."),
    ),
    responses(
        (
            status = 200,
            description = "Requested data",
            body = MempoolWalletPaginatedActivityByAddress,
            example = json!(serde_json::Value::from_str(EXAMPLE_RESPONSE).unwrap())
        ),
        (status = 400, description = "Malformed query parameters"),
        (status = 404, description = "Requested entity not found on-chain"),
        (status = 500, description = "Internal server error"),
    )
)]
#[tracing::instrument(
    name = "WALLET_SATOSHI_ACTIVITY_BY_ADDRESS",
    level = "info",
    skip(tikv, arranger)
)]
/// Wallet Satoshi Activity by Address (Mempool-aware)
///
/// Returns all transactions for a given address or script pubkey, allowing insight into when the balance increased, decreased, or remained the same. Mempool data is included by default. This endpoint supports customization to narrow results by time, transaction type, or ordering, enabling tailored historical views.
pub async fn wallet_satoshi_activity_by_address(
    Path(addr_or_pk): Path<String>,
    page_params: Query<HeightPaginationParams>,
    query_params: Query<QueryParams>,
    mut tikv: Extension<TiKVAdapter>,
    Extension(arranger): Extension<Arranger>,
) -> Result<impl IntoResponse, Error> {
    // Parse `mempool` query param, for enabling / disabling response data from the mempool.
    let mempool_mode: bool = query_params.mempool.unwrap_or(true);

    // Initialize mempool-specific values.
    if mempool_mode {
        // Take as many mempool blocks as available.
        tikv.init_mempool(REQUIRED_REDUCERS, None).await?;
    } else {
        // Do not take any mempool blocks.
        tikv.init_tip(REQUIRED_REDUCERS).await?;
    }
    let snapshot_chain_tip = tikv.get_snapshot_point()?;
    let snapshot_mempool_view = tikv.get_snapshot_mempool_info()?;
    let found_mempool_blocks = snapshot_mempool_view.map(|x| x.mempool_blocks).unwrap_or(0);

    // Initialize `next_cursor`.
    let mut next_cursor: Option<String> = None;

    let sat_txs_encoder = tikv.get_encoder(ReducerType::SatTxsByScriptHash)?;

    let script_bytes = match tikv.parse_address_or_script_bytes(&addr_or_pk).await {
        Ok((_, script_bytes)) => script_bytes,
        Err(Error::NotFound) => {
            // User param is a Bitcoin address, but the corresponding script pub key could not be
            // found in store.
            let out = MempoolWalletPaginatedResponse {
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

    // Check if the script is an OP_RETURN script pubkey and reject if so.
    check_op_return_script(&script)?;

    let script_hash = script.script_hash().to_byte_array();

    // Parse pagination params.
    let page_params = ParsedHeightPaginationParams::parse::<_, SatTxsByScriptHashCursor>(
        page_params.0,
        &sat_txs_encoder,
        &Reducer::SatTxsByScriptHash,
        Some(script_hash),
    )?;

    // Parse `exclude_self_transfers` filter param.
    let excl_self_transfers: bool = query_params.exclude_self_transfers.unwrap_or(false);

    // Rule out the ill-formed combination of `excl_self_transfers` on and `activity_kind` set to self-transfers.
    if excl_self_transfers
        && query_params.activity_kind == Some(ActivityKindByAddress::SelfTransfer)
    {
        return Err(Error::MalformedRequest(
            "Ill-formed combination of excl_self_transfers and activity_kind.".into(),
        ));
    }

    // Fetch KVs from store.
    let kvs = if let Some(activity_kind) = &query_params.activity_kind {
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
    };

    // Zip KVs with the USD prices for each of them (None if no price service is configured).
    let mut kvs = zip_with_exchange_rates(
        kvs.clone(),
        &snapshot_chain_tip.block_height,
        arranger.get_sat_prices_path().as_deref(),
        &mut tikv,
    )
    .await?
    .enumerate();

    let mut activity: Vec<WalletActivityByAddress> = vec![];

    // Process fetched kvs.
    while let Some((i, ((key, value), exchange_rate))) = kvs.next() {
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

        activity.push(WalletActivityByAddress {
            height: key.height,
            confirmations: (snapshot_chain_tip.block_height + 1).saturating_sub(key.height),
            mempool: key.height > snapshot_chain_tip.block_height,
            tx_hash: Txid::from_byte_array(key.tx_hash).to_string(),
            sat_activity: WalletSatActivity {
                kind: match value.activity_type {
                    SatActivityType::SelfTransferred => ActivityKindByAddress::SelfTransfer,
                    SatActivityType::Increased => ActivityKindByAddress::Increase,
                    SatActivityType::Decreased => ActivityKindByAddress::Decrease,
                },
                amount: value.amount.to_string(),
                usd_amount: exchange_rate.map(|exchange_rate| {
                    format!("{:.2}", (value.amount as f64 * exchange_rate) / 100000000.0)
                }),
            },
        });
    }

    let out = MempoolWalletPaginatedResponse {
        data: activity,
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

pub static EXAMPLE_RESPONSE: &str = r##"{
    "data": [{
        "height": 901937,
        "confirmations": 1,
        "mempool": false,
        "tx_hash": "7032180634bc691471b92099250b9370e07a0c8c8ca1420e518806404c7b6cf3",
        "sat_activity": {
            "kind": "increase",
            "amount": "603733",
            "usd_amount": "629.17"
        }
    }],
    "indexer_info": {
        "chain_tip": {
            "block_hash": "00000000000000000001ae26ce7b25ef2bd13f4c0069b634a233b1472f0c0a17",
            "block_height": 901937
        },
        "mempool_timestamp": "2025-06-19 19:57:37",
        "estimated_blocks": [{
            "block_height": 901938,
            "sats_per_vb": {
                "min": 1,
                "median": 4,
                "max": 99
            }
        }]
    },
    "next_cursor": null
}"##;
