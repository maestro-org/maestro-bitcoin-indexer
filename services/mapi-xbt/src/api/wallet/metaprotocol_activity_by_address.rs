use axum::{
    extract::{Path, Query},
    response::IntoResponse,
    Extension, Json,
};
use bitcoin::{hashes::Hash, Address, Script, Txid};
use reqwest::StatusCode;
use serde::Deserialize;
use std::collections::HashMap;
use std::str::FromStr;
use timbre_xbt::{
    reducers::{
        inscription_activity_by_script_hash::{
            Key as InscriptionActivityByScriptHashKey,
            Value as InscriptionActivityByScriptHashValue,
        },
        rune_txs_by_script_hash::{
            Key as RuneTxsByScriptHashKey, Value as RuneTxsByScriptHashValue,
        },
        sat_txs_by_script_hash::{
            Cursor as SatTxsByScriptHashCursor, Key as SatTxsByScriptHashKey, SatActivityType,
            Value as SatTxsByScriptHashValue,
        },
        Height,
    },
    Encode, Reducer,
};

use crate::{
    api::wallet::common::{build_rune_prices_map, zip_with_exchange_rates},
    error::Error,
    options::{arranger::Arranger, Mode},
    tikv::{adapter::TiKVAdapter, key_resolver::ReducerType, Scanner},
    types::{
        ActivityKindByAddress, CountParam, HeightPaginationParams, MempoolLastUpdated,
        MempoolWalletPaginatedResponse, OrderParam, WalletActivityByAddressWithMetaprotocols,
        WalletSatActivity,
    },
    util::{
        build_inscription_activity, build_wallet_rune_activity, check_op_return_script,
        estimate_indexer_blocks, timestamp_to_string, ParsedHeightPaginationParams,
    },
};

#[derive(Debug, Deserialize)]
pub struct QueryParams {
    pub mempool: Option<bool>,
}

pub static REQUIRED_REDUCERS: &[ReducerType] = &[
    // Parsing address param and resolving script hashes.
    ReducerType::ScriptByScriptHash,
    ReducerType::ScriptHashByAddressPayloadHash,
    // BTC activity.
    ReducerType::SatTxsByScriptHash,
    // Inscription activity.
    ReducerType::InscriptionActivityByScriptHash,
    // Rune activity.
    ReducerType::RuneTxsByScriptHash,
    // Rune etching terms, including decimals and minting.
    ReducerType::EtchingByRuneId,
    // Required to fetch chain tip timestamp and query Arranger for USD-BTC exchange rate with.
    ReducerType::BlockInfo,
    // Estimated block fees.
    ReducerType::SatsPerVbByBlock,
];

#[utoipa::path(
    tag = "Addresses",
    get,
    path = "/wallet/addresses/{address}/activity/metaprotocols",
    params(
        ("address" = String, Path, description = "Bitcoin address or hex encoded script pubkey", example="bc1qcx7ys0ahvtfqcc63sfn6axls0qrhkadnslpd94"),

        // Pagination params applicable regardless of the sorting order and property.
        ("order" = inline(Option<OrderParam>), Query, description = "The order in which the results are sorted. Supported values: asc, desc"),
        ("count" = inline(Option<CountParam>), Query, description = "The max number of results per page"),
        ("from" = inline(Option<u64>), Query, description = "Return only UTxOs created on or after a specific height"),
        ("to" = inline(Option<u64>), Query, description = "Return only UTxOs created on or before a specific height"),
        ("cursor" = inline(Option<String>), Query, description = "Pagination cursor string, use the cursor included in a page of results to fetch the next page"),

        // Include mempool data.
        ("mempool" = Option<bool>, Query, description = "Include mempool data. Default: true."),
    ),
    responses(
        (
            status = 200,
            description = "Requested data",
            body = MempoolWalletPaginatedActivityByAddressWithMetaprotocols,
            example = json!(serde_json::Value::from_str(EXAMPLE_RESPONSE).unwrap())
        ),
        (status = 400, description = "Malformed query parameters"),
        (status = 404, description = "Requested entity not found on-chain"),
        (status = 500, description = "Internal server error"),
    )
)]
#[tracing::instrument(
    name = "WALLET_METAPROTOCOL_ACTIVITY_BY_ADDRESS",
    level = "info",
    skip(tikv, mode, arranger)
)]
/// Metaprotocol Activity by Address
///
/// Return all transactions where the specified address has satoshi and/or metaprotocols activity. Supported metaprotocols: runes, inscriptions.
pub async fn wallet_metaprotocol_activity_by_address(
    Path(addr_or_pk): Path<String>,
    page_params: Query<HeightPaginationParams>,
    query_params: Query<QueryParams>,
    mut tikv: Extension<TiKVAdapter>,
    mode: Extension<Mode>,
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

    // Parse address param.
    let (query_address, query_script_bytes) =
        match tikv.parse_address_or_script_bytes(&addr_or_pk).await {
            Ok(x) => x,
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
    let script = Script::from_bytes(&query_script_bytes);

    // Check if the script is an OP_RETURN script pubkey and reject if so.
    check_op_return_script(&script)?;

    let script_hash = script.script_hash().to_byte_array();

    // Since every time there's rune and inscription activity we know there also sat activity,
    // we use the `SatTxsByScriptHash` reducer for building the pagination cursor.
    let sat_txs_encoder = tikv.get_encoder(ReducerType::SatTxsByScriptHash)?;

    // Parse pagination params.
    let page_params = ParsedHeightPaginationParams::parse::<_, SatTxsByScriptHashCursor>(
        page_params.0,
        &sat_txs_encoder,
        &Reducer::SatTxsByScriptHash,
        Some(script_hash),
    )?;

    // Scan `SatTxsByScriptHash` KVs.
    let kvs = Scanner::new(page_params.key_range())
        .count(page_params.count() + 1)
        .order(page_params.order())
        .execute::<SatTxsByScriptHashKey, SatTxsByScriptHashValue>(
            &mut tikv,
            ReducerType::SatTxsByScriptHash,
        )
        .await?;

    // Fetch rune prices and compute USD equivalent for the rune activity.
    let mut cached_rune_activity = HashMap::new();
    let mut rune_activity_for_prices = vec![];

    for (key, _) in kvs.iter() {
        let rune_activity_key = RuneTxsByScriptHashKey {
            script_hash: key.script_hash,
            height: key.height,
            activity_tx_index: key.activity_tx_index,
            tx_hash: key.tx_hash,
        };
        let optional_rune_activity_value = tikv
            .get_reducer_key_maybe::<RuneTxsByScriptHashKey, RuneTxsByScriptHashValue>(
                (
                    ReducerType::RuneTxsByScriptHash,
                    Reducer::RuneTxsByScriptHash,
                ),
                &rune_activity_key,
            )
            .await?;

        cached_rune_activity.insert(
            rune_activity_key.clone(),
            optional_rune_activity_value.clone(),
        );

        if let Some(rune_activity_value) = optional_rune_activity_value {
            rune_activity_for_prices.push((rune_activity_key, rune_activity_value));
        }
    }

    // `None` when no external price service is configured; USD amounts are then null.
    let rune_prices: Option<HashMap<Height, HashMap<String, f64>>> =
        if !rune_activity_for_prices.is_empty() {
            build_rune_prices_map(
                &rune_activity_for_prices,
                snapshot_chain_tip.block_height,
                arranger.get_rune_prices_path().as_deref(),
                &mut tikv,
            )
            .await?
        } else {
            arranger.get_rune_prices_path().map(|_| HashMap::new())
        };

    // Zip KVs with the USD prices for each of them (None if no price service is configured).
    let kvs = zip_with_exchange_rates(
        kvs.clone(),
        &snapshot_chain_tip.block_height,
        arranger.get_sat_prices_path().as_deref(),
        &mut tikv,
    )
    .await?;

    // Resolve each script hash only once.
    let mut resolved_script_hashes: HashMap<[u8; 20], (Option<Address>, Vec<u8>)> = HashMap::new();

    // `decimals_and_minting` is used to avoid re-fetching etching terms for rune kinds that we have already processed.
    let mut decimals_and_minting: HashMap<(u64, u32), (usize, u128)> = HashMap::new();

    // Initialize response data vector.
    let mut activity: Vec<WalletActivityByAddressWithMetaprotocols> = vec![];

    let mut kvs = kvs.enumerate();

    // Process fetched KVs, adding to each the inscription and rune activity if they exist.
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

        // Fetch inscription activity in this tx, and structure data accordingly if found.
        let inscription_activity = tikv
            .get_reducer_key_maybe::<InscriptionActivityByScriptHashKey, InscriptionActivityByScriptHashValue>(
                (
                    ReducerType::InscriptionActivityByScriptHash,
                    Reducer::InscriptionActivityByScriptHash,
                ),
                &InscriptionActivityByScriptHashKey {
                    script_hash: key.script_hash,
                    height: key.height,
                    activity_tx_index: key.activity_tx_index,
                    tx_hash: key.tx_hash,
                },
            )
            .await?;

        let inscription_activity = if let Some(inscription_activity) = inscription_activity {
            Some(
                build_inscription_activity(
                    inscription_activity,
                    &query_address,
                    &query_script_bytes,
                    key.tx_hash,
                    key.height,
                    &mut resolved_script_hashes,
                    &mut tikv,
                    &mode,
                )
                .await?,
            )
        } else {
            None
        };

        // Fetch rune activity in this tx, and structure data accordingly if found.
        let rune_activity_key = RuneTxsByScriptHashKey {
            script_hash: key.script_hash,
            height: key.height,
            activity_tx_index: key.activity_tx_index,
            tx_hash: key.tx_hash,
        };

        let rune_activity =
            if let Some(Some(rune_activity_value)) = cached_rune_activity.get(&rune_activity_key) {
                Some(
                    build_wallet_rune_activity(
                        rune_activity_value.clone(),
                        key.height,
                        &rune_prices,
                        &mut decimals_and_minting,
                        &mut tikv,
                    )
                    .await?,
                )
            } else {
                None
            };

        activity.push(WalletActivityByAddressWithMetaprotocols {
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
            inscription_activity,
            rune_activity,
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

static EXAMPLE_RESPONSE: &str = r##"{
    "data": [{
        "height": 902843,
        "confirmations": 53,
        "mempool": false,
        "tx_hash": "4717042047235b65fa7cf7d7a5fe2f0b9d51398f5c84676b65bde053edbac418",
        "sat_activity": {
            "kind": "decrease",
            "amount": "546",
            "usd_amount": "0.59"
        },
        "inscription_activity": null,
        "rune_activity": {
            "etched_rune": null,
            "minted": null,
            "self_transfers": [],
            "increased_balances": [],
            "decreased_balances": [{
                "rune_id": "840000:3",
                "amount": "209938.14943",
                "usd_amount": "863.26"
            }]
        }
    }, {
        "height": 902847,
        "confirmations": 49,
        "mempool": false,
        "tx_hash": "fe9cc01f0a2446da2bb71474652643d7b3d1d0c41fd05358373e0df921024575",
        "sat_activity": {
            "kind": "increase",
            "amount": "546",
            "usd_amount": "0.59"
        },
        "inscription_activity": {
            "self_transferred": [],
            "sent": [],
            "received": [{
                "inscription_id": "e484a11516b74a06f5d104a83b1974db8d26e7a38cbb495d29bf5ed6b1f4e156i277",
                "from": {
                  "address": "bc1pqqeyklpuh5kx6yg3zqwy0tn9ysxtg6un0y7dl0hp6wz5y5xwvsvs6due29",
                  "script_pubkey": "512000324b7c3cbd2c6d1111101c47ae65240cb46b93793cdfbee1d3854250ce6419",
                  "input_index": 0,
                  "sat_offset": 0
                },
                "to": {
                  "address": "bc1px7ff6446jwmh79uu9df6dejvqayn9d6tlvwe5tudehj4j0cz58xsfr0dw9",
                  "script_pubkey": "512037929d56ba93b77f179c2b53a6e64c074932b74bfb1d9a2f8dcde5593f02a1cd",
                  "output_vout": 0,
                  "sat_offset": 0,
                  "output_txid": "fe9cc01f0a2446da2bb71474652643d7b3d1d0c41fd05358373e0df921024575"
                }
            }]
        },
        "rune_activity": null
    }],
    "indexer_info": {
        "chain_tip": {
            "block_hash": "00000000000000000001929ab2c8fa214ccb7f025c9b591514adfc39c8d18fdd",
            "block_height": 902895
        },
        "mempool_timestamp": null,
        "estimated_blocks": []
    },
    "next_cursor": null
}"##;
