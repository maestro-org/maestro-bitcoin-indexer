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
use tikv_client::KvPair;
use timbre_xbt::{
    reducers::inscription_activity_by_script_hash::{
        Cursor as InscriptionActivityByScriptHashCursor, Key as InscriptionActivityByScriptHashKey,
        Value as InscriptionActivityByScriptHashValue,
    },
    Decode, Encode, Reducer,
};

use crate::{
    error::Error,
    options::Mode,
    tikv::{adapter::TiKVAdapter, key_resolver::ReducerType, Scanner},
    types::{
        inscriptions::{InscriptionActivityKindByAddress, WalletInscriptionActivityByAddress},
        CountParam, HeightPaginationParams, MempoolLastUpdated, MempoolWalletPaginatedResponse,
        OrderParam,
    },
    util::{
        build_inscription_activity, check_op_return_script, estimate_indexer_blocks,
        parse_inscription_id, timestamp_to_string, ParsedHeightPaginationParams,
    },
};

#[derive(Debug, Deserialize)]
pub struct QueryParams {
    pub activity_kind: Option<InscriptionActivityKindByAddress>,
    pub exclude_self_transfers: Option<bool>,
    pub inscription_id: Option<String>,
    pub mempool: Option<bool>,
}

pub static REQUIRED_REDUCERS: &[ReducerType] = &[
    ReducerType::InscriptionActivityByScriptHash,
    // resolving script hashes
    ReducerType::ScriptByScriptHash,
    ReducerType::ScriptHashByAddressPayloadHash,
    // estimated block fees
    ReducerType::SatsPerVbByBlock,
];

#[utoipa::path(
    tag = "Addresses",
    get,
    path = "/wallet/addresses/{address}/inscriptions/activity",
    params(
        ("address" = String, Path, description = "Bitcoin address or hex encoded script pubkey", example="bc1p27j3fa2mr3d50m3uaavr0ntyzr0v2a27n48lc9gxpkzd4xye6dgs2tzx6p"),

        // Pagination params applicable regardless of the sorting order and property.
        ("order" = inline(Option<OrderParam>), Query, description = "The order in which the results are sorted. Supported values: asc, desc"),
        ("count" = inline(Option<CountParam>), Query, description = "The max number of results per page"),
        ("from" = inline(Option<u64>), Query, description = "Return only transactions created on or after a specific height"),
        ("to" = inline(Option<u64>), Query, description = "Return only transactions created on or before a specific height"),
        ("cursor" = inline(Option<String>), Query, description = "Pagination cursor string, use the cursor included in a page of results to fetch the next page"),

        // Filter by presence of specific inscription.
        ("inscription_id" = Option<String>, Query, description = "Return only transactions containing a specific inscription, specified by an inscription ID. In presence of activity_kind, it relates to this specific inscription. In presence of exclude_self_transfers, it is this specific inscription that should be sent or received but not self-transferred.", example="6fb976ab49dcec017f1e201e84395983204ae1a7c2abf7ced0a85d692e442799i0"),

        // Filter by activity kind.
        ("activity_kind" = Option<InscriptionActivityKindByAddress>, Query, description = "Filter txs by presence of specific activity kind. Supported values: send, receive, self_transfer. In presence of inscription filter, the activity kind relates to that specific inscription. In presence of exclude_self_transfers, this activity kind cannot be self_transfer."),

        // Filter self-transfers out.
        ("exclude_self_transfers" = Option<bool>, Query, description = "Exclude txs only containing inscriptions self-transfers. In presence of activity_kind, it cannot be self_transfer. In presence of inscription filter, that specific inscription should be sent or received, not self-transferred."),

        // Include mempool data.
        ("mempool" = Option<bool>, Query, description = "Include mempool data. Default: true."),
    ),
    responses(
        (
            status = 200,
            description = "Requested data",
            body = MempoolWalletPaginatedInscriptionActivityByAddress,
            example = json!(serde_json::Value::from_str(EXAMPLE_RESPONSE).unwrap())
        ),
        (status = 400, description = "Malformed query parameters"),
        (status = 404, description = "Requested entity not found on-chain"),
        (status = 500, description = "Internal server error"),
    )
)]
#[tracing::instrument(
    name = "WALLET_INSCRIPTION_ACTIVITY_BY_ADDRESS",
    level = "info",
    skip(tikv, mode)
)]
/// Inscription Activity by Address (Mempool-aware)
///
/// Returns all inscription-related transactions involving a specific address. Can be filtered by activity type (send, receive, self-transfer), narrowed to a specific inscription, and sorted chronologically. Mempool data is included by default. Useful for building dashboards, tracking user behavior, or filtering unwanted spam activity.
pub async fn wallet_inscription_activity_by_address(
    Path(addr_or_pk): Path<String>,
    page_params: Query<HeightPaginationParams>,
    query_params: Query<QueryParams>,
    mut tikv: Extension<TiKVAdapter>,
    mode: Extension<Mode>,
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

    let inscription_activity_encoder =
        tikv.get_encoder(ReducerType::InscriptionActivityByScriptHash)?;

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

    // Parse pagination params.
    let page_params =
        ParsedHeightPaginationParams::parse::<_, InscriptionActivityByScriptHashCursor>(
            page_params.0,
            &inscription_activity_encoder,
            &Reducer::InscriptionActivityByScriptHash,
            Some(script_hash),
        )?;

    // Parse `inscription_id` filter param.
    let inscription = match &query_params.inscription_id {
        Some(inscription_id) => Some(parse_inscription_id(&inscription_id)?),
        None => None,
    };

    // Parse `exclude_self_transfers` filter param.
    let excl_self_transfers: bool = query_params.exclude_self_transfers.unwrap_or(false);

    // Rule out the ill-formed combination of `excl_self_transfers` on and `activity_kind` set to self-transfers.
    if excl_self_transfers
        && query_params.activity_kind == Some(InscriptionActivityKindByAddress::SelfTransfer)
    {
        return Err(Error::MalformedRequest(
            "Ill-formed combination of excl_self_transfers and activity_kind.".into(),
        ));
    }

    let kvs = if let Some(activity_kind) = &query_params.activity_kind {
        // There's an activity kind filter. There may also be an inscription filter and the exclude
        // self-transfers flag may be on. Still, we don't need to check `excl_self_transfers`
        // because we have already ruled out the ill-formed case.
        let filter_fn = Some(|kv: &KvPair| {
            let value = InscriptionActivityByScriptHashValue::decode(&kv.1)
                .unwrap()
                .0;

            if let Some(id) = inscription {
                // If both an inscription filter and an activity kind filter is provided, they are
                // combined.
                match activity_kind {
                    InscriptionActivityKindByAddress::SelfTransfer => {
                        value.self_transfers.iter().any(|x| x.inscription_id == id)
                    }
                    InscriptionActivityKindByAddress::Send => {
                        value.sent.iter().any(|x| x.inscription_id == id)
                    }
                    InscriptionActivityKindByAddress::Receive => {
                        value.received.iter().any(|x| x.inscription_id == id)
                    }
                }
            } else {
                // If an activity kind filter is provided but there's no inscription filter, we
                // check for non-emptiness of each kind of activity independently of the
                // particular inscription it involves.
                match activity_kind {
                    InscriptionActivityKindByAddress::SelfTransfer => {
                        !value.self_transfers.is_empty()
                    }
                    InscriptionActivityKindByAddress::Send => !value.sent.is_empty(),
                    InscriptionActivityKindByAddress::Receive => !value.received.is_empty(),
                }
            }
        });

        Scanner::new(page_params.key_range())
            .count(page_params.count() + 1)
            .order(page_params.order())
            .execute_with_filter::<InscriptionActivityByScriptHashKey, InscriptionActivityByScriptHashValue, _>(
                &mut tikv,
                ReducerType::InscriptionActivityByScriptHash,
                filter_fn,
            )
            .await?
    } else if excl_self_transfers {
        // There's no activity kind filter. There may still be an inscription filter.
        let filter_fn = Some(|kv: &KvPair| {
            let value = InscriptionActivityByScriptHashValue::decode(&kv.1)
                .unwrap()
                .0;

            // Exclude activity that only involves self-transfers.
            if let Some(id) = inscription {
                // Inscription must be in either sent or received.
                value.sent.iter().any(|x| x.inscription_id == id)
                    || value.received.iter().any(|x| x.inscription_id == id)
            } else {
                // If no inscription filter is provided, we exclude txs whose activity consists only of self-transfers.
                value.self_transfers.is_empty()
                    || !value.sent.is_empty()
                    || !value.received.is_empty()
            }
        });

        Scanner::new(page_params.key_range())
            .count(page_params.count() + 1)
            .order(page_params.order())
            .execute_with_filter::<InscriptionActivityByScriptHashKey, InscriptionActivityByScriptHashValue, _>(
                &mut tikv,
                ReducerType::InscriptionActivityByScriptHash,
                filter_fn,
            )
            .await?
    } else if let Some(id) = inscription {
        // Inscription must be part of the activity of the address, either in self-transfers, sent
        // or received.
        let filter_fn = Some(|kv: &KvPair| {
            let value = InscriptionActivityByScriptHashValue::decode(&kv.1)
                .unwrap()
                .0;

            value.self_transfers.iter().any(|x| x.inscription_id == id)
                || value.sent.iter().any(|x| x.inscription_id == id)
                || value.received.iter().any(|x| x.inscription_id == id)
        });

        Scanner::new(page_params.key_range())
            .count(page_params.count() + 1)
            .order(page_params.order())
            .execute_with_filter::<InscriptionActivityByScriptHashKey, InscriptionActivityByScriptHashValue, _>(
                &mut tikv,
                ReducerType::InscriptionActivityByScriptHash,
                filter_fn,
            )
            .await?
    } else {
        // No filter was provided.
        Scanner::new(page_params.key_range())
            .count(page_params.count() + 1)
            .order(page_params.order())
            .execute::<InscriptionActivityByScriptHashKey, InscriptionActivityByScriptHashValue>(
                &mut tikv,
                ReducerType::InscriptionActivityByScriptHash,
            )
            .await?
    };

    let mut kvs = kvs.into_iter().enumerate();

    let mut resolved_script_hashes: HashMap<[u8; 20], (Option<Address>, Vec<u8>)> = HashMap::new();

    let mut activity: Vec<WalletInscriptionActivityByAddress> = vec![];

    // Process fetched KVs.
    while let Some((i, (key, value))) = kvs.next() {
        // If this is the last result of the page, check if there is a subsequent result (and
        // therefore we need to return a cursor for next page).
        if i == (page_params.count() - 1) && kvs.next().is_some() {
            next_cursor = Some(
                InscriptionActivityByScriptHashCursor {
                    height: key.height,
                    activity_tx_index: key.activity_tx_index,
                    tx_hash: key.tx_hash,
                }
                .encode_base64(),
            );
        }

        // Build inscription activity.
        let inscription_activity = build_inscription_activity(
            value,
            &query_address,
            &query_script_bytes,
            key.tx_hash,
            key.height,
            &mut resolved_script_hashes,
            &mut tikv,
            &mode,
        )
        .await?;

        activity.push(WalletInscriptionActivityByAddress {
            height: key.height,
            confirmations: (snapshot_chain_tip.block_height + 1).saturating_sub(key.height),
            mempool: key.height > snapshot_chain_tip.block_height,
            tx_hash: Txid::from_byte_array(key.tx_hash).to_string(),
            inscription_activity,
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
        "height": 901787,
        "confirmations": 0,
        "mempool": true,
        "tx_hash": "cf5caab63c40314fdd2f421b19541f9b25926d2a6318845ad8bc3266b4b8a8be",
        "inscription_activity": {
            "self_transferred": [],
            "sent": [],
            "received": [{
                "inscription_id": "1e6f91f13fe9a4359e1b9d6e7723cacb815d250337dab921f7b90fca62913e73i577",
                "from": {
                    "address": "bc1q8jtuypcd0p9v7eu8zq9uhd42tvy6gwh89zuchh",
                    "script_pubkey": "00143c97c2070d784acf6787100bcbb6aa5b09a43ae7",
                    "input_index": 0,
                    "sat_offset": 0
                },
                "to": {
                    "address": "bc1pck742mdgmrd473upp553jj5x2w62q6mzd3enxjdegrxx2sc7rcmqnndp86",
                    "script_pubkey": "5120c5bd556da8d8db5f47810d29194a8653b4a06b626c733349b940cc65431e1e36",
                    "output_vout": 0,
                    "sat_offset": 0,
                    "output_txid": "cf5caab63c40314fdd2f421b19541f9b25926d2a6318845ad8bc3266b4b8a8be"
                }
            }]
        }
    }, {
        "height": 901780,
        "confirmations": 7,
        "mempool": false,
        "tx_hash": "09b6730e0f11f87c31f8d4977785292e9c741b16834d59a0b2f352ee01a43e91",
        "inscription_activity": {
            "self_transferred": [],
            "sent": [],
            "received": [{
                "inscription_id": "a8a3f114d24e0e270a4d66457ff1ca1d11eb21d4daa02ffcd1b643a1c6731a2ci1388",
                "from": {
                    "address": "bc1p7jwyezderr5qxepw57fepw9dmdetn9pqkj2e3m7ufffthspdj5aqdxsdw5",
                    "script_pubkey": "5120f49c4c89b918e803642ea79390b8addb72b99420b49598efdc4a52bbc02d953a",
                    "input_index": 0,
                    "sat_offset": 0
                },
                "to": {
                    "address": "bc1pck742mdgmrd473upp553jj5x2w62q6mzd3enxjdegrxx2sc7rcmqnndp86",
                    "script_pubkey": "5120c5bd556da8d8db5f47810d29194a8653b4a06b626c733349b940cc65431e1e36",
                    "output_vout": 0,
                    "sat_offset": 0,
                    "output_txid": "09b6730e0f11f87c31f8d4977785292e9c741b16834d59a0b2f352ee01a43e91"
                }
            }]
        }
    }],
    "indexer_info": {
        "chain_tip": {
            "block_hash": "00000000000000000000d7398966d32c809e5acad484574547150c97d39eea91",
            "block_height": 901786
        },
        "mempool_timestamp": "2025-06-18 16:28:43",
        "estimated_blocks": [{
            "block_height": 901787,
            "sats_per_vb": {
                "min": 1,
                "median": 5,
                "max": 991
            }
        }]
    },
    "next_cursor": "Aw3ClAEBkT6kAe5S87KgWU2DFht0nC4phXeX1PgxfPgRDw5ztgk"
}"##;
