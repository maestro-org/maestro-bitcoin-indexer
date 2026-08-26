use crate::{
    error::Error,
    options::Mode,
    tikv::{adapter::TiKVAdapter, key_resolver::ReducerType, Scanner},
    timer::Timer,
    types::{
        runes::RuneHolder, BlockSatsPerVb, CountParam, CursorPaginationParams, EstimatedBlock,
        MempoolLastUpdated, MempoolPaginatedResponse,
    },
    util::{decimal, timestamp_to_string, ParsedPaginationParams, RuneIdentifier},
};
use axum::{
    extract::{Path, Query},
    response::IntoResponse,
    Extension, Json,
};
use reqwest::StatusCode;
use std::str::FromStr;
use timbre_xbt::{
    reducers::{
        balances_by_rune_id::Key as BalancesByRuneIdKey, etching_by_rune_id, sats_per_vb_by_block,
    },
    Encode, Reducer,
};

pub static REQUIRED_REDUCERS: &[ReducerType] = &[
    ReducerType::BalancesByRuneId,
    ReducerType::EtchingByRuneId,
    ReducerType::RuneIdByRuneName,
    // resolving script hashes
    ReducerType::ScriptByScriptHash,
    // estimated block fees
    ReducerType::SatsPerVbByBlock,
];

#[utoipa::path(
    tag = "Runes",
    get,
    path = "/mempool/assets/runes/{rune}/holders",
    params(
        ("rune" = String, Path, description = "Rune, specified either by the Rune ID (etching block number and transaction index) or name (spaced or un-spaced)", example="2519999:31"),
        ("count" = inline(Option<CountParam>), Query, description = "The max number of results per page"),
        ("cursor" = inline(Option<String>), Query, description = "Pagination cursor string, use the cursor included in a page of results to fetch the next page"),
    ),
    responses(
        (
            status = 200,
            description = "Requested data",
            body = MempoolPaginatedRuneHolder,
            example = json!(serde_json::Value::from_str(EXAMPLE_RESPONSE).unwrap())
        ),
        (status = 400, description = "Malformed query parameters"),
        (status = 404, description = "Requested entity not found on-chain"),
        (status = 500, description = "Internal server error"),
    )
)]
#[tracing::instrument(name = "MEMPOOL_HOLDERS_BY_RUNE", level = "info", skip(tikv, mode))]
/// Holders by Rune (Mempool-aware)
///
/// Lists all addresses currently holding a given Rune, with corresponding balances. Helps visualize token distribution and adoption.
///
/// In addition to confirmed transactions, mempool endpoints return data which reflects pending transactions in some number of "estimated" blocks - predicted blocks containing transactions which have been propagated around the network but not yet included in a mined block, with transactions with a higher sat/vB value being prioritised. The response details how many of these estimated blocks were considered when fetching the data.
pub async fn mempool_holders_by_rune(
    page_params: Query<CursorPaginationParams>,
    Path(rune_id): Path<String>,
    mut tikv: Extension<TiKVAdapter>,
    mode: Extension<Mode>,
) -> Result<impl IntoResponse, Error> {
    let mut timer = Timer::new();

    // ---

    tikv.init_mempool(REQUIRED_REDUCERS, None).await?;

    let balances_encoder = tikv.get_encoder(ReducerType::BalancesByRuneId)?;

    let snapshot_chain_tip = tikv.get_snapshot_point()?;

    let snapshot_mempool_view = tikv.get_snapshot_mempool_info()?;

    let found_mempool_blocks = snapshot_mempool_view.map(|x| x.mempool_blocks).unwrap_or(0);

    timer.checkpoint("initialise tikv adapter");

    // --- parse rune id

    let rune_id = match RuneIdentifier::parse(rune_id)? {
        RuneIdentifier::Id(id) => id,
        RuneIdentifier::Name(n) => tikv.resolve_rune_name(n).await?.unwrap_or_default(), // return empty vec instead of 404
    };

    timer.checkpoint("process rune id");

    // --- initialise `next_cursor`

    let mut next_cursor = None;

    // --- scan total balances by ticker

    let page_params = ParsedPaginationParams::parse_no_height::<_, [u8; 20]>(
        page_params.0,
        &balances_encoder,
        &Reducer::BalancesByRuneId,
        Some(rune_id),
    )?;

    let kvs = Scanner::new(page_params.key_range())
        .count(page_params.count() + 1)
        .execute::<BalancesByRuneIdKey, u128>(&mut tikv, ReducerType::BalancesByRuneId)
        .await?;

    timer.checkpoint("scan kvs");

    // --- process fetched kvs

    let mut kvs = kvs.into_iter().enumerate();

    let mut holders: Vec<RuneHolder> = Vec::new();

    let dec = tikv
        .get_reducer_key::<_, etching_by_rune_id::Value>(
            (ReducerType::EtchingByRuneId, Reducer::EtchingByRuneId),
            &etching_by_rune_id::Key { rune_id },
        )
        .await?
        .divisibility
        .unwrap_or(0);

    timer.checkpoint("fetch etch");

    // TODO: cleaner
    while let Some((i, (key, value))) = kvs.next() {
        // if this is the last result of the page, check if there is a subsequent
        // result (and therefore we need to return a cursor for next page)
        if i == (page_params.count() - 1) && kvs.next().is_some() {
            // TODO: timbre Cursor::from(key)
            next_cursor = Some(key.script_hash.encode_base64());
        };

        let (address, script) = tikv.resolve_script_hash(mode.0, key.script_hash).await?;

        holders.push(RuneHolder {
            address: address.map(|x| x.to_string()),
            script_pubkey: hex::encode(script),
            balance: decimal(value, dec as usize),
        })
    }

    timer.checkpoint("process kvs");

    // ---

    let mut estimated_blocks = vec![];

    for i in 0..found_mempool_blocks as u64 {
        let estimated_block_height = snapshot_chain_tip.block_height + (i + 1);

        let sats_per_vb_vals = tikv
            .get_reducer_key::<_, sats_per_vb_by_block::Value>(
                (ReducerType::SatsPerVbByBlock, Reducer::SatsPerVbByBlock),
                &sats_per_vb_by_block::Key {
                    height: estimated_block_height,
                },
            )
            .await?;

        estimated_blocks.push(EstimatedBlock {
            block_height: estimated_block_height,
            sats_per_vb: BlockSatsPerVb {
                min: sats_per_vb_vals.min,
                median: sats_per_vb_vals.median,
                max: sats_per_vb_vals.max,
            },
        })
    }

    let out = MempoolPaginatedResponse {
        data: holders,
        indexer_info: MempoolLastUpdated {
            chain_tip: snapshot_chain_tip,
            mempool_timestamp: snapshot_mempool_view
                .map(|x| timestamp_to_string(x.mempool_view_ts)),
            estimated_blocks,
        },
        next_cursor,
    };

    timer.finish();

    Ok((StatusCode::OK, Json(out)))
}

static EXAMPLE_RESPONSE: &str = r##"{
    "data": [{
        "address": "bc1q764zfcx3uw0dcvcdh7nnwm5fvsml0c2tgn942v",
        "script_pubkey": "0014f6aa24e0d1e39edc330dbfa7376e896437f7e14b",
        "balance": "9000000.000000000000000000"
    }, {
        "address": "bc1pqfhj6tlxgpvc72mvn9hh0z666k45fpxgsej92d9q08sfe572mhlqca68ld",
        "script_pubkey": "5120026f2d2fe640598f2b6c996f778b5ad5ab4484c886645534a079e09cd3caddfe",
        "balance": "420000.000000000000000000"
    }],
    "indexer_info": {
        "chain_tip": {
            "block_hash": "00000000000000000002da06787fe86324e1cc1421861d899b7bd1e340aa1930",
            "block_height": 867154
        },
        "mempool_timestamp": "2025-01-06 16:43:32",
        "estimated_blocks": [{
            "block_height": 867155,
            "sats_per_vb": {
                "min": 12,
                "median": 14,
                "max": 16
            }
        }]
    },
    "next_cursor": null
}"##;
