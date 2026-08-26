use crate::{
    tikv::{adapter::TiKVAdapter, key_resolver::ReducerType},
    timer::Timer,
    types::{MempoolLastUpdated, MempoolTimestampedResponse},
    util::{estimate_indexer_blocks, timestamp_to_string},
};
use axum::{extract::Path, response::IntoResponse, Extension, Json};
use bitcoin::{hashes::Hash, Script};
use reqwest::StatusCode;
use std::str::FromStr;
use timbre_xbt::{
    reducers::{
        reducer_key_range,
        utxos_by_script_hash::{Key as UtxosByScriptHashKey, Value as UtxosByScriptHashValue},
    },
    Reducer,
};

use crate::{error::Error, tikv::Scanner};

pub static REQUIRED_REDUCERS: &[ReducerType] = &[
    ReducerType::UtxosByScriptHash,
    // parsing address parameter
    ReducerType::ScriptByScriptHash,
    ReducerType::ScriptHashByAddressPayloadHash,
    // estimated block fees
    ReducerType::SatsPerVbByBlock,
];

#[utoipa::path(
    tag = "Addresses",
    get,
    path = "/mempool/addresses/{address}/balance",
    params(
        ("address" = String, Path, description = "Bitcoin address or hex encoded script pubkey", example="bc1phyrmjs2jm5c98tldke2ykp0h66lsx3wy0ey8ug2fjj5mxsn8ftqsa24un8"),
    ),
    responses(
        (
            status = 200,
            description = "Requested data",
            body = MempoolTimestampedSatoshis,
            example = json!(serde_json::Value::from_str(EXAMPLE_RESPONSE).unwrap())
        ),
        (status = 400, description = "Malformed query parameters"),
        (status = 404, description = "Requested entity not found on-chain"),
        (status = 500, description = "Internal server error"),
    )
)]
#[tracing::instrument(
    name = "MEMPOOL_SATOSHI_BALANCE_BY_ADDRESS",
    level = "info",
    skip(tikv)
)]
/// Satoshi Balance by Address (Mempool-aware)
///
/// Returns the total balance in satoshis held at the specified address or script pubkey by summing all unspent outputs (UTXOs).
///
/// In addition to confirmed transactions, mempool endpoints return data which reflects pending transactions in some number of "estimated" blocks - predicted blocks containing transactions which have been propagated around the network but not yet included in a mined block, with transactions with a higher sat/vB value being prioritised. The response details how many of these estimated blocks were considered when fetching the data.
pub async fn mempool_satoshi_balance_by_address(
    Path(addr_or_pk): Path<String>,
    mut tikv: Extension<TiKVAdapter>,
) -> Result<impl IntoResponse, Error> {
    let mut timer = Timer::new();

    // ---
    tikv.init_mempool(REQUIRED_REDUCERS, None).await?;

    let snapshot_chain_tip = tikv.get_snapshot_point()?;
    let snapshot_mempool_view = tikv.get_snapshot_mempool_info()?;
    let found_mempool_blocks = snapshot_mempool_view.map(|x| x.mempool_blocks).unwrap_or(0);

    let utxos_encoder = tikv.get_encoder(ReducerType::UtxosByScriptHash)?;

    timer.checkpoint("initialise tikv adapter");

    // --- parse and try to decode address
    let script_bytes = match tikv.parse_address_or_script_bytes(&addr_or_pk).await {
        Ok((_, script_bytes)) => script_bytes,
        Err(Error::NotFound) => {
            // User param is a Bitcoin address, but the corresponding script pub key could not be
            // found in store.
            tikv.init_mempool(REQUIRED_REDUCERS, None).await?;
            let snapshot_chain_tip = tikv.get_snapshot_point()?;
            let snapshot_mempool_view = tikv.get_snapshot_mempool_info()?;
            let found_mempool_blocks = snapshot_mempool_view.map(|x| x.mempool_blocks).unwrap_or(0);

            let out = MempoolTimestampedResponse {
                data: 0.to_string(),
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
            };

            return Ok((StatusCode::OK, Json(out)));
        }
        Err(e) => return Err(e),
    };
    let script = Script::from_bytes(&script_bytes);
    let script_hash = script.script_hash();

    timer.checkpoint("parse params");

    // --- scan keys

    let (utxos_range_lower, utxos_range_upper) = reducer_key_range(
        utxos_encoder.namespace(),
        &Reducer::UtxosByScriptHash,
        &Some(script_hash.to_byte_array()),
        None::<u64>,
        None::<u64>,
    );

    let kvs = Scanner::new(utxos_range_lower..utxos_range_upper)
        .execute::<UtxosByScriptHashKey, UtxosByScriptHashValue>(
            &mut tikv,
            ReducerType::UtxosByScriptHash,
        )
        .await?;

    timer.checkpoint("fetch kvs");

    // --- process fetched kvs

    let balance: u128 = kvs.into_iter().map(|(_, v)| v.satoshis as u128).sum();

    timer.checkpoint("process kvs");

    let out = MempoolTimestampedResponse {
        data: balance.to_string(),
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
    };

    timer.finish();

    Ok((StatusCode::OK, Json(out)))
}

static EXAMPLE_RESPONSE: &str = r##"{
    "data": "695100",
    "indexer_info": {
        "chain_tip": {
            "block_hash": "000000000000000000012c80ffd2f0bd17f1f92a0bb4c098236d7108f727bfe5",
            "block_height": 874584
        },
        "mempool_timestamp": "2025-01-06 16:43:32",
        "estimated_blocks": [{
            "block_height": 874585,
            "sats_per_vb": {
                "min": 1,
                "median": 8,
                "max": 504
            }
        }]
    }
}"##;
