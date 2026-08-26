use axum::{extract::Path, response::IntoResponse, Extension, Json};
use bitcoin::{hashes::Hash, Txid};
use hex;
use reqwest::StatusCode;
use std::str::FromStr;
use timbre_xbt::{
    reducers::{
        etching_by_rune_id::{Key as EtchingByRuneIdKey, Value as EtchingByRuneIdValue},
        sats_per_vb_by_block,
        spending_tx_by_txo::{Key as SpendingTxByTxoKey, Value as SpendingTxByTxoValue},
        tx_info::{Key as TxInfoKey, Value as TxInfoValue},
    },
    Reducer,
};

use crate::{
    error::Error,
    options::Mode,
    tikv::{adapter::TiKVAdapter, key_resolver::ReducerType},
    timer::Timer,
    types::{
        transactions::TxOutMetaprotocols, BlockSatsPerVb, EstimatedBlock, InscriptionAndOffset,
        MempoolLastUpdated, MempoolTimestampedResponse, RuneAndAmount,
    },
    util::{decimal, parse_tx_hash, timestamp_to_string},
};

pub static REQUIRED_REDUCERS: &[ReducerType] = &[
    ReducerType::EtchingByRuneId,
    ReducerType::ScriptByScriptHash,
    ReducerType::SpendingTxByTxo,
    ReducerType::TxInfo,
    // estimated block fees
    ReducerType::SatsPerVbByBlock,
];

#[utoipa::path(
    tag = "Transactions",
    get,
    path = "/mempool/transactions/{tx_hash}/outputs/{output_index}",
    params(
        ("tx_hash" = String, Path, description = "Transaction hash", example="b077b8d829004197c5d71bbb755cf23914891db4768d642458c8ef245b3af7fe"),
        ("output_index" = String, Path, description = "Transaction output index", example="0"),
    ),
    responses(
        (
            status = 200,
            description = "Requested data",
            body = MempoolTimestampedTxOutMetaprotocols,
            example = json!(serde_json::Value::from_str(EXAMPLE_RESPONSE).unwrap())
        ),
        (status = 400, description = "Malformed query parameters"),
        (status = 404, description = "Requested entity not found on-chain"),
        (status = 500, description = "Internal server error"),
    )
)]
#[tracing::instrument(
    name = "MEMPOOL_TRANSACTION_OUTPUT_INFO",
    level = "info",
    skip(tikv, mode)
)]
/// Transaction Output Info (Mempool-aware)
///
/// Provides detailed information for a single transaction output, including its value, spend status, and any attached metadata such as Ordinal inscriptions, Runes, or BRC20 data.
///
/// In addition to confirmed transactions, mempool endpoints return data which reflects pending transactions in some number of "estimated" blocks - predicted blocks containing transactions which have been propagated around the network but not yet included in a mined block, with transactions with a higher sat/vB value being prioritised. The response details how many of these estimated blocks were considered when fetching the data.
pub async fn mempool_tx_output_info(
    Path((tx_hash, output_index)): Path<(String, String)>,
    mut tikv: Extension<TiKVAdapter>,
    mode: Extension<Mode>,
) -> Result<impl IntoResponse, Error> {
    let mut timer = Timer::new();

    // ---

    tikv.init_mempool(REQUIRED_REDUCERS, None).await?;

    let snapshot_chain_tip = tikv.get_snapshot_point()?;

    let snapshot_mempool_view = tikv.get_snapshot_mempool_info()?;

    let found_mempool_blocks = snapshot_mempool_view.map(|x| x.mempool_blocks).unwrap_or(0);

    timer.checkpoint("initialise tikv adapter");

    // --- parse tx_hash into block height and tx index

    let output_index: u32 = output_index
        .parse()
        .map_err(|_| Error::MalformedRequest("invalid output index".into()))?;

    let tx_hash = parse_tx_hash(&tx_hash)?;

    // --- fetch tx info

    let tx_info = tikv
        .get_reducer_key_maybe::<TxInfoKey, TxInfoValue>(
            (ReducerType::TxInfo, Reducer::TxInfo),
            &TxInfoKey { tx_hash },
        )
        .await?
        .ok_or_else(|| Error::NotFound)?;

    timer.checkpoint("fetch tx info");

    // ---

    let tx_out = tx_info
        .outputs
        .get(output_index as usize)
        .ok_or_else(|| Error::NotFound)?
        .clone();

    let script_hash = tx_out.script_hash;
    let (address, script_bytes) = tikv.resolve_script_hash(mode.0, script_hash).await?;

    timer.checkpoint("resolve output address");

    // --- parse inscriptions
    let mut inscriptions = vec![];
    for (offset, (reveal_tx_hash, inscription_index)) in tx_out.inscriptions.into_iter() {
        let inscription_id = format!(
            "{}i{}",
            Txid::from_byte_array(reveal_tx_hash),
            inscription_index,
        );

        inscriptions.push(InscriptionAndOffset {
            offset,
            inscription_id,
        })
    }

    timer.checkpoint("process inscriptions in output");

    // --- parse runes
    let mut runes = vec![];
    for (rune_id, amount) in tx_out.runes.into_iter() {
        runes.push(RuneAndAmount {
            rune_id: format!("{}:{}", rune_id.0, rune_id.1),
            amount: {
                let dec = tikv
                    .get_reducer_key::<_, EtchingByRuneIdValue>(
                        (ReducerType::EtchingByRuneId, Reducer::EtchingByRuneId),
                        &EtchingByRuneIdKey { rune_id },
                    )
                    .await?
                    .divisibility
                    .unwrap_or(0) as usize;

                decimal(amount, dec)
            },
        })
    }

    timer.checkpoint("process runes in output");

    // --- fetch info about this output having been spent

    let spending_tx = tikv
        .get_reducer_key_maybe::<SpendingTxByTxoKey, SpendingTxByTxoValue>(
            (ReducerType::SpendingTxByTxo, Reducer::SpendingTxByTxo),
            &SpendingTxByTxoKey {
                utxo_tx_hash: tx_hash,
                utxo_vout: output_index as u32,
            },
        )
        .await?
        .map(|v| Txid::from_byte_array(v.tx_hash).to_string());

    timer.checkpoint("fetch spending tx info");

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

    let out = MempoolTimestampedResponse {
        data: TxOutMetaprotocols {
            address: address.map(|x| x.to_string()),
            script_pubkey: hex::encode(script_bytes),
            satoshis: tx_out.satoshis.to_string(),
            spending_tx,
            inscriptions,
            runes,
        },
        indexer_info: MempoolLastUpdated {
            chain_tip: snapshot_chain_tip,
            mempool_timestamp: snapshot_mempool_view
                .map(|x| timestamp_to_string(x.mempool_view_ts)),
            estimated_blocks,
        },
    };

    timer.finish();

    Ok((StatusCode::OK, Json(out)))
}

static EXAMPLE_RESPONSE: &str = r##"{
    "data": {
        "address": "bc1qr46dacxy28zz5apsjrvs5jdgvs5sdcf2ed4tvl",
        "script_pubkey": "00141d74dee0c451c42a743090d90a49a8642906e12a",
        "satoshis": "85000",
        "spending_tx": null,
        "inscriptions": [],
        "runes": []
    },
    "indexer_info": {
        "chain_tip": {
            "block_hash": "0000000000000000000085563bb7da463844c02d6c82bca13e3eec5411f8c8ed",
            "block_height": 897991
        },
        "mempool_timestamp": "2025-05-23 11:43:08",
        "estimated_blocks": [{
            "block_height": 897992,
            "sats_per_vb": {
                "min": 1,
                "median": 4,
                "max": 210
            }
        }]
    }
}"##;
