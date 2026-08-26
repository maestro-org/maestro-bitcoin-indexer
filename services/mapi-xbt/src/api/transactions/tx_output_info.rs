use axum::{extract::Path, response::IntoResponse, Extension, Json};
use bitcoin::{hashes::Hash, Txid};
use hex;
use reqwest::StatusCode;
use std::str::FromStr;
use timbre_xbt::{
    reducers::{
        etching_by_rune_id::{Key as EtchingByRuneIdKey, Value as EtchingByRuneIdValue},
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
        transactions::TxOutMetaprotocols, InscriptionAndOffset, RuneAndAmount, TimestampedResponse,
    },
    util::{decimal, parse_tx_hash},
};

pub static REQUIRED_REDUCERS: &[ReducerType] = &[
    ReducerType::EtchingByRuneId,
    ReducerType::ScriptByScriptHash,
    ReducerType::SpendingTxByTxo,
    ReducerType::TxInfo,
];

#[utoipa::path(
    tag = "Transactions",
    get,
    path = "/transactions/{tx_hash}/outputs/{output_index}",
    params(
        ("tx_hash" = String, Path, description = "Transaction hash", example="1b07f02356aed6ddca37db8226c6292f2953d55ea741d7f58d44427976e7d4ee"),
        ("output_index" = String, Path, description = "Transaction output index", example="1"),
    ),
    responses(
        (
            status = 200,
            description = "Requested data",
            body = TimestampedTxOutMetaprotocols,
            example = json!(serde_json::Value::from_str(EXAMPLE_RESPONSE).unwrap())
        ),
        (status = 400, description = "Malformed query parameters"),
        (status = 404, description = "Requested entity not found on-chain"),
        (status = 500, description = "Internal server error"),
    )
)]
#[tracing::instrument(name = "TRANSACTION_OUTPUT_INFO", level = "info", skip(tikv, mode))]
/// Transaction Output Info
///
/// Provides detailed information for a single transaction output, including its value, spend status, and any attached metadata such as Ordinal inscriptions, Runes, or BRC20 data.
pub async fn tx_output_info(
    Path((tx_hash, output_index)): Path<(String, String)>,
    mut tikv: Extension<TiKVAdapter>,
    mode: Extension<Mode>,
) -> Result<impl IntoResponse, Error> {
    let mut timer = Timer::new();

    // ---

    tikv.init_tip(REQUIRED_REDUCERS).await?;

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

    let out = TimestampedResponse {
        data: TxOutMetaprotocols {
            address: address.map(|x| x.to_string()),
            script_pubkey: hex::encode(script_bytes),
            satoshis: tx_out.satoshis.to_string(),
            spending_tx,
            inscriptions,
            runes,
        },
        last_updated: tikv.get_snapshot_point()?,
    };

    timer.finish();

    Ok((StatusCode::OK, Json(out)))
}

static EXAMPLE_RESPONSE: &str = r##"{
    "data": {
        "address": "3G7gSaxPY7BhbEASd2pnZY5cg7uEQMQvd8",
        "script_pubkey": "a9149e3be5b19b788c2eb2d590a779c06b9b7a09782e87",
        "satoshis": "564",
        "spending_tx": null,
        "inscriptions": [],
        "runes": [{
            "rune_id": "840000:3",
            "amount": "88980600000"
        }]
    },
    "last_updated": {
        "block_hash": "00000000000000000000f2e6c4af3271ca47435d5178eca0bd6d86612d96d4b3",
        "block_height": 884469
    }
}"##;
