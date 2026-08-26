use std::str::FromStr;

use axum::{extract::Path, response::IntoResponse, Extension, Json};
use bitcoin::{hashes::Hash, Script};
use reqwest::StatusCode;
use timbre_xbt::{
    reducers::{
        reducer_key_range,
        rune_utxos_by_script_hash::{
            Key as RuneUtxosByScriptHashKey, Value as RuneUtxosByScriptHashValue,
        },
        sat_balance_by_script_hash::{
            Key as SatBalanceByScriptHashKey, Value as SatBalanceByScriptHashValue,
        },
        total_inscriptions_by_script_hash::{
            Key as TotalInscriptionsByScriptHashKey, Value as TotalInscriptionsByScriptHashValue,
        },
        total_outputs_by_script_hash::{
            Key as TotalOutputsByScriptHashKey, Value as TotalOutputsByScriptHashValue,
        },
        total_sat_in_inputs_by_script_hash::{
            Key as TotalSatInInputsByScriptHashKey, Value as TotalSatInInputsByScriptHashValue,
        },
        total_sat_in_outputs_by_script_hash::{
            Key as TotalSatInOutputsByScriptHashKey, Value as TotalSatInOutputsByScriptHashValue,
        },
        total_txs_by_script_hash::{
            Key as TotalTxsByScriptHashKey, Value as TotalTxsByScriptHashValue,
        },
        total_utxos_by_script_hash::{
            Key as TotalUtxosByScriptHashKey, Value as TotalUtxosByScriptHashValue,
        },
    },
    Reducer,
};

use crate::{
    error::Error,
    tikv::{adapter::TiKVAdapter, key_resolver::ReducerType, Scanner},
    types::{AddressStatistics, TimestampedResponse},
};

pub static REQUIRED_REDUCERS: &[ReducerType] = &[
    ReducerType::TotalInscriptionsByScriptHash,
    ReducerType::TotalOutputsByScriptHash,
    ReducerType::TotalSatInInputsByScriptHash,
    ReducerType::TotalSatInOutputsByScriptHash,
    ReducerType::TotalTxsByScriptHash,
    ReducerType::TotalUtxosByScriptHash,
    ReducerType::RuneUtxosByScriptHash,
    ReducerType::SatBalanceByScriptHash,
    ReducerType::ScriptByScriptHash,
    ReducerType::ScriptHashByAddressPayloadHash,
];

#[utoipa::path(
    tag = "Addresses",
    get,
    path = "/addresses/{address}/statistics",
    params(
        ("address" = String, Path, description = "Bitcoin address or hex encoded script pubkey", example="bc1qcx7ys0ahvtfqcc63sfn6axls0qrhkadnslpd94"),
    ),
    responses(
        (
            status = 200,
            description = "Requested data",
            body = TimestampedAddressStatistics,
            example = json!(serde_json::Value::from_str(EXAMPLE_RESPONSE).unwrap()),
        ),
        (status = 400, description = "Malformed query parameters"),
        (status = 404, description = "Requested entity not found on-chain"),
        (status = 500, description = "Internal server error"),
    )
)]
#[tracing::instrument(name = "ADDRESS_STATISTICS", level = "info", skip(tikv))]
/// Address Statistics
///
/// Returns all current statistics of the address: total txs the address was involved in, total unspent outputs controlled by the address, current satoshi, control of any runes and inscription balance.
pub async fn address_statistics(
    Path(addr_or_pk): Path<String>,
    mut tikv: Extension<TiKVAdapter>,
) -> Result<impl IntoResponse, Error> {
    tikv.init_tip(REQUIRED_REDUCERS).await?;

    let last_updated = tikv.get_snapshot_point()?;

    let script_bytes = match tikv.parse_address_or_script_bytes(&addr_or_pk).await {
        Ok((_, script_bytes)) => script_bytes,
        Err(Error::NotFound) => {
            // User param is a Bitcoin address, but the corresponding script pub key could not be
            // found in store.
            let out = TimestampedResponse {
                data: AddressStatistics {
                    total_txs: 0,
                    total_inputs: 0,
                    total_sat_in_inputs: 0,
                    total_outputs: 0,
                    total_sat_in_outputs: 0,
                    total_utxos: 0,
                    runes: false,
                    total_inscriptions: 0,
                    sat_balance: 0.to_string(),
                },
                last_updated,
            };

            return Ok((StatusCode::OK, Json(out)));
        }
        Err(e) => return Err(e),
    };
    let script = Script::from_bytes(&script_bytes);
    let script_hash = script.script_hash().to_byte_array();

    // Fetch KV written by TotalTxsByScriptHash.
    let total_txs = tikv
        .get_reducer_key_maybe::<_, TotalTxsByScriptHashValue>(
            (
                ReducerType::TotalTxsByScriptHash,
                Reducer::TotalTxsByScriptHash,
            ),
            &TotalTxsByScriptHashKey { script_hash },
        )
        .await?
        .map(|x| x.total_txs)
        .unwrap_or_default();

    // Fetch KV written by TotalOutputsByScriptHash.
    let total_outputs = tikv
        .get_reducer_key_maybe::<_, TotalOutputsByScriptHashValue>(
            (
                ReducerType::TotalOutputsByScriptHash,
                Reducer::TotalOutputsByScriptHash,
            ),
            &TotalOutputsByScriptHashKey { script_hash },
        )
        .await?
        .map(|x| x.total_outputs)
        .unwrap_or_default();

    // Fetch KV written by TotalSatInInputsByScriptHash.
    let total_sat_in_inputs = tikv
        .get_reducer_key_maybe::<_, TotalSatInInputsByScriptHashValue>(
            (
                ReducerType::TotalSatInInputsByScriptHash,
                Reducer::TotalSatInInputsByScriptHash,
            ),
            &TotalSatInInputsByScriptHashKey { script_hash },
        )
        .await?
        .map(|x| x.total_sat_in_inputs)
        .unwrap_or_default();

    // Fetch KV written by TotalSatInOutputsByScriptHash.
    let total_sat_in_outputs = tikv
        .get_reducer_key_maybe::<_, TotalSatInOutputsByScriptHashValue>(
            (
                ReducerType::TotalSatInOutputsByScriptHash,
                Reducer::TotalSatInOutputsByScriptHash,
            ),
            &TotalSatInOutputsByScriptHashKey { script_hash },
        )
        .await?
        .map(|x| x.total_sat_in_outputs)
        .unwrap_or_default();

    // Fetch KV written by TotalUtxosByScriptHash.
    let total_utxos = tikv
        .get_reducer_key_maybe::<_, TotalUtxosByScriptHashValue>(
            (
                ReducerType::TotalUtxosByScriptHash,
                Reducer::TotalUtxosByScriptHash,
            ),
            &TotalUtxosByScriptHashKey { script_hash },
        )
        .await?
        .map(|value| value.total_utxos)
        .unwrap_or_default();

    // Difference between total outputs (spent or unspent) and total UTxOs (unspent) gives total inputs (spent outputs).
    let total_inputs = total_outputs.saturating_sub(total_utxos);

    // Mark existence of runes by trying to fetch any KV written by RuneUtxosByScriptHash.
    let runes_encoder = tikv.get_encoder(ReducerType::RuneUtxosByScriptHash)?;

    let (range_lower, range_upper) = reducer_key_range(
        &runes_encoder.namespace(),
        &Reducer::RuneUtxosByScriptHash,
        &Some(script_hash), // First field in KV: script hash.
        None::<u64>,        // Second field in KV: block height.
        None::<u64>,        // Second field in KV: block height.
    );

    let runes = {
        let kv = Scanner::new(range_lower..range_upper)
            .count(1)
            .execute::<RuneUtxosByScriptHashKey, RuneUtxosByScriptHashValue>(
                &mut tikv,
                ReducerType::RuneUtxosByScriptHash,
            )
            .await?;

        kv.len() > 0
    };

    // Fetch KV written by TotalInscriptionsByScriptHash.
    let total_inscriptions = tikv
        .get_reducer_key_maybe::<_, TotalInscriptionsByScriptHashValue>(
            (
                ReducerType::TotalInscriptionsByScriptHash,
                Reducer::TotalInscriptionsByScriptHash,
            ),
            &TotalInscriptionsByScriptHashKey { script_hash },
        )
        .await?
        .map(|value| value.total_inscriptions)
        .unwrap_or_default();

    // Fetch KV written by SatBalanceByScriptHash.
    let sat_balance = tikv
        .get_reducer_key_maybe::<_, SatBalanceByScriptHashValue>(
            (
                ReducerType::SatBalanceByScriptHash,
                Reducer::SatBalanceByScriptHash,
            ),
            &SatBalanceByScriptHashKey { script_hash },
        )
        .await?
        .map(|value| value.satoshis)
        .unwrap_or_default()
        .to_string();

    let out = TimestampedResponse {
        data: AddressStatistics {
            total_txs,
            total_inputs,
            total_sat_in_inputs,
            total_outputs,
            total_sat_in_outputs,
            total_utxos,
            runes,
            total_inscriptions,
            sat_balance,
        },
        last_updated,
    };

    Ok((StatusCode::OK, Json(out)))
}

pub static EXAMPLE_RESPONSE: &str = r##"{
    "data": {
        "total_txs": 3,
        "total_inputs": 0,
        "total_sat_in_inputs": 0,
        "total_outputs": 3,
        "total_sat_in_outputs": 209258,
        "total_utxos": 3,
        "runes": false,
        "total_inscriptions": 0,
        "sat_balance": "209258"
    },
    "last_updated": {
        "block_hash": "000000000000000000017e71733448d2c8e2f4c08105d97f7c1868acb4acc7ba",
        "block_height": 903881
    }
}"##;
