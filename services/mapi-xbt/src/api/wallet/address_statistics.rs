use axum::{extract::Path, response::IntoResponse, Extension, Json};
use bitcoin::{hashes::Hash, Script};
use reqwest::StatusCode;
use std::str::FromStr;
use timbre_xbt::{
    reducers::{
        block_info::{Key as BlockInfoKey, Value as BlockInfoValue},
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
    options::arranger::Arranger,
    tikv::{adapter::TiKVAdapter, key_resolver::ReducerType, Scanner},
    types::{
        MempoolLastUpdated, MempoolWalletTimestampedResponse, OrderParam, PendingAddressStatistics,
        WalletAddressStatistics,
    },
    util::{estimate_indexer_blocks, fetch_sat_prices, timestamp_to_string},
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
    // Required to fetch chain tip timestamp and with that query Arranger for USD-BTC exchange rate.
    ReducerType::BlockInfo,
    // estimated block fees
    ReducerType::SatsPerVbByBlock,
];

#[utoipa::path(
    tag = "Addresses",
    get,
    path = "/wallet/addresses/{address}/statistics",
    params(
        ("address" = String, Path, description = "Bitcoin address or hex encoded script pubkey", example="bc1qcx7ys0ahvtfqcc63sfn6axls0qrhkadnslpd94"),
    ),
    responses(
        (
            status = 200,
            description = "Requested data",
            body = MempoolWalletTimestampedAddressStatistics,
            example = json!(serde_json::Value::from_str(EXAMPLE_RESPONSE).unwrap())
        ),
        (status = 400, description = "Malformed query parameters"),
        (status = 404, description = "Requested entity not found on-chain"),
        (status = 500, description = "Internal server error"),
    )
)]
#[tracing::instrument(
    name = "WALLET_ADDRESS_STATISTICS",
    level = "info",
    skip(tikv, arranger)
)]
/// Address Statistics (Mempool-aware)
///
/// Returns all current statistics of the address: total txs the address was involved in, total unspent outputs controlled by the address, current satoshi, control of any runes and inscription balance, distinguishing between confirmed and pending (still in the mempool) data.
pub async fn wallet_address_statistics(
    Path(addr_or_pk): Path<String>,
    mut tikv: Extension<TiKVAdapter>,
    Extension(arranger): Extension<Arranger>,
) -> Result<impl IntoResponse, Error> {
    // First, fetch confirmed data.
    tikv.init_tip(REQUIRED_REDUCERS).await?;

    // Parse and try decode user params.
    let script_bytes = match tikv.parse_address_or_script_bytes(&addr_or_pk).await {
        Ok((_, script_bytes)) => script_bytes,
        Err(Error::NotFound) => {
            // User param is a Bitcoin address, but the corresponding script pub key could not be
            // found in store.
            tikv.init_mempool(REQUIRED_REDUCERS, None).await?;
            let snapshot_chain_tip = tikv.get_snapshot_point()?;
            let snapshot_mempool_view = tikv.get_snapshot_mempool_info()?;
            let found_mempool_blocks = snapshot_mempool_view.map(|x| x.mempool_blocks).unwrap_or(0);

            let out = MempoolWalletTimestampedResponse {
                data: WalletAddressStatistics {
                    total_txs: 0,
                    total_inputs: 0,
                    total_sat_in_inputs: 0,
                    total_outputs: 0,
                    total_sat_in_outputs: 0,
                    total_utxos: 0,
                    runes: false,
                    total_inscriptions: 0,
                    sat_balance: 0.to_string(),
                    usd_balance: arranger.get_sat_prices_path().map(|_| 0.to_string()),
                    pending: PendingAddressStatistics {
                        txs: 0,
                        inputs: 0,
                        sat_in_inputs: 0,
                        outputs: 0,
                        sat_in_outputs: 0,
                        utxos: 0,
                        sat_balance: 0.to_string(),
                        usd_balance: arranger.get_sat_prices_path().map(|_| 0.to_string()),
                    },
                },
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
    let script_hash = script.script_hash().to_byte_array();

    // Fetch KV written by TotalTxsByScriptHash.
    let total_confirmed_txs = tikv
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

    // Fetch KV written by TotalSatInInputsByScriptHash.
    let total_sat_in_confirmed_inputs = tikv
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

    // Fetch KV written by TotalOutputsByScriptHash.
    let total_confirmed_outputs = tikv
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

    // Fetch KV written by TotalSatInOutputsByScriptHash.
    let total_sat_in_confirmed_outputs = tikv
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
    let total_confirmed_utxos = tikv
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

    // Difference between total confirmed outputs (spent or unspent) and total confirmed UTxOs (unspent) gives total confirmed inputs (spent outputs).
    let total_confirmed_inputs = total_confirmed_outputs.saturating_sub(total_confirmed_utxos);

    // Fetch KV written by SatBalanceByScriptHash.
    let confirmed_sat_balance = tikv
        .get_reducer_key_maybe::<_, SatBalanceByScriptHashValue>(
            (
                ReducerType::SatBalanceByScriptHash,
                Reducer::SatBalanceByScriptHash,
            ),
            &SatBalanceByScriptHashKey { script_hash },
        )
        .await?
        .map(|value| value.satoshis)
        .unwrap_or_default();

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

    // If an external price service is configured, fetch block at chain tip, then use its
    // timestamp to fetch the USD-BTC exchange rate. Otherwise USD balances are null.
    let tip_exchange_rate: Option<f64> = if let Some(sat_prices_path) =
        arranger.get_sat_prices_path()
    {
        let block_info_encoder = tikv.get_encoder(ReducerType::BlockInfo)?;

        let (range_lower, range_upper) = reducer_key_range(
            &block_info_encoder.namespace(),
            &Reducer::BlockInfo,
            &None::<u64>,
            None::<u64>,
            None::<u64>,
        );

        let block_timestamp = {
            let kv = Scanner::new(range_lower..range_upper)
                .count(1)
                .order(OrderParam::Desc)
                .execute::<BlockInfoKey, BlockInfoValue>(&mut tikv, ReducerType::BlockInfo)
                .await?;

            match kv.get(0).and_then(|(_, value)| value.timestamp) {
                Some(timestamp) => timestamp,
                None => {
                    return Err(Error::Internal("Unable to fetch block at chain tip".into()));
                }
            }
        };

        let tip_exchange_rate = fetch_sat_prices(vec![block_timestamp], &sat_prices_path).await?;
        match tip_exchange_rate.get(0) {
            Some(usd_price) => Some(*usd_price),
            None => {
                return Err(Error::Internal(
                    "Unable to get USD-BTC exchange rate".into(),
                ));
            }
        }
    } else {
        None
    };

    // Now fetch mempool data, starting by initializing mempool snapshot.
    tikv.init_mempool(REQUIRED_REDUCERS, None).await?;

    let snapshot_chain_tip = tikv.get_snapshot_point()?;
    let snapshot_mempool_view = tikv.get_snapshot_mempool_info()?;
    let found_mempool_blocks = snapshot_mempool_view
        .map(|x| x.mempool_blocks)
        .unwrap_or_default();

    let mempool_txs = tikv
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

    let total_sat_in_mempool_inputs = tikv
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

    let total_mempool_outputs = tikv
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

    let total_sat_in_mempool_outputs = tikv
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

    let total_mempool_utxos = tikv
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

    // Difference between total mempool outputs (spent or unspent) and total mempool UTxOs (unspent) gives total mempool inputs (spent outputs).
    let total_mempool_inputs = total_mempool_outputs.saturating_sub(total_mempool_utxos);

    let mempool_sat_balance = tikv
        .get_reducer_key_maybe::<_, SatBalanceByScriptHashValue>(
            (
                ReducerType::SatBalanceByScriptHash,
                Reducer::SatBalanceByScriptHash,
            ),
            &SatBalanceByScriptHashKey { script_hash },
        )
        .await?
        .map(|value| value.satoshis)
        .unwrap_or_default();

    let pending_balance = mempool_sat_balance as i64 - confirmed_sat_balance as i64;

    // Finally, collect data to compute difference between confirmed and mempool data and build the response.
    let out = MempoolWalletTimestampedResponse {
        data: WalletAddressStatistics {
            total_txs: total_confirmed_txs,
            total_inputs: total_confirmed_inputs,
            total_sat_in_inputs: total_sat_in_confirmed_inputs,
            total_outputs: total_confirmed_outputs,
            total_sat_in_outputs: total_sat_in_confirmed_outputs,
            total_utxos: total_confirmed_utxos,
            runes,
            total_inscriptions,
            sat_balance: confirmed_sat_balance.to_string(),
            usd_balance: tip_exchange_rate
                .map(|rate| format!("{:.2}", (confirmed_sat_balance as f64 * rate) / 100000000.0)),
            pending: PendingAddressStatistics {
                txs: mempool_txs.saturating_sub(total_confirmed_txs),
                inputs: total_mempool_inputs - total_confirmed_inputs,
                sat_in_inputs: total_sat_in_mempool_inputs - total_sat_in_confirmed_inputs,
                outputs: total_mempool_outputs - total_confirmed_outputs,
                sat_in_outputs: total_sat_in_mempool_outputs - total_sat_in_confirmed_outputs,
                utxos: total_mempool_utxos as i64 - total_confirmed_utxos as i64,
                sat_balance: (mempool_sat_balance as i64 - confirmed_sat_balance as i64)
                    .to_string(),
                usd_balance: tip_exchange_rate
                    .map(|rate| format!("{:.2}", (pending_balance as f64 * rate) / 100000000.0)),
            },
        },
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

    Ok((StatusCode::OK, Json(out)))
}

pub static EXAMPLE_RESPONSE: &str = r##"{
    "data": {
        "total_txs": 6,
        "total_inputs": 6,
        "total_sat_in_inputs": 540481308,
        "total_outputs": 6,
        "total_sat_in_outputs": 539817108,
        "total_utxos": 0,
        "runes": false,
        "total_inscriptions": 0,
        "sat_balance": "0",
        "usd_balance": "0.00",
        "pending": {
            "txs": 1,
            "inputs": 1,
            "sat_in_inputs": 89692768,
            "outputs": 1,
            "sat_in_outputs": 89582068,
            "utxos": 0,
            "sat_balance": "0",
            "usd_balance": "0.00"
        }
    },
    "indexer_info": {
        "chain_tip": {
            "block_hash": "000000000000000000019f3ff6e0e9b59a5f13e9514c21c8912c92d7592de88c",
            "block_height": 903987
        },
        "mempool_timestamp": "2025-07-04 15:26:13",
        "estimated_blocks": [{
            "block_height": 903988,
            "sats_per_vb": {
                "min": 1,
                "median": 1,
                "max": 120
            }
        }]
    }
}"##;
