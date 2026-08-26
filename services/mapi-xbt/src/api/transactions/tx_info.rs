use std::collections::HashMap;
use std::str::FromStr;

use axum::{extract::Path, response::IntoResponse, Extension, Json};
use bitcoin::{hashes::Hash, Address, BlockHash, Txid};
use hex;
use reqwest::StatusCode;
use timbre_xbt::{
    reducers::{
        spending_tx_by_txo::{Key as SpendingTxByTxoKey, Value as SpendingTxByTxoValue},
        tx_info::{Key as TxInfoKey, Value as TxInfoValue},
        txs_by_block::{Key as TxsByBlockKey, Value as TxsByBlockValue},
    },
    Reducer,
};

use crate::{
    error::Error,
    options::Mode,
    tikv::{adapter::TiKVAdapter, key_resolver::ReducerType},
    timer::Timer,
    types::{
        transactions::{TxIn, TxInfo, TxOut},
        Metaprotocol, TimestampedResponse,
    },
    util::{parse_tx_hash, timestamp_to_string},
};

pub static REQUIRED_REDUCERS: &[ReducerType] = &[
    ReducerType::ScriptByScriptHash,
    ReducerType::SpendingTxByTxo,
    ReducerType::TxsByBlock,
    ReducerType::TxInfo,
];

#[utoipa::path(
    tag = "Transactions",
    get,
    path = "/transactions/{tx_hash}",
    params(
        ("tx_hash" = String, Path, description = "Transaction hash", example="123828d4f3afe397a9e512b910c54fa3ea6288b7c26796e601be6be8bc2d572b"),
    ),
    responses(
        (
            status = 200,
            description = "Requested data",
            body = TimestampedTxInfo,
            example = json!(serde_json::Value::from_str(EXAMPLE_RESPONSE).unwrap())
        ),
        (status = 400, description = "Malformed query parameters"),
        (status = 404, description = "Requested entity not found on-chain"),
        (status = 500, description = "Internal server error"),
    )
)]
#[tracing::instrument(name = "TRANSACTION_INFO", level = "info", skip(tikv, mode))]
/// Transaction Info
///
/// Returns a full breakdown of a Bitcoin transaction by its hash. Includes inputs, outputs, fees, block confirmation details, and protocol-specific data (e.g., Ordinals, Runes, BRC20). This is useful for explorers, audit tools, or any application requiring full visibility into how funds and inscriptions are moved.
pub async fn tx_info(
    Path(tx_hash): Path<String>,
    mut tikv: Extension<TiKVAdapter>,
    mode: Extension<Mode>,
) -> Result<impl IntoResponse, Error> {
    let mut timer = Timer::new();

    // ---

    tikv.init_tip(REQUIRED_REDUCERS).await?;

    timer.checkpoint("initialise tikv adapter");

    // --- fetch tx info

    let tx_hash = parse_tx_hash(&tx_hash)?;

    let tx_info = tikv
        .get_reducer_key_maybe::<TxInfoKey, TxInfoValue>(
            (ReducerType::TxInfo, Reducer::TxInfo),
            &TxInfoKey { tx_hash },
        )
        .await?
        .ok_or_else(|| Error::NotFound)?;

    timer.checkpoint("fetch tx info");

    let tx_index =
        tikv.get_reducer_key::<TxsByBlockKey, TxsByBlockValue>(
            (ReducerType::TxsByBlock, Reducer::TxsByBlock),
            &TxsByBlockKey {
                height: tx_info.block_height,
            },
        )
        .await?
        .tx_hashes
        .iter()
        .position(|hash| *hash == tx_hash)
        .ok_or_else(|| Error::Internal("missing tx hash in block txs".into()))? as u32;

    timer.checkpoint("fetch tx index");

    // --- resolve addresses in inputs and outputs
    let mut resolved_script_hashes: HashMap<[u8; 20], (Option<Address>, Vec<u8>)> = HashMap::new();

    let mut inputs = vec![];
    for tx_in in tx_info.inputs.iter() {
        let script_hash = tx_in.script_hash;
        let (address, script_bytes) = match resolved_script_hashes.get(&script_hash) {
            Some(res) => res.clone(),
            None => {
                let (address, script_bytes) = tikv.resolve_script_hash(mode.0, script_hash).await?;

                resolved_script_hashes.insert(script_hash, (address.clone(), script_bytes.clone()));

                (address, script_bytes)
            }
        };

        timer.checkpoint("resolved input address");

        inputs.push(TxIn {
            txid: Txid::from_byte_array(tx_in.utxo_hash).to_string(),
            vout: tx_in.utxo_vout,
            address: address.map(|x| x.to_string()),
            script_pubkey: hex::encode(script_bytes),
            satoshis: tx_in.satoshis.to_string(),
        });
    }

    let mut outputs = vec![];
    for (vout, tx_out) in tx_info.outputs.into_iter().enumerate() {
        let script_hash = tx_out.script_hash;
        let (address, script_bytes) = match resolved_script_hashes.get(&script_hash) {
            Some(res) => res.clone(),
            None => {
                let (address, script_bytes) = tikv.resolve_script_hash(mode.0, script_hash).await?;

                resolved_script_hashes.insert(script_hash, (address.clone(), script_bytes.clone()));

                (address, script_bytes)
            }
        };

        timer.checkpoint("resolved output address");

        // --- fetch info about this output having been spent
        let spending_tx = tikv
            .get_reducer_key_maybe::<SpendingTxByTxoKey, SpendingTxByTxoValue>(
                (ReducerType::SpendingTxByTxo, Reducer::SpendingTxByTxo),
                &SpendingTxByTxoKey {
                    utxo_tx_hash: tx_hash,
                    utxo_vout: vout as u32,
                },
            )
            .await?
            .map(|v| Txid::from_byte_array(v.tx_hash).to_string());

        timer.checkpoint("fetched spending tx info");

        outputs.push(TxOut {
            address: address.map(|x| x.to_string()),
            script_pubkey: hex::encode(script_bytes),
            satoshis: tx_out.satoshis.to_string(),
            spending_tx,
        });
    }

    let unix_timestamp = tx_info
        .timestamp
        .ok_or(Error::Internal("no block timestamp".into()))?;

    let block_hash = tx_info
        .block_hash
        .ok_or(Error::Internal("no block hash".into()))?;

    let last_updated = tikv.get_snapshot_point()?;

    let mut metaprotocols = vec![];

    if tx_info.involves_inscriptions {
        metaprotocols.push(Metaprotocol::Inscriptions);
    }

    if tx_info.involves_runes {
        metaprotocols.push(Metaprotocol::Runes);
    }

    if tx_info.involves_brc20 {
        metaprotocols.push(Metaprotocol::Brc20);
    }

    let out = TimestampedResponse {
        data: TxInfo {
            height: tx_info.block_height,
            block_hash: BlockHash::from_byte_array(block_hash).to_string(),
            confirmations: last_updated
                .block_height
                .saturating_sub(tx_info.block_height),
            timestamp: timestamp_to_string(unix_timestamp as u64),
            unix_timestamp,
            tx_index,
            volume: tx_info.volume.to_string(),
            fees: tx_info.fees.to_string(),
            sats_per_vb: tx_info.sats_per_vb,
            metaprotocols,
            inputs,
            outputs,
        },
        last_updated,
    };

    timer.finish();

    Ok((StatusCode::OK, Json(out)))
}

static EXAMPLE_RESPONSE: &str = r##"{
    "data": {
        "height": 855413,
        "block_hash": "0000000000000000000224d324bbb5df0e74e202d9ccf752d76341046fab3ec0",
        "confirmations": 20035,
        "unix_timestamp": 1722814700,
        "timestamp": "2024-08-04 23:38:20",
        "tx_index": 18,
        "volume": "284418",
        "fees": "8000",
        "sats_per_vb": 35,
        "metaprotocols": [
            "inscriptions",
            "runes",
            "brc20"
        ],
        "inputs": [{
            "txid": "54f5c0dba6874d5d7305e108cba2c903145886bccb35e964f2a9b5063dd28f11",
            "vout": 1,
            "address": "bc1qtlvtyurmupvg0g0a9tg0799hp3uwncj0wlg429",
            "script_pubkey": "00145fd8b2707be05887a1fd2ad0ff14b70c78e9e24f",
            "satoshis": "546"
        }, {
            "txid": "d4c93d92f6704bf48dc693b92efa9b0675a6f6d171bdedb90cbf0f64f50749bb",
            "vout": 3,
            "address": "bc1q6kxj39dwva5xfv5278vcp3uhmql567qlpldtcr",
            "script_pubkey": "0014d58d2895ae676864b28af1d980c797d83f4d781f",
            "satoshis": "291872"
        }],
        "outputs": [{
            "address": null,
            "script_pubkey": "6a5d0b00c0a23303a9878cc30d01",
            "satoshis": "0",
            "spending_tx": null
        }, {
            "address": "bc1qj7dam98j6ktjcp320qu77y2vrylv49c2k2hkmu",
            "script_pubkey": "0014979bdd94f2d5972c062a7839ef114c193eca970a",
            "satoshis": "546",
            "spending_tx": "794c74fcf67db3a2bf5c517a5fbc073f087b476c6c5bc7e25f01178aca739451"
        }, {
            "address": "bc1q6kxj39dwva5xfv5278vcp3uhmql567qlpldtcr",
            "script_pubkey": "0014d58d2895ae676864b28af1d980c797d83f4d781f",
            "satoshis": "283872",
            "spending_tx": "68337ace84eb959c98139aa6b43b642539622fa58d45d4362464edc2df26ea82"
        }]
    },
    "last_updated": {
        "block_hash": "00000000000000000000c0ab8525c3e1839e991ff6e06665d206370133ca2c96",
        "block_height": 875448
    }
}"##;
