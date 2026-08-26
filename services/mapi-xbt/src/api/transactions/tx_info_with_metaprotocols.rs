use std::collections::HashMap;
use std::str::FromStr;

use axum::{extract::Path, response::IntoResponse, Extension, Json};
use bitcoin::{hashes::Hash, Address, BlockHash, Txid};
use hex;
use reqwest::StatusCode;
use timbre_xbt::{
    reducers::{
        etching_by_rune_id::{Key as EtchingByRuneIdKey, Value as EtchingByRuneIdValue},
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
        transactions::{TxInMetaprotocols, TxInfoMetaprotocols, TxOutMetaprotocols},
        InscriptionAndOffset, Metaprotocol, RuneAndAmount, TimestampedResponse,
    },
    util::{decimal, parse_tx_hash, timestamp_to_string},
};

pub static REQUIRED_REDUCERS: &[ReducerType] = &[
    ReducerType::EtchingByRuneId,
    ReducerType::ScriptByScriptHash,
    ReducerType::SpendingTxByTxo,
    ReducerType::TxsByBlock,
    ReducerType::TxInfo,
];

#[utoipa::path(
    tag = "Transactions",
    get,
    path = "/transactions/{tx_hash}/metaprotocols",
    params(
        ("tx_hash" = String, Path, description = "Transaction hash", example="1b07f02356aed6ddca37db8226c6292f2953d55ea741d7f58d44427976e7d4ee"),
    ),
    responses(
        (
            status = 200,
            description = "Requested data",
            body = TimestampedTxInfoMetaprotocols,
            example = json!(serde_json::Value::from_str(EXAMPLE_RESPONSE).unwrap())
        ),
        (status = 400, description = "Malformed query parameters"),
        (status = 404, description = "Requested entity not found on-chain"),
        (status = 500, description = "Internal server error"),
    )
)]
#[tracing::instrument(
    name = "TRANSACTION_INFO_WITH_METAPROTOCOLS",
    level = "info",
    skip(tikv, mode)
)]
/// Transaction Info with Metaprotocols
///
/// Returns an enhanced view of the transaction, including info about metaprotocols in both inputs and outputs. Useful for deep inspection tools.
pub async fn tx_info_with_metaprotocols(
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
    for tx_in in tx_info.inputs.into_iter() {
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

        // --- parse inscriptions
        let mut inscriptions = vec![];
        for (offset, (reveal_tx_hash, inscription_index)) in tx_in.inscriptions.into_iter() {
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

        timer.checkpoint("processed inscriptions in input");

        // --- parse runes
        let mut runes = vec![];
        for ((etching_block, etching_tx), amount) in tx_in.runes.into_iter() {
            runes.push(RuneAndAmount {
                rune_id: format!("{}:{}", etching_block, etching_tx),
                amount: {
                    let dec = tikv
                        .get_reducer_key::<_, EtchingByRuneIdValue>(
                            (ReducerType::EtchingByRuneId, Reducer::EtchingByRuneId),
                            &EtchingByRuneIdKey {
                                rune_id: (etching_block, etching_tx),
                            },
                        )
                        .await?
                        .divisibility
                        .unwrap_or(0) as usize;
                    decimal(amount, dec)
                },
            })
        }

        timer.checkpoint("processed runes in input");

        inputs.push(TxInMetaprotocols {
            txid: Txid::from_byte_array(tx_in.utxo_hash).to_string(),
            vout: tx_in.utxo_vout,
            address: address.map(|x| x.to_string()),
            script_pubkey: hex::encode(script_bytes),
            satoshis: tx_in.satoshis.to_string(),
            inscriptions,
            runes,
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

        timer.checkpoint("processed inscriptions in output");

        // --- parse runes
        let mut runes = vec![];
        for ((etching_block, etching_tx), amount) in tx_out.runes.into_iter() {
            runes.push(RuneAndAmount {
                rune_id: format!("{}:{}", etching_block, etching_tx),
                amount: {
                    let dec = tikv
                        .get_reducer_key::<_, EtchingByRuneIdValue>(
                            (ReducerType::EtchingByRuneId, Reducer::EtchingByRuneId),
                            &EtchingByRuneIdKey {
                                rune_id: (etching_block, etching_tx),
                            },
                        )
                        .await?
                        .divisibility
                        .unwrap_or(0) as usize;
                    decimal(amount, dec)
                },
            })
        }

        timer.checkpoint("processed runes in output");

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

        outputs.push(TxOutMetaprotocols {
            address: address.map(|x| x.to_string()),
            script_pubkey: hex::encode(script_bytes),
            satoshis: tx_out.satoshis.to_string(),
            spending_tx,
            inscriptions,
            runes,
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
        data: TxInfoMetaprotocols {
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
        "height": 866807,
        "block_hash": "000000000000000000011c58d06536d2c1453a4742274efc2d4456921f027e98",
        "confirmations": 95,
        "unix_timestamp": 1729577794,
        "timestamp": "2024-10-22 06:16:34",
        "tx_index": 12,
        "volume": "20606",
        "fees": "1548800",
        "sats_per_vb": 1600,
        "metaprotocols": [
            "inscriptions",
            "brc20"
        ],
        "inputs": [{
            "txid": "47c7260764af2ee17aa584d9c035f2e5429aefd96b8016cfe0e3f0bcf04869a3",
            "vout": 0,
            "address": "bc1ppth27qnr74qhusy9pmcyeaelgvsfky6qzquv9nf56gqmte59vfhqwkqguh",
            "script_pubkey": "51200aeeaf0263f5417e40850ef04cf73f43209b13401038c2cd34d201b5e685626e",
            "satoshis": "606",
            "inscriptions": [{
                "offset": 0,
                "inscription_id": "6fb976ab49dcec017f1e201e84395983204ae1a7c2abf7ced0a85d692e442799i0"
            }],
            "runes": []
        }, {
            "txid": "fbf8ccedf64109568cb69485b28de876547a8c51bbf8f623c1906a69fdf16be4",
            "vout": 0,
            "address": "bc1pfqgphdlagqahqe5yf5a4yja9nz4hdeycvcmk3j0r3kr79ltetfvqhnlguu",
            "script_pubkey": "512048101bb7fd403b7066844d3b524ba598ab76e498663768c9e38d87e2fd795a58",
            "satoshis": "10000",
            "inscriptions": [],
            "runes": []
        }, {
            "txid": "42d82813e9fb16bcf14b75bd78a1e16983e40d2e04f6c7087dde83e1d93c509b",
            "vout": 0,
            "address": "bc1paefyrryx8j75fe2njzd7flnss9vgse0f9njewm7qpxaaypafhjasjn7qgw",
            "script_pubkey": "5120ee52418c863cbd44e553909be4fe7081588865e92ce5976fc009bbd207a9bcbb",
            "satoshis": "1558800",
            "inscriptions": [],
            "runes": []
        }],
        "outputs": [{
            "address": "bc1p6ka80aqd57wjskxknnfzmswl27tegkejcsfa0v600s6rxle5lagqg38e88",
            "script_pubkey": "5120d5ba77f40da79d2858d69cd22dc1df5797945b32c413d7b34f7c34337f34ff50",
            "satoshis": "606",
            "spending_tx": null,
            "inscriptions": [{
                "offset": 0,
                "inscription_id": "6fb976ab49dcec017f1e201e84395983204ae1a7c2abf7ced0a85d692e442799i0"
            }],
            "runes": []
        }, {
            "address": "bc1p5kjec5vl67yydqqljy4t97hdxq05s62qyqmmgzxhmqcd628ecuqs8c9w97",
            "script_pubkey": "5120a5a59c519fd78846801f912ab2faed301f4869402037b408d7d830dd28f9c701",
            "satoshis": "10000",
            "spending_tx": null,
            "inscriptions": [{
                "offset": 0,
                "inscription_id": "1b07f02356aed6ddca37db8226c6292f2953d55ea741d7f58d44427976e7d4eei0"
            }],
            "runes": []
        }, {
            "address": "bc1p6uzgus82tyx8d4xt7eh0f6kxqn53a03529cyh9wmqcwchm6uvmtqkt5sdv",
            "script_pubkey": "5120d7048e40ea590c76d4cbf66ef4eac604e91ebe3451704b95db061d8bef5c66d6",
            "satoshis": "10000",
            "spending_tx": "72b6ccd9289cc2aac7c9e55b3fa9185115313702e10bee31c79c87fd5ef5fff6",
            "inscriptions": [],
            "runes": [{
                "rune_id": "866807:12",
                "amount": "50000000"
            }]
        }, {
            "address": null,
            "script_pubkey": "6a5d23020704f0daddcdf988a80303400580e9070680e1eb170ae80708d086030cdafe341602",
            "satoshis": "0",
            "spending_tx": null,
            "inscriptions": [],
            "runes": []
        }]
    },
    "last_updated": {
        "block_hash": "00000000000000000001676a1898b7804be18303e68e8ceacc00c713011b0ef4",
        "block_height": 866902
    }
}"##;
