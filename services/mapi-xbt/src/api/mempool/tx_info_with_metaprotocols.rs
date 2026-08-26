use std::collections::HashMap;
use std::str::FromStr;

use axum::{extract::Path, response::IntoResponse, Extension, Json};
use bitcoin::{hashes::Hash, Address, Txid};
use hex;
use reqwest::StatusCode;
use timbre_xbt::{
    reducers::{
        etching_by_rune_id::{Key as EtchingByRuneIdKey, Value as EtchingByRuneIdValue},
        sats_per_vb_by_block::{Key as SatsPerVbByBlockKey, Value as SatsPerVbByBlockValue},
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
        transactions::{MempoolTxInfoMetaprotocols, TxInMetaprotocols, TxOutMetaprotocols},
        BlockSatsPerVb, EstimatedBlock, InscriptionAndOffset, MempoolLastUpdated,
        MempoolTimestampedResponse, Metaprotocol, RuneAndAmount,
    },
    util::{decimal, parse_tx_hash, timestamp_to_string},
};

pub static REQUIRED_REDUCERS: &[ReducerType] = &[
    ReducerType::EtchingByRuneId,
    // resolving script hashes
    ReducerType::ScriptByScriptHash,
    // estimated block fees
    ReducerType::SatsPerVbByBlock,
    ReducerType::SpendingTxByTxo,
    ReducerType::TxInfo,
];

#[utoipa::path(
    tag = "Transactions",
    get,
    path = "/mempool/transactions/{tx_hash}/metaprotocols",
    params(
        ("tx_hash" = String, Path, description = "Transaction hash", example="1b07f02356aed6ddca37db8226c6292f2953d55ea741d7f58d44427976e7d4ee"),
    ),
    responses(
        (
            status = 200,
            description = "Requested data",
            body = MempoolTimestampedTxInfoMetaprotocols,
            example = json!(serde_json::Value::from_str(EXAMPLE_RESPONSE).unwrap())
        ),
        (status = 400, description = "Malformed query parameters"),
        (status = 404, description = "Requested entity not found on-chain"),
        (status = 500, description = "Internal server error"),
    )
)]
#[tracing::instrument(
    name = "MEMPOOL_TRANSACTION_INFO_WITH_METAPROTOCOLS",
    level = "info",
    skip(tikv, mode)
)]
/// Transaction Info with Metaprotocols (Mempool-aware)
///
/// Returns an enhanced view of the transaction, including info about metaprotocols in both inputs and outputs. Useful for deep inspection tools.
///
/// In addition to confirmed transactions, mempool endpoints return data which reflects pending transactions in some number of "estimated" blocks - predicted blocks containing transactions which have been propagated around the network but not yet included in a mined block, with transactions with a higher sat/vB value being prioritised. The response details how many of these estimated blocks were considered when fetching the data.
pub async fn tx_info_with_metaprotocols(
    Path(tx_hash): Path<String>,
    mut tikv: Extension<TiKVAdapter>,
    mode: Extension<Mode>,
) -> Result<impl IntoResponse, Error> {
    let mut timer = Timer::new();

    tikv.init_mempool(REQUIRED_REDUCERS, None).await?;
    timer.checkpoint("Initialize tikv adapter");

    let snapshot_chain_tip = tikv.get_snapshot_point()?;
    let snapshot_mempool_view = tikv.get_snapshot_mempool_info()?;
    let found_mempool_blocks = snapshot_mempool_view.map(|x| x.mempool_blocks).unwrap_or(0);

    // Fetch tx info.
    let tx_hash = parse_tx_hash(&tx_hash)?;

    let tx_info = tikv
        .get_reducer_key_maybe::<TxInfoKey, TxInfoValue>(
            (ReducerType::TxInfo, Reducer::TxInfo),
            &TxInfoKey { tx_hash },
        )
        .await?
        .ok_or_else(|| Error::NotFound)?;

    timer.checkpoint("Fetch tx info");

    // Resolve addresses in inputs and outputs.
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

        timer.checkpoint("Resolved input address");

        // Parse inscriptions.
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

        timer.checkpoint("Processed inscriptions in input");

        // Parse runes.
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

        timer.checkpoint("Processed runes in input");

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

        // Parse inscriptions.
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

        timer.checkpoint("Processed inscriptions in output");

        // Parse runes.
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

        timer.checkpoint("Processed runes in output");

        // Fetch info about this output having been spent.
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

        timer.checkpoint("Fetched spending tx info");

        outputs.push(TxOutMetaprotocols {
            address: address.map(|x| x.to_string()),
            script_pubkey: hex::encode(script_bytes),
            satoshis: tx_out.satoshis.to_string(),
            spending_tx,
            inscriptions,
            runes,
        });
    }

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

    // Compute indexer info specific to the mempool (estimated blocks).
    let mut estimated_blocks = vec![];

    for i in 0..found_mempool_blocks as u64 {
        let estimated_block_height = snapshot_chain_tip.block_height + (i + 1);

        let sats_per_vb_vals = tikv
            .get_reducer_key::<_, SatsPerVbByBlockValue>(
                (ReducerType::SatsPerVbByBlock, Reducer::SatsPerVbByBlock),
                &SatsPerVbByBlockKey {
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
        data: MempoolTxInfoMetaprotocols {
            height: tx_info.block_height,
            volume: tx_info.volume.to_string(),
            fees: tx_info.fees.to_string(),
            sats_per_vb: tx_info.sats_per_vb,
            metaprotocols,
            inputs,
            outputs,
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
        "height": 900962,
        "volume": "3666788",
        "fees": "508",
        "sats_per_vb": 2,
        "metaprotocols": [
            "runes"
        ],
        "inputs": [
            {
                "txid": "ed6273c6ff9259ec6a68dff0b21ddc4499e2e6d9837981f8c5089ac4935f6cf5",
                "vout": 0,
                "address": "bc1qyvtvxfyz60mmudgsmtxxpf22jl040ejmpj5mqnwv9lwr30jzmygqhr9tv8",
                "script_pubkey": "00202316c32482d3f7be3510dacc60a54a97df57e65b0ca9b04dcc2fdc38be42d910",
                "satoshis": "3666696",
                "inscriptions": [],
                "runes": []
            },
            {
                "txid": "a115a1818805fa06a80eaa57287c0c3f57d5993c1f0d3f771523b4d11711d31f",
                "vout": 1,
                "address": "bc1pn2w92qyh7mwpf9rzmnvjnc0whswft243n0ntj50lurpnsjwwfy9sjur75v",
                "script_pubkey": "51209a9c550097f6dc149462dcd929e1eebc1c95aab19be6b951ffe0c33849ce490b",
                "satoshis": "600",
                "inscriptions": [],
                "runes": []
            }
        ],
        "outputs": [
            {
                "address": null,
                "script_pubkey": "6a5d0b00c0a23303ffbedbb50201",
                "satoshis": "0",
                "spending_tx": null,
                "inscriptions": [],
                "runes": []
            },
            {
                "address": "bc1pdruptufwh9awfepzq506kh03392lk074n4a5cy3v8zu0ma0sjvtqhwdu85",
                "script_pubkey": "512068f815f12eb97ae4e422051fab5df18955fb3fd59d7b4c122c38b8fdf5f09316",
                "satoshis": "546",
                "spending_tx": null,
                "inscriptions": [],
                "runes": []
            },
            {
                "address": "bc1qyvtvxfyz60mmudgsmtxxpf22jl040ejmpj5mqnwv9lwr30jzmygqhr9tv8",
                "script_pubkey": "00202316c32482d3f7be3510dacc60a54a97df57e65b0ca9b04dcc2fdc38be42d910",
                "satoshis": "3641305",
                "spending_tx": null,
                "inscriptions": [],
                "runes": []
            },
            {
                "address": "bc1qp8j9sx6609h7llqufurxjgrwsqwt020tqzn0gs",
                "script_pubkey": "001409e4581b5a796feffc1c4f0669206e801cb7a9eb",
                "satoshis": "580",
                "spending_tx": null,
                "inscriptions": [],
                "runes": []
            },
            {
                "address": "bc1qqx7h6wrl52hxwqnp8v8k072ahnr3sq8huzynww",
                "script_pubkey": "001401bd7d387fa2ae6702613b0f67f95dbcc71800f7",
                "satoshis": "24357",
                "spending_tx": null,
                "inscriptions": [],
                "runes": []
            }
        ]
    },
    "indexer_info": {
        "chain_tip": {
            "block_hash": "0000000000000000000148c7dbf4f8721db8912485cc6860e5b22f9b62e09870",
            "block_height": 900961
        },
        "mempool_timestamp": "2025-06-12 17:27:29",
        "estimated_blocks": [
            {
                "block_height": 900962,
                "sats_per_vb": {
                    "min": 2,
                    "median": 4,
                    "max": 101
                }
            }
        ]
    }
}"##;
