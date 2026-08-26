use axum::{
    extract::{Path, Query},
    response::IntoResponse,
    Extension, Json,
};
use bitcoin::{hashes::Hash, Address, Txid};
use reqwest::StatusCode;
use std::collections::HashMap;
use std::str::FromStr;
use timbre_xbt::{
    reducers::{
        height_by_block_hash::{Key as HeightByBlockHashKey, Value as HeightByBlockHashValue},
        spending_tx_by_txo::{Key as SpendingTxByTxoKey, Value as SpendingTxByTxoValue},
        tx_info::{Key as TxInfoKey, Value as TxInfoValue},
        txs_by_block::{
            Cursor as TxsByBlockCursor, Key as TxsByBlockKey, Value as TxsByBlockValue,
        },
    },
    Decode, Encode, Reducer,
};

use crate::{
    error::Error,
    options::Mode,
    tikv::{adapter::TiKVAdapter, key_resolver::ReducerType},
    timer::Timer,
    types::{
        blocks::{TxByBlock, TxInByBlock, TxOutByBlock},
        BlockParam, CountParam, CursorPaginationParams, Metaprotocol, PaginatedResponse,
    },
    util::{parse_block_param, ParsedPaginationParams, TXS_BY_BLOCK_MAX_UTXOS},
};

pub static REQUIRED_REDUCERS: &[ReducerType] = &[
    ReducerType::HeightByBlockHash,
    ReducerType::ScriptByScriptHash,
    ReducerType::SpendingTxByTxo,
    ReducerType::TxsByBlock,
    ReducerType::TxInfo,
];

#[utoipa::path(
    tag = "Blocks",
    get,
    path = "/blocks/{height_or_hash}/transactions",
    params(
        ("height_or_hash" = String, Path, description = "Block height or block hash", example="878011"),

        ("count" = inline(Option<CountParam>), Query, description = "The max number of transactions per page"),

        ("cursor" = inline(Option<String>), Query, description = "Pagination cursor string, use the cursor included in a page of results to fetch the next page"),
    ),
    responses(
        (
            status = 200,
            description = "Requested data",
            body = PaginatedTxsByBlock,
            example = json!(serde_json::Value::from_str(EXAMPLE_RESPONSE).unwrap()),
        ),
        (status = 400, description = "Malformed query parameters"),
        (status = 404, description = "Requested entity not found on-chain"),
        (status = 500, description = "Internal server error"),
    )
)]
#[tracing::instrument(name = "TXS_BY_BLOCK", level = "info", skip(tikv, mode))]
/// Transactions by Block
///
/// Returns a list of all transaction hashes included in the specified block. Supports pagination for blocks with a large number of transactions.
pub async fn txs_by_block(
    page_params: Query<CursorPaginationParams>,
    Path(height_or_hash): Path<String>,
    mut tikv: Extension<TiKVAdapter>,
    mode: Extension<Mode>,
) -> Result<impl IntoResponse, Error> {
    // --- initialize timer
    let mut timer = Timer::new();

    // --- initialize TiKVAdapter
    tikv.init_tip(REQUIRED_REDUCERS).await?;

    timer.checkpoint("finished initializing TiKVAdapter");

    // --- get `Prefix` related to `TxsByBlock` reducer, needed to build the keys range
    let txs_by_block_encoder = tikv.get_encoder(ReducerType::TxsByBlock)?;

    timer.checkpoint("fetched prefix from TiKV");

    // --- initialize next_cursor
    let mut next_cursor = None;

    // --- parse block hash into block height
    let height = match parse_block_param(&height_or_hash, false)? {
        BlockParam::Hash(block_hash) => {
            tikv.get_reducer_key_maybe::<HeightByBlockHashKey, HeightByBlockHashValue>(
                (ReducerType::HeightByBlockHash, Reducer::HeightByBlockHash),
                &HeightByBlockHashKey { block_hash },
            )
            .await?
            .ok_or_else(|| Error::NotFound)?
            .block_height
        }
        BlockParam::Height(height) => height,
        BlockParam::Timestamp(_) => {
            return Err(Error::Internal(format!(
                "Timestamps are not supported for this endpoint ({})",
                height_or_hash
            )))
        }
    };

    let mut tx_hashes = tikv
        .get_reducer_key_maybe::<TxsByBlockKey, TxsByBlockValue>(
            (ReducerType::TxsByBlock, Reducer::TxsByBlock),
            &TxsByBlockKey { height },
        )
        .await?
        .ok_or_else(|| Error::NotFound)?
        .tx_hashes;

    // Some(x) if x was the index of the last tx hash of the previous page
    // None if no cursor
    let cursor = if let Some(encoded_cursor) = &page_params.cursor {
        let (cursor, _) = TxsByBlockCursor::decode_base64(&encoded_cursor)
            .map_err(|_| Error::MalformedRequest("Malformed cursor: unable to decode".into()))?;

        let to_drain = std::cmp::min((cursor.tx_index + 1) as usize, tx_hashes.len());

        tx_hashes.drain(..to_drain);

        Some(cursor.tx_index)
    } else {
        None
    };

    let page_params = ParsedPaginationParams::parse_no_height::<_, TxsByBlockCursor>(
        page_params.0,
        &txs_by_block_encoder,
        &Reducer::TxsByBlock,
        Some(height),
    )?;

    timer.checkpoint("fetched txs in block");

    // --- resolve addresses in inputs and outputs
    let mut resolved_script_hashes: HashMap<[u8; 20], (Option<Address>, Vec<u8>)> = HashMap::new();

    let mut data: Vec<TxByBlock> = vec![];

    let mut txs_by_block = tx_hashes.into_iter().enumerate();

    while let Some((i, tx_hash)) = txs_by_block.next() {
        let tx_index = match cursor {
            Some(cursor_idx) => i as u32 + cursor_idx + 1,
            None => i as u32,
        };

        // --- if page limit has been reached and there are more txs in the block, return a cursor
        if i == (page_params.count() - 1) && txs_by_block.next().is_some() {
            next_cursor = Some(TxsByBlockCursor { tx_index }.encode_base64());
        }

        // --- fetch tx info
        let tx_info = tikv
            .get_reducer_key::<TxInfoKey, TxInfoValue>(
                (ReducerType::TxInfo, Reducer::TxInfo),
                &TxInfoKey { tx_hash },
            )
            .await
            .map_err(|e| Error::Internal([format!("Could not find tx info ({e})")].concat()))?;

        timer.checkpoint("feched tx info");

        // --- build list of metaprotocols
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

        let mut inputs: Vec<TxInByBlock> = vec![];

        // --- include at most TXS_BY_BLOCK_MAX_UTXOS inputs
        for input in tx_info.inputs.into_iter().take(TXS_BY_BLOCK_MAX_UTXOS) {
            // --- resolve address and store for future use
            let script_hash = input.script_hash;
            let (address, script_bytes) = match resolved_script_hashes.get(&script_hash) {
                Some(res) => res.clone(),
                None => {
                    let (address, script_bytes) = tikv
                        .resolve_script_hash(mode.0, script_hash)
                        .await
                        .map_err(|e| {
                            Error::Internal([format!("Could not resolve address ({e})")].concat())
                        })?;

                    resolved_script_hashes
                        .insert(script_hash, (address.clone(), script_bytes.clone()));

                    (address, script_bytes)
                }
            };

            inputs.push(TxInByBlock {
                txid: Txid::from_byte_array(input.utxo_hash).to_string(),
                vout: input.utxo_vout,
                address: address.map(|x| x.to_string()),
                script_pubkey: hex::encode(script_bytes),
                satoshis: input.satoshis.to_string(),
                inscriptions: input.inscriptions.len() as u128,
                runes: input.runes.len() as u128,
            });

            timer.checkpoint("finished processing input");
        }

        let mut outputs: Vec<TxOutByBlock> = vec![];

        // --- include at most TXS_BY_BLOCK_MAX_UTXOS outputs
        for (output_index, output) in tx_info
            .outputs
            .into_iter()
            .take(TXS_BY_BLOCK_MAX_UTXOS)
            .enumerate()
        {
            // --- resolve address and store for future use
            let script_hash = output.script_hash;
            let (address, script_bytes) = match resolved_script_hashes.get(&script_hash) {
                Some(res) => res.clone(),
                None => {
                    let (address, script_bytes) =
                        tikv.resolve_script_hash(mode.0, script_hash).await?;

                    resolved_script_hashes
                        .insert(script_hash, (address.clone(), script_bytes.clone()));

                    (address, script_bytes)
                }
            };

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

            timer.checkpoint("fetched spending tx info");

            outputs.push(TxOutByBlock {
                vout: output_index as u32,
                address: address.map(|x| x.to_string()),
                script_pubkey: hex::encode(script_bytes),
                spending_tx,
                satoshis: output.satoshis.to_string(),
                inscriptions: output.inscriptions.len() as u128,
                runes: output.runes.len() as u128,
            });

            timer.checkpoint("finished processing output");
        }

        let tx_hash = Txid::from_byte_array(tx_hash).to_string();

        data.push(TxByBlock {
            tx_hash,
            tx_index,
            volume: tx_info.volume.to_string(),
            fees: tx_info.fees.to_string(),
            sats_per_vb: tx_info.sats_per_vb,
            metaprotocols,
            total_inputs: inputs.len() as u64,
            inputs,
            total_outputs: outputs.len() as u64,
            outputs,
        });

        // page full
        if next_cursor.is_some() {
            break;
        }
    }

    timer.finish();

    let out = PaginatedResponse {
        data,
        last_updated: tikv.get_snapshot_point()?,
        next_cursor,
    };

    Ok((StatusCode::OK, Json(out)))
}

static EXAMPLE_RESPONSE: &str = r##"{
    "data": [{
        "tx_hash": "a306a1cb42f0e703219e0c2abb8cca08c85c8a3a55d4c0a119d62156a4cf435e",
        "tx_index": 0,
        "volume": "314010952",
        "fees": "0",
        "sats_per_vb": 0,
        "metaprotocols": [
            "inscriptions"
        ],
        "total_inputs": 0,
        "inputs": [],
        "total_outputs": 4,
        "outputs": [{
            "vout": 0,
            "address": "bc1p8k4v4xuz55dv49svzjg43qjxq2whur7ync9tm0xgl5t4wjl9ca9snxgmlt",
            "script_pubkey": "51203daaca9b82a51aca960c1491588246029d7e0fc49e0abdbcc8fd17574be5c74b",
            "spending_tx": null,
            "satoshis": "546",
            "inscriptions": 0,
            "runes": 0
        }, {
            "vout": 1,
            "address": "bc1qwzrryqr3ja8w7hnja2spmkgfdcgvqwp5swz4af4ngsjecfz0w0pqud7k38",
            "script_pubkey": "00207086320071974eef5e72eaa01dd9096e10c0383483855ea6b344259c244f73c2",
            "spending_tx": "1179ebbad0019e5743f25c86148f51b70bc705e76392ce1195205b7fb0d3f8d5",
            "satoshis": "314010406",
            "inscriptions": 3519,
            "runes": 0
        }, {
        "vout": 2,
            "address": null,
            "script_pubkey": "6a24aa21a9edf17f948f3b2f268b144bd8677617399ea639a55f0e849ba5e6f35af26cc39904",
            "spending_tx": null,
            "satoshis": "0",
            "inscriptions": 0,
            "runes": 0
        }, {
            "vout": 3,
            "address": null,
            "script_pubkey": "6a2d434f5245012e50087fb834747606ed01ad67ad0f32129ab431e6d18fda214e5b9f350ffc7b6cf3058b9026e765",
            "spending_tx": null,
            "satoshis": "0",
            "inscriptions": 0,
            "runes": 0
        }]
    }, {
        "tx_hash": "04ac1f8968de3d1db8a6fc6504005e391ab8a85bb4a48b3d7f3e66e747d559ea",
        "tx_index": 1,
        "volume": "94223592",
        "fees": "30300",
        "sats_per_vb": 151,
        "metaprotocols": [],
        "total_inputs": 1,
        "inputs": [{
            "txid": "65ff8bb5ab3feeeb1cdd47a200fc3d54ac70e4e4b1ca1c7e33f56207db7ddf86",
            "vout": 1,
            "address": "bc1q5yw0e7hq42zwshvvq3sy2z07n7lg72elhdt8s577egxe2agm3xnq9nekyr",
            "script_pubkey": "0020a11cfcfae0aa84e85d8c04604509fe9fbe8f2b3fbb567853deca0d95751b89a6",
            "satoshis": "94253892",
            "inscriptions": 0,
            "runes": 0
        }],
        "total_outputs": 2,
        "outputs": [{
            "vout": 0,
            "address": "bc1q0wu0tqp2u3rtunjl0h0rsl9pvf86acy6sep63st0lp7lgg67ykzqeq89pn",
            "script_pubkey": "00207bb8f5802ae446be4e5f7dde387ca1624faee09a8643a8c16ff87df4235e2584",
            "spending_tx": "0c135cbd187ef5ed2ab266f02910cabfd6fa0993d1dbf6627a33363fba38181d",
            "satoshis": "9790000",
            "inscriptions": 0,
            "runes": 0
        }, {
            "vout": 1,
            "address": "bc1qnq7td9m6xex4gqeu2d3t6j8tqkpwfxwt2dn2t07m6hnl9svcn54qu5hax4",
            "script_pubkey": "0020983cb6977a364d54033c5362bd48eb0582e499cb5366a5bfdbd5e7f2c1989d2a",
            "spending_tx": "4075db2355cdcac155657c55a24a46e2fd083fc88dedd54bc900288a61a688e7",
            "satoshis": "84433592",
            "inscriptions": 0,
            "runes": 0
        }]

    }],
    "last_updated": {
        "block_hash": "0000000000000000000122b2c240af790ee979f6e96175c00045cf54aa5a7001",
        "block_height": 878055
    },
    "next_cursor": "AQE"
}"##;
