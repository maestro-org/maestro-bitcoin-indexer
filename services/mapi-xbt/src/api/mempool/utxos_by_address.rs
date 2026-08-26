use crate::{
    tikv::{adapter::TiKVAdapter, key_resolver::ReducerType},
    types::{
        BlockSatsPerVb, EstimatedBlock, InscriptionAndOffset, MempoolLastUpdated,
        MempoolPaginatedResponse, MempoolUtxo,
    },
    util::{decimal, estimate_indexer_blocks, timestamp_to_string},
};
use axum::{
    extract::{Path, Query},
    response::IntoResponse,
    Extension, Json,
};
use bitcoin::{hashes::Hash, BlockHash, Script, Txid};
use reqwest::StatusCode;
use serde::Deserialize;
use std::str::FromStr;
use tikv_client::KvPair;
use timbre_xbt::{
    reducers::{
        etching_by_rune_id, inscription_utxos_by_script_hash, rune_utxos_by_script_hash,
        sats_per_vb_by_block,
        utxos_by_script_hash::{
            Cursor as UtxosByScriptHashCursor, Key as UtxosByScriptHashKey,
            Value as UtxosByScriptHashValue,
        },
    },
    Decode, Encode, Reducer,
};

use crate::{
    error::Error,
    tikv::Scanner,
    types::{CountParam, HeightPaginationParams, OrderParam, RuneAndAmount},
    util::ParsedHeightPaginationParams,
};

#[derive(Debug, Deserialize)]
pub struct Params {
    pub filter_dust: Option<bool>,
    pub filter_dust_threshold: Option<u64>,
    pub exclude_metaprotocols: Option<bool>,
    pub ignore_used_brc20: Option<bool>,
    pub mempool_blocks_limit: Option<u8>,
}

pub static REQUIRED_REDUCERS: &[ReducerType] = &[
    ReducerType::UtxosByScriptHash,
    ReducerType::RuneUtxosByScriptHash,
    ReducerType::EtchingByRuneId,
    ReducerType::InscriptionUtxosByScriptHash,
    // estimated block fees
    ReducerType::SatsPerVbByBlock,
    // parsing address parameter
    ReducerType::ScriptByScriptHash,
    ReducerType::ScriptHashByAddressPayloadHash,
    // for ignore_used_brc20 functionality
    ReducerType::ContentByInscriptionId,
];

#[utoipa::path(
    tag = "Addresses",
    get,
    path = "/mempool/addresses/{address}/utxos",
    params(
        ("address" = String, Path, description = "Bitcoin address or hex encoded script pubkey", example="bc1phyrmjs2jm5c98tldke2ykp0h66lsx3wy0ey8ug2fjj5mxsn8ftqsa24un8"),
        ("filter_dust" = Option<bool>, Query, description = "Ignore UTxOs containing less than 100000 sats"),
        ("filter_dust_threshold" = Option<u64>, Query, description = "Ignore UTxOs containing less than specified satoshis"),
        ("exclude_metaprotocols" = Option<bool>, Query, description = "Exclude UTxOs involved in metaprotocols (currently only runes and inscriptions will be discovered, more metaprotocols may be supported in future)"),
        ("ignore_used_brc20" = Option<bool>, Query, description = "When used with exclude_metaprotocols=true, still include UTXOs which only contain used BRC20 inscriptions"),

        ("count" = inline(Option<CountParam>), Query, description = "The max number of results per page"),

        ("order" = inline(Option<OrderParam>), Query, description = "The order in which the results are sorted (by height at which UTxO was produced)"),
        ("from" = inline(Option<u64>), Query, description = "Return only UTxOs created on or after a specific height"),
        ("to" = inline(Option<u64>), Query, description = "Return only UTxOs created on or before a specific height"),

        ("mempool_blocks_limit" = Option<u8>, Query, description = "Limit the number of estimated mempool blocks to be reflected in the data (default: as many as available)"),

        ("cursor" = inline(Option<String>), Query, description = "Pagination cursor string, use the cursor included in a page of results to fetch the next page"),
    ),
    responses(
        (
            status = 200,
            description = "Requested data",
            body = MempoolPaginatedUtxo,
            example = json!(serde_json::Value::from_str(EXAMPLE_RESPONSE).unwrap())
        ),
        (status = 400, description = "Malformed query parameters"),
        (status = 404, description = "Requested entity not found on-chain"),
        (status = 500, description = "Internal server error"),
    )
)]
#[tracing::instrument(name = "MEMPOOL_UTXOS_BY_ADDRESS", level = "info", skip(tikv))]
/// UTxOs by Address (Mempool-aware)
///
/// Retrieves all UTXOs associated with a Bitcoin address or script pubkey. Ideal for wallet views, dust filtering, or balance calculations. Can be tailored to exclude certain categories of UTXOs such as those used in metaprotocols.
///
/// In addition to confirmed transactions, mempool endpoints return data which reflects pending transactions in some number of "estimated" blocks - predicted blocks containing transactions which have been propagated around the network but not yet included in a mined block, with transactions with a higher sat/vB value being prioritised. The response details how many of these estimated blocks were considered when fetching the data.
pub async fn mempool_utxos_by_address(
    page_params: Query<HeightPaginationParams>,
    Path(addr_or_pk): Path<String>,
    params: Query<Params>,
    mut tikv: Extension<TiKVAdapter>,
) -> Result<impl IntoResponse, Error> {
    tikv.init_mempool(REQUIRED_REDUCERS, params.mempool_blocks_limit)
        .await?;

    let utxos_encoder = tikv.get_encoder(ReducerType::UtxosByScriptHash)?;

    let snapshot_chain_tip = tikv.get_snapshot_point()?;

    let snapshot_mempool_view = tikv.get_snapshot_mempool_info()?;

    let found_mempool_blocks = snapshot_mempool_view.map(|x| x.mempool_blocks).unwrap_or(0);

    // --- initialise `next_cursor`

    let mut next_cursor = None;

    // --- parse and try to decode address

    let (address, script_bytes) = match tikv.parse_address_or_script_bytes(&addr_or_pk).await {
        Ok(x) => x,
        Err(Error::NotFound) => {
            // User param is a Bitcoin address, but the corresponding script pub key could not be
            // found in store.
            let out = MempoolPaginatedResponse {
                data: vec![],
                indexer_info: MempoolLastUpdated {
                    chain_tip: snapshot_chain_tip.clone(),
                    mempool_timestamp: None,
                    estimated_blocks: estimate_indexer_blocks(
                        &snapshot_chain_tip.block_height,
                        found_mempool_blocks as u64,
                        &mut tikv,
                    )
                    .await?,
                },
                next_cursor,
            };
            return Ok((StatusCode::OK, Json(out)));
        }
        Err(e) => return Err(e),
    };

    let script = Script::from_bytes(&script_bytes);
    let script_hash = script.script_hash();

    // TODO: cleaner
    let page_params = ParsedHeightPaginationParams::parse::<_, UtxosByScriptHashCursor>(
        page_params.0,
        &utxos_encoder,
        &Reducer::UtxosByScriptHash,
        Some(script_hash.to_byte_array()),
    )?;

    let filter_dust = params.filter_dust.unwrap_or(false);

    let threshold = params
        .filter_dust_threshold
        .unwrap_or(if filter_dust { 100_000 } else { 0 });

    let filter = if threshold > 0 {
        Some(|kv: &KvPair| u64::decode(&kv.1).unwrap().0 >= threshold)
    } else {
        None
    };

    let excl_metaprotocols: bool = params.exclude_metaprotocols.unwrap_or(false);
    let ignore_used_brc20: bool = params.ignore_used_brc20.unwrap_or(false);

    // --- scan keys

    let scanner = Scanner::new(page_params.key_range())
        .count(page_params.count() + 1)
        .order(page_params.order());

    let kvs: Vec<(UtxosByScriptHashKey, UtxosByScriptHashValue)> = if excl_metaprotocols {
        scanner
            .get_non_metaprotocol_utxos(&mut tikv, filter, ignore_used_brc20)
            .await?
    } else {
        scanner
            .execute_with_filter(&mut tikv, ReducerType::UtxosByScriptHash, filter)
            .await?
    };

    // --- process fetched kvs

    let mut kvs = kvs.into_iter().enumerate();

    let mut utxos = Vec::new();

    while let Some((i, (key, value))) = kvs.next() {
        // if this is the last result of the page, check if there is a subsequent
        // result (and therefore we need to return a cursor for next page)
        if i == (page_params.count() - 1) && kvs.next().is_some() {
            next_cursor = Some(
                UtxosByScriptHashCursor {
                    height: key.height,
                    utxo_hash: key.utxo_hash,
                    utxo_index: key.utxo_index,
                }
                .encode_base64(),
            );
        }
        let runes: Vec<RuneAndAmount> = if excl_metaprotocols {
            // if metaprotocol UTxOs are excluded, it's certain this UTxO has no Runes
            vec![]
        } else {
            // build Runes information for this UTxO
            let fetched_runes = tikv
                .get_reducer_key_maybe::<_, rune_utxos_by_script_hash::Value>(
                    (
                        ReducerType::RuneUtxosByScriptHash,
                        Reducer::RuneUtxosByScriptHash,
                    ),
                    &key, // key structure is the same
                )
                .await?
                .map(|x| x.runes)
                .unwrap_or_default();

            let mut fetched_runes = fetched_runes.into_iter().collect::<Vec<_>>();

            fetched_runes.sort_by_key(|(rid, _)| *rid);

            let mut out_runes = Vec::new();

            for (rune_id, amount) in fetched_runes {
                let dec = tikv
                    .get_reducer_key::<_, etching_by_rune_id::Value>(
                        (ReducerType::EtchingByRuneId, Reducer::EtchingByRuneId),
                        &etching_by_rune_id::Key { rune_id },
                    )
                    .await?
                    .divisibility
                    .unwrap_or(0) as usize;

                out_runes.push(RuneAndAmount {
                    rune_id: format!("{}:{}", rune_id.0, rune_id.1),
                    amount: decimal(amount, dec),
                })
            }

            out_runes
        };

        let inscriptions: Vec<InscriptionAndOffset> = if excl_metaprotocols {
            // if metaprotocol UTxOs are excluded, it's certain this UTxO has no inscriptions
            vec![]
        } else {
            // build inscriptions for this UTxO
            let mut inscriptions = tikv
                .get_reducer_key_maybe::<_, inscription_utxos_by_script_hash::Value>(
                    (
                        ReducerType::InscriptionUtxosByScriptHash,
                        Reducer::InscriptionUtxosByScriptHash,
                    ),
                    &key, // key structure is the same
                )
                .await?
                .map(|x| x.inscriptions)
                .unwrap_or_default();

            inscriptions.sort_by_key(|(offset, _)| *offset);

            inscriptions
                .into_iter()
                .map(|(offset, inscription)| InscriptionAndOffset {
                    offset,
                    inscription_id: format!(
                        "{}i{}",
                        Txid::from_byte_array(inscription.0),
                        inscription.1
                    ),
                })
                .collect()
        };

        utxos.push(MempoolUtxo {
            txid: BlockHash::from_byte_array(key.utxo_hash).to_string(),
            vout: key.utxo_index,
            address: address.as_ref().map(|x| x.to_string()),
            script_pubkey: script.to_hex_string(),
            satoshis: value.satoshis.to_string(),
            height: key.height,
            mempool: key.height > snapshot_chain_tip.block_height,
            runes,
            inscriptions,
        })
    }

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
        data: utxos,
        indexer_info: MempoolLastUpdated {
            chain_tip: snapshot_chain_tip,
            mempool_timestamp: snapshot_mempool_view
                .map(|x| timestamp_to_string(x.mempool_view_ts)),
            estimated_blocks,
        },
        next_cursor,
    };

    Ok((StatusCode::OK, Json(out)))
}

static EXAMPLE_RESPONSE: &str = r##"{
    "data": [{
        "txid": "e60e70271cce70df4cc1f9d0217d7cc9cbced26f0526c6ba946945cd155b49e4",
        "vout": 0,
        "address": "bc1pkh05juaxqc3d388klrjq8msszzzfr33nnn5kt2na00jja3mue89q5wxvew",
        "script_pubkey": "5120b5df4973a60622d89cf6f8e403ee10108491c6339ce965aa7d7be52ec77cc9ca",
        "satoshis": "546",
        "height": 867154,
        "mempool": false,
        "runes": [{
            "rune_id": "867138:1861",
            "amount": "2000"
        }],
        "inscriptions": []
    }, {
        "txid": "9f00f52bc6e9d95797e5597ea50427258ba873df059b13a319f0868ca9da1265",
        "vout": 0,
        "address": "bc1pkh05juaxqc3d388klrjq8msszzzfr33nnn5kt2na00jja3mue89q5wxvew",
        "script_pubkey": "5120b5df4973a60622d89cf6f8e403ee10108491c6339ce965aa7d7be52ec77cc9ca",
        "satoshis": "546",
        "height": 867155,
        "mempool": true,
        "runes": [{
            "rune_id": "867138:1861",
            "amount": "44000"
        }],
        "inscriptions": [{
            "offset": 0,
            "inscription_id": "47bb5438d366863b25b4b1782af0d0cf0a89a922adce5da81253790d3e651501i0"
        }]
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
