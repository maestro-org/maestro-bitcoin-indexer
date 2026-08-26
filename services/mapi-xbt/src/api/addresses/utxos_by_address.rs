use crate::{
    tikv::{adapter::TiKVAdapter, key_resolver::ReducerType},
    timer::Timer,
    util::decimal,
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
    types::{
        CountParam, HeightPaginationParams, InscriptionAndOffset, OrderParam, PaginatedResponse,
        RuneAndAmount, Utxo,
    },
    util::ParsedHeightPaginationParams,
};

#[derive(Debug, Deserialize)]
pub struct Params {
    pub filter_dust: Option<bool>,
    pub filter_dust_threshold: Option<u64>,
    pub exclude_metaprotocols: Option<bool>,
    pub ignore_used_brc20: Option<bool>,
}

pub static REQUIRED_REDUCERS: &[ReducerType] = &[
    ReducerType::UtxosByScriptHash,
    ReducerType::RuneUtxosByScriptHash,
    ReducerType::EtchingByRuneId,
    ReducerType::InscriptionUtxosByScriptHash,
    // parsing address parameter
    ReducerType::ScriptByScriptHash,
    ReducerType::ScriptHashByAddressPayloadHash,
    // for ignore_used_brc20 functionality
    ReducerType::ContentByInscriptionId,
];

#[utoipa::path(
    tag = "Addresses",
    get,
    path = "/addresses/{address}/utxos",
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

        ("cursor" = inline(Option<String>), Query, description = "Pagination cursor string, use the cursor included in a page of results to fetch the next page"),
    ),
    responses(
        (
            status = 200,
            description = "Requested data",
            body = PaginatedUtxo,
            example = json!(serde_json::Value::from_str(EXAMPLE_RESPONSE).unwrap())
        ),
        (status = 400, description = "Malformed query parameters"),
        (status = 404, description = "Requested entity not found on-chain"),
        (status = 500, description = "Internal server error"),
    )
)]
#[tracing::instrument(name = "UTXOS_BY_ADDRESS", level = "info", skip(tikv))]
/// UTxOs by Address
///
/// Retrieves all UTXOs associated with a Bitcoin address or script pubkey. Ideal for wallet views, dust filtering, or balance calculations. Can be tailored to exclude certain categories of UTXOs such as those used in metaprotocols.
pub async fn utxos_by_address(
    page_params: Query<HeightPaginationParams>,
    Path(addr_or_pk): Path<String>,
    params: Query<Params>,
    mut tikv: Extension<TiKVAdapter>,
) -> Result<impl IntoResponse, Error> {
    let mut timer = Timer::new();

    // ---

    tikv.init_tip(REQUIRED_REDUCERS).await?;

    let utxos_encoder = tikv.get_encoder(ReducerType::UtxosByScriptHash)?;

    let last_updated = tikv.get_snapshot_point()?;

    timer.checkpoint("initialise tikv adapter");

    // --- initialise `next_cursor`

    let mut next_cursor = None;

    // --- parse and try to decode address

    let (address, script_bytes) = match tikv.parse_address_or_script_bytes(&addr_or_pk).await {
        Ok(x) => x,
        Err(Error::NotFound) => {
            // User param is a Bitcoin address, but the corresponding script pub key could not be
            // found in store.
            let out = PaginatedResponse {
                data: vec![],
                last_updated,
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

    timer.checkpoint("parse params");

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

    timer.checkpoint("fetch kvs");

    // --- process fetched kvs

    let mut kvs = kvs.into_iter().enumerate();

    let mut utxos: Vec<Utxo> = Vec::new();

    // TODO: cleaner
    while let Some((i, (key, value))) = kvs.next() {
        // if this is the last result of the page, check if there is a subsequent
        // result (and therefore we need to return a cursor for next page)
        if i == (page_params.count() - 1) && kvs.next().is_some() {
            // TODO: timbre Cursor::from(key)
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
                    &key, // key structure is same
                )
                .await?
                .map(|x| x.runes)
                .unwrap_or_default()
                .into_iter()
                .collect::<Vec<_>>();

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

        timer.checkpoint("process utxo runes");

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
                    &key, // key structure is same
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

        timer.checkpoint("process utxo inscriptions");

        let confirmations = (last_updated.block_height + 1).saturating_sub(key.height);

        utxos.push(Utxo {
            txid: BlockHash::from_byte_array(key.utxo_hash).to_string(),
            vout: key.utxo_index,
            address: address.as_ref().map(|x| x.to_string()),
            script_pubkey: script.to_hex_string(),
            satoshis: value.satoshis.to_string(),
            confirmations,
            height: key.height,
            runes,
            inscriptions,
        })
    }

    timer.finish();

    let out = PaginatedResponse {
        data: utxos,
        last_updated,
        next_cursor,
    };

    Ok((StatusCode::OK, Json(out)))
}

static EXAMPLE_RESPONSE: &str = r##"{
    "data": [{
        "txid": "cb9532641412a6fa6f329e8728a8f7554dcaa6cdaec1355bd2bd2903cd61c6eb",
        "vout": 1,
        "address": "bc1phyrmjs2jm5c98tldke2ykp0h66lsx3wy0ey8ug2fjj5mxsn8ftqsa24un8",
        "script_pubkey": "00140df0d276eacd58b09a74d5e0288756e8505787f5",
        "satoshis": "546",
        "confirmations": 23,
        "height": 2815497,
        "runes": [],
        "inscriptions": []
    }, {
        "txid": "6312095baa2a42dec6aa41888c01fd74ecce2ab139c17a0a6d01d42a89b67953",
        "vout": 1,
        "address": "bc1phyrmjs2jm5c98tldke2ykp0h66lsx3wy0ey8ug2fjj5mxsn8ftqsa24un8",
        "script_pubkey": "00140df0d276eacd58b09a74d5e0288756e8505787f5",
        "satoshis": "546",
        "confirmations": 20,
        "height": 2815500,
        "runes": [{
            "rune_id": "840000:1",
            "amount": "1.00"
        }],
        "inscriptions": [{
            "offset": 0,
            "inscription_id": "47bb5438d366863b25b4b1782af0d0cf0a89a922adce5da81253790d3e651501i0"
        }]
    }],
    "last_updated": {
        "block_hash": "0000000088bee3b517745636443c8f11aff45209449300a5d8461c336e467925",
        "block_height": 2815520
    },
    "next_cursor": "AAAAAAAq9gxgU3m2iSrUAW0KesE5sSrO7HT9AYyIQarG3kIqqlsJEmNgAAAAAQ"
}"##;
