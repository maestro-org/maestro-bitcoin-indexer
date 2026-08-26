use std::collections::HashMap;

use axum::{
    extract::{Path, Query},
    response::IntoResponse,
    Extension, Json,
};
use bitcoin::{hashes::Hash, Script};
use reqwest::StatusCode;
use serde::Deserialize;
use std::str::FromStr;
use timbre_xbt::{
    reducers::{
        etching_by_rune_id, reducer_key_range,
        rune_utxos_by_script_hash::{
            Key as RuneUtxosByScriptHashKey, Value as RuneUtxosByScriptHashValue,
        },
    },
    Reducer,
};

use crate::{
    error::Error,
    tikv::{adapter::TiKVAdapter, key_resolver::ReducerType, Scanner},
    types::{runes::RuneInfoBrief, TimestampedResponse},
    util::{decimal, fetch_rune_info_brief},
};

#[derive(Debug, Deserialize)]
pub struct Params {
    pub include_info: Option<bool>,
}

pub static REQUIRED_REDUCERS: &[ReducerType] = &[
    ReducerType::RuneUtxosByScriptHash,
    ReducerType::EtchingByRuneId,
    ReducerType::BalancesByRuneId,
    ReducerType::MintsByRuneId,
    // parsing address parameter
    ReducerType::ScriptByScriptHash,
    ReducerType::ScriptHashByAddressPayloadHash,
];

#[utoipa::path(
    tag = "Addresses",
    get,
    path = "/addresses/{address}/runes",
    params(
        ("address" = String, Path, description = "Bitcoin address or hex encoded script pubkey", example="bc1phyrmjs2jm5c98tldke2ykp0h66lsx3wy0ey8ug2fjj5mxsn8ftqsa24un8"),
        ("include_info" = Option<bool>, Query, description = "Include full rune information for each rune (default: false)"),
    ),
    responses(
        (
            status = 200,
            description = "Requested data",
            body = TimestampedRuneQuantities,
            example = json!(serde_json::Value::from_str(EXAMPLE_RESPONSE).unwrap())
        ),
        (status = 400, description = "Malformed query parameters"),
        (status = 404, description = "Requested entity not found on-chain"),
        (status = 500, description = "Internal server error"),
    )
)]
#[tracing::instrument(name = "RUNES_BY_ADDRESS", level = "info", skip(tikv))]
/// Runes by Address
///
/// Provides a list of all Rune assets held by the specified address. It returns both total and available balances, allowing for token inventory management and portfolio tracking.
pub async fn runes_by_address(
    Path(addr_or_pk): Path<String>,
    params: Query<Params>,
    mut tikv: Extension<TiKVAdapter>,
) -> Result<impl IntoResponse, Error> {
    // TODO: RuneBalancesByScriptHash?
    tikv.init_tip(REQUIRED_REDUCERS).await?;

    let utxos_encoder = tikv.get_encoder(ReducerType::RuneUtxosByScriptHash)?;

    // -- parse and try decode user params
    let script_bytes = match tikv.parse_address_or_script_bytes(&addr_or_pk).await {
        Ok((_, script_bytes)) => script_bytes,
        Err(Error::NotFound) => {
            // User param is a Bitcoin address, but the corresponding script pub key could not be
            // found in store.
            let out = TimestampedResponse {
                data: HashMap::new(),
                last_updated: tikv.get_snapshot_point()?,
            };

            return Ok((StatusCode::OK, Json(out)));
        }
        Err(e) => return Err(e),
    };

    let script = Script::from_bytes(&script_bytes);
    let script_hash = script.script_hash();

    // --- scan utxos for address

    let (utxos_range_lower, utxos_range_upper) = reducer_key_range(
        utxos_encoder.namespace(),
        &Reducer::RuneUtxosByScriptHash,
        &Some(script_hash.to_byte_array()),
        None::<u64>,
        None::<u64>,
    );

    let kvs = Scanner::new(utxos_range_lower..utxos_range_upper)
        .execute::<RuneUtxosByScriptHashKey, RuneUtxosByScriptHashValue>(
            &mut tikv,
            ReducerType::RuneUtxosByScriptHash,
        )
        .await?;

    // --- process fetched kvs

    let include_info = params.include_info.unwrap_or(false);
    let mut rune_balances: HashMap<(u64, u32), u128> = HashMap::new();
    let mut rune_info_cache: HashMap<(u64, u32), RuneInfoBrief> = HashMap::new();

    for (_, v) in kvs {
        for (rune_id, amount) in v.runes {
            rune_balances
                .entry(rune_id)
                .and_modify(|x| *x += amount)
                .or_insert(amount);

            // Fetch rune info immediately when first encountered if include_info is true
            if include_info && !rune_info_cache.contains_key(&rune_id) {
                let rune_info = fetch_rune_info_brief(rune_id, &mut tikv).await?;
                rune_info_cache.insert(rune_id, rune_info);
            }
        }
    }

    // Sort runes for consistent ordering
    let mut rune_balances = rune_balances.into_iter().collect::<Vec<_>>();
    rune_balances.sort_by_key(|(rid, _)| *rid);

    let mut result_data = HashMap::new();

    // Process each rune with or without info
    for (rune_id, balance) in rune_balances {
        if include_info {
            let rune_info = rune_info_cache.get(&rune_id).unwrap();
            let dec = rune_info.divisibility as usize;

            result_data.insert(
                format!("{}:{}", rune_id.0, rune_id.1),
                serde_json::json!({
                    "balance": decimal(balance, dec),
                    "info": rune_info
                }),
            );
        } else {
            let dec = tikv
                .get_reducer_key::<_, etching_by_rune_id::Value>(
                    (ReducerType::EtchingByRuneId, Reducer::EtchingByRuneId),
                    &etching_by_rune_id::Key { rune_id },
                )
                .await?
                .divisibility
                .unwrap_or(0) as usize;

            result_data.insert(
                format!("{}:{}", rune_id.0, rune_id.1),
                serde_json::json!(decimal(balance, dec)),
            );
        }
    }

    let out = TimestampedResponse {
        data: result_data,
        last_updated: tikv.get_snapshot_point()?,
    };

    Ok((StatusCode::OK, Json(out)))
}

static EXAMPLE_RESPONSE: &str = r##"{
    "data": {
        "840000:1": {
            "balance": "1111.00",
            "info": {
                "id": "840000:1",
                "etching_cenotaph": false,
                "etching_tx": "2bb85f4b004be6da54f766c17c1e855187327112c231ef2ff35ebad0ea67c69e",
                "etching_height": 840000,
                "name": "ZZZZZFEHUZZZZZ",
                "spaced_name": "Z•Z•Z•Z•Z•FEHU•Z•Z•Z•Z•Z",
                "symbol": "ᚠ",
                "divisibility": 2,
                "premine": "110000000.00",
                "terms": {
                    "mint_txs_cap": "1111111",
                    "amount_per_mint": "1.00",
                    "start_height": null,
                    "end_height": null,
                    "start_offset": null,
                    "end_offset": null
                }
            }
        }
    },
    "last_updated": {
        "block_hash": "0000000088bee3b517745636443c8f11aff45209449300a5d8461c336e467925",
        "block_height": 2815520
    }
}"##;
