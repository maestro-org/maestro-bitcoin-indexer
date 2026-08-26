use crate::{
    tikv::{adapter::TiKVAdapter, key_resolver::ReducerType},
    timer::Timer,
    types::TimestampedResponse,
};
use axum::{extract::Path, response::IntoResponse, Extension, Json};
use bitcoin::{hashes::Hash, Script};
use reqwest::StatusCode;
use std::str::FromStr;
use timbre_xbt::{
    reducers::sat_balance_by_script_hash::{
        Key as SatBalanceByScriptHashKey, Value as SatBalanceByScriptHashValue,
    },
    Reducer,
};

use crate::error::Error;

pub static REQUIRED_REDUCERS: &[ReducerType] = &[
    ReducerType::UtxosByScriptHash,
    ReducerType::SatBalanceByScriptHash,
    // parsing address parameter
    ReducerType::ScriptByScriptHash,
    ReducerType::ScriptHashByAddressPayloadHash,
];

#[utoipa::path(
    tag = "Addresses",
    get,
    path = "/addresses/{address}/balance",
    params(
        ("address" = String, Path, description = "Bitcoin address or hex encoded script pubkey", example="bc1phyrmjs2jm5c98tldke2ykp0h66lsx3wy0ey8ug2fjj5mxsn8ftqsa24un8"),
    ),
    responses(
        (
            status = 200,
            description = "Requested data",
            body = TimestampedSatoshis,
            example = json!(serde_json::Value::from_str(EXAMPLE_RESPONSE).unwrap())
        ),
        (status = 400, description = "Malformed query parameters"),
        (status = 404, description = "Requested entity not found on-chain"),
        (status = 500, description = "Internal server error"),
    )
)]
#[tracing::instrument(name = "SATOSHI_BALANCE_BY_ADDRESS", level = "info", skip(tikv))]
/// Satoshi Balance by Address
///
/// Returns the total balance in satoshis held at the specified address or script pubkey by summing all unspent outputs (UTXOs). This is a direct snapshot of the address's spendable funds and does not include mempool transactions.
pub async fn satoshi_balance_by_address(
    Path(addr_or_pk): Path<String>,
    mut tikv: Extension<TiKVAdapter>,
) -> Result<impl IntoResponse, Error> {
    let mut timer = Timer::new();

    tikv.init_tip(REQUIRED_REDUCERS).await?;

    let last_updated = tikv.get_snapshot_point()?;

    timer.checkpoint("initialise tikv adapter");

    // --- parse and try to decode address
    let script_bytes = match tikv.parse_address_or_script_bytes(&addr_or_pk).await {
        Ok((_, script_bytes)) => script_bytes,
        Err(Error::NotFound) => {
            // User param is a Bitcoin address, but the corresponding script pub key could not be
            // found in store.
            let out = TimestampedResponse {
                data: 0.to_string(),
                last_updated,
            };

            return Ok((StatusCode::OK, Json(out)));
        }
        Err(e) => return Err(e),
    };
    let script = Script::from_bytes(&script_bytes);
    let script_hash = script.script_hash();

    timer.checkpoint("parse params");

    // --- scan keys
    let satoshis = tikv
        .get_reducer_key_maybe::<SatBalanceByScriptHashKey, SatBalanceByScriptHashValue>(
            (
                ReducerType::SatBalanceByScriptHash,
                Reducer::SatBalanceByScriptHash,
            ),
            &SatBalanceByScriptHashKey {
                script_hash: script_hash.to_byte_array(),
            },
        )
        .await?
        .map(|value| value.satoshis)
        .unwrap_or(0);

    timer.checkpoint("fetch kvs");

    // --- process fetched kvs

    timer.checkpoint("process kvs");

    timer.finish();

    let out = TimestampedResponse {
        data: satoshis.to_string(),
        last_updated,
    };

    Ok((StatusCode::OK, Json(out)))
}

static EXAMPLE_RESPONSE: &str = r##"{
    "data": "695100",
    "last_updated": {
        "block_hash": "000000000000000000005075404edd6edc806976389a9f7e2ff71db1c2cf9b83",
        "block_height": 884991
    }
}"##;
