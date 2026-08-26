use std::collections::HashMap;

use axum::{extract::Path, response::IntoResponse, Extension, Json};
use bitcoin::{hashes::Hash, Script};
use reqwest::StatusCode;
use std::str::FromStr;
use timbre_xbt::{
    reducers::{
        brc20_balances_by_script_hash::Key as Brc20BalanceKey, brc20_terms_by_ticker,
        reducer_key_range,
    },
    Reducer,
};

use crate::{
    error::Error,
    tikv::{adapter::TiKVAdapter, key_resolver::ReducerType, Scanner},
    types::{inscriptions::Brc20Balances, TimestampedResponse},
    util::decimal,
};

pub static REQUIRED_REDUCERS: &[ReducerType] = &[
    ReducerType::Brc20BalancesByScriptHash,
    ReducerType::Brc20TermsByTicker,
    // parsing address parameter
    ReducerType::ScriptByScriptHash,
    ReducerType::ScriptHashByAddressPayloadHash,
];

#[utoipa::path(
    tag = "Addresses",
    get,
    path = "/addresses/{address}/brc20",
    params(
        ("address" = String, Path, description = "Bitcoin address or hex encoded script pubkey", example="bc1phyrmjs2jm5c98tldke2ykp0h66lsx3wy0ey8ug2fjj5mxsn8ftqsa24un8"),
    ),
    responses(
        (
            status = 200,
            description = "Requested data",
            body = TimestampedBrc20Quantities,
            example = json!(serde_json::Value::from_str(EXAMPLE_RESPONSE).unwrap()),
        ),
        (status = 400, description = "Malformed query parameters"),
        (status = 404, description = "Requested entity not found on-chain"),
        (status = 500, description = "Internal server error"),
    )
)]
#[tracing::instrument(name = "BRC20_BY_ADDRESS", level = "info", skip(tikv))]
/// BRC20 by Address
///
/// Returns a collection of BRC20 tokens associated with the address, showing both the total and available (transferable) balances. This is essential for building BRC20 token wallets and dashboards.
pub async fn brc20_by_address(
    Path(addr_or_pk): Path<String>,
    mut tikv: Extension<TiKVAdapter>,
) -> Result<impl IntoResponse, Error> {
    tikv.init_tip(REQUIRED_REDUCERS).await?;

    let balances_encoder = tikv.get_encoder(ReducerType::Brc20BalancesByScriptHash)?;

    // Parse and try decode user params.
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

    // --- scan total balances by address

    let (total_range_lower, total_range_upper) = reducer_key_range(
        &balances_encoder.namespace(),
        &Reducer::Brc20TotalBalanceByScriptHash,
        &Some(script_hash.to_byte_array()),
        None::<u64>,
        None::<u64>,
    );

    let total_kvs = Scanner::new(total_range_lower..total_range_upper)
        .execute::<Brc20BalanceKey, u128>(&mut tikv, ReducerType::Brc20BalancesByScriptHash)
        .await?;

    let (available_range_lower, available_range_upper) = reducer_key_range(
        &balances_encoder.namespace(),
        &Reducer::Brc20AvailableBalanceByScriptHash,
        &Some(script_hash.to_byte_array()),
        None::<u64>,
        None::<u64>,
    );

    let available_kvs = Scanner::new(available_range_lower..available_range_upper)
        .execute::<Brc20BalanceKey, u128>(&mut tikv, ReducerType::Brc20BalancesByScriptHash)
        .await?;

    if total_kvs.len() != available_kvs.len() {
        return Err(Error::Internal("brc20 balance len mismatch".into()));
    }

    // --- process fetched kvs

    let mut brc20_balances = HashMap::new();

    for ((total_k, total_v), (available_k, available_v)) in total_kvs.into_iter().zip(available_kvs)
    {
        if total_k.ticker.0 != available_k.ticker.0 {
            return Err(Error::Internal("brc20 balance ticker mismatch".into()));
        }

        // omit tokens with both 0 amounts
        if total_v == 0 && available_v == 0 {
            continue;
        };

        let dec = tikv
            .get_reducer_key::<_, brc20_terms_by_ticker::Value>(
                (ReducerType::Brc20TermsByTicker, Reducer::Brc20TermsByTicker),
                &brc20_terms_by_ticker::Key {
                    ticker: total_k.ticker.clone(),
                },
            )
            .await?
            .dec as usize;

        brc20_balances.insert(
            String::from_utf8_lossy(&total_k.ticker.0).to_string(),
            Brc20Balances {
                total: decimal(total_v, dec),
                available: decimal(available_v, dec),
            },
        );
    }

    let out = TimestampedResponse {
        data: brc20_balances,
        last_updated: tikv.get_snapshot_point()?,
    };

    Ok((StatusCode::OK, Json(out)))
}

static EXAMPLE_RESPONSE: &str = r##"{
    "data": {
        "ABCD": {
            "total": "312000.000",
            "available": "0.123"
        }
    },
    "last_updated": {
        "block_hash": "000000009ed3f5385c1807ca04630b9b2273398670726f93282fd41ba88dc6b8",
        "block_height": 2413542
    }
}"##;
