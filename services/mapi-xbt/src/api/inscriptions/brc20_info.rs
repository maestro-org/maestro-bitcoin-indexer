use axum::{extract::Path, response::IntoResponse, Extension, Json};
use bitcoin::{hashes::Hash, Txid};
use reqwest::StatusCode;
use std::str::FromStr;
use timbre_xbt::{
    reducers::{
        balances_by_brc20::Key as Brc20BalanceKey, brc20_terms_by_ticker, reducer_key_range,
    },
    Reducer, ShortByteString,
};

use crate::{
    error::Error,
    tikv::{adapter::TiKVAdapter, key_resolver::ReducerType, Scanner},
    types::{
        inscriptions::{Brc20Info, Brc20Terms},
        TimestampedResponse,
    },
    util::decimal,
};

pub static REQUIRED_REDUCERS: &[ReducerType] = &[
    ReducerType::BalancesByBrc20,
    ReducerType::Brc20TermsByTicker,
];

#[utoipa::path(
    tag = "BRC20",
    get,
    path = "/assets/brc20/{ticker}",
    params(
        ("ticker" = String, Path, description = "BRC20 ticker string", example="FCTB"),
    ),
    responses(
        (
            status = 200,
            description = "Requested data",
            body = TimestampedBrc20Info,
            example = json!(serde_json::Value::from_str(EXAMPLE_RESPONSE).unwrap())
        ),
        (status = 400, description = "Malformed query parameters"),
        (status = 404, description = "Requested entity not found on-chain"),
        (status = 500, description = "Internal server error"),
    )
)]
#[tracing::instrument(name = "BRC20_INFO", level = "info", skip(tikv))]
/// BRC20 Info
///
/// Information about a BRC20 token’s metadata and current state, including its symbol, deployment details, minting rules, total holders, and minted supply.
pub async fn brc20_info(
    Path(ticker): Path<String>,
    mut tikv: Extension<TiKVAdapter>,
) -> Result<impl IntoResponse, Error> {
    tikv.init_tip(REQUIRED_REDUCERS).await?;

    let balances_encoder = tikv.get_encoder(ReducerType::BalancesByBrc20)?;

    // -- parse and try decode user params

    let ticker = ticker.clone();

    let ticker = ticker.to_lowercase();

    // --- fetch terms

    let terms = tikv
        .get_reducer_key_maybe::<_, brc20_terms_by_ticker::Value>(
            (ReducerType::Brc20TermsByTicker, Reducer::Brc20TermsByTicker),
            &brc20_terms_by_ticker::Key {
                ticker: ShortByteString(ticker.as_bytes().to_vec()),
            },
        )
        .await?
        .ok_or_else(|| Error::NotFound)?;

    let dec = terms.dec as usize;

    // --- scan total balances by ticker

    let (balances_range_lower, balances_range_upper) = reducer_key_range(
        &balances_encoder.namespace(),
        &Reducer::BalancesByBrc20,
        &Some(ShortByteString(ticker.as_bytes().to_vec())),
        None::<u64>,
        None::<u64>,
    );

    let kvs = Scanner::new(balances_range_lower..balances_range_upper)
        .execute::<Brc20BalanceKey, u128>(&mut tikv, ReducerType::BalancesByBrc20)
        .await?;

    // --- process fetched kvs

    let holders = kvs.len();
    let minted_supply: u128 = kvs.into_iter().map(|(_, x)| x).sum();

    let deploy_inscription = format!(
        "{}i{}",
        Txid::from_byte_array(terms.deploy_id.0).to_string(),
        terms.deploy_id.1
    );

    let out = TimestampedResponse {
        data: Brc20Info {
            ticker: ticker.clone(),
            ticker_hex: hex::encode(ticker),
            deploy_inscription,
            holders: holders as u64,
            minted_supply: decimal(minted_supply, dec),
            terms: Brc20Terms {
                max: decimal(terms.max, dec),
                limit: decimal(terms.limit, dec),
                dec: terms.dec,
                self_mint: terms.self_mint,
            },
        },
        last_updated: tikv.get_snapshot_point()?,
    };

    Ok((StatusCode::OK, Json(out)))
}

static EXAMPLE_RESPONSE: &str = r##"{
    "data": {
        "ticker": "fctb",
        "ticker_hex": "66637462",
        "deploy_inscription": "3d983a724310f511cfca0031ee2b980b474a0abe1e7e995b7e6d2873e2cbfd5fi0",
        "holders": 7,
        "minted_supply": "19420000.000000000000000000",
        "terms": {
            "max": "21000000.000000000000000000",
            "limit": "1000000.000000000000000000",
            "dec": 18,
            "self_mint": false
        }
    },
    "last_updated": {
        "block_hash": "0000000000000000000214bfa0a73e1cc7663917a933bfb1c66a6613f88dabdd",
        "block_height": 851556
    }
}"##;
