use axum::{extract::Path, response::IntoResponse, Extension, Json};
use reqwest::StatusCode;
use std::str::FromStr;
use timbre_xbt::{
    reducers::etching_by_rune_id::{Key, Value},
    Reducer,
};

use crate::{
    error::Error,
    tikv::{adapter::TiKVAdapter, key_resolver::ReducerType},
    types::TimestampedResponse,
    util::{fetch_rune_info, RuneIdentifier},
};

pub static REQUIRED_REDUCERS: &[ReducerType] = &[
    ReducerType::BalancesByRuneId,
    ReducerType::EtchingByRuneId,
    ReducerType::RuneIdByRuneName,
    ReducerType::MintsByRuneId,
];

#[utoipa::path(
    tag = "Runes",
    get,
    path = "/assets/runes/{rune}",
    params(
        ("rune" = String, Path, description = "Rune, specified either by the Rune ID (etching block number and transaction index) or name (spaced or un-spaced)", example="2519999:31"),
    ),
    responses(
        (
            status = 200,
            description = "Requested data",
            body = TimestampedRuneInfo,
            example = json!(serde_json::Value::from_str(EXAMPLE_RESPONSE).unwrap())
        ),
        (status = 400, description = "Malformed query parameters"),
        (status = 404, description = "Requested entity not found on-chain"),
        (status = 500, description = "Internal server error"),
    )
)]
#[tracing::instrument(name = "INFO_BY_RUNE", level = "info", skip(tikv))]
/// Runes Info
///
/// Returns full details for a specific Rune token, such as its etching (origin) transaction, supply, number of holders.
pub async fn info_by_rune(
    Path(rune_id): Path<String>,
    mut tikv: Extension<TiKVAdapter>,
) -> Result<impl IntoResponse, Error> {
    tikv.init_tip(REQUIRED_REDUCERS).await?;

    // -- parse and try decode user params

    let rune_id = match RuneIdentifier::parse(rune_id)? {
        RuneIdentifier::Id(x) => x,
        RuneIdentifier::Name(n) => tikv
            .resolve_rune_name(n)
            .await?
            .ok_or_else(|| Error::NotFound)?,
    };

    // Check if rune exists
    tikv.get_reducer_key_maybe::<_, Value>(
        (ReducerType::EtchingByRuneId, Reducer::EtchingByRuneId),
        &Key { rune_id },
    )
    .await?
    .ok_or_else(|| Error::NotFound)?;

    // --- fetch data using shared utility

    let out = fetch_rune_info(rune_id, &mut tikv).await?;

    let out = TimestampedResponse {
        data: out,
        last_updated: tikv.get_snapshot_point()?,
    };

    Ok((StatusCode::OK, Json(out)))
}

static EXAMPLE_RESPONSE: &str = r##"{
    "data": {
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
        },
        "max_supply": "111111111.00",
        "circulating_supply": "5226.00",
        "mints": 5226,
        "unique_holders": 1568
    },
    "last_updated": {
        "block_hash": "00000000000000000001332b3017e2b72bdd063145bbf808b3c1722a0fd60859",
        "block_height": 840051
    }
}"##;
