use crate::{
    error::Error,
    options::Mode,
    tikv::{adapter::TiKVAdapter, key_resolver::ReducerType, Scanner},
    types::{runes::RuneHolder, CountParam, CursorPaginationParams, PaginatedResponse},
    util::{decimal, ParsedPaginationParams, RuneIdentifier},
};
use axum::{
    extract::{Path, Query},
    response::IntoResponse,
    Extension, Json,
};
use reqwest::StatusCode;
use std::str::FromStr;
use timbre_xbt::{
    reducers::{balances_by_rune_id::Key as BalancesByRuneIdKey, etching_by_rune_id},
    Encode, Reducer,
};

pub static REQUIRED_REDUCERS: &[ReducerType] = &[
    ReducerType::BalancesByRuneId,
    ReducerType::EtchingByRuneId,
    ReducerType::RuneIdByRuneName,
    // resolving script hashes
    ReducerType::ScriptByScriptHash,
];

#[utoipa::path(
    tag = "Runes",
    get,
    path = "/assets/runes/{rune}/holders",
    params(
        ("rune" = String, Path, description = "Rune, specified either by the Rune ID (etching block number and transaction index) or name (spaced or un-spaced)", example="2519999:31"),
        ("count" = inline(Option<CountParam>), Query, description = "The max number of results per page"),
        ("cursor" = inline(Option<String>), Query, description = "Pagination cursor string, use the cursor included in a page of results to fetch the next page"),
    ),
    responses(
        (
            status = 200,
            description = "Requested data",
            body = PaginatedRuneHolder,
            example = json!(serde_json::Value::from_str(EXAMPLE_RESPONSE).unwrap())
        ),
        (status = 400, description = "Malformed query parameters"),
        (status = 404, description = "Requested entity not found on-chain"),
        (status = 500, description = "Internal server error"),
    )
)]
#[tracing::instrument(name = "HOLDERS_BY_RUNE", level = "info", skip(tikv, mode))]
/// Holders by Rune
///
/// Lists all addresses currently holding a given Rune, with corresponding balances. Helps visualize token distribution and adoption.
pub async fn holders_by_rune(
    page_params: Query<CursorPaginationParams>,
    Path(rune_id): Path<String>,
    mut tikv: Extension<TiKVAdapter>,
    mode: Extension<Mode>,
) -> Result<impl IntoResponse, Error> {
    tikv.init_tip(REQUIRED_REDUCERS).await?;

    let balances_encoder = tikv.get_encoder(ReducerType::BalancesByRuneId)?;

    // --- parse rune id

    let rune_id = match RuneIdentifier::parse(rune_id)? {
        RuneIdentifier::Id(id) => id,
        RuneIdentifier::Name(n) => tikv.resolve_rune_name(n).await?.unwrap_or_default(), // return empty vec instead of 404
    };

    // --- initialise `next_cursor`

    let mut next_cursor = None;

    // --- scan total balances by ticker

    let page_params = ParsedPaginationParams::parse_no_height::<_, [u8; 20]>(
        page_params.0,
        &balances_encoder,
        &Reducer::BalancesByRuneId,
        Some(rune_id),
    )?;

    let kvs = Scanner::new(page_params.key_range())
        .count(page_params.count() + 1)
        .execute::<BalancesByRuneIdKey, u128>(&mut tikv, ReducerType::BalancesByRuneId)
        .await?;

    // --- process fetched kvs

    let mut kvs = kvs.into_iter().enumerate();

    let mut holders: Vec<RuneHolder> = Vec::new();

    let dec = tikv
        .get_reducer_key::<_, etching_by_rune_id::Value>(
            (ReducerType::EtchingByRuneId, Reducer::EtchingByRuneId),
            &etching_by_rune_id::Key { rune_id },
        )
        .await?
        .divisibility
        .unwrap_or(0);

    // TODO: cleaner
    while let Some((i, (key, value))) = kvs.next() {
        // if this is the last result of the page, check if there is a subsequent
        // result (and therefore we need to return a cursor for next page)
        if i == (page_params.count() - 1) && kvs.next().is_some() {
            // TODO: timbre Cursor::from(key)
            next_cursor = Some(key.script_hash.encode_base64());
        };

        let (address, script) = tikv.resolve_script_hash(mode.0, key.script_hash).await?;

        holders.push(RuneHolder {
            address: address.map(|x| x.to_string()),
            script_pubkey: hex::encode(script),
            balance: decimal(value, dec as usize),
        })
    }

    let out = PaginatedResponse {
        data: holders,
        last_updated: tikv.get_snapshot_point()?,
        next_cursor,
    };

    Ok((StatusCode::OK, Json(out)))
}

static EXAMPLE_RESPONSE: &str = r##"{
    "data": [{
        "address": "bc1q764zfcx3uw0dcvcdh7nnwm5fvsml0c2tgn942v",
        "script_pubkey": "0014f6aa24e0d1e39edc330dbfa7376e896437f7e14b",
        "balance": "9000000.000000000000000000"
    }, {
        "address": "bc1pqfhj6tlxgpvc72mvn9hh0z666k45fpxgsej92d9q08sfe572mhlqca68ld",
        "script_pubkey": "5120026f2d2fe640598f2b6c996f778b5ad5ab4484c886645534a079e09cd3caddfe",
        "balance": "420000.000000000000000000"
    }],
    "last_updated": {
        "block_hash": "00000000000000000002747b9e3c0097172bc23489d686e8b885a6fa89c2c4da",
        "block_height": 850534
    },
    "next_cursor": "19FwuaejD9hE1R4ckTQKaqe0ecA"
}"##;
