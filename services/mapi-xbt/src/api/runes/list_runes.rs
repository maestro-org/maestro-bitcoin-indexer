use axum::{extract::Query, response::IntoResponse, Extension, Json};
use ordinals::{Rune, SpacedRune};
use reqwest::StatusCode;
use std::str::FromStr;
use timbre_xbt::{
    reducers::etching_by_rune_id::{Key as EtchingKey, Value as EtchingValue},
    Encode, Reducer,
};

use crate::{
    error::Error,
    tikv::{adapter::TiKVAdapter, key_resolver::ReducerType, Scanner},
    timer::Timer,
    types::{runes::RuneIdAndName, CountParam, CursorPaginationParams, PaginatedResponse},
    util::ParsedPaginationParams,
};

pub static REQUIRED_REDUCERS: &[ReducerType] = &[ReducerType::EtchingByRuneId];

#[utoipa::path(
    tag = "Runes",
    get,
    path = "/assets/runes",
    params(
        ("count" = inline(Option<CountParam>), Query, description = "The max number of results per page"),
        ("cursor" = inline(Option<String>), Query, description = "Pagination cursor string, use the cursor included in a page of results to fetch the next page"),
    ),
    responses(
        (
            status = 200,
            description = "Requested data",
            body = PaginatedRuneIdAndName,
            example = json!(serde_json::Value::from_str(EXAMPLE_RESPONSE).unwrap())
        ),
        (status = 400, description = "Malformed query parameters"),
        (status = 404, description = "Requested entity not found on-chain"),
        (status = 500, description = "Internal server error"),
    )
)]
#[tracing::instrument(name = "LIST_RUNES", level = "info", skip(tikv))]
/// List Runes
///
/// Lists all Rune tokens deployed, including names and IDs.
pub async fn list_runes(
    page_params: Query<CursorPaginationParams>,
    mut tikv: Extension<TiKVAdapter>,
) -> Result<impl IntoResponse, Error> {
    let mut timer = Timer::new();

    // ---

    tikv.init_tip(REQUIRED_REDUCERS).await?;

    let etchings_encoder = tikv.get_encoder(ReducerType::EtchingByRuneId)?;

    timer.checkpoint("initialise");

    // --- initialise `next_cursor`

    let mut next_cursor = None;

    // --- scan total balances by address

    let page_params = ParsedPaginationParams::parse_no_height::<_, EtchingKey>(
        page_params.0,
        &etchings_encoder,
        &Reducer::EtchingByRuneId,
        None::<u64>,
    )?;

    let kvs = Scanner::new(page_params.key_range())
        .count(page_params.count() + 1)
        .execute::<EtchingKey, EtchingValue>(&mut tikv, ReducerType::EtchingByRuneId)
        .await?;

    timer.checkpoint("fetch");

    // --- process fetched kvs

    let mut kvs = kvs.into_iter().enumerate();

    let mut tickers: Vec<RuneIdAndName> = Vec::new();

    // TODO: cleaner
    while let Some((i, (key, info))) = kvs.next() {
        // if this is the last result of the page, check if there is a subsequent
        // result (and therefore we need to return a cursor for next page)
        if i == (page_params.count() - 1) && kvs.next().is_some() {
            // TODO: timbre Cursor::from(key)
            next_cursor = Some(key.encode_base64());
        };

        let rune = info
            .name
            .map(|x| Rune(x))
            .unwrap_or(Rune::reserved(key.rune_id.0, key.rune_id.1));

        let spaced_name = SpacedRune {
            rune,
            spacers: info.spacers.unwrap_or(0),
        }
        .to_string();

        tickers.push(RuneIdAndName {
            id: format!("{}:{}", key.rune_id.0, key.rune_id.1),
            spaced_name,
        });
    }

    timer.checkpoint("process");
    timer.finish();

    // ---

    let out = PaginatedResponse {
        data: tickers,
        last_updated: tikv.get_snapshot_point()?,
        next_cursor,
    };

    Ok((StatusCode::OK, Json(out)))
}

static EXAMPLE_RESPONSE: &str = r##"{
    "data": [{
        "id": "840000:1",
        "spaced_name": "Z•Z•Z•Z•Z•FEHU•Z•Z•Z•Z•Z"
    }, {
        "id": "840000:2",
        "spaced_name": "DECENTRALIZED"
    }, {
        "id": "840000:3",
        "spaced_name": "DOG•GO•TO•THE•MOON"
    }],
    "last_updated": {
        "block_hash": "00000000000000000002c0cc73626b56fb3ee1ce605b0ce125cc4fb58775a0a9",
        "block_height": 840002
    },
    "next_cursor": "AAAAAAAM0UAAAAAD"
}"##;
