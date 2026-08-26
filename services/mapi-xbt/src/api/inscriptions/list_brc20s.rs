use std::str::from_utf8;
use std::str::FromStr;

use axum::{extract::Query, response::IntoResponse, Extension, Json};
use reqwest::StatusCode;
use timbre_xbt::{
    reducers::brc20_terms_by_ticker::{Key as Brc20TermsKey, Value as Brc20TermsValue},
    Encode, Reducer, ShortByteString,
};

use crate::{
    error::Error,
    tikv::{adapter::TiKVAdapter, key_resolver::ReducerType, Scanner},
    types::{CountParam, CursorPaginationParams, PaginatedResponse},
    util::ParsedPaginationParams,
};

pub static REQUIRED_REDUCERS: &[ReducerType] = &[ReducerType::Brc20TermsByTicker];

#[utoipa::path(
    tag = "BRC20",
    get,
    path = "/assets/brc20",
    params(
        ("count" = inline(Option<CountParam>), Query, description = "The max number of results per page"),
        ("cursor" = inline(Option<String>), Query, description = "Pagination cursor string, use the cursor included in a page of results to fetch the next page"),
    ),
    responses(
        (
            status = 200,
            description = "Requested data",
            body = PaginatedBrc20Ticker,
            example = json!(serde_json::Value::from_str(EXAMPLE_RESPONSE).unwrap())
        ),
        (status = 400, description = "Malformed query parameters"),
        (status = 404, description = "Requested entity not found on-chain"),
        (status = 500, description = "Internal server error"),
    )
)]
#[tracing::instrument(name = "LIST_BRC20", level = "info", skip(tikv))]
/// List BRC20
///
/// Retrieves a list of tickers of all deployed BRC20 assets.
pub async fn list_brc20s(
    page_params: Query<CursorPaginationParams>,
    mut tikv: Extension<TiKVAdapter>,
) -> Result<impl IntoResponse, Error> {
    tikv.init_tip(REQUIRED_REDUCERS).await?;

    let terms_encoder = tikv.get_encoder(ReducerType::Brc20TermsByTicker)?;

    // --- initialise `next_cursor`

    let mut next_cursor = None;

    // --- scan total balances by address

    let page_params = ParsedPaginationParams::parse_no_height::<_, ShortByteString>(
        page_params.0,
        &terms_encoder,
        &Reducer::Brc20TermsByTicker,
        None::<u64>,
    )?;

    let kvs = Scanner::new(page_params.key_range())
        .count(page_params.count() + 1)
        .execute::<Brc20TermsKey, Brc20TermsValue>(&mut tikv, ReducerType::Brc20TermsByTicker)
        .await?;

    // --- process fetched kvs

    let mut kvs = kvs.into_iter().enumerate();

    let mut tickers: Vec<String> = Vec::new();

    // TODO: cleaner
    while let Some((i, (key, _))) = kvs.next() {
        // if this is the last result of the page, check if there is a subsequent
        // result (and therefore we need to return a cursor for next page)
        if i == (page_params.count() - 1) && kvs.next().is_some() {
            // TODO: timbre Cursor::from(key)
            next_cursor = Some(key.ticker.encode_base64());
        };

        tickers.push(from_utf8(&key.ticker.0).unwrap().to_string())
    }

    let out = PaginatedResponse {
        data: tickers,
        last_updated: tikv.get_snapshot_point()?,
        next_cursor,
    };

    Ok((StatusCode::OK, Json(out)))
}

static EXAMPLE_RESPONSE: &str = r##"{
    "data": ["aosi", "ap'q", "fctb"],
    "last_updated": {
        "block_hash": "0000000000000000000235c5edb5c89ef52715452a2aca610949194b3361ef7d",
        "block_height": 850368
    },
    "next_cursor": "BGZjdGI"
}"##;
