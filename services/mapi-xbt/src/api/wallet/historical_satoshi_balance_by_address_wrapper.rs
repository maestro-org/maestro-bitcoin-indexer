use crate::{
    api::addresses::historical_satoshi_balance_by_address::{
        historical_satoshi_balance_by_address as inner, HeightOrTimestampParam, EXAMPLE_RESPONSE,
    },
    tikv::adapter::TiKVAdapter,
    types::{CountParam, HeightPaginationParams, OrderParam},
};
use axum::{
    extract::{Path, Query},
    response::IntoResponse,
    Extension,
};
use std::str::FromStr;

use crate::{error::Error, options::arranger::Arranger};

#[utoipa::path(
    tag = "Addresses",
    get,
    path = "/wallet/addresses/{address}/balance/historical",
    params(
        ("address" = String, Path, description = "Bitcoin address or hex encoded script pubkey", example="bc1phyrmjs2jm5c98tldke2ykp0h66lsx3wy0ey8ug2fjj5mxsn8ftqsa24un8"),

        // Pagination params applicable regardless of the sorting order and property.
        ("order" = inline(Option<OrderParam>), Query, description = "The order in which the results are sorted. Supported values: asc, desc"),
        ("count" = inline(Option<CountParam>), Query, description = "The max number of results per page"),
        ("from" = inline(Option<u64>), Query, description = "Return only blocks included on or after a specific height or timestamps. If this parameter is not provided, the starting point will be the first block where the address has seen its balance increase or decrease."),
        ("to" = inline(Option<u64>), Query, description = "Return only blocks included on or before a specific height or timestamp"),
        ("cursor" = inline(Option<String>), Query, description = "Pagination cursor string, use the cursor included in a page of results to fetch the next page"),

        // How to read from and to query params.
        ("height_params" = inline(Option<bool>), Query, description = "Whether the from and to integer query params should be read as timestamps or as block heights. True (the default) means from and to params should be read as block heights."),
    ),
    responses(
        (
            status = 200,
            description = "Requested data",
            body = PaginatedHistoricalSatBalanceByAddress,
            example = json!(serde_json::Value::from_str(EXAMPLE_RESPONSE).unwrap())
        ),
        (status = 400, description = "Malformed query parameters"),
        (status = 404, description = "Requested entity not found on-chain"),
        (status = 500, description = "Internal server error"),
    )
)]
#[tracing::instrument(
    name = "WALLET_HISTORICAL_SATOSHI_BALANCE_BY_ADDRESS",
    level = "info",
    skip(tikv, arranger)
)]
/// Historical Satoshi Balance by Address
///
/// Returns the historical satoshi balances, itemized by block and including USD price.
pub async fn wallet_historical_satoshi_balance_by_address(
    addr_or_pk: Path<String>,
    page_params: Query<HeightPaginationParams>,
    param_type: Query<HeightOrTimestampParam>,
    tikv: Extension<TiKVAdapter>,
    arranger: Extension<Arranger>,
) -> Result<impl IntoResponse, Error> {
    inner(addr_or_pk, page_params, param_type, tikv, arranger).await
}
