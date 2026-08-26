use axum::{extract::Path, response::IntoResponse, Extension, Json};
use reqwest::StatusCode;
use std::str::FromStr;
use timbre_xbt::{CollectionIngestor, ShortByteString};

use crate::{
    error::Error,
    tikv::{adapter::TiKVAdapter, key_resolver::ReducerType},
    types::{collections::CollectionStats, TimestampedResponse},
};

pub static REQUIRED_REDUCERS: &[ReducerType] = &[ReducerType::ContentByInscriptionId];

#[utoipa::path(
    tag = "Inscriptions",
    get,
    path = "/assets/collections/{collection_symbol}/stats",
    params(("collection_symbol" = String, Path, description = "Collection symbol (UTF-8)", example="twick")),
    responses(
        (
            status = 200,
            description = "Requested data",
            body = TimestampedCollectionStatsBySymbol,
            example = json!(serde_json::Value::from_str(EXAMPLE_RESPONSE).unwrap())
        ),
        (status = 400, description = "Malformed query parameters"),
        (status = 404, description = "Requested entity not found on-chain"),
        (status = 500, description = "Internal server error"),
    )
)]
#[tracing::instrument(
    name = "COLLECTION_STATS_BY_COLLECTION_SYMBOL",
    level = "info",
    skip(tikv)
)]
/// Collection Stats by Collection Symbol
///
/// Provides stats for a given inscription: total volume (in sats), floor price (in sats), and total listed. This is useful for rendering collection summaries or for display in marketplaces and aggregators.
pub async fn collection_stats_by_collection_symbol(
    Path(collection_symbol): Path<String>,
    mut tikv: Extension<TiKVAdapter>,
) -> Result<impl IntoResponse, Error> {
    tikv.init_tip(REQUIRED_REDUCERS).await?;

    tikv.with_collection_metadata().await?;

    // ---

    let maybe_collection_metadata = tikv
        .get_collection_key_maybe::<ShortByteString>(
            &CollectionIngestor::StatsBySymbol,
            &ShortByteString(collection_symbol.as_bytes().to_vec()),
        )
        .await?;

    let json_bytes = match maybe_collection_metadata {
        Some(collection_metadata) => collection_metadata,
        None => {
            // --- collection doesn't exist
            return Err(Error::NotFound);
        }
    };

    let data: CollectionStats = serde_json::from_slice(&json_bytes).unwrap();

    let out = TimestampedResponse {
        data,
        last_updated: tikv.get_snapshot_point()?,
    };

    Ok((StatusCode::OK, Json(out)))
}

static EXAMPLE_RESPONSE: &str = r##"{
    "data": {
        "totalVolume": "123456789",
        "owners": "42",
        "supply": "1000",
        "floorPrice": "0.05",
        "totalListed": "150",
        "pendingTransactions": "3",
        "inscriptionNumberMin": "100000",
        "inscriptionNumberMax": "200000",
        "symbol": "twick"
    },
    "last_updated": {
        "block_hash": "00000000000000000001998e2059bcbb25f76fd0ef39db8ddfc5c31c5ea95f1f",
        "block_height": 876644
    }
}"##;
