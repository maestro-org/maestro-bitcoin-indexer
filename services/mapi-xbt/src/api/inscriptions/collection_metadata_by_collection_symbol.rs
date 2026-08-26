use axum::{extract::Path, response::IntoResponse, Extension, Json};
use reqwest::StatusCode;
use std::str::FromStr;
use timbre_xbt::{CollectionIngestor, ShortByteString};

use crate::{
    error::Error,
    tikv::{adapter::TiKVAdapter, key_resolver::ReducerType},
    types::{collections::CollectionMetadata, TimestampedResponse},
};

pub static REQUIRED_REDUCERS: &[ReducerType] = &[ReducerType::ContentByInscriptionId];

#[utoipa::path(
    tag = "Inscriptions",
    get,
    path = "/assets/collections/{collection_symbol}/metadata",
    params(("collection_symbol" = String, Path, description = "Collection symbol (UTF-8)", example="twick")),
    responses(
        (
            status = 200,
            description = "Requested data",
            body = TimestampedCollectionMetadataBySymbol,
            example = json!(serde_json::Value::from_str(EXAMPLE_RESPONSE).unwrap())
        ),
        (status = 400, description = "Malformed query parameters"),
        (status = 404, description = "Requested entity not found on-chain"),
        (status = 500, description = "Internal server error"),
    )
)]
#[tracing::instrument(
    name = "COLLECTION_METADATA_BY_COLLECTION_SYMBOL",
    level = "info",
    skip(tikv)
)]
/// Collection Metadata by Collection Symbol
///
/// Provides metadata for a given inscription collection symbol, including its name, image, supply, and external links. This is useful for rendering collection summaries or for display in marketplaces and aggregators.
pub async fn collection_metadata_by_collection_symbol(
    Path(collection_symbol): Path<String>,
    mut tikv: Extension<TiKVAdapter>,
) -> Result<impl IntoResponse, Error> {
    tikv.init_tip(REQUIRED_REDUCERS).await?;

    tikv.with_collection_metadata().await?;

    // ---

    let maybe_collection_metadata = tikv
        .get_collection_key_maybe::<ShortByteString>(
            &CollectionIngestor::MetadataBySymbol,
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

    let data: CollectionMetadata = serde_json::from_slice(&json_bytes)?;

    let out = TimestampedResponse {
        data,
        last_updated: tikv.get_snapshot_point()?,
    };

    Ok((StatusCode::OK, Json(out)))
}

static EXAMPLE_RESPONSE: &str = r##"{
    "data": {
        "symbol": "twick",
        "name": "1/1 ART BY TWICK",
        "imageURI": "https://creator-hub-prod.s3.us-east-2.amazonaws.com/ord-twick_pfp_1705357607896.jpeg",
        "chain": "btc",
        "inscriptionIcon": "736d7692fb2b405f7efdd05b57787a10bb0385b7dd394aa67654d62119e9d825i0",
        "description": "A collection of 1/1 art inscribed forever on the Bitcoin Blockchain. All 1/1s are inscribed on special sats.",
        "supply": 10,
        "twitterLink": "https://twitter.com/Twickert_",
        "discordLink": "",
        "websiteLink": "https://www.artbytwick.com/",
        "min_inscription_number": "31921195",
        "max_inscription_number": "63111093",
        "createdAt": "Sun, 17 Sep 2023 06:38:15 GMT",
        "labels": []
    },
    "last_updated": {
        "block_hash": "00000000000000000001998e2059bcbb25f76fd0ef39db8ddfc5c31c5ea95f1f",
        "block_height": 876644
    }
}"##;
