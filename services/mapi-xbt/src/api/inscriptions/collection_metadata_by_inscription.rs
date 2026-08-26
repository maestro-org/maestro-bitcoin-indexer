use axum::{extract::Path, response::IntoResponse, Extension, Json};
use reqwest::StatusCode;
use std::str::FromStr;
use timbre_xbt::CollectionIngestor;

use crate::{
    error::Error,
    tikv::{adapter::TiKVAdapter, key_resolver::ReducerType},
    types::{
        collections::{CollectionMetadata, InscriptionId},
        TimestampedResponse,
    },
    util::parse_inscription_id,
};

pub static REQUIRED_REDUCERS: &[ReducerType] = &[ReducerType::ContentByInscriptionId];

#[utoipa::path(
    tag = "Inscriptions",
    get,
    path = "/assets/inscriptions/{inscription_id}/collection",
    params(("inscription_id" = String, Path, description = "Inscription ID", example="0001a5f7af47c79fa8acfb4fb4d4588d561a86b9f45dd1cf9120663dc74c0a08i0")),
    responses(
        (
            status = 200,
            description = "Requested data",
            body = TimestampedCollectionMetadataByInscription,
            example = json!(serde_json::Value::from_str(EXAMPLE_RESPONSE).unwrap())
        ),
        (status = 400, description = "Malformed query parameters"),
        (status = 404, description = "Requested entity not found on-chain"),
        (status = 500, description = "Internal server error"),
    )
)]
#[tracing::instrument(
    name = "COLLECTION_METADATA_BY_INSCRIPTION",
    level = "info",
    skip(tikv)
)]
/// Collection Metadata by Inscription
///
/// Returns metadata of a collection for a given inscription ID, including its name, image, supply and external links.
pub async fn collection_metadata_by_inscription(
    Path(inscription_id): Path<String>,
    mut tikv: Extension<TiKVAdapter>,
) -> Result<impl IntoResponse, Error> {
    tikv.init_tip(REQUIRED_REDUCERS).await?;

    tikv.with_collection_metadata().await?;

    // --- fetch cursor key for last updated

    let (reveal_tx_hash, inscription_index) = parse_inscription_id(&inscription_id)?;
    let inscription_id = InscriptionId {
        reveal_tx_hash,
        inscription_index,
    };

    let maybe_collection_metadata = tikv
        .get_collection_key_maybe::<InscriptionId>(
            &CollectionIngestor::MetadataByInscription,
            &inscription_id,
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
        "symbol": "aaclub",
        "name": "99999 Action Alien Club",
        "imageURI": "https://bafkreihillpn43rubd2wffzot5ccungccpen6btcofn2kylutexkpkaymq.ipfs.nftstorage.link/",
        "chain": "btc",
        "inscriptionIcon": "46b576bc669c03227f273103654c46fa9b516374ed6a2b8d03cc786e95fd50f3i0",
        "description": "Ordinal #99999 has 50 alien friends. Would you like to join his Action Alien Club?",
        "supply": 50,
        "twitterLink": "http://www.twitter.com/oxfordyazuka",
        "discordLink": "",
        "websiteLink": "",
        "min_inscription_number": "90551",
        "max_inscription_number": "114274",
        "createdAt": "Fri, 26 May 2023 04:23:17 GMT",
        "labels": []
    },
    "last_updated": {
        "block_hash": "00000000000000000001998e2059bcbb25f76fd0ef39db8ddfc5c31c5ea95f1f",
        "block_height": 876644
    }
}"##;
