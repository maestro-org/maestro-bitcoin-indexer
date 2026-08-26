use axum::{extract::Path, response::IntoResponse, Extension, Json};
use reqwest::StatusCode;
use timbre_xbt::CollectionIngestor;

use crate::{
    error::Error,
    tikv::{adapter::TiKVAdapter, key_resolver::ReducerType},
    types::{
        collections::{InscriptionId, OmgColorGroup},
        TimestampedResponse,
    },
    util::parse_inscription_id,
};

pub static REQUIRED_REDUCERS: &[ReducerType] = &[ReducerType::ContentByInscriptionId];

#[tracing::instrument(name = "OMB_COLOR_GROUP_BY_INSCRIPTION", level = "info", skip(tikv))]
/// OMB color group by inscription
///
/// Liquidium one-off: aggregates over inscriptions that are part of a specific OMB color group.
pub async fn omb_color_group_by_inscription(
    Path(inscription_id): Path<String>,
    mut tikv: Extension<TiKVAdapter>,
) -> Result<impl IntoResponse, Error> {
    tikv.init_tip(REQUIRED_REDUCERS).await?;

    tikv.with_collection_metadata().await?;

    // ---

    let (reveal_tx_hash, inscription_index) = parse_inscription_id(&inscription_id)?;
    let inscription_id = InscriptionId {
        reveal_tx_hash,
        inscription_index,
    };

    let maybe_collection_metadata = tikv
        .get_collection_key_maybe::<InscriptionId>(
            &CollectionIngestor::OmbColorGroupByInscription,
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

    let data: OmgColorGroup = serde_json::from_slice(&json_bytes).unwrap();

    let out = TimestampedResponse {
        data,
        last_updated: tikv.get_snapshot_point()?,
    };

    Ok((StatusCode::OK, Json(out)))
}
