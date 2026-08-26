use axum::{
    extract::{Path, Query},
    response::IntoResponse,
    Extension, Json,
};
use reqwest::StatusCode;
use std::str::FromStr;
use timbre_xbt::{CollectionIngestor, Decode, Encode, ShortByteString};

use crate::{
    error::Error,
    tikv::{adapter::TiKVAdapter, key_resolver::ReducerType},
    types::{CountParam, CursorPaginationParams, PaginatedResponse},
    util::MAX_PAGE_COUNT,
};

pub static REQUIRED_REDUCERS: &[ReducerType] = &[ReducerType::ContentByInscriptionId];

#[utoipa::path(
    tag = "Inscriptions",
    get,
    path = "/assets/collections/{collection_symbol}/inscriptions",
    params(
        ("collection_symbol" = String, Path, description = "Collection symbol (UTF-8)", example="twick"),

        ("count" = inline(Option<CountParam>), Query, description = "The max number of results per page"),
        ("cursor" = inline(Option<String>), Query, description = "Pagination cursor string, use the cursor included in a page of results to fetch the next page"),
    ),
    responses(
        (
            status = 200,
            description = "Requested data",
            body = PaginatedInscriptionsByCollectionSymbol,
            example = json!(serde_json::Value::from_str(EXAMPLE_RESPONSE).unwrap())
        ),
        (status = 400, description = "Malformed query parameters"),
        (status = 404, description = "Requested entity not found on-chain"),
        (status = 500, description = "Internal server error"),
    )
)]
#[tracing::instrument(
    name = "INSCRIPTION_IDS_BY_COLLECTION_SYMBOL",
    level = "info",
    skip(tikv)
)]
/// Inscription IDs by Collection Symbol
///
/// List of all inscriptions in the collection represented by the queried symbol.
pub async fn inscriptions_by_collection_symbol(
    page_params: Query<CursorPaginationParams>,
    Path(collection_symbol): Path<String>,
    mut tikv: Extension<TiKVAdapter>,
) -> Result<impl IntoResponse, Error> {
    tikv.init_tip(REQUIRED_REDUCERS).await?;

    tikv.with_collection_metadata().await?;

    // --- initialise `next_cursor`
    let mut next_cursor: Option<String> = None;

    // --- parse count param
    let count = match page_params.count {
        Some(CountParam(count)) => {
            if count > MAX_PAGE_COUNT || count == 0 {
                return Err(Error::MalformedRequest("Invalid page size".into()));
            }
            count
        }
        None => MAX_PAGE_COUNT,
    };

    // --- parse cursor param
    let cursor = if let Some(cursor) = &page_params.cursor {
        match <usize>::decode_base64(&cursor) {
            Ok((cursor, _)) => cursor,
            Err(_) => {
                return Err(Error::MalformedRequest(
                    "Error while decoding cursor".into(),
                ))
            }
        }
    } else {
        0usize
    };

    let mut data: Vec<String> = vec![];

    let maybe_inscription_ids = tikv
        .get_collection_key_maybe::<ShortByteString>(
            &CollectionIngestor::InscriptionsBySymbol,
            &ShortByteString(collection_symbol.as_bytes().to_vec()),
        )
        .await?;

    let inscription_ids = match maybe_inscription_ids {
        Some(inscription_ids) => inscription_ids,
        None => {
            // --- collection either doesn't exist or has no inscriptions
            let out = PaginatedResponse {
                data: vec![],
                last_updated: tikv.get_snapshot_point()?,
                next_cursor,
            };
            return Ok((StatusCode::OK, Json(out)));
        }
    };

    let mut current_items = 0;
    for inscription_id in inscription_ids.chunks_exact(36).skip(cursor) {
        if current_items == count {
            next_cursor = Some((cursor + count).encode_base64());
            break;
        }
        let (reveal_tx_hash, inscription_index) = inscription_id.split_at(32);
        let reveal_tx_hash = hex::encode(reveal_tx_hash);
        let inscription_index = inscription_index.try_into().unwrap();
        let inscription_index = <u32>::from_be_bytes(inscription_index);
        data.push(format!("{}i{}", reveal_tx_hash, inscription_index));
        current_items += 1;
    }

    let out = PaginatedResponse {
        data,
        last_updated: tikv.get_snapshot_point()?,
        next_cursor,
    };

    Ok((StatusCode::OK, Json(out)))
}

static EXAMPLE_RESPONSE: &str = r##"{
    "data": [
        "85a4c2531f149cd8c69a34a9139cace0bdce99e906ee5ad5a4620cdef6adfc6ci0",
        "7d75fe5bea7f11823050d029d281a8b23e9660c5cc133ad3f8b36fef8c8329c0i0",
        "0c80a14912bf35b9303de0758eee6e91532f539104db7f34e760ed1574170d88i0",
        "7fc5d6c9ffffe461b31dc96570134b3d658da767b51225d1771ba7ccffcf6201i0",
        "3204df2e4908f47939878a0681733852ed3ddffca425f25bf67746fd099c78b8i0",
        "491b5144bb2b67a05360e8cbd8ff0beba2a5e3823944796aa7e2680583403184i0",
        "23fa74866576bd731b69fdc900d14b4d9ce904a9be16790d7dd76ac9cf652862i0",
        "70abb5f531adf694438f3b14167f885336c280277f329d6c5fe720b8f5bb20aai0",
        "793a7423c399c87de15f98499656df8bdffabf452879185f452ca33a5943bfb6i0",
        "ced74edbbeb7ccac032a8a218a2ee6a7068963a1c4ae9a403969cc621ffe4f2ei0"
    ],
    "last_updated": {
        "block_hash": "00000000000000000001998e2059bcbb25f76fd0ef39db8ddfc5c31c5ea95f1f",
        "block_height": 876644
    },
    "next_cursor": null
}"##;
