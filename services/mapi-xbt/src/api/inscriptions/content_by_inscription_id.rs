use axum::{
    extract::{Path, Query},
    response::IntoResponse,
    Extension, Json,
};
use base64::{engine::general_purpose, Engine};
use reqwest::StatusCode;
use std::cmp::min;
use std::str::FromStr;
use timbre_xbt::{
    reducers::content_by_inscription_id::{
        Cursor as ContentByInscriptionIdCursor, Key as ContentByInscriptionIdKey,
        Value as ContentByInscriptionIdValue,
    },
    Decode, Encode, Reducer,
};

use crate::{
    error::Error,
    tikv::{adapter::TiKVAdapter, key_resolver::ReducerType},
    types::{inscriptions::ContentBody, CountParam, CursorPaginationParams, PaginatedContent},
    util::{parse_inscription_id, DEFAULT_CONTENT_BODY_SIZE, MAX_CONTENT_BODY_SIZE},
};

pub static REQUIRED_REDUCERS: &[ReducerType] = &[ReducerType::ContentByInscriptionId];

#[utoipa::path(
    tag = "Inscriptions",
    get,
    path = "/assets/inscriptions/{inscription_id}/content_body",
    params(
        ("inscription_id" = String, Path, description = "Inscription ID", example="7d0a2dd897222913d58fc957b0429526117a0a61c964642fe93b077f328ccec1i0"),

        ("count" = inline(Option<CountParam>), Query, description = "The max number of bytes per page"),
        ("cursor" = inline(Option<String>), Query, description = "Pagination cursor string: the offset in the content body, in the form of an integer. Use the cursor included in a page of results to fetch the next page"),
    ),
    responses(
        (
            status = 200,
            description = "Requested data",
            body = PaginatedContentBody,
            example = json!(serde_json::Value::from_str(EXAMPLE_RESPONSE).unwrap())
        ),
        (status = 400, description = "Malformed query parameters"),
        (status = 404, description = "Requested entity not found on-chain"),
        (status = 500, description = "Internal server error"),
    )
)]
#[tracing::instrument(name = "CONTENT_BY_INSCRIPTION_ID", level = "info", skip(tikv))]
/// Content by Inscription ID
///
/// Retrieves the content body byte array of an inscription. This endpoint is complementary to the `/assets/inscriptions/{inscription_id}` (Inscription Information) endpoint.
pub async fn content_by_inscription_id(
    Path(inscription_id): Path<String>,
    Query(page_params): Query<CursorPaginationParams>,
    mut tikv: Extension<TiKVAdapter>,
) -> Result<impl IntoResponse, Error> {
    tikv.init_tip(REQUIRED_REDUCERS).await?;

    // --- parse inscription ID
    let parsed_key: ([u8; 32], u32) = parse_inscription_id(&inscription_id)?;

    // --- fetch inscription info
    let info: ContentByInscriptionIdValue = tikv
        .get_reducer_key_maybe::<ContentByInscriptionIdKey, ContentByInscriptionIdValue>(
            (
                ReducerType::ContentByInscriptionId,
                Reducer::ContentByInscriptionId,
            ),
            &ContentByInscriptionIdKey {
                inscription_id: parsed_key,
            },
        )
        .await?
        .ok_or_else(|| Error::NotFound)?;

    let total_length: u64 = info.content_body.len() as u64;

    // --- parse pagination params
    let cursor: u64 = match page_params.cursor {
        Some(c) => match ContentByInscriptionIdCursor::decode_base64(&c) {
            Ok((res, _)) => res.offset,
            Err(_) => {
                return Err(Error::MalformedRequest(
                    "Error while decoding cursor".into(),
                ))
            }
        },
        None => 0,
    };

    if cursor > total_length {
        return Err(Error::MalformedRequest(
            "Cursor exceeds content length".into(),
        ));
    }

    let count: u64 = page_params
        .count
        .map(|c| c.0 as u64)
        .unwrap_or(DEFAULT_CONTENT_BODY_SIZE);
    if count > MAX_CONTENT_BODY_SIZE {
        return Err(Error::MalformedRequest("Max response size exceeded".into()));
    }

    let page_start = cursor as usize;
    let page_end = min(cursor + count, total_length) as usize;
    let content_body_page =
        general_purpose::STANDARD.encode(&info.content_body[page_start..page_end]);

    let remaining_bytes: u64 = total_length.saturating_sub(cursor + count);

    let next_cursor = if remaining_bytes != 0 {
        Some(
            ContentByInscriptionIdCursor {
                offset: cursor + count,
            }
            .encode_base64(),
        )
    } else {
        None
    };

    let out = PaginatedContent {
        data: ContentBody {
            content_body_page,
            total_length,
            remaining_bytes,
        },
        last_updated: tikv.get_snapshot_point()?,
        next_cursor,
    };

    Ok((StatusCode::OK, Json(out)))
}

static EXAMPLE_RESPONSE: &str = r##"{
    "data": {
        "content_body_page": "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4vMDEyMzQ1Njc4OTo7PD0+P0BBQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWltcXV5fYGFiYw==",
        "total_length": 3035,
        "remaining_bytes": 2935
    },
    "last_updated": {
        "block_hash": "00000000000000000000ec10254178fe52253f40c1fad252e892d9aa22ee8fa7",
        "block_height": 866710
    },
    "next_cursor": "AAAAAAAAAGQ"
}"##;
