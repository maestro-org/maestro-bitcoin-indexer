use std::collections::HashMap;

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use bb8_redis_cluster::redis_cluster_async::redis::RedisError;
use timbre_xbt::TimbreError;
use tracing::error;

use crate::tikv::{key_resolver::ReducerType, redis_entry::RedisEntry};

pub type MapiResult<T> = Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Invalid hex value: {0}")]
    InvalidHex(String),

    #[error("Hex Error: {0}")]
    Hex(#[from] hex::FromHexError),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Unable to find user requested data")]
    NotFound,

    #[error("Users request/query was malformed: {0}")]
    MalformedRequest(String),

    #[error("TiKV error: {0}")]
    TiKV(#[from] tikv_client::Error),

    #[error("Redis error: {0}")]
    Redis(#[from] RedisError),

    #[error("NoInstanceIntersect: {0:?}")]
    NoInstanceIntersect(HashMap<ReducerType, Vec<RedisEntry>>),

    #[error("NoTimestampEntries: {0}:{1}")]
    NoTimestampEntires(u8, u16),

    #[error("RedisPoolError {0}")]
    RedisPoolError(bb8_redis_cluster::bb8::RunError<RedisError>),

    #[error("AdapterNotInitialised")]
    AdapterNotInitialised,

    #[error("AdapterMissingReducer: {0}")]
    AdapterMissingReducer(ReducerType),

    #[error("KeyResolverMalformed: {0:?}")]
    KeyResolverMalformed(Vec<u8>),

    #[error("KeyResolverNoEntries: {0}")]
    KeyResolverNoEntries(String),

    #[error("Missing expected data in storage: {0:?}")]
    MissingData(Vec<u8>),

    #[error("Unable to decode data from storage: {0:?} {1:?}")]
    MalformedData(Vec<u8>, Option<TimbreError>),

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("{0}")]
    Failure(String),

    #[error("Unexpected mode: GenerateOpenApi")]
    GenerateOpenApiIsUnexpected(),

    #[error("Unexpected mode: GenerateOpenApiMempool")]
    GenerateOpenApiMempoolIsUnexpected(),
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        match self {
            Error::NotFound => (
                StatusCode::NOT_FOUND,
                format!("Unable to find requested data on-chain"),
            )
                .into_response(),
            Error::InvalidHex(e) => {
                (StatusCode::BAD_REQUEST, format!("Invalid Hash: {e}")).into_response()
            }
            Error::MalformedRequest(e) => (
                StatusCode::BAD_REQUEST,
                format!("Unable to parse request parameters: {e}"),
            )
                .into_response(),
            _ => {
                error!("Internal server error: {}", self);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Internal server error"),
                )
                    .into_response()
            }
        }
    }
}
