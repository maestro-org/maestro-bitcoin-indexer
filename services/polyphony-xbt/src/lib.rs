pub mod bootstrap;
pub mod constants;
pub mod crosscut;
pub mod model;
pub mod prelude;
pub mod reducers;
pub mod rollback;
pub mod sources;
pub mod storage;

use std::fmt::Display;

use crosscut::{Point, PointArg};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("configuration error: {0}")]
    ConfigError(String),

    #[error("network error: {0}")]
    NetworkError(String),

    #[error("missing utxo: {0}")]
    MissingUtxo(String),

    #[error("source error: {0}")]
    SourceError(String),

    #[error("storage error: {0}")]
    StorageError(String),

    #[error("intersect not found")]
    IntersectNotFound,

    #[error("{0}")]
    Message(String),

    #[error("{0}")]
    Custom(String),

    #[error("{0}")]
    Encoding(String),

    #[error(
        "could not undo effects of rollbacked blocks as point {0} not found in rollback buffer"
    )]
    RollbackOutOfRange(String),
}

impl Error {
    pub fn config(text: impl Into<String>) -> Error {
        Error::ConfigError(text.into())
    }

    pub fn message(text: impl Into<String>) -> Error {
        Error::Message(text.into())
    }

    pub fn network(error: impl Display) -> Error {
        Error::NetworkError(error.to_string())
    }

    pub fn missing_utxo(utxo_key: impl Display) -> Error {
        Error::MissingUtxo(utxo_key.to_string())
    }

    pub fn source(error: impl Display) -> Error {
        Error::SourceError(error.to_string())
    }

    pub fn storage(error: impl Display) -> Error {
        Error::StorageError(error.to_string())
    }

    pub fn custom(error: Box<dyn std::error::Error>) -> Error {
        Error::Custom(format!("{}", error))
    }

    pub fn encoding(error: impl Display) -> Error {
        Error::Encoding(error.to_string())
    }

    pub fn rollback(point: Point) -> Error {
        Error::RollbackOutOfRange(PointArg::from(point).to_string())
    }
}

impl From<Box<dyn std::error::Error>> for Error {
    fn from(err: Box<dyn std::error::Error>) -> Self {
        Error::custom(err)
    }
}
