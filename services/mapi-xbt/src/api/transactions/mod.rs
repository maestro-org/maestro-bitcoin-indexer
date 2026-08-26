use axum::{routing::get, Router};

pub mod tx_info;
pub mod tx_info_with_metaprotocols;
pub mod tx_output_info;

pub fn router() -> Router {
    Router::new()
        .route("/:tx_hash", get(tx_info::tx_info))
        .route(
            "/:tx_hash/metaprotocols",
            get(tx_info_with_metaprotocols::tx_info_with_metaprotocols),
        )
        .route(
            "/:tx_hash/outputs/:output",
            get(tx_output_info::tx_output_info),
        )
}
