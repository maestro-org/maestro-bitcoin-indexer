use axum::{routing::get, Router};

pub mod block_info;
pub mod txs_by_block;

pub fn router() -> Router {
    Router::new()
        .route("/:height_or_hash", get(block_info::block_info))
        .route(
            "/:height_or_hash/transactions",
            get(txs_by_block::txs_by_block),
        )
}
