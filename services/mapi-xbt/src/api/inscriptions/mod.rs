use axum::{routing::get, Router};

pub mod activity_by_inscription;
pub mod brc20_by_address;
pub mod brc20_holders_by_ticker;
pub mod brc20_info;
pub mod brc20_transfer_inscriptions_by_address;
pub mod collection_metadata_by_collection_symbol;
pub mod collection_metadata_by_inscription;
pub mod collection_stats_by_collection_symbol;
pub mod content_by_inscription_id;
pub mod inscription_activity_by_address;
pub mod inscription_activity_by_block;
pub mod inscription_activity_by_tx;
pub mod inscription_info;
pub mod inscriptions_by_address;
pub mod inscriptions_by_collection_symbol;
pub mod list_brc20s;
pub mod omb_color_group_by_inscription;
pub mod token_metadata_by_inscription;

pub fn router() -> Router {
    Router::new()
        .route(
            "/addresses/:address/brc20",
            get(brc20_by_address::brc20_by_address),
        )
        .route(
            "/addresses/:address/inscriptions",
            get(inscriptions_by_address::inscriptions_by_address),
        )
        .route(
            "/addresses/:address/inscriptions/activity",
            get(inscription_activity_by_address::inscription_activity_by_address),
        )
        .route(
            "/addresses/:address/brc20/transfer_inscriptions",
            get(brc20_transfer_inscriptions_by_address::brc20_transfer_inscriptions_by_address),
        )
        .route(
            "/assets/brc20/:id/holders",
            get(brc20_holders_by_ticker::brc20_holders_by_ticker),
        )
        .route("/assets/brc20/:id", get(brc20_info::brc20_info))
        .route("/assets/brc20", get(list_brc20s::list_brc20s))
        .route(
            "/blocks/:height_or_hash/inscriptions/activity",
            get(inscription_activity_by_block::inscription_activity_by_block),
        )
        .route(
            "/assets/inscriptions/:inscription_id",
            get(inscription_info::inscription_info),
        )
        .route(
            "/assets/inscriptions/:inscription_id/activity",
            get(activity_by_inscription::activity_by_inscription),
        )
        .route(
            "/assets/inscriptions/:inscription_id/content_body",
            get(content_by_inscription_id::content_by_inscription_id),
        )
        .route(
            "/transactions/:tx_hash/inscriptions/activity",
            get(inscription_activity_by_tx::inscription_activity_by_tx),
        )
        .route(
            "/assets/collections/:collection_symbol/inscriptions",
            get(inscriptions_by_collection_symbol::inscriptions_by_collection_symbol),
        )
        .route(
            "/assets/collections/:collection_symbol/metadata",
            get(collection_metadata_by_collection_symbol::collection_metadata_by_collection_symbol),
        )
        .route(
            "/assets/collections/:collection_symbol/stats",
            get(collection_stats_by_collection_symbol::collection_stats_by_collection_symbol),
        )
        .route(
            "/assets/inscriptions/:inscription_id/collection",
            get(collection_metadata_by_inscription::collection_metadata_by_inscription),
        )
        .route(
            "/assets/inscriptions/:inscription_id/metadata",
            get(token_metadata_by_inscription::token_metadata_by_inscription),
        )
        .route(
            "/assets/inscriptions/:inscription_id/omb_color_group",
            get(omb_color_group_by_inscription::omb_color_group_by_inscription),
        )
}
