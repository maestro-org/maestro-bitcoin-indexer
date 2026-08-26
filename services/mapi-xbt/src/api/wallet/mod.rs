use axum::{routing::get, Router};

pub mod address_statistics;
pub mod common;
pub mod historical_satoshi_balance_by_address_wrapper;
pub mod inscription_activity_by_address;
pub mod metaprotocol_activity_by_address;
pub mod rune_activity_by_address;
pub mod satoshi_activity_by_address;

pub fn router() -> Router {
    Router::new()
        .route(
            "/wallet/addresses/:address/activity",
            get(satoshi_activity_by_address::wallet_satoshi_activity_by_address),
        )
        .route(
            "/wallet/addresses/:address/activity/metaprotocols",
            get(metaprotocol_activity_by_address::wallet_metaprotocol_activity_by_address),
        )
        .route(
            "/wallet/addresses/:address/balance/historical",
            get(historical_satoshi_balance_by_address_wrapper::wallet_historical_satoshi_balance_by_address),
        )
        .route(
            "/wallet/addresses/:address/inscriptions/activity",
            get(inscription_activity_by_address::wallet_inscription_activity_by_address),
        )
        .route(
            "/wallet/addresses/:address/runes/activity",
            get(rune_activity_by_address::wallet_rune_activity_by_address),
        )
        .route(
            "/wallet/addresses/:address/statistics",
            get(address_statistics::wallet_address_statistics),
        )
}
