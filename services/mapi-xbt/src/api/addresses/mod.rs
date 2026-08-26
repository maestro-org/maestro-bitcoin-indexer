use axum::{routing::get, Router};

pub mod address_statistics;
pub mod historical_satoshi_balance_by_address;
pub mod satoshi_activity_by_address;
pub mod satoshi_balance_by_address;
pub mod txs_by_address;
pub mod utxos_by_address;

pub fn router() -> Router {
    Router::new()
        .route(
            "/:address/activity",
            get(satoshi_activity_by_address::satoshi_activity_by_address),
        )
        .route(
            "/:address/balance",
            get(satoshi_balance_by_address::satoshi_balance_by_address),
        )
        .route(
            "/:address/balance/historical",
            get(historical_satoshi_balance_by_address::historical_satoshi_balance_by_address),
        )
        .route(
            "/:address/statistics",
            get(address_statistics::address_statistics),
        )
        .route("/:address/txs", get(txs_by_address::txs_by_address))
        .route("/:address/utxos", get(utxos_by_address::utxos_by_address))
}
