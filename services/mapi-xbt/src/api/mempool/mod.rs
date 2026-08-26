use axum::{routing::get, Router};

pub mod fee_rates;
pub mod holders_by_rune;
pub mod rune_utxos_by_address;
pub mod runes_by_address;
pub mod satoshi_balance_by_address;
pub mod tx_info_with_metaprotocols;
pub mod tx_output_info;
pub mod utxos_by_address;

pub fn router() -> Router {
    Router::new()
        .route("/fee_rates", get(fee_rates::fee_rates))
        .route(
            "/assets/runes/:rune/holders",
            get(holders_by_rune::mempool_holders_by_rune),
        )
        .route(
            "/addresses/:address/runes",
            get(runes_by_address::mempool_runes_by_address),
        )
        .route(
            "/addresses/:address/runes/utxos",
            get(rune_utxos_by_address::mempool_rune_utxos_by_address),
        )
        .route(
            "/addresses/:address/balance",
            get(satoshi_balance_by_address::mempool_satoshi_balance_by_address),
        )
        .route(
            "/addresses/:address/utxos",
            get(utxos_by_address::mempool_utxos_by_address),
        )
        .route(
            "/transactions/:tx_hash/metaprotocols",
            get(tx_info_with_metaprotocols::tx_info_with_metaprotocols),
        )
        .route(
            "/transactions/:tx_hash/outputs/:output",
            get(tx_output_info::mempool_tx_output_info),
        )
}
