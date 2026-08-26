use axum::{routing::get, Router};

pub mod activity_by_rune;
pub mod holders_by_rune;
pub mod info_by_rune;
pub mod list_runes;
pub mod rune_activity_by_address;
pub mod rune_utxos_by_address;
pub mod rune_utxos_by_address_v2;
pub mod runes_by_address;
pub mod utxos_by_rune;

pub fn router() -> Router {
    Router::new()
        .route(
            "/addresses/:address/runes",
            get(runes_by_address::runes_by_address),
        )
        .route(
            "/addresses/:address/runes/activity",
            get(rune_activity_by_address::rune_activity_by_address),
        )
        .route(
            "/addresses/:address/runes/utxos",
            get(rune_utxos_by_address_v2::rune_utxos_by_address_v2),
        )
        .route(
            "/addresses/:address/runes/:rune",
            // deprecated endpoint kept for API compatibility; superseded by
            // /addresses/:address/runes/utxos
            #[allow(deprecated)]
            get(rune_utxos_by_address::rune_utxos_by_address),
        )
        .route("/assets/runes", get(list_runes::list_runes))
        .route("/assets/runes/:id", get(info_by_rune::info_by_rune))
        .route(
            "/assets/runes/:rune/activity",
            get(activity_by_rune::activity_by_rune),
        )
        .route("/assets/runes/:id/utxos", get(utxos_by_rune::utxos_by_rune))
        .route(
            "/assets/runes/:id/holders",
            get(holders_by_rune::holders_by_rune),
        )
}
