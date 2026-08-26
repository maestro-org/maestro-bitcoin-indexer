use axum::{routing::get, Router};

pub mod healthcheck_data_view;

pub fn router() -> Router {
    Router::new().route(
        "/data_view_healthcheck",
        get(healthcheck_data_view::data_view_healthcheck),
    )
}
