use axum::{routing::post, Router};

use crate::{handlers::producer::produce_handler, models::AppState};

pub fn producer_routes() -> Router<AppState> {
    Router::new().route("/produce", post(produce_handler))
}
