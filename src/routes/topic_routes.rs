use axum::{routing::post, Router};

use crate::{handlers::topic::create_topic, models::AppState};

pub fn topic_routes() -> Router<AppState> {
    Router::new().route("/produce", post(create_topic))
}
