use axum::{routing::post, Router};

use crate::{
    handlers::consumer::{consume_handler, subscribe_topic},
    models::AppState,
};

pub fn consumer_routes() -> Router<AppState> {
    Router::new()
        .route("/produce", post(consume_handler))
        .route("/subscribe", post(subscribe_topic))
}
