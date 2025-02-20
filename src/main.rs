use std::usize;

use axum::{routing::{get, post},  Router};



async fn server_init(app: Router) {
    println!("Running on localhost:3000");
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

pub async fn produce_handler(request: axum::http::Request<axum::body::Body>) -> &'static str{
    let body = request.into_body();
    let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();

    let message = String::from_utf8(bytes.to_vec()).unwrap();
    println!("Body: {} ", message);
    "Producer called"
}

pub async fn consume_handler(request: axum::http::Request<axum::body::Body>) -> &'static str{
    let body = request.into_body();
    let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
    let message = String::from_utf8(bytes.to_vec()).unwrap();

    println!("Consumer Body: {}",message);

    "Consumer Called"
}

#[tokio::main]
async fn main() {
    let app: Router<()> = Router::new().route("/", get(|| async {"Hello, Rusty"}))
        .route("/produce", post(produce_handler))
        .route("/consume", post(consume_handler));

  server_init(app).await;
    
}
