use std::usize;
use std::sync::{Arc, RwLock};

use axum::{body::Bytes, routing::{get, post}, Router, extract::State};
use queues::*;
use serde_json::Value;


#[derive(Clone)]
struct AppState{
    queue: Arc<std::sync::RwLock<queues::Queue<Bytes>>>,
}



async fn server_init(app: Router) {
    println!("Running on localhost:3000");
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

pub async fn produce_handler(State(state): State<AppState>, request: axum::http::Request<axum::body::Body>) -> &'static str{
    let body = request.into_body();
    let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
    let message = String::from_utf8(bytes.to_vec()).unwrap();
    
    let queue = state.queue.clone();
    let mut q = queue.write().unwrap();
    
    q.add(bytes);
    println!("Body: {} ", message);
    println!("Size of queue {}", q.size());
    "Producer called"
}

pub async fn consume_handler(State(state): State<AppState> ,request: axum::http::Request<axum::body::Body>) -> String {
    let body = request.into_body();
    let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
    let message = String::from_utf8(bytes.to_vec()).unwrap();
    let queue = state.queue.clone();
    let mut q = queue.write().unwrap();
    let extract_bytes = q.remove();

    println!("Consumer Body: {}",message);
    
    match extract_bytes {
        Ok(value) => {
            let string_of_bytes = std::str::from_utf8(&value);
            match string_of_bytes {
                Ok(val ) => val.to_string(),
                Err(e) => "Error converting bytes to string".to_string()
            }
        },
        Err(e) => e.to_string()
    }
}

#[tokio::main]
async fn main() {
    
    let shared_state = AppState {
        queue: Arc::new(RwLock::new(queue![]))
    };
    let app: Router<()> = Router::new().route("/", get(|| async {"Hello, Rusty"}))
        .route("/produce", post(produce_handler))
        .route("/consume", post(consume_handler))
        .with_state(shared_state);

  server_init(app).await;
    
}
