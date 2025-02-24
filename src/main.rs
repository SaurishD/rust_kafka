use std::fmt::format;
use std::fs::File;
use std::usize;
use std::sync::{Arc, RwLock};

use axum::{body::Bytes, routing::{get, post}, Router, extract::{State, Path}};
use queues::*;



#[derive(Clone)]
struct AppState{
    queue: Arc<std::sync::RwLock<queues::Queue<Bytes>>>,
}

struct DataProduceFormat {
    topic: String,
    data: Bytes
}

struct DataStorageFormat {
    data: Bytes,
    offset: u32, 
    expiration_time: String,
}



async fn server_init(app: Router) {
    println!("Running on localhost:3000");
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

pub async fn produce_handler(State(state): State<AppState>, request: axum::http::Request<axum::body::Body>) -> String{
    let body = request.into_body();
    let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
    let message = String::from_utf8(bytes.to_vec()).unwrap();
    
    let queue = state.queue.clone();
    let mut q = queue.write().unwrap();
    
    let result = q.add(bytes);
    println!("Body: {} ", message);
    match result {
        Ok(_) => {
            println!("Size of the queue: {}", q.size());
            "Produced".to_string()
        },
        Err(e ) => e.to_string()
    }
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
                Err(_)  => "Error converting bytes to string".to_string()
            }
        },
        Err(e) => e.to_string()
    }
}

pub async fn create_topic(Path(topic_name):Path<String> ) -> String {
    let file_path = format!("topics/{}.log", topic_name);
    let file_created = File::create_new(file_path);
    match file_created {
        Ok(_) => "Topic successfully created".to_string(),
        Err(e) => format!("Topic not created due to error: {}", e).to_string()
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
        .route("/create_topic/{topic_name}", post(create_topic))
        .with_state(shared_state);

  server_init(app).await;
    
}
