use std::sync::{Arc, RwLock};
use axum::{ routing::{get, post}, Router};
use kv::{ Config, Store};
use queues::*;
use rust_kafka::models::{BucketDirectory, AppState};
use rust_kafka::handlers::{producer::produce_handler, consumer::consume_handler, topic::{create_topic,subscribe_topic}};




async fn server_init(app: Router) {
    println!("Running on localhost:3000");
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}


pub fn init_store() -> Result<BucketDirectory<'static>, kv::Error> {
    let offset_config = Config::new("./offsets").flush_every_ms(500);
    let offset_store = Store::new(offset_config)?;

    let producer_bucket = offset_store.bucket::<String,String>(Some("ProducerBucket"))?;

    Ok( BucketDirectory {
        producer_bucket: producer_bucket,
        offset_store: offset_store
    })
}

#[tokio::main]
async fn main() {
    
    

    let bucket_directory: BucketDirectory;

    let extract_directory = init_store();
    match extract_directory {
        Ok(str) => {
            bucket_directory = str;
        }Err(e) => {
            println!("Something went wrong: {}", e);
            return;
        }
    }

    let shared_state = AppState {
        queue: Arc::new(RwLock::new(queue![])),
        bucket_directory: bucket_directory
    };
    
    
    let app: Router<()> = Router::new().route("/", get(|| async {"Hello, Rusty"}))
        .route("/produce", post(produce_handler))
        .route("/consume", post(consume_handler))
        .route("/create_topic/{topic_name}", post(create_topic))
        .route("/subscribe", post(subscribe_topic))
        .with_state(shared_state);

  server_init(app).await;
    
}


