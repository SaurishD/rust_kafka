use std::fmt::format;
use std::fs::File;
use std::usize;
use std::sync::{Arc, RwLock};

use axum::{body::Bytes, routing::{get, post}, Router, extract::{State, Path}};
use kv::{Bucket, Config, Store};
use queues::*;

#[derive(Clone)]
struct BucketDirectory <'a> {
    producer_bucket: Bucket<'a, String,String>,
    consumer_bucket: Bucket<'a, String,String>
}


#[derive(Clone)]
struct AppState<'a>{
    queue: Arc<std::sync::RwLock<queues::Queue<Bytes>>>,
    store_directory: BucketDirectory<'a>
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

pub async fn produce_handler(State(state): State<AppState<'_>>, request: axum::http::Request<axum::body::Body>) -> String{
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

pub async fn consume_handler(State(state): State<AppState<'_>> ,request: axum::http::Request<axum::body::Body>) -> String {
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

pub async fn create_topic(State(state): State<AppState<'_>> ,Path(topic_name):Path<String> ) -> String {
    let file_path = format!("topics/{}.log", topic_name);
    let file_created = File::create_new(file_path);
    match file_created {
        Ok(_) => {
            let producer_bucket = state.store_directory.producer_bucket;
            let offset_initialized = producer_bucket.set(&topic_name, &"0".to_string());
            match offset_initialized {
                Ok(_) => "Topic successfully created".to_string(),
                Err(e) => format!("Topic not created due to error: {}", e).to_string()
            }
            
        },
        Err(e) => format!("Topic not created due to error: {}", e).to_string()
    }
 
}

pub fn init_store() -> Result<BucketDirectory<'static>, kv::Error> {
    let offset_config = Config::new("./offsets");
    let offset_store = Store::new(offset_config)?;

    let consumer_bucket = offset_store.bucket::<String,String>(Some("ProducerBucket"))?;
    let producer_bucket = offset_store.bucket::<String,String>(Some("ConsumerBucker"))?;

    Ok( BucketDirectory {
        producer_bucket: producer_bucket,
        consumer_bucket: consumer_bucket
    })
}

#[tokio::main]
async fn main() {
    
    

    let store_directory: BucketDirectory;

    let extract_directory = init_store();
    match extract_directory {
        Ok(str) => {
            store_directory = str;
        }Err(e) => {
            println!("Something went wrong: {}", e);
            return;
        }
    }

    let shared_state = AppState {
        queue: Arc::new(RwLock::new(queue![])),
        store_directory: store_directory
    };
    
    
    let app: Router<()> = Router::new().route("/", get(|| async {"Hello, Rusty"}))
        .route("/produce", post(produce_handler))
        .route("/consume", post(consume_handler))
        .route("/create_topic/{topic_name}", post(create_topic))
        .with_state(shared_state);

  server_init(app).await;
    
}


