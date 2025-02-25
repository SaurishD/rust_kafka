use std::fs::{File, OpenOptions};
use std::io::Write;
use std::usize;
use std::sync::{Arc, RwLock};

use axum::Json;
use axum::{body::Bytes, routing::{get, post}, Router, extract::{State, Path}};
use kv::{Bucket, Config, Store};
use queues::*;
use serde::Deserialize;
use serde_json::Value;


#[derive(Clone)]
struct BucketDirectory <'a> {
    producer_bucket: Bucket<'a, String,String>,
    consumer_bucket: Bucket<'a, String,String>
}


#[derive(Clone)]
struct AppState<'a>{
    queue: Arc<std::sync::RwLock<queues::Queue<Bytes>>>,
    bucket_directory: BucketDirectory<'a>
}

#[derive(Deserialize)]
struct DataProduceFormat {
    topic: String,
    data: Value
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

pub async fn produce_handler(State(state): State<AppState<'_>>, Json(payload): Json<DataProduceFormat>) -> String{
    let topic = payload.topic;
    let message = payload.data;
    
    
    
    let file_path = format!("topics/{}.log",topic);
    let open_file = OpenOptions::new().append(true).open(file_path);
    match open_file{
        Ok(mut f) => {
            let offset = state.bucket_directory.producer_bucket.get(&topic);
            match offset{
                Ok(Some(offset_str)) => {
                  
                    
                    let log = format!("{} {}",offset_str,message);
                    //let bytes = Bytes::from(log);

                    let written= writeln!(f, "{}", log);
                    match written {
                        Ok(_) => {
                            let num =offset_str.parse::<u32>().expect("Error parsing offset number") + 1;
                            state.bucket_directory.producer_bucket.set(&topic, &num.to_string()).expect("Error updating the offset in bucket");
                            format!("Offset updated successfully!").to_string()
                        }
                        Err(e) => format!("Error updating offset {}", e).to_string()
                    }
                    
                }Ok(None) => format!("Topic not initialized"),
                Err(e) => {
                    return format!("Error fetching topic offset: {}", e);
                }
            }
        }Err(e) => format!("Cannot open log file for topic: {}", e).to_string()
        
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
            let producer_bucket = state.bucket_directory.producer_bucket;
            let producer_offset_initialized = producer_bucket.set(&topic_name, &"0".to_string());
            match producer_offset_initialized {
                Ok(_) => {},
                Err(e) => {
                    return format!("Topic not created due to error: {}", e).to_string();
                }
            }

            let consumer_bucket = state.bucket_directory.consumer_bucket;
            let consumer_offset_initialised = consumer_bucket.set(&topic_name, &"0".to_string());
            match consumer_offset_initialised {
                Ok(_) => "Topic created successfully".to_string(),
                Err(e)=> format!("Error initialising consumer offset {}", e).to_string()
            }
            
        },
        Err(e) => format!("Topic not created due to error: {}", e).to_string()
    }
 
}

pub fn init_store() -> Result<BucketDirectory<'static>, kv::Error> {
    let offset_config = Config::new("./offsets").flush_every_ms(500);
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
        .with_state(shared_state);

  server_init(app).await;
    
}


