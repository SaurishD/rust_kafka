use std::fs::File;
use std::sync::{Arc, RwLock};
use axum::{ routing::{get, post}, Router, extract::{State, Path}};
use kv::{ Config, Store};
use queues::*;
use kafka::models::{BucketDirectory, AppState};
use kafka::handlers::{producer::produce_handler, consumer::consume_handler};




async fn server_init(app: Router) {
    println!("Running on localhost:3000");
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
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


