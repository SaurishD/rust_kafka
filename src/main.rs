use axum::Router;
use kv::{Config, Store};
use queues::*;
use rust_kafka::handlers::garbage_collector::clear_expired_logs;
use rust_kafka::models::{AppState, BucketDirectory, Topic};
use rust_kafka::routes::producer_routes::producer_routes;
use rust_kafka::routes::{consumer_routes::consumer_routes, topic_routes::topic_routes};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;

async fn server_init(app: Router) {
    println!("Running on localhost:3000");
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

pub fn init_store() -> Result<BucketDirectory, kv::Error> {
    let offset_config = Config::new("./offsets").flush_every_ms(500);
    let offset_store = Store::new(offset_config)?;

    let producer_bucket = offset_store.bucket::<String, String>(Some("ProducerBucket"))?;

    Ok(BucketDirectory {
        producer_bucket: producer_bucket,
        offset_store: offset_store,
    })
}

#[tokio::main]
async fn main() {
    let metadata_store_config = Config::new("./metadata").flush_every_ms(500);
    let metadata_store =
        Store::new(metadata_store_config).expect("topic metadata store not initialized 1");
    let topic_metadata_bucket = metadata_store
        .bucket::<String, Topic>(Some("TopicMetadata"))
        .expect("topic metadata bucket not initialized");
    let handle = thread::spawn(move || loop {
        #[allow(unused_must_use)]
        clear_expired_logs(&topic_metadata_bucket);
        thread::sleep(Duration::from_secs(2));
    });
    let bucket_directory: BucketDirectory;

    let extract_directory = init_store();
    match extract_directory {
        Ok(str) => {
            bucket_directory = str;
        }
        Err(e) => {
            println!("Something went wrong: {}", e);
            return;
        }
    }

    let topic_metata_bucket = metadata_store
        .bucket::<String, Topic>(Some("TopicMetadata"))
        .expect("topic metadata bucket not initialized");

    let shared_state = AppState {
        queue: Arc::new(RwLock::new(queue![])),
        bucket_directory: bucket_directory,
        topic_metadata: topic_metata_bucket,
    };
    let app: Router<()> = Router::<AppState>::new()
        .merge(producer_routes())
        .merge(consumer_routes())
        .merge(topic_routes())
        .with_state(shared_state);

    // let app = Router::new().merge(producer_routes());

    server_init(app).await;
    handle.join().unwrap();
}
