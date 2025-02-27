use axum::body::Bytes;
use kv::{Bucket, Store};
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;

#[derive(Clone)]
pub struct BucketDirectory {
    pub producer_bucket: Bucket<'static, String, String>,
    pub offset_store: Store,
}

#[derive(Clone)]
pub struct AppState {
    pub queue: Arc<std::sync::RwLock<queues::Queue<Bytes>>>,
    pub bucket_directory: BucketDirectory,
}

#[derive(Deserialize)]
pub struct DataProduceFormat {
    pub topic: String,
    pub data: Value,
}
#[derive(serde::Serialize, Deserialize)]
pub struct DataStorageFormat {
    pub data: Value,
    pub offset: u32,
}

#[derive(Deserialize)]
pub struct ConsumerRequest {
    pub group_id: String,
    pub topic: String,
}
