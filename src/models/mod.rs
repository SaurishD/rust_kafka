use kv::Bucket;
use axum::body::Bytes;
use std::sync::Arc;
use serde::Deserialize;
use serde_json::Value;

#[derive(Clone)]
pub struct BucketDirectory <'a> {
    pub producer_bucket: Bucket<'a, String,String>,
    pub consumer_bucket: Bucket<'a, String,String>
}


#[derive(Clone)]
pub struct AppState<'a>{
    pub queue: Arc<std::sync::RwLock<queues::Queue<Bytes>>>,
    pub bucket_directory: BucketDirectory<'a>
}

#[derive(Deserialize)]
pub struct DataProduceFormat {
    pub topic: String,
    pub data: Value
}
#[derive(serde::Serialize,Deserialize)]
pub struct DataStorageFormat {
    pub data: Value,
    pub offset: u32, 
}


