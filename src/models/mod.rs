use axum::body::Bytes;
use kv::Value as Kv_Value;
use kv::{Bucket, Store};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sled::IVec;
use std::sync::Arc;

#[derive(Clone, Serialize, Deserialize)]
pub struct Topic {
    pub name: String,
    pub partition_number: u8,
    pub retaintion_time: u32,
    pub max_size: u32,
}

impl Kv_Value for Topic {
    fn to_raw_value(&self) -> Result<IVec, kv::Error> {
        // Serialize the struct to a byte vector
        let bytes = serde_json::to_vec(self).expect("Error converting into bytes");
        // Convert the byte vector to `sled::IVec`
        Ok(IVec::from(bytes))
    }

    fn from_raw_value(bytes: IVec) -> Result<Self, kv::Error> {
        Ok(serde_json::from_slice(&bytes).expect("Error converting from raw value"))
    }
}

#[derive(Clone)]
pub struct BucketDirectory {
    pub producer_bucket: Bucket<'static, String, String>,
    pub offset_store: Store,
}

#[derive(Clone)]
pub struct AppState {
    pub queue: Arc<std::sync::RwLock<queues::Queue<Bytes>>>,
    pub bucket_directory: BucketDirectory,
    pub topic_metadata: Bucket<'static, String, Topic>,
}

#[derive(Deserialize)]
pub struct DataProduceFormat {
    pub topic: String,
    pub data: Value,
}
#[derive(Serialize, Deserialize)]
pub struct DataStorageFormat {
    pub data: Value,
    pub offset: u32,
}

#[derive(Deserialize)]
pub struct ConsumerRequest {
    pub group_id: String,
    pub topic: String,
}
