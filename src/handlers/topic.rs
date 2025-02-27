use crate::models::AppState;
use axum::extract::{Path, State};
use std::fs::File;

pub async fn create_topic(State(state): State<AppState>, Path(topic_name): Path<String>) -> String {
    let file_path = format!("topics/{}.log", topic_name);
    let file_created = File::create_new(file_path);
    match file_created {
        Ok(_) => {
            let producer_bucket = state.bucket_directory.producer_bucket;
            let producer_offset_initialized = producer_bucket.set(&topic_name, &"0".to_string());
            match producer_offset_initialized {
                Ok(_) => "Topic created successfully".to_string(),
                Err(e) => format!("Topic not created due to error: {}", e).to_string(),
            }
        }
        Err(e) => format!("Topic not created due to error: {}", e).to_string(),
    }
}
