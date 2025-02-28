use crate::models::{AppState, Topic};
use axum::{extract::State, Json};
use std::fs::File;

pub async fn create_topic(State(state): State<AppState>, Json(topic): Json<Topic>) -> String {
    let file_path = format!("topics/{}.log", topic.name);
    let file_created = File::create_new(file_path);
    let topic_name = topic.name.to_string(); //Clone the topic name
    match file_created {
        Ok(_) => {
            let producer_bucket = state.bucket_directory.producer_bucket;
            let producer_offset_initialized = producer_bucket.set(&topic_name, &"0".to_string());
            match producer_offset_initialized {
                Ok(_) => {
                    let topic_metadata_bucket = state.topic_metadata;
                    let topic_initialized = topic_metadata_bucket.set(&topic_name, &topic);
                    match topic_initialized {
                        Ok(_) => "Topic initilized successfully".to_string(),
                        Err(e) => format!("Topic not created due to error: {}", e).to_string(),
                    }
                }
                Err(e) => format!("Topic not created due to error: {}", e).to_string(),
            }
        }
        Err(e) => format!("Topic not created due to error: {}", e).to_string(),
    }
}
