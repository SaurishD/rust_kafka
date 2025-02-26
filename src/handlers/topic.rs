
use std::fs::File;
use axum::{extract::{Path, State}, Json};
use crate::models::{AppState, ConsumerRequest};


pub async fn subscribe_topic(State(state): State<AppState<'_>>, Json(body): Json<ConsumerRequest>) -> String {
    let group_id = body.group_id;
    let topic_name = body.topic;
    let consumer_bucket = state.bucket_directory.offset_store.bucket::<String,String>(Some(&group_id)).expect("Error creating consumer bucket");
    let consumer_offset_initialised = consumer_bucket.set(&topic_name, &"0".to_string());
    match consumer_offset_initialised {
        Ok(_) => "Topic created successfully".to_string(),
        Err(e)=> format!("Error initialising consumer offset {}", e).to_string()
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
                Ok(_) => "Topic created successfully".to_string(),
                Err(e) => format!("Topic not created due to error: {}", e).to_string()
            }
            
        },
        Err(e) => format!("Topic not created due to error: {}", e).to_string()
    }
 
}