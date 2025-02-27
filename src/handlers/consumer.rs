use std::fs::File;
use std::io::{self, BufRead};

use crate::models::{AppState, ConsumerRequest, DataStorageFormat};
use crate::utils::get_file_path;
use axum::{extract::State, Json};

pub async fn consume_handler(
    State(state): State<AppState>,
    Json(body): Json<ConsumerRequest>,
) -> String {
    let topic = body.topic;
    let group_id = body.group_id;
    let consumer_group_bucket = state
        .bucket_directory
        .offset_store
        .bucket::<String, String>(Some(&group_id))
        .expect("Error getting group_id bucket");
    let offset = consumer_group_bucket.get(&topic);
    match offset {
        Ok(Some(offs)) => {
            let offset_num = offs
                .parse::<u32>()
                .expect("Unable to parse offset for consumer");

            let file_path = get_file_path(&topic);
            let log_file = File::open(file_path).expect("Error opening filr");

            let reader = io::BufReader::new(log_file);

            for line in reader.lines() {
                let line = line.expect("Error reading line from reader");
                let data_storage: DataStorageFormat =
                    serde_json::from_str(&line).expect("Error decoding line");
                if data_storage.offset == offset_num {
                    let next_offs = (offset_num + 1).to_string();
                    consumer_group_bucket
                        .set(&topic, &next_offs)
                        .expect("Error updating the offset");
                    return data_storage.data.to_string();
                }
            }

            format!("Something went wrong: {}", topic).to_string()
        }
        Ok(None) => format!("Consumer offset not initialised for toptic {}", topic).to_string(),
        Err(e) => format!("Error getting consumer offset for topic: {}", e).to_string(),
    }
}

pub async fn subscribe_topic(
    State(state): State<AppState>,
    Json(body): Json<ConsumerRequest>,
) -> String {
    let group_id = body.group_id;
    let topic_name = body.topic;
    let consumer_bucket = state
        .bucket_directory
        .offset_store
        .bucket::<String, String>(Some(&group_id))
        .expect("Error creating consumer bucket");
    let consumer_offset_initialised = consumer_bucket.set(&topic_name, &"0".to_string());
    match consumer_offset_initialised {
        Ok(_) => "Topic created successfully".to_string(),
        Err(e) => format!("Error initialising consumer offset {}", e).to_string(),
    }
}
