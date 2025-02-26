use std::fs::File;
use std::io::{self, BufRead};

use axum::{extract::State, Json};
use serde::Deserialize;
use crate::models::{AppState, DataStorageFormat};
use crate::utils::get_file_path;
#[derive(Deserialize)]
pub struct Topic {
    topic: String,
}

pub async fn consume_handler(State(state): State<AppState<'_>> ,Json(body) : Json<Topic> ) -> String {
    let topic = body.topic;
    let offset = state.bucket_directory.consumer_bucket.get(&topic);
    match offset {
        Ok(Some(offs)) => {

            let offset_num = offs.parse::<u32>().expect("Unable to parse offset for consumer");

            let file_path = get_file_path(&topic);
            let log_file = File::open(file_path).expect("Error opening filr");

            let reader = io::BufReader::new(log_file);

            for line in reader.lines() {
                let line = line.expect("Error reading line from reader");
                let data_storage: DataStorageFormat = serde_json::from_str(&line).expect("Error decoding line"); 
                if data_storage.offset == offset_num {
                    let next_offs = (offset_num+1).to_string();
                    state.bucket_directory.consumer_bucket.set(&topic, &next_offs).expect("Error updating the offset");
                    return data_storage.data.to_string();
                }
            }

            format!("Something went wrong: {}", topic).to_string()
        }
        Ok(None) => format!("Consumer offset not initialised for toptic {}", topic).to_string(),
        Err(e) => format!("Error getting consumer offset for topic: {}", e).to_string()
    }
}
