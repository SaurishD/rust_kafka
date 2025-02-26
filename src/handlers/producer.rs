use std::fs::OpenOptions;
use std::io::Write;
use axum::Json;
use axum::extract::State;

use crate::models::{AppState, DataProduceFormat, DataStorageFormat};


pub async fn produce_handler(State(state): State<AppState<'_>>, Json(payload): Json<DataProduceFormat>) -> String{
    let topic = payload.topic;
    let message = payload.data;
    
    
    
    let file_path = format!("topics/{}.log",topic);
    let mut f = OpenOptions::new().append(true).open(file_path).expect("Error opening log file for topic");
    
    let offset = state.bucket_directory.producer_bucket.get(&topic);
    match offset{
        Ok(Some(offset_str)) => {
            let offset_num = offset_str.parse::<u32>().expect("Error parsing offset number");
            let data_to_store = DataStorageFormat {
                data: message,
                offset: offset_num
            };
            //let bytes = Bytes::from(log);
            let data_string = serde_json::to_string(&data_to_store).expect("Error stringify object");

            let written= writeln!(f, "{}", data_string);
            match written {
                Ok(_) => {
                    let num =offset_str.parse::<u32>().expect("Error parsing offset number") + 1;
                    state.bucket_directory.producer_bucket.set(&topic, &num.to_string()).expect("Error updating the offset in bucket");
                    format!("Offset updated successfully!").to_string()
                }
                Err(e) => format!("Error updating offset {}", e).to_string()
            }
            
        }Ok(None) => format!("Topic not initialized"),
        Err(e) => {
            return format!("Error fetching topic offset: {}", e);
        }
        
    }
        
}
