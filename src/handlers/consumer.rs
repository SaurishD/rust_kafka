use std::usize;
use axum::extract::State;
use queues::*;
use crate::models::AppState;

pub async fn consume_handler(State(state): State<AppState<'_>> ,request: axum::http::Request<axum::body::Body>) -> String {
    let body = request.into_body();
    let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
    let message = String::from_utf8(bytes.to_vec()).unwrap();
    let queue = state.queue.clone();
    let mut q = queue.write().unwrap();
    let extract_bytes = q.remove();

    println!("Consumer Body: {}",message);
    
    match extract_bytes {
        Ok(value) => {
            let string_of_bytes = std::str::from_utf8(&value);
            match string_of_bytes {
                Ok(val ) => val.to_string(),
                Err(_)  => "Error converting bytes to string".to_string()
            }
        },
        Err(e) => e.to_string()
    }
}
