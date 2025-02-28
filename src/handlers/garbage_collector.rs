use std::fs::{self, File};
use std::time::SystemTime;

use kv::Bucket;

use crate::models::Topic;
use crate::utils::get_file_path;

pub fn clear_expired_logs(
    topic_metadata_bucket: &Bucket<'static, String, Topic>,
) -> std::io::Result<()> {
    let bucket = topic_metadata_bucket.iter();
    for item in bucket {
        let item = item.expect("Error destructuring bucket item");
        let topic_name = item.key::<String>().expect("Error fetching key");
        let topic_data = item
            .value::<Topic>()
            .expect("Error fetching topic data for topic");

        let path = get_file_path(&topic_name);

        let file = fs::metadata(path.clone())?;
        let creation_time = file.created()?;
        let duration_since = SystemTime::now()
            .duration_since(creation_time)
            .expect("error comparing time")
            .as_millis();
        let file_size = file.len();
        if file_size >= topic_data.max_size || duration_since >= topic_data.retaintion_time {
            #[allow(unused_variables)]
            let is_removed = fs::remove_file(path.clone());
            match is_removed {
                Ok(_) => {
                    File::create_new(path.clone())?;
                }
                Err(e) => {
                    print!("Error removing file {} {}", path, e);
                }
            }
        }

        println!("{} {:?} {}", topic_name, duration_since, file_size)
    }

    Ok(())
}
