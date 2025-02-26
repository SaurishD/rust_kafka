pub fn get_file_path(topic_name: &String) -> String {
    format!("topics/{}.log", topic_name)
}
