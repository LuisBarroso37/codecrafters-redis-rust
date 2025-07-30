use std::{
    collections::VecDeque,
    sync::Arc,
    time::{SystemTime, SystemTimeError},
};
use tokio::sync::Mutex;

use crate::key_value_store::{DataType, KeyValueStore};

pub async fn validate_stream_id(
    store: &mut Arc<Mutex<KeyValueStore>>,
    key: &str,
    stream_id: &str,
) -> Result<String, String> {
    if stream_id == "*" {
        let millisecond_timestamp = get_timestamp_in_milliseconds()
            .map_err(|_| "System time is before unix epoch".to_string())?;
        let index = get_next_stream_id_index(&store, key, millisecond_timestamp).await?;

        return Ok(format!("{}-{}", millisecond_timestamp, index));
    }

    let split_stream_id = stream_id.split("-").collect::<Vec<&str>>();

    if split_stream_id.len() != 2 {
        return Err("Invalid stream ID format".to_string());
    }

    let first_stream_id_part = split_stream_id[0]
        .parse::<u128>()
        .map_err(|_| "The ID specified in XADD must be greater than 0-0".to_string())?;

    if split_stream_id[1] == "*" {
        let index = get_next_stream_id_index(&store, key, first_stream_id_part).await?;

        return Ok(format!("{}-{}", first_stream_id_part, index));
    }

    let index = split_stream_id[1]
        .parse::<u128>()
        .map_err(|_| "The ID specified in XADD must be greater than 0-0".to_string())?;

    if format!("{}-{}", first_stream_id_part, index) == "0-0" {
        return Err("The ID specified in XADD must be greater than 0-0".to_string());
    }

    let next_index = get_next_stream_id_index(&store, key, first_stream_id_part).await?;

    if index >= next_index {
        Ok(stream_id.to_string())
    } else {
        return Err(
            "The ID specified in XADD is equal or smaller than the target stream top item"
                .to_string(),
        );
    }
}

async fn get_next_stream_id_index(
    store: &Arc<Mutex<KeyValueStore>>,
    key: &str,
    stream_id: u128,
) -> Result<u128, String> {
    let store_guard = store.lock().await;

    if let Some(value) = store_guard.get(key) {
        match value.data {
            DataType::Stream(ref stream) => {
                if let Some(max_key_element) = stream.iter().max_by_key(|s| s.0) {
                    let split_key = max_key_element.0.split("-").collect::<Vec<&str>>();

                    if split_key.len() != 2 {
                        return Err("Invalid stream ID format".to_string());
                    }

                    let first_key_part = split_key[0]
                        .parse::<u128>()
                        .map_err(|_| "Invalid stream ID format".to_string())?;

                    if stream_id == first_key_part {
                        let index = split_key[1]
                            .parse::<u128>()
                            .map_err(|_| "Invalid stream ID format".to_string())?;

                        return Ok(index + 1);
                    } else if stream_id > first_key_part {
                        return Ok(0);
                    } else {
                        return Err("The ID specified in XADD is equal or smaller than the target stream top item".to_string());
                    }
                } else {
                    return Ok(0);
                }
            }
            _ => return Err("Invalid data type for key".to_string()),
        }
    } else {
        if stream_id == 0 {
            return Ok(1);
        } else {
            return Ok(0);
        }
    };
}

pub fn get_timestamp_in_milliseconds() -> Result<u128, SystemTimeError> {
    let current_system_time = SystemTime::now();
    let duration_since_epoch = current_system_time.duration_since(SystemTime::UNIX_EPOCH)?;
    let milliseconds_timestamp = duration_since_epoch.as_millis();

    Ok(milliseconds_timestamp)
}

pub fn validate_range_indexes(
    list: &VecDeque<String>,
    start_index: isize,
    end_index: isize,
) -> Result<(usize, usize), &str> {
    let len = list.len() as isize;

    let mut start = if start_index < 0 {
        len + start_index
    } else {
        start_index
    };
    let mut end = if end_index < 0 {
        len + end_index
    } else {
        end_index
    };

    start = start.max(0);
    end = end.min(len - 1);

    if start >= len {
        return Err("Start index is out of bounds");
    }

    if start > end {
        return Err("Start index is bigger than end index after processing");
    }

    Ok((start as usize, end as usize))
}
