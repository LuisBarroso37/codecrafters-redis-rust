use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{SystemTime, SystemTimeError},
};

use tokio::sync::Mutex;

use crate::{
    commands::command_error::CommandError,
    key_value_store::{DataType, KeyValueStore, Value},
    resp::RespValue,
    state::State,
};

pub async fn xadd(
    store: &mut Arc<Mutex<KeyValueStore>>,
    state: &mut Arc<Mutex<State>>,
    arguments: Vec<String>,
) -> Result<String, CommandError> {
    if arguments.len() < 4 {
        return Err(CommandError::InvalidXAddCommand);
    }

    let store_key = arguments[0].clone();
    let validated_stream_id =
        validate_stream_id_against_store(store, &store_key, arguments[1].as_str())
            .await
            .map_err(|e| {
                if e == "Invalid data type for key" {
                    CommandError::InvalidDataTypeForKey
                } else {
                    CommandError::InvalidStreamId(e)
                }
            })?;
    let key_value_pairs = arguments[2..].to_vec();

    let mut entries = BTreeMap::new();

    for (key, value) in key_value_pairs
        .chunks(2)
        .map(|chunk| (chunk.get(0), chunk.get(1)))
    {
        if let (Some(key), Some(value)) = (key, value) {
            entries.insert(key.clone(), value.clone());
        } else {
            return Err(CommandError::InvalidXAddCommand);
        }
    }

    let mut store_guard = store.lock().await;

    if let Some(value) = store_guard.get_mut(&store_key) {
        if let DataType::Stream(ref mut stream) = value.data {
            stream.insert(validated_stream_id.clone(), entries);
        } else {
            return Err(CommandError::InvalidDataTypeForKey);
        }
    } else {
        store_guard.insert(
            store_key.clone(),
            Value {
                data: DataType::Stream(BTreeMap::from([(validated_stream_id.clone(), entries)])),
                expiration: None,
            },
        );
    }

    let mut state_guard = state.lock().await;
    state_guard.send_to_subscriber("XREAD", &store_key, true);

    Ok(RespValue::BulkString(validated_stream_id).encode())
}

async fn validate_stream_id_against_store(
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

fn get_timestamp_in_milliseconds() -> Result<u128, SystemTimeError> {
    let current_system_time = SystemTime::now();
    let duration_since_epoch = current_system_time.duration_since(SystemTime::UNIX_EPOCH)?;
    let milliseconds_timestamp = duration_since_epoch.as_millis();

    Ok(milliseconds_timestamp)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, HashMap},
        sync::Arc,
    };

    use tokio::sync::Mutex;

    use crate::key_value_store::{DataType, Value};

    use super::{get_timestamp_in_milliseconds, validate_stream_id_against_store};
    #[test]
    fn test_get_timestamp_in_milliseconds() {
        assert!(get_timestamp_in_milliseconds().is_ok());
    }

    #[tokio::test]
    async fn test_validate_stream_id_against_store() {
        let mut store = Arc::new(Mutex::new(HashMap::from([
            (
                "fruits".to_string(),
                Value {
                    data: DataType::Stream(BTreeMap::from([
                        (
                            "0-0".to_string(),
                            BTreeMap::from([("apple".to_string(), "mango".to_string())]),
                        ),
                        (
                            "1-1".to_string(),
                            BTreeMap::from([("raspberry".to_string(), "apple".to_string())]),
                        ),
                    ])),
                    expiration: None,
                },
            ),
            (
                "sensor".to_string(),
                Value {
                    data: DataType::Stream(BTreeMap::from([(
                        "1526919030474-0".to_string(),
                        BTreeMap::from([("temperature".to_string(), "37".to_string())]),
                    )])),
                    expiration: None,
                },
            ),
        ])));

        let test_cases = vec![
            (
                "key",
                "stream_id",
                Err("Invalid stream ID format".to_string()),
            ),
            (
                "key",
                "invalid",
                Err("Invalid stream ID format".to_string()),
            ),
            ("key", "-1-1", Err("Invalid stream ID format".to_string())),
            (
                "key",
                "invalid-1",
                Err("The ID specified in XADD must be greater than 0-0".to_string()),
            ),
            (
                "key",
                "1-invalid",
                Err("The ID specified in XADD must be greater than 0-0".to_string()),
            ),
            (
                "key",
                "0-0",
                Err("The ID specified in XADD must be greater than 0-0".to_string()),
            ),
            (
                "fruits",
                "0-2",
                Err(
                    "The ID specified in XADD is equal or smaller than the target stream top item"
                        .to_string(),
                ),
            ),
            (
                "fruits",
                "1-0",
                Err(
                    "The ID specified in XADD is equal or smaller than the target stream top item"
                        .to_string(),
                ),
            ),
            (
                "sensor",
                "1526919030474-0",
                Err(
                    "The ID specified in XADD is equal or smaller than the target stream top item"
                        .to_string(),
                ),
            ),
            (
                "sensor",
                "1526919030474-*",
                Ok("1526919030474-1".to_string()),
            ),
            (
                "sensor",
                "1526919030474-1",
                Ok("1526919030474-1".to_string()),
            ),
            (
                "sensor",
                "1526919030474-2",
                Ok("1526919030474-2".to_string()),
            ),
            (
                "sensor",
                "1526919030484-0",
                Ok("1526919030484-0".to_string()),
            ),
            (
                "sensor",
                "1526919030484-1",
                Ok("1526919030484-1".to_string()),
            ),
            ("key", "0-*", Ok("0-1".to_string())),
        ];

        for (key, stream_id, expected_result) in test_cases {
            assert_eq!(
                validate_stream_id_against_store(&mut store, key, stream_id).await,
                expected_result
            );
        }

        assert!(
            validate_stream_id_against_store(&mut store, "sensor", "*")
                .await
                .is_ok()
        );
    }
}
