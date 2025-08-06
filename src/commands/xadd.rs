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

/// Handles the Redis XADD command.
///
/// Adds a new entry to a Redis stream with the specified ID and field-value pairs.
/// If the stream doesn't exist, it creates a new one. The stream ID must be greater
/// than the last ID in the stream to maintain ordering.
///
/// # Arguments
///
/// * `store` - A thread-safe reference to the key-value store
/// * `state` - A thread-safe reference to the server state (for XREAD notifications)
/// * `arguments` - A vector containing: [key, stream_id, field1, value1, field2, value2, ...]
///
/// # Returns
///
/// * `Ok(String)` - A RESP-encoded bulk string containing the generated stream ID
/// * `Err(CommandError::InvalidXAddCommand)` - If fewer than 4 arguments provided
/// * `Err(CommandError::InvalidDataTypeForKey)` - If key exists but is not a stream
/// * `Err(CommandError::InvalidStreamId)` - If the stream ID is invalid or out of order
///
/// # Examples
///
/// ```
/// // XADD mystream * temperature 25 humidity 60
/// let result = xadd(&mut store, &mut state, vec![
///     "mystream".to_string(), "*".to_string(),
///     "temperature".to_string(), "25".to_string(),
///     "humidity".to_string(), "60".to_string()
/// ]).await;
/// // Returns: "$19\r\n1518951480106-0\r\n" (generated stream ID)
/// ```
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
    state_guard.send_to_xread_subscribers(&store_key, &validated_stream_id, true)?;

    Ok(RespValue::BulkString(validated_stream_id).encode())
}

/// Validates and generates a stream ID for use in XADD operations.
///
/// Handles both explicit stream IDs and auto-generation using "*".
/// Ensures that the new stream ID is greater than existing IDs in the stream
/// to maintain chronological ordering.
///
/// # Arguments
///
/// * `store` - A thread-safe reference to the key-value store
/// * `key` - The stream key to validate against
/// * `stream_id` - The stream ID to validate ("*" for auto-generation or "timestamp-sequence")
///
/// # Returns
///
/// * `Ok(String)` - A validated stream ID in format "timestamp-sequence"
/// * `Err(String)` - Error message if the stream ID is invalid or out of order
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

/// Calculates the next sequence number for a stream ID with a given timestamp.
///
/// For auto-generated stream IDs, this function determines what sequence number
/// should be used for a given timestamp, ensuring uniqueness and proper ordering.
///
/// # Arguments
///
/// * `store` - A thread-safe reference to the key-value store
/// * `key` - The stream key to check for existing entries
/// * `stream_id` - The timestamp part of the stream ID
///
/// # Returns
///
/// * `Ok(u128)` - The next available sequence number for this timestamp
/// * `Err(String)` - Error message if the stream contains invalid data
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

/// Gets the current system time as milliseconds since Unix epoch.
///
/// Used for generating the timestamp part of auto-generated stream IDs.
/// This ensures that stream entries are naturally ordered by creation time.
///
/// # Returns
///
/// * `Ok(u128)` - Current time in milliseconds since Unix epoch
/// * `Err(SystemTimeError)` - If system time is before Unix epoch (should not happen in practice)
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
