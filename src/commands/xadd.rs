use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{SystemTime, SystemTimeError},
};

use tokio::sync::Mutex;

use crate::{
    commands::{command_error::CommandError, command_handler::CommandResult, validate_stream_id},
    key_value_store::{DataType, KeyValueStore, Value},
    resp::RespValue,
    state::State,
};

pub struct XaddArguments {
    key: String,
    stream_id: String,
    entries: BTreeMap<String, String>,
}

impl XaddArguments {
    pub fn parse(arguments: Vec<String>) -> Result<Self, CommandError> {
        if arguments.len() < 4 {
            return Err(CommandError::InvalidXAddCommand);
        }

        if arguments[2..].len() % 2 != 0 {
            return Err(CommandError::InvalidXAddCommand);
        }

        Ok(Self {
            key: arguments[0].clone(),
            stream_id: arguments[1].clone(),
            entries: arguments[2..]
                .chunks(2)
                .map(|chunk| (chunk[0].clone(), chunk[1].clone()))
                .collect::<BTreeMap<String, String>>(),
        })
    }
}

pub async fn xadd(
    store: Arc<Mutex<KeyValueStore>>,
    state: Arc<Mutex<State>>,
    arguments: Vec<String>,
) -> Result<CommandResult, CommandError> {
    let xadd_arguments = XaddArguments::parse(arguments)?;

    let validated_stream_id = validate_stream_id_against_store(
        Arc::clone(&store),
        &xadd_arguments.key,
        &xadd_arguments.stream_id,
    )
    .await
    .map_err(|e| match e.as_str() {
        "Invalid data type for key" => CommandError::InvalidDataTypeForKey,
        _ => CommandError::InvalidStreamId(e),
    })?;

    let mut store_guard = store.lock().await;

    match store_guard.get_mut(&xadd_arguments.key) {
        Some(value) => {
            let DataType::Stream(ref mut stream) = value.data else {
                return Err(CommandError::InvalidDataTypeForKey);
            };

            stream.insert(validated_stream_id.clone(), xadd_arguments.entries);
        }
        None => {
            store_guard.insert(
                xadd_arguments.key.clone(),
                Value {
                    data: DataType::Stream(BTreeMap::from([(
                        validated_stream_id.clone(),
                        xadd_arguments.entries,
                    )])),
                    expiration: None,
                },
            );
        }
    };

    let mut state_guard = state.lock().await;
    state_guard.send_to_xread_subscribers(&xadd_arguments.key, &validated_stream_id, true)?;

    Ok(CommandResult::Response(
        RespValue::BulkString(validated_stream_id).encode(),
    ))
}

async fn validate_stream_id_against_store(
    store: Arc<Mutex<KeyValueStore>>,
    key: &str,
    stream_id: &str,
) -> Result<String, String> {
    if stream_id == "*" {
        let timestamp = get_timestamp_in_milliseconds()
            .map_err(|_| "System time is before unix epoch".to_string())?;
        let sequence = get_next_sequence_for_timestamp(store, key, timestamp).await?;

        return Ok(format!("{}-{}", timestamp, sequence));
    }

    let (timestamp, sequence_part) = parse_stream_id_parts(stream_id)?;

    if sequence_part == "*" {
        let sequence = get_next_sequence_for_timestamp(store, key, timestamp).await?;

        return Ok(format!("{}-{}", timestamp, sequence));
    }

    let sequence = sequence_part
        .parse::<u128>()
        .map_err(|_| "The ID specified in XADD must be greater than 0-0".to_string())?;

    if timestamp == 0 && sequence == 0 {
        return Err("The ID specified in XADD must be greater than 0-0".to_string());
    }

    validate_against_existing_entries(store, key, timestamp, sequence).await?;

    Ok(stream_id.to_string())
}

fn parse_stream_id_parts(stream_id: &str) -> Result<(u128, &str), String> {
    let parts = stream_id.split('-').collect::<Vec<&str>>();

    if parts.len() != 2 || parts[0].is_empty() || parts[1].is_empty() {
        return Err("Invalid stream ID format".to_string());
    }

    let timestamp = parts[0]
        .parse::<u128>()
        .map_err(|_| "The ID specified in XADD must be greater than 0-0")?;

    Ok((timestamp, parts[1]))
}

async fn validate_against_existing_entries(
    store: Arc<Mutex<KeyValueStore>>,
    key: &str,
    timestamp: u128,
    sequence: u128,
) -> Result<(), String> {
    let min_sequence = get_next_sequence_for_timestamp(store, key, timestamp).await?;

    if sequence < min_sequence {
        return Err(
            "The ID specified in XADD is equal or smaller than the target stream top item"
                .to_string(),
        );
    }

    Ok(())
}

async fn get_next_sequence_for_timestamp(
    store: Arc<Mutex<KeyValueStore>>,
    key: &str,
    timestamp: u128,
) -> Result<u128, String> {
    let store_guard = store.lock().await;

    let Some(value) = store_guard.get(key) else {
        if timestamp == 0 {
            return Ok(1);
        } else {
            return Ok(0);
        }
    };

    let DataType::Stream(ref stream) = value.data else {
        return Err("Invalid data type for key".to_string());
    };

    let Some(max_stream_id) = stream.keys().max() else {
        return Ok(0);
    };

    let (max_timestamp, max_sequence) = validate_stream_id(max_stream_id, true)?;

    let Some(sequence) = max_sequence else {
        return Err("Invalid stream ID format".to_string());
    };

    if timestamp == max_timestamp {
        Ok(sequence + 1)
    } else if timestamp > max_timestamp {
        Ok(0)
    } else {
        Err(
            "The ID specified in XADD is equal or smaller than the target stream top item"
                .to_string(),
        )
    }
}

fn get_timestamp_in_milliseconds() -> Result<u128, SystemTimeError> {
    let current_system_time = SystemTime::now();
    let duration_since_epoch = current_system_time.duration_since(SystemTime::UNIX_EPOCH)?;
    let milliseconds_timestamp = duration_since_epoch.as_millis();

    Ok(milliseconds_timestamp)
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use tokio::sync::Mutex;

    use crate::key_value_store::{DataType, KeyValueStore, Value};

    use super::{
        get_next_sequence_for_timestamp, get_timestamp_in_milliseconds, parse_stream_id_parts,
        validate_against_existing_entries, validate_stream_id_against_store,
    };

    #[test]
    fn test_get_timestamp_in_milliseconds() {
        assert_eq!(get_timestamp_in_milliseconds().is_ok(), true);
    }

    #[test]
    fn test_parse_stream_id_parts() {
        let test_cases = vec![
            ("1234-5", Ok((1234, "5"))),
            ("0-0", Ok((0, "0"))),
            ("1526919030474-0", Ok((1526919030474, "0"))),
            ("123-*", Ok((123, "*"))),
            (
                "999999999999999999999-123",
                Ok((999999999999999999999, "123")),
            ),
            ("invalid", Err("Invalid stream ID format".to_string())),
            ("", Err("Invalid stream ID format".to_string())),
            ("123", Err("Invalid stream ID format".to_string())),
            ("123-456-789", Err("Invalid stream ID format".to_string())),
            ("-123", Err("Invalid stream ID format".to_string())),
            ("123-", Err("Invalid stream ID format".to_string())),
            (
                "invalid-123",
                Err("The ID specified in XADD must be greater than 0-0".to_string()),
            ),
        ];

        for (input, expected) in test_cases {
            let result = parse_stream_id_parts(input);
            assert_eq!(result, expected, "Failed for input: {}", input);
        }
    }

    #[tokio::test]
    async fn test_get_next_sequence_for_timestamp() {
        let test_cases = vec![
            (KeyValueStore::new(), "nonexistent", 0, Ok(1)),
            (KeyValueStore::new(), "nonexistent", 1234, Ok(0)),
            (
                KeyValueStore::from([(
                    "stream1".to_string(),
                    Value {
                        data: DataType::Stream(BTreeMap::from([(
                            "0-1".to_string(),
                            BTreeMap::new(),
                        )])),
                        expiration: None,
                    },
                )]),
                "stream1",
                0,
                Ok(2),
            ),
            (
                KeyValueStore::from([(
                    "stream1".to_string(),
                    Value {
                        data: DataType::Stream(BTreeMap::from([(
                            "0-1".to_string(),
                            BTreeMap::new(),
                        )])),
                        expiration: None,
                    },
                )]),
                "stream1",
                1,
                Ok(0),
            ),
            (
                KeyValueStore::from([(
                    "stream2".to_string(),
                    Value {
                        data: DataType::Stream(BTreeMap::from([(
                            "1234-5".to_string(),
                            BTreeMap::new(),
                        )])),
                        expiration: None,
                    },
                )]),
                "stream2",
                1234,
                Ok(6),
            ),
            (
                KeyValueStore::from([(
                    "stream2".to_string(),
                    Value {
                        data: DataType::Stream(BTreeMap::from([(
                            "1234-5".to_string(),
                            BTreeMap::new(),
                        )])),
                        expiration: None,
                    },
                )]),
                "stream2",
                1235,
                Ok(0),
            ),
            (
                KeyValueStore::from([(
                    "stream2".to_string(),
                    Value {
                        data: DataType::Stream(BTreeMap::from([(
                            "1234-5".to_string(),
                            BTreeMap::new(),
                        )])),
                        expiration: None,
                    },
                )]),
                "stream2",
                1233,
                Err(
                    "The ID specified in XADD is equal or smaller than the target stream top item"
                        .to_string(),
                ),
            ),
            (
                KeyValueStore::from([(
                    "string_key".to_string(),
                    Value {
                        data: DataType::String("not a stream".to_string()),
                        expiration: None,
                    },
                )]),
                "string_key",
                1234,
                Err("Invalid data type for key".to_string()),
            ),
        ];

        for (store_data, key, timestamp, expected_sequence) in test_cases {
            let store = Arc::new(Mutex::new(store_data));
            let result = get_next_sequence_for_timestamp(store, key, timestamp).await;
            assert_eq!(
                result, expected_sequence,
                "Failed for key: {}, timestamp: {}",
                key, timestamp
            );
        }
    }

    #[tokio::test]
    async fn test_validate_against_existing_entries() {
        let test_cases = vec![
            (KeyValueStore::new(), "nonexistent", 1234, 0, Ok(())),
            (KeyValueStore::new(), "nonexistent", 0, 1, Ok(())),
            (
                KeyValueStore::from([(
                    "stream1".to_string(),
                    Value {
                        data: DataType::Stream(BTreeMap::from([(
                            "1234-5".to_string(),
                            BTreeMap::new(),
                        )])),
                        expiration: None,
                    },
                )]),
                "stream1",
                1234,
                6,
                Ok(()),
            ),
            (
                KeyValueStore::from([(
                    "stream1".to_string(),
                    Value {
                        data: DataType::Stream(BTreeMap::from([(
                            "1234-5".to_string(),
                            BTreeMap::new(),
                        )])),
                        expiration: None,
                    },
                )]),
                "stream1",
                1234,
                5,
                Err(
                    "The ID specified in XADD is equal or smaller than the target stream top item"
                        .to_string(),
                ),
            ),
            (
                KeyValueStore::from([(
                    "stream1".to_string(),
                    Value {
                        data: DataType::Stream(BTreeMap::from([(
                            "1234-5".to_string(),
                            BTreeMap::new(),
                        )])),
                        expiration: None,
                    },
                )]),
                "stream1",
                1234,
                4,
                Err(
                    "The ID specified in XADD is equal or smaller than the target stream top item"
                        .to_string(),
                ),
            ),
            (
                KeyValueStore::from([(
                    "stream1".to_string(),
                    Value {
                        data: DataType::Stream(BTreeMap::from([(
                            "1234-5".to_string(),
                            BTreeMap::new(),
                        )])),
                        expiration: None,
                    },
                )]),
                "stream1",
                1235,
                0,
                Ok(()),
            ),
        ];

        for (store_data, key, timestamp, sequence, expected) in test_cases {
            let store = Arc::new(Mutex::new(store_data));
            let result = validate_against_existing_entries(store, key, timestamp, sequence).await;
            assert_eq!(
                result, expected,
                "Failed for key: {}, timestamp: {}, sequence: {}",
                key, timestamp, sequence
            );
        }
    }

    #[tokio::test]
    async fn test_validate_stream_id_against_store() {
        let store = Arc::new(Mutex::new(KeyValueStore::from([
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
            (
                "string_key".to_string(),
                Value {
                    data: DataType::String("not a stream".to_string()),
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
            ("key", "0-*", Ok("0-1".to_string())),
            ("key", "1234-*", Ok("1234-0".to_string())),
            ("nonexistent", "1234-5", Ok("1234-5".to_string())),
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
            ("fruits", "1-2", Ok("1-2".to_string())),
            ("fruits", "2-0", Ok("2-0".to_string())),
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
            (
                "string_key",
                "1234-5",
                Err("Invalid data type for key".to_string()),
            ),
        ];

        for (key, stream_id, expected) in test_cases {
            let result = validate_stream_id_against_store(Arc::clone(&store), key, stream_id).await;
            assert_eq!(
                result, expected,
                "Failed for key: {}, stream_id: {}",
                key, stream_id
            );
        }

        // Test auto-generation with "*"
        let result = validate_stream_id_against_store(store, "sensor", "*").await;
        assert!(result.is_ok());

        // Should be greater than existing entry
        let generated_id = result.unwrap();
        let parts: Vec<&str> = generated_id.split('-').collect();
        let timestamp: u128 = parts[0].parse().unwrap();
        assert!(timestamp >= 1526919030474);
    }
}
