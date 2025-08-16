use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{SystemTime, SystemTimeError},
};

use tokio::sync::Mutex;

use crate::{
    commands::{command_error::CommandError, validate_stream_id},
    key_value_store::{DataType, KeyValueStore, Value},
    resp::RespValue,
    state::State,
};

/// Represents the parsed arguments for XADD command
pub struct XaddArguments {
    /// The Redis stream key where the entry will be added
    key: String,
    /// The stream ID for the new entry ("*" for auto-generation or "timestamp-sequence")
    stream_id: String,
    /// Field-value pairs that make up the stream entry content
    entries: BTreeMap<String, String>,
}

impl XaddArguments {
    /// Parses command arguments into an `XaddArguments` struct.
    ///
    /// Validates that the minimum required arguments are provided and that
    /// field-value pairs are properly matched (even number of field/value arguments).
    ///
    /// # Arguments
    ///
    /// * `arguments` - Vector of command arguments [key, stream_id, field1, value1, field2, value2, ...]
    ///
    /// # Returns
    ///
    /// * `Ok(XaddArguments)` - Successfully parsed arguments
    /// * `Err(CommandError::InvalidXAddCommand)` - If fewer than 4 arguments provided or
    ///   field-value pairs don't match (odd number of field/value arguments)
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let args = vec!["mystream".to_string(), "*".to_string(), "temp".to_string(), "25".to_string()];
    /// let parsed = XaddArguments::parse(args)?;
    /// assert_eq!(parsed.key, "mystream");
    /// assert_eq!(parsed.stream_id, "*");
    /// assert_eq!(parsed.entries.get("temp"), Some(&"25".to_string()));
    /// ```
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
/// ```ignore
/// // XADD mystream * temperature 25 humidity 60
/// let result = xadd(&mut store, &mut state, vec![
///     "mystream".to_string(), "*".to_string(),
///     "temperature".to_string(), "25".to_string(),
///     "humidity".to_string(), "60".to_string()
/// ]).await;
/// // Returns: "$19\r\n1518951480106-0\r\n" (generated stream ID)
/// ```
pub async fn xadd(
    store: Arc<Mutex<KeyValueStore>>,
    state: Arc<Mutex<State>>,
    arguments: Vec<String>,
) -> Result<String, CommandError> {
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

/// Parses a stream ID into its timestamp and sequence components.
///
/// Splits a stream ID string on the hyphen delimiter and validates the format.
/// The timestamp part must be a valid u128, while the sequence part is returned
/// as a string slice to handle both numeric values and "*" for auto-generation.
///
/// # Arguments
///
/// * `stream_id` - The stream ID string to parse (format: "timestamp-sequence")
///
/// # Returns
///
/// * `Ok((u128, &str))` - Parsed timestamp and sequence part as string slice
/// * `Err(String)` - Error message if the format is invalid or timestamp cannot be parsed
///
/// # Examples
///
/// ```ignore
/// let (timestamp, sequence) = parse_stream_id_parts("1234567890-5")?;
/// assert_eq!(timestamp, 1234567890);
/// assert_eq!(sequence, "5");
///
/// let (timestamp, sequence) = parse_stream_id_parts("1234567890-*")?;
/// assert_eq!(timestamp, 1234567890);
/// assert_eq!(sequence, "*");
/// ```
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

/// Validates an explicit stream ID against existing entries in the stream.
///
/// Ensures that the proposed stream ID is greater than all existing stream IDs
/// in the target stream to maintain chronological ordering. This is called for
/// explicit stream IDs (not auto-generated ones).
///
/// # Arguments
///
/// * `store` - A thread-safe reference to the key-value store
/// * `key` - The stream key to validate against
/// * `timestamp` - The timestamp part of the proposed stream ID
/// * `sequence` - The sequence part of the proposed stream ID
///
/// # Returns
///
/// * `Ok(())` - The stream ID is valid and can be used
/// * `Err(String)` - Error message if the stream ID would violate ordering constraints
///
/// # Examples
///
/// ```ignore
/// // Assuming stream has entries up to "1234-5"
/// validate_against_existing_entries(&store, "mystream", 1234, 6).await?; // OK
/// validate_against_existing_entries(&store, "mystream", 1235, 0).await?; // OK
/// validate_against_existing_entries(&store, "mystream", 1234, 4).await; // Error: too small
/// ```
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

/// Calculates the next sequence number for a stream ID with a given timestamp.
///
/// For auto-generated stream IDs, this function determines what sequence number
/// should be used for a given timestamp, ensuring uniqueness and proper ordering.
///
/// # Arguments
///
/// * `store` - A thread-safe reference to the key-value store
/// * `key` - The stream key to check for existing entries
/// * `timestamp` - The timestamp part of the stream ID
///
/// # Returns
///
/// * `Ok(u128)` - The next available sequence number for this timestamp
/// * `Err(String)` - Error message if the stream contains invalid data
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
        assert_eq!(result.is_ok(), true);

        // Should be greater than existing entry
        let generated_id = result.unwrap();
        let parts: Vec<&str> = generated_id.split('-').collect();
        let timestamp: u128 = parts[0].parse().unwrap();
        assert_eq!(timestamp >= 1526919030474, true);
    }
}
