use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::Mutex;

use crate::{
    commands::{
        command_error::CommandError,
        stream_utils::{parse_stream_entries_to_resp, validate_stream_id},
    },
    key_value_store::{DataType, KeyValueStore, Stream},
    resp::RespValue,
};

/// Represents the parsed arguments for the XRANGE command.
///
/// The XRANGE command in Redis returns a range of entries from a stream stored at the given key.
/// This struct holds the key and the start and end stream IDs for the range operation.
pub struct XrangeArguments {
    ///  The name of the stream key to operate on
    key: String,
    /// The starting stream ID for the range (can be "-" for the beginning)
    start_stream_id: String,
    /// The ending stream ID for the range (can be "+" for the end)
    end_stream_id: String,
}

impl XrangeArguments {
    /// Parses and validates the arguments for the XRANGE command.
    ///
    /// The XRANGE command requires exactly three arguments: the key, the start stream ID, and the end stream ID.
    /// This function checks the argument count and clones the arguments into the struct fields.
    ///
    /// # Arguments
    ///
    /// * `arguments` - A vector of command arguments: [key, start_stream_id, end_stream_id]
    ///
    /// # Returns
    ///
    /// * `Ok(XrangeArguments)` - If the arguments are valid
    /// * `Err(CommandError::InvalidXRangeCommand)` - If the number of arguments is not exactly 3
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Valid usage
    /// let args = XrangeArguments::parse(vec![
    ///     "mystream".to_string(),
    ///     "-".to_string(),
    ///     "+".to_string()
    /// ]).unwrap();
    /// assert_eq!(args.key, "mystream");
    /// assert_eq!(args.start_stream_id, "-");
    /// assert_eq!(args.end_stream_id, "+");
    ///
    /// // Invalid usage: not enough arguments
    /// let err = XrangeArguments::parse(vec!["mystream".to_string()]);
    /// assert!(err.is_err());
    /// ```
    pub fn parse(arguments: Vec<String>) -> Result<Self, CommandError> {
        if arguments.len() != 3 {
            return Err(CommandError::InvalidXRangeCommand);
        }

        let key = arguments[0].clone();
        let start_stream_id = arguments[1].clone();
        let end_stream_id = arguments[2].clone();

        Ok(Self {
            key,
            start_stream_id,
            end_stream_id,
        })
    }
}

/// Handles the Redis XRANGE command.
///
/// Returns a range of entries from a Redis stream between two stream IDs.
/// Supports special values "-" (start from beginning) and "+" (end at latest).
/// The range is inclusive on both ends.
///
/// # Arguments
///
/// * `store` - A thread-safe reference to the key-value store
/// * `arguments` - A vector containing exactly 3 elements: [key, start_id, end_id]
///
/// # Returns
///
/// * `Ok(String)` - A RESP-encoded array containing the matching stream entries
/// * `Err(CommandError::InvalidXRangeCommand)` - If the number of arguments is not exactly 3
/// * `Err(CommandError::InvalidDataTypeForKey)` - If key exists but is not a stream
/// * `Err(CommandError::InvalidStreamId)` - If start or end stream ID is invalid
///
/// # Examples
///
/// ```ignore
/// // XRANGE mystream - +  (get all entries)
/// let result = xrange(&mut store, vec!["mystream".to_string(), "-".to_string(), "+".to_string()]).await;
/// // Returns: "*2\r\n*2\r\n$15\r\n1518951480106-0\r\n*4\r\n$4\r\ntemp\r\n$2\r\n25\r\n..."
///
/// // XRANGE mystream 1518951480106-0 1518951480107-0  (get entries in range)
/// let result = xrange(&mut store, vec![
///     "mystream".to_string(),
///     "1518951480106-0".to_string(),
///     "1518951480107-0".to_string()
/// ]).await;
/// // Returns: "*1\r\n*2\r\n$15\r\n1518951480106-0\r\n*4\r\n$4\r\ntemp\r\n$2\r\n25\r\n..."
/// ```
pub async fn xrange(
    store: &mut Arc<Mutex<KeyValueStore>>,
    arguments: Vec<String>,
) -> Result<String, CommandError> {
    let xrange_arguments = XrangeArguments::parse(arguments)?;

    let store_guard = store.lock().await;

    let Some(value) = store_guard.get(&xrange_arguments.key) else {
        return Err(CommandError::DataNotFound);
    };

    let DataType::Stream(ref stream) = value.data else {
        return Err(CommandError::InvalidDataTypeForKey);
    };

    let Some(start_stream_id) =
        validate_start_stream_id(stream, &xrange_arguments.start_stream_id)?
    else {
        return Ok(RespValue::Array(Vec::with_capacity(0)).encode());
    };
    let Some(end_stream_id) = validate_end_stream_id(stream, &xrange_arguments.end_stream_id)?
    else {
        return Ok(RespValue::Array(Vec::with_capacity(0)).encode());
    };

    let entries = stream
        .iter()
        .filter_map(|(id, entries)| {
            let stream_id = validate_stream_id(id, true).ok()?;

            if is_stream_id_in_range(&stream_id, &start_stream_id, &end_stream_id) {
                Some((id, entries))
            } else {
                None
            }
        })
        .collect::<Vec<(&String, &Stream)>>();

    let resp_value = parse_stream_entries_to_resp(entries);
    return Ok(resp_value.encode());
}

/// Validates and resolves the start stream ID for XRANGE operations.
///
/// Handles the special case of "-" which represents the beginning of the stream,
/// as well as explicit stream IDs. When "-" is provided, it finds the minimum
/// (earliest) stream ID in the stream and validates it.
///
/// # Arguments
///
/// * `stream` - A reference to the BTreeMap representing the Redis stream
/// * `start_stream_id` - The start stream ID string ("-" for beginning or explicit ID)
///
/// # Returns
///
/// * `Ok(Some((u128, Option<u128>)))` - Successfully validated stream ID as (timestamp, sequence)
/// * `Ok(None)` - When "-" is used but the stream is empty
/// * `Err(CommandError::InvalidStreamId)` - When the stream ID format is invalid
///
/// # Examples
///
/// ```ignore
/// // For an empty stream
/// let result = validate_start_stream_id(&empty_stream, "-");
/// assert_eq!(result, Ok(None));
///
/// // For "-" with existing entries
/// let result = validate_start_stream_id(&stream, "-");
/// assert_eq!(result, Ok(Some((1234567890, Some(0)))));
///
/// // For explicit stream ID
/// let result = validate_start_stream_id(&stream, "1234567890-5");
/// assert_eq!(result, Ok(Some((1234567890, Some(5)))));
/// ```
fn validate_start_stream_id(
    stream: &BTreeMap<String, Stream>,
    start_stream_id: &str,
) -> Result<Option<(u128, Option<u128>)>, CommandError> {
    match start_stream_id {
        "-" => {
            let Some(min_stream_id) = stream.keys().min() else {
                return Ok(None);
            };

            let validated_stream_id = validate_stream_id(min_stream_id, true)
                .map_err(|e| CommandError::InvalidStreamId(e))?;

            Ok(Some(validated_stream_id))
        }
        stream_id => {
            let validated_stream_id = validate_stream_id(stream_id, true)
                .map_err(|e| CommandError::InvalidStreamId(e))?;

            Ok(Some(validated_stream_id))
        }
    }
}

/// Validates and resolves the end stream ID for XRANGE operations.
///
/// Handles the special case of "+" which represents the end of the stream,
/// as well as explicit stream IDs. When "+" is provided, it finds the maximum
/// (latest) stream ID in the stream and validates it.
///
/// # Arguments
///
/// * `stream` - A reference to the BTreeMap representing the Redis stream
/// * `end_stream_id` - The end stream ID string ("+" for end or explicit ID)
///
/// # Returns
///
/// * `Ok(Some((u128, Option<u128>)))` - Successfully validated stream ID as (timestamp, sequence)
/// * `Ok(None)` - When "+" is used but the stream is empty
/// * `Err(CommandError::InvalidStreamId)` - When the stream ID format is invalid
///
/// # Examples
///
/// ```ignore
/// // For an empty stream
/// let result = validate_end_stream_id(&empty_stream, "+");
/// assert_eq!(result, Ok(None));
///
/// // For "+" with existing entries
/// let result = validate_end_stream_id(&stream, "+");
/// assert_eq!(result, Ok(Some((1234567890, Some(10)))));
///
/// // For explicit stream ID
/// let result = validate_end_stream_id(&stream, "1234567890-5");
/// assert_eq!(result, Ok(Some((1234567890, Some(5)))));
/// ```
fn validate_end_stream_id(
    stream: &BTreeMap<String, Stream>,
    end_stream_id: &str,
) -> Result<Option<(u128, Option<u128>)>, CommandError> {
    match end_stream_id {
        "+" => {
            let Some(max_stream_id) = stream.keys().max() else {
                return Ok(None);
            };

            let validated_stream_id = validate_stream_id(max_stream_id, true)
                .map_err(|e| CommandError::InvalidStreamId(e))?;

            Ok(Some(validated_stream_id))
        }
        stream_id => {
            let validated_stream_id = validate_stream_id(stream_id, true)
                .map_err(|e| CommandError::InvalidStreamId(e))?;

            Ok(Some(validated_stream_id))
        }
    }
}

/// Checks if a stream ID falls within a given range (inclusive).
///
/// Compares stream IDs which consist of a timestamp and optional sequence number.
/// The comparison is done first by timestamp, then by sequence number if timestamps are equal.
///
/// # Arguments
///
/// * `stream_id` - The stream ID to check (timestamp, sequence)
/// * `start_stream_id` - The start of the range (inclusive)
/// * `end_stream_id` - The end of the range (inclusive)
///
/// # Returns
///
/// * `bool` - true if the stream ID is within the range, false otherwise
fn is_stream_id_in_range(
    stream_id: &(u128, Option<u128>),
    start_stream_id: &(u128, Option<u128>),
    end_stream_id: &(u128, Option<u128>),
) -> bool {
    if stream_id.0 > start_stream_id.0 && stream_id.0 < end_stream_id.0 {
        return true;
    }

    if stream_id.0 == start_stream_id.0 && stream_id.0 == end_stream_id.0 {
        return is_sequence_in_range(&stream_id.1, &start_stream_id.1, &end_stream_id.1);
    }

    if stream_id.0 == start_stream_id.0 {
        return is_sequence_at_or_after(&stream_id.1, &start_stream_id.1);
    }

    if stream_id.0 == end_stream_id.0 {
        return is_sequence_at_or_before(&stream_id.1, &end_stream_id.1);
    }

    false
}

/// Checks if a sequence number falls within a given range (inclusive).
///
/// Handles the case where sequence numbers might be None (representing 0).
/// Used when comparing stream IDs with the same timestamp.
///
/// # Arguments
///
/// * `sequence` - The sequence number to check
/// * `start_sequence` - The start of the range (inclusive)
/// * `end_sequence` - The end of the range (inclusive)
///
/// # Returns
///
/// * `bool` - true if the sequence is within the range, false otherwise
fn is_sequence_in_range(
    sequence: &Option<u128>,
    start_sequence: &Option<u128>,
    end_sequence: &Option<u128>,
) -> bool {
    match (sequence, start_sequence, end_sequence) {
        (Some(s), Some(start), Some(end)) => s >= start && s <= end,
        (Some(s), None, Some(end)) => s <= end,
        (Some(s), Some(start), None) => s >= start,
        (Some(_), None, None) => true,
        _ => false,
    }
}

/// Checks if a sequence number is greater than or equal to a start sequence.
///
/// Used for range boundary checking when the timestamp matches the start timestamp.
/// None is treated as 0 for comparison purposes.
///
/// # Arguments
///
/// * `sequence` - The sequence number to check
/// * `start_sequence` - The minimum sequence number (inclusive)
///
/// # Returns
///
/// * `bool` - true if sequence >= start_sequence, false otherwise
fn is_sequence_at_or_after(sequence: &Option<u128>, start_sequence: &Option<u128>) -> bool {
    match (sequence, start_sequence) {
        (Some(s), Some(start)) => s >= start,
        (Some(_), None) => true,
        _ => false,
    }
}

/// Checks if a sequence number is less than or equal to an end sequence.
///
/// Used for range boundary checking when the timestamp matches the end timestamp.
/// None is treated as 0 for comparison purposes.
///
/// # Arguments
///
/// * `sequence` - The sequence number to check
/// * `end_sequence` - The maximum sequence number (inclusive)
///
/// # Returns
///
/// * `bool` - true if sequence <= end_sequence, false otherwise
fn is_sequence_at_or_before(sequence: &Option<u128>, end_sequence: &Option<u128>) -> bool {
    match (sequence, end_sequence) {
        (Some(s), Some(end)) => s <= end,
        (Some(_), None) => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::commands::CommandError;

    use super::{
        is_sequence_at_or_after, is_sequence_at_or_before, is_sequence_in_range,
        is_stream_id_in_range, validate_end_stream_id, validate_start_stream_id,
    };

    #[test]
    fn test_validate_start_stream_id() {
        let empty_stream = BTreeMap::new();
        let stream = BTreeMap::from([
            ("1000-0".to_string(), BTreeMap::new()),
            ("2000-5".to_string(), BTreeMap::new()),
            ("3000-10".to_string(), BTreeMap::new()),
        ]);

        let test_cases = vec![
            (&empty_stream, "-", Ok(None)),
            (&stream, "-", Ok(Some((1000, Some(0))))),
            (&empty_stream, "1500-7", Ok(Some((1500, Some(7))))),
            (&stream, "1500-7", Ok(Some((1500, Some(7))))),
            (&stream, "2000-5", Ok(Some((2000, Some(5))))),
            (
                &stream,
                "invalid",
                Err(CommandError::InvalidStreamId(
                    "Timestamp specified must be greater than 0".to_string(),
                )),
            ),
            (&stream, "1000", Ok(Some((1000, None)))),
            (
                &stream,
                "1000-",
                Err(CommandError::InvalidStreamId(
                    "Sequence specified must be greater than 0".to_string(),
                )),
            ),
        ];

        for (stream_data, start_id, expected_result) in test_cases {
            assert_eq!(
                validate_start_stream_id(stream_data, start_id),
                expected_result,
                "Failed for start_id: {}",
                start_id
            );
        }
    }

    #[test]
    fn test_validate_end_stream_id() {
        let empty_stream = BTreeMap::new();
        let stream = BTreeMap::from([
            ("1000-0".to_string(), BTreeMap::new()),
            ("2000-5".to_string(), BTreeMap::new()),
            ("3000-10".to_string(), BTreeMap::new()),
        ]);

        let test_cases = vec![
            (&empty_stream, "+", Ok(None)),
            (&stream, "+", Ok(Some((3000, Some(10))))),
            (&empty_stream, "1500-7", Ok(Some((1500, Some(7))))),
            (&stream, "1500-7", Ok(Some((1500, Some(7))))),
            (&stream, "2000-5", Ok(Some((2000, Some(5))))),
            (
                &stream,
                "invalid",
                Err(CommandError::InvalidStreamId(
                    "Timestamp specified must be greater than 0".to_string(),
                )),
            ),
            (&stream, "1000", Ok(Some((1000, None)))),
            (
                &stream,
                "1000-",
                Err(CommandError::InvalidStreamId(
                    "Sequence specified must be greater than 0".to_string(),
                )),
            ),
        ];

        for (stream_data, end_id, expected_result) in test_cases {
            assert_eq!(
                validate_end_stream_id(stream_data, end_id),
                expected_result,
                "Failed for end_id: {}",
                end_id
            );
        }
    }

    #[test]
    fn test_is_stream_id_in_range() {
        let test_cases = vec![
            ((1000, Some(5)), (500, Some(3)), (1500, Some(7)), true),
            ((400, Some(5)), (500, Some(3)), (1500, Some(7)), false),
            ((2000, Some(5)), (500, Some(3)), (1500, Some(7)), false),
            ((1000, Some(5)), (1000, Some(3)), (1000, Some(7)), true),
            ((1000, Some(2)), (1000, Some(3)), (1000, Some(7)), false),
            ((1000, Some(3)), (1000, Some(3)), (1500, Some(7)), true),
            ((1000, Some(2)), (1000, Some(3)), (1500, Some(7)), false),
            ((1500, Some(7)), (1000, Some(3)), (1500, Some(7)), true),
            ((1500, Some(8)), (1000, Some(3)), (1500, Some(7)), false),
            ((1000, None), (1000, None), (1000, None), false),
            ((1000, None), (1000, Some(3)), (1000, Some(7)), false),
            ((1000, Some(5)), (1000, None), (1000, None), true),
            ((1000, Some(5)), (1000, None), (1000, Some(7)), true),
            ((1000, Some(5)), (1000, Some(3)), (1000, None), true),
        ];

        for (stream_id, start_stream_id, end_stream_id, expected) in test_cases {
            assert_eq!(
                is_stream_id_in_range(&stream_id, &start_stream_id, &end_stream_id),
                expected,
                "testing stream_id={:?}, start={:?}, end={:?}",
                stream_id,
                start_stream_id,
                end_stream_id
            );
        }
    }

    #[test]
    fn test_is_sequence_in_range() {
        let test_cases = vec![
            (Some(5), Some(3), Some(7), true),
            (Some(3), Some(3), Some(7), true),
            (Some(7), Some(3), Some(7), true),
            (Some(2), Some(3), Some(7), false),
            (Some(8), Some(3), Some(7), false),
            (Some(5), None, Some(7), true),
            (Some(8), None, Some(7), false),
            (Some(5), Some(3), None, true),
            (Some(2), Some(3), None, false),
            (Some(5), None, None, true),
            (None, Some(3), Some(7), false),
            (None, None, Some(7), false),
            (None, Some(3), None, false),
            (None, None, None, false),
        ];

        for (sequence, start_sequence, end_sequence, expected) in test_cases {
            assert_eq!(
                is_sequence_in_range(&sequence, &start_sequence, &end_sequence),
                expected,
                "testing sequence={:?}, start={:?}, end={:?}",
                sequence,
                start_sequence,
                end_sequence
            );
        }
    }

    #[test]
    fn test_is_sequence_at_or_after() {
        let test_cases = vec![
            (Some(5), Some(3), true),
            (Some(3), Some(3), true),
            (Some(2), Some(3), false),
            (Some(5), None, true),
            (None, Some(3), false),
            (None, None, false),
        ];

        for (sequence, start_sequence, expected) in test_cases {
            assert_eq!(
                is_sequence_at_or_after(&sequence, &start_sequence),
                expected,
                "testing sequence={:?}, start={:?}",
                sequence,
                start_sequence
            );
        }
    }

    #[test]
    fn test_is_sequence_at_or_before() {
        let test_cases = vec![
            (Some(5), Some(7), true),
            (Some(7), Some(7), true),
            (Some(8), Some(7), false),
            (Some(5), None, true),
            (None, Some(7), false),
            (None, None, false),
        ];

        for (sequence, end_sequence, expected) in test_cases {
            assert_eq!(
                is_sequence_at_or_before(&sequence, &end_sequence),
                expected,
                "testing sequence={:?}, end={:?}",
                sequence,
                end_sequence
            );
        }
    }
}
