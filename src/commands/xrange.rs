use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::Mutex;

use crate::{
    commands::{
        command_error::CommandError,
        command_handler::CommandResult,
        stream_utils::{parse_stream_entries_to_resp, validate_stream_id},
    },
    key_value_store::{DataType, KeyValueStore, Stream},
    resp::RespValue,
};

pub struct XrangeArguments {
    key: String,
    start_stream_id: String,
    end_stream_id: String,
}

impl XrangeArguments {
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

pub async fn xrange(
    store: Arc<Mutex<KeyValueStore>>,
    arguments: Vec<String>,
) -> Result<CommandResult, CommandError> {
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
        return Ok(CommandResult::Response(
            RespValue::Array(Vec::new()).encode(),
        ));
    };
    let Some(end_stream_id) = validate_end_stream_id(stream, &xrange_arguments.end_stream_id)?
    else {
        return Ok(CommandResult::Response(
            RespValue::Array(Vec::new()).encode(),
        ));
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
    return Ok(CommandResult::Response(resp_value.encode()));
}

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

fn is_sequence_at_or_after(sequence: &Option<u128>, start_sequence: &Option<u128>) -> bool {
    match (sequence, start_sequence) {
        (Some(s), Some(start)) => s >= start,
        (Some(_), None) => true,
        _ => false,
    }
}

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
