use std::sync::Arc;
use tokio::sync::Mutex;

use crate::{
    commands::{
        command_error::CommandError,
        x_range_and_xread_utils::{parse_stream_entries_to_resp, validate_stream_id},
    },
    key_value_store::{DataType, KeyValueStore},
    resp::RespValue,
};

pub async fn xrange(
    store: &mut Arc<Mutex<KeyValueStore>>,
    arguments: Vec<String>,
) -> Result<String, CommandError> {
    if arguments.len() != 3 {
        return Err(CommandError::InvalidXRangeCommand);
    }

    let store_guard = store.lock().await;

    if let Some(value) = store_guard.get(&arguments[0]) {
        let stream = match value.data {
            DataType::Stream(ref stream) => stream,
            _ => return Err(CommandError::InvalidDataTypeForKey),
        };

        let start_stream_id = match arguments[1].as_str() {
            "-" => {
                let first_key_value_pair = stream.first_key_value();

                match first_key_value_pair {
                    Some((stream_id, _)) => validate_stream_id(stream_id, true)
                        .map_err(|e| CommandError::InvalidStreamId(e))?,
                    None => return Ok(RespValue::Array(Vec::with_capacity(0)).encode()),
                }
            }
            argument => {
                validate_stream_id(argument, true).map_err(|e| CommandError::InvalidStreamId(e))?
            }
        };

        let end_stream_id = match arguments[2].as_str() {
            "+" => {
                let last_key_value_pair = stream.last_key_value();

                match last_key_value_pair {
                    Some((stream_id, _)) => validate_stream_id(stream_id, true)
                        .map_err(|e| CommandError::InvalidStreamId(e))?,
                    None => return Ok(RespValue::Array(Vec::with_capacity(0)).encode()),
                }
            }
            argument => {
                validate_stream_id(argument, true).map_err(|e| CommandError::InvalidStreamId(e))?
            }
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
            .collect::<Vec<_>>();

        let resp_value = parse_stream_entries_to_resp(entries);
        return Ok(resp_value.encode());
    } else {
        return Err(CommandError::DataNotFound);
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
    use super::{
        is_sequence_at_or_after, is_sequence_at_or_before, is_sequence_in_range,
        is_stream_id_in_range,
    };

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
