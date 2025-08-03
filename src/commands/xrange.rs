use std::{collections::BTreeMap, sync::Arc};

use tokio::sync::Mutex;

use crate::{
    commands::command_error::CommandError,
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
                    Some((stream_id, _)) => validate_stream_id(stream_id)
                        .map_err(|e| CommandError::InvalidStreamId(e))?,
                    None => return Ok(RespValue::Array(Vec::with_capacity(0)).encode()),
                }
            }
            argument => {
                validate_stream_id(argument).map_err(|e| CommandError::InvalidStreamId(e))?
            }
        };

        let end_stream_id =
            validate_stream_id(&arguments[2]).map_err(|e| CommandError::InvalidStreamId(e))?;

        let entries = stream
            .iter()
            .filter_map(|(id, entries)| {
                let stream_id = validate_stream_id(id).ok()?;

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

fn validate_stream_id(command_argument: &str) -> Result<(u128, Option<u128>), String> {
    let split_command_argument = command_argument.split("-").collect::<Vec<&str>>();

    if split_command_argument.len() > 2 {
        return Err("Stream ID cannot have more than 2 elements split by a hyphen".to_string());
    }

    let first_stream_id_part = split_command_argument[0]
        .parse::<u128>()
        .map_err(|_| "The stream ID specified must be greater than 0".to_string())?;

    if split_command_argument.len() == 1 {
        return Ok((first_stream_id_part, None));
    } else {
        let index = split_command_argument[1]
            .parse::<u128>()
            .map_err(|_| "The index specified must be greater than 0".to_string())?;

        if format!("{}-{}", first_stream_id_part, index) == "0-0" {
            return Err("The stream id must be greater than 0-0".to_string());
        }

        return Ok((first_stream_id_part, Some(index)));
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
        return is_sequence_in_range(stream_id.1, start_stream_id.1, end_stream_id.1);
    }

    if stream_id.0 == start_stream_id.0 {
        return is_sequence_at_or_after(stream_id.1, start_stream_id.1);
    }

    if stream_id.0 == end_stream_id.0 {
        return is_sequence_at_or_before(stream_id.1, end_stream_id.1);
    }

    false
}

fn is_sequence_in_range(
    sequence: Option<u128>,
    start_sequence: Option<u128>,
    end_sequence: Option<u128>,
) -> bool {
    match (sequence, start_sequence, end_sequence) {
        (Some(s), Some(start), Some(end)) => s >= start && s <= end,
        (Some(s), None, Some(end)) => s <= end,
        (Some(s), Some(start), None) => s >= start,
        (Some(_), None, None) => true,
        _ => false,
    }
}

fn is_sequence_at_or_after(sequence: Option<u128>, start_sequence: Option<u128>) -> bool {
    match (sequence, start_sequence) {
        (Some(s), Some(start)) => s >= start,
        (Some(_), None) => true,
        _ => false,
    }
}

fn is_sequence_at_or_before(sequence: Option<u128>, end_sequence: Option<u128>) -> bool {
    match (sequence, end_sequence) {
        (Some(s), Some(end)) => s <= end,
        (Some(_), None) => true,
        _ => false,
    }
}

fn parse_stream_entries_to_resp(entries: Vec<(&String, &BTreeMap<String, String>)>) -> RespValue {
    let array_length = entries.len();
    let mut response: Vec<RespValue> = Vec::with_capacity(array_length);

    let resp_stream_data = entries
        .iter()
        .map(|(id, values)| {
            let mut stream_vec: Vec<RespValue> = Vec::with_capacity(2);
            stream_vec.push(RespValue::BulkString(id.to_string()));

            let mut stream_values_vec: Vec<RespValue> = Vec::with_capacity(values.len());

            for (key, value) in values.iter() {
                stream_values_vec.push(RespValue::BulkString(key.to_string()));
                stream_values_vec.push(RespValue::BulkString(value.to_string()));
            }

            stream_vec.push(RespValue::Array(stream_values_vec));
            return stream_vec;
        })
        .collect::<Vec<_>>();

    for resp_vector in resp_stream_data {
        response.push(RespValue::Array(resp_vector));
    }

    RespValue::Array(response)
}

#[cfg(test)]
mod tests {
    use crate::resp::RespValue;

    use super::{
        is_sequence_at_or_after, is_sequence_at_or_before, is_sequence_in_range,
        is_stream_id_in_range, parse_stream_entries_to_resp, validate_stream_id,
    };

    #[test]
    fn test_validate_stream_id() {
        let test_cases = vec![
            (
                "invalid",
                Err("The stream ID specified must be greater than 0".to_string()),
            ),
            (
                "invalid-key-",
                Err("Stream ID cannot have more than 2 elements split by a hyphen".to_string()),
            ),
            (
                "invalid-0",
                Err("The stream ID specified must be greater than 0".to_string()),
            ),
            (
                "0-invalid",
                Err("The index specified must be greater than 0".to_string()),
            ),
            (
                "0-0",
                Err("The stream id must be greater than 0-0".to_string()),
            ),
            ("1526919030484", Ok((1526919030484, None))),
            ("1526919030484-3", Ok((1526919030484, Some(3)))),
        ];

        for (stream_id, expected_result) in test_cases {
            assert_eq!(
                validate_stream_id(stream_id),
                expected_result,
                "validating stream id {}",
                &stream_id
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
                is_sequence_in_range(sequence, start_sequence, end_sequence),
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
                is_sequence_at_or_after(sequence, start_sequence),
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
                is_sequence_at_or_before(sequence, end_sequence),
                expected,
                "testing sequence={:?}, end={:?}",
                sequence,
                end_sequence
            );
        }
    }

    #[test]
    fn test_parse_stream_entries_to_resp() {
        use std::collections::BTreeMap;

        let empty_entries: Vec<(&String, &BTreeMap<String, String>)> = vec![];
        let result = parse_stream_entries_to_resp(empty_entries);
        assert_eq!(result, RespValue::Array(vec![]));

        let mut map1 = BTreeMap::new();
        map1.insert("field1".to_string(), "value1".to_string());
        let id1 = "1000-0".to_string();
        let entries = vec![(&id1, &map1)];
        let result = parse_stream_entries_to_resp(entries);

        let expected = RespValue::Array(vec![RespValue::Array(vec![
            RespValue::BulkString("1000-0".to_string()),
            RespValue::Array(vec![
                RespValue::BulkString("field1".to_string()),
                RespValue::BulkString("value1".to_string()),
            ]),
        ])]);
        assert_eq!(result, expected);

        let mut map2 = BTreeMap::new();
        map2.insert("field1".to_string(), "value1".to_string());
        map2.insert("field2".to_string(), "value2".to_string());
        let id2 = "1001-0".to_string();
        let entries = vec![(&id2, &map2)];
        let result = parse_stream_entries_to_resp(entries);

        let expected = RespValue::Array(vec![RespValue::Array(vec![
            RespValue::BulkString("1001-0".to_string()),
            RespValue::Array(vec![
                RespValue::BulkString("field1".to_string()),
                RespValue::BulkString("value1".to_string()),
                RespValue::BulkString("field2".to_string()),
                RespValue::BulkString("value2".to_string()),
            ]),
        ])]);
        assert_eq!(result, expected);

        let mut map3 = BTreeMap::new();
        map3.insert("name".to_string(), "Alice".to_string());
        let mut map4 = BTreeMap::new();
        map4.insert("name".to_string(), "Bob".to_string());
        map4.insert("age".to_string(), "30".to_string());

        let id3 = "1002-0".to_string();
        let id4 = "1003-0".to_string();
        let entries = vec![(&id3, &map3), (&id4, &map4)];

        let result = parse_stream_entries_to_resp(entries);
        let expected = RespValue::Array(vec![
            RespValue::Array(vec![
                RespValue::BulkString("1002-0".to_string()),
                RespValue::Array(vec![
                    RespValue::BulkString("name".to_string()),
                    RespValue::BulkString("Alice".to_string()),
                ]),
            ]),
            RespValue::Array(vec![
                RespValue::BulkString("1003-0".to_string()),
                RespValue::Array(vec![
                    RespValue::BulkString("age".to_string()),
                    RespValue::BulkString("30".to_string()),
                    RespValue::BulkString("name".to_string()),
                    RespValue::BulkString("Bob".to_string()),
                ]),
            ]),
        ]);
        assert_eq!(result, expected);
    }
}
