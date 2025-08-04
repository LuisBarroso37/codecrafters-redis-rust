use std::sync::Arc;
use tokio::sync::Mutex;

use crate::{
    commands::{
        command_error::CommandError,
        x_range_and_xread_utils::{parse_stream_entries_to_resp, validate_stream_id},
    },
    key_value_store::{DataType, KeyValueStore, Stream},
    resp::RespValue,
};

pub async fn xread(
    store: &mut Arc<Mutex<KeyValueStore>>,
    arguments: Vec<String>,
) -> Result<String, CommandError> {
    if arguments.len() < 3 {
        return Err(CommandError::InvalidXReadCommand);
    }

    if arguments[0].to_lowercase() != "streams" {
        return Err(CommandError::InvalidXReadOption);
    }

    if arguments[1..].len() % 2 != 0 {
        return Err(CommandError::InvalidXReadCommand);
    }

    let index = arguments
        .iter()
        .position(|arg| {
            let split_argument = arg.split("-").collect::<Vec<&str>>();

            if split_argument.len() == 2 {
                return true;
            }

            let parsed_stream_id = arg.parse::<u128>().ok();
            parsed_stream_id.is_some()
        })
        .ok_or_else(|| CommandError::InvalidXReadCommand)?;

    let expected_index = f32::ceil(arguments.len() as f32 / 2 as f32) as usize;

    if index != expected_index {
        return Err(CommandError::InvalidXReadCommand);
    }

    let mut key_stream_id_pairs: Vec<(String, String)> = Vec::new();

    for i in 0..index - 1 {
        println!("{} {}", &arguments[i + 1], &arguments[index + i]);
        key_stream_id_pairs.push((arguments[i + 1].clone(), arguments[index + i].clone()));
    }

    let store_guard = store.lock().await;

    let mut resp = Vec::with_capacity(key_stream_id_pairs.len());

    for (key, stream_id) in key_stream_id_pairs {
        if let Some(value) = store_guard.get(&key) {
            let stream = match value.data {
                DataType::Stream(ref stream) => stream,
                _ => return Err(CommandError::InvalidDataTypeForKey),
            };

            let start_stream_id = validate_stream_id(&stream_id, false)
                .map_err(|e| CommandError::InvalidStreamId(e))?;

            let entries = stream
                .iter()
                .filter_map(|(id, entries)| {
                    let stream_id = validate_stream_id(id, false).ok()?;

                    if is_stream_id_in_range(&stream_id, &start_stream_id) {
                        Some((id, entries))
                    } else {
                        None
                    }
                })
                .collect::<Vec<(&String, &Stream)>>();

            let resp_stream = parse_stream_entries_to_resp(entries);
            resp.push(RespValue::Array(vec![
                RespValue::BulkString(key.clone()),
                resp_stream,
            ]));
        } else {
            return Err(CommandError::DataNotFound);
        }
    }

    return Ok(RespValue::Array(resp).encode());
}

fn is_stream_id_in_range(
    stream_id: &(u128, Option<u128>),
    start_stream_id: &(u128, Option<u128>),
) -> bool {
    if stream_id.0 > start_stream_id.0 {
        return true;
    }

    if stream_id.0 == start_stream_id.0 {
        return is_sequence_after(&stream_id.1, &start_stream_id.1);
    }

    false
}

fn is_sequence_after(sequence: &Option<u128>, start_sequence: &Option<u128>) -> bool {
    match (sequence, start_sequence) {
        (Some(s), Some(start)) => s > start,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::is_sequence_after;

    #[test]
    fn test_is_sequence_after() {
        let test_cases = vec![
            (Some(5), Some(3), true),
            (Some(3), Some(3), false),
            (Some(2), Some(3), false),
            (Some(5), None, false),
            (None, Some(3), false),
            (None, None, false),
        ];

        for (sequence, start_sequence, expected) in test_cases {
            assert_eq!(
                is_sequence_after(&sequence, &start_sequence),
                expected,
                "testing sequence={:?}, start={:?}",
                sequence,
                start_sequence
            );
        }
    }
}
