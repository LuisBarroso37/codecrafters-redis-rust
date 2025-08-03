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

pub async fn xread(
    store: &mut Arc<Mutex<KeyValueStore>>,
    arguments: Vec<String>,
) -> Result<String, CommandError> {
    if arguments.len() != 3 {
        return Err(CommandError::InvalidXReadCommand);
    }

    if arguments[0].to_lowercase() != "streams" {
        return Err(CommandError::InvalidXReadOption);
    }

    let store_guard = store.lock().await;

    if let Some(value) = store_guard.get(&arguments[1]) {
        let stream = match value.data {
            DataType::Stream(ref stream) => stream,
            _ => return Err(CommandError::InvalidDataTypeForKey),
        };

        let start_stream_id = validate_stream_id(&arguments[2], false)
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
            .collect::<Vec<_>>();

        let resp_stream = parse_stream_entries_to_resp(entries);
        let resp = RespValue::Array(vec![RespValue::Array(vec![
            RespValue::BulkString(arguments[1].clone()),
            resp_stream,
        ])]);
        return Ok(resp.encode());
    } else {
        return Err(CommandError::DataNotFound);
    }
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
