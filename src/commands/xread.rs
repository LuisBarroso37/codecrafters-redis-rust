use std::{sync::Arc, time::Duration};
use tokio::sync::{Mutex, mpsc};

use crate::{
    commands::{
        command_error::CommandError,
        stream_utils::{parse_stream_entries_to_resp, validate_stream_id},
    },
    key_value_store::{DataType, KeyValueStore, Stream},
    resp::RespValue,
    state::{State, XreadSubscriber},
};

pub async fn xread(
    server_address: String,
    store: &mut Arc<Mutex<KeyValueStore>>,
    state: &mut Arc<Mutex<State>>,
    arguments: Vec<String>,
) -> Result<String, CommandError> {
    if arguments.len() < 3 {
        return Err(CommandError::InvalidXReadCommand);
    }

    let blocking_duration = match arguments[0].to_lowercase().as_str() {
        "block" => {
            let block_duration = arguments[1]
                .parse::<u64>()
                .map_err(|_| CommandError::InvalidXReadBlockDuration)?;

            if arguments[2].to_lowercase() != "streams" {
                return Err(CommandError::InvalidXReadOption);
            }

            Some(block_duration)
        }
        "streams" => None,
        _ => {
            return Err(CommandError::InvalidXReadOption);
        }
    };

    let start_data_index = match blocking_duration {
        Some(_) => 3,
        None => 1,
    };

    let data = arguments[start_data_index..].to_vec();

    if data.len() % 2 != 0 {
        return Err(CommandError::InvalidXReadCommand);
    }

    let index = data.len() / 2;

    let mut key_stream_id_pairs: Vec<(String, String)> = Vec::new();

    for i in 0..index {
        let key = data[i].clone();
        let mut stream_id = data[index + i].clone();

        if stream_id == "$" {
            let store_guard = store.lock().await;
            if let Some(value) = store_guard.get(&key) {
                let stream = match value.data {
                    DataType::Stream(ref stream) => stream,
                    _ => return Err(CommandError::InvalidDataTypeForKey),
                };

                let last_key_value = stream.last_key_value();

                match last_key_value {
                    Some((last_stream_id, _)) => {
                        stream_id = last_stream_id.clone();
                    }
                    None => {
                        return Ok(RespValue::Array(Vec::new()).encode());
                    }
                }
            } else {
                return Ok(RespValue::Array(Vec::new()).encode());
            }
        }

        key_stream_id_pairs.push((key, stream_id));
    }

    if blocking_duration.is_none() {
        return read_streams(store, key_stream_id_pairs).await;
    }

    let direct_call_response = read_streams(store, key_stream_id_pairs.clone()).await?;

    if direct_call_response != RespValue::Array(vec![]).encode() {
        return Ok(direct_call_response);
    }

    let (sender, mut receiver) = mpsc::channel(32);

    for (key, stream_id) in &key_stream_id_pairs {
        let subscriber = XreadSubscriber {
            server_address: server_address.clone(),
            sender: sender.clone(),
        };

        let mut state_guard = state.lock().await;
        state_guard.add_xread_subscriber(key.clone(), stream_id.clone(), subscriber);
    }

    let result = match blocking_duration {
        Some(0) => receiver.recv().await,
        Some(num) => {
            match tokio::time::timeout(Duration::from_millis(num), receiver.recv()).await {
                Ok(result) => result,
                Err(_) => {
                    for (key, stream_id) in &key_stream_id_pairs {
                        let mut state_guard = state.lock().await;
                        state_guard.remove_xread_subscriber(key, stream_id, &server_address);
                        drop(state_guard);
                    }

                    return Ok(RespValue::Null.encode());
                }
            }
        }
        _ => {
            return Err(CommandError::InvalidXReadCommand);
        }
    };

    match result {
        Some(_) => {
            return read_streams(store, key_stream_id_pairs).await;
        }
        None => {
            for (key, stream_id) in &key_stream_id_pairs {
                let mut state_guard = state.lock().await;
                state_guard.remove_xread_subscriber(key, stream_id, &server_address);
                drop(state_guard);
            }

            return Ok(RespValue::Null.encode());
        }
    }
}

async fn read_streams(
    store: &mut Arc<Mutex<KeyValueStore>>,
    key_stream_id_pairs: Vec<(String, String)>,
) -> Result<String, CommandError> {
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

                    if is_xread_stream_id_after(&stream_id, &start_stream_id) {
                        Some((id, entries))
                    } else {
                        None
                    }
                })
                .collect::<Vec<(&String, &Stream)>>();

            if entries.len() != 0 {
                let resp_stream = parse_stream_entries_to_resp(entries);
                resp.push(RespValue::Array(vec![
                    RespValue::BulkString(key.clone()),
                    resp_stream,
                ]));
            }
        }
    }

    return Ok(RespValue::Array(resp).encode());
}

pub fn is_xread_stream_id_after(
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
