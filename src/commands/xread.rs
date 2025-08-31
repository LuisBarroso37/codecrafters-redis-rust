use std::{sync::Arc, time::Duration};
use tokio::sync::{Mutex, mpsc};

use crate::{
    commands::{
        command_error::CommandError,
        command_handler::CommandResult,
        stream_utils::{parse_stream_entries_to_resp, validate_stream_id},
    },
    key_value_store::{DataType, KeyValueStore, Stream},
    resp::RespValue,
    state::{State, XreadSubscriber},
};

pub struct XreadArguments {
    blocking_duration: Option<u64>,
    key_stream_pairs: Vec<(String, String)>,
}

impl XreadArguments {
    pub fn parse(arguments: Vec<String>) -> Result<Self, CommandError> {
        if arguments.len() < 3 {
            return Err(CommandError::InvalidXReadCommand);
        }

        let (blocking_duration, start_data_index) = match arguments[0].to_lowercase().as_str() {
            "block" => {
                let duration_ms = arguments[1]
                    .parse::<u64>()
                    .map_err(|_| CommandError::InvalidXReadBlockDuration)?;

                if arguments[2].to_lowercase() != "streams" {
                    return Err(CommandError::InvalidXReadOption);
                }

                (Some(duration_ms), 3)
            }
            "streams" => (None, 1),
            _ => {
                return Err(CommandError::InvalidXReadOption);
            }
        };

        let data = arguments[start_data_index..].to_vec();

        if data.len() % 2 != 0 {
            return Err(CommandError::InvalidXReadCommand);
        }

        let split_index = data.len() / 2;

        let mut key_stream_pairs = Vec::with_capacity(split_index);

        for i in 0..split_index {
            let key = data[i].clone();
            let stream_id = data[split_index + i].clone();
            key_stream_pairs.push((key, stream_id));
        }

        Ok(Self {
            blocking_duration,
            key_stream_pairs,
        })
    }
}

pub async fn xread(
    client_address: &str,
    store: Arc<Mutex<KeyValueStore>>,
    state: Arc<Mutex<State>>,
    arguments: Vec<String>,
) -> Result<CommandResult, CommandError> {
    let xread_arguments = XreadArguments::parse(arguments)?;

    let parsed_stream_ids =
        match parse_stream_ids(Arc::clone(&store), xread_arguments.key_stream_pairs).await {
            Ok(ids) => ids,
            Err(CommandError::DataNotFound) => {
                return Ok(CommandResult::Response(
                    RespValue::Array(Vec::new()).encode(),
                ));
            }
            Err(e) => return Err(e),
        };

    let Some(blocking_duration_ms) = xread_arguments.blocking_duration else {
        match read_streams(store, parsed_stream_ids).await {
            Ok(response) => return Ok(CommandResult::Response(response)),
            Err(e) => return Err(e),
        }
    };

    let direct_call_response = read_streams(Arc::clone(&store), parsed_stream_ids.clone()).await?;

    if direct_call_response != RespValue::Array(Vec::new()).encode() {
        return Ok(CommandResult::Response(direct_call_response));
    }

    let (sender, mut receiver) = mpsc::channel(32);
    add_subscribers(
        Arc::clone(&state),
        &parsed_stream_ids,
        client_address,
        sender.clone(),
    )
    .await;

    let result = wait_for_data(&mut receiver, blocking_duration_ms).await;
    remove_subscribers(state, &parsed_stream_ids, &client_address).await;

    match result {
        Some(_) => match read_streams(store, parsed_stream_ids).await {
            Ok(response) => return Ok(CommandResult::Response(response)),
            Err(e) => return Err(e),
        },
        None => Ok(CommandResult::Response(RespValue::NullArray.encode())),
    }
}

async fn parse_stream_ids(
    store: Arc<Mutex<KeyValueStore>>,
    key_stream_id_pairs: Vec<(String, String)>,
) -> Result<Vec<(String, String)>, CommandError> {
    let mut parsed_key_stream_id_pairs: Vec<(String, String)> =
        Vec::with_capacity(key_stream_id_pairs.len());

    for (key, stream_id) in key_stream_id_pairs {
        let parsed_stream_id = if stream_id == "$" {
            resolve_special_id(Arc::clone(&store), &key).await?
        } else {
            stream_id
        };

        parsed_key_stream_id_pairs.push((key, parsed_stream_id));
    }

    Ok(parsed_key_stream_id_pairs)
}

async fn resolve_special_id(
    store: Arc<Mutex<KeyValueStore>>,
    key: &str,
) -> Result<String, CommandError> {
    let store_guard = store.lock().await;

    let Some(value) = store_guard.get(key) else {
        return Err(CommandError::DataNotFound);
    };

    let DataType::Stream(ref stream) = value.data else {
        return Err(CommandError::InvalidDataTypeForKey);
    };

    let Some(last_stream_id) = stream.keys().last().cloned() else {
        return Err(CommandError::DataNotFound);
    };

    Ok(last_stream_id)
}

async fn add_subscribers(
    state: Arc<Mutex<State>>,
    key_stream_id_pairs: &Vec<(String, String)>,
    client_address: &str,
    sender: mpsc::Sender<bool>,
) {
    for (key, stream_id) in key_stream_id_pairs {
        let subscriber = XreadSubscriber {
            client_address: client_address.to_string(),
            sender: sender.clone(),
        };

        let mut state_guard = state.lock().await;
        state_guard.add_xread_subscriber(key.clone(), stream_id.clone(), subscriber);
    }
}

async fn remove_subscribers(
    state: Arc<Mutex<State>>,
    key_stream_id_pairs: &Vec<(String, String)>,
    client_address: &str,
) {
    let mut state_guard = state.lock().await;

    for (key, stream_id) in key_stream_id_pairs {
        state_guard.remove_xread_subscriber(key, stream_id, &client_address);
    }
}

async fn wait_for_data(
    receiver: &mut mpsc::Receiver<bool>,
    blocking_duration_ms: u64,
) -> Option<bool> {
    match blocking_duration_ms {
        0 => receiver.recv().await,
        duration => {
            match tokio::time::timeout(Duration::from_millis(duration), receiver.recv()).await {
                Ok(result) => result,
                Err(_) => None,
            }
        }
    }
}

async fn read_streams(
    store: Arc<Mutex<KeyValueStore>>,
    key_stream_id_pairs: Vec<(String, String)>,
) -> Result<String, CommandError> {
    let store_guard = store.lock().await;
    let mut result_streams = Vec::new();

    for (key, stream_id) in key_stream_id_pairs {
        let Some(value) = store_guard.get(&key) else {
            continue;
        };

        let DataType::Stream(stream) = &value.data else {
            return Err(CommandError::InvalidDataTypeForKey);
        };

        let start_stream_id =
            validate_stream_id(&stream_id, false).map_err(CommandError::InvalidStreamId)?;

        let matching_entries = stream
            .iter()
            .filter_map(|(id, entries)| {
                let current_stream_id = validate_stream_id(id, false).ok()?;
                if is_xread_stream_id_after(&current_stream_id, &start_stream_id) {
                    Some((id, entries))
                } else {
                    None
                }
            })
            .collect::<Vec<(&String, &Stream)>>();

        if !matching_entries.is_empty() {
            let entries_resp = parse_stream_entries_to_resp(matching_entries);
            result_streams.push(RespValue::Array(vec![
                RespValue::BulkString(key),
                entries_resp,
            ]));
        }
    }

    return Ok(RespValue::Array(result_streams).encode());
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
    use std::{collections::BTreeMap, sync::Arc, time::Duration};
    use tokio::sync::{Mutex, mpsc};

    use crate::{
        commands::command_error::CommandError,
        key_value_store::{DataType, KeyValueStore, Value},
        state::State,
    };

    use super::{
        add_subscribers, is_sequence_after, is_xread_stream_id_after, parse_stream_ids,
        read_streams, remove_subscribers, resolve_special_id, wait_for_data,
    };

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

    #[test]
    fn test_is_xread_stream_id_after() {
        let test_cases = vec![
            ((1001, Some(0)), (1000, Some(999)), true),
            ((1000, Some(0)), (1001, Some(0)), false),
            ((1000, Some(5)), (1000, Some(3)), true),
            ((1000, Some(3)), (1000, Some(5)), false),
            ((1000, Some(3)), (1000, Some(3)), false),
            ((1000, Some(1)), (1000, None), false),
            ((1000, None), (1000, Some(1)), false),
            ((1000, None), (1000, None), false),
            ((2000, Some(0)), (1000, Some(999)), true),
            ((999, Some(999)), (1000, Some(0)), false),
        ];

        for (stream_id, start_stream_id, expected) in test_cases {
            assert_eq!(
                is_xread_stream_id_after(&stream_id, &start_stream_id),
                expected,
                "testing stream_id={:?}, start_stream_id={:?}",
                stream_id,
                start_stream_id
            );
        }
    }

    #[tokio::test]
    async fn test_parse_stream_ids() {
        let mut store = KeyValueStore::new();

        let stream_entries = BTreeMap::from([("temperature".to_string(), "25".to_string())]);
        let stream = BTreeMap::from([
            ("1000-0".to_string(), stream_entries.clone()),
            ("2000-5".to_string(), stream_entries),
        ]);
        store.insert(
            "mystream".to_string(),
            Value {
                data: DataType::Stream(stream),
                expiration: None,
            },
        );

        let key_value_store = Arc::new(Mutex::new(store));

        let test_cases = vec![
            (
                vec![("stream1".to_string(), "1234-0".to_string())],
                Ok(vec![("stream1".to_string(), "1234-0".to_string())]),
            ),
            (
                vec![("mystream".to_string(), "$".to_string())],
                Ok(vec![("mystream".to_string(), "2000-5".to_string())]),
            ),
            (
                vec![
                    ("stream1".to_string(), "1234-0".to_string()),
                    ("mystream".to_string(), "$".to_string()),
                ],
                Ok(vec![
                    ("stream1".to_string(), "1234-0".to_string()),
                    ("mystream".to_string(), "2000-5".to_string()),
                ]),
            ),
            (
                vec![("nonexistent".to_string(), "$".to_string())],
                Err(CommandError::DataNotFound),
            ),
        ];

        for (input, expected_result) in test_cases {
            assert_eq!(
                parse_stream_ids(Arc::clone(&key_value_store), input.clone()).await,
                expected_result,
                "Failed for input: {:?}",
                input
            );
        }
    }

    #[tokio::test]
    async fn test_resolve_special_id() {
        let mut store = KeyValueStore::new();

        let stream = BTreeMap::from([
            ("1000-0".to_string(), BTreeMap::new()),
            ("2000-5".to_string(), BTreeMap::new()),
            ("3000-10".to_string(), BTreeMap::new()),
        ]);
        store.insert(
            "mystream".to_string(),
            Value {
                data: DataType::Stream(stream),
                expiration: None,
            },
        );
        store.insert(
            "empty_stream".to_string(),
            Value {
                data: DataType::Stream(BTreeMap::new()),
                expiration: None,
            },
        );
        store.insert(
            "not_a_stream".to_string(),
            Value {
                data: DataType::String("hello".to_string()),
                expiration: None,
            },
        );

        let key_value_store = Arc::new(Mutex::new(store));

        let test_cases = vec![
            ("mystream", Ok("3000-10".to_string())),
            ("empty_stream", Err(CommandError::DataNotFound)),
            ("nonexistent", Err(CommandError::DataNotFound)),
            ("not_a_stream", Err(CommandError::InvalidDataTypeForKey)),
        ];

        for (key, expected_result) in test_cases {
            assert_eq!(
                resolve_special_id(Arc::clone(&key_value_store), key).await,
                expected_result,
                "Failed for key: {}",
                key
            );
        }
    }

    #[tokio::test]
    async fn test_add_and_remove_subscribers() {
        let state = Arc::new(Mutex::new(State::new()));
        let (sender, _receiver) = mpsc::channel(32);

        let key_stream_pairs = vec![
            ("stream1".to_string(), "1000-0".to_string()),
            ("stream2".to_string(), "2000-0".to_string()),
        ];

        let client_address = "server-1".to_string();

        add_subscribers(
            Arc::clone(&state),
            &key_stream_pairs,
            &client_address,
            sender,
        )
        .await;

        {
            let state_guard = state.lock().await;

            for (key, stream_id) in &key_stream_pairs {
                let has_subscriber = state_guard
                    .xread_subscribers
                    .get(key)
                    .and_then(|stream_map| stream_map.get(stream_id))
                    .map(|subscribers| {
                        subscribers
                            .iter()
                            .any(|sub| sub.client_address == client_address)
                    })
                    .unwrap_or(false);

                assert_eq!(
                    has_subscriber, true,
                    "Subscriber not found for key: {}, stream_id: {}",
                    key, stream_id
                );
            }
        }

        remove_subscribers(Arc::clone(&state), &key_stream_pairs, &client_address).await;

        {
            let state_guard = state.lock().await;

            for (key, stream_id) in &key_stream_pairs {
                let has_subscriber = state_guard
                    .xread_subscribers
                    .get(key)
                    .and_then(|stream_map| stream_map.get(stream_id))
                    .map(|subscribers| {
                        subscribers
                            .iter()
                            .any(|sub| sub.client_address == client_address)
                    })
                    .unwrap_or(false);

                assert_eq!(
                    has_subscriber, false,
                    "Subscriber not found for key: {}, stream_id: {}",
                    key, stream_id
                );
            }
        }
    }

    #[tokio::test]
    async fn test_wait_for_data_immediate_timeout() {
        let (_, mut receiver) = mpsc::channel::<bool>(32);

        let result = wait_for_data(&mut receiver, 1).await;
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_wait_for_data_with_notification() {
        let (sender, mut receiver) = mpsc::channel::<bool>(32);

        // Send the notification in a background task to avoid blocking
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let _ = sender.send(true).await;
        });

        let result = wait_for_data(&mut receiver, 3000).await;
        assert_eq!(result, Some(true));
    }

    #[tokio::test]
    async fn test_wait_for_data_infinite_blocking_with_notification() {
        let (sender, mut receiver) = mpsc::channel::<bool>(32);

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = sender.send(true).await;
        });

        let result = wait_for_data(&mut receiver, 0).await;
        assert_eq!(result, Some(true));
    }

    #[tokio::test]
    async fn test_wait_for_data_sender_dropped() {
        let (sender, mut receiver) = mpsc::channel::<bool>(32);

        // Drop the sender immediately
        drop(sender);

        // Test with any timeout (should return None due to dropped sender)
        let result = wait_for_data(&mut receiver, 1000).await;
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_read_streams() {
        let mut store = KeyValueStore::new();

        let entry1 = BTreeMap::from([("temp".to_string(), "25".to_string())]);
        let entry2 = BTreeMap::from([("temp".to_string(), "30".to_string())]);
        let stream = BTreeMap::from([
            ("1000-0".to_string(), entry1),
            ("2000-0".to_string(), entry2),
        ]);

        store.insert(
            "mystream".to_string(),
            Value {
                data: DataType::Stream(stream),
                expiration: None,
            },
        );
        store.insert(
            "not_a_stream".to_string(),
            Value {
                data: DataType::String("hello".to_string()),
                expiration: None,
            },
        );

        let key_value_store = Arc::new(Mutex::new(store));

        let test_cases = vec![
            (
                vec![("mystream".to_string(), "0-0".to_string())],
                Ok("*1\r\n*2\r\n$8\r\nmystream\r\n*2\r\n*2\r\n$6\r\n1000-0\r\n*2\r\n$4\r\ntemp\r\n$2\r\n25\r\n*2\r\n$6\r\n2000-0\r\n*2\r\n$4\r\ntemp\r\n$2\r\n30\r\n".to_string()),
            ),
            (
                vec![("mystream".to_string(), "1500-0".to_string())],
                Ok("*1\r\n*2\r\n$8\r\nmystream\r\n*1\r\n*2\r\n$6\r\n2000-0\r\n*2\r\n$4\r\ntemp\r\n$2\r\n30\r\n".to_string()),
            ),
            (
                vec![("mystream".to_string(), "3000-0".to_string())],
                Ok("*0\r\n".to_string()),
            ),
            (
                vec![("nonexistent".to_string(), "0-0".to_string())],
                Ok("*0\r\n".to_string()),
            ),
            (
                vec![("not_a_stream".to_string(), "0-0".to_string())],
                Err(CommandError::InvalidDataTypeForKey),
            ),
            (
                vec![("mystream".to_string(), "invalid-id".to_string())],
                Err(CommandError::InvalidStreamId("Timestamp specified must be greater than 0".to_string())),
            ),
        ];

        for (input, expected_result) in test_cases {
            assert_eq!(
                read_streams(Arc::clone(&key_value_store), input.clone()).await,
                expected_result,
                "Failed for input: {:?}",
                input
            );
        }
    }
}
