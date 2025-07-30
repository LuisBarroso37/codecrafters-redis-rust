use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    sync::Arc,
    time::Duration,
};
use thiserror::Error;
use tokio::{
    sync::{Mutex, oneshot},
    time::Instant,
};

use crate::{
    command_utils::{validate_range_indexes, validate_stream_id},
    key_value_store::{DataType, KeyValueStore, Value},
    resp::RespValue,
    state::{State, Subscriber},
};

#[derive(Error, Debug, PartialEq)]
pub enum CommandError {
    #[error("invalid command")]
    InvalidCommand,
    #[error("invalid command argument")]
    InvalidCommandArgument,
    #[error("invalid ECHO command")]
    InvalidEchoCommand,
    #[error("invalid GET command")]
    InvalidGetCommand,
    #[error("invalid SET command")]
    InvalidSetCommand,
    #[error("invalid SET command argument")]
    InvalidSetCommandArgument,
    #[error("invalid SET command expiration")]
    InvalidSetCommandExpiration,
    #[error("invalid RPUSH command")]
    InvalidRPushCommand,
    #[error("data not found")]
    DataNotFound,
    #[error("invalid LRANGE command")]
    InvalidLRangeCommand,
    #[error("invalid LRANGE command argument")]
    InvalidLRangeCommandArgument,
    #[error("invalid LPUSH command")]
    InvalidLPushCommand,
    #[error("invalid LLEN command")]
    InvalidLLenCommand,
    #[error("invalid LPOP command")]
    InvalidLPopCommand,
    #[error("invalid LPOP command argument")]
    InvalidLPopCommandArgument,
    #[error("invalid BLPOP command")]
    InvalidBLPopCommand,
    #[error("invalid BLPOP command argument")]
    InvalidBLPopCommandArgument,
    #[error("invalid TYPE command")]
    InvalidTypeCommand,
    #[error("invalid XADD command")]
    InvalidXAddCommand,
    #[error("{0}")]
    InvalidXAddStreamId(String),
    #[error("invalid data type for key")]
    InvalidDataTypeForKey,
}

impl CommandError {
    pub fn as_string(&self) -> String {
        match self {
            CommandError::InvalidCommand => {
                RespValue::Error("ERR Invalid command".to_string()).encode()
            }
            CommandError::InvalidCommandArgument => {
                RespValue::Error("ERR Invalid command argument".to_string()).encode()
            }
            CommandError::InvalidEchoCommand => {
                RespValue::Error("ERR Invalid ECHO command".to_string()).encode()
            }
            CommandError::InvalidGetCommand => {
                RespValue::Error("ERR Invalid GET command".to_string()).encode()
            }
            CommandError::InvalidSetCommand => {
                RespValue::Error("ERR Invalid SET command".to_string()).encode()
            }
            CommandError::InvalidSetCommandArgument => {
                RespValue::Error("ERR Invalid SET command argument".to_string()).encode()
            }
            CommandError::InvalidSetCommandExpiration => {
                RespValue::Error("ERR Invalid SET command expiration".to_string()).encode()
            }
            CommandError::InvalidRPushCommand => {
                RespValue::Error("ERR Invalid RPUSH command".to_string()).encode()
            }
            CommandError::DataNotFound => {
                RespValue::Error("ERR Data not found".to_string()).encode()
            }
            CommandError::InvalidLRangeCommand => {
                RespValue::Error("ERR Invalid LRANGE command".to_string()).encode()
            }
            CommandError::InvalidLRangeCommandArgument => {
                RespValue::Error("ERR Invalid LRANGE command argument".to_string()).encode()
            }
            CommandError::InvalidLPushCommand => {
                RespValue::Error("ERR Invalid LPUSH command".to_string()).encode()
            }
            CommandError::InvalidLLenCommand => {
                RespValue::Error("ERR Invalid LLEN command".to_string()).encode()
            }
            CommandError::InvalidLPopCommand => {
                RespValue::Error("ERR Invalid LPOP command".to_string()).encode()
            }
            CommandError::InvalidLPopCommandArgument => {
                RespValue::Error("ERR Invalid LPOP command argument".to_string()).encode()
            }
            CommandError::InvalidBLPopCommand => {
                RespValue::Error("ERR Invalid BLPOP command".to_string()).encode()
            }
            CommandError::InvalidBLPopCommandArgument => {
                RespValue::Error("ERR Invalid BLPOP command argument".to_string()).encode()
            }
            CommandError::InvalidTypeCommand => {
                RespValue::Error("ERR Invalid TYPE command".to_string()).encode()
            }
            CommandError::InvalidXAddCommand => {
                RespValue::Error("ERR Invalid XADD command".to_string()).encode()
            }
            CommandError::InvalidXAddStreamId(str) => {
                RespValue::Error(format!("ERR {}", str)).encode()
            }
            CommandError::InvalidDataTypeForKey => {
                RespValue::Error("ERR Invalid data type for key".to_string()).encode()
            }
        }
    }
}

#[derive(Debug)]
pub struct Command {
    name: String,
    args: Vec<String>,
}

impl Command {
    fn new(input: Vec<RespValue>) -> Result<Self, CommandError> {
        if input.len() != 1 {
            return Err(CommandError::InvalidCommand);
        }

        match input.get(0) {
            Some(RespValue::Array(elements)) => {
                let name = match elements.get(0) {
                    Some(RespValue::BulkString(s)) => Ok(s.to_string()),
                    _ => Err(CommandError::InvalidCommandArgument),
                }?
                .to_uppercase();

                let mut args: Vec<String> = Vec::new();

                for element in elements[1..].iter() {
                    let arg = match element {
                        RespValue::BulkString(s) => Ok(s.to_string()),
                        _ => Err(CommandError::InvalidCommand),
                    }?;
                    args.push(arg);
                }

                Ok(Command { name, args })
            }
            _ => return Err(CommandError::InvalidCommand),
        }
    }
}

pub async fn handle_command(
    server_addr: String,
    input: Vec<RespValue>,
    store: &mut Arc<Mutex<KeyValueStore>>,
    state: &mut Arc<Mutex<State>>,
) -> Result<String, CommandError> {
    let command = Command::new(input)?;

    match command.name.as_str() {
        "PING" => Ok(RespValue::SimpleString("PONG".to_string()).encode()),
        "ECHO" => {
            if command.args.len() != 1 {
                return Err(CommandError::InvalidEchoCommand);
            }

            let arg = command.args[0].clone();
            return Ok(RespValue::BulkString(arg).encode());
        }
        "GET" => {
            if command.args.len() != 1 {
                return Err(CommandError::InvalidGetCommand);
            }

            let mut store_guard = store.lock().await;
            let stored_data = store_guard.get(&command.args[0]);

            match stored_data {
                Some(value) => {
                    if let Some(expiration) = value.expiration {
                        if Instant::now() > expiration {
                            store_guard.remove(&command.args[0]);
                            return Ok(RespValue::Null.encode());
                        }
                    }

                    match value.data {
                        DataType::String(ref s) => {
                            return Ok(RespValue::BulkString(s.clone()).encode());
                        }
                        _ => {
                            return Ok(RespValue::Null.encode());
                        }
                    }
                }
                None => {
                    return Ok(RespValue::Null.encode());
                }
            }
        }
        "SET" => {
            if command.args.len() != 2 && command.args.len() != 4 {
                return Err(CommandError::InvalidSetCommand);
            }

            let mut expiration: Option<Instant> = None;

            if command.args.len() == 4 {
                if command.args[2].to_lowercase() != "px" {
                    return Err(CommandError::InvalidSetCommandArgument);
                }

                if let Ok(expiration_time) = command.args[3].parse::<u64>() {
                    expiration = Some(Instant::now() + Duration::from_millis(expiration_time))
                } else {
                    return Err(CommandError::InvalidSetCommandExpiration);
                }
            }

            let mut store_guard = store.lock().await;
            store_guard.insert(
                command.args[0].clone(),
                Value {
                    data: DataType::String(command.args[1].clone()),
                    expiration,
                },
            );

            return Ok(RespValue::SimpleString("OK".to_string()).encode());
        }
        "RPUSH" => push_array_operations(&command, store, state, false).await,
        "LRANGE" => {
            if command.args.len() != 3 {
                return Err(CommandError::InvalidLRangeCommand);
            }

            let start_index = match command.args[1].parse::<isize>() {
                Ok(num) => num,
                Err(_) => return Err(CommandError::InvalidLRangeCommandArgument),
            };

            let end_index = match command.args[2].parse::<isize>() {
                Ok(num) => num,
                Err(_) => return Err(CommandError::InvalidLRangeCommandArgument),
            };

            let store_guard = store.lock().await;
            let stored_data = store_guard.get(&command.args[0]);

            match stored_data {
                Some(value) => {
                    if let DataType::Array(ref list) = value.data {
                        let (start, end) = if let Ok((start, end)) =
                            validate_range_indexes(list, start_index, end_index)
                        {
                            (start, end)
                        } else {
                            return Ok(RespValue::Array(vec![]).encode());
                        };

                        let range = list
                            .range(start..=end)
                            .map(|s| s.to_string())
                            .collect::<Vec<String>>();

                        if !range.is_empty() {
                            return Ok(RespValue::encode_array_from_strings(range));
                        } else {
                            return Ok(RespValue::Array(vec![]).encode());
                        }
                    } else {
                        return Ok(RespValue::Array(vec![]).encode());
                    }
                }
                None => {
                    return Ok(RespValue::Array(vec![]).encode());
                }
            }
        }
        "LPUSH" => push_array_operations(&command, store, state, true).await,
        "LLEN" => {
            if command.args.len() != 1 {
                return Err(CommandError::InvalidLLenCommand);
            }

            let store_guard = store.lock().await;
            let stored_data = store_guard.get(&command.args[0]);

            match stored_data {
                Some(value) => {
                    if let DataType::Array(ref list) = value.data {
                        return Ok(RespValue::Integer(list.len() as i64).encode());
                    } else {
                        return Ok(RespValue::Integer(0).encode());
                    }
                }
                None => {
                    return Ok(RespValue::Integer(0).encode());
                }
            }
        }
        "LPOP" => {
            if command.args.len() < 1 || command.args.len() > 2 {
                return Err(CommandError::InvalidLPopCommand);
            }

            let mut store_guard = store.lock().await;
            let stored_data = store_guard.get_mut(&command.args[0]);

            let amount_of_elements_to_delete = if let Some(val) = command.args.get(1) {
                val.parse::<usize>()
                    .map_err(|_| CommandError::InvalidLPopCommandArgument)?
            } else {
                1
            };

            match stored_data {
                Some(value) => {
                    if let DataType::Array(ref mut list) = value.data {
                        let mut vec = Vec::new();

                        for _ in 0..amount_of_elements_to_delete {
                            if let Some(removed) = list.pop_front() {
                                vec.push(removed);
                            }
                        }

                        if vec.len() == 0 {
                            return Ok(RespValue::Null.encode());
                        } else if vec.len() == 1 {
                            return Ok(RespValue::BulkString(vec[0].clone()).encode());
                        } else {
                            return Ok(RespValue::encode_array_from_strings(vec));
                        }
                    } else {
                        return Ok(RespValue::Null.encode());
                    }
                }
                None => {
                    return Ok(RespValue::Null.encode());
                }
            }
        }
        "BLPOP" => {
            if command.args.len() != 2 {
                return Err(CommandError::InvalidBLPopCommand);
            }

            let mut store_guard = store.lock().await;
            let immediate_result = if let Some(value) = store_guard.get_mut(&command.args[0]) {
                if let DataType::Array(ref mut list) = value.data {
                    list.pop_front()
                } else {
                    None
                }
            } else {
                None
            };
            drop(store_guard);

            if let Some(value) = immediate_result {
                return Ok(RespValue::encode_array_from_strings(vec![
                    command.args[0].clone(),
                    value,
                ]));
            }

            let duration = command.args[1]
                .parse::<f64>()
                .map_err(|_| CommandError::InvalidBLPopCommandArgument)?;

            let (sender, receiver) = oneshot::channel();
            let subscriber = Subscriber {
                server_addr: server_addr.clone(),
                sender,
            };

            let mut state_guard = state.lock().await;
            state_guard.add_subscriber("BLPOP".to_string(), command.args[0].clone(), subscriber);
            drop(state_guard);

            let result = match duration {
                0.0 => receiver.await,
                num => match tokio::time::timeout(Duration::from_secs_f64(num), receiver).await {
                    Ok(result) => result,
                    Err(_) => {
                        let mut state_guard = state.lock().await;
                        state_guard.remove_subscriber(
                            "BLPOP",
                            command.args[0].as_str(),
                            &server_addr,
                        );
                        return Ok(RespValue::Null.encode());
                    }
                },
            };

            match result {
                Ok(_) => {
                    let mut store_guard = store.lock().await;
                    let popped_value =
                        if let Some(stored_data) = store_guard.get_mut(&command.args[0]) {
                            if let DataType::Array(ref mut list) = stored_data.data {
                                list.pop_front()
                            } else {
                                None
                            }
                        } else {
                            None
                        };
                    drop(store_guard);

                    let mut state_guard = state.lock().await;
                    state_guard.remove_subscriber("BLPOP", command.args[0].as_str(), &server_addr);
                    drop(state_guard);

                    match popped_value {
                        Some(value) => Ok(RespValue::encode_array_from_strings(vec![
                            command.args[0].clone(),
                            value,
                        ])),
                        None => Ok(RespValue::Null.encode()),
                    }
                }
                Err(_) => {
                    let mut state_guard = state.lock().await;
                    state_guard.remove_subscriber("BLPOP", command.args[0].as_str(), &server_addr);
                    return Ok(RespValue::Null.encode());
                }
            }
        }
        "TYPE" => {
            if command.args.len() != 1 {
                return Err(CommandError::InvalidTypeCommand);
            }

            let store_guard = store.lock().await;

            if let Some(value) = store_guard.get(&command.args[0]) {
                match value.data {
                    DataType::String(_) => {
                        return Ok(RespValue::SimpleString("string".to_string()).encode());
                    }
                    DataType::Array(_) => {
                        return Ok(RespValue::SimpleString("list".to_string()).encode());
                    }
                    DataType::Stream(_) => {
                        return Ok(RespValue::SimpleString("stream".to_string()).encode());
                    }
                }
            } else {
                return Ok(RespValue::SimpleString("none".to_string()).encode());
            }
        }
        "XADD" => {
            if command.args.len() < 4 {
                return Err(CommandError::InvalidXAddCommand);
            }

            let store_key = command.args[0].clone();
            let validated_stream_id =
                validate_stream_id(store, &store_key, command.args[1].as_str())
                    .await
                    .map_err(|e| {
                        if e == "Invalid data type for key" {
                            CommandError::InvalidDataTypeForKey
                        } else {
                            CommandError::InvalidXAddStreamId(e)
                        }
                    })?;
            let key_value_pairs = command.args[2..].to_vec();

            let mut entries = HashMap::new();

            for (key, value) in key_value_pairs
                .chunks(2)
                .map(|chunk| (chunk.get(0), chunk.get(1)))
            {
                if let (Some(key), Some(value)) = (key, value) {
                    entries.insert(key.clone(), value.clone());
                } else {
                    return Err(CommandError::InvalidXAddCommand);
                }
            }

            let mut store_guard = store.lock().await;

            if let Some(value) = store_guard.get_mut(&store_key) {
                if let DataType::Stream(ref mut stream) = value.data {
                    stream.insert(validated_stream_id.clone(), entries);
                } else {
                    return Err(CommandError::InvalidDataTypeForKey);
                }
            } else {
                store_guard.insert(
                    store_key,
                    Value {
                        data: DataType::Stream(BTreeMap::from([(
                            validated_stream_id.clone(),
                            entries,
                        )])),
                        expiration: None,
                    },
                );
            }

            Ok(RespValue::BulkString(validated_stream_id).encode())
        }
        _ => {
            return Err(CommandError::InvalidCommand);
        }
    }
}

async fn push_array_operations(
    command: &Command,
    store: &mut Arc<Mutex<KeyValueStore>>,
    state: &mut Arc<Mutex<State>>,
    should_prepend: bool,
) -> Result<String, CommandError> {
    if command.args.len() < 2 {
        return if should_prepend {
            Err(CommandError::InvalidLPushCommand)
        } else {
            Err(CommandError::InvalidRPushCommand)
        };
    }

    let mut array_length = 0;
    let mut was_empty_before = false;

    let mut store_guard = store.lock().await;
    store_guard
        .entry(command.args[0].clone())
        .and_modify(|v| {
            if let DataType::Array(ref mut list) = v.data {
                was_empty_before = list.is_empty();

                for i in 1..command.args.len() {
                    if should_prepend {
                        list.push_front(command.args[i].clone());
                    } else {
                        list.push_back(command.args[i].clone());
                    }
                }

                array_length = list.len();
            }
        })
        .or_insert_with(|| {
            let mut list = VecDeque::new();
            was_empty_before = true; // New list is considered as was empty before

            for i in 1..command.args.len() {
                if should_prepend {
                    list.push_front(command.args[i].clone());
                } else {
                    list.push_back(command.args[i].clone());
                }
            }

            array_length = list.len();

            return Value {
                data: DataType::Array(list),
                expiration: None,
            };
        });
    drop(store_guard);

    if array_length == 0 {
        return Err(CommandError::DataNotFound);
    }

    // Only notify subscribers if the list was empty before and now has elements
    if was_empty_before && array_length > 0 {
        let mut state_guard = state.lock().await;
        state_guard.send_to_subscriber("BLPOP", &command.args[0], true);
    }

    return Ok(RespValue::Integer(array_length as i64).encode());
}
