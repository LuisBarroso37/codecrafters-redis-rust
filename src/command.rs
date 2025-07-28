use std::{collections::VecDeque, sync::Arc, time::Duration};
use thiserror::Error;
use tokio::{
    sync::{Mutex, mpsc},
    time::Instant,
};

use crate::{
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
}

impl CommandError {
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            CommandError::InvalidCommand => b"-ERR invalid command\r\n",
            CommandError::InvalidCommandArgument => b"-ERR invalid command argument\r\n",
            CommandError::InvalidEchoCommand => b"-ERR invalid ECHO command\r\n",
            CommandError::InvalidGetCommand => b"-ERR invalid GET command\r\n",
            CommandError::InvalidSetCommand => b"-ERR invalid SET command\r\n",
            CommandError::InvalidSetCommandArgument => b"-ERR invalid SET command argument\r\n",
            CommandError::InvalidSetCommandExpiration => b"-ERR invalid SET command expiration\r\n",
            CommandError::InvalidRPushCommand => b"-ERR invalid RPUSH command\r\n",
            CommandError::DataNotFound => b"-ERR data not found\r\n",
            CommandError::InvalidLRangeCommand => b"-ERR invalid LRANGE command\r\n",
            CommandError::InvalidLRangeCommandArgument => {
                b"-ERR invalid LRANGE command argument\r\n"
            }
            CommandError::InvalidLPushCommand => b"-ERR invalid LPUSH command\r\n",
            CommandError::InvalidLLenCommand => b"-ERR invalid LLEN command\r\n",
            CommandError::InvalidLPopCommand => b"-ERR invalid LPOP command\r\n",
            CommandError::InvalidLPopCommandArgument => b"-ERR invalid LPOP command argument\r\n",
            CommandError::InvalidBLPopCommand => b"-ERR invalid BLPOP command\r\n",
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
    input: Vec<RespValue>,
    store: &mut Arc<Mutex<KeyValueStore>>,
    state: &mut Arc<Mutex<State>>,
) -> Result<String, CommandError> {
    let command = Command::new(input)?;

    match command.name.as_str() {
        "PING" => Ok("+PONG\r\n".to_string()),
        "ECHO" => {
            if command.args.len() != 1 {
                return Err(CommandError::InvalidEchoCommand);
            }

            let arg = command.args[0].clone();
            return Ok(format!("${}\r\n{}\r\n", arg.len(), arg));
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
                            return Ok("$-1\r\n".to_string());
                        }
                    }

                    match value.data {
                        DataType::String(ref s) => {
                            return Ok(format!("${}\r\n{}\r\n", s.len(), s));
                        }
                        _ => {
                            return Ok("$-1\r\n".to_string());
                        }
                    }
                }
                None => {
                    return Ok("$-1\r\n".to_string());
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

            return Ok("+OK\r\n".to_string());
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
                            return Ok("*0\r\n".to_string());
                        };

                        let range = list.range(start..=end).collect::<Vec<&String>>();

                        if !range.is_empty() {
                            let formatted_vec = range
                                .iter()
                                .map(|s| format!("${}\r\n{}", s.len(), s))
                                .collect::<Vec<String>>();

                            return Ok(format!(
                                "*{}\r\n{}\r\n",
                                formatted_vec.len(),
                                formatted_vec.join("\r\n")
                            ));
                        } else {
                            return Ok("*0\r\n".to_string());
                        }
                    } else {
                        return Ok("*0\r\n".to_string());
                    }
                }
                None => {
                    return Ok("*0\r\n".to_string());
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
                        return Ok(format!(":{}\r\n", list.len()));
                    } else {
                        return Ok(":0\r\n".to_string());
                    }
                }
                None => {
                    return Ok(":0\r\n".to_string());
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
                            return Ok("$-1\r\n".to_string());
                        } else if vec.len() == 1 {
                            return Ok(format!("${}\r\n{}\r\n", vec[0].len(), vec[0]));
                        } else {
                            let formatted_vec = vec
                                .iter()
                                .map(|s| format!("${}\r\n{}", s.len(), s))
                                .collect::<Vec<String>>();
                            return Ok(format!(
                                "*{}\r\n{}\r\n",
                                vec.len(),
                                formatted_vec.join("\r\n")
                            ));
                        }
                    } else {
                        return Ok("$-1\r\n".to_string());
                    }
                }
                None => {
                    return Ok("$-1\r\n".to_string());
                }
            }
        }
        "BLPOP" => {
            if command.args.len() != 2 {
                return Err(CommandError::InvalidBLPopCommand);
            }

            let mut store_guard = store.lock().await;
            let stored_data = store_guard.get_mut(&command.args[0]);

            match stored_data {
                Some(value) => {
                    if let DataType::Array(ref mut list) = value.data {
                        let removed_value = list.pop_front();

                        if let Some(value) = removed_value {
                            return Ok(format!("${}\r\n{}\r\n", value.len(), value));
                        }

                        return Ok("$-1\r\n".to_string());
                    } else {
                        return Ok("$-1\r\n".to_string());
                    }
                }
                None => {
                    drop(store_guard);
                    let mut state_guard = state.lock().await;
                    let (sender, mut receiver) = mpsc::channel(32);
                    let subscriber = Subscriber {
                        key: command.args[0].clone(),
                        sender: sender.clone(),
                    };

                    state_guard
                        .subscribers
                        .entry("BLPOP".to_string())
                        .and_modify(|v| v.push_back(subscriber.clone()))
                        .or_insert(VecDeque::from([subscriber]));
                    drop(state_guard);

                    let value = receiver.recv().await;

                    let v = if let Some(_) = value {
                        let mut store_guard = store.lock().await;
                        let stored_data = store_guard.get_mut(&command.args[0]);

                        if let Some(stored_data) = stored_data {
                            if let DataType::Array(ref mut list) = stored_data.data {
                                list.pop_front()
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    } else {
                        return Ok("$-1\r\n".to_string());
                    };

                    match v {
                        Some(value) => {
                            let vec = vec![
                                format!("${}\r\n{}", command.args[0].len(), command.args[0]),
                                format!("${}\r\n{}", value.len(), value),
                            ];

                            Ok(format!("*2\r\n{}\r\n", vec.join("\r\n")))
                        }
                        None => Ok("$-1\r\n".to_string()),
                    }
                }
            }
        }
        _ => {
            return Err(CommandError::InvalidEchoCommand);
        }
    }
}

pub fn validate_range_indexes(
    list: &VecDeque<String>,
    start_index: isize,
    end_index: isize,
) -> Result<(usize, usize), &str> {
    let len = list.len() as isize;

    let mut start = if start_index < 0 {
        len + start_index
    } else {
        start_index
    };
    let mut end = if end_index < 0 {
        len + end_index
    } else {
        end_index
    };

    start = start.max(0);
    end = end.min(len - 1);

    if start >= len {
        return Err("Start index is out of bounds");
    }

    if start > end {
        return Err("Start index is bigger than end index after processing");
    }

    Ok((start as usize, end as usize))
}

pub async fn push_array_operations(
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

    let mut store_guard = store.lock().await;
    store_guard
        .entry(command.args[0].clone())
        .and_modify(|v| {
            if let DataType::Array(ref mut list) = v.data {
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

    if array_length == 0 {
        return Err(CommandError::DataNotFound);
    }

    let state_guard = state.lock().await;

    match state_guard.subscribers.get("BLPOP") {
        Some(subscribers) => {
            for subscriber in subscribers {
                if subscriber.key == command.args[0] {
                    if let Err(_) = subscriber.sender.send(true).await {
                        // receiver dropped
                    }
                }
            }
        }
        None => {}
    }

    return Ok(format!(":{}\r\n", array_length));
}
