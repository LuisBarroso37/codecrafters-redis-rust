use std::time::Duration;

use thiserror::Error;
use tokio::time::Instant;

use crate::{
    key_value_store::{DataType, KeyValueStore, Value},
    resp::RespValue,
};

#[derive(Error, Debug)]
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

pub fn handle_command(
    input: Vec<RespValue>,
    store: &mut KeyValueStore,
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

            let stored_data = store.get(&command.args[0]);

            match stored_data {
                Some(value) => {
                    if let Some(expiration) = value.expiration {
                        if Instant::now() > expiration {
                            store.remove(&command.args[0]);
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

            store.insert(
                command.args[0].clone(),
                Value {
                    data: DataType::String(command.args[1].clone()),
                    expiration,
                },
            );

            return Ok("+OK\r\n".to_string());
        }
        "RPUSH" => {
            if command.args.len() < 2 {
                return Err(CommandError::InvalidRPushCommand);
            }

            let mut array_length = 0;

            store
                .entry(command.args[0].clone())
                .and_modify(|v| {
                    if let DataType::Array(ref mut list) = v.data {
                        for i in 1..command.args.len() {
                            list.push(command.args[i].clone());
                        }

                        array_length = list.len();
                    }
                })
                .or_insert_with(|| {
                    let mut list = Vec::new();

                    for i in 1..command.args.len() {
                        list.push(command.args[i].clone());
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

            return Ok(format!(":{}\r\n", array_length));
        }
        "LRANGE" => {
            if command.args.len() != 3 {
                return Err(CommandError::InvalidLRangeCommand);
            }

            let start_index = match command.args[1].parse::<usize>() {
                Ok(num) => num,
                Err(_) => return Err(CommandError::InvalidLRangeCommandArgument),
            };

            let mut end_index = match command.args[2].parse::<usize>() {
                Ok(num) => num,
                Err(_) => return Err(CommandError::InvalidLRangeCommandArgument),
            };

            if start_index > end_index {
                return Ok("*0\r\n".to_string());
            }

            let stored_data = store.get(&command.args[0]);

            match stored_data {
                Some(value) => {
                    if let DataType::Array(ref list) = value.data {
                        if start_index >= list.len() - 1 {
                            return Ok("*0\r\n".to_string());
                        } else if end_index > list.len() - 1 {
                            end_index = list.len() - 1;
                        }

                        let range = list.get(start_index..=end_index);

                        if let Some(range) = range {
                            let mut vec = Vec::new();

                            for item in range {
                                vec.push(format!("${}", item.len()));
                                vec.push(item.clone());
                            }

                            return Ok(format!("*{}\r\n{}\r\n", range.len(), vec.join("\r\n")));
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
        _ => {
            return Err(CommandError::InvalidEchoCommand);
        }
    }
}
