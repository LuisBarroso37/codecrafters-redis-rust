use std::sync::Arc;

use jiff::Timestamp;
use tokio::sync::Mutex;

use crate::{
    commands::{command_error::CommandError, command_handler::CommandResult},
    key_value_store::{DataType, KeyValueStore, Value},
    resp::RespValue,
};

pub struct GetArguments {
    key: String,
}

impl GetArguments {
    pub fn parse(arguments: Vec<String>) -> Result<Self, CommandError> {
        if arguments.len() != 1 {
            return Err(CommandError::InvalidGetCommand);
        }

        return Ok(Self {
            key: arguments[0].clone(),
        });
    }
}

pub async fn get(
    store: Arc<Mutex<KeyValueStore>>,
    arguments: Vec<String>,
) -> Result<CommandResult, CommandError> {
    let get_arguments = GetArguments::parse(arguments)?;

    let mut store_guard = store.lock().await;
    let stored_data = store_guard.get(&get_arguments.key);

    let Some(value) = stored_data else {
        return Ok(CommandResult::Response(RespValue::NullBulkString.encode()));
    };

    if is_value_expired(&value) {
        store_guard.remove(&get_arguments.key);
        return Ok(CommandResult::Response(RespValue::NullBulkString.encode()));
    }

    match value.data {
        DataType::String(ref s) => Ok(CommandResult::Response(
            RespValue::BulkString(s.clone()).encode(),
        )),
        _ => Ok(CommandResult::Response(RespValue::NullBulkString.encode())),
    }
}

fn is_value_expired(value: &Value) -> bool {
    if let Some(expiration) = value.expiration {
        if Timestamp::now() > expiration {
            return true;
        }

        return false;
    }

    return false;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_is_value_expired() {
        let test_cases = vec![
            (None, false),
            (
                Some(
                    Timestamp::now()
                        .checked_add(Duration::from_secs(60))
                        .unwrap(),
                ),
                false,
            ),
            (
                Some(
                    Timestamp::now()
                        .checked_sub(Duration::from_secs(60))
                        .unwrap(),
                ),
                true,
            ),
            (
                Some(
                    Timestamp::now()
                        .checked_sub(Duration::from_millis(1))
                        .unwrap(),
                ),
                true,
            ),
        ];

        for (expiration, expected) in test_cases {
            let value = Value {
                data: DataType::String("test".to_string()),
                expiration,
            };

            let result = is_value_expired(&value);
            assert_eq!(result, expected, "Unexpected expiration status");
        }
    }
}
