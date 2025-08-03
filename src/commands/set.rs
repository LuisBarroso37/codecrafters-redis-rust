use std::{sync::Arc, time::Duration};

use tokio::{sync::Mutex, time::Instant};

use crate::{
    commands::command_error::CommandError,
    key_value_store::{DataType, KeyValueStore, Value},
    resp::RespValue,
};

pub async fn set(
    store: &mut Arc<Mutex<KeyValueStore>>,
    arguments: Vec<String>,
) -> Result<String, CommandError> {
    if arguments.len() != 2 && arguments.len() != 4 {
        return Err(CommandError::InvalidSetCommand);
    }

    let mut expiration: Option<Instant> = None;

    if arguments.len() == 4 {
        if arguments[2].to_lowercase() != "px" {
            return Err(CommandError::InvalidSetCommandArgument);
        }

        if let Ok(expiration_time) = arguments[3].parse::<u64>() {
            expiration = Some(Instant::now() + Duration::from_millis(expiration_time))
        } else {
            return Err(CommandError::InvalidSetCommandExpiration);
        }
    }

    let mut store_guard = store.lock().await;
    store_guard.insert(
        arguments[0].clone(),
        Value {
            data: DataType::String(arguments[1].clone()),
            expiration,
        },
    );

    return Ok(RespValue::SimpleString("OK".to_string()).encode());
}
