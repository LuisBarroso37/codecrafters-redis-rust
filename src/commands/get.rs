use std::sync::Arc;

use tokio::{sync::Mutex, time::Instant};

use crate::{
    commands::command_error::CommandError,
    key_value_store::{DataType, KeyValueStore},
    resp::RespValue,
};

pub async fn get(
    store: &mut Arc<Mutex<KeyValueStore>>,
    arguments: Vec<String>,
) -> Result<String, CommandError> {
    if arguments.len() != 1 {
        return Err(CommandError::InvalidGetCommand);
    }

    let mut store_guard = store.lock().await;
    let stored_data = store_guard.get(&arguments[0]);

    match stored_data {
        Some(value) => {
            if let Some(expiration) = value.expiration {
                if Instant::now() > expiration {
                    store_guard.remove(&arguments[0]);
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
