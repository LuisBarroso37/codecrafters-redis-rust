use std::sync::Arc;

use tokio::sync::Mutex;

use crate::{
    commands::command_error::CommandError,
    key_value_store::{DataType, KeyValueStore},
    resp::RespValue,
};

pub async fn llen(
    store: &mut Arc<Mutex<KeyValueStore>>,
    arguments: Vec<String>,
) -> Result<String, CommandError> {
    if arguments.len() != 1 {
        return Err(CommandError::InvalidLLenCommand);
    }

    let store_guard = store.lock().await;
    let stored_data = store_guard.get(&arguments[0]);

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
