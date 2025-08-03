use std::sync::Arc;

use tokio::sync::Mutex;

use crate::{
    commands::command_error::CommandError,
    key_value_store::{DataType, KeyValueStore},
    resp::RespValue,
};

pub async fn type_command(
    store: &mut Arc<Mutex<KeyValueStore>>,
    arguments: Vec<String>,
) -> Result<String, CommandError> {
    if arguments.len() != 1 {
        return Err(CommandError::InvalidTypeCommand);
    }

    let store_guard = store.lock().await;

    if let Some(value) = store_guard.get(&arguments[0]) {
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
