use std::sync::Arc;

use tokio::sync::Mutex;

use crate::{
    commands::command_error::CommandError,
    key_value_store::{DataType, KeyValueStore},
    resp::RespValue,
};

/// Handles the Redis TYPE command.
///
/// Returns the data type of the value stored at a given key.
/// This is useful for determining what operations can be performed on a key.
///
/// # Arguments
///
/// * `store` - A thread-safe reference to the key-value store
/// * `arguments` - A vector containing exactly one string (the key to check)
///
/// # Returns
///
/// * `Ok(String)` - A RESP-encoded simple string indicating the type:
///   - "string" for string values
///   - "list" for array/list values  
///   - "stream" for stream values
///   - "none" if the key doesn't exist
/// * `Err(CommandError::InvalidTypeCommand)` - If the number of arguments is not exactly 1
///
/// # Examples
///
/// ```
/// // TYPE mykey
/// let result = type_command(&mut store, vec!["mykey".to_string()]).await;
/// // Returns: "+string\r\n" or "+none\r\n"
/// ```
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
