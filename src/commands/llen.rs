use std::sync::Arc;

use tokio::sync::Mutex;

use crate::{
    commands::command_error::CommandError,
    key_value_store::{DataType, KeyValueStore},
    resp::RespValue,
};

/// Handles the Redis LLEN command.
///
/// Returns the length (number of elements) of a list stored at the given key.
/// If the key doesn't exist or doesn't contain a list, returns 0.
///
/// # Arguments
///
/// * `store` - A thread-safe reference to the key-value store
/// * `arguments` - A vector containing exactly one string (the key of the list)
///
/// # Returns
///
/// * `Ok(String)` - A RESP-encoded integer representing the list length
/// * `Err(CommandError::InvalidLLenCommand)` - If the number of arguments is not exactly 1
///
/// # Examples
///
/// ```
/// // LLEN mylist
/// let result = llen(&mut store, vec!["mylist".to_string()]).await;
/// // Returns: ":3\r\n" (if list has 3 elements) or ":0\r\n" (if empty/non-existent)
/// ```
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
