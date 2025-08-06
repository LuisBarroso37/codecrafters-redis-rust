use std::sync::Arc;

use tokio::{sync::Mutex, time::Instant};

use crate::{
    commands::command_error::CommandError,
    key_value_store::{DataType, KeyValueStore},
    resp::RespValue,
};

/// Handles the Redis GET command.
///
/// Retrieves the value associated with a key from the key-value store.
/// If the key has an expiration time and has expired, it will be automatically
/// removed from the store and null will be returned.
///
/// # Arguments
///
/// * `store` - A thread-safe reference to the key-value store
/// * `arguments` - A vector containing exactly one string (the key to retrieve)
///
/// # Returns
///
/// * `Ok(String)` - A RESP-encoded response:
///   - Bulk string containing the value if key exists and hasn't expired
///   - Null if key doesn't exist, has expired, or stores non-string data
/// * `Err(CommandError::InvalidGetCommand)` - If the number of arguments is not exactly 1
///
/// # Examples
///
/// ```
/// // GET mykey
/// let result = get(&mut store, vec!["mykey".to_string()]).await;
/// // Returns: "$5\r\nhello\r\n" or "$-1\r\n" (null)
/// ```
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
