use std::{sync::Arc, time::Duration};

use tokio::{sync::Mutex, time::Instant};

use crate::{
    commands::command_error::CommandError,
    key_value_store::{DataType, KeyValueStore, Value},
    resp::RespValue,
};

/// Handles the Redis SET command.
///
/// Stores a key-value pair in the key-value store with optional expiration.
/// Supports the PX option to set expiration time in milliseconds.
///
/// # Arguments
///
/// * `store` - A thread-safe reference to the key-value store
/// * `arguments` - A vector containing either:
///   - 2 elements: [key, value] for permanent storage
///   - 4 elements: [key, value, "PX", milliseconds] for expiring storage
///
/// # Returns
///
/// * `Ok(String)` - A RESP-encoded "OK" simple string on success
/// * `Err(CommandError::InvalidSetCommand)` - If wrong number of arguments
/// * `Err(CommandError::InvalidSetCommandArgument)` - If expiration option is not "PX"
/// * `Err(CommandError::InvalidSetCommandExpiration)` - If expiration time is invalid
///
/// # Examples
///
/// ```
/// // SET mykey "hello"
/// let result = set(&mut store, vec!["mykey".to_string(), "hello".to_string()]).await;
/// // Returns: "+OK\r\n"
///
/// // SET mykey "hello" PX 1000  (expires in 1 second)
/// let result = set(&mut store, vec![
///     "mykey".to_string(),
///     "hello".to_string(),
///     "PX".to_string(),
///     "1000".to_string()
/// ]).await;
/// // Returns: "+OK\r\n"
/// ```
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
