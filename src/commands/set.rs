use std::{sync::Arc, time::Duration};

use tokio::{sync::Mutex, time::Instant};

use crate::{
    commands::command_error::CommandError,
    key_value_store::{DataType, KeyValueStore, Value},
    resp::RespValue,
};

/// Represents the parsed arguments for GET command
pub struct SetArguments {
    /// The key name to retrieve from the store
    key: String,
    /// The value to be stored under the given key
    value: String,
    /// Expiration of key value pair
    expiration: Option<Instant>,
}

impl SetArguments {
    /// Parses command arguments into a SetArguments structure.
    ///
    /// This function validates and processes the arguments for the SET command,
    /// which supports both permanent and expiring key-value storage. It handles
    /// two formats: basic key-value pairs and key-value pairs with expiration.
    ///
    /// # Arguments
    ///
    /// * `arguments` - A vector of strings representing the command arguments:
    ///   - Format 1: `[key, value]` - For permanent storage
    ///   - Format 2: `[key, value, "PX", milliseconds]` - For expiring storage
    ///
    /// # Returns
    ///
    /// * `Ok(SetArguments)` - Successfully parsed arguments containing:
    ///   - `key`: The key name for storage
    ///   - `value`: The value to be stored
    ///   - `expiration`: Optional expiration time calculated from current time + milliseconds
    /// * `Err(CommandError::InvalidSetCommand)` - If the number of arguments is not 2 or 4
    /// * `Err(CommandError::InvalidSetCommandArgument)` - If the expiration option is not "PX"
    /// * `Err(CommandError::InvalidSetCommandExpiration)` - If the expiration time is not a valid integer
    ///
    /// # Expiration Logic
    ///
    /// When 4 arguments are provided:
    /// 1. The 3rd argument must be "PX" (case-insensitive)
    /// 2. The 4th argument must be a valid u64 representing milliseconds
    /// 3. Expiration time is calculated as `current_time + milliseconds`
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Basic SET without expiration
    /// let result = SetArguments::parse(vec!["mykey".to_string(), "hello".to_string()]);
    /// // Returns: Ok(SetArguments { key: "mykey", value: "hello", expiration: None })
    ///
    /// // SET with expiration (expires in 1000ms)
    /// let result = SetArguments::parse(vec![
    ///     "mykey".to_string(),
    ///     "hello".to_string(),
    ///     "PX".to_string(),
    ///     "1000".to_string()
    /// ]);
    /// // Returns: Ok(SetArguments { key: "mykey", value: "hello", expiration: Some(future_instant) })
    ///
    /// // Invalid argument count
    /// let result = SetArguments::parse(vec!["mykey".to_string()]);
    /// // Returns: Err(CommandError::InvalidSetCommand)
    ///
    /// // Invalid expiration option
    /// let result = SetArguments::parse(vec![
    ///     "mykey".to_string(),
    ///     "hello".to_string(),
    ///     "EX".to_string(),  // Should be "PX"
    ///     "1000".to_string()
    /// ]);
    /// // Returns: Err(CommandError::InvalidSetCommandArgument)
    /// ```
    fn parse(arguments: Vec<String>) -> Result<Self, CommandError> {
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

        Ok(Self {
            key: arguments[0].clone(),
            value: arguments[1].clone(),
            expiration,
        })
    }
}

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
/// ```ignore
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
    let set_arguments = SetArguments::parse(arguments)?;

    let mut store_guard = store.lock().await;
    store_guard.insert(
        set_arguments.key,
        Value {
            data: DataType::String(set_arguments.value),
            expiration: set_arguments.expiration,
        },
    );

    Ok(RespValue::SimpleString("OK".to_string()).encode())
}
