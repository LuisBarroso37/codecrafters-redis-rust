use std::sync::Arc;

use tokio::{sync::Mutex, time::Instant};

use crate::{
    commands::command_error::CommandError,
    key_value_store::{DataType, KeyValueStore, Value},
    resp::RespValue,
};

/// Represents the parsed arguments for GET command
struct GetArguments {
    /// The key name to retrieve from the store
    key: String,
}

impl GetArguments {
    /// Parses command arguments into a GetArguments structure.
    ///
    /// This function validates that exactly one argument is provided for the GET command
    /// and creates a GetArguments instance containing the key to be retrieved.
    ///
    /// # Arguments
    ///
    /// * `arguments` - A vector of strings representing the command arguments
    ///
    /// # Returns
    ///
    /// * `Ok(GetArguments)` - Successfully parsed arguments with the key to retrieve
    /// * `Err(CommandError::InvalidGetCommand)` - If the number of arguments is not exactly 1
    ///
    /// # Examples
    ///
    /// ```
    /// let result = GetArguments::parse(vec!["mykey".to_string()]);
    /// // Returns: Ok(GetArguments { key: "mykey".to_string() })
    ///
    /// let result = GetArguments::parse(vec!["key1".to_string(), "key2".to_string()]);
    /// // Returns: Err(CommandError::InvalidGetCommand)
    /// ```
    fn parse(arguments: Vec<String>) -> Result<Self, CommandError> {
        if arguments.len() != 1 {
            return Err(CommandError::InvalidGetCommand);
        }

        return Ok(Self {
            key: arguments[0].clone(),
        });
    }
}

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
    let get_arguments = GetArguments::parse(arguments)?;

    let mut store_guard = store.lock().await;
    let stored_data = store_guard.get(&get_arguments.key);

    let Some(value) = stored_data else {
        return Ok(RespValue::Null.encode());
    };

    if is_value_expiration(&value) {
        store_guard.remove(&get_arguments.key);
        return Ok(RespValue::Null.encode());
    }

    match value.data {
        DataType::String(ref s) => Ok(RespValue::BulkString(s.clone()).encode()),
        _ => Ok(RespValue::Null.encode()),
    }
}

/// Checks if a stored value has expired based on its expiration timestamp.
///
/// This function examines the expiration field of a value and compares it
/// against the current time to determine if the value should be considered expired.
/// Values without an expiration time are never considered expired.
///
/// # Arguments
///
/// * `value` - A reference to the Value to check for expiration
///
/// # Returns
///
/// * `true` - If the value has an expiration time and that time has passed
/// * `false` - If the value has no expiration time or the expiration time hasn't been reached
///
/// # Examples
///
/// ```
/// let expired_value = Value { expiration: Some(past_instant), data: DataType::String("test".to_string()) };
/// let result = is_value_expiration(&expired_value);
/// // Returns: true
///
/// let valid_value = Value { expiration: None, data: DataType::String("test".to_string()) };
/// let result = is_value_expiration(&valid_value);
/// // Returns: false
/// ```
fn is_value_expiration(value: &Value) -> bool {
    if let Some(expiration) = value.expiration {
        if Instant::now() > expiration {
            return true;
        }

        return false;
    }

    return false;
}
