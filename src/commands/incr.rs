use std::sync::Arc;

use tokio::sync::Mutex;

use crate::{
    commands::CommandError,
    key_value_store::{DataType, KeyValueStore, Value},
    resp::RespValue,
};

/// Represents the parsed arguments for the INCR command.
///
/// The INCR command takes exactly one argument: the key name to increment.
/// Format: `INCR key`
struct IncrArguments {
    /// The key name to increment in the store
    key: String,
}

impl IncrArguments {
    /// Parses command arguments into structured IncrArguments.
    ///
    /// Validates that exactly one argument (the key name) is provided.
    ///
    /// # Arguments
    ///
    /// * `arguments` - Raw command arguments from the Redis client
    ///
    /// # Returns
    ///
    /// * `Ok(IncrArguments)` - Successfully parsed arguments with the key name
    /// * `Err(CommandError::InvalidIncrCommand)` - If not exactly one argument is provided
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Valid: INCR mykey
    /// let args = IncrArguments::parse(vec!["mykey".to_string()])?;
    /// assert_eq!(args.key, "mykey");
    ///
    /// // Invalid: INCR (no arguments)
    /// let result = IncrArguments::parse(vec![]);
    /// assert!(result.is_err());
    ///
    /// // Invalid: INCR key1 key2 (too many arguments)
    /// let result = IncrArguments::parse(vec!["key1".to_string(), "key2".to_string()]);
    /// assert!(result.is_err());
    /// ```
    fn parse(arguments: Vec<String>) -> Result<Self, CommandError> {
        if arguments.len() != 1 {
            return Err(CommandError::InvalidIncrCommand);
        }

        Ok(Self {
            key: arguments[0].clone(),
        })
    }
}

/// Handles the Redis INCR command.
///
/// Increments the integer value stored at the specified key by one. If the key
/// doesn't exist, it is initialized to 0 before incrementing, resulting in a
/// value of 1. The key must contain a string representation of an integer.
///
/// # Behavior
///
/// - **Key doesn't exist**: Creates the key with value 1
/// - **Key contains integer string**: Increments the value by 1
/// - **Key contains non-integer string**: Returns an error
/// - **Key contains non-string data**: Returns an error
///
/// # Arguments
///
/// * `store` - Thread-safe reference to the key-value store
/// * `arguments` - Command arguments containing exactly one key name
///
/// # Returns
///
/// * `Ok(String)` - RESP-encoded integer representing the new value after increment
/// * `Err(CommandError::InvalidIncrCommand)` - If not exactly one argument is provided
/// * `Err(CommandError::InvalidIncrValue)` - If the stored value is not a valid integer
/// * `Err(CommandError::InvalidDataTypeForKey)` - If the key contains non-string data
///
/// # Examples
///
/// ```ignore
/// // Increment existing integer key
/// // SET mykey "42"
/// // INCR mykey
/// let result = incr(&mut store, vec!["mykey".to_string()]).await?;
/// // Returns: ":43\r\n" (RESP integer 43)
///
/// // Increment non-existent key
/// // INCR newkey
/// let result = incr(&mut store, vec!["newkey".to_string()]).await?;
/// // Returns: ":1\r\n" (RESP integer 1)
///
/// // Error: non-integer value
/// // SET stringkey "hello"
/// // INCR stringkey
/// let result = incr(&mut store, vec!["stringkey".to_string()]).await;
/// // Returns: Err(CommandError::InvalidIncrValue)
/// ```
///
/// # Redis Compatibility
///
/// This implementation follows Redis INCR semantics:
/// - Atomic increment operation
/// - Initialization of missing keys to 0 before increment
/// - Error on non-integer string values
/// - Returns the new value after increment
pub async fn incr(
    store: &mut Arc<Mutex<KeyValueStore>>,
    arguments: Vec<String>,
) -> Result<String, CommandError> {
    let incr_arguments = IncrArguments::parse(arguments)?;

    let mut store_guard = store.lock().await;

    let Some(value) = store_guard.get_mut(&incr_arguments.key) else {
        store_guard.insert(
            incr_arguments.key,
            Value {
                data: DataType::String("1".to_string()),
                expiration: None,
            },
        );
        return Ok(RespValue::Integer(1).encode());
    };

    match value.data {
        DataType::String(ref mut stored_data) => {
            let int = stored_data
                .parse::<i64>()
                .map_err(|_| CommandError::InvalidIncrValue)?;
            let incremented_int = int + 1;
            *stored_data = incremented_int.to_string();

            Ok(RespValue::Integer(incremented_int).encode())
        }
        _ => return Err(CommandError::InvalidDataTypeForKey),
    }
}
