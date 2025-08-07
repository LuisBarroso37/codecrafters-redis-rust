use std::sync::Arc;

use tokio::sync::Mutex;

use crate::{
    commands::command_error::CommandError,
    key_value_store::{DataType, KeyValueStore},
    resp::RespValue,
};

/// Represents the parsed arguments for LLEN command
struct LlenArguments {
    /// The key name to retrieve from the store
    key: String,
}

impl LlenArguments {
    /// Parses command arguments into an LlenArguments structure.
    ///
    /// This function validates that exactly one argument is provided for the LLEN command
    /// and creates an LlenArguments instance containing the list key to check.
    /// The LLEN command requires only a single argument: the key name of the list
    /// whose length should be determined.
    ///
    /// # Arguments
    ///
    /// * `arguments` - A vector of strings representing the command arguments.
    ///   Must contain exactly one element: the key name of the list.
    ///
    /// # Returns
    ///
    /// * `Ok(LlenArguments)` - Successfully parsed arguments containing:
    ///   - `key`: The name of the list key to check for length
    /// * `Err(CommandError::InvalidLLenCommand)` - If the number of arguments is not exactly 1
    ///
    /// # Redis Command Format
    ///
    /// The Redis LLEN command has the format: `LLEN key`
    /// - `key`: The name of the list whose length to return
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Valid LLEN command with one argument
    /// let result = LlenArguments::parse(vec!["mylist".to_string()]);
    /// // Returns: Ok(LlenArguments { key: "mylist".to_string() })
    ///
    /// // Invalid: no arguments provided
    /// let result = LlenArguments::parse(vec![]);
    /// // Returns: Err(CommandError::InvalidLLenCommand)
    ///
    /// // Invalid: too many arguments provided
    /// let result = LlenArguments::parse(vec!["list1".to_string(), "list2".to_string()]);
    /// // Returns: Err(CommandError::InvalidLLenCommand)
    /// ```
    fn parse(arguments: Vec<String>) -> Result<Self, CommandError> {
        if arguments.len() != 1 {
            return Err(CommandError::InvalidLLenCommand);
        }

        Ok(Self {
            key: arguments[0].clone(),
        })
    }
}

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
/// ```ignore
/// // LLEN mylist
/// let result = llen(&mut store, vec!["mylist".to_string()]).await;
/// // Returns: ":3\r\n" (if list has 3 elements) or ":0\r\n" (if empty/non-existent)
/// ```
pub async fn llen(
    store: &mut Arc<Mutex<KeyValueStore>>,
    arguments: Vec<String>,
) -> Result<String, CommandError> {
    let llen_arguments = LlenArguments::parse(arguments)?;

    let store_guard = store.lock().await;
    let stored_data = store_guard.get(&llen_arguments.key);

    let Some(value) = stored_data else {
        return Ok(RespValue::Integer(0).encode());
    };

    if let DataType::Array(ref list) = value.data {
        return Ok(RespValue::Integer(list.len() as i64).encode());
    } else {
        return Ok(RespValue::Integer(0).encode());
    }
}
