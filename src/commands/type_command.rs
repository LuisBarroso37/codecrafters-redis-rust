use std::sync::Arc;

use tokio::sync::Mutex;

use crate::{
    commands::{command_error::CommandError, command_handler::CommandResult},
    key_value_store::{DataType, KeyValueStore},
    resp::RespValue,
};

/// Represents the parsed arguments for TYPE command
pub struct TypeArguments {
    /// The key name to retrieve from the store
    key: String,
}

impl TypeArguments {
    /// Parses command arguments into a `TypeArguments` struct.
    ///
    /// Validates that exactly one argument (the key) is provided, as required
    /// by the Redis TYPE command specification.
    ///
    /// # Arguments
    ///
    /// * `arguments` - Vector of command arguments, should contain exactly one key
    ///
    /// # Returns
    ///
    /// * `Ok(TypeArguments)` - Successfully parsed arguments with the key
    /// * `Err(CommandError::InvalidTypeCommand)` - If the number of arguments is not exactly 1
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let args = vec!["mykey".to_string()];
    /// let parsed = TypeArguments::parse(args)?;
    /// assert_eq!(parsed.key, "mykey");
    ///
    /// // Invalid: too many arguments
    /// let invalid_args = vec!["key1".to_string(), "key2".to_string()];
    /// assert!(TypeArguments::parse(invalid_args).is_err());
    /// ```
    pub fn parse(arguments: Vec<String>) -> Result<Self, CommandError> {
        if arguments.len() != 1 {
            return Err(CommandError::InvalidTypeCommand);
        }

        Ok(Self {
            key: arguments[0].clone(),
        })
    }
}

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
/// ```ignore
/// // TYPE mykey
/// let result = type_command(&mut store, vec!["mykey".to_string()]).await;
/// // Returns: "+string\r\n" or "+none\r\n"
/// ```
pub async fn type_command(
    store: Arc<Mutex<KeyValueStore>>,
    arguments: Vec<String>,
) -> Result<CommandResult, CommandError> {
    let type_arguments = TypeArguments::parse(arguments)?;

    let store_guard = store.lock().await;

    let Some(value) = store_guard.get(&type_arguments.key) else {
        return Ok(CommandResult::Response(
            RespValue::SimpleString("none".to_string()).encode(),
        ));
    };

    match value.data {
        DataType::String(_) => {
            return Ok(CommandResult::Response(
                RespValue::SimpleString("string".to_string()).encode(),
            ));
        }
        DataType::Array(_) => {
            return Ok(CommandResult::Response(
                RespValue::SimpleString("list".to_string()).encode(),
            ));
        }
        DataType::Stream(_) => {
            return Ok(CommandResult::Response(
                RespValue::SimpleString("stream".to_string()).encode(),
            ));
        }
    }
}
