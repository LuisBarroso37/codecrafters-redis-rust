use std::sync::Arc;

use tokio::sync::Mutex;

use crate::{
    commands::command_error::CommandError,
    key_value_store::{DataType, KeyValueStore},
    resp::RespValue,
};

/// Represents the parsed arguments for LPOP command
///
/// Contains the key name and count of elements to pop from the left side of a list.
/// The LPOP command supports both single element and multiple element operations.
struct LpopArguments {
    /// The key name of the list to pop elements from
    key: String,
    /// Number of elements to pop from the left side (defaults to 1 if not specified)
    count: usize,
}

impl LpopArguments {
    /// Parses command arguments into an LpopArguments structure.
    ///
    /// This function validates and processes the arguments for the LPOP command,
    /// which removes elements from the left (head) of a list. It supports both
    /// single element removal and bulk removal with a specified count.
    ///
    /// # Arguments
    ///
    /// * `arguments` - A vector of strings representing the command arguments:
    ///   - Format 1: `[key]` - Pop one element from the list
    ///   - Format 2: `[key, count]` - Pop specified number of elements from the list
    ///
    /// # Returns
    ///
    /// * `Ok(LpopArguments)` - Successfully parsed arguments containing:
    ///   - `key`: The name of the list key to pop elements from
    ///   - `count`: Number of elements to pop (1 if not specified, parsed value if provided)
    /// * `Err(CommandError::InvalidLPopCommand)` - If the number of arguments is not 1 or 2
    /// * `Err(CommandError::InvalidLPopCommandArgument)` - If the count argument is not a valid positive integer
    ///
    /// # Default Behavior
    ///
    /// When only the key is provided (1 argument), the count defaults to 1,
    /// meaning a single element will be popped from the list.
    ///
    /// # Redis Command Format
    ///
    /// The Redis LPOP command has the format: `LPOP key [count]`
    /// - `key`: The name of the list to pop elements from
    /// - `count` (optional): Number of elements to pop (must be positive integer)
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Pop single element (default behavior)
    /// let result = LpopArguments::parse(vec!["mylist".to_string()]);
    /// // Returns: Ok(LpopArguments { key: "mylist", count: 1 })
    ///
    /// // Pop multiple elements
    /// let result = LpopArguments::parse(vec!["mylist".to_string(), "3".to_string()]);
    /// // Returns: Ok(LpopArguments { key: "mylist", count: 3 })
    ///
    /// // Invalid: no arguments provided
    /// let result = LpopArguments::parse(vec![]);
    /// // Returns: Err(CommandError::InvalidLPopCommand)
    ///
    /// // Invalid: too many arguments
    /// let result = LpopArguments::parse(vec!["key1".to_string(), "2".to_string(), "extra".to_string()]);
    /// // Returns: Err(CommandError::InvalidLPopCommand)
    ///
    /// // Invalid: count is not a valid integer
    /// let result = LpopArguments::parse(vec!["mylist".to_string(), "invalid".to_string()]);
    /// // Returns: Err(CommandError::InvalidLPopCommandArgument)
    /// ```
    fn parse(arguments: Vec<String>) -> Result<Self, CommandError> {
        if arguments.len() < 1 || arguments.len() > 2 {
            return Err(CommandError::InvalidLPopCommand);
        }

        let count = if let Some(val) = arguments.get(1) {
            val.parse::<usize>()
                .map_err(|_| CommandError::InvalidLPopCommandArgument)?
        } else {
            1
        };

        Ok(Self {
            key: arguments[0].clone(),
            count,
        })
    }
}

/// Handles the Redis LPOP command.
///
/// Removes and returns one or more elements from the left (head) of a list.
/// If no count is specified, removes and returns a single element.
/// If the key doesn't exist or doesn't contain a list, returns null.
///
/// # Arguments
///
/// * `store` - A thread-safe reference to the key-value store
/// * `arguments` - A vector containing:
///   - 1 element: \[key\] - pops one element
///   - 2 elements: \[key, count\] - pops specified number of elements
///
/// # Returns
///
/// * `Ok(String)` - A RESP-encoded response:
///   - Bulk string if popping one element
///   - Array if popping multiple elements
///   - Null if key doesn't exist, not a list, or list is empty
/// * `Err(CommandError::InvalidLPopCommand)` - If wrong number of arguments
/// * `Err(CommandError::InvalidLPopCommandArgument)` - If count is not a valid integer
///
/// # Examples
///
/// ```ignore
/// // LPOP mylist
/// let result = lpop(&mut store, vec!["mylist".to_string()]).await;
/// // Returns: "$3\r\nval\r\n" (single element) or "$-1\r\n" (null if empty)
///
/// // LPOP mylist 3
/// let result = lpop(&mut store, vec!["mylist".to_string(), "3".to_string()]).await;
/// // Returns: "*3\r\n$3\r\none\r\n$3\r\ntwo\r\n$5\r\nthree\r\n" (array of elements)
/// ```
pub async fn lpop(
    store: &mut Arc<Mutex<KeyValueStore>>,
    arguments: Vec<String>,
) -> Result<String, CommandError> {
    let lpop_arguments = LpopArguments::parse(arguments)?;

    let mut store_guard = store.lock().await;

    let Some(value) = store_guard.get_mut(&lpop_arguments.key) else {
        return Ok(RespValue::NullBulkString.encode());
    };

    let DataType::Array(ref mut list) = value.data else {
        return Ok(RespValue::NullBulkString.encode());
    };

    let mut vec = Vec::new();

    for _ in 0..lpop_arguments.count {
        if let Some(removed) = list.pop_front() {
            vec.push(removed);
        }
    }

    match vec.len() {
        0 => Ok(RespValue::NullBulkString.encode()),
        1 => Ok(RespValue::BulkString(vec[0].clone()).encode()),
        _ => Ok(RespValue::encode_array_from_strings(vec)),
    }
}
