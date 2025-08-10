use std::{collections::VecDeque, sync::Arc};

use tokio::sync::Mutex;

use crate::{
    commands::command_error::CommandError,
    key_value_store::{DataType, KeyValueStore, Value},
    resp::RespValue,
    state::State,
};

/// Represents the parsed arguments for LPUSH and RPUSH commands
pub struct PushArrayOperations {
    /// The key name to retrieve from the store
    key: String,
    /// The values to be pushed to the list
    values: Vec<String>,
}

impl PushArrayOperations {
    /// Parses command arguments into a `PushArrayOperations` struct.
    ///
    /// Validates that at least a key and one value are provided, returning
    /// appropriate errors based on the operation type (LPUSH vs RPUSH).
    ///
    /// # Arguments
    ///
    /// * `arguments` - Vector of command arguments [key, value1, value2, ...]
    /// * `should_prepend` - Flag indicating if this is for LPUSH (true) or RPUSH (false)
    ///
    /// # Returns
    ///
    /// * `Ok(PushArrayOperations)` - Successfully parsed arguments
    /// * `Err(CommandError::InvalidLPushCommand)` - If LPUSH has insufficient arguments
    /// * `Err(CommandError::InvalidRPushCommand)` - If RPUSH has insufficient arguments
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let args = vec!["mylist".to_string(), "value1".to_string(), "value2".to_string()];
    /// let parsed = PushArrayOperations::parse(args, true)?;
    /// assert_eq!(parsed.key, "mylist");
    /// assert_eq!(parsed.values, vec!["value1", "value2"]);
    /// ```
    pub fn parse(arguments: Vec<String>, should_prepend: bool) -> Result<Self, CommandError> {
        if arguments.len() < 2 {
            return if should_prepend {
                Err(CommandError::InvalidLPushCommand)
            } else {
                Err(CommandError::InvalidRPushCommand)
            };
        }

        Ok(Self {
            key: arguments[0].clone(),
            values: arguments[1..].to_vec(),
        })
    }
}

/// Handles the Redis RPUSH command.
///
/// Pushes one or more elements to the right (tail) of a list.
/// If the key doesn't exist, a new list is created.
/// If the key exists but is not a list, an error is returned.
///
/// # Arguments
///
/// * `store` - A thread-safe reference to the key-value store
/// * `state` - A thread-safe reference to the server state (for notifications)
/// * `arguments` - A vector containing [key, element1, element2, ...]
///
/// # Returns
///
/// * `Ok(String)` - A RESP-encoded integer representing the new length of the list
/// * `Err(CommandError::InvalidRPushCommand)` - If fewer than 2 arguments provided
/// * `Err(CommandError::InvalidDataTypeForKey)` - If key exists but is not a list
///
/// # Examples
///
/// ```ignore
/// // RPUSH mylist "hello" "world"
/// let result = rpush(&mut store, &mut state, vec!["mylist".to_string(), "hello".to_string(), "world".to_string()]).await;
/// // Returns: ":2\r\n" (new length of the list)
/// ```
pub async fn rpush(
    store: &mut Arc<Mutex<KeyValueStore>>,
    state: &mut Arc<Mutex<State>>,
    arguments: Vec<String>,
) -> Result<String, CommandError> {
    return push_array_operations(store, state, arguments, false).await;
}

/// Handles the Redis LPUSH command.
///
/// Pushes one or more elements to the left (head) of a list.
/// If the key doesn't exist, a new list is created.
/// If the key exists but is not a list, an error is returned.
///
/// # Arguments
///
/// * `store` - A thread-safe reference to the key-value store
/// * `state` - A thread-safe reference to the server state (for notifications)
/// * `arguments` - A vector containing [key, element1, element2, ...]
///
/// # Returns
///
/// * `Ok(String)` - A RESP-encoded integer representing the new length of the list
/// * `Err(CommandError::InvalidLPushCommand)` - If fewer than 2 arguments provided
/// * `Err(CommandError::InvalidDataTypeForKey)` - If key exists but is not a list
///
/// # Examples
///
/// ```ignore
/// // LPUSH mylist "world" "hello"
/// let result = lpush(&mut store, &mut state, vec!["mylist".to_string(), "world".to_string(), "hello".to_string()]).await;
/// // Returns: ":2\r\n" (new length of the list)
/// ```
pub async fn lpush(
    store: &mut Arc<Mutex<KeyValueStore>>,
    state: &mut Arc<Mutex<State>>,
    arguments: Vec<String>,
) -> Result<String, CommandError> {
    return push_array_operations(store, state, arguments, true).await;
}

/// Internal function that implements the push operations for both LPUSH and RPUSH.
///
/// Handles the common logic for adding elements to either end of a list,
/// including creating new lists, validating data types, and notifying blocked clients.
///
/// # Arguments
///
/// * `store` - A thread-safe reference to the key-value store
/// * `state` - A thread-safe reference to the server state (for BLPOP notifications)
/// * `arguments` - A vector containing [key, element1, element2, ...]
/// * `should_prepend` - If true, prepends elements (LPUSH); if false, appends elements (RPUSH)
///
/// # Returns
///
/// * `Ok(String)` - A RESP-encoded integer representing the new length of the list
/// * `Err(CommandError)` - Various errors based on input validation or data type conflicts
async fn push_array_operations(
    store: &mut Arc<Mutex<KeyValueStore>>,
    state: &mut Arc<Mutex<State>>,
    arguments: Vec<String>,
    should_prepend: bool,
) -> Result<String, CommandError> {
    let push_array_arguments = PushArrayOperations::parse(arguments, should_prepend)?;

    let (pushed_values_count, was_empty_before) = {
        let mut store_guard = store.lock().await;

        match store_guard.get_mut(&push_array_arguments.key) {
            Some(value) => {
                let DataType::Array(ref mut list) = value.data else {
                    return Err(CommandError::InvalidDataTypeForKey);
                };

                let was_empty_before = list.is_empty();
                add_values_to_list(list, &push_array_arguments.values, should_prepend);

                (list.len(), was_empty_before)
            }
            None => {
                let mut list = VecDeque::new();
                add_values_to_list(&mut list, &push_array_arguments.values, should_prepend);

                let list_length = list.len();
                store_guard.insert(
                    push_array_arguments.key.clone(),
                    Value {
                        data: DataType::Array(list),
                        expiration: None,
                    },
                );

                (list_length, true)
            }
        }
    };

    // This should never happen with the validation in place in the beginning of the function
    if pushed_values_count == 0 {
        return Err(CommandError::DataNotFound);
    }

    // Notify BLPOP subscribers if list was empty or key did not exist before
    if was_empty_before {
        let mut state_guard = state.lock().await;
        state_guard.send_to_blpop_subscriber(&push_array_arguments.key, true);
    }

    return Ok(RespValue::Integer(pushed_values_count as i64).encode());
}

/// Adds multiple values to a list in the specified direction.
///
/// This helper function handles the actual insertion of values into a `VecDeque`,
/// either at the front (for LPUSH) or at the back (for RPUSH). Values are added
/// in the order they appear in the input slice.
///
/// # Arguments
///
/// * `list` - Mutable reference to the list where values will be added
/// * `values` - Slice of string values to be added to the list
/// * `should_prepend` - If true, adds values to the front; if false, adds to the back
///
/// # Behavior
///
/// - For LPUSH (`should_prepend = true`): Values are added to the front in order,
///   so the first value in the slice becomes the new head of the list
/// - For RPUSH (`should_prepend = false`): Values are added to the back in order,
///   so the last value in the slice becomes the new tail of the list
///
/// # Examples
///
/// ```ignore
/// let mut list = VecDeque::new();
/// let values = ["a", "b", "c"];
///
/// // LPUSH behavior: list becomes ["c", "b", "a"]
/// add_values_to_list(&mut list, &values, true);
///
/// // RPUSH behavior: list becomes ["a", "b", "c"]
/// add_values_to_list(&mut list, &values, false);
/// ```
fn add_values_to_list(list: &mut VecDeque<String>, values: &[String], should_prepend: bool) {
    for value in values {
        if should_prepend {
            list.push_front(value.clone());
        } else {
            list.push_back(value.clone());
        }
    }
}
