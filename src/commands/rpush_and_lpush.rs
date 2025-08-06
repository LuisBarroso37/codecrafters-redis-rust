use std::{collections::VecDeque, sync::Arc};

use tokio::sync::Mutex;

use crate::{
    commands::command_error::CommandError,
    key_value_store::{DataType, KeyValueStore, Value},
    resp::RespValue,
    state::State,
};

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
/// ```
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
/// ```
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
    if arguments.len() < 2 {
        return if should_prepend {
            Err(CommandError::InvalidLPushCommand)
        } else {
            Err(CommandError::InvalidRPushCommand)
        };
    }

    let mut array_length = 0;
    let mut was_empty_before = false;

    let mut store_guard = store.lock().await;

    store_guard
        .entry(arguments[0].clone())
        .and_modify(|v| {
            if let DataType::Array(ref mut list) = v.data {
                was_empty_before = list.is_empty();

                for i in 1..arguments.len() {
                    if should_prepend {
                        list.push_front(arguments[i].clone());
                    } else {
                        list.push_back(arguments[i].clone());
                    }
                }

                array_length = list.len();
            }
        })
        .or_insert_with(|| {
            let mut list = VecDeque::new();
            was_empty_before = true; // New list is considered as was empty before

            for i in 1..arguments.len() {
                if should_prepend {
                    list.push_front(arguments[i].clone());
                } else {
                    list.push_back(arguments[i].clone());
                }
            }

            array_length = list.len();

            return Value {
                data: DataType::Array(list),
                expiration: None,
            };
        });
    drop(store_guard);

    if array_length == 0 {
        return Err(CommandError::DataNotFound);
    }

    // Only notify subscribers if the list was empty before and now has elements
    if was_empty_before {
        let mut state_guard = state.lock().await;
        state_guard.send_to_blpop_subscriber(&arguments[0], true);
    }

    return Ok(RespValue::Integer(array_length as i64).encode());
}
