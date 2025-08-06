use std::sync::Arc;

use tokio::sync::Mutex;

use crate::{
    commands::command_error::CommandError,
    key_value_store::{DataType, KeyValueStore},
    resp::RespValue,
};

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
///   - 1 element: [key] - pops one element
///   - 2 elements: [key, count] - pops specified number of elements
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
/// ```
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
    if arguments.len() < 1 || arguments.len() > 2 {
        return Err(CommandError::InvalidLPopCommand);
    }

    let mut store_guard = store.lock().await;
    let stored_data = store_guard.get_mut(&arguments[0]);

    let amount_of_elements_to_delete = if let Some(val) = arguments.get(1) {
        val.parse::<usize>()
            .map_err(|_| CommandError::InvalidLPopCommandArgument)?
    } else {
        1
    };

    match stored_data {
        Some(value) => {
            if let DataType::Array(ref mut list) = value.data {
                let mut vec = Vec::new();

                for _ in 0..amount_of_elements_to_delete {
                    if let Some(removed) = list.pop_front() {
                        vec.push(removed);
                    }
                }

                if vec.len() == 0 {
                    return Ok(RespValue::Null.encode());
                } else if vec.len() == 1 {
                    return Ok(RespValue::BulkString(vec[0].clone()).encode());
                } else {
                    return Ok(RespValue::encode_array_from_strings(vec));
                }
            } else {
                return Ok(RespValue::Null.encode());
            }
        }
        None => {
            return Ok(RespValue::Null.encode());
        }
    }
}
