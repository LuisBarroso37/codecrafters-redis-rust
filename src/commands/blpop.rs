use std::{sync::Arc, time::Duration};

use tokio::sync::{Mutex, oneshot};

use crate::{
    commands::command_error::CommandError,
    key_value_store::{DataType, KeyValueStore},
    resp::RespValue,
    state::{BlpopSubscriber, State},
};

/// Represents the parsed arguments for BLPOP command
///
/// Contains the key name and blocking duration for the BLPOP operation.
/// The BLPOP command blocks until an element is available or timeout expires.
pub struct BlpopArguments {
    /// The key name of the list to block and pop from
    key: String,
    /// Blocking duration in seconds (0.0 means block indefinitely)
    block_duration_secs: f64,
}

impl BlpopArguments {
    /// Parses command arguments into a BlpopArguments structure.
    ///
    /// This function validates and processes the arguments for the BLPOP command,
    /// which blocks until an element becomes available in the specified list
    /// or until the timeout expires.
    ///
    /// # Arguments
    ///
    /// * `arguments` - A vector of strings representing the command arguments:
    ///   - Format: `[key, timeout_seconds]`
    ///   - `key`: The name of the list to pop from
    ///   - `timeout_seconds`: Timeout in seconds (can be fractional, 0 means infinite)
    ///
    /// # Returns
    ///
    /// * `Ok(BlpopArguments)` - Successfully parsed arguments containing:
    ///   - `key`: The name of the list key to pop from
    ///   - `block_duration_secs`: Timeout duration (0.0 for infinite blocking)
    /// * `Err(CommandError::InvalidBLPopCommand)` - If the number of arguments is not exactly 2
    /// * `Err(CommandError::InvalidBLPopCommandArgument)` - If timeout is not a valid number
    ///
    /// # Redis Command Format
    ///
    /// The Redis BLPOP command has the format: `BLPOP key timeout`
    /// - `key`: The name of the list to pop from
    /// - `timeout`: Timeout in seconds (0 means block indefinitely)
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Block for up to 5 seconds
    /// let result = BlpopArguments::parse(vec!["mylist".to_string(), "5.0".to_string()]);
    /// // Returns: Ok(BlpopArguments { key: "mylist", block_duration_secs: 5.0 })
    ///
    /// // Block indefinitely
    /// let result = BlpopArguments::parse(vec!["mylist".to_string(), "0".to_string()]);
    /// // Returns: Ok(BlpopArguments { key: "mylist", block_duration_secs: 0.0 })
    ///
    /// // Invalid: wrong number of arguments
    /// let result = BlpopArguments::parse(vec!["mylist".to_string()]);
    /// // Returns: Err(CommandError::InvalidBLPopCommand)
    ///
    /// // Invalid: timeout is not a number
    /// let result = BlpopArguments::parse(vec!["mylist".to_string(), "invalid".to_string()]);
    /// // Returns: Err(CommandError::InvalidBLPopCommandArgument)
    /// ```
    pub fn parse(arguments: Vec<String>) -> Result<Self, CommandError> {
        if arguments.len() != 2 {
            return Err(CommandError::InvalidBLPopCommand);
        }

        let block_duration_secs = arguments[1]
            .parse::<f64>()
            .map_err(|_| CommandError::InvalidBLPopCommandArgument)?;

        Ok(Self {
            key: arguments[0].clone(),
            block_duration_secs,
        })
    }
}

/// Handles the Redis BLPOP command.
///
/// Blocking version of LPOP that waits for an element to become available.
/// If the list is empty or doesn't exist, the command blocks until:
/// 1. An element is pushed to the list by another client
/// 2. The timeout expires
///
/// # Arguments
///
/// * `server_address` - The address of the current server (for subscriber identification)
/// * `store` - A thread-safe reference to the key-value store
/// * `state` - A thread-safe reference to the server state (for blocking operations)
/// * `arguments` - A vector containing exactly 2 elements: [key, timeout_seconds]
///
/// # Returns
///
/// * `Ok(String)` - A RESP-encoded response:
///   - Array with [key, value] if an element is available
///   - Null if timeout expires without finding an element
/// * `Err(CommandError::InvalidBLPopCommand)` - If the number of arguments is not exactly 2
/// * `Err(CommandError::InvalidBLPopCommandArgument)` - If timeout is not a valid number
///
/// # Examples
///
/// ```ignore
/// // BLPOP mylist 5  (block for up to 5 seconds)
/// let result = blpop("127.0.0.1:6379".to_string(), &mut store, &mut state,
///                   vec!["mylist".to_string(), "5".to_string()]).await;
/// // Returns: "*2\r\n$6\r\nmylist\r\n$5\r\nvalue\r\n" (key-value pair) or "$-1\r\n" (timeout)
/// ```
pub async fn blpop(
    server_address: String,
    store: &mut Arc<Mutex<KeyValueStore>>,
    state: &mut Arc<Mutex<State>>,
    arguments: Vec<String>,
) -> Result<String, CommandError> {
    let blpop_arguments = BlpopArguments::parse(arguments)?;

    if let Some(value) = remove_first_element_from_list(store, &blpop_arguments.key).await {
        return Ok(RespValue::encode_array_from_strings(vec![
            blpop_arguments.key,
            value,
        ]));
    }

    let (sender, mut receiver) = oneshot::channel();
    add_subscriber(
        state,
        blpop_arguments.key.clone(),
        server_address.clone(),
        sender,
    )
    .await;

    let data = wait_for_data(&mut receiver, blpop_arguments.block_duration_secs).await;
    remove_subscriber(state, &blpop_arguments.key, &server_address).await;

    if data.is_none() {
        return Ok(RespValue::NullBulkString.encode());
    }

    if let Some(value) = remove_first_element_from_list(store, &blpop_arguments.key).await {
        Ok(RespValue::encode_array_from_strings(vec![
            blpop_arguments.key,
            value,
        ]))
    } else {
        Ok(RespValue::NullBulkString.encode())
    }
}

/// Removes and returns the first element from a list stored at the given key.
///
/// This is a helper function for BLPOP that atomically removes the leftmost
/// element from a list. If the key doesn't exist, doesn't contain a list,
/// or the list is empty, returns None.
///
/// # Arguments
///
/// * `store` - A thread-safe reference to the key-value store
/// * `key` - The key name of the list to pop from
///
/// # Returns
///
/// * `Some(String)` - The leftmost element if the list exists and has elements
/// * `None` - If the key doesn't exist, is not a list, or the list is empty
async fn remove_first_element_from_list(
    store: &mut Arc<Mutex<KeyValueStore>>,
    key: &str,
) -> Option<String> {
    let mut store_guard = store.lock().await;

    let Some(stored_data) = store_guard.get_mut(key) else {
        return None;
    };

    if let DataType::Array(ref mut list) = stored_data.data {
        list.pop_front()
    } else {
        None
    }
}

/// Adds a BLPOP subscriber to the server state for the given key.
///
/// This function registers a client that is waiting for data to be pushed
/// to a specific list key. When an element is added to the list, the subscriber
/// will be notified through the oneshot channel.
///
/// # Arguments
///
/// * `state` - A thread-safe reference to the server state
/// * `key` - The key name the subscriber is waiting for
/// * `server_address` - The address of the subscriber's server (for identification)
/// * `sender` - Oneshot sender to notify the subscriber when data becomes available
async fn add_subscriber(
    state: &mut Arc<Mutex<State>>,
    key: String,
    server_address: String,
    sender: oneshot::Sender<bool>,
) {
    let subscriber = BlpopSubscriber {
        server_address,
        sender,
    };

    let mut state_guard = state.lock().await;
    state_guard.add_blpop_subscriber(key, subscriber);
}

/// Removes a BLPOP subscriber from the server state for the given key.
///
/// This function unregisters a client that was waiting for data to be pushed
/// to a specific list key. This is called when the blocking operation times out
/// or when data is successfully delivered to the subscriber.
///
/// # Arguments
///
/// * `state` - A thread-safe reference to the server state
/// * `key` - The key name the subscriber was waiting for
/// * `server_address` - The address of the subscriber's server (for identification)
async fn remove_subscriber(state: &mut Arc<Mutex<State>>, key: &str, server_address: &str) {
    let mut state_guard = state.lock().await;
    state_guard.remove_blpop_subscriber(key, &server_address);
}

/// Waits for data to become available on a oneshot channel with optional timeout.
///
/// This function handles the blocking behavior of BLPOP by waiting for a notification
/// that data has become available. It supports both infinite blocking (when duration is 0.0)
/// and timeout-based blocking.
///
/// # Arguments
///
/// * `receiver` - Oneshot receiver to wait for notification
/// * `blocking_duration_secs` - Timeout in seconds (0.0 means block indefinitely)
///
/// # Returns
///
/// * `Some(bool)` - If notification is received before timeout
/// * `None` - If the operation times out or the sender is dropped
///
/// # Behavior
///
/// - If `blocking_duration_secs` is 0.0, blocks indefinitely until notification
/// - If `blocking_duration_secs` > 0.0, blocks for the specified duration
/// - Returns None if timeout expires or channel is closed
async fn wait_for_data(
    receiver: &mut oneshot::Receiver<bool>,
    blocking_duration_secs: f64,
) -> Option<bool> {
    match blocking_duration_secs {
        0.0 => receiver.await.ok(),
        duration => match tokio::time::timeout(Duration::from_secs_f64(duration), receiver).await {
            Ok(result) => result.ok(),
            Err(_) => None,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key_value_store::Value;
    use std::collections::VecDeque;

    #[tokio::test]
    async fn test_remove_first_element_from_list_success() {
        let mut store = Arc::new(Mutex::new(KeyValueStore::new()));

        let mut list = VecDeque::new();
        list.push_back("first".to_string());
        list.push_back("second".to_string());
        list.push_back("third".to_string());

        let value = Value {
            data: DataType::Array(list),
            expiration: None,
        };

        {
            let mut store_guard = store.lock().await;
            store_guard.insert("mylist".to_string(), value);
        }

        let result = remove_first_element_from_list(&mut store, "mylist").await;
        assert_eq!(result, Some("first".to_string()));

        let store_guard = store.lock().await;
        if let Some(stored_value) = store_guard.get("mylist") {
            if let DataType::Array(ref remaining_list) = stored_value.data {
                assert_eq!(remaining_list.len(), 2);
                assert_eq!(remaining_list[0], "second");
                assert_eq!(remaining_list[1], "third");
            }
        }
    }

    #[tokio::test]
    async fn test_remove_first_element_from_empty_list() {
        let mut store = Arc::new(Mutex::new(KeyValueStore::new()));

        let empty_list = VecDeque::new();
        let value = Value {
            data: DataType::Array(empty_list),
            expiration: None,
        };

        {
            let mut store_guard = store.lock().await;
            store_guard.insert("emptylist".to_string(), value);
        }

        let result = remove_first_element_from_list(&mut store, "emptylist").await;
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_remove_first_element_from_nonexistent_key() {
        let mut store = Arc::new(Mutex::new(KeyValueStore::new()));

        let result = remove_first_element_from_list(&mut store, "nonexistent").await;
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_remove_first_element_from_non_list() {
        let mut store = Arc::new(Mutex::new(KeyValueStore::new()));

        let value = Value {
            data: DataType::String("not a list".to_string()),
            expiration: None,
        };

        {
            let mut store_guard = store.lock().await;
            store_guard.insert("stringkey".to_string(), value);
        }

        let result = remove_first_element_from_list(&mut store, "stringkey").await;
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_add_and_remove_subscriber() {
        let mut state = Arc::new(Mutex::new(State::new()));
        let (sender, _receiver) = oneshot::channel();

        add_subscriber(
            &mut state,
            "testkey".to_string(),
            "127.0.0.1:6379".to_string(),
            sender,
        )
        .await;

        {
            let state_guard = state.lock().await;
            assert_eq!(
                state_guard.blpop_subscribers.contains_key("testkey"),
                true,
                "Subscriber should be added to state"
            );
            assert_eq!(
                state_guard
                    .blpop_subscribers
                    .get("testkey")
                    .unwrap()
                    .is_empty(),
                false,
                "Subscriber queue should not be empty"
            );
        }

        remove_subscriber(&mut state, "testkey", "127.0.0.1:6379").await;

        {
            let state_guard = state.lock().await;
            let has_subscribers = state_guard
                .blpop_subscribers
                .get("testkey")
                .map_or(false, |queue| !queue.is_empty());
            assert_eq!(
                has_subscribers, false,
                "Subscriber should be removed from state"
            );
        }
    }

    #[tokio::test]
    async fn test_wait_for_data_immediate_timeout() {
        let (_sender, mut receiver) = oneshot::channel::<bool>();

        let result = wait_for_data(&mut receiver, 0.001).await;
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_wait_for_data_with_notification() {
        let (sender, mut receiver) = oneshot::channel::<bool>();

        let _ = sender.send(true);

        let result = wait_for_data(&mut receiver, 5.0).await;
        assert_eq!(result, Some(true));
    }

    #[tokio::test]
    async fn test_wait_for_data_infinite_blocking_with_notification() {
        let (sender, mut receiver) = oneshot::channel::<bool>();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let _ = sender.send(true);
        });

        let result = wait_for_data(&mut receiver, 0.0).await;
        assert_eq!(result, Some(true));
    }

    #[tokio::test]
    async fn test_wait_for_data_sender_dropped() {
        let (sender, mut receiver) = oneshot::channel::<bool>();

        // Drop the sender immediately
        drop(sender);

        // Test with any timeout (should return None due to dropped sender)
        let result = wait_for_data(&mut receiver, 1.0).await;
        assert_eq!(result, None);
    }
}
