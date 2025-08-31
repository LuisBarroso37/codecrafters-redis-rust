use std::{sync::Arc, time::Duration};

use tokio::sync::{Mutex, oneshot};

use crate::{
    commands::{command_error::CommandError, command_handler::CommandResult},
    key_value_store::{DataType, KeyValueStore},
    resp::RespValue,
    state::{BlpopSubscriber, State},
};

pub struct BlpopArguments {
    key: String,
    block_duration_secs: f64,
}

impl BlpopArguments {
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

pub async fn blpop(
    client_address: &str,
    store: Arc<Mutex<KeyValueStore>>,
    state: Arc<Mutex<State>>,
    arguments: Vec<String>,
) -> Result<CommandResult, CommandError> {
    let blpop_arguments = BlpopArguments::parse(arguments)?;

    if let Some(value) =
        remove_first_element_from_list(Arc::clone(&store), &blpop_arguments.key).await
    {
        return Ok(CommandResult::Response(
            RespValue::encode_array_from_strings(vec![blpop_arguments.key, value]),
        ));
    }

    let (sender, mut receiver) = oneshot::channel();
    add_subscriber(
        Arc::clone(&state),
        blpop_arguments.key.clone(),
        client_address.to_string(),
        sender,
    )
    .await;

    let data = wait_for_data(&mut receiver, blpop_arguments.block_duration_secs).await;
    remove_subscriber(state, &blpop_arguments.key, &client_address).await;

    if data.is_none() {
        return Ok(CommandResult::Response(RespValue::NullArray.encode()));
    }

    if let Some(value) = remove_first_element_from_list(store, &blpop_arguments.key).await {
        Ok(CommandResult::Response(
            RespValue::encode_array_from_strings(vec![blpop_arguments.key, value]),
        ))
    } else {
        Ok(CommandResult::Response(RespValue::NullArray.encode()))
    }
}

async fn remove_first_element_from_list(
    store: Arc<Mutex<KeyValueStore>>,
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

async fn add_subscriber(
    state: Arc<Mutex<State>>,
    key: String,
    client_address: String,
    sender: oneshot::Sender<bool>,
) {
    let subscriber = BlpopSubscriber {
        client_address,
        sender,
    };

    let mut state_guard = state.lock().await;
    state_guard.add_blpop_subscriber(key, subscriber);
}

async fn remove_subscriber(state: Arc<Mutex<State>>, key: &str, client_address: &str) {
    let mut state_guard = state.lock().await;
    state_guard.remove_blpop_subscriber(key, &client_address);
}

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
        let store = Arc::new(Mutex::new(KeyValueStore::new()));

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

        let result = remove_first_element_from_list(Arc::clone(&store), "mylist").await;
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
        let store = Arc::new(Mutex::new(KeyValueStore::new()));

        let empty_list = VecDeque::new();
        let value = Value {
            data: DataType::Array(empty_list),
            expiration: None,
        };

        {
            let mut store_guard = store.lock().await;
            store_guard.insert("emptylist".to_string(), value);
        }

        let result = remove_first_element_from_list(store, "emptylist").await;
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_remove_first_element_from_nonexistent_key() {
        let store = Arc::new(Mutex::new(KeyValueStore::new()));

        let result = remove_first_element_from_list(store, "nonexistent").await;
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_remove_first_element_from_non_list() {
        let store = Arc::new(Mutex::new(KeyValueStore::new()));

        let value = Value {
            data: DataType::String("not a list".to_string()),
            expiration: None,
        };

        {
            let mut store_guard = store.lock().await;
            store_guard.insert("stringkey".to_string(), value);
        }

        let result = remove_first_element_from_list(store, "stringkey").await;
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_add_and_remove_subscriber() {
        let state = Arc::new(Mutex::new(State::new()));
        let (sender, _receiver) = oneshot::channel();

        add_subscriber(
            Arc::clone(&state),
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

        remove_subscriber(Arc::clone(&state), "testkey", "127.0.0.1:6379").await;

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
