use std::collections::{HashMap, VecDeque};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

use crate::commands::{CommandError, is_xread_stream_id_after, validate_stream_id};

#[derive(Error, Debug, PartialEq)]
pub enum StateError {
    #[error("Transaction already started")]
    TransactionAlreadyStarted,
    #[error("No transaction in progress")]
    NoTransactionInProgress,
}

impl StateError {
    pub fn as_string(&self) -> String {
        match self {
            StateError::TransactionAlreadyStarted => "Transaction already started".to_string(),
            StateError::NoTransactionInProgress => "No transaction in progress".to_string(),
        }
    }
}

/// Represents a client waiting for a BLPOP operation to complete.
///
/// BLPOP is a blocking operation that waits for an element to be pushed
/// to a list. This struct holds the information needed to notify the
/// client when an element becomes available.
#[derive(Debug)]
pub struct BlpopSubscriber {
    /// The server address of the waiting client
    pub server_address: String,
    /// Channel to send notification when an element is available
    pub sender: oneshot::Sender<bool>,
}

/// Represents a client waiting for an XREAD operation to complete.
///
/// XREAD can block waiting for new entries to be added to streams.
/// This struct holds the information needed to notify the client
/// when new stream entries become available.
#[derive(Debug, Clone)]
pub struct XreadSubscriber {
    /// The server address of the waiting client
    pub server_address: String,
    /// Channel to send notification when new stream entries are available
    pub sender: mpsc::Sender<bool>,
}

/// Manages server state for blocking operations and client subscriptions.
///
/// This structure tracks clients that are waiting for blocking operations
/// like BLPOP and XREAD to complete. When data becomes available, it notifies
/// the appropriate waiting clients.
#[derive(Debug)]
pub struct State {
    /// Maps list keys to queues of clients waiting for BLPOP operations
    pub blpop_subscribers: HashMap<String, VecDeque<BlpopSubscriber>>, // key --> subscriber
    /// Maps stream keys to stream IDs to clients waiting for XREAD operations
    pub xread_subscribers: HashMap<String, HashMap<String, Vec<XreadSubscriber>>>, // key --> stream id --> subscriber
    pub transactions: HashMap<String, Vec<Vec<String>>>,
}

impl State {
    /// Creates a new empty State instance.
    ///
    /// Initializes empty subscription maps for both BLPOP and XREAD operations.
    pub fn new() -> Self {
        State {
            blpop_subscribers: HashMap::new(),
            xread_subscribers: HashMap::new(),
            transactions: HashMap::new(),
        }
    }

    /// Adds a client to the BLPOP subscriber queue for a specific key.
    ///
    /// Clients are queued in FIFO order, so the first client to request
    /// a BLPOP will be the first to receive an element when it becomes available.
    ///
    /// # Arguments
    ///
    /// * `key` - The list key the client is waiting on
    /// * `subscriber` - The client information and notification channel
    pub fn add_blpop_subscriber(&mut self, key: String, subscriber: BlpopSubscriber) {
        match self.blpop_subscribers.get_mut(&key) {
            Some(subscriber_vec) => {
                subscriber_vec.push_back(subscriber);
            }
            None => {
                self.blpop_subscribers
                    .insert(key, VecDeque::from([subscriber]));
            }
        }
    }

    /// Removes a BLPOP subscriber for a specific key and server address.
    ///
    /// This is called when a BLPOP operation times out or is cancelled,
    /// ensuring that the client won't receive future notifications.
    ///
    /// # Arguments
    ///
    /// * `key` - The list key to remove the subscriber from
    /// * `server_address` - The server address of the client to remove
    pub fn remove_blpop_subscriber(&mut self, key: &str, server_address: &str) {
        if let Some(subscriber_vec) = self.blpop_subscribers.get_mut(key) {
            subscriber_vec.retain(|subscriber| subscriber.server_address != server_address);
        }
    }

    /// Sends a notification to the first BLPOP subscriber waiting on a key.
    ///
    /// When an element is pushed to a list, this method notifies the first
    /// client in the BLPOP queue that an element is now available. If this
    /// was the last subscriber for the key, the key is removed from the map.
    ///
    /// # Arguments
    ///
    /// * `key` - The list key that received a new element
    /// * `message` - The notification message (typically true)
    pub fn send_to_blpop_subscriber(&mut self, key: &str, message: bool) {
        if let Some(subscriber_vec) = self.blpop_subscribers.get_mut(key) {
            if let Some(subscriber) = subscriber_vec.pop_front() {
                let _ = subscriber.sender.send(message);
            }

            if subscriber_vec.is_empty() {
                self.blpop_subscribers.remove(key);
            }
        }
    }

    /// Adds a client to the XREAD subscriber list for a specific stream key and stream ID.
    ///
    /// Clients waiting for XREAD operations are organized by stream key and then
    /// by the stream ID they're waiting to read from.
    ///
    /// # Arguments
    ///
    /// * `key` - The stream key the client is waiting on
    /// * `stream_id` - The stream ID the client is waiting to read from
    /// * `subscriber` - The client information and notification channel
    pub fn add_xread_subscriber(
        &mut self,
        key: String,
        stream_id: String,
        subscriber: XreadSubscriber,
    ) {
        let Some(streams) = self.xread_subscribers.get_mut(&key) else {
            self.xread_subscribers
                .insert(key, HashMap::from([(stream_id, vec![subscriber])]));
            return;
        };

        if let Some(subscriber_vec) = streams.get_mut(&stream_id) {
            subscriber_vec.push(subscriber);
        } else {
            streams.insert(stream_id, vec![subscriber]);
        }
    }

    /// Removes an XREAD subscriber for a specific stream key, stream ID, and server address.
    ///
    /// This is called when an XREAD operation times out or is cancelled,
    /// ensuring that the client won't receive future notifications.
    ///
    /// # Arguments
    ///
    /// * `key` - The stream key to remove the subscriber from
    /// * `stream_id` - The stream ID to remove the subscriber from
    /// * `server_address` - The server address of the client to remove
    pub fn remove_xread_subscriber(&mut self, key: &str, stream_id: &str, server_address: &str) {
        if let Some(streams) = self.xread_subscribers.get_mut(key) {
            if let Some(subscriber_vec) = streams.get_mut(stream_id) {
                subscriber_vec.retain(|subscriber| subscriber.server_address != server_address);
            }
        }
    }

    /// Sends notifications to XREAD subscribers waiting for new stream entries.
    ///
    /// When a new entry is added to a stream, this method notifies all clients
    /// that are waiting for entries after stream IDs that come before the new entry.
    ///
    /// # Arguments
    ///
    /// * `key` - The stream key that received a new entry
    /// * `stream_id` - The ID of the new stream entry
    /// * `message` - The notification message (typically true)
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Notifications sent successfully
    /// * `Err(CommandError::InvalidStreamId)` - If the stream ID is invalid
    pub fn send_to_xread_subscribers(
        &mut self,
        key: &str,
        stream_id: &str,
        message: bool,
    ) -> Result<(), CommandError> {
        let new_stream_id =
            validate_stream_id(stream_id, true).map_err(|e| CommandError::InvalidStreamId(e))?;

        let Some(streams) = self.xread_subscribers.get_mut(key) else {
            return Ok(());
        };

        let mut subscribers_to_notify = Vec::new();
        let mut stream_ids_to_remove = Vec::new();

        for (waiting_stream_id, subscriber_vec) in streams.iter() {
            let waiting_id = match validate_stream_id(waiting_stream_id, true) {
                Ok(id) => id,
                Err(_) => {
                    // Log this as a warning in a real system
                    continue; // Skip invalid IDs rather than failing the entire operation
                }
            };

            if is_xread_stream_id_after(&new_stream_id, &waiting_id) {
                subscribers_to_notify.extend(subscriber_vec.iter().cloned());
                stream_ids_to_remove.push(waiting_stream_id.clone());
            }
        }

        for stream_id in stream_ids_to_remove {
            streams.remove(&stream_id);
        }

        for subscriber in subscribers_to_notify {
            let _ = subscriber.sender.try_send(message);
        }

        if streams.is_empty() {
            self.xread_subscribers.remove(key);
        }

        return Ok(());
    }

    pub fn start_transaction(&mut self, server_address: String) -> Result<(), StateError> {
        match self.transactions.get_mut(&server_address) {
            Some(_) => Err(StateError::TransactionAlreadyStarted),
            None => {
                self.transactions.insert(server_address, Vec::new());
                Ok(())
            }
        }
    }

    pub fn get_transaction(&mut self, server_address: &str) -> Option<&Vec<Vec<String>>> {
        self.transactions.get(server_address)
    }

    pub fn add_to_transaction(
        &mut self,
        server_address: String,
        command: Vec<String>,
    ) -> Result<(), StateError> {
        match self.transactions.get_mut(&server_address) {
            Some(transactions) => {
                transactions.push(command);
                Ok(())
            }
            None => Err(StateError::NoTransactionInProgress),
        }
    }

    pub fn remove_transaction(
        &mut self,
        server_address: String,
    ) -> Result<Vec<Vec<String>>, StateError> {
        match self.transactions.remove(&server_address) {
            Some(transaction) => Ok(transaction),
            None => Err(StateError::NoTransactionInProgress),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{BlpopSubscriber, CommandError, State, XreadSubscriber};
    use tokio::sync::{mpsc, oneshot};

    #[test]
    fn test_state_new() {
        let state = State::new();
        assert_eq!(state.blpop_subscribers.is_empty(), true);
        assert_eq!(state.xread_subscribers.is_empty(), true);
    }

    #[test]
    fn test_add_blpop_subscriber_new_key() {
        let mut state = State::new();
        let (sender, _receiver) = oneshot::channel();
        let subscriber = BlpopSubscriber {
            server_address: "127.0.0.1:8080".to_string(),
            sender,
        };

        state.add_blpop_subscriber("mylist".to_string(), subscriber);

        assert_eq!(state.blpop_subscribers.len(), 1);
        assert_eq!(state.blpop_subscribers.contains_key("mylist"), true);
        assert_eq!(state.blpop_subscribers["mylist"].len(), 1);
    }

    #[test]
    fn test_add_blpop_subscriber_existing_key() {
        let mut state = State::new();

        let (sender1, _receiver1) = oneshot::channel();
        let subscriber1 = BlpopSubscriber {
            server_address: "127.0.0.1:8080".to_string(),
            sender: sender1,
        };
        state.add_blpop_subscriber("mylist".to_string(), subscriber1);

        let (sender2, _receiver2) = oneshot::channel();
        let subscriber2 = BlpopSubscriber {
            server_address: "127.0.0.1:8081".to_string(),
            sender: sender2,
        };
        state.add_blpop_subscriber("mylist".to_string(), subscriber2);

        assert_eq!(state.blpop_subscribers.len(), 1);
        assert_eq!(state.blpop_subscribers["mylist"].len(), 2);
    }

    #[test]
    fn test_remove_blpop_subscriber() {
        let mut state = State::new();

        let (sender1, _receiver1) = oneshot::channel();
        let subscriber1 = BlpopSubscriber {
            server_address: "127.0.0.1:8080".to_string(),
            sender: sender1,
        };
        state.add_blpop_subscriber("mylist".to_string(), subscriber1);

        let (sender2, _receiver2) = oneshot::channel();
        let subscriber2 = BlpopSubscriber {
            server_address: "127.0.0.1:8081".to_string(),
            sender: sender2,
        };
        state.add_blpop_subscriber("mylist".to_string(), subscriber2);

        state.remove_blpop_subscriber("mylist", "127.0.0.1:8080");

        assert_eq!(state.blpop_subscribers["mylist"].len(), 1);
        assert_eq!(
            state.blpop_subscribers["mylist"][0].server_address,
            "127.0.0.1:8081"
        );
    }

    #[test]
    fn test_remove_blpop_subscriber_nonexistent_key() {
        let mut state = State::new();

        state.remove_blpop_subscriber("nonexistent", "127.0.0.1:8080");

        assert_eq!(state.blpop_subscribers.is_empty(), true);
    }

    #[tokio::test]
    async fn test_send_to_blpop_subscriber_success() {
        let mut state = State::new();
        let (sender, receiver) = oneshot::channel();
        let subscriber = BlpopSubscriber {
            server_address: "127.0.0.1:8080".to_string(),
            sender,
        };

        state.add_blpop_subscriber("mylist".to_string(), subscriber);
        state.send_to_blpop_subscriber("mylist", true);

        let result = receiver.await;
        assert_eq!(result.is_ok(), true);
        assert_eq!(result.unwrap(), true);

        assert_eq!(state.blpop_subscribers.contains_key("mylist"), false);
    }

    #[test]
    fn test_send_to_blpop_subscriber_nonexistent_key() {
        let mut state = State::new();

        state.send_to_blpop_subscriber("nonexistent", true);

        assert_eq!(state.blpop_subscribers.is_empty(), true);
    }

    #[tokio::test]
    async fn test_send_to_blpop_subscriber_fifo_order() {
        let mut state = State::new();

        // Add two subscribers
        let (sender1, receiver1) = oneshot::channel();
        let subscriber1 = BlpopSubscriber {
            server_address: "127.0.0.1:8080".to_string(),
            sender: sender1,
        };
        state.add_blpop_subscriber("mylist".to_string(), subscriber1);

        let (sender2, receiver2) = oneshot::channel();
        let subscriber2 = BlpopSubscriber {
            server_address: "127.0.0.1:8081".to_string(),
            sender: sender2,
        };
        state.add_blpop_subscriber("mylist".to_string(), subscriber2);

        // Send notification - first subscriber should receive it
        state.send_to_blpop_subscriber("mylist", true);

        // First subscriber should receive the message
        let result1 = receiver1.await;
        assert_eq!(result1.is_ok(), true);
        assert_eq!(result1.unwrap(), true);

        // Second subscriber should still be waiting
        assert_eq!(state.blpop_subscribers["mylist"].len(), 1);
        assert_eq!(
            state.blpop_subscribers["mylist"][0].server_address,
            "127.0.0.1:8081"
        );

        // Verify second receiver hasn't received anything yet
        tokio::select! {
            _ = receiver2 => panic!("Second receiver should not have received a message"),
            _ = tokio::time::sleep(tokio::time::Duration::from_millis(10)) => {
                // This is expected - receiver2 should still be waiting
            }
        }
    }

    #[test]
    fn test_add_xread_subscriber_new_key() {
        let mut state = State::new();
        let (sender, _receiver) = mpsc::channel(1);
        let subscriber = XreadSubscriber {
            server_address: "127.0.0.1:8080".to_string(),
            sender,
        };

        state.add_xread_subscriber("mystream".to_string(), "1234-0".to_string(), subscriber);

        assert_eq!(state.xread_subscribers.len(), 1);
        assert_eq!(state.xread_subscribers.contains_key("mystream"), true);
        assert_eq!(
            state.xread_subscribers["mystream"].contains_key("1234-0"),
            true
        );
        assert_eq!(state.xread_subscribers["mystream"]["1234-0"].len(), 1);
    }

    #[test]
    fn test_add_xread_subscriber_existing_key_new_stream_id() {
        let mut state = State::new();
        let (sender1, _receiver1) = mpsc::channel(1);
        let subscriber1 = XreadSubscriber {
            server_address: "127.0.0.1:8080".to_string(),
            sender: sender1,
        };
        state.add_xread_subscriber("mystream".to_string(), "1234-0".to_string(), subscriber1);

        let (sender2, _receiver2) = mpsc::channel(1);
        let subscriber2 = XreadSubscriber {
            server_address: "127.0.0.1:8081".to_string(),
            sender: sender2,
        };
        state.add_xread_subscriber("mystream".to_string(), "1235-0".to_string(), subscriber2);

        assert_eq!(state.xread_subscribers.len(), 1);
        assert_eq!(state.xread_subscribers["mystream"].len(), 2);
        assert_eq!(
            state.xread_subscribers["mystream"].contains_key("1234-0"),
            true
        );
        assert_eq!(
            state.xread_subscribers["mystream"].contains_key("1235-0"),
            true
        );
    }

    #[test]
    fn test_add_xread_subscriber_existing_key_existing_stream_id() {
        let mut state = State::new();
        let (sender1, _receiver1) = mpsc::channel(1);
        let subscriber1 = XreadSubscriber {
            server_address: "127.0.0.1:8080".to_string(),
            sender: sender1,
        };
        state.add_xread_subscriber("mystream".to_string(), "1234-0".to_string(), subscriber1);

        let (sender2, _receiver2) = mpsc::channel(1);
        let subscriber2 = XreadSubscriber {
            server_address: "127.0.0.1:8081".to_string(),
            sender: sender2,
        };
        state.add_xread_subscriber("mystream".to_string(), "1234-0".to_string(), subscriber2);

        assert_eq!(state.xread_subscribers.len(), 1);
        assert_eq!(state.xread_subscribers["mystream"].len(), 1);
        assert_eq!(state.xread_subscribers["mystream"]["1234-0"].len(), 2);
    }

    #[test]
    fn test_remove_xread_subscriber() {
        let mut state = State::new();
        let (sender1, _receiver1) = mpsc::channel(1);
        let subscriber1 = XreadSubscriber {
            server_address: "127.0.0.1:8080".to_string(),
            sender: sender1,
        };
        state.add_xread_subscriber("mystream".to_string(), "1234-0".to_string(), subscriber1);

        let (sender2, _receiver2) = mpsc::channel(1);
        let subscriber2 = XreadSubscriber {
            server_address: "127.0.0.1:8081".to_string(),
            sender: sender2,
        };
        state.add_xread_subscriber("mystream".to_string(), "1234-0".to_string(), subscriber2);

        state.remove_xread_subscriber("mystream", "1234-0", "127.0.0.1:8080");

        assert_eq!(state.xread_subscribers["mystream"]["1234-0"].len(), 1);
        assert_eq!(
            state.xread_subscribers["mystream"]["1234-0"][0].server_address,
            "127.0.0.1:8081"
        );
    }

    #[test]
    fn test_remove_xread_subscriber_nonexistent() {
        let mut state = State::new();

        state.remove_xread_subscriber("nonexistent", "1234-0", "127.0.0.1:8080");

        assert_eq!(state.xread_subscribers.is_empty(), true);
    }

    #[tokio::test]
    async fn test_send_to_xread_subscribers_invalid_stream_id() {
        let mut state = State::new();

        let result = state.send_to_xread_subscribers("mystream", "invalid-id", true);

        assert_eq!(result.is_err(), true);
        assert!(matches!(
            result.unwrap_err(),
            CommandError::InvalidStreamId(_)
        ));
    }

    #[tokio::test]
    async fn test_send_to_xread_subscribers_no_subscribers() {
        let mut state = State::new();

        let result = state.send_to_xread_subscribers("nonexistent", "1234-0", true);

        assert_eq!(result.is_ok(), true);
    }

    #[tokio::test]
    async fn test_send_to_xread_subscribers_success() {
        let mut state = State::new();
        let (sender, mut receiver) = mpsc::channel(1);
        let subscriber = XreadSubscriber {
            server_address: "127.0.0.1:8080".to_string(),
            sender,
        };

        // Add subscriber waiting for entries after "1233-0"
        state.add_xread_subscriber("mystream".to_string(), "1233-0".to_string(), subscriber);

        // Send notification for new entry "1234-0" (which is after "1233-0")
        let result = state.send_to_xread_subscribers("mystream", "1234-0", true);

        assert_eq!(result.is_ok(), true);

        // Subscriber should receive notification
        let message = receiver.recv().await;
        assert_eq!(message.is_some(), true);
        assert_eq!(message.unwrap(), true);

        // Subscriber should be removed after notification
        assert_eq!(state.xread_subscribers.contains_key("mystream"), false);
    }

    #[tokio::test]
    async fn test_send_to_xread_subscribers_no_notification_for_earlier_id() {
        let mut state = State::new();
        let (sender, mut receiver) = mpsc::channel(1);
        let subscriber = XreadSubscriber {
            server_address: "127.0.0.1:8080".to_string(),
            sender,
        };

        // Add subscriber waiting for entries after "1235-0"
        state.add_xread_subscriber("mystream".to_string(), "1235-0".to_string(), subscriber);

        // Send notification for new entry "1234-0" (which is before "1235-0")
        let result = state.send_to_xread_subscribers("mystream", "1234-0", true);

        assert_eq!(result.is_ok(), true);

        // Subscriber should NOT receive notification
        tokio::select! {
            _ = receiver.recv() => panic!("Subscriber should not have received a message"),
            _ = tokio::time::sleep(tokio::time::Duration::from_millis(10)) => {
                // This is expected - no notification should be sent
            }
        }

        // Subscriber should still be waiting
        assert_eq!(state.xread_subscribers.contains_key("mystream"), true);
        assert_eq!(state.xread_subscribers["mystream"]["1235-0"].len(), 1);
    }

    #[tokio::test]
    async fn test_send_to_xread_subscribers_multiple_subscribers() {
        let mut state = State::new();

        let (sender1, mut receiver1) = mpsc::channel(1);
        let subscriber1 = XreadSubscriber {
            server_address: "127.0.0.1:8080".to_string(),
            sender: sender1,
        };
        state.add_xread_subscriber("mystream".to_string(), "1233-0".to_string(), subscriber1);

        let (sender2, mut receiver2) = mpsc::channel(1);
        let subscriber2 = XreadSubscriber {
            server_address: "127.0.0.1:8081".to_string(),
            sender: sender2,
        };
        state.add_xread_subscriber("mystream".to_string(), "1233-5".to_string(), subscriber2);

        // Send notification for "1234-0" - both should be notified
        let result = state.send_to_xread_subscribers("mystream", "1234-0", true);

        assert_eq!(result.is_ok(), true);

        // Both subscribers should receive notifications
        let message1 = receiver1.recv().await;
        assert_eq!(message1.is_some(), true);
        assert_eq!(message1.unwrap(), true);

        let message2 = receiver2.recv().await;
        assert_eq!(message2.is_some(), true);
        assert_eq!(message2.unwrap(), true);

        // All subscribers should be removed
        assert_eq!(state.xread_subscribers.contains_key("mystream"), false);
    }
}
