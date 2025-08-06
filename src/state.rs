use std::collections::{HashMap, VecDeque};

use tokio::sync::{mpsc, oneshot};

use crate::commands::{CommandError, is_xread_stream_id_after, validate_stream_id};

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
#[derive(Debug)]
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
}

impl State {
    /// Creates a new empty State instance.
    ///
    /// Initializes empty subscription maps for both BLPOP and XREAD operations.
    pub fn new() -> Self {
        State {
            blpop_subscribers: HashMap::new(),
            xread_subscribers: HashMap::new(),
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
        if let Some(subscriber_vec) = self.blpop_subscribers.get_mut(&key) {
            subscriber_vec.push_back(subscriber);
        } else {
            self.blpop_subscribers
                .insert(key, VecDeque::from([subscriber]));
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
    /// client in the BLPOP queue that an element is now available.
    ///
    /// # Arguments
    ///
    /// * `key` - The list key that received a new element
    /// * `message` - The notification message (typically true)
    pub fn send_to_blpop_subscriber(&mut self, key: &str, message: bool) {
        if let Some(subscriber_vec) = self.blpop_subscribers.get_mut(key) {
            if let Some(subscriber) = subscriber_vec.pop_front() {
                let _ = subscriber.sender.send(message);

                if subscriber_vec.is_empty() {
                    self.blpop_subscribers.remove(key);
                }
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
        if let Some(streams) = self.xread_subscribers.get_mut(&key) {
            if let Some(subscriber_vec) = streams.get_mut(&stream_id) {
                subscriber_vec.push(subscriber);
            } else {
                streams.insert(stream_id, vec![subscriber]);
            }
        } else {
            self.xread_subscribers
                .insert(key, HashMap::from([(stream_id, vec![subscriber])]));
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
        if let Some(streams) = self.xread_subscribers.get_mut(key) {
            let parsed_stream_id = validate_stream_id(stream_id, true)
                .map_err(|e| CommandError::InvalidStreamId(e))?;

            let mut failed_validation_stream_ids = Vec::new();

            let read_stream_ids = streams
                .extract_if(|start_stream_id, _| {
                    // The stream ids in the state should already have been validated before being inserted so it is ok to just return false
                    let parsed_start_stream_id = match validate_stream_id(start_stream_id, true) {
                        Ok(stream_id) => stream_id,
                        Err(_) => {
                            failed_validation_stream_ids.push(start_stream_id.clone());
                            return false;
                        }
                    };

                    return is_xread_stream_id_after(&parsed_stream_id, &parsed_start_stream_id);
                })
                .collect::<HashMap<String, Vec<XreadSubscriber>>>();

            if failed_validation_stream_ids.len() > 0 {
                return Err(CommandError::InvalidStreamId(
                    "Invalid stream ids stored in state".to_string(),
                ));
            }

            for (_, subscriber_vec) in read_stream_ids {
                for subscriber in subscriber_vec {
                    let _ = subscriber.sender.try_send(message);
                }
            }
        }

        return Ok(());
    }
}
