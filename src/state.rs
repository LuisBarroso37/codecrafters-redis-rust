use std::collections::{HashMap, VecDeque};

use tokio::sync::{mpsc, oneshot};

use crate::commands::{CommandError, is_xread_stream_id_after, validate_stream_id};

#[derive(Debug)]
pub struct BlpopSubscriber {
    pub server_address: String,
    pub sender: oneshot::Sender<bool>,
}

#[derive(Debug)]
pub struct XreadSubscriber {
    pub server_address: String,
    pub sender: mpsc::Sender<bool>,
}

#[derive(Debug)]
pub struct State {
    pub blpop_subscribers: HashMap<String, VecDeque<BlpopSubscriber>>, // key --> subscriber
    pub xread_subscribers: HashMap<String, HashMap<String, Vec<XreadSubscriber>>>, // key --> stream id --> subscriber
}

impl State {
    pub fn new() -> Self {
        State {
            blpop_subscribers: HashMap::new(),
            xread_subscribers: HashMap::new(),
        }
    }

    pub fn add_blpop_subscriber(&mut self, key: String, subscriber: BlpopSubscriber) {
        if let Some(subscriber_vec) = self.blpop_subscribers.get_mut(&key) {
            subscriber_vec.push_back(subscriber);
        } else {
            self.blpop_subscribers
                .insert(key, VecDeque::from([subscriber]));
        }
    }

    pub fn remove_blpop_subscriber(&mut self, key: &str, server_address: &str) {
        if let Some(subscriber_vec) = self.blpop_subscribers.get_mut(key) {
            subscriber_vec.retain(|subscriber| subscriber.server_address != server_address);
        }
    }

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

    pub fn remove_xread_subscriber(&mut self, key: &str, stream_id: &str, server_address: &str) {
        if let Some(streams) = self.xread_subscribers.get_mut(key) {
            if let Some(subscriber_vec) = streams.get_mut(stream_id) {
                subscriber_vec.retain(|subscriber| subscriber.server_address != server_address);
            }
        }
    }

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
