use std::collections::{HashMap, VecDeque};

use tokio::sync::mpsc;

#[derive(Debug)]
pub struct Subscriber {
    pub server_address: String,
    pub sender: mpsc::Sender<bool>,
}

#[derive(Debug)]
pub struct State {
    pub subscribers: HashMap<String, HashMap<String, VecDeque<Subscriber>>>, // HashMap<<command>>, HashMap<<arg>, VecDeque<Subscriber>>>
}

impl State {
    pub fn new() -> Self {
        State {
            subscribers: HashMap::new(),
        }
    }

    pub fn add_subscriber(&mut self, command: String, arg: String, subscriber: Subscriber) {
        if let Some(args) = self.subscribers.get_mut(&command) {
            if let Some(subscriber_vec) = args.get_mut(&arg) {
                subscriber_vec.push_back(subscriber);
            } else {
                args.insert(arg, VecDeque::from([subscriber]));
            }
        } else {
            self.subscribers.insert(
                command,
                HashMap::from([(arg, VecDeque::from([subscriber]))]),
            );
        }
    }

    pub fn remove_subscriber(&mut self, command: &str, arg: &str, server_address: &str) {
        if let Some(args) = self.subscribers.get_mut(command) {
            if let Some(subscriber_vec) = args.get_mut(arg) {
                subscriber_vec.retain(|subscriber| subscriber.server_address != server_address);
            }
        }
    }

    pub fn send_to_subscriber(&mut self, command: &str, arg: &str, message: bool) {
        if let Some(args) = self.subscribers.get_mut(command) {
            if let Some(subscriber_vec) = args.get_mut(arg) {
                if let Some(subscriber) = subscriber_vec.pop_front() {
                    let _ = subscriber.sender.try_send(message);
                }
            }
        }
    }
}
