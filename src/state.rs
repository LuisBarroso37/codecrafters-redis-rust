use std::collections::{HashMap, VecDeque};

use tokio::sync::oneshot;

#[derive(Debug)]
pub struct Subscriber {
    pub server_addr: String,
    pub sender: oneshot::Sender<bool>,
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

    pub fn remove_subscriber(&mut self, command: &str, arg: &str, server_addr: &str) {
        if let Some(args) = self.subscribers.get_mut(command) {
            if let Some(subscriber_vec) = args.get_mut(arg) {
                subscriber_vec.retain(|subscriber| subscriber.server_addr != server_addr);
            }
        }
    }

    pub fn send_to_subscriber(&mut self, command: &str, arg: &str, message: bool) {
        if let Some(args) = self.subscribers.get_mut(command) {
            if let Some(subscriber_vec) = args.get_mut(arg) {
                if let Some(subscriber) = subscriber_vec.pop_front() {
                    let _ = subscriber.sender.send(message);
                }
            }
        }
    }
}
