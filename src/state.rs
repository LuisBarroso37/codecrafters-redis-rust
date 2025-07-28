use std::collections::{HashMap, VecDeque};

use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub struct Subscriber {
    pub key: String, // Key is the key of the key value store
    pub sender: mpsc::Sender<bool>,
}

pub struct State {
    pub subscribers: HashMap<String, VecDeque<Subscriber>>, // Key is command
}

impl State {
    pub fn new() -> Self {
        State {
            subscribers: HashMap::new(),
        }
    }
}
