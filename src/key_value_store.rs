use std::collections::{HashMap, VecDeque};

use tokio::time::Instant;

pub type Stream = HashMap<String, String>;

#[derive(Debug, PartialEq)]
pub enum DataType {
    String(String),
    Array(VecDeque<String>),
    Stream(HashMap<String, Stream>),
}

#[derive(Debug, PartialEq)]
pub struct Value {
    pub data: DataType,
    pub expiration: Option<Instant>,
}

pub type KeyValueStore = HashMap<String, Value>;
