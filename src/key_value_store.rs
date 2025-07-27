use std::collections::HashMap;

use tokio::time::Instant;

#[derive(Debug, PartialEq)]
pub enum DataType {
    String(String),
    Array(Vec<String>),
}

#[derive(Debug, PartialEq)]
pub struct Value {
    pub data: DataType,
    pub expiration: Option<Instant>,
}

pub type KeyValueStore = HashMap<String, Value>;
