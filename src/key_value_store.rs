use std::collections::{BTreeMap, HashMap, VecDeque};

use jiff::Timestamp;

pub type Stream = BTreeMap<String, String>;

#[derive(Debug, PartialEq)]
pub enum DataType {
    String(String),
    Array(VecDeque<String>),
    Stream(BTreeMap<String, Stream>),
}

#[derive(Debug, PartialEq)]
pub struct Value {
    pub data: DataType,
    pub expiration: Option<Timestamp>,
}

pub type KeyValueStore = HashMap<String, Value>;
