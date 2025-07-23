use std::collections::HashMap;

pub struct Value {
    pub value: String,
    pub expiration: Option<std::time::Instant>,
}

pub type KeyValueStore = HashMap<String, Value>;
