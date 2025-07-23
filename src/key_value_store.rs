use std::collections::HashMap;

use tokio::time::Instant;

pub struct Value {
    pub value: String,
    pub expiration: Option<Instant>,
}

pub type KeyValueStore = HashMap<String, Value>;
