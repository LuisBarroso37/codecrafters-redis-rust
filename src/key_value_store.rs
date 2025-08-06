use std::collections::{BTreeMap, HashMap, VecDeque};

use tokio::time::Instant;

/// Type alias for a Redis stream entry.
///
/// Each stream entry contains field-value pairs stored as a BTreeMap.
/// The BTreeMap ensures consistent ordering of fields.
pub type Stream = BTreeMap<String, String>;

/// Represents the different data types that can be stored in Redis.
///
/// Redis supports various data structures, each optimized for different use cases:
/// - String: Simple key-value storage
/// - Array: List/queue operations (LIFO/FIFO)
/// - Stream: Time-ordered log of entries with field-value pairs
#[derive(Debug, PartialEq)]
pub enum DataType {
    /// A simple string value
    String(String),
    /// A double-ended queue for list operations (LPUSH, RPUSH, LPOP, etc.)
    Array(VecDeque<String>),
    /// A stream containing time-ordered entries, where each entry has an ID and field-value pairs
    Stream(BTreeMap<String, Stream>),
}

/// Represents a value stored in the Redis key-value store.
///
/// Each value has a data type and an optional expiration time.
/// Values with expiration times are automatically considered invalid
/// after the specified time has passed.
#[derive(Debug, PartialEq)]
pub struct Value {
    /// The actual data stored
    pub data: DataType,
    /// Optional expiration time (None means no expiration)
    pub expiration: Option<Instant>,
}

/// Type alias for the main Redis key-value store.
///
/// Uses a HashMap for O(1) average case key lookups.
/// Maps string keys to Value structures containing data and metadata.
pub type KeyValueStore = HashMap<String, Value>;
