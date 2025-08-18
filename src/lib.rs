//! A Redis server implementation in Rust.
//!
//! This crate provides a Redis-compatible server implementation that supports
//! core Redis functionality including:
//!
//! - Basic key-value operations (GET, SET, INCR)
//! - List operations (LPUSH, RPUSH, LPOP, BLPOP, LRANGE, LLEN)  
//! - Stream operations (XADD, XRANGE, XREAD)
//! - Server commands (PING, ECHO, INFO, TYPE)
//! - Master-replica replication
//! - Blocking operations with client notifications
//!
//! The server uses the Redis Serialization Protocol (RESP) for client communication
//! and supports concurrent connections through async/await with Tokio.

pub mod commands;
pub mod connection;
pub mod input;
pub mod key_value_store;
pub mod resp;
pub mod server;
pub mod state;
