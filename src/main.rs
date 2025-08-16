use crate::server::RedisServer;

mod commands;
mod connection;
mod input;
mod key_value_store;
mod resp;
mod server;
mod state;

/// Main entry point for the Redis server implementation.
///
/// Sets up a TCP server listening on port 6379 (standard Redis port) and handles
/// incoming client connections. Each connection is processed in a separate async task
/// to support concurrent clients.
///
/// The server maintains shared state including:
/// - A key-value store for data storage
/// - Server state for managing blocking operations and client subscriptions
#[tokio::main]
async fn main() {
    let server = match RedisServer::new(std::env::args()) {
        Ok(server) => server,
        Err(e) => {
            eprintln!("Failed to create Redis server: {}", e);
            return;
        }
    };

    server.run().await;
}
