use crate::server::RedisServer;

mod commands;
mod connection;
mod input;
mod key_value_store;
mod rdb;
mod resp;
mod server;
mod state;
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
