use std::{collections::HashMap, sync::Arc};

use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
    sync::{Mutex, RwLock},
};

use crate::{
    commands::CommandDispatcher,
    input::{handshake, read_and_parse_command},
    key_value_store::KeyValueStore,
    server::{RedisRole, RedisServer},
    state::State,
};

mod commands;
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

    let listener = match TcpListener::bind(format!("127.0.0.1:{}", server.port)).await {
        Ok(listener) => listener,
        Err(e) => {
            eprintln!("Failed to bind TCP listener: {}", e);
            return;
        }
    };

    let server_role = server.role.clone();

    let store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));
    let server: Arc<RwLock<RedisServer>> = Arc::new(RwLock::new(server));

    match server_role {
        RedisRole::Replica((address, port)) => {
            let mut stream = match TcpStream::connect(format!("{}:{}", address, port)).await {
                Ok(stream) => stream,
                Err(e) => {
                    eprintln!("Failed to connect to replica: {}", e);
                    return;
                }
            };

            match handshake(&mut stream, &server).await {
                Ok(()) => {
                    tokio::spawn(async move { loop {} });
                }
                Err(e) => {
                    eprintln!("Failed to perform handshake: {}", e);
                    return;
                }
            }
        }
        _ => {}
    }

    // Accept connections and spawn tasks to handle each client
    loop {
        match listener.accept().await {
            Ok((mut stream, _addr)) => {
                let mut store = Arc::clone(&store);
                let mut state = Arc::clone(&state);
                let server = Arc::clone(&server);

                // Handle each client connection in a separate task
                tokio::spawn(async move {
                    loop {
                        let (command_handler, client_address) =
                            match read_and_parse_command(&mut stream).await {
                                Ok((cmd, addr)) => (cmd, addr),
                                Err(e) => {
                                    let _ = stream.write_all(e.as_string().as_bytes()).await;
                                    continue;
                                }
                            };

                        let dispatch_result =
                            match CommandDispatcher::new(client_address.clone(), state.clone())
                                .dispatch_command(command_handler)
                                .await
                            {
                                Ok(result) => result,
                                Err(e) => {
                                    let _ = stream.write_all(e.as_string().as_bytes()).await;
                                    continue;
                                }
                            };

                        let response = dispatch_result
                            .handle_dispatch_result(&server, client_address, &mut store, &mut state)
                            .await;
                        let _ = stream.write_all(response.as_bytes()).await;
                    }
                });
            }
            Err(e) => {
                eprintln!("error: {}", e);
            }
        }
    }
}
