use std::{collections::HashMap, sync::Arc};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::Mutex,
};

use crate::{
    commands::{CommandDispatcher, CommandHandler},
    input::parse_input,
    key_value_store::KeyValueStore,
    resp::RespValue,
    state::State,
};

mod commands;
mod input;
mod key_value_store;
mod resp;
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
    // Bind to the standard Redis port
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    // Initialize shared state protected by mutexes for thread-safe access
    let store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));

    // Accept connections and spawn tasks to handle each client
    loop {
        match listener.accept().await {
            Ok((mut stream, _addr)) => {
                let mut store = Arc::clone(&store);
                let mut state = Arc::clone(&state);

                // Handle each client connection in a separate task
                tokio::spawn(async move {
                    loop {
                        let mut buf = [0; 1024];
                        let number_of_bytes = match stream.read(&mut buf).await {
                            Ok(n) => n,
                            Err(_) => break,
                        };

                        if number_of_bytes == 0 {
                            break; // Connection closed
                        }

                        let input = match parse_input(&buf) {
                            Ok(input) => input,
                            Err(e) => {
                                let _ = stream.write_all(e.as_bytes()).await;
                                continue;
                            }
                        };

                        let parsed_input = match RespValue::parse(input) {
                            Ok(input) => input,
                            Err(e) => {
                                let _ = stream.write_all(e.as_bytes()).await;
                                continue;
                            }
                        };

                        let server_address = match stream.peer_addr() {
                            Ok(address) => address.to_string(),
                            Err(_) => {
                                let _ = stream.write_all(b"ERR failed to get server address").await;
                                continue;
                            }
                        };

                        let command_handler = match CommandHandler::new(parsed_input) {
                            Ok(cmd) => cmd,
                            Err(e) => {
                                let _ = stream.write_all(e.as_string().as_bytes()).await;
                                continue;
                            }
                        };

                        let dispatch_result =
                            match CommandDispatcher::new(server_address.clone(), state.clone())
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
                            .handle_dispatch_result(server_address, &mut store, &mut state)
                            .await;
                        let _ = stream.write_all(response.as_bytes()).await;
                    }
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
