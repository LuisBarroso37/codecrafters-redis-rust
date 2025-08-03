use std::{collections::HashMap, sync::Arc};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::Mutex,
};

use crate::{
    commands::CommandProcessor, input::parse_input, key_value_store::KeyValueStore,
    resp::RespValue, state::State,
};

mod commands;
mod input;
mod key_value_store;
mod resp;
mod state;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    let store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));

    loop {
        match listener.accept().await {
            Ok((mut stream, _addr)) => {
                let mut store = Arc::clone(&store);
                let mut state = Arc::clone(&state);

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

                        let parsed_command = match RespValue::parse(input) {
                            Ok(cmd) => cmd,
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

                        let command_processor = match CommandProcessor::new(parsed_command) {
                            Ok(cmd) => cmd,
                            Err(e) => {
                                let _ = stream.write_all(e.as_string().as_bytes()).await;
                                continue;
                            }
                        };

                        match command_processor
                            .handle_command(server_address, &mut store, &mut state)
                            .await
                        {
                            Ok(resp) => {
                                let _ = stream.write_all(resp.as_bytes()).await;
                            }
                            Err(e) => {
                                let _ = stream.write_all(e.as_string().as_bytes()).await;
                            }
                        }
                    }
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
