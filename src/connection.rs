use std::sync::Arc;

use tokio::io::AsyncWriteExt;
use tokio::{
    net::TcpStream,
    sync::{Mutex, RwLock},
};

use crate::{
    commands::{CommandDispatcher, CommandHandler, handle_extra_action},
    input::{CommandReadError, read_and_parse_resp},
    key_value_store::KeyValueStore,
    server::RedisServer,
    state::State,
};

pub async fn handle_client_connection(
    stream: Arc<Mutex<TcpStream>>,
    server: Arc<RwLock<RedisServer>>,
    client_address: String,
    store: Arc<Mutex<KeyValueStore>>,
    state: Arc<Mutex<State>>,
) {
    let mut buffer = [0; 1024];

    loop {
        let parsed_input = match read_and_parse_resp(Arc::clone(&stream), &mut buffer).await {
            Ok(cmd) => cmd,
            Err(e) => match e {
                CommandReadError::ConnectionClosed => {
                    let mut server_guard = server.write().await;

                    if let Some(replicas) = &mut server_guard.replicas {
                        replicas.remove(&client_address);
                    }

                    break;
                }
                _ => {
                    let mut stream_guard = stream.lock().await;
                    let _ = stream_guard.write_all(e.as_string().as_bytes()).await;
                    continue;
                }
            },
        };
        let command_handler = match CommandHandler::new(&parsed_input) {
            Ok(handler) => handler,
            Err(e) => {
                let mut stream_guard = stream.lock().await;
                let _ = stream_guard.write_all(e.as_string().as_bytes()).await;
                continue;
            }
        };

        let dispatch_result = match CommandDispatcher::new(&client_address, Arc::clone(&state))
            .dispatch_command(command_handler)
            .await
        {
            Ok(result) => result,
            Err(e) => {
                let mut stream_guard = stream.lock().await;
                let _ = stream_guard.write_all(e.as_string().as_bytes()).await;
                continue;
            }
        };

        let (response, extra_action) = dispatch_result
            .handle_dispatch_result(
                Arc::clone(&server),
                &client_address,
                Arc::clone(&store),
                Arc::clone(&state),
            )
            .await;

        {
            let mut stream_guard = stream.lock().await;
            let _ = stream_guard.write_all(response.as_bytes()).await;
        }

        if let Some(extra_action) = extra_action {
            if let Some(action_response) = handle_extra_action(
                parsed_input,
                &client_address,
                Arc::clone(&stream),
                Arc::clone(&server),
                extra_action,
            )
            .await
            {
                let mut stream_guard = stream.lock().await;
                let _ = stream_guard.write_all(action_response.as_slice()).await;
            }
        }
    }
}

pub async fn handle_master_connection(
    master_address: &str,
    stream: Arc<Mutex<TcpStream>>,
    server: Arc<RwLock<RedisServer>>,
    store: Arc<Mutex<KeyValueStore>>,
    state: Arc<Mutex<State>>,
) {
    let mut buffer = [0; 1024];

    loop {
        let parsed_input = match read_and_parse_resp(Arc::clone(&stream), &mut buffer).await {
            Ok(cmd) => cmd,
            Err(e) => match e {
                CommandReadError::ConnectionClosed => {
                    break;
                }
                _ => {
                    continue;
                }
            },
        };

        let command_handler = match CommandHandler::new(&parsed_input) {
            Ok(handler) => handler,
            Err(_) => {
                continue;
            }
        };

        match command_handler
            .handle_command(
                Arc::clone(&server),
                master_address,
                Arc::clone(&store),
                Arc::clone(&state),
            )
            .await
        {
            Ok(_) => (),
            Err(_) => {
                continue;
            }
        }
    }
}
