use std::sync::Arc;

use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::{
    net::TcpStream,
    sync::{Mutex, RwLock},
};

use crate::resp::RespValue;
use crate::server::RedisRole;
use crate::{
    commands::{CommandDispatcher, CommandHandler, handle_extra_action},
    input::{CommandReadError, read_and_parse_resp},
    key_value_store::KeyValueStore,
    server::RedisServer,
    state::State,
};

pub async fn handle_client_connection(
    stream: TcpStream,
    server: Arc<RwLock<RedisServer>>,
    client_address: String,
    store: Arc<Mutex<KeyValueStore>>,
    state: Arc<Mutex<State>>,
) {
    let mut buffer = [0; 1024];

    let (mut reader, writer) = stream.into_split();
    let writer = Arc::new(RwLock::new(writer));

    loop {
        let parsed_input = match read_and_parse_resp(&mut reader, &mut buffer).await {
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
                    if let Err(e) =
                        write_to_stream(Arc::clone(&writer), e.as_string().as_bytes()).await
                    {
                        eprintln!("Error writing to stream: {}", e);
                    }
                    continue;
                }
            },
        };

        for input in parsed_input {
            let command_handler = match CommandHandler::new(&input) {
                Ok(handler) => handler,
                Err(e) => {
                    if let Err(e) =
                        write_to_stream(Arc::clone(&writer), e.as_string().as_bytes()).await
                    {
                        eprintln!("Error writing to stream: {}", e);
                    }
                    continue;
                }
            };

            if are_write_commands_forbidden(Arc::clone(&server), &command_handler).await {
                let error_message =
                    RespValue::Error("ERR write commands not allowed in replica".to_string())
                        .encode();

                if let Err(e) = write_to_stream(Arc::clone(&writer), error_message.as_bytes()).await
                {
                    eprintln!("Error writing to stream: {}", e);
                }
                continue;
            };

            let dispatch_result = match CommandDispatcher::new(&client_address, Arc::clone(&state))
                .dispatch_command(command_handler)
                .await
            {
                Ok(result) => result,
                Err(e) => {
                    if let Err(e) =
                        write_to_stream(Arc::clone(&writer), e.as_string().as_bytes()).await
                    {
                        eprintln!("Error writing to stream: {}", e);
                    }
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

            if let Err(e) = write_to_stream(Arc::clone(&writer), response.as_bytes()).await {
                eprintln!("Error writing response to stream: {}", e);
                continue;
            }

            if let Some(extra_action) = extra_action {
                match handle_extra_action(
                    input,
                    &client_address,
                    Arc::clone(&writer),
                    Arc::clone(&server),
                    extra_action,
                )
                .await
                {
                    Ok(()) => (),
                    Err(e) => {
                        if let Err(e) =
                            write_to_stream(Arc::clone(&writer), e.to_string().as_bytes()).await
                        {
                            eprintln!("Error writing to stream: {}", e);
                        }
                    }
                }
            }
        }
    }
}

pub async fn handle_master_connection(
    master_address: &str,
    stream: &mut TcpStream,
    server: Arc<RwLock<RedisServer>>,
    store: Arc<Mutex<KeyValueStore>>,
    state: Arc<Mutex<State>>,
) {
    let mut buffer = [0; 1024];

    loop {
        let parsed_input = match read_and_parse_resp(stream, &mut buffer).await {
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

        for input in parsed_input {
            let command_handler = match CommandHandler::new(&input) {
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
}

pub async fn write_to_stream(
    writer: Arc<RwLock<OwnedWriteHalf>>,
    response: &[u8],
) -> tokio::io::Result<()> {
    let mut writer_guard = writer.write().await;
    writer_guard.write_all(response).await?;
    writer_guard.flush().await?;

    Ok(())
}

async fn are_write_commands_forbidden(
    server: Arc<RwLock<RedisServer>>,
    command_handler: &CommandHandler,
) -> bool {
    let server_guard = server.read().await;

    if let RedisRole::Replica(_) = server_guard.role {
        return server_guard
            .write_commands
            .contains(&command_handler.name.as_str());
    }

    false
}
