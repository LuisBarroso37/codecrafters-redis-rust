use std::sync::Arc;

use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::{
    net::TcpStream,
    sync::{Mutex, RwLock},
};

use crate::commands::{CommandHandler, CommandResult, run_transaction_commands};
use crate::rdb::stream_rdb_file;
use crate::resp::RespValue;
use crate::{
    input::{CommandReadError, read_and_parse_resp},
    key_value_store::KeyValueStore,
    server::RedisServer,
    state::State,
};

pub async fn handle_master_to_client_connection(
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
                        thread_safe_write_to_stream(Arc::clone(&writer), e.as_string().as_bytes())
                            .await
                    {
                        eprintln!("Error writing to stream: {}", e);
                    }
                    continue;
                }
            },
        };

        for input in parsed_input {
            let command_handler = match CommandHandler::new(input) {
                Ok(handler) => handler,
                Err(e) => {
                    if let Err(e) =
                        thread_safe_write_to_stream(Arc::clone(&writer), e.as_string().as_bytes())
                            .await
                    {
                        eprintln!("Error writing to stream: {}", e);
                    }
                    continue;
                }
            };

            let command_result = match command_handler
                .handle_command_for_master_server(
                    Arc::clone(&server),
                    &client_address,
                    Arc::clone(&store),
                    Arc::clone(&state),
                )
                .await
            {
                Ok(response) => response,
                Err(e) => {
                    if let Err(e) =
                        thread_safe_write_to_stream(Arc::clone(&writer), e.as_string().as_bytes())
                            .await
                    {
                        eprintln!("Error writing to stream: {}", e);
                    }
                    continue;
                }
            };

            match command_result {
                CommandResult::NoResponse => (),
                CommandResult::Response(response) => {
                    if let Err(e) =
                        thread_safe_write_to_stream(Arc::clone(&writer), response.as_bytes()).await
                    {
                        eprintln!("Error writing to stream: {}", e);
                        continue;
                    }
                }
                CommandResult::Sync(response) => {
                    if let Err(e) =
                        thread_safe_write_to_stream(Arc::clone(&writer), response.as_bytes()).await
                    {
                        eprintln!("Error writing to stream: {}", e);
                        continue;
                    }

                    if let Err(e) =
                        stream_rdb_file(&client_address, Arc::clone(&writer), Arc::clone(&server))
                            .await
                    {
                        eprintln!("Error writing to stream: {}", e);
                        continue;
                    }
                }
                CommandResult::Batch(commands) => {
                    match run_transaction_commands(
                        &client_address,
                        Arc::clone(&server),
                        Arc::clone(&store),
                        Arc::clone(&state),
                        commands,
                    )
                    .await
                    {
                        Ok(response) => {
                            if let Err(e) = thread_safe_write_to_stream(
                                Arc::clone(&writer),
                                response.as_bytes(),
                            )
                            .await
                            {
                                eprintln!("Error writing to stream: {}", e);
                                continue;
                            }
                        }
                        Err(e) => {
                            if let Err(e) = thread_safe_write_to_stream(
                                Arc::clone(&writer),
                                e.as_string().as_bytes(),
                            )
                            .await
                            {
                                eprintln!("Error writing to stream: {}", e);
                            }
                            continue;
                        }
                    }
                }
            }
        }
    }
}

pub async fn handle_master_to_replica_connection(
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
                    eprintln!("Error reading command: {}", e);
                    continue;
                }
            },
        };

        for input in parsed_input {
            let command_handler = match CommandHandler::new(input) {
                Ok(handler) => handler,
                Err(_) => {
                    continue;
                }
            };

            let command_result = match command_handler
                .handle_command_for_replica_master_connection(
                    Arc::clone(&server),
                    master_address,
                    Arc::clone(&store),
                    Arc::clone(&state),
                )
                .await
            {
                Ok(response) => response,
                Err(_) => {
                    continue;
                }
            };

            match command_result {
                CommandResult::NoResponse => (),
                CommandResult::Response(response) => {
                    if let Err(e) = write_to_stream(stream, response.as_bytes()).await {
                        eprintln!("Error writing to stream: {}", e);
                        continue;
                    }
                }
                CommandResult::Sync(_) => {
                    let error_msg = RespValue::Error(
                        "ERR PSYNC command should not be handled by replica server".to_string(),
                    )
                    .encode();

                    if let Err(e) = write_to_stream(stream, error_msg.as_bytes()).await {
                        eprintln!("Error writing to stream: {}", e);
                        continue;
                    }
                }
                CommandResult::Batch(commands) => {
                    if let Err(e) = run_transaction_commands(
                        &master_address,
                        Arc::clone(&server),
                        Arc::clone(&store),
                        Arc::clone(&state),
                        commands,
                    )
                    .await
                    {
                        eprintln!("Error writing to stream: {}", e);
                        continue;
                    }
                }
            }
        }
    }
}

pub async fn handle_replica_to_client_connection(
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
                    break;
                }
                _ => {
                    if let Err(e) =
                        thread_safe_write_to_stream(Arc::clone(&writer), e.as_string().as_bytes())
                            .await
                    {
                        eprintln!("Error writing to stream: {}", e);
                    }
                    continue;
                }
            },
        };

        for input in parsed_input {
            let command_handler = match CommandHandler::new(input) {
                Ok(handler) => handler,
                Err(e) => {
                    if let Err(e) =
                        thread_safe_write_to_stream(Arc::clone(&writer), e.as_string().as_bytes())
                            .await
                    {
                        eprintln!("Error writing to stream: {}", e);
                    }
                    continue;
                }
            };

            let command_result = match command_handler
                .handle_command_for_replica_server(
                    Arc::clone(&server),
                    &client_address,
                    Arc::clone(&store),
                    Arc::clone(&state),
                )
                .await
            {
                Ok(response) => response,
                Err(e) => {
                    if let Err(e) =
                        thread_safe_write_to_stream(Arc::clone(&writer), e.as_string().as_bytes())
                            .await
                    {
                        eprintln!("Error writing to stream: {}", e);
                    }
                    continue;
                }
            };

            match command_result {
                CommandResult::NoResponse => (),
                CommandResult::Response(response) => {
                    if let Err(e) =
                        thread_safe_write_to_stream(Arc::clone(&writer), response.as_bytes()).await
                    {
                        eprintln!("Error writing to stream: {}", e);
                        continue;
                    }
                }
                CommandResult::Sync(_) => {
                    let error_msg = RespValue::Error(
                        "ERR PSYNC command should not be handled by replica server".to_string(),
                    )
                    .encode();

                    if let Err(e) =
                        thread_safe_write_to_stream(Arc::clone(&writer), error_msg.as_bytes()).await
                    {
                        eprintln!("Error writing to stream: {}", e);
                        continue;
                    }
                }
                CommandResult::Batch(_) => {
                    let error_msg = RespValue::Error(
                        "ERR transactions should not be handled by replica server".to_string(),
                    )
                    .encode();

                    if let Err(e) =
                        thread_safe_write_to_stream(Arc::clone(&writer), error_msg.as_bytes()).await
                    {
                        eprintln!("Error writing to stream: {}", e);
                        continue;
                    }
                }
            }
        }
    }
}

async fn thread_safe_write_to_stream(
    writer: Arc<RwLock<OwnedWriteHalf>>,
    response: &[u8],
) -> tokio::io::Result<()> {
    let mut writer_guard = writer.write().await;
    writer_guard.write_all(response).await?;
    writer_guard.flush().await?;

    Ok(())
}

async fn write_to_stream<W>(writer: &mut W, response: &[u8]) -> tokio::io::Result<()>
where
    W: AsyncWriteExt + Unpin,
{
    writer.write_all(response).await?;
    writer.flush().await?;

    Ok(())
}
