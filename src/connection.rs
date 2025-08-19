//! Connection handling for Redis server.
//!
//! This module provides functions for managing TCP connections between clients and the server,
//! as well as connections between master and replica servers in a Redis replication setup.
//! It handles command processing, response writing, and connection lifecycle management.

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

/// Handles a client connection for the Redis server.
///
/// This function manages the complete lifecycle of a client connection, including:
/// - Reading and parsing RESP commands from the client
/// - Validating command permissions based on server role (master/replica)
/// - Dispatching commands to appropriate handlers
/// - Writing responses back to the client
/// - Managing replica registrations for master servers
///
/// The function runs in a loop until the connection is closed by the client
/// or an unrecoverable error occurs.
///
/// # Arguments
///
/// * `stream` - The TCP stream representing the client connection
/// * `server` - A thread-safe reference to the Redis server configuration
/// * `client_address` - String representation of the client's address for logging and identification
/// * `store` - A thread-safe reference to the key-value store
/// * `state` - A thread-safe reference to the server state for blocking operations
///
/// # Behavior
///
/// - For master servers: All commands are allowed, and replica connections are tracked
/// - For replica servers: Write commands are rejected with an error response
/// - Connection errors and command parsing errors are handled gracefully
/// - The function terminates when the client closes the connection
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

/// Handles commands received from a master server on a replica.
///
/// This function is used by replica servers to process commands sent from their
/// master server during replication. Unlike client connections, this function:
/// - Does not send responses back to the master (replication is one-way)
/// - Continues processing even if individual commands fail
/// - Applies all commands received from the master to maintain data consistency
///
/// The function runs in a loop until the master connection is closed or an
/// unrecoverable network error occurs.
///
/// # Arguments
///
/// * `master_address` - String representation of the master server's address
/// * `stream` - Mutable reference to the TCP stream connected to the master
/// * `server` - A thread-safe reference to the Redis server configuration
/// * `store` - A thread-safe reference to the key-value store to update
/// * `state` - A thread-safe reference to the server state
///
/// # Behavior
///
/// - Commands are processed silently without sending responses
/// - Command parsing errors are ignored to maintain connection stability
/// - The function terminates when the master closes the connection
/// - All successful commands update the replica's data store
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
                    eprintln!("Error reading command: {}", e);
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
                Ok(response) => {
                    update_replica_offset(Arc::clone(&server), input).await;

                    if command_handler.name == "REPLCONF" {
                        if let Err(e) = stream.write_all(response.as_bytes()).await {
                            eprintln!("Error writing to stream: {}", e);
                        }
                        if let Err(e) = stream.flush().await {
                            eprintln!("Error flushing stream: {}", e);
                        }

                        continue;
                    }
                }
                Err(_) => {
                    continue;
                }
            }
        }
    }
}

async fn update_replica_offset(server: Arc<RwLock<RedisServer>>, input: RespValue) {
    let mut server_guard = server.write().await;
    server_guard.repl_offset += input.encode().as_bytes().len();
}

/// Writes a response to a TCP stream in a thread-safe manner.
///
/// This function handles writing data to a shared TCP stream writer, ensuring
/// that concurrent writes are properly synchronized. It acquires an exclusive
/// lock on the writer before performing the write and flush operations.
///
/// # Arguments
///
/// * `writer` - A thread-safe reference to the owned write half of the TCP stream
/// * `response` - The byte slice containing the data to write to the stream
///
/// # Returns
///
/// * `Ok(())` - If the write and flush operations complete successfully
/// * `Err(tokio::io::Error)` - If any I/O error occurs during writing or flushing
///
/// # Behavior
///
/// - Acquires an exclusive lock on the writer to prevent concurrent access
/// - Writes all bytes in the response buffer
/// - Flushes the stream to ensure data is transmitted immediately
/// - Automatically releases the lock when the function completes
async fn write_to_stream(
    writer: Arc<RwLock<OwnedWriteHalf>>,
    response: &[u8],
) -> tokio::io::Result<()> {
    let mut writer_guard = writer.write().await;
    writer_guard.write_all(response).await?;
    writer_guard.flush().await?;

    Ok(())
}

/// Determines whether write commands should be forbidden for the current server role.
///
/// This function implements the Redis replication rule that replica servers should
/// not accept write commands from clients, only from their master server. It checks
/// the server's current role and whether the given command is classified as a write operation.
///
/// # Arguments
///
/// * `server` - A thread-safe reference to the Redis server configuration
/// * `command_handler` - The command handler containing the command name to check
///
/// # Returns
///
/// * `true` - If the server is a replica and the command is a write command
/// * `false` - If the server is a master, or if the command is not a write command
///
/// # Write Commands
///
/// The following commands are considered write operations:
/// - SET, RPUSH, LPUSH, INCR, LPOP, BLPOP, XADD
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::{RedisRole, RedisServer};
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_are_write_commands_forbidden() {
        let test_cases = [
            (
                RedisRole::Master,
                "SET",
                vec!["SET", "RPUSH", "LPUSH"],
                false,
                "master allows write commands",
            ),
            (
                RedisRole::Master,
                "GET",
                vec!["SET", "RPUSH", "LPUSH"],
                false,
                "master allows read commands",
            ),
            (
                RedisRole::Master,
                "UNKNOWN",
                vec!["SET", "RPUSH", "LPUSH"],
                false,
                "master allows unknown commands",
            ),
            (
                RedisRole::Replica(("localhost".to_string(), 6380)),
                "SET",
                vec!["SET", "RPUSH", "LPUSH"],
                true,
                "replica forbids SET command",
            ),
            (
                RedisRole::Replica(("localhost".to_string(), 6380)),
                "RPUSH",
                vec!["SET", "RPUSH", "LPUSH"],
                true,
                "replica forbids RPUSH command",
            ),
            (
                RedisRole::Replica(("localhost".to_string(), 6380)),
                "LPUSH",
                vec!["SET", "RPUSH", "LPUSH"],
                true,
                "replica forbids LPUSH command",
            ),
            (
                RedisRole::Replica(("localhost".to_string(), 6380)),
                "GET",
                vec!["SET", "RPUSH", "LPUSH"],
                false,
                "replica allows read commands",
            ),
            (
                RedisRole::Replica(("localhost".to_string(), 6380)),
                "UNKNOWN",
                vec!["SET", "RPUSH", "LPUSH"],
                false,
                "replica allows unknown commands",
            ),
            (
                RedisRole::Replica(("localhost".to_string(), 6380)),
                "SET",
                vec![],
                false,
                "replica allows all when write_commands is empty",
            ),
            (
                RedisRole::Replica(("localhost".to_string(), 6380)),
                "INCR",
                vec!["SET", "RPUSH", "LPUSH"],
                false,
                "replica allows commands not in write_commands list",
            ),
        ];

        for (role, command_name, write_commands, expected, description) in test_cases {
            let replicas = match role {
                RedisRole::Master => Some(HashMap::new()),
                RedisRole::Replica(_) => None,
            };

            let server = Arc::new(RwLock::new(RedisServer {
                port: 6379,
                role,
                repl_id: "test_id".to_string(),
                repl_offset: 0,
                replicas,
                write_commands,
            }));

            let command_handler = CommandHandler {
                name: command_name.to_string(),
                arguments: vec!["arg1".to_string(), "arg2".to_string()],
            };

            let result = are_write_commands_forbidden(server, &command_handler).await;
            assert_eq!(
                result, expected,
                "Failed for {}: command '{}'",
                description, command_name
            );
        }
    }
}
