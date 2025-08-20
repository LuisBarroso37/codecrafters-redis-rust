use std::{sync::Arc, time::Duration};

use tokio::io::AsyncWriteExt;
use tokio::{sync::RwLock, time::timeout};

use crate::commands::command_handler::CommandResult;
use crate::{commands::CommandError, resp::RespValue, server::RedisServer};

pub struct WaitArguments {
    pub number_of_replicas: usize,
    pub timeout_ms: Option<Duration>,
}

impl WaitArguments {
    pub fn parse(arguments: Vec<String>) -> Result<Self, CommandError> {
        if arguments.len() != 2 {
            return Err(CommandError::InvalidWaitCommand);
        }

        let number_of_replicas = arguments[0]
            .parse::<usize>()
            .map_err(|_| CommandError::InvalidWaitCommandArgument)?;

        let timeout = arguments[1]
            .parse::<u64>()
            .map_err(|_| CommandError::InvalidWaitCommandArgument)?;

        let timeout_ms = match timeout {
            0 => None,
            _ => Some(Duration::from_millis(timeout)),
        };

        Ok(Self {
            number_of_replicas,
            timeout_ms,
        })
    }
}

pub async fn wait(
    server: Arc<RwLock<RedisServer>>,
    arguments: Vec<String>,
) -> Result<CommandResult, CommandError> {
    let wait_arguments = WaitArguments::parse(arguments)?;

    let response = match wait_arguments.timeout_ms {
        Some(duration) => match timeout(
            duration,
            check_if_replica_processed_commands(
                Arc::clone(&server),
                wait_arguments.number_of_replicas,
            ),
        )
        .await
        {
            Ok(replicas) => replicas,
            Err(_) => get_synced_replica_count(Arc::clone(&server)).await,
        },
        None => {
            check_if_replica_processed_commands(
                Arc::clone(&server),
                wait_arguments.number_of_replicas,
            )
            .await
        }
    }?;
    // Return number of replicas that processed commands before timeout or until it matches given argument

    Ok(CommandResult::Response(
        RespValue::Integer(response as i64).encode(),
    ))
}

async fn check_if_replica_processed_commands(
    server: Arc<RwLock<RedisServer>>,
    number_of_replicas: usize,
) -> Result<usize, CommandError> {
    let synced_count = get_synced_replica_count(Arc::clone(&server)).await?;

    if synced_count >= number_of_replicas {
        return Ok(synced_count);
    }

    send_getack_to_unsynced_replicas(Arc::clone(&server)).await?;

    loop {
        let synced_count = get_synced_replica_count(Arc::clone(&server)).await?;

        if synced_count >= number_of_replicas {
            return Ok(synced_count);
        }

        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn get_synced_replica_count(server: Arc<RwLock<RedisServer>>) -> Result<usize, CommandError> {
    let server_guard = server.read().await;
    let Some(ref replicas) = server_guard.replicas else {
        return Err(CommandError::InvalidWaitCommandForReplica);
    };

    Ok(replicas
        .iter()
        .filter(|(_, replica)| replica.offset >= server_guard.repl_offset)
        .count())
}

async fn send_getack_to_unsynced_replicas(
    server: Arc<RwLock<RedisServer>>,
) -> Result<(), CommandError> {
    let server_guard = server.read().await;
    let Some(ref replicas) = server_guard.replicas else {
        return Err(CommandError::InvalidWaitCommand);
    };

    let replicas_to_check = replicas
        .iter()
        .filter(|(_, replica)| replica.offset < server_guard.repl_offset);

    for (_, replica) in replicas_to_check {
        let mut replica_writer_guard = replica.writer.write().await;

        if let Err(_) = replica_writer_guard
            .write_all(
                RespValue::Array(vec![
                    RespValue::BulkString("REPLCONF".to_string()),
                    RespValue::BulkString("GETACK".to_string()),
                    RespValue::BulkString("*".to_string()),
                ])
                .encode()
                .as_bytes(),
            )
            .await
        {
            return Err(CommandError::InvalidWaitCommand);
        }

        if let Err(_) = replica_writer_guard.flush().await {
            return Err(CommandError::InvalidWaitCommand);
        }
    }

    Ok(())
}
