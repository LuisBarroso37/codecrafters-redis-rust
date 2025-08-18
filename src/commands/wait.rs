use std::{sync::Arc, time::Duration};

use tokio::sync::RwLock;

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

        Ok(Self { number_of_replicas, timeout_ms })
    }
}

pub async fn wait(server: Arc<RwLock<RedisServer>>, arguments: Vec<String>) -> Result<String, CommandError> {
    let wait_args = WaitArguments::parse(arguments)?;

    let server_guard = server.read().await;
    let Some(ref replicas) = server_guard.replicas else {
        return Err(CommandError::InvalidWaitCommand);
    };

    Ok(RespValue::Integer(replicas.len() as i64).encode())
}