use std::time::Duration;

use crate::{commands::CommandError, resp::RespValue};

pub struct WaitArguments {
    pub number_of_replicas: u32,
    pub timeout_ms: Option<Duration>,
}

impl WaitArguments {
    pub fn parse(arguments: Vec<String>) -> Result<Self, CommandError> {
        if arguments.len() != 2 {
            return Err(CommandError::InvalidWaitCommand);
        }

        let number_of_replicas = arguments[0]
            .parse::<u32>()
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

pub async fn wait(arguments: Vec<String>) -> Result<String, CommandError> {
    let wait_args = WaitArguments::parse(arguments)?;

    Ok(RespValue::Integer(0).encode())
}