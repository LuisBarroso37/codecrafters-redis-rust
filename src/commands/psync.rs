use std::sync::Arc;

use tokio::sync::RwLock;

use crate::{commands::CommandError, resp::RespValue, server::RedisServer};

pub struct PsyncArguments {
    master_repl_id: String,
    offset: i32,
}

impl PsyncArguments {
    pub fn parse(arguments: Vec<String>) -> Result<Self, CommandError> {
        if arguments.len() > 2 {
            return Err(CommandError::InvalidPsyncCommand);
        }

        let master_repl_id = arguments[0].clone();
        let offset = arguments[1]
            .parse::<i32>()
            .map_err(|_| CommandError::InvalidPsyncOffset)?;

        Ok(Self {
            master_repl_id,
            offset,
        })
    }
}

pub async fn psync(
    server: &Arc<RwLock<RedisServer>>,
    arguments: Vec<String>,
) -> Result<String, CommandError> {
    let psync_arguments = PsyncArguments::parse(arguments)?;

    let server_guard = server.read().await;

    let master_repl_id = match psync_arguments.master_repl_id.as_str() {
        "?" => server_guard.repl_id.clone(),
        repl_id => {
            if repl_id != server_guard.repl_id {
                return Err(CommandError::InvalidPsyncReplicationId);
            }

            repl_id.to_string()
        }
    };

    Ok(RespValue::SimpleString(format!(
        "FULLRESYNC {} {}",
        master_repl_id, server_guard.repl_offset
    ))
    .encode())
}
