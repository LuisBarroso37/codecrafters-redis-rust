use std::sync::Arc;

use tokio::sync::RwLock;

use crate::{
    commands::{CommandError, CommandResult},
    resp::RespValue,
    server::RedisServer,
};

pub struct ConfigGetArguments {
    pub parameters: Vec<String>,
}

impl ConfigGetArguments {
    pub fn parse(arguments: Vec<String>) -> Result<Self, CommandError> {
        if arguments.is_empty() {
            return Err(CommandError::InvalidConfigGetCommand);
        }

        Ok(ConfigGetArguments {
            parameters: arguments,
        })
    }
}

pub async fn config_get(
    server: Arc<RwLock<RedisServer>>,
    arguments: Vec<String>,
) -> Result<CommandResult, CommandError> {
    let config_get_arguments = ConfigGetArguments::parse(arguments)?;
    let mut response = Vec::new();

    for arg in config_get_arguments.parameters {
        match arg.as_str() {
            "dir" => {
                let server_guard = server.read().await;
                let dir = server_guard.rdb_directory.clone();
                response.push(RespValue::BulkString("dir".to_string()));
                response.push(RespValue::BulkString(dir));
            }
            "dbfilename" => {
                let server_guard = server.read().await;
                let file = server_guard.rdb_filename.clone();
                response.push(RespValue::BulkString("dbfilename".to_string()));
                response.push(RespValue::BulkString(file));
            }
            _ => return Err(CommandError::InvalidConfigGetCommandArgument),
        }
    }

    Ok(CommandResult::Response(RespValue::Array(response).encode()))
}
