use std::sync::Arc;

use tokio::sync::RwLock;

use crate::{commands::CommandError, resp::RespValue, server::RedisServer};

enum InfoSection {
    DEFAULT,
    REPLICATION,
}

pub struct InfoArguments {
    section: InfoSection,
}

impl InfoArguments {
    pub fn parse(arguments: Vec<String>) -> Result<Self, CommandError> {
        if arguments.len() > 1 {
            return Err(CommandError::InvalidInfoCommand);
        }

        if arguments.is_empty() {
            return Ok(InfoArguments {
                section: InfoSection::DEFAULT,
            });
        }

        let section = match arguments[0].as_str() {
            "replication" => InfoSection::REPLICATION,
            _ => return Err(CommandError::InvalidInfoSection),
        };

        Ok(InfoArguments { section })
    }
}

pub async fn info(
    server: Arc<RwLock<RedisServer>>,
    arguments: Vec<String>,
) -> Result<String, CommandError> {
    let info_arguments = InfoArguments::parse(arguments)?;

    let server_guard = server.read().await;
    let server_role = server_guard.role.as_string();

    let mut replication = Vec::new();

    if server_role == "master" {
        replication.push(format!("role:{}", server_role));
        replication.push(format!("connected_slaves:{}", 0));
        replication.push(format!("master_replid:{}", server_guard.repl_id));
        replication.push(format!("master_repl_offset:{}", server_guard.repl_offset));
    } else {
        replication.push(format!("role:{}", server_role));
    }

    match info_arguments.section {
        InfoSection::DEFAULT => Ok(RespValue::BulkString(replication.join("\r\n")).encode()),
        InfoSection::REPLICATION => Ok(RespValue::BulkString(replication.join("\r\n")).encode()),
    }
}
