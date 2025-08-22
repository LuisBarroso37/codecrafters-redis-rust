use std::sync::Arc;

use tokio::sync::RwLock;

use crate::{
    commands::{CommandError, command_handler::CommandResult},
    resp::RespValue,
    server::RedisServer,
};

enum ReplconfConfiguration {
    ListeningPort,
    Capabilities,
    GetAck,
    Ack(usize),
}

pub struct ReplconfArguments {
    configuration: ReplconfConfiguration,
}

impl ReplconfArguments {
    pub fn parse(arguments: Vec<String>) -> Result<Self, CommandError> {
        if arguments.len() != 2 {
            return Err(CommandError::InvalidReplconfCommand);
        }

        let configuration = match arguments[0].to_lowercase().as_str() {
            "listening-port" => {
                arguments[1]
                    .parse::<u32>()
                    .map_err(|_| CommandError::InvalidReplconfCommand)?;

                ReplconfConfiguration::ListeningPort
            }
            "capa" => {
                if arguments[1] != "psync2" {
                    return Err(CommandError::InvalidReplconfCommand);
                }

                ReplconfConfiguration::Capabilities
            }
            "getack" => {
                if arguments[1] != "*" {
                    return Err(CommandError::InvalidReplconfCommand);
                }

                ReplconfConfiguration::GetAck
            }
            "ack" => {
                let offset = arguments[1]
                    .parse::<usize>()
                    .map_err(|_| CommandError::InvalidReplconfCommand)?;

                ReplconfConfiguration::Ack(offset)
            }

            _ => return Err(CommandError::InvalidReplconfCommand),
        };

        Ok(Self { configuration })
    }
}

pub async fn replconf(
    client_address: &str,
    server: Arc<RwLock<RedisServer>>,
    arguments: Vec<String>,
) -> Result<CommandResult, CommandError> {
    let replconf_arguments = ReplconfArguments::parse(arguments)?;

    match replconf_arguments.configuration {
        ReplconfConfiguration::ListeningPort => Ok(CommandResult::Response(
            RespValue::SimpleString("OK".to_string()).encode(),
        )),
        ReplconfConfiguration::Capabilities => Ok(CommandResult::Response(
            RespValue::SimpleString("OK".to_string()).encode(),
        )),
        ReplconfConfiguration::GetAck => {
            let server_guard = server.read().await;

            Ok(CommandResult::Response(
                RespValue::Array(vec![
                    RespValue::BulkString("REPLCONF".to_string()),
                    RespValue::BulkString("ACK".to_string()),
                    RespValue::BulkString(server_guard.repl_offset.to_string()),
                ])
                .encode(),
            ))
        }
        ReplconfConfiguration::Ack(offset) => {
            let mut server_guard = server.write().await;

            if let Some(ref mut replicas) = server_guard.replicas {
                if let Some(replica) = replicas.get_mut(client_address) {
                    replica.offset = offset;
                }
            }

            Ok(CommandResult::NoResponse)
        }
    }
}
