//! REPLCONF command implementation for Redis replication configuration.
//!
//! The REPLCONF command is used during the Redis replication handshake process
//! to exchange configuration information between master and replica servers.

use std::sync::Arc;

use tokio::sync::RwLock;

use crate::{commands::CommandError, resp::RespValue, server::RedisServer};

enum ReplconfConfiguration {
    ListeningPort,
    Capabilities,
    GetAck,
    Ack(usize),
}

/// Represents the parsed arguments for the REPLCONF command.
///
/// REPLCONF is used during replication setup to configure various aspects
/// of the master-replica connection, such as listening ports and capabilities.
pub struct ReplconfArguments {
    /// The configuration arguments provided to REPLCONF
    configuration: ReplconfConfiguration,
}

impl ReplconfArguments {
    /// Parses and validates arguments for the REPLCONF command.
    ///
    /// REPLCONF typically accepts 0-2 arguments depending on the specific
    /// configuration being set (e.g., "listening-port", "6379").
    ///
    /// # Arguments
    ///
    /// * `arguments` - Vector of command arguments (0-2 elements expected)
    ///
    /// # Returns
    ///
    /// * `Ok(ReplconfArguments)` - Successfully parsed arguments
    /// * `Err(CommandError::InvalidReplconfCommand)` - If more than 2 arguments provided
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

/// Handles the Redis REPLCONF command.
///
/// REPLCONF is used during replication handshake to configure the connection
/// between master and replica servers. This implementation accepts the command
/// arguments but simply returns "OK" as required by the Redis protocol.
///
/// # Arguments
///
/// * `arguments` - Command arguments (typically configuration key-value pairs)
///
/// # Returns
///
/// * `Ok(String)` - Always returns "OK" encoded as RESP simple string
/// * `Err(CommandError::InvalidReplconfCommand)` - If argument parsing fails
///
/// # Examples
///
/// ```ignore
/// // REPLCONF listening-port 6380
/// let result = replconf(vec!["listening-port".to_string(), "6380".to_string()]).await;
/// // Returns: Ok("+OK\r\n")
/// ```
pub async fn replconf(
    client_address: &str,
    server: Arc<RwLock<RedisServer>>,
    arguments: Vec<String>,
) -> Result<String, CommandError> {
    let replconf_arguments = ReplconfArguments::parse(arguments)?;

    match replconf_arguments.configuration {
        ReplconfConfiguration::ListeningPort => {
            Ok(RespValue::SimpleString("OK".to_string()).encode())
        }
        ReplconfConfiguration::Capabilities => {
            Ok(RespValue::SimpleString("OK".to_string()).encode())
        }
        ReplconfConfiguration::GetAck => {
            let server_guard = server.read().await;

            Ok(RespValue::Array(vec![
                RespValue::BulkString("REPLCONF".to_string()),
                RespValue::BulkString("ACK".to_string()),
                RespValue::BulkString(server_guard.repl_offset.to_string()),
            ])
            .encode())
        }
        ReplconfConfiguration::Ack(offset) => {
            let mut server_guard = server.write().await;

            if let Some(ref mut replicas) = server_guard.replicas {
                if let Some(replica) = replicas.get_mut(client_address) {
                    replica.offset = offset;
                }
            }

            // Return empty string for Ack, caller should not send a response
            Ok(String::new())
        }
    }
}
