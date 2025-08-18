//! REPLCONF command implementation for Redis replication configuration.
//!
//! The REPLCONF command is used during the Redis replication handshake process
//! to exchange configuration information between master and replica servers.

use crate::{commands::CommandError, resp::RespValue};

/// Represents the parsed arguments for the REPLCONF command.
///
/// REPLCONF is used during replication setup to configure various aspects
/// of the master-replica connection, such as listening ports and capabilities.
pub struct ReplconfArguments {
    /// The configuration arguments provided to REPLCONF
    arguments: Vec<String>,
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
        if arguments.len() > 2 {
            return Err(CommandError::InvalidReplconfCommand);
        }

        Ok(Self { arguments })
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
pub async fn replconf(arguments: Vec<String>) -> Result<String, CommandError> {
    let _ = ReplconfArguments::parse(arguments)?;

    Ok(RespValue::SimpleString("OK".to_string()).encode())
}
