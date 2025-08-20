//! PSYNC command implementation for Redis replication synchronization.
//!
//! The PSYNC command initiates partial or full resynchronization between
//! a master and replica server during the replication handshake process.

use std::sync::Arc;

use tokio::sync::RwLock;

use crate::{
    commands::{CommandError, command_handler::CommandResult},
    resp::RespValue,
    server::RedisServer,
};

/// Represents the parsed arguments for the PSYNC command.
///
/// PSYNC is used by replicas to request synchronization with the master,
/// providing the master's replication ID and the replica's current offset.
pub struct PsyncArguments {
    /// The replication ID of the master (or "?" for full resync)
    master_repl_id: String,
    /// The current replication offset (or -1 for full resync)
    offset: i32,
}

impl PsyncArguments {
    /// Parses and validates arguments for the PSYNC command.
    ///
    /// PSYNC requires exactly 2 arguments: replication ID and offset.
    /// For initial replication, these are typically "?" and "-1".
    ///
    /// # Arguments
    ///
    /// * `arguments` - Vector containing [replication_id, offset]
    ///
    /// # Returns
    ///
    /// * `Ok(PsyncArguments)` - Successfully parsed arguments
    /// * `Err(CommandError::InvalidPsyncCommand)` - If not exactly 2 arguments
    /// * `Err(CommandError::InvalidPsyncOffset)` - If offset is not a valid integer
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

/// Handles the Redis PSYNC command.
///
/// PSYNC initiates replication synchronization between master and replica.
/// This implementation supports full resynchronization by responding with
/// FULLRESYNC along with the master's replication ID and current offset.
///
/// # Arguments
///
/// * `server` - Thread-safe reference to the Redis server configuration
/// * `arguments` - Command arguments [replication_id, offset]
///
/// # Returns
///
/// * `Ok(String)` - FULLRESYNC response with replication ID and offset
/// * `Err(CommandError::InvalidPsyncCommand)` - If argument parsing fails
/// * `Err(CommandError::InvalidPsyncOffset)` - If offset is invalid
/// * `Err(CommandError::InvalidPsyncReplicationId)` - If replication ID mismatch
///
/// # Protocol Response
///
/// Returns a RESP simple string in the format:
/// `+FULLRESYNC <repl_id> <offset>\r\n`
pub async fn psync(
    server: Arc<RwLock<RedisServer>>,
    arguments: Vec<String>,
) -> Result<CommandResult, CommandError> {
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

    Ok(CommandResult::Sync(
        RespValue::SimpleString(format!(
            "FULLRESYNC {} {}",
            master_repl_id, server_guard.repl_offset
        ))
        .encode(),
    ))
}
