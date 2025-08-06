use crate::{commands::command_error::CommandError, resp::RespValue};

/// Handles the Redis PING command.
///
/// The PING command is used to test if the Redis server is alive and responding.
/// It returns "PONG" to indicate that the server is working correctly.
/// This implementation only supports the no-argument version of PING.
///
/// # Arguments
///
/// * `arguments` - Must be an empty vector (no arguments allowed)
///
/// # Returns
///
/// * `Ok(String)` - A RESP-encoded simple string "PONG"
/// * `Err(CommandError::InvalidPingCommand)` - If any arguments are provided
///
/// # Examples
///
/// ```
/// // PING
/// let result = ping(vec![]);
/// // Returns: "+PONG\r\n"
/// ```
pub fn ping(arguments: Vec<String>) -> Result<String, CommandError> {
    if arguments.len() != 0 {
        return Err(CommandError::InvalidPingCommand);
    }

    return Ok(RespValue::SimpleString("PONG".to_string()).encode());
}
