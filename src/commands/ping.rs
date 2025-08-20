use crate::{
    commands::{command_error::CommandError, command_handler::CommandResult},
    resp::RespValue,
};

pub struct PingArguments;

/// Represents the parsed arguments for the PING command.
///
/// The PING command in Redis is typically used without any arguments to check if the server is alive.
/// This struct is used to validate that no arguments are provided.
impl PingArguments {
    /// Parses and validates the arguments for the PING command.
    ///
    /// The PING command does not accept any arguments. This function checks that the provided
    /// arguments vector is empty. If any arguments are present, it returns an error.
    ///
    /// # Arguments
    ///
    /// * `arguments` - A vector of command arguments (should be empty for PING)
    ///
    /// # Returns
    ///
    /// * `Ok(PingArguments)` - If no arguments are provided
    /// * `Err(CommandError::InvalidPingCommand)` - If any arguments are present
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Valid usage
    /// let args = PingArguments::parse(vec![]).unwrap();
    ///
    /// // Invalid usage
    /// let err = PingArguments::parse(vec!["unexpected".to_string()]);
    /// assert!(err.is_err());
    /// ```
    pub fn parse(arguments: Vec<String>) -> Result<Self, CommandError> {
        if arguments.len() != 0 {
            return Err(CommandError::InvalidPingCommand);
        }

        Ok(Self)
    }
}

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
/// ```ignore
/// // PING
/// let result = ping(vec![]);
/// // Returns: "+PONG\r\n"
/// ```
pub fn ping(arguments: Vec<String>) -> Result<CommandResult, CommandError> {
    PingArguments::parse(arguments)?;

    return Ok(CommandResult::Response(
        RespValue::SimpleString("PONG".to_string()).encode(),
    ));
}
