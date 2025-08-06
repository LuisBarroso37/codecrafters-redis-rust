use crate::{commands::command_error::CommandError, resp::RespValue};

/// Handles the Redis ECHO command.
///
/// The ECHO command returns the exact string provided as an argument.
/// This is commonly used for testing connectivity and ensuring the Redis
/// server is responding correctly.
///
/// # Arguments
///
/// * `arguments` - A vector containing exactly one string argument to echo back
///
/// # Returns
///
/// * `Ok(String)` - A RESP-encoded bulk string containing the echoed argument
/// * `Err(CommandError::InvalidEchoCommand)` - If the number of arguments is not exactly 1
///
/// # Examples
///
/// ```
/// // ECHO "hello world"
/// let result = echo(vec!["hello world".to_string()]);
/// // Returns: "$11\r\nhello world\r\n"
/// ```
pub fn echo(arguments: Vec<String>) -> Result<String, CommandError> {
    if arguments.len() != 1 {
        return Err(CommandError::InvalidEchoCommand);
    }

    let arg = arguments[0].clone();
    return Ok(RespValue::BulkString(arg).encode());
}
