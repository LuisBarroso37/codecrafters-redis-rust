use crate::{commands::command_error::CommandError, resp::RespValue};

/// Represents the parsed arguments for ECHO command
struct EchoArguments {
    /// String to return as response
    argument: String,
}

impl EchoArguments {
    /// Parses command arguments into an EchoArguments structure.
    ///
    /// This function validates that exactly one argument is provided for the ECHO command
    /// and creates an EchoArguments instance containing the argument to be echoed back.
    ///
    /// # Arguments
    ///
    /// * `arguments` - A vector of strings representing the command arguments
    ///
    /// # Returns
    ///
    /// * `Ok(EchoArguments)` - Successfully parsed arguments with the string to echo
    /// * `Err(CommandError::InvalidEchoCommand)` - If the number of arguments is not exactly 1
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let result = EchoArguments::parse(vec!["hello".to_string()]);
    /// // Returns: Ok(EchoArguments { argument: "hello".to_string() })
    ///
    /// let result = EchoArguments::parse(vec![]);
    /// // Returns: Err(CommandError::InvalidEchoCommand)
    /// ```
    fn parse(arguments: Vec<String>) -> Result<Self, CommandError> {
        if arguments.len() != 1 {
            return Err(CommandError::InvalidEchoCommand);
        }

        Ok(Self {
            argument: arguments[0].clone(),
        })
    }
}

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
/// ```ignore
/// // ECHO "hello world"
/// let result = echo(vec!["hello world".to_string()]);
/// // Returns: "$11\r\nhello world\r\n"
/// ```
pub fn echo(arguments: Vec<String>) -> Result<String, CommandError> {
    let echo_arguments = EchoArguments::parse(arguments)?;

    Ok(RespValue::BulkString(echo_arguments.argument).encode())
}
