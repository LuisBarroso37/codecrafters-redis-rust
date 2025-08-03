use crate::{commands::command_error::CommandError, resp::RespValue};

pub fn ping(arguments: Vec<String>) -> Result<String, CommandError> {
    if arguments.len() != 0 {
        return Err(CommandError::InvalidPingCommand);
    }

    return Ok(RespValue::SimpleString("PONG".to_string()).encode());
}
