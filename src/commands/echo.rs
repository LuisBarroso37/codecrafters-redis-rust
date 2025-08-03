use crate::{commands::command_error::CommandError, resp::RespValue};

pub fn echo(arguments: Vec<String>) -> Result<String, CommandError> {
    if arguments.len() != 1 {
        return Err(CommandError::InvalidEchoCommand);
    }

    let arg = arguments[0].clone();
    return Ok(RespValue::BulkString(arg).encode());
}
