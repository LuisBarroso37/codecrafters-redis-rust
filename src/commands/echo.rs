use crate::{
    commands::{command_error::CommandError, command_handler::CommandResult},
    resp::RespValue,
};

pub struct EchoArguments {
    argument: String,
}

impl EchoArguments {
    pub fn parse(arguments: Vec<String>) -> Result<Self, CommandError> {
        if arguments.len() != 1 {
            return Err(CommandError::InvalidEchoCommand);
        }

        Ok(Self {
            argument: arguments[0].clone(),
        })
    }
}

pub fn echo(arguments: Vec<String>) -> Result<CommandResult, CommandError> {
    let echo_arguments = EchoArguments::parse(arguments)?;

    Ok(CommandResult::Response(
        RespValue::BulkString(echo_arguments.argument).encode(),
    ))
}
