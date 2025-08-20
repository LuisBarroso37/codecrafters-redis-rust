use crate::{
    commands::{command_error::CommandError, command_handler::CommandResult},
    resp::RespValue,
};

pub struct PingArguments;

impl PingArguments {
    pub fn parse(arguments: Vec<String>) -> Result<Self, CommandError> {
        if arguments.len() != 0 {
            return Err(CommandError::InvalidPingCommand);
        }

        Ok(Self)
    }
}

pub fn ping(arguments: Vec<String>) -> Result<CommandResult, CommandError> {
    PingArguments::parse(arguments)?;

    return Ok(CommandResult::Response(
        RespValue::SimpleString("PONG".to_string()).encode(),
    ));
}
