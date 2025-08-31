use crate::{
    commands::{command_error::CommandError, command_handler::CommandResult},
    resp::RespValue,
};

pub struct SubscribePingArguments;

impl SubscribePingArguments {
    pub fn parse(arguments: Vec<String>) -> Result<Self, CommandError> {
        if arguments.len() != 0 {
            return Err(CommandError::InvalidPingCommand);
        }

        Ok(Self)
    }
}

pub fn subscribe_ping(arguments: Vec<String>) -> Result<CommandResult, CommandError> {
    SubscribePingArguments::parse(arguments)?;

    return Ok(CommandResult::Response(
        RespValue::Array(vec![
            RespValue::BulkString("pong".to_string()),
            RespValue::BulkString("".to_string()),
        ])
        .encode(),
    ));
}
