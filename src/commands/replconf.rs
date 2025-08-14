use crate::{commands::CommandError, resp::RespValue};

pub struct ReplconfArguments {
    arguments: Vec<String>,
}

impl ReplconfArguments {
    pub fn parse(arguments: Vec<String>) -> Result<Self, CommandError> {
        if arguments.len() > 2 {
            return Err(CommandError::InvalidReplconfCommand);
        }

        Ok(Self { arguments })
    }
}

pub async fn replconf(arguments: Vec<String>) -> Result<String, CommandError> {
    let _ = ReplconfArguments::parse(arguments)?;

    Ok(RespValue::SimpleString("OK".to_string()).encode())
}
