use std::sync::Arc;

use tokio::sync::Mutex;

use crate::{
    commands::{CommandError, command_handler::CommandResult},
    resp::RespValue,
    state::State,
};

pub struct MultiArguments;

impl MultiArguments {
    pub fn parse(arguments: Vec<String>) -> Result<Self, CommandError> {
        if !arguments.is_empty() {
            return Err(CommandError::InvalidMultiCommand);
        }

        Ok(Self)
    }
}

pub async fn multi(
    client_address: &str,
    state: Arc<Mutex<State>>,
    arguments: Vec<String>,
) -> Result<CommandResult, CommandError> {
    MultiArguments::parse(arguments)?;

    let mut state_guard = state.lock().await;
    state_guard.start_transaction(client_address.to_string())?;

    Ok(CommandResult::Response(
        RespValue::SimpleString("OK".to_string()).encode(),
    ))
}
