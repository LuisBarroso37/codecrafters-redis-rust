use std::sync::Arc;

use tokio::sync::Mutex;

use crate::{
    commands::{CommandError, command_handler::CommandResult},
    resp::RespValue,
    state::State,
};

pub struct DiscardArguments;

impl DiscardArguments {
    pub fn parse(arguments: Vec<String>) -> Result<Self, CommandError> {
        if !arguments.is_empty() {
            return Err(CommandError::InvalidDiscardCommand);
        }

        Ok(Self)
    }
}

pub async fn discard(
    client_address: &str,
    state: Arc<Mutex<State>>,
    arguments: Vec<String>,
) -> Result<CommandResult, CommandError> {
    DiscardArguments::parse(arguments)?;

    let mut state_guard = state.lock().await;
    let Ok(_) = state_guard.remove_transaction(client_address.to_string()) else {
        return Err(CommandError::DiscardWithoutMulti);
    };

    Ok(CommandResult::Response(
        RespValue::SimpleString("OK".to_string()).encode(),
    ))
}
