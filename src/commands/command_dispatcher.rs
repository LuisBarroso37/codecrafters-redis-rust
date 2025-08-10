use std::sync::Arc;

use thiserror::Error;
use tokio::sync::Mutex;

use crate::{
    commands::{CommandError, CommandHandler},
    key_value_store::KeyValueStore,
    resp::RespValue,
    state::{State, StateError},
};

#[derive(Error, Debug, PartialEq)]
pub enum DispatchError {
    #[error("Executed EXEC without MULTI")]
    ExecWithoutMulti,
    #[error("Transaction error")]
    DispatchError(#[from] StateError),
    #[error("Invalid command in transaction queue")]
    InvalidQueueCommand(#[from] CommandError),
    #[error("Executed DISCARD without MULTI")]
    DiscardWithoutMulti,
}

impl DispatchError {
    pub fn as_string(&self) -> String {
        match self {
            DispatchError::ExecWithoutMulti => {
                RespValue::Error("ERR EXEC without MULTI".to_string()).encode()
            }
            DispatchError::DispatchError(err) => {
                RespValue::Error(format!("ERR {}", err.as_string())).encode()
            }
            DispatchError::InvalidQueueCommand(err) => {
                RespValue::Error(format!("ERR {}", err.as_string())).encode()
            }
            DispatchError::DiscardWithoutMulti => {
                RespValue::Error("ERR DISCARD without MULTI".to_string()).encode()
            }
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum DispatchResult {
    ImmediateResponse(String),
    ExecuteTransactionCommands(Vec<CommandHandler>),
    ExecuteSingleCommand(CommandHandler),
}

impl DispatchResult {
    pub async fn handle_dispatch_result(
        &self,
        server_address: String,
        store: &mut Arc<Mutex<KeyValueStore>>,
        state: &mut Arc<Mutex<State>>,
    ) -> String {
        match self {
            DispatchResult::ImmediateResponse(value) => value.clone(),
            DispatchResult::ExecuteSingleCommand(command) => {
                match command
                    .handle_command(server_address.clone(), store, state)
                    .await
                {
                    Ok(resp) => resp,
                    Err(e) => e.as_string(),
                }
            }
            DispatchResult::ExecuteTransactionCommands(commands) => {
                let mut responses = Vec::with_capacity(commands.len() + 1);
                responses.push(format!("*{}\r\n", commands.len()));

                for cmd in commands {
                    match cmd
                        .handle_command(server_address.clone(), store, state)
                        .await
                    {
                        Ok(resp) => {
                            responses.push(resp);
                        }
                        Err(e) => {
                            responses.push(e.as_string());
                        }
                    }
                }

                responses.join("")
            }
        }
    }
}

pub struct CommandDispatcher {
    pub server_address: String,
    pub state: Arc<Mutex<State>>,
}

impl CommandDispatcher {
    pub fn new(server_address: String, state: Arc<Mutex<State>>) -> Self {
        CommandDispatcher {
            server_address,
            state,
        }
    }

    pub async fn dispatch_command(
        &self,
        command: CommandHandler,
    ) -> Result<DispatchResult, DispatchError> {
        match command.name.as_str() {
            "MULTI" => {
                let mut state_guard = self.state.lock().await;
                state_guard.start_transaction(self.server_address.clone())?;

                Ok(DispatchResult::ImmediateResponse(
                    RespValue::SimpleString("OK".to_string()).encode(),
                ))
            }
            "EXEC" => {
                let mut state_guard = self.state.lock().await;

                let Ok(transaction) = state_guard.remove_transaction(self.server_address.clone())
                else {
                    return Err(DispatchError::ExecWithoutMulti);
                };

                if transaction.is_empty() {
                    Ok(DispatchResult::ImmediateResponse(
                        RespValue::Array(Vec::new()).encode(),
                    ))
                } else {
                    Ok(DispatchResult::ExecuteTransactionCommands(transaction))
                }
            }
            "DISCARD" => {
                let mut state_guard = self.state.lock().await;

                let Ok(_) = state_guard.remove_transaction(self.server_address.clone()) else {
                    return Err(DispatchError::DiscardWithoutMulti);
                };

                Ok(DispatchResult::ImmediateResponse(
                    RespValue::SimpleString("OK".to_string()).encode(),
                ))
            }
            _ => {
                let mut state_guard = self.state.lock().await;

                let Some(_) = state_guard.get_transaction(&self.server_address) else {
                    return Ok(DispatchResult::ExecuteSingleCommand(command));
                };

                match command.validate_command_arguments() {
                    Some(err) => return Err(DispatchError::InvalidQueueCommand(err)),
                    None => {
                        state_guard.add_to_transaction(self.server_address.clone(), command)?;
                    }
                }

                Ok(DispatchResult::ImmediateResponse(
                    RespValue::SimpleString("QUEUED".to_string()).encode(),
                ))
            }
        }
    }
}
