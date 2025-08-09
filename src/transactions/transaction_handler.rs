use std::sync::Arc;

use thiserror::Error;
use tokio::sync::Mutex;

use crate::{
    commands::CommandProcessor,
    resp::RespValue,
    state::{State, StateError},
};

#[derive(Error, Debug, PartialEq)]
pub enum TransactionError {
    #[error("Executed EXEC without MULTI")]
    ExecWithoutMulti,
    #[error("Transaction error")]
    TransactionError(#[from] StateError),
}

impl TransactionError {
    pub fn as_string(&self) -> String {
        match self {
            TransactionError::ExecWithoutMulti => {
                RespValue::Error("ERR EXEC without MULTI".to_string()).encode()
            }
            TransactionError::TransactionError(err) => {
                RespValue::Error(format!("ERR {}", err.as_string())).encode()
            }
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum TransactionResult {
    ImmediateResponse(String),
    ExecuteCommands(Vec<CommandProcessor>),
}

pub struct TransactionHandler {
    pub server_address: String,
    pub state: Arc<Mutex<State>>,
}

impl TransactionHandler {
    pub fn new(server_address: String, state: Arc<Mutex<State>>) -> Self {
        TransactionHandler {
            server_address,
            state,
        }
    }

    pub async fn handle_transaction(
        &self,
        command: CommandProcessor,
    ) -> Result<TransactionResult, TransactionError> {
        match command.name.as_str() {
            "MULTI" => {
                let mut state_guard = self.state.lock().await;
                state_guard.start_transaction(self.server_address.clone())?;

                Ok(TransactionResult::ImmediateResponse(
                    RespValue::SimpleString("OK".to_string()).encode(),
                ))
            }
            "EXEC" => {
                let mut state_guard = self.state.lock().await;

                let Ok(transaction) = state_guard.remove_transaction(self.server_address.clone())
                else {
                    return Err(TransactionError::ExecWithoutMulti);
                };

                if transaction.is_empty() {
                    Ok(TransactionResult::ImmediateResponse(
                        RespValue::Array(Vec::new()).encode(),
                    ))
                } else {
                    Ok(TransactionResult::ExecuteCommands(transaction))
                }
            }
            _ => {
                let mut state_guard = self.state.lock().await;

                let Some(_) = state_guard.get_transaction(&self.server_address) else {
                    return Ok(TransactionResult::ExecuteCommands(vec![command]));
                };

                state_guard.add_to_transaction(self.server_address.clone(), command)?;
                Ok(TransactionResult::ImmediateResponse(
                    RespValue::SimpleString("QUEUED".to_string()).encode(),
                ))
            }
        }
    }
}
