use std::sync::Arc;

use thiserror::Error;
use tokio::sync::Mutex;

use crate::{
    resp::RespValue,
    state::{State, StateError},
};

#[derive(Error, Debug)]
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
        command_name: &str,
    ) -> Result<Option<String>, TransactionError> {
        match command_name {
            "MULTI" => {
                let mut state_guard = self.state.lock().await;
                state_guard.start_transaction(self.server_address.clone())?;

                Ok(Some(RespValue::SimpleString("OK".to_string()).encode()))
            }
            "EXEC" => {
                let mut state_guard = self.state.lock().await;

                if let Some(_) = state_guard.get_transaction(&self.server_address) {
                    Ok(Some(RespValue::SimpleString("OK".to_string()).encode()))
                } else {
                    return Err(TransactionError::ExecWithoutMulti);
                }
            }
            _ => Ok(None),
        }
    }
}
