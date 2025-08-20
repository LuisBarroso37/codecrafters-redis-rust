use std::sync::Arc;

use tokio::sync::Mutex;

use crate::{
    commands::{command_error::CommandError, command_handler::CommandResult},
    key_value_store::{DataType, KeyValueStore},
    resp::RespValue,
};

pub struct LlenArguments {
    key: String,
}

impl LlenArguments {
    pub fn parse(arguments: Vec<String>) -> Result<Self, CommandError> {
        if arguments.len() != 1 {
            return Err(CommandError::InvalidLLenCommand);
        }

        Ok(Self {
            key: arguments[0].clone(),
        })
    }
}

pub async fn llen(
    store: Arc<Mutex<KeyValueStore>>,
    arguments: Vec<String>,
) -> Result<CommandResult, CommandError> {
    let llen_arguments = LlenArguments::parse(arguments)?;

    let store_guard = store.lock().await;
    let stored_data = store_guard.get(&llen_arguments.key);

    let Some(value) = stored_data else {
        return Ok(CommandResult::Response(RespValue::Integer(0).encode()));
    };

    if let DataType::Array(ref list) = value.data {
        return Ok(CommandResult::Response(
            RespValue::Integer(list.len() as i64).encode(),
        ));
    } else {
        return Ok(CommandResult::Response(RespValue::Integer(0).encode()));
    }
}
