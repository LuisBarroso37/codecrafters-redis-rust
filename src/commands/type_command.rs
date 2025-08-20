use std::sync::Arc;

use tokio::sync::Mutex;

use crate::{
    commands::{command_error::CommandError, command_handler::CommandResult},
    key_value_store::{DataType, KeyValueStore},
    resp::RespValue,
};

pub struct TypeArguments {
    key: String,
}

impl TypeArguments {
    pub fn parse(arguments: Vec<String>) -> Result<Self, CommandError> {
        if arguments.len() != 1 {
            return Err(CommandError::InvalidTypeCommand);
        }

        Ok(Self {
            key: arguments[0].clone(),
        })
    }
}

pub async fn type_command(
    store: Arc<Mutex<KeyValueStore>>,
    arguments: Vec<String>,
) -> Result<CommandResult, CommandError> {
    let type_arguments = TypeArguments::parse(arguments)?;

    let store_guard = store.lock().await;

    let Some(value) = store_guard.get(&type_arguments.key) else {
        return Ok(CommandResult::Response(
            RespValue::SimpleString("none".to_string()).encode(),
        ));
    };

    match value.data {
        DataType::String(_) => {
            return Ok(CommandResult::Response(
                RespValue::SimpleString("string".to_string()).encode(),
            ));
        }
        DataType::Array(_) => {
            return Ok(CommandResult::Response(
                RespValue::SimpleString("list".to_string()).encode(),
            ));
        }
        DataType::Stream(_) => {
            return Ok(CommandResult::Response(
                RespValue::SimpleString("stream".to_string()).encode(),
            ));
        }
    }
}
