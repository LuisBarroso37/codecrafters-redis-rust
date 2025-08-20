use std::sync::Arc;

use tokio::sync::Mutex;

use crate::{
    commands::{CommandError, command_handler::CommandResult},
    key_value_store::{DataType, KeyValueStore, Value},
    resp::RespValue,
};

pub struct IncrArguments {
    key: String,
}

impl IncrArguments {
    pub fn parse(arguments: Vec<String>) -> Result<Self, CommandError> {
        if arguments.len() != 1 {
            return Err(CommandError::InvalidIncrCommand);
        }

        Ok(Self {
            key: arguments[0].clone(),
        })
    }
}

pub async fn incr(
    store: Arc<Mutex<KeyValueStore>>,
    arguments: Vec<String>,
) -> Result<CommandResult, CommandError> {
    let incr_arguments = IncrArguments::parse(arguments)?;

    let mut store_guard = store.lock().await;

    let Some(value) = store_guard.get_mut(&incr_arguments.key) else {
        store_guard.insert(
            incr_arguments.key,
            Value {
                data: DataType::String("1".to_string()),
                expiration: None,
            },
        );
        return Ok(CommandResult::Response(RespValue::Integer(1).encode()));
    };

    match value.data {
        DataType::String(ref mut stored_data) => {
            let int = stored_data
                .parse::<i64>()
                .map_err(|_| CommandError::InvalidIncrValue)?;
            let incremented_int = int + 1;
            *stored_data = incremented_int.to_string();

            Ok(CommandResult::Response(
                RespValue::Integer(incremented_int).encode(),
            ))
        }
        _ => return Err(CommandError::InvalidDataTypeForKey),
    }
}
