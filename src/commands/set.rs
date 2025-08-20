use std::{sync::Arc, time::Duration};

use tokio::{sync::Mutex, time::Instant};

use crate::{
    commands::{command_error::CommandError, command_handler::CommandResult},
    key_value_store::{DataType, KeyValueStore, Value},
    resp::RespValue,
};

pub struct SetArguments {
    key: String,
    value: String,
    expiration: Option<Instant>,
}

impl SetArguments {
    pub fn parse(arguments: Vec<String>) -> Result<Self, CommandError> {
        if arguments.len() != 2 && arguments.len() != 4 {
            return Err(CommandError::InvalidSetCommand);
        }

        let mut expiration: Option<Instant> = None;

        if arguments.len() == 4 {
            if arguments[2].to_lowercase() != "px" {
                return Err(CommandError::InvalidSetCommandArgument);
            }

            if let Ok(expiration_time) = arguments[3].parse::<u64>() {
                expiration = Some(Instant::now() + Duration::from_millis(expiration_time))
            } else {
                return Err(CommandError::InvalidSetCommandExpiration);
            }
        }

        Ok(Self {
            key: arguments[0].clone(),
            value: arguments[1].clone(),
            expiration,
        })
    }
}

pub async fn set(
    store: Arc<Mutex<KeyValueStore>>,
    arguments: Vec<String>,
) -> Result<CommandResult, CommandError> {
    let set_arguments = SetArguments::parse(arguments)?;

    let mut store_guard = store.lock().await;
    store_guard.insert(
        set_arguments.key,
        Value {
            data: DataType::String(set_arguments.value),
            expiration: set_arguments.expiration,
        },
    );

    Ok(CommandResult::Response(
        RespValue::SimpleString("OK".to_string()).encode(),
    ))
}
