use std::sync::Arc;

use tokio::sync::Mutex;

use crate::{
    commands::{command_error::CommandError, command_handler::CommandResult},
    key_value_store::{DataType, KeyValueStore},
    resp::RespValue,
};

pub struct LpopArguments {
    key: String,
    count: usize,
}

impl LpopArguments {
    pub fn parse(arguments: Vec<String>) -> Result<Self, CommandError> {
        if arguments.len() < 1 || arguments.len() > 2 {
            return Err(CommandError::InvalidLPopCommand);
        }

        let count = if let Some(val) = arguments.get(1) {
            val.parse::<usize>()
                .map_err(|_| CommandError::InvalidLPopCommandArgument)?
        } else {
            1
        };

        Ok(Self {
            key: arguments[0].clone(),
            count,
        })
    }
}

pub async fn lpop(
    store: Arc<Mutex<KeyValueStore>>,
    arguments: Vec<String>,
) -> Result<CommandResult, CommandError> {
    let lpop_arguments = LpopArguments::parse(arguments)?;

    let mut store_guard = store.lock().await;

    let Some(value) = store_guard.get_mut(&lpop_arguments.key) else {
        return Ok(CommandResult::Response(RespValue::NullBulkString.encode()));
    };

    let DataType::Array(ref mut list) = value.data else {
        return Ok(CommandResult::Response(RespValue::NullBulkString.encode()));
    };

    let mut vec = Vec::new();

    for _ in 0..lpop_arguments.count {
        if let Some(removed) = list.pop_front() {
            vec.push(removed);
        }
    }

    match vec.len() {
        0 => Ok(CommandResult::Response(RespValue::NullBulkString.encode())),
        1 => Ok(CommandResult::Response(
            RespValue::BulkString(vec[0].clone()).encode(),
        )),
        _ => Ok(CommandResult::Response(
            RespValue::encode_array_from_strings(vec),
        )),
    }
}
