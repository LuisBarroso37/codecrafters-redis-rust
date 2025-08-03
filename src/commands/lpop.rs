use std::sync::Arc;

use tokio::sync::Mutex;

use crate::{
    commands::command_error::CommandError,
    key_value_store::{DataType, KeyValueStore},
    resp::RespValue,
};

pub async fn lpop(
    store: &mut Arc<Mutex<KeyValueStore>>,
    arguments: Vec<String>,
) -> Result<String, CommandError> {
    if arguments.len() < 1 || arguments.len() > 2 {
        return Err(CommandError::InvalidLPopCommand);
    }

    let mut store_guard = store.lock().await;
    let stored_data = store_guard.get_mut(&arguments[0]);

    let amount_of_elements_to_delete = if let Some(val) = arguments.get(1) {
        val.parse::<usize>()
            .map_err(|_| CommandError::InvalidLPopCommandArgument)?
    } else {
        1
    };

    match stored_data {
        Some(value) => {
            if let DataType::Array(ref mut list) = value.data {
                let mut vec = Vec::new();

                for _ in 0..amount_of_elements_to_delete {
                    if let Some(removed) = list.pop_front() {
                        vec.push(removed);
                    }
                }

                if vec.len() == 0 {
                    return Ok(RespValue::Null.encode());
                } else if vec.len() == 1 {
                    return Ok(RespValue::BulkString(vec[0].clone()).encode());
                } else {
                    return Ok(RespValue::encode_array_from_strings(vec));
                }
            } else {
                return Ok(RespValue::Null.encode());
            }
        }
        None => {
            return Ok(RespValue::Null.encode());
        }
    }
}
