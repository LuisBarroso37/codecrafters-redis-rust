use std::{collections::VecDeque, sync::Arc};

use tokio::sync::Mutex;

use crate::{
    commands::{command_error::CommandError, command_handler::CommandResult},
    key_value_store::{DataType, KeyValueStore, Value},
    resp::RespValue,
    state::State,
};

pub struct PushArrayOperations {
    key: String,
    values: Vec<String>,
}

impl PushArrayOperations {
    pub fn parse(arguments: Vec<String>, should_prepend: bool) -> Result<Self, CommandError> {
        if arguments.len() < 2 {
            return if should_prepend {
                Err(CommandError::InvalidLPushCommand)
            } else {
                Err(CommandError::InvalidRPushCommand)
            };
        }

        Ok(Self {
            key: arguments[0].clone(),
            values: arguments[1..].to_vec(),
        })
    }
}

pub async fn rpush(
    store: Arc<Mutex<KeyValueStore>>,
    state: Arc<Mutex<State>>,
    arguments: Vec<String>,
) -> Result<CommandResult, CommandError> {
    return push_array_operations(store, state, arguments, false).await;
}

pub async fn lpush(
    store: Arc<Mutex<KeyValueStore>>,
    state: Arc<Mutex<State>>,
    arguments: Vec<String>,
) -> Result<CommandResult, CommandError> {
    return push_array_operations(store, state, arguments, true).await;
}

async fn push_array_operations(
    store: Arc<Mutex<KeyValueStore>>,
    state: Arc<Mutex<State>>,
    arguments: Vec<String>,
    should_prepend: bool,
) -> Result<CommandResult, CommandError> {
    let push_array_arguments = PushArrayOperations::parse(arguments, should_prepend)?;

    let (pushed_values_count, was_empty_before) = {
        let mut store_guard = store.lock().await;

        match store_guard.get_mut(&push_array_arguments.key) {
            Some(value) => {
                let DataType::Array(ref mut list) = value.data else {
                    return Err(CommandError::InvalidDataTypeForKey);
                };

                let was_empty_before = list.is_empty();
                add_values_to_list(list, &push_array_arguments.values, should_prepend);

                (list.len(), was_empty_before)
            }
            None => {
                let mut list = VecDeque::new();
                add_values_to_list(&mut list, &push_array_arguments.values, should_prepend);

                let list_length = list.len();
                store_guard.insert(
                    push_array_arguments.key.clone(),
                    Value {
                        data: DataType::Array(list),
                        expiration: None,
                    },
                );

                (list_length, true)
            }
        }
    };

    // This should never happen with the validation in place in the beginning of the function
    if pushed_values_count == 0 {
        return Err(CommandError::DataNotFound);
    }

    // Notify BLPOP subscribers if list was empty or key did not exist before
    if was_empty_before {
        let mut state_guard = state.lock().await;
        state_guard.send_to_blpop_subscriber(&push_array_arguments.key, true);
    }

    return Ok(CommandResult::Response(
        RespValue::Integer(pushed_values_count as i64).encode(),
    ));
}

fn add_values_to_list(list: &mut VecDeque<String>, values: &[String], should_prepend: bool) {
    for value in values {
        if should_prepend {
            list.push_front(value.clone());
        } else {
            list.push_back(value.clone());
        }
    }
}
