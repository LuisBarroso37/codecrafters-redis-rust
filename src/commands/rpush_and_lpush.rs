use std::{collections::VecDeque, sync::Arc};

use tokio::sync::Mutex;

use crate::{
    commands::command_error::CommandError,
    key_value_store::{DataType, KeyValueStore, Value},
    resp::RespValue,
    state::State,
};

pub async fn rpush(
    store: &mut Arc<Mutex<KeyValueStore>>,
    state: &mut Arc<Mutex<State>>,
    arguments: Vec<String>,
) -> Result<String, CommandError> {
    return push_array_operations(store, state, arguments, false).await;
}

pub async fn lpush(
    store: &mut Arc<Mutex<KeyValueStore>>,
    state: &mut Arc<Mutex<State>>,
    arguments: Vec<String>,
) -> Result<String, CommandError> {
    return push_array_operations(store, state, arguments, true).await;
}

async fn push_array_operations(
    store: &mut Arc<Mutex<KeyValueStore>>,
    state: &mut Arc<Mutex<State>>,
    arguments: Vec<String>,
    should_prepend: bool,
) -> Result<String, CommandError> {
    if arguments.len() < 2 {
        return if should_prepend {
            Err(CommandError::InvalidLPushCommand)
        } else {
            Err(CommandError::InvalidRPushCommand)
        };
    }

    let mut array_length = 0;
    let mut was_empty_before = false;

    let mut store_guard = store.lock().await;
    store_guard
        .entry(arguments[0].clone())
        .and_modify(|v| {
            if let DataType::Array(ref mut list) = v.data {
                was_empty_before = list.is_empty();

                for i in 1..arguments.len() {
                    if should_prepend {
                        list.push_front(arguments[i].clone());
                    } else {
                        list.push_back(arguments[i].clone());
                    }
                }

                array_length = list.len();
            }
        })
        .or_insert_with(|| {
            let mut list = VecDeque::new();
            was_empty_before = true; // New list is considered as was empty before

            for i in 1..arguments.len() {
                if should_prepend {
                    list.push_front(arguments[i].clone());
                } else {
                    list.push_back(arguments[i].clone());
                }
            }

            array_length = list.len();

            return Value {
                data: DataType::Array(list),
                expiration: None,
            };
        });
    drop(store_guard);

    if array_length == 0 {
        return Err(CommandError::DataNotFound);
    }

    // Only notify subscribers if the list was empty before and now has elements
    if was_empty_before && array_length > 0 {
        let mut state_guard = state.lock().await;
        state_guard.send_to_subscriber("BLPOP", &arguments[0], true);
    }

    return Ok(RespValue::Integer(array_length as i64).encode());
}
