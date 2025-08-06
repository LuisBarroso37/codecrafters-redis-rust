use std::{sync::Arc, time::Duration};

use tokio::sync::{Mutex, oneshot};

use crate::{
    commands::command_error::CommandError,
    key_value_store::{DataType, KeyValueStore},
    resp::RespValue,
    state::{BlpopSubscriber, State},
};

pub async fn blpop(
    server_address: String,
    store: &mut Arc<Mutex<KeyValueStore>>,
    state: &mut Arc<Mutex<State>>,
    arguments: Vec<String>,
) -> Result<String, CommandError> {
    if arguments.len() != 2 {
        return Err(CommandError::InvalidBLPopCommand);
    }

    let mut store_guard = store.lock().await;
    let immediate_result = if let Some(value) = store_guard.get_mut(&arguments[0]) {
        if let DataType::Array(ref mut list) = value.data {
            list.pop_front()
        } else {
            None
        }
    } else {
        None
    };
    drop(store_guard);

    if let Some(value) = immediate_result {
        return Ok(RespValue::encode_array_from_strings(vec![
            arguments[0].clone(),
            value,
        ]));
    }

    let duration = arguments[1]
        .parse::<f64>()
        .map_err(|_| CommandError::InvalidBLPopCommandArgument)?;

    let (sender, receiver) = oneshot::channel();
    let subscriber = BlpopSubscriber {
        server_address: server_address.clone(),
        sender,
    };

    let mut state_guard = state.lock().await;
    state_guard.add_blpop_subscriber(arguments[0].clone(), subscriber);
    drop(state_guard);

    let result = match duration {
        0.0 => receiver.await,
        num => match tokio::time::timeout(Duration::from_secs_f64(num), receiver).await {
            Ok(result) => result,
            Err(_) => {
                let mut state_guard = state.lock().await;
                state_guard.remove_blpop_subscriber(arguments[0].as_str(), &server_address);
                return Ok(RespValue::Null.encode());
            }
        },
    };

    match result {
        Ok(_) => {
            let mut store_guard = store.lock().await;
            let popped_value = if let Some(stored_data) = store_guard.get_mut(&arguments[0]) {
                if let DataType::Array(ref mut list) = stored_data.data {
                    list.pop_front()
                } else {
                    None
                }
            } else {
                None
            };
            drop(store_guard);

            let mut state_guard = state.lock().await;
            state_guard.remove_blpop_subscriber(arguments[0].as_str(), &server_address);
            drop(state_guard);

            match popped_value {
                Some(value) => Ok(RespValue::encode_array_from_strings(vec![
                    arguments[0].clone(),
                    value,
                ])),
                None => Ok(RespValue::Null.encode()),
            }
        }
        Err(_) => {
            let mut state_guard = state.lock().await;
            state_guard.remove_blpop_subscriber(arguments[0].as_str(), &server_address);
            return Ok(RespValue::Null.encode());
        }
    }
}
