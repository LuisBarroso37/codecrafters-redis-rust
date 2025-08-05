use std::{sync::Arc, time::Duration};

use tokio::sync::{Mutex, mpsc};

use crate::{
    commands::command_error::CommandError,
    key_value_store::{DataType, KeyValueStore},
    resp::RespValue,
    state::{State, Subscriber},
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

    let (sender, mut receiver) = mpsc::channel(1);
    let subscriber = Subscriber {
        server_address: server_address.clone(),
        sender: sender.clone(), // Clone the sender so we keep one reference
    };

    let mut state_guard = state.lock().await;
    state_guard.add_subscriber("BLPOP".to_string(), arguments[0].clone(), subscriber);
    drop(state_guard);

    // Don't drop the sender here - let it be dropped naturally when the function ends
    // This keeps the channel alive while we're waiting

    let result = match duration {
        0.0 => receiver.recv().await,
        num => match tokio::time::timeout(Duration::from_secs_f64(num), receiver.recv()).await {
            Ok(result) => result,
            Err(_) => {
                let mut state_guard = state.lock().await;
                state_guard.remove_subscriber("BLPOP", arguments[0].as_str(), &server_address);
                return Ok(RespValue::Null.encode());
            }
        },
    };

    match result {
        Some(_) => {
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
            state_guard.remove_subscriber("BLPOP", arguments[0].as_str(), &server_address);
            drop(state_guard);

            match popped_value {
                Some(value) => Ok(RespValue::encode_array_from_strings(vec![
                    arguments[0].clone(),
                    value,
                ])),
                None => Ok(RespValue::Null.encode()),
            }
        }
        None => {
            let mut state_guard = state.lock().await;
            state_guard.remove_subscriber("BLPOP", arguments[0].as_str(), &server_address);
            return Ok(RespValue::Null.encode());
        }
    }
}
