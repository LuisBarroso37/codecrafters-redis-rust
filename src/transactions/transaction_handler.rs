use std::sync::Arc;

use tokio::sync::Mutex;

use crate::{resp::RespValue, state::State};

pub async fn handle_transaction(
    _server_address: String,
    _state: &mut Arc<Mutex<State>>,
    command_name: &str,
) -> Option<String> {
    match command_name {
        "MULTI" => Some(RespValue::SimpleString("OK".to_string()).encode()),
        "EXEC" => Some(RespValue::SimpleString("OK".to_string()).encode()),
        _ => None,
    }
}
