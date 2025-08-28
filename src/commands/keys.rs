use std::sync::Arc;

use globset::Glob;
use tokio::sync::Mutex;

use crate::{
    commands::{CommandError, CommandResult},
    key_value_store::KeyValueStore,
    resp::RespValue,
};

pub struct KeysArguments {
    pub key: String,
}

impl KeysArguments {
    pub fn parse(arguments: Vec<String>) -> Result<Self, CommandError> {
        if arguments.len() != 1 {
            return Err(CommandError::InvalidKeysCommand);
        }

        Ok(KeysArguments {
            key: arguments[0].clone(),
        })
    }
}

pub async fn keys(
    store: Arc<Mutex<KeyValueStore>>,
    arguments: Vec<String>,
) -> Result<CommandResult, CommandError> {
    let keys_arguments = KeysArguments::parse(arguments)?;
    let store_guard = store.lock().await;

    let glob = Glob::new(&keys_arguments.key)
        .map_err(|e| CommandError::InvalidGlobPattern(e.to_string()))?
        .compile_matcher();
    let mut response = Vec::new();

    for key in store_guard.keys() {
        if glob.is_match(key) {
            response.push(RespValue::BulkString(key.to_string()));
        }
    }

    Ok(CommandResult::Response(RespValue::Array(response).encode()))
}
