use std::sync::Arc;

use tokio::sync::{Mutex, RwLock};

use crate::{
    commands::{CommandError, CommandHandler, command_handler::CommandResult},
    key_value_store::KeyValueStore,
    resp::RespValue,
    server::RedisServer,
    state::State,
};

pub struct ExecArguments;

impl ExecArguments {
    pub fn parse(arguments: Vec<String>) -> Result<Self, CommandError> {
        if !arguments.is_empty() {
            return Err(CommandError::InvalidExecCommand);
        }

        Ok(Self)
    }
}

pub async fn exec(
    client_address: &str,
    state: Arc<Mutex<State>>,
    arguments: Vec<String>,
) -> Result<CommandResult, CommandError> {
    ExecArguments::parse(arguments)?;

    let mut state_guard = state.lock().await;

    let Ok(transaction) = state_guard.remove_transaction(client_address.to_string()) else {
        return Err(CommandError::ExecWithoutMulti);
    };

    if transaction.is_empty() {
        Ok(CommandResult::Response(
            RespValue::Array(Vec::new()).encode(),
        ))
    } else {
        Ok(CommandResult::Batch(transaction))
    }
}

pub async fn run_transaction_commands_for_master_server(
    client_address: &str,
    server: Arc<RwLock<RedisServer>>,
    store: Arc<Mutex<KeyValueStore>>,
    state: Arc<Mutex<State>>,
    commands: Vec<CommandHandler>,
) -> Result<String, CommandError> {
    let mut responses = Vec::with_capacity(commands.len() + 1);
    responses.push(format!("*{}\r\n", commands.len()));

    for cmd in commands {
        match cmd
            .handle_command_for_master_server(
                &client_address,
                Arc::clone(&server),
                Arc::clone(&store),
                Arc::clone(&state),
            )
            .await
        {
            Ok(resp) => {
                match resp {
                    CommandResult::Response(data) => {
                        responses.push(data);
                    }
                    _ => (), // ignore other command results
                }
            }
            Err(e) => {
                responses.push(e.as_string());
            }
        }
    }

    Ok(responses.join(""))
}

pub async fn run_transaction_commands_for_replica_server(
    client_address: &str,
    server: Arc<RwLock<RedisServer>>,
    store: Arc<Mutex<KeyValueStore>>,
    state: Arc<Mutex<State>>,
    commands: Vec<CommandHandler>,
) -> Result<String, CommandError> {
    let mut responses = Vec::with_capacity(commands.len() + 1);
    responses.push(format!("*{}\r\n", commands.len()));

    for cmd in commands {
        match cmd
            .handle_command_for_replica_master_connection(
                &client_address,
                Arc::clone(&server),
                Arc::clone(&store),
                Arc::clone(&state),
            )
            .await
        {
            Ok(resp) => {
                match resp {
                    CommandResult::Response(data) => {
                        responses.push(data);
                    }
                    _ => (), // ignore other command results
                }
            }
            Err(e) => {
                responses.push(e.as_string());
            }
        }
    }

    Ok(responses.join(""))
}
