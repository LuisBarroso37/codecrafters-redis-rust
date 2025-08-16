use std::sync::Arc;

use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::{
    fs::File,
    io::{AsyncReadExt, BufReader},
    sync::{Mutex, RwLock},
};

use crate::{
    commands::{CommandError, CommandHandler},
    key_value_store::KeyValueStore,
    resp::RespValue,
    server::RedisServer,
    state::{State, StateError},
};

/// Represents errors that can occur during command dispatching and transaction handling.
///
/// This enum covers errors such as executing `EXEC` or `DISCARD` without a transaction,
/// transaction state errors, and invalid commands in the transaction queue.
#[derive(Error, Debug, PartialEq)]
pub enum DispatchError {
    /// Attempted to execute `EXEC` without a preceding `MULTI`.
    #[error("Executed EXEC without MULTI")]
    ExecWithoutMulti,
    /// Error occurred in transaction state management.
    #[error("Transaction error")]
    DispatchError(#[from] StateError),
    /// Invalid command was added to the transaction queue.
    #[error("Invalid command in transaction queue")]
    InvalidQueueCommand(#[from] CommandError),
    /// Attempted to execute `DISCARD` without a preceding `MULTI`.
    #[error("Executed DISCARD without MULTI")]
    DiscardWithoutMulti,
}

impl DispatchError {
    /// Converts the error into a RESP-encoded error string suitable for client responses.
    pub fn as_string(&self) -> String {
        match self {
            DispatchError::ExecWithoutMulti => {
                RespValue::Error("ERR EXEC without MULTI".to_string()).encode()
            }
            DispatchError::DispatchError(err) => {
                RespValue::Error(format!("ERR {}", err.as_string())).encode()
            }
            DispatchError::InvalidQueueCommand(err) => {
                RespValue::Error(format!("ERR {}", err.as_string())).encode()
            }
            DispatchError::DiscardWithoutMulti => {
                RespValue::Error("ERR DISCARD without MULTI".to_string()).encode()
            }
        }
    }
}

pub enum ExtraAction {
    SendRdbFile,
    SendWriteCommand,
}

pub async fn handle_extra_action(
    parsed_input: Vec<RespValue>,
    client_address: &str,
    writer: Arc<RwLock<OwnedWriteHalf>>,
    server: Arc<RwLock<RedisServer>>,
    action: ExtraAction,
) -> tokio::io::Result<()> {
    match action {
        ExtraAction::SendRdbFile => {
            // Stream RDB file instead of loading it all into memory
            let file = File::open("empty.rdb").await?;
            let file_size = file.metadata().await?.len();

            // First send the bulk string header
            let header = format!("${}\r\n", file_size);

            let mut writer_guard = writer.write().await;
            writer_guard.write_all(header.as_bytes()).await?;

            // Stream the file contents in chunks
            let mut reader = BufReader::new(file);
            let mut buffer = [0u8; 4096]; // 4KB chunks

            loop {
                match reader.read(&mut buffer).await {
                    Ok(0) => break, // EOF
                    Ok(n) => {
                        writer_guard.write_all(&buffer[..n]).await?;
                    }
                    Err(e) => {
                        eprintln!("Error streaming RDB file: {}", e);
                        break;
                    }
                }
            }

            writer_guard.flush().await?;
            drop(writer_guard); // Release the lock

            // Add replica to replication list after successful RDB streaming
            let mut server_guard = server.write().await;
            if let Some(replicas) = &mut server_guard.replicas {
                replicas.insert(client_address.to_string(), writer);
            }

            Ok(())
        }
        ExtraAction::SendWriteCommand => {
            let server_guard = server.read().await;

            if let Some(ref replicas) = server_guard.replicas {
                for replica in replicas.values() {
                    let mut replica_guard = replica.write().await;
                    replica_guard
                        .write_all(parsed_input[0].encode().as_bytes())
                        .await?;
                    replica_guard.flush().await?;
                }
            }

            Ok(())
        }
    }
}

/// Represents the result of dispatching a command.
///
/// This enum distinguishes between immediate responses (such as "OK" or "QUEUED"),
/// execution of a single command, and execution of a batch of commands in a transaction.
#[derive(Debug, PartialEq)]
pub enum DispatchResult {
    /// An immediate response string to be sent to the client.
    ImmediateResponse(String),
    /// A batch of commands to be executed as part of a transaction.
    ExecuteTransactionCommands(Vec<CommandHandler>),
    /// A single command to be executed immediately.
    ExecuteSingleCommand(CommandHandler),
}

impl DispatchResult {
    /// Handles the result of command dispatching and produces a RESP-encoded response string.
    ///
    /// This method executes any commands as needed and collects their responses.
    ///
    /// # Arguments
    ///
    /// * `client_address` - The address of the client instance (used for transaction context)
    /// * `store` - A mutable reference to the key-value store
    /// * `state` - A mutable reference to the server state
    ///
    /// # Returns
    ///
    /// * `String` - A RESP-encoded response to be sent to the client
    pub async fn handle_dispatch_result(
        &self,
        server: Arc<RwLock<RedisServer>>,
        client_address: &str,
        store: Arc<Mutex<KeyValueStore>>,
        state: Arc<Mutex<State>>,
    ) -> (String, Option<ExtraAction>) {
        match self {
            DispatchResult::ImmediateResponse(value) => (value.clone(), None),
            DispatchResult::ExecuteSingleCommand(command) => {
                let mut extra_action = None;

                if command.name == "PSYNC" {
                    extra_action = Some(ExtraAction::SendRdbFile);
                } else if Vec::from(["SET", "RPUSH", "LPUSH", "INCR", "LPOP", "XADD"])
                    .contains(&command.name.as_str())
                {
                    extra_action = Some(ExtraAction::SendWriteCommand);
                }

                match command
                    .handle_command(
                        Arc::clone(&server),
                        client_address,
                        Arc::clone(&store),
                        Arc::clone(&state),
                    )
                    .await
                {
                    Ok(resp) => (resp, extra_action),
                    Err(e) => (e.as_string(), None),
                }
            }
            DispatchResult::ExecuteTransactionCommands(commands) => {
                let mut responses = Vec::with_capacity(commands.len() + 1);
                responses.push(format!("*{}\r\n", commands.len()));

                for cmd in commands {
                    match cmd
                        .handle_command(
                            Arc::clone(&server),
                            client_address,
                            Arc::clone(&store),
                            Arc::clone(&state),
                        )
                        .await
                    {
                        Ok(resp) => {
                            responses.push(resp);
                        }
                        Err(e) => {
                            responses.push(e.as_string());
                        }
                    }
                }

                (responses.join(""), None)
            }
        }
    }
}

/// The main dispatcher responsible for handling and routing Redis commands.
///
/// This struct manages the execution of commands, including transaction state,
/// queuing, and error handling. It supports both transactional and non-transactional
/// command flows.
pub struct CommandDispatcher {
    /// The address of the server instance (used for transaction context).
    pub client_address: String,
    /// Shared state for managing transactions and blocking operations.
    pub state: Arc<Mutex<State>>,
}

impl CommandDispatcher {
    /// Creates a new `CommandDispatcher` with the given server address and state.
    pub fn new(client_address: &str, state: Arc<Mutex<State>>) -> Self {
        CommandDispatcher {
            client_address: client_address.to_string(),
            state,
        }
    }

    /// Dispatches a command for execution, handling transactional and non-transactional logic.
    ///
    /// This method determines whether to queue the command (if inside a transaction),
    /// execute it immediately, or return an immediate response (such as "OK" or "QUEUED").
    ///
    /// # Arguments
    ///
    /// * `command` - The command to be dispatched and executed
    ///
    /// # Returns
    ///
    /// * `Ok(DispatchResult)` - The result of dispatching the command
    /// * `Err(DispatchError)` - If an error occurs during dispatch or transaction handling
    ///
    /// # Transactional Behavior
    ///
    /// - `MULTI`: Starts a new transaction and returns "OK"
    /// - `EXEC`: Executes all queued commands in the transaction
    /// - `DISCARD`: Discards the transaction and returns "OK"
    /// - Other commands: Queued if inside a transaction, executed immediately otherwise
    pub async fn dispatch_command(
        &self,
        command: CommandHandler,
    ) -> Result<DispatchResult, DispatchError> {
        match command.name.as_str() {
            "MULTI" => {
                let mut state_guard = self.state.lock().await;
                state_guard.start_transaction(self.client_address.clone())?;

                Ok(DispatchResult::ImmediateResponse(
                    RespValue::SimpleString("OK".to_string()).encode(),
                ))
            }
            "EXEC" => {
                let mut state_guard = self.state.lock().await;

                let Ok(transaction) = state_guard.remove_transaction(self.client_address.clone())
                else {
                    return Err(DispatchError::ExecWithoutMulti);
                };

                if transaction.is_empty() {
                    Ok(DispatchResult::ImmediateResponse(
                        RespValue::Array(Vec::new()).encode(),
                    ))
                } else {
                    Ok(DispatchResult::ExecuteTransactionCommands(transaction))
                }
            }
            "DISCARD" => {
                let mut state_guard = self.state.lock().await;

                let Ok(_) = state_guard.remove_transaction(self.client_address.clone()) else {
                    return Err(DispatchError::DiscardWithoutMulti);
                };

                Ok(DispatchResult::ImmediateResponse(
                    RespValue::SimpleString("OK".to_string()).encode(),
                ))
            }
            _ => {
                let mut state_guard = self.state.lock().await;

                let Some(_) = state_guard.get_transaction(&self.client_address) else {
                    return Ok(DispatchResult::ExecuteSingleCommand(command));
                };

                match command.validate_command_arguments() {
                    Some(err) => return Err(DispatchError::InvalidQueueCommand(err)),
                    None => {
                        state_guard.add_to_transaction(self.client_address.clone(), command)?;
                    }
                }

                Ok(DispatchResult::ImmediateResponse(
                    RespValue::SimpleString("QUEUED".to_string()).encode(),
                ))
            }
        }
    }
}
