use std::sync::Arc;

use tokio::sync::{Mutex, RwLock};

use crate::{
    commands::{
        blpop::{BlpopArguments, blpop},
        command_error::CommandError,
        echo::{EchoArguments, echo},
        get::{GetArguments, get},
        incr::{IncrArguments, incr},
        info::{InfoArguments, info},
        llen::{LlenArguments, llen},
        lpop::{LpopArguments, lpop},
        lrange::{LrangeArguments, lrange},
        ping::{PingArguments, ping},
        psync::{PsyncArguments, psync},
        replconf::{ReplconfArguments, replconf},
        rpush_and_lpush::{PushArrayOperations, lpush, rpush},
        set::{SetArguments, set},
        transactions::{DiscardArguments, ExecArguments, MultiArguments, discard, exec, multi},
        type_command::{TypeArguments, type_command},
        wait::{WaitArguments, wait},
        xadd::{XaddArguments, xadd},
        xrange::{XrangeArguments, xrange},
        xread::{XreadArguments, xread},
    },
    key_value_store::KeyValueStore,
    resp::RespValue,
    server::RedisServer,
    state::State,
};

#[derive(Debug)]
pub enum CommandResult {
    NoResponse,
    Response(String),
    Sync(String),
    Batch(Vec<CommandHandler>),
}

/// Represents a parsed Redis command with its name and arguments.
///
/// This struct is responsible for parsing RESP arrays into Redis commands
/// and dispatching them to the appropriate command handlers.
#[derive(Debug, PartialEq, Clone)]
pub struct CommandHandler {
    /// The name of the Redis command (e.g., "GET", "SET", "PING")
    pub name: String,
    /// The arguments passed to the command
    pub arguments: Vec<String>,
    pub input: RespValue,
}

impl CommandHandler {
    /// Creates a new CommandHandler from a RESP array.
    ///
    /// Parses a RESP array containing a Redis command and its arguments.
    /// The first element should be the command name, and subsequent elements
    /// should be the command arguments.
    ///
    /// # Arguments
    ///
    /// * `input` - A vector containing exactly one RESP array value
    ///
    /// # Returns
    ///
    /// * `Ok(CommandHandler)` - Successfully parsed command
    /// * `Err(CommandError)` - If input is invalid or cannot be parsed
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let resp_array = vec![RespValue::Array(vec![
    ///     RespValue::BulkString("GET".to_string()),
    ///     RespValue::BulkString("mykey".to_string())
    /// ])];
    /// let processor = CommandHandler::new(resp_array)?;
    /// ```
    pub fn new(input: RespValue) -> Result<Self, CommandError> {
        let RespValue::Array(elements) = &input else {
            return Err(CommandError::InvalidCommand);
        };

        let name = match elements.get(0) {
            Some(RespValue::BulkString(s)) => Ok(s.to_string().to_uppercase()),
            _ => Err(CommandError::InvalidCommandArgument),
        }?;

        let mut arguments: Vec<String> = Vec::new();

        for element in elements[1..].iter() {
            let arg = match element {
                RespValue::BulkString(s) => Ok(s.to_string()),
                _ => Err(CommandError::InvalidCommand),
            }?;
            arguments.push(arg);
        }

        Ok(Self {
            name,
            arguments,
            input,
        })
    }

    /// Validates the arguments for the parsed Redis command.
    ///
    /// This method checks whether the arguments provided to the command match the expected
    /// format for that specific Redis command. It delegates validation to the corresponding
    /// argument parser for each supported command. If the arguments are invalid, it returns
    /// the specific `CommandError` produced by the parser. If the arguments are valid, it returns `None`.
    ///
    /// # Returns
    ///
    /// * `Some(CommandError)` - If the arguments are invalid for the command
    /// * `None` - If the arguments are valid
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let processor = CommandHandler {
    ///     name: "GET".to_string(),
    ///     arguments: vec!["mykey".to_string()],
    /// };
    /// assert_eq!(processor.validate_command_arguments().is_none(), true);
    ///
    /// let bad_processor = CommandHandler {
    ///     name: "GET".to_string(),
    ///     arguments: vec![],
    /// };
    /// assert!(bad_processor.validate_command_arguments().is_some());
    /// ```
    pub fn validate_command_arguments(&self) -> Option<CommandError> {
        match self.name.as_str() {
            "PING" => PingArguments::parse(self.arguments.clone()).err(),
            "ECHO" => EchoArguments::parse(self.arguments.clone()).err(),
            "GET" => GetArguments::parse(self.arguments.clone()).err(),
            "SET" => SetArguments::parse(self.arguments.clone()).err(),
            "RPUSH" => PushArrayOperations::parse(self.arguments.clone(), false).err(),
            "LPUSH" => PushArrayOperations::parse(self.arguments.clone(), true).err(),
            "LRANGE" => LrangeArguments::parse(self.arguments.clone()).err(),
            "LLEN" => LlenArguments::parse(self.arguments.clone()).err(),
            "LPOP" => LpopArguments::parse(self.arguments.clone()).err(),
            "BLPOP" => BlpopArguments::parse(self.arguments.clone()).err(),
            "TYPE" => TypeArguments::parse(self.arguments.clone()).err(),
            "XADD" => XaddArguments::parse(self.arguments.clone()).err(),
            "XRANGE" => XrangeArguments::parse(self.arguments.clone()).err(),
            "XREAD" => XreadArguments::parse(self.arguments.clone()).err(),
            "INCR" => IncrArguments::parse(self.arguments.clone()).err(),
            "MULTI" => MultiArguments::parse(self.arguments.clone()).err(),
            "EXEC" => ExecArguments::parse(self.arguments.clone()).err(),
            "DISCARD" => DiscardArguments::parse(self.arguments.clone()).err(),
            "INFO" => InfoArguments::parse(self.arguments.clone()).err(),
            "REPLCONF" => ReplconfArguments::parse(self.arguments.clone()).err(),
            "PSYNC" => PsyncArguments::parse(self.arguments.clone()).err(),
            "WAIT" => WaitArguments::parse(self.arguments.clone()).err(),
            _ => Some(CommandError::InvalidCommand),
        }
    }

    /// Executes the parsed Redis command by dispatching to the appropriate handler.
    ///
    /// This method matches the command name against known Redis commands and
    /// calls the corresponding handler function with the provided arguments.
    ///
    /// # Arguments
    ///
    /// * `client_address` - The address of the current client server instance
    /// * `store` - A thread-safe reference to the key-value store
    /// * `state` - A thread-safe reference to the server state (for blocking operations)
    ///
    /// # Returns
    ///
    /// * `Ok(String)` - RESP-encoded response from the command handler
    /// * `Err(CommandError::InvalidCommand)` - If the command is not recognized
    /// * `Err(CommandError)` - Various errors from individual command handlers
    ///
    /// # Supported Commands
    ///
    /// - Basic: PING, ECHO
    /// - String: GET, SET, INCR
    /// - List: RPUSH, LPUSH, LRANGE, LLEN, LPOP, BLPOP
    /// - Stream: XADD, XRANGE, XREAD
    /// - Transactions: MULTI, EXEC, DISCARD
    /// - Utility: TYPE, INFO
    /// - Replication: REPLCONF, PSYNC, WAIT
    async fn handle_command(
        &self,
        server: Arc<RwLock<RedisServer>>,
        client_address: &str,
        store: Arc<Mutex<KeyValueStore>>,
        state: Arc<Mutex<State>>,
    ) -> Result<CommandResult, CommandError> {
        match self.name.as_str() {
            "PING" => ping(self.arguments.clone()),
            "ECHO" => echo(self.arguments.clone()),
            "GET" => get(store, self.arguments.clone()).await,
            "SET" => {
                match set(store, self.arguments.clone()).await {
                    Ok(response) => {
                        let mut server_guard = server.write().await;
                        server_guard
                            .update_replication_offset(self.input.clone())
                            .await;

                        return Ok(response);
                    }
                    Err(err) => return Err(err),
                };
            }
            "RPUSH" => {
                match rpush(store, state, self.arguments.clone()).await {
                    Ok(response) => {
                        let mut server_guard = server.write().await;
                        server_guard
                            .update_replication_offset(self.input.clone())
                            .await;

                        return Ok(response);
                    }
                    Err(err) => return Err(err),
                };
            }
            "LPUSH" => {
                match lpush(store, state, self.arguments.clone()).await {
                    Ok(response) => {
                        let mut server_guard = server.write().await;
                        server_guard
                            .update_replication_offset(self.input.clone())
                            .await;

                        return Ok(response);
                    }
                    Err(err) => return Err(err),
                };
            }
            "LRANGE" => lrange(store, self.arguments.clone()).await,
            "LLEN" => llen(store, self.arguments.clone()).await,
            "LPOP" => {
                match lpop(store, self.arguments.clone()).await {
                    Ok(response) => {
                        let mut server_guard = server.write().await;
                        server_guard
                            .update_replication_offset(self.input.clone())
                            .await;

                        return Ok(response);
                    }
                    Err(err) => return Err(err),
                };
            }
            "BLPOP" => {
                match blpop(client_address, store, state, self.arguments.clone()).await {
                    Ok(response) => {
                        let mut server_guard = server.write().await;
                        server_guard
                            .update_replication_offset(self.input.clone())
                            .await;

                        return Ok(response);
                    }
                    Err(err) => return Err(err),
                };
            }
            "TYPE" => type_command(store, self.arguments.clone()).await,
            "XADD" => {
                match xadd(store, state, self.arguments.clone()).await {
                    Ok(response) => {
                        let mut server_guard = server.write().await;
                        server_guard
                            .update_replication_offset(self.input.clone())
                            .await;

                        return Ok(response);
                    }
                    Err(err) => return Err(err),
                };
            }
            "XRANGE" => xrange(store, self.arguments.clone()).await,
            "XREAD" => xread(client_address, store, state, self.arguments.clone()).await,
            "INCR" => {
                match incr(store, self.arguments.clone()).await {
                    Ok(response) => {
                        let mut server_guard = server.write().await;
                        server_guard
                            .update_replication_offset(self.input.clone())
                            .await;

                        return Ok(response);
                    }
                    Err(err) => return Err(err),
                };
            }
            "MULTI" => multi(client_address, state, self.arguments.clone()).await,
            "EXEC" => exec(client_address, state, self.arguments.clone()).await,
            "DISCARD" => discard(client_address, state, self.arguments.clone()).await,
            "INFO" => info(server, self.arguments.clone()).await,
            "REPLCONF" => replconf(client_address, server, self.arguments.clone()).await,
            "PSYNC" => psync(server, self.arguments.clone()).await,
            "WAIT" => wait(server, self.arguments.clone()).await,
            _ => Err(CommandError::InvalidCommand),
        }
    }

    async fn queue_command_if_in_transaction(
        &self,
        client_address: &str,
        state: Arc<Mutex<State>>,
    ) -> Result<Option<String>, CommandError> {
        let transaction_commands = Vec::from(["MULTI", "EXEC", "DISCARD"]);

        if transaction_commands.contains(&self.name.as_str()) {
            return Ok(None);
        }

        let mut state_guard = state.lock().await;

        let Some(_) = state_guard.get_transaction(&client_address) else {
            return Ok(None);
        };

        match self.validate_command_arguments() {
            Some(err) => return Err(err),
            None => {
                state_guard.add_to_transaction(client_address.to_string(), self.clone())?;
            }
        }

        Ok(Some(RespValue::SimpleString("QUEUED".to_string()).encode()))
    }

    pub async fn handle_command_for_master_server(
        &self,
        server: Arc<RwLock<RedisServer>>,
        client_address: &str,
        store: Arc<Mutex<KeyValueStore>>,
        state: Arc<Mutex<State>>,
    ) -> Result<CommandResult, CommandError> {
        match self
            .queue_command_if_in_transaction(client_address, Arc::clone(&state))
            .await?
        {
            Some(response) => {
                return Ok(CommandResult::Response(response));
            }
            None => (),
        }

        let command_result = self
            .handle_command(Arc::clone(&server), client_address, store, state)
            .await?;

        {
            let server_guard = server.read().await;
            server_guard
                .should_replicate_write_command(self.input.clone(), self.name.as_str())
                .await
                .unwrap();
        }

        Ok(command_result)
    }

    pub async fn handle_command_for_replica_master_connection(
        &self,
        server: Arc<RwLock<RedisServer>>,
        client_address: &str,
        store: Arc<Mutex<KeyValueStore>>,
        state: Arc<Mutex<State>>,
    ) -> Result<CommandResult, CommandError> {
        match self
            .queue_command_if_in_transaction(client_address, Arc::clone(&state))
            .await?
        {
            Some(response) => {
                return Ok(CommandResult::Response(response));
            }
            None => (),
        }

        let command_result = self
            .handle_command(Arc::clone(&server), client_address, store, state)
            .await?;

        // Hack so that codecrafters test runs successfully
        if self.name.as_str() == "PING" || self.name.as_str() == "REPLCONF" {
            let mut server_guard = server.write().await;
            server_guard
                .update_replication_offset(self.input.clone())
                .await;
        }

        match command_result {
            CommandResult::NoResponse => Ok(CommandResult::NoResponse),
            CommandResult::Response(response) => {
                if self.name.as_str() == "REPLCONF" {
                    return Ok(CommandResult::Response(response));
                }

                Ok(CommandResult::NoResponse)
            }
            CommandResult::Batch(commands) => Ok(CommandResult::Batch(commands)),
            CommandResult::Sync(response) => Ok(CommandResult::Sync(response)),
        }
    }

    pub async fn handle_command_for_replica_server(
        &self,
        server: Arc<RwLock<RedisServer>>,
        client_address: &str,
        store: Arc<Mutex<KeyValueStore>>,
        state: Arc<Mutex<State>>,
    ) -> Result<CommandResult, CommandError> {
        match self.name.as_str() {
            "PING" => ping(self.arguments.clone()),
            "ECHO" => echo(self.arguments.clone()),
            "GET" => get(store, self.arguments.clone()).await,
            "LRANGE" => lrange(store, self.arguments.clone()).await,
            "LLEN" => llen(store, self.arguments.clone()).await,
            "TYPE" => type_command(store, self.arguments.clone()).await,
            "XRANGE" => xrange(store, self.arguments.clone()).await,
            "XREAD" => xread(client_address, store, state, self.arguments.clone()).await,
            "INFO" => info(server, self.arguments.clone()).await,
            _ => Err(CommandError::ReplicaReadOnlyCommands),
        }
    }
}
