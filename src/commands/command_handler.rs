use std::sync::Arc;

use tokio::sync::{Mutex, RwLock};

use crate::{
    commands::{
        blpop::{BlpopArguments, blpop},
        command_error::CommandError,
        echo::{EchoArguments, echo},
        get::{GetArguments, get},
        incr::{IncrArguments, incr},
        info::info,
        llen::{LlenArguments, llen},
        lpop::{LpopArguments, lpop},
        lrange::{LrangeArguments, lrange},
        ping::{PingArguments, ping},
        rpush_and_lpush::{PushArrayOperations, lpush, rpush},
        set::{SetArguments, set},
        type_command::{TypeArguments, type_command},
        xadd::{XaddArguments, xadd},
        xrange::{XrangeArguments, xrange},
        xread::{XreadArguments, xread},
    },
    key_value_store::KeyValueStore,
    resp::RespValue,
    server::RedisServer,
    state::State,
};

/// Represents a parsed Redis command with its name and arguments.
///
/// This struct is responsible for parsing RESP arrays into Redis commands
/// and dispatching them to the appropriate command handlers.
#[derive(Debug, PartialEq)]
pub struct CommandHandler {
    /// The name of the Redis command (e.g., "GET", "SET", "PING")
    pub name: String,
    /// The arguments passed to the command
    pub arguments: Vec<String>,
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
    pub fn new(input: Vec<RespValue>) -> Result<Self, CommandError> {
        if input.len() != 1 {
            return Err(CommandError::InvalidCommand);
        }

        let Some(RespValue::Array(elements)) = input.get(0) else {
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

        Ok(Self { name, arguments })
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
    /// - String: GET, SET
    /// - List: RPUSH, LPUSH, LRANGE, LLEN, LPOP, BLPOP
    /// - Stream: XADD, XRANGE, XREAD
    /// - Utility: TYPE
    pub async fn handle_command(
        &self,
        server: &Arc<RwLock<RedisServer>>,
        client_address: String,
        store: &mut Arc<Mutex<KeyValueStore>>,
        state: &mut Arc<Mutex<State>>,
    ) -> Result<String, CommandError> {
        match self.name.as_str() {
            "PING" => ping(self.arguments.clone()),
            "ECHO" => echo(self.arguments.clone()),
            "GET" => get(store, self.arguments.clone()).await,
            "SET" => set(store, self.arguments.clone()).await,
            "RPUSH" => rpush(store, state, self.arguments.clone()).await,
            "LPUSH" => lpush(store, state, self.arguments.clone()).await,
            "LRANGE" => lrange(store, self.arguments.clone()).await,
            "LLEN" => llen(store, self.arguments.clone()).await,
            "LPOP" => lpop(store, self.arguments.clone()).await,
            "BLPOP" => blpop(client_address, store, state, self.arguments.clone()).await,
            "TYPE" => type_command(store, self.arguments.clone()).await,
            "XADD" => xadd(store, state, self.arguments.clone()).await,
            "XRANGE" => xrange(store, self.arguments.clone()).await,
            "XREAD" => xread(client_address, store, state, self.arguments.clone()).await,
            "INCR" => incr(store, self.arguments.clone()).await,
            "INFO" => info(server, self.arguments.clone()).await,
            _ => Err(CommandError::InvalidCommand),
        }
    }
}
