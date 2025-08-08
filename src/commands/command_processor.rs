use std::sync::Arc;

use tokio::sync::Mutex;

use crate::{
    commands::{
        blpop::blpop,
        command_error::CommandError,
        echo::echo,
        get::get,
        incr::incr,
        llen::llen,
        lpop::lpop,
        lrange::lrange,
        ping::ping,
        rpush_and_lpush::{lpush, rpush},
        set::set,
        type_command::type_command,
        xadd::xadd,
        xrange::xrange,
        xread::xread,
    },
    key_value_store::KeyValueStore,
    resp::RespValue,
    state::State,
};

/// Represents a parsed Redis command with its name and arguments.
///
/// This struct is responsible for parsing RESP arrays into Redis commands
/// and dispatching them to the appropriate command handlers.
#[derive(Debug)]
pub struct CommandProcessor {
    /// The name of the Redis command (e.g., "GET", "SET", "PING")
    pub name: String,
    /// The arguments passed to the command
    pub arguments: Vec<String>,
}

impl CommandProcessor {
    /// Creates a new CommandProcessor from a RESP array.
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
    /// * `Ok(CommandProcessor)` - Successfully parsed command
    /// * `Err(CommandError)` - If input is invalid or cannot be parsed
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let resp_array = vec![RespValue::Array(vec![
    ///     RespValue::BulkString("GET".to_string()),
    ///     RespValue::BulkString("mykey".to_string())
    /// ])];
    /// let processor = CommandProcessor::new(resp_array)?;
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

    /// Executes the parsed Redis command by dispatching to the appropriate handler.
    ///
    /// This method matches the command name against known Redis commands and
    /// calls the corresponding handler function with the provided arguments.
    ///
    /// # Arguments
    ///
    /// * `server_address` - The address of the current Redis server instance
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
        server_address: String,
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
            "BLPOP" => blpop(server_address, store, state, self.arguments.clone()).await,
            "TYPE" => type_command(store, self.arguments.clone()).await,
            "XADD" => xadd(store, state, self.arguments.clone()).await,
            "XRANGE" => xrange(store, self.arguments.clone()).await,
            "XREAD" => xread(server_address, store, state, self.arguments.clone()).await,
            "INCR" => incr(store, self.arguments.clone()).await,
            _ => Err(CommandError::InvalidCommand),
        }
    }
}
