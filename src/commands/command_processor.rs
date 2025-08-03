use std::sync::Arc;

use tokio::sync::Mutex;

use crate::{
    commands::{
        blpop::blpop,
        command_error::CommandError,
        echo::echo,
        get::get,
        llen::llen,
        lpop::lpop,
        lrange::lrange,
        rpush_and_lpush::{lpush, rpush},
        set::set,
        type_command::type_command,
        xadd::xadd,
        xrange::xrange,
    },
    key_value_store::KeyValueStore,
    resp::RespValue,
    state::State,
};

#[derive(Debug)]
pub struct CommandProcessor {
    pub name: String,
    pub arguments: Vec<String>,
}

impl CommandProcessor {
    pub fn new(input: Vec<RespValue>) -> Result<Self, CommandError> {
        if input.len() != 1 {
            return Err(CommandError::InvalidCommand);
        }

        match input.get(0) {
            Some(RespValue::Array(elements)) => {
                let name = match elements.get(0) {
                    Some(RespValue::BulkString(s)) => Ok(s.to_string()),
                    _ => Err(CommandError::InvalidCommandArgument),
                }?
                .to_uppercase();

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
            _ => return Err(CommandError::InvalidCommand),
        }
    }

    pub async fn handle_command(
        &self,
        server_address: String,
        store: &mut Arc<Mutex<KeyValueStore>>,
        state: &mut Arc<Mutex<State>>,
    ) -> Result<String, CommandError> {
        match self.name.as_str() {
            "PING" => Ok(RespValue::SimpleString("PONG".to_string()).encode()),
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
            "XADD" => xadd(store, self.arguments.clone()).await,
            "XRANGE" => xrange(store, self.arguments.clone()).await,
            _ => Err(CommandError::InvalidCommand),
        }
    }
}
