use std::{collections::HashMap, sync::Arc, time::Duration};

use codecrafters_redis::{
    commands::{CommandDispatcher, CommandError, CommandHandler, DispatchError, DispatchResult},
    key_value_store::KeyValueStore,
    resp::RespValue,
    server::{RedisRole, RedisServer},
    state::State,
};
use tokio::{
    sync::{Mutex, RwLock},
    time::timeout,
};

/// Test utilities for simplifying Redis command tests
pub struct TestUtils;

/// Test environment containing store and state
pub struct TestEnv {
    pub store: Arc<Mutex<KeyValueStore>>,
    pub state: Arc<Mutex<State>>,
    pub server: Arc<RwLock<RedisServer>>,
}

impl TestEnv {
    /// Create a new test environment with a master server
    pub fn new_master_server() -> Self {
        Self {
            store: Arc::new(Mutex::new(HashMap::new())),
            state: Arc::new(Mutex::new(State::new())),
            server: Arc::new(RwLock::new(RedisServer {
                port: 6379,
                role: RedisRole::Master,
                repl_id: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
                repl_offset: 0,
                replicas: Some(HashMap::new()),
                write_commands: vec!["SET", "RPUSH", "LPUSH", "INCR", "LPOP", "XADD"],
            })),
        }
    }

    /// Create a new test environment with a replica server
    pub fn new_replica_server(replica_port: u32) -> Self {
        Self {
            store: Arc::new(Mutex::new(HashMap::new())),
            state: Arc::new(Mutex::new(State::new())),
            server: Arc::new(RwLock::new(RedisServer {
                port: replica_port,
                role: RedisRole::Replica(("127.0.0.1".to_string(), 6379)),
                repl_id: "c673350b6868f3661bd1231ad1b5389310d0a201".to_string(),
                repl_offset: 0,
                replicas: None,
                write_commands: vec!["SET", "RPUSH", "LPUSH", "INCR", "LPOP", "XADD"],
            })),
        }
    }

    /// Clone the test environment
    pub fn clone(&self) -> Self {
        Self {
            store: Arc::clone(&self.store),
            state: Arc::clone(&self.state),
            server: Arc::clone(&self.server),
        }
    }

    /// Clone the environment for use in async tasks
    fn clone_env(
        &self,
    ) -> (
        Arc<Mutex<KeyValueStore>>,
        Arc<Mutex<State>>,
        Arc<RwLock<RedisServer>>,
    ) {
        (
            Arc::clone(&self.store),
            Arc::clone(&self.state),
            Arc::clone(&self.server),
        )
    }

    /// Execute a command and return the result
    async fn exec_command(
        &mut self,
        command: RespValue,
        client_address: &str,
    ) -> Result<String, CommandError> {
        let command_handler = CommandHandler::new(&command)?;

        command_handler
            .handle_command(
                Arc::clone(&self.server),
                client_address,
                Arc::clone(&self.store),
                Arc::clone(&self.state),
            )
            .await
    }

    /// Execute a command and assert it succeeds with expected result
    pub async fn exec_command_ok(
        &mut self,
        command: RespValue,
        client_address: &str,
        expected: &str,
    ) {
        let result = self.exec_command(command, client_address).await;
        assert_eq!(result, Ok(expected.to_string()));
    }

    /// Execute a command and assert it fails
    pub async fn exec_command_err(
        &mut self,
        command: RespValue,
        client_address: &str,
        expected_error: CommandError,
    ) {
        let result = self.exec_command(command, client_address).await;
        assert!(result.is_err());
        assert_eq!(result, Err(expected_error));
    }

    /// Execute a transaction command and return the result
    async fn exec_transaction_command(
        &mut self,
        command: CommandHandler,
        client_address: &str,
    ) -> Result<DispatchResult, DispatchError> {
        let command_dispatcher = CommandDispatcher::new(client_address, Arc::clone(&self.state));

        command_dispatcher.dispatch_command(command).await
    }

    /// Execute a transaction command and assert it succeeds with an immediate response
    pub async fn exec_transaction_immediate_response(
        &mut self,
        command: RespValue,
        client_address: &str,
        expected: &str,
    ) {
        let command_handler = CommandHandler::new(&command).unwrap();
        let result = self
            .exec_transaction_command(command_handler, client_address)
            .await;
        assert_eq!(result.is_ok(), true);

        match result.unwrap() {
            DispatchResult::ImmediateResponse(resp) => {
                assert_eq!(resp, expected.to_string());
            }
            _ => panic!("Expected immediate response"),
        }
    }

    /// Execute a transaction command and assert it returns the expected commands
    pub async fn exec_transaction_expected_commands(
        &mut self,
        command: RespValue,
        client_address: &str,
        expected: &[CommandHandler],
    ) {
        let command_handler = CommandHandler::new(&command).unwrap();
        let result = self
            .exec_transaction_command(command_handler, client_address)
            .await;
        assert_eq!(result.is_ok(), true);

        match result.unwrap() {
            DispatchResult::ExecuteTransactionCommands(commands) => {
                assert_eq!(commands, expected);
            }
            _ => panic!("Expected commands, got something else"),
        }
    }

    /// Execute a transaction command and assert it fails
    pub async fn exec_transaction_err(
        &mut self,
        command: RespValue,
        client_address: &str,
        expected_error: DispatchError,
    ) {
        let command_handler = CommandHandler::new(&command).unwrap();
        let result = self
            .exec_transaction_command(command_handler, client_address)
            .await;
        assert_eq!(result.is_err(), true);
        assert_eq!(result, Err(expected_error));
    }

    /// Execute queued transaction commands and assert it returns the expected response
    pub async fn exec_transaction_execute_commands(
        &mut self,
        command: RespValue,
        client_address: &str,
        expected: String,
    ) {
        let command_handler = CommandHandler::new(&command).unwrap();
        let result = self
            .exec_transaction_command(command_handler, client_address)
            .await;
        assert_eq!(result.is_ok(), true);

        let response = result
            .unwrap()
            .handle_dispatch_result(
                Arc::clone(&self.server),
                client_address,
                Arc::clone(&self.store),
                Arc::clone(&self.state),
            )
            .await;

        assert_eq!(response.0, expected);
    }

    /// Get a reference to the store for inspection
    pub async fn get_store(&self) -> tokio::sync::MutexGuard<'_, KeyValueStore> {
        self.store.lock().await
    }

    /// Get a reference to the state for inspection
    pub async fn get_state(&self) -> tokio::sync::MutexGuard<'_, State> {
        self.state.lock().await
    }
}

impl TestUtils {
    /// Create a BLPOP command
    pub fn blpop_command(key: &str, timeout_seconds: &str) -> RespValue {
        RespValue::Array(vec![
            RespValue::BulkString("BLPOP".to_string()),
            RespValue::BulkString(key.to_string()),
            RespValue::BulkString(timeout_seconds.to_string()),
        ])
    }

    /// Create an RPUSH command with multiple values
    pub fn rpush_command(key: &str, values: &[&str]) -> RespValue {
        let mut command = vec![
            RespValue::BulkString("RPUSH".to_string()),
            RespValue::BulkString(key.to_string()),
        ];

        for value in values {
            command.push(RespValue::BulkString(value.to_string()));
        }

        RespValue::Array(command)
    }

    /// Create an LPUSH command with multiple values
    pub fn lpush_command(key: &str, values: &[&str]) -> RespValue {
        let mut command = vec![
            RespValue::BulkString("LPUSH".to_string()),
            RespValue::BulkString(key.to_string()),
        ];

        for value in values {
            command.push(RespValue::BulkString(value.to_string()));
        }

        RespValue::Array(command)
    }

    /// Create an LLEN command
    pub fn llen_command(key: &str) -> RespValue {
        RespValue::Array(vec![
            RespValue::BulkString("LLEN".to_string()),
            RespValue::BulkString(key.to_string()),
        ])
    }

    /// Create an LRANGE command
    pub fn lrange_command(key: &str, start: i32, stop: i32) -> RespValue {
        RespValue::Array(vec![
            RespValue::BulkString("LRANGE".to_string()),
            RespValue::BulkString(key.to_string()),
            RespValue::BulkString(start.to_string()),
            RespValue::BulkString(stop.to_string()),
        ])
    }

    /// Create an LPOP command
    pub fn lpop_command(key: &str) -> RespValue {
        RespValue::Array(vec![
            RespValue::BulkString("LPOP".to_string()),
            RespValue::BulkString(key.to_string()),
        ])
    }

    /// Create an LPOP command for mutiple items
    pub fn lpop_command_multiple_items(key: &str, num_items: u32) -> RespValue {
        RespValue::Array(vec![
            RespValue::BulkString("LPOP".to_string()),
            RespValue::BulkString(key.to_string()),
            RespValue::BulkString(num_items.to_string()),
        ])
    }

    /// Create a PING command
    pub fn ping_command() -> RespValue {
        RespValue::Array(vec![RespValue::BulkString("PING".to_string())])
    }

    /// Create an ECHO command
    pub fn echo_command(message: &str) -> RespValue {
        RespValue::Array(vec![
            RespValue::BulkString("ECHO".to_string()),
            RespValue::BulkString(message.to_string()),
        ])
    }

    /// Create a GET command
    pub fn get_command(key: &str) -> RespValue {
        RespValue::Array(vec![
            RespValue::BulkString("GET".to_string()),
            RespValue::BulkString(key.to_string()),
        ])
    }

    /// Create a SET command
    pub fn set_command(key: &str, value: &str) -> RespValue {
        RespValue::Array(vec![
            RespValue::BulkString("SET".to_string()),
            RespValue::BulkString(key.to_string()),
            RespValue::BulkString(value.to_string()),
        ])
    }

    /// Create a SET command with expiration
    pub fn set_command_with_expiration(key: &str, value: &str, expiration_ms: u64) -> RespValue {
        RespValue::Array(vec![
            RespValue::BulkString("SET".to_string()),
            RespValue::BulkString(key.to_string()),
            RespValue::BulkString(value.to_string()),
            RespValue::BulkString("px".to_string()),
            RespValue::BulkString(expiration_ms.to_string()),
        ])
    }

    /// Create a TYPE command
    pub fn type_command(key: &str) -> RespValue {
        RespValue::Array(vec![
            RespValue::BulkString("TYPE".to_string()),
            RespValue::BulkString(key.to_string()),
        ])
    }

    /// Create a XADD command
    pub fn xadd_command(key: &str, stream_id: &str, entries: &[&str]) -> RespValue {
        let mut vec = vec![
            RespValue::BulkString("XADD".to_string()),
            RespValue::BulkString(key.to_string()),
            RespValue::BulkString(stream_id.to_string()),
        ];

        for entry in entries {
            vec.push(RespValue::BulkString(entry.to_string()));
        }

        RespValue::Array(vec)
    }

    /// Create a XRANGE command
    pub fn xrange_command(key: &str, start_stream_id: &str, end_stream_id: &str) -> RespValue {
        RespValue::Array(vec![
            RespValue::BulkString("XRANGE".to_string()),
            RespValue::BulkString(key.to_string()),
            RespValue::BulkString(start_stream_id.to_string()),
            RespValue::BulkString(end_stream_id.to_string()),
        ])
    }

    /// Create a XREAD command
    pub fn xread_command(keys: &[&str], start_stream_ids: &[&str]) -> RespValue {
        let mut vec = vec![
            RespValue::BulkString("XREAD".to_string()),
            RespValue::BulkString("STREAMS".to_string()),
        ];

        for key in keys {
            vec.push(RespValue::BulkString(key.to_string()));
        }

        for stream_id in start_stream_ids {
            vec.push(RespValue::BulkString(stream_id.to_string()));
        }

        RespValue::Array(vec)
    }

    /// Create a blocking XREAD command
    pub fn xread_blocking_command(
        timeout_milliseconds: &str,
        keys: &[&str],
        start_stream_ids: &[&str],
    ) -> RespValue {
        let mut vec = vec![
            RespValue::BulkString("XREAD".to_string()),
            RespValue::BulkString("BLOCK".to_string()),
            RespValue::BulkString(timeout_milliseconds.to_string()),
            RespValue::BulkString("STREAMS".to_string()),
        ];

        for key in keys {
            vec.push(RespValue::BulkString(key.to_string()));
        }

        for stream_id in start_stream_ids {
            vec.push(RespValue::BulkString(stream_id.to_string()));
        }

        RespValue::Array(vec)
    }

    /// Create an INCR command
    pub fn incr_command(key: &str) -> RespValue {
        RespValue::Array(vec![
            RespValue::BulkString("INCR".to_string()),
            RespValue::BulkString(key.to_string()),
        ])
    }

    /// Create a MULTI command
    pub fn multi_command() -> RespValue {
        RespValue::Array(vec![RespValue::BulkString("MULTI".to_string())])
    }

    /// Create an EXEC command
    pub fn exec_command() -> RespValue {
        RespValue::Array(vec![RespValue::BulkString("EXEC".to_string())])
    }

    /// Create a DISCARD command
    pub fn discard_command() -> RespValue {
        RespValue::Array(vec![RespValue::BulkString("DISCARD".to_string())])
    }

    /// Create an INFO command
    pub fn info_command(message: Option<&str>) -> RespValue {
        if let Some(info_section) = message {
            RespValue::Array(vec![
                RespValue::BulkString("INFO".to_string()),
                RespValue::BulkString(info_section.to_string()),
            ])
        } else {
            RespValue::Array(vec![RespValue::BulkString("INFO".to_string())])
        }
    }

    /// Create a REPLCONF command
    pub fn replconf_command(key: &str, value: &str) -> RespValue {
        RespValue::Array(vec![
            RespValue::BulkString("REPLCONF".to_string()),
            RespValue::BulkString(key.to_string()),
            RespValue::BulkString(value.to_string()),
        ])
    }

    /// Create a PSYNC command
    pub fn psync_command(replication_id: &str, offset: &str) -> RespValue {
        RespValue::Array(vec![
            RespValue::BulkString("PSYNC".to_string()),
            RespValue::BulkString(replication_id.to_string()),
            RespValue::BulkString(offset.to_string()),
        ])
    }

    /// Create an invalid command
    pub fn invalid_command(args: &[&str]) -> RespValue {
        let mut vec = Vec::new();

        for arg in args {
            vec.push(RespValue::BulkString(arg.to_string()));
        }

        RespValue::Array(vec)
    }

    /// Generate a unique server address for testing
    pub fn client_address(port: u16) -> String {
        format!("127.0.0.1:{}", port)
    }

    /// Spawn a BLPOP task that blocks on the given key
    pub fn spawn_blpop_task(
        env: &TestEnv,
        key: &str,
        timeout_seconds: &str,
        client_address: &str,
    ) -> tokio::task::JoinHandle<Result<String, codecrafters_redis::commands::CommandError>> {
        let (store_clone, state_clone, server_clone) = env.clone_env();
        let blpop_command = Self::blpop_command(key, timeout_seconds);
        let client_address = client_address.to_string();

        tokio::spawn(async move {
            let command_handler = CommandHandler::new(&blpop_command)?;

            command_handler
                .handle_command(
                    Arc::clone(&server_clone),
                    &client_address,
                    Arc::clone(&store_clone),
                    Arc::clone(&state_clone),
                )
                .await
        })
    }

    /// Spawn a XREAD task that blocks on the given key
    pub fn spawn_xread_task(
        env: &TestEnv,
        keys: &[&str],
        stream_ids: &[&str],
        timeout_milliseconds: &str,
        client_address: &str,
    ) -> tokio::task::JoinHandle<Result<String, codecrafters_redis::commands::CommandError>> {
        let (store_clone, state_clone, server_clone) = env.clone_env();
        let xread_blocking_command =
            Self::xread_blocking_command(timeout_milliseconds, keys, stream_ids);
        let client_address = client_address.to_string();

        tokio::spawn(async move {
            let command_handler = CommandHandler::new(&xread_blocking_command)?;

            command_handler
                .handle_command(
                    Arc::clone(&server_clone),
                    &client_address,
                    Arc::clone(&store_clone),
                    Arc::clone(&state_clone),
                )
                .await
        })
    }

    /// Wait for a task with timeout and expect it to complete (success or failure)
    pub async fn wait_for_completion(
        task: tokio::task::JoinHandle<Result<String, codecrafters_redis::commands::CommandError>>,
        timeout_duration: Duration,
    ) -> Result<String, codecrafters_redis::commands::CommandError> {
        timeout(timeout_duration, task)
            .await
            .expect("Task should complete within timeout")
            .expect("Task should not panic")
    }

    /// Create expected bulk string response
    pub fn expected_bulk_string(value: &str) -> String {
        format!("${}\r\n{}\r\n", value.len(), value)
    }

    /// Create expected integer response
    pub fn expected_integer(value: i64) -> String {
        format!(":{}\r\n", value)
    }

    /// Create expected simple string response
    pub fn expected_simple_string(value: &str) -> String {
        format!("+{}\r\n", value)
    }

    /// Create expected null response
    pub fn expected_null() -> String {
        "$-1\r\n".to_string()
    }

    /// Create expected bulk string array response
    pub fn expected_bulk_string_array(items: &[&str]) -> String {
        let mut response = format!("*{}\r\n", items.len());
        for item in items {
            response.push_str(&format!("${}\r\n{}\r\n", item.len(), item));
        }
        response
    }

    /// Async sleep helper
    pub async fn sleep_ms(ms: u64) {
        tokio::time::sleep(Duration::from_millis(ms)).await;
    }

    /// Filter successful results containing a specific substring
    pub fn filter_successful_results_containing<'a>(
        results: &'a [Result<String, codecrafters_redis::commands::CommandError>],
        substring: &str,
    ) -> Vec<&'a String> {
        results
            .iter()
            .filter_map(|r| r.as_ref().ok())
            .filter(|s| s.contains(substring))
            .collect()
    }
}
