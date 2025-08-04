use std::{collections::HashMap, sync::Arc, time::Duration};

use codecrafters_redis::{
    commands::{CommandError, CommandProcessor},
    key_value_store::KeyValueStore,
    resp::RespValue,
    state::State,
};
use tokio::{sync::Mutex, time::timeout};

/// Test utilities for simplifying Redis command tests
pub struct TestUtils;

/// Test environment containing store and state
pub struct TestEnv {
    pub store: Arc<Mutex<KeyValueStore>>,
    pub state: Arc<Mutex<State>>,
}

impl TestEnv {
    /// Create a new test environment with empty store and state
    pub fn new() -> Self {
        Self {
            store: Arc::new(Mutex::new(HashMap::new())),
            state: Arc::new(Mutex::new(State::new())),
        }
    }

    /// Clone the test environment
    pub fn clone(&self) -> Self {
        Self {
            store: Arc::clone(&self.store),
            state: Arc::clone(&self.state),
        }
    }

    /// Clone the environment for use in async tasks
    fn clone_env(&self) -> (Arc<Mutex<KeyValueStore>>, Arc<Mutex<State>>) {
        (Arc::clone(&self.store), Arc::clone(&self.state))
    }

    /// Execute a command and return the result
    async fn exec_command(
        &mut self,
        command: Vec<RespValue>,
        server_addr: &str,
    ) -> Result<String, codecrafters_redis::commands::CommandError> {
        let command_processor = CommandProcessor::new(command)?;

        command_processor
            .handle_command(server_addr.to_string(), &mut self.store, &mut self.state)
            .await
    }

    /// Execute a command and assert it succeeds with expected result
    pub async fn exec_command_ok(
        &mut self,
        command: Vec<RespValue>,
        server_addr: &str,
        expected: &str,
    ) {
        let result = self.exec_command(command, server_addr).await;
        assert_eq!(result, Ok(expected.to_string()));
    }

    /// Execute a command and assert it fails
    pub async fn exec_command_err(
        &mut self,
        command: Vec<RespValue>,
        server_addr: &str,
        expected_error: CommandError,
    ) {
        let result = self.exec_command(command, server_addr).await;
        assert!(result.is_err());
        assert_eq!(result, Err(expected_error));
    }

    /// Get a reference to the store for inspection
    pub async fn get_store(&self) -> tokio::sync::MutexGuard<'_, KeyValueStore> {
        self.store.lock().await
    }
}

impl TestUtils {
    /// Create a BLPOP command
    pub fn blpop_command(key: &str, timeout: &str) -> Vec<RespValue> {
        vec![RespValue::Array(vec![
            RespValue::BulkString("BLPOP".to_string()),
            RespValue::BulkString(key.to_string()),
            RespValue::BulkString(timeout.to_string()),
        ])]
    }

    /// Create an RPUSH command with multiple values
    pub fn rpush_command(key: &str, values: &[&str]) -> Vec<RespValue> {
        let mut command = vec![
            RespValue::BulkString("RPUSH".to_string()),
            RespValue::BulkString(key.to_string()),
        ];

        for value in values {
            command.push(RespValue::BulkString(value.to_string()));
        }

        vec![RespValue::Array(command)]
    }

    /// Create an LPUSH command with multiple values
    pub fn lpush_command(key: &str, values: &[&str]) -> Vec<RespValue> {
        let mut command = vec![
            RespValue::BulkString("LPUSH".to_string()),
            RespValue::BulkString(key.to_string()),
        ];

        for value in values {
            command.push(RespValue::BulkString(value.to_string()));
        }

        vec![RespValue::Array(command)]
    }

    /// Create an LLEN command
    pub fn llen_command(key: &str) -> Vec<RespValue> {
        vec![RespValue::Array(vec![
            RespValue::BulkString("LLEN".to_string()),
            RespValue::BulkString(key.to_string()),
        ])]
    }

    /// Create an LRANGE command
    pub fn lrange_command(key: &str, start: i32, stop: i32) -> Vec<RespValue> {
        vec![RespValue::Array(vec![
            RespValue::BulkString("LRANGE".to_string()),
            RespValue::BulkString(key.to_string()),
            RespValue::BulkString(start.to_string()),
            RespValue::BulkString(stop.to_string()),
        ])]
    }

    /// Create an LPOP command
    pub fn lpop_command(key: &str) -> Vec<RespValue> {
        vec![RespValue::Array(vec![
            RespValue::BulkString("LPOP".to_string()),
            RespValue::BulkString(key.to_string()),
        ])]
    }

    /// Create an LPOP command for mutiple items
    pub fn lpop_command_multiple_items(key: &str, num_items: u32) -> Vec<RespValue> {
        vec![RespValue::Array(vec![
            RespValue::BulkString("LPOP".to_string()),
            RespValue::BulkString(key.to_string()),
            RespValue::BulkString(num_items.to_string()),
        ])]
    }

    /// Create a PING command
    pub fn ping_command() -> Vec<RespValue> {
        vec![RespValue::Array(vec![RespValue::BulkString(
            "PING".to_string(),
        )])]
    }

    /// Create an ECHO command
    pub fn echo_command(message: &str) -> Vec<RespValue> {
        vec![RespValue::Array(vec![
            RespValue::BulkString("ECHO".to_string()),
            RespValue::BulkString(message.to_string()),
        ])]
    }

    /// Create a GET command
    pub fn get_command(key: &str) -> Vec<RespValue> {
        vec![RespValue::Array(vec![
            RespValue::BulkString("GET".to_string()),
            RespValue::BulkString(key.to_string()),
        ])]
    }

    /// Create a SET command
    pub fn set_command(key: &str, value: &str) -> Vec<RespValue> {
        vec![RespValue::Array(vec![
            RespValue::BulkString("SET".to_string()),
            RespValue::BulkString(key.to_string()),
            RespValue::BulkString(value.to_string()),
        ])]
    }

    /// Create a SET command with expiration
    pub fn set_command_with_expiration(
        key: &str,
        value: &str,
        expiration_ms: u64,
    ) -> Vec<RespValue> {
        vec![RespValue::Array(vec![
            RespValue::BulkString("SET".to_string()),
            RespValue::BulkString(key.to_string()),
            RespValue::BulkString(value.to_string()),
            RespValue::BulkString("px".to_string()),
            RespValue::BulkString(expiration_ms.to_string()),
        ])]
    }

    /// Create a TYPE command
    pub fn type_command(key: &str) -> Vec<RespValue> {
        vec![RespValue::Array(vec![
            RespValue::BulkString("TYPE".to_string()),
            RespValue::BulkString(key.to_string()),
        ])]
    }

    /// Create a XADD command
    pub fn xadd_command(key: &str, stream_id: &str, entries: &[&str]) -> Vec<RespValue> {
        let mut vec = vec![
            RespValue::BulkString("XADD".to_string()),
            RespValue::BulkString(key.to_string()),
            RespValue::BulkString(stream_id.to_string()),
        ];

        for entry in entries {
            vec.push(RespValue::BulkString(entry.to_string()));
        }

        vec![RespValue::Array(vec)]
    }

    /// Create a XRANGE command
    pub fn xrange_command(key: &str, start_stream_id: &str, end_stream_id: &str) -> Vec<RespValue> {
        vec![RespValue::Array(vec![
            RespValue::BulkString("XRANGE".to_string()),
            RespValue::BulkString(key.to_string()),
            RespValue::BulkString(start_stream_id.to_string()),
            RespValue::BulkString(end_stream_id.to_string()),
        ])]
    }

    /// Create a XREAD command
    pub fn xread_command(keys: &[&str], start_stream_ids: &[&str]) -> Vec<RespValue> {
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

        vec![RespValue::Array(vec)]
    }

    /// Create an invalid command
    pub fn invalid_command(args: &[&str]) -> Vec<RespValue> {
        let mut vec = Vec::new();

        for arg in args {
            vec.push(RespValue::BulkString(arg.to_string()));
        }

        vec![RespValue::Array(vec)]
    }

    /// Generate a unique server address for testing
    pub fn server_addr(port: u16) -> String {
        format!("127.0.0.1:{}", port)
    }

    /// Spawn a BLPOP task that blocks on the given key
    pub fn spawn_blpop_task(
        env: &TestEnv,
        key: &str,
        timeout: &str,
        server_addr: &str,
    ) -> tokio::task::JoinHandle<Result<String, codecrafters_redis::commands::CommandError>> {
        let (store_clone, state_clone) = env.clone_env();
        let blpop_command = Self::blpop_command(key, timeout);
        let server_addr = server_addr.to_string();

        tokio::spawn(async move {
            let command_processor = CommandProcessor::new(blpop_command)?;

            command_processor
                .handle_command(
                    server_addr,
                    &mut store_clone.clone(),
                    &mut state_clone.clone(),
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

    /// Create expected array response
    pub fn expected_array(items: &[&str]) -> String {
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
