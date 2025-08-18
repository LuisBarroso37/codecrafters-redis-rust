//! Redis server implementation and configuration.
//!
//! This module contains the core Redis server logic, including server configuration,
//! role management (master/replica), and the main server loop that handles incoming
//! client connections. It supports both standalone and replication modes.

use std::{collections::HashMap, sync::Arc};

use rand::distr::{Alphanumeric, SampleString};
use regex::Regex;
use thiserror::Error;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Mutex, RwLock},
};

use crate::connection::handle_master_connection;
use crate::input::handshake;
use crate::{connection::handle_client_connection, state::State};

/// Errors that can occur during command-line argument parsing and server setup.
#[derive(Error, Debug, PartialEq, Clone)]
pub enum CliError {
    #[error("Invalid command line flag")]
    InvalidCommandLineFlag,
    #[error("Invalid port flag value")]
    InvalidPortFlagValue,
    #[error("Invalid master address")]
    InvalidMasterAddress,
    #[error("Invalid master port")]
    InvalidMasterPort,
}

/// Represents the role of a Redis server instance.
///
/// A Redis server can operate in one of two modes:
/// - Master: Accepts write commands from clients and replicates them to replicas
/// - Replica: Receives commands from a master server and serves read-only requests
#[derive(Debug, PartialEq, Clone)]
pub enum RedisRole {
    /// A master server that can accept write commands
    Master,
    /// A replica server connected to a master at the specified (host, port)
    Replica((String, u32)),
}

impl RedisRole {
    /// Returns a string representation of the server role.
    ///
    /// This is used for the INFO command and other introspection operations.
    ///
    /// # Returns
    ///
    /// * "master" - If the server is operating as a master
    /// * "slave" - If the server is operating as a replica (uses Redis terminology)
    pub fn as_string(&self) -> &str {
        match self {
            RedisRole::Master => "master",
            RedisRole::Replica(_) => "slave",
        }
    }
}

/// Configuration and state for a Redis server instance.
///
/// Contains all the necessary configuration and runtime state for a Redis server,
/// including network settings, replication configuration, and connection management.
/// Supports both master and replica server modes.
#[derive(Debug, Clone)]
pub struct RedisServer {
    /// The TCP port number the server listens on
    pub port: u32,
    /// The server's role (Master or Replica with master address)
    pub role: RedisRole,
    /// Unique replication ID for this server instance (40-character hex string)
    pub repl_id: String,
    /// Current replication offset for tracking synchronized data
    pub repl_offset: usize,
    /// Map of replica connections (only present for master servers)
    pub replicas: Option<HashMap<String, Arc<RwLock<OwnedWriteHalf>>>>,
    /// List of commands that are considered write operations
    pub write_commands: Vec<&'static str>,
}

impl RedisServer {
    /// Creates a new Redis server instance from command-line arguments.
    ///
    /// Parses command-line arguments to configure the server's port, role, and
    /// replication settings. Generates a unique replication ID for the server.
    ///
    /// # Arguments
    ///
    /// * `command_line_args` - Iterator over command-line arguments (typically from `std::env::args()`)
    ///
    /// # Supported Arguments
    ///
    /// * `--port <port>` - Port number to listen on (default: 6379)
    /// * `--replicaof <host> <port>` - Configure as replica of the specified master
    ///
    /// # Returns
    ///
    /// * `Ok(RedisServer)` - Successfully configured server instance
    /// * `Err(CliError)` - If argument parsing fails or invalid values are provided
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Create a master server on port 6379
    /// let server = RedisServer::new(vec!["redis-server".to_string()])?;
    ///
    /// // Create a replica server
    /// let server = RedisServer::new(vec![
    ///     "redis-server".to_string(),
    ///     "--replicaof".to_string(),
    ///     "127.0.0.1 6379".to_string()
    /// ])?;
    /// ```
    pub fn new<I: IntoIterator<Item = String>>(command_line_args: I) -> Result<Self, CliError> {
        let mut iter = command_line_args.into_iter().skip(1);
        let mut port: Option<u32> = None;
        let mut redis_role: Option<RedisRole> = None;

        while let Some(arg) = iter.next() {
            match arg.as_str() {
                "--port" => {
                    let Some(port_str) = iter.next() else {
                        return Err(CliError::InvalidCommandLineFlag);
                    };

                    let port_number = validate_port_flag(&port_str)?;

                    port = Some(port_number);
                }
                "--replicaof" => {
                    let Some(master_address) = iter.next() else {
                        return Err(CliError::InvalidCommandLineFlag);
                    };

                    let validated_address = validate_master_address(&master_address)?;

                    redis_role = Some(RedisRole::Replica((
                        validated_address.0,
                        validated_address.1,
                    )));
                }
                _ => return Err(CliError::InvalidCommandLineFlag),
            }
        }

        let role = redis_role.unwrap_or(RedisRole::Master);

        let replicas = if role == RedisRole::Master {
            Some(HashMap::new())
        } else {
            None
        };

        Ok(RedisServer {
            port: port.unwrap_or(6379),
            role,
            repl_id: Alphanumeric.sample_string(&mut rand::rng(), 40),
            repl_offset: 0,
            replicas,
            write_commands: Vec::from(["SET", "RPUSH", "LPUSH", "INCR", "LPOP", "BLPOP", "XADD"]),
        })
    }

    /// Runs the Redis server, handling connections and replication.
    ///
    /// This is the main server loop that:
    /// - For replica servers: Connects to the master, performs handshake, and processes replication
    /// - For master servers: Listens for client connections and handles them concurrently
    ///
    /// The method runs indefinitely until the process is terminated or a fatal error occurs.
    ///
    /// # Behavior
    ///
    /// ## Master Mode
    /// - Binds to the configured port and listens for TCP connections
    /// - Spawns a new async task for each client connection
    /// - Manages replica connections and forwards write commands to them
    ///
    /// ## Replica Mode
    /// - Connects to the specified master server
    /// - Performs the Redis replication handshake (PING, REPLCONF, PSYNC)
    /// - Receives and applies commands from the master
    /// - Also listens for client connections to serve read-only requests
    ///
    /// # Panics
    ///
    /// May panic if critical system resources (like TCP listeners) cannot be created.
    pub async fn run(&self) {
        let store = Arc::new(Mutex::new(HashMap::new()));
        let state = Arc::new(Mutex::new(State::new()));
        let server = Arc::new(RwLock::new(self.clone()));

        match &self.role {
            RedisRole::Replica((address, port)) => {
                let master_address = format!("{}:{}", address, port);

                let mut stream = match TcpStream::connect(&master_address).await {
                    Ok(stream) => stream,
                    Err(e) => {
                        eprintln!("Failed to connect to replica: {}", e);
                        return;
                    }
                };

                let server_clone = Arc::clone(&server);
                let store_clone = Arc::clone(&store);
                let state_clone = Arc::clone(&state);

                if let Err(e) = handshake(&mut stream, Arc::clone(&server)).await {
                    eprintln!("Failed to perform handshake: {}", e);
                    return;
                };

                tokio::spawn(async move {
                    handle_master_connection(
                        &master_address,
                        &mut stream,
                        server_clone,
                        store_clone,
                        state_clone,
                    )
                    .await;
                });
            }
            _ => (),
        }

        let listener = match TcpListener::bind(format!("127.0.0.1:{}", self.port)).await {
            Ok(listener) => listener,
            Err(e) => {
                eprintln!("Failed to bind TCP listener: {}", e);
                return;
            }
        };

        loop {
            match listener.accept().await {
                Ok((stream, client_address)) => {
                    let server_clone = Arc::clone(&server);
                    let store_clone = Arc::clone(&store);
                    let state_clone = Arc::clone(&state);

                    tokio::spawn(async move {
                        handle_client_connection(
                            stream,
                            server_clone,
                            client_address.to_string(),
                            store_clone,
                            state_clone,
                        )
                        .await;
                    });
                }
                Err(e) => {
                    eprintln!("error: {}", e);
                    break;
                }
            }
        }
    }
}

/// Validates a port number from the --port command-line flag.
///
/// # Arguments
///
/// * `port` - String representation of the port number
///
/// # Returns
///
/// * `Ok(u32)` - Valid port number in range 1-65535
/// * `Err(CliError::InvalidPortFlagValue)` - If the port is invalid
fn validate_port_flag(port: &str) -> Result<u32, CliError> {
    validate_port_with_error(port, CliError::InvalidPortFlagValue)
}

/// Validates a port number from a master address specification.
///
/// # Arguments
///
/// * `port` - String representation of the port number
///
/// # Returns
///
/// * `Ok(u32)` - Valid port number in range 1-65535
/// * `Err(CliError::InvalidMasterPort)` - If the port is invalid
fn validate_master_port(port: &str) -> Result<u32, CliError> {
    validate_port_with_error(port, CliError::InvalidMasterPort)
}

/// Generic port validation with custom error type.
///
/// Validates that a port number string represents a valid TCP port number
/// in the range 1-65535.
///
/// # Arguments
///
/// * `port` - String representation of the port number
/// * `error` - The specific error type to return if validation fails
///
/// # Returns
///
/// * `Ok(u32)` - Valid port number
/// * `Err(CliError)` - The specified error if validation fails
fn validate_port_with_error(port: &str, error: CliError) -> Result<u32, CliError> {
    let port_number = port.parse::<u32>().map_err(|_| error.clone())?;

    if port_number < 1 || port_number > 65535 {
        return Err(error);
    }

    Ok(port_number)
}

/// Validates and parses a master server address specification.
///
/// Parses a master address in the format "host port" where host can be
/// an IPv4 address or hostname, and port is a valid TCP port number.
///
/// # Arguments
///
/// * `master_address` - Space-separated string containing host and port (e.g., "127.0.0.1 6379")
///
/// # Returns
///
/// * `Ok((String, u32))` - Tuple containing (hostname/IP, port)
/// * `Err(CliError::InvalidMasterAddress)` - If the format is invalid
/// * `Err(CliError::InvalidMasterPort)` - If the port number is invalid
///
/// # Supported Host Formats
///
/// - IPv4 addresses: "192.168.1.1"
/// - Hostnames: "localhost", "redis-master.example.com"
/// - Must not contain invalid characters or exceed reasonable length limits
fn validate_master_address(master_address: &str) -> Result<(String, u32), CliError> {
    let ipv4_regex = Regex::new(r"^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$").unwrap();
    let hostname_regex = Regex::new(r"^[a-zA-Z0-9\-\.]+$").unwrap();

    let split_address = master_address.split_whitespace().collect::<Vec<&str>>();

    if split_address.len() != 2 {
        return Err(CliError::InvalidMasterAddress);
    }

    let address = split_address[0];

    let valid_address = if let Some(caps) = ipv4_regex.captures(address) {
        caps.iter().skip(1).all(|octet| {
            octet
                .map(|m| m.as_str().parse::<u16>().map(|v| v <= 255).unwrap_or(false))
                .unwrap_or(false)
        })
    } else {
        hostname_regex.is_match(address)
    };

    if !valid_address {
        return Err(CliError::InvalidMasterAddress);
    }

    let port_number = validate_master_port(split_address[1])?;

    Ok((address.to_string(), port_number))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_port_flag() {
        let test_cases = [
            ("6379", Ok(6379), "valid standard port"),
            ("1", Ok(1), "minimum valid port"),
            ("65535", Ok(65535), "maximum valid port"),
            ("0", Err(CliError::InvalidPortFlagValue), "zero port"),
            (
                "65536",
                Err(CliError::InvalidPortFlagValue),
                "port too high",
            ),
            (
                "not_a_number",
                Err(CliError::InvalidPortFlagValue),
                "invalid format",
            ),
            ("-1", Err(CliError::InvalidPortFlagValue), "negative port"),
            ("", Err(CliError::InvalidPortFlagValue), "empty string"),
            (
                "80.5",
                Err(CliError::InvalidPortFlagValue),
                "decimal number",
            ),
            (
                "999999",
                Err(CliError::InvalidPortFlagValue),
                "very high port",
            ),
        ];

        for (input, expected, description) in test_cases {
            let result = validate_port_flag(input);
            match expected {
                Ok(expected_port) => {
                    assert!(result.is_ok(), "Failed for {}: {}", description, input);
                    assert_eq!(
                        result.unwrap(),
                        expected_port,
                        "Wrong port for {}: {}",
                        description,
                        input
                    );
                }
                Err(expected_error) => {
                    assert!(
                        result.is_err(),
                        "Should fail for {}: {}",
                        description,
                        input
                    );

                    assert_eq!(
                        result,
                        Err(expected_error),
                        "Wrong error type for {}: {}",
                        description,
                        input
                    );
                }
            }
        }
    }

    #[test]
    fn test_validate_master_address() {
        let test_cases = [
            (
                "127.0.0.1 6379",
                Ok(("127.0.0.1".to_string(), 6379)),
                "valid IPv4 address",
            ),
            (
                "localhost 6380",
                Ok(("localhost".to_string(), 6380)),
                "valid hostname",
            ),
            (
                "redis-server 1024",
                Ok(("redis-server".to_string(), 1024)),
                "valid hostname with minimum port",
            ),
            (
                "example.com 65535",
                Ok(("example.com".to_string(), 65535)),
                "valid domain with maximum port",
            ),
            (
                "192.168.1.100 8080",
                Ok(("192.168.1.100".to_string(), 8080)),
                "valid IPv4 with custom port",
            ),
            (
                "localhost 100000",
                Err(CliError::InvalidMasterPort),
                "port too high",
            ),
            (
                "localhost 0",
                Err(CliError::InvalidMasterPort),
                "port too low",
            ),
            (
                "localhost",
                Err(CliError::InvalidMasterAddress),
                "missing port",
            ),
            (
                "localhost 6379 extra",
                Err(CliError::InvalidMasterAddress),
                "too many arguments",
            ),
            ("", Err(CliError::InvalidMasterAddress), "empty string"),
            (
                "localhost not_a_port",
                Err(CliError::InvalidMasterPort),
                "invalid port format",
            ),
            (
                "localhost -1",
                Err(CliError::InvalidMasterPort),
                "negative port",
            ),
        ];

        for (input, expected, description) in test_cases {
            let result = validate_master_address(input);
            match expected {
                Ok((expected_host, expected_port)) => {
                    assert!(result.is_ok(), "Failed for {}: {}", description, input);

                    let (host, port) = result.unwrap();
                    assert_eq!(
                        host, expected_host,
                        "Wrong host for {}: {}",
                        description, input
                    );
                    assert_eq!(
                        port, expected_port,
                        "Wrong port for {}: {}",
                        description, input
                    );
                }
                Err(expected_error) => {
                    assert!(
                        result.is_err(),
                        "Should fail for {}: {}",
                        description,
                        input
                    );
                    assert_eq!(
                        result,
                        Err(expected_error),
                        "Wrong error type for {}: {}",
                        description,
                        input
                    );
                }
            }
        }
    }

    #[test]
    fn test_redis_server_creation_without_flags() {
        let args = vec!["codecrafters-redis".to_string()];

        let server = RedisServer::new(args).unwrap();
        assert_eq!(server.port, 6379);
    }

    #[test]
    fn test_redis_server_creation_with_invalid_flags() {
        let test_cases = vec![
            (
                vec!["codecrafters-redis".to_string(), "--port".to_string()],
                CliError::InvalidCommandLineFlag,
            ),
            (
                vec![
                    "codecrafters-redis".to_string(),
                    "--port".to_string(),
                    "invalid".to_string(),
                ],
                CliError::InvalidPortFlagValue,
            ),
            (
                vec![
                    "codecrafters-redis".to_string(),
                    "--port".to_string(),
                    "70000".to_string(),
                ],
                CliError::InvalidPortFlagValue,
            ),
            (
                vec!["codecrafters-redis".to_string(), "invalid".to_string()],
                CliError::InvalidCommandLineFlag,
            ),
            (
                vec!["codecrafters-redis".to_string(), "--replicaof".to_string()],
                CliError::InvalidCommandLineFlag,
            ),
            (
                vec![
                    "codecrafters-redis".to_string(),
                    "--replicaof".to_string(),
                    "invalid".to_string(),
                ],
                CliError::InvalidMasterAddress,
            ),
            (
                vec![
                    "codecrafters-redis".to_string(),
                    "--replicaof".to_string(),
                    "127.0.0.1 invalid".to_string(),
                ],
                CliError::InvalidMasterPort,
            ),
            (
                vec![
                    "codecrafters-redis".to_string(),
                    "--replicaof".to_string(),
                    "127.0.0.1".to_string(),
                ],
                CliError::InvalidMasterAddress,
            ),
            (
                vec![
                    "codecrafters-redis".to_string(),
                    "--replicaof".to_string(),
                    "256.0.0.1 6379".to_string(),
                ],
                CliError::InvalidMasterAddress,
            ),
            (
                vec![
                    "codecrafters-redis".to_string(),
                    "--replicaof".to_string(),
                    "my_host! 6379".to_string(),
                ],
                CliError::InvalidMasterAddress,
            ),
            (
                vec![
                    "codecrafters-redis".to_string(),
                    "--replicaof".to_string(),
                    "127.0.0.1 70000".to_string(),
                ],
                CliError::InvalidMasterPort,
            ),
        ];

        for (args, expected_error) in test_cases {
            let result = RedisServer::new(args);
            assert_eq!(result.is_err(), true);
            assert_eq!(result.unwrap_err(), expected_error);
        }
    }

    #[test]
    fn test_redis_server_creation_success_cases() {
        let test_cases = vec![
            (
                vec!["codecrafters-redis".to_string()],
                6379,
                RedisRole::Master,
            ),
            (
                vec![
                    "codecrafters-redis".to_string(),
                    "--port".to_string(),
                    "6677".to_string(),
                ],
                6677,
                RedisRole::Master,
            ),
            (
                vec![
                    "codecrafters-redis".to_string(),
                    "--replicaof".to_string(),
                    "127.0.0.1 6380".to_string(),
                ],
                6379,
                RedisRole::Replica(("127.0.0.1".to_string(), 6380)),
            ),
            (
                vec![
                    "codecrafters-redis".to_string(),
                    "--port".to_string(),
                    "7000".to_string(),
                    "--replicaof".to_string(),
                    "localhost 6381".to_string(),
                ],
                7000,
                RedisRole::Replica(("localhost".to_string(), 6381)),
            ),
            (
                vec![
                    "codecrafters-redis".to_string(),
                    "--port".to_string(),
                    "8000".to_string(),
                    "--replicaof".to_string(),
                    "redis-master 6500".to_string(),
                ],
                8000,
                RedisRole::Replica(("redis-master".to_string(), 6500)),
            ),
        ];

        for (args, expected_port, expected_role) in test_cases {
            let server = RedisServer::new(args).unwrap();
            assert_eq!(server.port, expected_port);
            assert_eq!(server.role, expected_role);
        }
    }
}
