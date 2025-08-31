use std::{collections::HashMap, sync::Arc};

use rand::distr::{Alphanumeric, SampleString};
use regex::Regex;
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Mutex, RwLock},
};

use crate::connection::{handle_master_to_replica_connection, handle_replica_to_client_connection};
use crate::input::handshake;
use crate::rdb::parse_rdb_file;
use crate::resp::RespValue;
use crate::{connection::handle_master_to_client_connection, state::State};

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
    #[error("Invalid RDB directory path")]
    InvalidRdbDirectoryPath,
    #[error("Invalid RDB file name")]
    InvalidRdbFileName,
}

#[derive(Debug, PartialEq, Clone)]
pub enum RedisRole {
    Master,
    Replica((String, u32)),
}

impl RedisRole {
    pub fn as_string(&self) -> &str {
        match self {
            RedisRole::Master => "master",
            RedisRole::Replica(_) => "slave",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Replica {
    pub writer: Arc<RwLock<OwnedWriteHalf>>,
    pub offset: usize,
}

#[derive(Debug, Clone)]
pub struct RedisServer {
    pub port: u32,
    pub role: RedisRole,
    pub repl_id: String,
    pub repl_offset: usize,
    pub replicas: Option<HashMap<String, Replica>>,
    pub write_commands: Vec<&'static str>,
    pub rdb_directory: String,
    pub rdb_filename: String,
    pub pub_sub_channels: HashMap<String, HashMap<String, Arc<RwLock<OwnedWriteHalf>>>>,
}

impl RedisServer {
    pub fn new<I: IntoIterator<Item = String>>(command_line_args: I) -> Result<Self, CliError> {
        let mut iter = command_line_args.into_iter().skip(1);
        let mut port: Option<u32> = None;
        let mut redis_role: Option<RedisRole> = None;
        let mut directory_path: Option<String> = None;
        let mut rdb_filename: Option<String> = None;

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
                "--dir" => {
                    let Some(dir) = iter.next() else {
                        return Err(CliError::InvalidCommandLineFlag);
                    };

                    let validated_dir = validate_directory_path(dir)?;

                    directory_path = Some(validated_dir);
                }
                "--dbfilename" => {
                    let Some(filename) = iter.next() else {
                        return Err(CliError::InvalidCommandLineFlag);
                    };

                    let validated_filename = validate_rdb_file_name(filename)?;

                    rdb_filename = Some(validated_filename);
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
            rdb_directory: directory_path.unwrap_or("./src".to_string()),
            rdb_filename: rdb_filename.unwrap_or("dump.rdb".to_string()),
            pub_sub_channels: HashMap::new(),
        })
    }

    pub async fn update_replication_offset(&mut self, input: RespValue) {
        self.repl_offset += input.encode().as_bytes().len();
    }

    pub async fn should_replicate_write_command(
        &self,
        input: RespValue,
        command_name: &str,
    ) -> tokio::io::Result<()> {
        if !self.write_commands.contains(&command_name) {
            return Ok(());
        }

        if let Some(ref replicas) = self.replicas {
            for replica in replicas.values() {
                let mut replica_writer_guard = replica.writer.write().await;
                replica_writer_guard
                    .write_all(input.encode().as_bytes())
                    .await?;
                replica_writer_guard.flush().await?;
            }
        }

        Ok(())
    }

    pub async fn run(&self) {
        let store = Arc::new(Mutex::new(HashMap::new()));
        let state = Arc::new(Mutex::new(State::new()));
        let server = Arc::new(RwLock::new(self.clone()));

        if let Err(e) = parse_rdb_file(Arc::clone(&server), Arc::clone(&store)).await {
            if e.kind() == tokio::io::ErrorKind::NotFound {
                eprintln!("RDB file not found, proceeding without it");
            } else {
                eprintln!("Failed to parse RDB file: {}", e);
                return;
            }
        }

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

                if let Err(e) =
                    handshake(&mut stream, Arc::clone(&server), Arc::clone(&store)).await
                {
                    eprintln!("Failed to perform handshake: {}", e);
                    return;
                };

                tokio::spawn(async move {
                    handle_master_to_replica_connection(
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
                        let role = {
                            let server_guard = server_clone.read().await;
                            server_guard.role.clone()
                        };

                        match role {
                            RedisRole::Master => {
                                handle_master_to_client_connection(
                                    stream,
                                    server_clone,
                                    client_address.to_string(),
                                    store_clone,
                                    state_clone,
                                )
                                .await
                            }
                            RedisRole::Replica(_) => {
                                handle_replica_to_client_connection(
                                    stream,
                                    server_clone,
                                    client_address.to_string(),
                                    store_clone,
                                    state_clone,
                                )
                                .await;
                            }
                        }
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

fn validate_port_flag(port: &str) -> Result<u32, CliError> {
    validate_port_with_error(port, CliError::InvalidPortFlagValue)
}

fn validate_master_port(port: &str) -> Result<u32, CliError> {
    validate_port_with_error(port, CliError::InvalidMasterPort)
}

fn validate_port_with_error(port: &str, error: CliError) -> Result<u32, CliError> {
    let port_number = port.parse::<u32>().map_err(|_| error.clone())?;

    if port_number < 1 || port_number > 65535 {
        return Err(error);
    }

    Ok(port_number)
}

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

fn validate_directory_path(dir: String) -> Result<String, CliError> {
    let regex = Regex::new(r"^(\/|\.{1,2}|(\.?\.?\/)?[a-zA-Z0-9-_]+(\/[a-zA-Z0-9-_]+)*)$").unwrap();
    if regex.is_match(&dir) {
        Ok(dir)
    } else {
        Err(CliError::InvalidRdbDirectoryPath)
    }
}

fn validate_rdb_file_name(file_name: String) -> Result<String, CliError> {
    let regex = Regex::new(r"^[a-zA-Z0-9-_]+\.rdb$").unwrap();
    if regex.is_match(&file_name) {
        Ok(file_name)
    } else {
        Err(CliError::InvalidRdbFileName)
    }
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
            (
                vec![
                    "codecrafters-redis".to_string(),
                    "--dir".to_string(),
                    "//tmp/redis-files".to_string(),
                ],
                CliError::InvalidRdbDirectoryPath,
            ),
            (
                vec![
                    "codecrafters-redis".to_string(),
                    "--dir".to_string(),
                    "/tmp.setup/redis".to_string(),
                ],
                CliError::InvalidRdbDirectoryPath,
            ),
            (
                vec![
                    "codecrafters-redis".to_string(),
                    "--dir".to_string(),
                    "/tmp/redis/".to_string(),
                ],
                CliError::InvalidRdbDirectoryPath,
            ),
            (
                vec![
                    "codecrafters-redis".to_string(),
                    "--dbfilename".to_string(),
                    "dump".to_string(),
                ],
                CliError::InvalidRdbFileName,
            ),
            (
                vec![
                    "codecrafters-redis".to_string(),
                    "--dbfilename".to_string(),
                    "dump.pdf".to_string(),
                ],
                CliError::InvalidRdbFileName,
            ),
            (
                vec![
                    "codecrafters-redis".to_string(),
                    "--dbfilename".to_string(),
                    "dump.development.rdb".to_string(),
                ],
                CliError::InvalidRdbFileName,
            ),
        ];

        for (args, expected_error) in test_cases {
            let result = RedisServer::new(args);
            assert!(result.is_err());
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
                "./src",
                "dump.rdb",
            ),
            (
                vec![
                    "codecrafters-redis".to_string(),
                    "--port".to_string(),
                    "6677".to_string(),
                ],
                6677,
                RedisRole::Master,
                "./src",
                "dump.rdb",
            ),
            (
                vec![
                    "codecrafters-redis".to_string(),
                    "--replicaof".to_string(),
                    "127.0.0.1 6380".to_string(),
                ],
                6379,
                RedisRole::Replica(("127.0.0.1".to_string(), 6380)),
                "./src",
                "dump.rdb",
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
                "./src",
                "dump.rdb",
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
                "./src",
                "dump.rdb",
            ),
            (
                vec![
                    "codecrafters-redis".to_string(),
                    "--dir".to_string(),
                    "/tmp/redis-files".to_string(),
                ],
                6379,
                RedisRole::Master,
                "/tmp/redis-files",
                "dump.rdb",
            ),
            (
                vec![
                    "codecrafters-redis".to_string(),
                    "--dir".to_string(),
                    "/".to_string(),
                ],
                6379,
                RedisRole::Master,
                "/",
                "dump.rdb",
            ),
            (
                vec![
                    "codecrafters-redis".to_string(),
                    "--dir".to_string(),
                    "tmp/redis".to_string(),
                ],
                6379,
                RedisRole::Master,
                "tmp/redis",
                "dump.rdb",
            ),
            (
                vec![
                    "codecrafters-redis".to_string(),
                    "--dir".to_string(),
                    "redis".to_string(),
                ],
                6379,
                RedisRole::Master,
                "redis",
                "dump.rdb",
            ),
            (
                vec![
                    "codecrafters-redis".to_string(),
                    "--dbfilename".to_string(),
                    "redis.rdb".to_string(),
                ],
                6379,
                RedisRole::Master,
                "./src",
                "redis.rdb",
            ),
        ];

        for (args, expected_port, expected_role, expected_rdb_directory, expected_rdb_filename) in
            test_cases
        {
            let server = RedisServer::new(args).unwrap();
            assert_eq!(server.port, expected_port);
            assert_eq!(server.role, expected_role);
            assert_eq!(server.rdb_directory, expected_rdb_directory);
            assert_eq!(server.rdb_filename, expected_rdb_filename);
        }
    }
}
