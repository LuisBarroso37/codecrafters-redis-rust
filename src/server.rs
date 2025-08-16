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

use crate::connection::handle_master_connection;
use crate::input::handshake;
use crate::{connection::handle_client_connection, state::State};

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
pub struct RedisServer {
    pub port: u32,
    pub role: RedisRole,
    pub repl_id: String,
    pub repl_offset: u64,
    pub replicas: Option<HashMap<String, Arc<RwLock<OwnedWriteHalf>>>>,
}

impl RedisServer {
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
        })
    }

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
                Ok((mut stream, _addr)) => {
                    let client_address = match stream.peer_addr() {
                        Ok(address) => address.to_string(),
                        Err(e) => {
                            let _ = stream.write_all(e.to_string().as_bytes()).await;
                            break;
                        }
                    };

                    let server_clone = Arc::clone(&server);
                    let store_clone = Arc::clone(&store);
                    let state_clone = Arc::clone(&state);

                    tokio::spawn(async move {
                        handle_client_connection(
                            stream,
                            server_clone,
                            client_address,
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
