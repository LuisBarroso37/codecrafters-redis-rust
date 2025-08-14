use std::sync::Arc;

use regex::Regex;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::RwLock;

use crate::commands::{CommandError, CommandHandler};
use crate::resp::{RespError, RespValue};
use crate::server::RedisServer;

#[derive(Error, Debug, PartialEq)]
pub enum CommandReadError {
    #[error("I/O error: {0}")]
    IoError(String),
    #[error("Connection closed")]
    ConnectionClosed,
    #[error("Invalid UTF-8 sequence")]
    InvalidUtf8(#[from] std::str::Utf8Error),
    #[error("RESP parse error")]
    RespParseError(#[from] RespError),
    #[error("Failed to get client address")]
    ClientAddressError,
    #[error("Command construction error")]
    CommandError(#[from] CommandError),
    #[error("Invalid response from master")]
    InvalidResponseFromMaster,
}

impl CommandReadError {
    pub fn as_string(&self) -> String {
        match self {
            CommandReadError::IoError(msg) => RespValue::Error(format!("Err {}", msg)).encode(),
            CommandReadError::ConnectionClosed => {
                RespValue::Error("Err connection closed".to_string()).encode()
            }
            CommandReadError::InvalidUtf8(err) => RespValue::Error(format!("Err {}", err)).encode(),
            CommandReadError::RespParseError(err) => err.as_string(),
            CommandReadError::ClientAddressError => {
                RespValue::Error("Err failed to get client address".to_string()).encode()
            }
            CommandReadError::CommandError(err) => err.as_string(),
            CommandReadError::InvalidResponseFromMaster => {
                RespValue::Error("Err invalid response from master".to_string()).encode()
            }
        }
    }
}

/// Parses raw input bytes into string lines for RESP processing.
///
/// Converts the byte stream from a TCP connection into string lines
/// that can be processed by the RESP parser. Handles UTF-8 validation
/// and splits on RESP line terminators.
///
/// # Arguments
///
/// * `input` - Raw bytes received from the client
///
/// # Returns
///
/// * `Ok(Vec<&str>)` - Vector of string lines ready for RESP parsing
/// * `Err(CommandReadError::InvalidUtf8)` - If the input contains invalid UTF-8
///
/// # Examples
///
/// ```ignore
/// let input = b"*2\r\n$4\r\nPING\r\n";
/// let lines = parse_input(input)?;
/// // Returns: vec!["*2", "$4", "PING"]
/// ```
pub fn parse_input(input: &[u8]) -> Result<Vec<&str>, CommandReadError> {
    let str = str::from_utf8(input)?;

    Ok(str
        .split_terminator("\r\n")
        .filter(|s| !s.contains("\0"))
        .collect::<Vec<&str>>())
}

async fn read_and_parse_resp(stream: &mut TcpStream) -> Result<Vec<RespValue>, CommandReadError> {
    let mut buf = [0; 1024];
    let number_of_bytes = match stream.read(&mut buf).await {
        Ok(n) => n,
        Err(e) => return Err(CommandReadError::IoError(e.to_string())),
    };

    if number_of_bytes == 0 {
        return Err(CommandReadError::ConnectionClosed);
    }

    let input = parse_input(&buf)?;
    let parsed_input = RespValue::parse(input)?;

    Ok(parsed_input)
}

pub async fn read_and_parse_command(
    stream: &mut TcpStream,
) -> Result<(CommandHandler, String), CommandReadError> {
    let parsed_input = read_and_parse_resp(stream).await?;

    let client_address = match stream.peer_addr() {
        Ok(address) => address.to_string(),
        Err(_) => return Err(CommandReadError::ClientAddressError),
    };

    let command_handler = CommandHandler::new(parsed_input)?;

    Ok((command_handler, client_address))
}

pub async fn handshake(
    stream: &mut TcpStream,
    server: &Arc<RwLock<RedisServer>>,
) -> Result<(), CommandReadError> {
    let response = send_and_handle_handshake_command(
        stream,
        RespValue::Array(vec![RespValue::BulkString("PING".to_string())]),
    )
    .await?;

    if response != RespValue::SimpleString("PONG".to_string()) {
        return Err(CommandReadError::InvalidResponseFromMaster);
    }

    {
        let server_guard = server.read().await;
        let response = send_and_handle_handshake_command(
            stream,
            RespValue::Array(vec![
                RespValue::BulkString("REPLCONF".to_string()),
                RespValue::BulkString("listening-port".to_string()),
                RespValue::BulkString(server_guard.port.to_string()),
            ]),
        )
        .await?;

        if response != RespValue::SimpleString("OK".to_string()) {
            return Err(CommandReadError::InvalidResponseFromMaster);
        }
    }

    let response = send_and_handle_handshake_command(
        stream,
        RespValue::Array(vec![
            RespValue::BulkString("REPLCONF".to_string()),
            RespValue::BulkString("capa".to_string()),
            RespValue::BulkString("psync2".to_string()),
        ]),
    )
    .await?;

    if response != RespValue::SimpleString("OK".to_string()) {
        return Err(CommandReadError::InvalidResponseFromMaster);
    }

    let response = send_and_handle_handshake_command(
        stream,
        RespValue::Array(vec![
            RespValue::BulkString("PSYNC".to_string()),
            RespValue::BulkString("?".to_string()),
            RespValue::BulkString("-1".to_string()),
        ]),
    )
    .await?;

    match response {
        RespValue::Array(split_response) => {
            if split_response[0] == RespValue::BulkString("FULLRESYNC".to_string()) {
                let parts: Vec<&str> = split_response[1..]
                    .iter()
                    .filter_map(|v| {
                        if let RespValue::BulkString(s) = v {
                            Some(s.as_str())
                        } else {
                            None
                        }
                    })
                    .collect();

                if parts.len() != 3 || !is_valid_repl_id(parts[1]) || parts[2] == "0" {
                    return Err(CommandReadError::InvalidResponseFromMaster);
                }
            } else {
                return Err(CommandReadError::InvalidResponseFromMaster);
            }
        }
        _ => {
            return Err(CommandReadError::InvalidResponseFromMaster);
        }
    };

    Ok(())
}

fn is_valid_repl_id(repl_id: &str) -> bool {
    let re = Regex::new(r"^[a-zA-Z0-9]{40}$").unwrap();
    re.is_match(repl_id)
}

async fn send_and_handle_handshake_command(
    stream: &mut TcpStream,
    command: RespValue,
) -> Result<RespValue, CommandReadError> {
    stream
        .write_all(command.encode().as_bytes())
        .await
        .map_err(|e| CommandReadError::IoError(e.to_string()))?;
    stream
        .flush()
        .await
        .map_err(|e| CommandReadError::IoError(e.to_string()))?;

    let resp_value = read_and_parse_resp(stream).await?;

    if resp_value.len() != 1 {
        return Err(CommandReadError::InvalidResponseFromMaster);
    }

    Ok(resp_value[0].clone())
}

#[cfg(test)]
mod tests {
    use super::parse_input;

    #[test]
    fn test_parse_input() {
        let test_cases = vec![
            (
                "*3\r\n$5\r\nRPUSH\r\n$10\r\nstrawberry\r\n$5\r\napple\r\n".as_bytes(),
                Ok(vec![
                    "*3",
                    "$5",
                    "RPUSH",
                    "$10",
                    "strawberry",
                    "$5",
                    "apple",
                ]),
            ),
            (
                "*3\r\n*2\r\n$4\r\npear\r\n$10\r\nstrawberry\r\n$5\r\napple\r\n$6\r\nbanana\r\n"
                    .as_bytes(),
                Ok(vec![
                    "*3",
                    "*2",
                    "$4",
                    "pear",
                    "$10",
                    "strawberry",
                    "$5",
                    "apple",
                    "$6",
                    "banana",
                ]),
            ),
        ];

        for (input, expected) in test_cases {
            assert_eq!(
                parse_input(input),
                expected,
                "parsing input {}",
                String::from_utf8_lossy(input)
            );
        }
    }
}
