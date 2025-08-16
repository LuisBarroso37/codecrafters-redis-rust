use std::sync::Arc;

use regex::Regex;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, RwLock};

use crate::commands::CommandError;
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

pub async fn read_and_parse_resp(
    stream: Arc<Mutex<TcpStream>>,
    buffer: &mut [u8; 1024],
) -> Result<Vec<RespValue>, CommandReadError> {
    let number_of_bytes = {
        let mut stream_guard = stream.lock().await;

        match stream_guard.read(buffer).await {
            Ok(n) => n,
            Err(e) => return Err(CommandReadError::IoError(e.to_string())),
        }
    };

    if number_of_bytes == 0 {
        return Err(CommandReadError::ConnectionClosed);
    }

    println!(
        "Received {} bytes - {:?}",
        number_of_bytes,
        String::from_utf8_lossy(&buffer[..number_of_bytes])
    );
    let input = parse_input(&buffer[..number_of_bytes])?;
    let parsed_input = RespValue::parse(input)?;

    Ok(parsed_input)
}

pub async fn handshake(
    stream: Arc<Mutex<TcpStream>>,
    server: Arc<RwLock<RedisServer>>,
) -> Result<(), CommandReadError> {
    let mut buffer: [u8; 1024] = [0; 1024];

    let response = send_and_handle_handshake_command(
        &mut buffer,
        Arc::clone(&stream),
        RespValue::Array(vec![RespValue::BulkString("PING".to_string())]),
    )
    .await?;

    if response != RespValue::SimpleString("PONG".to_string()) {
        return Err(CommandReadError::InvalidResponseFromMaster);
    }

    {
        let server_guard = server.read().await;
        let response = send_and_handle_handshake_command(
            &mut buffer,
            Arc::clone(&stream),
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
        &mut buffer,
        Arc::clone(&stream),
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
        &mut buffer,
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
            if split_response[0] != RespValue::BulkString("FULLRESYNC".to_string()) {
                return Err(CommandReadError::InvalidResponseFromMaster);
            }

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
            println!(
                "Received FULLRESYNC response, {:?} {:?}",
                split_response, parts
            );

            if parts.len() != 3 || !is_valid_repl_id(parts[1]) || parts[2] == "0" {
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
    buffer: &mut [u8; 1024],
    stream: Arc<Mutex<TcpStream>>,
    command: RespValue,
) -> Result<RespValue, CommandReadError> {
    {
        let mut stream_guard = stream.lock().await;

        stream_guard
            .write_all(command.encode().as_bytes())
            .await
            .map_err(|e| CommandReadError::IoError(e.to_string()))?;
        stream_guard
            .flush()
            .await
            .map_err(|e| CommandReadError::IoError(e.to_string()))?;
    }

    let resp_value = read_and_parse_resp(stream, buffer).await?;

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
