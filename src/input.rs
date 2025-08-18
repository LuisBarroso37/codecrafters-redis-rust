//! Input handling and network communication for Redis server.
//!
//! This module provides functionality for reading and parsing commands from network streams,
//! handling the Redis replication handshake process, and managing communication with master
//! servers in replica mode. It bridges the gap between raw TCP data and parsed RESP commands.

use std::sync::Arc;

use regex::Regex;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::RwLock;

use crate::commands::CommandError;
use crate::resp::{RespError, RespValue};
use crate::server::RedisServer;

/// Errors that can occur while reading and parsing commands from network streams.
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

/// Reads raw data from a stream and parses it into RESP values.
///
/// This function handles the low-level reading from network streams and converts
/// the raw bytes into parsed RESP protocol values that can be processed as Redis commands.
///
/// # Type Parameters
///
/// * `R` - Any type that implements AsyncReadExt and Unpin (typically a TcpStream)
///
/// # Arguments
///
/// * `stream` - The stream to read data from
/// * `buffer` - A mutable buffer to store the read data (must be exactly 1024 bytes)
///
/// # Returns
///
/// * `Ok(Vec<RespValue>)` - Successfully parsed RESP values
/// * `Err(CommandReadError::IoError)` - If reading from the stream fails
/// * `Err(CommandReadError::ConnectionClosed)` - If the connection is closed (0 bytes read)
/// * `Err(CommandReadError::InvalidUtf8)` - If the data contains invalid UTF-8
/// * `Err(CommandReadError::RespParseError)` - If RESP parsing fails
pub async fn read_and_parse_resp<R>(
    stream: &mut R,
    buffer: &mut [u8; 1024],
) -> Result<Vec<RespValue>, CommandReadError>
where
    R: AsyncReadExt + Unpin,
{
    let number_of_bytes = match stream.read(buffer).await {
        Ok(n) => n,
        Err(e) => return Err(CommandReadError::IoError(e.to_string())),
    };

    if number_of_bytes == 0 {
        return Err(CommandReadError::ConnectionClosed);
    }

    let input = parse_input(&buffer[..number_of_bytes])?;
    let parsed_input = RespValue::parse(input)?;

    Ok(parsed_input)
}

/// Performs the Redis replication handshake between replica and master.
///
/// This function implements the complete Redis replication handshake protocol:
/// 1. PING/PONG exchange to verify connectivity
/// 2. REPLCONF listening-port to announce replica's port
/// 3. REPLCONF capa psync2 to negotiate protocol capabilities  
/// 4. PSYNC command to initiate replication
/// 5. Receiving the RDB snapshot from the master
///
/// After successful handshake, the replica is ready to receive ongoing
/// replication commands from the master.
///
/// # Arguments
///
/// * `stream` - Mutable TCP stream connected to the master server
/// * `server` - Thread-safe reference to the replica server configuration
///
/// # Returns
///
/// * `Ok(())` - Handshake completed successfully
/// * `Err(CommandReadError)` - If any step of the handshake fails
///
/// # Protocol Details
///
/// The handshake follows the Redis replication protocol:
/// - Master must respond with PONG to PING
/// - Master must respond with OK to both REPLCONF commands
/// - Master must respond with FULLRESYNC to PSYNC
/// - Master must send a valid RDB file after FULLRESYNC
pub async fn handshake(
    stream: &mut TcpStream,
    server: Arc<RwLock<RedisServer>>,
) -> Result<(), CommandReadError> {
    let mut buffer: [u8; 1024] = [0; 1024];

    let response = send_and_handle_handshake_command(
        &mut buffer,
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
            &mut buffer,
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
        &mut buffer,
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

    let response = send_and_handle_psync_command(
        stream,
        RespValue::Array(vec![
            RespValue::BulkString("PSYNC".to_string()),
            RespValue::BulkString("?".to_string()),
            RespValue::BulkString("-1".to_string()),
        ]),
    )
    .await?;

    match response {
        RespValue::SimpleString(fullresync_line) => {
            let parts: Vec<&str> = fullresync_line.split_whitespace().collect();

            if parts.len() != 3 || parts[0] != "FULLRESYNC" {
                return Err(CommandReadError::InvalidResponseFromMaster);
            }

            let repl_id = parts[1];
            let offset = parts[2];

            if !is_valid_repl_id(repl_id) || offset != "0" {
                return Err(CommandReadError::InvalidResponseFromMaster);
            }
        }
        _ => {
            return Err(CommandReadError::InvalidResponseFromMaster);
        }
    }

    // Now separately receive the RDB file
    receive_rdb_file(stream).await?;

    Ok(())
}

async fn send_and_handle_handshake_command(
    buffer: &mut [u8; 1024],
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

    let resp_value = read_and_parse_resp(stream, buffer).await?;

    if resp_value.len() != 1 {
        return Err(CommandReadError::InvalidResponseFromMaster);
    }

    Ok(resp_value[0].clone())
}

fn is_valid_repl_id(repl_id: &str) -> bool {
    let re = Regex::new(r"^[a-zA-Z0-9]{40}$").unwrap();
    re.is_match(repl_id)
}

async fn send_and_handle_psync_command(
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

    // Read only the FULLRESYNC line, byte by byte to avoid reading RDB data
    let mut line = Vec::new();
    let mut byte = [0u8; 1];

    // Read the '+' at the beginning
    stream
        .read_exact(&mut byte)
        .await
        .map_err(|e| CommandReadError::IoError(e.to_string()))?;

    if byte[0] != b'+' {
        return Err(CommandReadError::InvalidResponseFromMaster);
    }

    // Read until \r\n
    loop {
        stream
            .read_exact(&mut byte)
            .await
            .map_err(|e| CommandReadError::IoError(e.to_string()))?;

        line.push(byte[0]);

        if line.len() >= 2 && line[line.len() - 2] == b'\r' && line[line.len() - 1] == b'\n' {
            break;
        }
    }

    let fullresync_line =
        String::from_utf8(line).map_err(|_| CommandReadError::InvalidResponseFromMaster)?;

    // Remove the trailing \r\n
    let fullresync_line = fullresync_line.trim_end_matches("\r\n").to_string();

    Ok(RespValue::SimpleString(fullresync_line))
}

async fn receive_rdb_file(stream: &mut TcpStream) -> Result<(), CommandReadError> {
    // Read the RDB bulk string header ($<size>\r\n)
    let mut size_line = Vec::new();
    let mut byte = [0u8; 1];

    loop {
        stream
            .read_exact(&mut byte)
            .await
            .map_err(|e| CommandReadError::IoError(e.to_string()))?;

        size_line.push(byte[0]);

        if size_line.len() >= 2
            && size_line[size_line.len() - 2] == b'\r'
            && size_line[size_line.len() - 1] == b'\n'
        {
            break;
        }
    }

    // Parse RDB size
    let size_str = std::str::from_utf8(&size_line[1..size_line.len() - 2]) // Skip $ and \r\n
        .map_err(CommandReadError::InvalidUtf8)?;
    let rdb_size: usize = size_str
        .parse()
        .map_err(|_| CommandReadError::InvalidResponseFromMaster)?;

    // Stream the RDB data in chunks instead of loading all at once
    let mut total_received: usize = 0;
    let mut buffer: [u8; 4096] = [0; 4096]; // 4KB chunks

    while total_received < rdb_size {
        let remaining = rdb_size - total_received;
        let chunk_size = std::cmp::min(buffer.len(), remaining);

        stream
            .read_exact(&mut buffer[..chunk_size])
            .await
            .map_err(|e| CommandReadError::IoError(e.to_string()))?;

        total_received += chunk_size;

        // Optional: Process RDB chunk here if needed
        // For now, we just consume it
    }

    Ok(())
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

    #[test]
    fn test_is_valid_repl_id() {
        let test_cases = [
            (
                "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
                true,
                "valid 40-char hex string",
            ),
            (
                "8371B4FB1155B71F4a04d3e1bc3e18c4a990aeeb",
                true,
                "valid with mixed case",
            ),
            (
                "ABCDEF1234567890ABCDEF1234567890ABCDEF12",
                true,
                "all uppercase hex",
            ),
            (
                "abcdef1234567890abcdef1234567890abcdef12",
                true,
                "all lowercase hex",
            ),
            (
                "8371b4fb1155b71f4a04d3e1bc3e18c4a990aezz",
                true,
                "alphanumeric chars (z is valid in this regex)",
            ),
            (
                "ABC1234567890123456789012345678901234XYZ",
                true,
                "alphanumeric with XYZ",
            ),
            (
                "8371b4fb1155b71f4a04d3e1bc3e18c4a990aee",
                false,
                "too short (39 chars)",
            ),
            (
                "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeba",
                false,
                "too long (41 chars)",
            ),
            (
                "8371b4fb1155b71f4a04d3e1bc3e18c4a990ae$g",
                false,
                "invalid characters ($)",
            ),
            ("", false, "empty string"),
            ("short", false, "very short string"),
            (
                "8371b4fb1155b71f4a04d3e1bc3e18c4a990ae g",
                false,
                "contains space",
            ),
            (
                "8371b4fb-1155-b71f-4a04-d3e1bc3e18c4a990aeeb",
                false,
                "contains hyphens",
            ),
        ];

        for (input, expected, description) in test_cases {
            let result = super::is_valid_repl_id(input);
            assert_eq!(result, expected, "Failed for {}: '{}'", description, input);
        }
    }
}
