use thiserror::Error;

/// Errors that can occur during input parsing.
///
/// These errors represent issues with the raw bytes received from clients
/// before RESP parsing begins.
#[derive(Error, Debug, PartialEq)]
pub enum InputError {
    #[error("invalid UTF-8 sequence")]
    InvalidUtf8,
}

impl InputError {
    /// Converts the error to bytes suitable for sending as a Redis error response.
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            InputError::InvalidUtf8 => b"-ERR invalid UTF-8 sequence\r\n",
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
/// * `Err(InputError::InvalidUtf8)` - If the input contains invalid UTF-8
///
/// # Examples
///
/// ```
/// let input = b"*2\r\n$4\r\nPING\r\n";
/// let lines = parse_input(input)?;
/// // Returns: vec!["*2", "$4", "PING"]
/// ```
pub fn parse_input(input: &[u8]) -> Result<Vec<&str>, InputError> {
    let str = str::from_utf8(input).map_err(|_| InputError::InvalidUtf8)?;

    Ok(str
        .split_terminator("\r\n")
        .filter(|s| !s.contains("\0"))
        .collect::<Vec<&str>>())
}
