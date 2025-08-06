use std::slice::Iter;

use thiserror::Error;

/// Errors that can occur during RESP (Redis Serialization Protocol) parsing.
///
/// RESP is the protocol used by Redis for communication between clients and servers.
/// These errors represent various parsing failures that can occur.
#[derive(Error, Debug, PartialEq)]
pub enum RespError {
    #[error("unknown RESP type")]
    UnknownRespType,
    #[error("failed to parse integer")]
    FailedToParseInteger,
    #[error("invalid bulk string")]
    InvalidBulkString,
    #[error("invalid array")]
    InvalidArray,
}

impl RespError {
    /// Converts the error to bytes suitable for sending as a Redis error response.
    ///
    /// Returns RESP-formatted error messages that can be sent directly to clients.
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            RespError::UnknownRespType => b"-ERR unknown RESP type\r\n",
            RespError::FailedToParseInteger => b"-ERR failed to parse integer\r\n",
            RespError::InvalidBulkString => b"-ERR invalid bulk string\r\n",
            RespError::InvalidArray => b"-ERR invalid array\r\n",
        }
    }
}

/// Represents a value in the Redis Serialization Protocol (RESP).
///
/// RESP supports several data types that correspond to different Redis values:
/// - SimpleString: Single-line strings without spaces (e.g., "OK", "PONG")
/// - Error: Error messages from the server
/// - Integer: 64-bit signed integers
/// - BulkString: Binary-safe strings of any length
/// - Array: Ordered collections of RESP values
/// - Null: Represents absence of a value
#[derive(Debug, PartialEq)]
pub enum RespValue {
    /// Simple string values (prefixed with '+')
    SimpleString(String),
    /// Error messages (prefixed with '-')
    Error(String),
    /// Integer values (prefixed with ':')
    Integer(i64),
    /// Binary-safe string values (prefixed with '$')
    BulkString(String),
    /// Arrays of RESP values (prefixed with '*')
    Array(Vec<RespValue>),
    /// Null value (represented as "$-1\r\n")
    Null,
}

impl RespValue {
    /// Parses a vector of string lines into RESP values.
    ///
    /// Takes raw protocol data split by lines and converts it into
    /// structured RESP values that can be processed by command handlers.
    ///
    /// # Arguments
    ///
    /// * `data` - Vector of string lines from the RESP protocol
    ///
    /// # Returns
    ///
    /// * `Ok(Vec<RespValue>)` - Successfully parsed RESP values
    /// * `Err(RespError)` - If parsing fails due to invalid format
    pub fn parse(data: Vec<&str>) -> Result<Vec<RespValue>, RespError> {
        let mut data_iter = data.iter();

        let mut vec = Vec::new();

        while let Some(value) = data_iter.next() {
            let decoded = Self::decode(value, &mut data_iter)?;
            vec.push(decoded);
        }

        Ok(vec)
    }

    /// Decodes a single RESP value from a string and iterator of remaining data.
    ///
    /// This is the core parsing function that handles different RESP type prefixes
    /// and extracts the appropriate data from the protocol stream.
    ///
    /// # Arguments
    ///
    /// * `value` - The current line containing the RESP type prefix and metadata
    /// * `rest_of_data` - Iterator over remaining lines for multi-line values
    ///
    /// # Returns
    ///
    /// * `Ok(RespValue)` - Successfully decoded RESP value
    /// * `Err(RespError)` - If the value format is invalid
    pub fn decode(value: &str, rest_of_data: &mut Iter<'_, &str>) -> Result<Self, RespError> {
        if value.starts_with("$") {
            let bulk_string_info = value.trim_start_matches("$");

            if let Some(next_line) = rest_of_data.next() {
                let bulk_string_length = bulk_string_info
                    .parse::<usize>()
                    .map_err(|_| RespError::InvalidBulkString)?;

                if next_line.len() == bulk_string_length {
                    Ok(RespValue::BulkString(next_line.to_string()))
                } else {
                    return Err(RespError::InvalidBulkString);
                }
            } else {
                return Err(RespError::InvalidBulkString);
            }
        } else if value.starts_with("+") {
            let content = value.trim_start_matches("+").to_string();
            Ok(RespValue::SimpleString(content))
        } else if value.starts_with(":") {
            let content = value.trim_start_matches(":");

            if let Ok(value) = content.parse::<i64>() {
                Ok(RespValue::Integer(value))
            } else {
                Err(RespError::FailedToParseInteger)
            }
        } else if value.starts_with("*") {
            let array_info = value.trim_start_matches("*");

            let array_length = array_info
                .parse::<usize>()
                .map_err(|_| RespError::FailedToParseInteger)?;

            let mut array_elements: Vec<RespValue> = Vec::with_capacity(array_length);

            while array_elements.len() < array_length {
                if let Some(mut next_element) = rest_of_data.next() {
                    let decoded_element = Self::decode(&mut next_element, rest_of_data)?;
                    array_elements.push(decoded_element);
                } else {
                    return Err(RespError::InvalidArray);
                }
            }

            Ok(RespValue::Array(array_elements))
        } else {
            return Err(RespError::UnknownRespType);
        }
    }

    /// Encodes a RESP value back into its wire format.
    ///
    /// Converts a structured RESP value into the string format that can be
    /// sent over the network to Redis clients. Each value type has its own
    /// encoding format with appropriate prefixes and terminators.
    ///
    /// # Returns
    ///
    /// * `String` - The RESP-encoded string representation
    ///
    /// # Examples
    ///
    /// ```
    /// let value = RespValue::SimpleString("OK".to_string());
    /// assert_eq!(value.encode(), "+OK\r\n");
    ///
    /// let value = RespValue::Integer(42);
    /// assert_eq!(value.encode(), ":42\r\n");
    /// ```
    pub fn encode(&self) -> String {
        match self {
            RespValue::SimpleString(s) => {
                format!("+{}\r\n", s)
            }
            RespValue::Error(e) => {
                format!("-{}\r\n", e)
            }
            RespValue::Integer(i) => {
                format!(":{}\r\n", i)
            }
            RespValue::BulkString(s) => {
                format!("${}\r\n{}\r\n", s.len(), s)
            }
            RespValue::Array(elements) => {
                let mut encoded_elements = Vec::new();
                for element in elements {
                    encoded_elements.push(element.encode());
                }

                format!(
                    "*{}\r\n{}",
                    encoded_elements.len(),
                    encoded_elements.join("")
                )
            }
            RespValue::Null => {
                format!("$-1\r\n")
            }
        }
    }

    /// Convenience method to encode a vector of strings as a RESP array.
    ///
    /// Creates a RESP array where each string becomes a bulk string element.
    /// This is commonly used for encoding Redis command responses that contain
    /// multiple string values.
    ///
    /// # Arguments
    ///
    /// * `elements` - Vector of strings to encode as an array
    ///
    /// # Returns
    ///
    /// * `String` - RESP-encoded array of bulk strings
    ///
    /// # Examples
    ///
    /// ```
    /// let elements = vec!["hello".to_string(), "world".to_string()];
    /// let encoded = RespValue::encode_array_from_strings(elements);
    /// // Returns: "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n"
    /// ```
    pub fn encode_array_from_strings(elements: Vec<String>) -> String {
        let mut encoded_elements = Vec::new();
        for element in elements {
            encoded_elements.push(RespValue::BulkString(element).encode());
        }

        format!(
            "*{}\r\n{}",
            encoded_elements.len(),
            encoded_elements.join("")
        )
    }
}
