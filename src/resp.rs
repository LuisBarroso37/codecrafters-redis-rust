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
    /// Null string value (represented as "$-1\r\n")
    NullBulkString,
    /// Null array value (represented as "*-1\r\n")
    NullArray,
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
        let Some(prefix) = value.chars().next() else {
            return Err(RespError::UnknownRespType);
        };

        let content = &value[1..];

        match prefix {
            '$' => Self::decode_bulk_string(content, rest_of_data),
            '+' => Ok(RespValue::SimpleString(content.to_string())),
            ':' => {
                let integer = content
                    .parse::<i64>()
                    .map_err(|_| RespError::FailedToParseInteger)?;

                Ok(RespValue::Integer(integer))
            }
            '*' => Self::decode_array(content, rest_of_data),
            _ => Err(RespError::UnknownRespType),
        }
    }

    /// Helper function to decode RESP bulk strings.
    ///
    /// Bulk strings are binary-safe strings that can contain any sequence of bytes.
    /// They are encoded as `$<length>\r\n<data>\r\n` where length is the number
    /// of bytes in the data portion. Special case: `$-1\r\n` represents a null bulk string.
    ///
    /// # Arguments
    ///
    /// * `length_str` - The length portion of the bulk string (without the '$' prefix)
    /// * `rest_of_data` - Iterator over remaining lines containing the actual string data
    ///
    /// # Returns
    ///
    /// * `Ok(RespValue::BulkString)` - Successfully decoded bulk string
    /// * `Ok(RespValue::NullBulkString)` - If length is -1 (null bulk string)
    /// * `Err(RespError::InvalidBulkString)` - If length parsing fails, length is negative (except -1),
    ///   no data line follows, or data length doesn't match declared length
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // For input "$5\r\nhello\r\n"
    /// decode_bulk_string("5", &mut iter) // Returns: Ok(BulkString("hello"))
    ///
    /// // For null bulk string "$-1\r\n"
    /// decode_bulk_string("-1", &mut iter) // Returns: Ok(NullBulkString)
    /// ```
    fn decode_bulk_string(
        length_str: &str,
        rest_of_data: &mut Iter<'_, &str>,
    ) -> Result<RespValue, RespError> {
        let bulk_string_length = length_str
            .parse::<i32>()
            .map_err(|_| RespError::InvalidBulkString)?;

        // Handle null bulk string
        if bulk_string_length == -1 {
            return Ok(RespValue::NullBulkString);
        }

        // Negative lengths other than -1 are invalid
        if bulk_string_length < 0 {
            return Err(RespError::InvalidBulkString);
        }

        let Some(next_line) = rest_of_data.next() else {
            return Err(RespError::InvalidBulkString);
        };

        if next_line.len() != bulk_string_length as usize {
            return Err(RespError::InvalidBulkString);
        }

        Ok(RespValue::BulkString(next_line.to_string()))
    }

    /// Helper function to decode RESP arrays.
    ///
    /// Arrays are ordered collections of RESP values encoded as `*<count>\r\n<element1><element2>...`
    /// where count is the number of elements. Each element is a complete RESP value.
    /// Special case: `*-1\r\n` represents a null array.
    ///
    /// # Arguments
    ///
    /// * `length_str` - The count portion of the array (without the '*' prefix)
    /// * `rest_of_data` - Iterator over remaining lines containing the array elements
    ///
    /// # Returns
    ///
    /// * `Ok(RespValue::Array)` - Successfully decoded array with all elements
    /// * `Ok(RespValue::NullArray)` - If count is -1 (null array)
    /// * `Err(RespError::InvalidArray)` - If count parsing fails, count is negative (except -1),
    ///   insufficient data for declared number of elements, or any element fails to decode
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // For input "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n"
    /// decode_array("2", &mut iter) // Returns: Ok(Array([BulkString("hello"), BulkString("world")]))
    ///
    /// // For null array "*-1\r\n"
    /// decode_array("-1", &mut iter) // Returns: Ok(NullArray)
    /// ```
    fn decode_array(
        length_str: &str,
        rest_of_data: &mut Iter<'_, &str>,
    ) -> Result<RespValue, RespError> {
        let array_length = length_str
            .parse::<i32>()
            .map_err(|_| RespError::InvalidArray)?;

        // Handle null array
        if array_length == -1 {
            return Ok(RespValue::NullArray);
        }

        // Negative lengths other than -1 are invalid
        if array_length < 0 {
            return Err(RespError::InvalidArray);
        }

        let array_length = array_length as usize;
        let mut array_elements: Vec<RespValue> = Vec::with_capacity(array_length);

        while array_elements.len() < array_length {
            let Some(next_element) = rest_of_data.next() else {
                return Err(RespError::InvalidArray);
            };

            let decoded_element = Self::decode(next_element, rest_of_data)?;
            array_elements.push(decoded_element);
        }

        Ok(RespValue::Array(array_elements))
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
    /// ```ignore
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
            RespValue::NullBulkString => {
                format!("$-1\r\n")
            }
            RespValue::NullArray => {
                format!("*0\r\n")
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
    /// ```ignore
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

#[cfg(test)]
mod tests {
    use super::{RespError, RespValue};

    #[test]
    fn test_parse_to_resp_values() {
        let test_cases = vec![
            (
                vec!["*3", "$5", "RPUSH", "$10", "strawberry", "$5", "apple"],
                Ok(vec![RespValue::Array(vec![
                    RespValue::BulkString("RPUSH".into()),
                    RespValue::BulkString("strawberry".into()),
                    RespValue::BulkString("apple".into()),
                ])]),
            ),
            (
                vec![
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
                ],
                Ok(vec![RespValue::Array(vec![
                    RespValue::Array(vec![
                        RespValue::BulkString("pear".into()),
                        RespValue::BulkString("strawberry".into()),
                    ]),
                    RespValue::BulkString("apple".into()),
                    RespValue::BulkString("banana".into()),
                ])]),
            ),
        ];

        for (input, expected) in test_cases {
            assert_eq!(RespValue::parse(input), expected,);
        }
    }

    #[test]
    fn test_decode_bulk_string() {
        let test_cases = vec![
            (
                "5",
                vec!["hello"],
                Ok(RespValue::BulkString("hello".to_string())),
            ),
            ("0", vec![""], Ok(RespValue::BulkString("".to_string()))),
            (
                "11",
                vec!["hello world"],
                Ok(RespValue::BulkString("hello world".to_string())),
            ),
            (
                "3",
                vec!["123"],
                Ok(RespValue::BulkString("123".to_string())),
            ),
            ("-1", vec![], Ok(RespValue::NullBulkString)),
            ("invalid", vec!["hello"], Err(RespError::InvalidBulkString)),
            ("-2", vec![], Err(RespError::InvalidBulkString)),
            ("5", vec![], Err(RespError::InvalidBulkString)), // Missing data
            ("5", vec!["hi"], Err(RespError::InvalidBulkString)), // Length mismatch
            ("3", vec!["hello"], Err(RespError::InvalidBulkString)), // Length mismatch
        ];

        for (length_str, data, expected) in test_cases {
            let mut iter = data.iter();
            let result = RespValue::decode_bulk_string(length_str, &mut iter);
            assert_eq!(
                result, expected,
                "Failed for length_str: '{}', data: {:?}",
                length_str, data
            );
        }
    }

    #[test]
    fn test_decode_array() {
        let test_cases = vec![
            (
                "2",
                vec!["$5", "hello", "$5", "world"],
                Ok(RespValue::Array(vec![
                    RespValue::BulkString("hello".to_string()),
                    RespValue::BulkString("world".to_string()),
                ])),
            ),
            ("0", vec![], Ok(RespValue::Array(vec![]))),
            (
                "1",
                vec![":42"],
                Ok(RespValue::Array(vec![RespValue::Integer(42)])),
            ),
            (
                "3",
                vec!["+OK", ":123", "$4", "test"],
                Ok(RespValue::Array(vec![
                    RespValue::SimpleString("OK".to_string()),
                    RespValue::Integer(123),
                    RespValue::BulkString("test".to_string()),
                ])),
            ),
            (
                "2",
                vec!["*1", ":1", "*1", ":2"],
                Ok(RespValue::Array(vec![
                    RespValue::Array(vec![RespValue::Integer(1)]),
                    RespValue::Array(vec![RespValue::Integer(2)]),
                ])),
            ),
            ("-1", vec![], Ok(RespValue::NullArray)),
            ("invalid", vec![], Err(RespError::InvalidArray)),
            ("-2", vec![], Err(RespError::InvalidArray)),
            ("2", vec![":1"], Err(RespError::InvalidArray)), // Missing element
            ("1", vec![], Err(RespError::InvalidArray)),     // No elements provided
            ("1", vec!["invalid"], Err(RespError::UnknownRespType)), // Invalid element type
        ];

        for (length_str, data, expected) in test_cases {
            let mut iter = data.iter();
            let result = RespValue::decode_array(length_str, &mut iter);
            assert_eq!(
                result, expected,
                "Failed for length_str: '{}', data: {:?}",
                length_str, data
            );
        }
    }

    #[test]
    fn test_decode() {
        let test_cases = vec![
            ("+OK", vec![], Ok(RespValue::SimpleString("OK".to_string()))),
            (
                "+PONG",
                vec![],
                Ok(RespValue::SimpleString("PONG".to_string())),
            ),
            ("+", vec![], Ok(RespValue::SimpleString("".to_string()))),
            (":42", vec![], Ok(RespValue::Integer(42))),
            (":0", vec![], Ok(RespValue::Integer(0))),
            (":-123", vec![], Ok(RespValue::Integer(-123))),
            (":invalid", vec![], Err(RespError::FailedToParseInteger)),
            (
                "$5",
                vec!["hello"],
                Ok(RespValue::BulkString("hello".to_string())),
            ),
            ("$-1", vec![], Ok(RespValue::NullBulkString)),
            ("*0", vec![], Ok(RespValue::Array(vec![]))),
            ("*-1", vec![], Ok(RespValue::NullArray)),
            (
                "*2",
                vec![":1", ":2"],
                Ok(RespValue::Array(vec![
                    RespValue::Integer(1),
                    RespValue::Integer(2),
                ])),
            ),
            ("", vec![], Err(RespError::UnknownRespType)),
            ("?invalid", vec![], Err(RespError::UnknownRespType)),
            ("@test", vec![], Err(RespError::UnknownRespType)),
        ];

        for (value, data, expected) in test_cases {
            let mut iter = data.iter();
            let result = RespValue::decode(value, &mut iter);
            assert_eq!(
                result, expected,
                "Failed for value: '{}', data: {:?}",
                value, data
            );
        }
    }

    #[test]
    fn test_encode() {
        let test_cases = vec![
            (RespValue::SimpleString("OK".to_string()), "+OK\r\n"),
            (RespValue::SimpleString("".to_string()), "+\r\n"),
            (
                RespValue::Error("Error message".to_string()),
                "-Error message\r\n",
            ),
            (RespValue::Integer(42), ":42\r\n"),
            (RespValue::Integer(-123), ":-123\r\n"),
            (RespValue::Integer(0), ":0\r\n"),
            (
                RespValue::BulkString("hello".to_string()),
                "$5\r\nhello\r\n",
            ),
            (RespValue::BulkString("".to_string()), "$0\r\n\r\n"),
            (
                RespValue::BulkString("hello world".to_string()),
                "$11\r\nhello world\r\n",
            ),
            (RespValue::NullBulkString, "$-1\r\n"),
            (RespValue::NullArray, "*0\r\n"),
            (
                RespValue::Array(vec![
                    RespValue::BulkString("hello".to_string()),
                    RespValue::BulkString("world".to_string()),
                ]),
                "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n",
            ),
            (RespValue::Array(vec![]), "*0\r\n"),
            (
                RespValue::Array(vec![
                    RespValue::SimpleString("OK".to_string()),
                    RespValue::Integer(42),
                    RespValue::BulkString("test".to_string()),
                ]),
                "*3\r\n+OK\r\n:42\r\n$4\r\ntest\r\n",
            ),
            (
                RespValue::Array(vec![
                    RespValue::Array(vec![RespValue::Integer(1)]),
                    RespValue::Array(vec![RespValue::Integer(2)]),
                ]),
                "*2\r\n*1\r\n:1\r\n*1\r\n:2\r\n",
            ),
        ];

        for (input, expected) in test_cases {
            let result = input.encode();
            assert_eq!(result, expected, "Failed encoding: {:?}", input);
        }
    }

    #[test]
    fn test_encode_array_from_strings() {
        let test_cases = vec![
            (vec![], "*0\r\n"),
            (vec!["hello".to_string()], "*1\r\n$5\r\nhello\r\n"),
            (
                vec!["hello".to_string(), "world".to_string()],
                "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n",
            ),
            (
                vec!["".to_string(), "test".to_string(), "123".to_string()],
                "*3\r\n$0\r\n\r\n$4\r\ntest\r\n$3\r\n123\r\n",
            ),
        ];

        for (input, expected) in test_cases {
            let result = RespValue::encode_array_from_strings(input.clone());
            assert_eq!(
                result, expected,
                "Failed encoding array from strings: {:?}",
                input
            );
        }
    }
}
