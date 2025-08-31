use std::slice::Iter;

use thiserror::Error;

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
    pub fn as_string(&self) -> String {
        match self {
            RespError::UnknownRespType => {
                RespValue::Error("ERR unknown RESP type".to_string()).encode()
            }
            RespError::FailedToParseInteger => {
                RespValue::Error("ERR failed to parse integer".to_string()).encode()
            }
            RespError::InvalidBulkString => {
                RespValue::Error("ERR invalid bulk string".to_string()).encode()
            }
            RespError::InvalidArray => RespValue::Error("ERR invalid array".to_string()).encode(),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(String),
    Array(Vec<RespValue>),
    NullBulkString,
    NullArray,
}

impl RespValue {
    pub fn parse(data: Vec<&str>) -> Result<Vec<RespValue>, RespError> {
        let mut data_iter = data.iter();
        let mut vec = Vec::new();

        while let Some(value) = data_iter.next() {
            let decoded = Self::decode(value, &mut data_iter)?;
            vec.push(decoded);
        }

        Ok(vec)
    }

    pub fn decode(value: &str, rest_of_data: &mut Iter<'_, &str>) -> Result<Self, RespError> {
        let Some(prefix) = value.chars().next() else {
            return Err(RespError::UnknownRespType);
        };

        let content = &value[1..];

        match prefix {
            '$' => Self::decode_bulk_string(content, rest_of_data),
            '+' => Ok(RespValue::SimpleString(content.to_string())),
            '-' => Ok(RespValue::Error(content.to_string())),
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
                format!("*-1\r\n")
            }
        }
    }

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
            (RespValue::NullArray, "*-1\r\n"),
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
