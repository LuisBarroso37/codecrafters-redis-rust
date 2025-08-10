use bytes::BytesMut;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum RespError {
    #[error("invalid UTF-8 sequence")]
    InvalidUtf8,
    #[error("unknown RESP type")]
    UnknownRespType,
    #[error("failed to parse integer")]
    FailedToParseInteger,
    #[error("invalid bulk string")]
    InvalidBulkString,
    #[error("invalid array")]
    InvalidArray,
}

#[derive(Debug)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(String),
    Array(Vec<RespValue>),
}

impl RespValue {
    pub fn parse(bytes: &mut BytesMut) -> Result<Vec<RespValue>, RespError> {
        let mut vec = Vec::new();
        
        while let Some(mut line) = Self::parse_line(bytes) {
            let decoded = Self::decode(&mut line, bytes)?;
            vec.push(decoded);
        }

        Ok(vec)
    }

    pub fn parse_line(bytes: &mut BytesMut) -> Option<BytesMut> {
        if let Some(pos) = bytes.windows(2).position(|window| window == b"\r\n") {
            let line = bytes.split_to(pos + 2);
            Some(line)
        } else {
            None
        }
    }

    pub fn decode(line: &BytesMut, next: &mut BytesMut) -> Result<Self, RespError> {
        let data = str::from_utf8(line).map_err(|_| RespError::InvalidUtf8)?.trim_end_matches("\r\n");

        if data.starts_with("$") {
            let bulk_string_info = data.trim_start_matches("$");

            if let Some(next_line) = Self::parse_line(next) {
                let bulk_string_length = bulk_string_info
                    .parse::<usize>()
                    .map_err(|_| RespError::InvalidBulkString)?;

                let bulk_string = str::from_utf8(&next_line).map_err(|_| RespError::InvalidUtf8)?.trim_end_matches("\r\n");

                if bulk_string.len() == bulk_string_length {
                    Ok(RespValue::BulkString(bulk_string.to_string()))
                } else {
                    return Err(RespError::InvalidBulkString);
                }
            } else {
                return Err(RespError::InvalidBulkString);
            }
        } else if data.starts_with("+") {
            let content = data.trim_start_matches("+").to_string();
            Ok(RespValue::SimpleString(content))
        } else if data.starts_with(":") {
            let content = data.trim_start_matches(":");

            if let Ok(value) = content.parse::<i64>() {
                Ok(RespValue::Integer(value))
            } else {
                Err(RespError::FailedToParseInteger)
            }
        } else if data.starts_with("*") {
            let array_info = data.trim_start_matches("*");

            let array_length = array_info
                .parse::<usize>()
                .map_err(|_| RespError::FailedToParseInteger)?;

            let mut array_elements: Vec<RespValue> = Vec::with_capacity(array_length);

            while array_elements.len() < array_length {
                if let Some(mut next_element) = Self::parse_line(next) {
                    let decoded_element = Self::decode(&mut next_element, next)?;
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
}

fn main() {
    // let input: &[u8] = b"*3\r\n$5\r\nRPUSH\r\n$4\r\npear\r\n$9\r\nraspberry\r\n";
    let input: &[u8] = b"*3\r\n*2\r\n$5\r\nRPUSH\r\n$4\r\npear\r\n$9\r\nraspberry\r\n$6\r\nbanana\r\n";

    let mut bytes = BytesMut::from(input);

    let hey = RespValue::parse(&mut bytes).unwrap();
    match hey[0] {
        RespValue::Array(ref elements) => {
            for element in elements {
                match element {
                    RespValue::BulkString(s) => {
                        println!("Bulk string: {}", s);
                    }
                    RespValue::SimpleString(s) => {
                        println!("Simple string: {}", s);
                    }
                    RespValue::Integer(i) => {
                        println!("Integer: {}", i);
                    }
                    RespValue::Array(inner_elements) => {
                        println!("Array of length {}: {:?}", inner_elements.len(), inner_elements);
                    }
                    RespValue::Error(e) => {
                        println!("Error: {}", e);
                    }
                }
            }
        }
        _ => {
            println!("Expected an array");
        }
    }
}