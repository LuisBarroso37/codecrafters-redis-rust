use std::slice::Iter;

use thiserror::Error;

#[derive(Error, Debug, PartialEq)]
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

impl RespError {
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            RespError::InvalidUtf8 => b"-ERR invalid UTF-8 sequence\r\n",
            RespError::UnknownRespType => b"-ERR unknown RESP type\r\n",
            RespError::FailedToParseInteger => b"-ERR failed to parse integer\r\n",
            RespError::InvalidBulkString => b"-ERR invalid bulk string\r\n",
            RespError::InvalidArray => b"-ERR invalid array\r\n",
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(String),
    Array(Vec<RespValue>),
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
}
