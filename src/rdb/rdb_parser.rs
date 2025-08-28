use std::collections::HashMap;

use jiff::Timestamp;

use crate::{
    key_value_store::{DataType, Value},
    rdb::opcode::{OpCodeResponse, parse_magic_string, parse_opcode},
};

#[derive(Debug)]
pub struct RdbParser {
    buffer: Vec<u8>,
    cursor: usize,
    pub magic_string: Option<String>,
    pub redis_version: Option<String>,
    pub metadata: HashMap<String, String>,
    pub db_number: Option<String>,
    pub hash_table_size: Option<String>,
    pub expiry_hash_table_size: Option<String>,
    pub key_value_store: HashMap<String, Value>,
    pub crc64_checksum: Option<Vec<u8>>,
}

impl RdbParser {
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
            cursor: 0,
            magic_string: None,
            redis_version: None,
            metadata: HashMap::new(),
            db_number: None,
            hash_table_size: None,
            expiry_hash_table_size: None,
            key_value_store: HashMap::new(),
            crc64_checksum: None,
        }
    }

    pub fn parse(&mut self, buffer: Vec<u8>) -> tokio::io::Result<()> {
        self.buffer.extend_from_slice(&buffer);

        if self.magic_string.is_none() {
            let magic_string_response = parse_magic_string(&self.buffer, self.cursor)?;
            self.cursor += magic_string_response.number_of_read_bytes;
            self.magic_string = Some(magic_string_response.magic_string);
            self.redis_version = Some(magic_string_response.redis_version);
        }

        while self.cursor < self.buffer.len() {
            let (result, new_cursor) = match parse_opcode(&mut self.buffer, self.cursor) {
                Ok(response) => response,
                Err(e) => {
                    if e.kind() == tokio::io::ErrorKind::UnexpectedEof {
                        self.buffer.drain(..self.cursor);
                    } else {
                        eprintln!("Error parsing opcode: {}", e);
                    }

                    self.cursor = 0;
                    break;
                }
            };

            match result {
                OpCodeResponse::Metadata { key, value } => {
                    self.metadata.insert(key, value);
                }
                OpCodeResponse::ResizeDb {
                    db_hash_table_size,
                    expiry_hash_table_size,
                } => {
                    self.hash_table_size = Some(db_hash_table_size);
                    self.expiry_hash_table_size = Some(expiry_hash_table_size);
                }
                OpCodeResponse::Database { database_number } => {
                    self.db_number = Some(database_number);
                }
                OpCodeResponse::ExpirationSeconds {
                    key,
                    value,
                    expiration,
                } => {
                    let expiration = Timestamp::from_second(expiration).map_err(|e| {
                        tokio::io::Error::new(
                            tokio::io::ErrorKind::InvalidData,
                            format!("Invalid expiration timestamp: {}", e),
                        )
                    })?;
                    self.key_value_store.insert(
                        key,
                        Value {
                            data: DataType::String(value),
                            expiration: Some(expiration),
                        },
                    );
                }
                OpCodeResponse::ExpirationMilliseconds {
                    key,
                    value,
                    expiration,
                } => {
                    let expiration = Timestamp::from_millisecond(expiration).map_err(|e| {
                        tokio::io::Error::new(
                            tokio::io::ErrorKind::InvalidData,
                            format!("Invalid expiration timestamp: {}", e),
                        )
                    })?;
                    self.key_value_store.insert(
                        key,
                        Value {
                            data: DataType::String(value),
                            expiration: Some(expiration),
                        },
                    );
                }
                OpCodeResponse::EndOfFile { crc64_checksum } => {
                    self.crc64_checksum = Some(crc64_checksum);
                }
                OpCodeResponse::KeyValuePair { key, value } => {
                    self.key_value_store.insert(
                        key,
                        Value {
                            data: DataType::String(value),
                            expiration: None,
                        },
                    );
                }
            }

            self.cursor += new_cursor;
        }

        Ok(())
    }
}
