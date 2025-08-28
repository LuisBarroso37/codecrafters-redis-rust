use crate::rdb::{
    encoding::{parse_length_encoded_integer, parse_value},
    get_slice::get_buffer_slice,
};

const METADATA_OPCODE: u8 = 0xFA;
const RESIZE_DB_OPCODE: u8 = 0xFB;
const DATABASE_OPCODE: u8 = 0xFE;
const EXPIRATION_SECONDS_OPCODE: u8 = 0xFD;
const EXPIRATION_MILLISECONDS_OPCODE: u8 = 0xFC;
const END_OF_FILE_OPCODE: u8 = 0xFF;
const STRING_VALUE_TYPE: u8 = 0x00; // 0
// const LIST_VALUE_TYPE: u8 = 0x01; // 1
// const SET_VALUE_TYPE: u8 = 0x02; // 2
// const SORTED_SET_VALUE_TYPE: u8 = 0x03; // 3
// const HASH_VALUE_TYPE: u8 = 0x04; // 4
// const ZIPMAP_VALUE_TYPE: u8 = 0x09; // 9
// const ZIPLIST_VALUE_TYPE: u8 = 0x0A; // 10
// const INTSET_VALUE_TYPE: u8 = 0x0B; // 11
// const SORTED_SET_ZIPLIST_VALUE_TYPE: u8 = 0x0C; // 12
// const HASHMAP_ZIPLIST_VALUE_TYPE: u8 = 0x0D; // 13
// const LIST_QUICKLIST_VALUE_TYPE: u8 = 0x0E; // 14

pub enum OpCodeResponse {
    Metadata {
        key: String,
        value: String,
    },
    ResizeDb {
        db_hash_table_size: String,
        expiry_hash_table_size: String,
    },
    Database {
        database_number: String,
    },
    ExpirationSeconds {
        key: String,
        value: String,
        expiration: i64,
    },
    ExpirationMilliseconds {
        key: String,
        value: String,
        expiration: i64,
    },
    EndOfFile {
        crc64_checksum: Vec<u8>,
    },
    KeyValuePair {
        key: String,
        value: String,
    },
}

pub fn parse_opcode(bytes: &[u8], cursor: usize) -> tokio::io::Result<(OpCodeResponse, usize)> {
    let mut temp_cursor = cursor.clone();
    let opcode = get_buffer_slice(bytes, temp_cursor, 1)?;
    temp_cursor += 1;

    let response = match opcode[0] {
        METADATA_OPCODE => {
            let (key, key_cursor) = parse_value(bytes, temp_cursor)?;
            temp_cursor += key_cursor;
            let (value, value_cursor) = parse_value(bytes, temp_cursor)?;
            temp_cursor += value_cursor;

            Ok(OpCodeResponse::Metadata { key, value })
        }
        RESIZE_DB_OPCODE => {
            let (db_hash_table_size, db_hash_table_size_cursor) =
                parse_length_encoded_integer(bytes, temp_cursor)?;
            temp_cursor += db_hash_table_size_cursor;

            let (expiry_hash_table_size, expiry_hash_table_size_cursor) =
                parse_length_encoded_integer(bytes, temp_cursor)?;
            temp_cursor += expiry_hash_table_size_cursor;

            Ok(OpCodeResponse::ResizeDb {
                db_hash_table_size,
                expiry_hash_table_size,
            })
        }
        DATABASE_OPCODE => {
            let (database_number, database_number_cursor) =
                parse_length_encoded_integer(bytes, temp_cursor)?;
            temp_cursor += database_number_cursor;

            Ok(OpCodeResponse::Database { database_number })
        }
        EXPIRATION_SECONDS_OPCODE => {
            let byte_slice = get_buffer_slice(bytes, temp_cursor, 4)?;
            temp_cursor += 4;

            let unix_timestamp: [u8; 4] = byte_slice.try_into().map_err(|_| {
                tokio::io::Error::new(
                    tokio::io::ErrorKind::UnexpectedEof,
                    "Not enough bytes for u32",
                )
            })?;
            let expiration = u32::from_le_bytes(unix_timestamp);

            let (key_value_pair, key_value_cursor) = parse_opcode(bytes, temp_cursor)?;
            temp_cursor += key_value_cursor;

            match key_value_pair {
                OpCodeResponse::KeyValuePair { key, value } => {
                    Ok(OpCodeResponse::ExpirationSeconds {
                        key,
                        value,
                        expiration: expiration as i64,
                    })
                }
                _ => Err(tokio::io::Error::new(
                    tokio::io::ErrorKind::InvalidData,
                    "Expected KeyValuePair",
                )),
            }
        }
        EXPIRATION_MILLISECONDS_OPCODE => {
            let byte_slice = get_buffer_slice(bytes, temp_cursor, 8)?;
            temp_cursor += 8;

            let unix_timestamp: [u8; 8] = byte_slice.try_into().map_err(|_| {
                tokio::io::Error::new(
                    tokio::io::ErrorKind::UnexpectedEof,
                    "Not enough bytes for u64",
                )
            })?;
            let expiration = u64::from_le_bytes(unix_timestamp);

            let (key_value_pair, key_value_cursor) = parse_opcode(bytes, temp_cursor)?;
            temp_cursor += key_value_cursor;

            match key_value_pair {
                OpCodeResponse::KeyValuePair { key, value } => {
                    Ok(OpCodeResponse::ExpirationMilliseconds {
                        key,
                        value,
                        expiration: expiration as i64,
                    })
                }
                _ => Err(tokio::io::Error::new(
                    tokio::io::ErrorKind::InvalidData,
                    "Expected KeyValuePair",
                )),
            }
        }
        END_OF_FILE_OPCODE => {
            let crc64_checksum = get_buffer_slice(bytes, temp_cursor, 8)?;
            temp_cursor += 8;

            Ok(OpCodeResponse::EndOfFile { crc64_checksum })
        }
        STRING_VALUE_TYPE => {
            let (key, key_cursor) = parse_value(bytes, temp_cursor)?;
            temp_cursor += key_cursor;
            let (value, value_cursor) = parse_value(bytes, temp_cursor)?;
            temp_cursor += value_cursor;

            Ok(OpCodeResponse::KeyValuePair { key, value })
        }
        opcode => Err(tokio::io::Error::new(
            tokio::io::ErrorKind::InvalidData,
            format!("Unknown OpCode: 0x{:02X}", opcode),
        )),
    }?;

    let bytes_read = temp_cursor - cursor;

    Ok((response, bytes_read))
}

pub struct MagicStringResponse {
    pub number_of_read_bytes: usize,
    pub magic_string: String,
    pub redis_version: String,
}

pub fn parse_magic_string(bytes: &[u8], cursor: usize) -> tokio::io::Result<MagicStringResponse> {
    if cursor > 0 {
        return Err(tokio::io::Error::new(
            tokio::io::ErrorKind::AlreadyExists,
            "Magic string should already have been read",
        ));
    }

    let byte_slice = get_buffer_slice(bytes, cursor, 5)?;
    let magic_string = String::from_utf8(byte_slice)
        .map_err(|e| tokio::io::Error::new(tokio::io::ErrorKind::InvalidData, e.to_string()))?;

    if magic_string != "REDIS" {
        return Err(tokio::io::Error::new(
            tokio::io::ErrorKind::InvalidData,
            "Invalid magic string",
        ));
    }

    let byte_slice = get_buffer_slice(bytes, 5, 4)?;
    let redis_version = String::from_utf8(byte_slice)
        .map_err(|e| tokio::io::Error::new(tokio::io::ErrorKind::InvalidData, e.to_string()))?;

    let version_num = redis_version
        .parse::<u32>()
        .map_err(|e| tokio::io::Error::new(tokio::io::ErrorKind::InvalidData, e.to_string()))?;

    if version_num < 1 || version_num > 12 {
        return Err(tokio::io::Error::new(
            tokio::io::ErrorKind::InvalidData,
            "Invalid Redis version",
        ));
    }

    Ok(MagicStringResponse {
        number_of_read_bytes: 9,
        magic_string,
        redis_version,
    })
}
