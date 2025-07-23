

use std::time::Duration;

use tokio::{sync::MutexGuard, time::Instant};

use crate::key_value_store::{KeyValueStore, Value};

#[derive(Debug)]
pub struct BulkString {
    length: usize,
    content: String,
}

impl BulkString {
    pub fn from(data_information: &str, data: &str) -> Result<Self, anyhow::Error> {
        let data_length = data_information.strip_prefix('$');
        let declared_length: usize;

        if let Some(length) = data_length {
            declared_length = length.parse::<usize>().unwrap_or(0);
        } else {
            return Err(anyhow::anyhow!("Invalid bulk string format"));
        }

        if data.len() != declared_length {
            return Err(anyhow::anyhow!("String length mismatch"));
        }

        Ok(BulkString {
            length: declared_length,
            content: data.to_string(),
        })
    }
}

#[derive(Debug)]
pub struct BulkArray {
    length: usize,
    arguments: Vec<BulkString>,
}

impl BulkArray {
    pub fn from(data_information: &str, arguments: Vec<&str>) -> Result<Self, anyhow::Error> {
        let data_length = data_information.strip_prefix('*');
        let number_of_arguments: usize;

        if let Some(length) = data_length {
            // Parse the length of the bulk string
            number_of_arguments = length.parse::<usize>().unwrap_or(0);
        } else {
            return Err(anyhow::anyhow!("Invalid bulk array format"));
        }

        if arguments.len() / 2 != number_of_arguments {
            return Err(anyhow::anyhow!("Argument count mismatch"));
        }

        let validated_arguments: Result<Vec<_>, _> = arguments
            .chunks(2)
            .map(|chunk| {
                if chunk.len() == 2 {
                    BulkString::from(&chunk[0], &chunk[1])
                } else {
                    Err(anyhow::anyhow!("Invalid argument chunk"))
                }
            })
            .collect();

        Ok(BulkArray {
            length: number_of_arguments,
            arguments: validated_arguments?,
        })
    }
}

pub fn process_command(bulk_array: BulkArray, store: &mut MutexGuard<KeyValueStore>) -> String {
    if bulk_array.length == 0 {
        return "-ERR unknown command\r\n".to_string();
    }

    let command = bulk_array.arguments[0].content.to_uppercase();

    match command.as_str() {
        "PING" => "+PONG\r\n".to_string(),
        "ECHO" => {
            if bulk_array.length < 2 {
                return "-ERR wrong number of arguments for 'ECHO' command\r\n".to_string();
            }

            format!(
                "${}\r\n{}\r\n",
                bulk_array.arguments[1].length, bulk_array.arguments[1].content
            )
        }
        "GET" => {
            if bulk_array.length < 2 {
                return "-ERR wrong number of arguments for 'GET' command\r\n".to_string();
            }

            if let Some(data) = store.get(bulk_array.arguments[1].content.as_str()) {
                if let Some(expiration) = data.expiration {
                    if Instant::now() > expiration {
                        store.remove(bulk_array.arguments[1].content.as_str());
                        return "$-1\r\n".to_string();
                    }
                }

                return format!("${}\r\n{}\r\n", data.value.len(), data.value);
            } else {
                return "$-1\r\n".to_string();
            }
        }
        "SET" => {
            if bulk_array.length != 3 && bulk_array.length != 5 {
                return "-ERR wrong number of arguments for 'SET' command\r\n".to_string();
            }

            let mut expiration: Option<Instant> = None;

            if bulk_array.length == 5 {
                if bulk_array.arguments[3].content.to_uppercase() != "PX" {
                    return format!(
                        "-ERR unknown SET command argument {} \r\n",
                        bulk_array.arguments[3].content
                    );
                }

                if let Ok(expiration_time) = bulk_array.arguments[4].content.parse::<u64>() {
                    expiration = Some(
                        Instant::now()
                            + Duration::from_millis(expiration_time),
                    )
                } else {
                    return "-ERR invalid expiration time\r\n".to_string();
                }
            }

            store.insert(
                bulk_array.arguments[1].content.clone(),
                Value {
                    value: bulk_array.arguments[2].content.clone(),
                    expiration,
                },
            );

            return "+OK\r\n".to_string();
        }
        _ => {
            return "-ERR unknown command\r\n".to_string();
        }
    }
}

pub fn parse_command(
    buffer: &[u8],
    store: &mut MutexGuard<KeyValueStore>,
) -> Result<String, String> {
    let is_ascii = buffer.is_ascii();

    if !is_ascii {
        return Err("Input is not ASCII".to_string());
    }

    let ascii = buffer.to_ascii_lowercase();
    let command = str::from_utf8(ascii.as_slice());

    match command {
        Ok(cmd) => {
            let arguments = cmd.split("\r\n").filter(|s| !s.contains("\0")).collect::<Vec<_>>();

            if cmd.starts_with("*") {
                let bulk_array = BulkArray::from(arguments[0], arguments[1..].to_vec());

                match bulk_array {
                    Ok(array) => {
                        Ok(process_command(array, store))
                    }
                    Err(e) => Ok(format!("-ERR {}\r\n", e)),
                }
            } else if cmd.starts_with("$") {
                Ok("-ERR unknown command\r\n".to_string())
            } else if cmd.starts_with("+") {
                Ok("-ERR unknown command\r\n".to_string())
            } else if cmd.starts_with("-") {
                Ok("-ERR unknown command\r\n".to_string())
            } else if cmd.starts_with(":") {
                Ok("-ERR unknown command\r\n".to_string())
            } else {
                Ok("-ERR unknown command\r\n".to_string())
            }
        }
        Err(_) => Ok("-ERR invalid UTF-8 sequence\r\n".to_string()),
    }
}
