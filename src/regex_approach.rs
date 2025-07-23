use std::time::Duration;

use regex::Regex;
use tokio::{sync::MutexGuard, time::Instant};

use crate::key_value_store::{KeyValueStore, Value};

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
            // Use a simpler approach: parse the array header, then extract all bulk strings dynamically
            let array_header_re = Regex::new(r"^\*(\d+)\r\n").unwrap();
            
            if let Some(array_caps) = array_header_re.captures(cmd) {
                let array_count: usize = array_caps[1].parse().unwrap_or(0);
                
                // Extract all bulk strings after the array header
                let mut remaining = &cmd[array_caps[0].len()..];
                let mut elements = Vec::new();
                
                // Parse each bulk string dynamically
                for i in 0..array_count {
                    // Match bulk string pattern: $<length>\r\n<data>\r\n
                    let bulk_re = Regex::new(r"^\$(\d+)\r\n").unwrap();
                    
                    if let Some(bulk_caps) = bulk_re.captures(remaining) {
                        let declared_length: usize = bulk_caps[1].parse().unwrap_or(0);
                        let header_len = bulk_caps[0].len();
                        
                        // Extract the data part
                        if remaining.len() >= header_len + declared_length + 2 {
                            let data = &remaining[header_len..header_len + declared_length];
                            let expected_suffix = &remaining[header_len + declared_length..header_len + declared_length + 2];
                            
                            // Validate CRLF after data
                            if expected_suffix != "\r\n" {
                                return Ok(format!(
                                    "-ERR bulk string {} missing CRLF terminator\r\n", i
                                ));
                            }
                            
                            // Validate declared length matches actual length
                            if data.len() != declared_length {
                                return Ok(format!(
                                    "-ERR bulk string {} length mismatch: declared {}, actual {}\r\n",
                                    i, declared_length, data.len()
                                ));
                            }
                            
                            elements.push(data);
                            
                            // Move to next bulk string
                            remaining = &remaining[header_len + declared_length + 2..];
                        } else {
                            return Ok(format!(
                                "-ERR bulk string {} incomplete data\r\n", i
                            ));
                        }
                    } else {
                        return Ok(format!(
                            "-ERR bulk string {} missing header\r\n", i
                        ));
                    }
                }
                
                // Validate array count matches actual elements found
                if elements.len() != array_count {
                    return Ok(format!(
                        "-ERR array count mismatch: declared {}, found {}\r\n", 
                        array_count, elements.len()
                    ));
                }
                
                if elements.is_empty() {
                    return Ok("-ERR empty command\r\n".to_string());
                }
                
                let command_str = elements[0].to_uppercase();
                let args: Vec<&str> = elements[1..].iter().copied().collect();

                println!("Parsed command: {} with {} args: {:?}", command_str, args.len(), args);

                // Handle specific commands according to RESP spec
                match command_str.as_str() {
                    "PING" => return Ok("+PONG\r\n".to_string()),
                    "ECHO" if !args.is_empty() => {
                        let response = format!("${}\r\n{}\r\n", args[0].len(), args[0]);
                        return Ok(response);
                    }
                    "GET" if args.len() == 1 => {
                        if let Some(data) = store.get(args[0]) {
                            if let Some(expiration) = data.expiration {
                                if Instant::now() > expiration {
                                    store.remove(args[0]);
                                    return Ok("$-1\r\n".to_string());
                                }
                            }

                            let response =
                                format!("${}\r\n{}\r\n", data.value.len(), data.value);
                            return Ok(response);
                        } else {
                            return Ok("$-1\r\n".to_string());
                        }
                    }
                    "SET" => {
                        println!("SET command received {:?}", args);
                        if args.len() == 2 {
                            store.insert(
                                args[0].to_string(),
                                Value {
                                    value: args[1].to_string(),
                                    expiration: None,
                                },
                            );
                            return Ok("+OK\r\n".to_string());
                        } else if args.len() == 4 {
                            if args[2].to_uppercase() != "PX" {
                                return Ok(
                                    "-ERR invalid SET command argument\r\n".to_string()
                                );
                            }

                            match args[3].parse() {
                                Ok(duration) => {
                                    let expiration =
                                        Instant::now() + Duration::from_millis(duration);
                                    store.insert(
                                        args[0].to_string(),
                                        Value {
                                            value: args[1].to_string(),
                                            expiration: Some(expiration),
                                        },
                                    );
                                    return Ok("+OK\r\n".to_string());
                                }
                                Err(_) => {
                                    return Ok(
                                        "-ERR invalid SET command argument\r\n".to_string()
                                    );
                                }
                            }
                        } else {
                            return Ok("-ERR invalid SET command\r\n".to_string());
                        }
                    }
                    _ => return Ok("-ERR unknown command\r\n".to_string()),
                }
            } else {
                return Ok("-ERR protocol error\r\n".to_string());
            }
        }
        Err(_) => Ok("-ERR invalid UTF-8 sequence\r\n".to_string()),
    }
}
