use std::{collections::HashMap, sync::MutexGuard};

use regex::Regex;

fn parse_command(
    buffer: &[u8],
    data: &mut MutexGuard<HashMap<String, String>>,
) -> Result<String, String> {
    let is_ascii = buffer.is_ascii();

    if !is_ascii {
        return Err("Input is not ASCII".to_string());
    }

    let ascii = buffer.to_ascii_lowercase();
    let command = str::from_utf8(ascii.as_slice());

    match command {
        Ok(cmd) => {
            let re = Regex::new(
                r"(?x)
                # RESP Array: *<number-of-elements>\r\n<element-1>...<element-n>
                \*(?P<array_count>\d+)\r\n
                (?:
                    # Bulk String: $<length>\r\n<data>\r\n (command)
                    \$(?P<cmd_len>\d+)\r\n
                    (?P<command>[^\r\n]*)\r\n
                )?
                (?:
                    # Bulk String: $<length>\r\n<data>\r\n (arg1)
                    \$(?P<arg1_len>\d+)\r\n
                    (?P<arg1>[^\r\n]*)\r\n
                )?
                (?:
                    # Bulk String: $<length>\r\n<data>\r\n (arg2)
                    \$(?P<arg2_len>\d+)\r\n
                    (?P<arg2>[^\r\n]*)\r\n
                )?
                (?:
                    # Bulk String: $<length>\r\n<data>\r\n (arg3)
                    \$(?P<arg3_len>\d+)\r\n
                    (?P<arg3>[^\r\n]*)\r\n
                )?
                (?:
                    # Additional arguments (flexible)
                    (?:\$\d+\r\n[^\r\n]*\r\n)*
                )?
                |
                # Alternative RESP types for responses
                (?P<simple_string>\+[^\r\n]*\r\n)|      # Simple string: +OK\r\n
                (?P<simple_error>-[^\r\n]*\r\n)|        # Simple error: -ERR message\r\n  
                (?P<integer>:[+-]?\d+\r\n)|             # Integer: :123\r\n or :-456\r\n
                (?P<bulk_string>\$(?P<bulk_len>\d+)\r\n(?P<bulk_data>[^\r\n]*)\r\n) # Single bulk string
                ",
            )
            .unwrap();

            if let Some(caps) = re.captures(cmd) {
                // Handle RESP Array (typical command format)
                if let Some(_) = caps.name("array_count") {
                    if let Some(command_match) = caps.name("command") {
                        let command_str = command_match.as_str().to_uppercase();

                        // Validate command length matches declared length
                        if let Some(cmd_len) = caps.name("cmd_len") {
                            let declared_len: usize = cmd_len.as_str().parse().unwrap_or(0);
                            if command_str.len() != declared_len {
                                return Ok("-ERR command length mismatch\r\n".to_string());
                            }
                        }

                        // Extract and validate arguments
                        let mut args = Vec::new();

                        for i in 1..=3 {
                            if let Some(arg) = caps.name(&format!("arg{}", i)) {
                                let arg_str = arg.as_str();
                                args.push(arg_str);

                                // Validate argument length if available
                                if let Some(arg_len) = caps.name(&format!("arg{}_len", i)) {
                                    let declared_len: usize = arg_len.as_str().parse().unwrap_or(0);
                                    if arg_str.len() != declared_len {
                                        return Ok(format!(
                                            "-ERR argument in index {} has length mismatch\r\n",
                                            i
                                        ));
                                    }
                                }
                            }
                        }

                        // Handle specific commands according to RESP spec
                        match command_str.as_str() {
                            "PING" => return Ok("+PONG\r\n".to_string()),
                            "ECHO" if !args.is_empty() => {
                                let response = format!("${}\r\n{}\r\n", args[0].len(), args[0]);
                                return Ok(response);
                            }
                            "GET" if !args.is_empty() => {
                                if let Some(value) = data.get(args[0]) {
                                    let response = format!("${}\r\n{}\r\n", value.len(), value);
                                    return Ok(response);
                                } else {
                                    return Ok("$-1\r\n".to_string());
                                }
                            }
                            "SET" if args.len() == 2 => {
                                data.insert(args[0].to_string(), args[1].to_string());
                                return Ok("+OK\r\n".to_string());
                            }
                            _ => return Ok("-ERR unknown command\r\n".to_string()),
                        }
                    }
                }
            } else {
                return Ok("-ERR protocol error\r\n".to_string());
            }

            Ok("-ERR unknown command\r\n".to_string())
        }
        Err(_) => Ok("-ERR invalid UTF-8 sequence\r\n".to_string()),
    }
}
