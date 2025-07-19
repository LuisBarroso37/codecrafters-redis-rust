use std::{
    io::{Read, Write},
    net::TcpListener,
    thread,
};

use regex::Regex;

fn parse_command(buffer: &[u8]) -> Result<String, String> {
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
            
            println!("Received command: {}", cmd);
            
            if let Some(caps) = re.captures(cmd) {
                println!("Parsed RESP: {:?}", caps);
                
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
                                        return Ok(format!("-ERR argument in index {} has length mismatch\r\n", i));
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

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                thread::spawn(move || {
                    loop {
                        let mut buf = [0; 1024];
                        let number_of_bytes = stream.read(&mut buf).unwrap();
                        if number_of_bytes == 0 {
                            break; // Connection closed
                        }

                        let response = parse_command(&buf).unwrap();
                        stream.write_all(response.as_bytes()).unwrap();
                    }
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
