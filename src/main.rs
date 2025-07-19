#![allow(unused_imports)]
use std::{io::{Read, Write}, net::TcpListener};

use bytes::{buf, Buf};

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                loop {
                    let mut buf = [0; 1024];
                    let number_of_bytes = stream.read(&mut buf).unwrap();
                    if number_of_bytes == 0 {
                        break; // Connection closed
                    }
                    println!("Received: {:?}", str::from_utf8(&buf[..number_of_bytes]).unwrap());
                    
                    stream.write_all(b"+PONG\r\n").unwrap();
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
