use std::{
    io::{Read, Write},
    net::TcpListener,
    thread,
};

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
                        stream.write_all(b"+PONG\r\n").unwrap();
                    }
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
