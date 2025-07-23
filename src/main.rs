use std::{
    collections::HashMap,
    io::{Read, Write},
    net::TcpListener,
    sync::Arc,
};

use tokio::sync::Mutex;

use crate::{key_value_store::KeyValueStore, resp::parse_command};

mod key_value_store;
mod resp;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    let store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let store = Arc::clone(&store);

                tokio::spawn(async move {
                    loop {
                        let mut buf = [0; 1024];
                        let number_of_bytes = stream.read(&mut buf).unwrap();
                        if number_of_bytes == 0 {
                            break; // Connection closed
                        }

                        let mut store = store.lock().await;
                        let response = parse_command(&buf, &mut store).unwrap();
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
