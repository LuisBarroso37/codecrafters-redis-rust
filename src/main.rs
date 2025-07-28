use std::{
    collections::HashMap,
    io::{Read, Write},
    net::TcpListener,
    sync::Arc,
};

use tokio::sync::Mutex;

use crate::{
    command::handle_command, input::parse_input, key_value_store::KeyValueStore, resp::RespValue,
    state::State,
};

mod command;
mod input;
mod key_value_store;
mod resp;
mod state;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    let store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let mut store = Arc::clone(&store);
                let mut state = Arc::clone(&state);

                tokio::spawn(async move {
                    loop {
                        let mut buf = [0; 1024];
                        let number_of_bytes = stream.read(&mut buf).unwrap();
                        if number_of_bytes == 0 {
                            break; // Connection closed
                        }

                        let input = match parse_input(&buf) {
                            Ok(input) => input,
                            Err(e) => {
                                stream.write_all(e.as_bytes()).unwrap();
                                continue;
                            }
                        };

                        let parsed_command = match RespValue::parse(input) {
                            Ok(cmd) => cmd,
                            Err(e) => {
                                stream.write_all(e.as_bytes()).unwrap();
                                continue;
                            }
                        };

                        match handle_command(parsed_command, &mut store, &mut state).await {
                            Ok(resp) => stream.write_all(resp.as_bytes()).unwrap(),
                            Err(e) => stream.write_all(e.as_bytes()).unwrap(),
                        }
                    }
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
