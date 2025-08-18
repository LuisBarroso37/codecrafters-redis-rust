use std::time::Duration;
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
    time::timeout,
};

use codecrafters_redis::{
    connection::{handle_client_connection, handle_master_connection},
    key_value_store::{DataType, Value},
    resp::RespValue,
};

use crate::test_utils::{TestEnv, TestUtils};

#[tokio::test]
async fn test_handle_client_connection_basic_commands() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();

    let env = TestEnv::new_master_server();
    let (store, state, server) = (env.store.clone(), env.state.clone(), env.server.clone());

    // Spawn server to handle client connections
    let server_handle = tokio::spawn(async move {
        let (stream, addr) = listener.accept().await.unwrap();
        let client_address = addr.to_string();

        handle_client_connection(stream, server, client_address, store, state).await;
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Connect as client and send commands
    let mut client = TcpStream::connect(server_addr).await.unwrap();
    let mut buffer = [0; 1024];

    TestUtils::send_command_and_receive_master_server(
        &mut client,
        &mut buffer,
        TestUtils::ping_command(),
        RespValue::SimpleString("PONG".to_string()),
    )
    .await;

    // Test SET command
    TestUtils::send_command_and_receive_master_server(
        &mut client,
        &mut buffer,
        TestUtils::set_command("test_key", "test_value"),
        RespValue::SimpleString("OK".to_string()),
    )
    .await;

    // Test GET command
    TestUtils::send_command_and_receive_master_server(
        &mut client,
        &mut buffer,
        TestUtils::get_command("test_key"),
        RespValue::BulkString("test_value".to_string()),
    )
    .await;

    // Close the connection to trigger cleanup
    drop(client);

    // Wait for server handler to complete
    let _ = timeout(Duration::from_secs(2), server_handle).await;
}

#[tokio::test]
async fn test_handle_client_connection_replica_write_forbidden() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();

    let env = TestEnv::new_replica_server(6384);
    let (store, state, server) = env.clone_env();

    // Spawn server to handle client connections
    let server_handle = tokio::spawn(async move {
        let (stream, addr) = listener.accept().await.unwrap();
        let client_address = addr.to_string();

        handle_client_connection(stream, server, client_address, store, state).await;
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Connect as client
    let mut client = TcpStream::connect(server_addr).await.unwrap();
    let mut buffer = [0; 1024];

    // Test SET command (should be forbidden on replica)
    TestUtils::send_command_and_receive_master_server(
        &mut client,
        &mut buffer,
        TestUtils::set_command("test_key", "test_value"),
        RespValue::Error("ERR write commands not allowed in replica".to_string()),
    )
    .await;

    // Test GET command (should work on replica)
    TestUtils::send_command_and_receive_master_server(
        &mut client,
        &mut buffer,
        TestUtils::get_command("nonexistent_key"),
        RespValue::NullBulkString,
    )
    .await;

    // Close connection
    drop(client);
    let _ = timeout(Duration::from_secs(2), server_handle).await;
}

#[tokio::test]
async fn test_handle_master_connection_processes_commands() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let replica_addr = listener.local_addr().unwrap();

    let env = TestEnv::new_replica_server(6385);
    let (store, state, server) = env.clone_env();

    // Spawn replica to handle master connection
    let replica_handle = tokio::spawn(async move {
        let (mut stream, addr) = listener.accept().await.unwrap();
        let master_address = addr.to_string();

        handle_master_connection(&master_address, &mut stream, server, store, state).await;
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Connect as master and send commands
    let mut master_stream = TcpStream::connect(replica_addr).await.unwrap();

    // Send SET command from master (should be processed by replica)
    TestUtils::send_command_and_receive_replica_server(
        &mut master_stream,
        TestUtils::set_command("replicated_key", "replicated_value"),
    )
    .await;

    // Send another command
    TestUtils::send_command_and_receive_replica_server(
        &mut master_stream,
        TestUtils::set_command("another_key", "another_value"),
    )
    .await;

    // Give time for commands to be processed
    tokio::time::sleep(Duration::from_millis(500)).await;

    let store = env.get_store().await;

    let value = store.get("replicated_key");
    assert_eq!(
        value,
        Some(&Value {
            data: DataType::String("replicated_value".to_string()),
            expiration: None
        })
    );
    let value = store.get("another_key");
    assert_eq!(
        value,
        Some(&Value {
            data: DataType::String("another_value".to_string()),
            expiration: None
        })
    );

    // Close the connection to terminate the handler
    drop(master_stream);

    // Wait for handler to complete
    let _ = timeout(Duration::from_secs(2), replica_handle).await;
}

#[tokio::test]
async fn test_handle_master_connection_invalid_commands() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let replica_addr = listener.local_addr().unwrap();

    let env = TestEnv::new_replica_server(6386);
    let (store, state, server) = env.clone_env();

    // Spawn replica to handle master connection
    let replica_handle = tokio::spawn(async move {
        let (mut stream, addr) = listener.accept().await.unwrap();
        let master_address = addr.to_string();

        handle_master_connection(&master_address, &mut stream, server, store, state).await;
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Connect as master and send invalid data
    let mut master_stream = TcpStream::connect(replica_addr).await.unwrap();

    // Send malformed RESP data
    master_stream.write_all(b"invalid\r\n").await.unwrap();
    master_stream.flush().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send valid command after invalid one
    TestUtils::send_command_and_receive_replica_server(
        &mut master_stream,
        TestUtils::set_command("mango", "juice"),
    )
    .await;

    // Give time for processing
    tokio::time::sleep(Duration::from_millis(500)).await;

    let store = env.get_store().await;
    let value = store.get("mango");
    assert_eq!(
        value,
        Some(&Value {
            data: DataType::String("juice".to_string()),
            expiration: None
        })
    );

    // Close connection
    drop(master_stream);
    let _ = timeout(Duration::from_secs(2), replica_handle).await;
}

/// Test connection handling with connection close scenarios
#[tokio::test]
async fn test_connection_close_handling() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();

    let env = TestEnv::new_master_server();
    let (store, state, server) = (env.store.clone(), env.state.clone(), env.server.clone());

    // Spawn server to handle client connections
    let server_handle = tokio::spawn(async move {
        let (stream, addr) = listener.accept().await.unwrap();
        let client_address = addr.to_string();

        handle_client_connection(stream, server, client_address, store, state).await;
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Connect and immediately close
    let client = TcpStream::connect(server_addr).await.unwrap();
    drop(client); // Immediately close connection

    // Wait for server handler to detect close and terminate
    let timeout_result = timeout(Duration::from_secs(2), server_handle).await;
    assert!(timeout_result.is_ok(), "Server did not timeout");
    let result = timeout_result.unwrap();
    assert!(
        result.is_ok(),
        "Server handler should terminate gracefully on connection close"
    );
}
