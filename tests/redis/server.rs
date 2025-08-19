use codecrafters_redis::input::read_and_parse_resp;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use codecrafters_redis::resp::RespValue;

use crate::test_utils::TestUtils;

#[tokio::test]
async fn test_master_replica_handshake_and_replication() {
    TestUtils::run_master_server(6380).await;

    // Give master server time to start
    tokio::time::sleep(Duration::from_millis(200)).await;

    TestUtils::run_replica_server(6381, 6380).await;

    // Give replica server time to start and complete handshake
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Create a separate client connection to send commands to master
    let mut master_client = TcpStream::connect("127.0.0.1:6380").await.unwrap();
    let mut buffer = [0; 1024];

    TestUtils::send_command_and_receive_master_server(
        &mut master_client,
        &mut buffer,
        TestUtils::set_command("test_key", "test_value"),
        RespValue::SimpleString("OK".to_string()),
    )
    .await;

    // Give time for replication to occur
    tokio::time::sleep(Duration::from_millis(500)).await;

    TestUtils::send_command_and_receive_master_server(
        &mut master_client,
        &mut buffer,
        TestUtils::set_command("key2", "value2"),
        RespValue::SimpleString("OK".to_string()),
    )
    .await;

    // Give time for replication to occur
    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut replica_client = TcpStream::connect("127.0.0.1:6381").await.unwrap();
    let mut buffer = [0; 1024];

    TestUtils::send_command_and_receive_master_server(
        &mut replica_client,
        &mut buffer,
        TestUtils::get_command("test_key"),
        RespValue::BulkString("test_value".to_string()),
    )
    .await;

    TestUtils::send_command_and_receive_master_server(
        &mut replica_client,
        &mut buffer,
        TestUtils::get_command("key2"),
        RespValue::BulkString("value2".to_string()),
    )
    .await;
}

#[tokio::test]
async fn test_wait_command_multiple_replicas() {
    TestUtils::run_master_server(6390).await;

    // Give master server time to start
    tokio::time::sleep(Duration::from_millis(200)).await;

    TestUtils::run_replica_server(6391, 6390).await;
    TestUtils::run_replica_server(6392, 6390).await;
    TestUtils::run_replica_server(6393, 6390).await;

    // Give replica servers time to start and complete handshake
    tokio::time::sleep(Duration::from_millis(1000)).await;

    let mut master_client = TcpStream::connect("127.0.0.1:6390").await.unwrap();
    let mut buffer = [0; 1024];

    // Wait for at least 1 replica with a timeout of 1000ms

    TestUtils::send_command_and_receive_master_server(
        &mut master_client,
        &mut buffer,
        TestUtils::set_command("test_key", "test_value"),
        RespValue::SimpleString("OK".to_string()),
    )
    .await;

    TestUtils::send_command_and_receive_master_server(
        &mut master_client,
        &mut buffer,
        TestUtils::wait_command(1, 1000),
        RespValue::Integer(3),
    )
    .await;

    // Wait for at least 2 replicas with a timeout of 1000ms

    TestUtils::send_command_and_receive_master_server(
        &mut master_client,
        &mut buffer,
        TestUtils::set_command("test_key2", "test_value2"),
        RespValue::SimpleString("OK".to_string()),
    )
    .await;

    TestUtils::send_command_and_receive_master_server(
        &mut master_client,
        &mut buffer,
        TestUtils::wait_command(2, 1000),
        RespValue::Integer(3),
    )
    .await;

    // Wait for all replicas with a timeout of 10ms

    TestUtils::send_command_and_receive_master_server(
        &mut master_client,
        &mut buffer,
        TestUtils::set_command("test_key3", "test_value3"),
        RespValue::SimpleString("OK".to_string()),
    )
    .await;

    TestUtils::send_command_and_receive_master_server(
        &mut master_client,
        &mut buffer,
        TestUtils::wait_command(3, 10),
        RespValue::Integer(3),
    )
    .await;

    // Wait for at least 2 replicas with no timeout (blocking indefinitely)

    TestUtils::send_command_and_receive_master_server(
        &mut master_client,
        &mut buffer,
        TestUtils::set_command("test_key4", "test_value4"),
        RespValue::SimpleString("OK".to_string()),
    )
    .await;

    TestUtils::send_command_and_receive_master_server(
        &mut master_client,
        &mut buffer,
        TestUtils::wait_command(2, 0),
        RespValue::Integer(3),
    )
    .await;

    // Wait for at least 5 replicas with a timeout of 1000ms

    TestUtils::send_command_and_receive_master_server(
        &mut master_client,
        &mut buffer,
        TestUtils::set_command("test_key5", "test_value5"),
        RespValue::SimpleString("OK".to_string()),
    )
    .await;

    TestUtils::send_command_and_receive_master_server(
        &mut master_client,
        &mut buffer,
        TestUtils::wait_command(5, 1000),
        RespValue::Integer(3),
    )
    .await;
}

#[tokio::test]
async fn test_wait_command_faulty_replica() {
    TestUtils::run_master_server(6370).await;

    // Give master server time to start
    tokio::time::sleep(Duration::from_millis(200)).await;

    TestUtils::run_replica_server(6371, 6370).await;
    TestUtils::run_replica_server(6372, 6370).await;

    tokio::spawn(async move {
        let mut stream = TcpStream::connect("127.0.0.1:6370").await.unwrap();
        let mut buf = [0u8; 1024];

        // Perform handshake
        stream
            .write_all(TestUtils::ping_command().encode().as_bytes())
            .await
            .unwrap();

        let response = read_and_parse_resp(&mut stream, &mut buf).await.unwrap();
        assert_eq!(response.len(), 1);
        assert_eq!(response[0], RespValue::SimpleString("PONG".to_string()));

        stream
            .write_all(
                TestUtils::replconf_command("listening-port", "6370")
                    .encode()
                    .as_bytes(),
            )
            .await
            .unwrap();

        let response = read_and_parse_resp(&mut stream, &mut buf).await.unwrap();
        assert_eq!(response.len(), 1);
        assert_eq!(response[0], RespValue::SimpleString("OK".to_string()));

        stream
            .write_all(
                TestUtils::replconf_command("capa", "psync2")
                    .encode()
                    .as_bytes(),
            )
            .await
            .unwrap();

        let response = read_and_parse_resp(&mut stream, &mut buf).await.unwrap();
        assert_eq!(response.len(), 1);
        assert_eq!(response[0], RespValue::SimpleString("OK".to_string()));

        stream
            .write_all(TestUtils::psync_command("?", "-1").encode().as_bytes())
            .await
            .unwrap();

        // Now just read and ignore everything, never send REPLCONF ACK
        loop {
            let _ = stream.read(&mut buf).await;
        }
    });

    // Give replica servers time to start and complete handshake
    tokio::time::sleep(Duration::from_millis(1000)).await;

    let mut master_client = TcpStream::connect("127.0.0.1:6370").await.unwrap();
    let mut buffer = [0; 1024];

    TestUtils::send_command_and_receive_master_server(
        &mut master_client,
        &mut buffer,
        TestUtils::set_command("test_key", "test_value"),
        RespValue::SimpleString("OK".to_string()),
    )
    .await;

    TestUtils::send_command_and_receive_master_server(
        &mut master_client,
        &mut buffer,
        TestUtils::wait_command(3, 1000),
        RespValue::Integer(2),
    )
    .await;
}
