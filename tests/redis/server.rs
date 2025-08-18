use std::time::Duration;
use tokio::net::TcpStream;

use codecrafters_redis::{resp::RespValue, server::RedisServer};

use crate::test_utils::TestUtils;

#[tokio::test]
async fn test_master_replica_handshake_and_replication() {
    let master_args = vec![
        "redis-server".to_string(),
        "--port".to_string(),
        "6380".to_string(),
    ];
    let master_server = RedisServer::new(master_args).unwrap();

    tokio::spawn(async move {
        master_server.run().await;
    });

    // Give master server time to start
    tokio::time::sleep(Duration::from_millis(200)).await;

    let replica_args = vec![
        "redis-server".to_string(),
        "--port".to_string(),
        "6381".to_string(),
        "--replicaof".to_string(),
        "127.0.0.1 6380".to_string(),
    ];
    let replica_server = RedisServer::new(replica_args).unwrap();

    tokio::spawn(async move {
        replica_server.run().await;
    });

    // Give replica server time to start and complete handshake
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Create a separate client connection to send SET command to master
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
