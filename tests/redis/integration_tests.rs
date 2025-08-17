use std::time::Duration;
use tokio::{io::AsyncWriteExt, net::TcpStream, time::timeout};

use codecrafters_redis::{input::read_and_parse_resp, resp::RespValue, server::RedisServer};

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
    let mut client_stream = TcpStream::connect("127.0.0.1:6380").await.unwrap();

    let set_command = RespValue::Array(vec![
        RespValue::BulkString("SET".to_string()),
        RespValue::BulkString("test_key".to_string()),
        RespValue::BulkString("test_value".to_string()),
    ]);

    client_stream
        .write_all(set_command.encode().as_bytes())
        .await
        .unwrap();
    client_stream.flush().await.unwrap();

    let mut buffer = [0; 1024];
    let master_response = timeout(
        Duration::from_secs(2),
        read_and_parse_resp(&mut client_stream, &mut buffer),
    )
    .await;

    assert!(master_response.is_ok(), "Master response timed out");
    let master_response = master_response.unwrap().unwrap();
    assert_eq!(
        master_response[0],
        RespValue::SimpleString("OK".to_string())
    );

    // Give time for replication to occur
    tokio::time::sleep(Duration::from_millis(500)).await;

    let set_command2 = RespValue::Array(vec![
        RespValue::BulkString("SET".to_string()),
        RespValue::BulkString("key2".to_string()),
        RespValue::BulkString("value2".to_string()),
    ]);

    client_stream
        .write_all(set_command2.encode().as_bytes())
        .await
        .unwrap();
    client_stream.flush().await.unwrap();

    let mut buffer2 = [0; 1024];
    let master_response2 = timeout(
        Duration::from_secs(2),
        read_and_parse_resp(&mut client_stream, &mut buffer2),
    )
    .await;

    assert!(master_response2.is_ok(), "Master response2 timed out");
    let master_response2 = master_response2.unwrap().unwrap();
    assert_eq!(
        master_response2[0],
        RespValue::SimpleString("OK".to_string())
    );

    // Give time for replication to occur
    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut replica_client = TcpStream::connect("127.0.0.1:6381").await.unwrap();

    // Test GET command for first key
    let get_command1 = RespValue::Array(vec![
        RespValue::BulkString("GET".to_string()),
        RespValue::BulkString("test_key".to_string()),
    ]);

    replica_client
        .write_all(get_command1.encode().as_bytes())
        .await
        .unwrap();
    replica_client.flush().await.unwrap();

    let mut buffer = [0; 1024];
    let replica_response1 = timeout(
        Duration::from_secs(2),
        read_and_parse_resp(&mut replica_client, &mut buffer),
    )
    .await;

    assert!(replica_response1.is_ok(), "Replica GET response1 timed out");
    let replica_response1 = replica_response1.unwrap().unwrap();
    assert_eq!(
        replica_response1[0],
        RespValue::BulkString("test_value".to_string()),
        "Replica should have replicated first SET command"
    );

    let get_command2 = RespValue::Array(vec![
        RespValue::BulkString("GET".to_string()),
        RespValue::BulkString("key2".to_string()),
    ]);

    replica_client
        .write_all(get_command2.encode().as_bytes())
        .await
        .unwrap();
    replica_client.flush().await.unwrap();

    let mut buffer2 = [0; 1024];
    let replica_response2 = timeout(
        Duration::from_secs(2),
        read_and_parse_resp(&mut replica_client, &mut buffer2),
    )
    .await;

    assert!(replica_response2.is_ok(), "Replica GET response2 timed out");
    let replica_response2 = replica_response2.unwrap().unwrap();
    assert_eq!(
        replica_response2[0],
        RespValue::BulkString("value2".to_string()),
        "Replica should have replicated second SET command"
    );
}
