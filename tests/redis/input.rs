use std::time::Duration;
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
    time::timeout,
};

use codecrafters_redis::{
    input::{handshake, read_and_parse_resp},
    resp::RespValue,
};

use crate::test_utils::TestEnv;

#[tokio::test]
async fn test_handshake_success() {
    // Start a mock master server
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let master_addr = listener.local_addr().unwrap();

    // Spawn mock master server
    let master_handle = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buffer = [0; 1024];

        // Expect PING command
        let ping_cmd = read_and_parse_resp(&mut stream, &mut buffer).await.unwrap();
        assert_eq!(ping_cmd.len(), 1);
        if let RespValue::Array(ping_array) = &ping_cmd[0] {
            assert_eq!(ping_array.len(), 1);
            assert_eq!(ping_array[0], RespValue::BulkString("PING".to_string()));
        }

        // Send PONG response
        stream
            .write_all(
                RespValue::SimpleString("PONG".to_string())
                    .encode()
                    .as_bytes(),
            )
            .await
            .unwrap();
        stream.flush().await.unwrap();

        // Expect REPLCONF listening-port
        let replconf1 = read_and_parse_resp(&mut stream, &mut buffer).await.unwrap();
        assert_eq!(replconf1.len(), 1);
        if let RespValue::Array(replconf_array) = &replconf1[0] {
            assert_eq!(replconf_array.len(), 3);
            assert_eq!(
                replconf_array[0],
                RespValue::BulkString("REPLCONF".to_string())
            );
            assert_eq!(
                replconf_array[1],
                RespValue::BulkString("listening-port".to_string())
            );
        }

        // Send OK response
        stream
            .write_all(
                RespValue::SimpleString("OK".to_string())
                    .encode()
                    .as_bytes(),
            )
            .await
            .unwrap();
        stream.flush().await.unwrap();

        // Expect REPLCONF capa
        let replconf2 = read_and_parse_resp(&mut stream, &mut buffer).await.unwrap();
        assert_eq!(replconf2.len(), 1);
        if let RespValue::Array(replconf_array) = &replconf2[0] {
            assert_eq!(replconf_array.len(), 3);
            assert_eq!(
                replconf_array[0],
                RespValue::BulkString("REPLCONF".to_string())
            );
            assert_eq!(replconf_array[1], RespValue::BulkString("capa".to_string()));
            assert_eq!(
                replconf_array[2],
                RespValue::BulkString("psync2".to_string())
            );
        }

        // Send OK response
        stream
            .write_all(
                RespValue::SimpleString("OK".to_string())
                    .encode()
                    .as_bytes(),
            )
            .await
            .unwrap();
        stream.flush().await.unwrap();

        // Expect PSYNC command
        let psync = read_and_parse_resp(&mut stream, &mut buffer).await.unwrap();
        assert_eq!(psync.len(), 1);
        if let RespValue::Array(psync_array) = &psync[0] {
            assert_eq!(psync_array.len(), 3);
            assert_eq!(psync_array[0], RespValue::BulkString("PSYNC".to_string()));
            assert_eq!(psync_array[1], RespValue::BulkString("?".to_string()));
            assert_eq!(psync_array[2], RespValue::BulkString("-1".to_string()));
        }

        // Send FULLRESYNC response
        let fullresync_response = "+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n";
        stream
            .write_all(fullresync_response.as_bytes())
            .await
            .unwrap();
        stream.flush().await.unwrap();

        // Send empty RDB file
        let empty_rdb = b"$88\r\nREDIS0011\xfa\x09redis-ver\x057.2.0\xfa\nredis-bits\xc0@\xfa\x05ctime\xc2m\x08\xbc\x65\xfa\x08used-mem\xc2\xb0\xc4\x10\x00\xfa\x08aof-base\xc0\x00\xff\xf0n;\xfe\xc0\xff\x5a\xa2";
        stream.write_all(empty_rdb).await.unwrap();
        stream.flush().await.unwrap();
    });

    // Give master time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Create replica server and connect
    let env = TestEnv::new_replica_server(6382);
    let mut replica_stream = TcpStream::connect(master_addr).await.unwrap();

    // Test handshake
    let result = timeout(
        Duration::from_secs(5),
        handshake(&mut replica_stream, env.server),
    )
    .await;

    assert!(result.is_ok(), "Handshake should not timeout");
    assert!(result.unwrap().is_ok(), "Handshake should succeed");

    // Wait for master handler to complete
    let _ = timeout(Duration::from_secs(2), master_handle).await;
}

/// Test the handshake function with invalid master responses
#[tokio::test]
async fn test_handshake_invalid_pong_response() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let master_addr = listener.local_addr().unwrap();

    // Spawn mock master server that sends invalid PONG
    let master_handle = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buffer = [0; 1024];

        // Read PING command
        let _ = read_and_parse_resp(&mut stream, &mut buffer).await.unwrap();

        // Send invalid response instead of PONG
        stream
            .write_all(
                RespValue::SimpleString("INVALID".to_string())
                    .encode()
                    .as_bytes(),
            )
            .await
            .unwrap();
        stream.flush().await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    let env = TestEnv::new_replica_server(6383);
    let mut replica_stream = TcpStream::connect(master_addr).await.unwrap();

    let result = timeout(
        Duration::from_secs(2),
        handshake(&mut replica_stream, env.server),
    )
    .await;

    assert!(result.is_ok(), "Should not timeout");
    assert!(
        result.unwrap().is_err(),
        "Handshake should fail with invalid PONG"
    );

    let _ = timeout(Duration::from_millis(500), master_handle).await;
}
