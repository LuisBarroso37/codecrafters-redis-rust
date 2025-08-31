use std::sync::Arc;

use codecrafters_redis::commands::CommandError;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::RwLock,
};

use crate::test_utils::{TestEnv, TestUtils};

#[tokio::test]
async fn test_handle_subscribe_command() {
    let mut env = TestEnv::new_master_server();
    let (client_address, writer) = TestEnv::new_client_connection().await;

    env.exec_pub_sub_command_success_response(
        TestUtils::subscribe_command("channel1"),
        &client_address,
        writer,
        Some("*3\r\n$9\r\nsubscribe\r\n$8\r\nchannel1\r\n:1\r\n".to_string()),
    )
    .await;

    let server_guard = env.server.read().await;
    let pub_sub_channels = &server_guard.pub_sub_channels;
    assert!(pub_sub_channels.contains_key("channel1"));
}

#[tokio::test]
async fn test_handle_consecutive_subscribe_commands_for_same_channel() {
    let mut env = TestEnv::new_master_server();
    let (client_address, writer) = TestEnv::new_client_connection().await;

    env.exec_pub_sub_command_success_response(
        TestUtils::subscribe_command("channel1"),
        &client_address,
        Arc::clone(&writer),
        Some("*3\r\n$9\r\nsubscribe\r\n$8\r\nchannel1\r\n:1\r\n".to_string()),
    )
    .await;

    env.exec_pub_sub_command_success_response(
        TestUtils::subscribe_command("channel1"),
        &client_address,
        writer,
        Some("*3\r\n$9\r\nsubscribe\r\n$8\r\nchannel1\r\n:1\r\n".to_string()),
    )
    .await;

    let server_guard = env.server.read().await;
    let pub_sub_channels = &server_guard.pub_sub_channels;
    assert!(pub_sub_channels.contains_key("channel1"));
}

#[tokio::test]
async fn test_handle_consecutive_subscribe_commands_for_different_channel() {
    let mut env = TestEnv::new_master_server();
    let (client_address, writer) = TestEnv::new_client_connection().await;

    env.exec_pub_sub_command_success_response(
        TestUtils::subscribe_command("channel1"),
        &client_address,
        Arc::clone(&writer),
        Some("*3\r\n$9\r\nsubscribe\r\n$8\r\nchannel1\r\n:1\r\n".to_string()),
    )
    .await;

    env.exec_pub_sub_command_success_response(
        TestUtils::subscribe_command("channel2"),
        &client_address,
        writer,
        Some("*3\r\n$9\r\nsubscribe\r\n$8\r\nchannel2\r\n:2\r\n".to_string()),
    )
    .await;

    let server_guard = env.server.read().await;
    let pub_sub_channels = &server_guard.pub_sub_channels;
    assert!(pub_sub_channels.contains_key("channel1"));
    assert!(pub_sub_channels.contains_key("channel2"));
}

#[tokio::test]
async fn test_handle_subscribe_command_invalid() {
    let mut env = TestEnv::new_master_server();
    let client_address = &TestUtils::client_address(0);
    let listener = TcpListener::bind(&client_address).await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = TcpStream::connect(addr).await;
    });
    let (tcp_stream, _) = listener.accept().await.unwrap();
    let (_, writer) = tcp_stream.into_split();
    let writer = Arc::new(RwLock::new(writer));

    let test_cases = vec![
        (
            TestUtils::invalid_command(&["SUBSCRIBE"]),
            CommandError::InvalidSubscribeCommand,
        ),
        (
            TestUtils::invalid_command(&["SUBSCRIBE", "channel1", "channel2"]),
            CommandError::InvalidSubscribeCommand,
        ),
    ];

    for (command, expected_error) in test_cases {
        env.exec_pub_sub_command_error_response(
            command,
            client_address,
            Arc::clone(&writer),
            expected_error,
        )
        .await;
    }
}
