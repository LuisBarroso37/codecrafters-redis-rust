use std::sync::Arc;

use codecrafters_redis::commands::CommandError;

use crate::test_utils::{TestEnv, TestUtils};

#[tokio::test]
async fn test_handle_ping_after_subscribe_command() {
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
        TestUtils::ping_command(),
        &client_address,
        writer,
        Some("*2\r\n$4\r\npong\r\n$0\r\n\r\n".to_string()),
    )
    .await;
}

#[tokio::test]
async fn test_handle_ping_without_subscribe_command() {
    let mut env = TestEnv::new_master_server();
    let (client_address, writer) = TestEnv::new_client_connection().await;

    env.exec_pub_sub_command_success_response(
        TestUtils::ping_command(),
        &client_address,
        writer,
        None,
    )
    .await;
}

#[tokio::test]
async fn test_handle_ping_command_invalid() {
    let mut env = TestEnv::new_master_server();

    env.exec_command_immediate_error_response(
        TestUtils::invalid_command(&["PING", "grape"]),
        &TestUtils::client_address(41844),
        CommandError::InvalidPingCommand,
    )
    .await;
}
