use std::sync::Arc;

use codecrafters_redis::commands::CommandError;

use crate::test_utils::{TestEnv, TestUtils};

#[tokio::test]
async fn test_handle_publish_command_without_subscribers() {
    let mut env = TestEnv::new_master_server();
    let (client_address, writer) = TestEnv::new_client_connection().await;

    env.exec_pub_sub_command_success_response(
        TestUtils::publish_command("channel1", "hello there"),
        &client_address,
        writer,
        Some(TestUtils::expected_integer(0)),
    )
    .await;
}

#[tokio::test]
async fn test_handle_publish_command_with_subscribers() {
    let mut env = TestEnv::new_master_server();
    let (client_address, writer) = TestEnv::new_client_connection().await;
    let (client_address2, writer2) = TestEnv::new_client_connection().await;
    let (client_address3, writer3) = TestEnv::new_client_connection().await;

    env.exec_pub_sub_command_success_response(
        TestUtils::subscribe_command("channel1"),
        &client_address,
        Arc::clone(&writer),
        Some("*3\r\n$9\r\nsubscribe\r\n$8\r\nchannel1\r\n:1\r\n".to_string()),
    )
    .await;

    env.exec_pub_sub_command_success_response(
        TestUtils::subscribe_command("channel1"),
        &client_address2,
        writer2,
        Some("*3\r\n$9\r\nsubscribe\r\n$8\r\nchannel1\r\n:1\r\n".to_string()),
    )
    .await;

    env.exec_pub_sub_command_success_response(
        TestUtils::subscribe_command("channel2"),
        &client_address3,
        writer3,
        Some("*3\r\n$9\r\nsubscribe\r\n$8\r\nchannel2\r\n:1\r\n".to_string()),
    )
    .await;

    env.exec_pub_sub_command_success_response(
        TestUtils::publish_command("channel1", "hello there"),
        &client_address,
        writer,
        Some(TestUtils::expected_integer(2)),
    )
    .await;
}

#[tokio::test]
async fn test_handle_publish_command_invalid() {
    let mut env = TestEnv::new_master_server();
    let (client_address, writer) = TestEnv::new_client_connection().await;

    let test_cases = vec![
        (
            TestUtils::invalid_command(&["PUBLISH"]),
            CommandError::InvalidPublishCommand,
        ),
        (
            TestUtils::invalid_command(&["PUBLISH", "channel1", "hey", "there"]),
            CommandError::InvalidPublishCommand,
        ),
    ];

    for (command, expected_error) in test_cases {
        env.exec_pub_sub_command_error_response(
            command,
            &client_address,
            Arc::clone(&writer),
            expected_error,
        )
        .await;
    }
}
