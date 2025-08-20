use codecrafters_redis::commands::CommandError;

use crate::test_utils::{TestEnv, TestUtils};

#[tokio::test]
async fn test_handle_ping_command() {
    let mut env = TestEnv::new_master_server();

    env.exec_command_immediate_success_response(
        TestUtils::ping_command(),
        &TestUtils::client_address(41844),
        &TestUtils::expected_simple_string("PONG"),
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
