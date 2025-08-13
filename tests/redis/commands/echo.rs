use codecrafters_redis::commands::CommandError;

use crate::test_utils::{TestEnv, TestUtils};

#[tokio::test]
async fn test_handle_echo_command() {
    let mut env = TestEnv::new_master_server();

    env.exec_command_ok(
        TestUtils::echo_command("Hello, World!"),
        &TestUtils::client_address(41844),
        &TestUtils::expected_bulk_string("Hello, World!"),
    )
    .await;
}

#[tokio::test]
async fn test_handle_echo_command_invalid() {
    let mut env = TestEnv::new_master_server();

    let test_cases = vec![
        (
            TestUtils::invalid_command(&["ECHO"]),
            CommandError::InvalidEchoCommand,
        ),
        (
            TestUtils::invalid_command(&["ECHO", "grape", "mango"]),
            CommandError::InvalidEchoCommand,
        ),
    ];

    for (command, expected_error) in test_cases {
        env.exec_command_err(command, &TestUtils::client_address(41844), expected_error)
            .await;
    }
}
