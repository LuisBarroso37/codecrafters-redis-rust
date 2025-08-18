use codecrafters_redis::commands::CommandError;

use crate::test_utils::{TestEnv, TestUtils};

#[tokio::test]
async fn test_handle_replconf_command() {
    let mut env = TestEnv::new_master_server();

    let test_cases = vec![
        (
            TestUtils::replconf_command("listening-port", "6380"),
            TestUtils::expected_simple_string("OK"),
        ),
        (
            TestUtils::replconf_command("capa", "psync2"),
            TestUtils::expected_simple_string("OK"),
        ),
        (
            TestUtils::replconf_command("GETACK", "*"),
            TestUtils::expected_bulk_string_array(&["REPLCONF", "ACK", "0"]),
        ),
    ];

    for (command, response) in test_cases {
        env.exec_command_ok(command, &TestUtils::client_address(41844), &response)
            .await;
    }
}

#[tokio::test]
async fn test_handle_replconf_command_invalid() {
    let mut env = TestEnv::new_master_server();

    let test_cases = vec![
        (
            TestUtils::invalid_command(&["REPLCONF", "capa", "sync2", "invalid"]),
            CommandError::InvalidReplconfCommand,
        ),
        (
            TestUtils::invalid_command(&["REPLCONF", "capa", "invalid"]),
            CommandError::InvalidReplconfCommand,
        ),
        (
            TestUtils::invalid_command(&["REPLCONF", "listening-port", "invalid"]),
            CommandError::InvalidReplconfCommand,
        ),
        (
            TestUtils::invalid_command(&["REPLCONF", "invalid"]),
            CommandError::InvalidReplconfCommand,
        ),
        (
            TestUtils::invalid_command(&["REPLCONF", "capa"]),
            CommandError::InvalidReplconfCommand,
        ),
    ];

    for (command, expected_error) in test_cases {
        env.exec_command_err(command, &TestUtils::client_address(41844), expected_error)
            .await;
    }
}
