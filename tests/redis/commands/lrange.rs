use codecrafters_redis::commands::CommandError;

use crate::test_utils::{TestEnv, TestUtils};

#[tokio::test]
async fn test_handle_lrange_command() {
    let mut env = TestEnv::new_master_server();

    env.exec_command_ok(
        TestUtils::rpush_command(
            "grape",
            &["mango", "raspberry", "apple", "banana", "kiwi", "pear"],
        ),
        &TestUtils::client_address(41844),
        &TestUtils::expected_integer(6),
    )
    .await;

    let test_cases = vec![
        (
            TestUtils::lrange_command("grape", 0, -4),
            "*3\r\n$5\r\nmango\r\n$9\r\nraspberry\r\n$5\r\napple\r\n",
        ),
        (
            TestUtils::lrange_command("grape", -4, -1),
            "*4\r\n$5\r\napple\r\n$6\r\nbanana\r\n$4\r\nkiwi\r\n$4\r\npear\r\n",
        ),
        (
            TestUtils::lrange_command("grape", 0, -1),
            "*6\r\n$5\r\nmango\r\n$9\r\nraspberry\r\n$5\r\napple\r\n$6\r\nbanana\r\n$4\r\nkiwi\r\n$4\r\npear\r\n",
        ),
        (TestUtils::lrange_command("grape", -1, -2), "*0\r\n"),
    ];

    for (command, expected_response) in test_cases {
        env.exec_command_ok(
            command,
            &TestUtils::client_address(41844),
            expected_response,
        )
        .await;
    }
}

#[tokio::test]
async fn test_handle_lrange_command_invalid() {
    let mut env = TestEnv::new_master_server();

    let test_cases = vec![
        (
            TestUtils::invalid_command(&["LRANGE", "grape", "0"]),
            CommandError::InvalidLRangeCommand,
        ),
        (
            TestUtils::invalid_command(&["LRANGE", "grape", "0", "1", "mango"]),
            CommandError::InvalidLRangeCommand,
        ),
    ];

    for (command, expected_error) in test_cases {
        env.exec_command_err(command, &TestUtils::client_address(41844), expected_error)
            .await;
    }
}
