use codecrafters_redis::commands::CommandError;

use crate::test_utils::{TestEnv, TestUtils};

#[tokio::test]
async fn test_handle_info_command() {
    let mut env = TestEnv::new();

    let test_cases = vec![
        (TestUtils::info_command(None), "$11\r\nrole:master\r\n"),
        (
            TestUtils::info_command(Some("replication")),
            "$11\r\nrole:master\r\n",
        ),
    ];

    for (command, response) in test_cases {
        env.exec_command_ok(command, &TestUtils::server_addr(41844), response)
            .await;
    }
}

#[tokio::test]
async fn test_handle_info_command_invalid() {
    let mut env = TestEnv::new();

    env.exec_command_err(
        TestUtils::invalid_command(&["INFO", "replication", "server"]),
        &TestUtils::server_addr(41844),
        CommandError::InvalidInfoCommand,
    )
    .await;
}

#[tokio::test]
async fn test_handle_info_command_invalid_section() {
    let mut env = TestEnv::new();

    env.exec_command_err(
        TestUtils::invalid_command(&["INFO", "random"]),
        &TestUtils::server_addr(41844),
        CommandError::InvalidInfoSection,
    )
    .await;
}
