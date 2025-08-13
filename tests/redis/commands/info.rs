use codecrafters_redis::commands::CommandError;

use crate::test_utils::{TestEnv, TestUtils};

#[tokio::test]
async fn test_handle_info_command_master_server() {
    let mut env = TestEnv::new_master_server();

    let test_cases = vec![
        (TestUtils::info_command(None), "$11\r\nrole:master\r\n"),
        (
            TestUtils::info_command(Some("replication")),
            "$11\r\nrole:master\r\n",
        ),
    ];

    for (command, response) in test_cases {
        env.exec_command_ok(command, &TestUtils::client_address(41844), response)
            .await;
    }
}

#[tokio::test]
async fn test_handle_info_command_replica_server() {
    let mut env = TestEnv::new_replica_server(6380);

    let test_cases = vec![
        (TestUtils::info_command(None), "$10\r\nrole:slave\r\n"),
        (
            TestUtils::info_command(Some("replication")),
            "$10\r\nrole:slave\r\n",
        ),
    ];

    for (command, response) in test_cases {
        env.exec_command_ok(command, &TestUtils::client_address(41844), response)
            .await;
    }
}

#[tokio::test]
async fn test_handle_info_command_invalid() {
    let mut env = TestEnv::new_master_server();

    env.exec_command_err(
        TestUtils::invalid_command(&["INFO", "replication", "server"]),
        &TestUtils::client_address(41844),
        CommandError::InvalidInfoCommand,
    )
    .await;
}

#[tokio::test]
async fn test_handle_info_command_invalid_section() {
    let mut env = TestEnv::new_master_server();

    env.exec_command_err(
        TestUtils::invalid_command(&["INFO", "random"]),
        &TestUtils::client_address(41844),
        CommandError::InvalidInfoSection,
    )
    .await;
}
