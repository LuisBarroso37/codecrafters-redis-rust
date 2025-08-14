use codecrafters_redis::commands::CommandError;

use crate::test_utils::{TestEnv, TestUtils};

#[tokio::test]
async fn test_handle_psync_command() {
    let mut env = TestEnv::new_master_server();

    let test_cases = vec![
        (
            TestUtils::psync_command("?", "-1"),
            TestUtils::expected_simple_string(
                "FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb -1",
            ),
        ),
        (
            TestUtils::psync_command("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb", "0"),
            TestUtils::expected_simple_string(
                "FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0",
            ),
        ),
    ];

    for (command, response) in test_cases {
        env.exec_command_ok(command, &TestUtils::client_address(41844), &response)
            .await;
    }
}

#[tokio::test]
async fn test_handle_psync_command_replication_id_does_not_match() {
    let mut env = TestEnv::new_master_server();

    env.exec_command_err(
        TestUtils::invalid_command(&["PSYNC", "59211996145553b6fd49a914e49210391ac94854", "0"]),
        &TestUtils::client_address(41844),
        CommandError::InvalidPsyncReplicationId,
    )
    .await;
}

#[tokio::test]
async fn test_handle_psync_command_invalid() {
    let mut env = TestEnv::new_master_server();

    env.exec_command_err(
        TestUtils::invalid_command(&[
            "PSYNC",
            "59211996145553b6fd49a914e49210391ac94854",
            "0",
            "random",
        ]),
        &TestUtils::client_address(41844),
        CommandError::InvalidPsyncCommand,
    )
    .await;
}
