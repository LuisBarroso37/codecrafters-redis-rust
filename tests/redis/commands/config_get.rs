use codecrafters_redis::commands::CommandError;

use crate::test_utils::{TestEnv, TestUtils};

#[tokio::test]
async fn test_handle_config_get_command() {
    let mut env = TestEnv::new_master_server();

    let test_cases = vec![
        (
            TestUtils::config_get_command(&["dir"]),
            TestUtils::expected_bulk_string_array(&["dir", "/tmp/redis-files"]),
        ),
        (
            TestUtils::config_get_command(&["dbfilename"]),
            TestUtils::expected_bulk_string_array(&["dbfilename", "dump.rdb"]),
        ),
        (
            TestUtils::config_get_command(&["dir", "dbfilename"]),
            TestUtils::expected_bulk_string_array(&[
                "dir",
                "/tmp/redis-files",
                "dbfilename",
                "dump.rdb",
            ]),
        ),
    ];

    for (command, expected_response) in test_cases {
        env.exec_command_immediate_success_response(
            command,
            &TestUtils::client_address(41844),
            &expected_response,
        )
        .await;
    }
}

#[tokio::test]
async fn test_handle_config_get_command_invalid() {
    let mut env = TestEnv::new_master_server();

    env.exec_command_immediate_error_response(
        TestUtils::invalid_command(&["CONFIG GET"]),
        &TestUtils::client_address(41844),
        CommandError::InvalidConfigGetCommand,
    )
    .await;
}
