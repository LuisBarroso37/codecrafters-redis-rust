use codecrafters_redis::commands::CommandError;

use crate::test_utils::{TestEnv, TestUtils};

#[tokio::test]
async fn test_handle_llen_command() {
    let mut env = TestEnv::new();

    env.exec_command_ok(
        TestUtils::rpush_command("grape", &["mango", "raspberry", "apple"]),
        &TestUtils::server_addr(41844),
        &TestUtils::expected_integer(3),
    )
    .await;

    env.exec_command_ok(
        TestUtils::llen_command("grape"),
        &TestUtils::server_addr(41844),
        &TestUtils::expected_integer(3),
    )
    .await;
}

#[tokio::test]
async fn test_handle_llen_command_not_found() {
    let mut env = TestEnv::new();

    env.exec_command_ok(
        TestUtils::llen_command("grape"),
        &TestUtils::server_addr(41844),
        &TestUtils::expected_integer(0),
    )
    .await;
}

#[tokio::test]
async fn test_handle_llen_command_wrong_data_type() {
    let mut env = TestEnv::new();

    env.exec_command_ok(
        TestUtils::set_command("grape", "mango"),
        &TestUtils::server_addr(41844),
        &TestUtils::expected_simple_string("OK"),
    )
    .await;

    env.exec_command_ok(
        TestUtils::llen_command("grape"),
        &TestUtils::server_addr(41844),
        &TestUtils::expected_integer(0),
    )
    .await;
}

#[tokio::test]
async fn test_handle_llen_command_invalid() {
    let mut env = TestEnv::new();

    env.exec_command_err(
        TestUtils::invalid_command(&["LLEN"]),
        &TestUtils::server_addr(41844),
        CommandError::InvalidLLenCommand,
    )
    .await;
}
