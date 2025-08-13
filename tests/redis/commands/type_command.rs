use codecrafters_redis::commands::CommandError;

use crate::test_utils::{TestEnv, TestUtils};

#[tokio::test]
async fn test_handle_type_command_string() {
    let mut env = TestEnv::new_master_server();

    env.exec_command_ok(
        TestUtils::set_command("grape", "mango"),
        &TestUtils::client_address(41844),
        &TestUtils::expected_simple_string("OK"),
    )
    .await;

    env.exec_command_ok(
        TestUtils::type_command("grape"),
        &TestUtils::client_address(41844),
        &TestUtils::expected_simple_string("string"),
    )
    .await;
}

#[tokio::test]
async fn test_handle_type_command_list() {
    let mut env = TestEnv::new_master_server();

    env.exec_command_ok(
        TestUtils::rpush_command("grape", &["mango", "raspberry", "apple"]),
        &TestUtils::client_address(41844),
        &TestUtils::expected_integer(3),
    )
    .await;

    env.exec_command_ok(
        TestUtils::type_command("grape"),
        &TestUtils::client_address(41844),
        &TestUtils::expected_simple_string("list"),
    )
    .await;
}

#[tokio::test]
async fn test_handle_type_command_stream() {
    let mut env = TestEnv::new_master_server();
    let key = "fruits";
    let stream_id = "1526919030474-0";

    env.exec_command_ok(
        TestUtils::xadd_command(key, stream_id, &["mango", "apple"]),
        &TestUtils::client_address(41844),
        &TestUtils::expected_bulk_string(stream_id),
    )
    .await;

    env.exec_command_ok(
        TestUtils::type_command(key),
        &TestUtils::client_address(41844),
        &TestUtils::expected_simple_string("stream"),
    )
    .await;
}

#[tokio::test]
async fn test_handle_type_command_missing_key() {
    let mut env = TestEnv::new_master_server();

    env.exec_command_ok(
        TestUtils::type_command("grape"),
        &TestUtils::client_address(41844),
        &TestUtils::expected_simple_string("none"),
    )
    .await;
}

#[tokio::test]
async fn test_handle_type_command_invalid() {
    let mut env = TestEnv::new_master_server();

    let test_cases = vec![
        (
            TestUtils::invalid_command(&["TYPE"]),
            CommandError::InvalidTypeCommand,
        ),
        (
            TestUtils::invalid_command(&["TYPE", "grape", "mango"]),
            CommandError::InvalidTypeCommand,
        ),
    ];

    for (command, expected_error) in test_cases {
        env.exec_command_err(command, &TestUtils::client_address(41844), expected_error)
            .await;
    }
}
