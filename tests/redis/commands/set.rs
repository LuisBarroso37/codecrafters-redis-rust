use std::time::Duration;

use codecrafters_redis::{
    commands::CommandError,
    key_value_store::{DataType, Value},
};
use tokio::time::Instant;

use crate::test_utils::{TestEnv, TestUtils};

#[tokio::test]
async fn test_handle_set_command() {
    let mut env = TestEnv::new_master_server();

    env.exec_command_ok(
        TestUtils::set_command("grape", "mango"),
        &TestUtils::client_address(41844),
        &TestUtils::expected_simple_string("OK"),
    )
    .await;

    let store_guard = env.get_store().await;
    let value = store_guard.get("grape");
    assert_eq!(
        value,
        Some(&Value {
            data: DataType::String("mango".to_string()),
            expiration: None,
        })
    );
}

#[tokio::test]
async fn test_handle_set_command_with_expiration() {
    tokio::time::pause();
    let now = Instant::now();

    let mut env = TestEnv::new_master_server();

    env.exec_command_ok(
        TestUtils::set_command_with_expiration("grape", "mango", 100),
        &TestUtils::client_address(41844),
        &TestUtils::expected_simple_string("OK"),
    )
    .await;

    // Verify the value was stored correctly with expiration
    let store_guard = env.get_store().await;
    let value = store_guard.get("grape");
    assert_eq!(
        value,
        Some(&Value {
            data: DataType::String("mango".to_string()),
            expiration: Some(now + Duration::from_millis(100)),
        })
    );
}

#[tokio::test]
async fn test_handle_set_command_invalid() {
    let mut env = TestEnv::new_master_server();

    let test_cases = vec![
        (
            TestUtils::invalid_command(&["SET", "grape"]),
            CommandError::InvalidSetCommand,
        ),
        (
            TestUtils::invalid_command(&["SET", "grape", "mango", "px"]),
            CommandError::InvalidSetCommand,
        ),
        (
            TestUtils::invalid_command(&["SET", "grape", "mango", "random", "100"]),
            CommandError::InvalidSetCommandArgument,
        ),
        (
            TestUtils::invalid_command(&["SET", "grape", "mango", "px", "random"]),
            CommandError::InvalidSetCommandExpiration,
        ),
    ];

    for (command, expected_error) in test_cases {
        env.exec_command_err(command, &TestUtils::client_address(41844), expected_error)
            .await;
    }
}
