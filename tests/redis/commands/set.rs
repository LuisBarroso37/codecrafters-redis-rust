use std::time::Duration;

use codecrafters_redis::{
    commands::CommandError,
    key_value_store::{DataType, Value},
};
use jiff::{Timestamp, ToSpan, Unit};

use crate::test_utils::{TestEnv, TestUtils};

#[tokio::test]
async fn test_handle_set_command() {
    let mut env = TestEnv::new_master_server();

    env.exec_command_immediate_success_response(
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
    let mut env = TestEnv::new_master_server();
    let now = Timestamp::now().round(Unit::Millisecond).unwrap();

    env.exec_command_immediate_success_response(
        TestUtils::set_command_with_expiration("grape", "mango", 100),
        &TestUtils::client_address(41844),
        &TestUtils::expected_simple_string("OK"),
    )
    .await;

    let store_guard = env.get_store().await;
    let value = store_guard.get("grape");
    assert!(value.is_some());

    let value = value.unwrap();
    assert_eq!(value.data, DataType::String("mango".to_string()));
    assert!(value.expiration.is_some());

    // We have to do this due to sometimes the execution timing causing a 1 millisecond difference
    let expiration = value.expiration.unwrap();
    let expected = now.checked_add(Duration::from_millis(100)).unwrap();
    let actual = expiration.round(Unit::Millisecond).unwrap();
    let cmp = (actual - expected).abs().compare(1.millisecond()).unwrap();
    assert!(
        cmp == std::cmp::Ordering::Less || cmp == std::cmp::Ordering::Equal,
        "actual: {:?}, expected: {:?}, diff: {:?}",
        actual,
        expected,
        (actual - expected).abs()
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
        env.exec_command_immediate_error_response(
            command,
            &TestUtils::client_address(41844),
            expected_error,
        )
        .await;
    }
}
