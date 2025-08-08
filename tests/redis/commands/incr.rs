use codecrafters_redis::{
    commands::CommandError,
    key_value_store::{DataType, Value},
};

use crate::test_utils::{TestEnv, TestUtils};

#[tokio::test]
async fn test_handle_incr_command() {
    let mut env = TestEnv::new();

    env.exec_command_ok(
        TestUtils::set_command("grape", "5"),
        &TestUtils::server_addr(41844),
        &TestUtils::expected_simple_string("OK"),
    )
    .await;

    let store_guard = env.get_store().await;
    let value = store_guard.get("grape");
    assert_eq!(
        value,
        Some(&Value {
            data: DataType::String("5".to_string()),
            expiration: None,
        })
    );
    drop(store_guard);

    env.exec_command_ok(
        TestUtils::incr_command("grape"),
        &TestUtils::server_addr(41845),
        &&TestUtils::expected_integer(6),
    )
    .await;

    let store_guard = env.get_store().await;
    let value = store_guard.get("grape");
    assert_eq!(
        value,
        Some(&Value {
            data: DataType::String("6".to_string()),
            expiration: None,
        })
    );
}

#[tokio::test]
async fn test_handle_incr_command_invalid() {
    let mut env = TestEnv::new();

    let test_cases = vec![
        (
            TestUtils::invalid_command(&["INCR"]),
            CommandError::InvalidIncrCommand,
        ),
        (
            TestUtils::invalid_command(&["INCR", "grape", "mango"]),
            CommandError::InvalidIncrCommand,
        ),
    ];

    for (command, expected_error) in test_cases {
        env.exec_command_err(command, &TestUtils::server_addr(41844), expected_error)
            .await;
    }
}

#[tokio::test]
async fn test_handle_get_command_non_existent_key() {
    let mut env = TestEnv::new();

    let store_guard = env.get_store().await;
    let value = store_guard.get("grape");
    assert_eq!(value, None);
    drop(store_guard);

    env.exec_command_ok(
        TestUtils::incr_command("grape"),
        &TestUtils::server_addr(41844),
        &&TestUtils::expected_integer(1),
    )
    .await;

    let store_guard = env.get_store().await;
    let value = store_guard.get("grape");
    assert_eq!(
        value,
        Some(&Value {
            data: DataType::String("1".to_string()),
            expiration: None,
        })
    );
}
