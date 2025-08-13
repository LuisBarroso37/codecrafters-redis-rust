use std::collections::VecDeque;

use codecrafters_redis::{
    commands::CommandError,
    key_value_store::{DataType, Value},
};

use crate::test_utils::{TestEnv, TestUtils};

#[tokio::test]
async fn test_handle_lpush_command_insert() {
    let mut env = TestEnv::new_master_server();

    env.exec_command_ok(
        TestUtils::lpush_command("grape", &["mango", "raspberry", "apple"]),
        &TestUtils::client_address(41844),
        &TestUtils::expected_integer(3),
    )
    .await;

    let store_guard = env.get_store().await;
    let value = store_guard.get("grape");
    assert_eq!(
        value,
        Some(&Value {
            data: DataType::Array(VecDeque::from([
                "apple".to_string(),
                "raspberry".to_string(),
                "mango".to_string()
            ])),
            expiration: None,
        })
    );
}

#[tokio::test]
async fn test_handle_lpush_command_update() {
    let mut env = TestEnv::new_master_server();

    env.exec_command_ok(
        TestUtils::lpush_command("grape", &["mango", "raspberry", "apple"]),
        &TestUtils::client_address(41844),
        &TestUtils::expected_integer(3),
    )
    .await;

    let store_guard = env.get_store().await;
    let inserted_value = store_guard.get("grape");
    assert_eq!(
        inserted_value,
        Some(&Value {
            data: DataType::Array(VecDeque::from([
                "apple".to_string(),
                "raspberry".to_string(),
                "mango".to_string()
            ])),
            expiration: None,
        })
    );
    drop(store_guard);

    env.exec_command_ok(
        TestUtils::lpush_command("grape", &["pear"]),
        &TestUtils::client_address(41844),
        &TestUtils::expected_integer(4),
    )
    .await;

    let store_guard = env.get_store().await;
    let updated_value = store_guard.get("grape");
    assert_eq!(
        updated_value,
        Some(&Value {
            data: DataType::Array(VecDeque::from([
                "pear".to_string(),
                "apple".to_string(),
                "raspberry".to_string(),
                "mango".to_string(),
            ])),
            expiration: None,
        })
    );
}

#[tokio::test]
async fn test_handle_lpush_command_invalid() {
    let mut env = TestEnv::new_master_server();

    env.exec_command_err(
        TestUtils::invalid_command(&["LPUSH", "grape"]),
        &TestUtils::client_address(41844),
        CommandError::InvalidLPushCommand,
    )
    .await;
}
