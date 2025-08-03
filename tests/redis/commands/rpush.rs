use std::collections::VecDeque;

use codecrafters_redis::{
    commands::CommandError,
    key_value_store::{DataType, Value},
};

use crate::test_utils::{TestEnv, TestUtils};

#[tokio::test]
async fn test_handle_rpush_command_insert() {
    let mut env = TestEnv::new();

    env.exec_command_ok(
        TestUtils::rpush_command("grape", &["mango", "raspberry", "apple"]),
        "127.0.0.1:41844",
        &TestUtils::expected_integer(3),
    )
    .await;

    let store = env.get_store().await;
    let value = store.get("grape");
    assert_eq!(
        value,
        Some(&Value {
            data: DataType::Array(VecDeque::from([
                "mango".to_string(),
                "raspberry".to_string(),
                "apple".to_string()
            ])),
            expiration: None,
        })
    );
}

#[tokio::test]
async fn test_handle_rpush_command_update() {
    let mut env = TestEnv::new();

    env.exec_command_ok(
        TestUtils::rpush_command("grape", &["mango", "raspberry", "apple"]),
        &TestUtils::server_addr(41844),
        &TestUtils::expected_integer(3),
    )
    .await;

    let store = env.get_store().await;
    let inserted_value = store.get("grape");
    assert_eq!(
        inserted_value,
        Some(&Value {
            data: DataType::Array(VecDeque::from([
                "mango".to_string(),
                "raspberry".to_string(),
                "apple".to_string()
            ])),
            expiration: None,
        })
    );
    drop(store);

    env.exec_command_ok(
        TestUtils::rpush_command("grape", &["pear"]),
        &TestUtils::server_addr(41844),
        &TestUtils::expected_integer(4),
    )
    .await;

    let store = env.get_store().await;
    let updated_value = store.get("grape");
    assert_eq!(
        updated_value,
        Some(&Value {
            data: DataType::Array(VecDeque::from([
                "mango".to_string(),
                "raspberry".to_string(),
                "apple".to_string(),
                "pear".to_string(),
            ])),
            expiration: None,
        })
    );
}

#[tokio::test]
async fn test_handle_rpush_command_invalid() {
    let mut env = TestEnv::new();

    env.exec_command_err(
        TestUtils::invalid_command(&["RPUSH"]),
        &TestUtils::server_addr(41844),
        CommandError::InvalidRPushCommand,
    )
    .await;
}
