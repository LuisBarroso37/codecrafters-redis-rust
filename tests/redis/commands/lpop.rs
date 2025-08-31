use std::collections::VecDeque;

use codecrafters_redis::{
    commands::CommandError,
    key_value_store::{DataType, Value},
};

use crate::test_utils::{TestEnv, TestUtils};

#[tokio::test]
async fn test_handle_lpop_command() {
    let mut env = TestEnv::new_master_server();

    env.exec_command_immediate_success_response(
        TestUtils::rpush_command("grape", &["mango", "raspberry", "apple"]),
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
                "mango".to_string(),
                "raspberry".to_string(),
                "apple".to_string(),
            ])),
            expiration: None,
        })
    );
    drop(store_guard);

    env.exec_command_immediate_success_response(
        TestUtils::lpop_command("grape"),
        &TestUtils::client_address(41844),
        &TestUtils::expected_bulk_string("mango"),
    )
    .await;

    let store_guard = env.get_store().await;
    let updated_value = store_guard.get("grape");
    assert_eq!(
        updated_value,
        Some(&Value {
            data: DataType::Array(VecDeque::from([
                "raspberry".to_string(),
                "apple".to_string(),
            ])),
            expiration: None,
        })
    );
}

#[tokio::test]
async fn test_handle_lpop_command_invalid() {
    let mut env = TestEnv::new_master_server();

    env.exec_command_immediate_error_response(
        TestUtils::invalid_command(&["LPOP"]),
        &TestUtils::client_address(41844),
        CommandError::InvalidLPopCommand,
    )
    .await;
}

#[tokio::test]
async fn test_handle_lpop_command_not_found() {
    let mut env = TestEnv::new_master_server();

    env.exec_command_immediate_success_response(
        TestUtils::lpop_command("grape"),
        &TestUtils::client_address(41844),
        &TestUtils::expected_null_bulk_string(),
    )
    .await;
}

#[tokio::test]
async fn test_handle_lpop_command_wrong_data_type() {
    let mut env = TestEnv::new_master_server();

    env.exec_command_immediate_success_response(
        TestUtils::set_command("grape", "mango"),
        &TestUtils::client_address(41844),
        &TestUtils::expected_simple_string("OK"),
    )
    .await;

    env.exec_command_immediate_success_response(
        TestUtils::lpop_command("grape"),
        &TestUtils::client_address(41844),
        &TestUtils::expected_null_bulk_string(),
    )
    .await;
}

#[tokio::test]
async fn test_handle_lpop_command_multiple_elements() {
    let mut env = TestEnv::new_master_server();

    env.exec_command_immediate_success_response(
        TestUtils::rpush_command("grape", &["mango", "raspberry", "apple"]),
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
                "mango".to_string(),
                "raspberry".to_string(),
                "apple".to_string(),
            ])),
            expiration: None,
        })
    );
    drop(store_guard);

    env.exec_command_immediate_success_response(
        TestUtils::lpop_command_multiple_items("grape", 2),
        &TestUtils::client_address(41844),
        &TestUtils::expected_bulk_string_array(&["mango", "raspberry"]),
    )
    .await;

    let store_guard = env.get_store().await;
    let updated_value = store_guard.get("grape");
    assert_eq!(
        updated_value,
        Some(&Value {
            data: DataType::Array(VecDeque::from(["apple".to_string(),])),
            expiration: None,
        })
    );
}
