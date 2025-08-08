use codecrafters_redis::transactions::TransactionError;

use crate::test_utils::{TestEnv, TestUtils};

#[tokio::test]
async fn test_handle_multi_command() {
    let mut env = TestEnv::new();

    env.exec_transaction_ok(
        "MULTI",
        &TestUtils::server_addr(41844),
        &TestUtils::expected_simple_string("OK"),
    )
    .await;

    let mut state_guard = env.get_state().await;
    let transaction = state_guard.get_transaction(&TestUtils::server_addr(41844));
    assert_eq!(transaction, Some(&Vec::new()));
}

#[tokio::test]
async fn test_handle_exec_command_immediately_after_multi_command() {
    let mut env = TestEnv::new();

    env.exec_transaction_ok(
        "MULTI",
        &TestUtils::server_addr(41844),
        &TestUtils::expected_simple_string("OK"),
    )
    .await;

    let mut state_guard = env.get_state().await;
    let transaction = state_guard.get_transaction(&TestUtils::server_addr(41844));
    assert_eq!(transaction, Some(&Vec::new()));
    drop(state_guard);

    env.exec_transaction_ok(
        "EXEC",
        &TestUtils::server_addr(41844),
        &&TestUtils::expected_array(&[]),
    )
    .await;

    let mut state_guard = env.get_state().await;
    let transaction = state_guard.get_transaction(&TestUtils::server_addr(41844));
    assert_eq!(transaction, None);
}

#[tokio::test]
async fn test_handle_exec_command_without_using_multi_before() {
    let mut env = TestEnv::new();

    env.exec_transaction_err(
        "EXEC",
        &TestUtils::server_addr(41844),
        TransactionError::ExecWithoutMulti,
    )
    .await;
}
