use codecrafters_redis::commands::{CommandError, CommandHandler, DispatchError};

use crate::test_utils::{TestEnv, TestUtils};

#[tokio::test]
async fn test_handle_multi_command() {
    let mut env = TestEnv::new();

    env.exec_transaction_immediate_response(
        TestUtils::multi_command(),
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

    env.exec_transaction_immediate_response(
        TestUtils::multi_command(),
        &TestUtils::server_addr(41844),
        &TestUtils::expected_simple_string("OK"),
    )
    .await;

    let mut state_guard = env.get_state().await;
    let transaction = state_guard.get_transaction(&TestUtils::server_addr(41844));
    assert_eq!(transaction, Some(&Vec::new()));
    drop(state_guard);

    env.exec_transaction_immediate_response(
        TestUtils::exec_command(),
        &TestUtils::server_addr(41844),
        &TestUtils::expected_bulk_string_array(&[]),
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
        TestUtils::exec_command(),
        &TestUtils::server_addr(41844),
        DispatchError::ExecWithoutMulti,
    )
    .await;
}

#[tokio::test]
async fn test_handle_should_queue_commands() {
    let mut env = TestEnv::new();

    env.exec_transaction_immediate_response(
        TestUtils::multi_command(),
        &TestUtils::server_addr(41844),
        &TestUtils::expected_simple_string("OK"),
    )
    .await;

    env.exec_transaction_immediate_response(
        TestUtils::set_command("grapes", "4"),
        &TestUtils::server_addr(41844),
        &TestUtils::expected_simple_string("QUEUED"),
    )
    .await;

    env.exec_transaction_immediate_response(
        TestUtils::incr_command("grapes"),
        &TestUtils::server_addr(41844),
        &TestUtils::expected_simple_string("QUEUED"),
    )
    .await;

    let mut state_guard = env.get_state().await;
    let transaction = state_guard.get_transaction(&TestUtils::server_addr(41844));
    assert_eq!(
        transaction,
        Some(&vec![
            CommandHandler {
                name: "SET".to_string(),
                arguments: vec!["grapes".to_string(), "4".to_string()]
            },
            CommandHandler {
                name: "INCR".to_string(),
                arguments: vec!["grapes".to_string()]
            }
        ])
    );
}

#[tokio::test]
async fn test_handle_fail_to_add_invalid_command_to_transaction() {
    let mut env = TestEnv::new();

    env.exec_transaction_immediate_response(
        TestUtils::multi_command(),
        &TestUtils::server_addr(41844),
        &TestUtils::expected_simple_string("OK"),
    )
    .await;

    env.exec_transaction_err(
        TestUtils::invalid_command(&["GET", "key", "value"]),
        &TestUtils::server_addr(41844),
        DispatchError::InvalidQueueCommand(CommandError::InvalidGetCommand),
    )
    .await;
}

#[tokio::test]
async fn test_handle_should_return_queued_commands() {
    let mut env = TestEnv::new();

    env.exec_transaction_immediate_response(
        TestUtils::multi_command(),
        &TestUtils::server_addr(41844),
        &TestUtils::expected_simple_string("OK"),
    )
    .await;

    env.exec_transaction_immediate_response(
        TestUtils::set_command("grapes", "4"),
        &TestUtils::server_addr(41844),
        &TestUtils::expected_simple_string("QUEUED"),
    )
    .await;

    env.exec_transaction_immediate_response(
        TestUtils::incr_command("grapes"),
        &TestUtils::server_addr(41844),
        &TestUtils::expected_simple_string("QUEUED"),
    )
    .await;

    env.exec_transaction_expected_commands(
        TestUtils::exec_command(),
        &TestUtils::server_addr(41844),
        &[
            CommandHandler {
                name: "SET".to_string(),
                arguments: vec!["grapes".to_string(), "4".to_string()],
            },
            CommandHandler {
                name: "INCR".to_string(),
                arguments: vec!["grapes".to_string()],
            },
        ],
    )
    .await;

    let mut state_guard = env.get_state().await;
    let transaction = state_guard.get_transaction(&TestUtils::server_addr(41844));
    assert_eq!(transaction, None);
}

#[tokio::test]
async fn test_handle_should_run_single_command() {
    let mut env = TestEnv::new();

    env.exec_transaction_execute_commands(
        TestUtils::set_command("grapes", "4"),
        &TestUtils::server_addr(41844),
        TestUtils::expected_simple_string("OK"),
    )
    .await;
}

#[tokio::test]
async fn test_handle_should_run_queued_commands() {
    let mut env = TestEnv::new();

    env.exec_transaction_immediate_response(
        TestUtils::multi_command(),
        &TestUtils::server_addr(41844),
        &TestUtils::expected_simple_string("OK"),
    )
    .await;

    env.exec_transaction_immediate_response(
        TestUtils::set_command("grapes", "4"),
        &TestUtils::server_addr(41844),
        &TestUtils::expected_simple_string("QUEUED"),
    )
    .await;

    env.exec_transaction_immediate_response(
        TestUtils::incr_command("grapes"),
        &TestUtils::server_addr(41844),
        &TestUtils::expected_simple_string("QUEUED"),
    )
    .await;

    env.exec_transaction_execute_commands(
        TestUtils::exec_command(),
        &TestUtils::server_addr(41844),
        "*2\r\n+OK\r\n:5\r\n".to_string(),
    )
    .await;

    let mut state_guard = env.get_state().await;
    let transaction = state_guard.get_transaction(&TestUtils::server_addr(41844));
    assert_eq!(transaction, None);
}
