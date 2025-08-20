use codecrafters_redis::commands::{CommandError, CommandHandler};

use crate::test_utils::{TestEnv, TestUtils};

#[tokio::test]
async fn test_handle_multi_command() {
    let mut env = TestEnv::new_master_server();

    env.exec_command_immediate_success_response(
        TestUtils::multi_command(),
        &TestUtils::client_address(41844),
        &TestUtils::expected_simple_string("OK"),
    )
    .await;

    let mut state_guard = env.get_state().await;
    let transaction = state_guard.get_transaction(&TestUtils::client_address(41844));
    assert_eq!(transaction, Some(&Vec::new()));
}

#[tokio::test]
async fn test_handle_exec_command_immediately_after_multi_command() {
    let mut env = TestEnv::new_master_server();

    env.exec_command_immediate_success_response(
        TestUtils::multi_command(),
        &TestUtils::client_address(41844),
        &TestUtils::expected_simple_string("OK"),
    )
    .await;

    let mut state_guard = env.get_state().await;
    let transaction = state_guard.get_transaction(&TestUtils::client_address(41844));
    assert_eq!(transaction, Some(&Vec::new()));
    drop(state_guard);

    env.exec_command_immediate_success_response(
        TestUtils::exec_command(),
        &TestUtils::client_address(41844),
        &TestUtils::expected_bulk_string_array(&[]),
    )
    .await;

    let mut state_guard = env.get_state().await;
    let transaction = state_guard.get_transaction(&TestUtils::client_address(41844));
    assert_eq!(transaction, None);
}

#[tokio::test]
async fn test_handle_exec_command_without_using_multi_before() {
    let mut env = TestEnv::new_master_server();

    env.exec_command_immediate_error_response(
        TestUtils::exec_command(),
        &TestUtils::client_address(41844),
        CommandError::ExecWithoutMulti,
    )
    .await;
}

#[tokio::test]
async fn test_handle_should_queue_commands() {
    let mut env = TestEnv::new_master_server();

    env.exec_command_immediate_success_response(
        TestUtils::multi_command(),
        &TestUtils::client_address(41844),
        &TestUtils::expected_simple_string("OK"),
    )
    .await;

    env.exec_command_immediate_success_response(
        TestUtils::set_command("grapes", "4"),
        &TestUtils::client_address(41844),
        &TestUtils::expected_simple_string("QUEUED"),
    )
    .await;

    env.exec_command_immediate_success_response(
        TestUtils::incr_command("grapes"),
        &TestUtils::client_address(41844),
        &TestUtils::expected_simple_string("QUEUED"),
    )
    .await;

    let mut state_guard = env.get_state().await;
    let transaction = state_guard.get_transaction(&TestUtils::client_address(41844));
    assert_eq!(
        transaction,
        Some(&vec![
            CommandHandler {
                name: "SET".to_string(),
                arguments: vec!["grapes".to_string(), "4".to_string()],
                input: TestUtils::set_command("grapes", "4"),
            },
            CommandHandler {
                name: "INCR".to_string(),
                arguments: vec!["grapes".to_string()],
                input: TestUtils::incr_command("grapes"),
            }
        ])
    );
}

#[tokio::test]
async fn test_handle_fail_to_add_invalid_command_to_transaction() {
    let mut env = TestEnv::new_master_server();

    env.exec_command_immediate_success_response(
        TestUtils::multi_command(),
        &TestUtils::client_address(41844),
        &TestUtils::expected_simple_string("OK"),
    )
    .await;

    env.exec_command_immediate_error_response(
        TestUtils::invalid_command(&["GET", "key", "value"]),
        &TestUtils::client_address(41844),
        CommandError::InvalidGetCommand,
    )
    .await;
}

#[tokio::test]
async fn test_handle_should_return_queued_commands() {
    let mut env = TestEnv::new_master_server();

    env.exec_command_immediate_success_response(
        TestUtils::multi_command(),
        &TestUtils::client_address(41844),
        &TestUtils::expected_simple_string("OK"),
    )
    .await;

    env.exec_command_immediate_success_response(
        TestUtils::set_command("grapes", "4"),
        &TestUtils::client_address(41844),
        &TestUtils::expected_simple_string("QUEUED"),
    )
    .await;

    env.exec_command_immediate_success_response(
        TestUtils::incr_command("grapes"),
        &TestUtils::client_address(41844),
        &TestUtils::expected_simple_string("QUEUED"),
    )
    .await;

    env.exec_command_transaction_expected_commands(
        &TestUtils::client_address(41844),
        &[
            CommandHandler {
                name: "SET".to_string(),
                arguments: vec!["grapes".to_string(), "4".to_string()],
                input: TestUtils::set_command("grapes", "4"),
            },
            CommandHandler {
                name: "INCR".to_string(),
                arguments: vec!["grapes".to_string()],
                input: TestUtils::incr_command("grapes"),
            },
        ],
    )
    .await;

    let mut state_guard = env.get_state().await;
    let transaction = state_guard.get_transaction(&TestUtils::client_address(41844));
    assert_eq!(transaction, None);
}

#[tokio::test]
async fn test_handle_should_run_queued_commands() {
    let mut env = TestEnv::new_master_server();

    env.exec_command_immediate_success_response(
        TestUtils::multi_command(),
        &TestUtils::client_address(41844),
        &TestUtils::expected_simple_string("OK"),
    )
    .await;

    env.exec_command_immediate_success_response(
        TestUtils::set_command("grapes", "4"),
        &TestUtils::client_address(41844),
        &TestUtils::expected_simple_string("QUEUED"),
    )
    .await;

    env.exec_command_immediate_success_response(
        TestUtils::incr_command("grapes"),
        &TestUtils::client_address(41844),
        &TestUtils::expected_simple_string("QUEUED"),
    )
    .await;

    env.exec_command_transaction_success_response(
        &TestUtils::client_address(41844),
        "*2\r\n+OK\r\n:5\r\n",
    )
    .await;

    let mut state_guard = env.get_state().await;
    let transaction = state_guard.get_transaction(&TestUtils::client_address(41844));
    assert_eq!(transaction, None);
}

#[tokio::test]
async fn test_handle_discard_command_should_remove_transaction() {
    let mut env = TestEnv::new_master_server();

    env.exec_command_immediate_success_response(
        TestUtils::multi_command(),
        &TestUtils::client_address(41844),
        &TestUtils::expected_simple_string("OK"),
    )
    .await;

    let mut state_guard = env.get_state().await;
    let transaction = state_guard.get_transaction(&TestUtils::client_address(41844));
    assert_eq!(transaction, Some(&Vec::new()));
    drop(state_guard);

    env.exec_command_immediate_success_response(
        TestUtils::discard_command(),
        &TestUtils::client_address(41844),
        &TestUtils::expected_simple_string("OK"),
    )
    .await;

    let mut state_guard = env.get_state().await;
    let transaction = state_guard.get_transaction(&TestUtils::client_address(41844));
    assert_eq!(transaction, None);
}

#[tokio::test]
async fn test_handle_discard_command_without_using_multi_before() {
    let mut env = TestEnv::new_master_server();

    env.exec_command_immediate_error_response(
        TestUtils::discard_command(),
        &TestUtils::client_address(41844),
        CommandError::DiscardWithoutMulti,
    )
    .await;
}

#[tokio::test]
async fn test_handle_should_run_all_queued_commands_even_if_errors_occur() {
    let mut env = TestEnv::new_master_server();

    env.exec_command_immediate_success_response(
        TestUtils::multi_command(),
        &TestUtils::client_address(41844),
        &TestUtils::expected_simple_string("OK"),
    )
    .await;

    env.exec_command_immediate_success_response(
        TestUtils::set_command("grapes", "string"),
        &TestUtils::client_address(41844),
        &TestUtils::expected_simple_string("QUEUED"),
    )
    .await;

    env.exec_command_immediate_success_response(
        TestUtils::incr_command("grapes"),
        &TestUtils::client_address(41844),
        &TestUtils::expected_simple_string("QUEUED"),
    )
    .await;

    env.exec_command_transaction_success_response(
        &TestUtils::client_address(41844),
        "*2\r\n+OK\r\n-ERR value is not an integer or out of range\r\n",
    )
    .await;

    let mut state_guard = env.get_state().await;
    let transaction = state_guard.get_transaction(&TestUtils::client_address(41844));
    assert_eq!(transaction, None);
}
