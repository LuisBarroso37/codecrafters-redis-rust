use codecrafters_redis::commands::{CommandError, CommandResult};

use crate::test_utils::{TestEnv, TestUtils};

#[tokio::test]
async fn test_handle_keys_command() {
    let mut env = TestEnv::new_master_server();

    env.exec_command_immediate_success_response(
        TestUtils::rpush_command("fruits", &["mango", "raspberry", "apple"]),
        &TestUtils::client_address(41844),
        &TestUtils::expected_integer(3),
    )
    .await;
    env.exec_command_immediate_success_response(
        TestUtils::set_command("france", "paris"),
        &TestUtils::client_address(41844),
        &TestUtils::expected_simple_string("OK"),
    )
    .await;
    env.exec_command_immediate_success_response(
        TestUtils::set_command("random", "value"),
        &TestUtils::client_address(41844),
        &TestUtils::expected_simple_string("OK"),
    )
    .await;

    let test_cases = vec![
        (TestUtils::keys_command("*"), "*3"),
        (TestUtils::keys_command("fr*"), "*2"),
        (TestUtils::keys_command("f*"), "*2"),
        (TestUtils::keys_command("france*"), "*1"),
        (TestUtils::keys_command("random"), "*1"),
        (TestUtils::keys_command("random_key"), "*0"),
    ];

    for (command, expected_resp_array_length) in test_cases {
        let result = env
            .exec_command(command, &TestUtils::client_address(41844))
            .await;
        assert!(result.is_ok());

        let command_result = result.unwrap();
        let response = match command_result {
            CommandResult::Response(resp) => resp,
            _ => panic!("Expected response, got something else"),
        };

        assert_eq!(&response[..2], expected_resp_array_length);
    }
}

#[tokio::test]
async fn test_handle_echo_command_invalid() {
    let mut env = TestEnv::new_master_server();

    let test_cases = vec![
        (
            TestUtils::invalid_command(&["KEYS"]),
            CommandError::InvalidKeysCommand,
        ),
        (
            TestUtils::invalid_command(&["KEYS", "grape", "mango"]),
            CommandError::InvalidKeysCommand,
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
