use crate::test_utils::{TestEnv, TestUtils};

#[tokio::test]
async fn test_handle_echo_command() {
    let mut env = TestEnv::new();

    env.exec_command_ok(
        TestUtils::echo_command("Hello, World!"),
        &TestUtils::server_addr(41844),
        &TestUtils::expected_bulk_string("Hello, World!"),
    )
    .await;
}
