use crate::test_utils::{TestEnv, TestUtils};

#[tokio::test]
async fn test_handle_ping_command() {
    let mut env = TestEnv::new();

    env.exec_command_ok(
        TestUtils::ping_command(),
        &TestUtils::server_addr(41844),
        &TestUtils::expected_simple_string("PONG"),
    )
    .await;
}
