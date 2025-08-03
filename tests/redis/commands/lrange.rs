use crate::test_utils::{TestEnv, TestUtils};

#[tokio::test]
async fn test_handle_lrange_command() {
    let mut env = TestEnv::new();

    env.exec_command_ok(
        TestUtils::rpush_command(
            "grape",
            &["mango", "raspberry", "apple", "banana", "kiwi", "pear"],
        ),
        &TestUtils::server_addr(41844),
        &TestUtils::expected_integer(6),
    )
    .await;

    let test_cases = vec![
        (
            TestUtils::lrange_command("grape", 0, -4),
            "*3\r\n$5\r\nmango\r\n$9\r\nraspberry\r\n$5\r\napple\r\n",
        ),
        (
            TestUtils::lrange_command("grape", -4, -1),
            "*4\r\n$5\r\napple\r\n$6\r\nbanana\r\n$4\r\nkiwi\r\n$4\r\npear\r\n",
        ),
        (
            TestUtils::lrange_command("grape", 0, -1),
            "*6\r\n$5\r\nmango\r\n$9\r\nraspberry\r\n$5\r\napple\r\n$6\r\nbanana\r\n$4\r\nkiwi\r\n$4\r\npear\r\n",
        ),
        (TestUtils::lrange_command("grape", -1, -2), "*0\r\n"),
    ];

    for (command, expected_response) in test_cases {
        env.exec_command_ok(command, &TestUtils::server_addr(41844), expected_response)
            .await;
    }
}
