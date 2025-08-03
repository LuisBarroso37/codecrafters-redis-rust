use codecrafters_redis::commands::CommandError;

use crate::test_utils::{TestEnv, TestUtils};

#[tokio::test]
async fn test_handle_xread_command() {
    let mut env = TestEnv::new();

    for i in 0..=1 {
        let first_stream_id_part = format!("15269190304{}4", i);

        for j in 0..=3 {
            let stream_id = format!("{}-{}", &first_stream_id_part, j);

            env.exec_command_ok(
                TestUtils::xadd_command(
                    "fruits",
                    &stream_id,
                    &["mango", "apple", "raspberry", "pear"],
                ),
                &TestUtils::server_addr(41844),
                &TestUtils::expected_bulk_string(&stream_id),
            )
            .await;
        }
    }

    let test_cases = vec![
        (
            "1526919030404-1",
            "*1\r\n*2\r\n$6\r\nfruits\r\n*6\r\n*2\r\n$15\r\n1526919030404-2\r\n*4\r\n$5\r\nmango\r\n$5\r\napple\r\n$9\r\nraspberry\r\n$4\r\npear\r\n*2\r\n$15\r\n1526919030404-3\r\n*4\r\n$5\r\nmango\r\n$5\r\napple\r\n$9\r\nraspberry\r\n$4\r\npear\r\n*2\r\n$15\r\n1526919030414-0\r\n*4\r\n$5\r\nmango\r\n$5\r\napple\r\n$9\r\nraspberry\r\n$4\r\npear\r\n*2\r\n$15\r\n1526919030414-1\r\n*4\r\n$5\r\nmango\r\n$5\r\napple\r\n$9\r\nraspberry\r\n$4\r\npear\r\n*2\r\n$15\r\n1526919030414-2\r\n*4\r\n$5\r\nmango\r\n$5\r\napple\r\n$9\r\nraspberry\r\n$4\r\npear\r\n*2\r\n$15\r\n1526919030414-3\r\n*4\r\n$5\r\nmango\r\n$5\r\napple\r\n$9\r\nraspberry\r\n$4\r\npear\r\n",
        ),
        (
            "1526919030414-2",
            "*1\r\n*2\r\n$6\r\nfruits\r\n*1\r\n*2\r\n$15\r\n1526919030414-3\r\n*4\r\n$5\r\nmango\r\n$5\r\napple\r\n$9\r\nraspberry\r\n$4\r\npear\r\n",
        ),
        ("1526919030414-3", "*1\r\n*2\r\n$6\r\nfruits\r\n*0\r\n"),
    ];

    for (start_stream_id, expected_response) in test_cases {
        env.exec_command_ok(
            TestUtils::xread_command("fruits", start_stream_id),
            &TestUtils::server_addr(41844),
            expected_response,
        )
        .await;
    }
}

#[tokio::test]
async fn test_handle_xread_command_zero_zero_allowed() {
    let mut env = TestEnv::new();

    for i in 1..=2 {
        let stream_id = format!("0-{}", i);

        env.exec_command_ok(
            TestUtils::xadd_command(
                "fruits",
                &stream_id,
                &["mango", "apple", "raspberry", "pear"],
            ),
            &TestUtils::server_addr(41844),
            &TestUtils::expected_bulk_string(&stream_id),
        )
        .await;
    }

    env.exec_command_ok(
        TestUtils::xread_command("fruits", "0-0"),
        &TestUtils::server_addr(41844),
        "*1\r\n*2\r\n$6\r\nfruits\r\n*2\r\n*2\r\n$3\r\n0-1\r\n*4\r\n$5\r\nmango\r\n$5\r\napple\r\n$9\r\nraspberry\r\n$4\r\npear\r\n*2\r\n$3\r\n0-2\r\n*4\r\n$5\r\nmango\r\n$5\r\napple\r\n$9\r\nraspberry\r\n$4\r\npear\r\n",
    )
    .await;
}

#[tokio::test]
async fn test_handle_xread_command_data_not_found() {
    let mut env = TestEnv::new();

    for i in 0..=1 {
        let stream_id = format!("1526919030404-{}", i);

        env.exec_command_ok(
            TestUtils::xadd_command(
                "fruits",
                &stream_id,
                &["mango", "apple", "raspberry", "pear"],
            ),
            &TestUtils::server_addr(41844),
            &TestUtils::expected_bulk_string(&stream_id),
        )
        .await;
    }

    env.exec_command_ok(
        TestUtils::xread_command("fruits", "1526919030424-0"),
        &TestUtils::server_addr(41844),
        "*1\r\n*2\r\n$6\r\nfruits\r\n*0\r\n",
    )
    .await;
}

#[tokio::test]
async fn test_handle_xread_command_invalid_data_type() {
    let mut env = TestEnv::new();

    env.exec_command_ok(
        TestUtils::set_command("fruit", "mango"),
        &TestUtils::server_addr(41844),
        &&TestUtils::expected_simple_string("OK"),
    )
    .await;

    env.exec_command_err(
        TestUtils::xread_command("fruit", "1526919030424-0"),
        &TestUtils::server_addr(41844),
        CommandError::InvalidDataTypeForKey,
    )
    .await;
}

#[tokio::test]
async fn test_handle_xread_command_key_not_found() {
    let mut env = TestEnv::new();

    env.exec_command_err(
        TestUtils::xread_command("fruits", "1526919030424-0"),
        &TestUtils::server_addr(41844),
        CommandError::DataNotFound,
    )
    .await;
}

#[tokio::test]
async fn test_handle_xread_command_invalid() {
    let mut env = TestEnv::new();

    let test_cases = vec![
        (
            TestUtils::invalid_command(&["XREAD", "STREAMS"]),
            CommandError::InvalidXReadCommand,
        ),
        (
            TestUtils::invalid_command(&[
                "XREAD",
                "STREAMS",
                "mango",
                "1526919030424-0",
                "1526919030424-1",
            ]),
            CommandError::InvalidXReadCommand,
        ),
        (
            TestUtils::invalid_command(&["XREAD", "random", "mango", "1526919030424-0"]),
            CommandError::InvalidXReadOption,
        ),
    ];

    for (command, expected_error) in test_cases {
        env.exec_command_err(command, &TestUtils::server_addr(41844), expected_error)
            .await;
    }
}
