use std::collections::BTreeMap;

use codecrafters_redis::{
    commands::CommandError,
    key_value_store::{DataType, Value},
};

use crate::test_utils::{TestEnv, TestUtils};

#[tokio::test]
async fn test_handle_xadd_command() {
    let mut env = TestEnv::new_master_server();
    let stream_id = "1526919030474-0";

    env.exec_command_ok(
        TestUtils::xadd_command(
            "fruits",
            stream_id,
            &["mango", "apple", "raspberry", "pear"],
        ),
        &TestUtils::client_address(41844),
        &TestUtils::expected_bulk_string(stream_id),
    )
    .await;

    let store_guard = env.get_store().await;
    let value = store_guard.get("fruits");
    assert_eq!(
        value,
        Some(&Value {
            data: DataType::Stream(BTreeMap::from([(
                stream_id.to_string(),
                BTreeMap::from([
                    ("mango".to_string(), "apple".to_string()),
                    ("raspberry".to_string(), "pear".to_string()),
                ])
            ),])),
            expiration: None,
        })
    );
}

#[tokio::test]
async fn test_handle_xadd_command_invalid_data_type() {
    let mut env = TestEnv::new_master_server();
    let stream_id = "1526919030474-0";

    env.exec_command_ok(
        TestUtils::set_command("fruits", "mango"),
        &TestUtils::client_address(41844),
        &&TestUtils::expected_simple_string("OK"),
    )
    .await;

    env.exec_command_err(
        TestUtils::xadd_command(
            "fruits",
            stream_id,
            &["mango", "apple", "raspberry", "pear"],
        ),
        &TestUtils::client_address(41844),
        CommandError::InvalidDataTypeForKey,
    )
    .await;
}

#[tokio::test]
async fn test_handle_xadd_command_invalid() {
    let mut env = TestEnv::new_master_server();

    let test_cases = vec![
        (
            TestUtils::invalid_command(&["XADD"]),
            CommandError::InvalidXAddCommand,
        ),
        (
            TestUtils::invalid_command(&["XADD", "fruits", "1526919030474-0", "mango"]),
            CommandError::InvalidXAddCommand,
        ),
        (
            TestUtils::invalid_command(&[
                "XADD",
                "fruits",
                "1526919030474-0",
                "mango",
                "apple",
                "banana",
            ]),
            CommandError::InvalidXAddCommand,
        ),
        (
            TestUtils::invalid_command(&["XADD", "fruits", "invalid_stream_id", "mango", "apple"]),
            CommandError::InvalidStreamId("Invalid stream ID format".to_string()),
        ),
    ];

    for (command, expected_error) in test_cases {
        env.exec_command_err(command, &TestUtils::client_address(41844), expected_error)
            .await;
    }
}
