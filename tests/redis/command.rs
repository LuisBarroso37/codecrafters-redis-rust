use codecrafters_redis::{
    command::CommandError,
    key_value_store::{DataType, Value},
};
use std::{collections::VecDeque, time::Duration};
use tokio::time::Instant;

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

#[tokio::test]
async fn test_handle_set_command() {
    let mut env = TestEnv::new();

    env.exec_command_ok(
        TestUtils::set_command("grape", "mango"),
        &TestUtils::server_addr(41844),
        &TestUtils::expected_simple_string("OK"),
    )
    .await;

    let store_guard = env.get_store().await;
    let value = store_guard.get("grape");
    assert_eq!(
        value,
        Some(&Value {
            data: DataType::String("mango".to_string()),
            expiration: None,
        })
    );
}

#[tokio::test]
async fn test_handle_set_command_with_expiration() {
    tokio::time::pause();
    let now = Instant::now();

    let mut env = TestEnv::new();

    env.exec_command_ok(
        TestUtils::set_command_with_expiration("grape", "mango", 100),
        &TestUtils::server_addr(41844),
        &TestUtils::expected_simple_string("OK"),
    )
    .await;

    // Verify the value was stored correctly with expiration
    let store_guard = env.get_store().await;
    let value = store_guard.get("grape");
    assert_eq!(
        value,
        Some(&Value {
            data: DataType::String("mango".to_string()),
            expiration: Some(now + Duration::from_millis(100)),
        })
    );
}

#[tokio::test]
async fn test_handle_set_command_invalid() {
    let mut env = TestEnv::new();

    let test_cases = vec![
        (
            TestUtils::invalid_command(vec!["SET", "grape"]),
            CommandError::InvalidSetCommand,
        ),
        (
            TestUtils::invalid_command(vec!["SET", "grape", "mango", "px"]),
            CommandError::InvalidSetCommand,
        ),
        (
            TestUtils::invalid_command(vec!["SET", "grape", "mango", "random", "100"]),
            CommandError::InvalidSetCommandArgument,
        ),
        (
            TestUtils::invalid_command(vec!["SET", "grape", "mango", "px", "random"]),
            CommandError::InvalidSetCommandExpiration,
        ),
    ];

    for (command, expected_error) in test_cases {
        env.exec_command_err(command, &TestUtils::server_addr(41844), expected_error)
            .await;
    }
}

#[tokio::test]
async fn test_handle_get_command() {
    let mut env = TestEnv::new();

    env.exec_command_ok(
        TestUtils::set_command("grape", "mango"),
        &TestUtils::server_addr(41844),
        &TestUtils::expected_simple_string("OK"),
    )
    .await;

    env.exec_command_ok(
        TestUtils::get_command("grape"),
        &TestUtils::server_addr(41845),
        &TestUtils::expected_bulk_string("mango"),
    )
    .await;

    let store_guard = env.get_store().await;
    let value = store_guard.get("grape");
    assert_eq!(
        value,
        Some(&Value {
            data: DataType::String("mango".to_string()),
            expiration: None,
        })
    );
}

#[tokio::test]
async fn test_handle_get_command_with_expiration() {
    tokio::time::pause();
    let now = Instant::now();

    let mut env = TestEnv::new();

    env.exec_command_ok(
        TestUtils::set_command_with_expiration("grape", "mango", 100),
        &TestUtils::server_addr(41844),
        &TestUtils::expected_simple_string("OK"),
    )
    .await;

    env.exec_command_ok(
        TestUtils::get_command("grape"),
        &TestUtils::server_addr(41844),
        &TestUtils::expected_bulk_string("mango"),
    )
    .await;

    let store_guard = env.get_store().await;
    let value = store_guard.get("grape");
    assert_eq!(
        value,
        Some(&Value {
            data: DataType::String("mango".to_string()),
            expiration: Some(now + Duration::from_millis(100)),
        })
    );
    drop(store_guard);

    tokio::time::advance(Duration::from_millis(200)).await;

    env.exec_command_ok(
        TestUtils::get_command("grape"),
        &TestUtils::server_addr(41844),
        &&TestUtils::expected_null(),
    )
    .await;

    let store_guard = env.get_store().await;
    let value = store_guard.get("grape");
    assert_eq!(value, None);
}

#[tokio::test]
async fn test_handle_get_command_invalid() {
    let mut env = TestEnv::new();

    env.exec_command_ok(
        TestUtils::set_command_with_expiration("grape", "mango", 100),
        &TestUtils::server_addr(41844),
        &TestUtils::expected_simple_string("OK"),
    )
    .await;

    let test_cases = vec![
        (
            TestUtils::invalid_command(vec!["GET"]),
            CommandError::InvalidGetCommand,
        ),
        (
            TestUtils::invalid_command(vec!["GET", "grape", "mango"]),
            CommandError::InvalidGetCommand,
        ),
    ];

    for (command, expected_error) in test_cases {
        env.exec_command_err(command, &TestUtils::server_addr(41844), expected_error)
            .await;
    }
}

#[tokio::test]
async fn test_handle_get_command_not_found() {
    let mut env = TestEnv::new();

    env.exec_command_ok(
        TestUtils::get_command("grape"),
        &TestUtils::server_addr(41844),
        &&TestUtils::expected_null(),
    )
    .await;

    let store_guard = env.get_store().await;
    let value = store_guard.get("grape");
    assert_eq!(value, None);
}

#[tokio::test]
async fn test_handle_rpush_command_insert() {
    let mut env = TestEnv::new();

    env.exec_command_ok(
        TestUtils::rpush_command("grape", &["mango", "raspberry", "apple"]),
        "127.0.0.1:41844",
        &TestUtils::expected_integer(3),
    )
    .await;

    let store = env.get_store().await;
    let value = store.get("grape");
    assert_eq!(
        value,
        Some(&Value {
            data: DataType::Array(VecDeque::from([
                "mango".to_string(),
                "raspberry".to_string(),
                "apple".to_string()
            ])),
            expiration: None,
        })
    );
}

#[tokio::test]
async fn test_handle_rpush_command_update() {
    let mut env = TestEnv::new();

    env.exec_command_ok(
        TestUtils::rpush_command("grape", &["mango", "raspberry", "apple"]),
        &TestUtils::server_addr(41844),
        &TestUtils::expected_integer(3),
    )
    .await;

    let store = env.get_store().await;
    let inserted_value = store.get("grape");
    assert_eq!(
        inserted_value,
        Some(&Value {
            data: DataType::Array(VecDeque::from([
                "mango".to_string(),
                "raspberry".to_string(),
                "apple".to_string()
            ])),
            expiration: None,
        })
    );
    drop(store);

    env.exec_command_ok(
        TestUtils::rpush_command("grape", &["pear"]),
        &TestUtils::server_addr(41844),
        &TestUtils::expected_integer(4),
    )
    .await;

    let store = env.get_store().await;
    let updated_value = store.get("grape");
    assert_eq!(
        updated_value,
        Some(&Value {
            data: DataType::Array(VecDeque::from([
                "mango".to_string(),
                "raspberry".to_string(),
                "apple".to_string(),
                "pear".to_string(),
            ])),
            expiration: None,
        })
    );
}

#[tokio::test]
async fn test_handle_rpush_command_invalid() {
    let mut env = TestEnv::new();

    env.exec_command_err(
        TestUtils::invalid_command(vec!["RPUSH"]),
        &TestUtils::server_addr(41844),
        CommandError::InvalidRPushCommand,
    )
    .await;
}

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

#[tokio::test]
async fn test_handle_lpush_command_insert() {
    let mut env = TestEnv::new();

    env.exec_command_ok(
        TestUtils::lpush_command("grape", &["mango", "raspberry", "apple"]),
        &TestUtils::server_addr(41844),
        &TestUtils::expected_integer(3),
    )
    .await;

    let store_guard = env.get_store().await;
    let value = store_guard.get("grape");
    assert_eq!(
        value,
        Some(&Value {
            data: DataType::Array(VecDeque::from([
                "apple".to_string(),
                "raspberry".to_string(),
                "mango".to_string()
            ])),
            expiration: None,
        })
    );
}

#[tokio::test]
async fn test_handle_lpush_command_update() {
    let mut env = TestEnv::new();

    env.exec_command_ok(
        TestUtils::lpush_command("grape", &["mango", "raspberry", "apple"]),
        &TestUtils::server_addr(41844),
        &TestUtils::expected_integer(3),
    )
    .await;

    let store_guard = env.get_store().await;
    let inserted_value = store_guard.get("grape");
    assert_eq!(
        inserted_value,
        Some(&Value {
            data: DataType::Array(VecDeque::from([
                "apple".to_string(),
                "raspberry".to_string(),
                "mango".to_string()
            ])),
            expiration: None,
        })
    );
    drop(store_guard);

    env.exec_command_ok(
        TestUtils::lpush_command("grape", &["pear"]),
        &TestUtils::server_addr(41844),
        &TestUtils::expected_integer(4),
    )
    .await;

    let store_guard = env.get_store().await;
    let updated_value = store_guard.get("grape");
    assert_eq!(
        updated_value,
        Some(&Value {
            data: DataType::Array(VecDeque::from([
                "pear".to_string(),
                "apple".to_string(),
                "raspberry".to_string(),
                "mango".to_string(),
            ])),
            expiration: None,
        })
    );
}

#[tokio::test]
async fn test_handle_lpush_command_invalid() {
    let mut env = TestEnv::new();

    env.exec_command_err(
        TestUtils::invalid_command(vec!["LPUSH", "grape"]),
        &TestUtils::server_addr(41844),
        CommandError::InvalidLPushCommand,
    )
    .await;
}

#[tokio::test]
async fn test_handle_llen_command() {
    let mut env = TestEnv::new();

    env.exec_command_ok(
        TestUtils::rpush_command("grape", &["mango", "raspberry", "apple"]),
        &TestUtils::server_addr(41844),
        &TestUtils::expected_integer(3),
    )
    .await;

    env.exec_command_ok(
        TestUtils::llen_command("grape"),
        &TestUtils::server_addr(41844),
        &TestUtils::expected_integer(3),
    )
    .await;
}

#[tokio::test]
async fn test_handle_llen_command_not_found() {
    let mut env = TestEnv::new();

    env.exec_command_ok(
        TestUtils::llen_command("grape"),
        &TestUtils::server_addr(41844),
        &TestUtils::expected_integer(0),
    )
    .await;
}

#[tokio::test]
async fn test_handle_llen_command_wrong_data_type() {
    let mut env = TestEnv::new();

    env.exec_command_ok(
        TestUtils::set_command("grape", "mango"),
        &TestUtils::server_addr(41844),
        &TestUtils::expected_simple_string("OK"),
    )
    .await;

    env.exec_command_ok(
        TestUtils::llen_command("grape"),
        &TestUtils::server_addr(41844),
        &TestUtils::expected_integer(0),
    )
    .await;
}

#[tokio::test]
async fn test_handle_llen_command_invalid() {
    let mut env = TestEnv::new();

    env.exec_command_err(
        TestUtils::invalid_command(vec!["LLEN"]),
        &TestUtils::server_addr(41844),
        CommandError::InvalidLLenCommand,
    )
    .await;
}

#[tokio::test]
async fn test_handle_lpop_command() {
    let mut env = TestEnv::new();

    env.exec_command_ok(
        TestUtils::rpush_command("grape", &["mango", "raspberry", "apple"]),
        &TestUtils::server_addr(41844),
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

    env.exec_command_ok(
        TestUtils::lpop_command("grape"),
        &TestUtils::server_addr(41844),
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
    let mut env = TestEnv::new();

    env.exec_command_err(
        TestUtils::invalid_command(vec!["LPOP"]),
        &TestUtils::server_addr(41844),
        CommandError::InvalidLPopCommand,
    )
    .await;
}

#[tokio::test]
async fn test_handle_lpop_command_not_found() {
    let mut env = TestEnv::new();

    env.exec_command_ok(
        TestUtils::lpop_command("grape"),
        &TestUtils::server_addr(41844),
        &TestUtils::expected_null(),
    )
    .await;
}

#[tokio::test]
async fn test_handle_lpop_command_wrong_data_type() {
    let mut env = TestEnv::new();

    env.exec_command_ok(
        TestUtils::set_command("grape", "mango"),
        &TestUtils::server_addr(41844),
        &&TestUtils::expected_simple_string("OK"),
    )
    .await;

    env.exec_command_ok(
        TestUtils::lpop_command("grape"),
        &TestUtils::server_addr(41844),
        &TestUtils::expected_null(),
    )
    .await;
}

#[tokio::test]
async fn test_handle_lpop_command_multiple_elements() {
    let mut env = TestEnv::new();

    env.exec_command_ok(
        TestUtils::rpush_command("grape", &["mango", "raspberry", "apple"]),
        &TestUtils::server_addr(41844),
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

    env.exec_command_ok(
        TestUtils::lpop_command_multiple_items("grape", 2),
        &TestUtils::server_addr(41844),
        &TestUtils::expected_array(&["mango", "raspberry"]),
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

#[tokio::test]
async fn test_handle_blpop_command_direct_response() {
    let mut env = TestEnv::new();

    env.exec_command_ok(
        TestUtils::rpush_command("grape", &["mango", "raspberry", "apple"]),
        &TestUtils::server_addr(41844),
        &TestUtils::expected_integer(3),
    )
    .await;

    env.exec_command_ok(
        TestUtils::blpop_command("grape", "0"),
        &TestUtils::server_addr(41844),
        &TestUtils::expected_array(&["grape", "mango"]),
    )
    .await;
}
