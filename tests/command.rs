use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::Duration,
};

use codecrafters_redis::{
    command::{CommandError, handle_command},
    key_value_store::{DataType, KeyValueStore, Value},
    resp::RespValue,
    state::State,
};
use tokio::{sync::Mutex, time::Instant};

#[tokio::test]
async fn test_handle_ping_command() {
    let server_addr = "127.0.0.1:41844".to_string();
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));
    let list = vec![RespValue::Array(vec![RespValue::BulkString(
        "PING".to_string(),
    )])];

    assert_eq!(
        handle_command(server_addr, list, &mut store, &mut state).await,
        Ok("+PONG\r\n".to_string())
    );
}

#[tokio::test]
async fn test_handle_echo_command() {
    let server_addr = "127.0.0.1:41844".to_string();
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));
    let list = vec![RespValue::Array(vec![
        RespValue::BulkString("ECHO".to_string()),
        RespValue::BulkString("Hello, World!".to_string()),
    ])];

    assert_eq!(
        handle_command(server_addr, list, &mut store, &mut state).await,
        Ok("$13\r\nHello, World!\r\n".to_string())
    );
}

#[tokio::test]
async fn test_handle_set_command() {
    let server_addr = "127.0.0.1:41844".to_string();
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));
    let list = vec![RespValue::Array(vec![
        RespValue::BulkString("SET".to_string()),
        RespValue::BulkString("grape".to_string()),
        RespValue::BulkString("mango".to_string()),
    ])];

    assert_eq!(
        handle_command(server_addr, list, &mut store, &mut state).await,
        Ok("+OK\r\n".to_string())
    );

    let store_guard = store.lock().await;
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

    let server_addr = "127.0.0.1:41844".to_string();
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));
    let list = vec![RespValue::Array(vec![
        RespValue::BulkString("SET".to_string()),
        RespValue::BulkString("grape".to_string()),
        RespValue::BulkString("mango".to_string()),
        RespValue::BulkString("px".to_string()),
        RespValue::BulkString("100".to_string()),
    ])];

    assert_eq!(
        handle_command(server_addr, list, &mut store, &mut state).await,
        Ok("+OK\r\n".to_string())
    );

    let store_guard = store.lock().await;
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
    let server_addr = "127.0.0.1:41844".to_string();
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));

    let test_cases = vec![
        (
            vec![RespValue::Array(vec![
                RespValue::BulkString("SET".to_string()),
                RespValue::BulkString("grape".to_string()),
            ])],
            Err(CommandError::InvalidSetCommand),
        ),
        (
            vec![RespValue::Array(vec![
                RespValue::BulkString("SET".to_string()),
                RespValue::BulkString("grape".to_string()),
                RespValue::BulkString("mango".to_string()),
                RespValue::BulkString("px".to_string()),
            ])],
            Err(CommandError::InvalidSetCommand),
        ),
        (
            vec![RespValue::Array(vec![
                RespValue::BulkString("SET".to_string()),
                RespValue::BulkString("grape".to_string()),
                RespValue::BulkString("mango".to_string()),
                RespValue::BulkString("random".to_string()),
                RespValue::BulkString("100".to_string()),
            ])],
            Err(CommandError::InvalidSetCommandArgument),
        ),
        (
            vec![RespValue::Array(vec![
                RespValue::BulkString("SET".to_string()),
                RespValue::BulkString("grape".to_string()),
                RespValue::BulkString("mango".to_string()),
                RespValue::BulkString("px".to_string()),
                RespValue::BulkString("random".to_string()),
            ])],
            Err(CommandError::InvalidSetCommandExpiration),
        ),
    ];

    for (command, expected) in test_cases {
        assert_eq!(
            handle_command(server_addr.clone(), command, &mut store, &mut state).await,
            expected
        );
    }
}

#[tokio::test]
async fn test_handle_get_command() {
    let server_addr = "127.0.0.1:41844".to_string();
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));

    let set_list = vec![RespValue::Array(vec![
        RespValue::BulkString("SET".to_string()),
        RespValue::BulkString("grape".to_string()),
        RespValue::BulkString("mango".to_string()),
    ])];

    assert_eq!(
        handle_command(server_addr.clone(), set_list, &mut store, &mut state).await,
        Ok("+OK\r\n".to_string())
    );

    let get_list = vec![RespValue::Array(vec![
        RespValue::BulkString("GET".to_string()),
        RespValue::BulkString("grape".to_string()),
    ])];

    assert_eq!(
        handle_command(server_addr, get_list, &mut store, &mut state).await,
        Ok("$5\r\nmango\r\n".to_string())
    );

    let store_guard = store.lock().await;
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

    let server_addr = "127.0.0.1:41844".to_string();
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));

    let set_list = vec![RespValue::Array(vec![
        RespValue::BulkString("SET".to_string()),
        RespValue::BulkString("grape".to_string()),
        RespValue::BulkString("mango".to_string()),
        RespValue::BulkString("px".to_string()),
        RespValue::BulkString("100".to_string()),
    ])];

    assert_eq!(
        handle_command(server_addr.clone(), set_list, &mut store, &mut state).await,
        Ok("+OK\r\n".to_string())
    );

    let get_list = vec![RespValue::Array(vec![
        RespValue::BulkString("GET".to_string()),
        RespValue::BulkString("grape".to_string()),
    ])];

    assert_eq!(
        handle_command(server_addr.clone(), get_list, &mut store, &mut state).await,
        Ok("$5\r\nmango\r\n".to_string())
    );

    let store_guard = store.lock().await;
    let value = store_guard.get("grape");
    assert_eq!(
        value,
        Some(&Value {
            data: DataType::String("mango".to_string()),
            expiration: Some(now + Duration::from_millis(100)),
        })
    );
    drop(store_guard);

    let get_list = vec![RespValue::Array(vec![
        RespValue::BulkString("GET".to_string()),
        RespValue::BulkString("grape".to_string()),
    ])];

    tokio::time::advance(Duration::from_millis(200)).await;

    assert_eq!(
        handle_command(server_addr, get_list, &mut store, &mut state).await,
        Ok("$-1\r\n".to_string())
    );

    let store_guard = store.lock().await;
    let value = store_guard.get("grape");
    assert_eq!(value, None);
}

#[tokio::test]
async fn test_handle_get_command_invalid() {
    let server_addr = "127.0.0.1:41844".to_string();
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));

    let test_cases = vec![
        (
            vec![RespValue::Array(vec![RespValue::BulkString(
                "GET".to_string(),
            )])],
            Err(CommandError::InvalidGetCommand),
        ),
        (
            vec![RespValue::Array(vec![
                RespValue::BulkString("GET".to_string()),
                RespValue::BulkString("grape".to_string()),
                RespValue::BulkString("mango".to_string()),
            ])],
            Err(CommandError::InvalidGetCommand),
        ),
    ];

    for (command, expected) in test_cases {
        assert_eq!(
            handle_command(server_addr.clone(), command, &mut store, &mut state).await,
            expected
        );
    }
}

#[tokio::test]
async fn test_handle_get_command_not_found() {
    let server_addr = "127.0.0.1:41844".to_string();
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));

    let get_list = vec![RespValue::Array(vec![
        RespValue::BulkString("GET".to_string()),
        RespValue::BulkString("grape".to_string()),
    ])];

    assert_eq!(
        handle_command(server_addr, get_list, &mut store, &mut state).await,
        Ok("$-1\r\n".to_string())
    );

    let store_guard = store.lock().await;
    let value = store_guard.get("grape");
    assert_eq!(value, None);
}

#[tokio::test]
async fn test_handle_rpush_command_insert() {
    let server_addr = "127.0.0.1:41844".to_string();
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));
    let list = vec![RespValue::Array(vec![
        RespValue::BulkString("RPUSH".to_string()),
        RespValue::BulkString("grape".to_string()),
        RespValue::BulkString("mango".to_string()),
        RespValue::BulkString("raspberry".to_string()),
        RespValue::BulkString("apple".to_string()),
    ])];

    assert_eq!(
        handle_command(server_addr, list, &mut store, &mut state).await,
        Ok(":3\r\n".to_string())
    );

    let store_guard = store.lock().await;
    let value = store_guard.get("grape");
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
    let server_addr = "127.0.0.1:41844".to_string();
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));
    let insert_list = vec![RespValue::Array(vec![
        RespValue::BulkString("RPUSH".to_string()),
        RespValue::BulkString("grape".to_string()),
        RespValue::BulkString("mango".to_string()),
        RespValue::BulkString("raspberry".to_string()),
        RespValue::BulkString("apple".to_string()),
    ])];

    assert_eq!(
        handle_command(server_addr.clone(), insert_list, &mut store, &mut state).await,
        Ok(":3\r\n".to_string())
    );

    let store_guard = store.lock().await;
    let inserted_value = store_guard.get("grape");
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
    drop(store_guard);

    let update_list = vec![RespValue::Array(vec![
        RespValue::BulkString("RPUSH".to_string()),
        RespValue::BulkString("grape".to_string()),
        RespValue::BulkString("pear".to_string()),
    ])];
    assert_eq!(
        handle_command(server_addr, update_list, &mut store, &mut state).await,
        Ok(":4\r\n".to_string())
    );

    let store_guard = store.lock().await;
    let updated_value = store_guard.get("grape");
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
    let server_addr = "127.0.0.1:41844".to_string();
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));
    let list = vec![RespValue::Array(vec![
        RespValue::BulkString("RPUSH".to_string()),
        RespValue::BulkString("grape".to_string()),
    ])];

    assert_eq!(
        handle_command(server_addr, list, &mut store, &mut state).await,
        Err(CommandError::InvalidRPushCommand)
    );
}

#[tokio::test]
async fn test_handle_lrange_command() {
    let server_addr = "127.0.0.1:41844".to_string();
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));

    let list = vec![RespValue::Array(vec![
        RespValue::BulkString("RPUSH".to_string()),
        RespValue::BulkString("grape".to_string()),
        RespValue::BulkString("mango".to_string()),
        RespValue::BulkString("raspberry".to_string()),
        RespValue::BulkString("apple".to_string()),
        RespValue::BulkString("banana".to_string()),
        RespValue::BulkString("kiwi".to_string()),
        RespValue::BulkString("pear".to_string()),
    ])];

    assert_eq!(
        handle_command(server_addr.clone(), list, &mut store, &mut state).await,
        Ok(":6\r\n".to_string())
    );

    let test_cases = vec![
        (
            vec![RespValue::Array(vec![
                RespValue::BulkString("LRANGE".to_string()),
                RespValue::BulkString("grape".to_string()),
                RespValue::BulkString("0".to_string()),
                RespValue::BulkString("-4".to_string()),
            ])],
            Ok("*3\r\n$5\r\nmango\r\n$9\r\nraspberry\r\n$5\r\napple\r\n".to_string()),
        ),
        (
            vec![RespValue::Array(vec![
                RespValue::BulkString("LRANGE".to_string()),
                RespValue::BulkString("grape".to_string()),
                RespValue::BulkString("-4".to_string()),
                RespValue::BulkString("-1".to_string()),
            ])],
            Ok("*4\r\n$5\r\napple\r\n$6\r\nbanana\r\n$4\r\nkiwi\r\n$4\r\npear\r\n".to_string()),
        ),
        (
            vec![RespValue::Array(vec![
                RespValue::BulkString("LRANGE".to_string()),
                RespValue::BulkString("grape".to_string()),
                RespValue::BulkString("0".to_string()),
                RespValue::BulkString("-1".to_string()),
            ])],
            Ok("*6\r\n$5\r\nmango\r\n$9\r\nraspberry\r\n$5\r\napple\r\n$6\r\nbanana\r\n$4\r\nkiwi\r\n$4\r\npear\r\n".to_string()),
        ),
        (
            vec![RespValue::Array(vec![
                RespValue::BulkString("LRANGE".to_string()),
                RespValue::BulkString("grape".to_string()),
                RespValue::BulkString("-1".to_string()),
                RespValue::BulkString("-2".to_string()),
            ])],
            Ok("*0\r\n".to_string()),
        ),
    ];

    for (command, expected) in test_cases {
        assert_eq!(
            handle_command(server_addr.clone(), command, &mut store, &mut state).await,
            expected
        );
    }
}

#[tokio::test]
async fn test_handle_lpush_command_insert() {
    let server_addr = "127.0.0.1:41844".to_string();
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));
    let list = vec![RespValue::Array(vec![
        RespValue::BulkString("LPUSH".to_string()),
        RespValue::BulkString("grape".to_string()),
        RespValue::BulkString("mango".to_string()),
        RespValue::BulkString("raspberry".to_string()),
        RespValue::BulkString("apple".to_string()),
    ])];

    assert_eq!(
        handle_command(server_addr, list, &mut store, &mut state).await,
        Ok(":3\r\n".to_string())
    );

    let store_guard = store.lock().await;
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
    let server_addr = "127.0.0.1:41844".to_string();
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));
    let insert_list = vec![RespValue::Array(vec![
        RespValue::BulkString("LPUSH".to_string()),
        RespValue::BulkString("grape".to_string()),
        RespValue::BulkString("mango".to_string()),
        RespValue::BulkString("raspberry".to_string()),
        RespValue::BulkString("apple".to_string()),
    ])];

    assert_eq!(
        handle_command(server_addr.clone(), insert_list, &mut store, &mut state).await,
        Ok(":3\r\n".to_string())
    );

    let store_guard = store.lock().await;
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

    let update_list = vec![RespValue::Array(vec![
        RespValue::BulkString("LPUSH".to_string()),
        RespValue::BulkString("grape".to_string()),
        RespValue::BulkString("pear".to_string()),
    ])];
    assert_eq!(
        handle_command(server_addr, update_list, &mut store, &mut state).await,
        Ok(":4\r\n".to_string())
    );

    let store_guard = store.lock().await;
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
    let server_addr = "127.0.0.1:41844".to_string();
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));
    let list = vec![RespValue::Array(vec![
        RespValue::BulkString("LPUSH".to_string()),
        RespValue::BulkString("grape".to_string()),
    ])];

    assert_eq!(
        handle_command(server_addr, list, &mut store, &mut state).await,
        Err(CommandError::InvalidLPushCommand)
    );
}

#[tokio::test]
async fn test_handle_llen_command() {
    let server_addr = "127.0.0.1:41844".to_string();
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));

    let rpush_list = vec![RespValue::Array(vec![
        RespValue::BulkString("RPUSH".to_string()),
        RespValue::BulkString("grape".to_string()),
        RespValue::BulkString("mango".to_string()),
        RespValue::BulkString("raspberry".to_string()),
        RespValue::BulkString("apple".to_string()),
    ])];
    assert_eq!(
        handle_command(server_addr.clone(), rpush_list, &mut store, &mut state).await,
        Ok(":3\r\n".to_string())
    );

    let llen_list = vec![RespValue::Array(vec![
        RespValue::BulkString("LLEN".to_string()),
        RespValue::BulkString("grape".to_string()),
    ])];
    assert_eq!(
        handle_command(server_addr, llen_list, &mut store, &mut state).await,
        Ok(":3\r\n".to_string())
    );
}

#[tokio::test]
async fn test_handle_llen_command_not_found() {
    let server_addr = "127.0.0.1:41844".to_string();
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));

    let list = vec![RespValue::Array(vec![
        RespValue::BulkString("LLEN".to_string()),
        RespValue::BulkString("grape".to_string()),
    ])];
    assert_eq!(
        handle_command(server_addr, list, &mut store, &mut state).await,
        Ok(":0\r\n".to_string())
    );
}

#[tokio::test]
async fn test_handle_llen_command_wrong_data_type() {
    let server_addr = "127.0.0.1:41844".to_string();
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));

    let set_list = vec![RespValue::Array(vec![
        RespValue::BulkString("SET".to_string()),
        RespValue::BulkString("grape".to_string()),
        RespValue::BulkString("mango".to_string()),
    ])];

    assert_eq!(
        handle_command(server_addr.clone(), set_list, &mut store, &mut state).await,
        Ok("+OK\r\n".to_string())
    );

    let list = vec![RespValue::Array(vec![
        RespValue::BulkString("LLEN".to_string()),
        RespValue::BulkString("grape".to_string()),
    ])];
    assert_eq!(
        handle_command(server_addr, list, &mut store, &mut state).await,
        Ok(":0\r\n".to_string())
    );
}

#[tokio::test]
async fn test_handle_llen_command_invalid() {
    let server_addr = "127.0.0.1:41844".to_string();
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));

    let list = vec![RespValue::Array(vec![RespValue::BulkString(
        "LLEN".to_string(),
    )])];
    assert_eq!(
        handle_command(server_addr, list, &mut store, &mut state).await,
        Err(CommandError::InvalidLLenCommand)
    );
}

#[tokio::test]
async fn test_handle_lpop_command() {
    let server_addr = "127.0.0.1:41844".to_string();
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));

    let rpush_list = vec![RespValue::Array(vec![
        RespValue::BulkString("RPUSH".to_string()),
        RespValue::BulkString("grape".to_string()),
        RespValue::BulkString("mango".to_string()),
        RespValue::BulkString("raspberry".to_string()),
        RespValue::BulkString("apple".to_string()),
    ])];
    assert_eq!(
        handle_command(server_addr.clone(), rpush_list, &mut store, &mut state).await,
        Ok(":3\r\n".to_string())
    );

    let lpop_list = vec![RespValue::Array(vec![
        RespValue::BulkString("LPOP".to_string()),
        RespValue::BulkString("grape".to_string()),
    ])];
    assert_eq!(
        handle_command(server_addr, lpop_list, &mut store, &mut state).await,
        Ok("$5\r\nmango\r\n".to_string())
    );
}

#[tokio::test]
async fn test_handle_lpop_command_invalid() {
    let server_addr = "127.0.0.1:41844".to_string();
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));

    let list = vec![RespValue::Array(vec![RespValue::BulkString(
        "LPOP".to_string(),
    )])];
    assert_eq!(
        handle_command(server_addr, list, &mut store, &mut state).await,
        Err(CommandError::InvalidLPopCommand)
    );
}

#[tokio::test]
async fn test_handle_lpop_command_not_found() {
    let server_addr = "127.0.0.1:41844".to_string();
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));

    let list = vec![RespValue::Array(vec![
        RespValue::BulkString("LPOP".to_string()),
        RespValue::BulkString("grape".to_string()),
    ])];
    assert_eq!(
        handle_command(server_addr, list, &mut store, &mut state).await,
        Ok("$-1\r\n".to_string())
    );
}

#[tokio::test]
async fn test_handle_lpop_command_wrong_data_type() {
    let server_addr = "127.0.0.1:41844".to_string();
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));

    let set_list = vec![RespValue::Array(vec![
        RespValue::BulkString("SET".to_string()),
        RespValue::BulkString("grape".to_string()),
        RespValue::BulkString("mango".to_string()),
    ])];

    assert_eq!(
        handle_command(server_addr.clone(), set_list, &mut store, &mut state).await,
        Ok("+OK\r\n".to_string())
    );

    let list = vec![RespValue::Array(vec![
        RespValue::BulkString("LPOP".to_string()),
        RespValue::BulkString("grape".to_string()),
    ])];
    assert_eq!(
        handle_command(server_addr, list, &mut store, &mut state).await,
        Ok("$-1\r\n".to_string())
    );
}

#[tokio::test]
async fn test_handle_lpop_command_multiple_elements() {
    let server_addr = "127.0.0.1:41844".to_string();
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));

    let rpush_list = vec![RespValue::Array(vec![
        RespValue::BulkString("RPUSH".to_string()),
        RespValue::BulkString("grape".to_string()),
        RespValue::BulkString("mango".to_string()),
        RespValue::BulkString("raspberry".to_string()),
        RespValue::BulkString("apple".to_string()),
    ])];
    assert_eq!(
        handle_command(server_addr.clone(), rpush_list, &mut store, &mut state).await,
        Ok(":3\r\n".to_string())
    );

    let lpop_list = vec![RespValue::Array(vec![
        RespValue::BulkString("LPOP".to_string()),
        RespValue::BulkString("grape".to_string()),
        RespValue::BulkString("2".to_string()),
    ])];
    assert_eq!(
        handle_command(server_addr, lpop_list, &mut store, &mut state).await,
        Ok("*2\r\n$5\r\nmango\r\n$9\r\nraspberry\r\n".to_string())
    );
}

#[tokio::test]
async fn test_handle_blpop_command_direct_response() {
    let server_addr = "127.0.0.1:41844".to_string();
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));

    let rpush_list = vec![RespValue::Array(vec![
        RespValue::BulkString("RPUSH".to_string()),
        RespValue::BulkString("grape".to_string()),
        RespValue::BulkString("mango".to_string()),
        RespValue::BulkString("raspberry".to_string()),
        RespValue::BulkString("apple".to_string()),
    ])];
    assert_eq!(
        handle_command(server_addr.clone(), rpush_list, &mut store, &mut state).await,
        Ok(":3\r\n".to_string())
    );

    let blpop_list = vec![RespValue::Array(vec![
        RespValue::BulkString("BLPOP".to_string()),
        RespValue::BulkString("grape".to_string()),
        RespValue::BulkString("0".to_string()),
    ])];

    assert_eq!(
        handle_command(server_addr, blpop_list, &mut store, &mut state).await,
        Ok("*2\r\n$5\r\ngrape\r\n$5\r\nmango\r\n".to_string())
    );
}
