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
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));
    let list = vec![RespValue::Array(vec![RespValue::BulkString("PING".into())])];

    assert_eq!(
        handle_command(list, &mut store, &mut state).await,
        Ok("+PONG\r\n".into())
    );
}

#[tokio::test]
async fn test_handle_echo_command() {
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));
    let list = vec![RespValue::Array(vec![
        RespValue::BulkString("ECHO".into()),
        RespValue::BulkString("Hello, World!".into()),
    ])];

    assert_eq!(
        handle_command(list, &mut store, &mut state).await,
        Ok("$13\r\nHello, World!\r\n".into())
    );
}

#[tokio::test]
async fn test_handle_set_command() {
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));
    let list = vec![RespValue::Array(vec![
        RespValue::BulkString("SET".into()),
        RespValue::BulkString("grape".into()),
        RespValue::BulkString("mango".into()),
    ])];

    assert_eq!(
        handle_command(list, &mut store, &mut state).await,
        Ok("+OK\r\n".into())
    );

    let store_guard = store.lock().await;
    let value = store_guard.get("grape");
    assert_eq!(
        value,
        Some(&Value {
            data: DataType::String("mango".into()),
            expiration: None,
        })
    );
}

#[tokio::test]
async fn test_handle_set_command_with_expiration() {
    tokio::time::pause();
    let now = Instant::now();

    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));
    let list = vec![RespValue::Array(vec![
        RespValue::BulkString("SET".into()),
        RespValue::BulkString("grape".into()),
        RespValue::BulkString("mango".into()),
        RespValue::BulkString("px".into()),
        RespValue::BulkString("100".into()),
    ])];

    assert_eq!(
        handle_command(list, &mut store, &mut state).await,
        Ok("+OK\r\n".into())
    );

    let store_guard = store.lock().await;
    let value = store_guard.get("grape");
    assert_eq!(
        value,
        Some(&Value {
            data: DataType::String("mango".into()),
            expiration: Some(now + Duration::from_millis(100)),
        })
    );
}

#[tokio::test]
async fn test_handle_set_command_invalid() {
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));

    let test_cases = vec![
        (
            vec![RespValue::Array(vec![
                RespValue::BulkString("SET".into()),
                RespValue::BulkString("grape".into()),
            ])],
            Err(CommandError::InvalidSetCommand),
        ),
        (
            vec![RespValue::Array(vec![
                RespValue::BulkString("SET".into()),
                RespValue::BulkString("grape".into()),
                RespValue::BulkString("mango".into()),
                RespValue::BulkString("px".into()),
            ])],
            Err(CommandError::InvalidSetCommand),
        ),
        (
            vec![RespValue::Array(vec![
                RespValue::BulkString("SET".into()),
                RespValue::BulkString("grape".into()),
                RespValue::BulkString("mango".into()),
                RespValue::BulkString("random".into()),
                RespValue::BulkString("100".into()),
            ])],
            Err(CommandError::InvalidSetCommandArgument),
        ),
        (
            vec![RespValue::Array(vec![
                RespValue::BulkString("SET".into()),
                RespValue::BulkString("grape".into()),
                RespValue::BulkString("mango".into()),
                RespValue::BulkString("px".into()),
                RespValue::BulkString("random".into()),
            ])],
            Err(CommandError::InvalidSetCommandExpiration),
        ),
    ];

    for (command, expected) in test_cases {
        assert_eq!(
            handle_command(command, &mut store, &mut state).await,
            expected
        );
    }
}

#[tokio::test]
async fn test_handle_get_command() {
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));

    let set_list = vec![RespValue::Array(vec![
        RespValue::BulkString("SET".into()),
        RespValue::BulkString("grape".into()),
        RespValue::BulkString("mango".into()),
    ])];

    assert_eq!(
        handle_command(set_list, &mut store, &mut state).await,
        Ok("+OK\r\n".into())
    );

    let get_list = vec![RespValue::Array(vec![
        RespValue::BulkString("GET".into()),
        RespValue::BulkString("grape".into()),
    ])];

    assert_eq!(
        handle_command(get_list, &mut store, &mut state).await,
        Ok("$5\r\nmango\r\n".into())
    );

    let store_guard = store.lock().await;
    let value = store_guard.get("grape");
    assert_eq!(
        value,
        Some(&Value {
            data: DataType::String("mango".into()),
            expiration: None,
        })
    );
}

#[tokio::test]
async fn test_handle_get_command_with_expiration() {
    tokio::time::pause();
    let now = Instant::now();

    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));

    let set_list = vec![RespValue::Array(vec![
        RespValue::BulkString("SET".into()),
        RespValue::BulkString("grape".into()),
        RespValue::BulkString("mango".into()),
        RespValue::BulkString("px".into()),
        RespValue::BulkString("100".into()),
    ])];

    assert_eq!(
        handle_command(set_list, &mut store, &mut state).await,
        Ok("+OK\r\n".into())
    );

    let get_list = vec![RespValue::Array(vec![
        RespValue::BulkString("GET".into()),
        RespValue::BulkString("grape".into()),
    ])];

    assert_eq!(
        handle_command(get_list, &mut store, &mut state).await,
        Ok("$5\r\nmango\r\n".into())
    );

    let store_guard = store.lock().await;
    let value = store_guard.get("grape");
    assert_eq!(
        value,
        Some(&Value {
            data: DataType::String("mango".into()),
            expiration: Some(now + Duration::from_millis(100)),
        })
    );
    drop(store_guard);

    let get_list = vec![RespValue::Array(vec![
        RespValue::BulkString("GET".into()),
        RespValue::BulkString("grape".into()),
    ])];

    tokio::time::advance(Duration::from_millis(200)).await;

    assert_eq!(
        handle_command(get_list, &mut store, &mut state).await,
        Ok("$-1\r\n".into())
    );

    let store_guard = store.lock().await;
    let value = store_guard.get("grape");
    assert_eq!(value, None);
}

#[tokio::test]
async fn test_handle_get_command_invalid() {
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));

    let test_cases = vec![
        (
            vec![RespValue::Array(vec![RespValue::BulkString("GET".into())])],
            Err(CommandError::InvalidGetCommand),
        ),
        (
            vec![RespValue::Array(vec![
                RespValue::BulkString("GET".into()),
                RespValue::BulkString("grape".into()),
                RespValue::BulkString("mango".into()),
            ])],
            Err(CommandError::InvalidGetCommand),
        ),
    ];

    for (command, expected) in test_cases {
        assert_eq!(
            handle_command(command, &mut store, &mut state).await,
            expected
        );
    }
}

#[tokio::test]
async fn test_handle_get_command_not_found() {
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));

    let get_list = vec![RespValue::Array(vec![
        RespValue::BulkString("GET".into()),
        RespValue::BulkString("grape".into()),
    ])];

    assert_eq!(
        handle_command(get_list, &mut store, &mut state).await,
        Ok("$-1\r\n".into())
    );

    let store_guard = store.lock().await;
    let value = store_guard.get("grape");
    assert_eq!(value, None);
}

#[tokio::test]
async fn test_handle_rpush_command_insert() {
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));
    let list = vec![RespValue::Array(vec![
        RespValue::BulkString("RPUSH".into()),
        RespValue::BulkString("grape".into()),
        RespValue::BulkString("mango".into()),
        RespValue::BulkString("raspberry".into()),
        RespValue::BulkString("apple".into()),
    ])];

    assert_eq!(
        handle_command(list, &mut store, &mut state).await,
        Ok(":3\r\n".into())
    );

    let store_guard = store.lock().await;
    let value = store_guard.get("grape");
    assert_eq!(
        value,
        Some(&Value {
            data: DataType::Array(VecDeque::from([
                "mango".into(),
                "raspberry".into(),
                "apple".into()
            ])),
            expiration: None,
        })
    );
}

#[tokio::test]
async fn test_handle_rpush_command_update() {
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));
    let insert_list = vec![RespValue::Array(vec![
        RespValue::BulkString("RPUSH".into()),
        RespValue::BulkString("grape".into()),
        RespValue::BulkString("mango".into()),
        RespValue::BulkString("raspberry".into()),
        RespValue::BulkString("apple".into()),
    ])];

    assert_eq!(
        handle_command(insert_list, &mut store, &mut state).await,
        Ok(":3\r\n".into())
    );

    let store_guard = store.lock().await;
    let inserted_value = store_guard.get("grape");
    assert_eq!(
        inserted_value,
        Some(&Value {
            data: DataType::Array(VecDeque::from([
                "mango".into(),
                "raspberry".into(),
                "apple".into()
            ])),
            expiration: None,
        })
    );
    drop(store_guard);

    let update_list = vec![RespValue::Array(vec![
        RespValue::BulkString("RPUSH".into()),
        RespValue::BulkString("grape".into()),
        RespValue::BulkString("pear".into()),
    ])];
    assert_eq!(
        handle_command(update_list, &mut store, &mut state).await,
        Ok(":4\r\n".into())
    );

    let store_guard = store.lock().await;
    let updated_value = store_guard.get("grape");
    assert_eq!(
        updated_value,
        Some(&Value {
            data: DataType::Array(VecDeque::from([
                "mango".into(),
                "raspberry".into(),
                "apple".into(),
                "pear".into(),
            ])),
            expiration: None,
        })
    );
}

#[tokio::test]
async fn test_handle_rpush_command_invalid() {
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));
    let list = vec![RespValue::Array(vec![
        RespValue::BulkString("RPUSH".into()),
        RespValue::BulkString("grape".into()),
    ])];

    assert_eq!(
        handle_command(list, &mut store, &mut state).await,
        Err(CommandError::InvalidRPushCommand)
    );
}

#[tokio::test]
async fn test_handle_lrange_command() {
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));

    let list = vec![RespValue::Array(vec![
        RespValue::BulkString("RPUSH".into()),
        RespValue::BulkString("grape".into()),
        RespValue::BulkString("mango".into()),
        RespValue::BulkString("raspberry".into()),
        RespValue::BulkString("apple".into()),
        RespValue::BulkString("banana".into()),
        RespValue::BulkString("kiwi".into()),
        RespValue::BulkString("pear".into()),
    ])];

    assert_eq!(
        handle_command(list, &mut store, &mut state).await,
        Ok(":6\r\n".into())
    );

    let test_cases = vec![
        (
            vec![RespValue::Array(vec![
                RespValue::BulkString("LRANGE".into()),
                RespValue::BulkString("grape".into()),
                RespValue::BulkString("0".into()),
                RespValue::BulkString("-4".into()),
            ])],
            Ok("*3\r\n$5\r\nmango\r\n$9\r\nraspberry\r\n$5\r\napple\r\n".into()),
        ),
        (
            vec![RespValue::Array(vec![
                RespValue::BulkString("LRANGE".into()),
                RespValue::BulkString("grape".into()),
                RespValue::BulkString("-4".into()),
                RespValue::BulkString("-1".into()),
            ])],
            Ok("*4\r\n$5\r\napple\r\n$6\r\nbanana\r\n$4\r\nkiwi\r\n$4\r\npear\r\n".into()),
        ),
        (
            vec![RespValue::Array(vec![
                RespValue::BulkString("LRANGE".into()),
                RespValue::BulkString("grape".into()),
                RespValue::BulkString("0".into()),
                RespValue::BulkString("-1".into()),
            ])],
            Ok("*6\r\n$5\r\nmango\r\n$9\r\nraspberry\r\n$5\r\napple\r\n$6\r\nbanana\r\n$4\r\nkiwi\r\n$4\r\npear\r\n".into()),
        ),
        (
            vec![RespValue::Array(vec![
                RespValue::BulkString("LRANGE".into()),
                RespValue::BulkString("grape".into()),
                RespValue::BulkString("-1".into()),
                RespValue::BulkString("-2".into()),
            ])],
            Ok("*0\r\n".into()),
        ),
    ];

    for (command, expected) in test_cases {
        assert_eq!(
            handle_command(command, &mut store, &mut state).await,
            expected
        );
    }
}

#[tokio::test]
async fn test_handle_lpush_command_insert() {
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));
    let list = vec![RespValue::Array(vec![
        RespValue::BulkString("LPUSH".into()),
        RespValue::BulkString("grape".into()),
        RespValue::BulkString("mango".into()),
        RespValue::BulkString("raspberry".into()),
        RespValue::BulkString("apple".into()),
    ])];

    assert_eq!(
        handle_command(list, &mut store, &mut state).await,
        Ok(":3\r\n".into())
    );

    let store_guard = store.lock().await;
    let value = store_guard.get("grape");
    assert_eq!(
        value,
        Some(&Value {
            data: DataType::Array(VecDeque::from([
                "apple".into(),
                "raspberry".into(),
                "mango".into()
            ])),
            expiration: None,
        })
    );
}

#[tokio::test]
async fn test_handle_lpush_command_update() {
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));
    let insert_list = vec![RespValue::Array(vec![
        RespValue::BulkString("LPUSH".into()),
        RespValue::BulkString("grape".into()),
        RespValue::BulkString("mango".into()),
        RespValue::BulkString("raspberry".into()),
        RespValue::BulkString("apple".into()),
    ])];

    assert_eq!(
        handle_command(insert_list, &mut store, &mut state).await,
        Ok(":3\r\n".into())
    );

    let store_guard = store.lock().await;
    let inserted_value = store_guard.get("grape");
    assert_eq!(
        inserted_value,
        Some(&Value {
            data: DataType::Array(VecDeque::from([
                "apple".into(),
                "raspberry".into(),
                "mango".into()
            ])),
            expiration: None,
        })
    );
    drop(store_guard);

    let update_list = vec![RespValue::Array(vec![
        RespValue::BulkString("LPUSH".into()),
        RespValue::BulkString("grape".into()),
        RespValue::BulkString("pear".into()),
    ])];
    assert_eq!(
        handle_command(update_list, &mut store, &mut state).await,
        Ok(":4\r\n".into())
    );

    let store_guard = store.lock().await;
    let updated_value = store_guard.get("grape");
    assert_eq!(
        updated_value,
        Some(&Value {
            data: DataType::Array(VecDeque::from([
                "pear".into(),
                "apple".into(),
                "raspberry".into(),
                "mango".into(),
            ])),
            expiration: None,
        })
    );
}

#[tokio::test]
async fn test_handle_lpush_command_invalid() {
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));
    let list = vec![RespValue::Array(vec![
        RespValue::BulkString("LPUSH".into()),
        RespValue::BulkString("grape".into()),
    ])];

    assert_eq!(
        handle_command(list, &mut store, &mut state).await,
        Err(CommandError::InvalidLPushCommand)
    );
}

#[tokio::test]
async fn test_handle_llen_command() {
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));

    let rpush_list = vec![RespValue::Array(vec![
        RespValue::BulkString("RPUSH".into()),
        RespValue::BulkString("grape".into()),
        RespValue::BulkString("mango".into()),
        RespValue::BulkString("raspberry".into()),
        RespValue::BulkString("apple".into()),
    ])];
    assert_eq!(
        handle_command(rpush_list, &mut store, &mut state).await,
        Ok(":3\r\n".into())
    );

    let llen_list = vec![RespValue::Array(vec![
        RespValue::BulkString("LLEN".into()),
        RespValue::BulkString("grape".into()),
    ])];
    assert_eq!(
        handle_command(llen_list, &mut store, &mut state).await,
        Ok(":3\r\n".into())
    );
}

#[tokio::test]
async fn test_handle_llen_command_not_found() {
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));

    let list = vec![RespValue::Array(vec![
        RespValue::BulkString("LLEN".into()),
        RespValue::BulkString("grape".into()),
    ])];
    assert_eq!(
        handle_command(list, &mut store, &mut state).await,
        Ok(":0\r\n".into())
    );
}

#[tokio::test]
async fn test_handle_llen_command_wrong_data_type() {
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));

    let set_list = vec![RespValue::Array(vec![
        RespValue::BulkString("SET".into()),
        RespValue::BulkString("grape".into()),
        RespValue::BulkString("mango".into()),
    ])];

    assert_eq!(
        handle_command(set_list, &mut store, &mut state).await,
        Ok("+OK\r\n".into())
    );

    let list = vec![RespValue::Array(vec![
        RespValue::BulkString("LLEN".into()),
        RespValue::BulkString("grape".into()),
    ])];
    assert_eq!(
        handle_command(list, &mut store, &mut state).await,
        Ok(":0\r\n".into())
    );
}

#[tokio::test]
async fn test_handle_llen_command_invalid() {
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));

    let list = vec![RespValue::Array(vec![RespValue::BulkString("LLEN".into())])];
    assert_eq!(
        handle_command(list, &mut store, &mut state).await,
        Err(CommandError::InvalidLLenCommand)
    );
}

#[tokio::test]
async fn test_handle_lpop_command() {
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));

    let rpush_list = vec![RespValue::Array(vec![
        RespValue::BulkString("RPUSH".into()),
        RespValue::BulkString("grape".into()),
        RespValue::BulkString("mango".into()),
        RespValue::BulkString("raspberry".into()),
        RespValue::BulkString("apple".into()),
    ])];
    assert_eq!(
        handle_command(rpush_list, &mut store, &mut state).await,
        Ok(":3\r\n".into())
    );

    let lpop_list = vec![RespValue::Array(vec![
        RespValue::BulkString("LPOP".into()),
        RespValue::BulkString("grape".into()),
    ])];
    assert_eq!(
        handle_command(lpop_list, &mut store, &mut state).await,
        Ok("$5\r\nmango\r\n".into())
    );
}

#[tokio::test]
async fn test_handle_lpop_command_invalid() {
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));

    let list = vec![RespValue::Array(vec![RespValue::BulkString("LPOP".into())])];
    assert_eq!(
        handle_command(list, &mut store, &mut state).await,
        Err(CommandError::InvalidLPopCommand)
    );
}

#[tokio::test]
async fn test_handle_lpop_command_not_found() {
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));

    let list = vec![RespValue::Array(vec![
        RespValue::BulkString("LPOP".into()),
        RespValue::BulkString("grape".into()),
    ])];
    assert_eq!(
        handle_command(list, &mut store, &mut state).await,
        Ok("$-1\r\n".into())
    );
}

#[tokio::test]
async fn test_handle_lpop_command_wrong_data_type() {
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));

    let set_list = vec![RespValue::Array(vec![
        RespValue::BulkString("SET".into()),
        RespValue::BulkString("grape".into()),
        RespValue::BulkString("mango".into()),
    ])];

    assert_eq!(
        handle_command(set_list, &mut store, &mut state).await,
        Ok("+OK\r\n".into())
    );

    let list = vec![RespValue::Array(vec![
        RespValue::BulkString("LPOP".into()),
        RespValue::BulkString("grape".into()),
    ])];
    assert_eq!(
        handle_command(list, &mut store, &mut state).await,
        Ok("$-1\r\n".into())
    );
}

#[tokio::test]
async fn test_handle_lpop_command_multiple_elements() {
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));

    let rpush_list = vec![RespValue::Array(vec![
        RespValue::BulkString("RPUSH".into()),
        RespValue::BulkString("grape".into()),
        RespValue::BulkString("mango".into()),
        RespValue::BulkString("raspberry".into()),
        RespValue::BulkString("apple".into()),
    ])];
    assert_eq!(
        handle_command(rpush_list, &mut store, &mut state).await,
        Ok(":3\r\n".into())
    );

    let lpop_list = vec![RespValue::Array(vec![
        RespValue::BulkString("LPOP".into()),
        RespValue::BulkString("grape".into()),
        RespValue::BulkString("2".into()),
    ])];
    assert_eq!(
        handle_command(lpop_list, &mut store, &mut state).await,
        Ok("*2\r\n$5\r\nmango\r\n$9\r\nraspberry\r\n".into())
    );
}

#[tokio::test]
async fn test_handle_blpop_command_direct_response() {
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));

    let rpush_list = vec![RespValue::Array(vec![
        RespValue::BulkString("RPUSH".into()),
        RespValue::BulkString("grape".into()),
        RespValue::BulkString("mango".into()),
        RespValue::BulkString("raspberry".into()),
        RespValue::BulkString("apple".into()),
    ])];
    assert_eq!(
        handle_command(rpush_list, &mut store, &mut state).await,
        Ok(":3\r\n".into())
    );

    let blpop_list = vec![RespValue::Array(vec![
        RespValue::BulkString("BLPOP".into()),
        RespValue::BulkString("grape".into()),
        RespValue::BulkString("0".into()),
    ])];

    assert_eq!(
        handle_command(blpop_list, &mut store, &mut state).await,
        Ok("$5\r\nmango\r\n".into())
    );
}
