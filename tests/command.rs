use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use codecrafters_redis::{
    command::{CommandError, handle_command},
    key_value_store::{DataType, KeyValueStore, Value},
    resp::RespValue,
};
use tokio::time::Instant;

#[test]
fn test_handle_ping_command() {
    let store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let list = vec![RespValue::Array(vec![RespValue::BulkString("PING".into())])];

    let mut st = store.lock().unwrap();
    assert_eq!(handle_command(list, &mut st), Ok("+PONG\r\n".into()));
}

#[test]
fn test_handle_echo_command() {
    let store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let list = vec![RespValue::Array(vec![
        RespValue::BulkString("ECHO".into()),
        RespValue::BulkString("Hello, World!".into()),
    ])];

    let mut st = store.lock().unwrap();
    assert_eq!(
        handle_command(list, &mut st),
        Ok("$13\r\nHello, World!\r\n".into())
    );
}

#[test]
fn test_handle_set_command() {
    let store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let list = vec![RespValue::Array(vec![
        RespValue::BulkString("SET".into()),
        RespValue::BulkString("grape".into()),
        RespValue::BulkString("mango".into()),
    ])];

    let mut st = store.lock().unwrap();
    assert_eq!(handle_command(list, &mut st), Ok("+OK\r\n".into()));

    let value = st.get("grape");
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

    let store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let list = vec![RespValue::Array(vec![
        RespValue::BulkString("SET".into()),
        RespValue::BulkString("grape".into()),
        RespValue::BulkString("mango".into()),
        RespValue::BulkString("px".into()),
        RespValue::BulkString("100".into()),
    ])];

    let mut st = store.lock().unwrap();
    assert_eq!(handle_command(list, &mut st), Ok("+OK\r\n".into()));

    let value = st.get("grape");
    assert_eq!(
        value,
        Some(&Value {
            data: DataType::String("mango".into()),
            expiration: Some(now + Duration::from_millis(100)),
        })
    );
}

#[test]
fn test_handle_set_command_invalid() {
    let store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut st = store.lock().unwrap();

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
        assert_eq!(handle_command(command, &mut st), expected);
    }
}

#[test]
fn test_handle_get_command() {
    let store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut st = store.lock().unwrap();

    let set_list = vec![RespValue::Array(vec![
        RespValue::BulkString("SET".into()),
        RespValue::BulkString("grape".into()),
        RespValue::BulkString("mango".into()),
    ])];

    assert_eq!(handle_command(set_list, &mut st), Ok("+OK\r\n".into()));

    let get_list = vec![RespValue::Array(vec![
        RespValue::BulkString("GET".into()),
        RespValue::BulkString("grape".into()),
    ])];

    assert_eq!(
        handle_command(get_list, &mut st),
        Ok("$5\r\nmango\r\n".into())
    );

    let value = st.get("grape");
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

    let store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut st = store.lock().unwrap();

    let set_list = vec![RespValue::Array(vec![
        RespValue::BulkString("SET".into()),
        RespValue::BulkString("grape".into()),
        RespValue::BulkString("mango".into()),
        RespValue::BulkString("px".into()),
        RespValue::BulkString("100".into()),
    ])];

    assert_eq!(handle_command(set_list, &mut st), Ok("+OK\r\n".into()));

    let get_list = vec![RespValue::Array(vec![
        RespValue::BulkString("GET".into()),
        RespValue::BulkString("grape".into()),
    ])];

    assert_eq!(
        handle_command(get_list, &mut st),
        Ok("$5\r\nmango\r\n".into())
    );

    let value = st.get("grape");
    assert_eq!(
        value,
        Some(&Value {
            data: DataType::String("mango".into()),
            expiration: Some(now + Duration::from_millis(100)),
        })
    );

    let get_list = vec![RespValue::Array(vec![
        RespValue::BulkString("GET".into()),
        RespValue::BulkString("grape".into()),
    ])];

    tokio::time::advance(Duration::from_millis(200)).await;

    assert_eq!(handle_command(get_list, &mut st), Ok("$-1\r\n".into()));

    let value = st.get("grape");
    assert_eq!(value, None);
}

#[test]
fn test_handle_get_command_invalid() {
    let store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut st = store.lock().unwrap();

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
        assert_eq!(handle_command(command, &mut st), expected);
    }
}

#[test]
fn test_handle_get_command_not_found() {
    let store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut st = store.lock().unwrap();

    let get_list = vec![RespValue::Array(vec![
        RespValue::BulkString("GET".into()),
        RespValue::BulkString("grape".into()),
    ])];

    assert_eq!(handle_command(get_list, &mut st), Ok("$-1\r\n".into()));

    let value = st.get("grape");
    assert_eq!(value, None);
}

#[test]
fn test_handle_rpush_command_insert() {
    let store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let list = vec![RespValue::Array(vec![
        RespValue::BulkString("RPUSH".into()),
        RespValue::BulkString("grape".into()),
        RespValue::BulkString("mango".into()),
        RespValue::BulkString("raspberry".into()),
        RespValue::BulkString("apple".into()),
    ])];

    let mut st = store.lock().unwrap();
    assert_eq!(handle_command(list, &mut st), Ok(":3\r\n".into()));

    let value = st.get("grape");
    assert_eq!(
        value,
        Some(&Value {
            data: DataType::Array(vec!["mango".into(), "raspberry".into(), "apple".into(),]),
            expiration: None,
        })
    );
}

#[test]
fn test_handle_rpush_command_update() {
    let store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let insert_list = vec![RespValue::Array(vec![
        RespValue::BulkString("RPUSH".into()),
        RespValue::BulkString("grape".into()),
        RespValue::BulkString("mango".into()),
        RespValue::BulkString("raspberry".into()),
        RespValue::BulkString("apple".into()),
    ])];

    let mut st = store.lock().unwrap();
    assert_eq!(handle_command(insert_list, &mut st), Ok(":3\r\n".into()));

    let inserted_value = st.get("grape");
    assert_eq!(
        inserted_value,
        Some(&Value {
            data: DataType::Array(vec!["mango".into(), "raspberry".into(), "apple".into(),]),
            expiration: None,
        })
    );

    let update_list = vec![RespValue::Array(vec![
        RespValue::BulkString("RPUSH".into()),
        RespValue::BulkString("grape".into()),
        RespValue::BulkString("pear".into()),
    ])];
    assert_eq!(handle_command(update_list, &mut st), Ok(":4\r\n".into()));

    let updated_value = st.get("grape");
    assert_eq!(
        updated_value,
        Some(&Value {
            data: DataType::Array(vec![
                "mango".into(),
                "raspberry".into(),
                "apple".into(),
                "pear".into(),
            ]),
            expiration: None,
        })
    );
}

#[test]
fn test_handle_rpush_command_invalid() {
    let store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let list = vec![RespValue::Array(vec![
        RespValue::BulkString("RPUSH".into()),
        RespValue::BulkString("grape".into()),
    ])];

    let mut st = store.lock().unwrap();
    assert_eq!(
        handle_command(list, &mut st),
        Err(CommandError::InvalidRPushCommand)
    );
}

#[test]
fn test_handle_lrange_command() {
    let store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut st = store.lock().unwrap();

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

    assert_eq!(handle_command(list, &mut st), Ok(":6\r\n".into()));

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
        assert_eq!(handle_command(command, &mut st), expected);
    }
}
