use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    sync::Arc,
};

use codecrafters_redis::{
    command_utils::{get_timestamp_in_milliseconds, validate_range_indexes, validate_stream_id},
    key_value_store::{DataType, Value},
};
use tokio::sync::Mutex;

#[test]
fn test_validate_indexes() {
    let list = VecDeque::from([
        "grape".into(),
        "apple".into(),
        "pineapple".into(),
        "mango".into(),
        "raspberry".into(),
    ]);

    let test_cases = vec![
        (0, 2, Ok((0, 2))),
        (1, 3, Ok((1, 3))),
        (1, 1, Ok((1, 1))),
        (2, 9, Ok((2, 4))),
        (
            2,
            1,
            Err("Start index is bigger than end index after processing"),
        ),
        (4, 4, Ok((4, 4))),
        (5, 6, Err("Start index is out of bounds")),
        (-1, -1, Ok((4, 4))),
        (-2, -1, Ok((3, 4))),
        (-3, -1, Ok((2, 4))),
        (-9, -2, Ok((0, 3))),
        (-5, -3, Ok((0, 2))),
        (
            -2,
            -10,
            Err("Start index is bigger than end index after processing"),
        ),
    ];

    for (start_index, end_index, expected) in test_cases {
        assert_eq!(
            validate_range_indexes(&list, start_index, end_index),
            expected,
            "validating start index {} and end index {}",
            start_index,
            end_index
        );
    }
}

#[test]
fn test_get_timestamp_in_milliseconds() {
    assert!(get_timestamp_in_milliseconds().is_ok());
}

#[tokio::test]
async fn test_validate_stream_id() {
    let mut store = Arc::new(Mutex::new(HashMap::from([
        (
            "fruits".to_string(),
            Value {
                data: DataType::Stream(BTreeMap::from([
                    (
                        "0-0".to_string(),
                        HashMap::from([("apple".to_string(), "mango".to_string())]),
                    ),
                    (
                        "1-1".to_string(),
                        HashMap::from([("raspberry".to_string(), "apple".to_string())]),
                    ),
                ])),
                expiration: None,
            },
        ),
        (
            "sensor".to_string(),
            Value {
                data: DataType::Stream(BTreeMap::from([(
                    "1526919030474-0".to_string(),
                    HashMap::from([("temperature".to_string(), "37".to_string())]),
                )])),
                expiration: None,
            },
        ),
    ])));

    let test_cases = vec![
        (
            "key",
            "stream_id",
            Err("Invalid stream ID format".to_string()),
        ),
        (
            "key",
            "invalid",
            Err("Invalid stream ID format".to_string()),
        ),
        ("key", "-1-1", Err("Invalid stream ID format".to_string())),
        (
            "key",
            "invalid-1",
            Err("The ID specified in XADD must be greater than 0-0".to_string()),
        ),
        (
            "key",
            "1-invalid",
            Err("The ID specified in XADD must be greater than 0-0".to_string()),
        ),
        (
            "key",
            "0-0",
            Err("The ID specified in XADD must be greater than 0-0".to_string()),
        ),
        (
            "fruits",
            "0-2",
            Err(
                "The ID specified in XADD is equal or smaller than the target stream top item"
                    .to_string(),
            ),
        ),
        (
            "fruits",
            "1-0",
            Err(
                "The ID specified in XADD is equal or smaller than the target stream top item"
                    .to_string(),
            ),
        ),
        (
            "sensor",
            "1526919030474-0",
            Err(
                "The ID specified in XADD is equal or smaller than the target stream top item"
                    .to_string(),
            ),
        ),
        (
            "sensor",
            "1526919030474-*",
            Ok("1526919030474-1".to_string()),
        ),
        (
            "sensor",
            "1526919030474-1",
            Ok("1526919030474-1".to_string()),
        ),
        (
            "sensor",
            "1526919030474-2",
            Ok("1526919030474-2".to_string()),
        ),
        (
            "sensor",
            "1526919030484-0",
            Ok("1526919030484-0".to_string()),
        ),
        (
            "sensor",
            "1526919030484-1",
            Ok("1526919030484-1".to_string()),
        ),
        ("key", "0-*", Ok("0-1".to_string())),
    ];

    for (key, stream_id, expected_result) in test_cases {
        assert_eq!(
            validate_stream_id(&mut store, key, stream_id).await,
            expected_result
        );
    }

    assert!(validate_stream_id(&mut store, "sensor", "*").await.is_ok());
}
