use codecrafters_redis::resp::RespValue;

#[test]
fn test_parse_to_resp_values() {
    let test_cases = vec![
        (
            vec!["*3", "$5", "RPUSH", "$10", "strawberry", "$5", "apple"],
            Ok(vec![RespValue::Array(vec![
                RespValue::BulkString("RPUSH".into()),
                RespValue::BulkString("strawberry".into()),
                RespValue::BulkString("apple".into()),
            ])]),
        ),
        (
            vec![
                "*3",
                "*2",
                "$4",
                "pear",
                "$10",
                "strawberry",
                "$5",
                "apple",
                "$6",
                "banana",
            ],
            Ok(vec![RespValue::Array(vec![
                RespValue::Array(vec![
                    RespValue::BulkString("pear".into()),
                    RespValue::BulkString("strawberry".into()),
                ]),
                RespValue::BulkString("apple".into()),
                RespValue::BulkString("banana".into()),
            ])]),
        ),
    ];

    for (input, expected) in test_cases {
        assert_eq!(RespValue::parse(input), expected,);
    }
}
