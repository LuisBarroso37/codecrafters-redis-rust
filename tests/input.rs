use codecrafters_redis::input::parse_input;

#[test]
fn test_parse_input() {
    let test_cases = vec![
        (
            "*3\r\n$5\r\nRPUSH\r\n$10\r\nstrawberry\r\n$5\r\napple\r\n".as_bytes(),
            Ok(vec![
                "*3",
                "$5",
                "RPUSH",
                "$10",
                "strawberry",
                "$5",
                "apple",
            ]),
        ),
        (
            "*3\r\n*2\r\n$4\r\npear\r\n$10\r\nstrawberry\r\n$5\r\napple\r\n$6\r\nbanana\r\n"
                .as_bytes(),
            Ok(vec![
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
            ]),
        ),
    ];

    for (input, expected) in test_cases {
        assert_eq!(
            parse_input(input),
            expected,
            "parsing input {}",
            String::from_utf8_lossy(input)
        );
    }
}
