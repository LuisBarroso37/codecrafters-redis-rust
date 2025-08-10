use codecrafters_redis::server::{CliError, RedisServer};

#[test]
fn test_redis_server_creation_without_flags() {
    let args = vec!["codecrafters-redis".to_string()];

    let server = RedisServer::new(args).unwrap();
    assert_eq!(server.port, 6379);
}

#[test]
fn test_redis_server_creation_with_invalid_port_flag() {
    let test_cases = vec![
        (
            vec![
                "codecrafters-redis".to_string(),
                "--port".to_string(),
                "invalid".to_string(),
            ],
            CliError::InvalidCommandLineFlagValue,
        ),
        (
            vec![
                "codecrafters-redis".to_string(),
                "--port".to_string(),
                "70000".to_string(),
            ],
            CliError::InvalidCommandLineFlagValue,
        ),
        (
            vec!["codecrafters-redis".to_string(), "invalid".to_string()],
            CliError::InvalidCommandLineFlag,
        ),
    ];

    for (args, expected_error) in test_cases {
        let result = RedisServer::new(args);
        assert_eq!(result.is_err(), true);
        assert_eq!(result.unwrap_err(), expected_error);
    }
}

#[test]
fn test_redis_server_creation_with_port_flag() {
    let args = vec![
        "codecrafters-redis".to_string(),
        "--port".to_string(),
        "6677".to_string(),
    ];

    let server = RedisServer::new(args).unwrap();
    assert_eq!(server.port, 6677);
}
