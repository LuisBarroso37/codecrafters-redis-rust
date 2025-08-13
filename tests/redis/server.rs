use codecrafters_redis::server::{CliError, RedisRole, RedisServer};

#[test]
fn test_redis_server_creation_without_flags() {
    let args = vec!["codecrafters-redis".to_string()];

    let server = RedisServer::new(args).unwrap();
    assert_eq!(server.port, 6379);
}

#[test]
fn test_redis_server_creation_with_invalid_flags() {
    let test_cases = vec![
        (
            vec!["codecrafters-redis".to_string(), "--port".to_string()],
            CliError::InvalidCommandLineFlag,
        ),
        (
            vec![
                "codecrafters-redis".to_string(),
                "--port".to_string(),
                "invalid".to_string(),
            ],
            CliError::InvalidPortFlagValue,
        ),
        (
            vec![
                "codecrafters-redis".to_string(),
                "--port".to_string(),
                "70000".to_string(),
            ],
            CliError::InvalidPortFlagValue,
        ),
        (
            vec!["codecrafters-redis".to_string(), "invalid".to_string()],
            CliError::InvalidCommandLineFlag,
        ),
        (
            vec!["codecrafters-redis".to_string(), "--replicaof".to_string()],
            CliError::InvalidCommandLineFlag,
        ),
        (
            vec![
                "codecrafters-redis".to_string(),
                "--replicaof".to_string(),
                "invalid".to_string(),
            ],
            CliError::InvalidMasterAddress,
        ),
        (
            vec![
                "codecrafters-redis".to_string(),
                "--replicaof".to_string(),
                "127.0.0.1 invalid".to_string(),
            ],
            CliError::InvalidMasterPort,
        ),
        (
            vec![
                "codecrafters-redis".to_string(),
                "--replicaof".to_string(),
                "127.0.0.1".to_string(),
            ],
            CliError::InvalidMasterAddress,
        ),
        (
            vec![
                "codecrafters-redis".to_string(),
                "--replicaof".to_string(),
                "256.0.0.1 6379".to_string(),
            ],
            CliError::InvalidMasterAddress,
        ),
        (
            vec![
                "codecrafters-redis".to_string(),
                "--replicaof".to_string(),
                "my_host! 6379".to_string(),
            ],
            CliError::InvalidMasterAddress,
        ),
        (
            vec![
                "codecrafters-redis".to_string(),
                "--replicaof".to_string(),
                "127.0.0.1 70000".to_string(),
            ],
            CliError::InvalidMasterPort,
        ),
    ];

    for (args, expected_error) in test_cases {
        let result = RedisServer::new(args);
        assert_eq!(result.is_err(), true);
        assert_eq!(result.unwrap_err(), expected_error);
    }
}

#[test]
fn test_redis_server_creation_success_cases() {
    let test_cases = vec![
        (
            vec!["codecrafters-redis".to_string()],
            6379,
            RedisRole::Master,
        ),
        (
            vec![
                "codecrafters-redis".to_string(),
                "--port".to_string(),
                "6677".to_string(),
            ],
            6677,
            RedisRole::Master,
        ),
        (
            vec![
                "codecrafters-redis".to_string(),
                "--replicaof".to_string(),
                "127.0.0.1 6380".to_string(),
            ],
            6379,
            RedisRole::Replica(("127.0.0.1".to_string(), 6380)),
        ),
        (
            vec![
                "codecrafters-redis".to_string(),
                "--port".to_string(),
                "7000".to_string(),
                "--replicaof".to_string(),
                "localhost 6381".to_string(),
            ],
            7000,
            RedisRole::Replica(("localhost".to_string(), 6381)),
        ),
        (
            vec![
                "codecrafters-redis".to_string(),
                "--port".to_string(),
                "8000".to_string(),
                "--replicaof".to_string(),
                "redis-master 6500".to_string(),
            ],
            8000,
            RedisRole::Replica(("redis-master".to_string(), 6500)),
        ),
    ];

    for (args, expected_port, expected_role) in test_cases {
        let server = RedisServer::new(args).unwrap();
        assert_eq!(server.port, expected_port);
        assert_eq!(server.role, expected_role);
    }
}
