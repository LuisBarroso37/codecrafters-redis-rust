use std::{collections::HashMap, time::Duration};

use codecrafters_redis::commands::CommandError;

use crate::test_utils::{TestEnv, TestUtils};

#[tokio::test]
async fn test_handle_xread_command() {
    let mut env = TestEnv::new_master_server();

    for key in vec!["fruits", "exotic fruits"] {
        for i in 0..=1 {
            let first_stream_id_part = format!("15269190304{}4", i);

            for j in 0..=3 {
                let stream_id = format!("{}-{}", &first_stream_id_part, j);

                env.exec_command_ok(
                    TestUtils::xadd_command(
                        key,
                        &stream_id,
                        &["mango", "apple", "raspberry", "pear"],
                    ),
                    &TestUtils::client_address(41844),
                    &TestUtils::expected_bulk_string(&stream_id),
                )
                .await;
            }
        }
    }

    let test_cases: Vec<(&[&str], &[&str], &'static str)> = vec![
        (
            &["fruits"],
            &["1526919030404-1"],
            "*1\r\n*2\r\n$6\r\nfruits\r\n*6\r\n*2\r\n$15\r\n1526919030404-2\r\n*4\r\n$5\r\nmango\r\n$5\r\napple\r\n$9\r\nraspberry\r\n$4\r\npear\r\n*2\r\n$15\r\n1526919030404-3\r\n*4\r\n$5\r\nmango\r\n$5\r\napple\r\n$9\r\nraspberry\r\n$4\r\npear\r\n*2\r\n$15\r\n1526919030414-0\r\n*4\r\n$5\r\nmango\r\n$5\r\napple\r\n$9\r\nraspberry\r\n$4\r\npear\r\n*2\r\n$15\r\n1526919030414-1\r\n*4\r\n$5\r\nmango\r\n$5\r\napple\r\n$9\r\nraspberry\r\n$4\r\npear\r\n*2\r\n$15\r\n1526919030414-2\r\n*4\r\n$5\r\nmango\r\n$5\r\napple\r\n$9\r\nraspberry\r\n$4\r\npear\r\n*2\r\n$15\r\n1526919030414-3\r\n*4\r\n$5\r\nmango\r\n$5\r\napple\r\n$9\r\nraspberry\r\n$4\r\npear\r\n",
        ),
        (
            &["fruits"],
            &["1526919030414-2"],
            "*1\r\n*2\r\n$6\r\nfruits\r\n*1\r\n*2\r\n$15\r\n1526919030414-3\r\n*4\r\n$5\r\nmango\r\n$5\r\napple\r\n$9\r\nraspberry\r\n$4\r\npear\r\n",
        ),
        (&["fruits"], &["1526919030414-3"], "*0\r\n"),
        (
            &["fruits", "exotic fruits"],
            &["1526919030414-1", "1526919030414-2"],
            "*2\r\n*2\r\n$6\r\nfruits\r\n*2\r\n*2\r\n$15\r\n1526919030414-2\r\n*4\r\n$5\r\nmango\r\n$5\r\napple\r\n$9\r\nraspberry\r\n$4\r\npear\r\n*2\r\n$15\r\n1526919030414-3\r\n*4\r\n$5\r\nmango\r\n$5\r\napple\r\n$9\r\nraspberry\r\n$4\r\npear\r\n*2\r\n$13\r\nexotic fruits\r\n*1\r\n*2\r\n$15\r\n1526919030414-3\r\n*4\r\n$5\r\nmango\r\n$5\r\napple\r\n$9\r\nraspberry\r\n$4\r\npear\r\n",
        ),
        (
            &["fruits", "exotic fruits"],
            &["1526919030414-1", "1526919030414-3"],
            "*1\r\n*2\r\n$6\r\nfruits\r\n*2\r\n*2\r\n$15\r\n1526919030414-2\r\n*4\r\n$5\r\nmango\r\n$5\r\napple\r\n$9\r\nraspberry\r\n$4\r\npear\r\n*2\r\n$15\r\n1526919030414-3\r\n*4\r\n$5\r\nmango\r\n$5\r\napple\r\n$9\r\nraspberry\r\n$4\r\npear\r\n",
        ),
    ];

    for (keys, start_stream_ids, expected_response) in test_cases {
        env.exec_command_ok(
            TestUtils::xread_command(keys, start_stream_ids),
            &TestUtils::client_address(41844),
            expected_response,
        )
        .await;
    }
}

#[tokio::test]
async fn test_handle_xread_command_zero_zero_allowed() {
    let mut env = TestEnv::new_master_server();

    for i in 1..=2 {
        let stream_id = format!("0-{}", i);

        env.exec_command_ok(
            TestUtils::xadd_command(
                "fruits",
                &stream_id,
                &["mango", "apple", "raspberry", "pear"],
            ),
            &TestUtils::client_address(41844),
            &TestUtils::expected_bulk_string(&stream_id),
        )
        .await;
    }

    env.exec_command_ok(
        TestUtils::xread_command(&["fruits"], &["0-0"]),
        &TestUtils::client_address(41844),
        "*1\r\n*2\r\n$6\r\nfruits\r\n*2\r\n*2\r\n$3\r\n0-1\r\n*4\r\n$5\r\nmango\r\n$5\r\napple\r\n$9\r\nraspberry\r\n$4\r\npear\r\n*2\r\n$3\r\n0-2\r\n*4\r\n$5\r\nmango\r\n$5\r\napple\r\n$9\r\nraspberry\r\n$4\r\npear\r\n",
    )
    .await;
}

#[tokio::test]
async fn test_handle_xread_command_data_not_found() {
    let mut env = TestEnv::new_master_server();

    for i in 0..=1 {
        let stream_id = format!("1526919030404-{}", i);

        env.exec_command_ok(
            TestUtils::xadd_command(
                "fruits",
                &stream_id,
                &["mango", "apple", "raspberry", "pear"],
            ),
            &TestUtils::client_address(41844),
            &TestUtils::expected_bulk_string(&stream_id),
        )
        .await;
    }

    env.exec_command_ok(
        TestUtils::xread_command(&["fruits"], &["1526919030424-0"]),
        &TestUtils::client_address(41844),
        "*0\r\n",
    )
    .await;
}

#[tokio::test]
async fn test_handle_xread_command_invalid_data_type() {
    let mut env = TestEnv::new_master_server();

    env.exec_command_ok(
        TestUtils::set_command("fruit", "mango"),
        &TestUtils::client_address(41844),
        &TestUtils::expected_simple_string("OK"),
    )
    .await;

    env.exec_command_err(
        TestUtils::xread_command(&["fruit"], &["1526919030424-0"]),
        &TestUtils::client_address(41844),
        CommandError::InvalidDataTypeForKey,
    )
    .await;
}

#[tokio::test]
async fn test_handle_xread_command_key_not_found() {
    let mut env = TestEnv::new_master_server();

    env.exec_command_ok(
        TestUtils::xread_command(&["fruits"], &["1526919030424-0"]),
        &TestUtils::client_address(41844),
        &TestUtils::expected_bulk_string_array(&[]),
    )
    .await;
}

#[tokio::test]
async fn test_handle_xread_command_invalid() {
    let mut env = TestEnv::new_master_server();

    let test_cases = vec![
        (
            TestUtils::invalid_command(&["XREAD", "STREAMS"]),
            CommandError::InvalidXReadCommand,
        ),
        (
            TestUtils::invalid_command(&[
                "XREAD",
                "STREAMS",
                "mango",
                "1526919030424-0",
                "1526919030424-1",
            ]),
            CommandError::InvalidXReadCommand,
        ),
        (
            TestUtils::invalid_command(&["XREAD", "random", "mango", "1526919030424-0"]),
            CommandError::InvalidXReadOption,
        ),
        (
            TestUtils::invalid_command(&[
                "XREAD",
                "BLOCK",
                "invalid",
                "STREAMS",
                "mango",
                "1526919030424-0",
            ]),
            CommandError::InvalidXReadBlockDuration,
        ),
        (
            TestUtils::invalid_command(&[
                "XREAD",
                "invalid",
                "100",
                "STREAMS",
                "mango",
                "1526919030424-0",
            ]),
            CommandError::InvalidXReadOption,
        ),
        (
            TestUtils::invalid_command(&[
                "XREAD",
                "BLOCK",
                "1000",
                "invalid",
                "mango",
                "1526919030424-0",
            ]),
            CommandError::InvalidXReadOption,
        ),
    ];

    for (command, expected_error) in test_cases {
        env.exec_command_err(command, &TestUtils::client_address(41844), expected_error)
            .await;
    }
}

#[tokio::test]
async fn test_handle_xread_blocking_command_direct_response() {
    let mut env = TestEnv::new_master_server();

    let start_stream_id = "1526919030404-0";
    let added_stream_id = "1526919030404-1";

    env.exec_command_ok(
        TestUtils::xadd_command(
            "fruits",
            &added_stream_id,
            &["mango", "apple", "raspberry", "pear"],
        ),
        &TestUtils::client_address(41844),
        &TestUtils::expected_bulk_string(&added_stream_id),
    )
    .await;

    env.exec_command_ok(
        TestUtils::xread_blocking_command("0", &["fruits"], &[start_stream_id]),
        &TestUtils::client_address(41844),
        "*1\r\n*2\r\n$6\r\nfruits\r\n*1\r\n*2\r\n$15\r\n1526919030404-1\r\n*4\r\n$5\r\nmango\r\n$5\r\napple\r\n$9\r\nraspberry\r\n$4\r\npear\r\n",
    )
    .await;
}

#[tokio::test]
async fn test_xread_simple_blocking() {
    let env = TestEnv::new_master_server();

    let start_stream_id = "1526919030404-0";
    let added_stream_id = "1526919030404-1";

    // Client tries to XREAD from empty stream (should block)
    let client_task = TestUtils::spawn_xread_task(
        &env,
        &["fruits"],
        &[start_stream_id],
        "2000",
        &TestUtils::client_address(12345),
    );

    // Give client time to register as subscriber
    TestUtils::sleep_ms(500).await;

    // Push an element to unblock the client
    let mut env_mut = env.clone();

    env_mut
        .exec_command_ok(
            TestUtils::xadd_command(
                "fruits",
                added_stream_id,
                &["mango", "apple", "raspberry", "pear"],
            ),
            &TestUtils::client_address(41844),
            &TestUtils::expected_bulk_string(added_stream_id),
        )
        .await;

    // Wait for client to complete
    let client_result = TestUtils::wait_for_completion(client_task, Duration::from_secs(3)).await;

    // Client should get the item
    assert_eq!(client_result, Ok("*1\r\n*2\r\n$6\r\nfruits\r\n*1\r\n*2\r\n$15\r\n1526919030404-1\r\n*4\r\n$5\r\nmango\r\n$5\r\napple\r\n$9\r\nraspberry\r\n$4\r\npear\r\n".to_string()));
}

#[tokio::test]
async fn test_xread_blocking_timeout_behavior() {
    let mut env = TestEnv::new_master_server();

    let start_time = std::time::Instant::now();

    // Client tries XREAD with timeout on empty stream
    // Should timeout and return null
    env.exec_command_ok(
        TestUtils::xread_blocking_command("1000", &["empty_stream"], &["1526919030404-1"]),
        &TestUtils::client_address(12350),
        &TestUtils::expected_null(),
    )
    .await;

    let elapsed = start_time.elapsed();

    // Should take approximately 1 second (allow some tolerance)
    assert!(elapsed >= Duration::from_millis(900));
    assert!(elapsed <= Duration::from_millis(1200));
}

#[tokio::test]
async fn test_xread_zero_timeout_infinite_wait() {
    let env = TestEnv::new_master_server();

    let start_stream_id = "1526919030404-0";
    let added_stream_id = "1526919030404-1";

    // Client tries XREAD with zero timeout (infinite wait)
    let xread_task = TestUtils::spawn_xread_task(
        &env,
        &["fruits"],
        &[start_stream_id],
        "0", // Infinite timeout
        &TestUtils::client_address(12351),
    );

    // Wait a bit to ensure the client is blocking
    TestUtils::sleep_ms(200).await;

    // Push an item to unblock the client
    let mut env_mut = env.clone();

    env_mut
        .exec_command_ok(
            TestUtils::xadd_command(
                "fruits",
                added_stream_id,
                &["mango", "apple", "raspberry", "pear"],
            ),
            &TestUtils::client_address(41844),
            &TestUtils::expected_bulk_string(added_stream_id),
        )
        .await;

    // The XREAD should now complete
    let xread_result = TestUtils::wait_for_completion(xread_task, Duration::from_secs(1)).await;

    assert_eq!(xread_result, Ok("*1\r\n*2\r\n$6\r\nfruits\r\n*1\r\n*2\r\n$15\r\n1526919030404-1\r\n*4\r\n$5\r\nmango\r\n$5\r\napple\r\n$9\r\nraspberry\r\n$4\r\npear\r\n".to_string()));
}

#[tokio::test]
async fn test_xread_concurrent_clients_direct_response() {
    let mut env = TestEnv::new_master_server();

    let start_stream_id = "1526919030404-0";
    let added_stream_id = "1526919030404-1";

    // Pre-populate the stream
    env.exec_command_ok(
        TestUtils::xadd_command(
            "fruits",
            &added_stream_id,
            &["mango", "apple", "raspberry", "pear"],
        ),
        &TestUtils::client_address(41844),
        &TestUtils::expected_bulk_string(&added_stream_id),
    )
    .await;

    // Multiple clients try to XREAD simultaneously from populated list
    let mut tasks = vec![];

    for i in 0..3 {
        let client_addr = format!("127.0.0.1:1238{}", i + 1);
        let task = TestUtils::spawn_xread_task(
            &env,
            &["fruits"],
            &[start_stream_id],
            "1000",
            &client_addr,
        );

        tasks.push(task);
    }

    // Wait for all tasks to complete
    let mut results = vec![];
    for task in tasks {
        let result = TestUtils::wait_for_completion(task, Duration::from_millis(500)).await;
        results.push(result);
    }

    // All clients should get an item immediately (no blocking needed)
    let successful_results =
        TestUtils::filter_successful_results_containing(&results, added_stream_id);

    assert_eq!(
        successful_results.len(),
        3,
        "All clients should get an item immediately"
    );

    // Each result should be properly formatted
    for result in successful_results.iter() {
        assert_eq!(
            result.as_str(),
            "*1\r\n*2\r\n$6\r\nfruits\r\n*1\r\n*2\r\n$15\r\n1526919030404-1\r\n*4\r\n$5\r\nmango\r\n$5\r\napple\r\n$9\r\nraspberry\r\n$4\r\npear\r\n"
        );
    }
}

#[tokio::test]
async fn test_xread_concurrent_clients_different_keys() {
    let mut env = TestEnv::new_master_server();

    let start_stream_id = "1526919030404-0";
    let added_stream_id = "1526919030404-1";

    // Start clients waiting on different keys
    let mut tasks = HashMap::new();

    for i in 0..3 {
        let key_name = format!("fruits_{}", i);
        let client_addr = format!("127.0.0.1:1241{}", i);

        let task = TestUtils::spawn_xread_task(
            &env,
            &[&key_name],
            &[start_stream_id],
            "2000",
            &client_addr,
        );

        tasks.insert(key_name, task);
    }

    // Give clients time to register
    TestUtils::sleep_ms(200).await;

    // Push to each client
    for i in 0..3 {
        let key_name = format!("fruits_{}", i);
        let server_port = 12420 + i;

        env.exec_command_ok(
            TestUtils::xadd_command(&key_name, &added_stream_id, &["mango", "apple"]),
            &TestUtils::client_address(server_port),
            &TestUtils::expected_bulk_string(&added_stream_id),
        )
        .await;

        TestUtils::sleep_ms(50).await;
    }

    // Collect all results
    let mut results = vec![];

    for (key, task) in tasks.into_iter() {
        let result = TestUtils::wait_for_completion(task, Duration::from_secs(3)).await;
        results.push((key, result));
    }

    // All clients should get their respective items
    for (key, result) in results {
        assert!(result.is_ok());
        let response = result.unwrap();
        assert!(response.contains(&key));
        assert!(response.contains("apple"));
    }
}

#[tokio::test]
async fn test_xread_concurrent_clients_multiple_pushes_with_incremental_stream_ids() {
    let env = TestEnv::new_master_server();

    // Start multiple XREAD clients
    let mut xread_tasks = vec![];

    for i in 0..3 {
        let start_stream_id = format!("1526919030404-{}", i);
        let client_addr = format!("127.0.0.1:1236{}", i);

        let task = TestUtils::spawn_xread_task(
            &env,
            &["fruits"],
            &[&start_stream_id],
            "5000",
            &client_addr,
        );

        xread_tasks.push(task);
    }

    // Give clients time to register
    TestUtils::sleep_ms(100).await;

    // Push multiple items in sequence
    for i in 0..3 {
        let mut env_mut = env.clone();
        let client_addr = format!("127.0.0.1:1237{}", i);
        let added_stream_id = format!("1526919030404-{}", i + 1);

        env_mut
            .exec_command_ok(
                TestUtils::xadd_command(
                    "fruits",
                    &added_stream_id,
                    &["mango", "apple", "raspberry", "pear"],
                ),
                &client_addr,
                &TestUtils::expected_bulk_string(&added_stream_id),
            )
            .await;

        // Small delay between pushes
        TestUtils::sleep_ms(50).await;
    }

    // Collect all results
    let mut results = vec![];
    for task in xread_tasks {
        let result = TestUtils::wait_for_completion(task, Duration::from_secs(1)).await;
        results.push(result);
    }

    // All clients should get an item
    let successful_results = TestUtils::filter_successful_results_containing(&results, "fruits");
    assert_eq!(
        successful_results.len(),
        3,
        "All clients should get an item"
    );

    // All clients should get their respective items
    for (i, result) in results.iter().enumerate() {
        assert!(result.is_ok());
        let response = result.as_ref().unwrap();
        assert!(response.contains("fruits"));
        assert!(response.contains(format!("1526919030404-{}", i + 1).as_str()));
    }
}

#[tokio::test]
async fn test_xread_concurrent_clients_fanout() {
    let env = TestEnv::new_master_server();

    let start_stream_id = "1526919030404-0";
    let added_stream_id = "1526919030404-1";

    // Start multiple XREAD clients
    let mut xread_tasks = vec![];

    for i in 0..3 {
        let client_addr = format!("127.0.0.1:1236{}", i);

        let task = TestUtils::spawn_xread_task(
            &env,
            &["fruits"],
            &[start_stream_id],
            "5000",
            &client_addr,
        );

        xread_tasks.push(task);
    }

    // Give clients time to register
    TestUtils::sleep_ms(100).await;

    // Push item
    let mut env_mut = env.clone();

    env_mut
        .exec_command_ok(
            TestUtils::xadd_command(
                "fruits",
                &added_stream_id,
                &["mango", "apple", "raspberry", "pear"],
            ),
            &TestUtils::client_address(41882),
            &TestUtils::expected_bulk_string(&added_stream_id),
        )
        .await;

    // Collect all results
    let mut results = vec![];
    for task in xread_tasks {
        let result = TestUtils::wait_for_completion(task, Duration::from_secs(1)).await;
        results.push(result);
    }

    // All clients should get the same item
    let successful_results = TestUtils::filter_successful_results_containing(&results, "fruits");
    assert_eq!(
        successful_results.len(),
        3,
        "All clients should get an item"
    );

    // All clients should get their respective items
    for result in results.iter() {
        assert!(result.is_ok());
        let response = result.as_ref().unwrap();
        assert!(response.contains("fruits"));
        assert!(response.contains(format!("1526919030404-1").as_str()));
    }
}

#[tokio::test]
async fn test_xread_simple_blocking_with_special_id_return_immediately_if_stream_is_empty() {
    let env = TestEnv::new_master_server();
    // Client tries to XREAD from empty stream (should immediately return)
    let client_task = TestUtils::spawn_xread_task(
        &env,
        &["fruits"],
        &["$"],
        "2000",
        &TestUtils::client_address(12345),
    );

    // Wait for client to complete
    let client_result = TestUtils::wait_for_completion(client_task, Duration::from_secs(3)).await;

    // Client should get empty array
    assert_eq!(client_result, Ok("*0\r\n".to_string()));
}

#[tokio::test]
async fn test_xread_simple_blocking_with_special_id() {
    let mut env = TestEnv::new_master_server();

    env.exec_command_ok(
        TestUtils::xadd_command(
            "fruits",
            "1526919030404-0",
            &["mango", "apple", "raspberry", "pear"],
        ),
        &TestUtils::client_address(41844),
        &TestUtils::expected_bulk_string("1526919030404-0"),
    )
    .await;

    // Client tries to XREAD from empty stream (should block)
    let client_task = TestUtils::spawn_xread_task(
        &env.clone(),
        &["fruits"],
        &["$"],
        "2000",
        &TestUtils::client_address(12345),
    );

    // Give client time to register as subscriber
    TestUtils::sleep_ms(500).await;

    // Push an element to unblock the client
    let mut env_mut = env.clone();

    env_mut
        .exec_command_ok(
            TestUtils::xadd_command(
                "fruits",
                "1526919030404-1",
                &["mango", "apple", "raspberry", "pear"],
            ),
            &TestUtils::client_address(41844),
            &TestUtils::expected_bulk_string("1526919030404-1"),
        )
        .await;

    // Wait for client to complete
    let client_result = TestUtils::wait_for_completion(client_task, Duration::from_secs(3)).await;

    // Client should get the item
    assert_eq!(client_result, Ok("*1\r\n*2\r\n$6\r\nfruits\r\n*1\r\n*2\r\n$15\r\n1526919030404-1\r\n*4\r\n$5\r\nmango\r\n$5\r\napple\r\n$9\r\nraspberry\r\n$4\r\npear\r\n".to_string()));
}

#[tokio::test]
async fn test_xread_multiple_streams_single_client() {
    let env = TestEnv::new_master_server();

    let start_stream_id_fruits = "1526919030404-0";
    let start_stream_id_vegetables = "1526919030404-0";

    // Client tries to XREAD from multiple empty streams (should block)
    let client_task = TestUtils::spawn_xread_task(
        &env,
        &["fruits", "vegetables"],
        &[start_stream_id_fruits, start_stream_id_vegetables],
        "3000",
        &TestUtils::client_address(12345),
    );

    // Give client time to register as subscriber
    TestUtils::sleep_ms(500).await;

    // Push elements to both streams to unblock the client
    let mut env_mut = env.clone();

    env_mut
        .exec_command_ok(
            TestUtils::xadd_command("fruits", "1526919030404-1", &["mango", "apple"]),
            &TestUtils::client_address(41844),
            &TestUtils::expected_bulk_string("1526919030404-1"),
        )
        .await;

    env_mut
        .exec_command_ok(
            TestUtils::xadd_command("vegetables", "1526919030404-1", &["carrot", "potato"]),
            &TestUtils::client_address(41845),
            &TestUtils::expected_bulk_string("1526919030404-1"),
        )
        .await;

    // Wait for client to complete
    let client_result = TestUtils::wait_for_completion(client_task, Duration::from_secs(3)).await;

    // Client should get items from both streams
    assert!(client_result.is_ok());
    let response = client_result.unwrap();
    assert!(response.contains("fruits"));
    assert!(response.contains("vegetables"));
    assert!(response.contains("mango"));
    assert!(response.contains("carrot"));
}

#[tokio::test]
async fn test_xread_multiple_streams_concurrent_clients_partial_match() {
    let env = TestEnv::new_master_server();

    let start_stream_id = "1526919030404-0";

    // Client 1 listens to fruits and vegetables
    let client1_task = TestUtils::spawn_xread_task(
        &env,
        &["fruits", "vegetables"],
        &[start_stream_id, start_stream_id],
        "3000",
        &TestUtils::client_address(12345),
    );

    // Client 2 listens to only vegetables and animals
    let client2_task = TestUtils::spawn_xread_task(
        &env,
        &["vegetables", "animals"],
        &[start_stream_id, start_stream_id],
        "800",
        &TestUtils::client_address(12346),
    );

    // Give clients time to register
    TestUtils::sleep_ms(500).await;

    // Push only to fruits stream - should only unblock client1
    let mut env_mut = env.clone();

    env_mut
        .exec_command_ok(
            TestUtils::xadd_command("fruits", "1526919030404-1", &["mango", "apple"]),
            &TestUtils::client_address(41844),
            &TestUtils::expected_bulk_string("1526919030404-1"),
        )
        .await;

    // Wait for client1 to complete, client2 should timeout
    let client1_result = TestUtils::wait_for_completion(client1_task, Duration::from_secs(2)).await;
    let client2_result = TestUtils::wait_for_completion(client2_task, Duration::from_secs(1)).await;

    // Client1 should get the fruits item
    assert!(client1_result.is_ok());
    let response1 = client1_result.unwrap();
    assert!(response1.contains("fruits"));
    assert!(response1.contains("mango"));

    // Client2 should timeout (return null)
    assert!(client2_result.is_ok());
    let response2 = client2_result.unwrap();
    assert_eq!(response2, TestUtils::expected_null());
}

#[tokio::test]
async fn test_xread_multiple_streams_concurrent_clients_same_streams() {
    let env = TestEnv::new_master_server();

    let start_stream_id = "1526919030404-0";

    // Multiple clients listening to the same streams
    let mut client_tasks = vec![];

    for i in 0..3 {
        let client_addr = format!("127.0.0.1:1234{}", i + 5);
        let task = TestUtils::spawn_xread_task(
            &env,
            &["fruits", "vegetables"],
            &[start_stream_id, start_stream_id],
            "3000",
            &client_addr,
        );
        client_tasks.push(task);
    }

    // Give clients time to register
    TestUtils::sleep_ms(500).await;

    // Push to one of the streams
    let mut env_mut = env.clone();

    env_mut
        .exec_command_ok(
            TestUtils::xadd_command("fruits", "1526919030404-1", &["mango", "apple"]),
            &TestUtils::client_address(41844),
            &TestUtils::expected_bulk_string("1526919030404-1"),
        )
        .await;

    // Collect all results
    let mut results = vec![];
    for task in client_tasks {
        let result = TestUtils::wait_for_completion(task, Duration::from_secs(2)).await;
        results.push(result);
    }

    // All clients should get responses with both streams
    let successful_results = TestUtils::filter_successful_results_containing(&results, "fruits");
    assert_eq!(
        successful_results.len(),
        3,
        "All clients should get responses"
    );

    // Each response should contain both streams
    for result in successful_results.iter() {
        assert!(result.contains("fruits"));
        assert!(!result.contains("vegetables"));
        assert!(result.contains("mango"));
        assert!(!result.contains("carrot"));
    }
}

#[tokio::test]
async fn test_xread_multiple_streams_concurrent_clients_special_ids() {
    let mut env = TestEnv::new_master_server();

    // Pre-populate both streams
    env.exec_command_ok(
        TestUtils::xadd_command("fruits", "1526919030404-0", &["orange", "banana"]),
        &TestUtils::client_address(41844),
        &TestUtils::expected_bulk_string("1526919030404-0"),
    )
    .await;

    env.exec_command_ok(
        TestUtils::xadd_command("vegetables", "1526919030404-0", &["onion", "garlic"]),
        &TestUtils::client_address(41845),
        &TestUtils::expected_bulk_string("1526919030404-0"),
    )
    .await;

    // Multiple clients listening with special ID "$" (after last entry)
    let mut client_tasks = vec![];

    for i in 0..3 {
        let client_addr = format!("127.0.0.1:1235{}", i + 5);
        let task = TestUtils::spawn_xread_task(
            &env,
            &["fruits", "vegetables"],
            &["$", "$"],
            "3000",
            &client_addr,
        );
        client_tasks.push(task);
    }

    // Give clients time to register
    TestUtils::sleep_ms(500).await;

    // Push new entries to both streams
    env.exec_command_ok(
        TestUtils::xadd_command("fruits", "1526919030404-1", &["mango", "apple"]),
        &TestUtils::client_address(41844),
        &TestUtils::expected_bulk_string("1526919030404-1"),
    )
    .await;

    env.exec_command_ok(
        TestUtils::xadd_command("vegetables", "1526919030404-1", &["carrot", "potato"]),
        &TestUtils::client_address(41845),
        &TestUtils::expected_bulk_string("1526919030404-1"),
    )
    .await;

    // Collect all results
    let mut results = vec![];
    for task in client_tasks {
        let result = TestUtils::wait_for_completion(task, Duration::from_secs(2)).await;
        results.push(result);
    }

    // All clients should get responses with both streams
    let successful_results = TestUtils::filter_successful_results_containing(&results, "fruits");
    assert_eq!(
        successful_results.len(),
        3,
        "All clients should get responses"
    );

    // Each response should contain both streams with the new entries
    for result in successful_results.iter() {
        assert!(result.contains("fruits"));
        assert!(result.contains("vegetables"));
        assert!(result.contains("mango"));
        assert!(result.contains("carrot"));
        // Should NOT contain the pre-existing entries
        assert!(!result.contains("orange"));
        assert!(!result.contains("onion"));
    }
}
