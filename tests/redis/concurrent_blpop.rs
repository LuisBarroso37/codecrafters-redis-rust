use std::{collections::HashMap, time::Duration};

use codecrafters_redis::command::CommandError;

use crate::test_utils::{TestEnv, TestUtils};

#[tokio::test]
async fn test_blpop_concurrent_clients_simple_blocking() {
    let env = TestEnv::new();

    // Client tries to BLPOP from empty list (should block)
    let client_task =
        TestUtils::spawn_blpop_task(&env, "test_list", "2", &TestUtils::server_addr(12345));

    // Give client time to register as subscriber
    TestUtils::sleep_ms(500).await;

    // Push an element to unblock the client
    let mut env_mut = env.clone();

    env_mut
        .exec_command_ok(
            TestUtils::rpush_command("test_list", &["item1"]),
            &TestUtils::server_addr(12347),
            &TestUtils::expected_integer(1),
        )
        .await;

    // Wait for client to complete
    let client_result = TestUtils::wait_for_completion(client_task, Duration::from_secs(3)).await;

    // Client should get the item
    assert_eq!(
        client_result,
        Ok(TestUtils::expected_array(&["test_list", "item1"]))
    );
}

#[tokio::test]
async fn test_blpop_concurrent_clients_first_come_first_served() {
    let mut env = TestEnv::new();

    env.exec_command_ok(
        TestUtils::rpush_command("test_queue", &["single_item"]),
        &TestUtils::server_addr(12340),
        &TestUtils::expected_integer(1),
    )
    .await;

    // Multiple clients try to BLPOP simultaneously
    let mut tasks = vec![];

    for i in 0..3 {
        let client_addr = format!("127.0.0.1:1234{}", i + 1);

        let task = TestUtils::spawn_blpop_task(&env, "test_queue", "1", &client_addr);

        tasks.push(task);
    }

    // Wait for all tasks to complete
    let mut results = vec![];
    for task in tasks {
        let result = TestUtils::wait_for_completion(task, Duration::from_secs(2)).await;
        results.push(result);
    }

    // Only one client should get the item, others should timeout
    let successful_results =
        TestUtils::filter_successful_results_containing(&results, "single_item");
    assert_eq!(
        successful_results.len(),
        1,
        "Only one client should get the item"
    );

    // The successful result should be properly formatted
    assert_eq!(
        successful_results[0],
        &TestUtils::expected_array(&["test_queue", "single_item"])
    );
}

#[tokio::test]
async fn test_blpop_timeout_behavior() {
    let mut env = TestEnv::new();

    let start_time = std::time::Instant::now();

    // Client tries BLPOP with timeout on empty list
    // Should timeout and return null
    env.exec_command_ok(
        TestUtils::blpop_command("empty_list", "1"),
        &TestUtils::server_addr(12350),
        &TestUtils::expected_null(),
    )
    .await;

    let elapsed = start_time.elapsed();

    // Should take approximately 1 second (allow some tolerance)
    assert!(elapsed >= Duration::from_millis(900));
    assert!(elapsed <= Duration::from_millis(1200));
}

#[tokio::test]
async fn test_blpop_zero_timeout_infinite_wait() {
    let env = TestEnv::new();

    // Client tries BLPOP with zero timeout (infinite wait)
    let blpop_task = TestUtils::spawn_blpop_task(
        &env,
        "infinite_list",
        "0", // Infinite timeout
        &TestUtils::server_addr(12351),
    );

    // Wait a bit to ensure the client is blocking
    TestUtils::sleep_ms(200).await;

    // Push an item to unblock the client
    let mut env_mut = env.clone();

    env_mut
        .exec_command_ok(
            TestUtils::rpush_command("infinite_list", &["unblock_item"]),
            &TestUtils::server_addr(12352),
            &TestUtils::expected_integer(1),
        )
        .await;

    // The BLPOP should now complete
    let blpop_result = TestUtils::wait_for_completion(blpop_task, Duration::from_secs(1)).await;

    assert_eq!(
        blpop_result,
        Ok(TestUtils::expected_array(&[
            "infinite_list",
            "unblock_item"
        ]))
    );
}

#[tokio::test]
async fn test_blpop_multiple_pushes_multiple_clients() {
    let env = TestEnv::new();

    // Start multiple BLPOP clients
    let mut blpop_tasks = vec![];

    for i in 0..3 {
        let client_addr = format!("127.0.0.1:1236{}", i);

        let task = TestUtils::spawn_blpop_task(&env, "multi_queue", "5", &client_addr);

        blpop_tasks.push(task);
    }

    // Give clients time to register
    TestUtils::sleep_ms(100).await;

    // Push multiple items in sequence
    for i in 0..3 {
        let mut env_mut = env.clone();
        let client_addr = format!("127.0.0.1:1237{}", i);

        env_mut
            .exec_command_ok(
                TestUtils::rpush_command("multi_queue", &[format!("item_{}", i).as_str()]),
                &client_addr,
                &TestUtils::expected_integer(1),
            )
            .await;

        // Small delay between pushes
        TestUtils::sleep_ms(50).await;
    }

    // Collect all results
    let mut results = vec![];
    for task in blpop_tasks {
        let result = TestUtils::wait_for_completion(task, Duration::from_secs(1)).await;
        results.push(result);
    }

    // All clients should get an item
    let successful_results = TestUtils::filter_successful_results_containing(&results, "item_");
    assert_eq!(
        successful_results.len(),
        3,
        "All clients should get an item"
    );

    // Each result should be properly formatted
    for (i, result) in successful_results.iter().enumerate() {
        assert_eq!(
            result.as_str(),
            &TestUtils::expected_array(&["multi_queue", format!("item_{}", i).as_str()])
        );
    }
}

#[tokio::test]
async fn test_blpop_with_existing_items_concurrent() {
    let mut env = TestEnv::new();

    // Pre-populate the list with multiple items
    env.exec_command_ok(
        TestUtils::rpush_command("existing_queue", &["existing1", "existing2", "existing3"]),
        &TestUtils::server_addr(12380),
        &TestUtils::expected_integer(3),
    )
    .await;

    // Multiple clients try to BLPOP simultaneously from populated list
    let mut tasks = vec![];

    for i in 0..3 {
        let client_addr = format!("127.0.0.1:1238{}", i + 1);
        let task = TestUtils::spawn_blpop_task(&env, "existing_queue", "1", &client_addr);

        tasks.push(task);
    }

    // Wait for all tasks to complete
    let mut results = vec![];
    for task in tasks {
        let result = TestUtils::wait_for_completion(task, Duration::from_millis(500)).await;
        results.push(result);
    }

    // All clients should get an item immediately (no blocking needed)
    let successful_results = TestUtils::filter_successful_results_containing(&results, "existing");

    assert_eq!(
        successful_results.len(),
        3,
        "All clients should get an item immediately"
    );

    // Verify the list is now empty
    let mut env_mut = env.clone();

    env_mut
        .exec_command_ok(
            TestUtils::llen_command("existing_queue"),
            &TestUtils::server_addr(12390),
            &TestUtils::expected_integer(0),
        )
        .await;
}

#[tokio::test]
async fn test_blpop_invalid_arguments() {
    let mut env = TestEnv::new();

    env.exec_command_err(
        TestUtils::invalid_command(&["BLPOP", "test_list"]),
        &TestUtils::server_addr(12400),
        CommandError::InvalidBLPopCommand,
    )
    .await;

    env.exec_command_err(
        TestUtils::blpop_command("test_list", "invalid"),
        &TestUtils::server_addr(12401),
        CommandError::InvalidBLPopCommandArgument,
    )
    .await;
}

#[tokio::test]
async fn test_blpop_concurrent_different_keys() {
    let mut env = TestEnv::new();

    // Start clients waiting on different keys
    let mut tasks = HashMap::new();

    for i in 0..3 {
        let key_name = format!("queue_{}", i);
        let client_addr = format!("127.0.0.1:1241{}", i);

        let task = TestUtils::spawn_blpop_task(&env, &key_name, "2", &client_addr);

        tasks.insert(key_name, task);
    }

    // Give clients time to register
    TestUtils::sleep_ms(200).await;

    // Push to each queue
    for i in 0..3 {
        let key_name = format!("queue_{}", i);
        let server_port = 12420 + i;

        env.exec_command_ok(
            TestUtils::rpush_command(&key_name, &[format!("value_{}", i).as_str()]),
            &TestUtils::server_addr(server_port),
            &TestUtils::expected_integer(1),
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
        assert!(response.contains("value_"));
    }
}
