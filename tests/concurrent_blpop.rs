use std::{collections::HashMap, sync::Arc, time::Duration};

use codecrafters_redis::{
    command::handle_command, key_value_store::KeyValueStore, resp::RespValue, state::State,
};
use tokio::{sync::Mutex, time::timeout};

#[tokio::test]
async fn test_blpop_concurrent_clients_simple_blocking() {
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));

    // Client tries to BLPOP from empty list (should block)
    let store_clone = Arc::clone(&store);
    let state_clone = Arc::clone(&state);
    let client_task = tokio::spawn(async move {
        let blpop_command = vec![RespValue::Array(vec![
            RespValue::BulkString("BLPOP".to_string()),
            RespValue::BulkString("test_list".to_string()),
            RespValue::BulkString("2".to_string()), // 2 second timeout
        ])];

        handle_command(
            "127.0.0.1:12345".to_string(),
            blpop_command,
            &mut store_clone.clone(),
            &mut state_clone.clone(),
        )
        .await
    });

    // Give client time to register as subscriber
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Push an element to unblock the client
    let rpush_command = vec![RespValue::Array(vec![
        RespValue::BulkString("RPUSH".to_string()),
        RespValue::BulkString("test_list".to_string()),
        RespValue::BulkString("item1".to_string()),
    ])];

    let push_result = handle_command(
        "127.0.0.1:12347".to_string(),
        rpush_command,
        &mut store,
        &mut state,
    )
    .await;

    assert_eq!(push_result, Ok(":1\r\n".to_string()));

    // Wait for client to complete
    let client_result = timeout(Duration::from_secs(3), client_task)
        .await
        .expect("Client should complete")
        .expect("Client task should succeed");

    // Client should get the item
    assert_eq!(
        client_result,
        Ok("*2\r\n$9\r\ntest_list\r\n$5\r\nitem1\r\n".to_string())
    );
}

#[tokio::test]
async fn test_blpop_concurrent_clients_first_come_first_served() {
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));

    // Pre-populate the list with one item
    let rpush_command = vec![RespValue::Array(vec![
        RespValue::BulkString("RPUSH".to_string()),
        RespValue::BulkString("test_queue".to_string()),
        RespValue::BulkString("single_item".to_string()),
    ])];

    let push_result = handle_command(
        "127.0.0.1:12340".to_string(),
        rpush_command,
        &mut store,
        &mut state,
    )
    .await;

    assert_eq!(push_result, Ok(":1\r\n".to_string()));

    // Multiple clients try to BLPOP simultaneously
    let mut tasks = vec![];

    for i in 0..3 {
        let store_clone = Arc::clone(&store);
        let state_clone = Arc::clone(&state);
        let client_addr = format!("127.0.0.1:1234{}", i + 1);

        let task = tokio::spawn(async move {
            let blpop_command = vec![RespValue::Array(vec![
                RespValue::BulkString("BLPOP".to_string()),
                RespValue::BulkString("test_queue".to_string()),
                RespValue::BulkString("1".to_string()),
            ])];

            handle_command(
                client_addr,
                blpop_command,
                &mut store_clone.clone(),
                &mut state_clone.clone(),
            )
            .await
        });

        tasks.push(task);
    }

    // Wait for all tasks to complete
    let mut results = vec![];
    for task in tasks {
        let result = timeout(Duration::from_secs(2), task)
            .await
            .expect("Task should complete")
            .expect("Task should succeed");
        results.push(result);
    }

    // Only one client should get the item, others should timeout
    let successful_results: Vec<_> = results
        .iter()
        .filter(|r| r.is_ok() && r.as_ref().unwrap().contains("single_item"))
        .collect();

    assert_eq!(
        successful_results.len(),
        1,
        "Only one client should get the item"
    );

    // The successful result should be properly formatted
    assert_eq!(
        successful_results[0],
        &Ok("*2\r\n$10\r\ntest_queue\r\n$11\r\nsingle_item\r\n".to_string())
    );
}

#[tokio::test]
async fn test_blpop_timeout_behavior() {
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));

    let start_time = std::time::Instant::now();

    // Client tries BLPOP with timeout on empty list
    let blpop_command = vec![RespValue::Array(vec![
        RespValue::BulkString("BLPOP".to_string()),
        RespValue::BulkString("empty_list".to_string()),
        RespValue::BulkString("1".to_string()), // 1 second timeout
    ])];

    let result = handle_command(
        "127.0.0.1:12350".to_string(),
        blpop_command,
        &mut store,
        &mut state,
    )
    .await;

    let elapsed = start_time.elapsed();

    // Should timeout and return null
    assert_eq!(result, Ok("$-1\r\n".to_string()));

    // Should take approximately 1 second (allow some tolerance)
    assert!(elapsed >= Duration::from_millis(900));
    assert!(elapsed <= Duration::from_millis(1200));
}

#[tokio::test]
async fn test_blpop_zero_timeout_infinite_wait() {
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));

    // Client tries BLPOP with zero timeout (infinite wait)
    let store_clone = Arc::clone(&store);
    let state_clone = Arc::clone(&state);
    let blpop_task = tokio::spawn(async move {
        let blpop_command = vec![RespValue::Array(vec![
            RespValue::BulkString("BLPOP".to_string()),
            RespValue::BulkString("infinite_list".to_string()),
            RespValue::BulkString("0".to_string()), // Infinite timeout
        ])];

        handle_command(
            "127.0.0.1:12351".to_string(),
            blpop_command,
            &mut store_clone.clone(),
            &mut state_clone.clone(),
        )
        .await
    });

    // Wait a bit to ensure the client is blocking
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Push an item to unblock the client
    let rpush_command = vec![RespValue::Array(vec![
        RespValue::BulkString("RPUSH".to_string()),
        RespValue::BulkString("infinite_list".to_string()),
        RespValue::BulkString("unblock_item".to_string()),
    ])];

    let push_result = handle_command(
        "127.0.0.1:12352".to_string(),
        rpush_command,
        &mut store,
        &mut state,
    )
    .await;

    assert_eq!(push_result, Ok(":1\r\n".to_string()));

    // The BLPOP should now complete
    let blpop_result = timeout(Duration::from_secs(1), blpop_task)
        .await
        .expect("BLPOP should complete after push")
        .expect("BLPOP task should succeed");

    assert_eq!(
        blpop_result,
        Ok("*2\r\n$13\r\ninfinite_list\r\n$12\r\nunblock_item\r\n".to_string())
    );
}

#[tokio::test]
async fn test_blpop_multiple_pushes_multiple_clients() {
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));

    // Start multiple BLPOP clients
    let mut blpop_tasks = vec![];

    for i in 0..3 {
        let store_clone = Arc::clone(&store);
        let state_clone = Arc::clone(&state);
        let client_addr = format!("127.0.0.1:1236{}", i);

        let task = tokio::spawn(async move {
            let blpop_command = vec![RespValue::Array(vec![
                RespValue::BulkString("BLPOP".to_string()),
                RespValue::BulkString("multi_queue".to_string()),
                RespValue::BulkString("5".to_string()),
            ])];

            handle_command(
                client_addr,
                blpop_command,
                &mut store_clone.clone(),
                &mut state_clone.clone(),
            )
            .await
        });

        blpop_tasks.push(task);
    }

    // Give clients time to register
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Push multiple items in sequence
    for i in 0..3 {
        let rpush_command = vec![RespValue::Array(vec![
            RespValue::BulkString("RPUSH".to_string()),
            RespValue::BulkString("multi_queue".to_string()),
            RespValue::BulkString(format!("item_{}", i)),
        ])];

        let push_result = handle_command(
            format!("127.0.0.1:1237{}", i),
            rpush_command,
            &mut store,
            &mut state,
        )
        .await;

        assert_eq!(push_result, Ok(format!(":1\r\n")));

        // Small delay between pushes
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Collect all results
    let mut results = vec![];
    for task in blpop_tasks {
        let result = timeout(Duration::from_secs(1), task)
            .await
            .expect("Task should complete")
            .expect("Task should succeed");
        results.push(result);
    }

    // All clients should get an item
    let successful_results: Vec<_> = results
        .iter()
        .filter(|r| r.is_ok() && r.as_ref().unwrap().contains("item_"))
        .collect();

    assert_eq!(
        successful_results.len(),
        3,
        "All clients should get an item"
    );

    // Each result should be properly formatted
    for result in &successful_results {
        assert!(
            result
                .as_ref()
                .unwrap()
                .starts_with("*2\r\n$11\r\nmulti_queue\r\n")
        );
        assert!(result.as_ref().unwrap().contains("item_"));
    }
}

#[tokio::test]
async fn test_blpop_with_existing_items_concurrent() {
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));

    // Pre-populate the list with multiple items
    let rpush_command = vec![RespValue::Array(vec![
        RespValue::BulkString("RPUSH".to_string()),
        RespValue::BulkString("existing_queue".to_string()),
        RespValue::BulkString("existing1".to_string()),
        RespValue::BulkString("existing2".to_string()),
        RespValue::BulkString("existing3".to_string()),
    ])];

    let push_result = handle_command(
        "127.0.0.1:12380".to_string(),
        rpush_command,
        &mut store,
        &mut state,
    )
    .await;

    assert_eq!(push_result, Ok(":3\r\n".to_string()));

    // Multiple clients try to BLPOP simultaneously from populated list
    let mut tasks = vec![];

    for i in 0..3 {
        let store_clone = Arc::clone(&store);
        let state_clone = Arc::clone(&state);
        let client_addr = format!("127.0.0.1:1238{}", i + 1);

        let task = tokio::spawn(async move {
            let blpop_command = vec![RespValue::Array(vec![
                RespValue::BulkString("BLPOP".to_string()),
                RespValue::BulkString("existing_queue".to_string()),
                RespValue::BulkString("1".to_string()),
            ])];

            handle_command(
                client_addr,
                blpop_command,
                &mut store_clone.clone(),
                &mut state_clone.clone(),
            )
            .await
        });

        tasks.push(task);
    }

    // Wait for all tasks to complete
    let mut results = vec![];
    for task in tasks {
        let result = timeout(Duration::from_millis(500), task)
            .await
            .expect("Task should complete quickly")
            .expect("Task should succeed");
        results.push(result);
    }

    // All clients should get an item immediately (no blocking needed)
    let successful_results: Vec<_> = results
        .iter()
        .filter(|r| r.is_ok() && r.as_ref().unwrap().contains("existing"))
        .collect();

    assert_eq!(
        successful_results.len(),
        3,
        "All clients should get an item immediately"
    );

    // Verify the list is now empty
    let llen_command = vec![RespValue::Array(vec![
        RespValue::BulkString("LLEN".to_string()),
        RespValue::BulkString("existing_queue".to_string()),
    ])];

    let len_result = handle_command(
        "127.0.0.1:12390".to_string(),
        llen_command,
        &mut store,
        &mut state,
    )
    .await;

    assert_eq!(len_result, Ok(":0\r\n".to_string()));
}

#[tokio::test]
async fn test_blpop_invalid_arguments() {
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));

    // Test with insufficient arguments
    let blpop_command = vec![RespValue::Array(vec![
        RespValue::BulkString("BLPOP".to_string()),
        RespValue::BulkString("test_list".to_string()),
    ])];

    let result = handle_command(
        "127.0.0.1:12400".to_string(),
        blpop_command,
        &mut store,
        &mut state,
    )
    .await;

    assert!(result.is_err());

    // Test with invalid timeout
    let blpop_command = vec![RespValue::Array(vec![
        RespValue::BulkString("BLPOP".to_string()),
        RespValue::BulkString("test_list".to_string()),
        RespValue::BulkString("invalid".to_string()),
    ])];

    let result = handle_command(
        "127.0.0.1:12401".to_string(),
        blpop_command,
        &mut store,
        &mut state,
    )
    .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_blpop_concurrent_different_keys() {
    let mut store: Arc<Mutex<KeyValueStore>> = Arc::new(Mutex::new(HashMap::new()));
    let mut state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::new()));

    // Start clients waiting on different keys
    let mut tasks = vec![];

    for i in 0..3 {
        let store_clone = Arc::clone(&store);
        let state_clone = Arc::clone(&state);
        let key_name = format!("queue_{}", i);
        let client_addr = format!("127.0.0.1:1241{}", i);

        let task = tokio::spawn(async move {
            let blpop_command = vec![RespValue::Array(vec![
                RespValue::BulkString("BLPOP".to_string()),
                RespValue::BulkString(key_name.clone()),
                RespValue::BulkString("2".to_string()),
            ])];

            (
                key_name.clone(),
                handle_command(
                    client_addr,
                    blpop_command,
                    &mut store_clone.clone(),
                    &mut state_clone.clone(),
                )
                .await,
            )
        });

        tasks.push(task);
    }

    // Give clients time to register
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Push to each queue
    for i in 0..3 {
        let key_name = format!("queue_{}", i);
        let rpush_command = vec![RespValue::Array(vec![
            RespValue::BulkString("RPUSH".to_string()),
            RespValue::BulkString(key_name.clone()),
            RespValue::BulkString(format!("value_{}", i)),
        ])];

        let push_result = handle_command(
            format!("127.0.0.1:1242{}", i),
            rpush_command,
            &mut store,
            &mut state,
        )
        .await;

        assert_eq!(push_result, Ok(":1\r\n".to_string()));

        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Collect all results
    let mut results = vec![];
    for task in tasks {
        let (key, result) = timeout(Duration::from_secs(3), task)
            .await
            .expect("Task should complete")
            .expect("Task should succeed");
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
