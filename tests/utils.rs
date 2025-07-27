use codecrafters_redis::command::validate_range_indexes;

#[test]
fn test_validate_indexes() {
    let list = vec![
        "grape".into(),
        "apple".into(),
        "pineapple".into(),
        "mango".into(),
        "raspberry".into(),
    ];

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
        (4, 4, Err("Start index is out of bounds")),
        (5, 6, Err("Start index is out of bounds")),
        (-1, -1, Ok((4, 4))),
        (-2, -1, Ok((3, 4))),
        (-3, -1, Ok((2, 4))),
        (-9, -2, Ok((0, 3))),
        (-5, -3, Ok((0, 2))),
        (-2, -10, Err("Negative end index is out of bounds")),
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
