use std::{collections::VecDeque, sync::Arc};

use tokio::sync::Mutex;

use crate::{
    commands::command_error::CommandError,
    key_value_store::{DataType, KeyValueStore},
    resp::RespValue,
};

/// Handles the Redis LRANGE command.
///
/// Returns a range of elements from a list stored at the given key.
/// Both start and end indices can be negative to count from the end of the list.
/// If the key doesn't exist or doesn't contain a list, returns an empty array.
///
/// # Arguments
///
/// * `store` - A thread-safe reference to the key-value store
/// * `arguments` - A vector containing exactly 3 elements: [key, start_index, end_index]
///
/// # Returns
///
/// * `Ok(String)` - A RESP-encoded array containing the requested range of elements
/// * `Err(CommandError::InvalidLRangeCommand)` - If the number of arguments is not exactly 3
/// * `Err(CommandError::InvalidLRangeCommandArgument)` - If start or end index is not a valid integer
///
/// # Examples
///
/// ```
/// // LRANGE mylist 0 2  (get first 3 elements)
/// let result = lrange(&mut store, vec!["mylist".to_string(), "0".to_string(), "2".to_string()]).await;
/// // Returns: "*3\r\n$3\r\nval1\r\n$3\r\nval2\r\n$3\r\nval3\r\n"
///
/// // LRANGE mylist -2 -1  (get last 2 elements)
/// let result = lrange(&mut store, vec!["mylist".to_string(), "-2".to_string(), "-1".to_string()]).await;
/// // Returns: "*2\r\n$3\r\nval4\r\n$3\r\nval5\r\n"
/// ```
pub async fn lrange(
    store: &mut Arc<Mutex<KeyValueStore>>,
    arguments: Vec<String>,
) -> Result<String, CommandError> {
    if arguments.len() != 3 {
        return Err(CommandError::InvalidLRangeCommand);
    }

    let start_index = match arguments[1].parse::<isize>() {
        Ok(num) => num,
        Err(_) => return Err(CommandError::InvalidLRangeCommandArgument),
    };

    let end_index = match arguments[2].parse::<isize>() {
        Ok(num) => num,
        Err(_) => return Err(CommandError::InvalidLRangeCommandArgument),
    };

    let store_guard = store.lock().await;
    let stored_data = store_guard.get(&arguments[0]);

    match stored_data {
        Some(value) => {
            if let DataType::Array(ref list) = value.data {
                let (start, end) = if let Ok((start, end)) =
                    validate_range_indexes(list, start_index, end_index)
                {
                    (start, end)
                } else {
                    return Ok(RespValue::Array(vec![]).encode());
                };

                let range = list
                    .range(start..=end)
                    .map(|s| s.to_string())
                    .collect::<Vec<String>>();

                if !range.is_empty() {
                    return Ok(RespValue::encode_array_from_strings(range));
                } else {
                    return Ok(RespValue::Array(vec![]).encode());
                }
            } else {
                return Ok(RespValue::Array(vec![]).encode());
            }
        }
        None => {
            return Ok(RespValue::Array(vec![]).encode());
        }
    }
}

/// Validates and normalizes range indices for list operations.
///
/// Converts negative indices to positive equivalents and ensures the range is valid.
/// Negative indices count from the end of the list (-1 is the last element).
///
/// # Arguments
///
/// * `list` - The list to validate indices against
/// * `start_index` - The starting index (can be negative)
/// * `end_index` - The ending index (can be negative)
///
/// # Returns
///
/// * `Ok((usize, usize))` - Normalized start and end indices if valid
/// * `Err(&str)` - Error message if the range is invalid
///
/// # Examples
///
/// ```
/// // For a list of length 5:
/// // validate_range_indexes(&list, 0, 2) -> Ok((0, 2))
/// // validate_range_indexes(&list, -2, -1) -> Ok((3, 4))
/// // validate_range_indexes(&list, 5, 10) -> Err("invalid range")
/// ```
fn validate_range_indexes(
    list: &VecDeque<String>,
    start_index: isize,
    end_index: isize,
) -> Result<(usize, usize), &str> {
    let len = list.len() as isize;

    let mut start = if start_index < 0 {
        len + start_index
    } else {
        start_index
    };
    let mut end = if end_index < 0 {
        len + end_index
    } else {
        end_index
    };

    start = start.max(0);
    end = end.min(len - 1);

    if start >= len {
        return Err("Start index is out of bounds");
    }

    if start > end {
        return Err("Start index is bigger than end index after processing");
    }

    Ok((start as usize, end as usize))
}

#[cfg(test)]
mod tests {
    use super::validate_range_indexes;
    use std::collections::VecDeque;

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
}
