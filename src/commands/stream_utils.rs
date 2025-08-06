use std::collections::BTreeMap;

use crate::resp::RespValue;

/// Validates and parses a stream ID string into its components.
///
/// Stream IDs have the format "timestamp-sequence" where both parts are integers.
/// The sequence part is optional and defaults to 0 if not provided.
///
/// # Arguments
///
/// * `command_argument` - The stream ID string to validate (e.g., "1234567890-0")
/// * `is_zero_zero_forbidden` - Whether to reject the special "0-0" ID
///
/// # Returns
///
/// * `Ok((u128, Option<u128>))` - Parsed timestamp and optional sequence number
/// * `Err(String)` - Error message if the stream ID is invalid
///
/// # Examples
///
/// ```
/// let result = validate_stream_id("1234567890-5", false);
/// // Returns: Ok((1234567890, Some(5)))
///
/// let result = validate_stream_id("1234567890", false);
/// // Returns: Ok((1234567890, None))
/// ```
pub fn validate_stream_id(
    command_argument: &str,
    is_zero_zero_forbidden: bool,
) -> Result<(u128, Option<u128>), String> {
    let split_command_argument = command_argument.split("-").collect::<Vec<&str>>();

    if split_command_argument.len() > 2 {
        return Err("Stream ID cannot have more than 2 elements split by a hyphen".to_string());
    }

    let first_stream_id_part = split_command_argument[0]
        .parse::<u128>()
        .map_err(|_| "The stream ID specified must be greater than 0".to_string())?;

    if split_command_argument.len() == 1 {
        return Ok((first_stream_id_part, None));
    } else {
        let index = split_command_argument[1]
            .parse::<u128>()
            .map_err(|_| "The index specified must be greater than 0".to_string())?;

        if is_zero_zero_forbidden == true && format!("{}-{}", first_stream_id_part, index) == "0-0"
        {
            return Err("The stream id must be greater than 0-0".to_string());
        }

        return Ok((first_stream_id_part, Some(index)));
    }
}

/// Converts stream entries to RESP array format.
///
/// Takes a collection of stream entries (each with an ID and field-value map)
/// and converts them to the RESP format expected by Redis clients.
/// Each entry becomes a 2-element array: [stream_id, [field1, value1, field2, value2, ...]]
///
/// # Arguments
///
/// * `entries` - Vector of tuples containing (stream_id, field_value_map)
///
/// # Returns
///
/// * `RespValue` - A RESP array containing all the formatted stream entries
///
/// # Examples
///
/// ```
/// let entries = vec![
///     (&"1234-0".to_string(), &btreemap!{"temp".to_string() => "25".to_string()}),
///     (&"1235-0".to_string(), &btreemap!{"temp".to_string() => "26".to_string()})
/// ];
/// let result = parse_stream_entries_to_resp(entries);
/// // Returns: "*2\r\n*2\r\n$6\r\n1234-0\r\n*2\r\n$4\r\ntemp\r\n$2\r\n25\r\n*2\r\n$6\r\n1235-0\r\n*2\r\n$4\r\ntemp\r\n$2\r\n26\r\n"
/// ```
pub fn parse_stream_entries_to_resp(
    entries: Vec<(&String, &BTreeMap<String, String>)>,
) -> RespValue {
    let array_length = entries.len();
    let mut response: Vec<RespValue> = Vec::with_capacity(array_length);

    let resp_stream_data = entries
        .iter()
        .map(|(id, values)| {
            let mut stream_vec: Vec<RespValue> = Vec::with_capacity(2);
            stream_vec.push(RespValue::BulkString(id.to_string()));

            let mut stream_values_vec: Vec<RespValue> = Vec::with_capacity(values.len());

            for (key, value) in values.iter() {
                stream_values_vec.push(RespValue::BulkString(key.to_string()));
                stream_values_vec.push(RespValue::BulkString(value.to_string()));
            }

            stream_vec.push(RespValue::Array(stream_values_vec));
            return stream_vec;
        })
        .collect::<Vec<Vec<RespValue>>>();

    for resp_vector in resp_stream_data {
        response.push(RespValue::Array(resp_vector));
    }

    RespValue::Array(response)
}

#[cfg(test)]
mod tests {
    use crate::resp::RespValue;

    use super::{parse_stream_entries_to_resp, validate_stream_id};

    #[test]
    fn test_validate_stream_id() {
        let test_cases = vec![
            (
                "invalid",
                true,
                Err("The stream ID specified must be greater than 0".to_string()),
            ),
            (
                "invalid-key-",
                true,
                Err("Stream ID cannot have more than 2 elements split by a hyphen".to_string()),
            ),
            (
                "invalid-0",
                true,
                Err("The stream ID specified must be greater than 0".to_string()),
            ),
            (
                "0-invalid",
                true,
                Err("The index specified must be greater than 0".to_string()),
            ),
            (
                "0-0",
                true,
                Err("The stream id must be greater than 0-0".to_string()),
            ),
            ("0-0", false, Ok((0, Some(0)))),
            ("1526919030484", true, Ok((1526919030484, None))),
            ("1526919030484-3", true, Ok((1526919030484, Some(3)))),
        ];

        for (stream_id, is_zero_zero_forbidden, expected_result) in test_cases {
            assert_eq!(
                validate_stream_id(stream_id, is_zero_zero_forbidden),
                expected_result,
                "validating stream id {}",
                &stream_id
            );
        }
    }

    #[test]
    fn test_parse_stream_entries_to_resp() {
        use std::collections::BTreeMap;

        let empty_entries: Vec<(&String, &BTreeMap<String, String>)> = vec![];
        let result = parse_stream_entries_to_resp(empty_entries);
        assert_eq!(result, RespValue::Array(vec![]));

        let mut map1 = BTreeMap::new();
        map1.insert("field1".to_string(), "value1".to_string());
        let id1 = "1000-0".to_string();
        let entries = vec![(&id1, &map1)];
        let result = parse_stream_entries_to_resp(entries);

        let expected = RespValue::Array(vec![RespValue::Array(vec![
            RespValue::BulkString("1000-0".to_string()),
            RespValue::Array(vec![
                RespValue::BulkString("field1".to_string()),
                RespValue::BulkString("value1".to_string()),
            ]),
        ])]);
        assert_eq!(result, expected);

        let mut map2 = BTreeMap::new();
        map2.insert("field1".to_string(), "value1".to_string());
        map2.insert("field2".to_string(), "value2".to_string());
        let id2 = "1001-0".to_string();
        let entries = vec![(&id2, &map2)];
        let result = parse_stream_entries_to_resp(entries);

        let expected = RespValue::Array(vec![RespValue::Array(vec![
            RespValue::BulkString("1001-0".to_string()),
            RespValue::Array(vec![
                RespValue::BulkString("field1".to_string()),
                RespValue::BulkString("value1".to_string()),
                RespValue::BulkString("field2".to_string()),
                RespValue::BulkString("value2".to_string()),
            ]),
        ])]);
        assert_eq!(result, expected);

        let mut map3 = BTreeMap::new();
        map3.insert("name".to_string(), "Alice".to_string());
        let mut map4 = BTreeMap::new();
        map4.insert("name".to_string(), "Bob".to_string());
        map4.insert("age".to_string(), "30".to_string());

        let id3 = "1002-0".to_string();
        let id4 = "1003-0".to_string();
        let entries = vec![(&id3, &map3), (&id4, &map4)];

        let result = parse_stream_entries_to_resp(entries);
        let expected = RespValue::Array(vec![
            RespValue::Array(vec![
                RespValue::BulkString("1002-0".to_string()),
                RespValue::Array(vec![
                    RespValue::BulkString("name".to_string()),
                    RespValue::BulkString("Alice".to_string()),
                ]),
            ]),
            RespValue::Array(vec![
                RespValue::BulkString("1003-0".to_string()),
                RespValue::Array(vec![
                    RespValue::BulkString("age".to_string()),
                    RespValue::BulkString("30".to_string()),
                    RespValue::BulkString("name".to_string()),
                    RespValue::BulkString("Bob".to_string()),
                ]),
            ]),
        ]);
        assert_eq!(result, expected);
    }
}
