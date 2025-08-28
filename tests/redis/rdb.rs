use std::collections::HashMap;

use codecrafters_redis::key_value_store::{DataType, Value};
use codecrafters_redis::rdb::RdbParser;
use jiff::Timestamp;
use tokio::io::AsyncReadExt;
use tokio::{fs::File, io::BufReader};

#[tokio::test]
async fn test_rdb_parser_empty_file() {
    let file = File::open("./tests/redis/rdb_files/empty.rdb")
        .await
        .unwrap();
    let mut buf_reader = BufReader::new(file);
    let mut buffer: [u8; 44] = [0; 44];

    let mut rdb_parser = RdbParser::new();

    loop {
        let n = buf_reader.read(&mut buffer).await.unwrap();

        if n == 0 {
            break;
        }

        rdb_parser.parse(buffer[..n].to_vec()).unwrap();
    }

    assert_eq!(rdb_parser.magic_string, Some("REDIS".to_string()));
    assert_eq!(rdb_parser.redis_version, Some("0011".to_string()));
    assert_eq!(rdb_parser.metadata.len(), 5);
    assert_eq!(
        rdb_parser.metadata,
        HashMap::from([
            ("ctime".to_string(), "1829289061".to_string()),
            ("used-mem".to_string(), "2965639168".to_string()),
            ("redis-ver".to_string(), "7.2.0".to_string()),
            ("redis-bits".to_string(), "64".to_string()),
            ("aof-base".to_string(), "0".to_string()),
        ])
    );
    assert_eq!(rdb_parser.db_number, None);
    assert_eq!(rdb_parser.hash_table_size, None);
    assert_eq!(rdb_parser.expiry_hash_table_size, None);
    assert_eq!(rdb_parser.key_value_store.len(), 0);
    assert_eq!(rdb_parser.crc64_checksum.unwrap().iter().len(), 8);
}

#[tokio::test]
async fn test_rdb_parser_with_key_value_pairs_including_expiration() {
    let file = File::open("./tests/redis/rdb_files/multiple_keys_with_expiration.rdb")
        .await
        .unwrap();
    let mut buf_reader = BufReader::new(file);
    let mut buffer: [u8; 44] = [0; 44];

    let mut rdb_parser = RdbParser::new();

    loop {
        let n = buf_reader.read(&mut buffer).await.unwrap();

        if n == 0 {
            break;
        }

        rdb_parser.parse(buffer[..n].to_vec()).unwrap();
    }

    assert_eq!(rdb_parser.magic_string, Some("REDIS".to_string()));
    assert_eq!(rdb_parser.redis_version, Some("0011".to_string()));
    assert_eq!(rdb_parser.metadata.len(), 2);
    assert_eq!(
        rdb_parser.metadata,
        HashMap::from([
            ("redis-ver".to_string(), "7.2.0".to_string()),
            ("redis-bits".to_string(), "64".to_string()),
        ])
    );
    assert_eq!(rdb_parser.db_number, Some("0".to_string()));
    assert_eq!(rdb_parser.hash_table_size, Some("5".to_string()));
    assert_eq!(rdb_parser.expiry_hash_table_size, Some("1".to_string()));
    assert_eq!(rdb_parser.key_value_store.len(), 5);
    assert_eq!(
        rdb_parser.key_value_store.get("mango"),
        Some(&Value {
            data: DataType::String("pineapple".to_string()),
            expiration: None
        })
    );
    assert_eq!(
        rdb_parser.key_value_store.get("banana"),
        Some(&Value {
            data: DataType::String("grape".to_string()),
            expiration: None
        })
    );
    assert_eq!(
        rdb_parser.key_value_store.get("grape"),
        Some(&Value {
            data: DataType::String("mango".to_string()),
            expiration: None
        })
    );
    assert_eq!(
        rdb_parser.key_value_store.get("orange"),
        Some(&Value {
            data: DataType::String("raspberry".to_string()),
            expiration: Some("2032-01-01T00:00:00Z".parse::<Timestamp>().unwrap())
        })
    );
    assert_eq!(
        rdb_parser.key_value_store.get("strawberry"),
        Some(&Value {
            data: DataType::String("blueberry".to_string()),
            expiration: None
        })
    );
    assert_eq!(rdb_parser.crc64_checksum.unwrap().iter().len(), 8);
}
