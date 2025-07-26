use std::{collections::HashMap, str::FromStr};

use tokio::time::Instant;

pub enum DataType {
    String(String),
    Array(Vec<String>),
}

impl FromStr for DataType {
    type Err = ();

    fn from_str(input: &str) -> Result<DataType, Self::Err> {
        match input {
            "Bar" => Ok(DataType::String("Bar".into())),
            "Baz" => Ok(DataType::String("Baz".into())),
            "Bat" => Ok(DataType::String("Bat".into())),
            "Quux" => Ok(DataType::String("Quux".into())),
            _ => Err(()),
        }
    }
}

pub struct Value {
    pub data: DataType,
    pub expiration: Option<Instant>,
}

pub type KeyValueStore = HashMap<String, Value>;
