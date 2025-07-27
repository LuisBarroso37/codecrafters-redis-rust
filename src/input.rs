use thiserror::Error;

#[derive(Error, Debug, PartialEq)]
pub enum InputError {
    #[error("invalid UTF-8 sequence")]
    InvalidUtf8,
}

impl InputError {
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            InputError::InvalidUtf8 => b"-ERR invalid UTF-8 sequence\r\n",
        }
    }
}

pub fn parse_input(input: &[u8]) -> Result<Vec<&str>, InputError> {
    let str = str::from_utf8(input).map_err(|_| InputError::InvalidUtf8)?;

    Ok(str
        .split_terminator("\r\n")
        .filter(|s| !s.contains("\0"))
        .collect::<Vec<&str>>())
}
