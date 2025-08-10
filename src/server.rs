use thiserror::Error;

#[derive(Error, Debug, PartialEq)]
pub enum CliError {
    #[error("Invalid command line flag")]
    InvalidCommandLineFlag,
    #[error("Invalid command line flag value")]
    InvalidCommandLineFlagValue,
}

#[derive(Debug)]
pub struct RedisServer {
    pub port: u32,
}

impl RedisServer {
    pub fn new<I: IntoIterator<Item = String>>(command_line_args: I) -> Result<Self, CliError> {
        let mut iter = command_line_args.into_iter().skip(1);
        let mut port: Option<u32> = None;

        while let Some(arg) = iter.next() {
            println!("Creating Redis server with args: {}", arg);
            match arg.as_str() {
                "--port" => {
                    let Some(port_str) = iter.next() else {
                        return Err(CliError::InvalidCommandLineFlagValue);
                    };

                    let port_number = port_str
                        .parse::<u32>()
                        .map_err(|_| CliError::InvalidCommandLineFlagValue)?;

                    if port_number < 1 || port_number > 65535 {
                        return Err(CliError::InvalidCommandLineFlagValue);
                    }

                    port = Some(port_number);
                }
                _ => return Err(CliError::InvalidCommandLineFlag),
            }
        }

        Ok(RedisServer {
            port: port.unwrap_or(6379),
        })
    }
}
