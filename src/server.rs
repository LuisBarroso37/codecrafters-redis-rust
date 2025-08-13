use regex::Regex;
use thiserror::Error;

#[derive(Error, Debug, PartialEq)]
pub enum CliError {
    #[error("Invalid command line flag")]
    InvalidCommandLineFlag,
    #[error("Invalid port flag value")]
    InvalidPortFlagValue,
    #[error("Invalid master address")]
    InvalidMasterAddress,
    #[error("Invalid master port")]
    InvalidMasterPort,
}

#[derive(Debug, PartialEq, Clone)]
pub enum RedisRole {
    Master,
    Replica((String, u32)),
}

impl RedisRole {
    pub fn as_string(&self) -> &str {
        match self {
            RedisRole::Master => "master",
            RedisRole::Replica(_) => "slave",
        }
    }
}

#[derive(Debug, Clone)]
pub struct RedisServer {
    pub port: u32,
    pub role: RedisRole,
}

impl RedisServer {
    pub fn new<I: IntoIterator<Item = String>>(command_line_args: I) -> Result<Self, CliError> {
        let mut iter = command_line_args.into_iter().skip(1);
        let mut port: Option<u32> = None;
        let mut role: Option<RedisRole> = None;

        while let Some(arg) = iter.next() {
            match arg.as_str() {
                "--port" => {
                    let Some(port_str) = iter.next() else {
                        return Err(CliError::InvalidCommandLineFlag);
                    };

                    let port_number = port_str
                        .parse::<u32>()
                        .map_err(|_| CliError::InvalidPortFlagValue)?;

                    if port_number < 1 || port_number > 65535 {
                        return Err(CliError::InvalidPortFlagValue);
                    }

                    port = Some(port_number);
                }
                "--replicaof" => {
                    // Regex for IPv4 address
                    let ipv4_regex =
                        Regex::new(r"^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$").unwrap();
                    // Regex for hostname
                    let hostname_regex = Regex::new(r"^[a-zA-Z0-9\-\.]+$").unwrap();

                    let Some(master_address) = iter.next() else {
                        return Err(CliError::InvalidCommandLineFlag);
                    };

                    let split_address = master_address.split_whitespace().collect::<Vec<&str>>();

                    if split_address.len() != 2 {
                        return Err(CliError::InvalidMasterAddress);
                    }

                    let address = split_address[0];
                    let port_number = split_address[1]
                        .parse::<u32>()
                        .map_err(|_| CliError::InvalidMasterPort)?;

                    if port_number < 1 || port_number > 65535 {
                        return Err(CliError::InvalidMasterPort);
                    }

                    let valid_address = if ipv4_regex.is_match(address) {
                        if let Some(caps) = ipv4_regex.captures(address) {
                            caps.iter().skip(1).all(|octet| {
                                octet
                                    .map(|m| {
                                        m.as_str().parse::<u16>().map(|v| v <= 255).unwrap_or(false)
                                    })
                                    .unwrap_or(false)
                            })
                        } else {
                            false
                        }
                    } else {
                        hostname_regex.is_match(address)
                    };

                    if !valid_address {
                        return Err(CliError::InvalidMasterAddress);
                    }

                    role = Some(RedisRole::Replica((address.to_string(), port_number)));
                }
                _ => return Err(CliError::InvalidCommandLineFlag),
            }
        }

        Ok(RedisServer {
            port: port.unwrap_or(6379),
            role: role.unwrap_or(RedisRole::Master),
        })
    }
}
