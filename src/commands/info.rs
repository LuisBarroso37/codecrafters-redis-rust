use crate::{commands::CommandError, resp::RespValue};

enum InfoSection {
    DEFAULT,
    REPLICATION,
}

struct InfoArguments {
    section: InfoSection,
}

impl InfoArguments {
    pub fn parse(arguments: Vec<String>) -> Result<Self, CommandError> {
        if arguments.len() > 1 {
            return Err(CommandError::InvalidInfoCommand);
        }

        if arguments.is_empty() {
            return Ok(InfoArguments {
                section: InfoSection::DEFAULT,
            });
        }

        let section = match arguments[0].as_str() {
            "replication" => InfoSection::REPLICATION,
            _ => return Err(CommandError::InvalidInfoSection),
        };

        Ok(InfoArguments { section })
    }
}

pub fn info(arguments: Vec<String>) -> Result<String, CommandError> {
    let info_arguments = InfoArguments::parse(arguments)?;

    // $92\r\nrole:master\r\nconnected_slaves:0\r\nmaster_replid:xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\r\nmaster_repl_offset:0\r\n
    match info_arguments.section {
        InfoSection::DEFAULT => Ok(RespValue::BulkString("role:master".to_string()).encode()),
        InfoSection::REPLICATION => Ok(RespValue::BulkString("role:master".to_string()).encode()),
    }
}
