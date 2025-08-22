use thiserror::Error;

use crate::{resp::RespValue, state::StateError};

#[derive(Error, Debug, PartialEq)]
pub enum CommandError {
    #[error("invalid command")]
    InvalidCommand,
    #[error("invalid command argument")]
    InvalidCommandArgument,
    #[error("invalid PING command")]
    InvalidPingCommand,
    #[error("invalid ECHO command")]
    InvalidEchoCommand,
    #[error("invalid GET command")]
    InvalidGetCommand,
    #[error("invalid SET command")]
    InvalidSetCommand,
    #[error("invalid SET command argument")]
    InvalidSetCommandArgument,
    #[error("invalid SET command expiration")]
    InvalidSetCommandExpiration,
    #[error("invalid RPUSH command")]
    InvalidRPushCommand,
    #[error("data not found")]
    DataNotFound,
    #[error("invalid LRANGE command")]
    InvalidLRangeCommand,
    #[error("invalid LRANGE command argument")]
    InvalidLRangeCommandArgument,
    #[error("invalid LPUSH command")]
    InvalidLPushCommand,
    #[error("invalid LLEN command")]
    InvalidLLenCommand,
    #[error("invalid LPOP command")]
    InvalidLPopCommand,
    #[error("invalid LPOP command argument")]
    InvalidLPopCommandArgument,
    #[error("invalid BLPOP command")]
    InvalidBLPopCommand,
    #[error("invalid BLPOP command argument")]
    InvalidBLPopCommandArgument,
    #[error("invalid TYPE command")]
    InvalidTypeCommand,
    #[error("invalid XADD command")]
    InvalidXAddCommand,
    #[error("{0}")]
    InvalidStreamId(String),
    #[error("invalid data type for key")]
    InvalidDataTypeForKey,
    #[error("invalid XRANGE command")]
    InvalidXRangeCommand,
    #[error("invalid XREAD command")]
    InvalidXReadCommand,
    #[error("invalid XREAD command option")]
    InvalidXReadOption,
    #[error("invalid XREAD block duration")]
    InvalidXReadBlockDuration,
    #[error("invalid INCR command")]
    InvalidIncrCommand,
    #[error("invalid INCR value")]
    InvalidIncrValue,
    #[error("invalid MULTI command")]
    InvalidMultiCommand,
    #[error("transaction error")]
    TransactionError(#[from] StateError),
    #[error("invalid EXEC command")]
    InvalidExecCommand,
    #[error("EXEC without MULTI")]
    ExecWithoutMulti,
    #[error("invalid DISCARD command")]
    InvalidDiscardCommand,
    #[error("discard without multi")]
    DiscardWithoutMulti,
    #[error("invalid INFO command")]
    InvalidInfoCommand,
    #[error("invalid INFO section")]
    InvalidInfoSection,
    #[error("invalid REPLCONF command")]
    InvalidReplconfCommand,
    #[error("invalid PSYNC command")]
    InvalidPsyncCommand,
    #[error("invalid PSYNC replication ID")]
    InvalidPsyncReplicationId,
    #[error("invalid PSYNC offset")]
    InvalidPsyncOffset,
    #[error("invalid WAIT command")]
    InvalidWaitCommand,
    #[error("invalid WAIT command argument")]
    InvalidWaitCommandArgument,
    #[error("invalid WAIT command for replica")]
    InvalidWaitCommandForReplica,
    #[error("replica can only process read commands from clients")]
    ReplicaReadOnlyCommands,
    #[error("invalid CONFIG GET command")]
    InvalidConfigGetCommand,
    #[error("invalid CONFIG GET command argument")]
    InvalidConfigGetCommandArgument,
}

impl CommandError {
    pub fn as_string(&self) -> String {
        match self {
            CommandError::InvalidCommand => {
                RespValue::Error("ERR Invalid command".to_string()).encode()
            }
            CommandError::InvalidCommandArgument => {
                RespValue::Error("ERR Invalid command argument".to_string()).encode()
            }
            CommandError::InvalidPingCommand => {
                RespValue::Error("ERR Invalid PING command".to_string()).encode()
            }
            CommandError::InvalidEchoCommand => {
                RespValue::Error("ERR Invalid ECHO command".to_string()).encode()
            }
            CommandError::InvalidGetCommand => {
                RespValue::Error("ERR Invalid GET command".to_string()).encode()
            }
            CommandError::InvalidSetCommand => {
                RespValue::Error("ERR Invalid SET command".to_string()).encode()
            }
            CommandError::InvalidSetCommandArgument => {
                RespValue::Error("ERR Invalid SET command argument".to_string()).encode()
            }
            CommandError::InvalidSetCommandExpiration => {
                RespValue::Error("ERR Invalid SET command expiration".to_string()).encode()
            }
            CommandError::InvalidRPushCommand => {
                RespValue::Error("ERR Invalid RPUSH command".to_string()).encode()
            }
            CommandError::DataNotFound => {
                RespValue::Error("ERR Data not found".to_string()).encode()
            }
            CommandError::InvalidLRangeCommand => {
                RespValue::Error("ERR Invalid LRANGE command".to_string()).encode()
            }
            CommandError::InvalidLRangeCommandArgument => {
                RespValue::Error("ERR Invalid LRANGE command argument".to_string()).encode()
            }
            CommandError::InvalidLPushCommand => {
                RespValue::Error("ERR Invalid LPUSH command".to_string()).encode()
            }
            CommandError::InvalidLLenCommand => {
                RespValue::Error("ERR Invalid LLEN command".to_string()).encode()
            }
            CommandError::InvalidLPopCommand => {
                RespValue::Error("ERR Invalid LPOP command".to_string()).encode()
            }
            CommandError::InvalidLPopCommandArgument => {
                RespValue::Error("ERR Invalid LPOP command argument".to_string()).encode()
            }
            CommandError::InvalidBLPopCommand => {
                RespValue::Error("ERR Invalid BLPOP command".to_string()).encode()
            }
            CommandError::InvalidBLPopCommandArgument => {
                RespValue::Error("ERR Invalid BLPOP command argument".to_string()).encode()
            }
            CommandError::InvalidTypeCommand => {
                RespValue::Error("ERR Invalid TYPE command".to_string()).encode()
            }
            CommandError::InvalidXAddCommand => {
                RespValue::Error("ERR Invalid XADD command".to_string()).encode()
            }
            CommandError::InvalidStreamId(str) => RespValue::Error(format!("ERR {}", str)).encode(),
            CommandError::InvalidDataTypeForKey => {
                RespValue::Error("ERR Invalid data type for key".to_string()).encode()
            }
            CommandError::InvalidXRangeCommand => {
                RespValue::Error("ERR Invalid XRANGE command".to_string()).encode()
            }
            CommandError::InvalidXReadOption => {
                RespValue::Error("ERR Invalid XREAD command option".to_string()).encode()
            }
            CommandError::InvalidXReadCommand => {
                RespValue::Error("ERR Invalid XREAD command".to_string()).encode()
            }
            CommandError::InvalidXReadBlockDuration => {
                RespValue::Error("ERR Invalid XREAD block duration".to_string()).encode()
            }
            CommandError::InvalidIncrCommand => {
                RespValue::Error("ERR Invalid INCR command".to_string()).encode()
            }
            CommandError::InvalidIncrValue => {
                RespValue::Error("ERR value is not an integer or out of range".to_string()).encode()
            }
            CommandError::InvalidMultiCommand => {
                RespValue::Error("ERR Invalid MULTI command".to_string()).encode()
            }
            CommandError::TransactionError(e) => {
                RespValue::Error(format!("ERR {}", e.as_string())).encode()
            }
            CommandError::InvalidExecCommand => {
                RespValue::Error("ERR Invalid EXEC command".to_string()).encode()
            }
            CommandError::ExecWithoutMulti => {
                RespValue::Error("ERR EXEC without MULTI".to_string()).encode()
            }
            CommandError::InvalidDiscardCommand => {
                RespValue::Error("ERR Invalid DISCARD command".to_string()).encode()
            }
            CommandError::DiscardWithoutMulti => {
                RespValue::Error("ERR DISCARD without MULTI".to_string()).encode()
            }
            CommandError::InvalidInfoCommand => {
                RespValue::Error("ERR Invalid INFO command".to_string()).encode()
            }
            CommandError::InvalidInfoSection => {
                RespValue::Error("ERR Invalid INFO section".to_string()).encode()
            }
            CommandError::InvalidReplconfCommand => {
                RespValue::Error("ERR Invalid REPLCONF command".to_string()).encode()
            }
            CommandError::InvalidPsyncCommand => {
                RespValue::Error("ERR Invalid PSYNC command".to_string()).encode()
            }
            CommandError::InvalidPsyncReplicationId => {
                RespValue::Error("ERR Invalid PSYNC replication ID".to_string()).encode()
            }
            CommandError::InvalidPsyncOffset => {
                RespValue::Error("ERR Invalid PSYNC offset".to_string()).encode()
            }
            CommandError::InvalidWaitCommand => {
                RespValue::Error("ERR Invalid WAIT command".to_string()).encode()
            }
            CommandError::InvalidWaitCommandArgument => {
                RespValue::Error("ERR Invalid WAIT command argument".to_string()).encode()
            }
            CommandError::InvalidWaitCommandForReplica => {
                RespValue::Error("ERR Invalid WAIT command for replica".to_string()).encode()
            }
            CommandError::ReplicaReadOnlyCommands => RespValue::Error(
                "ERR replica can only process read commands from clients".to_string(),
            )
            .encode(),
            CommandError::InvalidConfigGetCommand => {
                RespValue::Error("ERR Invalid CONFIG GET command".to_string()).encode()
            }
            CommandError::InvalidConfigGetCommandArgument => {
                RespValue::Error("ERR Invalid CONFIG GET command argument".to_string()).encode()
            }
        }
    }
}
