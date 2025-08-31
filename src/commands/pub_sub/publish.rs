use std::sync::Arc;

use tokio::{io::AsyncWriteExt, sync::RwLock};

use crate::{
    commands::{command_error::CommandError, command_handler::CommandResult},
    resp::RespValue,
    server::RedisServer,
};

pub struct PublishArguments {
    pub channel: String,
    pub message: String,
}

impl PublishArguments {
    pub fn parse(arguments: Vec<String>) -> Result<Self, CommandError> {
        if arguments.len() != 2 {
            return Err(CommandError::InvalidPublishCommand);
        }

        Ok(Self {
            channel: arguments[0].clone(),
            message: arguments[1].clone(),
        })
    }
}

pub async fn publish(
    server: Arc<RwLock<RedisServer>>,
    arguments: Vec<String>,
) -> Result<CommandResult, CommandError> {
    let publish_arguments = PublishArguments::parse(arguments)?;
    let server_guard = server.read().await;

    let mut count = 0;

    if let Some(channel) = server_guard
        .pub_sub_channels
        .get(&publish_arguments.channel)
    {
        let message = RespValue::Array(vec![
            RespValue::BulkString("message".to_string()),
            RespValue::BulkString(publish_arguments.channel),
            RespValue::BulkString(publish_arguments.message.clone()),
        ]);

        for subscriber in channel.values() {
            let mut subscriber_guard = subscriber.write().await;
            subscriber_guard
                .write_all(message.encode().as_bytes())
                .await
                .map_err(|_| CommandError::IoError)?;
            subscriber_guard
                .flush()
                .await
                .map_err(|_| CommandError::IoError)?;
            count += 1;
        }
    }

    return Ok(CommandResult::Response(RespValue::Integer(count).encode()));
}
