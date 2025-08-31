use std::{collections::HashMap, sync::Arc};

use tokio::{net::tcp::OwnedWriteHalf, sync::RwLock};

use crate::{
    commands::{CommandError, CommandResult},
    resp::RespValue,
    server::RedisServer,
};

pub struct SubscribeArguments {
    pub channel: String,
}

impl SubscribeArguments {
    pub fn parse(arguments: Vec<String>) -> Result<Self, CommandError> {
        if arguments.len() != 1 {
            return Err(CommandError::InvalidSubscribeCommand);
        }

        Ok(Self {
            channel: arguments[0].clone(),
        })
    }
}

pub async fn subscribe(
    client_address: &str,
    writer: Arc<RwLock<OwnedWriteHalf>>,
    server: Arc<RwLock<RedisServer>>,
    arguments: Vec<String>,
) -> Result<CommandResult, CommandError> {
    let subscribe_arguments = SubscribeArguments::parse(arguments)?;
    let mut server_guard = server.write().await;

    let channel_map = server_guard
        .pub_sub_channels
        .entry(subscribe_arguments.channel.clone())
        .or_insert_with(HashMap::new);

    if channel_map.contains_key(client_address) {
        return Ok(find_number_of_subscribed_channels_for_client(
            client_address,
            &subscribe_arguments.channel,
            &server_guard.pub_sub_channels,
        ));
    }

    channel_map.insert(client_address.to_string(), writer);

    Ok(find_number_of_subscribed_channels_for_client(
        client_address,
        &subscribe_arguments.channel,
        &server_guard.pub_sub_channels,
    ))
}

pub fn find_number_of_subscribed_channels_for_client(
    client_address: &str,
    channel_name: &str,
    channels: &HashMap<String, HashMap<String, Arc<RwLock<OwnedWriteHalf>>>>,
) -> CommandResult {
    let mut count = 0;

    for channel in channels.values() {
        if channel.contains_key(client_address) {
            count += 1;
        }
    }

    CommandResult::Response(
        RespValue::Array(vec![
            RespValue::BulkString("subscribe".to_string()),
            RespValue::BulkString(channel_name.to_string()),
            RespValue::Integer(count),
        ])
        .encode(),
    )
}
