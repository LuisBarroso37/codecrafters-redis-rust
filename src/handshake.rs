use tokio::net::TcpStream;
use tokio::io::{self, AsyncWriteExt};

use crate::resp::RespValue;

pub async fn handshake(stream: &mut TcpStream) -> io::Result<()> {
    stream
        .write_all(
            RespValue::Array(vec![RespValue::BulkString("PING".to_string())])
                .encode()
                .as_bytes(),
        )
        .await?;
    stream.flush().await?;

    Ok(())
}