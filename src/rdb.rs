use std::sync::Arc;

use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::{
    fs::File,
    io::{AsyncReadExt, BufReader},
    sync::RwLock,
};

use crate::server::RedisServer;

pub async fn stream_rdb_file(
    client_address: &str,
    writer: Arc<RwLock<OwnedWriteHalf>>,
    server: Arc<RwLock<RedisServer>>,
) -> tokio::io::Result<()> {
    // Stream RDB file instead of loading it all into memory
    let file = File::open("empty.rdb").await?;
    let file_size = file.metadata().await?.len();

    // First send the bulk string header
    let header = format!("${}\r\n", file_size);

    let mut writer_guard = writer.write().await;
    writer_guard.write_all(header.as_bytes()).await?;

    // Stream the file contents in chunks
    let mut reader = BufReader::new(file);
    let mut buffer = [0u8; 4096]; // 4KB chunks

    loop {
        match reader.read(&mut buffer).await {
            Ok(0) => break,
            Ok(n) => {
                writer_guard.write_all(&buffer[..n]).await?;
            }
            Err(e) => {
                eprintln!("Error streaming RDB file: {}", e);
                break;
            }
        }
    }

    writer_guard.flush().await?;
    drop(writer_guard);

    // Add replica to replication list after successful RDB streaming
    let mut server_guard = server.write().await;
    if let Some(replicas) = &mut server_guard.replicas {
        replicas.insert(
            client_address.to_string(),
            crate::server::Replica { writer, offset: 0 },
        );
    }

    Ok(())
}
