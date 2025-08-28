use std::path::Path;
use std::sync::Arc;

use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::Mutex;
use tokio::{
    fs::File,
    io::{AsyncReadExt, BufReader},
    sync::RwLock,
};

use crate::key_value_store::KeyValueStore;
use crate::rdb::RdbParser;
use crate::server::RedisServer;

pub async fn stream_rdb_file(
    client_address: &str,
    writer: Arc<RwLock<OwnedWriteHalf>>,
    server: Arc<RwLock<RedisServer>>,
) -> tokio::io::Result<()> {
    // Stream RDB file instead of loading it all into memory
    let file = {
        let server_guard = server.read().await;
        File::open(format!(
            "{}/{}",
            &server_guard.rdb_directory, &server_guard.rdb_filename
        ))
        .await?
    };
    let file_size = file.metadata().await?.len();

    // First send the bulk string header
    let header = format!("${}\r\n", file_size);

    let mut writer_guard = writer.write().await;
    writer_guard.write_all(header.as_bytes()).await?;

    // Stream the file contents in chunks
    let mut reader = BufReader::new(file);
    let mut buffer: [u8; 4096] = [0; 4096]; // 4KB chunks

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

async fn parse_rdb_from_reader<R>(
    store: Arc<Mutex<KeyValueStore>>,
    reader: &mut R,
) -> tokio::io::Result<()>
where
    R: AsyncReadExt + Unpin,
{
    let mut buf_reader = BufReader::new(reader);
    let mut buffer: [u8; 4096] = [0; 4096];

    let mut rdb_parser = RdbParser::new();

    loop {
        let n = buf_reader.read(&mut buffer).await?;

        if n == 0 {
            break;
        }

        rdb_parser.parse(buffer[..n].to_vec())?;
    }

    let mut store_guard = store.lock().await;
    store_guard.extend(rdb_parser.key_value_store);

    Ok(())
}

pub async fn parse_rdb_file(
    server: Arc<RwLock<RedisServer>>,
    store: Arc<Mutex<KeyValueStore>>,
) -> tokio::io::Result<()> {
    let file_path = {
        let server_guard = server.read().await;
        let rdb_directory = &server_guard.rdb_directory;
        let rdb_filename = &server_guard.rdb_filename;

        let file_path = Path::new(rdb_directory).join(rdb_filename);

        if file_path.exists() {
            file_path
        } else {
            return Err(tokio::io::Error::new(
                tokio::io::ErrorKind::NotFound,
                format!("RDB file not found: {}", file_path.display()),
            ));
        }
    };

    let mut file = File::open(file_path).await?;
    parse_rdb_from_reader(store, &mut file).await?;

    Ok(())
}
