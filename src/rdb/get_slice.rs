pub fn get_buffer_slice(buffer: &[u8], cursor: usize, len: usize) -> tokio::io::Result<Vec<u8>> {
    if cursor + len > buffer.len() {
        return Err(tokio::io::Error::new(
            tokio::io::ErrorKind::UnexpectedEof,
            "Not enough data in buffer",
        ));
    }

    Ok(buffer[cursor..cursor + len].to_vec())
}
