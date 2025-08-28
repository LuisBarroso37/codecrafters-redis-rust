use crate::rdb::get_slice::get_buffer_slice;

#[derive(Debug)]
enum ValueEncoding {
    String(usize),
    Int8,
    Int16,
    Int32,
    LzfCompressedString,
}

fn parse_length_encoding(bytes: &[u8], cursor: usize) -> tokio::io::Result<(ValueEncoding, usize)> {
    let mut temp_cursor = cursor.clone();
    let byte = get_buffer_slice(bytes, temp_cursor, 1)?;
    temp_cursor += 1;

    // Byte is 8 bits long so to extract the first two bits we shift right by 6
    let first_two_bits = byte[0] >> 6;

    let value_encoding = match first_two_bits {
        0b00 => {
            // Use bitmask 0011_1111 to extract the last 6 bits
            // Example:
            // If byte[0] = 0b10101101 (173 in decimal)
            // 0b10101101 & 0b00111111 = 0b00101101 (45 in decimal)
            let last_six_bits = byte[0] & 0b0011_1111;

            Ok(ValueEncoding::String(last_six_bits as usize))
        }
        0b01 => {
            // "Higher bits" means the bits on the left (most significant), which represent larger values in a binary number.
            // "Lower bits" means the bits on the right (least significant), which represent smaller values.

            // Example for a byte:

            // 0b10110011
            // Higher bits: 1011 (left side, more important for the value)
            // Lower bits: 0011 (right side, less important for the value)

            let second_byte = get_buffer_slice(bytes, temp_cursor, 1)?;
            temp_cursor += 1;

            let lower_8_bits = second_byte[0] as u16; // e.g. 0b11001100 as u8 and 0b00000000_11001100 as u16

            // Extract the last 6 bits from the first byte (high bits of the length)
            let high_6_bits = (byte[0] & 0b0011_1111) as u16; // e.g. 0b00000000_00001010

            // Shift the high 6 bits left by 8 to make room for the lower 8 bits
            let high_6_bits_shifted = high_6_bits << 8; // e.g. 0b00001010_00000000

            // Combine the high 6 bits and lower 8 bits to form the 14-bit length
            // Example:
            // high_6_bits_shifted = 0b00001010_00000000 (2560 in decimal)
            // lower_8_bits        = 0b00000000_11001100 (204 in decimal)
            // OR operation:
            // 0b00001010_00000000
            // | 0b00000000_11001100
            // = 0b00001010_11001100 (2764 in decimal)
            let length_14_bits = high_6_bits_shifted | lower_8_bits;

            Ok(ValueEncoding::String(length_14_bits as usize))
        }
        0b10 => {
            // from_le_bytes: Interprets the bytes as a little-endian number (least significant byte first) - right to left.
            // from_be_bytes: Interprets the bytes as a big-endian number (most significant byte first) - left to right.
            //
            // Example for 4 bytes [0x01, 0x02, 0x03, 0x04]:
            // u32::from_le_bytes([0x01, 0x02, 0x03, 0x04]) → 0x04030201 (hex) = 67305985 (decimal)
            // u32::from_be_bytes([0x01, 0x02, 0x03, 0x04]) → 0x01020304 (hex) = 16909060 (decimal)
            match byte[0] {
                0x80 => {
                    let byte_slice = get_buffer_slice(bytes, temp_cursor, 4)?;
                    temp_cursor += 4;

                    let four_byte_slice: [u8; 4] = byte_slice.try_into().map_err(|_| {
                        tokio::io::Error::new(
                            tokio::io::ErrorKind::UnexpectedEof,
                            "Not enough bytes for u32",
                        )
                    })?;
                    let length_32_bits = u32::from_be_bytes(four_byte_slice);
                    Ok(ValueEncoding::String(length_32_bits as usize))
                }
                0x81 => {
                    let byte_slice = get_buffer_slice(bytes, temp_cursor, 8)?;
                    temp_cursor += 8;

                    let eight_byte_slice: [u8; 8] = byte_slice.try_into().map_err(|_| {
                        tokio::io::Error::new(
                            tokio::io::ErrorKind::UnexpectedEof,
                            "Not enough bytes for u64",
                        )
                    })?;
                    let length_64_bits = u64::from_be_bytes(eight_byte_slice);
                    Ok(ValueEncoding::String(length_64_bits as usize))
                }
                _ => Err(tokio::io::Error::new(
                    tokio::io::ErrorKind::InvalidData,
                    "Invalid length encoding",
                )),
            }
        }
        0b11 => {
            let last_six_bits = byte[0] & 0b0011_1111;

            match last_six_bits {
                0 => Ok(ValueEncoding::Int8),
                1 => Ok(ValueEncoding::Int16),
                2 => Ok(ValueEncoding::Int32),
                3 => Ok(ValueEncoding::LzfCompressedString),
                _ => Err(tokio::io::Error::new(
                    tokio::io::ErrorKind::InvalidData,
                    "Invalid length encoding",
                )),
            }
        }
        _ => Err(tokio::io::Error::new(
            tokio::io::ErrorKind::InvalidData,
            "Invalid length encoding",
        )),
    }?;

    let bytes_read = temp_cursor - cursor;

    Ok((value_encoding, bytes_read))
}

pub fn parse_length_encoded_integer(
    bytes: &[u8],
    cursor: usize,
) -> tokio::io::Result<(String, usize)> {
    let (value_encoding, length_cursor) = parse_length_encoding(bytes, cursor)?;
    match value_encoding {
        ValueEncoding::String(value) => Ok((value.to_string(), length_cursor)),
        _ => Err(tokio::io::Error::new(
            tokio::io::ErrorKind::InvalidData,
            "Value should be length encoded integer",
        )),
    }
}

pub fn parse_value(bytes: &[u8], cursor: usize) -> tokio::io::Result<(String, usize)> {
    let mut temp_cursor = cursor.clone();
    let (value_encoding, length_cursor) = parse_length_encoding(bytes, temp_cursor)?;
    temp_cursor += length_cursor;

    let value = match value_encoding {
        ValueEncoding::String(length) => {
            let byte_slice = get_buffer_slice(bytes, temp_cursor, length)?;
            temp_cursor += length;

            let string = String::from_utf8(byte_slice).map_err(|_| {
                tokio::io::Error::new(tokio::io::ErrorKind::InvalidData, "Invalid UTF-8")
            })?;

            Ok(string)
        }
        ValueEncoding::Int8 => {
            let byte_slice = get_buffer_slice(bytes, temp_cursor, 1)?;
            temp_cursor += 1;

            let byte: [u8; 1] = byte_slice.try_into().map_err(|_| {
                tokio::io::Error::new(
                    tokio::io::ErrorKind::UnexpectedEof,
                    "Not enough bytes for u8",
                )
            })?;
            let value = u8::from_be_bytes(byte);

            Ok(value.to_string())
        }
        ValueEncoding::Int16 => {
            let byte_slice = get_buffer_slice(bytes, temp_cursor, 2)?;
            temp_cursor += 2;

            let bytes: [u8; 2] = byte_slice.try_into().map_err(|_| {
                tokio::io::Error::new(
                    tokio::io::ErrorKind::UnexpectedEof,
                    "Not enough bytes for u16",
                )
            })?;
            let value = u16::from_be_bytes(bytes);

            Ok(value.to_string())
        }
        ValueEncoding::Int32 => {
            let byte_slice = get_buffer_slice(bytes, temp_cursor, 4)?;
            temp_cursor += 4;

            let bytes: [u8; 4] = byte_slice.try_into().map_err(|_| {
                tokio::io::Error::new(
                    tokio::io::ErrorKind::UnexpectedEof,
                    "Not enough bytes for u32",
                )
            })?;
            let value = u32::from_be_bytes(bytes);

            Ok(value.to_string())
        }
        _ => Err(tokio::io::Error::new(
            tokio::io::ErrorKind::Unsupported,
            "Unsupported value encoding",
        )),
    }?;

    let bytes_read = temp_cursor - cursor;

    Ok((value, bytes_read))
}
