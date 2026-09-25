use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use thiserror::Error;

const VERSION: u8 = 1;
const BODY_LEN: usize = 1 + 16 + 8;
const TOKEN_LEN: usize = BODY_LEN + 4;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum CursorError {
    #[error("cursor is not valid base64url")]
    InvalidEncoding,
    #[error("cursor has an unsupported shape or version")]
    InvalidShape,
    #[error("cursor checksum is invalid")]
    InvalidChecksum,
    #[error("cursor belongs to a different stream")]
    WrongStream,
}

pub fn encode_cursor(stream: &str, record_index: u64) -> String {
    let mut bytes = Vec::with_capacity(TOKEN_LEN);
    bytes.push(VERSION);
    bytes.extend_from_slice(stream_fingerprint(stream).as_slice());
    bytes.extend_from_slice(&record_index.to_be_bytes());
    let checksum = crc32fast::hash(&bytes);
    bytes.extend_from_slice(&checksum.to_be_bytes());
    URL_SAFE_NO_PAD.encode(bytes)
}

pub fn decode_cursor(stream: &str, cursor: &str) -> Result<u64, CursorError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| CursorError::InvalidEncoding)?;
    if bytes.len() != TOKEN_LEN || bytes[0] != VERSION {
        return Err(CursorError::InvalidShape);
    }
    let expected_checksum =
        u32::from_be_bytes(bytes[BODY_LEN..].try_into().expect("checksum length"));
    if crc32fast::hash(&bytes[..BODY_LEN]) != expected_checksum {
        return Err(CursorError::InvalidChecksum);
    }
    if bytes[1..17] != stream_fingerprint(stream) {
        return Err(CursorError::WrongStream);
    }
    Ok(u64::from_be_bytes(
        bytes[17..25].try_into().expect("record index length"),
    ))
}

pub fn stream_fingerprint(stream: &str) -> [u8; 16] {
    let digest = blake3::hash(stream.as_bytes());
    digest.as_bytes()[..16]
        .try_into()
        .expect("fingerprint length")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_round_trips_and_is_stream_scoped() {
        let cursor = encode_cursor("/orders/eu", 42);
        assert_eq!(decode_cursor("/orders/eu", &cursor), Ok(42));
        assert_eq!(
            decode_cursor("/orders/us", &cursor),
            Err(CursorError::WrongStream)
        );
    }

    #[test]
    fn cursor_detects_tampering() {
        let cursor = encode_cursor("/orders/eu", 7);
        let mut bytes = URL_SAFE_NO_PAD.decode(cursor).unwrap();
        bytes[20] ^= 1;
        assert_eq!(
            decode_cursor("/orders/eu", &URL_SAFE_NO_PAD.encode(bytes)),
            Err(CursorError::InvalidChecksum)
        );
    }
}
