use std::collections::BTreeMap;

use thiserror::Error;
use uuid::Uuid;

use crate::domain::StoredRecord;

const MAGIC_V1: &[u8; 4] = b"FSR1";
const MAGIC_V2: &[u8; 4] = b"FSR2";
const MAGIC_LZ4: &[u8; 4] = b"FSR3";
const MIN_COMPRESSION_BYTES: usize = 1024;
pub const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum CodecError {
    #[error("record frame is too large")]
    FrameTooLarge,
    #[error("record frame is truncated")]
    Truncated,
    #[error("record frame magic is invalid")]
    InvalidMagic,
    #[error("record frame checksum is invalid")]
    InvalidChecksum,
    #[error("record metadata key is too long")]
    MetadataKeyTooLong,
    #[error("record field is too large")]
    FieldTooLarge,
    #[error("record UUID is invalid")]
    InvalidUuid,
    #[error("record metadata key is not UTF-8")]
    InvalidMetadataKey,
    #[error("record frame compression is invalid")]
    InvalidCompression,
}

pub fn encode_record(record: &StoredRecord) -> Result<Vec<u8>, CodecError> {
    encode_record_with_compression(record, false)
}

pub fn encode_record_with_compression(
    record: &StoredRecord,
    use_compression: bool,
) -> Result<Vec<u8>, CodecError> {
    let mut body = Vec::new();
    body.extend_from_slice(MAGIC_V2);
    body.extend_from_slice(record.message_id.as_bytes());
    body.extend_from_slice(record.producer_id.as_bytes());
    body.extend_from_slice(&record.producer_sequence.to_be_bytes());
    body.extend_from_slice(&record.event_time_ns.to_be_bytes());
    body.extend_from_slice(&record.ingest_time_ns.to_be_bytes());
    put_u32(&mut body, record.key.len())?;
    put_u32(&mut body, record.payload.len())?;
    let metadata_count: u16 = record
        .metadata
        .len()
        .try_into()
        .map_err(|_| CodecError::FieldTooLarge)?;
    body.extend_from_slice(&metadata_count.to_be_bytes());
    body.extend_from_slice(&record.key);
    body.extend_from_slice(&record.payload);
    for (name, value) in &record.metadata {
        let name_len: u16 = name
            .len()
            .try_into()
            .map_err(|_| CodecError::MetadataKeyTooLong)?;
        body.extend_from_slice(&name_len.to_be_bytes());
        put_u32(&mut body, value.len())?;
        body.extend_from_slice(name.as_bytes());
        body.extend_from_slice(value);
    }
    if body.len() > MAX_FRAME_BYTES {
        return Err(CodecError::FrameTooLarge);
    }
    if use_compression && body.len() >= MIN_COMPRESSION_BYTES {
        let compressed = lz4_flex::block::compress(&body);
        if compressed.len() + 8 < body.len() {
            let mut envelope = Vec::with_capacity(compressed.len() + 8);
            envelope.extend_from_slice(MAGIC_LZ4);
            put_u32(&mut envelope, body.len())?;
            envelope.extend_from_slice(&compressed);
            body = envelope;
        }
    }
    let mut frame = Vec::with_capacity(body.len() + 8);
    frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
    frame.extend_from_slice(&body);
    frame.extend_from_slice(&crc32fast::hash(&body).to_be_bytes());
    Ok(frame)
}

fn checked_body(frame: &[u8]) -> Result<&[u8], CodecError> {
    if frame.len() < 8 {
        return Err(CodecError::Truncated);
    }
    let body_len =
        u32::from_be_bytes(frame[..4].try_into().map_err(|_| CodecError::Truncated)?) as usize;
    if body_len > MAX_FRAME_BYTES || frame.len() != body_len + 8 {
        return Err(if body_len > MAX_FRAME_BYTES {
            CodecError::FrameTooLarge
        } else {
            CodecError::Truncated
        });
    }
    let body = &frame[4..4 + body_len];
    let checksum = u32::from_be_bytes(
        frame[4 + body_len..]
            .try_into()
            .map_err(|_| CodecError::Truncated)?,
    );
    if crc32fast::hash(body) != checksum {
        return Err(CodecError::InvalidChecksum);
    }
    Ok(body)
}

pub fn logical_frame_size(frame: &[u8]) -> Result<usize, CodecError> {
    let body = checked_body(frame)?;
    let size = if body.starts_with(MAGIC_LZ4) {
        let mut envelope = Reader::new(body);
        envelope.take(4)?;
        envelope.u32()? as usize
    } else {
        body.len()
    };
    if size > MAX_FRAME_BYTES {
        return Err(CodecError::FrameTooLarge);
    }
    size.checked_add(8).ok_or(CodecError::FrameTooLarge)
}

pub fn decode_record(frame: &[u8]) -> Result<StoredRecord, CodecError> {
    let body = checked_body(frame)?;
    if body.starts_with(MAGIC_LZ4) {
        let mut envelope = Reader::new(body);
        envelope.take(4)?;
        let decoded_len = envelope.u32()? as usize;
        if decoded_len > MAX_FRAME_BYTES {
            return Err(CodecError::FrameTooLarge);
        }
        let mut decoded = vec![0; decoded_len];
        let actual = lz4_flex::block::decompress_into(&body[8..], &mut decoded)
            .map_err(|_| CodecError::InvalidCompression)?;
        if actual != decoded_len || !decoded.starts_with(MAGIC_V2) {
            return Err(CodecError::InvalidCompression);
        }
        return decode_body(&decoded);
    }
    decode_body(body)
}

fn decode_body(body: &[u8]) -> Result<StoredRecord, CodecError> {
    let mut reader = Reader::new(body);
    let magic = reader.take(4)?;
    if magic != MAGIC_V1 && magic != MAGIC_V2 {
        return Err(CodecError::InvalidMagic);
    }
    let message_id = Uuid::from_slice(reader.take(16)?).map_err(|_| CodecError::InvalidUuid)?;
    let producer_id = Uuid::from_slice(reader.take(16)?).map_err(|_| CodecError::InvalidUuid)?;
    let producer_sequence = reader.u64()?;
    let (event_time_ns, ingest_time_ns) = if magic == MAGIC_V1 {
        let timestamp_ms = reader.u64()?;
        let timestamp_ns =
            i64::try_from(timestamp_ms.saturating_mul(1_000_000)).unwrap_or(i64::MAX);
        (timestamp_ns, timestamp_ns)
    } else {
        (reader.i64()?, reader.i64()?)
    };
    let key_len = reader.u32()? as usize;
    let payload_len = reader.u32()? as usize;
    let metadata_count = reader.u16()?;
    let key = reader.take(key_len)?.to_vec();
    let payload = reader.take(payload_len)?.to_vec();
    let mut metadata = BTreeMap::new();
    for _ in 0..metadata_count {
        let name_len = reader.u16()? as usize;
        let value_len = reader.u32()? as usize;
        let name = std::str::from_utf8(reader.take(name_len)?)
            .map_err(|_| CodecError::InvalidMetadataKey)?
            .to_owned();
        metadata.insert(name, reader.take(value_len)?.to_vec());
    }
    if !reader.is_empty() {
        return Err(CodecError::Truncated);
    }
    Ok(StoredRecord {
        message_id,
        producer_id,
        producer_sequence,
        event_time_ns,
        ingest_time_ns,
        key,
        payload,
        metadata,
    })
}

fn put_u32(target: &mut Vec<u8>, value: usize) -> Result<(), CodecError> {
    let value: u32 = value.try_into().map_err(|_| CodecError::FieldTooLarge)?;
    target.extend_from_slice(&value.to_be_bytes());
    Ok(())
}

struct Reader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }
    fn take(&mut self, length: usize) -> Result<&'a [u8], CodecError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or(CodecError::Truncated)?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or(CodecError::Truncated)?;
        self.position = end;
        Ok(value)
    }
    fn u16(&mut self) -> Result<u16, CodecError> {
        Ok(u16::from_be_bytes(
            self.take(2)?
                .try_into()
                .map_err(|_| CodecError::Truncated)?,
        ))
    }
    fn u32(&mut self) -> Result<u32, CodecError> {
        Ok(u32::from_be_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| CodecError::Truncated)?,
        ))
    }
    fn u64(&mut self) -> Result<u64, CodecError> {
        Ok(u64::from_be_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| CodecError::Truncated)?,
        ))
    }
    fn i64(&mut self) -> Result<i64, CodecError> {
        Ok(i64::from_be_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| CodecError::Truncated)?,
        ))
    }
    fn is_empty(&self) -> bool {
        self.position == self.bytes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_record(payload: Vec<u8>) -> StoredRecord {
        StoredRecord {
            message_id: Uuid::from_u128(1),
            producer_id: Uuid::from_u128(2),
            producer_sequence: 9,
            event_time_ns: i64::MIN,
            ingest_time_ns: i64::MAX,
            key: b"customer-7".to_vec(),
            payload,
            metadata: BTreeMap::from([("source".to_owned(), vec![0, 255, 1])]),
        }
    }

    fn frame_with_body(body: &[u8]) -> Vec<u8> {
        let mut frame = Vec::new();
        frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
        frame.extend_from_slice(body);
        frame.extend_from_slice(&crc32fast::hash(body).to_be_bytes());
        frame
    }

    #[test]
    fn compression_is_opt_in_and_preserves_record_bytes() {
        let record = sample_record(b"{\"device\":\"meter\",\"bytesUp\":12345}".repeat(1000));
        let plain = encode_record(&record).unwrap();
        let compressed = encode_record_with_compression(&record, true).unwrap();
        assert_eq!(&plain[4..8], MAGIC_V2);
        assert_eq!(&compressed[4..8], MAGIC_LZ4);
        assert!(compressed.len() < plain.len() / 10);
        assert_eq!(logical_frame_size(&compressed).unwrap(), plain.len());
        assert_eq!(logical_frame_size(&plain).unwrap(), plain.len());
        assert_eq!(decode_record(&plain).unwrap(), record);
        assert_eq!(decode_record(&compressed).unwrap(), record);
        assert_eq!(
            encode_record_with_compression(&record, false).unwrap(),
            plain
        );
    }

    #[test]
    fn small_and_incompressible_records_keep_the_original_format() {
        let small = sample_record(vec![0, 1, 255]);
        assert_eq!(
            encode_record_with_compression(&small, true).unwrap(),
            encode_record(&small).unwrap()
        );
        let mut seed = 19_u64;
        let payload = (0..16384)
            .map(|_| {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                seed as u8
            })
            .collect();
        let random = sample_record(payload);
        assert_eq!(
            encode_record_with_compression(&random, true).unwrap(),
            encode_record(&random).unwrap()
        );
    }

    #[test]
    fn compressed_frames_reject_corruption_before_decompression() {
        let mut frame =
            encode_record_with_compression(&sample_record(vec![42; 4096]), true).unwrap();
        frame[12] ^= 1;
        assert!(matches!(
            decode_record(&frame),
            Err(CodecError::InvalidChecksum)
        ));
    }

    #[test]
    fn compressed_frames_bound_decoded_size_and_reject_invalid_blocks() {
        let mut body = MAGIC_LZ4.to_vec();
        body.extend_from_slice(&((MAX_FRAME_BYTES + 1) as u32).to_be_bytes());
        assert!(matches!(
            decode_record(&frame_with_body(&body)),
            Err(CodecError::FrameTooLarge)
        ));
        body[4..8].copy_from_slice(&100_u32.to_be_bytes());
        body.push(255);
        assert!(matches!(
            decode_record(&frame_with_body(&body)),
            Err(CodecError::InvalidCompression)
        ));
        let record = sample_record(vec![42; 4096]);
        let frame = encode_record_with_compression(&record, true).unwrap();
        let mut body = frame[4..frame.len() - 4].to_vec();
        let declared = u32::from_be_bytes(body[4..8].try_into().unwrap());
        body[4..8].copy_from_slice(&(declared + 1).to_be_bytes());
        assert!(matches!(
            decode_record(&frame_with_body(&body)),
            Err(CodecError::InvalidCompression)
        ));
    }

    #[test]
    fn compressed_frames_do_not_allow_nested_envelopes() {
        let mut body = MAGIC_LZ4.to_vec();
        body.extend_from_slice(&4_u32.to_be_bytes());
        body.extend_from_slice(&lz4_flex::block::compress(MAGIC_LZ4));
        assert!(matches!(
            decode_record(&frame_with_body(&body)),
            Err(CodecError::InvalidCompression)
        ));
    }

    #[test]
    fn binary_record_round_trips() {
        let record = StoredRecord {
            message_id: Uuid::new_v4(),
            producer_id: Uuid::new_v4(),
            producer_sequence: 9,
            event_time_ns: 123_456_789,
            ingest_time_ns: 123_456_999,
            key: b"customer-7".to_vec(),
            payload: vec![0, 1, 2, 255],
            metadata: BTreeMap::from([(
                "content-type".to_owned(),
                b"application/octet-stream".to_vec(),
            )]),
        };
        assert_eq!(
            decode_record(&encode_record(&record).unwrap()).unwrap(),
            record
        );
    }

    #[test]
    fn legacy_millisecond_record_decodes_to_nanoseconds() {
        let message_id = Uuid::new_v4();
        let producer_id = Uuid::new_v4();
        let timestamp_ms = 1_700_000_000_123_u64;
        let mut body = Vec::new();
        body.extend_from_slice(MAGIC_V1);
        body.extend_from_slice(message_id.as_bytes());
        body.extend_from_slice(producer_id.as_bytes());
        body.extend_from_slice(&7_u64.to_be_bytes());
        body.extend_from_slice(&timestamp_ms.to_be_bytes());
        body.extend_from_slice(&3_u32.to_be_bytes());
        body.extend_from_slice(&5_u32.to_be_bytes());
        body.extend_from_slice(&0_u16.to_be_bytes());
        body.extend_from_slice(b"key");
        body.extend_from_slice(b"value");
        let mut frame = Vec::new();
        frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
        frame.extend_from_slice(&body);
        frame.extend_from_slice(&crc32fast::hash(&body).to_be_bytes());

        let record = decode_record(&frame).unwrap();
        assert_eq!(record.message_id, message_id);
        assert_eq!(record.producer_id, producer_id);
        assert_eq!(record.event_time_ns, 1_700_000_000_123_000_000);
        assert_eq!(record.ingest_time_ns, record.event_time_ns);
    }

    #[test]
    fn binary_record_detects_corruption() {
        let record = StoredRecord {
            message_id: Uuid::new_v4(),
            producer_id: Uuid::new_v4(),
            producer_sequence: 1,
            event_time_ns: 1,
            ingest_time_ns: 2,
            key: vec![1],
            payload: vec![2],
            metadata: BTreeMap::new(),
        };
        let mut frame = encode_record(&record).unwrap();
        frame[20] ^= 1;
        assert!(matches!(
            decode_record(&frame),
            Err(CodecError::InvalidChecksum)
        ));
    }
}
