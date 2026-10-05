use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

pub const PRIMARY_KEYSPACE: &str = "index_primary";
pub const ENTRIES_KEYSPACE: &str = "index_entries";
pub const CHECKPOINT_KEYSPACE: &str = "index_checkpoints";

const KEY_VERSION: u8 = 1;
const POSTING: u8 = 1;
const UNIQUE_CLAIM: u8 = 2;
const MAX_COLUMNS: usize = 16;
const MAX_KEY_BYTES: usize = 60 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct IndexId(pub Uuid);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrimaryRef {
    pub feed_id: Uuid,
    pub application_key: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexValue {
    Null,
    Bool(bool),
    I64(i64),
    Text(String),
    Bytes(Vec<u8>),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexField {
    Key,
    Metadata(String),
    Projection {
        extractor: String,
        version: u32,
        field: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexDefinition {
    pub id: IndexId,
    pub feed_id: Uuid,
    pub name: String,
    pub fields: Vec<IndexField>,
    pub unique: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexEntry {
    pub id: IndexId,
    pub values: Vec<IndexValue>,
    pub unique: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexMutationPlan {
    pub primary_key: Vec<u8>,
    pub removals: Vec<Vec<u8>>,
    pub insertions: Vec<(Vec<u8>, Vec<u8>)>,
    pub unique_releases: Vec<Vec<u8>>,
    pub unique_claims: Vec<(Vec<u8>, Vec<u8>)>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum IndexKeyError {
    #[error("an Index primary reference must contain a non-empty application Key")]
    EmptyPrimaryKey,
    #[error("an Index requires between 1 and 16 encoded fields")]
    InvalidFieldCount,
    #[error("an encoded Index key exceeds the 60 KiB limit")]
    KeyTooLarge,
    #[error("one record cannot supply more than one value tuple for Index {0:?}")]
    DuplicateIndex(IndexId),
    #[error("a unique Index value is already claimed by another application Key")]
    UniqueConflict,
}

pub fn primary_key(reference: &PrimaryRef) -> Result<Vec<u8>, IndexKeyError> {
    if reference.application_key.is_empty() {
        return Err(IndexKeyError::EmptyPrimaryKey);
    }
    let mut key = vec![KEY_VERSION];
    key.extend_from_slice(reference.feed_id.as_bytes());
    append_escaped(&mut key, &reference.application_key)?;
    bounded(key)
}

pub fn posting_prefix(
    id: IndexId,
    leading_values: &[IndexValue],
) -> Result<Vec<u8>, IndexKeyError> {
    if leading_values.len() > MAX_COLUMNS {
        return Err(IndexKeyError::InvalidFieldCount);
    }
    let mut key = index_prefix(POSTING, id);
    for value in leading_values {
        append_value(&mut key, value)?;
    }
    bounded(key)
}

pub fn posting_key(entry: &IndexEntry, reference: &PrimaryRef) -> Result<Vec<u8>, IndexKeyError> {
    validate_fields(&entry.values)?;
    let mut key = posting_prefix(entry.id, &entry.values)?;
    key.extend_from_slice(&primary_key(reference)?);
    bounded(key)
}

pub fn unique_claim_key(entry: &IndexEntry) -> Result<Vec<u8>, IndexKeyError> {
    validate_fields(&entry.values)?;
    let mut key = index_prefix(UNIQUE_CLAIM, entry.id);
    for value in &entry.values {
        append_value(&mut key, value)?;
    }
    bounded(key)
}

pub fn text_prefix(
    id: IndexId,
    leading_values: &[IndexValue],
    partial_text: &str,
) -> Result<Vec<u8>, IndexKeyError> {
    let mut key = posting_prefix(id, leading_values)?;
    key.push(4);
    append_escaped_bytes(&mut key, partial_text.as_bytes())?;
    bounded(key)
}

pub fn i64_range_start(
    id: IndexId,
    leading_values: &[IndexValue],
    lower_bound: i64,
) -> Result<Vec<u8>, IndexKeyError> {
    let mut key = posting_prefix(id, leading_values)?;
    append_value(&mut key, &IndexValue::I64(lower_bound))?;
    bounded(key)
}

pub fn validate_unique_claim(
    existing_primary: Option<&[u8]>,
    requested_primary: &PrimaryRef,
) -> Result<(), IndexKeyError> {
    let requested = primary_key(requested_primary)?;
    if existing_primary.is_some_and(|existing| existing != requested.as_slice()) {
        return Err(IndexKeyError::UniqueConflict);
    }
    Ok(())
}

pub fn plan_projection_change(
    reference: &PrimaryRef,
    before: &[IndexEntry],
    after: &[IndexEntry],
) -> Result<IndexMutationPlan, IndexKeyError> {
    let primary_key = primary_key(reference)?;
    let old = entries_by_id(before)?;
    let new = entries_by_id(after)?;
    let mut result = IndexMutationPlan {
        primary_key: primary_key.clone(),
        removals: Vec::new(),
        insertions: Vec::new(),
        unique_releases: Vec::new(),
        unique_claims: Vec::new(),
    };
    for (id, entry) in &old {
        if new.get(id) == Some(entry) {
            continue;
        }
        result.removals.push(posting_key(entry, reference)?);
        if entry.unique && !entry.values.contains(&IndexValue::Null) {
            result.unique_releases.push(unique_claim_key(entry)?);
        }
    }
    for (id, entry) in &new {
        if old.get(id) == Some(entry) {
            continue;
        }
        result
            .insertions
            .push((posting_key(entry, reference)?, Vec::new()));
        if entry.unique && !entry.values.contains(&IndexValue::Null) {
            result
                .unique_claims
                .push((unique_claim_key(entry)?, primary_key.clone()));
        }
    }
    Ok(result)
}

fn entries_by_id(entries: &[IndexEntry]) -> Result<BTreeMap<IndexId, IndexEntry>, IndexKeyError> {
    let mut result = BTreeMap::new();
    for entry in entries {
        validate_fields(&entry.values)?;
        if result.insert(entry.id, entry.clone()).is_some() {
            return Err(IndexKeyError::DuplicateIndex(entry.id));
        }
    }
    Ok(result)
}

fn validate_fields(values: &[IndexValue]) -> Result<(), IndexKeyError> {
    if values.is_empty() || values.len() > MAX_COLUMNS {
        return Err(IndexKeyError::InvalidFieldCount);
    }
    Ok(())
}

fn index_prefix(kind: u8, id: IndexId) -> Vec<u8> {
    let mut key = vec![KEY_VERSION, kind];
    key.extend_from_slice(id.0.as_bytes());
    key
}

fn append_value(key: &mut Vec<u8>, value: &IndexValue) -> Result<(), IndexKeyError> {
    match value {
        IndexValue::Null => key.push(0),
        IndexValue::Bool(false) => key.extend_from_slice(&[1, 0]),
        IndexValue::Bool(true) => key.extend_from_slice(&[1, 1]),
        IndexValue::I64(value) => {
            key.push(2);
            key.extend_from_slice(&((*value as u64) ^ (1_u64 << 63)).to_be_bytes());
        }
        IndexValue::Bytes(value) => {
            key.push(3);
            append_escaped(key, value)?;
        }
        IndexValue::Text(value) => {
            key.push(4);
            append_escaped(key, value.as_bytes())?;
        }
    }
    if key.len() > MAX_KEY_BYTES {
        return Err(IndexKeyError::KeyTooLarge);
    }
    Ok(())
}

fn append_escaped(key: &mut Vec<u8>, bytes: &[u8]) -> Result<(), IndexKeyError> {
    append_escaped_bytes(key, bytes)?;
    key.extend_from_slice(&[0, 0]);
    if key.len() > MAX_KEY_BYTES {
        return Err(IndexKeyError::KeyTooLarge);
    }
    Ok(())
}

fn append_escaped_bytes(key: &mut Vec<u8>, bytes: &[u8]) -> Result<(), IndexKeyError> {
    if bytes.len() > MAX_KEY_BYTES {
        return Err(IndexKeyError::KeyTooLarge);
    }
    for &byte in bytes {
        if byte == 0 {
            key.extend_from_slice(&[0, 255]);
        } else {
            key.push(byte);
        }
        if key.len() > MAX_KEY_BYTES {
            return Err(IndexKeyError::KeyTooLarge);
        }
    }
    Ok(())
}

fn bounded(key: Vec<u8>) -> Result<Vec<u8>, IndexKeyError> {
    if key.len() > MAX_KEY_BYTES {
        return Err(IndexKeyError::KeyTooLarge);
    }
    Ok(key)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reference(key: &[u8]) -> PrimaryRef {
        PrimaryRef {
            feed_id: Uuid::from_u128(1),
            application_key: key.to_vec(),
        }
    }

    fn entry(value: IndexValue, unique: bool) -> IndexEntry {
        IndexEntry {
            id: IndexId(Uuid::from_u128(2)),
            values: vec![value],
            unique,
        }
    }

    #[test]
    fn primary_and_secondary_keys_are_unambiguous_for_binary_keys() {
        let a = primary_key(&reference(b"a\0b")).unwrap();
        let b = primary_key(&reference(b"a\0\0b")).unwrap();
        assert_ne!(a, b);
        let a = posting_key(
            &entry(IndexValue::Bytes(b"a\0b".to_vec()), false),
            &reference(b"k"),
        )
        .unwrap();
        let b = posting_key(
            &entry(IndexValue::Bytes(b"a\0\0b".to_vec()), false),
            &reference(b"k"),
        )
        .unwrap();
        assert_ne!(a, b);
    }

    #[test]
    fn duplicate_values_keep_distinct_logical_primary_references() {
        let value = entry(IndexValue::Text("same".to_owned()), false);
        let prefix = posting_prefix(value.id, &value.values).unwrap();
        let first = posting_key(&value, &reference(b"one")).unwrap();
        let second = posting_key(&value, &reference(b"two")).unwrap();
        assert!(first.starts_with(&prefix));
        assert!(second.starts_with(&prefix));
        assert_ne!(first, second);
        assert!(first < second);
    }

    #[test]
    fn composite_boundaries_and_index_ids_do_not_collide() {
        let first = IndexId(Uuid::from_u128(7));
        let second = IndexId(Uuid::from_u128(8));
        let left = [
            IndexValue::Text("a".to_owned()),
            IndexValue::Text("bc".to_owned()),
        ];
        let right = [
            IndexValue::Text("ab".to_owned()),
            IndexValue::Text("c".to_owned()),
        ];
        assert_ne!(
            posting_prefix(first, &left).unwrap(),
            posting_prefix(first, &right).unwrap()
        );
        assert_ne!(
            posting_prefix(first, &left).unwrap(),
            posting_prefix(second, &left).unwrap()
        );
        assert!(
            i64_range_start(first, &[], i64::MIN).unwrap()
                < i64_range_start(first, &[], i64::MAX).unwrap()
        );
    }

    #[test]
    fn composite_and_range_prefixes_preserve_typed_order() {
        let id = IndexId(Uuid::from_u128(7));
        let leading = [IndexValue::Text("US".to_owned())];
        let smaller = i64_range_start(id, &leading, -5).unwrap();
        let larger = i64_range_start(id, &leading, 8).unwrap();
        assert!(smaller < larger);
        let composite = IndexEntry {
            id,
            values: vec![leading[0].clone(), IndexValue::I64(8)],
            unique: false,
        };
        assert!(posting_key(&composite, &reference(b"x"))
            .unwrap()
            .starts_with(&larger));
        let prefix = text_prefix(id, &[], "ord").unwrap();
        assert!(posting_key(
            &IndexEntry {
                id,
                values: vec![IndexValue::Text("orders".to_owned())],
                unique: false
            },
            &reference(b"x")
        )
        .unwrap()
        .starts_with(&prefix));
    }

    #[test]
    fn changing_and_deleting_projection_removes_stale_entries() {
        let old = entry(IndexValue::Text("old".to_owned()), true);
        let new = entry(IndexValue::Text("new".to_owned()), true);
        let updated = plan_projection_change(
            &reference(b"key"),
            std::slice::from_ref(&old),
            std::slice::from_ref(&new),
        )
        .unwrap();
        assert_eq!(
            updated.removals,
            vec![posting_key(&old, &reference(b"key")).unwrap()]
        );
        assert_eq!(
            updated.unique_releases,
            vec![unique_claim_key(&old).unwrap()]
        );
        assert_eq!(
            updated.insertions[0].0,
            posting_key(&new, &reference(b"key")).unwrap()
        );
        assert_eq!(updated.unique_claims[0].0, unique_claim_key(&new).unwrap());
        assert_eq!(updated.unique_claims[0].1, updated.primary_key);
        let deleted = plan_projection_change(&reference(b"key"), &[new], &[]).unwrap();
        assert!(deleted.insertions.is_empty());
        assert_eq!(deleted.removals.len(), 1);
        assert_eq!(deleted.unique_releases.len(), 1);
    }

    #[test]
    fn nulls_have_no_unique_claim_and_invalid_or_large_keys_fail_closed() {
        let null = entry(IndexValue::Null, true);
        let planned = plan_projection_change(&reference(b"k"), &[], &[null]).unwrap();
        assert!(planned.unique_claims.is_empty());
        assert_eq!(
            primary_key(&reference(b"")),
            Err(IndexKeyError::EmptyPrimaryKey)
        );
        assert_eq!(
            posting_prefix(IndexId(Uuid::nil()), &vec![IndexValue::Null; 17]),
            Err(IndexKeyError::InvalidFieldCount)
        );
        assert_eq!(
            primary_key(&reference(&vec![b'a'; MAX_KEY_BYTES])),
            Err(IndexKeyError::KeyTooLarge)
        );
        assert_eq!(
            posting_prefix(
                IndexId(Uuid::nil()),
                &[IndexValue::Text("x".repeat(MAX_KEY_BYTES + 1))]
            ),
            Err(IndexKeyError::KeyTooLarge)
        );
    }

    #[test]
    fn uniqueness_claims_allow_identical_retries_but_reject_another_key() {
        let first = primary_key(&reference(b"one")).unwrap();
        assert_eq!(validate_unique_claim(None, &reference(b"one")), Ok(()));
        assert_eq!(
            validate_unique_claim(Some(&first), &reference(b"one")),
            Ok(())
        );
        assert_eq!(
            validate_unique_claim(Some(&first), &reference(b"two")),
            Err(IndexKeyError::UniqueConflict)
        );
    }

    #[test]
    fn one_record_cannot_define_duplicate_index_ids() {
        let value = entry(IndexValue::I64(1), false);
        assert_eq!(
            plan_projection_change(&reference(b"k"), &[value.clone(), value], &[]),
            Err(IndexKeyError::DuplicateIndex(IndexId(Uuid::from_u128(2))))
        );
    }
}
