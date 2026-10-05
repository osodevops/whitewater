use std::collections::BTreeMap;

use fjall::Readable;
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexRow {
    pub reference: PrimaryRef,
    pub payload: Vec<u8>,
    pub entries: Vec<IndexEntry>,
    pub applied_cursor: String,
}

#[derive(Debug, Error)]
pub enum IndexStoreError {
    #[error(transparent)]
    Key(#[from] IndexKeyError),
    #[error(transparent)]
    Engine(#[from] fjall::Error),
    #[error(transparent)]
    Serialization(#[from] serde_json::Error),
    #[error("a secondary Index entry points to a missing or inconsistent primary row")]
    DanglingPosting,
    #[error("an Index Cursor is empty or not valid UTF-8")]
    InvalidCursor,
    #[error("an Index primary row does not match its logical reference")]
    CorruptPrimary,
    #[error("the same Cursor cannot change an Index primary row twice")]
    CursorConflict,
    #[error("an Index projection exceeds the 4 MiB local storage limit")]
    RowTooLarge,
    #[error("an Index lookup limit must be between 1 and 1,000")]
    InvalidLimit,
    #[error("an Index lookup result exceeds the 16 MiB limit")]
    QueryTooLarge,
}

pub struct FjallIndexStore {
    db: fjall::SingleWriterTxDatabase,
    primary: fjall::SingleWriterTxKeyspace,
    entries: fjall::SingleWriterTxKeyspace,
    checkpoints: fjall::SingleWriterTxKeyspace,
}

impl FjallIndexStore {
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self, IndexStoreError> {
        let db = fjall::SingleWriterTxDatabase::builder(path).open()?;
        let primary = db.keyspace(PRIMARY_KEYSPACE, fjall::KeyspaceCreateOptions::default)?;
        let entries = db.keyspace(ENTRIES_KEYSPACE, fjall::KeyspaceCreateOptions::default)?;
        let checkpoints =
            db.keyspace(CHECKPOINT_KEYSPACE, fjall::KeyspaceCreateOptions::default)?;
        Ok(Self {
            db,
            primary,
            entries,
            checkpoints,
        })
    }

    pub fn keyspace_count(&self) -> usize {
        self.db.keyspace_count()
    }

    pub fn upsert(
        &self,
        reference: &PrimaryRef,
        payload: &[u8],
        entries: &[IndexEntry],
        cursor: &str,
    ) -> Result<(), IndexStoreError> {
        if cursor.is_empty() || cursor.len() > 512 {
            return Err(IndexStoreError::InvalidCursor);
        }
        if payload.len() > 4 * 1024 * 1024 || entries.len() > 64 {
            return Err(IndexStoreError::RowTooLarge);
        }
        let key = primary_key(reference)?;
        plan_projection_change(reference, &[], entries)?;
        let row = IndexRow {
            reference: reference.clone(),
            payload: payload.to_vec(),
            entries: entries.to_vec(),
            applied_cursor: cursor.to_owned(),
        };
        let encoded = serde_json::to_vec(&row)?;
        if encoded.len() > 4 * 1024 * 1024 {
            return Err(IndexStoreError::RowTooLarge);
        }
        let mut tx = self
            .db
            .write_tx()
            .durability(Some(fjall::PersistMode::SyncAll));
        let previous = tx
            .get(&self.primary, &key)?
            .map(|bytes| serde_json::from_slice::<IndexRow>(&bytes))
            .transpose()?;
        if let Some(previous) = &previous {
            if previous.reference != *reference {
                return Err(IndexStoreError::CorruptPrimary);
            }
            if previous.applied_cursor == cursor && previous != &row {
                return Err(IndexStoreError::CursorConflict);
            }
        }
        let previous_entries = previous
            .as_ref()
            .map_or(&[][..], |row| row.entries.as_slice());
        let plan = plan_projection_change(reference, previous_entries, entries)?;
        for (claim, _) in &plan.unique_claims {
            let existing = tx.get(&self.entries, claim)?;
            validate_unique_claim(existing.as_deref(), reference)?;
        }
        for key in plan.removals.into_iter().chain(plan.unique_releases) {
            tx.remove(&self.entries, key);
        }
        for (key, _) in plan.insertions {
            tx.insert(&self.entries, key, plan.primary_key.clone());
        }
        for (key, value) in plan.unique_claims {
            tx.insert(&self.entries, key, value);
        }
        tx.insert(&self.primary, key, encoded);
        for entry in entries {
            tx.insert(&self.checkpoints, entry.id.0.as_bytes(), cursor.as_bytes());
        }
        tx.commit()?;
        Ok(())
    }

    pub fn delete(&self, reference: &PrimaryRef, cursor: &str) -> Result<bool, IndexStoreError> {
        if cursor.is_empty() || cursor.len() > 512 {
            return Err(IndexStoreError::InvalidCursor);
        }
        let key = primary_key(reference)?;
        let mut tx = self
            .db
            .write_tx()
            .durability(Some(fjall::PersistMode::SyncAll));
        let Some(previous) = tx.get(&self.primary, &key)? else {
            return Ok(false);
        };
        let previous: IndexRow = serde_json::from_slice(&previous)?;
        if previous.reference != *reference {
            return Err(IndexStoreError::CorruptPrimary);
        }
        if previous.applied_cursor == cursor {
            return Err(IndexStoreError::CursorConflict);
        }
        let plan = plan_projection_change(reference, &previous.entries, &[])?;
        for key in plan.removals.into_iter().chain(plan.unique_releases) {
            tx.remove(&self.entries, key);
        }
        tx.remove(&self.primary, key);
        for entry in &previous.entries {
            tx.insert(&self.checkpoints, entry.id.0.as_bytes(), cursor.as_bytes());
        }
        tx.commit()?;
        Ok(true)
    }

    pub fn get(&self, reference: &PrimaryRef) -> Result<Option<IndexRow>, IndexStoreError> {
        let key = primary_key(reference)?;
        let Some(bytes) = self.db.read_tx().get(&self.primary, key)? else {
            return Ok(None);
        };
        let row: IndexRow = serde_json::from_slice(&bytes)?;
        if row.reference != *reference {
            return Err(IndexStoreError::CorruptPrimary);
        }
        Ok(Some(row))
    }

    pub fn lookup_exact(
        &self,
        id: IndexId,
        values: &[IndexValue],
        limit: usize,
    ) -> Result<Vec<IndexRow>, IndexStoreError> {
        validate_fields(values)?;
        if !(1..=1_000).contains(&limit) {
            return Err(IndexStoreError::InvalidLimit);
        }
        let prefix = posting_prefix(id, values)?;
        let snapshot = self.db.read_tx();
        let mut rows = Vec::new();
        let mut bytes_seen = 0_usize;
        for entry in snapshot.prefix(&self.entries, prefix).take(limit) {
            let (_, reference_key) = entry.into_inner()?;
            let stored = snapshot
                .get(&self.primary, &reference_key)?
                .ok_or(IndexStoreError::DanglingPosting)?;
            bytes_seen = bytes_seen
                .checked_add(stored.len())
                .ok_or(IndexStoreError::QueryTooLarge)?;
            if bytes_seen > 16 * 1024 * 1024 {
                return Err(IndexStoreError::QueryTooLarge);
            }
            let row: IndexRow = serde_json::from_slice(&stored)?;
            if primary_key(&row.reference)?.as_slice() != &*reference_key
                || !row
                    .entries
                    .iter()
                    .any(|entry| entry.id == id && entry.values == values)
            {
                return Err(IndexStoreError::DanglingPosting);
            }
            rows.push(row);
        }
        Ok(rows)
    }

    pub fn last_applied(&self, id: IndexId) -> Result<Option<String>, IndexStoreError> {
        self.db
            .read_tx()
            .get(&self.checkpoints, id.0.as_bytes())?
            .map(|bytes| {
                std::str::from_utf8(&bytes)
                    .map(str::to_owned)
                    .map_err(|_| IndexStoreError::InvalidCursor)
            })
            .transpose()
    }
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

    #[test]
    fn fjall_primary_and_city_index_survive_restart_without_a_keyspace_per_index() {
        let directory = tempfile::tempdir().unwrap();
        let city = entry(IndexValue::Text("London".to_owned()), false);
        let store = FjallIndexStore::open(directory.path()).unwrap();
        for (key, location) in [
            (b"alice".as_slice(), "London"),
            (b"bob".as_slice(), "London"),
            (b"charlie".as_slice(), "Paris"),
        ] {
            store
                .upsert(
                    &reference(key),
                    location.as_bytes(),
                    &[entry(IndexValue::Text(location.to_owned()), false)],
                    "cursor-1",
                )
                .unwrap();
        }
        let age = IndexEntry {
            id: IndexId(Uuid::from_u128(3)),
            values: vec![IndexValue::I64(23)],
            unique: false,
        };
        store
            .upsert(
                &reference(b"alice"),
                b"London",
                &[city.clone(), age.clone()],
                "cursor-2",
            )
            .unwrap();
        assert_eq!(store.keyspace_count(), 3);
        assert_eq!(
            store.lookup_exact(age.id, &age.values, 10).unwrap().len(),
            1
        );
        assert_eq!(
            store.lookup_exact(city.id, &city.values, 10).unwrap().len(),
            2
        );
        drop(store);
        let store = FjallIndexStore::open(directory.path()).unwrap();
        let london = store.lookup_exact(city.id, &city.values, 10).unwrap();
        assert_eq!(
            london
                .iter()
                .map(|row| row.reference.application_key.as_slice())
                .collect::<Vec<_>>(),
            vec![b"alice".as_slice(), b"bob".as_slice()]
        );
        assert_eq!(
            store.get(&reference(b"charlie")).unwrap().unwrap().payload,
            b"Paris"
        );
        assert_eq!(
            store.last_applied(city.id).unwrap().as_deref(),
            Some("cursor-2")
        );
    }

    #[test]
    fn fjall_single_writer_transaction_serializes_local_unique_claims() {
        let directory = tempfile::tempdir().unwrap();
        let store = FjallIndexStore::open(directory.path()).unwrap();
        let unique = entry(IndexValue::Text("same".to_owned()), true);
        let (alice, bob) = std::thread::scope(|threads| {
            let first = threads.spawn(|| {
                store.upsert(
                    &reference(b"alice"),
                    b"first",
                    std::slice::from_ref(&unique),
                    "cursor-a",
                )
            });
            let second = threads.spawn(|| {
                store.upsert(
                    &reference(b"bob"),
                    b"second",
                    std::slice::from_ref(&unique),
                    "cursor-b",
                )
            });
            (first.join().unwrap(), second.join().unwrap())
        });
        assert!(alice.is_ok() ^ bob.is_ok());
        assert_eq!(
            store
                .lookup_exact(unique.id, &unique.values, 10)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn fjall_update_delete_and_conflict_leave_no_stale_postings() {
        let directory = tempfile::tempdir().unwrap();
        let store = FjallIndexStore::open(directory.path()).unwrap();
        let london = entry(IndexValue::Text("London".to_owned()), true);
        let paris = entry(IndexValue::Text("Paris".to_owned()), true);
        store
            .upsert(
                &reference(b"alice"),
                b"original",
                std::slice::from_ref(&london),
                "cursor-1",
            )
            .unwrap();
        assert!(matches!(
            store.upsert(
                &reference(b"alice"),
                b"changed-without-new-cursor",
                std::slice::from_ref(&london),
                "cursor-1",
            ),
            Err(IndexStoreError::CursorConflict)
        ));
        assert!(store
            .upsert(
                &reference(b"bob"),
                b"conflict",
                std::slice::from_ref(&london),
                "cursor-2"
            )
            .is_err());
        assert!(store.get(&reference(b"bob")).unwrap().is_none());
        assert_eq!(
            store.last_applied(london.id).unwrap().as_deref(),
            Some("cursor-1")
        );
        store
            .upsert(
                &reference(b"alice"),
                b"moved",
                std::slice::from_ref(&paris),
                "cursor-3",
            )
            .unwrap();
        assert!(store
            .lookup_exact(london.id, &london.values, 10)
            .unwrap()
            .is_empty());
        assert_eq!(
            store.lookup_exact(paris.id, &paris.values, 10).unwrap()[0].payload,
            b"moved"
        );
        assert!(store.delete(&reference(b"alice"), "cursor-4").unwrap());
        assert!(store
            .lookup_exact(paris.id, &paris.values, 10)
            .unwrap()
            .is_empty());
        assert!(store.get(&reference(b"alice")).unwrap().is_none());
    }
}
