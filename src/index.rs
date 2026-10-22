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
    #[error("an encoded Index key is truncated or mistagged")]
    CorruptKey,
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

/// Decode one typed value beginning at `offset`, returning the value and
/// the offset just past it (past the escaped terminator for Bytes/Text).
/// Fails closed on truncated or mistagged bytes — postings under an
/// acknowledged prefix must decode cleanly.
fn decode_value_at(key: &[u8], offset: usize) -> Result<(IndexValue, usize), IndexKeyError> {
    match key.get(offset) {
        Some(0) => Ok((IndexValue::Null, offset + 1)),
        Some(1) => match key.get(offset + 1) {
            Some(0) => Ok((IndexValue::Bool(false), offset + 2)),
            Some(1) => Ok((IndexValue::Bool(true), offset + 2)),
            _ => Err(IndexKeyError::CorruptKey),
        },
        Some(2) => {
            let end = offset + 9;
            let raw: [u8; 8] = key
                .get(offset + 1..end)
                .and_then(|slice| <[u8; 8]>::try_from(slice).ok())
                .ok_or(IndexKeyError::CorruptKey)?;
            Ok((
                IndexValue::I64((u64::from_be_bytes(raw) ^ (1_u64 << 63)) as i64),
                end,
            ))
        }
        Some(tag @ (3 | 4)) => {
            let mut bytes = Vec::new();
            let mut cursor = offset + 1;
            loop {
                match key.get(cursor) {
                    Some(0) => match key.get(cursor + 1) {
                        Some(0) => {
                            cursor += 2;
                            break;
                        }
                        Some(1) => {
                            bytes.push(0);
                            cursor += 2;
                        }
                        _ => return Err(IndexKeyError::CorruptKey),
                    },
                    Some(&byte) => {
                        bytes.push(byte);
                        cursor += 1;
                    }
                    None => return Err(IndexKeyError::CorruptKey),
                }
            }
            let value = if *tag == 3 {
                IndexValue::Bytes(bytes)
            } else {
                IndexValue::Text(String::from_utf8(bytes).map_err(|_| IndexKeyError::CorruptKey)?)
            };
            Ok((value, cursor))
        }
        _ => Err(IndexKeyError::CorruptKey),
    }
}

/// Total order matching the encoded key order: Null < Bool < I64 < Bytes <
/// Text, with Bool false < true, I64 numeric, and Bytes/Text bytewise.
/// Used by bounded range scans to compare a decoded posting field against
/// caller bounds without re-encoding per key.
pub fn cmp_index_value(left: &IndexValue, right: &IndexValue) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    fn tag(value: &IndexValue) -> u8 {
        match value {
            IndexValue::Null => 0,
            IndexValue::Bool(_) => 1,
            IndexValue::I64(_) => 2,
            IndexValue::Bytes(_) => 3,
            IndexValue::Text(_) => 4,
        }
    }
    match tag(left).cmp(&tag(right)) {
        Ordering::Equal => match (left, right) {
            (IndexValue::Bool(a), IndexValue::Bool(b)) => a.cmp(b),
            (IndexValue::I64(a), IndexValue::I64(b)) => a.cmp(b),
            (IndexValue::Bytes(a), IndexValue::Bytes(b)) => a.cmp(b),
            (IndexValue::Text(a), IndexValue::Text(b)) => a.as_bytes().cmp(b.as_bytes()),
            _ => Ordering::Equal,
        },
        order => order,
    }
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
    /// Index generations whose unique claim this row lost to an earlier
    /// committed primary: the row keeps its payload and other postings,
    /// but the suppressed Index never resolves it. Populated by committed
    /// order so every replica derives the identical set.
    #[serde(default)]
    pub conflicted: Vec<IndexId>,
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

/// The narrow transactional contract every local Index replica engine must
/// honour. `upsert`/`delete` are serializable read-modify-write batches:
/// posting removals, unique-claim checks, the primary row, and applied-Cursor
/// checkpoints commit atomically and are durable (fsync) before the call
/// returns. Reads are snapshot-consistent and bounded by caller limits and
/// fixed byte budgets. The interface is intentionally engine-agnostic so the
/// selected engine remains an implementation detail of replica code.
/// The fixed little-endian payload of an additive-accumulator row:
/// `count u64 | sum i128` totalling 24 bytes.
pub const ACCUMULATOR_ROW_BYTES: usize = 24;

/// Encode an additive-accumulator payload.
pub fn encode_accumulator(count: u64, sum: i128) -> Vec<u8> {
    let mut payload = Vec::with_capacity(ACCUMULATOR_ROW_BYTES);
    payload.extend_from_slice(&count.to_le_bytes());
    payload.extend_from_slice(&sum.to_le_bytes());
    payload
}

/// Decode an additive-accumulator payload; `None` when the bytes are not a
/// well-formed accumulator.
pub fn decode_accumulator(payload: &[u8]) -> Option<(u64, i128)> {
    if payload.len() != ACCUMULATOR_ROW_BYTES {
        return None;
    }
    let count = u64::from_le_bytes(payload[..8].try_into().ok()?);
    let sum = i128::from_le_bytes(payload[8..].try_into().ok()?);
    Some((count, sum))
}

/// One additive merge inside an `accumulate_rows` batch: `count_delta` and
/// `sum_delta` add onto the row's accumulator payload and `entries` replace
/// the row's postings exactly like `upsert`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccumulateRowWrite {
    pub reference: PrimaryRef,
    pub count_delta: u64,
    /// One entry's value delta; the stored accumulator keeps i128 headroom
    /// so many i64 deltas cannot overflow the running sum.
    pub sum_delta: i64,
    pub entries: Vec<IndexEntry>,
}

pub trait IndexStore: Send + Sync {
    /// Apply one projected row mutation under `cursor`. Refuses a reused
    /// `cursor` that would change an existing row, unowned unique claims,
    /// oversized rows, and invalid cursors. `conflicted` names the Index
    /// generations whose claims the caller already arbitrated (suppressed
    /// entries); it is merged onto the row so queries can report the loss
    /// honestly, and ids present in `entries` clear their prior mark.
    fn upsert(
        &self,
        reference: &PrimaryRef,
        payload: &[u8],
        entries: &[IndexEntry],
        conflicted: &[IndexId],
        cursor: &str,
    ) -> Result<(), IndexStoreError>;

    /// The primary Key holding `id`'s unique claim on `values`, if any.
    /// Unique arbitration reads this before planning a claim so committed
    /// order decides ownership deterministically on every replica.
    fn claim_owner(
        &self,
        id: IndexId,
        values: &[IndexValue],
    ) -> Result<Option<Vec<u8>>, IndexStoreError>;

    /// Atomically merge a batch of additive accumulator rows under `cursor`:
    /// each item reads its current row, adds its deltas to the 24-byte
    /// accumulator payload, replaces its postings, and commits inside one
    /// durable transaction so the batch lands whole or not at all. A row
    /// already stamped with `cursor` is skipped, which makes a replayed
    /// batch a no-op per row instead of a double count. A unique claim
    /// already held by another row drops only that item's postings; the
    /// accumulator still merges, deterministically on every replica.
    fn accumulate_rows(
        &self,
        rows: &[AccumulateRowWrite],
        cursor: &str,
    ) -> Result<(), IndexStoreError>;

    /// Remove the row and all of its postings under `cursor`; `false` when no
    /// row exists for `reference`.
    fn delete(&self, reference: &PrimaryRef, cursor: &str) -> Result<bool, IndexStoreError>;

    /// Fetch the current projected row for `reference`.
    fn get(&self, reference: &PrimaryRef) -> Result<Option<IndexRow>, IndexStoreError>;

    /// Resolve up to `limit` (1..=1_000) rows by exact typed Index values.
    fn lookup_exact(
        &self,
        id: IndexId,
        values: &[IndexValue],
        limit: usize,
    ) -> Result<Vec<IndexRow>, IndexStoreError>;

    /// The last applied Cursor recorded for `id` by a committed batch.
    fn last_applied(&self, id: IndexId) -> Result<Option<String>, IndexStoreError>;

    /// Ordered primary-row scan for one `feed_id` scope (Index or
    /// StateStore), starting strictly after `after_key` and bounded by
    /// `limit` rows and an internal byte budget. Replica catch-up and
    /// checkpoint transfer page through this.
    fn scan(
        &self,
        feed_id: uuid::Uuid,
        after_key: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<IndexRow>, IndexStoreError>;

    /// Drop up to `limit` postings claimed by `id` (one shadow generation
    /// or one Index) and strip the entries from affected rows. Bounded per
    /// call; callers loop until it returns zero.
    fn drop_index_postings(&self, id: IndexId, limit: usize) -> Result<usize, IndexStoreError>;

    /// Prefix scan: rows whose posting under `id` starts with
    /// `leading_values`, optionally narrowing the next field to text
    /// beginning with `partial_text`. Bounded by `limit` rows and a
    /// payload-byte budget.
    fn lookup_prefix(
        &self,
        id: IndexId,
        leading_values: &[IndexValue],
        partial_text: Option<&str>,
        limit: usize,
    ) -> Result<Vec<IndexRow>, IndexStoreError>;

    /// Bounded range scan on the field following `leading_values`
    /// (leading fields are exact-equal, like a composite leftmost match).
    /// Bounds are typed `IndexValue` endpoints; inclusive/exclusive on
    /// either side.
    fn lookup_range(
        &self,
        id: IndexId,
        leading_values: &[IndexValue],
        lower: std::ops::Bound<IndexValue>,
        upper: std::ops::Bound<IndexValue>,
        limit: usize,
    ) -> Result<Vec<IndexRow>, IndexStoreError>;
}

/// The posting namespace one rebuild generation writes under: deterministic
/// so every replica derives the same shadow identity, and disjoint from the
/// serving generation's postings until activation swaps which generation
/// readers consult.
pub fn shadow_index_id(index_id: uuid::Uuid, generation: u64) -> IndexId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(index_id.as_bytes());
    hasher.update(&generation.to_be_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    IndexId(uuid::Uuid::from_bytes(bytes))
}

/// Fjall crash-recovery replays the write journal, so its size bounds reopen
/// time: the database rotates sealed journals once 64 MiB is outstanding and
/// each keyspace flushes its memtable at 16 MiB, keeping the surviving replay
/// tail small rather than proportional to total history.
const JOURNAL_REPLAY_BUDGET_BYTES: u64 = 64 * 1024 * 1024;
const MEMTABLE_FLUSH_BYTES: u64 = 16 * 1024 * 1024;

fn bounded_keyspace() -> fjall::KeyspaceCreateOptions {
    fjall::KeyspaceCreateOptions::default().max_memtable_size(MEMTABLE_FLUSH_BYTES)
}

pub struct FjallIndexStore {
    db: fjall::SingleWriterTxDatabase,
    primary: fjall::SingleWriterTxKeyspace,
    entries: fjall::SingleWriterTxKeyspace,
    checkpoints: fjall::SingleWriterTxKeyspace,
}

impl FjallIndexStore {
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self, IndexStoreError> {
        let db = fjall::SingleWriterTxDatabase::builder(path)
            .max_journaling_size(JOURNAL_REPLAY_BUDGET_BYTES)
            .open()?;
        let primary = db.keyspace(PRIMARY_KEYSPACE, bounded_keyspace)?;
        let entries = db.keyspace(ENTRIES_KEYSPACE, bounded_keyspace)?;
        let checkpoints = db.keyspace(CHECKPOINT_KEYSPACE, bounded_keyspace)?;
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
}

impl IndexStore for FjallIndexStore {
    fn upsert(
        &self,
        reference: &PrimaryRef,
        payload: &[u8],
        entries: &[IndexEntry],
        conflicted: &[IndexId],
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
            conflicted: conflicted.to_vec(),
            applied_cursor: cursor.to_owned(),
        };
        let mut tx = self
            .db
            .write_tx()
            .durability(Some(fjall::PersistMode::SyncAll));
        let previous = tx
            .get(&self.primary, &key)?
            .map(|bytes| serde_json::from_slice::<IndexRow>(&bytes))
            .transpose()?;
        // Conflict evidence merges across applies: ids re-projected by this
        // write clear their mark, ids the caller arbitrated are added, and
        // marks for untouched generations persist.
        let mut merged_conflicted = previous
            .as_ref()
            .map(|row| row.conflicted.clone())
            .unwrap_or_default();
        merged_conflicted
            .retain(|id| !entries.iter().any(|entry| entry.id == *id) || conflicted.contains(id));
        for id in conflicted {
            if !merged_conflicted.contains(id) {
                merged_conflicted.push(*id);
            }
        }
        let row = IndexRow {
            conflicted: merged_conflicted,
            ..row
        };
        let encoded = serde_json::to_vec(&row)?;
        if encoded.len() > 4 * 1024 * 1024 {
            return Err(IndexStoreError::RowTooLarge);
        }
        if let Some(previous) = &previous {
            if previous.reference != *reference {
                return Err(IndexStoreError::CorruptPrimary);
            }
            if previous.applied_cursor == cursor && previous != &row {
                // The same record cursor may legitimately write the row a
                // second time when a shadow rebuild generation registers an
                // additional entry: payload unchanged, prior entries intact,
                // only new-id entries added. Anything else at the same
                // cursor is a divergent write and still fences.
                let additive_only = previous.payload == row.payload
                    && row.entries.len() > previous.entries.len()
                    && previous
                        .entries
                        .iter()
                        .all(|entry| row.entries.contains(entry));
                if !additive_only {
                    return Err(IndexStoreError::CursorConflict);
                }
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
            // Per-id checkpoints never regress: a shadow build replaying
            // history behind the serving generation's frontier must not
            // lower that generation's recorded frontier.
            let existing = tx.get(&self.checkpoints, entry.id.0.as_bytes())?;
            let dominated = existing
                .as_deref()
                .is_some_and(|applied| applied >= cursor.as_bytes());
            if !dominated {
                tx.insert(&self.checkpoints, entry.id.0.as_bytes(), cursor.as_bytes());
            }
        }
        tx.commit()?;
        Ok(())
    }

    fn accumulate_rows(
        &self,
        rows: &[AccumulateRowWrite],
        cursor: &str,
    ) -> Result<(), IndexStoreError> {
        if cursor.is_empty() || cursor.len() > 512 {
            return Err(IndexStoreError::InvalidCursor);
        }
        if rows.is_empty() || rows.len() > 64 {
            return Err(IndexStoreError::RowTooLarge);
        }
        let mut tx = self
            .db
            .write_tx()
            .durability(Some(fjall::PersistMode::SyncAll));
        for row in rows {
            let key = primary_key(&row.reference)?;
            if row.entries.len() > 64 {
                return Err(IndexStoreError::RowTooLarge);
            }
            let previous = tx
                .get(&self.primary, &key)?
                .map(|bytes| serde_json::from_slice::<IndexRow>(&bytes))
                .transpose()?;
            if let Some(previous) = &previous {
                if previous.reference != row.reference {
                    return Err(IndexStoreError::CorruptPrimary);
                }
                if previous.applied_cursor == cursor {
                    // This batch already landed on the row: replaying the
                    // same cursor is an idempotent no-op, not a re-merge.
                    continue;
                }
            }
            let (count, sum) = previous
                .as_ref()
                .and_then(|row| decode_accumulator(&row.payload))
                .unwrap_or((0, 0));
            let payload = encode_accumulator(
                count.saturating_add(row.count_delta),
                sum.saturating_add(i128::from(row.sum_delta)),
            );
            let previous_entries = previous
                .as_ref()
                .map_or(&[][..], |row| row.entries.as_slice());
            // Unique claims the row cannot hold drop its postings rather
            // than the merge itself: the accumulator still advances and
            // every replica resolves the conflict identically.
            let conflicted = {
                let plan = plan_projection_change(&row.reference, previous_entries, &row.entries)?;
                let mut conflicted = false;
                for (claim, _) in &plan.unique_claims {
                    let existing = tx.get(&self.entries, claim)?;
                    if validate_unique_claim(existing.as_deref(), &row.reference).is_err() {
                        conflicted = true;
                        break;
                    }
                }
                conflicted
            };
            let target_entries: &[IndexEntry] = if conflicted { &[] } else { &row.entries };
            let index_row = IndexRow {
                reference: row.reference.clone(),
                payload,
                entries: target_entries.to_vec(),
                conflicted: previous
                    .as_ref()
                    .map(|row| row.conflicted.clone())
                    .unwrap_or_default(),
                applied_cursor: cursor.to_owned(),
            };
            let encoded = serde_json::to_vec(&index_row)?;
            if encoded.len() > 4 * 1024 * 1024 {
                return Err(IndexStoreError::RowTooLarge);
            }
            let plan = plan_projection_change(&row.reference, previous_entries, target_entries)?;
            for (claim, _) in &plan.unique_claims {
                let existing = tx.get(&self.entries, claim)?;
                validate_unique_claim(existing.as_deref(), &row.reference)?;
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
            for entry in target_entries {
                let existing = tx.get(&self.checkpoints, entry.id.0.as_bytes())?;
                let dominated = existing
                    .as_deref()
                    .is_some_and(|applied| applied >= cursor.as_bytes());
                if !dominated {
                    tx.insert(&self.checkpoints, entry.id.0.as_bytes(), cursor.as_bytes());
                }
            }
        }
        tx.commit()?;
        Ok(())
    }

    fn delete(&self, reference: &PrimaryRef, cursor: &str) -> Result<bool, IndexStoreError> {
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

    fn get(&self, reference: &PrimaryRef) -> Result<Option<IndexRow>, IndexStoreError> {
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

    fn claim_owner(
        &self,
        id: IndexId,
        values: &[IndexValue],
    ) -> Result<Option<Vec<u8>>, IndexStoreError> {
        let key = unique_claim_key(&IndexEntry {
            id,
            values: values.to_vec(),
            unique: true,
        })?;
        self.db
            .read_tx()
            .get(&self.entries, key)
            .map(|value| value.map(|slice| slice.to_vec()))
            .map_err(IndexStoreError::from)
    }

    fn lookup_exact(
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

    fn lookup_prefix(
        &self,
        id: IndexId,
        leading_values: &[IndexValue],
        partial_text: Option<&str>,
        limit: usize,
    ) -> Result<Vec<IndexRow>, IndexStoreError> {
        if !(1..=1_000).contains(&limit) {
            return Err(IndexStoreError::InvalidLimit);
        }
        // A partial-text clause only narrows a Text field.
        if partial_text.is_some() && leading_values.len() >= MAX_COLUMNS {
            return Err(IndexKeyError::InvalidFieldCount.into());
        }
        let prefix = match partial_text {
            Some(partial) => text_prefix(id, leading_values, partial)?,
            None => posting_prefix(id, leading_values)?,
        };
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
            if primary_key(&row.reference)?.as_slice() != &*reference_key {
                return Err(IndexStoreError::DanglingPosting);
            }
            let Some(entry) = row.entries.iter().find(|entry| entry.id == id) else {
                return Err(IndexStoreError::DanglingPosting);
            };
            if !entry.values.starts_with(leading_values)
                || partial_text.is_some_and(|partial| {
                    !matches!(
                        entry.values.get(leading_values.len()),
                        Some(IndexValue::Text(text)) if text.starts_with(partial)
                    )
                })
            {
                return Err(IndexStoreError::DanglingPosting);
            }
            rows.push(row);
        }
        Ok(rows)
    }

    fn lookup_range(
        &self,
        id: IndexId,
        leading_values: &[IndexValue],
        lower: std::ops::Bound<IndexValue>,
        upper: std::ops::Bound<IndexValue>,
        limit: usize,
    ) -> Result<Vec<IndexRow>, IndexStoreError> {
        use std::ops::Bound;
        if !(1..=1_000).contains(&limit) {
            return Err(IndexStoreError::InvalidLimit);
        }
        let leading_prefix = posting_prefix(id, leading_values)?;
        // Start the byte range at the inclusive encoding of the lower bound
        // (or the bare leading prefix when unbounded); ordering is
        // preserved by the key codec so this skips every key below lower.
        let start = match &lower {
            Bound::Included(value) | Bound::Excluded(value) => {
                let mut key = leading_prefix.clone();
                append_value(&mut key, value)?;
                key
            }
            Bound::Unbounded => leading_prefix.clone(),
        };
        let snapshot = self.db.read_tx();
        let mut rows = Vec::new();
        let mut bytes_seen = 0_usize;
        for guard in snapshot.range(&self.entries, start..) {
            if rows.len() >= limit {
                break;
            }
            let (key, reference_key) = guard.into_inner()?;
            if !key.starts_with(&leading_prefix) {
                break;
            }
            let (value, _) = decode_value_at(&key, leading_prefix.len())?;
            let above_lower = match &lower {
                Bound::Included(bound) => {
                    cmp_index_value(&value, bound) != std::cmp::Ordering::Less
                }
                Bound::Excluded(bound) => {
                    cmp_index_value(&value, bound) == std::cmp::Ordering::Greater
                }
                Bound::Unbounded => true,
            };
            let below_upper = match &upper {
                Bound::Included(bound) => {
                    cmp_index_value(&value, bound) != std::cmp::Ordering::Greater
                }
                Bound::Excluded(bound) => {
                    cmp_index_value(&value, bound) == std::cmp::Ordering::Less
                }
                Bound::Unbounded => true,
            };
            if !below_upper {
                break;
            }
            if !above_lower {
                continue;
            }
            let stored = snapshot
                .get(&self.primary, reference_key.as_ref())?
                .ok_or(IndexStoreError::DanglingPosting)?;
            bytes_seen = bytes_seen
                .checked_add(stored.len())
                .ok_or(IndexStoreError::QueryTooLarge)?;
            if bytes_seen > 16 * 1024 * 1024 {
                return Err(IndexStoreError::QueryTooLarge);
            }
            let row: IndexRow = serde_json::from_slice(&stored)?;
            if primary_key(&row.reference)?.as_slice() != reference_key.as_ref() {
                return Err(IndexStoreError::DanglingPosting);
            }
            let Some(entry) = row.entries.iter().find(|entry| entry.id == id) else {
                return Err(IndexStoreError::DanglingPosting);
            };
            if !entry.values.starts_with(leading_values)
                || entry.values.get(leading_values.len()) != Some(&value)
            {
                return Err(IndexStoreError::DanglingPosting);
            }
            rows.push(row);
        }
        Ok(rows)
    }

    fn scan(
        &self,
        feed_id: uuid::Uuid,
        after_key: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<IndexRow>, IndexStoreError> {
        let limit = limit.clamp(1, 1_000);
        let mut prefix = vec![KEY_VERSION];
        prefix.extend_from_slice(feed_id.as_bytes());
        let snapshot = self.db.read_tx();
        let mut rows = Vec::new();
        let mut bytes_seen = 0usize;
        for guard in snapshot.prefix(&self.primary, prefix) {
            let (key, value) = guard.into_inner()?;
            bytes_seen = bytes_seen
                .checked_add(value.len())
                .ok_or(IndexStoreError::QueryTooLarge)?;
            if bytes_seen > 16 * 1024 * 1024 {
                return Err(IndexStoreError::QueryTooLarge);
            }
            let row: IndexRow = serde_json::from_slice(&value)?;
            if primary_key(&row.reference)?.as_slice() != &*key {
                return Err(IndexStoreError::CorruptPrimary);
            }
            if row.reference.feed_id != feed_id {
                continue;
            }
            if let Some(after) = after_key {
                if row.reference.application_key.as_slice() <= after {
                    continue;
                }
            }
            rows.push(row);
            if rows.len() >= limit {
                break;
            }
        }
        Ok(rows)
    }

    fn drop_index_postings(&self, id: IndexId, limit: usize) -> Result<usize, IndexStoreError> {
        let limit = limit.clamp(1, 1_024);
        // Both families in `entries` (postings and unique claims) share the
        // `index_prefix(family, id)` layout and carry the primary row key as
        // their value, so one bounded sweep removes both.
        let snapshot = self.db.read_tx();
        let mut primary_keys = Vec::new();
        let mut posting_keys = Vec::new();
        for family in [POSTING, UNIQUE_CLAIM] {
            let prefix = index_prefix(family, id);
            for guard in snapshot.prefix(&self.entries, prefix) {
                let (key, value) = guard.into_inner()?;
                posting_keys.push(key.to_vec());
                primary_keys.push(value.to_vec());
                if posting_keys.len() >= limit {
                    break;
                }
            }
            if posting_keys.len() >= limit {
                break;
            }
        }
        if posting_keys.is_empty() {
            // Prefix is empty: the generation checkpoint goes with its last
            // posting so a later driver pass treats the id as fully retired.
            let mut tx = self
                .db
                .write_tx()
                .durability(Some(fjall::PersistMode::SyncAll));
            tx.remove(&self.checkpoints, id.0.as_bytes());
            tx.commit()?;
            return Ok(0);
        }
        let mut tx = self
            .db
            .write_tx()
            .durability(Some(fjall::PersistMode::SyncAll));
        let mut removed = 0usize;
        let mut touched = std::collections::BTreeSet::new();
        for (posting_key, reference_key) in posting_keys.iter().zip(primary_keys.iter()) {
            tx.remove(&self.entries, posting_key.clone());
            removed += 1;
            if touched.insert(reference_key.clone()) {
                if let Some(bytes) = tx.get(&self.primary, reference_key.clone())? {
                    let mut row: IndexRow = serde_json::from_slice(&bytes)?;
                    let before = row.entries.len();
                    row.entries.retain(|entry| entry.id != id);
                    if row.entries.len() != before {
                        tx.insert(
                            &self.primary,
                            reference_key.clone(),
                            serde_json::to_vec(&row)?,
                        );
                    }
                }
            }
        }
        // The generation's applied-Cursor checkpoint is dropped with its
        // last posting; builders re-derive it on the next pass.
        if removed < limit {
            tx.remove(&self.checkpoints, id.0.as_bytes());
        }
        tx.commit()?;
        Ok(removed)
    }

    fn last_applied(&self, id: IndexId) -> Result<Option<String>, IndexStoreError> {
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
    fn claim_owner_and_conflicted_marks_survive_upsert_and_restart() {
        let directory = tempfile::tempdir().unwrap();
        let id = IndexId(Uuid::from_u128(7));
        let winner = reference(b"winner");
        let loser = reference(b"loser");
        let claim = IndexEntry {
            id,
            values: vec![IndexValue::Text("dup".to_owned())],
            unique: true,
        };
        {
            let store = FjallIndexStore::open(directory.path()).unwrap();
            store
                .upsert(
                    &winner,
                    b"one",
                    std::slice::from_ref(&claim),
                    &[],
                    "cursor-1",
                )
                .unwrap();
            assert_eq!(
                store
                    .claim_owner(id, &[IndexValue::Text("dup".to_owned())])
                    .unwrap()
                    .as_deref(),
                Some(primary_key(&winner).unwrap().as_slice())
            );
            // The loser's caller already arbitrated: entry suppressed,
            // the conflicted mark lands instead of a claim attempt.
            store
                .upsert(&loser, b"two", &[], &[id], "cursor-2")
                .unwrap();
            let row = store.get(&loser).unwrap().unwrap();
            assert_eq!(row.conflicted, vec![id]);
            // Strictness is intact for callers that skip arbitration.
            assert!(matches!(
                store.upsert(
                    &loser,
                    b"two",
                    std::slice::from_ref(&claim),
                    &[],
                    "cursor-3"
                ),
                Err(IndexStoreError::Key(IndexKeyError::UniqueConflict))
            ));
            // A winner re-write re-projects the entry and keeps its claim.
            store
                .upsert(
                    &winner,
                    b"one-v2",
                    std::slice::from_ref(&claim),
                    &[],
                    "cursor-4",
                )
                .unwrap();
        }
        // Restart: claim ownership and conflict marks persist.
        let store = FjallIndexStore::open(directory.path()).unwrap();
        assert_eq!(
            store
                .claim_owner(id, &[IndexValue::Text("dup".to_owned())])
                .unwrap()
                .as_deref(),
            Some(primary_key(&winner).unwrap().as_slice())
        );
        assert_eq!(store.get(&loser).unwrap().unwrap().conflicted, vec![id]);
        // Re-projecting the entry on a later cursor clears the mark.
        assert!(matches!(
            store.upsert(&loser, b"two-v2", &[claim], &[id], "cursor-5"),
            Err(IndexStoreError::Key(IndexKeyError::UniqueConflict))
        ));
        // (still conflicting — winner holds it — so suppression remains)
        assert_eq!(store.get(&loser).unwrap().unwrap().conflicted, vec![id]);
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
                    &[],
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
                &[],
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
                    &[],
                    "cursor-a",
                )
            });
            let second = threads.spawn(|| {
                store.upsert(
                    &reference(b"bob"),
                    b"second",
                    std::slice::from_ref(&unique),
                    &[],
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
                &[],
                "cursor-1",
            )
            .unwrap();
        assert!(matches!(
            store.upsert(
                &reference(b"alice"),
                b"changed-without-new-cursor",
                std::slice::from_ref(&london),
                &[],
                "cursor-1",
            ),
            Err(IndexStoreError::CursorConflict)
        ));
        assert!(store
            .upsert(
                &reference(b"bob"),
                b"conflict",
                std::slice::from_ref(&london),
                &[],
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
                &[],
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

    #[test]
    fn fjall_multi_entry_batch_commits_all_or_nothing() {
        let directory = tempfile::tempdir().unwrap();
        let store = FjallIndexStore::open(directory.path()).unwrap();
        let claimed = entry(IndexValue::Text("taken".to_owned()), true);
        store
            .upsert(
                &reference(b"owner"),
                b"holder",
                std::slice::from_ref(&claimed),
                &[],
                "cursor-1",
            )
            .unwrap();
        let city = IndexEntry {
            id: IndexId(Uuid::from_u128(9)),
            values: vec![IndexValue::Text("Lisbon".to_owned())],
            unique: false,
        };
        // A conflicting unique claim must roll back the whole batch: the
        // unrelated `city` posting, the primary row, and both Index
        // checkpoints must all remain absent.
        assert!(matches!(
            store.upsert(
                &reference(b"new"),
                b"data",
                &[city.clone(), claimed.clone()],
                &[],
                "cursor-2"
            ),
            Err(IndexStoreError::Key(IndexKeyError::UniqueConflict))
        ));
        assert!(store.get(&reference(b"new")).unwrap().is_none());
        assert!(store
            .lookup_exact(city.id, &city.values, 10)
            .unwrap()
            .is_empty());
        assert_eq!(store.last_applied(city.id).unwrap(), None);
        assert_eq!(
            store.last_applied(claimed.id).unwrap().as_deref(),
            Some("cursor-1")
        );
    }

    #[test]
    fn fjall_enforces_row_cursor_and_query_budgets() {
        let directory = tempfile::tempdir().unwrap();
        let store = FjallIndexStore::open(directory.path()).unwrap();
        let row = entry(IndexValue::Text("v".to_owned()), false);
        assert!(matches!(
            store.upsert(
                &reference(b"k"),
                &[0u8; 4 * 1024 * 1024 + 1],
                std::slice::from_ref(&row),
                &[],
                "c1"
            ),
            Err(IndexStoreError::RowTooLarge)
        ));
        assert!(matches!(
            store.upsert(
                &reference(b"k"),
                b"payload",
                &vec![row.clone(); 65],
                &[],
                "c1"
            ),
            Err(IndexStoreError::RowTooLarge)
        ));
        for cursor in ["", &"x".repeat(513)] {
            assert!(matches!(
                store.upsert(
                    &reference(b"k"),
                    b"p",
                    std::slice::from_ref(&row),
                    &[],
                    cursor
                ),
                Err(IndexStoreError::InvalidCursor)
            ));
            assert!(matches!(
                store.delete(&reference(b"k"), cursor),
                Err(IndexStoreError::InvalidCursor)
            ));
        }
        for limit in [0, 1_001] {
            assert!(matches!(
                store.lookup_exact(row.id, &row.values, limit),
                Err(IndexStoreError::InvalidLimit)
            ));
        }
    }

    #[test]
    fn fjall_duplicate_cursor_is_idempotent_only_for_identical_rows() {
        let directory = tempfile::tempdir().unwrap();
        let store = FjallIndexStore::open(directory.path()).unwrap();
        let row = entry(IndexValue::Text("v".to_owned()), false);
        store
            .upsert(
                &reference(b"k"),
                b"payload",
                std::slice::from_ref(&row),
                &[],
                "cursor-9",
            )
            .unwrap();
        // Replaying the same mutation is a harmless retry; reusing the Cursor
        // for different bytes is a conflict.
        store
            .upsert(
                &reference(b"k"),
                b"payload",
                std::slice::from_ref(&row),
                &[],
                "cursor-9",
            )
            .unwrap();
        assert!(matches!(
            store.upsert(&reference(b"k"), b"other", &[row], &[], "cursor-9"),
            Err(IndexStoreError::CursorConflict)
        ));
    }

    #[test]
    fn store_contract_is_engine_agnostic_through_the_trait() {
        let directory = tempfile::tempdir().unwrap();
        let store: Box<dyn IndexStore> = Box::new(FjallIndexStore::open(directory.path()).unwrap());
        let row = entry(IndexValue::Text("Porto".to_owned()), false);
        store
            .upsert(
                &reference(b"ana"),
                b"payload",
                std::slice::from_ref(&row),
                &[],
                "cursor-1",
            )
            .unwrap();
        assert_eq!(
            store.lookup_exact(row.id, &row.values, 10).unwrap()[0]
                .reference
                .application_key,
            b"ana"
        );
        assert_eq!(
            store.last_applied(row.id).unwrap().as_deref(),
            Some("cursor-1")
        );
        assert!(store.delete(&reference(b"ana"), "cursor-2").unwrap());
        assert!(store.get(&reference(b"ana")).unwrap().is_none());
    }

    fn multi_entry(id: Uuid, values: Vec<IndexValue>) -> IndexEntry {
        IndexEntry {
            id: IndexId(id),
            values,
            unique: false,
        }
    }

    #[test]
    fn fjall_lookup_prefix_matches_leading_values_and_partial_text() {
        let directory = tempfile::tempdir().unwrap();
        let store = FjallIndexStore::open(directory.path()).unwrap();
        let id = Uuid::from_u128(41);
        for (key, city, age) in [
            (b"alice".as_slice(), "London", 23_i64),
            (b"amy".as_slice(), "London", 31),
            (b"bob".as_slice(), "London", 40),
            (b"carol".as_slice(), "Los Angeles", 19),
        ] {
            store
                .upsert(
                    &reference(key),
                    key,
                    &[multi_entry(
                        id,
                        vec![IndexValue::Text(city.to_owned()), IndexValue::I64(age)],
                    )],
                    &[],
                    "cursor-1",
                )
                .unwrap();
        }
        let index_id = IndexId(id);
        // Leading-only prefix on the first field.
        let london = store
            .lookup_prefix(index_id, &[IndexValue::Text("London".to_owned())], None, 10)
            .unwrap();
        assert_eq!(london.len(), 3);
        // Partial text narrows the *next* field — so an I64 next field
        // simply finds nothing rather than erroring.
        let leading_city = [IndexValue::Text("L".to_owned())];
        let cities = store.lookup_prefix(index_id, &[], Some("Lo"), 10).unwrap();
        assert_eq!(cities.len(), 4);
        assert!(leading_city[0] == IndexValue::Text("L".to_owned()));
        let lo = store.lookup_prefix(index_id, &[], Some("Lon"), 10).unwrap();
        assert_eq!(lo.len(), 3);
        // Limits bound results deterministically (posting-key order).
        let capped = store
            .lookup_prefix(index_id, &[IndexValue::Text("London".to_owned())], None, 1)
            .unwrap();
        assert_eq!(capped.len(), 1);
    }

    #[test]
    fn fjall_lookup_range_bounds_the_next_field_with_typed_order() {
        let directory = tempfile::tempdir().unwrap();
        let store = FjallIndexStore::open(directory.path()).unwrap();
        let id = Uuid::from_u128(42);
        for (key, region, score) in [
            (b"a".as_slice(), "US", -5_i64),
            (b"b".as_slice(), "US", 0),
            (b"c".as_slice(), "US", 7),
            (b"d".as_slice(), "US", 42),
            (b"e".as_slice(), "EU", 10),
        ] {
            store
                .upsert(
                    &reference(key),
                    key,
                    &[multi_entry(
                        id,
                        vec![IndexValue::Text(region.to_owned()), IndexValue::I64(score)],
                    )],
                    &[],
                    "cursor-1",
                )
                .unwrap();
        }
        let index_id = IndexId(id);
        let us = [IndexValue::Text("US".to_owned())];
        // Inclusive [0, 7] keeps b, c and respects signed order (a=-5 < 0).
        let rows = store
            .lookup_range(
                index_id,
                &us,
                std::ops::Bound::Included(IndexValue::I64(0)),
                std::ops::Bound::Included(IndexValue::I64(7)),
                10,
            )
            .unwrap();
        assert_eq!(
            rows.iter()
                .map(|row| row.reference.application_key.as_slice())
                .collect::<Vec<_>>(),
            vec![b"b".as_slice(), b"c".as_slice()]
        );
        // Exclusive lower bound drops 0.
        let rows = store
            .lookup_range(
                index_id,
                &us,
                std::ops::Bound::Excluded(IndexValue::I64(0)),
                std::ops::Bound::Unbounded,
                10,
            )
            .unwrap();
        assert_eq!(
            rows.iter()
                .map(|row| row.reference.application_key.as_slice())
                .collect::<Vec<_>>(),
            vec![b"c".as_slice(), b"d".as_slice()]
        );
        // Range stops at the leading-prefix boundary: EU rows never appear.
        let rows = store
            .lookup_range(
                index_id,
                &us,
                std::ops::Bound::Unbounded,
                std::ops::Bound::Unbounded,
                10,
            )
            .unwrap();
        assert_eq!(rows.len(), 4);
        assert!(rows
            .iter()
            .all(|row| row.reference.application_key != b"e".to_vec()));
    }

    #[test]
    fn fjall_lookup_rejects_dangling_postings_and_bad_limits() {
        let directory = tempfile::tempdir().unwrap();
        let store = FjallIndexStore::open(directory.path()).unwrap();
        let id = Uuid::from_u128(43);
        let entry = multi_entry(id, vec![IndexValue::I64(9)]);
        // Plant a posting with no primary row — the corruption the
        // dereference check exists to catch.
        let orphan = posting_key(&entry, &reference(b"gone")).unwrap();
        store.entries.insert(orphan, "").unwrap();
        let error = store.lookup_prefix(IndexId(id), &[], None, 10).unwrap_err();
        assert!(matches!(error, IndexStoreError::DanglingPosting));
        assert!(matches!(
            store
                .lookup_range(
                    IndexId(id),
                    &[],
                    std::ops::Bound::Unbounded,
                    std::ops::Bound::Unbounded,
                    0,
                )
                .unwrap_err(),
            IndexStoreError::InvalidLimit
        ));
    }
    fn accumulate_row(key: &[u8], count: u64, sum: i64) -> AccumulateRowWrite {
        AccumulateRowWrite {
            reference: reference(key),
            count_delta: count,
            sum_delta: sum,
            entries: Vec::new(),
        }
    }

    #[test]
    fn accumulator_payload_round_trips_and_rejects_wrong_lengths() {
        let payload = encode_accumulator(7, -13);
        assert_eq!(decode_accumulator(&payload), Some((7, -13)));
        assert_eq!(decode_accumulator(&payload[..20]), None);
        assert_eq!(decode_accumulator(&[]), None);
    }

    #[test]
    fn accumulate_rows_merges_each_row_and_skips_replayed_cursors() {
        let directory = tempfile::tempdir().unwrap();
        let store = FjallIndexStore::open(directory.path()).unwrap();

        store
            .accumulate_rows(
                &[accumulate_row(b"a", 1, 10), accumulate_row(b"b", 2, -4)],
                "c1",
            )
            .unwrap();
        let a = store.get(&reference(b"a")).unwrap().unwrap();
        let b = store.get(&reference(b"b")).unwrap().unwrap();
        assert_eq!(decode_accumulator(&a.payload), Some((1, 10)));
        assert_eq!(decode_accumulator(&b.payload), Some((2, -4)));

        // Replaying the same cursor (post-commit crash window before the
        // mutation journal's applied sequence lands) is a no-op, never a
        // double count.
        store
            .accumulate_rows(
                &[accumulate_row(b"a", 1, 10), accumulate_row(b"b", 2, -4)],
                "c1",
            )
            .unwrap();
        let a = store.get(&reference(b"a")).unwrap().unwrap();
        assert_eq!(decode_accumulator(&a.payload), Some((1, 10)));

        // A later cursor merges again while a skipped row keeps its postings.
        store
            .accumulate_rows(&[accumulate_row(b"a", 1, 5)], "c2")
            .unwrap();
        let a = store.get(&reference(b"a")).unwrap().unwrap();
        assert_eq!(decode_accumulator(&a.payload), Some((2, 15)));
        assert_eq!(a.applied_cursor, "c2");
    }

    #[test]
    fn accumulate_rows_replaces_postings_and_drops_only_conflicted_claims() {
        let directory = tempfile::tempdir().unwrap();
        let store = FjallIndexStore::open(directory.path()).unwrap();
        let index = IndexId(Uuid::from_u128(9));

        let mut holder = accumulate_row(b"holder", 1, 1);
        holder.entries = vec![IndexEntry {
            id: index,
            values: vec![IndexValue::Text("taken".to_owned())],
            unique: true,
        }];
        store.accumulate_rows(&[holder], "c1").unwrap();

        let mut loser = accumulate_row(b"loser", 3, 30);
        loser.entries = vec![IndexEntry {
            id: index,
            values: vec![IndexValue::Text("taken".to_owned())],
            unique: true,
        }];
        let mut winner = accumulate_row(b"winner", 4, 40);
        winner.entries = vec![IndexEntry {
            id: index,
            values: vec![IndexValue::Text("free".to_owned())],
            unique: true,
        }];
        store.accumulate_rows(&[loser, winner], "c2").unwrap();

        // The accumulator merges even when the unique claim is refused; only
        // the postings are dropped, deterministically on every replica.
        let loser = store.get(&reference(b"loser")).unwrap().unwrap();
        assert_eq!(decode_accumulator(&loser.payload), Some((3, 30)));
        assert!(loser.entries.is_empty());
        assert!(store
            .lookup_exact(index, &[IndexValue::Text("taken".to_owned())], 10)
            .unwrap()
            .iter()
            .all(|row| row.reference == reference(b"holder")));
        assert_eq!(
            store
                .lookup_exact(index, &[IndexValue::Text("free".to_owned())], 10)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn accumulate_rows_refuses_unbounded_batches_and_cursors() {
        let directory = tempfile::tempdir().unwrap();
        let store = FjallIndexStore::open(directory.path()).unwrap();
        assert!(matches!(
            store.accumulate_rows(&[], "c1"),
            Err(IndexStoreError::RowTooLarge)
        ));
        assert!(matches!(
            store.accumulate_rows(&vec![accumulate_row(b"a", 1, 1); 65], "c1"),
            Err(IndexStoreError::RowTooLarge)
        ));
        assert!(matches!(
            store.accumulate_rows(&[accumulate_row(b"a", 1, 1)], ""),
            Err(IndexStoreError::InvalidCursor)
        ));
    }
}
