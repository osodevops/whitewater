//! The Pipe driver: turns a declared `PipeDefinition` into committed,
//! applied effects. Each cycle reads the consumed Subscription's committed
//! frontier, fetches a bounded page of input records, commits a `Declare`
//! carrying the frontier evidence and deterministic output identities, then
//! drives that declaration to `Applied`.
//!
//! Deterministic identities make a crashed or restarted driver safe: the
//! same frontier yields the same `effect_id`, `request_id`, and per-record
//! writer identities, so journal replays and Feed append dedupe collapse
//! repeats instead of duplicating work. The driver never claims atomicity
//! beyond what the journal's apply step provides.

use std::collections::BTreeMap;
use std::sync::Arc;

use uuid::Uuid;

use crate::active_range::RangeId;
use crate::control::{PipeDefinition, PipeOperation, WindowAggregation, WindowTime};
use crate::effect::{
    effect_state_mutation_id, EffectConsume, EffectCoordinator, EffectJournalError, EffectMutation,
    EffectOutput, EffectStateMutation, EffectTransition,
};
use crate::index::IndexValue;
use crate::index_build::extract_index_values;
use crate::reader::{SubscriptionProgressCoordinator, SubscriptionProgressMutation};
use crate::state_store::{
    AccumulateRowEntry, NamedKeyValue, ReplaySourceRecord, StateStoreMutationOp,
};

/// One input record fetched after the Subscription frontier.
#[derive(Clone, Debug)]
pub struct FetchedRecord {
    pub message_id: Uuid,
    pub key_base64: String,
    pub payload_base64: String,
    pub metadata_base64: BTreeMap<String, String>,
    pub event_time_ns: i64,
    pub ingest_time_ns: i64,
}

/// A bounded page fetched after a committed frontier: records in delivery
/// order plus the resulting frontier cursor and per-range positions.
#[derive(Clone, Debug)]
pub struct FetchedPage {
    pub records: Vec<FetchedRecord>,
    pub cursor: String,
    pub positions: BTreeMap<RangeId, String>,
}

/// Deterministic identity for the effect one Pipe cycle declares over a
/// given frontier: the same frontier re-driven after a crash produces the
/// same identity, and the journal replays it idempotently.
pub fn pipe_effect_id(pipe_id: Uuid, subscription_id: Uuid, expected_cursor: &str) -> Uuid {
    derived_uuid(
        b"pipe-effect",
        &[
            pipe_id.as_bytes().as_slice(),
            subscription_id.as_bytes().as_slice(),
            expected_cursor.as_bytes(),
        ],
    )
}

/// The per-record writer session a Pipe output uses; identical input records
/// map to identical writer identities so owner-side dedupe collapses replays.
pub fn pipe_output_writer_session(pipe_id: Uuid, message_id: Uuid) -> Uuid {
    derived_uuid(
        b"pipe-writer",
        &[
            pipe_id.as_bytes().as_slice(),
            message_id.as_bytes().as_slice(),
        ],
    )
}

/// The Declare request identity for one effect.
pub fn pipe_declare_request_id(effect_id: Uuid) -> Uuid {
    derived_uuid(b"pipe-declare", &[effect_id.as_bytes().as_slice()])
}

/// The frontier-bootstrap request identity for one Subscription: concurrent
/// drivers (or a racing member join) produce the same mutation, so the
/// sequence-1 frontier write replays idempotently instead of forking.
pub fn pipe_frontier_request_id(subscription_id: Uuid, feed_id: Uuid) -> Uuid {
    derived_uuid(
        b"pipe-frontier",
        &[
            subscription_id.as_bytes().as_slice(),
            feed_id.as_bytes().as_slice(),
        ],
    )
}

fn derived_uuid(tag: &[u8], parts: &[&[u8]]) -> Uuid {
    let mut material = tag.to_vec();
    for part in parts {
        material.extend_from_slice(&(part.len() as u64).to_le_bytes());
        material.extend_from_slice(part);
    }
    let digest = blake3::hash(&material);
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    Uuid::from_bytes(bytes)
}

/// Maps a progress-journal failure into the effect surface: transient
/// quorum/availability failures stay retryable, genuine fencing conflicts
/// surface as conflicts.
fn progress_apply_error(error: crate::reader::SubscriptionProgressError) -> EffectJournalError {
    match error {
        crate::reader::SubscriptionProgressError::Unavailable
        | crate::reader::SubscriptionProgressError::AmbiguousCommit
        | crate::reader::SubscriptionProgressError::NoQuorum
        | crate::reader::SubscriptionProgressError::Engine(_)
        | crate::reader::SubscriptionProgressError::Serialization(_) => {
            EffectJournalError::Unavailable
        }
        _ => EffectJournalError::Conflict,
    }
}

/// Drives one declared Pipe over a Subscription's progress assignment: the
/// same assignment fences both the consumed frontier and the effect journal.
pub struct PipeDriver {
    pipe: PipeDefinition,
    journal: Arc<EffectCoordinator>,
    progress: Arc<SubscriptionProgressCoordinator>,
}

impl PipeDriver {
    pub fn new(
        pipe: PipeDefinition,
        journal: Arc<EffectCoordinator>,
        progress: Arc<SubscriptionProgressCoordinator>,
    ) -> Self {
        Self {
            pipe,
            journal,
            progress,
        }
    }

    pub fn pipe(&self) -> &PipeDefinition {
        &self.pipe
    }

    /// Run one bounded cycle: read the committed frontier, fetch up to
    /// `limit` records, commit the Declare, then apply it. Returns the
    /// committed `Applied` mutation, or `None` when the input is caught up.
    ///
    /// The cycle is safe to call concurrently across Nodes and across
    /// retries: the second caller re-reads the moved frontier and either
    /// builds the next page or replays the identical journal mutation, while
    /// a stale `expected_cursor` fences a consumer that raced ahead.
    pub async fn drive_once<F, Fut, B, BFut, A>(
        &self,
        limit: usize,
        fetch: F,
        bootstrap: B,
        apply: &A,
    ) -> Result<Option<EffectMutation>, EffectJournalError>
    where
        F: FnOnce(&SubscriptionProgressMutation, usize) -> Fut,
        Fut: std::future::Future<Output = Result<FetchedPage, EffectJournalError>>,
        B: FnOnce() -> BFut,
        BFut:
            std::future::Future<Output = Result<SubscriptionProgressMutation, EffectJournalError>>,
        A: crate::effect::EffectApply + ?Sized,
    {
        let frontier = match self
            .progress
            .read_committed()
            .await
            .map_err(progress_apply_error)?
        {
            Some(frontier) => frontier,
            None => {
                // A Pipe consumes without a member join, so the driver
                // establishes the declared-start frontier itself. The caller
                // supplies the sequence-1 mutation with deterministic request
                // identity; a concurrent bootstrap or racing first join is a
                // safe replay or a fenced conflict, never a fork.
                let mutation = bootstrap().await?;
                match self.progress.apply(mutation.clone()).await {
                    Ok(_) => {}
                    Err(
                        crate::reader::SubscriptionProgressError::AmbiguousCommit
                        | crate::reader::SubscriptionProgressError::NoQuorum,
                    ) => {
                        self.progress
                            .reconcile_retry(mutation)
                            .await
                            .map_err(progress_apply_error)?;
                    }
                    // A conflicting bootstrap (different request shape, same
                    // sequence) loses to whoever committed first; the
                    // committed re-read below then proceeds with it.
                    Err(crate::reader::SubscriptionProgressError::Conflict) => {}
                    Err(error) => return Err(progress_apply_error(error)),
                }
                self.progress
                    .read_committed()
                    .await
                    .map_err(progress_apply_error)?
                    .ok_or(EffectJournalError::Unavailable)?
            }
        };
        let limit = limit.clamp(1, crate::effect::EFFECT_MAX_OUTPUTS);
        let page = fetch(&frontier, limit).await?;
        if page.records.is_empty() {
            return Ok(None);
        }
        if page.records.len() > crate::effect::EFFECT_MAX_OUTPUTS
            || page.positions.len() > crate::effect::EFFECT_MAX_POSITIONS
        {
            return Err(EffectJournalError::TooLarge);
        }
        if page.cursor.is_empty() || page.positions.is_empty() {
            return Err(EffectJournalError::Conflict);
        }
        let effect_id = pipe_effect_id(
            self.pipe.pipe_id,
            frontier.subscription_id,
            &frontier.cursor,
        );
        let (outputs, state_mutations) = self.effect_plan(effect_id, &page)?;
        let declare = EffectMutation {
            effect_id,
            subscription_id: frontier.subscription_id,
            ownership_epoch: frontier.ownership_epoch,
            sequence: 1,
            request_id: pipe_declare_request_id(effect_id),
            transition: EffectTransition::Declare {
                consume: Some(EffectConsume {
                    feed_id: frontier.feed_id,
                    expected_cursor: Some(frontier.cursor.clone()),
                    cursor: page.cursor,
                    positions: page.positions,
                }),
                outputs,
                state_mutations,
            },
        };
        match self.journal.apply(declare.clone()).await {
            Ok(_) => {}
            Err(EffectJournalError::AmbiguousCommit | EffectJournalError::NoQuorum) => {
                if self.journal.read_committed(effect_id).await?.is_none() {
                    // Nothing committed: the Declare may be half-prepared, so
                    // replay it to settle the quorum before applying.
                    self.journal.reconcile_retry(declare).await?;
                }
            }
            Err(error) => return Err(error),
        }
        // The apply itself settles transient and ambiguous results in place
        // under the same committed Declare; one bounded re-drive covers the
        // failure classes that legitimately escape it (for example a sweep
        // waiting on an occupant whose own apply is mid-flight). A re-drive
        // replays the identical effect identity, so repeating is safe.
        let mut attempts = 0;
        loop {
            match self.journal.apply_committed(effect_id, apply).await {
                Err(error) if crate::effect::effect_retryable(&error) && attempts < 1 => {
                    attempts += 1;
                }
                result => return result.map(Some),
            }
        }
    }

    /// Plan one cycle's effect payload. `Forward` maps each record 1:1 to a
    /// deterministic output append; `Window` folds the page into a single
    /// `AccumulateRows` mutation so every touched pane advances exactly
    /// once inside the same atomic boundary as the consumed frontier.
    fn effect_plan(
        &self,
        effect_id: Uuid,
        page: &FetchedPage,
    ) -> Result<(Vec<EffectOutput>, Vec<EffectStateMutation>), EffectJournalError> {
        match &self.pipe.operation {
            PipeOperation::Forward => {
                let feed_id = self
                    .pipe
                    .output_feed_id
                    .ok_or(EffectJournalError::Conflict)?;
                Ok((
                    page.records
                        .iter()
                        .map(|record| EffectOutput {
                            feed_id,
                            key_base64: record.key_base64.clone(),
                            payload_base64: record.payload_base64.clone(),
                            metadata_base64: record.metadata_base64.clone(),
                            event_time_ns: record.event_time_ns,
                            writer_session_id: pipe_output_writer_session(
                                self.pipe.pipe_id,
                                record.message_id,
                            ),
                            writer_epoch: 1,
                            sequence: 1,
                        })
                        .collect(),
                    Vec::new(),
                ))
            }
            PipeOperation::Window {
                window_ns,
                key,
                aggregation,
                time,
            } => {
                let store_id = self
                    .pipe
                    .output_store_id
                    .ok_or(EffectJournalError::Conflict)?;
                let pane_ns = window_pane_ns(*window_ns);
                // (group key bytes, pane index) -> (count, sum, typed key).
                // BTreeMap order makes the emitted mutation byte-identical
                // across replays and replicas — mutation-equality dedupe
                // depends on it.
                let mut panes: BTreeMap<(Vec<u8>, u64), (u64, i64, IndexValue)> = BTreeMap::new();
                for record in &page.records {
                    let Some(source) = source_record(record) else {
                        continue;
                    };
                    let Ok(extracted) = extract_index_values(std::slice::from_ref(key), &source)
                    else {
                        // An unprojectable record contributes nothing rather
                        // than wedging the Pipe; late/malformed-event policy
                        // (quarantine, revisions) is follow-on work.
                        continue;
                    };
                    let Some(group) = extracted
                        .into_iter()
                        .next()
                        .and_then(|value| window_group_key_bytes(&value).map(|key| (key, value)))
                    else {
                        continue;
                    };
                    let sum_delta = match aggregation {
                        WindowAggregation::Count => 0i64,
                        WindowAggregation::Sum { value } | WindowAggregation::Average { value } => {
                            let Ok(extracted) =
                                extract_index_values(std::slice::from_ref(value), &source)
                            else {
                                continue;
                            };
                            match extracted.into_iter().next() {
                                Some(IndexValue::I64(value)) => value,
                                // Non-numeric contributions are skipped
                                // whole so an Average never dilutes.
                                _ => continue,
                            }
                        }
                    };
                    let instant = match time {
                        WindowTime::Event => record.event_time_ns,
                        WindowTime::Ingest => record.ingest_time_ns,
                    };
                    let pane = u64::try_from(instant.max(0)).unwrap_or(0) / pane_ns;
                    let entry = panes.entry((group.0, pane)).or_insert((0, 0, group.1));
                    entry.0 = entry.0.saturating_add(1);
                    entry.1 = entry.1.saturating_add(sum_delta);
                }
                if panes.is_empty() {
                    return Ok((Vec::new(), Vec::new()));
                }
                let entries: Vec<AccumulateRowEntry> = panes
                    .into_iter()
                    .map(
                        |((group, pane), (count, sum, group_value))| AccumulateRowEntry {
                            key: window_pane_row_key(&group, pane),
                            count_delta: count,
                            sum_delta: sum,
                            named: vec![
                                NamedKeyValue {
                                    name: WINDOW_KEY_NAME.to_owned(),
                                    values: vec![group_value],
                                    unique: false,
                                },
                                NamedKeyValue {
                                    name: WINDOW_PANE_NAME.to_owned(),
                                    values: vec![IndexValue::I64(pane as i64)],
                                    unique: false,
                                },
                            ],
                        },
                    )
                    .collect();
                Ok((
                    Vec::new(),
                    vec![EffectStateMutation {
                        store_id,
                        mutation_id: effect_state_mutation_id(effect_id, 0),
                        op: StateStoreMutationOp::AccumulateRows { entries },
                    }],
                ))
            }
        }
    }
}

/// A window shards into 16 equal accumulation panes: coarse enough that one
/// page touches few rows, fine enough that pane rows stay small.
pub const WINDOW_PANE_COUNT: u64 = 16;
/// Named-key postings every window pane row carries so the managed view is
/// enumerable by group key or pane index without knowing composite row keys.
pub const WINDOW_KEY_NAME: &str = "key";
pub const WINDOW_PANE_NAME: &str = "pane";

/// Pane length in nanoseconds for a window; never zero even for sub-pane
/// windows.
fn window_pane_ns(window_ns: u64) -> u64 {
    (window_ns / WINDOW_PANE_COUNT).max(1)
}

/// The StateStore row key for one (group, pane): length-prefixed so binary
/// group bytes stay unambiguous, big-endian pane so row keys order by time.
fn window_pane_row_key(group: &[u8], pane: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(4 + group.len() + 8);
    key.extend_from_slice(&(group.len() as u32).to_be_bytes());
    key.extend_from_slice(group);
    key.extend_from_slice(&pane.to_be_bytes());
    key
}

/// Tag-prefixed group bytes keep typed values distinct (Text "1" never
/// merges with I64 1); `Null` keys group nothing.
fn window_group_key_bytes(value: &IndexValue) -> Option<Vec<u8>> {
    let (tag, bytes): (u8, Vec<u8>) = match value {
        IndexValue::Null => return None,
        IndexValue::Bool(value) => (0, vec![u8::from(*value)]),
        IndexValue::I64(value) => (1, value.to_be_bytes().to_vec()),
        IndexValue::Text(value) => (2, value.as_bytes().to_vec()),
        IndexValue::Bytes(value) => (3, value.clone()),
    };
    let mut key = Vec::with_capacity(1 + bytes.len());
    key.push(tag);
    key.extend_from_slice(&bytes);
    Some(key)
}

/// Decode a fetched record into the replay-record shape extraction expects.
/// Corrupt wire fields contribute nothing rather than failing the cycle.
fn source_record(record: &FetchedRecord) -> Option<ReplaySourceRecord> {
    use base64::Engine;
    let decode = |value: &str| base64::engine::general_purpose::STANDARD.decode(value).ok();
    Some(ReplaySourceRecord {
        message_id: record.message_id,
        cursor: String::new(),
        key: decode(&record.key_base64)?,
        metadata: record
            .metadata_base64
            .iter()
            .map(|(name, value)| Some((name.clone(), decode(value)?)))
            .collect::<Option<BTreeMap<_, _>>>()?,
        payload: decode(&record.payload_base64)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::{PipeOperation, PipeStage, ResourceStatus};

    #[test]
    fn derived_identities_are_stable_and_scoped() {
        let pipe = Uuid::from_u128(1);
        let subscription = Uuid::from_u128(2);
        let first = pipe_effect_id(pipe, subscription, "cursor-a");
        assert_eq!(first, pipe_effect_id(pipe, subscription, "cursor-a"));
        assert_ne!(first, pipe_effect_id(pipe, subscription, "cursor-b"));
        assert_ne!(
            first,
            pipe_effect_id(Uuid::from_u128(9), subscription, "cursor-a")
        );
        let session = pipe_output_writer_session(pipe, Uuid::from_u128(50));
        assert_eq!(
            session,
            pipe_output_writer_session(pipe, Uuid::from_u128(50))
        );
        assert_ne!(
            session,
            pipe_output_writer_session(pipe, Uuid::from_u128(51))
        );
        assert_ne!(
            session,
            pipe_output_writer_session(Uuid::from_u128(9), Uuid::from_u128(50))
        );
        assert_eq!(
            pipe_declare_request_id(first),
            pipe_declare_request_id(first)
        );
    }

    #[test]
    fn forward_pipe_definition_carries_driver_inputs() {
        // The declared shape is all drive_once needs: consuming Subscription,
        // output Feed, and an operation that maps records 1:1.
        let pipe = PipeDefinition {
            pipe_id: Uuid::from_u128(7),
            name: "orders.forward".to_owned(),
            space_id: Uuid::from_u128(8),
            subscription_id: Uuid::from_u128(9),
            operation: PipeOperation::Forward,
            output_feed_id: Some(Uuid::from_u128(10)),
            output_store_id: None,
            stage: PipeStage::Declared,
            status: ResourceStatus::Active,
            created_at_ns: 0,
        };
        assert_eq!(pipe.operation, PipeOperation::Forward);
    }
}
