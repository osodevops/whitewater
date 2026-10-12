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

use serde::{Deserialize, Serialize};
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
    /// Enrich Pipes only: the lookup outcome resolved at fetch time. The
    /// resolution travels inside the Declare so an applied output never
    /// re-resolves; concurrent drives converge on the committed Declare.
    pub resolution: Option<EnrichResolution>,
}

/// The StateStore lookup outcome for one `Enrich` record: `matched` plus
/// the resolved row's payload (empty on a miss). Deterministic once
/// fetched — it is journaled inside the effect's outputs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnrichResolution {
    pub matched: bool,
    #[serde(default)]
    pub payload: Vec<u8>,
    /// The serving replica's applied journal sequence at read time — the
    /// versioned evidence behind `enrich.seq`, so an operator can answer
    /// "which version of the row enriched this record".
    #[serde(default)]
    pub applied_seq: u64,
}

/// A bounded page fetched after a committed frontier: records in delivery
/// order plus the resulting frontier cursor and per-range positions.
#[derive(Clone, Debug)]
pub struct FetchedPage {
    pub records: Vec<FetchedRecord>,
    pub cursor: String,
    pub positions: BTreeMap<RangeId, String>,
    /// Window Pipes only: closed panes resolved at fetch time. Emission
    /// travels inside the same Declare/Applied boundary as the pane
    /// accumulation, so a replayed effect never re-scans the store.
    pub emissions: Vec<WindowEmission>,
}

/// One closed window pane resolved for emission: the StateStore pane row
/// to delete and its decoded accumulator. Deterministic once fetched —
/// emitted outputs journal inside the effect's Declare.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WindowEmission {
    /// Full pane row key (`len|group|pane`) — the `DeleteRow` target.
    pub row_key: Vec<u8>,
    /// The group's typed value (for the result payload).
    pub group: IndexValue,
    /// Tag-prefixed group bytes — also the emitted record's key.
    pub group_tagged: Vec<u8>,
    /// Pane index; `pane_end = (pane + 1) * pane_ns` is the close bound.
    pub pane: u64,
    /// Decoded accumulator count for the pane.
    pub count: u64,
    /// Decoded accumulator sum for the pane.
    pub sum: i128,
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

/// The writer identity for one emitted pane: keyed by the pane row so a
/// retried append dedupes on the emission's own identity rather than
/// sharing the effect's session.
pub fn pipe_emission_writer_session(pipe_id: Uuid, pane_row_key: &[u8]) -> Uuid {
    derived_uuid(
        b"pipe-emission",
        &[pipe_id.as_bytes().as_slice(), pane_row_key],
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
            Err(EffectJournalError::Conflict) => {
                // Another drive committed a divergent Declare under this
                // effect identity first (Enrich resolutions may legitimately
                // differ between raced plans). The committed row is the
                // single source of truth — adopt it and apply that, so
                // exactly one Declare ever reaches the frontier CAS.
                if self.journal.read_committed(effect_id).await?.is_none() {
                    return Err(EffectJournalError::Conflict);
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
                max_late_ns,
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
                // Bounded lateness policy (event clock only): records
                // arriving more than `max_late_ns` behind their own event
                // time quarantine into a countable per-group `late` row
                // rather than revising closed panes or vanishing.
                let mut late: BTreeMap<Vec<u8>, (u64, IndexValue)> = BTreeMap::new();
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
                    let lateness = record.ingest_time_ns.saturating_sub(record.event_time_ns);
                    if matches!(time, WindowTime::Event)
                        && max_late_ns
                            .map(|bound| lateness > bound as i64)
                            .unwrap_or(false)
                    {
                        let entry = late.entry(group.0).or_insert((0, group.1));
                        entry.0 = entry.0.saturating_add(1);
                        continue;
                    }
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
                // Closed-pane emissions resolved at fetch time emit even
                // when this page accumulated nothing (all-late or empty
                // groups still release their aged panes).
                if panes.is_empty() && late.is_empty() && page.emissions.is_empty() {
                    return Ok((Vec::new(), Vec::new()));
                }
                let mut entries: Vec<AccumulateRowEntry> = panes
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
                // Quarantined late events accumulate a count under the
                // sentinel pane `u64::MAX` (posting value -1) — enumerable
                // per group without revising real pane rows.
                entries.extend(late.into_iter().map(|(group, (count, group_value))| {
                    AccumulateRowEntry {
                        key: window_pane_row_key(&group, WINDOW_LATE_PANE),
                        count_delta: count,
                        sum_delta: 0,
                        named: vec![
                            NamedKeyValue {
                                name: WINDOW_KEY_NAME.to_owned(),
                                values: vec![group_value],
                                unique: false,
                            },
                            NamedKeyValue {
                                name: WINDOW_PANE_NAME.to_owned(),
                                values: vec![IndexValue::I64(-1)],
                                unique: false,
                            },
                        ],
                    }
                }));
                let mut outputs = Vec::new();
                let mut mutations = Vec::new();
                if !entries.is_empty() {
                    mutations.push(EffectStateMutation {
                        store_id,
                        mutation_id: effect_state_mutation_id(effect_id, 0),
                        op: StateStoreMutationOp::AccumulateRows { entries },
                    });
                }
                // Emissions without a declared emit Feed fail closed —
                // silently dropping closed panes would wedge the store.
                if !page.emissions.is_empty() && self.pipe.output_feed_id.is_none() {
                    return Err(EffectJournalError::Conflict);
                }
                if let Some(feed_id) = self.pipe.output_feed_id {
                    // Row-key order is byte-identical across replays —
                    // mutation and writer identities depend on it.
                    let mut emissions = page.emissions.clone();
                    emissions.sort_by(|a, b| a.row_key.cmp(&b.row_key));
                    use base64::Engine;
                    for emission in emissions.iter().take(WINDOW_MAX_EMISSIONS_PER_DRIVE) {
                        let mut metadata = BTreeMap::new();
                        metadata.insert(
                            "window.emitted".to_owned(),
                            base64::engine::general_purpose::STANDARD.encode(b"true"),
                        );
                        outputs.push(EffectOutput {
                            feed_id,
                            key_base64: base64::engine::general_purpose::STANDARD
                                .encode(&emission.group_tagged),
                            payload_base64: base64::engine::general_purpose::STANDARD
                                .encode(window_emission_payload(emission, pane_ns, aggregation)),
                            metadata_base64: metadata,
                            event_time_ns: emission
                                .pane
                                .saturating_add(1)
                                .saturating_mul(pane_ns)
                                .saturating_sub(1)
                                .min(i64::MAX as u64)
                                as i64,
                            writer_session_id: pipe_emission_writer_session(
                                self.pipe.pipe_id,
                                &emission.row_key,
                            ),
                            writer_epoch: 1,
                            sequence: 1,
                        });
                        mutations.push(EffectStateMutation {
                            store_id,
                            mutation_id: effect_state_mutation_id(effect_id, mutations.len()),
                            op: StateStoreMutationOp::DeleteRow {
                                key: emission.row_key.clone(),
                            },
                        });
                    }
                }
                Ok((outputs, mutations))
            }
            PipeOperation::Enrich {
                on_miss,
                quarantine_feed_id,
                ..
            } => {
                let feed_id = self
                    .pipe
                    .output_feed_id
                    .ok_or(EffectJournalError::Conflict)?;
                // A quarantine declaration without a resolved Feed fails
                // closed — silently dropping or forwarding would corrupt the
                // declared miss contract.
                let quarantine_feed_id =
                    if matches!(on_miss, crate::control::EnrichMiss::Quarantine) {
                        Some(quarantine_feed_id.ok_or(EffectJournalError::Conflict)?)
                    } else {
                        None
                    };
                // Resolution was resolved at fetch time and rides inside the
                // Declare: outputs are the single committed source of truth,
                // so replays re-append identical bytes rather than re-read a
                // moved row. `enrich.*` Metadata keeps payloads untouched.
                Ok((
                    page.records
                        .iter()
                        .filter_map(|record| {
                            let mut metadata = record.metadata_base64.clone();
                            let resolution =
                                record.resolution.clone().unwrap_or(EnrichResolution {
                                    matched: false,
                                    payload: Vec::new(),
                                    applied_seq: 0,
                                });
                            // A declared drop policy consumes a miss without
                            // emitting — the frontier commits regardless, so
                            // the decision is durable and replay-identical.
                            if matches!(on_miss, crate::control::EnrichMiss::Drop)
                                && !resolution.matched
                            {
                                return None;
                            }
                            // A declared quarantine routes the miss to its
                            // own Feed.
                            let output_feed_id = match quarantine_feed_id {
                                Some(feed) if !resolution.matched => {
                                    metadata.insert(
                                        "enrich.quarantine".to_owned(),
                                        "dHJ1ZQ==".to_owned(), // "true"
                                    );
                                    feed
                                }
                                _ => feed_id,
                            };
                            metadata.insert(
                                "enrich.matched".to_owned(),
                                if resolution.matched {
                                    "dHJ1ZQ==".to_owned() // "true"
                                } else {
                                    "ZmFsc2U=".to_owned() // "false"
                                },
                            );
                            {
                                use base64::Engine;
                                metadata.insert(
                                    "enrich.seq".to_owned(),
                                    base64::engine::general_purpose::STANDARD
                                        .encode(resolution.applied_seq.to_string()),
                                );
                            }
                            if !resolution.payload.is_empty() {
                                use base64::Engine;
                                metadata.insert(
                                    "enrich.payload".to_owned(),
                                    base64::engine::general_purpose::STANDARD
                                        .encode(&resolution.payload),
                                );
                            }
                            Some(EffectOutput {
                                feed_id: output_feed_id,
                                key_base64: record.key_base64.clone(),
                                payload_base64: record.payload_base64.clone(),
                                metadata_base64: metadata,
                                event_time_ns: record.event_time_ns,
                                writer_session_id: pipe_output_writer_session(
                                    self.pipe.pipe_id,
                                    record.message_id,
                                ),
                                writer_epoch: 1,
                                sequence: 1,
                            })
                        })
                        .collect(),
                    Vec::new(),
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
/// Sentinel pane index for quarantined late events: sorts after every
/// real pane so row scans find it last, and it can never collide with a
/// genuine pane (only 16 exist).
pub const WINDOW_LATE_PANE: u64 = u64::MAX;

/// Closed-pane emissions ride the same effect mutation budget as the pane
/// accumulation itself: `EFFECT_MAX_STATE_MUTATIONS` minus the one
/// `AccumulateRows` leaves this many `DeleteRow`s per drive. A backlog
/// beyond the bound simply emits across subsequent drives.
pub const WINDOW_MAX_EMISSIONS_PER_DRIVE: usize = 15;

/// Pane length in nanoseconds for a window; never zero even for sub-pane
/// windows.
pub fn window_pane_ns(window_ns: u64) -> u64 {
    (window_ns / WINDOW_PANE_COUNT).max(1)
}

/// The StateStore row key for one (group, pane): length-prefixed so binary
/// group bytes stay unambiguous, big-endian pane so row keys order by time.
pub fn window_pane_row_key(group: &[u8], pane: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(4 + group.len() + 8);
    key.extend_from_slice(&(group.len() as u32).to_be_bytes());
    key.extend_from_slice(group);
    key.extend_from_slice(&pane.to_be_bytes());
    key
}

/// Inverse of `window_pane_row_key`: `(tagged group bytes, pane index)`.
/// `None` for a key that is not a well-formed pane row — a foreign row
/// carrying a `key` posting must never be deleted as a pane.
pub fn window_pane_row_key_parse(key: &[u8]) -> Option<(Vec<u8>, u64)> {
    let (head, pane_bytes) = key.split_at_checked(key.len().checked_sub(8)?)?;
    let pane = u64::from_be_bytes(pane_bytes.try_into().ok()?);
    let group_len = u32::from_be_bytes(head.get(..4)?.try_into().ok()?) as usize;
    let group = head.get(4..)?.to_vec();
    if group.len() != group_len || group.is_empty() {
        return None;
    }
    Some((group, pane))
}

/// Decide whether one `key`-posting row is a closed pane worth emitting:
/// well-formed pane row, belongs to `group_tagged`, real pane (never the
/// late sentinel), accumulator payload decodes, and the ingest clock has
/// passed `pane_end + max_late_ns`. Anything else is left alone.
pub fn window_emission_from_row(
    row_key: &[u8],
    payload: &[u8],
    group_tagged: &[u8],
    group: &IndexValue,
    pane_ns: u64,
    max_late_ns: u64,
    now_ns: u64,
) -> Option<WindowEmission> {
    let (row_group, pane) = window_pane_row_key_parse(row_key)?;
    if row_group != group_tagged || pane == WINDOW_LATE_PANE {
        return None;
    }
    let (count, sum) = crate::index::decode_accumulator(payload)?;
    let pane_end = pane.checked_add(1)?.checked_mul(pane_ns)?;
    if now_ns < pane_end.checked_add(max_late_ns)? {
        return None;
    }
    Some(WindowEmission {
        row_key: row_key.to_vec(),
        group: group.clone(),
        group_tagged: row_group,
        pane,
        count,
        sum,
    })
}

/// The emitted window result record: one JSON document per closed pane.
/// Timestamps carry as decimal strings (nanosecond precision survives
/// JavaScript), `sum` as a decimal string for i128 headroom, `average`
/// only for the Average aggregation.
fn window_emission_payload(
    emission: &WindowEmission,
    pane_ns: u64,
    aggregation: &WindowAggregation,
) -> Vec<u8> {
    let group = match &emission.group {
        IndexValue::Null => serde_json::Value::Null,
        IndexValue::Bool(value) => serde_json::Value::from(*value),
        IndexValue::I64(value) => serde_json::Value::from(*value),
        IndexValue::Text(value) => serde_json::Value::from(value.as_str()),
        IndexValue::Bytes(value) => {
            use base64::Engine;
            serde_json::Value::from(base64::engine::general_purpose::STANDARD.encode(value))
        }
    };
    let pane_start = emission.pane.saturating_mul(pane_ns);
    let pane_end = pane_start.saturating_add(pane_ns);
    let mut body = serde_json::json!({
        "group": group,
        "pane": emission.pane,
        "pane_start_ns": pane_start.to_string(),
        "pane_end_ns": pane_end.to_string(),
        "count": emission.count,
        "sum": serde_json::Value::Null,
        "average": serde_json::Value::Null,
    });
    match aggregation {
        WindowAggregation::Count => {}
        WindowAggregation::Sum { .. } => {
            body["sum"] = serde_json::Value::from(emission.sum.to_string());
        }
        WindowAggregation::Average { .. } => {
            body["sum"] = serde_json::Value::from(emission.sum.to_string());
            body["average"] = serde_json::Value::from(
                (emission.sum as f64 / emission.count.max(1) as f64).to_string(),
            );
        }
    }
    body.to_string().into_bytes()
}

/// Tag-prefixed group bytes keep typed values distinct (Text "1" never
/// merges with I64 1); `Null` keys group nothing.
pub fn window_group_key_bytes(value: &IndexValue) -> Option<Vec<u8>> {
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

/// Primary-key bytes for a point lookup: the byte contract an application
/// writes `PutRow` rows under — raw bytes, UTF-8 text, decimal integers,
/// and "true"/"false" booleans. `Null` resolves nothing: the record still
/// forwards, just unmatched.
pub fn enrich_lookup_key_bytes(value: &IndexValue) -> Option<Vec<u8>> {
    match value {
        IndexValue::Null => None,
        IndexValue::Bytes(value) => Some(value.clone()),
        IndexValue::Text(value) => Some(value.as_bytes().to_vec()),
        IndexValue::I64(value) => Some(value.to_string().into_bytes()),
        IndexValue::Bool(value) => Some(if *value {
            b"true".to_vec()
        } else {
            b"false".to_vec()
        }),
    }
}

/// Decode a fetched record into the replay-record shape extraction expects.
/// Corrupt wire fields contribute nothing rather than failing the cycle.
pub fn source_record(record: &FetchedRecord) -> Option<ReplaySourceRecord> {
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
    fn enrich_lookup_key_bytes_follow_the_putrow_byte_contract() {
        assert_eq!(
            enrich_lookup_key_bytes(&IndexValue::Bytes(b"raw".to_vec())),
            Some(b"raw".to_vec())
        );
        assert_eq!(
            enrich_lookup_key_bytes(&IndexValue::Text("txt".to_owned())),
            Some(b"txt".to_vec())
        );
        assert_eq!(
            enrich_lookup_key_bytes(&IndexValue::I64(42)),
            Some(b"42".to_vec())
        );
        assert_eq!(
            enrich_lookup_key_bytes(&IndexValue::Bool(true)),
            Some(b"true".to_vec())
        );
        assert_eq!(enrich_lookup_key_bytes(&IndexValue::Null), None);
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

    #[test]
    fn window_pane_row_key_round_trips_and_rejects_malformed_keys() {
        let group = window_group_key_bytes(&IndexValue::Text("acct".to_owned())).unwrap();
        let key = window_pane_row_key(&group, 42);
        assert_eq!(window_pane_row_key_parse(&key), Some((group.clone(), 42)));
        // Truncations and length lies are foreign rows, never panes.
        assert_eq!(window_pane_row_key_parse(&key[..key.len() - 1]), None);
        assert_eq!(window_pane_row_key_parse(&key[..8]), None);
        let mut lying = key.clone();
        lying[3] = lying[3].wrapping_add(1);
        assert_eq!(window_pane_row_key_parse(&lying), None);
        assert_eq!(window_pane_row_key_parse(&[]), None);
    }

    #[test]
    fn window_emission_from_row_emits_only_closed_real_panes() {
        let group_value = IndexValue::Text("acct".to_owned());
        let group = window_group_key_bytes(&group_value).unwrap();
        let pane_ns = 10u64;
        let payload = crate::index::encode_accumulator(3, 42);
        // pane 5 closes at ingest-time (5+1)*10 + 20 = 80.
        let key = window_pane_row_key(&group, 5);
        let emission =
            window_emission_from_row(&key, &payload, &group, &group_value, pane_ns, 20, 80)
                .expect("closed pane emits");
        assert_eq!(emission.pane, 5);
        assert_eq!(emission.count, 3);
        assert_eq!(emission.sum, 42);
        assert_eq!(emission.row_key, key);
        // One tick earlier the pane is still open.
        assert!(
            window_emission_from_row(&key, &payload, &group, &group_value, pane_ns, 20, 79)
                .is_none()
        );
        // The late-sentinel pane never emits.
        let late_key = window_pane_row_key(&group, WINDOW_LATE_PANE);
        assert!(window_emission_from_row(
            &late_key,
            &payload,
            &group,
            &group_value,
            pane_ns,
            20,
            u64::MAX
        )
        .is_none());
        // A foreign group's row is untouched.
        let other = window_group_key_bytes(&IndexValue::Text("other".to_owned())).unwrap();
        assert!(
            window_emission_from_row(&key, &payload, &other, &group_value, pane_ns, 20, 80)
                .is_none()
        );
        // Malformed payloads and keys never delete anything.
        assert!(window_emission_from_row(
            &key,
            b"not-an-accumulator",
            &group,
            &group_value,
            pane_ns,
            20,
            80
        )
        .is_none());
        assert!(
            window_emission_from_row(b"bad", &payload, &group, &group_value, pane_ns, 20, 80)
                .is_none()
        );
    }

    #[test]
    fn window_emission_payload_carries_the_result_document() {
        let group = window_group_key_bytes(&IndexValue::Text("acct".to_owned())).unwrap();
        let emission = WindowEmission {
            row_key: window_pane_row_key(&group, 7),
            group: IndexValue::Text("acct".to_owned()),
            group_tagged: group,
            pane: 7,
            count: 4,
            sum: 30,
        };
        let payload = window_emission_payload(
            &emission,
            10,
            &WindowAggregation::Average {
                value: crate::control::IndexFieldSpec::Key,
            },
        );
        let body: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(body["group"], "acct");
        assert_eq!(body["pane"], 7);
        assert_eq!(body["pane_start_ns"], "70");
        assert_eq!(body["pane_end_ns"], "80");
        assert_eq!(body["count"], 4);
        assert_eq!(body["sum"], "30");
        assert_eq!(body["average"], "7.5");
        // Count aggregations carry no sum/average fields.
        let payload = window_emission_payload(&emission, 10, &WindowAggregation::Count);
        let body: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(body["sum"], serde_json::Value::Null);
        assert_eq!(body["average"], serde_json::Value::Null);
    }
}
