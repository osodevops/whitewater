use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use thiserror::Error;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::{
    active_range::{
        ActiveRangeAssignment, CommitPosition, KeyToken, OwnershipEpoch, RangeGeneration, RangeId,
        RangeMap, RangeRoute, ReplicaSet, StagedWriterSequence, StorageNodeId,
        ACTIVE_RANGE_REPLICA_COUNT,
    },
    index::IndexValue,
    node::{NodeCapabilities, NodeCapacity},
    reader::SubscriptionProgressAssignment,
    storage::{LogStore, StorageError},
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ResourceStatus {
    Active,
    Dropped,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SpaceDefinition {
    pub space_id: Uuid,
    pub name: String,
    pub status: ResourceStatus,
    pub created_at_ns: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FeedDefinition {
    pub feed_id: Uuid,
    pub name: String,
    pub space_id: Uuid,
    pub storage_name: String,
    pub status: ResourceStatus,
    pub created_at_ns: i64,
    /// Declared at creation (`CREATE FEED <name> ENCRYPTED`): sealed
    /// segments encrypt under the Feed's data-encryption key so all
    /// three replicas store byte-identical ciphertext and `ERASE FEED`
    /// can cryptographically shred retained history by destroying the
    /// key. Binding encryption to creation keeps the seal decision
    /// deterministic — no mid-history flag flip can split replicas
    /// across plaintext/ciphertext boundaries.
    #[serde(default)]
    pub encrypted: bool,
    /// Set by `ERASE FEED`: the Feed is tombstoned and every range's
    /// stored artifacts are purged. Distinct from a plain DROP so
    /// operators can audit the difference between "definition retired,
    /// history retained" and "history destroyed".
    #[serde(default)]
    pub erased_at_ns: Option<i64>,
}

/// A Feed's data-encryption key (DEK). Replicated through the catalog
/// like every other declaration so all replicas seal identical
/// ciphertext. PROTOTYPE LIMITATION: key material lives in catalog
/// state — every Control Plane voter holds the DEKs, which is
/// consistent with the current development-transport posture but is
/// NOT a production key-management story. The real seam (KMS/HSM
/// envelope wrapping, rotation, and Node-scoped unwrap) is tracked
/// under the M9 security work; until then `ERASE FEED` destroys the
/// replicated key material and every copy derived from it.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FeedKey {
    pub feed_id: Uuid,
    /// 32-byte DEK as lowercase hex; cleared when the key is destroyed
    /// so no catalog copy retains recoverable material.
    #[serde(default)]
    pub dek_hex: String,
    pub status: FeedKeyStatus,
    pub created_at_ns: i64,
    #[serde(default)]
    pub destroyed_at_ns: Option<i64>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FeedKeyStatus {
    Active,
    /// The DEK was destroyed by `ERASE FEED` — every ciphertext sealed
    /// under it is permanently unrecoverable even if bytes survive
    /// somewhere (object store, backups, drained caches).
    Destroyed,
}

/// The key state a storage Node resolves for one Feed when opening
/// ranges or healing manifests — distinct from "no key row exists"
/// because a Destroyed row is positive evidence of erasure, not a
/// missing declaration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FeedKeyState {
    /// Sealed segments encrypt on seal; reads decrypt through it.
    Active([u8; 32]),
    /// The key is gone; ciphertext is unrecoverable. Reads fail as
    /// erased, never as corruption.
    Destroyed,
}

/// Declared retained-history policy for a Domain or Feed: how long a
/// sealed segment stays local before it is eligible to tier, when
/// retained history expires entirely, and an optional per-range
/// retained byte ceiling. Every field inherits independently — a Feed
/// override supplies only the fields it names and keeps the rest of
/// its Domain's policy; absent everywhere means the built-in default
/// (sealed segments tier immediately, history never expires).
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct HistoryPolicy {
    /// Sealed segments younger than this stay local — the "hot local
    /// duration" of the History Policy contract.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tier_after_ns: Option<u64>,
    /// Sealed history expires once its newest record's ingest time is
    /// this old. Enforcement lands with the deletion contract item;
    /// the field is declared and validated here so policy intent is
    /// already durable and explainable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expire_after_ns: Option<u64>,
    /// Per-range retained-history byte ceiling; oldest sealed spans
    /// expire first when the range exceeds it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_retained_bytes: Option<u64>,
}

impl HistoryPolicy {
    /// Field-level override: `lower` supplies every field `self` does
    /// not declare. A Feed policy layered over its Domain's resolves
    /// the effective policy a storage Node enforces.
    pub fn layered_over(&self, lower: &HistoryPolicy) -> HistoryPolicy {
        HistoryPolicy {
            tier_after_ns: self.tier_after_ns.or(lower.tier_after_ns),
            expire_after_ns: self.expire_after_ns.or(lower.expire_after_ns),
            max_retained_bytes: self.max_retained_bytes.or(lower.max_retained_bytes),
        }
    }

    fn is_empty(&self) -> bool {
        self.tier_after_ns.is_none()
            && self.expire_after_ns.is_none()
            && self.max_retained_bytes.is_none()
    }
}

/// A History Policy binds to exactly one catalog target: a Domain's
/// policy is the inherited default for every Feed it owns; a Feed
/// policy overrides individual fields for that Feed alone.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PolicyScope {
    Domain { name: String },
    Feed { name: String },
}

impl PolicyScope {
    /// The scoped resource name for audit targets and error messages.
    pub fn target_name(&self) -> &str {
        match self {
            Self::Domain { name } | Self::Feed { name } => name,
        }
    }
}

/// The durable catalog row: which resolved definition holds the
/// declared override, its name at set time, and when it last changed.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct HistoryPolicyAssignment {
    pub scope_kind: String,
    pub target_id: Uuid,
    pub name: String,
    pub policy: HistoryPolicy,
    pub updated_at_ns: i64,
}

/// A legal hold freezes all destruction of a Feed's retained history:
/// expiry, byte-budget trimming, cryptographic erasure, and `DROP FEED`
/// all refuse while one exists. The row keys on the immutable FeedId so
/// `RENAME FEED` cannot evade it, and the free-text reason is the
/// audit-facing matter reference an operator reads back under
/// `SHOW LEGAL HOLDS` or `DESCRIBE FEED`. Dropping the hold is the
/// explicit unfreeze step — a hold is never inherited, cascaded, or
/// removed implicitly.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LegalHold {
    pub feed_id: Uuid,
    /// The Feed's name when the hold was last (re)declared — display
    /// convenience only; `feed_id` is the identity that fences.
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub set_at_ns: i64,
}

/// One side of a declared record schema: the wire format applications
/// write and readers decode, plus the registry-style locator tooling
/// resolves definitions through. Descriptive metadata only — the
/// storage engine stays schema-neutral; compatibility gates,
/// validation, and a versioned registry land with the schema
/// companion service.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaDecl {
    /// `json`, `jsonschema`, `avro`, `protobuf`, `text`, `bytes`, or
    /// `custom.<name>` for formats outside the built-in set.
    pub format: String,
    /// Optional registry subject or logical name (`orders-value`,
    /// `com.acme.Order`) the schema companion resolves.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    /// Optional inline schema definition (JSON Schema, IDL, DDL).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub definition: Option<String>,
}

/// Declared record schema metadata for a Domain or Feed: which format
/// record keys and payloads carry. Every field inherits independently
/// — a Feed override supplies only the sides it names and keeps the
/// rest of its Domain's declaration; absent everywhere means
/// schema-agnostic bytes (the built-in default).
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct FeedSchema {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<SchemaDecl>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<SchemaDecl>,
}

impl FeedSchema {
    /// Field-level override: `lower` supplies every side `self` does
    /// not declare. A Feed schema layered over its Domain's resolves
    /// the effective schema a Feed surface reports.
    pub fn layered_over(&self, lower: &FeedSchema) -> FeedSchema {
        FeedSchema {
            key: self.key.clone().or_else(|| lower.key.clone()),
            value: self.value.clone().or_else(|| lower.value.clone()),
        }
    }

    fn is_empty(&self) -> bool {
        self.key.is_none() && self.value.is_none()
    }
}

/// The durable catalog row for a declared schema: which resolved
/// definition holds the override, its name at set time, and when it
/// last changed. Renames keep the row; drops cascade it.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaAssignment {
    pub scope_kind: String,
    pub target_id: Uuid,
    pub name: String,
    pub schema: FeedSchema,
    pub updated_at_ns: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StateStoreSourceRequest {
    Manual,
    Feed { feed: String },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StateStoreSource {
    Manual,
    Feed { feed_id: Uuid },
}

/// The maximum named secondary keys one StateStore may declare: bounds the
/// posting writes one row mutation performs and matches the SDK surface.
pub const MAX_STATE_STORE_NAMED_KEYS: usize = 10;

/// One named secondary lookup key on a StateStore. `unique` claims each
/// value for exactly one primary row (writes conflict on contention);
/// non-unique keys keep a postings list so one value resolves to every
/// matching row — the "group/list" shape.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct NamedStateStoreKey {
    pub name: String,
    #[serde(default)]
    pub unique: bool,
    /// Feed-sourced stores only: how one committed source record projects
    /// to this key's typed lookup values. Manual stores carry values on
    /// each `put_row` mutation instead and must leave this unset.
    #[serde(default)]
    pub extractor: Option<IndexFieldSpec>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StateStoreStage {
    Declared,
    Dropped,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct StateStoreDefinition {
    pub store_id: Uuid,
    pub name: String,
    pub space_id: Uuid,
    pub source: StateStoreSource,
    pub stage: StateStoreStage,
    /// Declared named secondary keys. Empty for stores defined before the
    /// named-key surface — they serve primary-key lookups only.
    #[serde(default)]
    pub named_keys: Vec<NamedStateStoreKey>,
    pub created_at_ns: i64,
}

/// The processing step a Pipe applies to each delivered input record.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum PipeOperation {
    /// Copy each delivered record to the output Feed unchanged; the effect
    /// journal replays dedupe on deterministic writer identity.
    Forward,
    /// Accumulate each delivered record into fixed event-time panes of a
    /// managed StateStore view. The driver groups each page by the
    /// extracted `key` and the record's `event_time_ns` pane, then commits
    /// one `AccumulateRows` StateStore mutation inside the same effect as
    /// the consumed frontier — so a pane advances exactly once per
    /// committed input, even across crashes and ownership moves. Panes are
    /// immutable once written; this slice deliberately leaves watermarks,
    /// late-event revision, and window-close output as follow-on work.
    Window {
        /// Window length in nanoseconds; the driver shards it into 16
        /// equal accumulation panes.
        window_ns: u64,
        /// Which typed value groups records into window rows.
        key: IndexFieldSpec,
        aggregation: WindowAggregation,
        /// Which record clock assigns panes; defaults to event time.
        #[serde(default)]
        time: WindowTime,
        /// Optional lateness bound in nanoseconds: a record arriving more
        /// than this far behind its own `event_time_ns` quarantines into a
        /// countable `late` row instead of a pane — bounded, explicit
        /// late-event policy rather than silent revision of closed panes.
        /// Meaningful only under the event-time clock.
        #[serde(default)]
        max_late_ns: Option<u64>,
        /// Optional event-time watermark in nanoseconds: the driver tracks
        /// a durable monotone horizon `max(event_time) - watermark_ns` per
        /// Pipe and closes a pane once the horizon passes its end — close
        /// decisions follow event-time disorder rather than ingest-clock
        /// arrival skew. Mutually exclusive with `max_late_ns` (one
        /// lateness model per Pipe); refused under the ingest clock.
        #[serde(default)]
        watermark_ns: Option<u64>,
        /// Optional revision bound in nanoseconds, requires `watermark_ns`:
        /// an event landing on an already-emitted pane while
        /// `horizon <= pane_end + grace_ns` revises the pane and re-emits
        /// the result under a bumped `revision`; once the horizon passes
        /// the grace bound the event counts into the `late` row instead.
        /// Without `GRACE` every emitted-pane event quarantines.
        #[serde(default)]
        grace_ns: Option<u64>,
        /// Optional pane-row retention bound in nanoseconds: a pane row
        /// deletes once `pane_end + expire_ns` is behind the close clock,
        /// emitted or not — the declared bound on how much window state
        /// the store retains. Must exceed `max_late_ns` when both are
        /// declared so a pane provably closes before it may expire.
        #[serde(default)]
        expire_ns: Option<u64>,
        /// Bound on distinct active group Keys held in pane/sentinel state;
        /// `None` leaves growth unbounded (the pre-limit behavior).
        #[serde(default)]
        key_limit: Option<u64>,
        /// Bound on accounted pane-state bytes (`window_pane_row_bytes`
        /// per row); `None` leaves byte growth unbounded. New (group,
        /// pane) rows beyond the bound refuse into `__bytecap__`.
        #[serde(default)]
        byte_limit: Option<u64>,
    },
    /// Point-lookup enrichment: for each delivered record, resolve one
    /// StateStore row by the projected `key` and copy its payload into the
    /// output record's Metadata (`enrich.payload`, plus `enrich.matched`).
    /// The resolution rides inside the effect's `Declare`, so an applied
    /// output is byte-identical across replays; concurrent drives over one
    /// frontier converge on whichever Declare committed first.
    Enrich {
        /// Which typed record value addresses the lookup row's primary key.
        key: IndexFieldSpec,
        /// How fresh the lookup row must be; defaults to the applied state
        /// the first reachable replica already serves.
        #[serde(default)]
        freshness: EnrichFreshness,
        /// Optional named secondary key to resolve through instead of the
        /// store's primary key — the same `by_named` contract public row
        /// reads use; the store must declare the named key.
        #[serde(default)]
        named: Option<String>,
        /// What a lookup miss does to the output record.
        #[serde(default)]
        on_miss: EnrichMiss,
        /// Resolved at define time when `on_miss` is `Quarantine`; the
        /// declared name resolves to a same-Domain Feed that is neither
        /// the input nor the output.
        #[serde(default)]
        quarantine_feed_id: Option<Uuid>,
    },
}

/// What an `Enrich` lookup miss does to the output record.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EnrichMiss {
    /// Forward the record with `enrich.matched = false` — the default:
    /// enrichment is best-effort and misses stay visible downstream.
    #[default]
    Forward,
    /// Consume the record without emitting an output — the input frontier
    /// still commits inside the same effect, so a dropped miss is an
    /// acknowledged, replay-safe decision rather than a lost record.
    Drop,
    /// Route the miss to a declared quarantine Feed (`quarantine_feed_id`)
    /// instead of the output — bad keys stay inspectable without
    /// contaminating the enriched stream.
    Quarantine,
}

/// How fresh an `Enrich` lookup must be before its result is journaled.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EnrichFreshness {
    /// Serve the replica's currently applied state — a reachable lagging
    /// replica may answer an older row. This is the safe default: the
    /// resolution is journaled either way, never re-read on replay.
    #[default]
    Latest,
    /// Require the replica's applied sequence to cover its committed
    /// sequence at read entry (the same strict contract public strict
    /// reads use). A replica that lags is skipped; when every replica lags
    /// or is unreachable the drive fails retryably — lagging state yields
    /// an explicit retry, never a journaled stale row.
    Strict,
}

/// Which record clock a `Window` Pipe buckets into panes. Both clocks are
/// signed 64-bit Unix epoch nanoseconds on the record itself; neither is a
/// physical partitioning concept.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WindowTime {
    /// `event_time_ns` — the application-stamped occurrence clock. This is
    /// the default: it reflects when things happened, not when they
    /// arrived.
    #[default]
    Event,
    /// `ingest_time_ns` — the durable-arrival clock, for pipelines whose
    /// business view genuinely tracks ingest order.
    Ingest,
}

/// What a `Window` Pipe adds to each pane's accumulator.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "aggregation", rename_all = "snake_case")]
pub enum WindowAggregation {
    /// Count records per (group key, pane); `sum` stays zero.
    Count,
    /// Sum one extracted integer value per pane.
    Sum { value: IndexFieldSpec },
    /// Track both count and integer sum per pane; readers divide.
    Average { value: IndexFieldSpec },
}

/// One typed field of an application-owned secondary Index — the catalog and
/// wire shape; the storage codec mapping to `index::IndexField` arrives with
/// the extraction pipeline.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum IndexFieldSpec {
    /// The record's application Key.
    Key,
    /// A named record Metadata entry.
    Metadata { name: String },
    /// A versioned extractor projection, e.g. a JSON-path field pull.
    Projection {
        extractor: String,
        version: u32,
        field: String,
    },
}

/// How an Index collates Text values. Persisted on the definition so a later
/// change is a new Index version rather than a silent semantic change.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IndexCollation {
    /// Bytewise UTF-8 ordering — matches the encoded posting order.
    #[default]
    Binary,
}

/// The read consistency a declared Index intends to honour once serving.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IndexConsistency {
    /// Lookups may lag the applied Cursor; freshness is reported.
    #[default]
    Eventual,
    /// Lookups fence on the requested applied Cursor. Declared only — strict
    /// queries stay unavailable until the replicated Feed-to-Index commit and
    /// safe rebuild protocol land.
    Strict,
}

/// Build state of a declared Index. Only `declared` exists until the
/// extraction/build pipeline and RF3 Index replicas are implemented.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IndexBuildStage {
    /// Persisted definition; no extraction or backfill has run.
    Declared,
    /// A rebuild pass is materializing generation `generation` into the
    /// shadow generation while the previous generation keeps serving.
    Building { generation: u64 },
    /// Generation `generation` is the serving generation.
    Active { generation: u64 },
}

impl IndexBuildStage {
    /// The generation currently building or serving, if any.
    pub fn generation(&self) -> Option<u64> {
        match self {
            Self::Declared => None,
            Self::Building { generation } | Self::Active { generation } => Some(*generation),
        }
    }
}

/// One inclusive/exclusive endpoint of a bounded Index range query.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct IndexBound {
    pub value: IndexValue,
    #[serde(default = "bound_inclusive")]
    pub inclusive: bool,
}

fn bound_inclusive() -> bool {
    true
}

/// The read shape a `GET INDEX` statement or typed query request asks the
/// data plane for. `Exact` supplies every declared field; `Prefix`
/// matches leading complete fields plus an optional text prefix on the
/// next field; `Range` bounds the field after `leading` exact-equals.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IndexQueryPredicate {
    Exact {
        values: Vec<IndexValue>,
    },
    Prefix {
        leading: Vec<IndexValue>,
        #[serde(default)]
        partial_text: Option<String>,
    },
    Range {
        #[serde(default)]
        leading: Vec<IndexValue>,
        lower: IndexBound,
        upper: IndexBound,
    },
}

/// A Control Plane-persisted secondary Index definition. `applied_cursor`
/// records build progress against the source Feed; `None` while `declared`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredIndexDefinition {
    pub index_id: Uuid,
    pub name: String,
    pub space_id: Uuid,
    pub feed_id: Uuid,
    pub fields: Vec<IndexFieldSpec>,
    pub collation: IndexCollation,
    pub consistency: IndexConsistency,
    /// UNIQUE Indexes arbitrate claims by committed order: the earliest
    /// committed record to claim a value wins identically on every
    /// replica, and losing rows carry the Index generation in their
    /// `conflicted` evidence instead of poisoning the apply pass.
    #[serde(default)]
    pub unique: bool,
    pub stage: IndexBuildStage,
    pub applied_cursor: Option<String>,
    pub created_at_ns: i64,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PipeStage {
    /// Persisted in the catalog and driven by the progress owner.
    Declared,
    /// Operator-paused: durable, visible, and skipped by every drive
    /// until resumed — in-flight effects finish first because pause is
    /// cooperative between drives, never a mid-effect cancellation.
    Paused,
}

/// A declared Pipe: consume one Subscription's deliveries and write one
/// output Feed, recording input progress and output identity through the
/// internal effect journal. Physical placement stays an implementation
/// detail.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PipeDefinition {
    pub pipe_id: Uuid,
    pub name: String,
    pub space_id: Uuid,
    /// The Subscription whose durable frontier drives this Pipe.
    pub subscription_id: Uuid,
    pub operation: PipeOperation,
    /// Present when the operation writes an output Feed (`Forward`).
    pub output_feed_id: Option<Uuid>,
    /// Present when the operation accumulates into a StateStore (`Window`).
    #[serde(default)]
    pub output_store_id: Option<Uuid>,
    pub stage: PipeStage,
    pub status: ResourceStatus,
    pub created_at_ns: i64,
}

impl PipeDefinition {
    /// Feeds this Pipe writes and therefore must never re-consume: the
    /// declared output (Forward/Enrich output, or a Window `EMIT TO
    /// FEED`) plus an `Enrich` miss quarantine route. A Domain-sourced
    /// Subscription covers those Feeds as member Feeds, so a drive that
    /// read them would loop its own writes back through the frontier as
    /// new input — the member-Feed extension of the exact-Feed
    /// `output != input` refusal.
    pub fn write_feeds(&self) -> std::collections::BTreeSet<Uuid> {
        let mut feeds = self.output_feed_id.into_iter().collect::<BTreeSet<Uuid>>();
        if let PipeOperation::Enrich {
            quarantine_feed_id: Some(feed),
            ..
        } = &self.operation
        {
            feeds.insert(*feed);
        }
        feeds
    }
}

/// Which storage-eligible Node may drive a Pipe right now. The lease is
/// pure catalog metadata — it owns no replica state of its own, so moving
/// it is a single epoch-bumping compare-and-set rather than a staged
/// replica cutover. Raced drives still converge on the deterministic
/// Declare journal and the Subscription frontier CAS, so an in-flight
/// drive finishing on the previous owner is safe; the fencing guarantee
/// is that no *new* drive starts there after the move commits.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PipeComputeLease {
    pub pipe_id: Uuid,
    pub owner: StorageNodeId,
    /// Bumped on every move; fences stale drain plans and operator moves.
    pub compute_epoch: u64,
}

/// A declared route to another Riverbed: reachable endpoints plus the
/// name of a per-Node environment variable holding the credential. Key
/// material is resolved locally at drive time — the replicated catalog
/// never stores secrets.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RiverbedLink {
    pub link_id: Uuid,
    pub name: String,
    /// Base URLs of the remote Riverbed's Nodes, tried in order.
    pub endpoints: Vec<String>,
    /// Environment variable each Node reads for the remote API key.
    pub key_env: String,
    pub status: ResourceStatus,
    pub created_at_ns: i64,
}

/// A declared Distributary: a managed copy of one remote Feed into one
/// local Feed, preserving Key, `event_time_ns`, and Metadata while
/// minting new local Cursors. Durable progress lives on the remote as a
/// named Reader frontier derived from `distributary_id`, so a lease move
/// resumes where the last owner acknowledged — the local lease only
/// decides which Node drives.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DistributaryDefinition {
    pub distributary_id: Uuid,
    pub name: String,
    /// Local Domain the target Feed lives in.
    pub space_id: Uuid,
    pub link_id: Uuid,
    /// Feed name on the remote Riverbed.
    pub source_feed: String,
    pub target_feed_id: Uuid,
    pub status: ResourceStatus,
    pub created_at_ns: i64,
}

/// Which storage-eligible Node may drive a Distributary right now. The
/// same single-epoch compare-and-set discipline as `PipeComputeLease`;
/// raced drives converge on remote Reader session fencing, so an
/// in-flight drive finishing on the previous owner cannot double-ack.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DistributaryLease {
    pub distributary_id: Uuid,
    pub owner: StorageNodeId,
    /// Bumped on every move; fences stale drain plans and operator moves.
    pub epoch: u64,
}

/// Driver-reported runtime state for one Distributary, published by the
/// lease owner through `ReportDistributaryStatus` on a state transition
/// or heartbeat and fenced to the committed lease — a previous owner's
/// stale report cannot masquerade as live evidence after a move.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DistributaryStatus {
    /// Lease epoch the reporting owner was driving under.
    pub lease_epoch: u64,
    /// Storage Node that observed the cycle.
    pub reported_by: StorageNodeId,
    /// Lifecycle the last completed cycle observed.
    pub run_state: DistributaryRunState,
    /// Records this owner observed appended under `lease_epoch`.
    pub copied_records: u64,
    /// When the last cycle appended records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_copied_at_ns: Option<i64>,
    /// Most recent failed cycle's refusal when the run is stalled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error_at_ns: Option<i64>,
    /// When the owner published this report.
    pub reported_at_ns: i64,
}

/// What one drive cycle observed; `caught_up` means the last remote
/// fetch drained the tail — the honest lag signal until the remote can
/// report a numeric frontier gap.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DistributaryRunState {
    /// Lease owner resolved but no drive cycle has completed.
    Starting,
    /// The last cycle appended records.
    Copying,
    /// The last fetch returned no waiting remote records.
    CaughtUp,
    /// The last cycle failed; `last_error` carries the refusal.
    Stalled,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct WriterDefinition {
    pub writer_id: Uuid,
    pub name: String,
    pub feed_id: Uuid,
    pub status: ResourceStatus,
    pub created_at_ns: i64,
    #[serde(default)]
    pub session_epoch: u64,
    #[serde(default = "default_writer_sequence")]
    pub next_sequence: u64,
    #[serde(default)]
    pub session_active: bool,
    #[serde(default)]
    pub range_next_sequences: BTreeMap<RangeId, u64>,
}

fn default_writer_sequence() -> u64 {
    1
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", content = "cursor", rename_all = "snake_case")]
pub enum ReaderStart {
    Beginning,
    Now,
    Cursor(String),
    Timestamp(i64),
}

/// Replay bookkeeping for the single-record delivery path: the Cursor the
/// page started from plus the bound it was read under let a duplicate
/// `request_id` re-derive and prove the identical page from immutable
/// history — the non-frontier counterpart of `ReaderFrontier::previous`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReaderDeliveryReplay {
    pub request_id: Uuid,
    pub previous_cursor: Option<String>,
    pub limit: usize,
    pub retry_after_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReaderDefinition {
    pub reader_id: Uuid,
    pub name: String,
    pub feed_id: Uuid,
    pub start: ReaderStart,
    pub acknowledged_cursor: Option<String>,
    #[serde(default)]
    pub delivered_cursor: Option<String>,
    /// The last committed delivery's replay bracket; set only on the
    /// non-frontier (single-record Cursor) path.
    #[serde(default)]
    pub last_delivery: Option<ReaderDeliveryReplay>,
    #[serde(default)]
    pub session_epoch: u64,
    #[serde(default)]
    pub session_capacity: usize,
    #[serde(default)]
    pub session_active: bool,
    pub status: ResourceStatus,
    pub created_at_ns: i64,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SubscriptionStage {
    Declared,
}

/// Versioned source scope a Subscription consumes. `Feed` pins the
/// immutable `feed_id` of one exact-Feed source — the wire field is still
/// named `feed_id`, so catalog rows and commands written before the
/// variant existed decode unchanged. `Domain` binds the immutable
/// `domain_id`: the Subscription covers every current and future Feed in
/// the Domain, with per-Feed progress tracked incrementally (no global
/// cross-Feed ordering is promised).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum SubscriptionSource {
    Feed { feed_id: Uuid },
    Domain { domain_id: Uuid },
}

impl SubscriptionSource {
    /// The pinned Feed identity for exact-Feed sources; `None` for
    /// Domain-scoped sources, whose member Feeds attach incrementally.
    pub fn feed_id(&self) -> Option<Uuid> {
        match self {
            Self::Feed { feed_id } => Some(*feed_id),
            Self::Domain { .. } => None,
        }
    }

    /// The bound Domain identity for Domain-scoped sources.
    pub fn domain_id(&self) -> Option<Uuid> {
        match self {
            Self::Domain { domain_id } => Some(*domain_id),
            Self::Feed { .. } => None,
        }
    }
}

/// Bound on member Feeds tracked per Domain-scoped Subscription;
/// attaching past it refuses rather than silently dropping tracking.
pub const MAX_SUBSCRIPTION_MEMBER_FEEDS: usize = 4096;

/// Explainable lifecycle state of one member Feed under a Domain-scoped
/// Subscription.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SubscriptionFeedState {
    /// Membership is attached; the initial frontier boundary is durable.
    Active,
    /// The Feed left the Domain or is being removed; pending work is
    /// fenced and never silently acknowledged or discarded.
    Draining,
    /// Attach is deferred (for example bound or authorization pressure);
    /// nothing is skipped and the detail explains why.
    Blocked,
}

/// One member Feed under a Domain-scoped Subscription, keyed by the
/// immutable FeedId so a rename retains progress and a same-name
/// replacement gets fresh progress.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SubscriptionFeedMembership {
    pub feed_id: Uuid,
    pub state: SubscriptionFeedState,
    /// Start policy this member honors. Existing Feeds honor the
    /// Subscription's declared start; newly discovered Feeds always start
    /// at `ReaderStart::Beginning` (their first retained record).
    pub start: ReaderStart,
    /// Catalog revision that durably attached the Feed: the discovery
    /// boundary after which no write to the Feed can be skipped.
    pub attached_revision: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SubscriptionDefinition {
    pub subscription_id: Uuid,
    pub name: String,
    pub space_id: Uuid,
    /// Serialized inline so the legacy `feed_id` column decodes as the
    /// `Feed` variant and Domain rows carry `domain_id` instead.
    #[serde(flatten)]
    pub source: SubscriptionSource,
    pub start: ReaderStart,
    /// Records become deliverable only once their ingest timestamp is at
    /// least this far in the past; `None` delivers immediately. Parked
    /// records keep their positions and never advance the frontier early.
    #[serde(default)]
    pub delivery_delay_ns: Option<u64>,
    /// The declared retry budget: when a work claim's durable lease epoch —
    /// advanced on every reclaim or transfer — exceeds
    /// `max_delivery_attempts`, the positions the claim would deliver are
    /// exhausted once per claim under `disposition`. `None` retries forever.
    #[serde(default)]
    pub exhausted_policy: Option<SubscriptionExhaustedPolicy>,
    pub stage: SubscriptionStage,
    pub status: ResourceStatus,
    pub created_at_ns: i64,
}

/// What `member.fetch` does to the positions an over-budget claim would
/// deliver, applied once per claim epoch.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SubscriptionExhaustedDisposition {
    /// Park the positions in the durable quarantine set: releasable evidence
    /// a member can return to delivery once resolved.
    #[default]
    Quarantine,
    /// Permanently pass the positions over; the durable `skipped` evidence
    /// is releasable only through an explicit Subscription SEEK.
    Skip,
}

/// The declared per-work retry budget on a Subscription. Attempts are
/// counted by the claim's durable lease epoch — each expiry or transfer
/// that regrants the work counts one — so positions parked under an
/// over-budget claim are the head work that kept failing to acknowledge.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionExhaustedPolicy {
    pub max_delivery_attempts: u64,
    #[serde(default)]
    pub disposition: SubscriptionExhaustedDisposition,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ReaderFrontier {
    pub acknowledged: BTreeMap<RangeId, String>,
    pub delivered: BTreeMap<RangeId, String>,
    /// The per-range positions the last delivered page started from —
    /// persisted so a duplicate `fetch_request_id` can re-read and prove
    /// an identical page rather than blindly refusing.
    #[serde(default)]
    pub previous: BTreeMap<RangeId, String>,
    /// The record bound the last delivered page used; replay is only
    /// provable under the identical bound.
    #[serde(default)]
    pub last_fetch_limit: usize,
    /// The backoff advice the last delivered page was paced under; replay
    /// reports the identical value on any ingress Node.
    #[serde(default)]
    pub last_fetch_retry_after_ms: u64,
    #[serde(default)]
    pub last_fetch_request_id: Option<Uuid>,
}

#[derive(Clone, Debug)]
pub(crate) struct ReaderCutoverSnapshot {
    pub reader: ReaderDefinition,
    pub frontier: Option<ReaderFrontier>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReaderFrontierTranslation {
    pub reader_id: Uuid,
    pub expected_session_epoch: u64,
    pub expected_acknowledged_cursor: Option<String>,
    pub expected_delivered_cursor: Option<String>,
    pub expected_acknowledged: BTreeMap<RangeId, String>,
    pub expected_delivered: BTreeMap<RangeId, String>,
    pub expected_has_frontier: bool,
    pub acknowledged: BTreeMap<RangeId, String>,
}

/// A Subscription progress frontier already translated onto a cutover's
/// candidate range map, committed inside the same activation command that
/// changes routing. Subscription progress lives on a separate RF3 quorum, so
/// the catalog cannot apply it directly; instead the command persists the
/// translation as durable pending evidence (`pending_subscription_cutovers`)
/// that the post-activation reconcile — or any later member operation —
/// applies deterministically through the frontier's Cursor compare-and-set.
/// A member acknowledgement that advanced the frontier after staging fails
/// the CAS rather than being silently overwritten, keeping the Subscription
/// fail-closed until an operator reconciles it with SEEK SUBSCRIPTION.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionCutoverTranslation {
    pub subscription_id: Uuid,
    pub feed_id: Uuid,
    pub plan_id: Uuid,
    /// The committed frontier Cursor staged when the translation was
    /// computed; the pending apply refuses when the frontier has since moved.
    pub expected_cursor: String,
    /// The staged frontier's sequence — the applied mutation commits at
    /// `frontier_sequence + 1`, keeping progress-shard sequence fencing.
    pub frontier_sequence: u64,
    /// Deterministic mutation identity derived from the plan and target
    /// sequence, so a replayed apply deduplicates instead of forking a
    /// second transition.
    pub request_id: Uuid,
    /// The Cursor the applied frontier carries — `cutover:{plan_id}`.
    pub cursor: String,
    pub positions: BTreeMap<RangeId, String>,
    #[serde(default)]
    pub quarantined: BTreeMap<RangeId, Vec<String>>,
    /// Final-disposition evidence carried through the cutover; legacy rows
    /// decode as empty. Skip is not releasable — only a Subscription SEEK
    /// replays the positions.
    #[serde(default)]
    pub skipped: BTreeMap<RangeId, Vec<String>>,
    /// Owed-work positions still deferred to member Key windows; they
    /// translate onto the new range map like the other evidence sets so a
    /// split or merge cannot strand undelivered work.
    #[serde(default)]
    pub deferred: BTreeMap<RangeId, Vec<String>>,
    pub tick: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RoleDefinition {
    pub role_id: Uuid,
    pub name: String,
    pub status: ResourceStatus,
    pub created_at_ns: i64,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "lowercase")]
pub enum PermissionAction {
    Read,
    Write,
    Manage,
}

/// Whether a namespace grant permits or refuses an action. Explicit
/// `DENY` rows evaluate before `GRANT` rows at request time: a
/// covering deny wins even against `manage`-implied permissions and
/// Riverbed-admin issued identities; the configured bootstrap key is
/// the only break-glass credential.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Default)]
#[serde(rename_all = "lowercase")]
pub enum GrantEffect {
    #[default]
    Allow,
    Deny,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct NamespaceGrant {
    pub grant_id: Uuid,
    pub role_id: Uuid,
    pub namespace: String,
    pub actions: BTreeSet<PermissionAction>,
    /// `allow` for legacy persisted rows; `deny` fences the action even
    /// when another grant would cover it.
    #[serde(default)]
    pub effect: GrantEffect,
    pub created_at_ns: i64,
}

/// An issued API-key identity persisted in the replicated catalog.
/// Only the BLAKE3-256 hex verifier of the caller-held secret is
/// stored: the plaintext key never enters WCL, typed commands, the
/// replicated log, or persisted catalog state.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ApiKeyIdentity {
    pub key_id: Uuid,
    pub name: String,
    /// BLAKE3-256 verifier of the caller-held key, as 64-digit hex.
    pub digest: String,
    /// The Role whose grants decide what this identity may do.
    pub role_id: Uuid,
    pub status: ResourceStatus,
    pub created_at_ns: i64,
    /// Optional epoch-nanosecond expiry; authorization refuses the
    /// identity at request time once this passes.
    #[serde(default)]
    pub expires_at_ns: Option<i64>,
    #[serde(default)]
    pub revoked_at_ns: Option<i64>,
}

/// The slice of an issued identity the request-time authorization path
/// needs; recomputed from committed catalog state after every apply.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IssuedApiKey {
    pub name: String,
    /// Whether the bound Role currently holds `manage` on the root `*`
    /// namespace, which admin endpoints require.
    pub riverbed_admin: bool,
    /// The bound Role's grants cloned at refresh so request-time
    /// namespace checks never touch the catalog lock.
    pub grants: Vec<IssuedGrant>,
    pub expires_at_ns: Option<i64>,
}

/// One namespace grant resolved against an issued identity's Role.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IssuedGrant {
    pub namespace: String,
    pub actions: BTreeSet<PermissionAction>,
    pub effect: GrantEffect,
}

/// Issued-identity authorization view keyed by verifier digest.
pub type IssuedApiKeySet = BTreeMap<[u8; 32], IssuedApiKey>;

/// Hard bound on catalog-issued identities: keeps the catalog small
/// and the request-time authorization scan bounded.
pub const MAX_ISSUED_API_KEYS: usize = 512;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RangeSplitStage {
    Prepared,
    CatchingUp,
    Ready,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeSplitPlan {
    pub plan_id: Uuid,
    pub feed_id: Uuid,
    pub source_range_id: RangeId,
    pub split_at: KeyToken,
    pub candidate_map: RangeMap,
    #[serde(default)]
    pub left_assignment: Option<ActiveRangeAssignment>,
    pub right_assignment: ActiveRangeAssignment,
    pub stage: RangeSplitStage,
    pub source_commit: Option<CommitPosition>,
    pub source_scanned_through: Option<CommitPosition>,
    pub right_commit: Option<CommitPosition>,
    pub checksum_verified: bool,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RangeMergeStage {
    Prepared,
    Staging,
    Ready,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeMergePlan {
    pub plan_id: Uuid,
    pub feed_id: Uuid,
    pub left_range_id: RangeId,
    pub right_range_id: RangeId,
    pub candidate_map: RangeMap,
    pub merged_assignment: ActiveRangeAssignment,
    pub stage: RangeMergeStage,
    #[serde(default)]
    pub left_commit: Option<CommitPosition>,
    #[serde(default)]
    pub right_commit: Option<CommitPosition>,
    #[serde(default)]
    pub merged_commit: Option<CommitPosition>,
    #[serde(default)]
    pub checksum_verified: bool,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RangeMoveStage {
    Prepared,
    CatchingUp,
    Ready,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeMovePlan {
    pub plan_id: Uuid,
    pub feed_id: Uuid,
    pub source_assignment: ActiveRangeAssignment,
    pub candidate_assignment: ActiveRangeAssignment,
    pub removed_replica: StorageNodeId,
    pub replacement_replica: StorageNodeId,
    pub stage: RangeMoveStage,
    pub source_commit: Option<CommitPosition>,
    pub target_commit: Option<CommitPosition>,
    pub checksum_verified: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeOwnerMovePlan {
    pub plan_id: Uuid,
    pub feed_id: Uuid,
    pub source_assignment: ActiveRangeAssignment,
    pub candidate_assignment: ActiveRangeAssignment,
    pub stage: RangeMoveStage,
    pub source_commit: Option<CommitPosition>,
    pub target_commit: Option<CommitPosition>,
    pub checksum_verified: bool,
}

/// A durable proof that one Range committed history through `position`
/// on a replica quorum. The Append Owner reports it through the
/// majority-replicated Control Plane after each quorum commit, so the
/// proof survives loss of two local copies; per-read replica
/// corroboration can then distinguish a lagging replica set from
/// committed history that is provably gone. Proofs are advance-only:
/// a stale position is a no-op, and a conflicting digest at the same
/// position is fork evidence and refused.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeCommitProof {
    pub feed_id: Uuid,
    pub range_id: RangeId,
    pub position: CommitPosition,
    pub frame_digest: [u8; 32],
    pub recorded_at_ns: i64,
    /// Replicas that persisted CommitPosition evidence for this boundary.
    pub evidence: Vec<StorageNodeId>,
    /// Append Owner that reported the proof.
    pub reporter: StorageNodeId,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageNodeRecord {
    pub node: StorageNodeId,
    pub endpoint: String,
    pub registered_at_ns: i64,
    /// blake3-256 certificate fingerprints (hex) this Node may present on
    /// the internal mTLS plane; empty means no certificate is registered.
    #[serde(default)]
    pub cert_pins: BTreeSet<String>,
    /// Declared data-plane roles for this Node. Records that predate
    /// capability descriptors default to every role, matching their
    /// historical placement behavior.
    #[serde(default)]
    pub capabilities: NodeCapabilities,
}

/// One committed self-reported capacity measurement for a storage
/// identity. `reported_at_ns` is stamped from the command's committed
/// issue time so replays and out-of-order reports never roll a newer
/// measurement backwards.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CapacityReport {
    pub capacity: NodeCapacity,
    pub reported_at_ns: i64,
    /// The protocol level the reporting Node claimed — `0` on reports
    /// from binaries predating version advertisement.
    #[serde(default)]
    pub protocol_version: u32,
    /// The package version the reporting Node claimed.
    #[serde(default)]
    pub build: String,
}

/// Evidence that a storage identity tried to report from an
/// incompatible protocol level: the claimed level, build, and newest
/// refusal instant. The row commits *with* the refusal so `SHOW
/// COMPATIBILITY` can explain "why isn't my new Node joining" from
/// replicated state rather than a dropped log line.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct IncompatibleNodeReport {
    pub protocol_version: u32,
    pub build: String,
    pub refused_at_ns: i64,
}

/// The catalog state-layout version this binary can read and write. A
/// Node opened against a *newer* schema is a rollback across an
/// irreversible storage migration: it must fail closed rather than
/// silently misread fields it does not understand. Bump this only when
/// persisted state changes in a way older binaries cannot tolerate —
/// additive `#[serde(default)]` fields stay compatible at 1.
pub const CATALOG_SCHEMA_VERSION: u32 = 1;

/// Bound on distinct identities remembered as incompatible reporters:
/// a churning Node updates its own row rather than growing the map.
const MAX_INCOMPATIBLE_REPORTS: usize = 256;

/// The shared minimum-baseline policy: which capacity evidence a Node
/// must commit before placement may target it. Both bounds default off;
/// enabling either makes a *missing* report an exclusion as well —
/// unmeasured Nodes fail closed once measurement is required. The policy
/// is static config that must be identical cluster-wide because
/// eligibility is derived inside deterministic apply.
#[derive(Clone, Copy, Debug, Default)]
pub struct CapacityPolicy {
    /// Maximum age of a committed report, in nanoseconds, before the
    /// Node is treated as unmeasured. `None` disables freshness checks.
    pub report_max_age_ns: Option<i64>,
    /// Minimum `disk_free_bytes` a reporting Node must show; 0 disables.
    pub min_disk_free_bytes: u64,
}

impl CapacityPolicy {
    pub fn active(&self) -> bool {
        self.report_max_age_ns.is_some() || self.min_disk_free_bytes > 0
    }
}

/// Hard bound on the replicated audit trail. This must stay a shared
/// constant — a configurable bound could diverge replicas (one Node's
/// ring evicting earlier than another's produces different catalog
/// snapshots), so tuning it takes the replicated-baseline route the
/// `CapacityPolicy` uses, not a per-Node env.
pub const AUDIT_EVENT_LIMIT: usize = 16_384;

/// The authenticated identity behind an administrative mutation,
/// recorded at propose time so every replica applies the same actor.
/// The bootstrap credential and Node-internal producers are named
/// kinds; issued API keys record their declared name — never a digest.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AuditActor {
    /// The configured `WHITEWATER_ADMIN_API_KEY` credential.
    BootstrapKey,
    /// A catalog-issued API-key identity, by declared name.
    ApiKey { name: String },
    /// An internal Node caller once the internal plane can prove one.
    Node { id: String },
    /// A Node-internal producer with no external caller (self-issued
    /// movement, cutover, and drain commands).
    System,
    /// Persisted entries predating actor recording.
    Unknown,
}

impl Default for AuditActor {
    fn default() -> Self {
        Self::Unknown
    }
}

/// How the apply path settled the audited command. Refusals still ride
/// the replicated log (their result is cached for idempotent replay),
/// so a refused administrative *attempt* is auditable evidence, not a
/// silently dropped rejection.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum AuditOutcome {
    Committed,
    Refused { error: String },
}

/// One committed administrative mutation. Events are append-only
/// inside `CatalogState` and survive eviction only by rolling off the
/// bounded front — that is retention, never mutation of surviving
/// rows. Fields derive entirely from replicated inputs (request id,
/// leader-stamped time, actor, command, apply verdict) so every
/// replica persists byte-identical events.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AuditEvent {
    /// Audit-local ordinal; keeps rising across ring eviction.
    pub seq: u64,
    /// Catalog revision the command committed under. A refused apply
    /// never advances the revision, so its events repeat the current
    /// one — `seq` is the ordering guarantee.
    pub revision: u64,
    /// Leader-stamped `issued_at_ns` — identical on every replica.
    pub at_ns: i64,
    pub request_id: Uuid,
    pub actor: AuditActor,
    /// WCL-style command label (`CREATE FEED`, `GRANT`, ...).
    pub action: String,
    /// Primary resource name (or id) the command acts on.
    pub target: Option<String>,
    /// Secondary context where a single name cannot carry it (grant
    /// namespaces, rename destinations, legal-hold reasons).
    pub detail: Option<String>,
    pub outcome: AuditOutcome,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CatalogState {
    schema_version: u32,
    revision: u64,
    spaces: BTreeMap<Uuid, SpaceDefinition>,
    feeds: BTreeMap<Uuid, FeedDefinition>,
    #[serde(default)]
    state_stores: BTreeMap<Uuid, StateStoreDefinition>,
    #[serde(default)]
    indexes: BTreeMap<Uuid, StoredIndexDefinition>,
    #[serde(default)]
    pipes: BTreeMap<Uuid, PipeDefinition>,
    writers: BTreeMap<Uuid, WriterDefinition>,
    readers: BTreeMap<Uuid, ReaderDefinition>,
    #[serde(default)]
    subscriptions: BTreeMap<Uuid, SubscriptionDefinition>,
    #[serde(default)]
    subscription_progress_assignments: BTreeMap<Uuid, SubscriptionProgressAssignment>,
    /// Translated Subscription frontiers committed by Active Range
    /// activations but not yet applied to the separate RF3 progress quorum —
    /// durable replayable evidence so an interrupted reconcile heals on the
    /// next member operation instead of stranding the Subscription.
    #[serde(default)]
    pending_subscription_cutovers: BTreeMap<Uuid, SubscriptionCutoverTranslation>,
    #[serde(default)]
    pipe_compute_leases: BTreeMap<Uuid, PipeComputeLease>,
    #[serde(default)]
    riverbed_links: BTreeMap<Uuid, RiverbedLink>,
    #[serde(default)]
    distributaries: BTreeMap<Uuid, DistributaryDefinition>,
    #[serde(default)]
    distributary_leases: BTreeMap<Uuid, DistributaryLease>,
    /// Lease-owner drive reports keyed by Distributary — operator-facing
    /// runtime state, fenced to the committed lease on write and marked
    /// stale on read once the lease moves.
    #[serde(default)]
    distributary_status: BTreeMap<Uuid, DistributaryStatus>,
    #[serde(default)]
    subscription_feed_memberships: BTreeMap<Uuid, BTreeMap<Uuid, SubscriptionFeedMembership>>,
    #[serde(default)]
    state_store_assignments: BTreeMap<Uuid, crate::state_store::StateStoreAssignment>,
    #[serde(default)]
    reader_frontiers: BTreeMap<Uuid, ReaderFrontier>,
    roles: BTreeMap<Uuid, RoleDefinition>,
    grants: BTreeMap<Uuid, NamespaceGrant>,
    #[serde(default)]
    api_keys: BTreeMap<Uuid, ApiKeyIdentity>,
    /// Declared History Policy overrides keyed by the target
    /// definition's id (a `space_id` or `feed_id`). Definitions own the
    /// row so renames keep the policy and drops cascade it away.
    #[serde(default)]
    history_policies: BTreeMap<Uuid, HistoryPolicyAssignment>,
    /// Legal holds keyed by FeedId — the freeze that every history-
    /// destruction path consults before deleting retained bytes.
    #[serde(default)]
    legal_holds: BTreeMap<Uuid, LegalHold>,
    /// Per-Feed data-encryption keys, minted at `CREATE FEED ... ENCRYPTED`.
    /// Rows persist after destruction (status flips, material cleared)
    /// so erasure stays auditable.
    #[serde(default)]
    feed_keys: BTreeMap<Uuid, FeedKey>,
    /// Declared record schema metadata keyed the same way — a
    /// `space_id` carries the Domain default; a `feed_id` overrides it.
    #[serde(default)]
    schemas: BTreeMap<Uuid, SchemaAssignment>,
    #[serde(default)]
    active_ranges: BTreeMap<Uuid, ActiveRangeAssignment>,
    #[serde(default)]
    range_maps: BTreeMap<Uuid, RangeMap>,
    #[serde(default)]
    range_assignments: BTreeMap<RangeId, ActiveRangeAssignment>,
    #[serde(default)]
    range_split_plans: BTreeMap<Uuid, RangeSplitPlan>,
    #[serde(default)]
    range_merge_plans: BTreeMap<Uuid, RangeMergePlan>,
    #[serde(default)]
    range_move_plans: BTreeMap<RangeId, RangeMovePlan>,
    #[serde(default)]
    completed_move_plans: BTreeMap<RangeId, RangeMovePlan>,
    #[serde(default)]
    owner_move_plans: BTreeMap<RangeId, RangeOwnerMovePlan>,
    #[serde(default)]
    completed_owner_move_plans: BTreeMap<RangeId, RangeOwnerMovePlan>,
    #[serde(default)]
    storage_nodes: BTreeMap<StorageNodeId, StorageNodeRecord>,
    #[serde(default)]
    retired_storage_nodes: BTreeSet<StorageNodeId>,
    #[serde(default)]
    draining_storage_nodes: BTreeSet<StorageNodeId>,
    /// Committed self-reported capacity per storage identity. Reports
    /// live in catalog state rather than gossip so eligibility checks
    /// inside apply read identical data on every replica.
    #[serde(default)]
    capacity_reports: BTreeMap<StorageNodeId, CapacityReport>,
    /// Refused incompatible-version reporters — durable gate evidence
    /// bounded by `MAX_INCOMPATIBLE_REPORTS` distinct identities.
    #[serde(default)]
    incompatible_reports: BTreeMap<StorageNodeId, IncompatibleNodeReport>,
    #[serde(default)]
    range_commit_proofs: BTreeMap<RangeId, RangeCommitProof>,
    #[serde(default)]
    applied_requests: BTreeMap<Uuid, ReplicatedCommandResult>,
    /// Append-only administrative audit trail, newest events at the
    /// back, bounded by `AUDIT_EVENT_LIMIT`. Replicated through the
    /// same commit path as every other mutation so all replicas and
    /// snapshots carry identical evidence.
    #[serde(default)]
    audit_events: VecDeque<AuditEvent>,
    /// Next audit ordinal — survives ring eviction so `seq` never
    /// repeats within a catalog lifetime.
    #[serde(default)]
    audit_seq: u64,
}

/// The rollback boundary, enforced at catalog open and snapshot
/// install: a Node asked to read state written by a *newer* schema was
/// rolled back across an irreversible storage migration. It refuses
/// rather than silently misreading fields it cannot interpret.
fn reject_rollback_past_schema(schema_version: u32, path: &Path) -> Result<(), ControlError> {
    if schema_version > CATALOG_SCHEMA_VERSION {
        return Err(ControlError::InvalidOperation(format!(
            "catalog at {} is schema version {schema_version} but this binary supports \
             {CATALOG_SCHEMA_VERSION}: the Node was rolled back across an irreversible \
             catalog migration — restore the newer binary or rebuild state",
            path.display()
        )));
    }
    Ok(())
}

impl CatalogState {
    /// Record one audited command outcome, evicting the oldest rows
    /// once the ring exceeds `AUDIT_EVENT_LIMIT`.
    fn push_audit_event(&mut self, mut event: AuditEvent) {
        self.audit_seq = self.audit_seq.saturating_add(1);
        event.seq = self.audit_seq;
        self.audit_events.push_back(event);
        while self.audit_events.len() > AUDIT_EVENT_LIMIT {
            self.audit_events.pop_front();
        }
    }

    fn migrate_range_metadata(&mut self) {
        for (feed_id, assignment) in &self.active_ranges {
            self.range_maps
                .entry(*feed_id)
                .or_insert_with(|| RangeMap::single(assignment.range_id, assignment.generation));
            self.range_assignments
                .entry(assignment.range_id)
                .or_insert_with(|| assignment.clone());
        }
    }
}

impl Default for CatalogState {
    fn default() -> Self {
        Self {
            schema_version: CATALOG_SCHEMA_VERSION,
            revision: 0,
            spaces: BTreeMap::new(),
            feeds: BTreeMap::new(),
            state_stores: BTreeMap::new(),
            indexes: BTreeMap::new(),
            pipes: BTreeMap::new(),
            writers: BTreeMap::new(),
            readers: BTreeMap::new(),
            subscriptions: BTreeMap::new(),
            subscription_progress_assignments: BTreeMap::new(),
            pending_subscription_cutovers: BTreeMap::new(),
            pipe_compute_leases: BTreeMap::new(),
            riverbed_links: BTreeMap::new(),
            distributaries: BTreeMap::new(),
            distributary_leases: BTreeMap::new(),
            distributary_status: BTreeMap::new(),
            subscription_feed_memberships: BTreeMap::new(),
            state_store_assignments: BTreeMap::new(),
            reader_frontiers: BTreeMap::new(),
            roles: BTreeMap::new(),
            grants: BTreeMap::new(),
            api_keys: BTreeMap::new(),
            history_policies: BTreeMap::new(),
            legal_holds: BTreeMap::new(),
            feed_keys: BTreeMap::new(),
            schemas: BTreeMap::new(),
            active_ranges: BTreeMap::new(),
            range_maps: BTreeMap::new(),
            range_assignments: BTreeMap::new(),
            storage_nodes: BTreeMap::new(),
            retired_storage_nodes: BTreeSet::new(),
            draining_storage_nodes: BTreeSet::new(),
            capacity_reports: BTreeMap::new(),
            incompatible_reports: BTreeMap::new(),
            range_split_plans: BTreeMap::new(),
            range_merge_plans: BTreeMap::new(),
            range_move_plans: BTreeMap::new(),
            completed_move_plans: BTreeMap::new(),
            owner_move_plans: BTreeMap::new(),
            completed_owner_move_plans: BTreeMap::new(),
            range_commit_proofs: BTreeMap::new(),
            applied_requests: BTreeMap::new(),
            audit_events: VecDeque::new(),
            audit_seq: 0,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ResourceKind {
    Domain,
    Space,
    Feed,
    Writer,
    Reader,
    Subscription,
    Role,
    Pipe,
    Index,
    StateStore,
    RiverbedLink,
    Distributary,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ShowKind {
    Domains,
    Spaces,
    Feeds,
    Writers,
    Readers,
    Subscriptions,
    Roles,
    Grants,
    ApiKeys,
    StorageNodes,
    HistoryPolicies,
    LegalHolds,
    Schemas,
    Pipes,
    Indexes,
    RiverbedLinks,
    Distributaries,
    StateStores,
    AuditEvents,
    Compatibility,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "command", rename_all = "snake_case")]
pub enum Command {
    CreateDomain {
        name: String,
    },
    CreateSpace {
        name: String,
    },
    CreateFeed {
        name: String,
        /// `CREATE FEED <name> ENCRYPTED` — mints the Feed's DEK at
        /// creation so every sealed segment is ciphertext.
        #[serde(default)]
        encrypted: bool,
    },
    /// Cryptographic erasure: tombstone the Feed, destroy its DEK, and
    /// purge every range's stored artifacts on all storage Nodes.
    /// Refuses while a legal hold stands or consumers are attached.
    EraseFeed {
        feed: String,
    },
    DefineStateStore {
        name: String,
        source: StateStoreSourceRequest,
        #[serde(default)]
        named_keys: Vec<NamedStateStoreKey>,
    },
    DefineIndex {
        name: String,
        feed: String,
        fields: Vec<IndexFieldSpec>,
        #[serde(default)]
        unique: bool,
        #[serde(default)]
        collation: IndexCollation,
        #[serde(default)]
        consistency: IndexConsistency,
    },
    /// Rebuild one declared Index from retained Feed history into a new
    /// shadow generation; activation is a separate atomic step.
    RebuildIndex {
        name: String,
    },
    /// Rebuild every declared Index whose source Feed lives in the Domain.
    RebuildIndexesInDomain {
        domain: String,
    },
    /// Atomically activate a completed shadow generation. The driver calls
    /// this once its local build caught the committed frontier.
    CompleteIndexBuild {
        index_id: Uuid,
        generation: u64,
        applied_cursor: String,
    },
    /// Data-plane read against an activated Index generation. The catalog
    /// never applies it — the API layer intercepts and answers against the
    /// local (or freshest reachable) materialized postings.
    GetIndex {
        index: String,
        predicate: IndexQueryPredicate,
        #[serde(default)]
        limit: Option<usize>,
        #[serde(default)]
        strict: bool,
        #[serde(default)]
        wait_ms: Option<u64>,
    },
    /// Operator pause: the Pipe stays declared and durable but every
    /// drive skips it until `ResumePipe`. Idempotent — pausing a paused
    /// Pipe is a no-op success.
    PausePipe {
        name: String,
    },
    /// Resume a paused Pipe; resuming a running Pipe is a no-op success.
    ResumePipe {
        name: String,
    },
    DefinePipe {
        name: String,
        subscription: String,
        /// `Forward` target; absent for `Window` Pipes.
        #[serde(default)]
        output_feed: Option<String>,
        /// `Window` target; absent for `Forward` Pipes.
        #[serde(default)]
        output_store: Option<String>,
        /// Miss-quarantine Feed name for `Enrich` Pipes
        /// (`MISS QUARANTINE <feed>`); resolved to
        /// `quarantine_feed_id` at define time.
        #[serde(default)]
        quarantine_feed: Option<String>,
        /// Closed-pane emission Feed name for `Window` Pipes
        /// (`EMIT TO FEED <feed>`); resolved to `output_feed_id` at
        /// define time. Requires `MAX_LATE` or `WATERMARK` — a pane
        /// only provably closes under a declared lateness bound.
        #[serde(default)]
        emit_feed: Option<String>,
        operation: Box<PipeOperation>,
    },
    /// Declare a route to another Riverbed. `key_env` names the
    /// environment variable holding the credential — the replicated
    /// catalog stores the variable name, never the key material.
    DefineRiverbedLink {
        name: String,
        endpoints: Vec<String>,
        key_env: String,
    },
    /// Declare a Distributary: managed copy of the remote `source_feed`
    /// reachable through `link` into local `target_feed`. Drops flow
    /// through the generic `Command::Drop` resource path.
    DefineDistributary {
        name: String,
        link: String,
        source_feed: String,
        target_feed: String,
    },
    MoveDistributaryLease {
        distributary: String,
        new_owner: StorageNodeId,
        #[serde(default)]
        expected_epoch: Option<u64>,
    },
    /// Lease-owner drive report: publishes observed runtime state into
    /// the catalog so `SHOW DISTRIBUTARIES` explains progress and stalls
    /// without tracing Node logs. Fenced to the committed lease — only
    /// the current owner at the current epoch may publish.
    ReportDistributaryStatus {
        distributary_id: Uuid,
        status: DistributaryStatus,
    },
    CreateWriter {
        name: String,
        feed: String,
    },
    OpenWriterSession {
        writer: String,
    },
    AllocateWriterSequence {
        writer: String,
        session_epoch: u64,
    },
    AllocateWriterRangeSequence {
        writer: String,
        session_epoch: u64,
        range_id: RangeId,
    },
    /// Rewind a Writer's per-Range allocation counter after the API heal
    /// path lands a record at the range-reported boundary: sequences
    /// between the boundary and the counter were allocated but never
    /// committed, so the counter must rewind or every later append
    /// repeats the same gap refusal. Applied only when it rewinds — a
    /// counter already at or below `next_sequence` stays put, and an
    /// absent allocation entry is never created.
    RepairWriterRangeSequence {
        writer: String,
        session_epoch: u64,
        range_id: RangeId,
        next_sequence: u64,
    },
    RevokeWriterSession {
        writer: String,
        session_epoch: u64,
    },
    CreateReader {
        name: String,
        feed: String,
        start: ReaderStart,
    },
    CreateSubscription {
        name: String,
        /// Exact-Feed source name; legacy serialized commands carry this
        /// field, so it stays optional rather than renamed.
        #[serde(default)]
        feed: Option<String>,
        /// Domain-wide source: the Subscription covers every Feed in the
        /// named Domain, current and future.
        #[serde(default)]
        domain: Option<String>,
        start: ReaderStart,
        /// Optional per-Subscription delivery holdback in nanoseconds; a
        /// record is eligible only `delivery_delay_ns` after its ingest
        /// timestamp. Legacy commands decode as `None` (immediate).
        #[serde(default)]
        delivery_delay_ns: Option<u64>,
        /// Optional declared retry budget for work claimed by members;
        /// legacy commands decode as `None` (unbounded retries).
        #[serde(default)]
        exhausted_policy: Option<SubscriptionExhaustedPolicy>,
    },
    OpenReaderSession {
        reader: String,
        capacity: usize,
    },
    RecordReaderDelivery {
        reader: String,
        session_epoch: u64,
        cursor: String,
        #[serde(default)]
        fetch_request_id: Option<Uuid>,
        /// The record bound the fetch used — persisted on the Reader so a
        /// duplicate request can replay provably identical records.
        #[serde(default)]
        fetch_limit: Option<usize>,
        /// The retry backoff the page was paced under — persisted so a
        /// replay reports the identical advice on any ingress Node.
        #[serde(default)]
        fetch_retry_after_ms: Option<u64>,
    },
    RecordReaderFrontier {
        reader: String,
        session_epoch: u64,
        cursor: String,
        positions: BTreeMap<RangeId, String>,
        #[serde(default)]
        expected_cursor: Option<String>,
        #[serde(default)]
        fence_delivery: bool,
        #[serde(default)]
        fetch_request_id: Option<Uuid>,
        /// The record bound the fetch used — persisted in the frontier so
        /// a duplicate request can replay provably identical records.
        #[serde(default)]
        fetch_limit: Option<usize>,
        /// The retry backoff the page was paced under — persisted so a
        /// replay reports the identical advice on any ingress Node.
        #[serde(default)]
        fetch_retry_after_ms: Option<u64>,
    },
    AcknowledgeReader {
        reader: String,
        session_epoch: u64,
        cursor: String,
    },
    CloseReaderSession {
        reader: String,
        session_epoch: u64,
    },
    CreateRole {
        name: String,
    },
    Rename {
        kind: ResourceKind,
        current: String,
        new: String,
    },
    Drop {
        kind: ResourceKind,
        name: String,
    },
    Show {
        kind: ShowKind,
        /// Optional row cap — parsed from `SHOW ... LIMIT <n>` for
        /// the bounded audit trail; every other listing ignores it.
        #[serde(default)]
        limit: Option<usize>,
    },
    Describe {
        kind: ResourceKind,
        name: String,
    },
    Grant {
        actions: BTreeSet<PermissionAction>,
        namespace: String,
        role: String,
        /// `deny` rows fence the action ahead of every covering allow.
        /// Legacy commands and replicated entries decode as `allow`.
        #[serde(default)]
        effect: GrantEffect,
    },
    /// Remove the listed actions from the Role's grant rows on an
    /// exact namespace. GRANTed `read, write` followed by REVOKEing
    /// `read` leaves `write` — an exact-subtraction model rather than
    /// all-or-nothing row removal.
    RevokeGrant {
        actions: BTreeSet<PermissionAction>,
        namespace: String,
        role: String,
        /// `deny` revokes from `DENY` rows (`REVOKE DENY ...`);
        /// `allow` from `GRANT` rows. Legacy decode defaults to allow.
        #[serde(default)]
        effect: GrantEffect,
    },
    /// Issue a named API-key identity bound to a Role. `digest` is the
    /// BLAKE3-256 hex verifier the caller computed; the plaintext key
    /// must never appear in a command.
    IssueApiKey {
        name: String,
        digest: String,
        role: String,
        #[serde(default)]
        expires_at_ns: Option<i64>,
    },
    /// Revoke an issued identity durably. Revocation is replicated and
    /// fenced on every Node's request-time authorization view.
    RevokeApiKey {
        name: String,
    },
    /// Upsert the declared History Policy on a Domain or Feed —
    /// field-level override of the inherited policy.
    SetHistoryPolicy {
        scope: PolicyScope,
        policy: HistoryPolicy,
    },
    /// Remove the declared override so the target returns to pure
    /// inheritance (Domains revert to built-in defaults).
    ClearHistoryPolicy {
        scope: PolicyScope,
    },
    /// Declare a legal hold on a Feed: expiry, byte-budget deletion,
    /// cryptographic erasure, and `DROP FEED` all refuse while it
    /// stands. Re-declaring refreshes the reason and timestamp.
    SetLegalHold {
        feed: String,
        reason: Option<String>,
    },
    /// Remove a declared legal hold — the explicit unfreeze step an
    /// operator runs before any destructive operation resumes.
    DropLegalHold {
        feed: String,
    },
    /// Upsert declared record schema metadata on a Domain or Feed —
    /// field-level override of the inherited declaration.
    SetSchema {
        scope: PolicyScope,
        schema: FeedSchema,
    },
    /// Remove the declared schema override so the target returns to
    /// pure inheritance (or schema-agnostic bytes).
    ClearSchema {
        scope: PolicyScope,
    },
    ExplainAccess {
        role: String,
        action: PermissionAction,
        feed: String,
    },
    SeekReader {
        reader: String,
        start: ReaderStart,
    },
    /// Reposition a Subscription's shared frontier. The catalog cannot
    /// resolve Active Range positions, so this command is answered by the
    /// data-plane Subscription seek endpoint, not the catalog handler.
    SeekSubscription {
        name: String,
        start: ReaderStart,
    },
    InspectPlacement {
        feed: String,
    },
    TransferActiveRangeOwnership {
        feed: String,
        owner: StorageNodeId,
    },
    RecoverActiveRangeOwnership {
        feed: String,
        expected_owner: StorageNodeId,
        expected_epoch: OwnershipEpoch,
        new_owner: StorageNodeId,
    },
    PrepareActiveRangeSplit {
        feed: String,
        split_at: KeyToken,
    },
    RecordActiveRangeSplitCatchUp {
        feed: String,
        plan_id: Uuid,
        source_commit: CommitPosition,
        source_scanned_through: CommitPosition,
        right_commit: CommitPosition,
        checksum_verified: bool,
    },
    ActivateActiveRangeSplit {
        feed: String,
        plan_id: Uuid,
        left_writer_sequences: Vec<StagedWriterSequence>,
        right_writer_sequences: Vec<StagedWriterSequence>,
        #[serde(default)]
        reader_translations: Vec<ReaderFrontierTranslation>,
        /// Translated Subscription frontiers committed as pending cutover
        /// evidence with this activation; empty for logs written before
        /// Subscription translations rode the command.
        #[serde(default)]
        subscription_translations: Vec<SubscriptionCutoverTranslation>,
    },
    PrepareActiveRangeMerge {
        feed: String,
        left_range_id: RangeId,
        right_range_id: RangeId,
    },
    RecordActiveRangeMergeStaging {
        feed: String,
        plan_id: Uuid,
        left_commit: CommitPosition,
        right_commit: CommitPosition,
        merged_commit: CommitPosition,
        checksum_verified: bool,
    },
    ActivateActiveRangeMerge {
        feed: String,
        plan_id: Uuid,
        writer_sequences: Vec<StagedWriterSequence>,
        #[serde(default)]
        reader_translations: Vec<ReaderFrontierTranslation>,
        /// Translated Subscription frontiers committed as pending cutover
        /// evidence with this activation; empty for logs written before
        /// Subscription translations rode the command.
        #[serde(default)]
        subscription_translations: Vec<SubscriptionCutoverTranslation>,
    },
    /// Abandons a staged split plan after its activation path failed, so a
    /// later attempt can re-prepare instead of blocking on
    /// `range_operation_pending` forever. `plan_id` fences the removal so a
    /// stale abandon cannot delete a newer plan.
    AbandonActiveRangeSplit {
        feed_id: Uuid,
        plan_id: Uuid,
    },
    /// Abandons a staged merge plan under the same fencing as
    /// `AbandonActiveRangeSplit`.
    AbandonActiveRangeMerge {
        feed_id: Uuid,
        plan_id: Uuid,
    },
    /// Abandons a staged follower-move plan after its cutover path failed,
    /// under the same `feed_id`/`plan_id` fencing.
    AbandonFollowerMove {
        feed_id: Uuid,
        plan_id: Uuid,
    },
    /// Abandons a staged owner-move plan after its cutover path failed,
    /// under the same `feed_id`/`plan_id` fencing.
    AbandonOwnerMove {
        feed_id: Uuid,
        plan_id: Uuid,
    },
    /// Removes one pending Subscription frontier translation after the
    /// separate progress quorum committed (or already covered) it. The
    /// `plan_id` guard prevents a stale clear from deleting a newer
    /// transition's evidence.
    ClearSubscriptionCutover {
        subscription_id: Uuid,
        plan_id: Uuid,
    },
    PrepareFollowerMove {
        feed: String,
        range_id: RangeId,
        removed_replica: StorageNodeId,
        replacement_replica: StorageNodeId,
    },
    RecordFollowerMoveCatchUp {
        feed: String,
        plan_id: Uuid,
        source_commit: CommitPosition,
        target_commit: CommitPosition,
        checksum_verified: bool,
    },
    ActivateFollowerMove {
        feed: String,
        plan_id: Uuid,
    },
    AbortFollowerMove {
        feed: String,
        plan_id: Uuid,
    },
    PrepareOwnerMove {
        feed: String,
        range_id: RangeId,
        new_owner: StorageNodeId,
    },
    RecordOwnerMoveCatchUp {
        feed: String,
        plan_id: Uuid,
        source_commit: CommitPosition,
        target_commit: CommitPosition,
        checksum_verified: bool,
    },
    ActivateOwnerMove {
        feed: String,
        plan_id: Uuid,
    },
    AbortOwnerMove {
        feed: String,
        plan_id: Uuid,
    },
    /// Advance the durable committed-frontier proof for a Range.
    /// Internal: reported by the Append Owner after a quorum commit.
    RecordRangeCommitProof {
        feed_id: Uuid,
        range_id: RangeId,
        position: CommitPosition,
        frame_digest: [u8; 32],
        evidence: Vec<StorageNodeId>,
        reporter: StorageNodeId,
    },
    RecoverSubscriptionProgressOwner {
        subscription_id: Uuid,
        expected_ownership_epoch: u64,
        new_owner: StorageNodeId,
    },
    MoveSubscriptionProgressReplica {
        subscription_id: Uuid,
        expected_ownership_epoch: u64,
        replaced: StorageNodeId,
        replacement: StorageNodeId,
    },
    /// Recover a dead StateStore journal owner onto a surviving replica:
    /// the committed mutation prefix already lives on the replica quorum
    /// and the new owner catches up through the periodic replica catch-up
    /// pass, so the CAS carries placement fencing only. Mutations allocate
    /// their sequence on the owner, which makes owner liveness a
    /// write-availability precondition.
    RecoverStateStoreOwner {
        store_id: Uuid,
        expected_ownership_epoch: u64,
        new_owner: StorageNodeId,
    },
    /// Move a Pipe's epoch-fenced compute lease to another
    /// storage-eligible Node. `expected_compute_epoch` fences stale
    /// drain plans; operator moves through `MOVE PIPE COMPUTE` leave it
    /// unset and take the current lease unconditionally.
    MovePipeComputeLease {
        pipe: String,
        new_owner: StorageNodeId,
        #[serde(default)]
        expected_compute_epoch: Option<u64>,
    },
    RegisterStorageNode {
        node: StorageNodeId,
        endpoint: String,
        #[serde(default)]
        cert_pins: BTreeSet<String>,
        #[serde(default)]
        capabilities: NodeCapabilities,
    },
    RetireStorageNode {
        node: StorageNodeId,
    },
    DrainStorageNode {
        node: StorageNodeId,
    },
    UndrainStorageNode {
        node: StorageNodeId,
    },
    InspectStorageDrain {
        node: StorageNodeId,
    },
    /// A Node's self-reported measured capacity, committed so every
    /// replica's eligibility evaluation reads identical data.
    ReportStorageNodeCapacity {
        node: StorageNodeId,
        capacity: NodeCapacity,
        /// The reporter's claimed protocol level — `0` when the
        /// reporting binary predates version advertisement.
        #[serde(default)]
        protocol_version: u32,
        /// The reporter's claimed package version.
        #[serde(default)]
        build: String,
    },
}

impl Command {
    pub fn is_read_only(&self) -> bool {
        matches!(
            self,
            Self::Show { .. }
                | Self::Describe { .. }
                | Self::ExplainAccess { .. }
                | Self::InspectPlacement { .. }
                | Self::InspectStorageDrain { .. }
                | Self::GetIndex { .. }
        )
    }

    /// Whether the command is an administrative mutation worth an audit
    /// event. The audit trail excludes read-only commands and the
    /// high-frequency data-plane bookkeeping (session open/close,
    /// per-batch sequence allocation, reader progress, catch-up
    /// progress records, and capacity heartbeats) that would churn the
    /// bounded ring without telling an operator anything new —
    /// lifecycle, security, policy, and movement commands all record.
    pub fn is_audited(&self) -> bool {
        !matches!(
            self,
            Self::Show { .. }
                | Self::Describe { .. }
                | Self::ExplainAccess { .. }
                | Self::InspectPlacement { .. }
                | Self::InspectStorageDrain { .. }
                | Self::GetIndex { .. }
                | Self::OpenWriterSession { .. }
                | Self::AllocateWriterSequence { .. }
                | Self::AllocateWriterRangeSequence { .. }
                | Self::RepairWriterRangeSequence { .. }
                | Self::OpenReaderSession { .. }
                | Self::RecordReaderDelivery { .. }
                | Self::RecordReaderFrontier { .. }
                | Self::AcknowledgeReader { .. }
                | Self::CloseReaderSession { .. }
                | Self::RecordRangeCommitProof { .. }
                | Self::RecordActiveRangeSplitCatchUp { .. }
                | Self::RecordActiveRangeMergeStaging { .. }
                | Self::RecordFollowerMoveCatchUp { .. }
                | Self::RecordOwnerMoveCatchUp { .. }
                | Self::ReportStorageNodeCapacity { .. }
        )
    }

    /// The primary resource an audited command acts on, for display.
    fn audit_target(&self) -> Option<String> {
        match self {
            Self::CreateDomain { name }
            | Self::CreateSpace { name }
            | Self::CreateFeed { name, .. }
            | Self::EraseFeed { feed: name }
            | Self::DefineStateStore { name, .. }
            | Self::DefineIndex { name, .. }
            | Self::RebuildIndex { name }
            | Self::RebuildIndexesInDomain { domain: name }
            | Self::PausePipe { name }
            | Self::ResumePipe { name }
            | Self::DefinePipe { name, .. }
            | Self::CreateWriter { name, .. }
            | Self::RevokeWriterSession { writer: name, .. }
            | Self::CreateReader { name, .. }
            | Self::CreateSubscription { name, .. }
            | Self::CreateRole { name }
            | Self::Grant { role: name, .. }
            | Self::RevokeGrant { role: name, .. }
            | Self::IssueApiKey { name, .. }
            | Self::RevokeApiKey { name }
            | Self::Drop { name, .. }
            | Self::Rename { current: name, .. }
            | Self::SetLegalHold { feed: name, .. }
            | Self::DropLegalHold { feed: name }
            | Self::SeekReader { reader: name, .. }
            | Self::SeekSubscription { name, .. }
            | Self::TransferActiveRangeOwnership { feed: name, .. }
            | Self::RecoverActiveRangeOwnership { feed: name, .. }
            | Self::PrepareActiveRangeSplit { feed: name, .. }
            | Self::ActivateActiveRangeSplit { feed: name, .. }
            | Self::PrepareActiveRangeMerge { feed: name, .. }
            | Self::ActivateActiveRangeMerge { feed: name, .. }
            | Self::PrepareFollowerMove { feed: name, .. }
            | Self::ActivateFollowerMove { feed: name, .. }
            | Self::AbortFollowerMove { feed: name, .. }
            | Self::PrepareOwnerMove { feed: name, .. }
            | Self::ActivateOwnerMove { feed: name, .. }
            | Self::AbortOwnerMove { feed: name, .. }
            | Self::MovePipeComputeLease { pipe: name, .. }
            | Self::DefineRiverbedLink { name, .. }
            | Self::DefineDistributary { name, .. }
            | Self::MoveDistributaryLease {
                distributary: name, ..
            } => Some(name.clone()),
            Self::SetHistoryPolicy { scope, .. }
            | Self::ClearHistoryPolicy { scope }
            | Self::SetSchema { scope, .. }
            | Self::ClearSchema { scope } => Some(scope.target_name().to_owned()),
            Self::CompleteIndexBuild { index_id, .. }
            | Self::AbandonActiveRangeSplit {
                feed_id: index_id, ..
            }
            | Self::AbandonActiveRangeMerge {
                feed_id: index_id, ..
            }
            | Self::AbandonFollowerMove {
                feed_id: index_id, ..
            }
            | Self::AbandonOwnerMove {
                feed_id: index_id, ..
            }
            | Self::ClearSubscriptionCutover {
                subscription_id: index_id,
                ..
            }
            | Self::RecoverSubscriptionProgressOwner {
                subscription_id: index_id,
                ..
            }
            | Self::MoveSubscriptionProgressReplica {
                subscription_id: index_id,
                ..
            }
            | Self::RecoverStateStoreOwner {
                store_id: index_id, ..
            } => Some(index_id.to_string()),
            Self::RegisterStorageNode { node, .. }
            | Self::RetireStorageNode { node }
            | Self::DrainStorageNode { node }
            | Self::UndrainStorageNode { node } => Some(node.to_string()),
            _ => None,
        }
    }

    /// Secondary audit context where a single target name cannot carry
    /// the command's meaning — grant namespaces, rename destinations,
    /// legal-hold reasons.
    fn audit_detail(&self) -> Option<String> {
        match self {
            Self::Grant { namespace, .. } | Self::RevokeGrant { namespace, .. } => {
                Some(namespace.clone())
            }
            Self::Rename { new, .. } => Some(new.clone()),
            Self::SetLegalHold { reason, .. } => reason.clone(),
            _ => None,
        }
    }

    /// Whether the command's own data carries its idempotency: an
    /// advance-only committed-frontier proof can re-apply harmlessly
    /// (same position and digest is a no-op, a conflicting digest must
    /// refuse), so caching its response in the unbounded
    /// `applied_requests` map would grow the catalog without bound under
    /// sustained appends.
    pub fn caches_applied_response(&self) -> bool {
        !matches!(
            self,
            Self::RecordRangeCommitProof { .. } | Self::ReportStorageNodeCapacity { .. }
        )
    }

    pub fn requires_internal_replica_authority(&self) -> bool {
        matches!(
            self,
            Self::RecordReaderFrontier { .. }
                | Self::TransferActiveRangeOwnership { .. }
                | Self::RecoverActiveRangeOwnership { .. }
                | Self::RecordActiveRangeSplitCatchUp { .. }
                | Self::ActivateActiveRangeSplit { .. }
                | Self::RecordActiveRangeMergeStaging { .. }
                | Self::ActivateActiveRangeMerge { .. }
                | Self::AbandonActiveRangeSplit { .. }
                | Self::AbandonActiveRangeMerge { .. }
                | Self::AbandonFollowerMove { .. }
                | Self::AbandonOwnerMove { .. }
                | Self::RecordRangeCommitProof { .. }
                | Self::ClearSubscriptionCutover { .. }
                | Self::RecordFollowerMoveCatchUp { .. }
                | Self::ActivateFollowerMove { .. }
                | Self::PrepareOwnerMove { .. }
                | Self::RecordOwnerMoveCatchUp { .. }
                | Self::ActivateOwnerMove { .. }
                | Self::AbortOwnerMove { .. }
                | Self::RecoverSubscriptionProgressOwner { .. }
                | Self::MoveSubscriptionProgressReplica { .. }
                | Self::RecoverStateStoreOwner { .. }
        )
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FixedActiveRangePlacement {
    pub owner: StorageNodeId,
    pub replicas: [StorageNodeId; ACTIVE_RANGE_REPLICA_COUNT],
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FixedSubscriptionProgressPlacement {
    pub owner: StorageNodeId,
    pub replicas: [StorageNodeId; ACTIVE_RANGE_REPLICA_COUNT],
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FixedStateStorePlacement {
    pub owner: StorageNodeId,
    pub replicas: [StorageNodeId; ACTIVE_RANGE_REPLICA_COUNT],
}

/// Consensus-prepared replica placements computed before a command is
/// committed, so every replica applies identical assignments.
#[derive(Clone, Debug, Default)]
pub struct FixedPlacements {
    pub active_range: Option<FixedActiveRangePlacement>,
    pub subscription_progress: Option<FixedSubscriptionProgressPlacement>,
    pub state_store: Option<FixedStateStorePlacement>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicatedCommand {
    pub request_id: Uuid,
    pub issued_at_ns: i64,
    pub command: Command,
    /// The authenticated caller recorded at propose time — stamped
    /// once so every replica audits the same identity. Replicated
    /// entries predating the field decode as `Unknown`.
    #[serde(default)]
    pub actor: AuditActor,
    #[serde(default)]
    pub fixed_active_range: Option<FixedActiveRangePlacement>,
    #[serde(default)]
    pub fixed_subscription_progress: Option<FixedSubscriptionProgressPlacement>,
    #[serde(default)]
    pub fixed_state_store: Option<FixedStateStorePlacement>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ReplicatedCommandResult {
    pub revision: u64,
    pub result: Option<StatementResult>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct StatementResult {
    pub statement: String,
    pub message: String,
    pub data: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ControlExecution {
    pub request_id: Uuid,
    pub revision: u64,
    pub authority: String,
    pub warning: String,
    pub results: Vec<StatementResult>,
}

#[derive(Debug, Error)]
pub enum ControlError {
    #[error("WCL syntax error: {0}")]
    Syntax(String),
    #[error("invalid name: {0}")]
    InvalidName(String),
    #[error("resource already exists: {0}")]
    AlreadyExists(String),
    #[error("resource not found: {0}")]
    NotFound(String),
    #[error("operation is not allowed: {0}")]
    InvalidOperation(String),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Serialization(#[from] serde_json::Error),
}

impl ControlError {
    pub fn is_client_error(&self) -> bool {
        matches!(
            self,
            Self::Syntax(_)
                | Self::InvalidName(_)
                | Self::AlreadyExists(_)
                | Self::NotFound(_)
                | Self::InvalidOperation(_)
        )
    }
}

#[derive(Clone)]
pub struct ControlController {
    path: Arc<PathBuf>,
    state: Arc<Mutex<CatalogState>>,
    eligible_storage_nodes: Arc<BTreeSet<StorageNodeId>>,
    capacity_policy: CapacityPolicy,
    /// Request-time authorization view of issued API-key digests,
    /// recomputed from catalog state on load, snapshot install, and
    /// every applied command so leader and replicas converge.
    issued_api_keys: Arc<std::sync::RwLock<IssuedApiKeySet>>,
    synced_replica: bool,
}

impl ControlController {
    pub fn open(path: impl Into<PathBuf>, store: Arc<dyn LogStore>) -> Result<Self, ControlError> {
        Self::open_with_storage_nodes(path, store, Vec::new())
    }

    pub fn open_with_storage_nodes(
        path: impl Into<PathBuf>,
        _store: Arc<dyn LogStore>,
        eligible_storage_nodes: Vec<StorageNodeId>,
    ) -> Result<Self, ControlError> {
        let path = path.into();
        let mut state = if path.exists() {
            let state: CatalogState = serde_json::from_slice(&fs::read(&path)?)?;
            reject_rollback_past_schema(state.schema_version, &path)?;
            state
        } else {
            CatalogState::default()
        };
        state.migrate_range_metadata();
        let issued_api_keys = issued_api_key_view(&state);
        Ok(Self {
            path: Arc::new(path),
            state: Arc::new(Mutex::new(state)),
            eligible_storage_nodes: Arc::new(eligible_storage_nodes.into_iter().collect()),
            capacity_policy: CapacityPolicy::default(),
            issued_api_keys: Arc::new(std::sync::RwLock::new(issued_api_keys)),
            synced_replica: false,
        })
    }

    /// Install the shared minimum-baseline policy. Static config, like
    /// `eligible_storage_nodes`: every Node derives eligibility locally,
    /// so the policy must be identical cluster-wide.
    pub fn with_capacity_policy(mut self, policy: CapacityPolicy) -> Self {
        self.capacity_policy = policy;
        self
    }

    /// Marks this catalog as a synced replica: a Node without a Control
    /// Plane voter role holds replicated state for fencing and lookup but
    /// must never mutate it locally, since local writes would diverge
    /// from majority-committed authority and be silently reverted by the
    /// next snapshot sync.
    pub fn into_synced_replica(mut self) -> Self {
        self.synced_replica = true;
        self
    }

    pub fn is_synced_replica(&self) -> bool {
        self.synced_replica
    }

    pub async fn prepare_replicated(
        &self,
        actor: AuditActor,
        request_id: Uuid,
        issued_at_ns: i64,
        command: Command,
    ) -> Result<ReplicatedCommand, ControlError> {
        let state = self.state.lock().await;
        if let Command::PrepareFollowerMove {
            replacement_replica,
            ..
        } = &command
        {
            if !self.storage_eligible(&state, replacement_replica, issued_at_ns) {
                return Err(ControlError::InvalidOperation(format!(
                    "replacement Node {replacement_replica} is not eligible for storage"
                )));
            }
        }
        let fixed_active_range = matches!(
            &command,
            Command::CreateFeed { .. } | Command::PrepareActiveRangeSplit { .. }
        )
        .then(|| {
            let seed = match &command {
                Command::PrepareActiveRangeSplit { .. } => {
                    derived_resource_id(request_id, "active-range-split-right")
                }
                _ => derived_resource_id(request_id, "active-range"),
            };
            self.select_fixed_active_range(&state, &seed, issued_at_ns)
        })
        .transpose()?;
        let fixed_subscription_progress = matches!(&command, Command::CreateSubscription { .. })
            .then(|| {
                self.select_fixed_subscription_progress(
                    &state,
                    derived_resource_id(request_id, "subscription"),
                    issued_at_ns,
                )
            })
            .transpose()?;
        let fixed_state_store = matches!(&command, Command::DefineStateStore { .. })
            .then(|| {
                self.select_fixed_state_store(
                    &state,
                    derived_resource_id(request_id, "state-store"),
                    issued_at_ns,
                )
            })
            .transpose()?;
        drop(state);
        Ok(ReplicatedCommand {
            request_id,
            issued_at_ns,
            command,
            actor,
            fixed_active_range,
            fixed_subscription_progress,
            fixed_state_store,
        })
    }

    fn storage_pool(&self, state: &CatalogState, now_ns: i64) -> BTreeSet<StorageNodeId> {
        self.eligible_storage_nodes
            .iter()
            .chain(
                state
                    .storage_nodes
                    .iter()
                    .filter(|(_, record)| record.capabilities.storage)
                    .map(|(node, _)| node),
            )
            .filter(|node| !state.retired_storage_nodes.contains(*node))
            .filter(|node| !state.draining_storage_nodes.contains(*node))
            .filter(|node| self.capacity_exclusion(state, node, now_ns).is_none())
            .cloned()
            .collect()
    }

    /// The capacity-policy reason a Node is excluded, if any. `now_ns`
    /// is the evaluation instant: committed `issued_at_ns` inside apply
    /// (deterministic across replicas) and wall time on advisory reads.
    fn capacity_exclusion(
        &self,
        state: &CatalogState,
        node: &StorageNodeId,
        now_ns: i64,
    ) -> Option<StorageExclusion> {
        capacity_exclusion(state, node, &self.capacity_policy, now_ns)
    }

    fn storage_eligible(&self, state: &CatalogState, node: &StorageNodeId, now_ns: i64) -> bool {
        (self.eligible_storage_nodes.contains(node) || state.storage_nodes.contains_key(node))
            && !state.retired_storage_nodes.contains(node)
            && !state.draining_storage_nodes.contains(node)
            && state
                .storage_nodes
                .get(node)
                .is_none_or(|record| record.capabilities.storage)
            && self.capacity_exclusion(state, node, now_ns).is_none()
    }

    pub async fn storage_drain_plan(
        &self,
        node: &StorageNodeId,
    ) -> Result<StorageDrainPlan, ControlError> {
        let state = self.state.lock().await;
        let known =
            state.storage_nodes.contains_key(node) || self.eligible_storage_nodes.contains(node);
        if !known {
            return Err(ControlError::NotFound(format!("storage Node {node}")));
        }
        Ok(storage_drain_plan(&state, self, node))
    }

    pub async fn draining_storage_nodes(&self) -> BTreeSet<StorageNodeId> {
        self.state.lock().await.draining_storage_nodes.clone()
    }

    /// Every currently storage-eligible Node (configured voters plus
    /// registered storage Nodes, minus retired/draining/capacity-excluded)
    /// under the wall clock — advisory placement pools like Pipe compute
    /// lease recovery candidates use this; committed apply still
    /// re-validates eligibility against `issued_at_ns`.
    pub async fn storage_pool_nodes(&self) -> BTreeSet<StorageNodeId> {
        let now_ns = unix_ns();
        let state = self.state.lock().await;
        self.storage_pool(&state, now_ns)
    }

    /// Endpoints of every independently registered storage Node.
    pub async fn registered_storage_node_endpoints(&self) -> BTreeMap<StorageNodeId, String> {
        self.state
            .lock()
            .await
            .storage_nodes
            .iter()
            .map(|(node, record)| (node.clone(), record.endpoint.clone()))
            .collect()
    }

    /// Internal endpoint for an independently registered storage Node.
    /// Configured voter endpoints are resolved separately and take priority.
    pub async fn storage_node_endpoint(&self, node: &StorageNodeId) -> Option<String> {
        self.state
            .lock()
            .await
            .storage_nodes
            .get(node)
            .map(|record| record.endpoint.clone())
    }

    /// Registered storage Node allowed to present the given certificate
    /// fingerprint on the internal mTLS plane.
    pub async fn storage_node_by_cert_pin(&self, fingerprint: &[u8; 32]) -> Option<StorageNodeId> {
        let hex = blake3::Hash::from(*fingerprint).to_hex().to_string();
        self.state
            .lock()
            .await
            .storage_nodes
            .values()
            .find_map(|record| record.cert_pins.contains(&hex).then(|| record.node.clone()))
    }

    /// In-flight Active Range movement plans (follower replacements and owner
    /// transfers) that have been prepared but not yet activated. Drain
    /// scheduling treats this count as the movement disruption budget.
    pub async fn pending_move_plan_count(&self) -> usize {
        let state = self.state.lock().await;
        state.range_move_plans.len() + state.owner_move_plans.len()
    }

    fn select_fixed_active_range(
        &self,
        state: &CatalogState,
        seed: &Uuid,
        now_ns: i64,
    ) -> Result<FixedActiveRangePlacement, ControlError> {
        let mut ranked = self
            .storage_pool(state, now_ns)
            .into_iter()
            .map(|node| {
                let mut hash = blake3::Hasher::new();
                hash.update(b"whitewater-active-range-v1");
                hash.update(seed.as_bytes());
                hash.update(node.as_str().as_bytes());
                (*hash.finalize().as_bytes(), node)
            })
            .collect::<Vec<_>>();
        ranked.sort_unstable_by(|left, right| {
            right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1))
        });
        let replicas = ranked
            .into_iter()
            .take(ACTIVE_RANGE_REPLICA_COUNT)
            .map(|(_, node)| node)
            .collect::<Vec<_>>();
        let replicas: [StorageNodeId; ACTIVE_RANGE_REPLICA_COUNT] = replicas.try_into().map_err(
            |replicas: Vec<StorageNodeId>| {
                ControlError::InvalidOperation(format!(
                    "Feed creation requires at least three eligible storage Nodes; only {} are configured",
                    replicas.len()
                ))
            },
        )?;
        Ok(FixedActiveRangePlacement {
            owner: replicas[0].clone(),
            replicas,
        })
    }

    fn select_fixed_subscription_progress(
        &self,
        state: &CatalogState,
        subscription_id: Uuid,
        now_ns: i64,
    ) -> Result<FixedSubscriptionProgressPlacement, ControlError> {
        let mut ranked = self
            .storage_pool(state, now_ns)
            .into_iter()
            .map(|node| {
                let mut hash = blake3::Hasher::new();
                hash.update(b"whitewater-subscription-progress-v1");
                hash.update(subscription_id.as_bytes());
                hash.update(node.as_str().as_bytes());
                (*hash.finalize().as_bytes(), node)
            })
            .collect::<Vec<_>>();
        ranked.sort_unstable_by(|left, right| {
            right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1))
        });
        let replicas: [StorageNodeId; ACTIVE_RANGE_REPLICA_COUNT] = ranked.into_iter()
            .take(ACTIVE_RANGE_REPLICA_COUNT).map(|(_, node)| node).collect::<Vec<_>>()
            .try_into().map_err(|replicas: Vec<StorageNodeId>| {
                ControlError::InvalidOperation(format!(
                    "Subscription progress requires at least three eligible storage Nodes; only {} are configured",
                    replicas.len(),
                ))
            })?;
        Ok(FixedSubscriptionProgressPlacement {
            owner: replicas[0].clone(),
            replicas,
        })
    }

    fn select_fixed_state_store(
        &self,
        state: &CatalogState,
        store_id: Uuid,
        now_ns: i64,
    ) -> Result<FixedStateStorePlacement, ControlError> {
        let mut ranked = self
            .storage_pool(state, now_ns)
            .into_iter()
            .map(|node| {
                let mut hash = blake3::Hasher::new();
                hash.update(b"whitewater-state-store-v1");
                hash.update(store_id.as_bytes());
                hash.update(node.as_str().as_bytes());
                (*hash.finalize().as_bytes(), node)
            })
            .collect::<Vec<_>>();
        ranked.sort_unstable_by(|left, right| {
            right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1))
        });
        let replicas: [StorageNodeId; ACTIVE_RANGE_REPLICA_COUNT] = ranked.into_iter()
            .take(ACTIVE_RANGE_REPLICA_COUNT).map(|(_, node)| node).collect::<Vec<_>>()
            .try_into().map_err(|replicas: Vec<StorageNodeId>| {
                ControlError::InvalidOperation(format!(
                    "StateStore requires at least three eligible storage Nodes; only {} are configured",
                    replicas.len(),
                ))
            })?;
        Ok(FixedStateStorePlacement {
            owner: replicas[0].clone(),
            replicas,
        })
    }

    pub async fn execute(&self, script: &str) -> Result<ControlExecution, ControlError> {
        self.execute_with_request_id(script, Uuid::new_v4()).await
    }

    pub async fn execute_with_request_id(
        &self,
        script: &str,
        request_id: Uuid,
    ) -> Result<ControlExecution, ControlError> {
        self.execute_commands_with_request_id(parse_wcl(script)?, request_id)
            .await
    }

    pub async fn execute_commands(
        &self,
        commands: Vec<Command>,
    ) -> Result<ControlExecution, ControlError> {
        self.execute_commands_with_request_id(commands, Uuid::new_v4())
            .await
    }

    pub async fn execute_commands_with_request_id(
        &self,
        commands: Vec<Command>,
        request_id: Uuid,
    ) -> Result<ControlExecution, ControlError> {
        self.execute_commands_as(AuditActor::System, commands, request_id)
            .await
    }

    /// Execute commands on behalf of a known caller — the actor rides
    /// the replicated command so every replica audits the same
    /// identity. Node-internal producers keep `AuditActor::System`.
    pub async fn execute_commands_as(
        &self,
        actor: AuditActor,
        commands: Vec<Command>,
        request_id: Uuid,
    ) -> Result<ControlExecution, ControlError> {
        if commands.is_empty() {
            return Err(ControlError::Syntax(
                "request contains no commands".to_owned(),
            ));
        }
        if self.synced_replica && commands.iter().any(|command| !command.is_read_only()) {
            return Err(ControlError::InvalidOperation(
                "catalog is a synced replica; submit mutations through a Control Plane voter"
                    .to_owned(),
            ));
        }
        let mut results = Vec::with_capacity(commands.len());
        let mut revision = self.revision().await;
        let issued_at_ns = unix_ns();
        for (index, command) in commands.into_iter().enumerate() {
            if command.is_read_only() {
                results.push(self.execute_query(command).await?);
                revision = self.revision().await;
                continue;
            }
            let replicated = self
                .prepare_replicated(
                    actor.clone(),
                    derive_command_request_id(request_id, index),
                    issued_at_ns,
                    command,
                )
                .await?;
            let response = self.apply_replicated(replicated).await;
            revision = response.revision;
            match (response.result, response.error) {
                (Some(result), None) => results.push(result),
                (_, Some(error)) => return Err(ControlError::InvalidOperation(error)),
                _ => {
                    return Err(ControlError::InvalidOperation(
                        "control command returned no result".to_owned(),
                    ))
                }
            }
        }
        Ok(ControlExecution {
            request_id,
            revision,
            authority: "local_prototype".to_owned(),
            warning: "Catalog state is local to this Node and is not replicated authority until the Control Plane is implemented.".to_owned(),
            results,
        })
    }

    pub async fn execute_query(&self, command: Command) -> Result<StatementResult, ControlError> {
        if !command.is_read_only() {
            return Err(ControlError::InvalidOperation(
                "execute_query accepts only SHOW, DESCRIBE, EXPLAIN ACCESS, and INSPECT PLACEMENT"
                    .to_owned(),
            ));
        }
        let statement = command_label(&command);
        let mut state = self.state.lock().await;
        let (message, data) = self
            .apply(
                &mut state,
                command,
                Uuid::nil(),
                0,
                FixedPlacements::default(),
            )
            .await?;
        Ok(StatementResult {
            statement,
            message,
            data,
        })
    }

    pub async fn apply_replicated(&self, request: ReplicatedCommand) -> ReplicatedCommandResult {
        let mut state = self.state.lock().await;
        if let Some(previous) = state.applied_requests.get(&request.request_id) {
            return previous.clone();
        }
        let previous_state = state.clone();
        let revision = state.revision.saturating_add(1);
        let ReplicatedCommand {
            request_id,
            issued_at_ns,
            command,
            actor,
            fixed_active_range,
            fixed_subscription_progress,
            fixed_state_store,
        } = request;
        let statement = command_label(&command);
        let audited = command.is_audited();
        let audit_action = audited.then(|| statement.clone());
        let audit_target = audited.then(|| command.audit_target()).flatten();
        let audit_detail = audited.then(|| command.audit_detail()).flatten();
        let cache_response = command.caches_applied_response();
        let response = match self
            .apply(
                &mut state,
                command,
                request_id,
                issued_at_ns,
                FixedPlacements {
                    active_range: fixed_active_range,
                    subscription_progress: fixed_subscription_progress,
                    state_store: fixed_state_store,
                },
            )
            .await
        {
            Ok((message, data)) => {
                state.revision = revision;
                ReplicatedCommandResult {
                    revision,
                    result: Some(StatementResult {
                        statement,
                        message,
                        data,
                    }),
                    error: None,
                }
            }
            Err(error) => ReplicatedCommandResult {
                revision: state.revision,
                result: None,
                error: Some(error.to_string()),
            },
        };
        if let Some(action) = audit_action {
            // The event lands in state before persist_state so a failed
            // write rolls the audit row back with the mutation it would
            // otherwise claim happened — evidence never outlives the
            // change it records.
            let outcome = if response.error.is_none() {
                AuditOutcome::Committed
            } else {
                AuditOutcome::Refused {
                    error: response.error.clone().unwrap_or_default(),
                }
            };
            state.push_audit_event(AuditEvent {
                seq: 0,
                revision: response.revision,
                at_ns: issued_at_ns,
                request_id,
                actor,
                action,
                target: audit_target,
                detail: audit_detail,
                outcome,
            });
        }
        if cache_response {
            state.applied_requests.insert(request_id, response.clone());
        }
        if let Err(error) = persist_state(&self.path, &state) {
            *state = previous_state;
            // The view must follow the restored state, never the
            // rolled-back attempt — a failed persist must not leave a
            // phantom issued key authorized.
            self.refresh_issued_api_keys(&state);
            return ReplicatedCommandResult {
                revision: state.revision,
                result: None,
                error: Some(error.to_string()),
            };
        }
        // Authorization view follows committed catalog state.
        self.refresh_issued_api_keys(&state);
        response
    }

    /// The directory this controller's catalog file sits in — the
    /// Node's data directory. Used by backup to copy the whole tree the
    /// catalog describes.
    pub fn data_dir(&self) -> PathBuf {
        self.path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| self.path.as_ref().clone())
    }

    pub async fn snapshot_bytes(&self) -> Result<Vec<u8>, ControlError> {
        Ok(serde_json::to_vec(&*self.state.lock().await)?)
    }

    pub async fn install_snapshot_bytes(&self, bytes: &[u8]) -> Result<(), ControlError> {
        let mut state: CatalogState = serde_json::from_slice(bytes)?;
        reject_rollback_past_schema(state.schema_version, &self.path)?;
        state.migrate_range_metadata();
        persist_state(&self.path, &state)?;
        let issued_api_keys = issued_api_key_view(&state);
        *self.state.lock().await = state;
        if let Ok(mut guard) = self.issued_api_keys.write() {
            *guard = issued_api_keys;
        }
        Ok(())
    }

    pub async fn revision(&self) -> u64 {
        self.state.lock().await.revision
    }

    /// Read access to the issued API-key authorization view. The guard
    /// is held only for the bounded digest lookup at request time.
    pub fn issued_api_keys(&self) -> std::sync::RwLockReadGuard<'_, IssuedApiKeySet> {
        self.issued_api_keys
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Recompute the request-time authorization view from catalog
    /// state. Called after every apply so issued/revoked/expired-key
    /// handling is identical on leader and replicas.
    fn refresh_issued_api_keys(&self, state: &CatalogState) {
        let view = issued_api_key_view(state);
        if let Ok(mut guard) = self.issued_api_keys.write() {
            *guard = view;
        }
    }

    pub async fn active_reader_by_id(&self, reader_id: Uuid) -> Option<ReaderDefinition> {
        self.state
            .lock()
            .await
            .readers
            .get(&reader_id)
            .filter(|reader| reader.status == ResourceStatus::Active)
            .cloned()
    }

    pub async fn active_reader_by_name(&self, name: &str) -> Option<ReaderDefinition> {
        self.state
            .lock()
            .await
            .readers
            .values()
            .find(|reader| reader.name == name && reader.status == ResourceStatus::Active)
            .cloned()
    }

    /// The declared Subscription by immutable id.
    pub async fn active_subscription_by_id(
        &self,
        subscription_id: Uuid,
    ) -> Option<SubscriptionDefinition> {
        self.state
            .lock()
            .await
            .subscriptions
            .values()
            .find(|subscription| {
                subscription.subscription_id == subscription_id
                    && subscription.status == ResourceStatus::Active
            })
            .cloned()
    }

    pub async fn active_subscriptions(&self) -> Vec<SubscriptionDefinition> {
        self.state
            .lock()
            .await
            .subscriptions
            .values()
            .filter(|subscription| subscription.status == ResourceStatus::Active)
            .cloned()
            .collect()
    }

    /// Every active Reader definition.
    pub async fn active_readers(&self) -> Vec<ReaderDefinition> {
        self.state
            .lock()
            .await
            .readers
            .values()
            .filter(|reader| reader.status == ResourceStatus::Active)
            .cloned()
            .collect()
    }

    pub async fn active_subscription_by_name(&self, name: &str) -> Option<SubscriptionDefinition> {
        self.state
            .lock()
            .await
            .subscriptions
            .values()
            .find(|item| item.name == name && item.status == ResourceStatus::Active)
            .cloned()
    }

    pub async fn active_subscription_progress_assignment_by_id(
        &self,
        subscription_id: Uuid,
    ) -> Option<SubscriptionProgressAssignment> {
        let state = self.state.lock().await;
        state
            .subscriptions
            .get(&subscription_id)
            .filter(|item| item.status == ResourceStatus::Active)?;
        let assignment = state
            .subscription_progress_assignments
            .get(&subscription_id)?;
        SubscriptionProgressAssignment::try_new(
            subscription_id,
            assignment.owner.clone(),
            assignment.replicas.clone(),
            assignment.ownership_epoch,
        )
        .ok()
    }

    /// Every active Subscription's current progress placement — the recovery
    /// supervisor's scan input. Declared Subscriptions without a placement
    /// have nothing to recover and are absent.
    pub async fn active_subscription_progress_assignments(
        &self,
    ) -> Vec<SubscriptionProgressAssignment> {
        let state = self.state.lock().await;
        state
            .subscription_progress_assignments
            .iter()
            .filter(|(subscription_id, _)| {
                state
                    .subscriptions
                    .get(*subscription_id)
                    .is_some_and(|item| item.status == ResourceStatus::Active)
            })
            .filter_map(|(subscription_id, assignment)| {
                SubscriptionProgressAssignment::try_new(
                    *subscription_id,
                    assignment.owner.clone(),
                    assignment.replicas.clone(),
                    assignment.ownership_epoch,
                )
                .ok()
            })
            .collect()
    }

    /// One Subscription's committed-but-unapplied cutover translation, if an
    /// Active Range activation left it pending for the progress quorum.
    pub async fn pending_subscription_cutover(
        &self,
        subscription_id: Uuid,
    ) -> Option<SubscriptionCutoverTranslation> {
        self.state
            .lock()
            .await
            .pending_subscription_cutovers
            .get(&subscription_id)
            .cloned()
    }

    /// Every pending Subscription cutover translation for one Feed.
    pub async fn pending_subscription_cutovers_for_feed(
        &self,
        feed_id: Uuid,
    ) -> Vec<SubscriptionCutoverTranslation> {
        self.state
            .lock()
            .await
            .pending_subscription_cutovers
            .values()
            .filter(|pending| pending.feed_id == feed_id)
            .cloned()
            .collect()
    }

    /// The catalog's member-Feed membership set for one Domain-scoped
    /// Subscription, keyed by immutable FeedId; empty for exact-Feed
    /// sources or while no Feed has attached.
    pub async fn subscription_feed_memberships(
        &self,
        subscription_id: Uuid,
    ) -> BTreeMap<Uuid, SubscriptionFeedMembership> {
        self.state
            .lock()
            .await
            .subscription_feed_memberships
            .get(&subscription_id)
            .cloned()
            .unwrap_or_default()
    }

    /// The RF3 mutation-journal placement of a still-active StateStore.
    pub async fn active_state_store_assignment_by_id(
        &self,
        store_id: Uuid,
    ) -> Option<crate::state_store::StateStoreAssignment> {
        let state = self.state.lock().await;
        // StateStoreDefinitions carry build `stage` rather than ResourceStatus;
        // a present definition with an assignment is active.
        state.state_stores.get(&store_id)?;
        state.state_store_assignments.get(&store_id).cloned()
    }

    /// Every StateStore with a live journal placement — the recovery
    /// supervisor's scan input. A present definition with an assignment is
    /// active; declared stores awaiting a placement have nothing to
    /// recover and are absent.
    pub async fn active_state_store_assignments(
        &self,
    ) -> Vec<crate::state_store::StateStoreAssignment> {
        let state = self.state.lock().await;
        state
            .state_store_assignments
            .iter()
            .filter(|(store_id, _)| state.state_stores.contains_key(*store_id))
            .map(|(_, assignment)| assignment.clone())
            .collect()
    }

    /// Every persisted StateStore definition; placements resolve separately.
    pub async fn state_store_definitions(&self) -> Vec<StateStoreDefinition> {
        self.state
            .lock()
            .await
            .state_stores
            .values()
            .cloned()
            .collect()
    }

    /// The progress row's scope identity: the exact Feed for Feed-sourced
    /// Subscriptions, the Domain itself for Domain-sourced ones. Replica
    /// validation compares committed `feed_id` against this identity, so a
    /// Domain row fences by Domain scope rather than an absent Feed.
    pub(crate) async fn active_subscription_progress_scope_by_id(
        &self,
        subscription_id: Uuid,
    ) -> Option<Uuid> {
        self.state
            .lock()
            .await
            .subscriptions
            .get(&subscription_id)
            .filter(|item| item.status == ResourceStatus::Active)
            .and_then(|item| item.source.feed_id().or_else(|| item.source.domain_id()))
    }

    /// Every active Feed's effective History Policy — the per-tick
    /// snapshot a storage-side scanner uses so it never awaits on the
    /// catalog inside blocking work.
    pub async fn effective_history_policies(
        &self,
    ) -> std::collections::HashMap<Uuid, HistoryPolicy> {
        let state = self.state.lock().await;
        state
            .feeds
            .values()
            .filter(|feed| feed.status == ResourceStatus::Active)
            .map(|feed| (feed.feed_id, effective_history_policy(&state, feed)))
            .collect()
    }

    /// The History Policy a storage Node enforces for `feed_id`: the
    /// Feed's declared override layered over its Domain's. `None` when
    /// the Feed is gone — an unresolvable target must not silently
    /// inherit some other Feed's bounds.
    pub async fn effective_history_policy_for_feed(&self, feed_id: Uuid) -> Option<HistoryPolicy> {
        let state = self.state.lock().await;
        state
            .feeds
            .get(&feed_id)
            .filter(|feed| feed.status == ResourceStatus::Active)
            .map(|feed| effective_history_policy(&state, feed))
    }

    /// The Feeds currently frozen by legal holds, by immutable FeedId —
    /// the set every history-destruction sweep consults before deleting
    /// retained bytes. Tombstoned definitions keep their holds so a drop
    /// race cannot unfreeze evidence.
    pub async fn legal_hold_feed_ids(&self) -> std::collections::HashSet<Uuid> {
        let state = self.state.lock().await;
        state.legal_holds.keys().copied().collect()
    }

    /// The declared legal hold on `feed_id`, for DESCRIBE output.
    pub async fn legal_hold_for_feed(&self, feed_id: Uuid) -> Option<LegalHold> {
        let state = self.state.lock().await;
        state.legal_holds.get(&feed_id).cloned()
    }

    /// Feeds marked for cryptographic erasure — `ERASE FEED` tombstoned
    /// them and every storage Node's sweep must purge their stored
    /// artifacts. Held feeds cannot appear here (the hold fences ERASE),
    /// but the sweep checks holds first anyway — defense in depth.
    pub async fn erased_feed_ids(&self) -> std::collections::HashSet<Uuid> {
        let state = self.state.lock().await;
        state
            .feeds
            .values()
            .filter(|feed| feed.erased_at_ns.is_some())
            .map(|feed| feed.feed_id)
            .collect()
    }

    /// Per-Feed DEK state for every declared key row, for the storage
    /// sweep's manifest-heal and seal decisions. Feeds without a row
    /// (unencrypted) do not appear.
    pub async fn feed_key_states(&self) -> std::collections::HashMap<Uuid, FeedKeyState> {
        let state = self.state.lock().await;
        state
            .feed_keys
            .values()
            .map(|key| {
                let state = match key.status {
                    FeedKeyStatus::Active => {
                        let mut dek = [0_u8; 32];
                        let decoded = (0..32)
                            .map(|i| {
                                u8::from_str_radix(
                                    key.dek_hex.get(i * 2..i * 2 + 2).unwrap_or("00"),
                                    16,
                                )
                                .unwrap_or(0)
                            })
                            .collect::<Vec<u8>>();
                        dek.copy_from_slice(&decoded);
                        FeedKeyState::Active(dek)
                    }
                    FeedKeyStatus::Destroyed => FeedKeyState::Destroyed,
                };
                (key.feed_id, state)
            })
            .collect()
    }

    /// One Feed's key state for store-open resolution. `None` means the
    /// Feed is unencrypted (no key row was ever minted).
    pub async fn feed_key_state(&self, feed_id: Uuid) -> Option<FeedKeyState> {
        let state = self.state.lock().await;
        state.feed_keys.get(&feed_id).map(|key| match key.status {
            FeedKeyStatus::Active => {
                let mut dek = [0_u8; 32];
                for (i, byte) in dek.iter_mut().enumerate() {
                    *byte =
                        u8::from_str_radix(key.dek_hex.get(i * 2..i * 2 + 2).unwrap_or("00"), 16)
                            .unwrap_or(0);
                }
                FeedKeyState::Active(dek)
            }
            FeedKeyStatus::Destroyed => FeedKeyState::Destroyed,
        })
    }

    pub(crate) async fn active_reader_frontier(&self, reader_id: Uuid) -> Option<ReaderFrontier> {
        let state = self.state.lock().await;
        state
            .readers
            .get(&reader_id)
            .filter(|reader| reader.status == ResourceStatus::Active)?;
        state.reader_frontiers.get(&reader_id).cloned()
    }

    pub(crate) async fn active_reader_cutover_snapshots(
        &self,
        feed_id: Uuid,
    ) -> Vec<ReaderCutoverSnapshot> {
        let state = self.state.lock().await;
        state
            .readers
            .values()
            .filter(|reader| reader.feed_id == feed_id && reader.status == ResourceStatus::Active)
            .map(|reader| ReaderCutoverSnapshot {
                reader: reader.clone(),
                frontier: state.reader_frontiers.get(&reader.reader_id).cloned(),
            })
            .collect()
    }

    pub async fn active_writer_by_name(&self, name: &str) -> Option<WriterDefinition> {
        self.state
            .lock()
            .await
            .writers
            .values()
            .find(|writer| writer.name == name && writer.status == ResourceStatus::Active)
            .cloned()
    }

    pub async fn validate_writer_append(
        &self,
        writer_id: Uuid,
        feed_id: Uuid,
        range_id: RangeId,
        session_epoch: u64,
        sequence: u64,
    ) -> Result<WriterDefinition, ControlError> {
        let state = self.state.lock().await;
        let writer = state
            .writers
            .get(&writer_id)
            .filter(|writer| writer.status == ResourceStatus::Active)
            .ok_or_else(|| ControlError::NotFound(format!("Writer {writer_id}")))?;
        if writer.feed_id != feed_id {
            return Err(ControlError::InvalidOperation(
                "Writer is bound to a different Feed".to_owned(),
            ));
        }
        validate_writer_epoch(writer, session_epoch)?;
        let next_sequence = writer
            .range_next_sequences
            .get(&range_id)
            .copied()
            .unwrap_or(writer.next_sequence);
        if sequence == 0 || sequence >= next_sequence {
            return Err(ControlError::InvalidOperation(format!(
                "Writer sequence {sequence} was not allocated for Range {range_id}; next unallocated sequence is {next_sequence}"
            )));
        }
        Ok(writer.clone())
    }

    pub async fn active_feed_by_id(&self, feed_id: Uuid) -> Option<FeedDefinition> {
        self.state
            .lock()
            .await
            .feeds
            .get(&feed_id)
            .filter(|feed| feed.status == ResourceStatus::Active)
            .cloned()
    }

    /// Resolves a Feed by immutable id regardless of status — dropped
    /// Feeds still deliver retained history, and the current name is the
    /// honest source label for those deliveries.
    pub(crate) async fn feed_definition_by_id(&self, feed_id: Uuid) -> Option<FeedDefinition> {
        self.state.lock().await.feeds.get(&feed_id).cloned()
    }

    pub async fn active_feed_by_name(&self, name: &str) -> Option<FeedDefinition> {
        self.state
            .lock()
            .await
            .feeds
            .values()
            .find(|feed| feed.name == name && feed.status == ResourceStatus::Active)
            .cloned()
    }

    pub async fn declared_state_store_by_name(&self, name: &str) -> Option<StateStoreDefinition> {
        self.state
            .lock()
            .await
            .state_stores
            .values()
            .find(|store| store.name == name && store.stage != StateStoreStage::Dropped)
            .cloned()
    }

    /// Every persisted Index definition — the rebuild driver scans these
    /// for `Building` stages each tick.
    pub async fn index_definitions(&self) -> Vec<StoredIndexDefinition> {
        self.state.lock().await.indexes.values().cloned().collect()
    }

    pub async fn declared_index_by_name(&self, name: &str) -> Option<StoredIndexDefinition> {
        self.state
            .lock()
            .await
            .indexes
            .values()
            .find(|item| item.name == name)
            .cloned()
    }

    /// An active Pipe definition by dotted name.
    pub async fn active_pipe_by_name(&self, name: &str) -> Option<PipeDefinition> {
        self.state
            .lock()
            .await
            .pipes
            .values()
            .find(|pipe| pipe.name == name && pipe.status == ResourceStatus::Active)
            .cloned()
    }

    /// The active Domain owning `resource` by longest dotted-prefix
    /// match — the same rule `owning_space` applies inside command
    /// apply. Request-path grant checks resolve a resource name's
    /// Domain through this accessor rather than assuming a single
    /// leading segment.
    pub async fn owning_domain_name(&self, resource: &str) -> Option<String> {
        self.state
            .lock()
            .await
            .spaces
            .values()
            .filter(|space| {
                space.status == ResourceStatus::Active
                    && resource.starts_with(&(space.name.clone() + "."))
            })
            .max_by_key(|space| space.name.len())
            .map(|space| space.name.clone())
    }

    /// The active Domain's name for telemetry attribution reporting.
    pub async fn active_domain_name(&self, space_id: Uuid) -> Option<String> {
        self.state
            .lock()
            .await
            .spaces
            .get(&space_id)
            .filter(|space| space.status == ResourceStatus::Active)
            .map(|space| space.name.clone())
    }

    /// The declared Pipe by immutable id.
    pub async fn active_pipe_by_id(&self, pipe_id: Uuid) -> Option<PipeDefinition> {
        self.state
            .lock()
            .await
            .pipes
            .values()
            .find(|pipe| pipe.pipe_id == pipe_id && pipe.status == ResourceStatus::Active)
            .cloned()
    }

    /// Every active Pipe in the catalog.
    /// The compute lease currently authorizing a Node to drive this
    /// Pipe. Pipes declared before compute leases existed synthesize a
    /// lease on the Subscription progress owner — exactly the previous
    /// implicit scheduling rule — until an explicit move writes a real
    /// entry.
    pub async fn pipe_compute_lease(&self, pipe_id: Uuid) -> Option<PipeComputeLease> {
        let state = self.state.lock().await;
        effective_pipe_compute_lease(&state, pipe_id)
    }

    /// Every explicit Pipe compute lease in placement order; Pipes riding
    /// the synthesized progress-owner lease are excluded — their authority
    /// moves with Subscription progress recovery instead.
    pub async fn pipe_compute_leases(&self) -> Vec<PipeComputeLease> {
        self.state
            .lock()
            .await
            .pipe_compute_leases
            .values()
            .cloned()
            .collect()
    }

    pub async fn active_pipes(&self) -> Vec<PipeDefinition> {
        self.state
            .lock()
            .await
            .pipes
            .values()
            .filter(|pipe| pipe.status == ResourceStatus::Active)
            .cloned()
            .collect()
    }

    /// Every declared Distributary lease; the driver supervisor diffs this
    /// against the local NodeId to decide what to drive.
    pub async fn distributary_leases(&self) -> Vec<DistributaryLease> {
        self.state
            .lock()
            .await
            .distributary_leases
            .values()
            .cloned()
            .collect()
    }

    /// The current lease row for one Distributary — the recovery path
    /// re-reads it so a racing mover's CAS fails closed on the epoch.
    pub async fn distributary_lease(&self, distributary_id: Uuid) -> Option<DistributaryLease> {
        self.state
            .lock()
            .await
            .distributary_leases
            .get(&distributary_id)
            .cloned()
    }

    /// The committed driver report for this Distributary — the drive
    /// loop dedupes unchanged stalls against it instead of publishing
    /// an identical row every tick.
    pub async fn distributary_status(&self, distributary_id: Uuid) -> Option<DistributaryStatus> {
        self.state
            .lock()
            .await
            .distributary_status
            .get(&distributary_id)
            .cloned()
    }

    /// The catalog-derived half of a support bundle: resource counts,
    /// bounded rows, storage-Node status, and the recent audit trail.
    /// Credentials, verifier digests, record payloads, and Cursor
    /// strings are never collected rather than scrubbed after the fact.
    pub async fn support_bundle_catalog(&self, now_ns: i64) -> serde_json::Value {
        const MAX_ROWS: usize = 256;
        const MAX_AUDIT: usize = 128;
        let state = self.state.lock().await;
        let feed_name = |feed_id: &Uuid| state.feeds.get(feed_id).map(|feed| feed.name.clone());
        let nodes = state
            .storage_nodes
            .keys()
            .chain(state.capacity_reports.keys())
            .chain(self.eligible_storage_nodes.iter())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .map(|node| {
                let record = state.storage_nodes.get(node);
                let report = state.capacity_reports.get(node);
                json!({
                    "node": node,
                    "registered": record.is_some(),
                    "endpoint": record.map(|r| r.endpoint.clone()),
                    "capabilities": record.map(|r| r.capabilities),
                    "draining": state.draining_storage_nodes.contains(node),
                    "retired": state.retired_storage_nodes.contains(node),
                    "eligible": self.storage_eligible(&state, node, now_ns),
                    "capacity": report.map(|r| r.capacity.clone()),
                    "protocol_version": report.map(|r| r.protocol_version),
                    "build": report.map(|r| r.build.clone()),
                    "last_reported_at_ns": report.map(|r| r.reported_at_ns),
                })
            })
            .collect::<Vec<_>>();
        let ranges = state
            .range_assignments
            .iter()
            .take(MAX_ROWS)
            .map(|(range_id, assignment)| {
                json!({
                    "feed": feed_name(&assignment.feed_id),
                    "range_id": range_id,
                    "generation": assignment.generation,
                    "owner": assignment.owner,
                    "replicas": assignment.replicas,
                    "ownership_epoch": assignment.ownership_epoch,
                })
            })
            .collect::<Vec<_>>();
        let distributaries = state
            .distributaries
            .values()
            .filter(|item| item.status == ResourceStatus::Active)
            .take(MAX_ROWS)
            .map(|item| {
                let lease = state.distributary_leases.get(&item.distributary_id);
                let status = state.distributary_status.get(&item.distributary_id);
                json!({
                    "name": item.name,
                    "link": state.riverbed_links.get(&item.link_id).map(|l| l.name.clone()),
                    "source_feed": item.source_feed,
                    "target_feed": feed_name(&item.target_feed_id),
                    "lease_owner": lease.map(|l| l.owner.as_str()),
                    "lease_epoch": lease.map(|l| l.epoch),
                    "status": status,
                    "status_stale": status.is_some_and(|s| {
                        lease.is_none_or(|l| {
                            l.epoch != s.lease_epoch || l.owner != s.reported_by
                        })
                    }),
                })
            })
            .collect::<Vec<_>>();
        let subscriptions = state
            .subscriptions
            .values()
            .filter(|item| item.status == ResourceStatus::Active)
            .take(MAX_ROWS)
            .map(|item| {
                let placement = state
                    .subscription_progress_assignments
                    .get(&item.subscription_id);
                json!({
                    "name": item.name,
                    "stage": item.stage,
                    "progress_owner": placement.map(|p| p.owner.as_str()),
                    "ownership_epoch": placement.map(|p| p.ownership_epoch),
                    "replicas": placement.map(|p| p.replicas.clone()),
                })
            })
            .collect::<Vec<_>>();
        let pipes = state
            .pipes
            .values()
            .filter(|item| item.status == ResourceStatus::Active)
            .take(MAX_ROWS)
            .map(|item| {
                let lease = effective_pipe_compute_lease(&state, item.pipe_id);
                json!({
                    "name": item.name,
                    "stage": item.stage,
                    "compute_owner": lease.as_ref().map(|l| l.owner.as_str()),
                    "compute_epoch": lease.map(|l| l.compute_epoch),
                })
            })
            .collect::<Vec<_>>();
        let indexes = state
            .indexes
            .values()
            .take(MAX_ROWS)
            .map(|item| {
                json!({
                    "name": item.name,
                    "feed": feed_name(&item.feed_id),
                    "unique": item.unique,
                })
            })
            .collect::<Vec<_>>();
        let state_stores = state
            .state_stores
            .values()
            .filter(|item| item.stage != StateStoreStage::Dropped)
            .take(MAX_ROWS)
            .map(|item| {
                json!({
                    "name": item.name,
                    "stage": item.stage,
                })
            })
            .collect::<Vec<_>>();
        let links = state
            .riverbed_links
            .values()
            .filter(|item| item.status == ResourceStatus::Active)
            .map(|item| {
                // key_env is a variable *name*, never the credential —
                // that distinction is exactly what makes it bundle-safe.
                json!({
                    "name": item.name,
                    "endpoints": item.endpoints,
                    "key_env": item.key_env,
                })
            })
            .collect::<Vec<_>>();
        let api_keys = state
            .api_keys
            .values()
            .map(|identity| {
                // The verifier digest never leaves the catalog: the
                // bundle carries identity and lifecycle only.
                json!({
                    "name": identity.name,
                    "role": state
                        .roles
                        .get(&identity.role_id)
                        .map(|role| role.name.clone()),
                    "status": identity.status,
                    "created_at_ns": identity.created_at_ns,
                    "expires_at_ns": identity.expires_at_ns,
                    "revoked_at_ns": identity.revoked_at_ns,
                })
            })
            .collect::<Vec<_>>();
        let audit_events = state
            .audit_events
            .iter()
            .rev()
            .take(MAX_AUDIT)
            .collect::<Vec<_>>();
        let counts = json!({
            "domains": state.spaces.values().filter(|s| s.status == ResourceStatus::Active).count(),
            "feeds": state.feeds.values().filter(|f| f.status == ResourceStatus::Active).count(),
            "writers": state.writers.values().filter(|w| w.status == ResourceStatus::Active).count(),
            "readers": state.readers.values().filter(|r| r.status == ResourceStatus::Active).count(),
            "subscriptions": subscriptions.len(),
            "pipes": pipes.len(),
            "indexes": state.indexes.len(),
            "state_stores": state_stores.len(),
            "riverbed_links": links.len(),
            "distributaries": distributaries.len(),
            "api_keys": api_keys.len(),
            "active_ranges": state.range_assignments.len(),
            "pending_split_plans": state.range_split_plans.len(),
            "pending_merge_plans": state.range_merge_plans.len(),
            "legal_holds": state.legal_holds.len(),
        });
        json!({
            "revision": state.revision,
            "schema_version": state.schema_version,
            "supported_schema_version": CATALOG_SCHEMA_VERSION,
            "counts": counts,
            "storage_nodes": nodes,
            "active_ranges": ranges,
            "active_ranges_truncated": state.range_assignments.len() > MAX_ROWS,
            "subscriptions": subscriptions,
            "pipes": pipes,
            "indexes": indexes,
            "state_stores": state_stores,
            "riverbed_links": links,
            "distributaries": distributaries,
            "api_keys": api_keys,
            "incompatible_reports": state
                .incompatible_reports
                .iter()
                .map(|(node, row)| json!({
                    "node": node,
                    "protocol_version": row.protocol_version,
                    "build": row.build,
                    "refused_at_ns": row.refused_at_ns,
                }))
                .collect::<Vec<_>>(),
            "audit_events": audit_events,
            "audit_truncated": state.audit_events.len() > MAX_AUDIT,
        })
    }

    pub async fn active_distributaries(&self) -> Vec<DistributaryDefinition> {
        self.state
            .lock()
            .await
            .distributaries
            .values()
            .filter(|item| item.status == ResourceStatus::Active)
            .cloned()
            .collect()
    }

    pub async fn riverbed_link(&self, link_id: Uuid) -> Option<RiverbedLink> {
        self.state
            .lock()
            .await
            .riverbed_links
            .get(&link_id)
            .filter(|item| item.status == ResourceStatus::Active)
            .cloned()
    }

    /// Every active Pipe consuming one Subscription's progress assignment;
    /// the journal driver needs this to attach effect scope.
    pub async fn pipes_for_subscription(&self, subscription_id: Uuid) -> Vec<PipeDefinition> {
        self.state
            .lock()
            .await
            .pipes
            .values()
            .filter(|pipe| {
                pipe.status == ResourceStatus::Active && pipe.subscription_id == subscription_id
            })
            .cloned()
            .collect()
    }

    pub async fn active_range_assignment(&self, feed_id: Uuid) -> Option<ActiveRangeAssignment> {
        self.state.lock().await.active_ranges.get(&feed_id).cloned()
    }

    pub async fn active_range_assignment_by_id(
        &self,
        range_id: RangeId,
    ) -> Option<ActiveRangeAssignment> {
        self.state
            .lock()
            .await
            .range_assignments
            .get(&range_id)
            .cloned()
    }

    pub async fn follower_move_plan(&self, range_id: RangeId) -> Option<RangeMovePlan> {
        self.state
            .lock()
            .await
            .range_move_plans
            .get(&range_id)
            .cloned()
    }

    /// The durable committed-frontier proof recorded for a Range, if any.
    pub async fn range_commit_proof(&self, range_id: RangeId) -> Option<RangeCommitProof> {
        self.state
            .lock()
            .await
            .range_commit_proofs
            .get(&range_id)
            .cloned()
    }

    /// Committed-frontier proofs for every Range this Feed's map routes.
    pub async fn feed_commit_proofs(&self, feed_id: Uuid) -> BTreeMap<RangeId, RangeCommitProof> {
        let state = self.state.lock().await;
        let Some(map) = state.range_maps.get(&feed_id) else {
            return BTreeMap::new();
        };
        map.routes()
            .iter()
            .filter_map(|route| {
                state
                    .range_commit_proofs
                    .get(&route.range_id)
                    .cloned()
                    .map(|proof| (route.range_id, proof))
            })
            .collect()
    }

    /// The staged split plan blocking this Feed's next range operation, if
    /// any — the operator-facing counterpart of `INSPECT PLACEMENT` output.
    pub async fn range_split_plan(&self, feed_id: Uuid) -> Option<RangeSplitPlan> {
        self.state
            .lock()
            .await
            .range_split_plans
            .get(&feed_id)
            .cloned()
    }

    pub async fn range_merge_plan(&self, feed_id: Uuid) -> Option<RangeMergePlan> {
        self.state
            .lock()
            .await
            .range_merge_plans
            .get(&feed_id)
            .cloned()
    }

    /// The staged follower-move plan blocking this Feed's next range
    /// operation, if any; move plans are keyed by RangeId, so this scans.
    pub async fn feed_follower_move_plan(&self, feed_id: Uuid) -> Option<RangeMovePlan> {
        self.state
            .lock()
            .await
            .range_move_plans
            .values()
            .find(|plan| plan.feed_id == feed_id)
            .cloned()
    }

    pub async fn feed_owner_move_plan(&self, feed_id: Uuid) -> Option<RangeOwnerMovePlan> {
        self.state
            .lock()
            .await
            .owner_move_plans
            .values()
            .find(|plan| plan.feed_id == feed_id)
            .cloned()
    }

    pub async fn completed_follower_move(&self, range_id: RangeId) -> Option<RangeMovePlan> {
        self.state
            .lock()
            .await
            .completed_move_plans
            .get(&range_id)
            .cloned()
    }

    pub async fn owner_move_plan(&self, range_id: RangeId) -> Option<RangeOwnerMovePlan> {
        self.state
            .lock()
            .await
            .owner_move_plans
            .get(&range_id)
            .cloned()
    }

    pub async fn completed_owner_move(&self, range_id: RangeId) -> Option<RangeOwnerMovePlan> {
        self.state
            .lock()
            .await
            .completed_owner_move_plans
            .get(&range_id)
            .cloned()
    }

    pub async fn active_range_map(&self, feed_id: Uuid) -> Option<RangeMap> {
        self.state.lock().await.range_maps.get(&feed_id).cloned()
    }

    pub async fn active_feed_range_maps(&self) -> Vec<(String, RangeMap)> {
        let state = self.state.lock().await;
        state
            .feeds
            .values()
            .filter(|feed| feed.status == ResourceStatus::Active)
            .filter_map(|feed| {
                state
                    .range_maps
                    .get(&feed.feed_id)
                    .cloned()
                    .map(|map| (feed.name.clone(), map))
            })
            .collect()
    }

    pub async fn active_feed_name_for_range(&self, range_id: RangeId) -> Option<String> {
        let state = self.state.lock().await;
        let feed_id = state.range_assignments.get(&range_id)?.feed_id;
        state
            .feeds
            .get(&feed_id)
            .filter(|feed| feed.status == ResourceStatus::Active)
            .map(|feed| feed.name.clone())
    }

    pub async fn active_range_assignments_for_feed(
        &self,
        feed_id: Uuid,
    ) -> Vec<ActiveRangeAssignment> {
        let state = self.state.lock().await;
        state
            .range_maps
            .get(&feed_id)
            .map(|map| {
                map.routes()
                    .iter()
                    .filter_map(|route| state.range_assignments.get(&route.range_id).cloned())
                    .collect()
            })
            .unwrap_or_default()
    }

    pub async fn active_range_for_key(
        &self,
        feed_id: Uuid,
        key: &[u8],
    ) -> Option<(RangeRoute, ActiveRangeAssignment)> {
        let state = self.state.lock().await;
        let route = state.range_maps.get(&feed_id)?.route_key(key).clone();
        let assignment = state.range_assignments.get(&route.range_id)?.clone();
        Some((route, assignment))
    }

    pub async fn active_feed_assignments(&self) -> Vec<(String, ActiveRangeAssignment)> {
        let state = self.state.lock().await;
        state
            .feeds
            .values()
            .filter(|feed| feed.status == ResourceStatus::Active)
            .filter_map(|feed| {
                state
                    .active_ranges
                    .get(&feed.feed_id)
                    .cloned()
                    .map(|assignment| (feed.name.clone(), assignment))
            })
            .collect()
    }

    pub async fn applied_result(&self, request_id: Uuid) -> Option<ReplicatedCommandResult> {
        self.state
            .lock()
            .await
            .applied_requests
            .get(&request_id)
            .cloned()
    }

    async fn apply(
        &self,
        state: &mut CatalogState,
        command: Command,
        request_id: Uuid,
        issued_at_ns: i64,
        fixed: FixedPlacements,
    ) -> Result<(String, Value), ControlError> {
        match command {
            Command::CreateDomain { name } | Command::CreateSpace { name } => {
                validate_dotted_name(&name)?;
                ensure_name_available(
                    state.spaces.values().map(|item| (&item.name, &item.status)),
                    &name,
                )?;
                let definition = SpaceDefinition {
                    space_id: derived_resource_id(request_id, "space"),
                    name: name.clone(),
                    status: ResourceStatus::Active,
                    created_at_ns: issued_at_ns,
                };
                state.spaces.insert(definition.space_id, definition.clone());
                Ok((format!("created Domain {name}"), json!(definition)))
            }
            Command::CreateFeed { name, encrypted } => {
                validate_dotted_name(&name)?;
                ensure_name_available(
                    state.feeds.values().map(|item| (&item.name, &item.status)),
                    &name,
                )?;
                let space_id = owning_space(state, &name)?.space_id;
                let feed_id = derived_resource_id(request_id, "feed");
                let placement = fixed.active_range.ok_or_else(|| {
                    ControlError::InvalidOperation(
                        "Feed creation has no consensus-prepared Active Range placement".to_owned(),
                    )
                })?;
                let assignment = ActiveRangeAssignment::try_new(
                    feed_id,
                    RangeId::from_uuid(derived_resource_id(request_id, "active-range")),
                    RangeGeneration::new(1),
                    placement.owner,
                    ReplicaSet::try_new(placement.replicas)
                        .map_err(|error| ControlError::InvalidOperation(error.to_string()))?,
                    OwnershipEpoch::new(1),
                )
                .map_err(|error| ControlError::InvalidOperation(error.to_string()))?;
                let storage_name = format!("/feeds/{}", feed_id.simple());
                let definition = FeedDefinition {
                    feed_id,
                    name: name.clone(),
                    space_id,
                    storage_name,
                    status: ResourceStatus::Active,
                    created_at_ns: issued_at_ns,
                    encrypted,
                    erased_at_ns: None,
                };
                state.feeds.insert(feed_id, definition.clone());
                if encrypted {
                    // The DEK derives deterministically from the request
                    // id — every replica applying this command derives
                    // identical key material, so segment ciphertext
                    // (and its manifest digest) agree across RF3.
                    let mut hasher = blake3::Hasher::new();
                    hasher.update(b"whitewater-feed-dek-v1");
                    hasher.update(request_id.as_bytes());
                    hasher.update(feed_id.as_bytes());
                    let dek: [u8; 32] = *hasher.finalize().as_bytes();
                    state.feed_keys.insert(
                        feed_id,
                        FeedKey {
                            feed_id,
                            dek_hex: dek.iter().map(|b| format!("{b:02x}")).collect(),
                            status: FeedKeyStatus::Active,
                            created_at_ns: issued_at_ns,
                            destroyed_at_ns: None,
                        },
                    );
                }
                state.range_maps.insert(
                    feed_id,
                    RangeMap::single(assignment.range_id, assignment.generation),
                );
                state
                    .range_assignments
                    .insert(assignment.range_id, assignment.clone());
                state.active_ranges.insert(feed_id, assignment);
                // Domain-scoped Subscriptions covering this Domain attach
                // the new Feed in the same catalog commit — the durable
                // discovery boundary — so no write can slip through
                // registration unseen.
                attach_feed_to_domain_subscriptions(
                    state,
                    &definition,
                    state.revision.saturating_add(1),
                )?;
                Ok((format!("created Feed {name}"), json!(definition)))
            }
            Command::DefineStateStore {
                name,
                source,
                named_keys,
            } => {
                validate_dotted_name(&name)?;
                validate_named_keys(&named_keys)?;
                if state.state_stores.values().any(|item| item.name == name) {
                    return Err(ControlError::AlreadyExists(name));
                }
                let space_id = owning_space(state, &name)?.space_id;
                let source = match source {
                    StateStoreSourceRequest::Manual => {
                        if named_keys.iter().any(|key| key.extractor.is_some()) {
                            return Err(ControlError::InvalidOperation(
                                "named-key extractors apply to feed-sourced StateStores;                                  manual stores take named values on each put_row mutation"
                                    .to_owned(),
                            ));
                        }
                        StateStoreSource::Manual
                    }
                    StateStoreSourceRequest::Feed { feed } => {
                        for key in &named_keys {
                            match &key.extractor {
                                Some(spec) => validate_index_fields(std::slice::from_ref(spec))?,
                                None => {
                                    return Err(ControlError::InvalidOperation(
                                        "feed-sourced named keys require an extractor                                          (KEY, METADATA 'name', or JSON 'path') so replay                                          can project typed values"
                                            .to_owned(),
                                    ));
                                }
                            }
                        }
                        let definition = active_feed(state, &feed)?;
                        if definition.space_id != space_id {
                            return Err(ControlError::InvalidOperation(
                                "StateStore source Feed must belong to its Domain".to_owned(),
                            ));
                        }
                        StateStoreSource::Feed {
                            feed_id: definition.feed_id,
                        }
                    }
                };
                let store_id = derived_resource_id(request_id, "state-store");
                let fixed = fixed.state_store.ok_or_else(|| {
                    ControlError::InvalidOperation(
                        "StateStore declaration has no consensus-prepared placement".to_owned(),
                    )
                })?;
                let replicas = ReplicaSet::try_new(fixed.replicas)
                    .map_err(|error| ControlError::InvalidOperation(error.to_string()))?;
                let assignment = crate::state_store::StateStoreAssignment::try_new(
                    store_id,
                    fixed.owner,
                    replicas,
                    1,
                )
                .map_err(|error| ControlError::InvalidOperation(error.to_string()))?;
                let definition = StateStoreDefinition {
                    store_id,
                    name: name.clone(),
                    space_id,
                    source,
                    stage: StateStoreStage::Declared,
                    named_keys,
                    created_at_ns: issued_at_ns,
                };
                state
                    .state_stores
                    .insert(definition.store_id, definition.clone());
                state.state_store_assignments.insert(store_id, assignment);
                Ok((
                    format!("declared StateStore {name} with RF3 mutation journal placement"),
                    json!(definition),
                ))
            }
            Command::DefineIndex {
                name,
                feed,
                fields,
                unique,
                collation,
                consistency,
            } => {
                validate_dotted_name(&name)?;
                validate_index_fields(&fields)?;
                if state.indexes.values().any(|item| item.name == name) {
                    return Err(ControlError::AlreadyExists(name));
                }
                let space_id = owning_space(state, &name)?.space_id;
                let feed_definition = active_feed(state, &feed)?;
                if feed_definition.space_id != space_id {
                    return Err(ControlError::InvalidOperation(
                        "Index source Feed must belong to its Domain".to_owned(),
                    ));
                }
                let definition = StoredIndexDefinition {
                    index_id: derived_resource_id(request_id, "index"),
                    name: name.clone(),
                    space_id,
                    feed_id: feed_definition.feed_id,
                    fields,
                    collation,
                    consistency,
                    unique,
                    stage: IndexBuildStage::Declared,
                    applied_cursor: None,
                    created_at_ns: issued_at_ns,
                };
                state
                    .indexes
                    .insert(definition.index_id, definition.clone());
                Ok((
                    format!("declared Index {name} on Feed {feed}; no data is served until build and RF3 activation"),
                    json!(definition),
                ))
            }
            Command::RebuildIndex { name } => {
                let index_id = state
                    .indexes
                    .values()
                    .find(|item| item.name == name)
                    .map(|item| item.index_id)
                    .ok_or_else(|| ControlError::NotFound(format!("Index {name}")))?;
                let item = state.indexes.get_mut(&index_id).expect("Index exists");
                let generation = match item.stage {
                    IndexBuildStage::Building { generation } => {
                        // Re-issuing the same in-flight rebuild is
                        // idempotent: the generation does not advance and
                        // drivers resume their existing shadow.
                        return Ok((
                            format!(
                                "Index {name} rebuild already in flight (generation {generation})"
                            ),
                            json!(item.clone()),
                        ));
                    }
                    IndexBuildStage::Declared => 1,
                    IndexBuildStage::Active { generation } => generation + 1,
                };
                item.stage = IndexBuildStage::Building { generation };
                item.applied_cursor = None;
                Ok((
                    format!("rebuilding Index {name} as shadow generation {generation}"),
                    json!(item.clone()),
                ))
            }
            Command::RebuildIndexesInDomain { domain } => {
                let space = active_space(state, &domain)?.space_id;
                let mut rebuilt = Vec::new();
                for item in state.indexes.values_mut() {
                    if item.space_id != space {
                        continue;
                    }
                    match item.stage {
                        IndexBuildStage::Building { .. } => {}
                        IndexBuildStage::Declared => {
                            item.stage = IndexBuildStage::Building { generation: 1 };
                            item.applied_cursor = None;
                            rebuilt.push(item.name.clone());
                        }
                        IndexBuildStage::Active { generation } => {
                            item.stage = IndexBuildStage::Building {
                                generation: generation + 1,
                            };
                            item.applied_cursor = None;
                            rebuilt.push(item.name.clone());
                        }
                    }
                }
                Ok((
                    format!("rebuilding {} Index(es) in Domain {domain}", rebuilt.len()),
                    json!({ "rebuilt": rebuilt }),
                ))
            }
            Command::CompleteIndexBuild {
                index_id,
                generation,
                applied_cursor,
            } => {
                let item = state
                    .indexes
                    .get_mut(&index_id)
                    .ok_or_else(|| ControlError::NotFound(format!("Index {index_id}")))?;
                match item.stage {
                    IndexBuildStage::Building {
                        generation: building,
                    } if building == generation => {
                        item.stage = IndexBuildStage::Active { generation };
                        item.applied_cursor = Some(applied_cursor.clone());
                        Ok((
                            format!(
                                "activated Index {} generation {generation} at Cursor {applied_cursor}",
                                item.name
                            ),
                            json!(item.clone()),
                        ))
                    }
                    _ => Err(ControlError::InvalidOperation(format!(
                        "Index {} is not building generation {generation}",
                        item.name
                    ))),
                }
            }
            Command::DefinePipe {
                name,
                subscription,
                output_feed,
                output_store,
                quarantine_feed,
                emit_feed,
                operation,
            } => {
                validate_dotted_name(&name)?;
                ensure_name_available(
                    state.pipes.values().map(|item| (&item.name, &item.status)),
                    &name,
                )?;
                let space_id = owning_space(state, &name)?.space_id;
                let subscription = active_subscription(state, &subscription)?;
                if subscription.space_id != space_id {
                    return Err(ControlError::InvalidOperation(
                        "Pipe input Subscription must belong to its Domain".to_owned(),
                    ));
                }
                let mut operation = operation;
                let (output_feed_id, output_store_id) = match operation.as_mut() {
                    PipeOperation::Forward => {
                        if emit_feed.is_some() {
                            return Err(ControlError::InvalidOperation(
                                "EMIT TO FEED is only valid on Window Pipes".to_owned(),
                            ));
                        }
                        let output_feed = output_feed.ok_or_else(|| {
                            ControlError::InvalidOperation(
                                "Forward Pipes require an output Feed".to_owned(),
                            )
                        })?;
                        let output = active_feed(state, &output_feed)?;
                        if output.space_id != space_id {
                            return Err(ControlError::InvalidOperation(
                                "Pipe output Feed must belong to its Domain".to_owned(),
                            ));
                        }
                        if subscription.source.feed_id() == Some(output.feed_id) {
                            return Err(ControlError::InvalidOperation(
                                "Pipe output Feed cannot be its own input Feed".to_owned(),
                            ));
                        }
                        (Some(output.feed_id), None)
                    }
                    PipeOperation::Window {
                        window_ns,
                        key,
                        aggregation,
                        time,
                        max_late_ns,
                        watermark_ns,
                        grace_ns,
                        expire_ns,
                        key_limit,
                        byte_limit,
                    } => {
                        if *window_ns == 0 {
                            return Err(ControlError::InvalidOperation(
                                "Window Pipes require a non-zero window".to_owned(),
                            ));
                        }
                        if let Some(bound) = max_late_ns {
                            if *bound == 0 {
                                return Err(ControlError::InvalidOperation(
                                    "MAX_LATE requires a non-zero duration".to_owned(),
                                ));
                            }
                            if matches!(time, WindowTime::Ingest) {
                                return Err(ControlError::InvalidOperation(
                                    "MAX_LATE is meaningless under the ingest clock — records can never arrive late to their own ingest time".to_owned(),
                                ));
                            }
                        }
                        if max_late_ns.is_some() && watermark_ns.is_some() {
                            return Err(ControlError::InvalidOperation(
                                "MAX_LATE and WATERMARK are alternative lateness models — declare exactly one".to_owned(),
                            ));
                        }
                        if let Some(bound) = watermark_ns {
                            if *bound == 0 {
                                return Err(ControlError::InvalidOperation(
                                    "WATERMARK requires a non-zero duration".to_owned(),
                                ));
                            }
                            if matches!(time, WindowTime::Ingest) {
                                return Err(ControlError::InvalidOperation(
                                    "WATERMARK is meaningless under the ingest clock — the horizon would follow arrival order, not event-time disorder".to_owned(),
                                ));
                            }
                        }
                        if let Some(bound) = grace_ns {
                            if *bound == 0 {
                                return Err(ControlError::InvalidOperation(
                                    "GRACE requires a non-zero duration".to_owned(),
                                ));
                            }
                            if watermark_ns.is_none() {
                                return Err(ControlError::InvalidOperation(
                                    "GRACE requires WATERMARK — revision bounds ride the event-time horizon".to_owned(),
                                ));
                            }
                        }
                        if let (Some(expire), Some(grace)) = (*expire_ns, *grace_ns) {
                            if expire <= grace {
                                return Err(ControlError::InvalidOperation(
                                    "EXPIRE must exceed GRACE — revision markers must outlive the revision bound they serve".to_owned(),
                                ));
                            }
                        }
                        if let Some(expire) = expire_ns {
                            if *expire == 0 {
                                return Err(ControlError::InvalidOperation(
                                    "EXPIRE requires a non-zero duration".to_owned(),
                                ));
                            }
                            if let Some(bound) = max_late_ns {
                                if *expire <= *bound {
                                    return Err(ControlError::InvalidOperation(
                                        "EXPIRE must exceed MAX_LATE — a pane must provably close before it may expire".to_owned(),
                                    ));
                                }
                            }
                        }
                        if matches!(key_limit, Some(0)) {
                            return Err(ControlError::InvalidOperation(
                                "LIMIT KEYS requires a non-zero bound".to_owned(),
                            ));
                        }
                        if key_limit
                            .map(|limit| limit > crate::pipe::WINDOW_MAX_KEY_LIMIT)
                            .unwrap_or(false)
                        {
                            return Err(ControlError::InvalidOperation(
                                "LIMIT KEYS accepts at most 512 active Keys — the fetch-time scan must enumerate the store's key posting exactly, and larger cardinalities need a counted posting".to_owned(),
                            ));
                        }
                        if matches!(byte_limit, Some(0)) {
                            return Err(ControlError::InvalidOperation(
                                "LIMIT BYTES requires a non-zero bound".to_owned(),
                            ));
                        }
                        if byte_limit
                            .map(|limit| limit > crate::pipe::WINDOW_MAX_BYTE_LIMIT)
                            .unwrap_or(false)
                        {
                            return Err(ControlError::InvalidOperation(
                                "LIMIT BYTES accepts at most 256 KiB of pane state — the fetch-time scan must enumerate the store's key posting exactly, and larger budgets need a counted posting".to_owned(),
                            ));
                        }
                                                let output_store = output_store.ok_or_else(|| {
                            ControlError::InvalidOperation(
                                "Window Pipes require an output StateStore".to_owned(),
                            )
                        })?;
                        let store = state
                            .state_stores
                            .values()
                            .find(|item| {
                                item.name == output_store
                                    && item.stage != StateStoreStage::Dropped
                            })
                            .ok_or_else(|| {
                                ControlError::NotFound(format!(
                                    "StateStore {output_store}"
                                ))
                            })?;
                        if store.space_id != space_id {
                            return Err(ControlError::InvalidOperation(
                                "Pipe output StateStore must belong to its Domain"
                                    .to_owned(),
                            ));
                        }
                        // Pane rows post their group key and pane index under
                        // two reserved named keys so the view is enumerable
                        // without knowing composite row keys.
                        for required in ["key", "pane"] {
                            if !store.named_keys.iter().any(|item| {
                                item.name == required && !item.unique
                            }) {
                                return Err(ControlError::InvalidOperation(format!(
                                    "Pipe output StateStore must declare non-unique named keys 'key' and 'pane' (missing '{required}')"
                                )));
                            }
                        }
                        validate_window_field(key)?;
                        if let WindowAggregation::Sum { value }
                        | WindowAggregation::Average { value } = aggregation
                        {
                            validate_window_field(value)?;
                        }
                        // `EMIT TO FEED` declares where closed panes
                        // publish. Without a lateness bound no pane ever
                        // provably closes (a record could always arrive
                        // legally) — either `MAX_LATE`'s ingest bound or
                        // `WATERMARK`'s committed event-time horizon
                        // supplies the close proof, and the two are
                        // mutually exclusive by the check above.
                        let emit_feed_id = match &emit_feed {
                            Some(name) => {
                                if max_late_ns.is_none() && watermark_ns.is_none() {
                                    return Err(ControlError::InvalidOperation(
                                        "EMIT TO FEED requires MAX_LATE or WATERMARK — without a lateness bound no pane ever provably closes".to_owned(),
                                    ));
                                }
                                let feed = active_feed(state, name)?;
                                if feed.space_id != space_id {
                                    return Err(ControlError::InvalidOperation(
                                        "Pipe emission Feed must belong to its Domain"
                                            .to_owned(),
                                    ));
                                }
                                if subscription.source.feed_id() == Some(feed.feed_id) {
                                    return Err(ControlError::InvalidOperation(
                                        "Pipe emission Feed cannot be its own input Feed"
                                            .to_owned(),
                                    ));
                                }
                                Some(feed.feed_id)
                            }
                            None => {
                                if output_feed.is_some() {
                                    return Err(ControlError::InvalidOperation(
                                        "Window Pipes emit through EMIT TO FEED, not TO FEED".to_owned(),
                                    ));
                                }
                                None
                            }
                        };
                        (emit_feed_id, Some(store.store_id))
                    }
                    PipeOperation::Enrich {
                        key,
                        freshness: _,
                        named,
                        on_miss,
                        quarantine_feed_id,
                    } => {
                        if emit_feed.is_some() {
                            return Err(ControlError::InvalidOperation(
                                "EMIT TO FEED is only valid on Window Pipes".to_owned(),
                            ));
                        }
                        if let Some(name) = named {
                            let valid = !name.is_empty()
                                && name.len() <= 64
                                && name.as_bytes()[0].is_ascii_lowercase()
                                && name.chars().all(|c| {
                                    c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'
                                });
                            if !valid {
                                return Err(ControlError::InvalidName(name.clone()));
                            }
                        }
                        let output_feed = output_feed.ok_or_else(|| {
                            ControlError::InvalidOperation(
                                "Enrich Pipes require an output Feed".to_owned(),
                            )
                        })?;
                        let output_store = output_store.ok_or_else(|| {
                            ControlError::InvalidOperation(
                                "Enrich Pipes require a lookup StateStore".to_owned(),
                            )
                        })?;
                        let output = active_feed(state, &output_feed)?;
                        if output.space_id != space_id {
                            return Err(ControlError::InvalidOperation(
                                "Pipe output Feed must belong to its Domain".to_owned(),
                            ));
                        }
                        if subscription.source.feed_id() == Some(output.feed_id) {
                            return Err(ControlError::InvalidOperation(
                                "Pipe output Feed cannot be its own input Feed".to_owned(),
                            ));
                        }
                        let store = state
                            .state_stores
                            .values()
                            .find(|item| {
                                item.name == output_store
                                    && item.stage != StateStoreStage::Dropped
                            })
                            .ok_or_else(|| {
                                ControlError::NotFound(format!("StateStore {output_store}"))
                            })?;
                        if store.space_id != space_id {
                            return Err(ControlError::InvalidOperation(
                                "Pipe lookup StateStore must belong to its Domain".to_owned(),
                            ));
                        }
                        validate_window_field(key)?;
                        // A declared quarantine routes misses to a resolved
                        // same-Domain Feed — never the input (re-processing
                        // loop) or the output (contamination).
                        *quarantine_feed_id = match (on_miss, &quarantine_feed) {
                            (EnrichMiss::Quarantine, Some(name)) => {
                                let quarantine = active_feed(state, name)?;
                                if quarantine.space_id != space_id {
                                    return Err(ControlError::InvalidOperation(
                                        "Pipe quarantine Feed must belong to its Domain"
                                            .to_owned(),
                                    ));
                                }
                                if subscription.source.feed_id()
                                    == Some(quarantine.feed_id)
                                    || quarantine.feed_id == output.feed_id
                                {
                                    return Err(ControlError::InvalidOperation(
                                        "Pipe quarantine Feed must differ from its input and output Feeds".to_owned(),
                                    ));
                                }
                                Some(quarantine.feed_id)
                            }
                            (EnrichMiss::Quarantine, None) => {
                                return Err(ControlError::InvalidOperation(
                                    "MISS QUARANTINE requires a quarantine Feed".to_owned(),
                                ));
                            }
                            (_, Some(_)) => {
                                return Err(ControlError::InvalidOperation(
                                    "a quarantine Feed is only valid with MISS QUARANTINE"
                                        .to_owned(),
                                ));
                            }
                            _ => None,
                        };
                        (Some(output.feed_id), Some(store.store_id))
                    }
                };
                // The compute lease starts co-located with the
                // Subscription progress owner (the previous implicit
                // scheduling rule); it moves independently afterwards.
                let compute_owner = state
                    .subscription_progress_assignments
                    .get(&subscription.subscription_id)
                    .map(|assignment| assignment.owner.clone())
                    .or_else(|| self.storage_pool(state, issued_at_ns).into_iter().next())
                    .ok_or_else(|| {
                        ControlError::InvalidOperation(
                            "no storage-eligible Node can hold the Pipe compute lease".to_owned(),
                        )
                    })?;
                let definition = PipeDefinition {
                    pipe_id: derived_resource_id(request_id, "pipe"),
                    name: name.clone(),
                    space_id,
                    subscription_id: subscription.subscription_id,
                    operation: *operation,
                    output_feed_id,
                    output_store_id,
                    stage: PipeStage::Declared,
                    status: ResourceStatus::Active,
                    created_at_ns: issued_at_ns,
                };
                state.pipes.insert(definition.pipe_id, definition.clone());
                let lease = PipeComputeLease {
                    pipe_id: definition.pipe_id,
                    owner: compute_owner,
                    compute_epoch: 1,
                };
                state
                    .pipe_compute_leases
                    .insert(definition.pipe_id, lease);
                Ok((
                    format!("declared Pipe {name}; no records flow until a Pipe driver exists"),
                    json!(definition),
                ))
            }
            Command::CreateWriter { name, feed } => {
                validate_dotted_name(&name)?;
                ensure_name_available(
                    state
                        .writers
                        .values()
                        .map(|item| (&item.name, &item.status)),
                    &name,
                )?;
                let feed_id = active_feed(state, &feed)?.feed_id;
                let definition = WriterDefinition {
                    writer_id: derived_resource_id(request_id, "writer"),
                    name: name.clone(),
                    feed_id,
                    status: ResourceStatus::Active,
                    created_at_ns: issued_at_ns,
                    session_epoch: 0,
                    next_sequence: 1,
                    session_active: false,
                    range_next_sequences: BTreeMap::new(),
                };
                state
                    .writers
                    .insert(definition.writer_id, definition.clone());
                Ok((format!("created Writer {name}"), json!(definition)))
            }
            Command::OpenWriterSession { writer } => {
                let definition = active_writer_mut(state, &writer)?;
                definition.session_epoch =
                    definition.session_epoch.checked_add(1).ok_or_else(|| {
                        ControlError::InvalidOperation("Writer session epoch overflow".to_owned())
                    })?;
                definition.next_sequence = 1;
                definition.range_next_sequences.clear();
                definition.session_active = true;
                Ok((
                    format!("opened Writer session {writer}"),
                    json!(definition.clone()),
                ))
            }
            Command::AllocateWriterSequence {
                writer,
                session_epoch,
            } => {
                let definition = active_writer_mut(state, &writer)?;
                validate_writer_epoch(definition, session_epoch)?;
                let sequence = definition.next_sequence;
                definition.next_sequence = sequence.checked_add(1).ok_or_else(|| {
                    ControlError::InvalidOperation("Writer sequence overflow".to_owned())
                })?;
                Ok((
                    format!("allocated Writer sequence {sequence} for {writer}"),
                    json!({
                        "writer_id": definition.writer_id,
                        "feed_id": definition.feed_id,
                        "session_epoch": definition.session_epoch,
                        "sequence": sequence
                    }),
                ))
            }
            Command::AllocateWriterRangeSequence {
                writer,
                session_epoch,
                range_id,
            } => {
                let definition = active_writer_mut(state, &writer)?;
                validate_writer_epoch(definition, session_epoch)?;
                let sequence = *definition.range_next_sequences.entry(range_id).or_insert(1);
                definition.range_next_sequences.insert(
                    range_id,
                    sequence.checked_add(1).ok_or_else(|| {
                        ControlError::InvalidOperation("Writer sequence overflow".to_owned())
                    })?,
                );
                Ok((
                    format!("allocated Writer range sequence {sequence} for {writer}"),
                    json!({
                        "writer_id": definition.writer_id,
                        "feed_id": definition.feed_id,
                        "range_id": range_id,
                        "session_epoch": definition.session_epoch,
                        "sequence": sequence
                    }),
                ))
            }
            Command::RepairWriterRangeSequence {
                writer,
                session_epoch,
                range_id,
                next_sequence,
            } => {
                let definition = active_writer_mut(state, &writer)?;
                validate_writer_epoch(definition, session_epoch)?;
                // Only rewind: numbers above `next_sequence` were
                // allocated but never committed (the heal path proved
                // the boundary), so reusing them is safe. A counter at
                // or below the bound is left alone — never skip forward.
                let rewound = matches!(
                    definition.range_next_sequences.get_mut(&range_id),
                    Some(current) if *current > next_sequence
                );
                if rewound {
                    definition
                        .range_next_sequences
                        .insert(range_id, next_sequence);
                }
                Ok((
                    format!("repaired Writer range sequence for {writer}"),
                    json!({
                        "writer_id": definition.writer_id,
                        "feed_id": definition.feed_id,
                        "range_id": range_id,
                        "session_epoch": definition.session_epoch,
                        "next_sequence": definition
                            .range_next_sequences
                            .get(&range_id)
                            .copied(),
                        "rewound": rewound
                    }),
                ))
            }
            Command::RevokeWriterSession {
                writer,
                session_epoch,
            } => {
                let definition = active_writer_mut(state, &writer)?;
                validate_writer_epoch(definition, session_epoch)?;
                definition.session_active = false;
                Ok((
                    format!("revoked Writer session {writer}"),
                    json!(definition.clone()),
                ))
            }
            Command::CreateReader { name, feed, start } => {
                validate_dotted_name(&name)?;
                ensure_name_available(
                    state
                        .readers
                        .values()
                        .map(|item| (&item.name, &item.status)),
                    &name,
                )?;
                let feed_id = active_feed(state, &feed)?.feed_id;
                let acknowledged_cursor = match &start {
                    ReaderStart::Cursor(cursor) => Some(cursor.clone()),
                    ReaderStart::Beginning | ReaderStart::Now | ReaderStart::Timestamp(_) => None,
                };
                let definition = ReaderDefinition {
                    reader_id: derived_resource_id(request_id, "reader"),
                    name: name.clone(),
                    feed_id,
                    start,
                    acknowledged_cursor: acknowledged_cursor.clone(),
                    delivered_cursor: acknowledged_cursor,
                    last_delivery: None,
                    session_epoch: 0,
                    session_capacity: 0,
                    session_active: false,
                    status: ResourceStatus::Active,
                    created_at_ns: issued_at_ns,
                };
                state
                    .readers
                    .insert(definition.reader_id, definition.clone());
                Ok((format!("created Reader {name}"), json!(definition)))
            }
            Command::CreateSubscription {
                name,
                feed,
                domain,
                start,
                delivery_delay_ns,
                exhausted_policy,
            } => {
                validate_dotted_name(&name)?;
                ensure_name_available(
                    state
                        .subscriptions
                        .values()
                        .map(|item| (&item.name, &item.status)),
                    &name,
                )?;
                let space_id = owning_space(state, &name)?.space_id;
                if let Some(delay) = delivery_delay_ns {
                    if delay == 0 {
                        return Err(ControlError::InvalidOperation(
                            "Subscription DELIVERY DELAY must be greater than zero".to_owned(),
                        ));
                    }
                    if delay > i64::MAX as u64 {
                        return Err(ControlError::InvalidOperation(
                            "Subscription DELIVERY DELAY exceeds the supported bound".to_owned(),
                        ));
                    }
                }
                if let Some(policy) = exhausted_policy {
                    if policy.max_delivery_attempts == 0 {
                        return Err(ControlError::InvalidOperation(
                            "Subscription MAX DELIVERY ATTEMPTS must be greater than zero"
                                .to_owned(),
                        ));
                    }
                }
                let source = match (feed, domain) {
                    (Some(feed), None) => {
                        let source = active_feed(state, &feed)?;
                        if source.space_id != space_id {
                            return Err(ControlError::InvalidOperation(
                                "Subscription and source Feed must belong to the same Domain"
                                    .to_owned(),
                            ));
                        }
                        SubscriptionSource::Feed {
                            feed_id: source.feed_id,
                        }
                    }
                    (None, Some(domain)) => {
                        // Domain-scoped source: binds the immutable Domain
                        // identity; member Feeds attach incrementally, so an
                        // empty Domain is a legal start. The Domain must
                        // still be the Subscription's own — cross-Domain
                        // consumption is refused like a cross-Domain Feed.
                        let source = active_space(state, &domain)?;
                        if source.space_id != space_id {
                            return Err(ControlError::InvalidOperation(
                                "Subscription and source Domain must be the same Domain"
                                    .to_owned(),
                            ));
                        }
                        SubscriptionSource::Domain {
                            domain_id: source.space_id,
                        }
                    }
                    _ => {
                        return Err(ControlError::InvalidOperation(
                            "Subscription needs exactly one source: feed or domain".to_owned(),
                        ))
                    }
                };
                let subscription_id = derived_resource_id(request_id, "subscription");
                if let Some(feed_id) = source.feed_id() {
                    let range_id = state
                        .range_maps
                        .get(&feed_id)
                        .and_then(|map| map.routes().first())
                        .map(|route| route.range_id)
                        .ok_or_else(|| {
                            ControlError::InvalidOperation(
                                "Subscription source Feed has no committed range placement"
                                    .to_owned(),
                            )
                        })?;
                    state
                        .range_assignments
                        .get(&range_id)
                        .filter(|assignment| assignment.feed_id == feed_id)
                        .ok_or_else(|| {
                            ControlError::InvalidOperation(
                                "Subscription source Feed placement is not applied".to_owned(),
                            )
                        })?;
                }
                let fixed = fixed.subscription_progress.ok_or_else(|| {
                    ControlError::InvalidOperation(
                        "Subscription progress has no consensus-prepared placement".to_owned(),
                    )
                })?;
                let replicas = ReplicaSet::try_new(fixed.replicas)
                    .map_err(|error| ControlError::InvalidOperation(error.to_string()))?;
                let placement = SubscriptionProgressAssignment::try_new(
                    subscription_id,
                    fixed.owner,
                    replicas,
                    1,
                )
                .map_err(|error| ControlError::InvalidOperation(error.to_string()))?;
                let definition = SubscriptionDefinition {
                    subscription_id,
                    name: name.clone(),
                    space_id,
                    source,
                    start,
                    delivery_delay_ns,
                    exhausted_policy,
                    stage: SubscriptionStage::Declared,
                    status: ResourceStatus::Active,
                    created_at_ns: issued_at_ns,
                };
                state
                    .subscription_progress_assignments
                    .insert(subscription_id, placement);
                state
                    .subscriptions
                    .insert(subscription_id, definition.clone());
                if definition.source.domain_id().is_some() {
                    // Existing member Feeds attach honoring the declared
                    // start policy, atomically with the declaration.
                    attach_subscription_feed_members(
                        state,
                        subscription_id,
                        space_id,
                        &definition.start,
                        state.revision.saturating_add(1),
                    );
                }
                Ok((
                    format!("declared Subscription {name}; shared consumption is not active"),
                    json!(definition),
                ))
            }
            Command::OpenReaderSession { reader, capacity } => {
                if capacity == 0 || capacity > 10_000 {
                    return Err(ControlError::InvalidOperation(
                        "Reader capacity must be between 1 and 10000".to_owned(),
                    ));
                }
                let definition = active_reader_mut(state, &reader)?;
                definition.session_epoch =
                    definition.session_epoch.checked_add(1).ok_or_else(|| {
                        ControlError::InvalidOperation("Reader session epoch overflow".to_owned())
                    })?;
                definition.session_capacity = capacity;
                definition.session_active = true;
                definition.delivered_cursor = definition.acknowledged_cursor.clone();
                definition.last_delivery = None;
                let updated = definition.clone();
                if let Some(frontier) = state.reader_frontiers.get_mut(&updated.reader_id) {
                    frontier.delivered = frontier.acknowledged.clone();
                    frontier.last_fetch_request_id = None;
                }
                Ok((format!("opened Reader session {reader}"), json!(updated)))
            }
            Command::RecordReaderDelivery {
                reader,
                session_epoch,
                cursor,
                fetch_request_id,
                fetch_limit,
                fetch_retry_after_ms,
            } => {
                let reader_id = {
                    let definition = active_reader_mut(state, &reader)?;
                    validate_reader_epoch(definition, session_epoch)?;
                    definition.reader_id
                };
                if state.reader_frontiers.contains_key(&reader_id) {
                    return Err(ControlError::InvalidOperation(
                        "Reader frontier cannot be replaced with a single-record delivery"
                            .to_owned(),
                    ));
                }
                let definition = active_reader_mut(state, &reader)?;
                if fetch_request_id.is_some()
                    && definition
                        .last_delivery
                        .as_ref()
                        .map(|replay| replay.request_id)
                        == fetch_request_id
                {
                    return Err(ControlError::InvalidOperation("Reader fetch request ID was already delivered; reopen the session to redeliver from acknowledged progress".to_owned()));
                }
                definition.last_delivery = fetch_request_id.map(|request_id| {
                    ReaderDeliveryReplay {
                        request_id,
                        previous_cursor: definition.delivered_cursor.clone(),
                        limit: fetch_limit.unwrap_or(0),
                        retry_after_ms: fetch_retry_after_ms.unwrap_or(0),
                    }
                });
                definition.delivered_cursor = Some(cursor);
                Ok((
                    format!("recorded Reader delivery {reader}"),
                    json!(definition.clone()),
                ))
            }
            Command::RecordReaderFrontier {
                reader,
                session_epoch,
                cursor,
                positions,
                expected_cursor,
                fence_delivery,
                fetch_request_id,
                fetch_limit,
                fetch_retry_after_ms,
            } => {
                let (reader_id, feed_id) = {
                    let definition = active_reader_mut(state, &reader)?;
                    validate_reader_epoch(definition, session_epoch)?;
                    if fence_delivery && definition.delivered_cursor != expected_cursor {
                        return Err(ControlError::InvalidOperation("Reader delivery advanced concurrently; reopen the session before retrying".to_owned()));
                    }
                    (definition.reader_id, definition.feed_id)
                };
                if cursor.is_empty()
                    || cursor.len() > 256
                    || positions.is_empty()
                    || positions.len() > 128
                    || positions.values().any(|value| value.len() > 256)
                    || state.range_maps.get(&feed_id).is_none_or(|map| {
                        positions.len() != map.routes().len()
                            || map
                                .routes()
                                .iter()
                                .any(|route| !positions.contains_key(&route.range_id))
                    })
                {
                    return Err(ControlError::InvalidOperation(
                        "Reader frontier exceeds bounds or references a stale range".to_owned(),
                    ));
                }
                let frontier = state.reader_frontiers.entry(reader_id).or_default();
                if fetch_request_id.is_some() && fetch_request_id == frontier.last_fetch_request_id
                {
                    return Err(ControlError::InvalidOperation("Reader fetch request ID was already delivered; reopen the session to redeliver from acknowledged progress".to_owned()));
                }
                frontier.previous = std::mem::replace(
                    &mut frontier.delivered,
                    positions.clone(),
                );
                frontier.last_fetch_limit = fetch_limit.unwrap_or(0);
                frontier.last_fetch_retry_after_ms = fetch_retry_after_ms.unwrap_or(0);
                frontier.last_fetch_request_id = fetch_request_id;
                let definition = active_reader_mut(state, &reader)?;
                definition.delivered_cursor = Some(cursor);
                Ok((
                    format!("recorded Reader frontier {reader}"),
                    json!({ "reader": definition.clone(), "positions": positions, "fetch_request_id": fetch_request_id }),
                ))
            }
            Command::AcknowledgeReader {
                reader,
                session_epoch,
                cursor,
            } => {
                let definition = active_reader_mut(state, &reader)?;
                validate_reader_epoch(definition, session_epoch)?;
                if definition.delivered_cursor.as_deref() != Some(cursor.as_str()) {
                    return Err(ControlError::InvalidOperation(
                        "Reader acknowledgement must match the latest delivered Cursor".to_owned(),
                    ));
                }
                definition.acknowledged_cursor = Some(cursor);
                let updated = definition.clone();
                if let Some(frontier) = state.reader_frontiers.get_mut(&updated.reader_id) {
                    frontier.acknowledged = frontier.delivered.clone();
                }
                Ok((format!("acknowledged Reader {reader}"), json!(updated)))
            }
            Command::CloseReaderSession {
                reader,
                session_epoch,
            } => {
                let definition = active_reader_mut(state, &reader)?;
                validate_reader_epoch(definition, session_epoch)?;
                definition.session_active = false;
                definition.session_capacity = 0;
                Ok((
                    format!("closed Reader session {reader}"),
                    json!(definition.clone()),
                ))
            }
            Command::CreateRole { name } => {
                validate_dotted_name(&name)?;
                ensure_name_available(
                    state.roles.values().map(|item| (&item.name, &item.status)),
                    &name,
                )?;
                let definition = RoleDefinition {
                    role_id: derived_resource_id(request_id, "role"),
                    name: name.clone(),
                    status: ResourceStatus::Active,
                    created_at_ns: issued_at_ns,
                };
                state.roles.insert(definition.role_id, definition.clone());
                Ok((format!("created Role {name}"), json!(definition)))
            }
            Command::PausePipe { name } => {
                self.set_pipe_stage(state, &name, PipeStage::Paused)
            }
            Command::ResumePipe { name } => {
                self.set_pipe_stage(state, &name, PipeStage::Declared)
            }
            Command::Rename { kind, current, new } => self.rename(state, kind, &current, &new),
            Command::Drop { kind, name } => self.drop_resource(state, kind, &name),
            Command::SetHistoryPolicy { scope, policy } => {
                validate_history_policy(&policy)?;
                let (scope_kind, target_id, name) = resolve_policy_target(state, &scope)?;
                let assignment = HistoryPolicyAssignment {
                    scope_kind: scope_kind.to_owned(),
                    target_id,
                    name: name.clone(),
                    policy,
                    updated_at_ns: issued_at_ns,
                };
                state
                    .history_policies
                    .insert(target_id, assignment.clone());
                Ok((
                    format!("set History Policy on {scope_kind} {name}"),
                    json!(assignment),
                ))
            }
            Command::ClearHistoryPolicy { scope } => {
                let (scope_kind, target_id, name) = resolve_policy_target(state, &scope)?;
                state
                    .history_policies
                    .remove(&target_id)
                    .map(|_| {
                        (
                            format!("cleared History Policy on {scope_kind} {name}"),
                            json!({ "scope_kind": scope_kind, "name": name }),
                        )
                    })
                    .ok_or_else(|| {
                        ControlError::NotFound(format!(
                            "History Policy on {scope_kind} {name}"
                        ))
                    })
            }
            Command::SetLegalHold { feed, reason } => {
                let (feed_id, feed_name) = {
                    let resolved = active_feed(state, &feed)?;
                    (resolved.feed_id, resolved.name.clone())
                };
                let hold = LegalHold {
                    feed_id,
                    name: feed_name.clone(),
                    reason,
                    set_at_ns: issued_at_ns,
                };
                state.legal_holds.insert(feed_id, hold.clone());
                Ok((format!("set legal hold on Feed {feed_name}"), json!(hold)))
            }
            Command::DropLegalHold { feed } => {
                let (feed_id, feed_name) = {
                    let resolved = active_feed(state, &feed)?;
                    (resolved.feed_id, resolved.name.clone())
                };
                state
                    .legal_holds
                    .remove(&feed_id)
                    .map(|hold| {
                        (
                            format!("dropped legal hold on Feed {feed_name}"),
                            json!(hold),
                        )
                    })
                    .ok_or_else(|| ControlError::NotFound(format!("legal hold on Feed {feed}")))
            }
            Command::EraseFeed { feed } => {
                let resolved = active_feed(state, &feed)?;
                let feed_id = resolved.feed_id;
                // Erasure is destruction: the same fences as DROP apply,
                // legal hold first — an erase under hold is exactly the
                // evidence destruction the hold exists to prevent.
                if state.legal_holds.contains_key(&feed_id) {
                    return Err(ControlError::InvalidOperation(format!(
                        "drop the legal hold on Feed {feed} before erasing it"
                    )));
                }
                if state
                    .writers
                    .values()
                    .any(|item| item.status == ResourceStatus::Active && item.feed_id == feed_id)
                    || state
                        .readers
                        .values()
                        .any(|item| item.status == ResourceStatus::Active && item.feed_id == feed_id)
                    || state.subscriptions.values().any(|item| {
                        item.status == ResourceStatus::Active
                            && (item.source.feed_id() == Some(feed_id)
                                || (item.source.domain_id().is_some()
                                    && item.space_id == resolved.space_id))
                    })
                    || state.pipes.values().any(|item| {
                        item.status == ResourceStatus::Active
                            && item.output_feed_id == Some(feed_id)
                    })
                    || state.distributaries.values().any(|item| {
                        item.status == ResourceStatus::Active
                            && item.target_feed_id == feed_id
                    })
                {
                    return Err(ControlError::InvalidOperation(
                        "drop attached Writers, Readers, Subscriptions, Pipes, and Distributaries first".to_owned(),
                    ));
                }
                let item = state.feeds.get_mut(&feed_id).expect("Feed exists");
                item.status = ResourceStatus::Dropped;
                item.erased_at_ns = Some(issued_at_ns);
                let erased = item.clone();
                // Key destruction makes retained ciphertext
                // unrecoverable everywhere — object stores, backups,
                // drained caches — even before the physical purge lands.
                let key_destroyed = if let Some(key) = state.feed_keys.get_mut(&feed_id) {
                    key.status = FeedKeyStatus::Destroyed;
                    key.dek_hex.clear();
                    key.destroyed_at_ns = Some(issued_at_ns);
                    true
                } else {
                    false
                };
                for members in state.subscription_feed_memberships.values_mut() {
                    members.remove(&feed_id);
                }
                state.history_policies.remove(&feed_id);
                state.legal_holds.remove(&feed_id);
                state.schemas.remove(&feed_id);
                Ok((
                    if key_destroyed {
                        format!(
                            "erased Feed {feed}: data key destroyed; stored history is being purged"
                        )
                    } else {
                        format!(
                            "erased Feed {feed}: stored history is being purged; no encryption key existed"
                        )
                    },
                    json!(erased),
                ))
            }
            Command::SetSchema { scope, schema } => {
                validate_feed_schema(&schema)?;
                let (scope_kind, target_id, name) = resolve_policy_target(state, &scope)?;
                let assignment = SchemaAssignment {
                    scope_kind: scope_kind.to_owned(),
                    target_id,
                    name: name.clone(),
                    schema,
                    updated_at_ns: issued_at_ns,
                };
                state.schemas.insert(target_id, assignment.clone());
                Ok((
                    format!("set schema on {scope_kind} {name}"),
                    json!(assignment),
                ))
            }
            Command::ClearSchema { scope } => {
                let (scope_kind, target_id, name) = resolve_policy_target(state, &scope)?;
                state
                    .schemas
                    .remove(&target_id)
                    .map(|_| {
                        (
                            format!("cleared schema on {scope_kind} {name}"),
                            json!({ "scope_kind": scope_kind, "name": name }),
                        )
                    })
                    .ok_or_else(|| {
                        ControlError::NotFound(format!("schema on {scope_kind} {name}"))
                    })
            }
            Command::Show { kind, limit } => Ok((
                format!("showing {}", show_name(&kind)),
                show_resources(
                    state,
                    kind,
                    limit,
                    &self.eligible_storage_nodes,
                    &self.capacity_policy,
                ),
            )),
            Command::Describe { kind, name } => Ok((
                format!("described {} {name}", resource_name(&kind)),
                describe_resource(state, kind, &name)?,
            )),
            Command::IssueApiKey {
                name,
                digest,
                role,
                expires_at_ns,
            } => {
                validate_dotted_name(&name)?;
                let digest = canonical_api_key_digest(&digest)?;
                if expires_at_ns.is_some_and(|expiry| expiry <= issued_at_ns) {
                    return Err(ControlError::InvalidOperation(format!(
                        "API key {name} expiry must lie in the future"
                    )));
                }
                let role_id = active_role(state, &role)?.role_id;
                if let Some(existing) = state.api_keys.values().find(|identity| {
                    identity.name == name && identity.status == ResourceStatus::Active
                }) {
                    if existing.digest == digest
                        && existing.role_id == role_id
                        && existing.expires_at_ns == expires_at_ns
                    {
                        return Ok((format!("API key {name} already issued"), json!(existing)));
                    }
                    return Err(ControlError::AlreadyExists(format!("API key {name}")));
                }
                if state.api_keys.len() >= MAX_ISSUED_API_KEYS {
                    return Err(ControlError::InvalidOperation(format!(
                        "issued API key catalog is full ({MAX_ISSUED_API_KEYS} identities)"
                    )));
                }
                let definition = ApiKeyIdentity {
                    key_id: derived_resource_id(request_id, "api-key"),
                    name: name.clone(),
                    digest,
                    role_id,
                    status: ResourceStatus::Active,
                    created_at_ns: issued_at_ns,
                    expires_at_ns,
                    revoked_at_ns: None,
                };
                state.api_keys.insert(definition.key_id, definition.clone());
                Ok((format!("issued API key {name}"), json!(definition)))
            }
            Command::RevokeApiKey { name } => {
                let identity = state
                    .api_keys
                    .values_mut()
                    .find(|identity| {
                        identity.name == name && identity.status == ResourceStatus::Active
                    })
                    .ok_or_else(|| ControlError::NotFound(format!("API key {name}")))?;
                identity.status = ResourceStatus::Dropped;
                identity.revoked_at_ns = Some(issued_at_ns);
                let identity = identity.clone();
                Ok((format!("revoked API key {name}"), json!(identity)))
            }
            Command::Grant {
                actions,
                namespace,
                role,
                effect,
            } => {
                validate_namespace_pattern(&namespace)?;
                let role_id = active_role(state, &role)?.role_id;
                if state.grants.values().any(|grant| {
                    grant.role_id == role_id
                        && grant.namespace == namespace
                        && grant.actions == actions
                        && grant.effect == effect
                }) {
                    return Err(ControlError::AlreadyExists(format!(
                        "grant for Role {role} on {namespace}"
                    )));
                }
                let definition = NamespaceGrant {
                    grant_id: derived_resource_id(request_id, "grant"),
                    role_id,
                    namespace: namespace.clone(),
                    actions,
                    effect,
                    created_at_ns: issued_at_ns,
                };
                state.grants.insert(definition.grant_id, definition.clone());
                let verb = match effect {
                    GrantEffect::Allow => "granted",
                    GrantEffect::Deny => "denied",
                };
                Ok((
                    format!("{verb} namespace permissions on {namespace} for Role {role}"),
                    json!(definition),
                ))
            }
            Command::RevokeGrant {
                actions,
                namespace,
                role,
                effect,
            } => {
                let role_id = active_role(state, &role)?.role_id;
                let mut changed = 0_usize;
                let mut emptied = Vec::new();
                for grant in state.grants.values_mut() {
                    if grant.role_id == role_id
                        && grant.namespace == namespace
                        && grant.effect == effect
                    {
                        let before = grant.actions.len();
                        grant.actions = grant.actions.difference(&actions).copied().collect();
                        if grant.actions.len() != before {
                            changed += 1;
                        }
                        if grant.actions.is_empty() {
                            emptied.push(grant.grant_id);
                        }
                    }
                }
                for grant_id in emptied {
                    state.grants.remove(&grant_id);
                }
                if changed == 0 {
                    return Err(ControlError::NotFound(format!(
                        "grant for Role {role} on {namespace}"
                    )));
                }
                let verb = match effect {
                    GrantEffect::Allow => "revoked",
                    GrantEffect::Deny => "un-denied",
                };
                Ok((
                    format!("{verb} namespace permissions on {namespace} for Role {role}"),
                    json!({
                        "role": role,
                        "namespace": namespace,
                        "effect": effect,
                        "actions_revoked": actions,
                        "grants_changed": changed,
                    }),
                ))
            }
            Command::ExplainAccess { role, action, feed } => {
                let role_id = active_role(state, &role)?.role_id;
                active_feed(state, &feed)?;
                let matched: Vec<_> = state
                    .grants
                    .values()
                    .filter(|grant| {
                        grant.role_id == role_id
                            && grant.effect == GrantEffect::Allow
                            && (grant.actions.contains(&action)
                                || grant.actions.contains(&PermissionAction::Manage))
                            && namespace_matches(&grant.namespace, &feed)
                    })
                    .cloned()
                    .collect();
                let denies: Vec<_> = state
                    .grants
                    .values()
                    .filter(|grant| {
                        grant.role_id == role_id
                            && grant.effect == GrantEffect::Deny
                            && (grant.actions.contains(&action)
                                || grant.actions.contains(&PermissionAction::Manage))
                            && namespace_matches(&grant.namespace, &feed)
                    })
                    .cloned()
                    .collect();
                // A covering DENY wins over every covering allow and
                // manage-implied coverage, matching request-time
                // authorization.
                let allowed = !matched.is_empty() && denies.is_empty();
                Ok((
                    format!(
                        "access {} for Role {role}",
                        if allowed { "allowed" } else { "denied" }
                    ),
                    json!({
                        "allowed": allowed,
                        "role": role,
                        "action": action,
                        "feed": feed,
                        "matched_grants": matched,
                        "matched_denies": denies,
                        "default": "deny"
                    }),
                ))
            }
            Command::SeekReader { reader, start } => {
                let definition = active_reader_mut(state, &reader)?;
                definition.acknowledged_cursor = match &start {
                    ReaderStart::Cursor(cursor) => Some(cursor.clone()),
                    ReaderStart::Beginning | ReaderStart::Now | ReaderStart::Timestamp(_) => None,
                };
                definition.start = start;
                definition.delivered_cursor = definition.acknowledged_cursor.clone();
                let updated = definition.clone();
                state.reader_frontiers.remove(&updated.reader_id);
                Ok((format!("moved Reader {reader} position"), json!(updated)))
            }
            Command::SeekSubscription { .. } => Err(ControlError::InvalidOperation(
                "SEEK SUBSCRIPTION is answered by the data-plane Subscription seek endpoint;                  repositioning resolves real Active Range positions, which the catalog cannot read"
                    .to_owned(),
            )),
            Command::GetIndex { .. } => Err(ControlError::InvalidOperation(
                "GET INDEX is answered by the data-plane Index query endpoint;                  the catalog does not materialize postings"
                    .to_owned(),
            )),
            Command::InspectPlacement { feed } => {
                let feed_id = active_feed(state, &feed)?.feed_id;
                let assignment = state.active_ranges.get(&feed_id).ok_or_else(|| {
                    ControlError::NotFound(format!("Active Range placement for Feed {feed}"))
                })?;
                let mut data = serde_json::to_value(assignment)?;
                if let Some(object) = data.as_object_mut() {
                    object.insert(
                        "range_map".to_owned(),
                        json!(state.range_maps.get(&feed_id)),
                    );
                    object.insert(
                        "range_split_plan".to_owned(),
                        json!(state.range_split_plans.get(&feed_id)),
                    );
                    object.insert(
                        "range_merge_plan".to_owned(),
                        json!(state.range_merge_plans.get(&feed_id)),
                    );
                    object.insert(
                        "range_move_plans".to_owned(),
                        json!(state
                            .range_move_plans
                            .values()
                            .filter(|plan| plan.feed_id == feed_id)
                            .collect::<Vec<_>>()),
                    );
                    object.insert(
                        "owner_move_plans".to_owned(),
                        json!(state
                            .owner_move_plans
                            .values()
                            .filter(|plan| plan.feed_id == feed_id)
                            .collect::<Vec<_>>()),
                    );
                    object.insert(
                        "range_assignments".to_owned(),
                        json!(state
                            .range_maps
                            .get(&feed_id)
                            .map(|map| map
                                .routes()
                                .iter()
                                .filter_map(|route| state.range_assignments.get(&route.range_id))
                                .collect::<Vec<_>>())
                            .unwrap_or_default()),
                    );
                    object.insert(
                        "commit_proofs".to_owned(),
                        json!(state
                            .range_maps
                            .get(&feed_id)
                            .map(|map| map
                                .routes()
                                .iter()
                                .filter_map(|route| {
                                    state
                                        .range_commit_proofs
                                        .get(&route.range_id)
                                        .map(|proof| (route.range_id.to_string(), json!(proof)))
                                })
                                .collect::<serde_json::Map<_, _>>())
                            .unwrap_or_default()),
                    );
                }
                Ok((format!("inspected placement for Feed {feed}"), data))
            }
            Command::TransferActiveRangeOwnership { .. } => Err(ControlError::InvalidOperation(
                "direct owner transfer is unsafe; use the verified owner-movement workflow"
                    .to_owned(),
            )),
            Command::RecoverActiveRangeOwnership {
                feed,
                expected_owner,
                expected_epoch,
                new_owner,
            } => {
                let feed_id = active_feed(state, &feed)?.feed_id;
                let assignment = state.active_ranges.get_mut(&feed_id).ok_or_else(|| {
                    ControlError::NotFound(format!("Active Range placement for Feed {feed}"))
                })?;
                if assignment.owner != expected_owner
                    || assignment.ownership_epoch != expected_epoch
                {
                    return Err(ControlError::InvalidOperation(
                        "Active Range ownership changed while recovery was being planned"
                            .to_owned(),
                    ));
                }
                let next_epoch = expected_epoch
                    .checked_next()
                    .map_err(|error| ControlError::InvalidOperation(error.to_string()))?;
                assignment
                    .transfer_ownership(new_owner, next_epoch)
                    .map_err(|error| ControlError::InvalidOperation(error.to_string()))?;
                let updated = assignment.clone();
                state
                    .range_assignments
                    .insert(updated.range_id, updated.clone());
                Ok((
                    format!("recovered Active Range ownership for Feed {feed}"),
                    json!(updated),
                ))
            }
            Command::RecoverSubscriptionProgressOwner {
                subscription_id,
                expected_ownership_epoch,
                new_owner,
            } => {
                let current = state
                    .subscription_progress_assignments
                    .get(&subscription_id)
                    .cloned()
                    .ok_or_else(|| {
                        ControlError::NotFound(format!(
                            "Subscription progress placement for {subscription_id}"
                        ))
                    })?;
                if current.ownership_epoch != expected_ownership_epoch {
                    return Err(ControlError::InvalidOperation(
                        "Subscription progress ownership changed while recovery was being planned"
                            .to_owned(),
                    ));
                }
                if !current.replicas.contains(&new_owner) {
                    return Err(ControlError::InvalidOperation(format!(
                        "replacement owner {new_owner} is not a member of the Subscription progress replica set"
                    )));
                }
                if !self.storage_eligible(state, &new_owner, issued_at_ns) {
                    return Err(ControlError::InvalidOperation(format!(
                        "replacement owner {new_owner} is not eligible for storage"
                    )));
                }
                let next_epoch = expected_ownership_epoch.checked_add(1).ok_or_else(|| {
                    ControlError::InvalidOperation(
                        "Subscription progress ownership epoch overflowed".to_owned(),
                    )
                })?;
                let next = SubscriptionProgressAssignment::try_new(
                    subscription_id,
                    new_owner,
                    current.replicas.clone(),
                    next_epoch,
                )
                .map_err(|error| ControlError::InvalidOperation(error.to_string()))?;
                state
                    .subscription_progress_assignments
                    .insert(subscription_id, next.clone());
                Ok((
                    format!(
                        "recovered Subscription progress ownership for {subscription_id} at epoch {next_epoch}"
                    ),
                    json!(next),
                ))
            }
            Command::MoveSubscriptionProgressReplica {
                subscription_id,
                expected_ownership_epoch,
                replaced,
                replacement,
            } => {
                let current = state
                    .subscription_progress_assignments
                    .get(&subscription_id)
                    .cloned()
                    .ok_or_else(|| {
                        ControlError::NotFound(format!(
                            "Subscription progress placement for {subscription_id}"
                        ))
                    })?;
                if current.ownership_epoch != expected_ownership_epoch {
                    return Err(ControlError::InvalidOperation(
                        "Subscription progress ownership changed while the move was being planned"
                            .to_owned(),
                    ));
                }
                if !current.replicas.contains(&replaced) {
                    return Err(ControlError::InvalidOperation(format!(
                        "replaced Node {replaced} is not a member of the Subscription progress replica set"
                    )));
                }
                if current.owner == replaced {
                    return Err(ControlError::InvalidOperation(
                        "the Subscription progress owner cannot be replaced; recover ownership to a surviving replica first"
                            .to_owned(),
                    ));
                }
                if current.replicas.contains(&replacement) {
                    return Err(ControlError::InvalidOperation(format!(
                        "replacement Node {replacement} is already a Subscription progress replica"
                    )));
                }
                if !self.storage_eligible(state, &replacement, issued_at_ns) {
                    return Err(ControlError::InvalidOperation(format!(
                        "replacement Node {replacement} is not eligible for storage"
                    )));
                }
                let mut swapped = current.replicas.as_array().clone();
                for slot in swapped.iter_mut() {
                    if *slot == replaced {
                        *slot = replacement.clone();
                    }
                }
                let replicas = ReplicaSet::try_new(swapped)
                    .map_err(|error| ControlError::InvalidOperation(error.to_string()))?;
                let next_epoch = expected_ownership_epoch.checked_add(1).ok_or_else(|| {
                    ControlError::InvalidOperation(
                        "Subscription progress ownership epoch overflowed".to_owned(),
                    )
                })?;
                let next = SubscriptionProgressAssignment::try_new(
                    subscription_id,
                    current.owner,
                    replicas,
                    next_epoch,
                )
                .map_err(|error| ControlError::InvalidOperation(error.to_string()))?;
                state
                    .subscription_progress_assignments
                    .insert(subscription_id, next.clone());
                Ok((
                    format!(
                        "moved Subscription progress replica {replaced} to {replacement} at epoch {next_epoch}"
                    ),
                    json!(next),
                ))
            }
            Command::RecoverStateStoreOwner {
                store_id,
                expected_ownership_epoch,
                new_owner,
            } => {
                let current = state
                    .state_store_assignments
                    .get(&store_id)
                    .cloned()
                    .ok_or_else(|| {
                        ControlError::NotFound(format!(
                            "StateStore journal placement for {store_id}"
                        ))
                    })?;
                if current.ownership_epoch != expected_ownership_epoch {
                    return Err(ControlError::InvalidOperation(
                        "StateStore ownership changed while recovery was being planned"
                            .to_owned(),
                    ));
                }
                if !current.replicas.contains(&new_owner) {
                    return Err(ControlError::InvalidOperation(format!(
                        "replacement owner {new_owner} is not a member of the StateStore journal replica set"
                    )));
                }
                if !self.storage_eligible(state, &new_owner, issued_at_ns) {
                    return Err(ControlError::InvalidOperation(format!(
                        "replacement owner {new_owner} is not eligible for storage"
                    )));
                }
                let next_epoch = expected_ownership_epoch.checked_add(1).ok_or_else(|| {
                    ControlError::InvalidOperation(
                        "StateStore ownership epoch overflowed".to_owned(),
                    )
                })?;
                let next = crate::state_store::StateStoreAssignment::try_new(
                    store_id,
                    new_owner,
                    current.replicas.clone(),
                    next_epoch,
                )
                .map_err(|error| ControlError::InvalidOperation(error.to_string()))?;
                state.state_store_assignments.insert(store_id, next.clone());
                Ok((
                    format!(
                        "recovered StateStore journal ownership for {store_id} at epoch {next_epoch}"
                    ),
                    json!(next),
                ))
            }
            Command::MovePipeComputeLease {
                pipe,
                new_owner,
                expected_compute_epoch,
            } => {
                let definition = state
                    .pipes
                    .values()
                    .find(|item| {
                        item.name == pipe && item.status == ResourceStatus::Active
                    })
                    .cloned()
                    .ok_or_else(|| ControlError::NotFound(format!("Pipe {pipe}")))?;
                let current = effective_pipe_compute_lease(state, definition.pipe_id)
                    .ok_or_else(|| {
                        ControlError::NotFound(format!("Pipe compute lease for {pipe}"))
                    })?;
                if let Some(expected) = expected_compute_epoch {
                    if current.compute_epoch != expected {
                        return Err(ControlError::InvalidOperation(
                            "Pipe compute lease changed while the move was being planned"
                                .to_owned(),
                        ));
                    }
                }
                if current.owner == new_owner {
                    return Err(ControlError::InvalidOperation(format!(
                        "{new_owner} already owns the compute lease for Pipe {pipe}"
                    )));
                }
                if !self.storage_eligible(state, &new_owner, issued_at_ns) {
                    return Err(ControlError::InvalidOperation(format!(
                        "compute lease target {new_owner} is not eligible for storage"
                    )));
                }
                let next_epoch = current.compute_epoch.checked_add(1).ok_or_else(|| {
                    ControlError::InvalidOperation(
                        "Pipe compute lease epoch overflowed".to_owned(),
                    )
                })?;
                let next = PipeComputeLease {
                    pipe_id: definition.pipe_id,
                    owner: new_owner.clone(),
                    compute_epoch: next_epoch,
                };
                state
                    .pipe_compute_leases
                    .insert(definition.pipe_id, next.clone());
                Ok((
                    format!(
                        "moved Pipe {pipe} compute lease to {new_owner} at epoch {next_epoch}"
                    ),
                    json!(next),
                ))
            }
            Command::DefineRiverbedLink {
                name,
                endpoints,
                key_env,
            } => {
                validate_dotted_name(&name)?;
                ensure_name_available(
                    state
                        .riverbed_links
                        .values()
                        .map(|item| (&item.name, &item.status)),
                    &name,
                )?;
                if endpoints.is_empty() || endpoints.len() > 8 {
                    return Err(ControlError::InvalidOperation(
                        "a Riverbed Link names between 1 and 8 endpoints".to_owned(),
                    ));
                }
                for endpoint in &endpoints {
                    if !(endpoint.starts_with("http://") || endpoint.starts_with("https://"))
                        || endpoint.len() > 512
                    {
                        return Err(ControlError::InvalidOperation(format!(
                            "Riverbed Link endpoint {endpoint} must be an http:// or https:// URL"
                        )));
                    }
                }
                if key_env.is_empty()
                    || key_env.len() > 128
                    || !key_env
                        .bytes()
                        .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
                    || !key_env.bytes().next().is_some_and(|b| b.is_ascii_uppercase())
                {
                    return Err(ControlError::InvalidOperation(format!(
                        "Riverbed Link credential {key_env} must name an environment variable (A-Z, 0-9, _)"
                    )));
                }
                let link = RiverbedLink {
                    link_id: derived_resource_id(request_id, "riverbed-link"),
                    name: name.clone(),
                    endpoints: endpoints.clone(),
                    key_env: key_env.clone(),
                    status: ResourceStatus::Active,
                    created_at_ns: issued_at_ns,
                };
                state.riverbed_links.insert(link.link_id, link.clone());
                Ok((format!("declared Riverbed Link {name}"), json!(link)))
            }
            Command::DefineDistributary {
                name,
                link,
                source_feed,
                target_feed,
            } => {
                validate_dotted_name(&name)?;
                ensure_name_available(
                    state
                        .distributaries
                        .values()
                        .map(|item| (&item.name, &item.status)),
                    &name,
                )?;
                let link_record = state
                    .riverbed_links
                    .values()
                    .find(|item| item.name == *link && item.status == ResourceStatus::Active)
                    .cloned()
                    .ok_or_else(|| ControlError::NotFound(format!("Riverbed Link {link}")))?;
                let feed = active_feed(state, &target_feed)?;
                let owner = self
                    .storage_pool(state, issued_at_ns)
                    .into_iter()
                    .next()
                    .ok_or_else(|| {
                        ControlError::InvalidOperation(
                            "no storage-eligible Node can hold the Distributary lease".to_owned(),
                        )
                    })?;
                let definition = DistributaryDefinition {
                    distributary_id: derived_resource_id(request_id, "distributary"),
                    name: name.clone(),
                    space_id: feed.space_id,
                    link_id: link_record.link_id,
                    source_feed: source_feed.clone(),
                    target_feed_id: feed.feed_id,
                    status: ResourceStatus::Active,
                    created_at_ns: issued_at_ns,
                };
                state
                    .distributaries
                    .insert(definition.distributary_id, definition.clone());
                let lease = DistributaryLease {
                    distributary_id: definition.distributary_id,
                    owner,
                    epoch: 1,
                };
                state
                    .distributary_leases
                    .insert(definition.distributary_id, lease);
                Ok((
                    format!(
                        "declared Distributary {name}; no records flow until a Distributary driver exists"
                    ),
                    json!(definition),
                ))
            }
            Command::MoveDistributaryLease {
                distributary,
                new_owner,
                expected_epoch,
            } => {
                let definition = state
                    .distributaries
                    .values()
                    .find(|item| {
                        item.name == *distributary && item.status == ResourceStatus::Active
                    })
                    .cloned()
                    .ok_or_else(|| {
                        ControlError::NotFound(format!("Distributary {distributary}"))
                    })?;
                let current = state
                    .distributary_leases
                    .get(&definition.distributary_id)
                    .cloned()
                    .ok_or_else(|| {
                        ControlError::NotFound(format!(
                            "Distributary lease for {distributary}"
                        ))
                    })?;
                if let Some(expected) = expected_epoch {
                    if current.epoch != expected {
                        return Err(ControlError::InvalidOperation(
                            "Distributary lease changed while the move was being planned"
                                .to_owned(),
                        ));
                    }
                }
                if current.owner == new_owner {
                    return Err(ControlError::InvalidOperation(format!(
                        "{new_owner} already owns the lease for Distributary {distributary}"
                    )));
                }
                if !self.storage_eligible(state, &new_owner, issued_at_ns) {
                    return Err(ControlError::InvalidOperation(format!(
                        "lease target {new_owner} is not eligible for storage"
                    )));
                }
                let next_epoch = current.epoch.checked_add(1).ok_or_else(|| {
                    ControlError::InvalidOperation(
                        "Distributary lease epoch overflowed".to_owned(),
                    )
                })?;
                let next = DistributaryLease {
                    distributary_id: definition.distributary_id,
                    owner: new_owner.clone(),
                    epoch: next_epoch,
                };
                state
                    .distributary_leases
                    .insert(definition.distributary_id, next.clone());
                Ok((
                    format!(
                        "moved Distributary {distributary} lease to {new_owner} at epoch {next_epoch}"
                    ),
                    json!(next),
                ))
            }
            Command::ReportDistributaryStatus {
                distributary_id,
                status,
            } => {
                let definition = state
                    .distributaries
                    .get(&distributary_id)
                    .filter(|item| item.status == ResourceStatus::Active)
                    .cloned()
                    .ok_or_else(|| {
                        ControlError::NotFound(format!("Distributary {distributary_id}"))
                    })?;
                let lease = state
                    .distributary_leases
                    .get(&distributary_id)
                    .cloned()
                    .ok_or_else(|| {
                        ControlError::NotFound(format!(
                            "Distributary lease for {distributary_id}"
                        ))
                    })?;
                // The committed lease is the fence: a report from a
                // previous owner or an older epoch must not overwrite
                // the live row after a raced move.
                if lease.epoch != status.lease_epoch || lease.owner != status.reported_by {
                    return Err(ControlError::InvalidOperation(format!(
                        "Distributary status report for {} does not match the committed lease",
                        definition.name
                    )));
                }
                state
                    .distributary_status
                    .insert(distributary_id, status.clone());
                Ok((
                    format!("recorded status for Distributary {}", definition.name),
                    json!(status),
                ))
            }
            Command::RegisterStorageNode {
                node,
                endpoint,
                cert_pins,
                capabilities,
            } => {
                if !capabilities.storage {
                    return Err(ControlError::InvalidOperation(
                        "REGISTER STORAGE NODE must keep the storage capability; a compute/gateway-only Node is never registered for storage".to_owned(),
                    ));
                }
                let mut normalized_pins = BTreeSet::new();
                for pin in &cert_pins {
                    let lower = pin.to_ascii_lowercase();
                    if lower.len() != 64 || !lower.bytes().all(|b| b.is_ascii_hexdigit()) {
                        return Err(ControlError::InvalidOperation(format!(
                            "storage Node {node} certificate pin must be a 64-digit hex fingerprint"
                        )));
                    }
                    normalized_pins.insert(lower);
                }
                // A certificate pin may belong to exactly one storage Node;
                // without uniqueness the pinned-peer lookup could map one
                // certificate to two identities.
                for (other, record) in &state.storage_nodes {
                    if *other == node {
                        continue;
                    }
                    if let Some(pin) = record.cert_pins.intersection(&normalized_pins).next() {
                        return Err(ControlError::InvalidOperation(format!(
                            "storage Node {node} certificate pin {pin} is already registered to storage Node {other}"
                        )));
                    }
                }
                if let Some(existing) = state.storage_nodes.get(&node) {
                    if existing.endpoint != endpoint {
                        return Err(ControlError::InvalidOperation(format!(
                            "storage Node {node} is already registered with a different endpoint; retire it before re-registering"
                        )));
                    }
                    // Re-registration replaces the certificate pin set so a
                    // Node can rotate certificates: register old and new pins
                    // while the new certificate rolls out, then drop the old
                    // pin once rotation completes.
                    let mut record = existing.clone();
                    record.cert_pins = normalized_pins;
                    record.capabilities = capabilities;
                    state.storage_nodes.insert(node.clone(), record.clone());
                    return Ok((format!("re-registered storage Node {node}"), json!(record)));
                }
                if endpoint.trim().is_empty()
                    || !matches!(
                        endpoint.split_once("://").map(|(scheme, _)| scheme),
                        Some("http" | "https")
                    )
                {
                    return Err(ControlError::InvalidOperation(format!(
                        "storage Node {node} endpoint must be an http or https URL"
                    )));
                }
                let record = StorageNodeRecord {
                    node: node.clone(),
                    endpoint,
                    registered_at_ns: issued_at_ns,
                    cert_pins: normalized_pins,
                    capabilities,
                };
                state.retired_storage_nodes.remove(&node);
                state.storage_nodes.insert(node.clone(), record.clone());
                Ok((format!("registered storage Node {node}"), json!(record)))
            }
            Command::RetireStorageNode { node } => {
                let known = state.storage_nodes.contains_key(&node)
                    || self.eligible_storage_nodes.contains(&node);
                if !known || state.retired_storage_nodes.contains(&node) {
                    return Err(ControlError::NotFound(format!("storage Node {node}")));
                }
                if let Some(holder) = storage_node_in_use(state, &node) {
                    return Err(ControlError::InvalidOperation(format!(
                        "storage Node {node} still holds {holder}; move every placement off it before retiring"
                    )));
                }
                state.storage_nodes.remove(&node);
                state.draining_storage_nodes.remove(&node);
                state.retired_storage_nodes.insert(node.clone());
                Ok((
                    format!("retired storage Node {node}"),
                    json!({ "node": node, "retired": true }),
                ))
            }
            Command::DrainStorageNode { node } => {
                let known = state.storage_nodes.contains_key(&node)
                    || self.eligible_storage_nodes.contains(&node);
                if !known || state.retired_storage_nodes.contains(&node) {
                    return Err(ControlError::NotFound(format!("storage Node {node}")));
                }
                if !state.draining_storage_nodes.insert(node.clone()) {
                    return Ok((
                        format!("storage Node {node} is already draining"),
                        json!({ "node": node, "draining": true }),
                    ));
                }
                Ok((
                    format!("draining storage Node {node}"),
                    json!({ "node": node, "draining": true }),
                ))
            }
            Command::UndrainStorageNode { node } => {
                if !state.draining_storage_nodes.remove(&node) {
                    return Err(ControlError::NotFound(format!(
                        "draining storage Node {node}"
                    )));
                }
                Ok((
                    format!("restored storage Node {node} eligibility"),
                    json!({ "node": node, "draining": false }),
                ))
            }
            Command::InspectStorageDrain { node } => {
                let known = state.storage_nodes.contains_key(&node)
                    || self.eligible_storage_nodes.contains(&node);
                if !known {
                    return Err(ControlError::NotFound(format!("storage Node {node}")));
                }
                let plan = storage_drain_plan(state, self, &node);
                Ok((
                    format!("inspected drain for storage Node {node}"),
                    json!(plan),
                ))
            }
            Command::ReportStorageNodeCapacity {
                node,
                capacity,
                protocol_version,
                build,
            } => {
                // Protocol gate: a versioned report outside the levels
                // this binary serves means the Node rolled ahead of — or
                // fell too far behind — this Riverbed. The refusal is
                // committed evidence in `incompatible_reports`, not just
                // a dropped response; version `0` is a pre-version Node
                // mid-upgrade and stays accepted.
                if protocol_version > crate::node::NODE_PROTOCOL_VERSION
                    || (protocol_version > 0
                        && protocol_version < crate::node::MIN_NODE_PROTOCOL_VERSION)
                {
                    state
                        .incompatible_reports
                        .entry(node.clone())
                        .and_modify(|row| {
                            row.protocol_version = protocol_version;
                            row.build = build.clone();
                            row.refused_at_ns = row.refused_at_ns.max(issued_at_ns);
                        })
                        .or_insert_with(|| IncompatibleNodeReport {
                            protocol_version,
                            build: build.clone(),
                            refused_at_ns: issued_at_ns,
                        });
                    while state.incompatible_reports.len() > MAX_INCOMPATIBLE_REPORTS {
                        let oldest = state
                            .incompatible_reports
                            .iter()
                            .min_by_key(|(_, row)| row.refused_at_ns)
                            .map(|(id, _)| id.clone());
                        match oldest {
                            Some(id) => {
                                state.incompatible_reports.remove(&id);
                            }
                            None => break,
                        }
                    }
                    return Err(ControlError::InvalidOperation(format!(
                        "storage Node {node} reports unsupported protocol {protocol_version} \
                         (this binary serves {}..={})",
                        crate::node::MIN_NODE_PROTOCOL_VERSION,
                        crate::node::NODE_PROTOCOL_VERSION
                    )));
                }
                let known = state.storage_nodes.contains_key(&node)
                    || self.eligible_storage_nodes.contains(&node);
                if !known {
                    return Err(ControlError::NotFound(format!(
                        "capacity report rejected for unknown storage Node {node}"
                    )));
                }
                // issued_at_ns is the committed instant: replayed or
                // out-of-order reports never roll the record backwards.
                let fresh = state
                    .capacity_reports
                    .get(&node)
                    .is_none_or(|existing| existing.reported_at_ns < issued_at_ns);
                if fresh {
                    state.capacity_reports.insert(
                        node.clone(),
                        CapacityReport {
                            capacity,
                            reported_at_ns: issued_at_ns,
                            protocol_version,
                            build: build.clone(),
                        },
                    );
                }
                Ok((
                    format!("recorded capacity report for storage Node {node}"),
                    json!(state.capacity_reports.get(&node)),
                ))
            }
            Command::PrepareActiveRangeSplit { feed, split_at } => {
                let feed_id = active_feed(state, &feed)?.feed_id;
                // A retry of the same logical split under a fresh request ID
                // resumes the staged plan rather than fencing on itself; a
                // different boundary or operation kind still fences so two
                // conflicting cutovers can never interleave.
                if let Some(plan) = state.range_split_plans.get(&feed_id) {
                    if plan.split_at == split_at {
                        return Ok((
                            format!("replayed staged Active Range split for Feed {feed}"),
                            json!(plan),
                        ));
                    }
                }
                if range_operation_pending(state, feed_id) {
                    return Err(ControlError::AlreadyExists(format!(
                        "range operation for Feed {feed}"
                    )));
                }
                let current_map = state
                    .range_maps
                    .get(&feed_id)
                    .ok_or_else(|| ControlError::NotFound(format!("RangeMap for Feed {feed}")))?;
                let source_route = current_map.route_token(split_at).clone();
                let source_assignment = state
                    .range_assignments
                    .get(&source_route.range_id)
                    .ok_or_else(|| {
                        ControlError::NotFound(format!(
                            "assignment for source Range {}",
                            source_route.range_id
                        ))
                    })?;
                let right_range_id =
                    RangeId::from_uuid(derived_resource_id(request_id, "active-range-split-right"));
                let candidate_map = current_map
                    .split(source_route.range_id, split_at, right_range_id)
                    .map_err(|error| ControlError::InvalidOperation(error.to_string()))?;
                let placement = fixed.active_range.ok_or_else(|| {
                    ControlError::InvalidOperation(
                        "split preparation has no consensus-prepared RF3 placement".to_owned(),
                    )
                })?;
                // Split staging reads the frozen source prefix on every
                // right-hand replica, so the right-hand range must stay on
                // Nodes that already hold the source Range; the consensus
                // draw only chooses which of them owns it.
                let replicas = source_assignment.replicas.clone();
                let owner = placement
                    .replicas
                    .iter()
                    .find(|node| **node != source_assignment.owner && replicas.contains(node))
                    .or_else(|| {
                        replicas
                            .iter()
                            .find(|node| *node != &source_assignment.owner)
                    })
                    .cloned()
                    .ok_or_else(|| {
                        ControlError::InvalidOperation(
                            "split source replicas cannot elect a distinct right-hand owner"
                                .to_owned(),
                        )
                    })?;
                let generation = candidate_map
                    .routes()
                    .iter()
                    .find(|route| route.range_id == right_range_id)
                    .ok_or_else(|| {
                        ControlError::InvalidOperation(
                            "candidate map omitted the right-hand range".to_owned(),
                        )
                    })?
                    .generation;
                let left_generation = candidate_map
                    .routes()
                    .iter()
                    .find(|route| route.range_id == source_route.range_id)
                    .ok_or_else(|| {
                        ControlError::InvalidOperation(
                            "candidate map omitted the left-hand range".to_owned(),
                        )
                    })?
                    .generation;
                let left_assignment = ActiveRangeAssignment::try_new(
                    feed_id,
                    source_route.range_id,
                    left_generation,
                    source_assignment.owner.clone(),
                    source_assignment.replicas.clone(),
                    source_assignment.ownership_epoch,
                )
                .map_err(|error| ControlError::InvalidOperation(error.to_string()))?;
                let right_assignment = ActiveRangeAssignment::try_new(
                    feed_id,
                    right_range_id,
                    generation,
                    owner,
                    replicas,
                    OwnershipEpoch::new(1),
                )
                .map_err(|error| ControlError::InvalidOperation(error.to_string()))?;
                let plan = RangeSplitPlan {
                    plan_id: derived_resource_id(request_id, "active-range-split-plan"),
                    feed_id,
                    source_range_id: source_route.range_id,
                    split_at,
                    candidate_map,
                    left_assignment: Some(left_assignment),
                    right_assignment,
                    stage: RangeSplitStage::Prepared,
                    source_commit: None,
                    source_scanned_through: None,
                    right_commit: None,
                    checksum_verified: false,
                };
                state.range_split_plans.insert(feed_id, plan.clone());
                Ok((
                    format!("prepared Active Range split for Feed {feed}"),
                    json!(plan),
                ))
            }
            Command::RecordActiveRangeSplitCatchUp {
                feed,
                plan_id,
                source_commit,
                source_scanned_through,
                right_commit,
                checksum_verified,
            } => {
                let feed_id = active_feed(state, &feed)?.feed_id;
                let plan = state.range_split_plans.get_mut(&feed_id).ok_or_else(|| {
                    ControlError::NotFound(format!("Active Range split plan for Feed {feed}"))
                })?;
                if plan.plan_id != plan_id {
                    return Err(ControlError::InvalidOperation(
                        "split catch-up evidence belongs to a stale plan".to_owned(),
                    ));
                }
                plan.source_commit = Some(source_commit);
                plan.source_scanned_through = Some(source_scanned_through);
                plan.right_commit = Some(right_commit);
                plan.checksum_verified = checksum_verified;
                plan.stage = if checksum_verified && source_commit == source_scanned_through {
                    RangeSplitStage::Ready
                } else {
                    RangeSplitStage::CatchingUp
                };
                Ok((
                    format!("recorded Active Range split catch-up for Feed {feed}"),
                    json!(plan.clone()),
                ))
            }
            Command::ActivateActiveRangeSplit {
                feed,
                plan_id,
                left_writer_sequences,
                right_writer_sequences,
                reader_translations,
                subscription_translations,
            } => {
                let feed_id = active_feed(state, &feed)?.feed_id;
                let plan = state
                    .range_split_plans
                    .get(&feed_id)
                    .cloned()
                    .ok_or_else(|| {
                        ControlError::NotFound(format!("Active Range split plan for Feed {feed}"))
                    })?;
                if plan.plan_id != plan_id || plan.stage != RangeSplitStage::Ready {
                    return Err(ControlError::InvalidOperation(
                        "split plan is stale or not ready for activation".to_owned(),
                    ));
                }
                let left_assignment = plan.left_assignment.clone().ok_or_else(|| {
                    ControlError::InvalidOperation(
                        "split plan has no staged left assignment".to_owned(),
                    )
                })?;
                validate_reader_translations(
                    state,
                    feed_id,
                    &plan.candidate_map,
                    &reader_translations,
                )?;
                validate_subscription_translations(
                    state,
                    feed_id,
                    &plan.candidate_map,
                    plan.plan_id,
                    &subscription_translations,
                )?;
                for (range_id, progress) in [
                    (left_assignment.range_id, left_writer_sequences),
                    (plan.right_assignment.range_id, right_writer_sequences),
                ] {
                    for writer_progress in progress {
                        if let Some(writer) =
                            state.writers.get_mut(&writer_progress.writer_session_id)
                        {
                            if writer.session_epoch == writer_progress.writer_epoch {
                                writer.range_next_sequences.insert(
                                    range_id,
                                    writer_progress.max_sequence.checked_add(1).ok_or_else(
                                        || {
                                            ControlError::InvalidOperation(
                                                "Writer range sequence overflow".to_owned(),
                                            )
                                        },
                                    )?,
                                );
                            }
                        }
                    }
                }
                state.range_maps.insert(feed_id, plan.candidate_map.clone());
                state
                    .range_assignments
                    .insert(left_assignment.range_id, left_assignment.clone());
                state.range_assignments.insert(
                    plan.right_assignment.range_id,
                    plan.right_assignment.clone(),
                );
                state.active_ranges.insert(feed_id, left_assignment);
                install_reader_translations(state, reader_translations);
                for translation in subscription_translations {
                    state
                        .pending_subscription_cutovers
                        .insert(translation.subscription_id, translation);
                }
                state.range_split_plans.remove(&feed_id);
                Ok((
                    format!("activated Active Range split for Feed {feed}"),
                    json!({
                        "range_map": plan.candidate_map,
                        "left_assignment": plan.left_assignment,
                        "right_assignment": plan.right_assignment
                    }),
                ))
            }
            Command::PrepareActiveRangeMerge {
                feed,
                left_range_id,
                right_range_id,
            } => {
                let feed_id = active_feed(state, &feed)?.feed_id;
                // Same-shape merge retries resume the staged plan; a
                // different pair or operation kind still fences.
                if let Some(plan) = state.range_merge_plans.get(&feed_id) {
                    if plan.left_range_id == left_range_id
                        && plan.right_range_id == right_range_id
                    {
                        return Ok((
                            format!("replayed staged Active Range merge for Feed {feed}"),
                            json!(plan),
                        ));
                    }
                }
                if range_operation_pending(state, feed_id) {
                    return Err(ControlError::AlreadyExists(format!(
                        "range operation for Feed {feed}"
                    )));
                }
                let current_map = state
                    .range_maps
                    .get(&feed_id)
                    .ok_or_else(|| ControlError::NotFound(format!("RangeMap for Feed {feed}")))?;
                let candidate_map = current_map
                    .merge_adjacent(left_range_id, right_range_id)
                    .map_err(|error| ControlError::InvalidOperation(error.to_string()))?;
                let left_assignment = state
                    .range_assignments
                    .get(&left_range_id)
                    .ok_or_else(|| ControlError::NotFound(format!("Range {left_range_id}")))?;
                state
                    .range_assignments
                    .get(&right_range_id)
                    .ok_or_else(|| ControlError::NotFound(format!("Range {right_range_id}")))?;
                let generation = candidate_map
                    .routes()
                    .iter()
                    .find(|route| route.range_id == left_range_id)
                    .ok_or_else(|| {
                        ControlError::InvalidOperation(
                            "candidate merge map omitted target range".to_owned(),
                        )
                    })?
                    .generation;
                let merged_assignment = ActiveRangeAssignment::try_new(
                    feed_id,
                    left_range_id,
                    generation,
                    left_assignment.owner.clone(),
                    left_assignment.replicas.clone(),
                    left_assignment.ownership_epoch,
                )
                .map_err(|error| ControlError::InvalidOperation(error.to_string()))?;
                let plan = RangeMergePlan {
                    plan_id: derived_resource_id(request_id, "active-range-merge-plan"),
                    feed_id,
                    left_range_id,
                    right_range_id,
                    candidate_map,
                    merged_assignment,
                    stage: RangeMergeStage::Prepared,
                    left_commit: None,
                    right_commit: None,
                    merged_commit: None,
                    checksum_verified: false,
                };
                state.range_merge_plans.insert(feed_id, plan.clone());
                Ok((
                    format!("prepared Active Range merge for Feed {feed}"),
                    json!(plan),
                ))
            }
            Command::RecordActiveRangeMergeStaging {
                feed,
                plan_id,
                left_commit,
                right_commit,
                merged_commit,
                checksum_verified,
            } => {
                let feed_id = active_feed(state, &feed)?.feed_id;
                let plan = state.range_merge_plans.get_mut(&feed_id).ok_or_else(|| {
                    ControlError::NotFound(format!("Active Range merge plan for Feed {feed}"))
                })?;
                if plan.plan_id != plan_id {
                    return Err(ControlError::InvalidOperation(
                        "merge staging evidence belongs to a stale plan".to_owned(),
                    ));
                }
                plan.left_commit = Some(left_commit);
                plan.right_commit = Some(right_commit);
                plan.merged_commit = Some(merged_commit);
                plan.checksum_verified = checksum_verified;
                plan.stage = if checksum_verified {
                    RangeMergeStage::Ready
                } else {
                    RangeMergeStage::Staging
                };
                Ok((
                    format!("recorded Active Range merge staging for Feed {feed}"),
                    json!(plan.clone()),
                ))
            }
            Command::ActivateActiveRangeMerge {
                feed,
                plan_id,
                writer_sequences,
                reader_translations,
                subscription_translations,
            } => {
                let feed_id = active_feed(state, &feed)?.feed_id;
                let plan = state
                    .range_merge_plans
                    .get(&feed_id)
                    .cloned()
                    .ok_or_else(|| {
                        ControlError::NotFound(format!("Active Range merge plan for Feed {feed}"))
                    })?;
                if plan.plan_id != plan_id || plan.stage != RangeMergeStage::Ready {
                    return Err(ControlError::InvalidOperation(
                        "merge plan is stale or not ready for activation".to_owned(),
                    ));
                }
                validate_reader_translations(
                    state,
                    feed_id,
                    &plan.candidate_map,
                    &reader_translations,
                )?;
                validate_subscription_translations(
                    state,
                    feed_id,
                    &plan.candidate_map,
                    plan.plan_id,
                    &subscription_translations,
                )?;
                for progress in writer_sequences {
                    if let Some(writer) = state.writers.get_mut(&progress.writer_session_id) {
                        if writer.session_epoch == progress.writer_epoch {
                            writer.range_next_sequences.insert(
                                plan.left_range_id,
                                progress.max_sequence.checked_add(1).ok_or_else(|| {
                                    ControlError::InvalidOperation(
                                        "Writer range sequence overflow".to_owned(),
                                    )
                                })?,
                            );
                            writer.range_next_sequences.remove(&plan.right_range_id);
                        }
                    }
                }
                state.range_maps.insert(feed_id, plan.candidate_map.clone());
                state.range_assignments.remove(&plan.right_range_id);
                state.range_assignments.insert(
                    plan.merged_assignment.range_id,
                    plan.merged_assignment.clone(),
                );
                state
                    .active_ranges
                    .insert(feed_id, plan.merged_assignment.clone());
                install_reader_translations(state, reader_translations);
                for translation in subscription_translations {
                    state
                        .pending_subscription_cutovers
                        .insert(translation.subscription_id, translation);
                }
                state.range_merge_plans.remove(&feed_id);
                Ok((
                    format!("activated Active Range merge for Feed {feed}"),
                    json!({
                        "range_map": plan.candidate_map,
                        "merged_assignment": plan.merged_assignment,
                        "retired_range_id": plan.right_range_id
                    }),
                ))
            }
            Command::AbandonActiveRangeSplit { feed_id, plan_id } => {
                // Fenced removal: a stale abandon or an already-activated
                // plan is a no-op instead of an error.
                let removed = match state.range_split_plans.get(&feed_id) {
                    Some(plan) if plan.plan_id == plan_id => {
                        state.range_split_plans.remove(&feed_id);
                        true
                    }
                    _ => false,
                };
                Ok((
                    format!("abandoned Active Range split plan {plan_id}"),
                    json!({ "abandoned": removed }),
                ))
            }
            Command::AbandonActiveRangeMerge { feed_id, plan_id } => {
                let removed = match state.range_merge_plans.get(&feed_id) {
                    Some(plan) if plan.plan_id == plan_id => {
                        state.range_merge_plans.remove(&feed_id);
                        true
                    }
                    _ => false,
                };
                Ok((
                    format!("abandoned Active Range merge plan {plan_id}"),
                    json!({ "abandoned": removed }),
                ))
            }
            Command::RecordRangeCommitProof {
                feed_id,
                range_id,
                position,
                frame_digest,
                evidence,
                reporter,
            } => {
                if !state.feeds.values().any(|feed| feed.feed_id == feed_id) {
                    return Err(ControlError::NotFound(format!(
                        "Feed {feed_id} for committed-frontier proof"
                    )));
                }
                let existing = state.range_commit_proofs.get(&range_id);
                if let Some(existing) = existing {
                    if existing.position > position {
                        return Ok((
                            format!(
                                "retained newer committed-frontier proof for Range {range_id}"
                            ),
                            json!(existing),
                        ));
                    }
                    if existing.position == position && existing.frame_digest != frame_digest {
                        return Err(ControlError::InvalidOperation(format!(
                            "conflicting committed-frontier proof for Range {range_id} at position {}: fork evidence",
                            position.value()
                        )));
                    }
                }
                let mut merged = evidence.clone();
                if let Some(existing) = existing {
                    for node in &existing.evidence {
                        if !merged.contains(node) {
                            merged.push(node.clone());
                        }
                    }
                }
                merged.sort();
                let proof = RangeCommitProof {
                    feed_id,
                    range_id,
                    position,
                    frame_digest,
                    recorded_at_ns: issued_at_ns,
                    evidence: merged,
                    reporter,
                };
                state
                    .range_commit_proofs
                    .insert(range_id, proof.clone());
                Ok((
                    format!(
                        "recorded committed-frontier proof for Range {range_id} at position {}",
                        position.value()
                    ),
                    json!(proof),
                ))
            }
            Command::AbandonFollowerMove { feed_id, plan_id } => {
                // Move plans are keyed by RangeId; fence removal on the
                // stored plan's Feed and plan identities so a stale abandon
                // cannot delete a newer plan.
                let range_id = state
                    .range_move_plans
                    .iter()
                    .find(|(_, plan)| plan.feed_id == feed_id && plan.plan_id == plan_id)
                    .map(|(range_id, _)| *range_id);
                let removed = match range_id {
                    Some(range_id) => {
                        state.range_move_plans.remove(&range_id);
                        true
                    }
                    None => false,
                };
                Ok((
                    format!("abandoned follower move plan {plan_id}"),
                    json!({ "abandoned": removed }),
                ))
            }
            Command::AbandonOwnerMove { feed_id, plan_id } => {
                let range_id = state
                    .owner_move_plans
                    .iter()
                    .find(|(_, plan)| plan.feed_id == feed_id && plan.plan_id == plan_id)
                    .map(|(range_id, _)| *range_id);
                let removed = match range_id {
                    Some(range_id) => {
                        state.owner_move_plans.remove(&range_id);
                        true
                    }
                    None => false,
                };
                Ok((
                    format!("abandoned owner move plan {plan_id}"),
                    json!({ "abandoned": removed }),
                ))
            }
            Command::ClearSubscriptionCutover {
                subscription_id,
                plan_id,
            } => {
                let cleared = match state
                    .pending_subscription_cutovers
                    .get(&subscription_id)
                {
                    Some(pending) if pending.plan_id == plan_id => state
                        .pending_subscription_cutovers
                        .remove(&subscription_id)
                        .is_some(),
                    _ => false,
                };
                Ok((
                    format!("cleared pending Subscription cutover {subscription_id}"),
                    json!({ "cleared": cleared }),
                ))
            }
            Command::PrepareOwnerMove {
                feed,
                range_id,
                new_owner,
            } => {
                let feed_id = active_feed(state, &feed)?.feed_id;
                // Same-target owner-move retries resume the staged plan; a
                // different target or operation kind still fences.
                if let Some(plan) = state.owner_move_plans.get(&range_id) {
                    if plan.feed_id == feed_id && plan.candidate_assignment.owner == new_owner {
                        return Ok((
                            format!("replayed staged owner movement for Feed {feed}"),
                            json!(plan),
                        ));
                    }
                }
                if range_operation_pending(state, feed_id) {
                    return Err(ControlError::AlreadyExists(format!(
                        "range operation for Feed {feed}"
                    )));
                }
                let map = state
                    .range_maps
                    .get(&feed_id)
                    .ok_or_else(|| ControlError::NotFound(format!("RangeMap for Feed {feed}")))?;
                if !map.routes().iter().any(|route| route.range_id == range_id) {
                    return Err(ControlError::NotFound(format!(
                        "active Range {range_id} for Feed {feed}"
                    )));
                }
                let source = state.range_assignments.get(&range_id).ok_or_else(|| {
                    ControlError::NotFound(format!("placement for active Range {range_id}"))
                })?;
                if source.feed_id != feed_id
                    || source.owner == new_owner
                    || !source.replicas.contains(&new_owner)
                {
                    return Err(ControlError::InvalidOperation(
                        "owner movement requires a different current RF3 follower".to_owned(),
                    ));
                }
                let epoch = source
                    .ownership_epoch
                    .checked_next()
                    .map_err(|error| ControlError::InvalidOperation(error.to_string()))?;
                let mut candidate = source.clone();
                candidate
                    .transfer_ownership(new_owner, epoch)
                    .map_err(|error| ControlError::InvalidOperation(error.to_string()))?;
                let plan = RangeOwnerMovePlan {
                    plan_id: derived_resource_id(request_id, "owner-move-plan"),
                    feed_id,
                    source_assignment: source.clone(),
                    candidate_assignment: candidate,
                    stage: RangeMoveStage::Prepared,
                    source_commit: None,
                    target_commit: None,
                    checksum_verified: false,
                };
                state.completed_owner_move_plans.remove(&range_id);
                state.owner_move_plans.insert(range_id, plan.clone());
                Ok((format!("prepared owner move for Feed {feed}"), json!(plan)))
            }
            Command::RecordOwnerMoveCatchUp {
                feed,
                plan_id,
                source_commit,
                target_commit,
                checksum_verified,
            } => {
                let feed_id = active_feed(state, &feed)?.feed_id;
                let plan = state
                    .owner_move_plans
                    .values_mut()
                    .find(|plan| plan.plan_id == plan_id && plan.feed_id == feed_id)
                    .ok_or_else(|| {
                        ControlError::NotFound(format!(
                            "owner movement plan {plan_id} for Feed {feed}"
                        ))
                    })?;
                if source_commit < plan.source_commit.unwrap_or_default()
                    || target_commit < plan.target_commit.unwrap_or_default()
                    || target_commit > source_commit
                {
                    return Err(ControlError::InvalidOperation(
                        "owner movement evidence is backwards or target is ahead of source"
                            .to_owned(),
                    ));
                }
                plan.source_commit = Some(source_commit);
                plan.target_commit = Some(target_commit);
                plan.checksum_verified = checksum_verified;
                plan.stage = if checksum_verified && target_commit == source_commit {
                    RangeMoveStage::Ready
                } else {
                    RangeMoveStage::CatchingUp
                };
                Ok((
                    format!("recorded owner move catch-up for Feed {feed}"),
                    json!(plan.clone()),
                ))
            }
            Command::ActivateOwnerMove { feed, plan_id } => {
                let feed_id = active_feed(state, &feed)?.feed_id;
                let plan = state
                    .owner_move_plans
                    .values()
                    .find(|plan| plan.plan_id == plan_id && plan.feed_id == feed_id)
                    .cloned()
                    .ok_or_else(|| {
                        ControlError::NotFound(format!(
                            "owner movement plan {plan_id} for Feed {feed}"
                        ))
                    })?;
                if plan.stage != RangeMoveStage::Ready
                    || !plan.checksum_verified
                    || plan.source_commit.is_none()
                    || plan.source_commit != plan.target_commit
                    || state
                        .range_assignments
                        .get(&plan.source_assignment.range_id)
                        != Some(&plan.source_assignment)
                {
                    return Err(ControlError::InvalidOperation(
                        "owner movement is not verified or its source placement changed; recheck catch-up".to_owned(),
                    ));
                }
                let updated = plan.candidate_assignment.clone();
                state
                    .range_assignments
                    .insert(updated.range_id, updated.clone());
                if state
                    .active_ranges
                    .get(&feed_id)
                    .map(|assignment| assignment.range_id)
                    == Some(updated.range_id)
                {
                    state.active_ranges.insert(feed_id, updated.clone());
                }
                state.owner_move_plans.remove(&updated.range_id);
                state
                    .completed_owner_move_plans
                    .insert(updated.range_id, plan);
                Ok((
                    format!("activated owner move for Feed {feed}"),
                    json!(updated),
                ))
            }
            Command::AbortOwnerMove { feed, plan_id } => {
                let feed_id = active_feed(state, &feed)?.feed_id;
                let range_id = state
                    .owner_move_plans
                    .iter()
                    .find(|(_, plan)| plan.plan_id == plan_id && plan.feed_id == feed_id)
                    .map(|(range_id, _)| *range_id)
                    .ok_or_else(|| {
                        ControlError::NotFound(format!(
                            "owner movement plan {plan_id} for Feed {feed}"
                        ))
                    })?;
                state.owner_move_plans.remove(&range_id);
                Ok((
                    format!("aborted owner move for Feed {feed}"),
                    json!({"plan_id": plan_id}),
                ))
            }
            Command::PrepareFollowerMove {
                feed,
                range_id,
                removed_replica,
                replacement_replica,
            } => {
                let feed_id = active_feed(state, &feed)?.feed_id;
                // Same-replacement follower-move retries resume the staged
                // plan; a different replica choice or operation kind still
                // fences.
                if let Some(plan) = state.range_move_plans.get(&range_id) {
                    if plan.feed_id == feed_id
                        && plan.removed_replica == removed_replica
                        && plan.replacement_replica == replacement_replica
                    {
                        return Ok((
                            format!("replayed staged follower movement for Feed {feed}"),
                            json!(plan),
                        ));
                    }
                }
                if range_operation_pending(state, feed_id) {
                    return Err(ControlError::AlreadyExists(format!(
                        "range operation for Feed {feed}"
                    )));
                }
                let map = state
                    .range_maps
                    .get(&feed_id)
                    .ok_or_else(|| ControlError::NotFound(format!("RangeMap for Feed {feed}")))?;
                if !map.routes().iter().any(|route| route.range_id == range_id) {
                    return Err(ControlError::NotFound(format!(
                        "active Range {range_id} for Feed {feed}"
                    )));
                }
                let source = state.range_assignments.get(&range_id).ok_or_else(|| {
                    ControlError::NotFound(format!("placement for active Range {range_id}"))
                })?;
                if source.feed_id != feed_id
                    || removed_replica == source.owner
                    || !source.replicas.contains(&removed_replica)
                    || source.replicas.contains(&replacement_replica)
                {
                    return Err(ControlError::InvalidOperation(
                        "follower movement requires an assigned non-owner follower and a distinct replacement Node"
                            .to_owned(),
                    ));
                }
                let replicas = source
                    .replicas
                    .iter()
                    .filter(|node| **node != removed_replica)
                    .cloned()
                    .chain(std::iter::once(replacement_replica.clone()))
                    .collect::<Vec<_>>();
                let replicas: [StorageNodeId; ACTIVE_RANGE_REPLICA_COUNT] =
                    replicas.try_into().map_err(|_| {
                        ControlError::InvalidOperation(
                            "RF3 movement has invalid replica count".to_owned(),
                        )
                    })?;
                let epoch = source
                    .ownership_epoch
                    .checked_next()
                    .map_err(|error| ControlError::InvalidOperation(error.to_string()))?;
                let candidate_assignment = ActiveRangeAssignment::try_new(
                    feed_id,
                    range_id,
                    source.generation,
                    source.owner.clone(),
                    ReplicaSet::try_new(replicas)
                        .map_err(|error| ControlError::InvalidOperation(error.to_string()))?,
                    epoch,
                )
                .map_err(|error| ControlError::InvalidOperation(error.to_string()))?;
                let plan = RangeMovePlan {
                    plan_id: derived_resource_id(request_id, "follower-move-plan"),
                    feed_id,
                    source_assignment: source.clone(),
                    candidate_assignment,
                    removed_replica,
                    replacement_replica,
                    stage: RangeMoveStage::Prepared,
                    source_commit: None,
                    target_commit: None,
                    checksum_verified: false,
                };
                state.completed_move_plans.remove(&range_id);
                state.range_move_plans.insert(range_id, plan.clone());
                Ok((
                    format!("prepared follower move for Feed {feed}"),
                    json!(plan),
                ))
            }
            Command::RecordFollowerMoveCatchUp {
                feed,
                plan_id,
                source_commit,
                target_commit,
                checksum_verified,
            } => {
                let feed_id = active_feed(state, &feed)?.feed_id;
                let plan = state
                    .range_move_plans
                    .values_mut()
                    .find(|plan| plan.plan_id == plan_id && plan.feed_id == feed_id)
                    .ok_or_else(|| {
                        ControlError::NotFound(format!(
                            "follower movement plan {plan_id} for Feed {feed}"
                        ))
                    })?;
                if source_commit < plan.source_commit.unwrap_or_default()
                    || target_commit < plan.target_commit.unwrap_or_default()
                {
                    return Err(ControlError::InvalidOperation(
                        "follower movement evidence cannot move backwards".to_owned(),
                    ));
                }
                plan.source_commit = Some(source_commit);
                plan.target_commit = Some(target_commit);
                plan.checksum_verified = checksum_verified;
                plan.stage = if checksum_verified && target_commit == source_commit {
                    RangeMoveStage::Ready
                } else {
                    RangeMoveStage::CatchingUp
                };
                Ok((
                    format!("recorded follower move catch-up for Feed {feed}"),
                    json!(plan.clone()),
                ))
            }
            Command::ActivateFollowerMove { feed, plan_id } => {
                let feed_id = active_feed(state, &feed)?.feed_id;
                let plan = state
                    .range_move_plans
                    .values()
                    .find(|plan| plan.plan_id == plan_id && plan.feed_id == feed_id)
                    .cloned()
                    .ok_or_else(|| {
                        ControlError::NotFound(format!(
                            "follower movement plan {plan_id} for Feed {feed}"
                        ))
                    })?;
                if plan.stage != RangeMoveStage::Ready
                    || !plan.checksum_verified
                    || plan.source_commit != plan.target_commit
                    || state
                        .range_assignments
                        .get(&plan.source_assignment.range_id)
                        != Some(&plan.source_assignment)
                {
                    return Err(ControlError::InvalidOperation(
                        "follower movement is not verified or its source placement changed; recheck catch-up"
                            .to_owned(),
                    ));
                }
                let updated = plan.candidate_assignment.clone();
                state
                    .range_assignments
                    .insert(updated.range_id, updated.clone());
                if state
                    .active_ranges
                    .get(&feed_id)
                    .map(|assignment| assignment.range_id)
                    == Some(updated.range_id)
                {
                    state.active_ranges.insert(feed_id, updated.clone());
                }
                state.range_move_plans.remove(&updated.range_id);
                state.completed_move_plans.insert(updated.range_id, plan);
                Ok((
                    format!("activated follower move for Feed {feed}"),
                    json!(updated),
                ))
            }
            Command::AbortFollowerMove { feed, plan_id } => {
                let feed_id = active_feed(state, &feed)?.feed_id;
                let range_id = state
                    .range_move_plans
                    .iter()
                    .find(|(_, plan)| plan.plan_id == plan_id && plan.feed_id == feed_id)
                    .map(|(range_id, _)| *range_id)
                    .ok_or_else(|| {
                        ControlError::NotFound(format!(
                            "follower movement plan {plan_id} for Feed {feed}"
                        ))
                    })?;
                state.range_move_plans.remove(&range_id);
                Ok((
                    format!("aborted follower move for Feed {feed}"),
                    json!({"plan_id": plan_id}),
                ))
            }
        }
    }

    fn rename(
        &self,
        state: &mut CatalogState,
        kind: ResourceKind,
        current: &str,
        new: &str,
    ) -> Result<(String, Value), ControlError> {
        validate_dotted_name(new)?;
        match kind {
            ResourceKind::Feed => {
                ensure_name_available(
                    state.feeds.values().map(|item| (&item.name, &item.status)),
                    new,
                )?;
                let new_space_id = owning_space(state, new)?.space_id;
                let existing = active_feed(state, current)?.clone();
                if new_space_id != existing.space_id
                    && state.subscriptions.values().any(|subscription| {
                        subscription.status == ResourceStatus::Active
                            && subscription.source.feed_id() == Some(existing.feed_id)
                    })
                {
                    return Err(ControlError::InvalidOperation(
                        "drop attached Subscriptions before moving a Feed to another Domain"
                            .to_owned(),
                    ));
                }
                let item = active_feed_mut(state, current)?;
                item.name = new.to_owned();
                item.space_id = new_space_id;
                let moved = item.clone();
                if new_space_id != existing.space_id {
                    // Renaming within a Domain keeps the immutable FeedId,
                    // so memberships retain progress untouched. A cross-
                    // Domain move instead drains coverage in the old
                    // Domain (fenced pending work, never silently
                    // discarded) and attaches as a newly discovered Feed
                    // under every covering Subscription in the new Domain.
                    let covering: Vec<Uuid> = state
                        .subscriptions
                        .values()
                        .filter(|item| {
                            item.status == ResourceStatus::Active
                                && item.source.domain_id() == Some(existing.space_id)
                        })
                        .map(|item| item.subscription_id)
                        .collect();
                    let revision = state.revision.saturating_add(1);
                    for subscription_id in covering {
                        let members = state
                            .subscription_feed_memberships
                            .entry(subscription_id)
                            .or_default();
                        let member = members.entry(moved.feed_id).or_insert_with(|| {
                            SubscriptionFeedMembership {
                                feed_id: moved.feed_id,
                                state: SubscriptionFeedState::Active,
                                start: ReaderStart::Beginning,
                                attached_revision: revision,
                                detail: None,
                            }
                        });
                        member.state = SubscriptionFeedState::Draining;
                        member.detail = Some(
                            "Feed moved to another Domain; pending work stays fenced".to_owned(),
                        );
                    }
                    attach_feed_to_domain_subscriptions(state, &moved, revision)?;
                }
                Ok((format!("renamed Feed {current} to {new}"), json!(moved)))
            }
            ResourceKind::Domain | ResourceKind::Space => {
                if state.feeds.values().any(|feed| {
                    feed.status == ResourceStatus::Active
                        && is_within_namespace(&feed.name, current)
                }) {
                    return Err(ControlError::InvalidOperation(
                        "rename child Feeds before renaming a Domain".to_owned(),
                    ));
                }
                ensure_name_available(
                    state.spaces.values().map(|item| (&item.name, &item.status)),
                    new,
                )?;
                let item = active_space_mut(state, current)?;
                item.name = new.to_owned();
                Ok((
                    format!("renamed Domain {current} to {new}"),
                    json!(item.clone()),
                ))
            }
            ResourceKind::Writer => rename_named(&mut state.writers, current, new, "Writer"),
            ResourceKind::Reader => rename_named(&mut state.readers, current, new, "Reader"),
            ResourceKind::Subscription => {
                let existing_space = state
                    .subscriptions
                    .values()
                    .find(|item| item.name == current && item.status == ResourceStatus::Active)
                    .ok_or_else(|| ControlError::NotFound(format!("Subscription {current}")))?
                    .space_id;
                if owning_space(state, new)?.space_id != existing_space {
                    return Err(ControlError::InvalidOperation(
                        "Subscription cannot be renamed across Domains".to_owned(),
                    ));
                }
                rename_named(&mut state.subscriptions, current, new, "Subscription")
            }
            ResourceKind::Role => rename_named(&mut state.roles, current, new, "Role"),
            ResourceKind::Pipe => rename_named(&mut state.pipes, current, new, "Pipe"),
            ResourceKind::RiverbedLink => {
                rename_named(&mut state.riverbed_links, current, new, "Riverbed Link")
            }
            ResourceKind::Distributary => {
                rename_named(&mut state.distributaries, current, new, "Distributary")
            }
            ResourceKind::Index => {
                // Declared Indexes are catalog-only: renaming changes the
                // definition name without touching storage state.
                if state.indexes.values().any(|item| item.name == new) {
                    return Err(ControlError::AlreadyExists(format!("Index {new}")));
                }
                let item = state
                    .indexes
                    .values_mut()
                    .find(|item| item.name == current)
                    .ok_or_else(|| ControlError::NotFound(format!("Index {current}")))?;
                item.name = new.to_owned();
                Ok((
                    format!("renamed Index {current} to {new}"),
                    serde_json::to_value(item.clone())?,
                ))
            }
            ResourceKind::StateStore => {
                // Renaming relabels the catalog entry only — store_id carries
                // identity for journals, placements, and named-key postings.
                // Dropped tombstones still occupy their names.
                if state.state_stores.values().any(|item| item.name == new) {
                    return Err(ControlError::AlreadyExists(format!("StateStore {new}")));
                }
                let item = state
                    .state_stores
                    .values_mut()
                    .find(|item| item.name == current && item.stage != StateStoreStage::Dropped)
                    .ok_or_else(|| ControlError::NotFound(format!("StateStore {current}")))?;
                item.name = new.to_owned();
                Ok((
                    format!("renamed StateStore {current} to {new}"),
                    serde_json::to_value(item.clone())?,
                ))
            }
        }
    }

    fn set_pipe_stage(
        &self,
        state: &mut CatalogState,
        name: &str,
        stage: PipeStage,
    ) -> Result<(String, Value), ControlError> {
        let pipe = state
            .pipes
            .values_mut()
            .find(|pipe| pipe.name == name && pipe.status == ResourceStatus::Active)
            .ok_or_else(|| ControlError::NotFound(format!("Pipe {name}")))?;
        pipe.stage = stage;
        let verb = match stage {
            PipeStage::Paused => "paused",
            PipeStage::Declared => "resumed",
        };
        Ok((format!("{verb} Pipe {name}"), json!(pipe.clone())))
    }

    fn drop_resource(
        &self,
        state: &mut CatalogState,
        kind: ResourceKind,
        name: &str,
    ) -> Result<(String, Value), ControlError> {
        match kind {
            ResourceKind::Feed => {
                let feed_id = active_feed(state, name)?.feed_id;
                // A legal hold outranks every other drop precondition:
                // destroying the definition while history is frozen is
                // the audit bypass the hold exists to prevent.
                if state.legal_holds.contains_key(&feed_id) {
                    return Err(ControlError::InvalidOperation(format!(
                        "drop the legal hold on Feed {name} before dropping it"
                    )));
                }
                if state
                    .writers
                    .values()
                    .any(|item| item.status == ResourceStatus::Active && item.feed_id == feed_id)
                    || state.readers.values().any(|item| {
                        item.status == ResourceStatus::Active && item.feed_id == feed_id
                    })
                    || state.subscriptions.values().any(|item| {
                        item.status == ResourceStatus::Active
                            && (item.source.feed_id() == Some(feed_id)
                                // A Domain-scoped Subscription covers this Feed
                                // as soon as membership attaches — dropping the
                                // Feed under it is unsafe until detach lands.
                                || (item.source.domain_id().is_some()
                                    && item.space_id
                                        == active_feed(state, name)
                                            .map(|feed| feed.space_id)
                                            .unwrap_or_default()))
                    })
                    || state.pipes.values().any(|item| {
                        item.status == ResourceStatus::Active
                            && item.output_feed_id == Some(feed_id)
                    })
                    || state.distributaries.values().any(|item| {
                        item.status == ResourceStatus::Active && item.target_feed_id == feed_id
                    })
                {
                    return Err(ControlError::InvalidOperation(
                        "drop attached Writers, Readers, Subscriptions, Pipes, and Distributaries first".to_owned(),
                    ));
                }
                let item = state.feeds.get_mut(&feed_id).expect("Feed exists");
                item.status = ResourceStatus::Dropped;
                let dropped = item.clone();
                // An explicit drop is the disposition that retires any
                // fenced membership rows the Feed still holds.
                for members in state.subscription_feed_memberships.values_mut() {
                    members.remove(&feed_id);
                }
                state.history_policies.remove(&feed_id);
                state.legal_holds.remove(&feed_id);
                state.schemas.remove(&feed_id);
                Ok((
                    format!("dropped Feed {name} without purging stored history"),
                    json!(dropped),
                ))
            }
            ResourceKind::Domain | ResourceKind::Space => {
                let space_id = active_space(state, name)?.space_id;
                if state
                    .feeds
                    .values()
                    .any(|item| item.status == ResourceStatus::Active && item.space_id == space_id)
                    || state.subscriptions.values().any(|item| {
                        item.status == ResourceStatus::Active && item.space_id == space_id
                    })
                {
                    return Err(ControlError::InvalidOperation(
                        "drop child Feeds and Subscriptions before dropping a Domain".to_owned(),
                    ));
                }
                let item = state.spaces.get_mut(&space_id).expect("Space exists");
                item.status = ResourceStatus::Dropped;
                state.history_policies.remove(&space_id);
                state.schemas.remove(&space_id);
                Ok((format!("dropped Domain {name}"), json!(item.clone())))
            }
            ResourceKind::Writer => drop_named(&mut state.writers, name, "Writer"),
            ResourceKind::Reader => {
                let reader_id = active_reader_mut(state, name)?.reader_id;
                let result = drop_named(&mut state.readers, name, "Reader")?;
                state.reader_frontiers.remove(&reader_id);
                Ok(result)
            }
            ResourceKind::Subscription => {
                let subscription_id = state
                    .subscriptions
                    .values()
                    .find(|item| item.name == name && item.status == ResourceStatus::Active)
                    .ok_or_else(|| ControlError::NotFound(format!("Subscription {name}")))?
                    .subscription_id;
                if state.pipes.values().any(|item| {
                    item.status == ResourceStatus::Active && item.subscription_id == subscription_id
                }) {
                    return Err(ControlError::InvalidOperation(
                        "drop the consuming Pipe first".to_owned(),
                    ));
                }
                let result = drop_named(&mut state.subscriptions, name, "Subscription")?;
                state
                    .subscription_progress_assignments
                    .remove(&subscription_id);
                state.pending_subscription_cutovers.remove(&subscription_id);
                state.subscription_feed_memberships.remove(&subscription_id);
                Ok(result)
            }
            ResourceKind::Pipe => {
                let outcome = drop_named(&mut state.pipes, name, "Pipe");
                if outcome.is_ok() {
                    if let Some(pipe) = state.pipes.values().find(|item| item.name == name) {
                        state.pipe_compute_leases.remove(&pipe.pipe_id);
                    }
                }
                outcome
            }
            ResourceKind::RiverbedLink => {
                let link_id = state
                    .riverbed_links
                    .values()
                    .find(|item| item.name == name && item.status == ResourceStatus::Active)
                    .ok_or_else(|| ControlError::NotFound(format!("Riverbed Link {name}")))?
                    .link_id;
                if state
                    .distributaries
                    .values()
                    .any(|item| item.status == ResourceStatus::Active && item.link_id == link_id)
                {
                    return Err(ControlError::InvalidOperation(
                        "drop the Distributaries using this Riverbed Link first".to_owned(),
                    ));
                }
                drop_named(&mut state.riverbed_links, name, "Riverbed Link")
            }
            ResourceKind::Distributary => {
                let outcome = drop_named(&mut state.distributaries, name, "Distributary");
                if outcome.is_ok() {
                    if let Some(item) = state.distributaries.values().find(|d| d.name == name) {
                        state.distributary_leases.remove(&item.distributary_id);
                        state.distributary_status.remove(&item.distributary_id);
                    }
                }
                outcome
            }
            ResourceKind::Index => {
                // Nothing depends on a declared Index and no built data can
                // exist yet, so dropping removes the definition outright.
                let index_id = state
                    .indexes
                    .values()
                    .find(|item| item.name == name)
                    .map(|item| item.index_id)
                    .ok_or_else(|| ControlError::NotFound(format!("Index {name}")))?;
                let item = state.indexes.remove(&index_id).expect("Index exists");
                Ok((
                    format!("dropped declared Index {name}; no built data existed"),
                    json!(item),
                ))
            }
            ResourceKind::StateStore => {
                // Dropping tombstones the definition so the name cannot be
                // recreated and removes the RF3 placement so mutation, read,
                // and replay paths refuse. Rows already applied on replicas
                // remain until a bounded reclaim pass — the same orphan-row
                // model as dropped Feed history.
                let store_id = state
                    .state_stores
                    .values()
                    .find(|item| item.name == name && item.stage != StateStoreStage::Dropped)
                    .map(|item| item.store_id)
                    .ok_or_else(|| ControlError::NotFound(format!("StateStore {name}")))?;
                let item = state
                    .state_stores
                    .get_mut(&store_id)
                    .expect("StateStore exists");
                item.stage = StateStoreStage::Dropped;
                let dropped = item.clone();
                state.state_store_assignments.remove(&store_id);
                Ok((
                    format!(
                        "dropped StateStore {name}; applied rows remain on replicas until reclaim"
                    ),
                    json!(dropped),
                ))
            }
            ResourceKind::Role => {
                let role_id = active_role(state, name)?.role_id;
                state.grants.retain(|_, grant| grant.role_id != role_id);
                let item = state.roles.get_mut(&role_id).expect("Role exists");
                item.status = ResourceStatus::Dropped;
                Ok((
                    format!("dropped Role {name} and its grants"),
                    json!(item.clone()),
                ))
            }
        }
    }
}

pub fn parse_wcl(script: &str) -> Result<Vec<Command>, ControlError> {
    split_statements(script)?
        .into_iter()
        .map(|statement| parse_statement(&statement))
        .collect()
}

fn split_statements(script: &str) -> Result<Vec<String>, ControlError> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut quoted = false;
    let mut chars = script.chars().peekable();
    while let Some(character) = chars.next() {
        match character {
            // Keep the doubled quote verbatim: this pass only finds statement
            // boundaries; tokenize() performs the actual un-escaping.
            '\'' if quoted && chars.peek() == Some(&'\'') => {
                current.push('\'');
                current.push('\'');
                chars.next();
            }
            '\'' => {
                quoted = !quoted;
                current.push(character);
            }
            ';' if !quoted => {
                if !current.trim().is_empty() {
                    statements.push(current.trim().to_owned());
                }
                current.clear();
            }
            _ => current.push(character),
        }
    }
    if quoted {
        return Err(ControlError::Syntax(
            "unterminated quoted string".to_owned(),
        ));
    }
    if !current.trim().is_empty() {
        statements.push(current.trim().to_owned());
    }
    Ok(statements)
}

fn tokenize(statement: &str) -> Result<Vec<String>, ControlError> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut quoted = false;
    let mut chars = statement.chars().peekable();
    while let Some(character) = chars.next() {
        match character {
            '\'' if quoted && chars.peek() == Some(&'\'') => {
                current.push('\'');
                chars.next();
            }
            '\'' => quoted = !quoted,
            ',' | '=' if !quoted => {
                if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current));
                }
                tokens.push(character.to_owned().to_string());
            }
            character if character.is_whitespace() && !quoted => {
                if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current));
                }
            }
            _ => current.push(character),
        }
    }
    if quoted {
        return Err(ControlError::Syntax(
            "unterminated quoted string".to_owned(),
        ));
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    Ok(tokens)
}

fn parse_statement(statement: &str) -> Result<Command, ControlError> {
    let tokens = tokenize(statement)?;
    let keyword = |index: usize| tokens.get(index).map(|token| token.to_ascii_uppercase());
    match keyword(0).as_deref() {
        Some("CREATE") => match keyword(1).as_deref() {
            Some("DOMAIN") => Ok(Command::CreateDomain {
                name: token(&tokens, 2)?.to_owned(),
            }),
            Some("SPACE") => Ok(Command::CreateSpace {
                name: token(&tokens, 2)?.to_owned(),
            }),
            Some("FEED") => {
                let name = token(&tokens, 2)?.to_owned();
                let encrypted = match token(&tokens, 3).ok() {
                    None => false,
                    Some(flag) if flag.eq_ignore_ascii_case("ENCRYPTED") => true,
                    Some(other) => {
                        return Err(ControlError::Syntax(format!(
                            "CREATE FEED accepts only ENCRYPTED after the name, not {other}"
                        )))
                    }
                };
                Ok(Command::CreateFeed { name, encrypted })
            }
            Some("ROLE") => Ok(Command::CreateRole {
                name: token(&tokens, 2)?.to_owned(),
            }),
            Some("WRITER") => {
                expect_keyword(&tokens, 3, "TO")?;
                Ok(Command::CreateWriter {
                    name: token(&tokens, 2)?.to_owned(),
                    feed: token(&tokens, 4)?.to_owned(),
                })
            }
            Some("RIVERBED") => {
                // CREATE RIVERBED LINK <name> AT '<url[,url...]>' WITH KEY ENV <VAR>
                expect_keyword(&tokens, 2, "LINK")?;
                expect_keyword(&tokens, 4, "AT")?;
                let endpoints: Vec<String> = token(&tokens, 5)?
                    .split(',')
                    .map(|item| item.trim().to_owned())
                    .filter(|item| !item.is_empty())
                    .collect();
                expect_keyword(&tokens, 6, "WITH")?;
                expect_keyword(&tokens, 7, "KEY")?;
                expect_keyword(&tokens, 8, "ENV")?;
                Ok(Command::DefineRiverbedLink {
                    name: token(&tokens, 3)?.to_owned(),
                    endpoints,
                    key_env: token(&tokens, 9)?.to_owned(),
                })
            }
            Some("DISTRIBUTARY") => {
                // CREATE DISTRIBUTARY <name> FROM LINK <link>
                //   FEED <remote_feed> INTO <local_feed>
                expect_keyword(&tokens, 3, "FROM")?;
                expect_keyword(&tokens, 4, "LINK")?;
                expect_keyword(&tokens, 6, "FEED")?;
                expect_keyword(&tokens, 8, "INTO")?;
                Ok(Command::DefineDistributary {
                    name: token(&tokens, 2)?.to_owned(),
                    link: token(&tokens, 5)?.to_owned(),
                    source_feed: token(&tokens, 7)?.to_owned(),
                    target_feed: token(&tokens, 9)?.to_owned(),
                })
            }
            Some("PIPE") => {
                // CREATE PIPE <name> FROM SUBSCRIPTION <sub> TO FEED <feed>
                // declares a forward Pipe. The window form declares a
                // managed per-Key pane accumulator:
                //   CREATE PIPE <name> FROM SUBSCRIPTION <sub>
                //     TO STATESTORE <store> WINDOW <n><ms|s|m|h> BY <spec>
                //     (COUNT | SUM <spec> | AVG <spec>)
                expect_keyword(&tokens, 3, "FROM")?;
                expect_keyword(&tokens, 4, "SUBSCRIPTION")?;
                expect_keyword(&tokens, 6, "TO")?;
                match token(&tokens, 7)?.to_ascii_uppercase().as_str() {
                    "FEED" => {
                        // Optional: `ENRICH STATESTORE <store> BY <spec>`
                        if tokens.get(9).is_some() {
                            expect_keyword(&tokens, 9, "ENRICH")?;
                            expect_keyword(&tokens, 10, "STATESTORE")?;
                            // `ENRICH STATESTORE <s> [STRICT] [NAMED '<n>']
                            //  BY <spec> [MISS DROP]`
                            let mut freshness = EnrichFreshness::Latest;
                            let mut named = None;
                            let mut at = 12;
                            loop {
                                match token(&tokens, at)?.to_ascii_uppercase().as_str()
                                {
                                    "STRICT" => {
                                        freshness = EnrichFreshness::Strict;
                                        at += 1;
                                    }
                                    "NAMED" => {
                                        named = Some(token(&tokens, at + 1)?.to_owned());
                                        at += 2;
                                    }
                                    _ => break,
                                }
                            }
                            expect_keyword(&tokens, at, "BY")?;
                            let (key, end) =
                                parse_index_field_spec(&tokens, at + 1, "ENRICH BY")?;
                            let (mut on_miss, mut quarantine_feed) =
                                (EnrichMiss::Forward, None);
                            if tokens.get(end).is_some() {
                                expect_keyword(&tokens, end, "MISS")?;
                                match token(&tokens, end + 1)?
                                    .to_ascii_uppercase()
                                    .as_str()
                                {
                                    "DROP" => on_miss = EnrichMiss::Drop,
                                    "QUARANTINE" => {
                                        on_miss = EnrichMiss::Quarantine;
                                        quarantine_feed =
                                            Some(token(&tokens, end + 2)?.to_owned());
                                    }
                                    other => {
                                        return Err(ControlError::Syntax(format!(
                                            "MISS accepts DROP or QUARANTINE <feed>, not {other}"
                                        )))
                                    }
                                }
                            }
                            return Ok(Command::DefinePipe {
                                name: token(&tokens, 2)?.to_owned(),
                                subscription: token(&tokens, 5)?.to_owned(),
                                output_feed: Some(token(&tokens, 8)?.to_owned()),
                                output_store: Some(token(&tokens, 11)?.to_owned()),
                                quarantine_feed,
                                emit_feed: None,
                                operation: Box::new(PipeOperation::Enrich {
                                    key,
                                    freshness,
                                    named,
                                    on_miss,
                                    quarantine_feed_id: None,
                                }),
                            });
                        }
                        Ok(Command::DefinePipe {
                            name: token(&tokens, 2)?.to_owned(),
                            subscription: token(&tokens, 5)?.to_owned(),
                            output_feed: Some(token(&tokens, 8)?.to_owned()),
                            output_store: None,
                            quarantine_feed: None,
                            emit_feed: None,
                            operation: Box::new(PipeOperation::Forward),
                        })
                    }
                    "STATESTORE" => {
                        expect_keyword(&tokens, 9, "WINDOW")?;
                        let window_ns = parse_duration_ns(token(&tokens, 10)?)?;
                        // Optional clock policy: `WINDOW 5m INGEST BY ...`;
                        // absent means the event-time default.
                        let (time, mut at) = match token(&tokens, 11)?
                            .to_ascii_uppercase()
                            .as_str()
                        {
                            "EVENT" => (WindowTime::Event, 12),
                            "INGEST" => (WindowTime::Ingest, 12),
                            _ => (WindowTime::Event, 11),
                        };
                        // Optional `MAX_LATE <dur>` bounds how far behind
                        // its own event time a record may arrive before it
                        // quarantines out of pane accumulation.
                        let mut max_late_ns = None;
                        if token(&tokens, at)
                            .map(|value| value.eq_ignore_ascii_case("MAX_LATE"))
                            .unwrap_or(false)
                        {
                            max_late_ns =
                                Some(parse_duration_ns(token(&tokens, at + 1)?)?);
                            at += 2;
                        }
                        // `WATERMARK <dur> [GRACE <dur>]` selects the
                        // event-time lateness model: panes close once the
                        // durable horizon `max(event)-watermark` passes
                        // their end, and `GRACE` bounds how long an
                        // emitted pane still accepts revising events.
                        let mut watermark_ns = None;
                        let mut grace_ns = None;
                        if token(&tokens, at)
                            .map(|value| value.eq_ignore_ascii_case("WATERMARK"))
                            .unwrap_or(false)
                        {
                            watermark_ns =
                                Some(parse_duration_ns(token(&tokens, at + 1)?)?);
                            at += 2;
                            if token(&tokens, at)
                                .map(|value| value.eq_ignore_ascii_case("GRACE"))
                                .unwrap_or(false)
                            {
                                grace_ns =
                                    Some(parse_duration_ns(token(&tokens, at + 1)?)?);
                                at += 2;
                            }
                        }
                        let mut expire_ns = None;
                        if token(&tokens, at)
                            .map(|value| value.eq_ignore_ascii_case("EXPIRE"))
                            .unwrap_or(false)
                        {
                            expire_ns = Some(parse_duration_ns(token(&tokens, at + 1)?)?);
                            at += 2;
                        }
                        // `LIMIT KEYS <n>` bounds the distinct active group
                        // Keys and `LIMIT BYTES <n>` the accounted pane-state
                        // bytes the window may hold. Rows beyond either bound
                        // refuse into a shared sentinel rather than growing
                        // state unboundedly; either order is accepted.
                        let mut key_limit = None;
                        let mut byte_limit = None;
                        while token(&tokens, at)
                            .map(|value| value.eq_ignore_ascii_case("LIMIT"))
                            .unwrap_or(false)
                        {
                            match token(&tokens, at + 1)?.to_ascii_uppercase().as_str()
                            {
                                "KEYS" => {
                                    key_limit = Some(
                                        token(&tokens, at + 2)?.parse::<u64>().map_err(
                                            |_| {
                                                ControlError::Syntax(
                                                    "LIMIT KEYS expects an unsigned integer".to_owned(),
                                                )
                                            },
                                        )?,
                                    );
                                }
                                "BYTES" => {
                                    byte_limit = Some(
                                        token(&tokens, at + 2)?.parse::<u64>().map_err(
                                            |_| {
                                                ControlError::Syntax(
                                                    "LIMIT BYTES expects an unsigned integer".to_owned(),
                                                )
                                            },
                                        )?,
                                    );
                                }
                                other => {
                                    return Err(ControlError::Syntax(format!(
                                        "LIMIT accepts KEYS or BYTES; got {other}"
                                    )))
                                }
                            }
                            at += 3;
                        }
                        let by = at;
                        expect_keyword(&tokens, by, "BY")?;
                        let (key, next) = parse_index_field_spec(&tokens, by + 1, "BY")?;
                        let (aggregation, after_agg) = match token(&tokens, next)?
                            .to_ascii_uppercase()
                            .as_str()
                        {
                            "COUNT" => (WindowAggregation::Count, next + 1),
                            "SUM" => {
                                let (value, end) =
                                    parse_index_field_spec(&tokens, next + 1, "SUM")?;
                                (WindowAggregation::Sum { value }, end)
                            }
                            "AVG" => {
                                let (value, end) =
                                    parse_index_field_spec(&tokens, next + 1, "AVG")?;
                                (WindowAggregation::Average { value }, end)
                            }
                            other => {
                                return Err(ControlError::Syntax(format!(
                                    "window aggregation accepts COUNT, SUM <spec>, or AVG <spec>; got {other}"
                                )))
                            }
                        };
                        // Optional tail: `EMIT TO FEED <feed>` declares
                        // the Feed closed panes publish into.
                        let mut emit_feed = None;
                        if tokens.get(after_agg).is_some() {
                            expect_keyword(&tokens, after_agg, "EMIT")?;
                            expect_keyword(&tokens, after_agg + 1, "TO")?;
                            expect_keyword(&tokens, after_agg + 2, "FEED")?;
                            emit_feed = Some(token(&tokens, after_agg + 3)?.to_owned());
                        }
                        Ok(Command::DefinePipe {
                            name: token(&tokens, 2)?.to_owned(),
                            subscription: token(&tokens, 5)?.to_owned(),
                            output_feed: None,
                            output_store: Some(token(&tokens, 8)?.to_owned()),
                            quarantine_feed: None,
                            emit_feed,
                            operation: Box::new(PipeOperation::Window {
                                window_ns,
                                max_late_ns,
                                watermark_ns,
                                grace_ns,
                                expire_ns,
                                key_limit,
                                byte_limit,
                                key,
                                aggregation,
                                time,
                            }),
                        })
                    }
                    other => Err(ControlError::Syntax(format!(
                        "CREATE PIPE target accepts FEED or STATESTORE; got {other}"
                    ))),
                }
            }
            Some("READER") | Some("SUBSCRIPTION") => {
                expect_keyword(&tokens, 3, "FROM")?;
                // `FROM DOMAIN <domain>` selects the Domain-wide source;
                // `FROM <feed>` keeps the exact-Feed form.
                let domain_source = keyword(4).as_deref() == Some("DOMAIN");
                let source_index = if domain_source { 5 } else { 4 };
                let (start, tail) = if tokens.len() == source_index + 1
                    || keyword(source_index + 1).as_deref() == Some("DELIVERY")
                    || keyword(source_index + 1).as_deref() == Some("MAX")
                {
                    (ReaderStart::Beginning, source_index + 1)
                } else {
                    expect_keyword(&tokens, source_index + 1, "START")?;
                    expect_keyword(&tokens, source_index + 2, "AT")?;
                    let start = parse_reader_start(&tokens, source_index + 3)?;
                    // CURSOR/TIMESTAMP consume a second token; the fixed
                    // keywords consume one. `tail` is where an optional
                    // DELIVERY DELAY clause may begin.
                    let consumed = match token(&tokens, source_index + 3)?
                        .to_ascii_uppercase()
                        .as_str()
                    {
                        "CURSOR" | "TIMESTAMP" => 2,
                        _ => 1,
                    };
                    (start, source_index + 3 + consumed)
                };
                let name = token(&tokens, 2)?.to_owned();
                let source = token(&tokens, source_index)?.to_owned();
                if keyword(1).as_deref() == Some("SUBSCRIPTION") {
                    let mut tail = tail;
                    let delivery_delay_ns = if tail < tokens.len()
                        && keyword(tail).as_deref() == Some("DELIVERY")
                    {
                        expect_keyword(&tokens, tail, "DELIVERY")?;
                        expect_keyword(&tokens, tail + 1, "DELAY")?;
                        if tail + 2 >= tokens.len() {
                            return Err(ControlError::Syntax(
                                "DELIVERY DELAY expects a <n><ms|s|m|h|d> duration".to_owned(),
                            ));
                        }
                        let delay = parse_duration_ns(token(&tokens, tail + 2)?)?;
                        tail += 3;
                        Some(delay)
                    } else {
                        None
                    };
                    let exhausted_policy = if tail < tokens.len() {
                        expect_keyword(&tokens, tail, "MAX")?;
                        expect_keyword(&tokens, tail + 1, "DELIVERY")?;
                        expect_keyword(&tokens, tail + 2, "ATTEMPTS")?;
                        let attempts = token(&tokens, tail + 3)
                            .ok()
                            .and_then(|token| token.parse::<u64>().ok())
                            .filter(|attempts| *attempts > 0)
                            .ok_or_else(|| {
                                ControlError::Syntax(
                                    "MAX DELIVERY ATTEMPTS expects a positive integer".to_owned(),
                                )
                            })?;
                        let mut tail = tail + 4;
                        let disposition = if tail < tokens.len() {
                            expect_keyword(&tokens, tail, "THEN")?;
                            let disposition = match token(&tokens, tail + 1)?
                                .to_ascii_uppercase()
                                .as_str()
                            {
                                "QUARANTINE" => SubscriptionExhaustedDisposition::Quarantine,
                                "SKIP" => SubscriptionExhaustedDisposition::Skip,
                                _ => {
                                    return Err(ControlError::Syntax(
                                        "MAX DELIVERY ATTEMPTS THEN expects QUARANTINE or SKIP"
                                            .to_owned(),
                                    ));
                                }
                            };
                            tail += 2;
                            disposition
                        } else {
                            SubscriptionExhaustedDisposition::Quarantine
                        };
                        if tail != tokens.len() {
                            return Err(ControlError::Syntax(
                                "unexpected tokens after MAX DELIVERY ATTEMPTS".to_owned(),
                            ));
                        }
                        Some(SubscriptionExhaustedPolicy {
                            max_delivery_attempts: attempts,
                            disposition,
                        })
                    } else {
                        None
                    };
                    Ok(Command::CreateSubscription {
                        name,
                        feed: (!domain_source).then_some(source.clone()),
                        domain: domain_source.then_some(source),
                        start,
                        delivery_delay_ns,
                        exhausted_policy,
                    })
                } else {
                    if domain_source {
                        return Err(ControlError::Syntax(
                            "Readers consume an exact Feed; FROM DOMAIN applies to SUBSCRIPTION"
                                .to_owned(),
                        ));
                    }
                    Ok(Command::CreateReader {
                        name,
                        feed: source,
                        start,
                    })
                }
            }
            Some("INDEX") | Some("UNIQUE") => {
                // CREATE INDEX <name> ON <feed> FIELDS <field>[,<field>]*
                // CREATE UNIQUE INDEX <name> ON <feed> FIELDS <field>[,<field>]*
                let unique = keyword(1).as_deref() == Some("UNIQUE");
                if unique {
                    expect_keyword(&tokens, 2, "INDEX")?;
                }
                let offset = if unique { 1 } else { 0 };
                expect_keyword(&tokens, 3 + offset, "ON")?;
                expect_keyword(&tokens, 5 + offset, "FIELDS")?;
                let mut position = 6 + offset;
                let mut fields = Vec::new();
                while position < tokens.len() {
                    let (field, next) = parse_index_field_spec(&tokens, position, "CREATE INDEX FIELDS")?;
                    position = next;
                    fields.push(field);
                    if position < tokens.len() {
                        expect_keyword(&tokens, position, ",")?;
                        position += 1;
                    }
                }
                Ok(Command::DefineIndex {
                    name: token(&tokens, 2 + offset)?.to_owned(),
                    feed: token(&tokens, 4 + offset)?.to_owned(),
                    fields,
                    unique,
                    collation: IndexCollation::Binary,
                    consistency: IndexConsistency::Eventual,
                })
            }
            _ => Err(ControlError::Syntax(
                "CREATE supports DOMAIN, FEED, WRITER, READER, SUBSCRIPTION, PIPE, ROLE, and INDEX (SPACE is a compatibility alias)".to_owned(),
            )),
        },
        Some("DEFINE") => {
            // DEFINE STATESTORE <name> [FROM FEED <feed>]
            //          [KEYS <name>[ UNIQUE][, <name>[ UNIQUE]]*]
            expect_keyword(&tokens, 1, "STATESTORE")?;
            let name = token(&tokens, 2)?.to_owned();
            let mut source = StateStoreSourceRequest::Manual;
            let mut named_keys = Vec::new();
            let mut position = 3;
            // FROM FEED and KEYS are both optional, in either present/absent
            // combination — check the actual keyword rather than position.
            if position < tokens.len() && keyword(position).as_deref() == Some("FROM") {
                expect_keyword(&tokens, position + 1, "FEED")?;
                source = StateStoreSourceRequest::Feed {
                    feed: token(&tokens, position + 2)?.to_owned(),
                };
                position += 3;
            }
            if position < tokens.len() {
                expect_keyword(&tokens, position, "KEYS")?;
                position += 1;
                while position < tokens.len() {
                    let key_name = token(&tokens, position)?.to_owned();
                    position += 1;
                    // `name = <extractor>` declares the source-Feed
                    // projection; `name` alone keeps the manual-store
                    // form where mutations carry the typed values.
                    let mut extractor = None;
                    if position < tokens.len() && keyword(position).as_deref() == Some("=") {
                        let (spec, next) = parse_index_field_spec(
                            &tokens,
                            position + 1,
                            "DEFINE STATESTORE KEYS",
                        )?;
                        extractor = Some(spec);
                        position = next;
                    }
                    let mut unique = false;
                    if position < tokens.len()
                        && keyword(position).as_deref() == Some("UNIQUE")
                    {
                        unique = true;
                        position += 1;
                    }
                    named_keys.push(NamedStateStoreKey {
                        name: key_name,
                        unique,
                        extractor,
                    });
                    if position < tokens.len() {
                        expect_keyword(&tokens, position, ",")?;
                        position += 1;
                    }
                }
            }
            if named_keys.is_empty() {
                // An empty trailing KEYS clause is a parse error; absence is fine.
                if tokens
                    .iter()
                    .any(|token| token.eq_ignore_ascii_case("KEYS"))
                {
                    return Err(ControlError::Syntax(
                        "DEFINE STATESTORE KEYS requires at least one key name".to_owned(),
                    ));
                }
            }
            Ok(Command::DefineStateStore {
                name,
                source,
                named_keys,
            })
        }
        Some("OPEN") => {
            expect_keyword(&tokens, 1, "WRITER")?;
            expect_keyword(&tokens, 2, "SESSION")?;
            Ok(Command::OpenWriterSession {
                writer: token(&tokens, 3)?.to_owned(),
            })
        }
        Some("ALLOCATE") => {
            expect_keyword(&tokens, 1, "WRITER")?;
            expect_keyword(&tokens, 2, "SEQUENCE")?;
            expect_keyword(&tokens, 4, "EPOCH")?;
            Ok(Command::AllocateWriterSequence {
                writer: token(&tokens, 3)?.to_owned(),
                session_epoch: token(&tokens, 5)?.parse().map_err(|_| {
                    ControlError::Syntax(
                        "Writer session epoch must be an unsigned integer".to_owned(),
                    )
                })?,
            })
        }
        Some("GET") => {
            expect_keyword(&tokens, 1, "INDEX")?;
            let index = token(&tokens, 2)?.to_owned();
            let mut position = 3;
            let predicate = match keyword(position).as_deref() {
                Some("EQ") => {
                    position += 1;
                    let (values, next) = parse_index_literal_list(&tokens, position)?;
                    position = next;
                    IndexQueryPredicate::Exact { values }
                }
                Some("PREFIX") => {
                    position += 1;
                    let (leading, next) = parse_index_literal_list(&tokens, position)?;
                    position = next;
                    let partial_text = if keyword(position).as_deref() == Some("MATCH") {
                        let text = token(&tokens, position + 1)?.to_owned();
                        position += 2;
                        Some(text)
                    } else {
                        None
                    };
                    IndexQueryPredicate::Prefix {
                        leading,
                        partial_text,
                    }
                }
                Some("RANGE") => {
                    position += 1;
                    let (lower, next) = parse_index_bound(&tokens, position)?;
                    position = next;
                    expect_keyword(&tokens, position, "TO")?;
                    let (upper, next) = parse_index_bound(&tokens, position + 1)?;
                    position = next;
                    IndexQueryPredicate::Range {
                        leading: Vec::new(),
                        lower,
                        upper,
                    }
                }
                _ => {
                    return Err(ControlError::Syntax(
                        "GET INDEX supports EQ, PREFIX [.. MATCH 'text'], and RANGE <lo> TO <hi>"
                            .to_owned(),
                    ))
                }
            };
            let mut limit = None;
            let mut strict = false;
            let mut wait_ms = None;
            while position < tokens.len() {
                match keyword(position).as_deref() {
                    Some("LIMIT") => {
                        limit = Some(
                            token(&tokens, position + 1)?.parse::<usize>().map_err(|_| {
                                ControlError::Syntax(
                                    "GET INDEX LIMIT must be a positive integer".to_owned(),
                                )
                            })?,
                        );
                        position += 2;
                    }
                    Some("STRICT") => {
                        strict = true;
                        position += 1;
                    }
                    Some("WAIT") => {
                        wait_ms = Some(
                            token(&tokens, position + 1)?.parse::<u64>().map_err(|_| {
                                ControlError::Syntax(
                                    "GET INDEX WAIT must be milliseconds".to_owned(),
                                )
                            })?,
                        );
                        position += 2;
                    }
                    _ => {
                        return Err(ControlError::Syntax(format!(
                            "unexpected GET INDEX clause at position {}",
                            position + 1
                        )))
                    }
                }
            }
            Ok(Command::GetIndex {
                index,
                predicate,
                limit,
                strict,
                wait_ms,
            })
        }
        Some("REBUILD") => match keyword(1).as_deref() {
            Some("INDEX") => Ok(Command::RebuildIndex {
                name: token(&tokens, 2)?.to_owned(),
            }),
            Some("INDEXES") => {
                expect_keyword(&tokens, 2, "IN")?;
                match keyword(3).as_deref() {
                    Some("DOMAIN") | Some("SPACE") => Ok(Command::RebuildIndexesInDomain {
                        domain: token(&tokens, 4)?.to_owned(),
                    }),
                    _ => Err(ControlError::Syntax(
                        "REBUILD INDEXES IN supports DOMAIN or SPACE".to_owned(),
                    )),
                }
            }
            _ => Err(ControlError::Syntax(
                "REBUILD supports INDEX and INDEXES IN DOMAIN".to_owned(),
            )),
        },
        Some("REVOKE") => match keyword(1).as_deref() {
            Some("WRITER") => {
                expect_keyword(&tokens, 2, "SESSION")?;
                expect_keyword(&tokens, 4, "EPOCH")?;
                Ok(Command::RevokeWriterSession {
                    writer: token(&tokens, 3)?.to_owned(),
                    session_epoch: token(&tokens, 5)?.parse().map_err(|_| {
                        ControlError::Syntax(
                            "Writer session epoch must be an unsigned integer".to_owned(),
                        )
                    })?,
                })
            }
            Some("API") => {
                expect_keyword(&tokens, 2, "KEY")?;
                Ok(Command::RevokeApiKey {
                    name: token(&tokens, 3)?.to_owned(),
                })
            }
            _ => parse_revoke_grant(&tokens),
        },
        Some("ISSUE") => {
            // ISSUE API KEY <name> ROLE <role> DIGEST <64-hex>
            //   [EXPIRES AT <unix-ns>]
            // The digest is the BLAKE3-256 verifier of a caller-held
            // key; the plaintext secret must never appear in WCL.
            expect_keyword(&tokens, 1, "API")?;
            expect_keyword(&tokens, 2, "KEY")?;
            expect_keyword(&tokens, 4, "ROLE")?;
            expect_keyword(&tokens, 6, "DIGEST")?;
            let expires_at_ns = match keyword(8).as_deref() {
                None => None,
                Some("EXPIRES") => {
                    expect_keyword(&tokens, 9, "AT")?;
                    Some(token(&tokens, 10)?.parse::<i64>().map_err(|_| {
                        ControlError::Syntax(
                            "API key expiry must be signed epoch nanoseconds".to_owned(),
                        )
                    })?)
                }
                Some(_) => {
                    return Err(ControlError::Syntax(
                        "expected EXPIRES AT <unix-ns> or end of statement".to_owned(),
                    ))
                }
            };
            Ok(Command::IssueApiKey {
                name: token(&tokens, 3)?.to_owned(),
                role: token(&tokens, 5)?.to_owned(),
                digest: token(&tokens, 7)?.to_owned(),
                expires_at_ns,
            })
        }
        Some("RENAME") => {
            let kind = parse_resource_kind(token(&tokens, 1)?)?;
            expect_keyword(&tokens, 3, "TO")?;
            Ok(Command::Rename {
                kind,
                current: token(&tokens, 2)?.to_owned(),
                new: token(&tokens, 4)?.to_owned(),
            })
        }
        Some("MOVE") => match keyword(1).as_deref() {
            Some("DISTRIBUTARY") => {
                // MOVE DISTRIBUTARY LEASE <name> TO NODE <node>
                expect_keyword(&tokens, 2, "LEASE")?;
                expect_keyword(&tokens, 4, "TO")?;
                expect_keyword(&tokens, 5, "NODE")?;
                Ok(Command::MoveDistributaryLease {
                    distributary: token(&tokens, 3)?.to_owned(),
                    new_owner: StorageNodeId::try_new(token(&tokens, 6)?.to_owned())
                        .map_err(|error| ControlError::Syntax(error.to_string()))?,
                    expected_epoch: None,
                })
            }
            _ => {
                expect_keyword(&tokens, 1, "PIPE")?;
                expect_keyword(&tokens, 2, "COMPUTE")?;
                expect_keyword(&tokens, 4, "TO")?;
                expect_keyword(&tokens, 5, "NODE")?;
                Ok(Command::MovePipeComputeLease {
                    pipe: token(&tokens, 3)?.to_owned(),
                    new_owner: StorageNodeId::try_new(token(&tokens, 6)?.to_owned())
                        .map_err(|error| ControlError::Syntax(error.to_string()))?,
                    expected_compute_epoch: None,
                })
            }
        },
        Some("PAUSE") => {
            expect_keyword(&tokens, 1, "PIPE")?;
            Ok(Command::PausePipe {
                name: token(&tokens, 2)?.to_owned(),
            })
        }
        Some("RESUME") => {
            expect_keyword(&tokens, 1, "PIPE")?;
            Ok(Command::ResumePipe {
                name: token(&tokens, 2)?.to_owned(),
            })
        }
        Some("DROP") => {
            // DROP HISTORY POLICY ON DOMAIN|FEED <name> removes the
            // declared override; every other DROP stays resource-named.
            if keyword(1).as_deref() == Some("LEGAL") {
                expect_keyword(&tokens, 2, "HOLD")?;
                expect_keyword(&tokens, 3, "ON")?;
                expect_keyword(&tokens, 4, "FEED")?;
                return Ok(Command::DropLegalHold {
                    feed: token(&tokens, 5)?.to_owned(),
                });
            }
            if keyword(1).as_deref() == Some("HISTORY") {
                expect_keyword(&tokens, 2, "POLICY")?;
                expect_keyword(&tokens, 3, "ON")?;
                let scope = parse_policy_scope(&tokens, 4, "HISTORY POLICY")?;
                return Ok(Command::ClearHistoryPolicy { scope });
            }
            if keyword(1).as_deref() == Some("SCHEMA") {
                expect_keyword(&tokens, 2, "ON")?;
                let scope = parse_policy_scope(&tokens, 3, "SCHEMA")?;
                return Ok(Command::ClearSchema { scope });
            }
            Ok(Command::Drop {
                kind: parse_resource_kind(token(&tokens, 1)?)?,
                name: token(&tokens, 2)?.to_owned(),
            })
        }
        Some("ERASE") => {
            expect_keyword(&tokens, 1, "FEED")?;
            Ok(Command::EraseFeed {
                feed: token(&tokens, 2)?.to_owned(),
            })
        }
        Some("SHOW") => {
            let (kind, limit) = if token(&tokens, 1)?.eq_ignore_ascii_case("HISTORY") {
                expect_keyword(&tokens, 2, "POLICIES")?;
                (ShowKind::HistoryPolicies, None)
            } else if token(&tokens, 1)?.eq_ignore_ascii_case("LEGAL") {
                expect_keyword(&tokens, 2, "HOLDS")?;
                (ShowKind::LegalHolds, None)
            } else if token(&tokens, 1)?.eq_ignore_ascii_case("STORAGE") {
                expect_keyword(&tokens, 2, "NODES")?;
                (ShowKind::StorageNodes, None)
            } else if token(&tokens, 1)?.eq_ignore_ascii_case("API") {
                expect_keyword(&tokens, 2, "KEYS")?;
                (ShowKind::ApiKeys, None)
            } else if token(&tokens, 1)?.eq_ignore_ascii_case("AUDIT") {
                expect_keyword(&tokens, 2, "EVENTS")?;
                let limit = match tokens.get(3) {
                    Some(keyword) if keyword.eq_ignore_ascii_case("LIMIT") => Some(
                        token(&tokens, 4)?
                            .parse::<usize>()
                            .map_err(|_| ControlError::Syntax("invalid audit LIMIT".to_owned()))?,
                    ),
                    None => None,
                    Some(other) => {
                        return Err(ControlError::Syntax(format!(
                            "expected LIMIT after SHOW AUDIT EVENTS, found {other}"
                        )))
                    }
                };
                (ShowKind::AuditEvents, limit)
            } else if token(&tokens, 1)?.eq_ignore_ascii_case("SCHEMAS") {
                (ShowKind::Schemas, None)
            } else if token(&tokens, 1)?.eq_ignore_ascii_case("COMPATIBILITY") {
                (ShowKind::Compatibility, None)
            } else {
                (parse_show_kind(token(&tokens, 1)?)?, None)
            };
            Ok(Command::Show { kind, limit })
        }
        Some("REGISTER") => {
            expect_keyword(&tokens, 1, "STORAGE")?;
            expect_keyword(&tokens, 2, "NODE")?;
            expect_keyword(&tokens, 4, "AT")?;
            let mut cert_pins = BTreeSet::new();
            let mut capabilities = NodeCapabilities::default();
            let mut position = 6;
            while position < tokens.len() {
                expect_keyword(&tokens, position, "WITH")?;
                match token(&tokens, position + 1)?.to_ascii_uppercase().as_str() {
                    "CERT" => {
                        expect_keyword(&tokens, position + 2, "PIN")?;
                        if !cert_pins.is_empty() {
                            return Err(ControlError::Syntax(
                                "WITH CERT PIN may appear at most once".to_owned(),
                            ));
                        }
                        for pin in token(&tokens, position + 3)?.split('|') {
                            cert_pins.insert(pin.to_owned());
                        }
                        position += 4;
                    }
                    "CAPABILITIES" => {
                        let mut list = token(&tokens, position + 2)?.to_owned();
                        position += 3;
                        while tokens.get(position).map(String::as_str) == Some(",") {
                            list.push(',');
                            list.push_str(token(&tokens, position + 1)?);
                            position += 2;
                        }
                        capabilities = NodeCapabilities::parse_list(&list)
                            .map_err(ControlError::Syntax)?;
                    }
                    other => {
                        return Err(ControlError::Syntax(format!(
                            "expected WITH CERT PIN or WITH CAPABILITIES, found WITH {other}"
                        )))
                    }
                }
            }
            Ok(Command::RegisterStorageNode {
                node: StorageNodeId::try_new(token(&tokens, 3)?.to_owned())
                    .map_err(|error| ControlError::Syntax(error.to_string()))?,
                endpoint: token(&tokens, 5)?.to_owned(),
                cert_pins,
                capabilities,
            })
        }
        Some("RETIRE") => {
            expect_keyword(&tokens, 1, "STORAGE")?;
            expect_keyword(&tokens, 2, "NODE")?;
            Ok(Command::RetireStorageNode {
                node: StorageNodeId::try_new(token(&tokens, 3)?.to_owned())
                    .map_err(|error| ControlError::Syntax(error.to_string()))?,
            })
        }
        Some("DRAIN") => {
            expect_keyword(&tokens, 1, "STORAGE")?;
            expect_keyword(&tokens, 2, "NODE")?;
            Ok(Command::DrainStorageNode {
                node: StorageNodeId::try_new(token(&tokens, 3)?.to_owned())
                    .map_err(|error| ControlError::Syntax(error.to_string()))?,
            })
        }
        Some("UNDRAIN") => {
            expect_keyword(&tokens, 1, "STORAGE")?;
            expect_keyword(&tokens, 2, "NODE")?;
            Ok(Command::UndrainStorageNode {
                node: StorageNodeId::try_new(token(&tokens, 3)?.to_owned())
                    .map_err(|error| ControlError::Syntax(error.to_string()))?,
            })
        }
        Some("REPORT") => {
            // REPORT STORAGE NODE <id> CAPACITY cpu_cores=N,
            // memory_bytes=N, disk_total_bytes=N, disk_free_bytes=N,
            // network_mbps=N, failure_domain='zone' — committed so every
            // replica's eligibility evaluation sees identical evidence.
            expect_keyword(&tokens, 1, "STORAGE")?;
            expect_keyword(&tokens, 2, "NODE")?;
            expect_keyword(&tokens, 4, "CAPACITY")?;
            let mut capacity = NodeCapacity::default();
            let mut position = 5;
            let mut declared = false;
            while position < tokens.len() {
                let key = token(&tokens, position)?.to_ascii_lowercase();
                expect_keyword(&tokens, position + 1, "=")?;
                let value = token(&tokens, position + 2)?;
                let parse_u64 = |what: &str| {
                    value.parse::<u64>().map_err(|_| {
                        ControlError::Syntax(format!(
                            "{what} must be an unsigned integer, found {value:?}"
                        ))
                    })
                };
                match key.as_str() {
                    "cpu_cores" => {
                        capacity.cpu_cores =
                            u32::try_from(parse_u64("cpu_cores")?).map_err(|_| {
                                ControlError::Syntax(
                                    "cpu_cores exceeds u32".to_owned(),
                                )
                            })?;
                    }
                    "memory_bytes" => capacity.memory_bytes = parse_u64("memory_bytes")?,
                    "disk_total_bytes" => {
                        capacity.disk_total_bytes = parse_u64("disk_total_bytes")?
                    }
                    "disk_free_bytes" => {
                        capacity.disk_free_bytes = parse_u64("disk_free_bytes")?
                    }
                    "network_mbps" => capacity.network_mbps = parse_u64("network_mbps")?,
                    "failure_domain" => capacity.failure_domain = value.to_owned(),
                    other => {
                        return Err(ControlError::Syntax(format!(
                            "unknown capacity field {other:?}"
                        )))
                    }
                }
                declared = true;
                position += 3;
                if tokens.get(position).map(String::as_str) == Some(",") {
                    position += 1;
                }
            }
            if !declared {
                return Err(ControlError::Syntax(
                    "REPORT STORAGE NODE CAPACITY requires at least one field".to_owned(),
                ));
            }
            Ok(Command::ReportStorageNodeCapacity {
                node: StorageNodeId::try_new(token(&tokens, 3)?.to_owned())
                    .map_err(|error| ControlError::Syntax(error.to_string()))?,
                capacity,
                // WCL-issued reports are operator tooling, not a Node's
                // self-report — they carry no claimed version.
                protocol_version: 0,
                build: String::new(),
            })
        }
        Some("SET") => match keyword(1).as_deref() {
            // SET LEGAL HOLD ON FEED <name> [REASON '<text>']
            Some("LEGAL") => {
                expect_keyword(&tokens, 2, "HOLD")?;
                expect_keyword(&tokens, 3, "ON")?;
                expect_keyword(&tokens, 4, "FEED")?;
                let feed = token(&tokens, 5)?.to_owned();
                let reason = match token(&tokens, 6).ok() {
                    None => None,
                    Some(_) => {
                        expect_keyword(&tokens, 6, "REASON")?;
                        let reason = token(&tokens, 7)?.to_owned();
                        if reason.is_empty() {
                            return Err(ControlError::Syntax(
                                "REASON requires a quoted value".to_owned(),
                            ));
                        }
                        Some(reason)
                    }
                };
                Ok(Command::SetLegalHold { feed, reason })
            }
            // SET HISTORY POLICY ON DOMAIN|FEED <name>
            //     (TIER AFTER <dur> | EXPIRE AFTER <dur> | MAX RETAINED BYTES <n>)+
            Some("HISTORY") => {
            expect_keyword(&tokens, 2, "POLICY")?;
            expect_keyword(&tokens, 3, "ON")?;
            let scope = parse_policy_scope(&tokens, 4, "HISTORY POLICY")?;
            let mut position = 6;
            if position >= tokens.len() {
                return Err(ControlError::Syntax(
                    "SET HISTORY POLICY requires at least one of TIER AFTER, EXPIRE AFTER,                      or MAX RETAINED BYTES"
                        .to_owned(),
                ));
            }
            let mut policy = HistoryPolicy::default();
            while position < tokens.len() {
                match token(&tokens, position)?.to_ascii_uppercase().as_str() {
                    "TIER" => {
                        expect_keyword(&tokens, position + 1, "AFTER")?;
                        policy.tier_after_ns =
                            Some(parse_duration_ns(token(&tokens, position + 2)?)?);
                        position += 3;
                    }
                    "EXPIRE" => {
                        expect_keyword(&tokens, position + 1, "AFTER")?;
                        policy.expire_after_ns =
                            Some(parse_duration_ns(token(&tokens, position + 2)?)?);
                        position += 3;
                    }
                    "MAX" => {
                        expect_keyword(&tokens, position + 1, "RETAINED")?;
                        expect_keyword(&tokens, position + 2, "BYTES")?;
                        let bytes: u64 = token(&tokens, position + 3)?.parse().map_err(|_| {
                            ControlError::Syntax(format!(
                                "MAX RETAINED BYTES '{}' is not a whole number",
                                token(&tokens, position + 3).unwrap_or("")
                            ))
                        })?;
                        if bytes == 0 {
                            return Err(ControlError::Syntax(
                                "MAX RETAINED BYTES must be positive".to_owned(),
                            ));
                        }
                        policy.max_retained_bytes = Some(bytes);
                        position += 4;
                    }
                    other => {
                        return Err(ControlError::Syntax(format!(
                            "SET HISTORY POLICY accepts TIER AFTER <dur>, EXPIRE AFTER <dur>,                              or MAX RETAINED BYTES <n>, not {other}"
                        )))
                    }
                }
            }
            Ok(Command::SetHistoryPolicy { scope, policy })
            }
            // SET SCHEMA ON DOMAIN|FEED <name>
            //     (KEY <format> [SUBJECT <s>] [DEFINITION <text>] |
            //      VALUE <format> [SUBJECT <s>] [DEFINITION <text>])+
            Some("SCHEMA") => {
                expect_keyword(&tokens, 2, "ON")?;
                let scope = parse_policy_scope(&tokens, 3, "SCHEMA")?;
                let mut position = 5;
                if position >= tokens.len() {
                    return Err(ControlError::Syntax(
                        "SET SCHEMA requires at least one of KEY <format> or VALUE <format>"
                            .to_owned(),
                    ));
                }
                let mut schema = FeedSchema::default();
                while position < tokens.len() {
                    let side = token(&tokens, position)?.to_ascii_uppercase();
                    match side.as_str() {
                        "KEY" | "VALUE" => {
                            let (decl, next) = parse_schema_decl(&tokens, position + 1)?;
                            if side == "KEY" {
                                schema.key = Some(decl);
                            } else {
                                schema.value = Some(decl);
                            }
                            position = next;
                        }
                        other => {
                            return Err(ControlError::Syntax(format!(
                                "SET SCHEMA accepts KEY <format> or VALUE <format> clauses, not {other}"
                            )))
                        }
                    }
                }
                Ok(Command::SetSchema { scope, schema })
            }
            other => Err(ControlError::Syntax(format!(
                "SET accepts HISTORY POLICY or SCHEMA, not {}",
                other.unwrap_or("<missing>")
            ))),
        },
        Some("DESCRIBE") => Ok(Command::Describe {
            kind: parse_resource_kind(token(&tokens, 1)?)?,
            name: token(&tokens, 2)?.to_owned(),
        }),
        Some("GRANT") => parse_grant(&tokens, GrantEffect::Allow),
        Some("DENY") => parse_grant(&tokens, GrantEffect::Deny),
        Some("EXPLAIN") => parse_explain(&tokens),
        Some("INSPECT") => {
            if token(&tokens, 1)?.eq_ignore_ascii_case("DRAIN") {
                expect_keyword(&tokens, 2, "FOR")?;
                expect_keyword(&tokens, 3, "STORAGE")?;
                expect_keyword(&tokens, 4, "NODE")?;
                return Ok(Command::InspectStorageDrain {
                    node: StorageNodeId::try_new(token(&tokens, 5)?.to_owned())
                        .map_err(|error| ControlError::Syntax(error.to_string()))?,
                });
            }
            expect_keyword(&tokens, 1, "PLACEMENT")?;
            expect_keyword(&tokens, 2, "FOR")?;
            expect_keyword(&tokens, 3, "FEED")?;
            Ok(Command::InspectPlacement {
                feed: token(&tokens, 4)?.to_owned(),
            })
        }
        Some("TRANSFER") => {
            expect_keyword(&tokens, 1, "ACTIVE")?;
            expect_keyword(&tokens, 2, "RANGE")?;
            expect_keyword(&tokens, 3, "OWNERSHIP")?;
            expect_keyword(&tokens, 4, "FOR")?;
            expect_keyword(&tokens, 5, "FEED")?;
            expect_keyword(&tokens, 7, "TO")?;
            let owner = StorageNodeId::try_new(token(&tokens, 8)?.to_owned())
                .map_err(|error| ControlError::Syntax(error.to_string()))?;
            Ok(Command::TransferActiveRangeOwnership {
                feed: token(&tokens, 6)?.to_owned(),
                owner,
            })
        }
        Some("SEEK") => {
            if keyword(1).as_deref() == Some("SUBSCRIPTION") {
                expect_keyword(&tokens, 3, "TO")?;
                let start = parse_reader_start(&tokens, 4)?;
                return Ok(Command::SeekSubscription {
                    name: token(&tokens, 2)?.to_owned(),
                    start,
                });
            }
            expect_keyword(&tokens, 1, "READER")?;
            expect_keyword(&tokens, 3, "TO")?;
            Ok(Command::SeekReader {
                reader: token(&tokens, 2)?.to_owned(),
                start: parse_reader_start(&tokens, 4)?,
            })
        }
        _ => Err(ControlError::Syntax(format!(
            "unsupported statement: {statement}"
        ))),
    }
}

fn parse_grant(tokens: &[String], effect: GrantEffect) -> Result<Command, ControlError> {
    let on_index = tokens
        .iter()
        .position(|token| token.eq_ignore_ascii_case("ON"))
        .ok_or_else(|| ControlError::Syntax("GRANT requires ON NAMESPACE".to_owned()))?;
    let mut actions = BTreeSet::new();
    for token in &tokens[1..on_index] {
        if token != "," {
            actions.insert(parse_action(token)?);
        }
    }
    if actions.is_empty() {
        return Err(ControlError::Syntax(
            "GRANT requires at least one action".to_owned(),
        ));
    }
    expect_keyword(tokens, on_index + 1, "NAMESPACE")?;
    let namespace = token(tokens, on_index + 2)?.to_owned();
    expect_keyword(tokens, on_index + 3, "TO")?;
    expect_keyword(tokens, on_index + 4, "ROLE")?;
    let role = token(tokens, on_index + 5)?.to_owned();
    Ok(Command::Grant {
        actions,
        namespace,
        role,
        effect,
    })
}

/// `REVOKE [DENY] <actions> ON NAMESPACE <pattern> FROM ROLE <name>`
/// — the GRANT grammar with `FROM` and an optional `DENY` prefix that
/// selects deny rows instead of allow rows.
fn parse_revoke_grant(tokens: &[String]) -> Result<Command, ControlError> {
    let (effect, start) = if token(tokens, 1)?.eq_ignore_ascii_case("DENY") {
        (GrantEffect::Deny, 2)
    } else {
        (GrantEffect::Allow, 1)
    };
    let on_index = tokens
        .iter()
        .position(|token| token.eq_ignore_ascii_case("ON"))
        .ok_or_else(|| ControlError::Syntax("REVOKE requires ON NAMESPACE".to_owned()))?;
    let mut actions = BTreeSet::new();
    for token in &tokens[start..on_index] {
        if token != "," {
            actions.insert(parse_action(token)?);
        }
    }
    if actions.is_empty() {
        return Err(ControlError::Syntax(
            "REVOKE requires at least one action".to_owned(),
        ));
    }
    expect_keyword(tokens, on_index + 1, "NAMESPACE")?;
    let namespace = token(tokens, on_index + 2)?.to_owned();
    expect_keyword(tokens, on_index + 3, "FROM")?;
    expect_keyword(tokens, on_index + 4, "ROLE")?;
    let role = token(tokens, on_index + 5)?.to_owned();
    Ok(Command::RevokeGrant {
        actions,
        namespace,
        role,
        effect,
    })
}

fn parse_explain(tokens: &[String]) -> Result<Command, ControlError> {
    expect_keyword(tokens, 1, "ACCESS")?;
    expect_keyword(tokens, 2, "FOR")?;
    expect_keyword(tokens, 3, "ROLE")?;
    expect_keyword(tokens, 5, "ACTION")?;
    expect_keyword(tokens, 7, "ON")?;
    expect_keyword(tokens, 8, "FEED")?;
    Ok(Command::ExplainAccess {
        role: token(tokens, 4)?.to_owned(),
        action: parse_action(token(tokens, 6)?)?,
        feed: token(tokens, 9)?.to_owned(),
    })
}

fn parse_reader_start(tokens: &[String], index: usize) -> Result<ReaderStart, ControlError> {
    match token(tokens, index)?.to_ascii_uppercase().as_str() {
        "BEGINNING" => Ok(ReaderStart::Beginning),
        "NOW" => Ok(ReaderStart::Now),
        "CURSOR" => Ok(ReaderStart::Cursor(token(tokens, index + 1)?.to_owned())),
        "TIMESTAMP" => Ok(ReaderStart::Timestamp(
            token(tokens, index + 1)?.parse().map_err(|_| {
                ControlError::Syntax("Reader timestamp must be signed epoch nanoseconds".to_owned())
            })?,
        )),
        _ => Err(ControlError::Syntax(
            "Reader position must be BEGINNING, NOW, TIMESTAMP <ns>, or CURSOR '<value>'"
                .to_owned(),
        )),
    }
}

fn parse_resource_kind(token: &str) -> Result<ResourceKind, ControlError> {
    match token.to_ascii_uppercase().as_str() {
        "DOMAIN" => Ok(ResourceKind::Domain),
        "SPACE" => Ok(ResourceKind::Space),
        "FEED" => Ok(ResourceKind::Feed),
        "WRITER" => Ok(ResourceKind::Writer),
        "READER" => Ok(ResourceKind::Reader),
        "SUBSCRIPTION" => Ok(ResourceKind::Subscription),
        "PIPE" => Ok(ResourceKind::Pipe),
        "INDEX" => Ok(ResourceKind::Index),
        "STATESTORE" => Ok(ResourceKind::StateStore),
        "ROLE" => Ok(ResourceKind::Role),
        "LINK" => Ok(ResourceKind::RiverbedLink),
        "DISTRIBUTARY" => Ok(ResourceKind::Distributary),
        _ => Err(ControlError::Syntax(format!(
            "unsupported resource type {token}"
        ))),
    }
}

fn parse_show_kind(token: &str) -> Result<ShowKind, ControlError> {
    match token.to_ascii_uppercase().as_str() {
        "DOMAINS" => Ok(ShowKind::Domains),
        "SPACES" => Ok(ShowKind::Spaces),
        "FEEDS" => Ok(ShowKind::Feeds),
        "WRITERS" => Ok(ShowKind::Writers),
        "READERS" => Ok(ShowKind::Readers),
        "SUBSCRIPTIONS" => Ok(ShowKind::Subscriptions),
        "PIPES" => Ok(ShowKind::Pipes),
        "INDEXES" => Ok(ShowKind::Indexes),
        "STATESTORES" => Ok(ShowKind::StateStores),
        "ROLES" => Ok(ShowKind::Roles),
        "GRANTS" => Ok(ShowKind::Grants),
        "LINKS" => Ok(ShowKind::RiverbedLinks),
        "DISTRIBUTARIES" => Ok(ShowKind::Distributaries),
        _ => Err(ControlError::Syntax(format!(
            "unsupported SHOW resource {token}"
        ))),
    }
}

/// One WCL literal inside a `GET INDEX` predicate: bare integers are
/// `I64`, `TRUE`/`FALSE`/`NULL` map to their typed values, `B64 'data'`
/// decodes base64 `Bytes`, and any other token is `Text`.
/// One `IndexFieldSpec` at `position` — `KEY`, `METADATA 'name'`, or
/// `JSON 'path'` — shared by `CREATE INDEX FIELDS` and
/// `DEFINE STATESTORE ... KEYS name = <spec>` projections. Returns the
/// spec and the next token position.
fn parse_index_field_spec(
    tokens: &[String],
    position: usize,
    clause: &str,
) -> Result<(IndexFieldSpec, usize), ControlError> {
    let keyword = |index: usize| tokens.get(index).map(|token| token.to_ascii_uppercase());
    match keyword(position).as_deref() {
        Some("KEY") => Ok((IndexFieldSpec::Key, position + 1)),
        Some("METADATA") => Ok((
            IndexFieldSpec::Metadata {
                name: token(tokens, position + 1)?.to_owned(),
            },
            position + 2,
        )),
        Some("JSON") => Ok((
            IndexFieldSpec::Projection {
                extractor: "json".to_owned(),
                version: 1,
                field: token(tokens, position + 1)?.to_owned(),
            },
            position + 2,
        )),
        _ => Err(ControlError::Syntax(format!(
            "{clause} accepts KEY, METADATA 'name', or JSON 'path' at position {}",
            position + 1
        ))),
    }
}

/// `<n><ms|s|m|h|d>` duration in nanoseconds, shared by WINDOW, EXPIRE,
/// and DELIVERY DELAY clauses.
fn parse_duration_ns(token: &str) -> Result<u64, ControlError> {
    let split = token
        .find(|character: char| !character.is_ascii_digit())
        .unwrap_or(token.len());
    let (number, unit) = token.split_at(split);
    let number: u64 = number
        .parse()
        .map_err(|_| ControlError::Syntax(format!("duration '{token}' is invalid")))?;
    let factor: u64 = match unit.to_ascii_lowercase().as_str() {
        "ms" => 1_000_000,
        "s" => 1_000_000_000,
        "m" => 60 * 1_000_000_000,
        "h" => 3_600 * 1_000_000_000,
        "d" => 86_400 * 1_000_000_000,
        _ => {
            return Err(ControlError::Syntax(format!(
                "duration '{token}' must end in ms, s, m, h, or d"
            )))
        }
    };
    number
        .checked_mul(factor)
        .filter(|value| *value > 0)
        .ok_or_else(|| {
            ControlError::Syntax(format!("WINDOW duration '{token}' overflows or is zero"))
        })
}

/// `ON DOMAIN <name>` or `ON FEED <name>` — the shared scope clause
/// `SET`/`DROP` policy and schema statements both parse.
fn parse_policy_scope(
    tokens: &[String],
    position: usize,
    context: &str,
) -> Result<PolicyScope, ControlError> {
    match token(tokens, position)?.to_ascii_uppercase().as_str() {
        "DOMAIN" | "SPACE" => Ok(PolicyScope::Domain {
            name: token(tokens, position + 1)?.to_owned(),
        }),
        "FEED" => Ok(PolicyScope::Feed {
            name: token(tokens, position + 1)?.to_owned(),
        }),
        other => Err(ControlError::Syntax(format!(
            "{context} scope is ON DOMAIN <name> or ON FEED <name>, not ON {other}"
        ))),
    }
}

/// A declared policy must bound at least one dimension; every
/// declared bound must be positive. `parse_duration_ns` already
/// refuses zero and overflow for the duration fields.
fn validate_history_policy(policy: &HistoryPolicy) -> Result<(), ControlError> {
    if policy.is_empty() {
        return Err(ControlError::InvalidOperation(
            "SET HISTORY POLICY requires at least one of TIER AFTER, EXPIRE AFTER,              or MAX RETAINED BYTES — use DROP HISTORY POLICY to revert to inheritance"
                .to_owned(),
        ));
    }
    if policy.max_retained_bytes == Some(0) {
        return Err(ControlError::InvalidOperation(
            "MAX RETAINED BYTES must be positive".to_owned(),
        ));
    }
    Ok(())
}

/// `KEY|VALUE <format> [SUBJECT <name>] [DEFINITION <text>]` — one
/// schema clause `SET SCHEMA` parses; returns the decl and the
/// position after it.
fn parse_schema_decl(
    tokens: &[String],
    position: usize,
) -> Result<(SchemaDecl, usize), ControlError> {
    let mut position = position;
    let format = match token(tokens, position)?.to_ascii_uppercase().as_str() {
        "JSON" => "json".to_owned(),
        "JSONSCHEMA" => "jsonschema".to_owned(),
        "AVRO" => "avro".to_owned(),
        "PROTOBUF" => "protobuf".to_owned(),
        "TEXT" => "text".to_owned(),
        "BYTES" => "bytes".to_owned(),
        "CUSTOM" => {
            let name = token(tokens, position + 1)?.to_ascii_lowercase();
            validate_schema_format_name(&name)?;
            position += 1;
            format!("custom.{name}")
        }
        other => {
            return Err(ControlError::Syntax(format!(
                "schema format is JSON, JSONSCHEMA, AVRO, PROTOBUF, TEXT, BYTES, or CUSTOM <name>, not {other}"
            )))
        }
    };
    position += 1;
    let mut decl = SchemaDecl {
        format,
        ..SchemaDecl::default()
    };
    while position < tokens.len() {
        match token(tokens, position)?.to_ascii_uppercase().as_str() {
            "SUBJECT" => {
                decl.subject = Some(token(tokens, position + 1)?.to_owned());
                position += 2;
            }
            "DEFINITION" => {
                decl.definition = Some(token(tokens, position + 1)?.to_owned());
                position += 2;
            }
            _ => break,
        }
    }
    Ok((decl, position))
}

/// `custom.<name>` format names keep the explainable lowercase
/// grammar so catalog rows stay greppable.
fn validate_schema_format_name(name: &str) -> Result<(), ControlError> {
    let valid = !name.is_empty()
        && name.len() <= 128
        && name.chars().all(|c| {
            c.is_ascii_lowercase() || c.is_ascii_digit() || c == '.' || c == '_' || c == '-'
        });
    if valid {
        Ok(())
    } else {
        Err(ControlError::InvalidOperation(format!(
            "custom schema format name '{name}' must be 1-128 lowercase alnum/./_/- characters"
        )))
    }
}

/// A declared schema must name at least one side; every declared
/// `format` is a built-in or a well-formed `custom.<name>` — typed
/// commands validate here since they bypass the WCL parser.
fn validate_feed_schema(schema: &FeedSchema) -> Result<(), ControlError> {
    if schema.is_empty() {
        return Err(ControlError::InvalidOperation(
            "SET SCHEMA requires at least one of KEY <format> or VALUE <format> — use DROP SCHEMA to revert to inheritance"
                .to_owned(),
        ));
    }
    for decl in [&schema.key, &schema.value].into_iter().flatten() {
        let builtin = matches!(
            decl.format.as_str(),
            "json" | "jsonschema" | "avro" | "protobuf" | "text" | "bytes"
        );
        let custom = decl
            .format
            .strip_prefix("custom.")
            .is_some_and(|name| validate_schema_format_name(name).is_ok());
        if !builtin && !custom {
            return Err(ControlError::InvalidOperation(format!(
                "unknown schema format '{}'",
                decl.format
            )));
        }
    }
    Ok(())
}

/// The schema a Feed surface actually reports: the Feed's declared
/// override layered side-by-side over its Domain's — `None` when
/// nothing is declared anywhere (schema-agnostic bytes).
fn effective_schema(state: &CatalogState, feed: &FeedDefinition) -> Option<FeedSchema> {
    let domain = state
        .schemas
        .get(&feed.space_id)
        .map(|assignment| &assignment.schema);
    let own = state
        .schemas
        .get(&feed.feed_id)
        .map(|assignment| &assignment.schema);
    match (own, domain) {
        (Some(own), Some(domain)) => Some(own.layered_over(domain)),
        (Some(own), None) => Some(own.clone()),
        (None, Some(domain)) => Some(domain.clone()),
        (None, None) => None,
    }
}

/// Resolve a scope's name to its live definition: the row keys on the
/// definition id so renames keep the assignment and dropped resources
/// refuse further edits.
fn resolve_policy_target(
    state: &CatalogState,
    scope: &PolicyScope,
) -> Result<(&'static str, Uuid, String), ControlError> {
    match scope {
        PolicyScope::Domain { name } => {
            let space = active_space(state, name)?;
            Ok(("Domain", space.space_id, space.name.clone()))
        }
        PolicyScope::Feed { name } => {
            let feed = active_feed(state, name)?;
            Ok(("Feed", feed.feed_id, feed.name.clone()))
        }
    }
}

/// The policy a storage Node actually enforces for a Feed: the Feed's
/// declared override layered field-by-field over its Domain's, with
/// undeclared fields falling back to the built-in defaults (tier
/// immediately, never expire).
fn effective_history_policy(state: &CatalogState, feed: &FeedDefinition) -> HistoryPolicy {
    let domain = state
        .history_policies
        .get(&feed.space_id)
        .map(|assignment| &assignment.policy);
    let own = state
        .history_policies
        .get(&feed.feed_id)
        .map(|assignment| &assignment.policy);
    match (own, domain) {
        (Some(own), Some(domain)) => own.layered_over(domain),
        (Some(own), None) => own.clone(),
        (None, Some(domain)) => domain.clone(),
        (None, None) => HistoryPolicy::default(),
    }
}

/// Window keys and aggregated values come only from deterministic record
/// projections — the same spec vocabulary Index fields use.
fn validate_window_field(spec: &IndexFieldSpec) -> Result<(), ControlError> {
    match spec {
        IndexFieldSpec::Key => Ok(()),
        IndexFieldSpec::Metadata { name } if !name.is_empty() && name.len() <= 256 => Ok(()),
        IndexFieldSpec::Projection {
            extractor,
            version,
            field,
        } if extractor == "json" && *version == 1 && !field.is_empty() && field.len() <= 512 => {
            Ok(())
        }
        _ => Err(ControlError::InvalidOperation(
            "Window fields accept KEY, METADATA 'name', or JSON 'path'".to_owned(),
        )),
    }
}

fn parse_index_literal(
    tokens: &[String],
    position: usize,
) -> Result<(IndexValue, usize), ControlError> {
    let raw = token(tokens, position)?;
    if raw.eq_ignore_ascii_case("B64") {
        let encoded = token(tokens, position + 1)?;
        let bytes =
            base64::engine::Engine::decode(&base64::engine::general_purpose::STANDARD, encoded)
                .map_err(|_| ControlError::Syntax("B64 literal is not valid base64".to_owned()))?;
        return Ok((IndexValue::Bytes(bytes), position + 2));
    }
    if raw.eq_ignore_ascii_case("TEXT") {
        return Ok((
            IndexValue::Text(token(tokens, position + 1)?.to_owned()),
            position + 2,
        ));
    }
    if raw.eq_ignore_ascii_case("TRUE") {
        return Ok((IndexValue::Bool(true), position + 1));
    }
    if raw.eq_ignore_ascii_case("FALSE") {
        return Ok((IndexValue::Bool(false), position + 1));
    }
    if raw.eq_ignore_ascii_case("NULL") {
        return Ok((IndexValue::Null, position + 1));
    }
    if let Ok(number) = raw.parse::<i64>() {
        return Ok((IndexValue::I64(number), position + 1));
    }
    Ok((IndexValue::Text(raw.to_owned()), position + 1))
}

fn parse_index_literal_list(
    tokens: &[String],
    mut position: usize,
) -> Result<(Vec<IndexValue>, usize), ControlError> {
    let mut values = Vec::new();
    loop {
        let (value, next) = parse_index_literal(tokens, position)?;
        values.push(value);
        position = next;
        if tokens.get(position).map(String::as_str) == Some(",") {
            position += 1;
            continue;
        }
        return Ok((values, position));
    }
}

fn parse_index_bound(
    tokens: &[String],
    position: usize,
) -> Result<(IndexBound, usize), ControlError> {
    let raw = token(tokens, position)?;
    if raw.eq_ignore_ascii_case("MIN") || raw.eq_ignore_ascii_case("MAX") {
        return Err(ControlError::Syntax(
            "open RANGE bounds (MIN/MAX) are not supported in WCL; use the typed query API"
                .to_owned(),
        ));
    }
    let (value, next) = parse_index_literal(tokens, position)?;
    Ok((
        IndexBound {
            value,
            inclusive: true,
        },
        next,
    ))
}

fn parse_action(token: &str) -> Result<PermissionAction, ControlError> {
    match token.to_ascii_uppercase().as_str() {
        "READ" => Ok(PermissionAction::Read),
        "WRITE" => Ok(PermissionAction::Write),
        "MANAGE" => Ok(PermissionAction::Manage),
        _ => Err(ControlError::Syntax(format!(
            "unsupported permission action {token}"
        ))),
    }
}

fn token(tokens: &[String], index: usize) -> Result<&str, ControlError> {
    tokens
        .get(index)
        .map(String::as_str)
        .ok_or_else(|| ControlError::Syntax(format!("missing token at position {}", index + 1)))
}

fn expect_keyword(tokens: &[String], index: usize, expected: &str) -> Result<(), ControlError> {
    if token(tokens, index)?.eq_ignore_ascii_case(expected) {
        Ok(())
    } else {
        Err(ControlError::Syntax(format!(
            "expected {expected} at position {}",
            index + 1
        )))
    }
}

fn validate_index_fields(fields: &[IndexFieldSpec]) -> Result<(), ControlError> {
    if fields.is_empty() || fields.len() > 16 {
        return Err(ControlError::InvalidOperation(format!(
            "an Index needs between 1 and 16 fields, got {}",
            fields.len()
        )));
    }
    for field in fields {
        match field {
            IndexFieldSpec::Key => {}
            IndexFieldSpec::Metadata { name } if !name.is_empty() && name.len() <= 256 => {}
            IndexFieldSpec::Metadata { .. } => {
                return Err(ControlError::InvalidOperation(
                    "Index Metadata field names must be 1-256 bytes".to_owned(),
                ));
            }
            IndexFieldSpec::Projection {
                extractor, field, ..
            } if !extractor.is_empty()
                && extractor.len() <= 256
                && !field.is_empty()
                && field.len() <= 256 => {}
            IndexFieldSpec::Projection { .. } => {
                return Err(ControlError::InvalidOperation(
                    "Index Projection extractors and fields must be 1-256 bytes".to_owned(),
                ));
            }
        }
    }
    Ok(())
}

/// Named keys are single-segment lowercase identifiers (`passport`,
/// `dept_code`): `^[a-z][a-z0-9_]*$`, at most 64 bytes, distinct per store.
fn validate_named_keys(named_keys: &[NamedStateStoreKey]) -> Result<(), ControlError> {
    if named_keys.len() > MAX_STATE_STORE_NAMED_KEYS {
        return Err(ControlError::InvalidOperation(format!(
            "a StateStore supports at most {MAX_STATE_STORE_NAMED_KEYS} named keys"
        )));
    }
    let mut seen = std::collections::BTreeSet::new();
    for key in named_keys {
        let name = &key.name;
        let valid = !name.is_empty()
            && name.len() <= 64
            && name.as_bytes()[0].is_ascii_lowercase()
            && name
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
        if !valid {
            return Err(ControlError::InvalidName(name.clone()));
        }
        if !seen.insert(name.clone()) {
            return Err(ControlError::AlreadyExists(format!(
                "duplicate named key {name}"
            )));
        }
    }
    Ok(())
}

pub(crate) fn validate_dotted_name(name: &str) -> Result<(), ControlError> {
    if name.is_empty() || name.len() > 512 || name.starts_with('.') || name.ends_with('.') {
        return Err(ControlError::InvalidName(name.to_owned()));
    }
    if name.split('.').any(|segment| {
        segment.is_empty()
            || !segment.as_bytes()[0].is_ascii_lowercase()
            || !segment
                .chars()
                .all(|character| character.is_ascii_lowercase() || character.is_ascii_digit())
    }) {
        return Err(ControlError::InvalidName(name.to_owned()));
    }
    Ok(())
}

/// Validate and canonicalize a BLAKE3-256 hex verifier supplied on
/// `ISSUE API KEY`. The plaintext key never appears in commands.
fn canonical_api_key_digest(digest: &str) -> Result<String, ControlError> {
    let normalized = digest.trim().to_ascii_lowercase();
    let hash = blake3::Hash::from_hex(&normalized).map_err(|_| {
        ControlError::InvalidOperation(
            "API key digest must be a 64-digit hex BLAKE3-256 verifier".to_owned(),
        )
    })?;
    Ok(hash.to_hex().to_string())
}

/// Compute the request-time authorization view from catalog state:
/// every Active issued identity whose digest authorizes as
/// `riverbed_admin` when its Role holds `manage` on the root `*`
/// namespace.
fn issued_api_key_view(state: &CatalogState) -> IssuedApiKeySet {
    let mut view = IssuedApiKeySet::new();
    for identity in state.api_keys.values() {
        if identity.status != ResourceStatus::Active {
            continue;
        }
        let Ok(digest) = blake3::Hash::from_hex(&identity.digest) else {
            continue;
        };
        let riverbed_admin = state.grants.values().any(|grant| {
            grant.role_id == identity.role_id
                && grant.effect == GrantEffect::Allow
                && grant.actions.contains(&PermissionAction::Manage)
                && grant.namespace == "*"
        });
        let grants = state
            .grants
            .values()
            .filter(|grant| grant.role_id == identity.role_id)
            .map(|grant| IssuedGrant {
                namespace: grant.namespace.clone(),
                actions: grant.actions.clone(),
                effect: grant.effect,
            })
            .collect();
        view.insert(
            *digest.as_bytes(),
            IssuedApiKey {
                name: identity.name.clone(),
                riverbed_admin,
                grants,
                expires_at_ns: identity.expires_at_ns,
            },
        );
    }
    view
}

fn validate_namespace_pattern(pattern: &str) -> Result<(), ControlError> {
    if pattern == "*" {
        return Ok(());
    }
    if let Some(prefix) = pattern.strip_suffix(".*") {
        validate_dotted_name(prefix)
    } else {
        validate_dotted_name(pattern)
    }
}

/// Whether a grant namespace `pattern` spans an entire Domain — the
/// root `*` or a `{domain}.*` wildcard. An exact grant names one
/// resource, never a Domain's member Feeds, so Domain-scoped
/// Subscriptions need one of the two spanning forms.
pub(crate) fn grant_covers_domain(pattern: &str, domain: &str) -> bool {
    pattern == "*" || pattern.strip_suffix(".*") == Some(domain)
}

pub(crate) fn namespace_matches(pattern: &str, feed: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    pattern
        .strip_suffix(".*")
        .map_or(pattern == feed, |prefix| {
            is_within_namespace(feed, prefix) && feed != prefix
        })
}

fn is_within_namespace(name: &str, namespace: &str) -> bool {
    name == namespace
        || name
            .strip_prefix(namespace)
            .is_some_and(|rest| rest.starts_with('.'))
}

fn ensure_name_available<'a>(
    mut items: impl Iterator<Item = (&'a String, &'a ResourceStatus)>,
    name: &str,
) -> Result<(), ControlError> {
    if items.any(|(existing, _)| existing == name) {
        Err(ControlError::AlreadyExists(name.to_owned()))
    } else {
        Ok(())
    }
}

/// Validates the Subscription frontier translations committed with an Active
/// Range activation. The catalog cannot compare `expected_cursor` against the
/// committed frontier — progress lives on a separate quorum — so validation
/// proves only catalog-visible consistency: the Subscription is an active
/// Feed-scoped member of this Feed, the translation names this plan, and the
/// translated positions cover exactly the candidate range map under the same
/// bounds the progress shard enforces.
fn validate_subscription_translations(
    state: &CatalogState,
    feed_id: Uuid,
    candidate: &RangeMap,
    plan_id: Uuid,
    translations: &[SubscriptionCutoverTranslation],
) -> Result<(), ControlError> {
    let candidate_ranges: BTreeSet<RangeId> = candidate
        .routes()
        .iter()
        .map(|route| route.range_id)
        .collect();
    if translations.len() > 1024 || serde_json::to_vec(translations)?.len() > 512 * 1024 {
        return Err(ControlError::InvalidOperation(
            "Subscription frontier cutover exceeds its bounded budget".to_owned(),
        ));
    }
    let mut seen = BTreeSet::new();
    for translation in translations {
        let subscription = state
            .subscriptions
            .get(&translation.subscription_id)
            .filter(|item| item.status == ResourceStatus::Active)
            .ok_or_else(|| {
                ControlError::InvalidOperation(
                    "Subscription cutover references an unknown Subscription".to_owned(),
                )
            })?;
        let positions: BTreeSet<RangeId> = translation.positions.keys().copied().collect();
        let quarantined_positions = translation
            .quarantined
            .values()
            .map(Vec::len)
            .sum::<usize>();
        // A translation scopes to one Feed: either the Subscription's exact
        // Feed source or an Active member Feed inside a Domain source — the
        // pending record then applies against that member Feed's entry.
        let covers_feed = subscription.source.feed_id() == Some(feed_id)
            || state
                .subscription_feed_memberships
                .get(&translation.subscription_id)
                .and_then(|members| members.get(&feed_id))
                .is_some_and(|member| member.state == SubscriptionFeedState::Active);
        if !seen.insert(translation.subscription_id)
            || !covers_feed
            || translation.feed_id != feed_id
            || translation.plan_id != plan_id
            || translation.expected_cursor.is_empty()
            || translation.expected_cursor.len() > 256
            || translation.cursor.len() > 256
            || translation.frontier_sequence.checked_add(1).is_none()
            || positions != candidate_ranges
            || translation
                .positions
                .values()
                .any(|cursor| cursor.len() > 256)
            || !translation
                .quarantined
                .keys()
                .all(|range| candidate_ranges.contains(range))
            || quarantined_positions > crate::reader::SUBSCRIPTION_QUARANTINE_MAX_POSITIONS
            || translation
                .quarantined
                .values()
                .flatten()
                .any(|cursor| cursor.is_empty() || cursor.len() > 256)
            || !translation
                .skipped
                .keys()
                .all(|range| candidate_ranges.contains(range))
            || translation.skipped.values().map(Vec::len).sum::<usize>()
                > crate::reader::SUBSCRIPTION_SKIP_MAX_POSITIONS
            || translation
                .skipped
                .values()
                .flatten()
                .any(|cursor| cursor.is_empty() || cursor.len() > 256)
        {
            return Err(ControlError::InvalidOperation(
                "Subscription frontier translation is inconsistent with the candidate range map"
                    .to_owned(),
            ));
        }
    }
    Ok(())
}

fn validate_reader_translations(
    state: &CatalogState,
    feed_id: Uuid,
    candidate: &RangeMap,
    translations: &[ReaderFrontierTranslation],
) -> Result<(), ControlError> {
    let readers = state
        .readers
        .values()
        .filter(|reader| reader.feed_id == feed_id && reader.status == ResourceStatus::Active)
        .count();
    if readers > 1024
        || translations.len() != readers
        || candidate.routes().len() > 128
        || translations.iter().any(|item| {
            item.expected_acknowledged.len() > 128
                || item.expected_delivered.len() > 128
                || item
                    .expected_acknowledged
                    .values()
                    .any(|cursor| cursor.len() > 256)
                || item
                    .expected_delivered
                    .values()
                    .any(|cursor| cursor.len() > 256)
                || item
                    .expected_acknowledged_cursor
                    .as_ref()
                    .is_some_and(|cursor| cursor.len() > 256)
                || item
                    .expected_delivered_cursor
                    .as_ref()
                    .is_some_and(|cursor| cursor.len() > 256)
        })
        || serde_json::to_vec(translations)?.len() > 512 * 1024
    {
        return Err(ControlError::InvalidOperation(
            "Reader frontier cutover is incomplete or exceeds its bounded budget".to_owned(),
        ));
    }
    let mut seen = BTreeSet::new();
    for translation in translations {
        let reader = state.readers.get(&translation.reader_id).ok_or_else(|| {
            ControlError::InvalidOperation("Reader cutover references an unknown Reader".to_owned())
        })?;
        let prior = state.reader_frontiers.get(&reader.reader_id);
        let previous = prior.cloned().unwrap_or_default();
        if !seen.insert(reader.reader_id)
            || reader.feed_id != feed_id
            || reader.status != ResourceStatus::Active
            || reader.session_epoch != translation.expected_session_epoch
            || reader.session_epoch.checked_add(1).is_none()
            || reader.acknowledged_cursor != translation.expected_acknowledged_cursor
            || reader.delivered_cursor != translation.expected_delivered_cursor
            || prior.is_some() != translation.expected_has_frontier
            || previous.acknowledged != translation.expected_acknowledged
            || previous.delivered != translation.expected_delivered
            || translation.acknowledged.len() != candidate.routes().len()
            || candidate
                .routes()
                .iter()
                .any(|route| !translation.acknowledged.contains_key(&route.range_id))
            || translation
                .acknowledged
                .values()
                .any(|cursor| cursor.len() > 256)
        {
            return Err(ControlError::InvalidOperation(
                "Reader frontier changed or translation does not match candidate ranges".to_owned(),
            ));
        }
    }
    Ok(())
}

fn install_reader_translations(
    state: &mut CatalogState,
    translations: Vec<ReaderFrontierTranslation>,
) {
    for translation in translations {
        if let Some(reader) = state.readers.get_mut(&translation.reader_id) {
            let preserve_unstarted = !translation.expected_has_frontier
                && reader.acknowledged_cursor.is_none()
                && reader.delivered_cursor.is_none()
                && !matches!(reader.start, ReaderStart::Beginning);
            reader.session_epoch = reader.session_epoch.saturating_add(1);
            reader.session_active = false;
            reader.session_capacity = 0;
            reader.delivered_cursor = reader.acknowledged_cursor.clone();
            if !preserve_unstarted {
                state.reader_frontiers.insert(
                    reader.reader_id,
                    ReaderFrontier {
                        acknowledged: translation.acknowledged.clone(),
                        delivered: translation.acknowledged,
                        previous: BTreeMap::new(),
                        last_fetch_limit: 0,
                        last_fetch_retry_after_ms: 0,
                        last_fetch_request_id: None,
                    },
                );
            }
        }
    }
}

fn range_operation_pending(state: &CatalogState, feed_id: Uuid) -> bool {
    state.range_split_plans.contains_key(&feed_id)
        || state.range_merge_plans.contains_key(&feed_id)
        || state
            .range_move_plans
            .values()
            .any(|plan| plan.feed_id == feed_id)
        || state
            .owner_move_plans
            .values()
            .any(|plan| plan.feed_id == feed_id)
}

fn owning_space<'a>(
    state: &'a CatalogState,
    feed: &str,
) -> Result<&'a SpaceDefinition, ControlError> {
    state
        .spaces
        .values()
        .filter(|space| {
            space.status == ResourceStatus::Active && feed.starts_with(&(space.name.clone() + "."))
        })
        .max_by_key(|space| space.name.len())
        .ok_or_else(|| ControlError::NotFound(format!("owning Domain for Feed {feed}")))
}

fn active_space<'a>(
    state: &'a CatalogState,
    name: &str,
) -> Result<&'a SpaceDefinition, ControlError> {
    state
        .spaces
        .values()
        .find(|item| item.name == name && item.status == ResourceStatus::Active)
        .ok_or_else(|| ControlError::NotFound(format!("Domain {name}")))
}

fn active_space_mut<'a>(
    state: &'a mut CatalogState,
    name: &str,
) -> Result<&'a mut SpaceDefinition, ControlError> {
    state
        .spaces
        .values_mut()
        .find(|item| item.name == name && item.status == ResourceStatus::Active)
        .ok_or_else(|| ControlError::NotFound(format!("Domain {name}")))
}

fn active_subscription<'a>(
    state: &'a CatalogState,
    name: &str,
) -> Result<&'a SubscriptionDefinition, ControlError> {
    state
        .subscriptions
        .values()
        .find(|item| item.name == name && item.status == ResourceStatus::Active)
        .ok_or_else(|| ControlError::NotFound(format!("Subscription {name}")))
}

fn active_feed<'a>(
    state: &'a CatalogState,
    name: &str,
) -> Result<&'a FeedDefinition, ControlError> {
    state
        .feeds
        .values()
        .find(|item| item.name == name && item.status == ResourceStatus::Active)
        .ok_or_else(|| ControlError::NotFound(format!("Feed {name}")))
}

fn active_feed_mut<'a>(
    state: &'a mut CatalogState,
    name: &str,
) -> Result<&'a mut FeedDefinition, ControlError> {
    state
        .feeds
        .values_mut()
        .find(|item| item.name == name && item.status == ResourceStatus::Active)
        .ok_or_else(|| ControlError::NotFound(format!("Feed {name}")))
}

fn active_writer_mut<'a>(
    state: &'a mut CatalogState,
    name: &str,
) -> Result<&'a mut WriterDefinition, ControlError> {
    state
        .writers
        .values_mut()
        .find(|item| item.name == name && item.status == ResourceStatus::Active)
        .ok_or_else(|| ControlError::NotFound(format!("Writer {name}")))
}

fn validate_writer_epoch(
    writer: &WriterDefinition,
    session_epoch: u64,
) -> Result<(), ControlError> {
    if !writer.session_active {
        return Err(ControlError::InvalidOperation(format!(
            "Writer {} has no active session",
            writer.name
        )));
    }
    if writer.session_epoch != session_epoch {
        return Err(ControlError::InvalidOperation(format!(
            "stale Writer session epoch: current={}, supplied={session_epoch}",
            writer.session_epoch
        )));
    }
    Ok(())
}

fn validate_reader_epoch(
    reader: &ReaderDefinition,
    session_epoch: u64,
) -> Result<(), ControlError> {
    if !reader.session_active {
        return Err(ControlError::InvalidOperation(format!(
            "Reader {} has no active session",
            reader.name
        )));
    }
    if reader.session_epoch != session_epoch {
        return Err(ControlError::InvalidOperation(format!(
            "stale Reader session epoch: current={}, supplied={session_epoch}",
            reader.session_epoch
        )));
    }
    Ok(())
}

fn active_reader_mut<'a>(
    state: &'a mut CatalogState,
    name: &str,
) -> Result<&'a mut ReaderDefinition, ControlError> {
    state
        .readers
        .values_mut()
        .find(|item| item.name == name && item.status == ResourceStatus::Active)
        .ok_or_else(|| ControlError::NotFound(format!("Reader {name}")))
}

fn active_role<'a>(
    state: &'a CatalogState,
    name: &str,
) -> Result<&'a RoleDefinition, ControlError> {
    state
        .roles
        .values()
        .find(|item| item.name == name && item.status == ResourceStatus::Active)
        .ok_or_else(|| ControlError::NotFound(format!("Role {name}")))
}

trait NamedResource {
    fn name(&self) -> &str;
    fn set_name(&mut self, name: String);
    fn status(&self) -> &ResourceStatus;
    fn set_status(&mut self, status: ResourceStatus);
}

macro_rules! impl_named_resource {
    ($type:ty) => {
        impl NamedResource for $type {
            fn name(&self) -> &str {
                &self.name
            }
            fn set_name(&mut self, name: String) {
                self.name = name;
            }
            fn status(&self) -> &ResourceStatus {
                &self.status
            }
            fn set_status(&mut self, status: ResourceStatus) {
                self.status = status;
            }
        }
    };
}

impl_named_resource!(WriterDefinition);
impl_named_resource!(ReaderDefinition);
impl_named_resource!(SubscriptionDefinition);
impl_named_resource!(PipeDefinition);
impl_named_resource!(RoleDefinition);
impl_named_resource!(RiverbedLink);
impl_named_resource!(DistributaryDefinition);

fn rename_named<T: Clone + Serialize + NamedResource>(
    items: &mut BTreeMap<Uuid, T>,
    current: &str,
    new: &str,
    kind: &str,
) -> Result<(String, Value), ControlError> {
    if items.values().any(|item| item.name() == new) {
        return Err(ControlError::AlreadyExists(format!("{kind} {new}")));
    }
    let item = items
        .values_mut()
        .find(|item| item.name() == current && *item.status() == ResourceStatus::Active)
        .ok_or_else(|| ControlError::NotFound(format!("{kind} {current}")))?;
    item.set_name(new.to_owned());
    Ok((
        format!("renamed {kind} {current} to {new}"),
        serde_json::to_value(item.clone())?,
    ))
}

/// Attaches one Feed to every active Domain-scoped Subscription covering
/// its Domain. A newly discovered Feed starts at `ReaderStart::Beginning`
/// — its first retained record — regardless of each Subscription's
/// declared start. Refuses when a Subscription is at its member bound
/// rather than silently skipping the Feed.
fn attach_feed_to_domain_subscriptions(
    state: &mut CatalogState,
    feed: &FeedDefinition,
    revision: u64,
) -> Result<(), ControlError> {
    let covering: Vec<(Uuid, String)> = state
        .subscriptions
        .values()
        .filter(|item| {
            item.status == ResourceStatus::Active && item.source.domain_id() == Some(feed.space_id)
        })
        .map(|item| (item.subscription_id, item.name.clone()))
        .collect();
    for (subscription_id, subscription_name) in covering {
        let members = state
            .subscription_feed_memberships
            .entry(subscription_id)
            .or_default();
        if let Some(existing) = members.get_mut(&feed.feed_id) {
            // Reattaching a Feed that moved out and back keeps its member
            // identity (immutable FeedId) and original start policy; the
            // fresh revision is the discovery boundary for resumed coverage.
            existing.state = SubscriptionFeedState::Active;
            existing.attached_revision = revision;
            existing.detail = None;
            continue;
        }
        if members.len() >= MAX_SUBSCRIPTION_MEMBER_FEEDS {
            return Err(ControlError::InvalidOperation(format!(
                "Domain Subscription {subscription_name} reached its {MAX_SUBSCRIPTION_MEMBER_FEEDS} member-Feed bound; drop the Subscription or detach member Feeds first"
            )));
        }
        members.insert(
            feed.feed_id,
            SubscriptionFeedMembership {
                feed_id: feed.feed_id,
                state: SubscriptionFeedState::Active,
                start: ReaderStart::Beginning,
                attached_revision: revision,
                detail: None,
            },
        );
    }
    Ok(())
}

/// Attaches every active Feed in the Subscription's Domain, honoring the
/// declared start policy, in the same commit that declares it.
fn attach_subscription_feed_members(
    state: &mut CatalogState,
    subscription_id: Uuid,
    space_id: Uuid,
    declared_start: &ReaderStart,
    revision: u64,
) {
    let feed_ids: Vec<Uuid> = state
        .feeds
        .values()
        .filter(|item| item.status == ResourceStatus::Active && item.space_id == space_id)
        .map(|item| item.feed_id)
        .collect();
    let members = state
        .subscription_feed_memberships
        .entry(subscription_id)
        .or_default();
    for feed_id in feed_ids {
        members
            .entry(feed_id)
            .or_insert_with(|| SubscriptionFeedMembership {
                feed_id,
                state: SubscriptionFeedState::Active,
                start: declared_start.clone(),
                attached_revision: revision,
                detail: None,
            });
    }
}

fn drop_named<T: Clone + Serialize + NamedResource>(
    items: &mut BTreeMap<Uuid, T>,
    name: &str,
    kind: &str,
) -> Result<(String, Value), ControlError> {
    let item = items
        .values_mut()
        .find(|item| item.name() == name && *item.status() == ResourceStatus::Active)
        .ok_or_else(|| ControlError::NotFound(format!("{kind} {name}")))?;
    item.set_status(ResourceStatus::Dropped);
    Ok((
        format!("dropped {kind} {name}"),
        serde_json::to_value(item.clone())?,
    ))
}

fn show_resources(
    state: &CatalogState,
    kind: ShowKind,
    limit: Option<usize>,
    configured: &BTreeSet<StorageNodeId>,
    policy: &CapacityPolicy,
) -> Value {
    match kind {
        ShowKind::AuditEvents => {
            // Newest-first: the events an operator wants are the most
            // recent administrative actions. `LIMIT` is the caller's
            // page size; the full bounded trail is the default.
            json!(state
                .audit_events
                .iter()
                .rev()
                .take(limit.unwrap_or(usize::MAX))
                .collect::<Vec<_>>())
        }
        ShowKind::Compatibility => json!({
            // The rollback boundary: the catalog layout this state was
            // written as versus the newest this binary can read. A Node
            // whose persisted schema exceeds the supported level refuses
            // to boot rather than misread it.
            "catalog_schema_version": state.schema_version,
            "supported_catalog_schema_version": CATALOG_SCHEMA_VERSION,
            // The protocol gate: levels this binary accepts in
            // self-reported capacity reports. A report outside them is
            // refused and recorded below.
            "node_protocol_version": crate::node::NODE_PROTOCOL_VERSION,
            "min_node_protocol_version": crate::node::MIN_NODE_PROTOCOL_VERSION,
            "build": crate::node::BUILD_VERSION,
            // Every identity the Riverbed knows: configured-eligible
            // Nodes, registered Nodes, and reporters — deduplicated and
            // sorted so the surface is deterministic. An eligible Node
            // still shows its advertised version before it registers.
            "nodes": state
                .storage_nodes
                .keys()
                .chain(state.capacity_reports.keys())
                .chain(configured.iter())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .map(|node| {
                    let record = state.storage_nodes.get(node);
                    let report = state.capacity_reports.get(node);
                    json!({
                        "node": node,
                        "registered": record.is_some(),
                        "endpoint": record.map(|r| r.endpoint.clone()),
                        "capabilities": record.map(|r| r.capabilities),
                        "protocol_version": report.map(|r| r.protocol_version),
                        "build": report.map(|r| r.build.clone()),
                        "last_reported_at_ns": report.map(|r| r.reported_at_ns),
                    })
                })
                .collect::<Vec<_>>(),
            "incompatible_reports": state
                .incompatible_reports
                .iter()
                .map(|(node, row)| json!({
                    "node": node,
                    "protocol_version": row.protocol_version,
                    "build": row.build,
                    "refused_at_ns": row.refused_at_ns,
                }))
                .collect::<Vec<_>>(),
        }),
        ShowKind::Domains | ShowKind::Spaces => json!(state
            .spaces
            .values()
            .filter(|item| item.status == ResourceStatus::Active)
            .collect::<Vec<_>>()),
        ShowKind::Feeds => json!(state
            .feeds
            .values()
            .filter(|item| item.status == ResourceStatus::Active)
            .map(|item| {
                let mut row = serde_json::to_value(item).unwrap_or_else(|_| json!({}));
                row["schema"] = json!(state
                    .schemas
                    .get(&item.feed_id)
                    .map(|assignment| assignment.schema.clone()));
                row["effective_schema"] = json!(effective_schema(state, item));
                row
            })
            .collect::<Vec<_>>()),
        ShowKind::Writers => json!(state
            .writers
            .values()
            .filter(|item| item.status == ResourceStatus::Active)
            .collect::<Vec<_>>()),
        ShowKind::Readers => json!(state
            .readers
            .values()
            .filter(|item| item.status == ResourceStatus::Active)
            .collect::<Vec<_>>()),
        ShowKind::Subscriptions => json!(state
            .subscriptions
            .values()
            .filter(|item| item.status == ResourceStatus::Active)
            .collect::<Vec<_>>()),
        ShowKind::Pipes => json!(state
            .pipes
            .values()
            .filter(|item| item.status == ResourceStatus::Active)
            .map(|item| {
                let mut row = serde_json::to_value(item).unwrap_or_else(|_| json!({}));
                if let Some(lease) = effective_pipe_compute_lease(state, item.pipe_id) {
                    row["compute_owner"] = json!(lease.owner.as_str());
                    row["compute_epoch"] = json!(lease.compute_epoch);
                }
                row
            })
            .collect::<Vec<_>>()),
        ShowKind::Indexes => json!(state.indexes.values().collect::<Vec<_>>()),
        ShowKind::RiverbedLinks => json!(state
            .riverbed_links
            .values()
            .filter(|item| item.status == ResourceStatus::Active)
            .collect::<Vec<_>>()),
        ShowKind::Distributaries => json!(state
            .distributaries
            .values()
            .filter(|item| item.status == ResourceStatus::Active)
            .map(|item| {
                let mut row = serde_json::to_value(item).unwrap_or_else(|_| json!({}));
                if let Some(link) = state.riverbed_links.get(&item.link_id) {
                    row["link"] = json!(link.name.clone());
                }
                if let Some(feed) = state.feeds.get(&item.target_feed_id) {
                    row["target_feed"] = json!(feed.name.clone());
                }
                if let Some(lease) = state.distributary_leases.get(&item.distributary_id) {
                    row["lease_owner"] = json!(lease.owner.as_str());
                    row["lease_epoch"] = json!(lease.epoch);
                }
                if let Some(status) = state.distributary_status.get(&item.distributary_id) {
                    row["run_state"] = json!(status.run_state);
                    row["copied_records"] = json!(status.copied_records);
                    if let Some(ts) = status.last_copied_at_ns {
                        row["last_copied_at_ns"] = json!(ts);
                    }
                    if let Some(error) = &status.last_error {
                        row["last_error"] = json!(error);
                        row["last_error_at_ns"] = json!(status.last_error_at_ns);
                    }
                    row["reported_at_ns"] = json!(status.reported_at_ns);
                    // A report predating the committed lease is history,
                    // not live evidence — mark it rather than hide it.
                    row["status_stale"] = json!(state
                        .distributary_leases
                        .get(&item.distributary_id)
                        .is_none_or(|lease| lease.epoch != status.lease_epoch
                            || lease.owner != status.reported_by));
                }
                row
            })
            .collect::<Vec<_>>()),
        ShowKind::ApiKeys => json!(state
            .api_keys
            .values()
            .map(|identity| {
                let role = state
                    .roles
                    .get(&identity.role_id)
                    .map(|role| role.name.clone());
                json!({
                    "key_id": identity.key_id,
                    "name": identity.name,
                    "role": role,
                    // Only a verifier prefix is exposed; the full digest
                    // stays internal even though it is not a secret.
                    "digest_prefix": identity.digest.chars().take(16).collect::<String>(),
                    "status": identity.status,
                    "created_at_ns": identity.created_at_ns,
                    "expires_at_ns": identity.expires_at_ns,
                    "revoked_at_ns": identity.revoked_at_ns,
                })
            })
            .collect::<Vec<_>>()),
        ShowKind::StateStores => json!(state
            .state_stores
            .values()
            .filter(|item| item.stage != StateStoreStage::Dropped)
            .collect::<Vec<_>>()),
        ShowKind::HistoryPolicies => json!(state.history_policies.values().collect::<Vec<_>>()),
        ShowKind::LegalHolds => json!(state.legal_holds.values().collect::<Vec<_>>()),
        ShowKind::Schemas => json!(state.schemas.values().collect::<Vec<_>>()),
        ShowKind::Roles => json!(state
            .roles
            .values()
            .filter(|item| item.status == ResourceStatus::Active)
            .collect::<Vec<_>>()),
        ShowKind::Grants => json!(state.grants.values().collect::<Vec<_>>()),
        ShowKind::StorageNodes => {
            let registered: BTreeSet<&StorageNodeId> = state.storage_nodes.keys().collect();
            json!(
                storage_eligibility_report(state, configured, policy, unix_ns(),)
                    .into_iter()
                    .map(|entry| {
                        json!({
                            "node": entry.node.as_str(),
                            "endpoint": state
                                .storage_nodes
                                .get(&entry.node)
                                .map(|record| record.endpoint.as_str()),
                            "cert_pins": state
                                .storage_nodes
                                .get(&entry.node)
                                .map(|record| record.cert_pins.iter().collect::<Vec<_>>())
                                .unwrap_or_default(),
                            "capabilities": state
                                .storage_nodes
                                .get(&entry.node)
                                .map(|record| record.capabilities),
                            "capacity": state
                                .capacity_reports
                                .get(&entry.node)
                                .map(|report| report.capacity.clone()),
                            "capacity_reported_at_ns": state
                                .capacity_reports
                                .get(&entry.node)
                                .map(|report| report.reported_at_ns.to_string()),
                            "source": if registered.contains(&entry.node) {
                                "registered"
                            } else {
                                "configured"
                            },
                            "eligible": entry.eligible,
                            "exclusions": entry.exclusions,
                            "draining": state.draining_storage_nodes.contains(&entry.node),
                        })
                    })
                    .collect::<Vec<_>>()
            )
        }
    }
}

fn describe_resource(
    state: &CatalogState,
    kind: ResourceKind,
    name: &str,
) -> Result<Value, ControlError> {
    match kind {
        ResourceKind::Domain | ResourceKind::Space => {
            let space = active_space(state, name)?;
            let mut value = json!(space);
            value["history_policy"] = json!(state
                .history_policies
                .get(&space.space_id)
                .map(|assignment| assignment.policy.clone()));
            value["schema"] = json!(state
                .schemas
                .get(&space.space_id)
                .map(|assignment| assignment.schema.clone()));
            Ok(value)
        }
        ResourceKind::Feed => {
            let feed = active_feed(state, name)?;
            let mut value = json!(feed);
            value["history_policy"] = json!(state
                .history_policies
                .get(&feed.feed_id)
                .map(|assignment| assignment.policy.clone()));
            value["effective_history_policy"] = json!(effective_history_policy(state, feed));
            value["legal_hold"] = json!(state.legal_holds.get(&feed.feed_id));
            value["encrypted"] = json!(feed.encrypted);
            value["erased_at_ns"] = json!(feed.erased_at_ns);
            value["schema"] = json!(state
                .schemas
                .get(&feed.feed_id)
                .map(|assignment| assignment.schema.clone()));
            value["effective_schema"] = json!(effective_schema(state, feed));
            Ok(value)
        }
        ResourceKind::Writer => state
            .writers
            .values()
            .find(|item| item.name == name && item.status == ResourceStatus::Active)
            .map(|item| json!(item))
            .ok_or_else(|| ControlError::NotFound(format!("Writer {name}"))),
        ResourceKind::Reader => state
            .readers
            .values()
            .find(|item| item.name == name && item.status == ResourceStatus::Active)
            .map(|item| json!(item))
            .ok_or_else(|| ControlError::NotFound(format!("Reader {name}"))),
        ResourceKind::Subscription => {
            let item = state
                .subscriptions
                .values()
                .find(|item| item.name == name && item.status == ResourceStatus::Active)
                .cloned()
                .ok_or_else(|| ControlError::NotFound(format!("Subscription {name}")))?;
            let mut value = json!(item);
            if item.source.domain_id().is_some() {
                let members: Vec<SubscriptionFeedMembership> = state
                    .subscription_feed_memberships
                    .get(&item.subscription_id)
                    .map(|members| members.values().cloned().collect())
                    .unwrap_or_default();
                value["member_feeds"] = json!(members);
            }
            Ok(value)
        }
        ResourceKind::Pipe => state
            .pipes
            .values()
            .find(|item| item.name == name && item.status == ResourceStatus::Active)
            .map(|item| json!(item))
            .ok_or_else(|| ControlError::NotFound(format!("Pipe {name}"))),
        ResourceKind::Index => state
            .indexes
            .values()
            .find(|item| item.name == name)
            .map(|item| json!(item))
            .ok_or_else(|| ControlError::NotFound(format!("Index {name}"))),
        ResourceKind::StateStore => state
            .state_stores
            .values()
            .find(|item| item.name == name && item.stage != StateStoreStage::Dropped)
            .map(|item| json!(item))
            .ok_or_else(|| ControlError::NotFound(format!("StateStore {name}"))),
        ResourceKind::Role => Ok(json!(active_role(state, name)?)),
        ResourceKind::RiverbedLink => state
            .riverbed_links
            .values()
            .find(|item| item.name == name && item.status == ResourceStatus::Active)
            .map(|item| json!(item))
            .ok_or_else(|| ControlError::NotFound(format!("Riverbed Link {name}"))),
        ResourceKind::Distributary => {
            let item = state
                .distributaries
                .values()
                .find(|item| item.name == name && item.status == ResourceStatus::Active)
                .ok_or_else(|| ControlError::NotFound(format!("Distributary {name}")))?;
            let mut row = serde_json::to_value(item).unwrap_or_else(|_| json!({}));
            if let Some(link) = state.riverbed_links.get(&item.link_id) {
                row["link"] = json!(link.name.clone());
            }
            if let Some(feed) = state.feeds.get(&item.target_feed_id) {
                row["target_feed"] = json!(feed.name.clone());
            }
            if let Some(lease) = state.distributary_leases.get(&item.distributary_id) {
                row["lease"] = json!(lease);
            }
            if let Some(status) = state.distributary_status.get(&item.distributary_id) {
                row["status"] = json!(status);
                row["status_stale"] = json!(state
                    .distributary_leases
                    .get(&item.distributary_id)
                    .is_none_or(|lease| lease.epoch != status.lease_epoch
                        || lease.owner != status.reported_by));
            }
            Ok(row)
        }
    }
}

/// Returns which live or in-flight placement still references a storage Node,
/// or None when retiring it cannot strand replicas or owners.
fn storage_node_in_use(state: &CatalogState, node: &StorageNodeId) -> Option<&'static str> {
    fn touches(assignment: &ActiveRangeAssignment, node: &StorageNodeId) -> bool {
        assignment.owner == *node || assignment.replicas.contains(node)
    }
    if state.range_assignments.values().any(|a| touches(a, node)) {
        return Some("an Active Range assignment");
    }
    if state.range_split_plans.values().any(|plan| {
        plan.left_assignment
            .as_ref()
            .is_some_and(|a| touches(a, node))
            || touches(&plan.right_assignment, node)
    }) {
        return Some("a pending Active Range split");
    }
    if state
        .range_merge_plans
        .values()
        .any(|plan| touches(&plan.merged_assignment, node))
    {
        return Some("a pending Active Range merge");
    }
    if state.range_move_plans.values().any(|plan| {
        touches(&plan.source_assignment, node)
            || touches(&plan.candidate_assignment, node)
            || plan.removed_replica == *node
            || plan.replacement_replica == *node
    }) {
        return Some("a pending follower move");
    }
    if state.owner_move_plans.values().any(|plan| {
        touches(&plan.source_assignment, node) || touches(&plan.candidate_assignment, node)
    }) {
        return Some("a pending owner move");
    }
    if state
        .subscription_progress_assignments
        .values()
        .any(|assignment| assignment.owner == *node || assignment.replicas.contains(node))
    {
        return Some("a Subscription progress assignment");
    }
    if state
        .pipe_compute_leases
        .values()
        .any(|lease| lease.owner == *node)
    {
        return Some("a Pipe compute lease");
    }
    if state
        .distributary_leases
        .values()
        .any(|lease| lease.owner == *node)
    {
        return Some("a Distributary lease");
    }
    None
}

/// Why a Node is excluded from storage placement.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StorageExclusion {
    /// DRAIN STORAGE NODE is set: the Node keeps serving existing replicas
    /// but nothing new may land on it.
    Draining,
    /// RETIRE removed the identity; it must re-register before serving.
    Retired,
    /// The registered descriptor does not declare the storage role.
    StorageUndeclared,
    /// The baseline policy requires measured evidence but the Node has
    /// committed no capacity report.
    CapacityUnreported,
    /// The last committed report is older than the configured freshness
    /// bound, so its evidence can no longer be trusted.
    CapacityStale,
    /// The committed report shows disk headroom below the configured
    /// minimum baseline.
    BelowBaseline,
}

/// One Node's storage-placement eligibility plus the reasons it is
/// excluded — the observable counterpart of the internal pool filter.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeStorageEligibility {
    pub node: StorageNodeId,
    pub eligible: bool,
    #[serde(default)]
    pub exclusions: Vec<StorageExclusion>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageDrainPlan {
    pub node: StorageNodeId,
    pub draining: bool,
    pub moves: Vec<Command>,
    pub unplannable: Vec<String>,
    pub ready_to_retire: bool,
    /// Placement eligibility for every storage identity the catalog knows
    /// about, with exclusion reasons, at the moment this plan was built.
    #[serde(default)]
    pub storage_pool: Vec<NodeStorageEligibility>,
}

/// Resolve the lease that currently authorizes driving `pipe_id`: the
/// persisted entry when one exists, otherwise the legacy co-location
/// with the Subscription progress owner.
fn effective_pipe_compute_lease(state: &CatalogState, pipe_id: Uuid) -> Option<PipeComputeLease> {
    let pipe = state.pipes.get(&pipe_id)?;
    if pipe.status != ResourceStatus::Active {
        return None;
    }
    if let Some(lease) = state.pipe_compute_leases.get(&pipe_id) {
        return Some(lease.clone());
    }
    let assignment = state
        .subscription_progress_assignments
        .get(&pipe.subscription_id)?;
    Some(PipeComputeLease {
        pipe_id,
        owner: assignment.owner.clone(),
        compute_epoch: assignment.ownership_epoch,
    })
}

/// The capacity-policy reason a Node is excluded, if any. `now_ns` is
/// the evaluation instant: committed `issued_at_ns` inside apply
/// (deterministic across replicas) and wall time on advisory reads.
fn capacity_exclusion(
    state: &CatalogState,
    node: &StorageNodeId,
    policy: &CapacityPolicy,
    now_ns: i64,
) -> Option<StorageExclusion> {
    if !policy.active() {
        return None;
    }
    let Some(report) = state.capacity_reports.get(node) else {
        return Some(StorageExclusion::CapacityUnreported);
    };
    if policy
        .report_max_age_ns
        .is_some_and(|max_age| now_ns.saturating_sub(report.reported_at_ns) > max_age)
    {
        return Some(StorageExclusion::CapacityStale);
    }
    if policy.min_disk_free_bytes > 0
        && report.capacity.disk_free_bytes < policy.min_disk_free_bytes
    {
        return Some(StorageExclusion::BelowBaseline);
    }
    None
}

/// Every storage identity the catalog knows about with the reasons it is
/// currently excluded from placement, so operators see *why* the pool has
/// its present shape rather than a silent absence.
fn storage_eligibility_report(
    state: &CatalogState,
    configured: &BTreeSet<StorageNodeId>,
    policy: &CapacityPolicy,
    now_ns: i64,
) -> Vec<NodeStorageEligibility> {
    let nodes: BTreeSet<StorageNodeId> = configured
        .iter()
        .chain(state.storage_nodes.keys())
        .chain(state.draining_storage_nodes.iter())
        .chain(state.retired_storage_nodes.iter())
        .cloned()
        .collect();
    nodes
        .into_iter()
        .map(|node| {
            let mut exclusions = Vec::new();
            if state.retired_storage_nodes.contains(&node) {
                exclusions.push(StorageExclusion::Retired);
            }
            if state.draining_storage_nodes.contains(&node) {
                exclusions.push(StorageExclusion::Draining);
            }
            if state
                .storage_nodes
                .get(&node)
                .is_some_and(|record| !record.capabilities.storage)
            {
                exclusions.push(StorageExclusion::StorageUndeclared);
            }
            if let Some(exclusion) = capacity_exclusion(state, &node, policy, now_ns) {
                exclusions.push(exclusion);
            }
            let declared = configured.contains(&node) || state.storage_nodes.contains_key(&node);
            NodeStorageEligibility {
                node,
                eligible: declared && exclusions.is_empty(),
                exclusions,
            }
        })
        .collect()
}

fn drain_replacement(
    state: &CatalogState,
    controller: &ControlController,
    excluded: &BTreeSet<StorageNodeId>,
    seed: &Uuid,
    now_ns: i64,
) -> Option<StorageNodeId> {
    let mut ranked = controller
        .storage_pool(state, now_ns)
        .into_iter()
        .filter(|node| !excluded.contains(node))
        .map(|node| {
            let mut hash = blake3::Hasher::new();
            hash.update(b"whitewater-storage-drain-v1");
            hash.update(seed.as_bytes());
            hash.update(node.as_str().as_bytes());
            (*hash.finalize().as_bytes(), node)
        })
        .collect::<Vec<_>>();
    ranked.sort_unstable_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1)));
    ranked.into_iter().next().map(|(_, node)| node)
}

fn storage_drain_plan(
    state: &CatalogState,
    controller: &ControlController,
    node: &StorageNodeId,
) -> StorageDrainPlan {
    let now_ns = unix_ns();
    let mut moves = Vec::new();
    let mut unplannable = Vec::new();
    for (range_id, assignment) in state.range_assignments.iter() {
        if !assignment.replicas.contains(node) {
            continue;
        }
        let feed = state
            .feeds
            .values()
            .find(|feed| feed.feed_id == assignment.feed_id)
            .map(|feed| feed.name.clone())
            .unwrap_or_else(|| assignment.feed_id.to_string());
        if assignment.owner == *node {
            let new_owner = assignment
                .replicas
                .iter()
                .find(|replica| **replica != *node)
                .cloned();
            match new_owner {
                Some(new_owner) => moves.push(Command::PrepareOwnerMove {
                    feed: feed.clone(),
                    range_id: *range_id,
                    new_owner,
                }),
                None => {
                    unplannable.push(format!(
                        "Active Range {range_id} has no other replica to own it"
                    ));
                    continue;
                }
            }
        }
        let excluded: BTreeSet<StorageNodeId> = assignment.replicas.iter().cloned().collect();
        match drain_replacement(state, controller, &excluded, &range_id.as_uuid(), now_ns) {
            Some(replacement) => moves.push(Command::PrepareFollowerMove {
                feed,
                range_id: *range_id,
                removed_replica: node.clone(),
                replacement_replica: replacement,
            }),
            None => unplannable.push(format!(
                "Active Range {range_id} has no eligible replacement Node for {node}"
            )),
        }
    }
    for (subscription_id, assignment) in state.subscription_progress_assignments.iter() {
        if !assignment.replicas.contains(node) {
            continue;
        }
        let mut epoch = assignment.ownership_epoch;
        if assignment.owner == *node {
            let new_owner = assignment
                .replicas
                .iter()
                .find(|replica| **replica != *node)
                .cloned();
            match new_owner {
                Some(new_owner) => {
                    moves.push(Command::RecoverSubscriptionProgressOwner {
                        subscription_id: *subscription_id,
                        expected_ownership_epoch: epoch,
                        new_owner,
                    });
                    epoch += 1;
                }
                None => {
                    unplannable.push(format!(
                        "Subscription progress {subscription_id} has no other replica to own it"
                    ));
                    continue;
                }
            }
        }
        let excluded: BTreeSet<StorageNodeId> = assignment.replicas.iter().cloned().collect();
        match drain_replacement(state, controller, &excluded, subscription_id, now_ns) {
            Some(replacement) => moves.push(Command::MoveSubscriptionProgressReplica {
                subscription_id: *subscription_id,
                expected_ownership_epoch: epoch,
                replaced: node.clone(),
                replacement,
            }),
            None => unplannable.push(format!(
                "Subscription progress {subscription_id} has no eligible replacement Node for {node}"
            )),
        }
    }
    for (pipe_id, lease) in state.pipe_compute_leases.iter() {
        if lease.owner != *node {
            continue;
        }
        let Some(pipe) = state.pipes.get(pipe_id) else {
            continue;
        };
        let excluded: BTreeSet<StorageNodeId> = [node.clone()].into_iter().collect();
        match drain_replacement(state, controller, &excluded, pipe_id, now_ns) {
            Some(replacement) => moves.push(Command::MovePipeComputeLease {
                pipe: pipe.name.clone(),
                new_owner: replacement,
                expected_compute_epoch: Some(lease.compute_epoch),
            }),
            None => unplannable.push(format!(
                "Pipe {} compute lease has no eligible replacement Node for {node}",
                pipe.name
            )),
        }
    }
    for (distributary_id, lease) in state.distributary_leases.iter() {
        if lease.owner != *node {
            continue;
        }
        let Some(distributary) = state.distributaries.get(distributary_id) else {
            continue;
        };
        let excluded: BTreeSet<StorageNodeId> = [node.clone()].into_iter().collect();
        match drain_replacement(state, controller, &excluded, distributary_id, now_ns) {
            Some(replacement) => moves.push(Command::MoveDistributaryLease {
                distributary: distributary.name.clone(),
                new_owner: replacement,
                expected_epoch: Some(lease.epoch),
            }),
            None => unplannable.push(format!(
                "Distributary {} lease has no eligible replacement Node for {node}",
                distributary.name
            )),
        }
    }
    StorageDrainPlan {
        storage_pool: storage_eligibility_report(
            state,
            controller.eligible_storage_nodes.as_ref(),
            &controller.capacity_policy,
            now_ns,
        ),
        node: node.clone(),
        draining: state.draining_storage_nodes.contains(node),
        moves,
        unplannable,
        ready_to_retire: storage_node_in_use(state, node).is_none(),
    }
}

fn persist_state(path: &Path, state: &CatalogState) -> Result<(), ControlError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    // Temp + fsync + rename: a crash or a concurrent backup copy must
    // always observe a complete catalog, never a torn serialization.
    let bytes = serde_json::to_vec_pretty(state)?;
    let temporary = path.with_extension("tmp");
    let mut file = std::fs::File::create(&temporary)?;
    std::io::Write::write_all(&mut file, &bytes)?;
    file.sync_all()?;
    fs::rename(temporary, path)?;
    Ok(())
}

pub fn derive_command_request_id(batch_request_id: Uuid, command_index: usize) -> Uuid {
    let mut hasher = blake3::Hasher::new();
    hasher.update(batch_request_id.as_bytes());
    hasher.update(&(command_index as u64).to_be_bytes());
    uuid_from_hash(hasher.finalize())
}

/// Deterministic request identity for a committed-frontier proof
/// report: retries of the same (Range, position, digest) dedup through
/// `applied_requests`, and each newer position is a fresh command.
pub fn commit_proof_request_id(proof: &RangeCommitProof) -> Uuid {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"range-commit-proof");
    hasher.update(proof.range_id.as_uuid().as_bytes());
    hasher.update(&proof.position.value().to_be_bytes());
    hasher.update(&proof.frame_digest);
    uuid_from_hash(hasher.finalize())
}

fn derived_resource_id(request_id: Uuid, resource_type: &str) -> Uuid {
    let mut hasher = blake3::Hasher::new();
    hasher.update(request_id.as_bytes());
    hasher.update(resource_type.as_bytes());
    uuid_from_hash(hasher.finalize())
}

fn uuid_from_hash(hash: blake3::Hash) -> Uuid {
    let mut bytes: [u8; 16] = hash.as_bytes()[..16]
        .try_into()
        .expect("UUID digest length");
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

pub fn unix_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .try_into()
        .unwrap_or(i64::MAX)
}

fn command_label(command: &Command) -> String {
    match command {
        Command::CreateDomain { .. } => "CREATE DOMAIN",
        Command::CreateSpace { .. } => "CREATE SPACE",
        Command::CreateFeed { .. } => "CREATE FEED",
        Command::EraseFeed { .. } => "ERASE FEED",
        Command::DefineStateStore { .. } => "DEFINE STATE STORE",
        Command::DefineIndex { .. } => "DEFINE INDEX",
        Command::RebuildIndex { .. } => "REBUILD INDEX",
        Command::GetIndex { .. } => "GET INDEX",
        Command::RebuildIndexesInDomain { .. } => "REBUILD INDEXES IN DOMAIN",
        Command::CompleteIndexBuild { .. } => "COMPLETE INDEX BUILD",
        Command::PausePipe { .. } => "PAUSE PIPE",
        Command::ResumePipe { .. } => "RESUME PIPE",
        Command::DefinePipe { .. } => "DEFINE PIPE",
        Command::CreateWriter { .. } => "CREATE WRITER",
        Command::OpenWriterSession { .. } => "OPEN WRITER SESSION",
        Command::AllocateWriterSequence { .. } => "ALLOCATE WRITER SEQUENCE",
        Command::AllocateWriterRangeSequence { .. } => "ALLOCATE WRITER RANGE SEQUENCE",
        Command::RepairWriterRangeSequence { .. } => "REPAIR WRITER RANGE SEQUENCE",
        Command::RevokeWriterSession { .. } => "REVOKE WRITER SESSION",
        Command::CreateReader { .. } => "CREATE READER",
        Command::CreateSubscription { .. } => "CREATE SUBSCRIPTION",
        Command::OpenReaderSession { .. } => "OPEN READER SESSION",
        Command::RecordReaderDelivery { .. } => "RECORD READER DELIVERY",
        Command::RecordReaderFrontier { .. } => "RECORD READER FRONTIER",
        Command::AcknowledgeReader { .. } => "ACKNOWLEDGE READER",
        Command::CloseReaderSession { .. } => "CLOSE READER SESSION",
        Command::CreateRole { .. } => "CREATE ROLE",
        Command::Rename { .. } => "RENAME",
        Command::Drop { .. } => "DROP",
        Command::Show { .. } => "SHOW",
        Command::Describe { .. } => "DESCRIBE",
        Command::Grant { effect, .. } => match effect {
            GrantEffect::Allow => "GRANT",
            GrantEffect::Deny => "DENY",
        },
        Command::RevokeGrant { effect, .. } => match effect {
            GrantEffect::Allow => "REVOKE",
            GrantEffect::Deny => "REVOKE DENY",
        },
        Command::IssueApiKey { .. } => "ISSUE API KEY",
        Command::RevokeApiKey { .. } => "REVOKE API KEY",
        Command::ExplainAccess { .. } => "EXPLAIN ACCESS",
        Command::SeekReader { .. } => "SEEK READER",
        Command::SeekSubscription { .. } => "SEEK SUBSCRIPTION",
        Command::InspectPlacement { .. } => "INSPECT PLACEMENT",
        Command::TransferActiveRangeOwnership { .. } => "TRANSFER ACTIVE RANGE OWNERSHIP",
        Command::RecoverActiveRangeOwnership { .. } => "RECOVER ACTIVE RANGE OWNERSHIP",
        Command::PrepareActiveRangeSplit { .. } => "PREPARE ACTIVE RANGE SPLIT",
        Command::AbandonActiveRangeSplit { .. } => "ABANDON ACTIVE RANGE SPLIT",
        Command::AbandonActiveRangeMerge { .. } => "ABANDON ACTIVE RANGE MERGE",
        Command::AbandonFollowerMove { .. } => "ABANDON FOLLOWER MOVE",
        Command::AbandonOwnerMove { .. } => "ABANDON OWNER MOVE",
        Command::RecordRangeCommitProof { .. } => "RECORD RANGE COMMIT PROOF",
        Command::RecordActiveRangeSplitCatchUp { .. } => "RECORD ACTIVE RANGE SPLIT CATCH UP",
        Command::ActivateActiveRangeSplit { .. } => "ACTIVATE ACTIVE RANGE SPLIT",
        Command::PrepareActiveRangeMerge { .. } => "PREPARE ACTIVE RANGE MERGE",
        Command::RecordActiveRangeMergeStaging { .. } => "RECORD ACTIVE RANGE MERGE STAGING",
        Command::ActivateActiveRangeMerge { .. } => "ACTIVATE ACTIVE RANGE MERGE",
        Command::ClearSubscriptionCutover { .. } => "CLEAR SUBSCRIPTION CUTOVER",
        Command::PrepareFollowerMove { .. } => "PREPARE FOLLOWER MOVE",
        Command::RecordFollowerMoveCatchUp { .. } => "RECORD FOLLOWER MOVE CATCH UP",
        Command::ActivateFollowerMove { .. } => "ACTIVATE FOLLOWER MOVE",
        Command::AbortFollowerMove { .. } => "ABORT FOLLOWER MOVE",
        Command::PrepareOwnerMove { .. } => "PREPARE OWNER MOVE",
        Command::RecordOwnerMoveCatchUp { .. } => "RECORD OWNER MOVE CATCH UP",
        Command::ActivateOwnerMove { .. } => "ACTIVATE OWNER MOVE",
        Command::AbortOwnerMove { .. } => "ABORT OWNER MOVE",
        Command::RecoverSubscriptionProgressOwner { .. } => "RECOVER SUBSCRIPTION PROGRESS OWNER",
        Command::MoveSubscriptionProgressReplica { .. } => "MOVE SUBSCRIPTION PROGRESS REPLICA",
        Command::RecoverStateStoreOwner { .. } => "RECOVER STATE STORE OWNER",
        Command::MovePipeComputeLease { .. } => "MOVE PIPE COMPUTE LEASE",
        Command::DefineRiverbedLink { .. } => "DEFINE RIVERBED LINK",
        Command::DefineDistributary { .. } => "DEFINE DISTRIBUTARY",
        Command::MoveDistributaryLease { .. } => "MOVE DISTRIBUTARY LEASE",
        Command::ReportDistributaryStatus { .. } => "REPORT DISTRIBUTARY STATUS",
        Command::RegisterStorageNode { .. } => "REGISTER STORAGE NODE",
        Command::RetireStorageNode { .. } => "RETIRE STORAGE NODE",
        Command::DrainStorageNode { .. } => "DRAIN STORAGE NODE",
        Command::UndrainStorageNode { .. } => "UNDRAIN STORAGE NODE",
        Command::InspectStorageDrain { .. } => "INSPECT DRAIN",
        Command::ReportStorageNodeCapacity { .. } => "REPORT STORAGE NODE CAPACITY",
        Command::SetHistoryPolicy { .. } => "SET HISTORY POLICY",
        Command::ClearHistoryPolicy { .. } => "DROP HISTORY POLICY",
        Command::SetLegalHold { .. } => "SET LEGAL HOLD",
        Command::DropLegalHold { .. } => "DROP LEGAL HOLD",
        Command::SetSchema { .. } => "SET SCHEMA",
        Command::ClearSchema { .. } => "DROP SCHEMA",
    }
    .to_owned()
}

fn resource_name(kind: &ResourceKind) -> &'static str {
    match kind {
        ResourceKind::Domain | ResourceKind::Space => "Domain",
        ResourceKind::Feed => "Feed",
        ResourceKind::Writer => "Writer",
        ResourceKind::Reader => "Reader",
        ResourceKind::Subscription => "Subscription",
        ResourceKind::Pipe => "Pipe",
        ResourceKind::Index => "Index",
        ResourceKind::StateStore => "StateStore",
        ResourceKind::Role => "Role",
        ResourceKind::RiverbedLink => "Riverbed Link",
        ResourceKind::Distributary => "Distributary",
    }
}

fn show_name(kind: &ShowKind) -> &'static str {
    match kind {
        ShowKind::Domains | ShowKind::Spaces => "Domains",
        ShowKind::Feeds => "Feeds",
        ShowKind::Writers => "Writers",
        ShowKind::Readers => "Readers",
        ShowKind::Subscriptions => "Subscriptions",
        ShowKind::Pipes => "Pipes",
        ShowKind::Indexes => "Indexes",
        ShowKind::RiverbedLinks => "Riverbed Links",
        ShowKind::Distributaries => "Distributaries",
        ShowKind::StateStores => "StateStores",
        ShowKind::Roles => "Roles",
        ShowKind::Grants => "Grants",
        ShowKind::ApiKeys => "API Keys",
        ShowKind::StorageNodes => "Storage Nodes",
        ShowKind::HistoryPolicies => "History Policies",
        ShowKind::LegalHolds => "Legal Holds",
        ShowKind::Schemas => "Schemas",
        ShowKind::AuditEvents => "Audit Events",
        ShowKind::Compatibility => "Compatibility",
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::storage::FileLogStore;

    fn new_controller(directory: &TempDir) -> ControlController {
        let store: Arc<dyn LogStore> =
            Arc::new(FileLogStore::open(directory.path().join("data")).unwrap());
        ControlController::open_with_storage_nodes(
            directory.path().join("catalog.json"),
            store,
            ["storage-1", "storage-2", "storage-3"]
                .into_iter()
                .map(|node| StorageNodeId::try_new(node).unwrap())
                .collect(),
        )
        .unwrap()
    }

    #[test]
    fn parser_supports_resources_grants_and_quoted_cursors() {
        let commands = parse_wcl(
            "CREATE SPACE orders; CREATE FEED orders.created; CREATE WRITER checkout TO orders.created; \
             CREATE READER audit FROM orders.created START AT CURSOR 'abc;123'; CREATE ROLE analytics; \
             GRANT READ, WRITE ON NAMESPACE orders.* TO ROLE analytics; \
             INSPECT PLACEMENT FOR FEED orders.created; \
             TRANSFER ACTIVE RANGE OWNERSHIP FOR FEED orders.created TO storage-2;",
        )
        .unwrap();
        assert_eq!(commands.len(), 8);
        assert!(
            matches!(&commands[3], Command::CreateReader { start: ReaderStart::Cursor(cursor), .. } if cursor == "abc;123")
        );
        assert!(matches!(&commands[5], Command::Grant { actions, .. } if actions.len() == 2));
        assert!(
            matches!(&commands[6], Command::InspectPlacement { feed } if feed == "orders.created")
        );
        assert!(matches!(
            &commands[7],
            Command::TransferActiveRangeOwnership { feed, owner }
                if feed == "orders.created" && owner.as_str() == "storage-2"
        ));
    }

    #[test]
    fn parser_supports_subscription_cursor_and_timestamp_starts() {
        let commands = parse_wcl(
            "CREATE DOMAIN orders; CREATE FEED orders.created;              CREATE SUBSCRIPTION orders.timeshifted FROM orders.created START AT TIMESTAMP 1735689600000000000;              CREATE SUBSCRIPTION orders.replaying FROM orders.created START AT CURSOR 'opaque;cursor';",
        )
        .unwrap();
        assert!(matches!(
            &commands[2],
            Command::CreateSubscription { start: ReaderStart::Timestamp(ns), .. } if *ns == 1735689600000000000
        ));
        assert!(matches!(
            &commands[3],
            Command::CreateSubscription { start: ReaderStart::Cursor(cursor), .. } if cursor == "opaque;cursor"
        ));
    }

    #[test]
    fn domain_wcl_is_accepted_alongside_legacy_space_syntax() {
        let commands = parse_wcl(
            "CREATE DOMAIN orders; SHOW DOMAINS; DESCRIBE DOMAIN orders; RENAME DOMAIN orders TO billing; DROP DOMAIN billing; CREATE SPACE legacy; SHOW SPACES;",
        ).unwrap();
        assert_eq!(commands.len(), 7, "Domain and Space syntax must both parse");
        assert!(matches!(&commands[0], Command::CreateDomain { .. }));
        assert!(matches!(
            &commands[1],
            Command::Show {
                kind: ShowKind::Domains,
                ..
            }
        ));
        assert!(matches!(
            &commands[2],
            Command::Describe {
                kind: ResourceKind::Domain,
                ..
            }
        ));
        assert!(matches!(
            &commands[3],
            Command::Rename {
                kind: ResourceKind::Domain,
                ..
            }
        ));
        assert!(matches!(
            &commands[4],
            Command::Drop {
                kind: ResourceKind::Domain,
                ..
            }
        ));
        assert!(matches!(&commands[5], Command::CreateSpace { .. }));
        assert!(matches!(
            &commands[6],
            Command::Show {
                kind: ShowKind::Spaces,
                ..
            }
        ));
        assert_eq!(
            serde_json::to_value(&commands[0]).unwrap()["command"],
            "create_domain"
        );
        assert_eq!(
            serde_json::to_value(&commands[1]).unwrap()["kind"],
            "domains"
        );
    }

    #[tokio::test]
    async fn domain_aliases_preserve_existing_catalog_identity_and_drop_guards() {
        let directory = TempDir::new().unwrap();
        let controller = new_controller(&directory);
        let created = controller
            .execute("CREATE DOMAIN orders; CREATE SPACE legacy;")
            .await
            .unwrap();
        assert_eq!(created.results[0].statement, "CREATE DOMAIN");
        assert_eq!(created.results[0].message, "created Domain orders");
        let domain_id = created.results[0].data["space_id"].clone();
        let listed = controller
            .execute("SHOW DOMAINS; SHOW SPACES; DESCRIBE DOMAIN orders; DESCRIBE SPACE orders;")
            .await
            .unwrap();
        assert_eq!(listed.results[0].data, listed.results[1].data);
        assert_eq!(listed.results[2].data, listed.results[3].data);
        assert_eq!(listed.results[2].data["space_id"], domain_id);
        controller
            .execute("CREATE FEED orders.created;")
            .await
            .unwrap();
        assert!(controller.execute("DROP DOMAIN orders;").await.is_err());
        drop(controller);
        let reopened = new_controller(&directory);
        let described = reopened.execute("DESCRIBE DOMAIN orders;").await.unwrap();
        assert_eq!(described.results[0].data["space_id"], domain_id);
        reopened
            .execute(
                "DROP FEED orders.created; RENAME DOMAIN orders TO billing; DROP DOMAIN billing;",
            )
            .await
            .unwrap();
        assert!(reopened.execute("DESCRIBE DOMAIN billing;").await.is_err());
        assert!(reopened.execute("DESCRIBE DOMAIN legacy;").await.is_ok());
    }

    #[tokio::test]
    async fn controller_creates_renames_persists_and_explains_access() {
        let directory = TempDir::new().unwrap();
        let controller = new_controller(&directory);
        let result = controller
            .execute(
                "CREATE SPACE orders; CREATE FEED orders.created; CREATE WRITER checkout TO orders.created; \
                 CREATE READER audit FROM orders.created START AT BEGINNING; CREATE ROLE analytics; \
                 GRANT READ ON NAMESPACE orders.* TO ROLE analytics; \
                 EXPLAIN ACCESS FOR ROLE analytics ACTION READ ON FEED orders.created; \
                 RENAME FEED orders.created TO orders.accepted;",
            )
            .await
            .unwrap();
        assert_eq!(result.results.len(), 8);
        assert_eq!(result.results[6].data["allowed"], true);
        let feed_id = result.results[1].data["feed_id"].clone();
        assert_eq!(result.results[7].data["feed_id"], feed_id);

        let reopened = new_controller(&directory);
        let described = reopened
            .execute("DESCRIBE FEED orders.accepted; SHOW WRITERS; SHOW READERS;")
            .await
            .unwrap();
        assert_eq!(described.results[0].data["feed_id"], feed_id);
        assert_eq!(described.results[1].data.as_array().unwrap().len(), 1);
        assert_eq!(described.results[2].data.as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn readers_keep_independent_durable_cursors() {
        let directory = TempDir::new().unwrap();
        let controller = new_controller(&directory);
        controller
            .execute(
                "CREATE SPACE orders; CREATE FEED orders.events; \
                 CREATE READER audit FROM orders.events START AT BEGINNING; \
                 CREATE READER analytics FROM orders.events START AT NOW; \
                 SEEK READER audit TO CURSOR 'cursor-a'; \
                 SEEK READER analytics TO CURSOR 'cursor-b';",
            )
            .await
            .unwrap();
        let result = controller
            .execute("DESCRIBE READER audit; DESCRIBE READER analytics;")
            .await
            .unwrap();
        assert_eq!(result.results[0].data["acknowledged_cursor"], "cursor-a");
        assert_eq!(result.results[1].data["acknowledged_cursor"], "cursor-b");
    }

    #[tokio::test]
    async fn reader_sessions_separate_delivery_acknowledgement_and_resume_after_restart() {
        let directory = TempDir::new().unwrap();
        let controller = new_controller(&directory);
        controller.execute("CREATE SPACE orders; CREATE FEED orders.events; CREATE READER audit FROM orders.events START AT BEGINNING;").await.unwrap();
        let opened = controller
            .execute_commands(vec![Command::OpenReaderSession {
                reader: "audit".to_owned(),
                capacity: 2,
            }])
            .await
            .unwrap();
        assert_eq!(opened.results[0].data["session_epoch"], 1);
        controller
            .execute_commands(vec![Command::RecordReaderDelivery {
                reader: "audit".to_owned(),
                session_epoch: 1,
                cursor: "cursor-2".to_owned(),
                fetch_request_id: None,
                fetch_limit: None,
                fetch_retry_after_ms: None,
            }])
            .await
            .unwrap();
        assert!(controller
            .execute_commands(vec![Command::AcknowledgeReader {
                reader: "audit".to_owned(),
                session_epoch: 1,
                cursor: "cursor-1".to_owned()
            }])
            .await
            .is_err());
        controller
            .execute_commands(vec![Command::AcknowledgeReader {
                reader: "audit".to_owned(),
                session_epoch: 1,
                cursor: "cursor-2".to_owned(),
            }])
            .await
            .unwrap();
        drop(controller);
        let reopened = new_controller(&directory);
        let session = reopened
            .execute_commands(vec![Command::OpenReaderSession {
                reader: "audit".to_owned(),
                capacity: 3,
            }])
            .await
            .unwrap();
        assert_eq!(session.results[0].data["session_epoch"], 2);
        assert_eq!(session.results[0].data["delivered_cursor"], "cursor-2");
        assert_eq!(session.results[0].data["acknowledged_cursor"], "cursor-2");
        assert!(reopened
            .execute_commands(vec![Command::RecordReaderDelivery {
                reader: "audit".to_owned(),
                session_epoch: 1,
                cursor: "cursor-3".to_owned(),
                fetch_request_id: None,
                fetch_limit: None,
                fetch_retry_after_ms: None,
            }])
            .await
            .is_err());
    }

    #[tokio::test]
    async fn reader_frontier_is_internal_bounded_acknowledged_and_restart_safe() {
        let directory = TempDir::new().unwrap();
        let controller = new_controller(&directory);
        controller.execute("CREATE SPACE orders; CREATE FEED orders.events; CREATE READER audit FROM orders.events START AT BEGINNING;").await.unwrap();
        let reader = controller.active_reader_by_name("audit").await.unwrap();
        let assignment = controller
            .active_range_assignment(reader.feed_id)
            .await
            .unwrap();
        controller
            .execute_commands(vec![Command::OpenReaderSession {
                reader: "audit".to_owned(),
                capacity: 10,
            }])
            .await
            .unwrap();
        let positions = BTreeMap::from([(assignment.range_id, "cursor-1".to_owned())]);
        let delivery = Command::RecordReaderFrontier {
            reader: "audit".to_owned(),
            session_epoch: 1,
            cursor: "opaque-reader-progress-1".to_owned(),
            positions: positions.clone(),
            expected_cursor: None,
            fence_delivery: true,
            fetch_request_id: Some(Uuid::from_u128(8_001)),
            fetch_limit: None,
            fetch_retry_after_ms: None,
        };
        let request_id = Uuid::from_u128(7_001);
        assert!(delivery.requires_internal_replica_authority());
        controller
            .execute_commands_with_request_id(vec![delivery.clone()], request_id)
            .await
            .unwrap();
        controller
            .execute_commands_with_request_id(vec![delivery], request_id)
            .await
            .unwrap();
        let frontier = controller
            .active_reader_frontier(reader.reader_id)
            .await
            .unwrap();
        assert!(frontier.acknowledged.is_empty());
        assert_eq!(frontier.delivered, positions);
        assert!(controller
            .execute_commands(vec![Command::AcknowledgeReader {
                reader: "audit".to_owned(),
                session_epoch: 1,
                cursor: "cursor-1".to_owned(),
            }])
            .await
            .is_err());
        controller
            .execute_commands(vec![Command::AcknowledgeReader {
                reader: "audit".to_owned(),
                session_epoch: 1,
                cursor: "opaque-reader-progress-1".to_owned(),
            }])
            .await
            .unwrap();
        drop(controller);
        let reopened = new_controller(&directory);
        let session = reopened
            .execute_commands(vec![Command::OpenReaderSession {
                reader: "audit".to_owned(),
                capacity: 10,
            }])
            .await
            .unwrap();
        assert_eq!(
            session.results[0].data["delivered_cursor"],
            "opaque-reader-progress-1"
        );
        let frontier = reopened
            .active_reader_frontier(reader.reader_id)
            .await
            .unwrap();
        assert_eq!(frontier.acknowledged, positions);
        assert_eq!(frontier.delivered, positions);
        assert!(reopened
            .execute_commands(vec![Command::RecordReaderFrontier {
                reader: "audit".to_owned(),
                session_epoch: 1,
                cursor: "stale".to_owned(),
                positions: positions.clone(),
                expected_cursor: Some("opaque-reader-progress-1".to_owned()),
                fence_delivery: true,
                fetch_request_id: Some(Uuid::from_u128(8_002)),
                fetch_limit: None,
                fetch_retry_after_ms: None,
            }])
            .await
            .is_err());
        assert!(reopened
            .execute_commands(vec![Command::RecordReaderFrontier {
                reader: "audit".to_owned(),
                session_epoch: 2,
                cursor: "invalid".to_owned(),
                positions: BTreeMap::from([(
                    RangeId::from_uuid(Uuid::from_u128(9_999)),
                    "cursor-2".to_owned()
                ),]),
                expected_cursor: Some("opaque-reader-progress-1".to_owned()),
                fence_delivery: true,
                fetch_request_id: Some(Uuid::from_u128(8_003)),
                fetch_limit: None,
                fetch_retry_after_ms: None,
            }])
            .await
            .is_err());
        reopened
            .execute_commands(vec![Command::SeekReader {
                reader: "audit".to_owned(),
                start: ReaderStart::Beginning,
            }])
            .await
            .unwrap();
        assert!(reopened
            .active_reader_frontier(reader.reader_id)
            .await
            .is_none());
    }

    #[tokio::test]
    async fn reader_frontier_persists_replay_evidence_for_duplicate_fetches() {
        let directory = TempDir::new().unwrap();
        let controller = new_controller(&directory);
        controller.execute("CREATE SPACE orders; CREATE FEED orders.events; CREATE READER audit FROM orders.events START AT BEGINNING;").await.unwrap();
        let audit = controller.active_reader_by_name("audit").await.unwrap();
        let range_id = controller
            .active_range_assignment(audit.feed_id)
            .await
            .unwrap()
            .range_id;
        controller
            .execute_commands(vec![Command::OpenReaderSession {
                reader: "audit".to_owned(),
                capacity: 2,
            }])
            .await
            .unwrap();
        let deliver =
            |event: &str, fetch_id: u128, limit: Option<usize>, retry_after_ms: Option<u64>| {
                Command::RecordReaderFrontier {
                    reader: "audit".to_owned(),
                    session_epoch: 1,
                    cursor: format!("rf_{fetch_id}"),
                    positions: BTreeMap::from([(range_id, event.to_owned())]),
                    expected_cursor: None,
                    fence_delivery: false,
                    fetch_request_id: Some(Uuid::from_u128(fetch_id)),
                    fetch_limit: limit,
                    fetch_retry_after_ms: retry_after_ms,
                }
            };
        controller
            .execute_commands(vec![deliver("event-1", 7, Some(64), Some(0))])
            .await
            .unwrap();
        controller
            .execute_commands(vec![deliver("event-2", 8, Some(32), Some(50))])
            .await
            .unwrap();
        let frontier = controller
            .active_reader_frontier(audit.reader_id)
            .await
            .unwrap();
        // The replay evidence is exactly what re-reading must reproduce:
        // the pre-delivery positions, the bound the fetch used, and the
        // advice the page was paced under.
        assert_eq!(frontier.delivered[&range_id], "event-2");
        assert_eq!(frontier.previous[&range_id], "event-1");
        assert_eq!(frontier.last_fetch_limit, 32);
        assert_eq!(frontier.last_fetch_retry_after_ms, 50);
        assert_eq!(frontier.last_fetch_request_id, Some(Uuid::from_u128(8)));

        drop(controller);
        let reopened = new_controller(&directory);
        let frontier = reopened
            .active_reader_frontier(audit.reader_id)
            .await
            .unwrap();
        assert_eq!(frontier.previous[&range_id], "event-1");
        assert_eq!(frontier.last_fetch_limit, 32);
        assert_eq!(frontier.last_fetch_retry_after_ms, 50);
    }

    #[tokio::test]
    async fn single_record_delivery_persists_replay_bracket_and_fences_duplicates() {
        let directory = TempDir::new().unwrap();
        let controller = new_controller(&directory);
        controller.execute("CREATE SPACE orders; CREATE FEED orders.events; CREATE READER audit FROM orders.events START AT BEGINNING;").await.unwrap();
        let audit = controller.active_reader_by_name("audit").await.unwrap();
        controller
            .execute_commands(vec![Command::OpenReaderSession {
                reader: "audit".to_owned(),
                capacity: 64,
            }])
            .await
            .unwrap();
        let deliver = |cursor: &str, fetch_id: u128| Command::RecordReaderDelivery {
            reader: "audit".to_owned(),
            session_epoch: 1,
            cursor: cursor.to_owned(),
            fetch_request_id: Some(Uuid::from_u128(fetch_id)),
            fetch_limit: Some(32),
            fetch_retry_after_ms: Some(50),
        };
        controller
            .execute_commands(vec![deliver("cursor-32", 7)])
            .await
            .unwrap();
        // The committed delivery carries its replay bracket: the Cursor the
        // page started from and the bound it was read under.
        let replay = controller
            .active_reader_by_name("audit")
            .await
            .unwrap()
            .last_delivery
            .unwrap();
        assert_eq!(replay.request_id, Uuid::from_u128(7));
        assert_eq!(replay.previous_cursor, None);
        assert_eq!(replay.limit, 32);
        assert_eq!(replay.retry_after_ms, 50);
        // A duplicate request ID is fenced at apply instead of advancing
        // delivered past a page the client may never have seen.
        assert!(controller
            .execute_commands(vec![deliver("cursor-64", 7)])
            .await
            .is_err());
        // A fresh fetch under a new request ID advances and re-brackets.
        controller
            .execute_commands(vec![deliver("cursor-64", 8)])
            .await
            .unwrap();
        let replay = controller
            .active_reader_by_name("audit")
            .await
            .unwrap()
            .last_delivery
            .unwrap();
        assert_eq!(replay.previous_cursor.as_deref(), Some("cursor-32"));

        drop(controller);
        let reopened = new_controller(&directory);
        let replay = reopened
            .active_reader_by_name("audit")
            .await
            .unwrap()
            .last_delivery
            .unwrap();
        assert_eq!(replay.request_id, Uuid::from_u128(8));
        assert_eq!(replay.previous_cursor.as_deref(), Some("cursor-32"));

        reopened
            .execute_commands(vec![Command::OpenReaderSession {
                reader: "audit".to_owned(),
                capacity: 8,
            }])
            .await
            .unwrap();
        assert!(reopened
            .active_reader_by_name("audit")
            .await
            .unwrap()
            .last_delivery
            .is_none());
        let _ = audit;
    }

    #[tokio::test]
    async fn independent_reader_frontiers_fence_concurrent_and_repeated_fetches() {
        let directory = TempDir::new().unwrap();
        let controller = new_controller(&directory);
        controller.execute("CREATE SPACE orders; CREATE FEED orders.events; CREATE READER audit FROM orders.events START AT BEGINNING; CREATE READER metrics FROM orders.events START AT BEGINNING;").await.unwrap();
        let audit = controller.active_reader_by_name("audit").await.unwrap();
        let metrics = controller.active_reader_by_name("metrics").await.unwrap();
        let range_id = controller
            .active_range_assignment(audit.feed_id)
            .await
            .unwrap()
            .range_id;
        for reader in ["audit", "metrics"] {
            controller
                .execute_commands(vec![Command::OpenReaderSession {
                    reader: reader.to_owned(),
                    capacity: 2,
                }])
                .await
                .unwrap();
        }
        let deliver =
            |reader: &str,
             token: &str,
             event: &str,
             fetch_id: u128,
             expected_cursor: Option<&str>| Command::RecordReaderFrontier {
                reader: reader.to_owned(),
                session_epoch: 1,
                cursor: token.to_owned(),
                positions: BTreeMap::from([(range_id, event.to_owned())]),
                expected_cursor: expected_cursor.map(str::to_owned),
                fence_delivery: true,
                fetch_request_id: Some(Uuid::from_u128(fetch_id)),
                fetch_limit: None,
                fetch_retry_after_ms: None,
            };
        controller
            .execute_commands(vec![deliver("audit", "rf1_a", "event-a", 1, None)])
            .await
            .unwrap();
        controller
            .execute_commands(vec![deliver("metrics", "rf1_m", "event-m", 2, None)])
            .await
            .unwrap();
        assert!(controller
            .execute_commands(vec![deliver("audit", "rf1_stale", "event-a2", 3, None)])
            .await
            .is_err());
        assert!(controller
            .execute_commands(vec![deliver(
                "audit",
                "rf1_repeat",
                "event-a2",
                1,
                Some("rf1_a")
            )])
            .await
            .is_err());
        assert_eq!(
            controller
                .active_reader_frontier(audit.reader_id)
                .await
                .unwrap()
                .delivered[&range_id],
            "event-a"
        );
        assert_eq!(
            controller
                .active_reader_frontier(metrics.reader_id)
                .await
                .unwrap()
                .delivered[&range_id],
            "event-m"
        );
        controller
            .execute_commands(vec![Command::AcknowledgeReader {
                reader: "audit".to_owned(),
                session_epoch: 1,
                cursor: "rf1_a".to_owned(),
            }])
            .await
            .unwrap();
        drop(controller);
        let reopened = new_controller(&directory);
        reopened
            .execute_commands(vec![Command::OpenReaderSession {
                reader: "audit".to_owned(),
                capacity: 2,
            }])
            .await
            .unwrap();
        reopened
            .execute_commands(vec![Command::OpenReaderSession {
                reader: "metrics".to_owned(),
                capacity: 2,
            }])
            .await
            .unwrap();
        assert_eq!(
            reopened
                .active_reader_frontier(audit.reader_id)
                .await
                .unwrap()
                .delivered[&range_id],
            "event-a"
        );
        assert!(reopened
            .active_reader_frontier(metrics.reader_id)
            .await
            .unwrap()
            .delivered
            .is_empty());
    }

    #[tokio::test]
    async fn writer_sessions_persist_allocate_idempotently_and_fence_stale_epochs() {
        let directory = TempDir::new().unwrap();
        let controller = new_controller(&directory);
        controller
            .execute(
                "CREATE SPACE orders; CREATE FEED orders.events; CREATE WRITER checkout TO orders.events; OPEN WRITER SESSION checkout;",
            )
            .await
            .unwrap();
        let request_id = Uuid::from_u128(8_001);
        let command = vec![Command::AllocateWriterSequence {
            writer: "checkout".to_owned(),
            session_epoch: 1,
        }];
        let first = controller
            .execute_commands_with_request_id(command.clone(), request_id)
            .await
            .unwrap();
        let retry = controller
            .execute_commands_with_request_id(command, request_id)
            .await
            .unwrap();
        assert_eq!(first.results[0].data, retry.results[0].data);
        assert_eq!(first.results[0].data["sequence"], 1);
        drop(controller);

        let reopened = new_controller(&directory);
        let opened = reopened
            .execute("OPEN WRITER SESSION checkout;")
            .await
            .unwrap();
        assert_eq!(opened.results[0].data["session_epoch"], 2);
        assert!(reopened
            .execute("ALLOCATE WRITER SEQUENCE checkout EPOCH 1;")
            .await
            .is_err());
        let allocated = reopened
            .execute("ALLOCATE WRITER SEQUENCE checkout EPOCH 2;")
            .await
            .unwrap();
        assert_eq!(allocated.results[0].data["sequence"], 1);
        reopened
            .execute("REVOKE WRITER SESSION checkout EPOCH 2;")
            .await
            .unwrap();
        assert!(reopened
            .execute("ALLOCATE WRITER SEQUENCE checkout EPOCH 2;")
            .await
            .is_err());
    }

    #[tokio::test]
    async fn repair_writer_range_sequence_only_rewinds_burned_counters() {
        let directory = TempDir::new().unwrap();
        let controller = new_controller(&directory);
        controller
            .execute(
                "CREATE SPACE orders; CREATE FEED orders.events; CREATE WRITER checkout TO orders.events; OPEN WRITER SESSION checkout;",
            )
            .await
            .unwrap();
        let range_id = RangeId::from_uuid(Uuid::from_u128(4_201));
        for sequence in [1_u64, 2] {
            let allocated = controller
                .execute_commands_with_request_id(
                    vec![Command::AllocateWriterRangeSequence {
                        writer: "checkout".to_owned(),
                        session_epoch: 1,
                        range_id,
                    }],
                    Uuid::from_u128(9_000 + sequence as u128),
                )
                .await
                .unwrap();
            assert_eq!(allocated.results[0].data["sequence"], sequence);
        }

        // A repair at-or-below the counter rewinds it to the boundary;
        // the next allocation reuses the previously burned number.
        let repaired = controller
            .execute_commands_with_request_id(
                vec![Command::RepairWriterRangeSequence {
                    writer: "checkout".to_owned(),
                    session_epoch: 1,
                    range_id,
                    next_sequence: 2,
                }],
                Uuid::from_u128(9_010),
            )
            .await
            .unwrap();
        assert_eq!(repaired.results[0].data["next_sequence"], 2);
        assert_eq!(repaired.results[0].data["rewound"], true);
        let allocated = controller
            .execute_commands_with_request_id(
                vec![Command::AllocateWriterRangeSequence {
                    writer: "checkout".to_owned(),
                    session_epoch: 1,
                    range_id,
                }],
                Uuid::from_u128(9_011),
            )
            .await
            .unwrap();
        assert_eq!(allocated.results[0].data["sequence"], 2);

        // A repair above the counter must never skip forward.
        let repaired = controller
            .execute_commands_with_request_id(
                vec![Command::RepairWriterRangeSequence {
                    writer: "checkout".to_owned(),
                    session_epoch: 1,
                    range_id,
                    next_sequence: 40,
                }],
                Uuid::from_u128(9_012),
            )
            .await
            .unwrap();
        assert_eq!(repaired.results[0].data["rewound"], false);
        let allocated = controller
            .execute_commands_with_request_id(
                vec![Command::AllocateWriterRangeSequence {
                    writer: "checkout".to_owned(),
                    session_epoch: 1,
                    range_id,
                }],
                Uuid::from_u128(9_013),
            )
            .await
            .unwrap();
        assert_eq!(allocated.results[0].data["sequence"], 3);

        // An unknown Range allocation entry is never created.
        let foreign = RangeId::from_uuid(Uuid::from_u128(4_299));
        let repaired = controller
            .execute_commands_with_request_id(
                vec![Command::RepairWriterRangeSequence {
                    writer: "checkout".to_owned(),
                    session_epoch: 1,
                    range_id: foreign,
                    next_sequence: 1,
                }],
                Uuid::from_u128(9_014),
            )
            .await
            .unwrap();
        assert_eq!(repaired.results[0].data["rewound"], false);
        assert_eq!(
            repaired.results[0].data["next_sequence"],
            serde_json::Value::Null
        );

        // A stale session epoch fences the repair like any allocation.
        assert!(controller
            .execute_commands_with_request_id(
                vec![Command::RepairWriterRangeSequence {
                    writer: "checkout".to_owned(),
                    session_epoch: 99,
                    range_id,
                    next_sequence: 2,
                }],
                Uuid::from_u128(9_015),
            )
            .await
            .is_err());
    }

    #[tokio::test]
    async fn replicated_commands_produce_identical_ids_and_timestamps() {
        let left_directory = TempDir::new().unwrap();
        let right_directory = TempDir::new().unwrap();
        let left = new_controller(&left_directory);
        let right = new_controller(&right_directory);
        let request = ReplicatedCommand {
            request_id: Uuid::new_v4(),
            issued_at_ns: 1_700_000_000_123_456_789,
            command: Command::CreateSpace {
                name: "orders".to_owned(),
            },
            actor: AuditActor::System,
            fixed_active_range: None,
            fixed_subscription_progress: None,
            fixed_state_store: None,
        };
        let left_result = left.apply_replicated(request.clone()).await;
        let right_result = right.apply_replicated(request.clone()).await;
        assert_eq!(left_result, right_result);
        assert_eq!(
            left_result.result.as_ref().unwrap().data["created_at_ns"],
            1_700_000_000_123_456_789_i64
        );
        let repeated = left.apply_replicated(request.clone()).await;
        assert_eq!(repeated, left_result);
        assert_eq!(left.revision().await, 1);
    }

    #[tokio::test]
    async fn encrypted_feed_dek_derives_identically_across_replicas() {
        let left_directory = TempDir::new().unwrap();
        let right_directory = TempDir::new().unwrap();
        let left = new_controller(&left_directory);
        let right = new_controller(&right_directory);
        let request_id = Uuid::new_v4();
        // The Domain must exist on both replicas before the Feed applies.
        let space = ReplicatedCommand {
            request_id: Uuid::new_v4(),
            issued_at_ns: 1_700_000_000_000_000_000,
            command: Command::CreateSpace {
                name: "orders".to_owned(),
            },
            actor: AuditActor::System,
            fixed_active_range: None,
            fixed_subscription_progress: None,
            fixed_state_store: None,
        };
        left.apply_replicated(space.clone()).await;
        right.apply_replicated(space).await;
        let create = ReplicatedCommand {
            request_id,
            issued_at_ns: 1_700_000_000_100_000_000,
            command: Command::CreateFeed {
                name: "orders.events".to_owned(),
                encrypted: true,
            },
            actor: AuditActor::System,
            fixed_active_range: Some(FixedActiveRangePlacement {
                owner: StorageNodeId::try_new("storage-1").unwrap(),
                replicas: [
                    StorageNodeId::try_new("storage-1").unwrap(),
                    StorageNodeId::try_new("storage-2").unwrap(),
                    StorageNodeId::try_new("storage-3").unwrap(),
                ],
            }),
            fixed_subscription_progress: None,
            fixed_state_store: None,
        };
        let left_result = left.apply_replicated(create.clone()).await;
        let right_result = right.apply_replicated(create).await;
        assert!(left_result.error.is_none(), "{:?}", left_result.error);
        assert_eq!(left_result, right_result);

        // Every replica applying the same command must hold identical key
        // material — ciphertext is only byte-identical when the DEK is.
        let feed_id = derived_resource_id(request_id, "feed");
        let left_key = left.feed_key_state(feed_id).await;
        let right_key = right.feed_key_state(feed_id).await;
        match (left_key, right_key) {
            (Some(FeedKeyState::Active(left)), Some(FeedKeyState::Active(right))) => {
                assert_eq!(left, right);
            }
            other => panic!("expected Active DEKs on both replicas, got {other:?}"),
        }
    }

    async fn show_audit(control: &ControlController, limit: Option<usize>) -> Vec<AuditEvent> {
        let mut execution = control
            .execute_commands(vec![Command::Show {
                kind: ShowKind::AuditEvents,
                limit,
            }])
            .await
            .unwrap();
        serde_json::from_value(execution.results.remove(0).data).unwrap()
    }

    #[tokio::test]
    async fn audit_events_record_mutations_and_refusals_with_actor() {
        let directory = TempDir::new().unwrap();
        let control = new_controller(&directory);
        let actor = AuditActor::ApiKey {
            name: "ci-bot".to_owned(),
        };
        control
            .execute_commands_as(
                actor.clone(),
                vec![
                    Command::CreateDomain {
                        name: "orders".to_owned(),
                    },
                    Command::CreateFeed {
                        name: "orders.events".to_owned(),
                        encrypted: false,
                    },
                ],
                Uuid::new_v4(),
            )
            .await
            .unwrap();
        // A refused attempt commits its verdict and audits the try.
        control
            .execute_commands_as(
                actor.clone(),
                vec![Command::CreateDomain {
                    name: "orders".to_owned(),
                }],
                Uuid::new_v4(),
            )
            .await
            .unwrap_err();

        let events = show_audit(&control, None).await;
        assert_eq!(events.len(), 3);
        // Newest first: the refusal, then the two commits.
        let refused = &events[0];
        assert_eq!(refused.action, "CREATE DOMAIN");
        assert_eq!(refused.target.as_deref(), Some("orders"));
        match &refused.outcome {
            AuditOutcome::Refused { error } => assert!(!error.is_empty()),
            other => panic!("expected refusal, got {other:?}"),
        }
        for (event, action, target) in [
            (&events[1], "CREATE FEED", "orders.events"),
            (&events[2], "CREATE DOMAIN", "orders"),
        ] {
            assert_eq!(event.action, action);
            assert_eq!(event.target.as_deref(), Some(target));
            assert_eq!(event.actor, actor);
            assert_eq!(event.outcome, AuditOutcome::Committed);
            assert!(event.at_ns > 0);
        }
        // seq is the strict ordering even when revisions repeat (a
        // refused apply never bumps the catalog revision).
        assert!(events[0].seq > events[1].seq && events[1].seq > events[2].seq);
        assert!(events[0].revision <= events[2].revision + 1);
    }

    #[tokio::test]
    async fn audit_events_skip_data_plane_and_progress_commands() {
        let directory = TempDir::new().unwrap();
        let control = new_controller(&directory);
        control
            .execute("CREATE DOMAIN orders; CREATE FEED orders.events; CREATE READER orders.audit FROM orders.events;")
            .await
            .unwrap();
        let events_before = show_audit(&control, None).await.len();
        // Session/progress commands churn per-request — refused or
        // committed, none of them is administrative evidence.
        for command in [
            Command::OpenReaderSession {
                reader: "orders.audit".to_owned(),
                capacity: 4,
            },
            Command::CloseReaderSession {
                reader: "orders.audit".to_owned(),
                session_epoch: 1,
            },
            Command::AllocateWriterSequence {
                writer: "missing".to_owned(),
                session_epoch: 1,
            },
        ] {
            // Committed or refused is irrelevant — neither may audit.
            let _ = control.execute_commands(vec![command]).await;
        }
        assert_eq!(show_audit(&control, None).await.len(), events_before);
    }

    #[tokio::test]
    async fn audit_events_default_actor_is_system_and_survive_reopen() {
        let directory = TempDir::new().unwrap();
        let catalog = directory.path().join("catalog.json");
        {
            let control = new_controller(&directory);
            control
                .execute_commands(vec![Command::CreateDomain {
                    name: "orders".to_owned(),
                }])
                .await
                .unwrap();
        }
        let store: Arc<dyn LogStore> =
            Arc::new(FileLogStore::open(directory.path().join("data")).unwrap());
        let reopened = ControlController::open_with_storage_nodes(
            catalog,
            store,
            ["storage-1", "storage-2", "storage-3"]
                .into_iter()
                .map(|node| StorageNodeId::try_new(node).unwrap())
                .collect(),
        )
        .unwrap();
        let events = show_audit(&reopened, None).await;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].actor, AuditActor::System);
        assert_eq!(events[0].action, "CREATE DOMAIN");
    }

    #[tokio::test]
    async fn audit_events_limit_pages_the_ring() {
        let directory = TempDir::new().unwrap();
        let control = new_controller(&directory);
        control
            .execute("CREATE DOMAIN a; CREATE DOMAIN b; CREATE DOMAIN c;")
            .await
            .unwrap();
        let page = show_audit(&control, Some(2)).await;
        assert_eq!(page.len(), 2);
        assert_eq!(page[0].target.as_deref(), Some("c"));
        assert_eq!(page[1].target.as_deref(), Some("b"));
    }

    #[test]
    fn audit_ring_evicts_oldest_and_seq_never_repeats() {
        let mut state = CatalogState::default();
        let event = |seq| AuditEvent {
            seq,
            revision: 1,
            at_ns: 1,
            request_id: Uuid::nil(),
            actor: AuditActor::System,
            action: "CREATE DOMAIN".to_owned(),
            target: None,
            detail: None,
            outcome: AuditOutcome::Committed,
        };
        for _ in 0..AUDIT_EVENT_LIMIT + 10 {
            state.push_audit_event(event(0));
        }
        assert_eq!(state.audit_events.len(), AUDIT_EVENT_LIMIT);
        assert_eq!(
            state.audit_events.front().unwrap().seq,
            (AUDIT_EVENT_LIMIT + 10 - AUDIT_EVENT_LIMIT + 1) as u64
        );
        assert_eq!(
            state.audit_events.back().unwrap().seq,
            (AUDIT_EVENT_LIMIT + 10) as u64
        );
    }

    #[test]
    fn replicated_command_decodes_without_actor() {
        // Persisted pre-upgrade entries carry no actor: they must keep
        // replaying, auditing themselves as Unknown rather than failing.
        let decoded: ReplicatedCommand = serde_json::from_value(json!({
            "request_id": Uuid::nil(),
            "issued_at_ns": 1_700_000_000_i64,
            "command": { "command": "create_domain", "name": "orders" },
        }))
        .unwrap();
        assert_eq!(decoded.actor, AuditActor::Unknown);
    }

    #[test]
    fn show_audit_events_parses_with_and_without_limit() {
        let commands = parse_wcl("SHOW AUDIT EVENTS").unwrap();
        assert!(matches!(
            &commands[0],
            Command::Show {
                kind: ShowKind::AuditEvents,
                limit: None
            }
        ));
        let commands = parse_wcl("SHOW AUDIT EVENTS LIMIT 7").unwrap();
        assert!(matches!(
            &commands[0],
            Command::Show {
                kind: ShowKind::AuditEvents,
                limit: Some(7)
            }
        ));
        assert!(parse_wcl("SHOW AUDIT EVENTS TAIL 7").is_err());
        assert!(parse_wcl("SHOW AUDIT EVENTS LIMIT nope").is_err());
    }

    #[tokio::test]
    async fn state_store_sources_are_scoped_idempotent_and_survive_snapshot() {
        let directory = TempDir::new().unwrap();
        let follower_directory = TempDir::new().unwrap();
        let leader = new_controller(&directory);
        leader.execute("CREATE SPACE accounts; CREATE FEED accounts.users; CREATE SPACE other; CREATE FEED other.users;").await.unwrap();
        let request_id = Uuid::from_u128(11_223);
        let command = vec![Command::DefineStateStore {
            name: "accounts.profiles".to_owned(),
            source: StateStoreSourceRequest::Feed {
                feed: "accounts.users".to_owned(),
            },
            named_keys: vec![],
        }];
        let first = leader
            .execute_commands_with_request_id(command.clone(), request_id)
            .await
            .unwrap();
        let repeated = leader
            .execute_commands_with_request_id(command, request_id)
            .await
            .unwrap();
        assert_eq!(first.results[0].data, repeated.results[0].data);
        assert_eq!(first.results[0].data["stage"], "declared");
        assert_eq!(first.results[0].data["source"]["kind"], "feed");
        assert!(leader
            .execute_commands(vec![Command::DefineStateStore {
                name: "accounts.profiles".to_owned(),
                source: StateStoreSourceRequest::Manual,
                named_keys: vec![],
            }])
            .await
            .is_err());
        let feed_id = leader
            .active_feed_by_name("accounts.users")
            .await
            .unwrap()
            .feed_id;
        assert_eq!(
            first.results[0].data["source"]["feed_id"],
            feed_id.to_string()
        );
        assert!(leader
            .execute_commands(vec![Command::DefineStateStore {
                name: "accounts.manual".to_owned(),
                source: StateStoreSourceRequest::Manual,
                named_keys: vec![],
            }])
            .await
            .is_ok());
        assert!(leader
            .execute_commands(vec![Command::DefineStateStore {
                name: "accounts.invalid".to_owned(),
                source: StateStoreSourceRequest::Feed {
                    feed: "other.users".to_owned()
                },
                named_keys: vec![],
            }])
            .await
            .is_err());
        assert!(leader
            .execute_commands(vec![Command::DefineStateStore {
                name: "accounts.unknown".to_owned(),
                source: StateStoreSourceRequest::Feed {
                    feed: "accounts.missing".to_owned()
                },
                named_keys: vec![],
            }])
            .await
            .is_err());
        let follower = new_controller(&follower_directory);
        follower
            .install_snapshot_bytes(&leader.snapshot_bytes().await.unwrap())
            .await
            .unwrap();
        assert_eq!(follower.state.lock().await.state_stores.len(), 2);
        assert_eq!(
            follower
                .declared_state_store_by_name("accounts.profiles")
                .await
                .unwrap()
                .source,
            StateStoreSource::Feed { feed_id },
        );
        // RF3 mutation-journal placement travels with the snapshot.
        let profiles = follower
            .declared_state_store_by_name("accounts.profiles")
            .await
            .unwrap();
        let assignment = follower
            .active_state_store_assignment_by_id(profiles.store_id)
            .await
            .expect("declared StateStore carries an RF3 placement");
        assert_eq!(assignment.ownership_epoch, 1);
        assert!(assignment.replicas.iter().count() >= 3);
        drop(leader);
        let reopened = new_controller(&directory);
        assert_eq!(
            reopened
                .declared_state_store_by_name("accounts.manual")
                .await
                .unwrap()
                .source,
            StateStoreSource::Manual,
        );
    }

    #[tokio::test]
    async fn state_store_named_keys_validate_and_persist() {
        let directory = TempDir::new().unwrap();
        let leader = new_controller(&directory);
        leader
            .execute("CREATE SPACE accounts; CREATE FEED accounts.users;")
            .await
            .unwrap();
        let named_keys = vec![
            NamedStateStoreKey {
                name: "passport".to_owned(),
                unique: true,
                extractor: None,
            },
            NamedStateStoreKey {
                name: "department".to_owned(),
                unique: false,
                extractor: None,
            },
        ];
        leader
            .execute_commands(vec![Command::DefineStateStore {
                name: "accounts.profiles".to_owned(),
                source: StateStoreSourceRequest::Manual,
                named_keys: named_keys.clone(),
            }])
            .await
            .unwrap();
        let store = leader
            .declared_state_store_by_name("accounts.profiles")
            .await
            .unwrap();
        assert_eq!(store.named_keys, named_keys);
        // Named keys require a manual source until a declared projection exists.
        assert!(leader
            .execute_commands(vec![Command::DefineStateStore {
                name: "accounts.feed_sourced".to_owned(),
                source: StateStoreSourceRequest::Feed {
                    feed: "accounts.users".to_owned(),
                },
                named_keys: named_keys.clone(),
            }])
            .await
            .is_err());
        // Duplicate key names are refused.
        assert!(leader
            .execute_commands(vec![Command::DefineStateStore {
                name: "accounts.dup".to_owned(),
                source: StateStoreSourceRequest::Manual,
                named_keys: vec![
                    NamedStateStoreKey {
                        name: "same".to_owned(),
                        unique: false,
                        extractor: None,
                    },
                    NamedStateStoreKey {
                        name: "same".to_owned(),
                        unique: true,
                        extractor: None,
                    },
                ],
            }])
            .await
            .is_err());
        // The named-key budget is bounded.
        assert!(leader
            .execute_commands(vec![Command::DefineStateStore {
                name: "accounts.toomany".to_owned(),
                source: StateStoreSourceRequest::Manual,
                named_keys: (0..=MAX_STATE_STORE_NAMED_KEYS)
                    .map(|i| NamedStateStoreKey {
                        name: format!("key_{i}"),
                        unique: false,
                        extractor: None,
                    })
                    .collect(),
            }])
            .await
            .is_err());
        // Key names are lowercase identifiers; invalid shapes are refused.
        assert!(leader
            .execute_commands(vec![Command::DefineStateStore {
                name: "accounts.badname".to_owned(),
                source: StateStoreSourceRequest::Manual,
                named_keys: vec![NamedStateStoreKey {
                    name: "Not-A-Key".to_owned(),
                    unique: false,
                    extractor: None,
                }],
            }])
            .await
            .is_err());
        // Declared keys survive restart.
        drop(leader);
        let reopened = new_controller(&directory);
        assert_eq!(
            reopened
                .declared_state_store_by_name("accounts.profiles")
                .await
                .unwrap()
                .named_keys,
            named_keys
        );
    }

    #[tokio::test]
    async fn wcl_define_statestore_parses_source_and_named_keys() {
        let directory = TempDir::new().unwrap();
        let leader = new_controller(&directory);
        leader
            .execute("CREATE SPACE accounts; CREATE FEED accounts.events;")
            .await
            .unwrap();
        // Manual store with named keys, declared through one WCL statement.
        leader
            .execute("DEFINE STATESTORE accounts.profiles KEYS department, passport UNIQUE;")
            .await
            .unwrap();
        let store = leader
            .declared_state_store_by_name("accounts.profiles")
            .await
            .unwrap();
        assert_eq!(store.source, StateStoreSource::Manual);
        assert_eq!(
            store.named_keys,
            vec![
                NamedStateStoreKey {
                    name: "department".to_owned(),
                    unique: false,
                    extractor: None,
                },
                NamedStateStoreKey {
                    name: "passport".to_owned(),
                    unique: true,
                    extractor: None,
                },
            ]
        );
        // Feed source without keys works; bare KEYS on it is refused —
        // feed-sourced named keys must declare a projection extractor.
        leader
            .execute("DEFINE STATESTORE accounts.tail FROM FEED accounts.events;")
            .await
            .unwrap();
        assert_eq!(
            leader
                .declared_state_store_by_name("accounts.tail")
                .await
                .unwrap()
                .source,
            StateStoreSource::Feed {
                feed_id: leader
                    .active_feed_by_name("accounts.events")
                    .await
                    .map(|feed| feed.feed_id)
                    .unwrap_or_default()
            }
        );
        assert!(leader
            .execute("DEFINE STATESTORE accounts.tail2 FROM FEED accounts.events KEYS dept;")
            .await
            .is_err());
        // `<name> = <extractor>` declares the projection replay applies.
        leader
            .execute(
                "DEFINE STATESTORE accounts.tagged FROM FEED accounts.events                  KEYS email = JSON 'mail', pp = METADATA 'id' UNIQUE;",
            )
            .await
            .unwrap();
        let tagged = leader
            .declared_state_store_by_name("accounts.tagged")
            .await
            .unwrap();
        assert_eq!(
            tagged.named_keys,
            vec![
                NamedStateStoreKey {
                    name: "email".to_owned(),
                    unique: false,
                    extractor: Some(IndexFieldSpec::Projection {
                        extractor: "json".to_owned(),
                        version: 1,
                        field: "mail".to_owned(),
                    }),
                },
                NamedStateStoreKey {
                    name: "pp".to_owned(),
                    unique: true,
                    extractor: Some(IndexFieldSpec::Metadata {
                        name: "id".to_owned(),
                    }),
                },
            ]
        );
        // Extractors on manual stores are refused — mutations carry the
        // typed values themselves.
        assert!(leader
            .execute("DEFINE STATESTORE accounts.wrong KEYS dept = JSON 'x';")
            .await
            .is_err());
        // Malformed spellings are syntax errors, not silent manual stores.
        assert!(leader
            .execute("DEFINE STATESTORE accounts.keys_only KEYS;")
            .await
            .is_err());
    }

    #[tokio::test]
    async fn dropped_state_stores_tombstone_names_and_fence_operations() {
        let directory = TempDir::new().unwrap();
        let leader = new_controller(&directory);
        leader.execute("CREATE SPACE accounts;").await.unwrap();
        leader
            .execute_commands(vec![Command::DefineStateStore {
                name: "accounts.profiles".to_owned(),
                source: StateStoreSourceRequest::Manual,
                named_keys: vec![],
            }])
            .await
            .unwrap();
        let store = leader
            .declared_state_store_by_name("accounts.profiles")
            .await
            .unwrap();
        assert!(leader
            .active_state_store_assignment_by_id(store.store_id)
            .await
            .is_some());

        // Renaming relabels the catalog while store_id stays stable.
        leader
            .execute("RENAME STATESTORE accounts.profiles TO accounts.directory;")
            .await
            .unwrap();
        let shown = leader.execute("SHOW STATESTORES;").await.unwrap();
        assert_eq!(shown.results.len(), 1);
        let stores = shown.results[0]
            .data
            .as_array()
            .cloned()
            .unwrap_or_default();
        assert_eq!(stores.len(), 1, "SHOW STATESTORES lists the live store");
        assert!(leader
            .declared_state_store_by_name("accounts.profiles")
            .await
            .is_none());
        assert_eq!(
            leader
                .declared_state_store_by_name("accounts.directory")
                .await
                .unwrap()
                .store_id,
            store.store_id
        );

        leader
            .execute("DROP STATESTORE accounts.directory;")
            .await
            .unwrap();
        // The placement is gone so mutation/read paths refuse, and the name
        // is tombstoned against recreation.
        assert!(leader
            .active_state_store_assignment_by_id(store.store_id)
            .await
            .is_none());
        assert!(leader
            .declared_state_store_by_name("accounts.directory")
            .await
            .is_none());
        assert!(leader
            .execute_commands(vec![Command::DefineStateStore {
                name: "accounts.directory".to_owned(),
                source: StateStoreSourceRequest::Manual,
                named_keys: vec![],
            }])
            .await
            .is_err());
        // Renaming onto a tombstoned name is refused too.
        leader
            .execute_commands(vec![Command::DefineStateStore {
                name: "accounts.other".to_owned(),
                source: StateStoreSourceRequest::Manual,
                named_keys: vec![],
            }])
            .await
            .unwrap();
        assert!(leader
            .execute("RENAME STATESTORE accounts.other TO accounts.directory;")
            .await
            .is_err());
        // A second drop reports the store as already gone.
        assert!(leader
            .execute("DROP STATESTORE accounts.directory;")
            .await
            .is_err());
        // Dropped stores are excluded from SHOW, like other resources.
        let shown = leader.execute("SHOW STATESTORES;").await.unwrap();
        let stores = shown.results[0]
            .data
            .as_array()
            .cloned()
            .unwrap_or_default();
        assert_eq!(
            stores
                .iter()
                .filter(|v| v["name"] == "accounts.other")
                .count(),
            1,
            "only accounts.other remains"
        );

        // The tombstone survives restart through the catalog snapshot.
        drop(leader);
        let reopened = new_controller(&directory);
        assert!(reopened
            .declared_state_store_by_name("accounts.directory")
            .await
            .is_none());
        assert!(reopened
            .active_state_store_assignment_by_id(store.store_id)
            .await
            .is_none());
        assert!(reopened
            .execute_commands(vec![Command::DefineStateStore {
                name: "accounts.directory".to_owned(),
                source: StateStoreSourceRequest::Manual,
                named_keys: vec![],
            }])
            .await
            .is_err());
    }

    #[tokio::test]
    async fn index_definitions_are_validated_scoped_idempotent_and_survive_snapshot() {
        let directory = TempDir::new().unwrap();
        let follower_directory = TempDir::new().unwrap();
        let leader = new_controller(&directory);
        leader
            .execute("CREATE SPACE accounts; CREATE FEED accounts.orders; CREATE SPACE other; CREATE FEED other.orders;")
            .await
            .unwrap();
        let fields = vec![
            IndexFieldSpec::Key,
            IndexFieldSpec::Metadata {
                name: "tenant".to_owned(),
            },
            IndexFieldSpec::Projection {
                extractor: "json.path".to_owned(),
                version: 1,
                field: "customer.id".to_owned(),
            },
        ];
        let request_id = Uuid::from_u128(99_001);
        let command = vec![Command::DefineIndex {
            name: "accounts.orders.by.customer".to_owned(),
            feed: "accounts.orders".to_owned(),
            fields: fields.clone(),
            unique: false,
            collation: IndexCollation::Binary,
            consistency: IndexConsistency::Eventual,
        }];
        let first = leader
            .execute_commands_with_request_id(command.clone(), request_id)
            .await
            .unwrap();
        let repeated = leader
            .execute_commands_with_request_id(command, request_id)
            .await
            .unwrap();
        assert_eq!(first.results[0].data, repeated.results[0].data);
        assert_eq!(first.results[0].data["stage"], "declared");
        assert_eq!(first.results[0].data["consistency"], "eventual");
        assert_eq!(first.results[0].data["collation"], "binary");
        let feed_id = leader
            .active_feed_by_name("accounts.orders")
            .await
            .unwrap()
            .feed_id;
        assert_eq!(first.results[0].data["feed_id"], feed_id.to_string());
        assert_eq!(first.results[0].data["fields"].as_array().unwrap().len(), 3);

        // UNIQUE defines and persists its arbitration contract: claims
        // resolve by committed order (first-committed-wins) identically on
        // every replica replaying the same committed history.
        let unique = leader
            .execute_commands(vec![Command::DefineIndex {
                name: "accounts.unique.customer".to_owned(),
                feed: "accounts.orders".to_owned(),
                fields: fields.clone(),
                unique: true,
                collation: IndexCollation::Binary,
                consistency: IndexConsistency::Eventual,
            }])
            .await
            .unwrap();
        assert_eq!(unique.results[0].data["unique"], true);
        // Duplicate name, cross-Domain feed, and unknown feed are refused.
        for (name, feed) in [
            ("accounts.orders.by.customer", "accounts.orders"),
            ("accounts.cross.domain", "other.orders"),
            ("accounts.unknown", "accounts.missing"),
        ] {
            assert!(leader
                .execute_commands(vec![Command::DefineIndex {
                    name: name.to_owned(),
                    feed: feed.to_owned(),
                    fields: fields.clone(),
                    unique: false,
                    collation: IndexCollation::Binary,
                    consistency: IndexConsistency::Eventual,
                }])
                .await
                .is_err());
        }
        // Field bounds: 1..=16 entries, non-empty and <=256-byte names.
        for bad in [
            vec![],
            vec![IndexFieldSpec::Key; 17],
            vec![IndexFieldSpec::Metadata {
                name: String::new(),
            }],
            vec![IndexFieldSpec::Projection {
                extractor: String::new(),
                version: 1,
                field: "f".to_owned(),
            }],
        ] {
            assert!(leader
                .execute_commands(vec![Command::DefineIndex {
                    name: "accounts.bad".to_owned(),
                    feed: "accounts.orders".to_owned(),
                    fields: bad,
                    unique: false,
                    collation: IndexCollation::Binary,
                    consistency: IndexConsistency::Eventual,
                }])
                .await
                .is_err());
        }

        // SHOW INDEXES and DESCRIBE INDEX expose the persisted definition.
        let shown = leader.execute("SHOW INDEXES;").await.unwrap();
        assert_eq!(shown.results[0].data.as_array().unwrap().len(), 2);
        let described = leader
            .execute("DESCRIBE INDEX accounts.orders.by.customer;")
            .await
            .unwrap();
        assert_eq!(
            described.results[0].data["name"],
            "accounts.orders.by.customer"
        );
        // Minimal typed-command JSON defaults unique/collation/consistency.
        let minimal: Command = serde_json::from_value(serde_json::json!({
            "command": "define_index",
            "name": "accounts.minimal",
            "feed": "accounts.orders",
            "fields": [{"kind": "key"}]
        }))
        .unwrap();
        assert!(matches!(
            minimal,
            Command::DefineIndex {
                unique: false,
                collation: IndexCollation::Binary,
                consistency: IndexConsistency::Eventual,
                ..
            }
        ));

        let follower = new_controller(&follower_directory);
        follower
            .install_snapshot_bytes(&leader.snapshot_bytes().await.unwrap())
            .await
            .unwrap();
        assert_eq!(follower.state.lock().await.indexes.len(), 2);
        assert_eq!(
            follower
                .declared_index_by_name("accounts.orders.by.customer")
                .await
                .unwrap()
                .feed_id,
            feed_id,
        );
        drop(leader);
        let reopened = new_controller(&directory);
        assert_eq!(
            reopened
                .declared_index_by_name("accounts.orders.by.customer")
                .await
                .unwrap()
                .stage,
            IndexBuildStage::Declared,
        );
    }

    #[tokio::test]
    async fn index_rebuild_advances_generations_and_activation_is_atomic() {
        let directory = TempDir::new().unwrap();
        let leader = new_controller(&directory);
        leader
            .execute("CREATE SPACE accounts; CREATE FEED accounts.orders;")
            .await
            .unwrap();
        let fields = vec![IndexFieldSpec::Key];
        for name in ["accounts.orders.by.customer", "accounts.orders.by.tenant"] {
            leader
                .execute_commands(vec![Command::DefineIndex {
                    name: name.to_owned(),
                    feed: "accounts.orders".to_owned(),
                    fields: fields.clone(),
                    unique: false,
                    collation: IndexCollation::Binary,
                    consistency: IndexConsistency::Eventual,
                }])
                .await
                .unwrap();
        }

        // REBUILD INDEX moves Declared -> Building{1}; re-issuing the same
        // rebuild is idempotent and does not advance the generation.
        leader
            .execute("REBUILD INDEX accounts.orders.by.customer;")
            .await
            .unwrap();
        let index = leader
            .declared_index_by_name("accounts.orders.by.customer")
            .await
            .unwrap();
        assert_eq!(index.stage, IndexBuildStage::Building { generation: 1 });
        leader
            .execute("REBUILD INDEX accounts.orders.by.customer;")
            .await
            .unwrap();
        assert_eq!(
            leader
                .declared_index_by_name("accounts.orders.by.customer")
                .await
                .unwrap()
                .stage,
            IndexBuildStage::Building { generation: 1 }
        );

        // Completion must name the in-flight generation; a stale or unknown
        // generation is refused.
        assert!(leader
            .execute_commands(vec![Command::CompleteIndexBuild {
                index_id: index.index_id,
                generation: 9,
                applied_cursor: "cursor-1".to_owned(),
            }])
            .await
            .is_err());
        leader
            .execute_commands(vec![Command::CompleteIndexBuild {
                index_id: index.index_id,
                generation: 1,
                applied_cursor: "cursor-3".to_owned(),
            }])
            .await
            .unwrap();
        let index = leader
            .declared_index_by_name("accounts.orders.by.customer")
            .await
            .unwrap();
        assert_eq!(index.stage, IndexBuildStage::Active { generation: 1 });
        assert_eq!(index.applied_cursor.as_deref(), Some("cursor-3"));

        // Rebuilding an Active index opens the NEXT shadow generation.
        leader
            .execute("REBUILD INDEX accounts.orders.by.customer;")
            .await
            .unwrap();
        assert_eq!(
            leader
                .declared_index_by_name("accounts.orders.by.customer")
                .await
                .unwrap()
                .stage,
            IndexBuildStage::Building { generation: 2 }
        );

        // REBUILD INDEXES IN DOMAIN covers every declared index in the
        // domain and leaves others alone.
        leader
            .execute("REBUILD INDEXES IN DOMAIN accounts;")
            .await
            .unwrap();
        assert_eq!(
            leader
                .declared_index_by_name("accounts.orders.by.tenant")
                .await
                .unwrap()
                .stage,
            IndexBuildStage::Building { generation: 1 }
        );
        // The already-building index keeps its in-flight generation.
        assert_eq!(
            leader
                .declared_index_by_name("accounts.orders.by.customer")
                .await
                .unwrap()
                .stage,
            IndexBuildStage::Building { generation: 2 }
        );
        // Unknown names and missing indexes refuse.
        assert!(leader
            .execute("REBUILD INDEX accounts.missing;")
            .await
            .is_err());
        assert!(leader
            .execute("REBUILD INDEXES IN DOMAIN missing;")
            .await
            .is_err());
    }

    #[test]
    fn rebuild_wcl_parses_index_and_domain_forms() {
        let commands = parse_wcl(
            "REBUILD INDEX orders.events.by.customer; \
             REBUILD INDEXES IN DOMAIN orders; \
             REBUILD INDEXES IN SPACE orders;",
        )
        .unwrap();
        assert_eq!(commands.len(), 3);
        assert!(matches!(
            &commands[0],
            Command::RebuildIndex { name } if name == "orders.events.by.customer"
        ));
        assert!(matches!(
            &commands[1],
            Command::RebuildIndexesInDomain { domain } if domain == "orders"
        ));
        assert!(matches!(
            &commands[2],
            Command::RebuildIndexesInDomain { domain } if domain == "orders"
        ));
        assert!(parse_wcl("REBUILD INDEXES orders;").is_err());
    }

    #[tokio::test]
    async fn pipe_definitions_are_scoped_validated_and_survive_snapshot() {
        let directory = TempDir::new().unwrap();
        let follower_directory = TempDir::new().unwrap();
        let leader = new_controller(&directory);
        leader
            .execute(
                "CREATE SPACE accounts; CREATE FEED accounts.events;                  CREATE FEED accounts.enriched; CREATE SUBSCRIPTION                  accounts.flow FROM accounts.events; CREATE SPACE other;                  CREATE FEED other.enriched; CREATE FEED other.relayed; CREATE SUBSCRIPTION other.flow                  FROM other.enriched;",
            )
            .await
            .unwrap();
        let request_id = Uuid::from_u128(22_331);
        let command = vec![Command::DefinePipe {
            name: "accounts.forward".to_owned(),
            subscription: "accounts.flow".to_owned(),
            output_feed: Some("accounts.enriched".to_owned()),
            output_store: None,
            quarantine_feed: None,
            emit_feed: None,
            operation: Box::new(PipeOperation::Forward),
        }];
        let first = leader
            .execute_commands_with_request_id(command.clone(), request_id)
            .await
            .unwrap();
        let repeated = leader
            .execute_commands_with_request_id(command, request_id)
            .await
            .unwrap();
        assert_eq!(first.results[0].data, repeated.results[0].data);
        assert_eq!(first.results[0].data["stage"], "declared");
        assert_eq!(first.results[0].data["operation"]["operation"], "forward");
        let subscription = leader
            .active_subscription_by_name("accounts.flow")
            .await
            .unwrap();
        let output = leader
            .active_feed_by_name("accounts.enriched")
            .await
            .unwrap();
        assert_eq!(
            first.results[0].data["subscription_id"],
            subscription.subscription_id.to_string()
        );
        assert_eq!(
            first.results[0].data["output_feed_id"],
            output.feed_id.to_string()
        );
        // A same-named Pipe conflicts rather than silently redeclaring.
        assert!(leader
            .execute_commands(vec![Command::DefinePipe {
                name: "accounts.forward".to_owned(),
                subscription: "accounts.flow".to_owned(),
                output_feed: Some("accounts.enriched".to_owned()),
                output_store: None,
                quarantine_feed: None,
                emit_feed: None,
                operation: Box::new(PipeOperation::Forward),
            }])
            .await
            .is_err());
        // Cross-Domain inputs and outputs, missing resources, and a
        // self-feeding loop are all refused.
        for (subscription, output_feed) in [
            ("other.flow", "accounts.enriched"),
            ("accounts.flow", "other.enriched"),
            ("accounts.missing", "accounts.enriched"),
            ("accounts.flow", "accounts.missing"),
            ("accounts.flow", "accounts.events"),
        ] {
            assert!(
                leader
                    .execute_commands(vec![Command::DefinePipe {
                        name: "accounts.invalid".to_owned(),
                        subscription: subscription.to_owned(),
                        output_feed: Some(output_feed.to_owned()),
                        output_store: None,
                        quarantine_feed: None,
                        emit_feed: None,
                        operation: Box::new(PipeOperation::Forward),
                    }])
                    .await
                    .is_err(),
                "{subscription} -> {output_feed} must fail"
            );
        }
        // PAUSE halts driving without losing the declaration; RESUME
        // returns it. Both are idempotent and refuse unknown Pipes.
        assert!(leader
            .execute("PAUSE PIPE accounts.missing;")
            .await
            .is_err());
        assert!(leader
            .execute("RESUME PIPE accounts.missing;")
            .await
            .is_err());
        leader
            .execute("PAUSE PIPE accounts.forward;")
            .await
            .unwrap();
        assert_eq!(
            leader
                .active_pipe_by_name("accounts.forward")
                .await
                .unwrap()
                .stage,
            PipeStage::Paused
        );
        // Idempotent re-pause.
        leader
            .execute("PAUSE PIPE accounts.forward;")
            .await
            .unwrap();
        // The stage shows through DESCRIBE.
        let described = leader
            .execute("DESCRIBE PIPE accounts.forward;")
            .await
            .unwrap();
        assert_eq!(described.results[0].data["stage"], "paused");
        // Idempotent resume restores driving.
        leader
            .execute("RESUME PIPE accounts.forward;")
            .await
            .unwrap();
        leader
            .execute("RESUME PIPE accounts.forward;")
            .await
            .unwrap();
        assert_eq!(
            leader
                .active_pipe_by_name("accounts.forward")
                .await
                .unwrap()
                .stage,
            PipeStage::Declared
        );

        // The consumed Subscription and output Feed cannot drop while the
        // Pipe is active.
        assert!(leader
            .execute("DROP SUBSCRIPTION accounts.flow;")
            .await
            .is_err());
        assert!(leader
            .execute("DROP FEED accounts.enriched;")
            .await
            .is_err());
        leader.execute("DROP PIPE accounts.forward;").await.unwrap();
        leader
            .execute("DROP SUBSCRIPTION accounts.flow; DROP FEED accounts.enriched;")
            .await
            .unwrap();
        // The dropped Pipe name is still reserved, matching sibling rules.
        assert!(leader
            .execute_commands(vec![Command::DefinePipe {
                name: "accounts.forward".to_owned(),
                subscription: "other.flow".to_owned(),
                output_feed: Some("other.enriched".to_owned()),
                output_store: None,
                quarantine_feed: None,
                emit_feed: None,
                operation: Box::new(PipeOperation::Forward),
            }])
            .await
            .is_err());
        leader
            .execute(
                "CREATE PIPE accounts.relay FROM SUBSCRIPTION other.flow TO FEED other.relayed;",
            )
            .await
            .unwrap_err();
        leader
            .execute("CREATE PIPE other.relay FROM SUBSCRIPTION other.flow TO FEED other.relayed;")
            .await
            .unwrap();
        let follower = new_controller(&follower_directory);
        follower
            .install_snapshot_bytes(&leader.snapshot_bytes().await.unwrap())
            .await
            .unwrap();
        let relay = follower.active_pipe_by_name("other.relay").await.unwrap();
        assert_eq!(relay.operation, PipeOperation::Forward);
        assert_eq!(relay.stage, PipeStage::Declared);
        assert_eq!(relay.status, ResourceStatus::Active);
        let other_subscription = follower
            .active_subscription_by_name("other.flow")
            .await
            .unwrap();
        assert_eq!(
            follower
                .pipes_for_subscription(other_subscription.subscription_id)
                .await
                .len(),
            1
        );
        drop(leader);
        let reopened = new_controller(&directory);
        assert!(reopened.active_pipe_by_name("other.relay").await.is_some());
        assert!(reopened
            .active_pipe_by_name("accounts.forward")
            .await
            .is_none());
    }

    #[tokio::test]
    async fn subscription_progress_selection_is_deterministic_for_candidate_order() {
        let first_dir = TempDir::new().unwrap();
        let second_dir = TempDir::new().unwrap();
        let nodes = (0..12)
            .map(|index| StorageNodeId::try_new(format!("storage-{index:02}")).unwrap())
            .collect::<Vec<_>>();
        let make = |directory: &TempDir, candidates: Vec<StorageNodeId>| {
            let store: Arc<dyn LogStore> =
                Arc::new(FileLogStore::open(directory.path().join("data")).unwrap());
            ControlController::open_with_storage_nodes(
                directory.path().join("catalog.json"),
                store,
                candidates,
            )
            .unwrap()
        };
        let first = make(&first_dir, nodes.clone());
        let second = make(&second_dir, nodes.iter().cloned().rev().collect());
        let mut used = BTreeSet::new();
        for index in 0..32 {
            let id = Uuid::from_u128(6000 + index);
            let left_state = first.state.lock().await;
            let left = first
                .select_fixed_subscription_progress(&left_state, id, unix_ns())
                .unwrap();
            drop(left_state);
            let right_state = second.state.lock().await;
            let right = second
                .select_fixed_subscription_progress(&right_state, id, unix_ns())
                .unwrap();
            drop(right_state);
            assert_eq!(left, right);
            assert!(left.replicas.contains(&left.owner));
            used.extend(left.replicas);
        }
        assert!(used.len() >= 8);
    }

    #[tokio::test]
    async fn subscription_delivery_delay_parses_validates_and_persists() {
        let directory = TempDir::new().unwrap();
        let controller = new_controller(&directory);
        controller
            .execute("CREATE SPACE orders; CREATE FEED orders.events;")
            .await
            .unwrap();
        // DELIVERY DELAY parses with and without an explicit start policy.
        let commands =
            parse_wcl("CREATE SUBSCRIPTION orders.d FROM orders.events DELIVERY DELAY 5s;")
                .unwrap();
        match &commands[0] {
            Command::CreateSubscription {
                delivery_delay_ns, ..
            } => assert_eq!(*delivery_delay_ns, Some(5_000_000_000)),
            other => panic!("unexpected command {other:?}"),
        }
        let commands = parse_wcl(
            "CREATE SUBSCRIPTION orders.d2 FROM orders.events START AT NOW DELIVERY DELAY 250ms;",
        )
        .unwrap();
        match &commands[0] {
            Command::CreateSubscription {
                start,
                delivery_delay_ns,
                ..
            } => {
                assert_eq!(*start, ReaderStart::Now);
                assert_eq!(*delivery_delay_ns, Some(250_000_000));
            }
            other => panic!("unexpected command {other:?}"),
        }
        // The declared delay persists on the definition and stays internal
        // to the public describe payload (no placement detail leaks).
        controller
            .execute("CREATE SUBSCRIPTION orders.d FROM orders.events DELIVERY DELAY 5s;")
            .await
            .unwrap();
        let definition = controller
            .active_subscription_by_name("orders.d")
            .await
            .unwrap();
        assert_eq!(definition.delivery_delay_ns, Some(5_000_000_000));
        let described = controller
            .execute("DESCRIBE SUBSCRIPTION orders.d;")
            .await
            .unwrap();
        assert_eq!(
            described.results[0].data["delivery_delay_ns"],
            json!(5_000_000_000_u64)
        );
        // A zero delay is meaningless and refuses at apply time.
        let refused = controller
            .execute("CREATE SUBSCRIPTION orders.z FROM orders.events DELIVERY DELAY 0s;")
            .await;
        assert!(refused.is_err());
        for bad in [
            "CREATE SUBSCRIPTION orders.b FROM orders.events DELIVERY DELAY;",
            "CREATE SUBSCRIPTION orders.b FROM orders.events DELIVERY FOO 5s;",
            "CREATE SUBSCRIPTION orders.b FROM orders.events DELIVERY DELAY 5x;",
            "CREATE SUBSCRIPTION orders.b FROM orders.events DELIVERY DELAY 5s EXTRA;",
            "CREATE SUBSCRIPTION orders.b FROM orders.events START AT NOW DELIVERY DELAY;",
        ] {
            assert!(parse_wcl(bad).is_err(), "{bad} should refuse");
        }
        // SEEK SUBSCRIPTION parses every start target; the catalog still
        // refuses at apply with data-plane guidance because it cannot
        // resolve Active Range positions.
        let commands =
            parse_wcl("SEEK SUBSCRIPTION orders.d TO TIMESTAMP 1700000000000000000;").unwrap();
        match &commands[0] {
            Command::SeekSubscription { name, start } => {
                assert_eq!(name, "orders.d");
                assert_eq!(*start, ReaderStart::Timestamp(1_700_000_000_000_000_000));
            }
            other => panic!("unexpected command {other:?}"),
        }
        for statement in [
            "SEEK SUBSCRIPTION orders.d TO BEGINNING;",
            "SEEK SUBSCRIPTION orders.d TO NOW;",
            "SEEK SUBSCRIPTION orders.d TO CURSOR 'cursor-4';",
        ] {
            assert!(parse_wcl(statement).is_ok(), "{statement} should parse");
        }
        assert!(parse_wcl("SEEK SUBSCRIPTION orders.d;").is_err());
        // The catalog cannot resolve Active Range positions, so it refuses
        // with data-plane guidance rather than guessing.
        let refused = controller
            .execute("SEEK SUBSCRIPTION orders.d TO BEGINNING;")
            .await;
        assert!(refused.is_err());
        // A legacy serialized command without the field decodes with no
        // delay so older journals replay unchanged.
        let legacy: Command = serde_json::from_value(json!({
            "command": "create_subscription",
            "name": "orders.legacy",
            "feed": "orders.events",
            "start": { "kind": "beginning" }
        }))
        .unwrap();
        match legacy {
            Command::CreateSubscription {
                delivery_delay_ns,
                exhausted_policy,
                ..
            } => {
                assert_eq!(delivery_delay_ns, None);
                assert_eq!(exhausted_policy, None);
            }
            other => panic!("unexpected command {other:?}"),
        }
    }

    #[tokio::test]
    async fn subscription_retry_budget_parses_validates_and_persists() {
        let directory = TempDir::new().unwrap();
        let controller = new_controller(&directory);
        controller
            .execute("CREATE SPACE orders; CREATE FEED orders.events;")
            .await
            .unwrap();
        // The budget clause parses bare, with a disposition, and combined
        // with the delivery-delay clause in either clause order.
        let commands =
            parse_wcl("CREATE SUBSCRIPTION orders.r FROM orders.events MAX DELIVERY ATTEMPTS 3;")
                .unwrap();
        match &commands[0] {
            Command::CreateSubscription {
                exhausted_policy, ..
            } => assert_eq!(
                *exhausted_policy,
                Some(SubscriptionExhaustedPolicy {
                    max_delivery_attempts: 3,
                    disposition: SubscriptionExhaustedDisposition::Quarantine,
                })
            ),
            other => panic!("unexpected command {other:?}"),
        }
        let commands = parse_wcl(
            "CREATE SUBSCRIPTION orders.r2 FROM orders.events MAX DELIVERY ATTEMPTS 2 THEN SKIP;",
        )
        .unwrap();
        match &commands[0] {
            Command::CreateSubscription {
                exhausted_policy, ..
            } => assert_eq!(
                *exhausted_policy,
                Some(SubscriptionExhaustedPolicy {
                    max_delivery_attempts: 2,
                    disposition: SubscriptionExhaustedDisposition::Skip,
                })
            ),
            other => panic!("unexpected command {other:?}"),
        }
        let commands = parse_wcl(
            "CREATE SUBSCRIPTION orders.r3 FROM orders.events DELIVERY DELAY 1s \
             MAX DELIVERY ATTEMPTS 5 THEN QUARANTINE;",
        )
        .unwrap();
        match &commands[0] {
            Command::CreateSubscription {
                delivery_delay_ns,
                exhausted_policy,
                ..
            } => {
                assert_eq!(*delivery_delay_ns, Some(1_000_000_000));
                assert_eq!(
                    *exhausted_policy,
                    Some(SubscriptionExhaustedPolicy {
                        max_delivery_attempts: 5,
                        disposition: SubscriptionExhaustedDisposition::Quarantine,
                    })
                );
            }
            other => panic!("unexpected command {other:?}"),
        }
        // The declared policy persists on the definition and describes.
        controller
            .execute(
                "CREATE SUBSCRIPTION orders.r FROM orders.events \
                 MAX DELIVERY ATTEMPTS 3 THEN SKIP;",
            )
            .await
            .unwrap();
        let definition = controller
            .active_subscription_by_name("orders.r")
            .await
            .unwrap();
        assert_eq!(
            definition.exhausted_policy,
            Some(SubscriptionExhaustedPolicy {
                max_delivery_attempts: 3,
                disposition: SubscriptionExhaustedDisposition::Skip,
            })
        );
        let described = controller
            .execute("DESCRIBE SUBSCRIPTION orders.r;")
            .await
            .unwrap();
        assert_eq!(
            described.results[0].data["exhausted_policy"],
            json!({
                "max_delivery_attempts": 3,
                "disposition": "skip"
            })
        );
        for bad in [
            "CREATE SUBSCRIPTION orders.b FROM orders.events MAX DELIVERY ATTEMPTS;",
            "CREATE SUBSCRIPTION orders.b FROM orders.events MAX DELIVERY ATTEMPTS 0;",
            "CREATE SUBSCRIPTION orders.b FROM orders.events MAX DELIVERY ATTEMPTS x;",
            "CREATE SUBSCRIPTION orders.b FROM orders.events MAX DELIVERY ATTEMPTS 2 THEN;",
            "CREATE SUBSCRIPTION orders.b FROM orders.events MAX DELIVERY ATTEMPTS 2 THEN DROP;",
            "CREATE SUBSCRIPTION orders.b FROM orders.events MAX DELIVERY ATTEMPTS 2 EXTRA;",
            "CREATE SUBSCRIPTION orders.b FROM orders.events MAX ATTEMPTS 2;",
        ] {
            assert!(parse_wcl(bad).is_err(), "{bad} should refuse");
        }
        // A zero budget is meaningless and also refuses at apply time for
        // typed commands that bypass WCL.
        let refused = controller
            .execute_commands(vec![Command::CreateSubscription {
                name: "orders.zero".to_owned(),
                feed: Some("orders.events".to_owned()),
                domain: None,
                start: ReaderStart::Beginning,
                delivery_delay_ns: None,
                exhausted_policy: Some(SubscriptionExhaustedPolicy {
                    max_delivery_attempts: 0,
                    disposition: SubscriptionExhaustedDisposition::Skip,
                }),
            }])
            .await;
        assert!(refused.is_err());
    }

    #[tokio::test]
    async fn subscription_definitions_are_scoped_idempotent_and_keep_feeds_unchanged() {
        let directory = TempDir::new().unwrap();
        let controller = new_controller(&directory);
        controller.execute("CREATE SPACE accounts; CREATE FEED accounts.users; CREATE SPACE other; CREATE FEED other.events;").await.unwrap();
        let request_id = Uuid::from_u128(77_001);
        let command = Command::CreateSubscription {
            name: "accounts.billing".to_owned(),
            feed: Some("accounts.users".to_owned()),
            domain: None,
            start: ReaderStart::Beginning,
            delivery_delay_ns: None,
            exhausted_policy: None,
        };
        let first = controller
            .execute_commands_with_request_id(vec![command.clone()], request_id)
            .await
            .unwrap();
        let retry = controller
            .execute_commands_with_request_id(vec![command], request_id)
            .await
            .unwrap();
        assert_eq!(first.results[0].data, retry.results[0].data);
        assert_eq!(first.results[0].data["stage"], "declared");
        let definition = controller
            .active_subscription_by_name("accounts.billing")
            .await
            .unwrap();
        assert_eq!(
            definition.space_id,
            controller
                .state
                .lock()
                .await
                .spaces
                .values()
                .find(|space| space.name == "accounts")
                .unwrap()
                .space_id
        );
        assert_eq!(
            definition.source.feed_id().unwrap(),
            controller
                .active_feed_by_name("accounts.users")
                .await
                .unwrap()
                .feed_id
        );
        let placement = controller
            .active_subscription_progress_assignment_by_id(definition.subscription_id)
            .await
            .unwrap();
        assert_eq!(placement.ownership_epoch, 1);
        assert!(placement.replicas.contains(&placement.owner));
        assert!(first.results[0].data.get("owner").is_none());
        assert!(first.results[0].data.get("replicas").is_none());
        assert!(controller
            .execute_commands(vec![Command::CreateSubscription {
                name: "accounts.billing".to_owned(),
                feed: Some("accounts.users".to_owned()),
                domain: None,
                start: ReaderStart::Beginning,
                delivery_delay_ns: None,
                exhausted_policy: None,
            }])
            .await
            .is_err());
        assert!(controller
            .execute_commands(vec![Command::CreateSubscription {
                name: "accounts.crossspace".to_owned(),
                feed: Some("other.events".to_owned()),
                domain: None,
                start: ReaderStart::Beginning,
                delivery_delay_ns: None,
                exhausted_policy: None,
            }])
            .await
            .is_err());
        assert!(controller
            .execute("DROP FEED accounts.users;")
            .await
            .is_err());
        assert!(controller
            .execute("RENAME SUBSCRIPTION accounts.billing TO other.billing;")
            .await
            .is_err());
        let feeds = controller.execute("SHOW FEEDS;").await.unwrap();
        assert_eq!(feeds.results[0].data.as_array().unwrap().len(), 2);
        let subscriptions = controller.execute("SHOW SUBSCRIPTIONS;").await.unwrap();
        assert_eq!(subscriptions.results[0].data.as_array().unwrap().len(), 1);
        assert!(controller.state.lock().await.reader_frontiers.is_empty());
        let legacy_dir = TempDir::new().unwrap();
        let mut legacy: Value =
            serde_json::from_slice(&controller.snapshot_bytes().await.unwrap()).unwrap();
        legacy
            .as_object_mut()
            .unwrap()
            .remove("subscription_progress_assignments");
        let legacy_controller = new_controller(&legacy_dir);
        legacy_controller
            .install_snapshot_bytes(&serde_json::to_vec(&legacy).unwrap())
            .await
            .unwrap();
        assert!(legacy_controller
            .active_subscription_by_name("accounts.billing")
            .await
            .is_some());
        assert!(legacy_controller
            .active_subscription_progress_assignment_by_id(definition.subscription_id)
            .await
            .is_none());
        drop(controller);
        let reopened = new_controller(&directory);
        assert_eq!(
            reopened
                .active_subscription_by_name("accounts.billing")
                .await
                .unwrap(),
            definition
        );
        assert_eq!(
            reopened
                .active_subscription_progress_assignment_by_id(definition.subscription_id)
                .await,
            Some(placement)
        );
        reopened
            .execute("DROP SUBSCRIPTION accounts.billing; DROP FEED accounts.users;")
            .await
            .unwrap();
        assert!(reopened
            .active_subscription_by_name("accounts.billing")
            .await
            .is_none());
        assert!(reopened
            .active_subscription_progress_assignment_by_id(definition.subscription_id)
            .await
            .is_none());
    }

    #[tokio::test]
    async fn subscription_progress_owner_recovery_requires_current_epoch_and_replica_member() {
        let directory = TempDir::new().unwrap();
        let controller = new_controller(&directory);
        controller.execute("CREATE SPACE orders; CREATE FEED orders.events; CREATE SUBSCRIPTION orders.billing FROM orders.events;").await.unwrap();
        let subscription = controller
            .active_subscription_by_name("orders.billing")
            .await
            .unwrap();
        let assignment = controller
            .active_subscription_progress_assignment_by_id(subscription.subscription_id)
            .await
            .unwrap();
        let survivor = assignment
            .replicas
            .iter()
            .find(|node| *node != &assignment.owner)
            .cloned()
            .unwrap();
        let outsider = StorageNodeId::try_new("storage-x").unwrap();

        assert!(controller
            .execute_commands(vec![Command::RecoverSubscriptionProgressOwner {
                subscription_id: subscription.subscription_id,
                expected_ownership_epoch: 1,
                new_owner: outsider,
            }])
            .await
            .is_err());
        assert!(controller
            .execute_commands(vec![Command::RecoverSubscriptionProgressOwner {
                subscription_id: subscription.subscription_id,
                expected_ownership_epoch: 9,
                new_owner: survivor.clone(),
            }])
            .await
            .is_err());
        assert!(controller
            .execute_commands(vec![Command::RecoverSubscriptionProgressOwner {
                subscription_id: Uuid::from_u128(9_999),
                expected_ownership_epoch: 1,
                new_owner: survivor.clone(),
            }])
            .await
            .is_err());

        controller
            .execute_commands(vec![Command::RecoverSubscriptionProgressOwner {
                subscription_id: subscription.subscription_id,
                expected_ownership_epoch: 1,
                new_owner: survivor.clone(),
            }])
            .await
            .unwrap();
        let moved = controller
            .active_subscription_progress_assignment_by_id(subscription.subscription_id)
            .await
            .unwrap();
        assert_eq!(moved.owner, survivor);
        assert_eq!(moved.ownership_epoch, 2);
        assert_eq!(moved.replicas, assignment.replicas);
        assert!(controller
            .execute_commands(vec![Command::RecoverSubscriptionProgressOwner {
                subscription_id: subscription.subscription_id,
                expected_ownership_epoch: 1,
                new_owner: survivor,
            }])
            .await
            .is_err());

        drop(controller);
        let reopened = new_controller(&directory);
        let persisted = reopened
            .active_subscription_progress_assignment_by_id(subscription.subscription_id)
            .await
            .unwrap();
        assert_eq!(persisted.owner, moved.owner);
        assert_eq!(persisted.ownership_epoch, 2);
    }

    #[tokio::test]
    async fn state_store_owner_recovery_requires_current_epoch_and_replica_member() {
        let directory = TempDir::new().unwrap();
        let controller = new_controller(&directory);
        controller
            .execute("CREATE SPACE orders; CREATE FEED orders.events;")
            .await
            .unwrap();
        controller
            .execute_commands(vec![Command::DefineStateStore {
                name: "orders.profiles".to_owned(),
                source: StateStoreSourceRequest::Manual,
                named_keys: vec![],
            }])
            .await
            .unwrap();
        let store = controller
            .state_store_definitions()
            .await
            .into_iter()
            .next()
            .unwrap();
        let assignment = controller
            .active_state_store_assignment_by_id(store.store_id)
            .await
            .unwrap();
        let survivor = assignment
            .replicas
            .iter()
            .find(|node| *node != &assignment.owner)
            .cloned()
            .unwrap();
        let outsider = StorageNodeId::try_new("storage-x").unwrap();

        assert!(controller
            .execute_commands(vec![Command::RecoverStateStoreOwner {
                store_id: store.store_id,
                expected_ownership_epoch: 1,
                new_owner: outsider,
            }])
            .await
            .is_err());
        assert!(controller
            .execute_commands(vec![Command::RecoverStateStoreOwner {
                store_id: store.store_id,
                expected_ownership_epoch: 9,
                new_owner: survivor.clone(),
            }])
            .await
            .is_err());
        assert!(controller
            .execute_commands(vec![Command::RecoverStateStoreOwner {
                store_id: Uuid::from_u128(9_999),
                expected_ownership_epoch: 1,
                new_owner: survivor.clone(),
            }])
            .await
            .is_err());

        controller
            .execute_commands(vec![Command::RecoverStateStoreOwner {
                store_id: store.store_id,
                expected_ownership_epoch: 1,
                new_owner: survivor.clone(),
            }])
            .await
            .unwrap();
        let moved = controller
            .active_state_store_assignment_by_id(store.store_id)
            .await
            .unwrap();
        assert_eq!(moved.owner, survivor);
        assert_eq!(moved.ownership_epoch, 2);
        assert_eq!(moved.replicas, assignment.replicas);
        // A stale expected epoch is fenced: the CAS cannot ratchet
        // ownership twice off one observation.
        assert!(controller
            .execute_commands(vec![Command::RecoverStateStoreOwner {
                store_id: store.store_id,
                expected_ownership_epoch: 1,
                new_owner: survivor,
            }])
            .await
            .is_err());

        drop(controller);
        let reopened = new_controller(&directory);
        let persisted = reopened
            .active_state_store_assignment_by_id(store.store_id)
            .await
            .unwrap();
        assert_eq!(persisted.owner, moved.owner);
        assert_eq!(persisted.ownership_epoch, 2);
        assert_eq!(reopened.active_state_store_assignments().await.len(), 1);
    }

    #[tokio::test]
    async fn subscription_progress_replica_move_swaps_membership_under_epoch_cas() {
        let directory = TempDir::new().unwrap();
        let store: Arc<dyn LogStore> =
            Arc::new(FileLogStore::open(directory.path().join("data")).unwrap());
        let nodes: Vec<StorageNodeId> = ["storage-1", "storage-2", "storage-3", "storage-4"]
            .into_iter()
            .map(|node| StorageNodeId::try_new(node).unwrap())
            .collect();
        let controller = ControlController::open_with_storage_nodes(
            directory.path().join("catalog.json"),
            store,
            nodes.clone(),
        )
        .unwrap();
        controller.execute("CREATE SPACE orders; CREATE FEED orders.events; CREATE SUBSCRIPTION orders.billing FROM orders.events;").await.unwrap();
        let subscription = controller
            .active_subscription_by_name("orders.billing")
            .await
            .unwrap();
        let assignment = controller
            .active_subscription_progress_assignment_by_id(subscription.subscription_id)
            .await
            .unwrap();
        let replaced = assignment
            .replicas
            .iter()
            .find(|node| *node != &assignment.owner)
            .cloned()
            .unwrap();
        let spare = nodes
            .iter()
            .find(|node| !assignment.replicas.contains(node))
            .cloned()
            .unwrap();
        let member = assignment
            .replicas
            .iter()
            .find(|node| *node != &replaced)
            .cloned()
            .unwrap();
        let outsider = StorageNodeId::try_new("storage-x").unwrap();

        // owner cannot be removed through replica move; recover ownership first.
        assert!(controller
            .execute_commands(vec![Command::MoveSubscriptionProgressReplica {
                subscription_id: subscription.subscription_id,
                expected_ownership_epoch: 1,
                replaced: assignment.owner.clone(),
                replacement: spare.clone(),
            }])
            .await
            .is_err());
        // stale epoch must be refused.
        assert!(controller
            .execute_commands(vec![Command::MoveSubscriptionProgressReplica {
                subscription_id: subscription.subscription_id,
                expected_ownership_epoch: 7,
                replaced: replaced.clone(),
                replacement: spare.clone(),
            }])
            .await
            .is_err());
        // replaced node must be a member.
        assert!(controller
            .execute_commands(vec![Command::MoveSubscriptionProgressReplica {
                subscription_id: subscription.subscription_id,
                expected_ownership_epoch: 1,
                replaced: spare.clone(),
                replacement: member.clone(),
            }])
            .await
            .is_err());
        // replacement must not already be a member.
        assert!(controller
            .execute_commands(vec![Command::MoveSubscriptionProgressReplica {
                subscription_id: subscription.subscription_id,
                expected_ownership_epoch: 1,
                replaced: replaced.clone(),
                replacement: member.clone(),
            }])
            .await
            .is_err());
        // replacement must be an eligible storage Node.
        assert!(controller
            .execute_commands(vec![Command::MoveSubscriptionProgressReplica {
                subscription_id: subscription.subscription_id,
                expected_ownership_epoch: 1,
                replaced: replaced.clone(),
                replacement: outsider,
            }])
            .await
            .is_err());

        controller
            .execute_commands(vec![Command::MoveSubscriptionProgressReplica {
                subscription_id: subscription.subscription_id,
                expected_ownership_epoch: 1,
                replaced: replaced.clone(),
                replacement: spare.clone(),
            }])
            .await
            .unwrap();
        let moved = controller
            .active_subscription_progress_assignment_by_id(subscription.subscription_id)
            .await
            .unwrap();
        assert_eq!(moved.owner, assignment.owner);
        assert_eq!(moved.ownership_epoch, 2);
        assert!(moved.replicas.contains(&spare));
        assert!(!moved.replicas.contains(&replaced));
        // the previous epoch is fenced.
        assert!(controller
            .execute_commands(vec![Command::MoveSubscriptionProgressReplica {
                subscription_id: subscription.subscription_id,
                expected_ownership_epoch: 1,
                replaced: member.clone(),
                replacement: replaced.clone(),
            }])
            .await
            .is_err());

        drop(controller);
        let store: Arc<dyn LogStore> =
            Arc::new(FileLogStore::open(directory.path().join("data")).unwrap());
        let reopened = ControlController::open_with_storage_nodes(
            directory.path().join("catalog.json"),
            store,
            nodes,
        )
        .unwrap();
        let persisted = reopened
            .active_subscription_progress_assignment_by_id(subscription.subscription_id)
            .await
            .unwrap();
        assert_eq!(persisted.owner, moved.owner);
        assert_eq!(persisted.ownership_epoch, 2);
        assert!(persisted.replicas.contains(&spare));
    }

    #[tokio::test]
    async fn feed_requires_an_owning_space_and_drop_is_safe() {
        let directory = TempDir::new().unwrap();
        let controller = new_controller(&directory);
        assert!(matches!(
            controller.execute("CREATE FEED orders.created;").await,
            Err(ControlError::InvalidOperation(_))
        ));
        controller
            .execute("CREATE SPACE orders; CREATE FEED orders.created;")
            .await
            .unwrap();
        let dropped = controller
            .execute("DROP FEED orders.created;")
            .await
            .unwrap();
        assert!(dropped.results[0]
            .message
            .contains("without purging stored history"));
    }

    #[test]
    fn wcl_parses_create_index_and_get_index_predicates() {
        // CREATE INDEX binds a logical name to a source Feed and typed fields.
        let commands = parse_wcl(
            "CREATE INDEX accounts.orders.by.customer ON accounts.orders FIELDS KEY, METADATA 'tenant', JSON 'customer.id';",
        )
        .unwrap();
        assert_eq!(commands.len(), 1);
        assert!(matches!(
            &commands[0],
            Command::DefineIndex { name, feed, fields, unique: false, .. }
                if name == "accounts.orders.by.customer"
                    && feed == "accounts.orders"
                    && fields.len() == 3
        ));

        // CREATE UNIQUE INDEX flips the arbitration contract on.
        let commands = parse_wcl(
            "CREATE UNIQUE INDEX accounts.orders.by.customer ON accounts.orders FIELDS KEY;",
        )
        .unwrap();
        assert!(matches!(
            &commands[0],
            Command::DefineIndex { unique: true, .. }
        ));

        // Exact predicate with mixed typed literals, limit, strict, wait.
        let commands = parse_wcl(
            "GET INDEX accounts.orders.by.customer EQ 42, 'it''s fine;', TRUE, NULL LIMIT 10 STRICT WAIT 250;",
        )
        .unwrap();
        assert_eq!(commands.len(), 1);
        match &commands[0] {
            Command::GetIndex {
                index,
                predicate,
                limit,
                strict,
                wait_ms,
            } => {
                assert_eq!(index, "accounts.orders.by.customer");
                assert_eq!(*limit, Some(10));
                assert!(*strict);
                assert_eq!(*wait_ms, Some(250));
                match predicate {
                    IndexQueryPredicate::Exact { values } => {
                        assert_eq!(
                            *values,
                            vec![
                                IndexValue::I64(42),
                                IndexValue::Text("it's fine;".to_owned()),
                                IndexValue::Bool(true),
                                IndexValue::Null,
                            ]
                        );
                    }
                    other => panic!("expected exact predicate, got {other:?}"),
                }
            }
            other => panic!("expected GET INDEX, got {other:?}"),
        }

        // Prefix predicate: leading complete fields plus MATCH text.
        let commands = parse_wcl("GET INDEX idx PREFIX 'US' MATCH 'ali'").unwrap();
        match &commands[0] {
            Command::GetIndex { predicate, .. } => match predicate {
                IndexQueryPredicate::Prefix {
                    leading,
                    partial_text,
                } => {
                    assert_eq!(*leading, vec![IndexValue::Text("US".to_owned())]);
                    assert_eq!(partial_text.as_deref(), Some("ali"));
                }
                other => panic!("expected prefix, got {other:?}"),
            },
            other => panic!("expected GET INDEX, got {other:?}"),
        }

        // Range predicate with inclusive integer bounds.
        let commands = parse_wcl("GET INDEX idx RANGE 0 TO 100 LIMIT 5").unwrap();
        match &commands[0] {
            Command::GetIndex { predicate, .. } => match predicate {
                IndexQueryPredicate::Range { lower, upper, .. } => {
                    assert_eq!(
                        *lower,
                        IndexBound {
                            value: IndexValue::I64(0),
                            inclusive: true
                        }
                    );
                    assert_eq!(
                        *upper,
                        IndexBound {
                            value: IndexValue::I64(100),
                            inclusive: true
                        }
                    );
                }
                other => panic!("expected range, got {other:?}"),
            },
            other => panic!("expected GET INDEX, got {other:?}"),
        }

        // TEXT literal forces string typing even for digits.
        let commands = parse_wcl("GET INDEX idx EQ TEXT '42'").unwrap();
        match &commands[0] {
            Command::GetIndex { predicate, .. } => match predicate {
                IndexQueryPredicate::Exact { values } => {
                    assert_eq!(*values, vec![IndexValue::Text("42".to_owned())]);
                }
                other => panic!("expected exact, got {other:?}"),
            },
            other => panic!("expected GET INDEX, got {other:?}"),
        }

        // GET INDEX is read-only; the catalog never applies it.
        assert!(commands[0].is_read_only());

        // Errors are actionable.
        assert!(parse_wcl("GET INDEX idx").is_err());
        assert!(parse_wcl("GET INDEX idx FOO 1").is_err());
        assert!(parse_wcl("GET INDEX idx RANGE 0").is_err());
        assert!(parse_wcl("GET INDEX idx RANGE MIN TO 5").is_err());
    }

    #[tokio::test]
    async fn domain_scoped_subscription_source_binds_domain_identity_and_stays_compatible() {
        let directory = TempDir::new().unwrap();
        let controller = new_controller(&directory);
        controller
            .execute("CREATE SPACE orders; CREATE FEED orders.events; CREATE SPACE other; CREATE FEED other.events;")
            .await
            .unwrap();

        // WCL FROM DOMAIN binds the immutable Domain identity.
        let commands = parse_wcl("CREATE SUBSCRIPTION orders.all FROM DOMAIN orders;").unwrap();
        assert!(matches!(
            &commands[0],
            Command::CreateSubscription {
                feed: None,
                domain: Some(domain),
                ..
            } if domain == "orders"
        ));
        controller.execute_commands(commands).await.unwrap();
        let definition = controller
            .active_subscription_by_name("orders.all")
            .await
            .unwrap();
        let domain = controller
            .state
            .lock()
            .await
            .spaces
            .values()
            .find(|space| space.name == "orders")
            .unwrap()
            .space_id;
        assert_eq!(
            definition.source,
            SubscriptionSource::Domain { domain_id: domain }
        );
        // Empty Domain coverage is legal: no Feed needed at declaration.
        // Readers stay exact-Feed: FROM DOMAIN is refused there.
        assert!(parse_wcl("CREATE READER orders.r FROM DOMAIN orders;").is_err());

        // Typed command takes either scope, but exactly one.
        controller
            .execute_commands(vec![Command::CreateSubscription {
                name: "orders.exact".to_owned(),
                feed: Some("orders.events".to_owned()),
                domain: None,
                start: ReaderStart::Beginning,
                delivery_delay_ns: None,
                exhausted_policy: None,
            }])
            .await
            .unwrap();
        assert!(matches!(
            controller
                .active_subscription_by_name("orders.exact")
                .await
                .unwrap()
                .source,
            SubscriptionSource::Feed { .. }
        ));
        assert!(controller
            .execute_commands(vec![Command::CreateSubscription {
                name: "orders.both".to_owned(),
                feed: Some("orders.events".to_owned()),
                domain: Some("orders".to_owned()),
                start: ReaderStart::Beginning,
                delivery_delay_ns: None,
                exhausted_policy: None,
            }])
            .await
            .is_err());
        assert!(controller
            .execute_commands(vec![Command::CreateSubscription {
                name: "orders.neither".to_owned(),
                feed: None,
                domain: None,
                start: ReaderStart::Beginning,
                delivery_delay_ns: None,
                exhausted_policy: None,
            }])
            .await
            .is_err());
        // Cross-Domain sources refuse like cross-Domain Feeds.
        assert!(controller
            .execute_commands(vec![Command::CreateSubscription {
                name: "other.borrowed".to_owned(),
                feed: None,
                domain: Some("orders".to_owned()),
                start: ReaderStart::Beginning,
                delivery_delay_ns: None,
                exhausted_policy: None,
            }])
            .await
            .is_err());

        // Legacy rows written before the variant deserialize as Feed.
        let legacy: SubscriptionDefinition = serde_json::from_value(serde_json::json!({
            "subscription_id": Uuid::from_u128(9),
            "name": "orders.legacy",
            "space_id": domain,
            "feed_id": Uuid::from_u128(8),
            "start": {"kind": "beginning"},
            "stage": "declared",
            "status": "active",
            "created_at_ns": 1
        }))
        .unwrap();
        assert_eq!(legacy.source.feed_id().unwrap(), Uuid::from_u128(8));

        // Dropping a Feed under a covering Domain-scoped Subscription is
        // fenced — membership detach lands with domain membership tracking.
        controller
            .execute("CREATE SUBSCRIPTION orders.cover FROM DOMAIN orders;")
            .await
            .unwrap();
        assert!(controller
            .execute("DROP FEED orders.events;")
            .await
            .is_err());
    }

    #[tokio::test]
    async fn domain_subscription_membership_tracks_feed_lifecycle_by_immutable_id() {
        let directory = TempDir::new().unwrap();
        let controller = new_controller(&directory);
        controller
            .execute("CREATE SPACE orders; CREATE FEED orders.events; CREATE SPACE other;")
            .await
            .unwrap();

        // Declaration attaches every existing Feed in the Domain honoring
        // the declared start, atomically with the commit.
        controller
            .execute("CREATE SUBSCRIPTION orders.cover FROM DOMAIN orders START AT NOW;")
            .await
            .unwrap();
        let subscription = controller
            .active_subscription_by_name("orders.cover")
            .await
            .unwrap();
        let events_id = controller
            .state
            .lock()
            .await
            .feeds
            .values()
            .find(|feed| feed.name == "orders.events")
            .unwrap()
            .feed_id;
        {
            let state = controller.state.lock().await;
            let members = &state.subscription_feed_memberships[&subscription.subscription_id];
            let member = members.get(&events_id).unwrap();
            assert_eq!(member.state, SubscriptionFeedState::Active);
            assert_eq!(member.start, ReaderStart::Now);
            assert!(member.attached_revision > 0);
        }

        // A new Feed attaches in the same commit that creates it — the
        // durable discovery boundary — starting at first retained record
        // (Beginning) regardless of the declared start.
        controller.execute("CREATE FEED orders.new;").await.unwrap();
        let new_id = {
            let state = controller.state.lock().await;
            let members = &state.subscription_feed_memberships[&subscription.subscription_id];
            assert_eq!(members.len(), 2);
            let member = members
                .values()
                .find(|member| member.feed_id != events_id)
                .unwrap();
            assert_eq!(member.state, SubscriptionFeedState::Active);
            assert_eq!(member.start, ReaderStart::Beginning);
            member.feed_id
        };

        // Renaming within the Domain retains membership (immutable FeedId).
        controller
            .execute("RENAME FEED orders.new TO orders.renamed;")
            .await
            .unwrap();
        {
            let state = controller.state.lock().await;
            let member = state.subscription_feed_memberships[&subscription.subscription_id]
                .get(&new_id)
                .unwrap();
            assert_eq!(member.state, SubscriptionFeedState::Active);
        }

        // Moving the Feed to another Domain drains its membership (fenced
        // pending work, not silently discarded) and attaches it under any
        // covering Subscription in the target Domain.
        controller
            .execute("CREATE SUBSCRIPTION other.cover FROM DOMAIN other;")
            .await
            .unwrap();
        controller
            .execute("RENAME FEED orders.renamed TO other.renamed;")
            .await
            .unwrap();
        let other_id = {
            let state = controller.state.lock().await;
            let source_members =
                &state.subscription_feed_memberships[&subscription.subscription_id];
            let drained = source_members.get(&new_id).unwrap();
            assert_eq!(drained.state, SubscriptionFeedState::Draining);
            assert!(drained.detail.is_some());
            let other_sub = state
                .subscriptions
                .values()
                .find(|item| item.name == "other.cover")
                .unwrap();
            let member = state.subscription_feed_memberships[&other_sub.subscription_id]
                .get(&new_id)
                .unwrap();
            assert_eq!(member.state, SubscriptionFeedState::Active);
            assert_eq!(member.start, ReaderStart::Beginning);
            other_sub.subscription_id
        };

        // Drop under coverage stays fenced; once the Feed lives in another
        // Domain the operator's explicit drop retires the fenced row.
        assert!(controller
            .execute("DROP FEED other.renamed;")
            .await
            .is_err());
        controller
            .execute("DROP SUBSCRIPTION other.cover;")
            .await
            .unwrap();
        {
            let state = controller.state.lock().await;
            assert!(!state.subscription_feed_memberships.contains_key(&other_id));
        }
        controller
            .execute("DROP FEED other.renamed;")
            .await
            .unwrap();
        {
            let state = controller.state.lock().await;
            assert!(
                !state.subscription_feed_memberships[&subscription.subscription_id]
                    .contains_key(&new_id)
            );
        }

        // Membership survives a catalog snapshot + restart (the map rides
        // CatalogState serde).
        let reopened = new_controller(&directory);
        let state = reopened.state.lock().await;
        let member = state.subscription_feed_memberships[&subscription.subscription_id]
            .get(&events_id)
            .unwrap();
        assert_eq!(member.state, SubscriptionFeedState::Active);
        drop(state);

        // DESCRIBE surfaces explainable member-Feed states.
        let described = reopened
            .execute("DESCRIBE SUBSCRIPTION orders.cover;")
            .await
            .unwrap();
        let members = described.results[0]
            .data
            .get("member_feeds")
            .unwrap()
            .as_array()
            .unwrap();
        assert!(members.iter().any(|member| {
            member["feed_id"].as_str().unwrap() == events_id.to_string()
                && member["state"] == "active"
        }));
    }
    #[tokio::test]
    async fn domain_subscription_member_bound_refuses_attach_instead_of_skipping() {
        let directory = TempDir::new().unwrap();
        let controller = new_controller(&directory);
        controller
            .execute("CREATE SPACE orders; CREATE SUBSCRIPTION orders.cover FROM DOMAIN orders;")
            .await
            .unwrap();
        let subscription = controller
            .active_subscription_by_name("orders.cover")
            .await
            .unwrap();
        // Seed the map to the bound; a real attach then refuses loudly.
        {
            let mut state = controller.state.lock().await;
            let members = state
                .subscription_feed_memberships
                .entry(subscription.subscription_id)
                .or_default();
            for index in 0..MAX_SUBSCRIPTION_MEMBER_FEEDS {
                members.insert(
                    Uuid::from_u128(0x1000 + index as u128),
                    SubscriptionFeedMembership {
                        feed_id: Uuid::from_u128(0x1000 + index as u128),
                        state: SubscriptionFeedState::Active,
                        start: ReaderStart::Beginning,
                        attached_revision: 0,
                        detail: None,
                    },
                );
            }
        }
        let error = controller
            .execute("CREATE FEED orders.overflow;")
            .await
            .unwrap_err();
        assert!(error.to_string().contains("member-Feed bound"));
    }
    #[tokio::test]
    async fn window_pipes_validate_their_output_statestore() {
        let directory = TempDir::new().unwrap();
        let leader = new_controller(&directory);
        leader
            .execute(
                "CREATE SPACE accounts; CREATE FEED accounts.events;                  CREATE SUBSCRIPTION accounts.flow FROM accounts.events;                  CREATE SPACE other; DEFINE STATESTORE other.panes KEYS key, pane;",
            )
            .await
            .unwrap();

        let define = |output_store: Option<String>, window_ns: u64| Command::DefinePipe {
            name: "accounts.rollup".to_owned(),
            subscription: "accounts.flow".to_owned(),
            output_feed: None,
            output_store,
            quarantine_feed: None,
            emit_feed: None,
            operation: Box::new(PipeOperation::Window {
                window_ns,
                key: IndexFieldSpec::Key,
                aggregation: WindowAggregation::Count,
                time: WindowTime::Event,
                max_late_ns: None,
                watermark_ns: None,
                grace_ns: None,
                expire_ns: None,
                key_limit: None,
                byte_limit: None,
            }),
        };
        // Missing store, foreign-Domain store, zero window, and a missing
        // output target are all refused.
        for command in [
            define(Some("accounts.missing".to_owned()), 60),
            define(Some("other.panes".to_owned()), 60),
            define(None, 60),
            define(Some("accounts.panes".to_owned()), 0),
        ] {
            assert!(leader.execute_commands(vec![command]).await.is_err());
        }
        // A store without the enumerable pane postings is refused.
        leader
            .execute("DEFINE STATESTORE accounts.plain KEYS dept;")
            .await
            .unwrap();
        assert!(leader
            .execute_commands(vec![define(Some("accounts.plain".to_owned()), 60)])
            .await
            .is_err());

        leader
            .execute("DEFINE STATESTORE accounts.panes KEYS key, pane;")
            .await
            .unwrap();
        let result = leader
            .execute_commands(vec![define(
                Some("accounts.panes".to_owned()),
                60_000_000_000,
            )])
            .await
            .unwrap();
        let store = leader
            .declared_state_store_by_name("accounts.panes")
            .await
            .unwrap();
        assert_eq!(
            result.results[0].data["output_store_id"],
            store.store_id.to_string()
        );
        assert!(result.results[0].data["output_feed_id"].is_null());
        assert_eq!(result.results[0].data["operation"]["operation"], "window");

        // The WCL form parses to the same typed command.
        let parsed = leader
            .execute(
                "CREATE PIPE accounts.minute.sum FROM SUBSCRIPTION accounts.flow                  TO STATESTORE accounts.panes WINDOW 5m BY KEY SUM JSON 'v';",
            )
            .await
            .unwrap();
        let relay = leader
            .active_pipe_by_name("accounts.minute.sum")
            .await
            .unwrap();
        assert!(matches!(
            relay.operation,
            PipeOperation::Window {
                window_ns: 300_000_000_000,
                key: IndexFieldSpec::Key,
                aggregation: WindowAggregation::Sum { .. },
                time: WindowTime::Event,
                ..
            }
        ));
        assert_eq!(relay.output_feed_id, None);
        assert_eq!(relay.output_store_id, Some(store.store_id));

        // `INGEST` selects the durable-arrival clock; absent defaults event.
        leader
            .execute(
                "CREATE PIPE accounts.ingest FROM SUBSCRIPTION accounts.flow                  TO STATESTORE accounts.panes WINDOW 1h INGEST BY KEY COUNT;",
            )
            .await
            .unwrap();
        assert!(matches!(
            leader
                .active_pipe_by_name("accounts.ingest")
                .await
                .unwrap()
                .operation,
            PipeOperation::Window {
                time: WindowTime::Ingest,
                ..
            }
        ));
        // A pre-policy serialized window operation decodes as event time
        // with no lateness bound.
        let legacy: PipeOperation = serde_json::from_value(serde_json::json!({
            "operation": "window",
            "window_ns": 1,
            "key": {"kind": "key"},
            "aggregation": {"aggregation": "count"}
        }))
        .unwrap();
        assert!(matches!(
            legacy,
            PipeOperation::Window {
                time: WindowTime::Event,
                max_late_ns: None,
                expire_ns: None,
                key_limit: None,
                byte_limit: None,
                ..
            }
        ));

        // MAX_LATE bounds how far behind its event time a record may
        // arrive before quarantining out of pane accumulation.
        leader
            .execute(
                "CREATE PIPE accounts.bounded FROM SUBSCRIPTION accounts.flow                  TO STATESTORE accounts.panes WINDOW 1h EVENT MAX_LATE 5m BY KEY COUNT;",
            )
            .await
            .unwrap();
        assert!(matches!(
            leader
                .active_pipe_by_name("accounts.bounded")
                .await
                .unwrap()
                .operation,
            PipeOperation::Window {
                time: WindowTime::Event,
                max_late_ns: Some(bound),
                expire_ns: None,
                key_limit: None,
                byte_limit: None,
                ..
            } if bound == 300_000_000_000
        ));

        // Malformed window clauses fail at parse, not at validation.
        for statement in [
            "CREATE PIPE accounts.bad1 FROM SUBSCRIPTION accounts.flow TO STATESTORE accounts.panes WINDOW 0s BY KEY COUNT;",
            "CREATE PIPE accounts.bad2 FROM SUBSCRIPTION accounts.flow TO STATESTORE accounts.panes WINDOW 5x BY KEY COUNT;",
            "CREATE PIPE accounts.bad3 FROM SUBSCRIPTION accounts.flow TO STATESTORE accounts.panes WINDOW 5m COUNT;",
            "CREATE PIPE accounts.bad4 FROM SUBSCRIPTION accounts.flow TO STATESTORE accounts.panes WINDOW 5m BY KEY MEDIAN;",
            // MAX_LATE under the ingest clock and a zero bound both refuse.
            "CREATE PIPE accounts.bad5w FROM SUBSCRIPTION accounts.flow TO STATESTORE accounts.panes WINDOW 5m INGEST MAX_LATE 1m BY KEY COUNT;",
            "CREATE PIPE accounts.bad6w FROM SUBSCRIPTION accounts.flow TO STATESTORE accounts.panes WINDOW 5m MAX_LATE 0s BY KEY COUNT;",
        ] {
            assert!(
                leader.execute(statement).await.is_err(),
                "{statement} must fail"
            );
        }
        let _ = parsed;

        // EMIT TO FEED declares where closed panes publish; it requires
        // MAX_LATE (a pane only provably closes inside a lateness bound)
        // and resolves like any other pipe output Feed.
        leader
            .execute("CREATE FEED accounts.results;")
            .await
            .unwrap();
        leader
            .execute(
                "CREATE PIPE accounts.emitter FROM SUBSCRIPTION accounts.flow                  TO STATESTORE accounts.panes WINDOW 1h EVENT MAX_LATE 5m BY KEY COUNT EMIT TO FEED accounts.results;",
            )
            .await
            .unwrap();
        let emitter = leader
            .active_pipe_by_name("accounts.emitter")
            .await
            .unwrap();
        let results_feed = leader
            .active_feed_by_name("accounts.results")
            .await
            .unwrap();
        assert_eq!(emitter.output_feed_id, Some(results_feed.feed_id));
        assert_eq!(emitter.output_store_id, Some(store.store_id));

        for statement in [
            // No MAX_LATE — no pane ever provably closes.
            "CREATE PIPE accounts.emit1 FROM SUBSCRIPTION accounts.flow TO STATESTORE accounts.panes WINDOW 1h BY KEY COUNT EMIT TO FEED accounts.results;",
            // Emission target must not be the pipe's own input Feed.
            "CREATE PIPE accounts.emit2 FROM SUBSCRIPTION accounts.flow TO STATESTORE accounts.panes WINDOW 1h MAX_LATE 5m BY KEY COUNT EMIT TO FEED accounts.events;",
            // Missing or foreign-Domain emission Feeds refuse.
            "CREATE PIPE accounts.emit3 FROM SUBSCRIPTION accounts.flow TO STATESTORE accounts.panes WINDOW 1h MAX_LATE 5m BY KEY COUNT EMIT TO FEED accounts.missing;",
            // A malformed tail fails at parse.
            "CREATE PIPE accounts.emit4 FROM SUBSCRIPTION accounts.flow TO STATESTORE accounts.panes WINDOW 1h MAX_LATE 5m BY KEY COUNT EMIT accounts.results;",
        ] {
            assert!(
                leader.execute(statement).await.is_err(),
                "{statement} must fail"
            );
        }

        // `WATERMARK <dur> [GRACE <dur>]` selects the durable
        // event-time lateness model — panes close once the committed
        // horizon `max(event)-watermark` passes their end, and GRACE
        // bounds how long an emitted pane accepts revising events.
        leader
            .execute(
                "CREATE PIPE accounts.watermarked FROM SUBSCRIPTION accounts.flow                  TO STATESTORE accounts.panes WINDOW 1h EVENT WATERMARK 5m GRACE 30s EXPIRE 2h BY KEY COUNT EMIT TO FEED accounts.results;",
            )
            .await
            .unwrap();
        let watermarked = leader
            .active_pipe_by_name("accounts.watermarked")
            .await
            .unwrap();
        assert!(matches!(
            watermarked.operation,
            PipeOperation::Window {
                max_late_ns: None,
                watermark_ns: Some(300_000_000_000),
                grace_ns: Some(30_000_000_000),
                expire_ns: Some(7_200_000_000_000),
                ..
            }
        ));
        // WATERMARK alone supplies the close proof EMIT TO FEED needs.
        assert_eq!(watermarked.output_feed_id, Some(results_feed.feed_id));

        for statement in [
            // MAX_LATE and WATERMARK are alternative lateness models.
            "CREATE PIPE accounts.wbad1 FROM SUBSCRIPTION accounts.flow TO STATESTORE accounts.panes WINDOW 1h MAX_LATE 5m WATERMARK 5m BY KEY COUNT;",
            // WATERMARK follows the event clock only.
            "CREATE PIPE accounts.wbad2 FROM SUBSCRIPTION accounts.flow TO STATESTORE accounts.panes WINDOW 1h INGEST WATERMARK 5m BY KEY COUNT;",
            // GRACE needs the horizon it bounds, and both need non-zero
            // durations.
            "CREATE PIPE accounts.wbad3 FROM SUBSCRIPTION accounts.flow TO STATESTORE accounts.panes WINDOW 1h MAX_LATE 5m GRACE 30s BY KEY COUNT;",
            "CREATE PIPE accounts.wbad4 FROM SUBSCRIPTION accounts.flow TO STATESTORE accounts.panes WINDOW 1h WATERMARK 0s BY KEY COUNT;",
            "CREATE PIPE accounts.wbad5 FROM SUBSCRIPTION accounts.flow TO STATESTORE accounts.panes WINDOW 1h WATERMARK 5m GRACE 0s BY KEY COUNT;",
            // Revision markers must outlive the revision bound.
            "CREATE PIPE accounts.wbad6 FROM SUBSCRIPTION accounts.flow TO STATESTORE accounts.panes WINDOW 1h WATERMARK 5m GRACE 30s EXPIRE 20s BY KEY COUNT;",
        ] {
            assert!(
                leader.execute(statement).await.is_err(),
                "{statement} must fail"
            );
        }

        // EXPIRE declares a pane-row retention bound independent of
        // emission — the store keeps no pane past pane_end + expire.
        leader
            .execute(
                "CREATE PIPE accounts.expiring FROM SUBSCRIPTION accounts.flow                  TO STATESTORE accounts.panes WINDOW 1h EVENT MAX_LATE 5m EXPIRE 2h BY KEY COUNT;",
            )
            .await
            .unwrap();
        assert!(matches!(
            leader
                .active_pipe_by_name("accounts.expiring")
                .await
                .unwrap()
                .operation,
            PipeOperation::Window {
                max_late_ns: Some(300_000_000_000),
                expire_ns: Some(7_200_000_000_000),
                key_limit: None,
                byte_limit: None,
                ..
            }
        ));
        // EXPIRE stands alone — retention without lateness or emission.
        leader
            .execute(
                "CREATE PIPE accounts.expiring2 FROM SUBSCRIPTION accounts.flow                  TO STATESTORE accounts.panes WINDOW 1h EXPIRE 2h BY KEY COUNT;",
            )
            .await
            .unwrap();
        assert!(matches!(
            leader
                .active_pipe_by_name("accounts.expiring2")
                .await
                .unwrap()
                .operation,
            PipeOperation::Window {
                expire_ns: Some(7_200_000_000_000),
                key_limit: None,
                byte_limit: None,
                ..
            }
        ));
        for statement in [
            // A pane must provably close before it may expire.
            "CREATE PIPE accounts.exp1 FROM SUBSCRIPTION accounts.flow TO STATESTORE accounts.panes WINDOW 1h MAX_LATE 5m EXPIRE 1m BY KEY COUNT;",
            // Zero retention refuses.
            "CREATE PIPE accounts.exp2 FROM SUBSCRIPTION accounts.flow TO STATESTORE accounts.panes WINDOW 1h EXPIRE 0s BY KEY COUNT;",
        ] {
            assert!(
                leader.execute(statement).await.is_err(),
                "{statement} must fail"
            );
        }

        // LIMIT KEYS bounds the distinct active Keys the window may hold
        // pane/sentinel state for; refusing Keys count one shared sentinel
        // row instead of growing state without limit.
        leader
            .execute(
                "CREATE PIPE accounts.capped FROM SUBSCRIPTION accounts.flow TO STATESTORE accounts.panes WINDOW 1h LIMIT KEYS 7 BY KEY COUNT;",
            )
            .await
            .unwrap();
        assert!(matches!(
            leader
                .active_pipe_by_name("accounts.capped")
                .await
                .unwrap()
                .operation,
            PipeOperation::Window {
                key_limit: Some(7),
                ..
            }
        ));
        for statement in [
            // A zero bound would refuse every record — rejected outright.
            "CREATE PIPE accounts.cap0 FROM SUBSCRIPTION accounts.flow TO STATESTORE accounts.panes WINDOW 1h LIMIT KEYS 0 BY KEY COUNT;",
            // LIMIT requires the KEYS keyword.
            "CREATE PIPE accounts.capx FROM SUBSCRIPTION accounts.flow TO STATESTORE accounts.panes WINDOW 1h LIMIT 7 BY KEY COUNT;",
            // The bound must be an unsigned integer.
            "CREATE PIPE accounts.capy FROM SUBSCRIPTION accounts.flow TO STATESTORE accounts.panes WINDOW 1h LIMIT KEYS many BY KEY COUNT;",
            // Above the enumerable scan bound the count can't be proven.
            "CREATE PIPE accounts.capz FROM SUBSCRIPTION accounts.flow TO STATESTORE accounts.panes WINDOW 1h LIMIT KEYS 513 BY KEY COUNT;",
        ] {
            assert!(
                leader.execute(statement).await.is_err(),
                "{statement} must fail"
            );
        }

        // LIMIT BYTES bounds accounted pane-state bytes; refused records
        // count one shared __bytecap__ sentinel. Either LIMIT order
        // parses, and both bounds may compose.
        leader
            .execute(
                "CREATE PIPE accounts.bytecap FROM SUBSCRIPTION accounts.flow TO STATESTORE accounts.panes WINDOW 1h LIMIT BYTES 4096 BY KEY COUNT;",
            )
            .await
            .unwrap();
        assert!(matches!(
            leader
                .active_pipe_by_name("accounts.bytecap")
                .await
                .unwrap()
                .operation,
            PipeOperation::Window {
                byte_limit: Some(4096),
                ..
            }
        ));
        leader
            .execute(
                "CREATE PIPE accounts.bothcaps FROM SUBSCRIPTION accounts.flow TO STATESTORE accounts.panes WINDOW 1h LIMIT BYTES 2048 LIMIT KEYS 3 BY KEY COUNT;",
            )
            .await
            .unwrap();
        assert!(matches!(
            leader
                .active_pipe_by_name("accounts.bothcaps")
                .await
                .unwrap()
                .operation,
            PipeOperation::Window {
                key_limit: Some(3),
                byte_limit: Some(2048),
                ..
            }
        ));
        for statement in [
            // A zero bound would refuse every record — rejected outright.
            "CREATE PIPE accounts.bcap0 FROM SUBSCRIPTION accounts.flow TO STATESTORE accounts.panes WINDOW 1h LIMIT BYTES 0 BY KEY COUNT;",
            // LIMIT requires a bound keyword.
            "CREATE PIPE accounts.bcapx FROM SUBSCRIPTION accounts.flow TO STATESTORE accounts.panes WINDOW 1h LIMIT 7 BY KEY COUNT;",
            // The bound must be an unsigned integer.
            "CREATE PIPE accounts.bcapy FROM SUBSCRIPTION accounts.flow TO STATESTORE accounts.panes WINDOW 1h LIMIT BYTES many BY KEY COUNT;",
            // Above the enumerable scan bound the byte total can't be
            // proven exactly — refused at declaration.
            "CREATE PIPE accounts.bcapz FROM SUBSCRIPTION accounts.flow TO STATESTORE accounts.panes WINDOW 1h LIMIT BYTES 262145 BY KEY COUNT;",
        ] {
            assert!(
                leader.execute(statement).await.is_err(),
                "{statement} must fail"
            );
        }

        // Enrich Pipes need both targets: an output Feed and a lookup
        // StateStore in the same Domain, plus a valid key projection.
        let enrich = |output_feed, output_store| Command::DefinePipe {
            name: "accounts.enrich".to_owned(),
            subscription: "accounts.flow".to_owned(),
            output_feed,
            output_store,
            quarantine_feed: None,
            emit_feed: None,
            operation: Box::new(PipeOperation::Enrich {
                key: IndexFieldSpec::Key,
                freshness: EnrichFreshness::Latest,
                named: None,
                on_miss: EnrichMiss::Forward,
                quarantine_feed_id: None,
            }),
        };
        leader
            .execute("CREATE FEED accounts.enriched; CREATE FEED accounts.dead;")
            .await
            .unwrap();
        for command in [
            enrich(
                Some("accounts.missing".to_owned()),
                Some("accounts.panes".to_owned()),
            ),
            enrich(
                Some("accounts.enriched".to_owned()),
                Some("accounts.missing".to_owned()),
            ),
            enrich(
                Some("accounts.enriched".to_owned()),
                Some("other.panes".to_owned()),
            ),
            // A self-feeding output loop is refused for Enrich too.
            enrich(
                Some("accounts.events".to_owned()),
                Some("accounts.panes".to_owned()),
            ),
            enrich(None, Some("accounts.panes".to_owned())),
            enrich(Some("accounts.enriched".to_owned()), None),
        ] {
            assert!(leader.execute_commands(vec![command]).await.is_err());
        }
        let result = leader
            .execute_commands(vec![enrich(
                Some("accounts.enriched".to_owned()),
                Some("accounts.panes".to_owned()),
            )])
            .await
            .unwrap();
        assert!(result.results[0].data["output_feed_id"].is_string());
        assert!(result.results[0].data["output_store_id"].is_string());
        assert_eq!(result.results[0].data["operation"]["operation"], "enrich");

        // The WCL form: CREATE PIPE ... TO FEED <f> ENRICH STATESTORE <s> BY <spec>.
        leader
            .execute(
                "CREATE PIPE accounts.enriched2 FROM SUBSCRIPTION accounts.flow                  TO FEED accounts.enriched ENRICH STATESTORE accounts.panes BY JSON 'cust';",
            )
            .await
            .unwrap();
        assert!(matches!(
            leader
                .active_pipe_by_name("accounts.enriched2")
                .await
                .unwrap()
                .operation,
            PipeOperation::Enrich {
                key: IndexFieldSpec::Projection { .. },
                freshness: EnrichFreshness::Latest,
                ..
            }
        ));

        // STRICT makes lagging-state reads retryable instead of journaled.
        leader
            .execute(
                "CREATE PIPE accounts.enriched3 FROM SUBSCRIPTION accounts.flow                  TO FEED accounts.enriched ENRICH STATESTORE accounts.panes STRICT BY JSON 'cust';",
            )
            .await
            .unwrap();
        assert!(matches!(
            leader
                .active_pipe_by_name("accounts.enriched3")
                .await
                .unwrap()
                .operation,
            PipeOperation::Enrich {
                key: IndexFieldSpec::Projection { .. },
                freshness: EnrichFreshness::Strict,
                ..
            }
        ));

        // Freshness, named, and on_miss all default when the serialized
        // form predates them.
        let legacy: PipeOperation =
            serde_json::from_str(r#"{"operation":"enrich","key":{"kind":"key"}}"#).unwrap();
        assert!(matches!(
            legacy,
            PipeOperation::Enrich {
                freshness: EnrichFreshness::Latest,
                named: None,
                on_miss: EnrichMiss::Forward,
                ..
            }
        ));

        // NAMED routes resolution through a declared secondary key; MISS
        // DROP consumes lookup misses without emitting.
        leader
            .execute(
                "CREATE PIPE accounts.enriched4 FROM SUBSCRIPTION accounts.flow                  TO FEED accounts.enriched ENRICH STATESTORE accounts.panes NAMED 'email' BY KEY MISS DROP;",
            )
            .await
            .unwrap();
        assert!(matches!(
            leader
                .active_pipe_by_name("accounts.enriched4")
                .await
                .unwrap()
                .operation,
            PipeOperation::Enrich {
                key: IndexFieldSpec::Key,
                named: Some(ref name),
                on_miss: EnrichMiss::Drop,
                ..
            } if name == "email"
        ));

        // MISS QUARANTINE <feed> resolves the declared Feed at define time.
        leader
            .execute(
                "CREATE PIPE accounts.enriched5 FROM SUBSCRIPTION accounts.flow                  TO FEED accounts.enriched ENRICH STATESTORE accounts.panes BY KEY MISS QUARANTINE accounts.dead;",
            )
            .await
            .unwrap();
        match leader
            .active_pipe_by_name("accounts.enriched5")
            .await
            .unwrap()
            .operation
        {
            PipeOperation::Enrich {
                on_miss: EnrichMiss::Quarantine,
                quarantine_feed_id: Some(id),
                ..
            } => {
                assert_eq!(
                    Some(id),
                    leader
                        .active_feed_by_name("accounts.dead")
                        .await
                        .map(|feed| feed.feed_id)
                );
            }
            other => panic!("expected quarantine enrich, got {other:?}"),
        }

        for statement in [
            "CREATE PIPE accounts.bad5 FROM SUBSCRIPTION accounts.flow TO FEED accounts.enriched ENRICH STATESTORE accounts.panes;",
            "CREATE PIPE accounts.bad6 FROM SUBSCRIPTION accounts.flow TO FEED accounts.enriched ENRICH accounts.panes BY KEY;",
            "CREATE PIPE accounts.bad7 FROM SUBSCRIPTION accounts.flow TO FEED accounts.enriched ENRICH STATESTORE accounts.panes NAMED 'Bad-Name' BY KEY;",
            "CREATE PIPE accounts.bad8 FROM SUBSCRIPTION accounts.flow TO FEED accounts.enriched ENRICH STATESTORE accounts.panes BY KEY MISS SKIP;",
            // Quarantine to the input Feed (loop), the output Feed
            // (contamination), and a missing Feed all refuse.
            "CREATE PIPE accounts.bad9 FROM SUBSCRIPTION accounts.flow TO FEED accounts.enriched ENRICH STATESTORE accounts.panes BY KEY MISS QUARANTINE accounts.events;",
            "CREATE PIPE accounts.bada FROM SUBSCRIPTION accounts.flow TO FEED accounts.enriched ENRICH STATESTORE accounts.panes BY KEY MISS QUARANTINE accounts.enriched;",
            "CREATE PIPE accounts.badb FROM SUBSCRIPTION accounts.flow TO FEED accounts.enriched ENRICH STATESTORE accounts.panes BY KEY MISS QUARANTINE accounts.missing;",
        ] {
            assert!(
                leader.execute(statement).await.is_err(),
                "{statement} must fail"
            );
        }
    }

    /// A Pipe never consumes a Feed it writes: `write_feeds` is the member
    /// -Feed exclusion set a Domain-sourced drive applies when probing
    /// entries — output and miss-quarantine routes both stay out of the
    /// input set so the pipe can never loop its own writes.
    #[tokio::test]
    async fn pipe_write_feeds_excludes_output_and_quarantine_routes() {
        let directory = TempDir::new().unwrap();
        let controller = new_controller(&directory);
        controller
            .execute(
                "CREATE DOMAIN accounts; CREATE FEED accounts.in;                  CREATE FEED accounts.out; CREATE FEED accounts.misses;                  DEFINE STATESTORE accounts.lookup KEYS dept;                  CREATE SUBSCRIPTION accounts.flow FROM DOMAIN accounts;                  CREATE PIPE accounts.copy FROM SUBSCRIPTION accounts.flow TO FEED accounts.out;                  CREATE PIPE accounts.lookuped FROM SUBSCRIPTION accounts.flow                    TO FEED accounts.out ENRICH STATESTORE accounts.lookup                    BY KEY MISS QUARANTINE accounts.misses;",
            )
            .await
            .unwrap();
        let (copy, lookuped) = {
            let subs = controller.active_pipes().await;
            (
                subs.iter()
                    .find(|p| p.name == "accounts.copy")
                    .unwrap()
                    .clone(),
                subs.iter()
                    .find(|p| p.name == "accounts.lookuped")
                    .unwrap()
                    .clone(),
            )
        };
        let out = controller
            .active_feed_by_name("accounts.out")
            .await
            .unwrap()
            .feed_id;
        let misses = controller
            .active_feed_by_name("accounts.misses")
            .await
            .unwrap()
            .feed_id;
        assert_eq!(copy.write_feeds(), BTreeSet::from([out]));
        assert_eq!(lookuped.write_feeds(), BTreeSet::from([out, misses]));
    }

    #[tokio::test]
    async fn pipe_compute_leases_seed_move_and_fence() {
        let directory = TempDir::new().unwrap();
        let controller = new_controller(&directory);
        controller
            .execute(
                "CREATE DOMAIN accounts; CREATE FEED accounts.events; \
                 CREATE FEED accounts.enriched; CREATE SUBSCRIPTION accounts.flow \
                 FROM accounts.events;",
            )
            .await
            .unwrap();
        controller
            .execute("CREATE PIPE accounts.forward FROM SUBSCRIPTION accounts.flow TO FEED accounts.enriched;")
            .await
            .unwrap();
        let pipe = controller
            .active_pipe_by_name("accounts.forward")
            .await
            .unwrap();
        let progress = controller
            .active_subscription_progress_assignment_by_id(pipe.subscription_id)
            .await
            .unwrap();
        // The seeded lease is co-located with the Subscription progress
        // owner — the previous implicit scheduling rule.
        let lease = controller.pipe_compute_lease(pipe.pipe_id).await.unwrap();
        assert_eq!(lease.owner, progress.owner);
        assert_eq!(lease.compute_epoch, 1);
        // SHOW PIPES exposes the effective lease.
        let shown = controller.execute("SHOW PIPES;").await.unwrap();
        assert_eq!(
            shown.results[0].data[0]["compute_owner"],
            lease.owner.as_str()
        );
        assert_eq!(shown.results[0].data[0]["compute_epoch"], 1);
        // MOVE PIPE COMPUTE migrates the lease to another eligible Node
        // and bumps its epoch while the progress placement stays put.
        let target = ["storage-1", "storage-2", "storage-3"]
            .into_iter()
            .map(|node| StorageNodeId::try_new(node).unwrap())
            .find(|node| *node != lease.owner)
            .unwrap();
        controller
            .execute(&format!(
                "MOVE PIPE COMPUTE accounts.forward TO NODE {target};"
            ))
            .await
            .unwrap();
        let moved = controller.pipe_compute_lease(pipe.pipe_id).await.unwrap();
        assert_eq!(moved.owner, target);
        assert_eq!(moved.compute_epoch, 2);
        let progress_after = controller
            .active_subscription_progress_assignment_by_id(pipe.subscription_id)
            .await
            .unwrap();
        assert_eq!(progress_after.owner, progress.owner);
        assert_eq!(progress_after.ownership_epoch, progress.ownership_epoch);
        // A stale expected epoch (a raced drain plan) is refused.
        assert!(controller
            .execute_commands(vec![Command::MovePipeComputeLease {
                pipe: "accounts.forward".to_owned(),
                new_owner: progress.owner.clone(),
                expected_compute_epoch: Some(1),
            }])
            .await
            .is_err());
        // An ineligible target and a no-op move are both refused.
        assert!(controller
            .execute("MOVE PIPE COMPUTE accounts.forward TO NODE storage-9;")
            .await
            .is_err());
        assert!(controller
            .execute(&format!(
                "MOVE PIPE COMPUTE accounts.forward TO NODE {target};"
            ))
            .await
            .is_err());
        // The lease survives a catalog snapshot/reopen.
        drop(controller);
        let reopened = new_controller(&directory);
        let persisted = reopened.pipe_compute_lease(pipe.pipe_id).await.unwrap();
        assert_eq!(persisted.owner, target);
        assert_eq!(persisted.compute_epoch, 2);
        // Dropping the Pipe retires its lease.
        reopened
            .execute("DROP PIPE accounts.forward;")
            .await
            .unwrap();
        assert!(reopened.pipe_compute_lease(pipe.pipe_id).await.is_none());
    }

    #[tokio::test]
    async fn distributaries_declare_lease_move_and_persist() {
        let directory = TempDir::new().unwrap();
        let controller = new_controller(&directory);
        controller
            .execute("CREATE DOMAIN accounts; CREATE FEED accounts.mirrored;")
            .await
            .unwrap();
        // A Riverbed Link declares endpoints plus an env-var credential
        // reference — never key material.
        let declared = controller
            .execute(
                "CREATE RIVERBED LINK dr AT 'https://dr-1:9392,https://dr-2:9392' \
                 WITH KEY ENV WHITEWATER_DR_KEY;",
            )
            .await
            .unwrap();
        let link_id = declared.results[0].data["link_id"]
            .as_str()
            .unwrap()
            .to_owned();
        assert_eq!(
            declared.results[0].data["endpoints"],
            json!(["https://dr-1:9392", "https://dr-2:9392"])
        );
        // Endpoint and credential shape are validated in apply so every
        // replica refuses the same bad declaration.
        for script in [
            "CREATE RIVERBED LINK bad1 AT 'ftp://x' WITH KEY ENV K;",
            "CREATE RIVERBED LINK bad2 AT '' WITH KEY ENV K;",
            "CREATE RIVERBED LINK bad3 AT 'https://x' WITH KEY ENV lowercase;",
            "CREATE RIVERBED LINK dr AT 'https://y' WITH KEY ENV K2;",
        ] {
            assert!(controller.execute(script).await.is_err(), "{script}");
        }
        // The Distributary binds a remote Feed to a local target Feed and
        // seeds a lease on a storage-eligible Node at epoch 1.
        let defined = controller
            .execute(
                "CREATE DISTRIBUTARY accounts.mirror FROM LINK dr \
                 FEED orders.events INTO accounts.mirrored;",
            )
            .await
            .unwrap();
        assert_eq!(defined.results[0].data["source_feed"], "orders.events");
        assert_eq!(
            defined.results[0].data["link_id"].as_str().unwrap(),
            link_id
        );
        let leases = controller.distributary_leases().await;
        assert_eq!(leases.len(), 1);
        assert_eq!(leases[0].epoch, 1);
        // Unknown link, unknown target Feed, and a duplicate name refuse.
        for script in [
            "CREATE DISTRIBUTARY accounts.a FROM LINK missing FEED orders.events INTO accounts.mirrored;",
            "CREATE DISTRIBUTARY accounts.b FROM LINK dr FEED orders.events INTO accounts.missing;",
            "CREATE DISTRIBUTARY accounts.mirror FROM LINK dr FEED orders.events INTO accounts.mirrored;",
        ] {
            assert!(controller.execute(script).await.is_err(), "{script}");
        }
        // SHOW surfaces the resolved link, target Feed, and lease.
        let shown = controller.execute("SHOW DISTRIBUTARIES;").await.unwrap();
        let row = &shown.results[0].data[0];
        assert_eq!(row["name"], "accounts.mirror");
        assert_eq!(row["link"], "dr");
        assert_eq!(row["target_feed"], "accounts.mirrored");
        assert_eq!(row["lease_owner"], leases[0].owner.as_str());
        assert_eq!(row["lease_epoch"], 1);
        let links = controller.execute("SHOW LINKS;").await.unwrap();
        assert_eq!(links.results[0].data.as_array().unwrap().len(), 1);
        let described = controller
            .execute("DESCRIBE DISTRIBUTARY accounts.mirror;")
            .await
            .unwrap();
        assert_eq!(described.results[0].data["lease"]["epoch"], 1);
        // The link cannot drop while a Distributary uses it, and the
        // target Feed cannot drop under an active Distributary.
        assert!(controller.execute("DROP LINK dr;").await.is_err());
        assert!(controller
            .execute("DROP FEED accounts.mirrored;")
            .await
            .is_err());
        // MOVE DISTRIBUTARY LEASE migrates with an epoch bump; a stale
        // expected epoch, an ineligible target, and a no-op move refuse.
        let target = ["storage-1", "storage-2", "storage-3"]
            .into_iter()
            .map(|node| StorageNodeId::try_new(node).unwrap())
            .find(|node| *node != leases[0].owner)
            .unwrap();
        controller
            .execute(&format!(
                "MOVE DISTRIBUTARY LEASE accounts.mirror TO NODE {target};"
            ))
            .await
            .unwrap();
        let moved = &controller.distributary_leases().await[0];
        assert_eq!(moved.owner, target);
        assert_eq!(moved.epoch, 2);
        assert!(controller
            .execute_commands(vec![Command::MoveDistributaryLease {
                distributary: "accounts.mirror".to_owned(),
                new_owner: leases[0].owner.clone(),
                expected_epoch: Some(1),
            }])
            .await
            .is_err());
        assert!(controller
            .execute("MOVE DISTRIBUTARY LEASE accounts.mirror TO NODE storage-9;")
            .await
            .is_err());
        assert!(controller
            .execute(&format!(
                "MOVE DISTRIBUTARY LEASE accounts.mirror TO NODE {target};"
            ))
            .await
            .is_err());
        // The drain plan vacates the lease like every other placement.
        let plan = controller.storage_drain_plan(&target).await.unwrap();
        assert!(plan
            .moves
            .iter()
            .any(|command| matches!(command, Command::MoveDistributaryLease { .. })));
        // Definitions, link, and lease survive a catalog snapshot/reopen.
        drop(controller);
        let reopened = new_controller(&directory);
        let persisted = &reopened.distributary_leases().await[0];
        assert_eq!(persisted.owner, target);
        assert_eq!(persisted.epoch, 2);
        assert_eq!(reopened.active_distributaries().await.len(), 1);
        // Dropping the Distributary retires its lease and frees the link
        // and target Feed for later drops.
        reopened
            .execute("DROP DISTRIBUTARY accounts.mirror;")
            .await
            .unwrap();
        assert!(reopened.distributary_leases().await.is_empty());
        reopened.execute("DROP LINK dr;").await.unwrap();
        reopened
            .execute("DROP FEED accounts.mirrored;")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn distributary_status_reports_fence_to_the_committed_lease() {
        let directory = TempDir::new().unwrap();
        let controller = new_controller(&directory);
        controller
            .execute("CREATE DOMAIN accounts; CREATE FEED accounts.mirrored;")
            .await
            .unwrap();
        controller
            .execute("CREATE RIVERBED LINK dr AT 'https://dr:9392' WITH KEY ENV WHITEWATER_DR_KEY;")
            .await
            .unwrap();
        controller
            .execute(
                "CREATE DISTRIBUTARY accounts.mirror FROM LINK dr                  FEED orders.events INTO accounts.mirrored;",
            )
            .await
            .unwrap();
        let distributary_id = controller.active_distributaries().await[0].distributary_id;
        let lease = controller
            .distributary_lease(distributary_id)
            .await
            .unwrap();
        let report = |run_state, epoch, owner: StorageNodeId, error: Option<&str>| {
            Command::ReportDistributaryStatus {
                distributary_id,
                status: DistributaryStatus {
                    lease_epoch: epoch,
                    reported_by: owner,
                    run_state,
                    copied_records: 7,
                    last_copied_at_ns: Some(11),
                    last_error: error.map(|message| message.to_owned()),
                    last_error_at_ns: error.map(|_| 12),
                    reported_at_ns: 13,
                },
            }
        };
        // The committed owner at the committed epoch publishes.
        controller
            .execute_commands(vec![report(
                DistributaryRunState::Copying,
                1,
                lease.owner.clone(),
                None,
            )])
            .await
            .unwrap();
        let shown = controller.execute("SHOW DISTRIBUTARIES;").await.unwrap();
        let row = &shown.results[0].data[0];
        assert_eq!(row["run_state"], "copying");
        assert_eq!(row["copied_records"], 7);
        assert_eq!(row["status_stale"], false);
        // A stale epoch or a non-owner report is refused and the
        // committed row is unchanged.
        for command in [
            report(
                DistributaryRunState::Stalled,
                0,
                lease.owner.clone(),
                Some("old epoch"),
            ),
            report(
                DistributaryRunState::Stalled,
                1,
                StorageNodeId::try_new("storage-9").unwrap(),
                Some("not the owner"),
            ),
        ] {
            assert!(controller.execute_commands(vec![command]).await.is_err());
        }
        let unchanged = controller
            .distributary_status(distributary_id)
            .await
            .unwrap();
        assert_eq!(unchanged.run_state, DistributaryRunState::Copying);
        // A stalled cycle surfaces the refusal for operators.
        controller
            .execute_commands(vec![report(
                DistributaryRunState::Stalled,
                1,
                lease.owner.clone(),
                Some("remote refused (503): unavailable"),
            )])
            .await
            .unwrap();
        let shown = controller.execute("SHOW DISTRIBUTARIES;").await.unwrap();
        let row = &shown.results[0].data[0];
        assert_eq!(row["run_state"], "stalled");
        assert_eq!(row["last_error"], "remote refused (503): unavailable");
        // After the lease moves the old report reads stale, and the new
        // owner's first report at the new epoch replaces it.
        let target = ["storage-1", "storage-2", "storage-3"]
            .into_iter()
            .map(|node| StorageNodeId::try_new(node).unwrap())
            .find(|node| *node != lease.owner)
            .unwrap();
        controller
            .execute(&format!(
                "MOVE DISTRIBUTARY LEASE accounts.mirror TO NODE {target};"
            ))
            .await
            .unwrap();
        let shown = controller.execute("SHOW DISTRIBUTARIES;").await.unwrap();
        assert_eq!(shown.results[0].data[0]["status_stale"], true);
        controller
            .execute_commands(vec![report(
                DistributaryRunState::Starting,
                2,
                target.clone(),
                None,
            )])
            .await
            .unwrap();
        let shown = controller.execute("SHOW DISTRIBUTARIES;").await.unwrap();
        let row = &shown.results[0].data[0];
        assert_eq!(row["run_state"], "starting");
        assert_eq!(row["status_stale"], false);
        // Dropping the Distributary clears its status row with the lease.
        controller
            .execute("DROP DISTRIBUTARY accounts.mirror;")
            .await
            .unwrap();
        assert!(controller
            .distributary_status(distributary_id)
            .await
            .is_none());
    }

    #[test]
    fn distributary_wcl_parses() {
        let commands = parse_wcl(
            "CREATE RIVERBED LINK dr AT 'https://a:1,https://b:2' WITH KEY ENV DR_KEY; \
             CREATE DISTRIBUTARY accounts.m FROM LINK dr FEED orders.e INTO accounts.m2; \
             MOVE DISTRIBUTARY LEASE accounts.m TO NODE storage-2; \
             DROP DISTRIBUTARY accounts.m; DROP LINK dr; SHOW LINKS; SHOW DISTRIBUTARIES;",
        )
        .unwrap();
        assert!(matches!(
            &commands[0],
            Command::DefineRiverbedLink { endpoints, key_env, .. }
                if endpoints.len() == 2 && key_env == "DR_KEY"
        ));
        assert!(matches!(
            &commands[1],
            Command::DefineDistributary { source_feed, target_feed, .. }
                if source_feed == "orders.e" && target_feed == "accounts.m2"
        ));
        assert!(matches!(
            &commands[2],
            Command::MoveDistributaryLease { .. }
        ));
        assert!(matches!(
            &commands[3],
            Command::Drop {
                kind: ResourceKind::Distributary,
                ..
            }
        ));
        assert!(matches!(
            &commands[4],
            Command::Drop {
                kind: ResourceKind::RiverbedLink,
                ..
            }
        ));
        assert!(matches!(
            &commands[5],
            Command::Show {
                kind: ShowKind::RiverbedLinks,
                ..
            }
        ));
        assert!(matches!(
            &commands[6],
            Command::Show {
                kind: ShowKind::Distributaries,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn storage_drain_plan_moves_pipe_compute_leases() {
        let directory = TempDir::new().unwrap();
        let controller = new_controller(&directory);
        controller
            .execute(
                "CREATE DOMAIN accounts; CREATE FEED accounts.events; \
                 CREATE FEED accounts.enriched; CREATE SUBSCRIPTION accounts.flow \
                 FROM accounts.events; \
                 CREATE PIPE accounts.forward FROM SUBSCRIPTION accounts.flow TO FEED accounts.enriched;",
            )
            .await
            .unwrap();
        let pipe = controller
            .active_pipe_by_name("accounts.forward")
            .await
            .unwrap();
        let lease = controller.pipe_compute_lease(pipe.pipe_id).await.unwrap();
        let plan = controller.storage_drain_plan(&lease.owner).await.unwrap();
        let planned = plan
            .moves
            .iter()
            .find_map(|command| match command {
                Command::MovePipeComputeLease {
                    pipe,
                    new_owner,
                    expected_compute_epoch,
                } if pipe == "accounts.forward" => {
                    Some((new_owner.clone(), *expected_compute_epoch))
                }
                _ => None,
            })
            .expect("drain plan must move the Pipe compute lease");
        assert_eq!(planned.1, Some(lease.compute_epoch));
        assert_ne!(planned.0, lease.owner);
        // Draining a Node that holds no lease plans no Pipe move.
        let other = ["storage-1", "storage-2", "storage-3"]
            .into_iter()
            .map(|node| StorageNodeId::try_new(node).unwrap())
            .find(|node| *node != lease.owner)
            .unwrap();
        let other_plan = controller.storage_drain_plan(&other).await.unwrap();
        assert!(!other_plan
            .moves
            .iter()
            .any(|command| matches!(command, Command::MovePipeComputeLease { .. })));
    }

    #[tokio::test]
    async fn capacity_reports_drive_baseline_exclusions() {
        let directory = TempDir::new().unwrap();
        let controller = new_controller(&directory).with_capacity_policy(CapacityPolicy {
            report_max_age_ns: None,
            min_disk_free_bytes: 1_000,
        });
        // With the policy enabled a Node without a committed report is
        // unmeasured and excluded; the report itself lifts the exclusion.
        let shown = controller.execute("SHOW STORAGE NODES;").await.unwrap();
        let rows = shown.results[0].data.as_array().unwrap();
        let entry = rows
            .iter()
            .find(|entry| entry["node"] == "storage-1")
            .unwrap();
        assert_eq!(entry["eligible"], false);
        assert_eq!(entry["exclusions"], json!(["capacity_unreported"]));
        let reported = controller
            .execute(
                "REPORT STORAGE NODE storage-1 CAPACITY cpu_cores=4,                  memory_bytes=1024, disk_total_bytes=5000, disk_free_bytes=4000,                  network_mbps=10000, failure_domain='zone-a';",
            )
            .await
            .unwrap();
        assert_eq!(
            reported.results[0].data["capacity"]["disk_free_bytes"],
            4000
        );
        let shown = controller.execute("SHOW STORAGE NODES;").await.unwrap();
        let rows = shown.results[0].data.as_array().unwrap();
        let entry = rows
            .iter()
            .find(|entry| entry["node"] == "storage-1")
            .unwrap();
        assert_eq!(entry["eligible"], true);
        assert_eq!(entry["exclusions"], json!([]));
        assert_eq!(entry["capacity"]["cpu_cores"], 4);
        assert_eq!(entry["capacity"]["failure_domain"], "zone-a");
        assert!(entry["capacity_reported_at_ns"].is_string());
        // A Node reporting below the configured floor is excluded.
        controller
            .execute("REPORT STORAGE NODE storage-2 CAPACITY disk_free_bytes=10;")
            .await
            .unwrap();
        let shown = controller.execute("SHOW STORAGE NODES;").await.unwrap();
        let rows = shown.results[0].data.as_array().unwrap();
        let entry = rows
            .iter()
            .find(|entry| entry["node"] == "storage-2")
            .unwrap();
        assert_eq!(entry["eligible"], false);
        assert_eq!(entry["exclusions"], json!(["below_baseline"]));
        // Reports for identities the catalog does not know are refused.
        assert!(controller
            .execute("REPORT STORAGE NODE ghost-9 CAPACITY disk_free_bytes=9999;")
            .await
            .is_err());
        // Unknown fields and missing values are syntax errors.
        assert!(controller
            .execute("REPORT STORAGE NODE storage-1 CAPACITY bogus=1;")
            .await
            .is_err());
    }

    #[tokio::test]
    async fn stale_capacity_reports_exclude_under_the_freshness_bound() {
        let directory = TempDir::new().unwrap();
        // One-nanosecond freshness: a committed report is stale by the
        // time the next command's issued_at lands.
        let controller = new_controller(&directory).with_capacity_policy(CapacityPolicy {
            report_max_age_ns: Some(1),
            min_disk_free_bytes: 0,
        });
        controller
            .execute("REPORT STORAGE NODE storage-1 CAPACITY disk_free_bytes=4000;")
            .await
            .unwrap();
        let shown = controller.execute("SHOW STORAGE NODES;").await.unwrap();
        let entry = shown.results[0]
            .data
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["node"] == "storage-1")
            .unwrap();
        assert_eq!(entry["eligible"], false);
        assert_eq!(entry["exclusions"], json!(["capacity_stale"]));
        // With no policy configured, capacity evidence is advisory only:
        // the same unreported Node stays eligible.
        let directory = TempDir::new().unwrap();
        let controller = new_controller(&directory);
        let shown = controller.execute("SHOW STORAGE NODES;").await.unwrap();
        let entry = shown.results[0]
            .data
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["node"] == "storage-1")
            .unwrap();
        assert_eq!(entry["eligible"], true);
        assert_eq!(entry["exclusions"], json!([]));
    }

    fn versioned_capacity_report(node: &str, protocol_version: u32, build: &str) -> Command {
        Command::ReportStorageNodeCapacity {
            node: StorageNodeId::try_new(node).unwrap(),
            capacity: NodeCapacity {
                disk_free_bytes: 9_999,
                ..Default::default()
            },
            protocol_version,
            build: build.to_owned(),
        }
    }

    #[tokio::test]
    async fn versioned_and_unversioned_capacity_reports_pass_the_protocol_gate() {
        let directory = TempDir::new().unwrap();
        let controller = new_controller(&directory);
        // A Node at this binary's level and a legacy Node that predates
        // version advertisement (level 0) are both inside the window.
        controller
            .execute_commands(vec![versioned_capacity_report(
                "storage-1",
                crate::node::NODE_PROTOCOL_VERSION,
                "9.9.9",
            )])
            .await
            .unwrap();
        controller
            .execute_commands(vec![versioned_capacity_report("storage-2", 0, "")])
            .await
            .unwrap();
        let shown = controller.execute("SHOW COMPATIBILITY;").await.unwrap();
        let data = &shown.results[0].data;
        assert_eq!(data["catalog_schema_version"], CATALOG_SCHEMA_VERSION);
        assert_eq!(
            data["supported_catalog_schema_version"],
            CATALOG_SCHEMA_VERSION
        );
        assert_eq!(
            data["node_protocol_version"],
            crate::node::NODE_PROTOCOL_VERSION
        );
        assert_eq!(
            data["min_node_protocol_version"],
            crate::node::MIN_NODE_PROTOCOL_VERSION
        );
        assert_eq!(data["build"], crate::node::BUILD_VERSION);
        let nodes = data["nodes"].as_array().unwrap();
        let one = nodes.iter().find(|row| row["node"] == "storage-1").unwrap();
        assert_eq!(one["protocol_version"], crate::node::NODE_PROTOCOL_VERSION);
        assert_eq!(one["build"], "9.9.9");
        assert!(data["incompatible_reports"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn incompatible_capacity_reports_are_refused_and_committed() {
        let directory = TempDir::new().unwrap();
        let controller = new_controller(&directory);
        // A Node claiming a protocol newer than this binary serves is
        // refused — and the refusal is committed gate evidence an
        // operator can query, not a dropped log line.
        let refusal = controller
            .execute_commands(vec![versioned_capacity_report(
                "storage-1",
                crate::node::NODE_PROTOCOL_VERSION + 1,
                "99.0.0",
            )])
            .await;
        assert!(refusal.is_err());
        let shown = controller.execute("SHOW COMPATIBILITY;").await.unwrap();
        let data = &shown.results[0].data;
        let rows = data["incompatible_reports"].as_array().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["node"], "storage-1");
        assert_eq!(
            rows[0]["protocol_version"],
            crate::node::NODE_PROTOCOL_VERSION + 1
        );
        assert_eq!(rows[0]["build"], "99.0.0");
        // The refused report leaves no accepted capacity evidence behind.
        let one = data["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| row["node"] == "storage-1")
            .unwrap();
        assert!(one["protocol_version"].is_null());
        assert!(one["build"].is_null());
        // A repeat refusal from the same identity updates its row rather
        // than growing the map.
        controller
            .execute_commands(vec![versioned_capacity_report(
                "storage-1",
                crate::node::NODE_PROTOCOL_VERSION + 2,
                "99.1.0",
            )])
            .await
            .ok();
        let shown = controller.execute("SHOW COMPATIBILITY;").await.unwrap();
        let rows = shown.results[0].data["incompatible_reports"]
            .as_array()
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0]["protocol_version"],
            crate::node::NODE_PROTOCOL_VERSION + 2
        );
    }

    #[tokio::test]
    async fn incompatible_reports_evict_oldest_beyond_the_bound() {
        let directory = TempDir::new().unwrap();
        let controller = new_controller(&directory);
        // Fill the ring, then push one more — the oldest evidence drops
        // first so the map stays bounded under a churning fleet.
        for index in 0..MAX_INCOMPATIBLE_REPORTS {
            controller
                .execute_commands(vec![versioned_capacity_report(
                    &format!("ghost-{index:04}"),
                    crate::node::NODE_PROTOCOL_VERSION + 1,
                    "x",
                )])
                .await
                .ok();
        }
        controller
            .execute_commands(vec![versioned_capacity_report(
                "ghost-9999",
                crate::node::NODE_PROTOCOL_VERSION + 1,
                "x",
            )])
            .await
            .ok();
        let shown = controller.execute("SHOW COMPATIBILITY;").await.unwrap();
        let rows = shown.results[0].data["incompatible_reports"]
            .as_array()
            .unwrap();
        assert_eq!(rows.len(), MAX_INCOMPATIBLE_REPORTS);
        assert!(!rows.iter().any(|row| row["node"] == "ghost-0000"));
        assert!(rows.iter().any(|row| row["node"] == "ghost-9999"));
    }

    #[tokio::test]
    async fn incompatible_reports_replay_identically_across_replicas() {
        let directory_a = TempDir::new().unwrap();
        let directory_b = TempDir::new().unwrap();
        let replica_a = new_controller(&directory_a);
        let replica_b = new_controller(&directory_b);
        // One committed command applied at two replicas must land
        // identical evidence — `refused_at_ns` is the command's committed
        // issued_at, never local wall time.
        let prepared = replica_a
            .prepare_replicated(
                AuditActor::System,
                Uuid::new_v4(),
                4_242,
                versioned_capacity_report(
                    "ghost-9",
                    crate::node::NODE_PROTOCOL_VERSION + 1,
                    "99.0.0",
                ),
            )
            .await
            .unwrap();
        replica_a.apply_replicated(prepared.clone()).await;
        replica_b.apply_replicated(prepared).await;
        assert_eq!(
            replica_a.snapshot_bytes().await.unwrap(),
            replica_b.snapshot_bytes().await.unwrap()
        );
    }

    #[tokio::test]
    async fn catalog_open_and_snapshot_install_refuse_a_newer_schema() {
        // Open: persisted state written by a newer binary is a rollback
        // across an irreversible catalog migration — refuse to boot
        // rather than misread it.
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("catalog.json");
        let mut catalog = serde_json::to_value(CatalogState::default()).unwrap();
        catalog["schema_version"] = json!(CATALOG_SCHEMA_VERSION + 1);
        std::fs::write(&path, serde_json::to_vec(&catalog).unwrap()).unwrap();
        let store: Arc<dyn LogStore> =
            Arc::new(FileLogStore::open(directory.path().join("data")).unwrap());
        let error = ControlController::open_with_storage_nodes(path, store, vec![])
            .err()
            .unwrap();
        assert!(error.to_string().contains("irreversible"));

        // Install: the same boundary applies to a snapshot arriving over
        // replication — a lagging replica must not ingest newer state.
        let directory = TempDir::new().unwrap();
        let controller = new_controller(&directory);
        let mut snapshot: serde_json::Value =
            serde_json::from_slice(&controller.snapshot_bytes().await.unwrap()).unwrap();
        snapshot["schema_version"] = json!(CATALOG_SCHEMA_VERSION + 1);
        let error = controller
            .install_snapshot_bytes(&serde_json::to_vec(&snapshot).unwrap())
            .await
            .err()
            .unwrap();
        assert!(error.to_string().contains("irreversible"));
        // A same-version snapshot still installs.
        snapshot["schema_version"] = json!(CATALOG_SCHEMA_VERSION);
        controller
            .install_snapshot_bytes(&serde_json::to_vec(&snapshot).unwrap())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn legacy_state_and_commands_without_version_fields_decode() {
        // A pre-upgrade catalog — no `incompatible_reports` key and no
        // version fields inside capacity reports — still opens.
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("catalog.json");
        let mut catalog = serde_json::to_value(CatalogState::default()).unwrap();
        catalog
            .as_object_mut()
            .unwrap()
            .remove("incompatible_reports");
        std::fs::write(&path, serde_json::to_vec(&catalog).unwrap()).unwrap();
        let store: Arc<dyn LogStore> =
            Arc::new(FileLogStore::open(directory.path().join("data")).unwrap());
        ControlController::open_with_storage_nodes(path, store, vec![]).unwrap();

        // A pre-upgrade replicated command — no protocol_version/build —
        // decodes as an unversioned level-0 report and passes the gate.
        let legacy = serde_json::json!({
            "command": "report_storage_node_capacity",
            "node": "storage-1",
            "capacity": {
                "cpu_cores": 1,
                "memory_bytes": 1,
                "disk_total_bytes": 1,
                "disk_free_bytes": 1,
            },
        });
        let command: Command = serde_json::from_value(legacy).unwrap();
        match command {
            Command::ReportStorageNodeCapacity {
                protocol_version,
                build,
                ..
            } => {
                assert_eq!(protocol_version, 0);
                assert_eq!(build, "");
            }
            _ => panic!("expected ReportStorageNodeCapacity"),
        }
    }

    #[tokio::test]
    async fn registered_nodes_carry_capability_descriptors_and_exclusion_reasons() {
        let directory = TempDir::new().unwrap();
        let controller = new_controller(&directory);
        // A registered Node declares its roles; storage must be one of
        // them because the registration itself is a storage claim.
        let registered = controller
            .execute(
                "REGISTER STORAGE NODE store-4 AT http://store-4:7070 \
                 WITH CAPABILITIES storage,compute;",
            )
            .await
            .unwrap();
        let record = &registered.results[0].data;
        assert_eq!(record["capabilities"]["storage"], true);
        assert_eq!(record["capabilities"]["compute"], true);
        assert_eq!(record["capabilities"]["gateway"], false);
        assert!(controller
            .execute(
                "REGISTER STORAGE NODE store-5 AT http://store-5:7070 \
                 WITH CAPABILITIES gateway;"
            )
            .await
            .is_err());
        // Re-registration refreshes the descriptor alongside pin rotation.
        let updated = controller
            .execute(
                "REGISTER STORAGE NODE store-4 AT http://store-4:7070 \
                 WITH CAPABILITIES storage,compute,gateway;",
            )
            .await
            .unwrap();
        assert_eq!(updated.results[0].data["capabilities"]["gateway"], true);
        // INSPECT DRAIN reports the whole pool: the draining Node carries
        // its exclusion reason, eligible Nodes carry none.
        controller
            .execute(
                "REGISTER STORAGE NODE store-5 AT http://store-5:7070; \
                 DRAIN STORAGE NODE store-5;",
            )
            .await
            .unwrap();
        let inspected = controller
            .execute("INSPECT DRAIN FOR STORAGE NODE store-5;")
            .await
            .unwrap();
        let pool = inspected.results[0].data["storage_pool"]
            .as_array()
            .unwrap();
        let entry = pool
            .iter()
            .find(|entry| entry["node"] == "store-5")
            .unwrap();
        assert_eq!(entry["eligible"], false);
        assert_eq!(entry["exclusions"], json!(["draining"]));
        let healthy = pool
            .iter()
            .find(|entry| entry["node"] == "storage-1")
            .unwrap();
        assert_eq!(healthy["eligible"], true);
        assert_eq!(healthy["exclusions"], json!([]));
        // A retired Node is still visible with its exclusion until the
        // identity is re-registered.
        controller
            .execute("UNDRAIN STORAGE NODE store-5; RETIRE STORAGE NODE store-5;")
            .await
            .unwrap();
        let inspected = controller
            .execute("INSPECT DRAIN FOR STORAGE NODE store-4;")
            .await
            .unwrap();
        let pool = inspected.results[0].data["storage_pool"]
            .as_array()
            .unwrap();
        let entry = pool
            .iter()
            .find(|entry| entry["node"] == "store-5")
            .unwrap();
        assert_eq!(entry["eligible"], false);
        assert_eq!(entry["exclusions"], json!(["retired"]));
        // SHOW STORAGE NODES shares the same eligibility report.
        let shown = controller.execute("SHOW STORAGE NODES;").await.unwrap();
        let rows = shown.results[0].data.as_array().unwrap();
        let shown_entry = rows
            .iter()
            .find(|entry| entry["node"] == "store-5")
            .unwrap();
        assert_eq!(shown_entry["eligible"], false);
        assert_eq!(shown_entry["exclusions"], json!(["retired"]));
        let shown4 = rows
            .iter()
            .find(|entry| entry["node"] == "store-4")
            .unwrap();
        assert_eq!(shown4["capabilities"]["storage"], true);
    }

    #[tokio::test]
    async fn legacy_pipes_synthesize_their_lease_from_the_progress_owner() {
        let directory = TempDir::new().unwrap();
        let controller = new_controller(&directory);
        controller
            .execute(
                "CREATE DOMAIN accounts; CREATE FEED accounts.events; \
                 CREATE FEED accounts.enriched; CREATE SUBSCRIPTION accounts.flow \
                 FROM accounts.events; \
                 CREATE PIPE accounts.forward FROM SUBSCRIPTION accounts.flow TO FEED accounts.enriched;",
            )
            .await
            .unwrap();
        let pipe = controller
            .active_pipe_by_name("accounts.forward")
            .await
            .unwrap();
        // Simulate a catalog written before compute leases existed.
        controller.state.lock().await.pipe_compute_leases.clear();
        let progress = controller
            .active_subscription_progress_assignment_by_id(pipe.subscription_id)
            .await
            .unwrap();
        let lease = controller.pipe_compute_lease(pipe.pipe_id).await.unwrap();
        assert_eq!(lease.owner, progress.owner);
        assert_eq!(lease.compute_epoch, progress.ownership_epoch);
        // A legacy Pipe still takes an explicit move: the synthesized
        // epoch is the CAS base.
        let target = ["storage-1", "storage-2", "storage-3"]
            .into_iter()
            .map(|node| StorageNodeId::try_new(node).unwrap())
            .find(|node| *node != lease.owner)
            .unwrap();
        controller
            .execute_commands(vec![Command::MovePipeComputeLease {
                pipe: "accounts.forward".to_owned(),
                new_owner: target.clone(),
                expected_compute_epoch: Some(lease.compute_epoch),
            }])
            .await
            .unwrap();
        let moved = controller.pipe_compute_lease(pipe.pipe_id).await.unwrap();
        assert_eq!(moved.owner, target);
        assert_eq!(moved.compute_epoch, progress.ownership_epoch + 1);
    }

    #[tokio::test]
    async fn issued_api_keys_persist_digest_only_and_drive_admin_view() {
        let directory = TempDir::new().unwrap();
        let controller = new_controller(&directory);
        let issued_secret = "issued-release-key-0123456789abcdef";
        let digest = blake3::hash(issued_secret.as_bytes()).to_hex().to_string();
        controller
            .execute("CREATE ROLE ops.platform; GRANT manage ON NAMESPACE * TO ROLE ops.platform;")
            .await
            .unwrap();
        // Issue binds the digest verifier to the Role; plaintext never
        // appears in the command or catalog.
        let issued = controller
            .execute(&format!(
                "ISSUE API KEY ops.release ROLE ops.platform DIGEST {digest};"
            ))
            .await
            .unwrap();
        let identity = &issued.results[0].data;
        assert_eq!(identity["digest"].as_str().unwrap(), digest);
        assert_eq!(identity["status"].as_str().unwrap(), "active");
        // The authorization view marks the key riverbed-admin through
        // its Role's `manage` grant on `*`.
        {
            let view = controller.issued_api_keys();
            let entry = view
                .get(blake3::hash(issued_secret.as_bytes()).as_bytes())
                .expect("issued digest must enter the authorization view");
            assert!(entry.riverbed_admin);
            assert_eq!(entry.name, "ops.release");
        }
        // An identical replay is idempotent; a conflicting re-issue is refused.
        controller
            .execute(&format!(
                "ISSUE API KEY ops.release ROLE ops.platform DIGEST {digest};"
            ))
            .await
            .unwrap();
        let other_digest = blake3::hash(b"different-secret-material-01234")
            .to_hex()
            .to_string();
        assert!(controller
            .execute(&format!(
                "ISSUE API KEY ops.release ROLE ops.platform DIGEST {other_digest};"
            ))
            .await
            .is_err());
        // A Domain-scoped Role does not grant riverbed admin.
        controller
            .execute("CREATE ROLE ops.reader; GRANT read ON NAMESPACE orders.* TO ROLE ops.reader;")
            .await
            .unwrap();
        let reader_digest = blake3::hash(b"reader-secret-material-0123456")
            .to_hex()
            .to_string();
        controller
            .execute(&format!(
                "ISSUE API KEY ops.readkey ROLE ops.reader DIGEST {reader_digest};"
            ))
            .await
            .unwrap();
        {
            let view = controller.issued_api_keys();
            let entry = view
                .get(blake3::hash(b"reader-secret-material-0123456").as_bytes())
                .expect("reader key must enter the authorization view");
            assert!(!entry.riverbed_admin);
        }
        // SHOW API KEYS masks the verifier to a prefix and resolves the Role.
        let shown = controller.execute("SHOW API KEYS;").await.unwrap();
        let rows = shown.results[0].data.as_array().unwrap();
        assert_eq!(rows.len(), 2);
        let release = rows
            .iter()
            .find(|row| row["name"] == "ops.release")
            .unwrap();
        assert_eq!(release["role"].as_str().unwrap(), "ops.platform");
        assert_eq!(release["digest_prefix"].as_str().unwrap(), &digest[..16]);
        assert!(release.get("digest").is_none());
        // Revocation is durable and removes the digest from the view.
        controller
            .execute("REVOKE API KEY ops.release;")
            .await
            .unwrap();
        assert!(controller
            .issued_api_keys()
            .get(blake3::hash(issued_secret.as_bytes()).as_bytes())
            .is_none());
        // Revoked names can be re-issued (rotation), while revoking an
        // unknown key fails closed.
        controller
            .execute(&format!(
                "ISSUE API KEY ops.release ROLE ops.platform DIGEST {digest};"
            ))
            .await
            .unwrap();
        assert!(controller
            .execute("REVOKE API KEY ops.release;")
            .await
            .is_ok());
        assert!(controller
            .execute("REVOKE API KEY ops.release;")
            .await
            .is_err());
        // Expiry must lie in the future at issue time.
        assert!(controller
            .execute(&format!(
                "ISSUE API KEY ops.expired ROLE ops.platform DIGEST {digest} EXPIRES AT 1;"
            ))
            .await
            .is_err());
        // Malformed digests and unknown Roles fail closed.
        assert!(controller
            .execute("ISSUE API KEY ops.bad ROLE ops.platform DIGEST deadbeef;")
            .await
            .is_err());
        assert!(controller
            .execute(&format!(
                "ISSUE API KEY ops.orphan ROLE ops.missing DIGEST {digest};"
            ))
            .await
            .is_err());
        // The catalog persists identities across reopen (digest only).
        let reopened = new_controller(&directory);
        let view = reopened.issued_api_keys();
        assert!(view
            .get(blake3::hash(b"reader-secret-material-0123456").as_bytes())
            .is_some());
        assert!(view
            .get(blake3::hash(issued_secret.as_bytes()).as_bytes())
            .is_none());
    }

    #[tokio::test]
    async fn root_namespace_pattern_matches_every_feed() {
        let directory = TempDir::new().unwrap();
        let controller = new_controller(&directory);
        let result = controller
            .execute(
                "CREATE DOMAIN orders; CREATE FEED orders.events;                  CREATE ROLE ops.everything;                  GRANT manage ON NAMESPACE * TO ROLE ops.everything;                  EXPLAIN ACCESS FOR ROLE ops.everything ACTION MANAGE ON FEED orders.events;",
            )
            .await
            .unwrap();
        assert_eq!(result.results[4].data["allowed"], true);
        assert!(namespace_matches("*", "orders.events"));
        assert!(namespace_matches("*", "anything.deep"));
        assert!(validate_namespace_pattern("*").is_ok());
    }

    #[test]
    fn domain_spanning_grant_forms_cover_only_the_named_domain() {
        assert!(grant_covers_domain("*", "orders"));
        assert!(grant_covers_domain("orders.*", "orders"));
        // An exact pattern names one resource, never the Domain itself.
        assert!(!grant_covers_domain("orders", "orders"));
        assert!(!grant_covers_domain("billing.*", "orders"));
        // orders.eu.* spans only the orders.eu subdomain, not orders.
        assert!(!grant_covers_domain("orders.eu.*", "orders"));
        assert!(grant_covers_domain("orders.eu.*", "orders.eu"));
    }

    #[tokio::test]
    async fn deny_grants_fence_covering_allows_and_coexist() {
        let directory = tempfile::tempdir().unwrap();
        let controller = new_controller(&directory);
        controller
            .execute(
                "CREATE DOMAIN orders; CREATE FEED orders.events; \
                 CREATE ROLE ops.reader; \
                 GRANT read ON NAMESPACE orders.* TO ROLE ops.reader; \
                 DENY read ON NAMESPACE orders.events TO ROLE ops.reader;",
            )
            .await
            .unwrap();
        // DENY reuses the GRANT grammar and lands as a typed Grant
        // command with the deny effect.
        let parsed =
            parse_wcl("DENY read, write ON NAMESPACE orders.* TO ROLE ops.reader;").unwrap();
        assert!(matches!(
            &parsed[0],
            Command::Grant {
                effect: GrantEffect::Deny,
                actions,
                ..
            } if actions.len() == 2
        ));
        // Allow and deny coexist on the same (Role, namespace, action)
        // triple because dedupe includes the effect; re-denying an
        // identical row is refused as a duplicate.
        assert!(controller
            .execute("DENY read ON NAMESPACE orders.events TO ROLE ops.reader;")
            .await
            .is_err());
        controller
            .execute("GRANT read ON NAMESPACE orders.events TO ROLE ops.reader;")
            .await
            .unwrap();
        // An issued key bound to the Role sees both effects in the
        // request-time view, so the exact deny fences the wildcard
        // allow at authorization time.
        let digest = blake3::hash(b"reader-deny-key-0123456789abcdef")
            .to_hex()
            .to_string();
        controller
            .execute(&format!(
                "ISSUE API KEY ops.denykey ROLE ops.reader DIGEST {digest};"
            ))
            .await
            .unwrap();
        let key_digest = *blake3::hash(b"reader-deny-key-0123456789abcdef").as_bytes();
        {
            let view = controller.issued_api_keys();
            let identity = view.get(&key_digest).expect("issued identity");
            let effects: BTreeSet<_> = identity.grants.iter().map(|g| g.effect).collect();
            assert!(effects.contains(&GrantEffect::Allow));
            assert!(effects.contains(&GrantEffect::Deny));
        }
        // EXPLAIN ACCESS reports the covering deny and refuses.
        let explained = controller
            .execute("EXPLAIN ACCESS FOR ROLE ops.reader ACTION read ON FEED orders.events;")
            .await
            .unwrap();
        assert_eq!(explained.results[0].data["allowed"], false);
        assert!(!explained.results[0].data["matched_denies"]
            .as_array()
            .unwrap()
            .is_empty());
        // The deny survives a catalog reopen through persistence.
        drop(controller);
        let reopened = new_controller(&directory);
        let view = reopened.issued_api_keys();
        let identity = view.get(&key_digest).expect("issued identity");
        let effects: BTreeSet<_> = identity.grants.iter().map(|g| g.effect).collect();
        assert!(effects.contains(&GrantEffect::Deny));
    }

    #[tokio::test]
    async fn revoke_subtracts_granted_actions_and_deny_rows() {
        let directory = tempfile::tempdir().unwrap();
        let controller = new_controller(&directory);
        controller
            .execute(
                "CREATE DOMAIN orders; CREATE FEED orders.events; CREATE FEED orders.other;                  CREATE ROLE ops.reader;                  GRANT read, write ON NAMESPACE orders.* TO ROLE ops.reader;                  DENY read ON NAMESPACE orders.events TO ROLE ops.reader;",
            )
            .await
            .unwrap();
        // REVOKE parses through the typed command as an allow effect.
        let parsed = parse_wcl("REVOKE read ON NAMESPACE orders.* FROM ROLE ops.reader;").unwrap();
        assert!(matches!(
            &parsed[0],
            Command::RevokeGrant {
                effect: GrantEffect::Allow,
                ..
            }
        ));
        // Revoking read subtracts it — write survives on the same row.
        controller
            .execute("REVOKE read ON NAMESPACE orders.* FROM ROLE ops.reader;")
            .await
            .unwrap();
        let explained = controller
            .execute("EXPLAIN ACCESS FOR ROLE ops.reader ACTION write ON FEED orders.other;")
            .await
            .unwrap();
        assert_eq!(explained.results[0].data["allowed"], true);
        let explained = controller
            .execute("EXPLAIN ACCESS FOR ROLE ops.reader ACTION read ON FEED orders.other;")
            .await
            .unwrap();
        assert_eq!(explained.results[0].data["allowed"], false);
        // REVOKE DENY removes the covering deny; the surviving write
        // allow then covers orders.events for write only.
        controller
            .execute("REVOKE DENY read ON NAMESPACE orders.events FROM ROLE ops.reader;")
            .await
            .unwrap();
        let explained = controller
            .execute("EXPLAIN ACCESS FOR ROLE ops.reader ACTION write ON FEED orders.events;")
            .await
            .unwrap();
        assert_eq!(explained.results[0].data["allowed"], true);
        // Revoking an action no remaining row contains, an unknown
        // Role, or a namespace with no grants fails closed.
        for script in [
            "REVOKE read ON NAMESPACE orders.* FROM ROLE ops.reader;",
            "REVOKE read ON NAMESPACE billing.* FROM ROLE ops.reader;",
            "REVOKE read ON NAMESPACE orders.* FROM ROLE ops.missing;",
        ] {
            assert!(
                controller.execute(script).await.is_err(),
                "{script} must fail"
            );
        }
        // The surviving write grant persists a catalog reopen.
        drop(controller);
        let reopened = new_controller(&directory);
        let explained = reopened
            .execute("EXPLAIN ACCESS FOR ROLE ops.reader ACTION write ON FEED orders.other;")
            .await
            .unwrap();
        assert_eq!(explained.results[0].data["allowed"], true);
    }

    #[test]
    fn activate_commands_deserialize_without_subscription_translations() {
        // Log entries written before translations rode the activation must
        // still replay: the field defaults to an empty pending set.
        let split: Command = serde_json::from_value(json!({
            "command": "activate_active_range_split",
            "feed": "orders.created",
            "plan_id": Uuid::nil(),
            "left_writer_sequences": [],
            "right_writer_sequences": [],
            "reader_translations": []
        }))
        .unwrap();
        assert!(matches!(
            split,
            Command::ActivateActiveRangeSplit {
                subscription_translations,
                ..
            } if subscription_translations.is_empty()
        ));
        let merge: Command = serde_json::from_value(json!({
            "command": "activate_active_range_merge",
            "feed": "orders.created",
            "plan_id": Uuid::nil(),
            "writer_sequences": [],
            "reader_translations": []
        }))
        .unwrap();
        assert!(matches!(
            merge,
            Command::ActivateActiveRangeMerge {
                subscription_translations,
                ..
            } if subscription_translations.is_empty()
        ));
        // The pending-clear command is internal cutover machinery: it must be
        // unreachable from the public command batch endpoint.
        let clear = Command::ClearSubscriptionCutover {
            subscription_id: Uuid::nil(),
            plan_id: Uuid::nil(),
        };
        assert!(clear.requires_internal_replica_authority());
        assert_eq!(command_label(&clear), "CLEAR SUBSCRIPTION CUTOVER");
    }

    #[tokio::test]
    async fn committed_frontier_proofs_advance_only_and_refuse_fork_evidence() {
        let directory = TempDir::new().unwrap();
        let controller = new_controller(&directory);
        controller
            .execute("CREATE SPACE orders; CREATE FEED orders.events;")
            .await
            .unwrap();
        let feed_id = controller
            .active_feed_by_name("orders.events")
            .await
            .unwrap()
            .feed_id;
        let assignment = controller
            .active_range_assignments_for_feed(feed_id)
            .await
            .pop()
            .unwrap();
        let reporter = assignment.owner.clone();
        let proof = |position: u64, digest: u8, request_id: u128| {
            (
                Command::RecordRangeCommitProof {
                    feed_id,
                    range_id: assignment.range_id,
                    position: CommitPosition::new(position),
                    frame_digest: [digest; 32],
                    evidence: assignment.replicas.iter().cloned().collect(),
                    reporter: reporter.clone(),
                },
                Uuid::from_u128(request_id),
            )
        };

        // First report records the quorum-committed frontier.
        let (command, request_id) = proof(5, 7, 1);
        controller
            .execute_commands_with_request_id(vec![command], request_id)
            .await
            .unwrap();
        let recorded = controller
            .range_commit_proof(assignment.range_id)
            .await
            .unwrap();
        assert_eq!(recorded.position, CommitPosition::new(5));
        assert_eq!(recorded.frame_digest, [7; 32]);

        // A replayed request dedups instead of reapplying.
        let (command, request_id) = proof(5, 7, 1);
        let replay = controller
            .execute_commands_with_request_id(vec![command], request_id)
            .await
            .unwrap();
        assert_eq!(replay.results[0].data["position"], 5);

        // A stale position is a no-op that keeps the newer proof.
        let (command, request_id) = proof(3, 9, 2);
        controller
            .execute_commands_with_request_id(vec![command], request_id)
            .await
            .unwrap();
        assert_eq!(
            controller
                .range_commit_proof(assignment.range_id)
                .await
                .unwrap()
                .position,
            CommitPosition::new(5)
        );

        // A different digest at the same position is fork evidence.
        let (command, request_id) = proof(5, 9, 3);
        let conflict = controller
            .execute_commands_with_request_id(vec![command], request_id)
            .await
            .unwrap_err();
        assert!(conflict.to_string().contains("fork evidence"));

        // A newer position advances and unions quorum evidence.
        let (command, request_id) = proof(9, 11, 4);
        controller
            .execute_commands_with_request_id(vec![command], request_id)
            .await
            .unwrap();
        let recorded = controller
            .range_commit_proof(assignment.range_id)
            .await
            .unwrap();
        assert_eq!(recorded.position, CommitPosition::new(9));
        assert_eq!(recorded.frame_digest, [11; 32]);

        // Internal authority only: the public command batch refuses it.
        let (command, _) = proof(12, 13, 5);
        assert!(command.requires_internal_replica_authority());

        // A proof for a Feed the catalog never knew is refused, not
        // recorded as phantom history.
        let phantom = Command::RecordRangeCommitProof {
            feed_id: Uuid::from_u128(99_999),
            range_id: assignment.range_id,
            position: CommitPosition::new(1),
            frame_digest: [1; 32],
            evidence: vec![reporter.clone()],
            reporter,
        };
        assert!(controller
            .execute_commands_with_request_id(vec![phantom], Uuid::from_u128(6))
            .await
            .is_err());
    }

    #[test]
    fn parser_supports_history_policy_verbs() {
        let commands = parse_wcl(
            "SET HISTORY POLICY ON DOMAIN orders TIER AFTER 30m EXPIRE AFTER 7d;              SET HISTORY POLICY ON FEED orders.events EXPIRE AFTER 14d MAX RETAINED BYTES 4096;              DROP HISTORY POLICY ON FEED orders.events;              DROP HISTORY POLICY ON DOMAIN orders;              SHOW HISTORY POLICIES;",
        )
        .unwrap();
        assert_eq!(commands.len(), 5);
        assert!(matches!(
            &commands[0],
            Command::SetHistoryPolicy {
                scope: PolicyScope::Domain { name },
                policy
            } if name == "orders"
                && policy.tier_after_ns == Some(30 * 60 * 1_000_000_000)
                && policy.expire_after_ns == Some(7 * 86_400 * 1_000_000_000)
                && policy.max_retained_bytes.is_none()
        ));
        assert!(matches!(
            &commands[1],
            Command::SetHistoryPolicy {
                scope: PolicyScope::Feed { name },
                policy
            } if name == "orders.events"
                && policy.tier_after_ns.is_none()
                && policy.expire_after_ns == Some(14 * 86_400 * 1_000_000_000)
                && policy.max_retained_bytes == Some(4_096)
        ));
        assert!(matches!(
            &commands[2],
            Command::ClearHistoryPolicy {
                scope: PolicyScope::Feed { name }
            } if name == "orders.events"
        ));
        assert!(matches!(
            &commands[3],
            Command::ClearHistoryPolicy {
                scope: PolicyScope::Domain { name }
            } if name == "orders"
        ));
        assert!(matches!(
            &commands[4],
            Command::Show {
                kind: ShowKind::HistoryPolicies,
                ..
            }
        ));
        for bad in [
            "SET HISTORY POLICY ON FEED orders.events;",
            "SET HISTORY POLICY ON orders TIER AFTER 1h;",
            "SET HISTORY POLICY ON FEED orders.events TIER 1h;",
            "SET HISTORY POLICY ON FEED orders.events MAX RETAINED BYTES 0;",
            "SET HISTORY POLICY ON FEED orders.events TIER AFTER 0s;",
            "DROP HISTORY POLICY ON FEED;",
            "DROP HISTORY ON FEED orders.events;",
            "SHOW HISTORY;",
        ] {
            assert!(parse_wcl(bad).is_err(), "{bad} must not parse");
        }
    }

    #[tokio::test]
    async fn history_policy_lifecycle_inherits_overrides_and_survives_reopen() {
        let directory = TempDir::new().unwrap();
        let leader = new_controller(&directory);
        leader
            .execute("CREATE DOMAIN orders; CREATE FEED orders.events;")
            .await
            .unwrap();
        leader
            .execute("SET HISTORY POLICY ON DOMAIN orders TIER AFTER 30m EXPIRE AFTER 7d;")
            .await
            .unwrap();
        leader
            .execute("SET HISTORY POLICY ON FEED orders.events EXPIRE AFTER 14d;")
            .await
            .unwrap();

        let feed_id = leader
            .active_feed_by_name("orders.events")
            .await
            .unwrap()
            .feed_id;
        let effective = leader
            .effective_history_policy_for_feed(feed_id)
            .await
            .unwrap();
        // The Feed override layers over the Domain's: expire from the
        // Feed, tier inherited, bytes undeclared anywhere.
        assert_eq!(effective.expire_after_ns, Some(14 * 86_400 * 1_000_000_000));
        assert_eq!(effective.tier_after_ns, Some(30 * 60 * 1_000_000_000));
        assert_eq!(effective.max_retained_bytes, None);

        let shown = leader.execute("SHOW HISTORY POLICIES;").await.unwrap();
        let rows: Vec<Value> = serde_json::from_value(shown.results[0].data.clone()).unwrap();
        assert_eq!(rows.len(), 2);

        let described = leader
            .execute("DESCRIBE FEED orders.events;")
            .await
            .unwrap();
        let detail = &described.results[0].data;
        assert_eq!(
            detail["history_policy"]["expire_after_ns"],
            json!(14 * 86_400 * 1_000_000_000_u64)
        );
        assert!(detail["history_policy"]["tier_after_ns"].is_null());
        assert_eq!(
            detail["effective_history_policy"]["tier_after_ns"],
            json!(30 * 60 * 1_000_000_000_u64)
        );

        // Catalog state survives a controller reopen — a synced replica
        // must resolve the same effective policy.
        drop(leader);
        let reopened = new_controller(&directory);
        let effective = reopened
            .effective_history_policy_for_feed(feed_id)
            .await
            .unwrap();
        assert_eq!(effective.expire_after_ns, Some(14 * 86_400 * 1_000_000_000));
        assert_eq!(effective.tier_after_ns, Some(30 * 60 * 1_000_000_000));

        // Renaming the Feed keeps its policy — rows key on the id, not
        // the mutable name.
        reopened
            .execute("RENAME FEED orders.events TO orders.audit;")
            .await
            .unwrap();
        let effective = reopened
            .effective_history_policy_for_feed(feed_id)
            .await
            .unwrap();
        assert_eq!(effective.expire_after_ns, Some(14 * 86_400 * 1_000_000_000));

        // Clearing reverts the Feed to pure Domain inheritance.
        reopened
            .execute("DROP HISTORY POLICY ON FEED orders.audit;")
            .await
            .unwrap();
        let effective = reopened
            .effective_history_policy_for_feed(feed_id)
            .await
            .unwrap();
        assert_eq!(effective.expire_after_ns, Some(7 * 86_400 * 1_000_000_000));
        assert!(reopened
            .execute("DROP HISTORY POLICY ON FEED orders.audit;")
            .await
            .is_err());
    }

    #[test]
    fn parser_supports_legal_hold_verbs() {
        let commands = parse_wcl(
            "SET LEGAL HOLD ON FEED orders.events;              SET LEGAL HOLD ON FEED orders.audit REASON 'matter 2024-118';              DROP LEGAL HOLD ON FEED orders.events;              SHOW LEGAL HOLDS;",
        )
        .unwrap();
        assert_eq!(commands.len(), 4);
        assert!(matches!(
            &commands[0],
            Command::SetLegalHold { feed, reason }
                if feed == "orders.events" && reason.is_none()
        ));
        assert!(matches!(
            &commands[1],
            Command::SetLegalHold { feed, reason }
                if feed == "orders.audit" && reason.as_deref() == Some("matter 2024-118")
        ));
        assert!(matches!(
            &commands[2],
            Command::DropLegalHold { feed } if feed == "orders.events"
        ));
        assert!(matches!(
            &commands[3],
            Command::Show {
                kind: ShowKind::LegalHolds,
                ..
            }
        ));
        // Malformed shapes stay syntax errors, not silent misreads.
        assert!(parse_wcl("SET LEGAL HOLD ON DOMAIN orders;").is_err());
        assert!(parse_wcl("SET LEGAL HOLD orders.events;").is_err());
        assert!(parse_wcl("DROP LEGAL HOLD orders.events;").is_err());
    }

    #[tokio::test]
    async fn legal_hold_fences_drop_and_survives_rename_and_reopen() {
        let directory = TempDir::new().unwrap();
        let leader = new_controller(&directory);
        leader
            .execute("CREATE DOMAIN orders; CREATE FEED orders.events;")
            .await
            .unwrap();
        let feed_id = leader
            .active_feed_by_name("orders.events")
            .await
            .unwrap()
            .feed_id;

        leader
            .execute("SET LEGAL HOLD ON FEED orders.events REASON 'matter 118';")
            .await
            .unwrap();
        let hold = leader.legal_hold_for_feed(feed_id).await.unwrap();
        assert_eq!(hold.reason.as_deref(), Some("matter 118"));
        assert_eq!(leader.legal_hold_feed_ids().await.len(), 1);

        // SHOW LEGAL HOLDS + DESCRIBE FEED surface the freeze.
        let shown = leader.execute("SHOW LEGAL HOLDS;").await.unwrap();
        let rows: Vec<Value> = serde_json::from_value(shown.results[0].data.clone()).unwrap();
        assert_eq!(rows.len(), 1);
        let described = leader
            .execute("DESCRIBE FEED orders.events;")
            .await
            .unwrap();
        assert_eq!(
            described.results[0].data["legal_hold"]["feed_id"],
            json!(feed_id)
        );

        // The hold outranks drop: even an unattached Feed refuses while
        // the freeze stands — the tombstone would retire the evidence.
        let refused = leader
            .execute("DROP FEED orders.events;")
            .await
            .unwrap_err();
        assert!(refused.to_string().contains("legal hold"), "{refused}");

        // Re-declaring refreshes the reason; renaming keeps the hold
        // because rows key on the immutable FeedId.
        leader
            .execute("SET LEGAL HOLD ON FEED orders.events REASON 'matter 119';")
            .await
            .unwrap();
        leader
            .execute("RENAME FEED orders.events TO orders.audit;")
            .await
            .unwrap();
        let hold = leader.legal_hold_for_feed(feed_id).await.unwrap();
        assert_eq!(hold.reason.as_deref(), Some("matter 119"));
        assert!(leader.execute("DROP FEED orders.audit;").await.is_err());

        // A synced replica sees the same freeze after reopen.
        drop(leader);
        let reopened = new_controller(&directory);
        assert!(reopened.legal_hold_feed_ids().await.contains(&feed_id));
        assert!(reopened.execute("DROP FEED orders.audit;").await.is_err());

        // The unfreeze is explicit: drop the hold, then the Feed drops.
        reopened
            .execute("DROP LEGAL HOLD ON FEED orders.audit;")
            .await
            .unwrap();
        assert!(reopened.legal_hold_feed_ids().await.is_empty());
        reopened.execute("DROP FEED orders.audit;").await.unwrap();
    }

    #[tokio::test]
    async fn legal_hold_refuses_missing_feeds_and_absent_drops() {
        let directory = TempDir::new().unwrap();
        let leader = new_controller(&directory);
        leader
            .execute("CREATE DOMAIN orders; CREATE FEED orders.events;")
            .await
            .unwrap();
        assert!(leader
            .execute("SET LEGAL HOLD ON FEED orders.missing;")
            .await
            .is_err());
        assert!(leader
            .execute("DROP LEGAL HOLD ON FEED orders.events;")
            .await
            .is_err());
    }

    #[tokio::test]
    async fn history_policy_refuses_empty_invalid_and_dropped_targets() {
        let directory = TempDir::new().unwrap();
        let leader = new_controller(&directory);
        leader
            .execute("CREATE DOMAIN orders; CREATE FEED orders.events;")
            .await
            .unwrap();
        // An all-absent policy is a no-op declaration — refuse it so a
        // mistyped statement cannot masquerade as a cleared override.
        assert!(leader
            .execute_commands(vec![Command::SetHistoryPolicy {
                scope: PolicyScope::Feed {
                    name: "orders.events".to_owned(),
                },
                policy: HistoryPolicy::default(),
            }])
            .await
            .is_err());
        assert!(leader
            .execute_commands(vec![Command::SetHistoryPolicy {
                scope: PolicyScope::Feed {
                    name: "orders.events".to_owned(),
                },
                policy: HistoryPolicy {
                    max_retained_bytes: Some(0),
                    ..HistoryPolicy::default()
                },
            }])
            .await
            .is_err());
        for statement in [
            "SET HISTORY POLICY ON DOMAIN missing TIER AFTER 1h;",
            "SET HISTORY POLICY ON FEED orders.missing TIER AFTER 1h;",
            "DROP HISTORY POLICY ON DOMAIN missing;",
            "DROP HISTORY POLICY ON FEED orders.missing;",
        ] {
            assert!(leader.execute(statement).await.is_err(), "{statement}");
        }
        // A dropped Feed cannot take new policy, and its row leaves
        // with it.
        leader
            .execute("SET HISTORY POLICY ON FEED orders.events EXPIRE AFTER 1d;")
            .await
            .unwrap();
        leader.execute("DROP FEED orders.events;").await.unwrap();
        assert!(leader
            .execute("SET HISTORY POLICY ON FEED orders.events EXPIRE AFTER 1d;")
            .await
            .is_err());
        let shown = leader.execute("SHOW HISTORY POLICIES;").await.unwrap();
        let rows: Vec<Value> = serde_json::from_value(shown.results[0].data.clone()).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn schema_wcl_parses_set_drop_and_show() {
        let commands = parse_wcl(
            "SET SCHEMA ON DOMAIN orders VALUE JSON SUBJECT 'order-v1';              SET SCHEMA ON FEED orders.events KEY TEXT VALUE PROTOBUF SUBJECT 'com.acme.Order';              SET SCHEMA ON FEED orders.events VALUE CUSTOM msgpack;              DROP SCHEMA ON FEED orders.events;              DROP SCHEMA ON DOMAIN orders;              SHOW SCHEMAS;",
        )
        .unwrap();
        assert_eq!(commands.len(), 6);
        assert!(matches!(
            &commands[0],
            Command::SetSchema {
                scope: PolicyScope::Domain { name },
                schema
            } if name == "orders"
                && schema.value.as_ref().is_some_and(|decl| decl.format == "json"
                    && decl.subject.as_deref() == Some("order-v1"))
                && schema.key.is_none()
        ));
        assert!(matches!(
            &commands[1],
            Command::SetSchema {
                scope: PolicyScope::Feed { name },
                schema
            } if name == "orders.events"
                && schema.key.as_ref().is_some_and(|decl| decl.format == "text")
                && schema.value.as_ref().is_some_and(|decl| decl.format == "protobuf"
                    && decl.subject.as_deref() == Some("com.acme.Order"))
        ));
        assert!(matches!(
            &commands[2],
            Command::SetSchema {
                scope: PolicyScope::Feed { name },
                schema
            } if name == "orders.events"
                && schema.value.as_ref().is_some_and(|decl| decl.format == "custom.msgpack")
        ));
        assert!(matches!(
            &commands[3],
            Command::ClearSchema {
                scope: PolicyScope::Feed { name }
            } if name == "orders.events"
        ));
        assert!(matches!(
            &commands[4],
            Command::ClearSchema {
                scope: PolicyScope::Domain { name }
            } if name == "orders"
        ));
        assert!(matches!(
            &commands[5],
            Command::Show {
                kind: ShowKind::Schemas,
                ..
            }
        ));
        for bad in [
            "SET SCHEMA ON FEED orders.events;",
            "SET SCHEMA ON orders VALUE JSON;",
            "SET SCHEMA ON FEED orders.events VALUE;",
            "SET SCHEMA ON FEED orders.events VALUE YAML;",
            "SET SCHEMA ON FEED orders.events VALUE JSON GARBAGE 'x';",
            "SET SCHEMA ON FEED orders.events VALUE CUSTOM 'Bad Name';",
            "DROP SCHEMA ON FEED;",
            "DROP SCHEMA orders.events;",
            "SHOW SCHEMA;",
        ] {
            assert!(parse_wcl(bad).is_err(), "{bad} must not parse");
        }
    }

    #[tokio::test]
    async fn schema_lifecycle_inherits_overrides_and_survives_reopen() {
        let directory = TempDir::new().unwrap();
        let leader = new_controller(&directory);
        leader
            .execute("CREATE DOMAIN orders; CREATE FEED orders.events;")
            .await
            .unwrap();
        leader
            .execute("SET SCHEMA ON DOMAIN orders VALUE JSON SUBJECT 'order-v1';")
            .await
            .unwrap();
        leader
            .execute("SET SCHEMA ON FEED orders.events KEY TEXT;")
            .await
            .unwrap();

        // The Feed override layers over the Domain's: key from the
        // Feed, value inherited.
        let described = leader
            .execute("DESCRIBE FEED orders.events;")
            .await
            .unwrap();
        let detail = &described.results[0].data;
        assert_eq!(detail["schema"]["key"]["format"], json!("text"));
        assert!(detail["schema"]["value"].is_null());
        assert_eq!(detail["effective_schema"]["key"]["format"], json!("text"));
        assert_eq!(detail["effective_schema"]["value"]["format"], json!("json"));
        assert_eq!(
            detail["effective_schema"]["value"]["subject"],
            json!("order-v1")
        );

        // SHOW FEEDS rows carry the same declared + effective schema —
        // the Domain snapshot SDKs build on.
        let shown = leader.execute("SHOW FEEDS;").await.unwrap();
        let rows: Vec<Value> = serde_json::from_value(shown.results[0].data.clone()).unwrap();
        let feed_row = rows
            .iter()
            .find(|row| row["name"] == "orders.events")
            .unwrap();
        assert_eq!(feed_row["effective_schema"]["key"]["format"], json!("text"));

        let schemas = leader.execute("SHOW SCHEMAS;").await.unwrap();
        let rows: Vec<Value> = serde_json::from_value(schemas.results[0].data.clone()).unwrap();
        assert_eq!(rows.len(), 2);

        // Catalog state survives a controller reopen — a synced replica
        // resolves the same effective schema.
        drop(leader);
        let reopened = new_controller(&directory);
        let described = reopened
            .execute("DESCRIBE FEED orders.events;")
            .await
            .unwrap();
        assert_eq!(
            described.results[0].data["effective_schema"]["key"]["format"],
            json!("text")
        );

        // Renaming the Feed keeps its schema — rows key on the id.
        reopened
            .execute("RENAME FEED orders.events TO orders.audit;")
            .await
            .unwrap();
        let described = reopened
            .execute("DESCRIBE FEED orders.audit;")
            .await
            .unwrap();
        assert_eq!(
            described.results[0].data["effective_schema"]["key"]["format"],
            json!("text")
        );

        // Clearing reverts the Feed to pure Domain inheritance.
        reopened
            .execute("DROP SCHEMA ON FEED orders.audit;")
            .await
            .unwrap();
        let described = reopened
            .execute("DESCRIBE FEED orders.audit;")
            .await
            .unwrap();
        assert!(described.results[0].data["schema"].is_null());
        assert_eq!(
            described.results[0].data["effective_schema"]["value"]["format"],
            json!("json")
        );
        assert!(reopened
            .execute("DROP SCHEMA ON FEED orders.audit;")
            .await
            .is_err());
    }

    #[tokio::test]
    async fn schema_refuses_empty_invalid_and_dropped_targets() {
        let directory = TempDir::new().unwrap();
        let leader = new_controller(&directory);
        leader
            .execute("CREATE DOMAIN orders; CREATE FEED orders.events;")
            .await
            .unwrap();
        // An all-absent schema is a no-op declaration — refuse it.
        assert!(leader
            .execute_commands(vec![Command::SetSchema {
                scope: PolicyScope::Feed {
                    name: "orders.events".to_owned(),
                },
                schema: FeedSchema::default(),
            }])
            .await
            .is_err());
        // An unknown format is refused by typed commands too — they
        // bypass the WCL parser.
        assert!(leader
            .execute_commands(vec![Command::SetSchema {
                scope: PolicyScope::Feed {
                    name: "orders.events".to_owned(),
                },
                schema: FeedSchema {
                    value: Some(SchemaDecl {
                        format: "yaml".to_owned(),
                        ..SchemaDecl::default()
                    }),
                    ..FeedSchema::default()
                },
            }])
            .await
            .is_err());
        for statement in [
            "SET SCHEMA ON DOMAIN missing VALUE JSON;",
            "SET SCHEMA ON FEED orders.missing VALUE JSON;",
            "DROP SCHEMA ON DOMAIN missing;",
            "DROP SCHEMA ON FEED orders.missing;",
        ] {
            assert!(leader.execute(statement).await.is_err(), "{statement}");
        }
        // A dropped Feed cannot take a new schema, and its row leaves
        // with it.
        leader
            .execute("SET SCHEMA ON FEED orders.events VALUE JSON;")
            .await
            .unwrap();
        leader.execute("DROP FEED orders.events;").await.unwrap();
        assert!(leader
            .execute("SET SCHEMA ON FEED orders.events VALUE JSON;")
            .await
            .is_err());
        let shown = leader.execute("SHOW SCHEMAS;").await.unwrap();
        let rows: Vec<Value> = serde_json::from_value(shown.results[0].data.clone()).unwrap();
        assert!(rows.is_empty());
    }
}
