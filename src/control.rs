use std::{
    collections::{BTreeMap, BTreeSet},
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReaderDefinition {
    pub reader_id: Uuid,
    pub name: String,
    pub feed_id: Uuid,
    pub start: ReaderStart,
    pub acknowledged_cursor: Option<String>,
    #[serde(default)]
    pub delivered_cursor: Option<String>,
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
    pub stage: SubscriptionStage,
    pub status: ResourceStatus,
    pub created_at_ns: i64,
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageNodeRecord {
    pub node: StorageNodeId,
    pub endpoint: String,
    pub registered_at_ns: i64,
    /// blake3-256 certificate fingerprints (hex) this Node may present on
    /// the internal mTLS plane; empty means no certificate is registered.
    #[serde(default)]
    pub cert_pins: BTreeSet<String>,
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
    subscription_feed_memberships: BTreeMap<Uuid, BTreeMap<Uuid, SubscriptionFeedMembership>>,
    #[serde(default)]
    state_store_assignments: BTreeMap<Uuid, crate::state_store::StateStoreAssignment>,
    #[serde(default)]
    reader_frontiers: BTreeMap<Uuid, ReaderFrontier>,
    roles: BTreeMap<Uuid, RoleDefinition>,
    grants: BTreeMap<Uuid, NamespaceGrant>,
    #[serde(default)]
    api_keys: BTreeMap<Uuid, ApiKeyIdentity>,
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
    #[serde(default)]
    applied_requests: BTreeMap<Uuid, ReplicatedCommandResult>,
}

impl CatalogState {
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
            schema_version: 1,
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
            subscription_feed_memberships: BTreeMap::new(),
            state_store_assignments: BTreeMap::new(),
            reader_frontiers: BTreeMap::new(),
            roles: BTreeMap::new(),
            grants: BTreeMap::new(),
            api_keys: BTreeMap::new(),
            active_ranges: BTreeMap::new(),
            range_maps: BTreeMap::new(),
            range_assignments: BTreeMap::new(),
            storage_nodes: BTreeMap::new(),
            retired_storage_nodes: BTreeSet::new(),
            draining_storage_nodes: BTreeSet::new(),
            range_split_plans: BTreeMap::new(),
            range_merge_plans: BTreeMap::new(),
            range_move_plans: BTreeMap::new(),
            completed_move_plans: BTreeMap::new(),
            owner_move_plans: BTreeMap::new(),
            completed_owner_move_plans: BTreeMap::new(),
            applied_requests: BTreeMap::new(),
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
    Pipes,
    Indexes,
    StateStores,
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
        /// define time. Requires `MAX_LATE` — a pane only provably
        /// closes once the ingest clock passes pane_end + bound.
        #[serde(default)]
        emit_feed: Option<String>,
        operation: PipeOperation,
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
    },
    OpenReaderSession {
        reader: String,
        capacity: usize,
    },
    RecordReaderDelivery {
        reader: String,
        session_epoch: u64,
        cursor: String,
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
                | Self::ClearSubscriptionCutover { .. }
                | Self::RecordFollowerMoveCatchUp { .. }
                | Self::ActivateFollowerMove { .. }
                | Self::PrepareOwnerMove { .. }
                | Self::RecordOwnerMoveCatchUp { .. }
                | Self::ActivateOwnerMove { .. }
                | Self::AbortOwnerMove { .. }
                | Self::RecoverSubscriptionProgressOwner { .. }
                | Self::MoveSubscriptionProgressReplica { .. }
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
            serde_json::from_slice(&fs::read(&path)?)?
        } else {
            CatalogState::default()
        };
        state.migrate_range_metadata();
        let issued_api_keys = issued_api_key_view(&state);
        Ok(Self {
            path: Arc::new(path),
            state: Arc::new(Mutex::new(state)),
            eligible_storage_nodes: Arc::new(eligible_storage_nodes.into_iter().collect()),
            issued_api_keys: Arc::new(std::sync::RwLock::new(issued_api_keys)),
            synced_replica: false,
        })
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
            if !self.storage_eligible(&state, replacement_replica) {
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
            self.select_fixed_active_range(&state, &seed)
        })
        .transpose()?;
        let fixed_subscription_progress = matches!(&command, Command::CreateSubscription { .. })
            .then(|| {
                self.select_fixed_subscription_progress(
                    &state,
                    derived_resource_id(request_id, "subscription"),
                )
            })
            .transpose()?;
        let fixed_state_store = matches!(&command, Command::DefineStateStore { .. })
            .then(|| {
                self.select_fixed_state_store(
                    &state,
                    derived_resource_id(request_id, "state-store"),
                )
            })
            .transpose()?;
        drop(state);
        Ok(ReplicatedCommand {
            request_id,
            issued_at_ns,
            command,
            fixed_active_range,
            fixed_subscription_progress,
            fixed_state_store,
        })
    }

    fn storage_pool(&self, state: &CatalogState) -> BTreeSet<StorageNodeId> {
        self.eligible_storage_nodes
            .iter()
            .chain(state.storage_nodes.keys())
            .filter(|node| !state.retired_storage_nodes.contains(*node))
            .filter(|node| !state.draining_storage_nodes.contains(*node))
            .cloned()
            .collect()
    }

    fn storage_eligible(&self, state: &CatalogState, node: &StorageNodeId) -> bool {
        (self.eligible_storage_nodes.contains(node) || state.storage_nodes.contains_key(node))
            && !state.retired_storage_nodes.contains(node)
            && !state.draining_storage_nodes.contains(node)
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
    ) -> Result<FixedActiveRangePlacement, ControlError> {
        let mut ranked = self
            .storage_pool(state)
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
    ) -> Result<FixedSubscriptionProgressPlacement, ControlError> {
        let mut ranked = self
            .storage_pool(state)
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
    ) -> Result<FixedStateStorePlacement, ControlError> {
        let mut ranked = self
            .storage_pool(state)
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
            fixed_active_range,
            fixed_subscription_progress,
            fixed_state_store,
        } = request;
        let statement = command_label(&command);
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
        state.applied_requests.insert(request_id, response.clone());
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

    pub async fn snapshot_bytes(&self) -> Result<Vec<u8>, ControlError> {
        Ok(serde_json::to_vec(&*self.state.lock().await)?)
    }

    pub async fn install_snapshot_bytes(&self, bytes: &[u8]) -> Result<(), ControlError> {
        let mut state: CatalogState = serde_json::from_slice(bytes)?;
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

    pub(crate) async fn active_subscription_feed_by_id(
        &self,
        subscription_id: Uuid,
    ) -> Option<Uuid> {
        self.state
            .lock()
            .await
            .subscriptions
            .get(&subscription_id)
            .filter(|item| item.status == ResourceStatus::Active)
            .and_then(|item| item.source.feed_id())
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
            Command::CreateFeed { name } => {
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
                };
                state.feeds.insert(feed_id, definition.clone());
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
                if unique {
                    return Err(ControlError::InvalidOperation(
                        "UNIQUE Indexes require the replicated conditional-claim protocol; declare non-unique Indexes until it exists"
                            .to_owned(),
                    ));
                }
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
                let (output_feed_id, output_store_id) = match &mut operation {
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
                        // publish. Without `MAX_LATE` no pane ever
                        // provably closes (a record could always arrive
                        // legally), so emission requires the bound.
                        let emit_feed_id = match &emit_feed {
                            Some(name) => {
                                if max_late_ns.is_none() {
                                    return Err(ControlError::InvalidOperation(
                                        "EMIT TO FEED requires MAX_LATE — without a lateness bound no pane ever provably closes".to_owned(),
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
                    .or_else(|| self.storage_pool(state).into_iter().next())
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
                    operation,
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
            Command::Show { kind } => Ok((
                format!("showing {}", show_name(&kind)),
                show_resources(state, kind, &self.eligible_storage_nodes),
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
                if !self.storage_eligible(state, &new_owner) {
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
                if !self.storage_eligible(state, &replacement) {
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
                if !self.storage_eligible(state, &new_owner) {
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
            Command::RegisterStorageNode {
                node,
                endpoint,
                cert_pins,
            } => {
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
            Command::PrepareActiveRangeSplit { feed, split_at } => {
                let feed_id = active_feed(state, &feed)?.feed_id;
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
                {
                    return Err(ControlError::InvalidOperation(
                        "drop attached Writers, Readers, Subscriptions, and Pipes first".to_owned(),
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
            Some("FEED") => Ok(Command::CreateFeed {
                name: token(&tokens, 2)?.to_owned(),
            }),
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
                                operation: PipeOperation::Enrich {
                                    key,
                                    freshness,
                                    named,
                                    on_miss,
                                    quarantine_feed_id: None,
                                },
                            });
                        }
                        Ok(Command::DefinePipe {
                            name: token(&tokens, 2)?.to_owned(),
                            subscription: token(&tokens, 5)?.to_owned(),
                            output_feed: Some(token(&tokens, 8)?.to_owned()),
                            output_store: None,
                            quarantine_feed: None,
                            emit_feed: None,
                            operation: PipeOperation::Forward,
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
                            operation: PipeOperation::Window {
                                window_ns,
                                max_late_ns,
                                expire_ns,
                                key_limit,
                                byte_limit,
                                key,
                                aggregation,
                                time,
                            },
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
                    let delivery_delay_ns = if tail < tokens.len() {
                        expect_keyword(&tokens, tail, "DELIVERY")?;
                        expect_keyword(&tokens, tail + 1, "DELAY")?;
                        if tail + 2 >= tokens.len() {
                            return Err(ControlError::Syntax(
                                "DELIVERY DELAY expects a <n><ms|s|m|h|d> duration".to_owned(),
                            ));
                        }
                        let delay = parse_duration_ns(token(&tokens, tail + 2)?)?;
                        if tail + 3 != tokens.len() {
                            return Err(ControlError::Syntax(
                                "unexpected tokens after DELIVERY DELAY <duration>".to_owned(),
                            ));
                        }
                        Some(delay)
                    } else {
                        None
                    };
                    Ok(Command::CreateSubscription {
                        name,
                        feed: (!domain_source).then_some(source.clone()),
                        domain: domain_source.then_some(source),
                        start,
                        delivery_delay_ns,
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
            Some("INDEX") => {
                // CREATE INDEX <name> ON <feed> FIELDS <field>[,<field>]*
                expect_keyword(&tokens, 3, "ON")?;
                expect_keyword(&tokens, 5, "FIELDS")?;
                let mut position = 6;
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
                    name: token(&tokens, 2)?.to_owned(),
                    feed: token(&tokens, 4)?.to_owned(),
                    fields,
                    unique: false,
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
        Some("MOVE") => {
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
        Some("DROP") => Ok(Command::Drop {
            kind: parse_resource_kind(token(&tokens, 1)?)?,
            name: token(&tokens, 2)?.to_owned(),
        }),
        Some("SHOW") => Ok(Command::Show {
            kind: if token(&tokens, 1)?.eq_ignore_ascii_case("STORAGE") {
                expect_keyword(&tokens, 2, "NODES")?;
                ShowKind::StorageNodes
            } else if token(&tokens, 1)?.eq_ignore_ascii_case("API") {
                expect_keyword(&tokens, 2, "KEYS")?;
                ShowKind::ApiKeys
            } else {
                parse_show_kind(token(&tokens, 1)?)?
            },
        }),
        Some("REGISTER") => {
            expect_keyword(&tokens, 1, "STORAGE")?;
            expect_keyword(&tokens, 2, "NODE")?;
            expect_keyword(&tokens, 4, "AT")?;
            let mut cert_pins = BTreeSet::new();
            if tokens.len() > 6 {
                expect_keyword(&tokens, 6, "WITH")?;
                expect_keyword(&tokens, 7, "CERT")?;
                expect_keyword(&tokens, 8, "PIN")?;
                for pin in token(&tokens, 9)?.split('|') {
                    cert_pins.insert(pin.to_owned());
                }
            }
            Ok(Command::RegisterStorageNode {
                node: StorageNodeId::try_new(token(&tokens, 3)?.to_owned())
                    .map_err(|error| ControlError::Syntax(error.to_string()))?,
                endpoint: token(&tokens, 5)?.to_owned(),
                cert_pins,
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

fn validate_dotted_name(name: &str) -> Result<(), ControlError> {
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
        if !seen.insert(translation.subscription_id)
            || subscription.source.feed_id() != Some(feed_id)
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
    configured: &BTreeSet<StorageNodeId>,
) -> Value {
    match kind {
        ShowKind::Domains | ShowKind::Spaces => json!(state
            .spaces
            .values()
            .filter(|item| item.status == ResourceStatus::Active)
            .collect::<Vec<_>>()),
        ShowKind::Feeds => json!(state
            .feeds
            .values()
            .filter(|item| item.status == ResourceStatus::Active)
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
        ShowKind::Roles => json!(state
            .roles
            .values()
            .filter(|item| item.status == ResourceStatus::Active)
            .collect::<Vec<_>>()),
        ShowKind::Grants => json!(state.grants.values().collect::<Vec<_>>()),
        ShowKind::StorageNodes => {
            let registered: BTreeSet<&StorageNodeId> = state.storage_nodes.keys().collect();
            json!(configured
                .iter()
                .chain(state.storage_nodes.keys())
                .chain(state.retired_storage_nodes.iter())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .map(|node| {
                    json!({
                        "node": node.as_str(),
                        "endpoint": state
                            .storage_nodes
                            .get(node)
                            .map(|record| record.endpoint.as_str()),
                        "cert_pins": state
                            .storage_nodes
                            .get(node)
                            .map(|record| record.cert_pins.iter().collect::<Vec<_>>())
                            .unwrap_or_default(),
                        "source": if registered.contains(node) {
                            "registered"
                        } else {
                            "configured"
                        },
                        "eligible": !state.retired_storage_nodes.contains(node)
                            && !state.draining_storage_nodes.contains(node),
                        "draining": state.draining_storage_nodes.contains(node),
                    })
                })
                .collect::<Vec<_>>())
        }
    }
}

fn describe_resource(
    state: &CatalogState,
    kind: ResourceKind,
    name: &str,
) -> Result<Value, ControlError> {
    match kind {
        ResourceKind::Domain | ResourceKind::Space => Ok(json!(active_space(state, name)?)),
        ResourceKind::Feed => Ok(json!(active_feed(state, name)?)),
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
    None
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageDrainPlan {
    pub node: StorageNodeId,
    pub draining: bool,
    pub moves: Vec<Command>,
    pub unplannable: Vec<String>,
    pub ready_to_retire: bool,
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

fn drain_replacement(
    state: &CatalogState,
    controller: &ControlController,
    excluded: &BTreeSet<StorageNodeId>,
    seed: &Uuid,
) -> Option<StorageNodeId> {
    let mut ranked = controller
        .storage_pool(state)
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
        match drain_replacement(state, controller, &excluded, &range_id.as_uuid()) {
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
        match drain_replacement(state, controller, &excluded, subscription_id) {
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
        match drain_replacement(state, controller, &excluded, pipe_id) {
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
    StorageDrainPlan {
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
    fs::write(path, serde_json::to_vec_pretty(state)?)?;
    Ok(())
}

pub fn derive_command_request_id(batch_request_id: Uuid, command_index: usize) -> Uuid {
    let mut hasher = blake3::Hasher::new();
    hasher.update(batch_request_id.as_bytes());
    hasher.update(&(command_index as u64).to_be_bytes());
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

fn unix_ns() -> i64 {
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
        Command::MovePipeComputeLease { .. } => "MOVE PIPE COMPUTE LEASE",
        Command::RegisterStorageNode { .. } => "REGISTER STORAGE NODE",
        Command::RetireStorageNode { .. } => "RETIRE STORAGE NODE",
        Command::DrainStorageNode { .. } => "DRAIN STORAGE NODE",
        Command::UndrainStorageNode { .. } => "UNDRAIN STORAGE NODE",
        Command::InspectStorageDrain { .. } => "INSPECT DRAIN",
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
        ShowKind::StateStores => "StateStores",
        ShowKind::Roles => "Roles",
        ShowKind::Grants => "Grants",
        ShowKind::ApiKeys => "API Keys",
        ShowKind::StorageNodes => "Storage Nodes",
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
                kind: ShowKind::Domains
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
                kind: ShowKind::Spaces
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
                cursor: "cursor-3".to_owned()
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

        // UNIQUE stays refused until the replicated conditional-claim
        // protocol exists.
        assert!(leader
            .execute_commands(vec![Command::DefineIndex {
                name: "accounts.unique.customer".to_owned(),
                feed: "accounts.orders".to_owned(),
                fields: fields.clone(),
                unique: true,
                collation: IndexCollation::Binary,
                consistency: IndexConsistency::Eventual,
            }])
            .await
            .is_err());
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
        assert_eq!(shown.results[0].data.as_array().unwrap().len(), 1);
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
        assert_eq!(follower.state.lock().await.indexes.len(), 1);
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
            operation: PipeOperation::Forward,
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
                operation: PipeOperation::Forward,
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
                        operation: PipeOperation::Forward,
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
                operation: PipeOperation::Forward,
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
                .select_fixed_subscription_progress(&left_state, id)
                .unwrap();
            drop(left_state);
            let right_state = second.state.lock().await;
            let right = second
                .select_fixed_subscription_progress(&right_state, id)
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
                delivery_delay_ns, ..
            } => assert_eq!(delivery_delay_ns, None),
            other => panic!("unexpected command {other:?}"),
        }
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
            operation: PipeOperation::Window {
                window_ns,
                key: IndexFieldSpec::Key,
                aggregation: WindowAggregation::Count,
                time: WindowTime::Event,
                max_late_ns: None,
                expire_ns: None,
                key_limit: None,
                byte_limit: None,
            },
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
            operation: PipeOperation::Enrich {
                key: IndexFieldSpec::Key,
                freshness: EnrichFreshness::Latest,
                named: None,
                on_miss: EnrichMiss::Forward,
                quarantine_feed_id: None,
            },
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
}
