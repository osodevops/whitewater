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

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StateStoreStage {
    Declared,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct StateStoreDefinition {
    pub store_id: Uuid,
    pub name: String,
    pub space_id: Uuid,
    pub source: StateStoreSource,
    pub stage: StateStoreStage,
    pub created_at_ns: i64,
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SubscriptionDefinition {
    pub subscription_id: Uuid,
    pub name: String,
    pub space_id: Uuid,
    pub feed_id: Uuid,
    pub start: ReaderStart,
    pub stage: SubscriptionStage,
    pub status: ResourceStatus,
    pub created_at_ns: i64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ReaderFrontier {
    pub acknowledged: BTreeMap<RangeId, String>,
    pub delivered: BTreeMap<RangeId, String>,
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct NamespaceGrant {
    pub grant_id: Uuid,
    pub role_id: Uuid,
    pub namespace: String,
    pub actions: BTreeSet<PermissionAction>,
    pub created_at_ns: i64,
}

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

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CatalogState {
    schema_version: u32,
    revision: u64,
    spaces: BTreeMap<Uuid, SpaceDefinition>,
    feeds: BTreeMap<Uuid, FeedDefinition>,
    #[serde(default)]
    state_stores: BTreeMap<Uuid, StateStoreDefinition>,
    writers: BTreeMap<Uuid, WriterDefinition>,
    readers: BTreeMap<Uuid, ReaderDefinition>,
    #[serde(default)]
    subscriptions: BTreeMap<Uuid, SubscriptionDefinition>,
    #[serde(default)]
    subscription_progress_assignments: BTreeMap<Uuid, SubscriptionProgressAssignment>,
    #[serde(default)]
    reader_frontiers: BTreeMap<Uuid, ReaderFrontier>,
    roles: BTreeMap<Uuid, RoleDefinition>,
    grants: BTreeMap<Uuid, NamespaceGrant>,
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
            writers: BTreeMap::new(),
            readers: BTreeMap::new(),
            subscriptions: BTreeMap::new(),
            subscription_progress_assignments: BTreeMap::new(),
            reader_frontiers: BTreeMap::new(),
            roles: BTreeMap::new(),
            grants: BTreeMap::new(),
            active_ranges: BTreeMap::new(),
            range_maps: BTreeMap::new(),
            range_assignments: BTreeMap::new(),
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
        feed: String,
        start: ReaderStart,
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
}

impl Command {
    pub fn is_read_only(&self) -> bool {
        matches!(
            self,
            Self::Show { .. }
                | Self::Describe { .. }
                | Self::ExplainAccess { .. }
                | Self::InspectPlacement { .. }
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
pub struct ReplicatedCommand {
    pub request_id: Uuid,
    pub issued_at_ns: i64,
    pub command: Command,
    #[serde(default)]
    pub fixed_active_range: Option<FixedActiveRangePlacement>,
    #[serde(default)]
    pub fixed_subscription_progress: Option<FixedSubscriptionProgressPlacement>,
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
        Ok(Self {
            path: Arc::new(path),
            state: Arc::new(Mutex::new(state)),
            eligible_storage_nodes: Arc::new(eligible_storage_nodes.into_iter().collect()),
        })
    }

    pub fn prepare_replicated(
        &self,
        request_id: Uuid,
        issued_at_ns: i64,
        command: Command,
    ) -> Result<ReplicatedCommand, ControlError> {
        if let Command::PrepareFollowerMove {
            replacement_replica,
            ..
        } = &command
        {
            if !self.eligible_storage_nodes.contains(replacement_replica) {
                return Err(ControlError::InvalidOperation(format!(
                    "replacement Node {replacement_replica} is not eligible for storage"
                )));
            }
        }
        let fixed_active_range = matches!(
            &command,
            Command::CreateFeed { .. } | Command::PrepareActiveRangeSplit { .. }
        )
        .then(|| self.select_fixed_active_range())
        .transpose()?;
        let fixed_subscription_progress = matches!(&command, Command::CreateSubscription { .. })
            .then(|| {
                self.select_fixed_subscription_progress(derived_resource_id(
                    request_id,
                    "subscription",
                ))
            })
            .transpose()?;
        Ok(ReplicatedCommand {
            request_id,
            issued_at_ns,
            command,
            fixed_active_range,
            fixed_subscription_progress,
        })
    }

    fn select_fixed_active_range(&self) -> Result<FixedActiveRangePlacement, ControlError> {
        let replicas = self
            .eligible_storage_nodes
            .iter()
            .take(ACTIVE_RANGE_REPLICA_COUNT)
            .cloned()
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
        subscription_id: Uuid,
    ) -> Result<FixedSubscriptionProgressPlacement, ControlError> {
        let mut ranked = self
            .eligible_storage_nodes
            .iter()
            .map(|node| {
                let mut hash = blake3::Hasher::new();
                hash.update(b"whitewater-subscription-progress-v1");
                hash.update(subscription_id.as_bytes());
                hash.update(node.as_str().as_bytes());
                (*hash.finalize().as_bytes(), node.clone())
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
        let mut results = Vec::with_capacity(commands.len());
        let mut revision = self.revision().await;
        let issued_at_ns = unix_ns();
        for (index, command) in commands.into_iter().enumerate() {
            if command.is_read_only() {
                results.push(self.execute_query(command).await?);
                revision = self.revision().await;
                continue;
            }
            let replicated = self.prepare_replicated(
                derive_command_request_id(request_id, index),
                issued_at_ns,
                command,
            )?;
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
            .apply(&mut state, command, Uuid::nil(), 0, None, None)
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
        } = request;
        let statement = command_label(&command);
        let response = match self
            .apply(
                &mut state,
                command,
                request_id,
                issued_at_ns,
                fixed_active_range,
                fixed_subscription_progress,
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
            return ReplicatedCommandResult {
                revision: state.revision,
                result: None,
                error: Some(error.to_string()),
            };
        }
        response
    }

    pub async fn snapshot_bytes(&self) -> Result<Vec<u8>, ControlError> {
        Ok(serde_json::to_vec(&*self.state.lock().await)?)
    }

    pub async fn install_snapshot_bytes(&self, bytes: &[u8]) -> Result<(), ControlError> {
        let mut state: CatalogState = serde_json::from_slice(bytes)?;
        state.migrate_range_metadata();
        persist_state(&self.path, &state)?;
        *self.state.lock().await = state;
        Ok(())
    }

    pub async fn revision(&self) -> u64 {
        self.state.lock().await.revision
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

    pub async fn active_subscription_by_name(&self, name: &str) -> Option<SubscriptionDefinition> {
        self.state
            .lock()
            .await
            .subscriptions
            .values()
            .find(|item| item.name == name && item.status == ResourceStatus::Active)
            .cloned()
    }

    pub(crate) async fn active_subscription_progress_assignment_by_id(
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
            .map(|item| item.feed_id)
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
            .find(|store| store.name == name)
            .cloned()
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
        fixed_active_range: Option<FixedActiveRangePlacement>,
        fixed_subscription_progress: Option<FixedSubscriptionProgressPlacement>,
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
                let placement = fixed_active_range.ok_or_else(|| {
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
                Ok((format!("created Feed {name}"), json!(definition)))
            }
            Command::DefineStateStore { name, source } => {
                validate_dotted_name(&name)?;
                if state.state_stores.values().any(|item| item.name == name) {
                    return Err(ControlError::AlreadyExists(name));
                }
                let space_id = owning_space(state, &name)?.space_id;
                let source = match source {
                    StateStoreSourceRequest::Manual => StateStoreSource::Manual,
                    StateStoreSourceRequest::Feed { feed } => {
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
                let definition = StateStoreDefinition {
                    store_id: derived_resource_id(request_id, "state-store"),
                    name: name.clone(),
                    space_id,
                    source,
                    stage: StateStoreStage::Declared,
                    created_at_ns: issued_at_ns,
                };
                state
                    .state_stores
                    .insert(definition.store_id, definition.clone());
                Ok((
                    format!("declared StateStore {name}; no data is served until RF3 activation"),
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
            Command::CreateSubscription { name, feed, start } => {
                validate_dotted_name(&name)?;
                ensure_name_available(
                    state
                        .subscriptions
                        .values()
                        .map(|item| (&item.name, &item.status)),
                    &name,
                )?;
                let space_id = owning_space(state, &name)?.space_id;
                let feed_id = {
                    let source = active_feed(state, &feed)?;
                    if source.space_id != space_id {
                        return Err(ControlError::InvalidOperation(
                            "Subscription and source Feed must belong to the same Domain"
                                .to_owned(),
                        ));
                    }
                    source.feed_id
                };
                let subscription_id = derived_resource_id(request_id, "subscription");
                let range_id = state
                    .range_maps
                    .get(&feed_id)
                    .and_then(|map| map.routes().first())
                    .map(|route| route.range_id)
                    .ok_or_else(|| {
                        ControlError::InvalidOperation(
                            "Subscription source Feed has no committed range placement".to_owned(),
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
                let fixed = fixed_subscription_progress.ok_or_else(|| {
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
                    feed_id,
                    start,
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
                frontier.delivered = positions.clone();
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
            Command::Rename { kind, current, new } => self.rename(state, kind, &current, &new),
            Command::Drop { kind, name } => self.drop_resource(state, kind, &name),
            Command::Show { kind } => Ok((
                format!("showing {}", show_name(&kind)),
                show_resources(state, kind),
            )),
            Command::Describe { kind, name } => Ok((
                format!("described {} {name}", resource_name(&kind)),
                describe_resource(state, kind, &name)?,
            )),
            Command::Grant {
                actions,
                namespace,
                role,
            } => {
                validate_namespace_pattern(&namespace)?;
                let role_id = active_role(state, &role)?.role_id;
                if state.grants.values().any(|grant| {
                    grant.role_id == role_id
                        && grant.namespace == namespace
                        && grant.actions == actions
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
                    created_at_ns: issued_at_ns,
                };
                state.grants.insert(definition.grant_id, definition.clone());
                Ok((
                    format!("granted namespace permissions on {namespace} to Role {role}"),
                    json!(definition),
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
                            && grant.actions.contains(&action)
                            && namespace_matches(&grant.namespace, &feed)
                    })
                    .cloned()
                    .collect();
                let allowed = !matched.is_empty();
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
                if !self.eligible_storage_nodes.contains(&new_owner) {
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
                if !self.eligible_storage_nodes.contains(&replacement) {
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
                let placement = fixed_active_range.ok_or_else(|| {
                    ControlError::InvalidOperation(
                        "split preparation has no consensus-prepared RF3 placement".to_owned(),
                    )
                })?;
                let fallback_owner = placement.owner;
                let replicas = ReplicaSet::try_new(placement.replicas)
                    .map_err(|error| ControlError::InvalidOperation(error.to_string()))?;
                let owner = replicas
                    .iter()
                    .find(|node| *node != &source_assignment.owner)
                    .cloned()
                    .unwrap_or(fallback_owner);
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
                let existing = active_feed(state, current)?;
                if new_space_id != existing.space_id
                    && state.subscriptions.values().any(|subscription| {
                        subscription.status == ResourceStatus::Active
                            && subscription.feed_id == existing.feed_id
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
                Ok((
                    format!("renamed Feed {current} to {new}"),
                    json!(item.clone()),
                ))
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
        }
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
                        item.status == ResourceStatus::Active && item.feed_id == feed_id
                    })
                {
                    return Err(ControlError::InvalidOperation(
                        "drop attached Writers, Readers, and Subscriptions first".to_owned(),
                    ));
                }
                let item = state.feeds.get_mut(&feed_id).expect("Feed exists");
                item.status = ResourceStatus::Dropped;
                Ok((
                    format!("dropped Feed {name} without purging stored history"),
                    json!(item.clone()),
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
                let result = drop_named(&mut state.subscriptions, name, "Subscription")?;
                state
                    .subscription_progress_assignments
                    .remove(&subscription_id);
                Ok(result)
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
            '\'' if quoted && chars.peek() == Some(&'\'') => {
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
            ',' if !quoted => {
                if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current));
                }
                tokens.push(",".to_owned());
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
            Some("READER") | Some("SUBSCRIPTION") => {
                expect_keyword(&tokens, 3, "FROM")?;
                let start = if tokens.len() == 5 {
                    ReaderStart::Beginning
                } else {
                    expect_keyword(&tokens, 5, "START")?;
                    expect_keyword(&tokens, 6, "AT")?;
                    parse_reader_start(&tokens, 7)?
                };
                let name = token(&tokens, 2)?.to_owned();
                let feed = token(&tokens, 4)?.to_owned();
                if keyword(1).as_deref() == Some("SUBSCRIPTION") {
                    Ok(Command::CreateSubscription { name, feed, start })
                } else {
                    Ok(Command::CreateReader { name, feed, start })
                }
            }
            _ => Err(ControlError::Syntax(
                "CREATE supports DOMAIN, FEED, WRITER, READER, SUBSCRIPTION, and ROLE (SPACE is a compatibility alias)".to_owned(),
            )),
        },
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
        Some("REVOKE") => {
            expect_keyword(&tokens, 1, "WRITER")?;
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
        Some("RENAME") => {
            let kind = parse_resource_kind(token(&tokens, 1)?)?;
            expect_keyword(&tokens, 3, "TO")?;
            Ok(Command::Rename {
                kind,
                current: token(&tokens, 2)?.to_owned(),
                new: token(&tokens, 4)?.to_owned(),
            })
        }
        Some("DROP") => Ok(Command::Drop {
            kind: parse_resource_kind(token(&tokens, 1)?)?,
            name: token(&tokens, 2)?.to_owned(),
        }),
        Some("SHOW") => Ok(Command::Show {
            kind: parse_show_kind(token(&tokens, 1)?)?,
        }),
        Some("DESCRIBE") => Ok(Command::Describe {
            kind: parse_resource_kind(token(&tokens, 1)?)?,
            name: token(&tokens, 2)?.to_owned(),
        }),
        Some("GRANT") => parse_grant(&tokens),
        Some("EXPLAIN") => parse_explain(&tokens),
        Some("INSPECT") => {
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

fn parse_grant(tokens: &[String]) -> Result<Command, ControlError> {
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
        "ROLES" => Ok(ShowKind::Roles),
        "GRANTS" => Ok(ShowKind::Grants),
        _ => Err(ControlError::Syntax(format!(
            "unsupported SHOW resource {token}"
        ))),
    }
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

fn validate_namespace_pattern(pattern: &str) -> Result<(), ControlError> {
    if let Some(prefix) = pattern.strip_suffix(".*") {
        validate_dotted_name(prefix)
    } else {
        validate_dotted_name(pattern)
    }
}

fn namespace_matches(pattern: &str, feed: &str) -> bool {
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

fn show_resources(state: &CatalogState, kind: ShowKind) -> Value {
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
        ShowKind::Roles => json!(state
            .roles
            .values()
            .filter(|item| item.status == ResourceStatus::Active)
            .collect::<Vec<_>>()),
        ShowKind::Grants => json!(state.grants.values().collect::<Vec<_>>()),
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
        ResourceKind::Subscription => state
            .subscriptions
            .values()
            .find(|item| item.name == name && item.status == ResourceStatus::Active)
            .map(|item| json!(item))
            .ok_or_else(|| ControlError::NotFound(format!("Subscription {name}"))),
        ResourceKind::Role => Ok(json!(active_role(state, name)?)),
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
        Command::CreateWriter { .. } => "CREATE WRITER",
        Command::OpenWriterSession { .. } => "OPEN WRITER SESSION",
        Command::AllocateWriterSequence { .. } => "ALLOCATE WRITER SEQUENCE",
        Command::AllocateWriterRangeSequence { .. } => "ALLOCATE WRITER RANGE SEQUENCE",
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
        Command::Grant { .. } => "GRANT",
        Command::ExplainAccess { .. } => "EXPLAIN ACCESS",
        Command::SeekReader { .. } => "SEEK READER",
        Command::InspectPlacement { .. } => "INSPECT PLACEMENT",
        Command::TransferActiveRangeOwnership { .. } => "TRANSFER ACTIVE RANGE OWNERSHIP",
        Command::RecoverActiveRangeOwnership { .. } => "RECOVER ACTIVE RANGE OWNERSHIP",
        Command::PrepareActiveRangeSplit { .. } => "PREPARE ACTIVE RANGE SPLIT",
        Command::RecordActiveRangeSplitCatchUp { .. } => "RECORD ACTIVE RANGE SPLIT CATCH UP",
        Command::ActivateActiveRangeSplit { .. } => "ACTIVATE ACTIVE RANGE SPLIT",
        Command::PrepareActiveRangeMerge { .. } => "PREPARE ACTIVE RANGE MERGE",
        Command::RecordActiveRangeMergeStaging { .. } => "RECORD ACTIVE RANGE MERGE STAGING",
        Command::ActivateActiveRangeMerge { .. } => "ACTIVATE ACTIVE RANGE MERGE",
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
        ShowKind::Roles => "Roles",
        ShowKind::Grants => "Grants",
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
            }])
            .await
            .is_ok());
        assert!(leader
            .execute_commands(vec![Command::DefineStateStore {
                name: "accounts.invalid".to_owned(),
                source: StateStoreSourceRequest::Feed {
                    feed: "other.users".to_owned()
                },
            }])
            .await
            .is_err());
        assert!(leader
            .execute_commands(vec![Command::DefineStateStore {
                name: "accounts.unknown".to_owned(),
                source: StateStoreSourceRequest::Feed {
                    feed: "accounts.missing".to_owned()
                },
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

    #[test]
    fn subscription_progress_selection_is_deterministic_for_candidate_order() {
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
            let left = first.select_fixed_subscription_progress(id).unwrap();
            let right = second.select_fixed_subscription_progress(id).unwrap();
            assert_eq!(left, right);
            assert!(left.replicas.contains(&left.owner));
            used.extend(left.replicas);
        }
        assert!(used.len() >= 8);
    }

    #[tokio::test]
    async fn subscription_definitions_are_scoped_idempotent_and_keep_feeds_unchanged() {
        let directory = TempDir::new().unwrap();
        let controller = new_controller(&directory);
        controller.execute("CREATE SPACE accounts; CREATE FEED accounts.users; CREATE SPACE other; CREATE FEED other.events;").await.unwrap();
        let request_id = Uuid::from_u128(77_001);
        let command = Command::CreateSubscription {
            name: "accounts.billing".to_owned(),
            feed: "accounts.users".to_owned(),
            start: ReaderStart::Beginning,
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
            definition.feed_id,
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
                feed: "accounts.users".to_owned(),
                start: ReaderStart::Beginning,
            }])
            .await
            .is_err());
        assert!(controller
            .execute_commands(vec![Command::CreateSubscription {
                name: "accounts.crossspace".to_owned(),
                feed: "other.events".to_owned(),
                start: ReaderStart::Beginning,
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
}
