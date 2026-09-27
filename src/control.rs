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
        ActiveRangeAssignment, OwnershipEpoch, RangeGeneration, RangeId, ReplicaSet, StorageNodeId,
        ACTIVE_RANGE_REPLICA_COUNT,
    },
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
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReaderDefinition {
    pub reader_id: Uuid,
    pub name: String,
    pub feed_id: Uuid,
    pub start: ReaderStart,
    pub acknowledged_cursor: Option<String>,
    pub status: ResourceStatus,
    pub created_at_ns: i64,
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

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CatalogState {
    schema_version: u32,
    revision: u64,
    spaces: BTreeMap<Uuid, SpaceDefinition>,
    feeds: BTreeMap<Uuid, FeedDefinition>,
    writers: BTreeMap<Uuid, WriterDefinition>,
    readers: BTreeMap<Uuid, ReaderDefinition>,
    roles: BTreeMap<Uuid, RoleDefinition>,
    grants: BTreeMap<Uuid, NamespaceGrant>,
    #[serde(default)]
    active_ranges: BTreeMap<Uuid, ActiveRangeAssignment>,
    #[serde(default)]
    applied_requests: BTreeMap<Uuid, ReplicatedCommandResult>,
}

impl Default for CatalogState {
    fn default() -> Self {
        Self {
            schema_version: 1,
            revision: 0,
            spaces: BTreeMap::new(),
            feeds: BTreeMap::new(),
            writers: BTreeMap::new(),
            readers: BTreeMap::new(),
            roles: BTreeMap::new(),
            grants: BTreeMap::new(),
            active_ranges: BTreeMap::new(),
            applied_requests: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ResourceKind {
    Space,
    Feed,
    Writer,
    Reader,
    Role,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ShowKind {
    Spaces,
    Feeds,
    Writers,
    Readers,
    Roles,
    Grants,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "command", rename_all = "snake_case")]
pub enum Command {
    CreateSpace {
        name: String,
    },
    CreateFeed {
        name: String,
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
    RevokeWriterSession {
        writer: String,
        session_epoch: u64,
    },
    CreateReader {
        name: String,
        feed: String,
        start: ReaderStart,
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
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FixedActiveRangePlacement {
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
        let state = if path.exists() {
            serde_json::from_slice(&fs::read(&path)?)?
        } else {
            CatalogState::default()
        };
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
        let fixed_active_range = matches!(&command, Command::CreateFeed { .. })
            .then(|| self.select_fixed_active_range())
            .transpose()?;
        Ok(ReplicatedCommand {
            request_id,
            issued_at_ns,
            command,
            fixed_active_range,
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
            .apply(&mut state, command, Uuid::nil(), 0, None)
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
        } = request;
        let statement = command_label(&command);
        let response = match self
            .apply(
                &mut state,
                command,
                request_id,
                issued_at_ns,
                fixed_active_range,
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
        let state: CatalogState = serde_json::from_slice(bytes)?;
        persist_state(&self.path, &state)?;
        *self.state.lock().await = state;
        Ok(())
    }

    pub async fn revision(&self) -> u64 {
        self.state.lock().await.revision
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

    pub async fn active_range_assignment(&self, feed_id: Uuid) -> Option<ActiveRangeAssignment> {
        self.state.lock().await.active_ranges.get(&feed_id).cloned()
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
    ) -> Result<(String, Value), ControlError> {
        match command {
            Command::CreateSpace { name } => {
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
                Ok((format!("created Space {name}"), json!(definition)))
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
                state.active_ranges.insert(feed_id, assignment);
                Ok((format!("created Feed {name}"), json!(definition)))
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
                    ReaderStart::Beginning | ReaderStart::Now => None,
                };
                let definition = ReaderDefinition {
                    reader_id: derived_resource_id(request_id, "reader"),
                    name: name.clone(),
                    feed_id,
                    start,
                    acknowledged_cursor,
                    status: ResourceStatus::Active,
                    created_at_ns: issued_at_ns,
                };
                state
                    .readers
                    .insert(definition.reader_id, definition.clone());
                Ok((format!("created Reader {name}"), json!(definition)))
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
                    ReaderStart::Beginning | ReaderStart::Now => None,
                };
                definition.start = start;
                Ok((
                    format!("moved Reader {reader} position"),
                    json!(definition.clone()),
                ))
            }
            Command::InspectPlacement { feed } => {
                let feed_id = active_feed(state, &feed)?.feed_id;
                let assignment = state.active_ranges.get(&feed_id).ok_or_else(|| {
                    ControlError::NotFound(format!("Active Range placement for Feed {feed}"))
                })?;
                Ok((
                    format!("inspected placement for Feed {feed}"),
                    json!(assignment),
                ))
            }
            Command::TransferActiveRangeOwnership { feed, owner } => {
                let feed_id = active_feed(state, &feed)?.feed_id;
                let assignment = state.active_ranges.get_mut(&feed_id).ok_or_else(|| {
                    ControlError::NotFound(format!("Active Range placement for Feed {feed}"))
                })?;
                if assignment.owner == owner {
                    return Err(ControlError::InvalidOperation(format!(
                        "Node {owner} already owns the Active Range for Feed {feed}"
                    )));
                }
                let next_epoch = assignment
                    .ownership_epoch
                    .checked_next()
                    .map_err(|error| ControlError::InvalidOperation(error.to_string()))?;
                assignment
                    .transfer_ownership(owner, next_epoch)
                    .map_err(|error| ControlError::InvalidOperation(error.to_string()))?;
                Ok((
                    format!("transferred Active Range ownership for Feed {feed}"),
                    json!(assignment.clone()),
                ))
            }
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
                Ok((
                    format!("recovered Active Range ownership for Feed {feed}"),
                    json!(assignment.clone()),
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
                let item = active_feed_mut(state, current)?;
                item.name = new.to_owned();
                item.space_id = new_space_id;
                Ok((
                    format!("renamed Feed {current} to {new}"),
                    json!(item.clone()),
                ))
            }
            ResourceKind::Space => {
                if state.feeds.values().any(|feed| {
                    feed.status == ResourceStatus::Active
                        && is_within_namespace(&feed.name, current)
                }) {
                    return Err(ControlError::InvalidOperation(
                        "rename child Feeds before renaming a Space".to_owned(),
                    ));
                }
                ensure_name_available(
                    state.spaces.values().map(|item| (&item.name, &item.status)),
                    new,
                )?;
                let item = active_space_mut(state, current)?;
                item.name = new.to_owned();
                Ok((
                    format!("renamed Space {current} to {new}"),
                    json!(item.clone()),
                ))
            }
            ResourceKind::Writer => rename_named(&mut state.writers, current, new, "Writer"),
            ResourceKind::Reader => rename_named(&mut state.readers, current, new, "Reader"),
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
                {
                    return Err(ControlError::InvalidOperation(
                        "drop attached Writers and Readers first".to_owned(),
                    ));
                }
                let item = state.feeds.get_mut(&feed_id).expect("Feed exists");
                item.status = ResourceStatus::Dropped;
                Ok((
                    format!("dropped Feed {name} without purging stored history"),
                    json!(item.clone()),
                ))
            }
            ResourceKind::Space => {
                let space_id = active_space(state, name)?.space_id;
                if state
                    .feeds
                    .values()
                    .any(|item| item.status == ResourceStatus::Active && item.space_id == space_id)
                {
                    return Err(ControlError::InvalidOperation(
                        "drop child Feeds before dropping a Space".to_owned(),
                    ));
                }
                let item = state.spaces.get_mut(&space_id).expect("Space exists");
                item.status = ResourceStatus::Dropped;
                Ok((format!("dropped Space {name}"), json!(item.clone())))
            }
            ResourceKind::Writer => drop_named(&mut state.writers, name, "Writer"),
            ResourceKind::Reader => drop_named(&mut state.readers, name, "Reader"),
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
            Some("READER") => {
                expect_keyword(&tokens, 3, "FROM")?;
                let start = if tokens.len() == 5 {
                    ReaderStart::Beginning
                } else {
                    expect_keyword(&tokens, 5, "START")?;
                    expect_keyword(&tokens, 6, "AT")?;
                    parse_reader_start(&tokens, 7)?
                };
                Ok(Command::CreateReader {
                    name: token(&tokens, 2)?.to_owned(),
                    feed: token(&tokens, 4)?.to_owned(),
                    start,
                })
            }
            _ => Err(ControlError::Syntax(
                "CREATE supports SPACE, FEED, WRITER, READER, and ROLE".to_owned(),
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
        _ => Err(ControlError::Syntax(
            "Reader position must be BEGINNING, NOW, or CURSOR '<value>'".to_owned(),
        )),
    }
}

fn parse_resource_kind(token: &str) -> Result<ResourceKind, ControlError> {
    match token.to_ascii_uppercase().as_str() {
        "SPACE" => Ok(ResourceKind::Space),
        "FEED" => Ok(ResourceKind::Feed),
        "WRITER" => Ok(ResourceKind::Writer),
        "READER" => Ok(ResourceKind::Reader),
        "ROLE" => Ok(ResourceKind::Role),
        _ => Err(ControlError::Syntax(format!(
            "unsupported resource type {token}"
        ))),
    }
}

fn parse_show_kind(token: &str) -> Result<ShowKind, ControlError> {
    match token.to_ascii_uppercase().as_str() {
        "SPACES" => Ok(ShowKind::Spaces),
        "FEEDS" => Ok(ShowKind::Feeds),
        "WRITERS" => Ok(ShowKind::Writers),
        "READERS" => Ok(ShowKind::Readers),
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
        .ok_or_else(|| ControlError::NotFound(format!("owning Space for Feed {feed}")))
}

fn active_space<'a>(
    state: &'a CatalogState,
    name: &str,
) -> Result<&'a SpaceDefinition, ControlError> {
    state
        .spaces
        .values()
        .find(|item| item.name == name && item.status == ResourceStatus::Active)
        .ok_or_else(|| ControlError::NotFound(format!("Space {name}")))
}

fn active_space_mut<'a>(
    state: &'a mut CatalogState,
    name: &str,
) -> Result<&'a mut SpaceDefinition, ControlError> {
    state
        .spaces
        .values_mut()
        .find(|item| item.name == name && item.status == ResourceStatus::Active)
        .ok_or_else(|| ControlError::NotFound(format!("Space {name}")))
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
        ShowKind::Spaces => json!(state
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
        ResourceKind::Space => Ok(json!(active_space(state, name)?)),
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
        Command::CreateSpace { .. } => "CREATE SPACE",
        Command::CreateFeed { .. } => "CREATE FEED",
        Command::CreateWriter { .. } => "CREATE WRITER",
        Command::OpenWriterSession { .. } => "OPEN WRITER SESSION",
        Command::AllocateWriterSequence { .. } => "ALLOCATE WRITER SEQUENCE",
        Command::RevokeWriterSession { .. } => "REVOKE WRITER SESSION",
        Command::CreateReader { .. } => "CREATE READER",
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
    }
    .to_owned()
}

fn resource_name(kind: &ResourceKind) -> &'static str {
    match kind {
        ResourceKind::Space => "Space",
        ResourceKind::Feed => "Feed",
        ResourceKind::Writer => "Writer",
        ResourceKind::Reader => "Reader",
        ResourceKind::Role => "Role",
    }
}

fn show_name(kind: &ShowKind) -> &'static str {
    match kind {
        ShowKind::Spaces => "Spaces",
        ShowKind::Feeds => "Feeds",
        ShowKind::Writers => "Writers",
        ShowKind::Readers => "Readers",
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
