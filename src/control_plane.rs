use std::{
    collections::BTreeMap,
    fmt::Debug,
    fs::{self, File},
    io::{Cursor, Write},
    ops::RangeBounds,
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use openraft::{
    error::{
        Fatal, InstallSnapshotError, NetworkError, RPCError, RaftError, ReplicationClosed,
        StreamingError, Unreachable,
    },
    network::{RPCOption, RaftNetwork, RaftNetworkFactory},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, SnapshotResponse, VoteRequest, VoteResponse,
    },
    storage::{Adaptor, LogState, RaftLogReader, RaftSnapshotBuilder, Snapshot},
    BasicNode, Config, Entry, EntryPayload, LogId, OptionalSend, Raft, RaftLogId, RaftStorage,
    RaftTypeConfig, SnapshotMeta, StorageError, StorageIOError, StoredMembership, Vote,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::control::{
    derive_command_request_id, parse_wcl, Command, ControlController, ControlError,
    ControlExecution, ReplicatedCommand, ReplicatedCommandResult,
};

pub type ControlNodeId = u64;

openraft::declare_raft_types!(
    pub ControlTypeConfig:
        D = ReplicatedCommand,
        R = ReplicatedCommandResult,
        Node = BasicNode,
);

pub type ControlRaft = Raft<ControlTypeConfig>;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PersistedSnapshot {
    meta: SnapshotMeta<ControlNodeId, BasicNode>,
    data: Vec<u8>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct StateMachineMetadata {
    last_applied_log: Option<LogId<ControlNodeId>>,
    last_membership: StoredMembership<ControlNodeId, BasicNode>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
struct PersistentRaftData {
    last_purged_log_id: Option<LogId<ControlNodeId>>,
    committed: Option<LogId<ControlNodeId>>,
    log: BTreeMap<u64, Entry<ControlTypeConfig>>,
    vote: Option<Vote<ControlNodeId>>,
    state_machine: StateMachineMetadata,
    snapshot_index: u64,
    current_snapshot: Option<PersistedSnapshot>,
}

#[derive(Clone)]
struct PersistentRaftStore {
    path: Arc<PathBuf>,
    data: Arc<RwLock<PersistentRaftData>>,
    controller: Arc<ControlController>,
}

impl PersistentRaftStore {
    fn open(
        path: impl Into<PathBuf>,
        controller: Arc<ControlController>,
    ) -> Result<Self, ControlPlaneError> {
        let path = path.into();
        let data = if path.exists() {
            serde_json::from_slice(&fs::read(&path)?)?
        } else {
            PersistentRaftData::default()
        };
        Ok(Self {
            path: Arc::new(path),
            data: Arc::new(RwLock::new(data)),
            controller,
        })
    }

    fn persist(&self, data: &PersistentRaftData) -> Result<(), std::io::Error> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        let bytes = serde_json::to_vec(data).map_err(std::io::Error::other)?;
        let temporary = self.path.with_extension("tmp");
        let mut file = File::create(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        if self.path.exists() {
            fs::remove_file(self.path.as_ref())?;
        }
        fs::rename(temporary, self.path.as_ref())?;
        Ok(())
    }
}

impl RaftLogReader<ControlTypeConfig> for PersistentRaftStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<ControlTypeConfig>>, StorageError<ControlNodeId>> {
        Ok(self
            .data
            .read()
            .await
            .log
            .range(range)
            .map(|(_, entry)| entry.clone())
            .collect())
    }
}

impl RaftSnapshotBuilder<ControlTypeConfig> for PersistentRaftStore {
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<ControlTypeConfig>, StorageError<ControlNodeId>> {
        let catalog = self
            .controller
            .snapshot_bytes()
            .await
            .map_err(|error| StorageIOError::read_state_machine(&error))?;
        let mut data = self.data.write().await;
        data.snapshot_index = data.snapshot_index.saturating_add(1);
        let snapshot_data = serde_json::to_vec(&ControlSnapshotData {
            catalog,
            last_applied_log: data.state_machine.last_applied_log,
            last_membership: data.state_machine.last_membership.clone(),
        })
        .map_err(|error| StorageIOError::read_state_machine(&error))?;
        let snapshot_id = data
            .state_machine
            .last_applied_log
            .map(|log_id| {
                format!(
                    "{}-{}-{}",
                    log_id.leader_id, log_id.index, data.snapshot_index
                )
            })
            .unwrap_or_else(|| format!("--{}", data.snapshot_index));
        let meta = SnapshotMeta {
            last_log_id: data.state_machine.last_applied_log,
            last_membership: data.state_machine.last_membership.clone(),
            snapshot_id,
        };
        data.current_snapshot = Some(PersistedSnapshot {
            meta: meta.clone(),
            data: snapshot_data.clone(),
        });
        self.persist(&data)
            .map_err(|error| StorageIOError::write_state_machine(&error))?;
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(snapshot_data)),
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ControlSnapshotData {
    catalog: Vec<u8>,
    last_applied_log: Option<LogId<ControlNodeId>>,
    last_membership: StoredMembership<ControlNodeId, BasicNode>,
}

impl RaftStorage<ControlTypeConfig> for PersistentRaftStore {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn get_log_state(
        &mut self,
    ) -> Result<LogState<ControlTypeConfig>, StorageError<ControlNodeId>> {
        let data = self.data.read().await;
        let last_log_id = data
            .log
            .values()
            .next_back()
            .map(|entry| *entry.get_log_id())
            .or(data.last_purged_log_id);
        Ok(LogState {
            last_purged_log_id: data.last_purged_log_id,
            last_log_id,
        })
    }

    async fn save_vote(
        &mut self,
        vote: &Vote<ControlNodeId>,
    ) -> Result<(), StorageError<ControlNodeId>> {
        let mut data = self.data.write().await;
        data.vote = Some(*vote);
        Ok(self
            .persist(&data)
            .map_err(|error| StorageIOError::write_vote(&error))?)
    }

    async fn read_vote(
        &mut self,
    ) -> Result<Option<Vote<ControlNodeId>>, StorageError<ControlNodeId>> {
        Ok(self.data.read().await.vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<ControlNodeId>>,
    ) -> Result<(), StorageError<ControlNodeId>> {
        let mut data = self.data.write().await;
        data.committed = committed;
        Ok(self
            .persist(&data)
            .map_err(|error| StorageIOError::write_logs(&error))?)
    }

    async fn read_committed(
        &mut self,
    ) -> Result<Option<LogId<ControlNodeId>>, StorageError<ControlNodeId>> {
        Ok(self.data.read().await.committed)
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<ControlNodeId>>,
            StoredMembership<ControlNodeId, BasicNode>,
        ),
        StorageError<ControlNodeId>,
    > {
        let data = self.data.read().await;
        Ok((
            data.state_machine.last_applied_log,
            data.state_machine.last_membership.clone(),
        ))
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<ControlNodeId>,
    ) -> Result<(), StorageError<ControlNodeId>> {
        let mut data = self.data.write().await;
        data.log.retain(|index, _| *index < log_id.index);
        Ok(self
            .persist(&data)
            .map_err(|error| StorageIOError::write_logs(&error))?)
    }

    async fn purge_logs_upto(
        &mut self,
        log_id: LogId<ControlNodeId>,
    ) -> Result<(), StorageError<ControlNodeId>> {
        let mut data = self.data.write().await;
        data.last_purged_log_id = Some(log_id);
        data.log.retain(|index, _| *index > log_id.index);
        Ok(self
            .persist(&data)
            .map_err(|error| StorageIOError::write_logs(&error))?)
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<ControlNodeId>>
    where
        I: IntoIterator<Item = Entry<ControlTypeConfig>> + OptionalSend,
    {
        let mut data = self.data.write().await;
        for entry in entries {
            data.log.insert(entry.log_id.index, entry);
        }
        Ok(self
            .persist(&data)
            .map_err(|error| StorageIOError::write_logs(&error))?)
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<ControlTypeConfig>],
    ) -> Result<Vec<ReplicatedCommandResult>, StorageError<ControlNodeId>> {
        let mut results = Vec::with_capacity(entries.len());
        for entry in entries {
            let result = match &entry.payload {
                EntryPayload::Blank => ReplicatedCommandResult {
                    revision: self.controller.revision().await,
                    result: None,
                    error: None,
                },
                EntryPayload::Normal(command) => {
                    self.controller.apply_replicated(command.clone()).await
                }
                EntryPayload::Membership(membership) => {
                    let mut data = self.data.write().await;
                    data.state_machine.last_membership =
                        StoredMembership::new(Some(entry.log_id), membership.clone());
                    ReplicatedCommandResult {
                        revision: self.controller.revision().await,
                        result: None,
                        error: None,
                    }
                }
            };
            let mut data = self.data.write().await;
            data.state_machine.last_applied_log = Some(entry.log_id);
            self.persist(&data)
                .map_err(|error| StorageIOError::write_state_machine(&error))?;
            results.push(result);
        }
        Ok(results)
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<<ControlTypeConfig as RaftTypeConfig>::SnapshotData>, StorageError<ControlNodeId>>
    {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<ControlNodeId, BasicNode>,
        snapshot: Box<<ControlTypeConfig as RaftTypeConfig>::SnapshotData>,
    ) -> Result<(), StorageError<ControlNodeId>> {
        let bytes = snapshot.into_inner();
        let snapshot_data: ControlSnapshotData = serde_json::from_slice(&bytes)
            .map_err(|error| StorageIOError::read_snapshot(Some(meta.signature()), &error))?;
        self.controller
            .install_snapshot_bytes(&snapshot_data.catalog)
            .await
            .map_err(|error| StorageIOError::write_state_machine(&error))?;
        let mut data = self.data.write().await;
        data.state_machine.last_applied_log = snapshot_data.last_applied_log;
        data.state_machine.last_membership = snapshot_data.last_membership;
        data.current_snapshot = Some(PersistedSnapshot {
            meta: meta.clone(),
            data: bytes,
        });
        Ok(self
            .persist(&data)
            .map_err(|error| StorageIOError::write_state_machine(&error))?)
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<ControlTypeConfig>>, StorageError<ControlNodeId>> {
        Ok(self
            .data
            .read()
            .await
            .current_snapshot
            .as_ref()
            .map(|snapshot| Snapshot {
                meta: snapshot.meta.clone(),
                snapshot: Box::new(Cursor::new(snapshot.data.clone())),
            }))
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }
}

#[derive(Clone)]
struct ControlNetworkFactory {
    key: String,
    http: reqwest::Client,
}

struct ControlNetwork {
    target: ControlNodeId,
    address: String,
    key: String,
    http: reqwest::Client,
}

impl RaftNetworkFactory<ControlTypeConfig> for ControlNetworkFactory {
    type Network = ControlNetwork;

    async fn new_client(&mut self, target: ControlNodeId, node: &BasicNode) -> Self::Network {
        ControlNetwork {
            target,
            address: node.addr.trim_end_matches('/').to_owned(),
            key: self.key.clone(),
            http: self.http.clone(),
        }
    }
}

impl ControlNetwork {
    async fn request<Request, Response, Error>(
        &self,
        path: &str,
        request: &Request,
    ) -> Result<Response, RPCError<ControlNodeId, BasicNode, Error>>
    where
        Request: Serialize + ?Sized,
        Response: DeserializeOwned,
        Error: std::error::Error + DeserializeOwned,
    {
        let url = format!("http://{}{}", self.address, path);
        let response = self
            .http
            .post(&url)
            .header("x-whitewater-control-key", &self.key)
            .json(request)
            .send()
            .await
            .map_err(|error| RPCError::Unreachable(Unreachable::new(&error)))?;
        if !response.status().is_success() {
            let error = std::io::Error::other(format!("HTTP {} from {url}", response.status()));
            return Err(RPCError::Network(NetworkError::new(&error)));
        }
        let result: Result<Response, Error> = response
            .json()
            .await
            .map_err(|error| RPCError::Network(NetworkError::new(&error)))?;
        result.map_err(|error| {
            RPCError::RemoteError(openraft::error::RemoteError::new(self.target, error))
        })
    }
}

impl RaftNetwork<ControlTypeConfig> for ControlNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<ControlTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        AppendEntriesResponse<ControlNodeId>,
        RPCError<ControlNodeId, BasicNode, RaftError<ControlNodeId>>,
    > {
        self.request("/internal/control-plane/raft/append", &rpc)
            .await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<ControlNodeId>,
        _option: RPCOption,
    ) -> Result<
        VoteResponse<ControlNodeId>,
        RPCError<ControlNodeId, BasicNode, RaftError<ControlNodeId>>,
    > {
        self.request("/internal/control-plane/raft/vote", &rpc)
            .await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<ControlTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<ControlNodeId>,
        RPCError<ControlNodeId, BasicNode, RaftError<ControlNodeId, InstallSnapshotError>>,
    > {
        self.request("/internal/control-plane/raft/install-snapshot", &rpc)
            .await
    }

    async fn full_snapshot(
        &mut self,
        vote: Vote<ControlNodeId>,
        snapshot: Snapshot<ControlTypeConfig>,
        _cancel: impl std::future::Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<
        SnapshotResponse<ControlNodeId>,
        StreamingError<ControlTypeConfig, Fatal<ControlNodeId>>,
    > {
        let request = FullSnapshotRequest {
            vote,
            meta: snapshot.meta,
            data: snapshot.snapshot.into_inner(),
        };
        self.request("/internal/control-plane/raft/snapshot", &request)
            .await
            .map_err(|error| match error {
                RPCError::Unreachable(error) => StreamingError::Unreachable(error),
                RPCError::Network(error) => StreamingError::Network(error),
                RPCError::RemoteError(error) => StreamingError::RemoteError(error),
                RPCError::Timeout(error) => StreamingError::Timeout(error),
                RPCError::PayloadTooLarge(error) => {
                    let error = std::io::Error::other(error.to_string());
                    StreamingError::Network(NetworkError::new(&error))
                }
            })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FullSnapshotRequest {
    pub vote: Vote<ControlNodeId>,
    pub meta: SnapshotMeta<ControlNodeId, BasicNode>,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InternalCommandsRequest {
    pub request_id: Uuid,
    pub commands: Vec<Command>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InternalCommandsResponse {
    pub execution: Option<ControlExecution>,
    pub error: Option<String>,
    pub leader_id: Option<ControlNodeId>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InternalWriteResponse {
    pub result: Option<ReplicatedCommandResult>,
    pub error: Option<String>,
    pub leader_id: Option<ControlNodeId>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ControlPlaneStatus {
    pub node_id: ControlNodeId,
    pub leader_id: Option<ControlNodeId>,
    pub state: String,
    pub current_term: u64,
    pub last_log_index: Option<u64>,
    pub last_applied_index: Option<u64>,
    pub membership: Vec<ControlNodeId>,
    pub catalog_revision: u64,
}

#[derive(Debug, Error)]
pub enum ControlPlaneError {
    #[error("Control Plane is unavailable: {0}")]
    Unavailable(String),
    #[error("Control Plane rejected command: {0}")]
    Rejected(String),
    #[error("Control Plane storage failed: {0}")]
    Storage(String),
    #[error(transparent)]
    Control(#[from] ControlError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Serialization(#[from] serde_json::Error),
}

#[derive(Clone)]
pub struct ControlPlane {
    node_id: ControlNodeId,
    peers: Arc<BTreeMap<ControlNodeId, BasicNode>>,
    key: String,
    raft: ControlRaft,
    controller: Arc<ControlController>,
    http: reqwest::Client,
}

impl ControlPlane {
    /// The replicated catalog controller this plane applies committed
    /// commands into. Read-only accessor for callers that need
    /// placement-authority reads alongside raft-backed writes.
    pub fn controller(&self) -> &std::sync::Arc<crate::control::ControlController> {
        &self.controller
    }

    pub async fn start(
        node_id: ControlNodeId,
        peers: BTreeMap<ControlNodeId, BasicNode>,
        key: String,
        storage_path: impl Into<PathBuf>,
        controller: Arc<ControlController>,
    ) -> Result<Self, ControlPlaneError> {
        if peers.len() < 3 || !peers.contains_key(&node_id) {
            return Err(ControlPlaneError::Unavailable(
                "Control Plane requires at least three configured voters including this Node"
                    .to_owned(),
            ));
        }
        if key.len() < 24 {
            return Err(ControlPlaneError::Unavailable(
                "FINNSTREAM_CONTROL_PLANE_KEY must contain at least 24 characters".to_owned(),
            ));
        }
        let store = PersistentRaftStore::open(storage_path, controller.clone())?;
        let (log_store, state_machine) = Adaptor::new(store);
        let config = Arc::new(
            Config {
                cluster_name: "whitewater-control-plane".to_owned(),
                heartbeat_interval: 500,
                election_timeout_min: 1_500,
                election_timeout_max: 3_000,
                snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(500),
                max_in_snapshot_log_to_keep: 100,
                ..Default::default()
            }
            .validate()
            .map_err(|error| ControlPlaneError::Unavailable(error.to_string()))?,
        );
        let network = ControlNetworkFactory {
            key: key.clone(),
            http: reqwest::Client::new(),
        };
        let raft = Raft::new(node_id, config, network, log_store, state_machine)
            .await
            .map_err(|error| ControlPlaneError::Unavailable(error.to_string()))?;
        Ok(Self {
            node_id,
            peers: Arc::new(peers),
            key,
            raft,
            controller,
            http: reqwest::Client::new(),
        })
    }

    pub async fn initialize(&self) -> Result<(), ControlPlaneError> {
        if self
            .raft
            .is_initialized()
            .await
            .map_err(|error| ControlPlaneError::Unavailable(error.to_string()))?
        {
            return Ok(());
        }
        self.raft
            .initialize(self.peers.as_ref().clone())
            .await
            .map_err(|error| ControlPlaneError::Unavailable(error.to_string()))
    }

    pub async fn execute_wcl(&self, script: &str) -> Result<ControlExecution, ControlPlaneError> {
        self.execute_wcl_with_request_id(script, Uuid::new_v4())
            .await
    }

    pub async fn execute_wcl_with_request_id(
        &self,
        script: &str,
        request_id: Uuid,
    ) -> Result<ControlExecution, ControlPlaneError> {
        self.execute_commands_with_request_id(parse_wcl(script)?, request_id)
            .await
    }

    pub async fn execute_commands(
        &self,
        commands: Vec<Command>,
    ) -> Result<ControlExecution, ControlPlaneError> {
        self.execute_commands_with_request_id(commands, Uuid::new_v4())
            .await
    }

    pub async fn execute_commands_with_request_id(
        &self,
        commands: Vec<Command>,
        request_id: Uuid,
    ) -> Result<ControlExecution, ControlPlaneError> {
        if commands.is_empty() {
            return Err(ControlPlaneError::Rejected(
                "request contains no commands".to_owned(),
            ));
        }
        match self.raft.current_leader().await {
            Some(leader_id) if leader_id != self.node_id => {
                return self.forward_commands(leader_id, commands, request_id).await;
            }
            None => {
                return Err(ControlPlaneError::Unavailable(
                    "leader election is in progress".to_owned(),
                ));
            }
            _ => {}
        }
        let mut results = Vec::with_capacity(commands.len());
        let mut revision = self.controller.revision().await;
        let issued_at_ns = unix_ns();
        for (index, command) in commands.into_iter().enumerate() {
            if command.is_read_only() {
                tokio::time::timeout(Duration::from_secs(5), self.raft.ensure_linearizable())
                    .await
                    .map_err(|_| {
                        ControlPlaneError::Unavailable(
                            "linearizable read timed out; the Control Plane may have lost quorum"
                                .to_owned(),
                        )
                    })?
                    .map_err(|error| ControlPlaneError::Unavailable(error.to_string()))?;
                results.push(self.controller.execute_query(command).await?);
                revision = self.controller.revision().await;
                continue;
            }
            let command_request_id = derive_command_request_id(request_id, index);
            let response = match self.controller.applied_result(command_request_id).await {
                Some(response) => response,
                None => {
                    let replicated = self
                        .controller
                        .prepare_replicated(command_request_id, issued_at_ns, command)
                        .await?;
                    self.submit(replicated).await?
                }
            };
            revision = response.revision;
            match (response.result, response.error) {
                (Some(result), None) => results.push(result),
                (_, Some(error)) => return Err(ControlPlaneError::Rejected(error)),
                _ => {}
            }
        }
        Ok(ControlExecution {
            request_id,
            revision,
            authority: "control_plane".to_owned(),
            warning: String::new(),
            results,
        })
    }

    async fn forward_commands(
        &self,
        leader_id: ControlNodeId,
        commands: Vec<Command>,
        request_id: Uuid,
    ) -> Result<ControlExecution, ControlPlaneError> {
        let leader = self.peers.get(&leader_id).ok_or_else(|| {
            ControlPlaneError::Unavailable(format!("leader {leader_id} has no address"))
        })?;
        let response = self
            .http
            .post(format!(
                "http://{}/internal/control-plane/commands",
                leader.addr.trim_end_matches('/')
            ))
            .header("x-whitewater-control-key", &self.key)
            .json(&InternalCommandsRequest {
                request_id,
                commands,
            })
            .send()
            .await
            .map_err(|error| ControlPlaneError::Unavailable(error.to_string()))?;
        if !response.status().is_success() {
            return Err(ControlPlaneError::Unavailable(format!(
                "leader returned HTTP {}",
                response.status()
            )));
        }
        let response: InternalCommandsResponse = response
            .json()
            .await
            .map_err(|error| ControlPlaneError::Unavailable(error.to_string()))?;
        response.execution.ok_or_else(|| {
            ControlPlaneError::Unavailable(
                response
                    .error
                    .unwrap_or_else(|| "leader returned no execution".to_owned()),
            )
        })
    }

    async fn submit(
        &self,
        command: ReplicatedCommand,
    ) -> Result<ReplicatedCommandResult, ControlPlaneError> {
        if self.raft.current_leader().await == Some(self.node_id) {
            return self.write_local(command).await;
        }
        let leader_id = self.raft.current_leader().await.ok_or_else(|| {
            ControlPlaneError::Unavailable("leader election is in progress".to_owned())
        })?;
        let leader = self.peers.get(&leader_id).ok_or_else(|| {
            ControlPlaneError::Unavailable(format!("leader {leader_id} has no address"))
        })?;
        let response = self
            .http
            .post(format!(
                "http://{}/internal/control-plane/write",
                leader.addr.trim_end_matches('/')
            ))
            .header("x-whitewater-control-key", &self.key)
            .json(&command)
            .send()
            .await
            .map_err(|error| ControlPlaneError::Unavailable(error.to_string()))?;
        if !response.status().is_success() {
            return Err(ControlPlaneError::Unavailable(format!(
                "leader returned HTTP {}",
                response.status()
            )));
        }
        let response: InternalWriteResponse = response
            .json()
            .await
            .map_err(|error| ControlPlaneError::Unavailable(error.to_string()))?;
        response.result.ok_or_else(|| {
            ControlPlaneError::Unavailable(
                response
                    .error
                    .unwrap_or_else(|| "leader returned no result".to_owned()),
            )
        })
    }

    pub async fn write_local(
        &self,
        command: ReplicatedCommand,
    ) -> Result<ReplicatedCommandResult, ControlPlaneError> {
        tokio::time::timeout(Duration::from_secs(5), self.raft.client_write(command))
            .await
            .map_err(|_| {
                ControlPlaneError::Unavailable(
                    "majority commit timed out; the Control Plane may have lost quorum".to_owned(),
                )
            })?
            .map(|response| response.data)
            .map_err(|error| ControlPlaneError::Unavailable(error.to_string()))
    }

    pub async fn status(&self) -> ControlPlaneStatus {
        let metrics = self.raft.metrics().borrow().clone();
        ControlPlaneStatus {
            node_id: self.node_id,
            leader_id: metrics.current_leader,
            state: format!("{:?}", metrics.state).to_ascii_lowercase(),
            current_term: metrics.current_term,
            last_log_index: metrics.last_log_index,
            last_applied_index: metrics.last_applied.map(|log_id| log_id.index),
            membership: metrics.membership_config.membership().voter_ids().collect(),
            catalog_revision: self.controller.revision().await,
        }
    }

    pub fn raft(&self) -> &ControlRaft {
        &self.raft
    }

    pub fn authorize_internal(&self, supplied: Option<&str>) -> bool {
        supplied
            .is_some_and(|supplied| constant_time_equal(self.key.as_bytes(), supplied.as_bytes()))
    }
}

fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

fn unix_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .try_into()
        .unwrap_or(i64::MAX)
}
