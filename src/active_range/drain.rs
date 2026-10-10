use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use serde::Serialize;
use thiserror::Error;

use crate::{
    active_range::{
        FollowerMoveControl, FollowerMoveError, FollowerMoveExecutor, MajorityAppendCoordinator,
        OwnerMoveControl, OwnerMoveError, OwnerMoveExecutor, ReplicaAppendService,
        ReplicaTransport, StorageNodeId,
    },
    control::{Command, ControlController, RangeMovePlan, RangeOwnerMovePlan, StorageDrainPlan},
};

/// Executes catalog mutations required by a drain plan. Test and local
/// deployments route through `ControlController`; replicated deployments route
/// through the Control Plane so every step is majority-committed.
#[async_trait]
pub trait DrainCommandAuthority: Send + Sync {
    async fn execute(&self, command: Command) -> Result<serde_json::Value, String>;
}

/// Executes one planned drain step. Production routes range moves through the
/// admin move endpoints (the same verified copy/freeze/activate orchestration
/// operators invoke) and Subscription progress commands through the Control
/// Plane; single-process deployments route through `LocalDrainDriver`.
#[async_trait]
pub trait DrainMoveDriver: Send + Sync {
    async fn apply(&self, command: Command) -> Result<(), String>;
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct StorageDrainReport {
    pub node: StorageNodeId,
    pub completed_moves: usize,
    pub unplannable: Vec<String>,
    pub ready_to_retire: bool,
}

/// Outcome of one supervisor drain pass over a single storage Node.
#[derive(Clone, Debug)]
pub struct StorageDrainOutcome {
    pub node: StorageNodeId,
    pub result: Result<StorageDrainReport, String>,
}

#[derive(Debug, Error)]
pub enum StorageDrainError {
    #[error("drain planning failed: {0}")]
    Plan(String),
    #[error("drain command failed: {0}")]
    Command(String),
    #[error("follower movement failed: {0}")]
    FollowerMove(#[from] FollowerMoveError),
    #[error("owner movement failed: {0}")]
    OwnerMove(#[from] OwnerMoveError),
    #[error("no local replica service exists for Node {0}")]
    MissingService(StorageNodeId),
    #[error("the drain plan did not make progress")]
    Stalled,
    #[error("drain plan emitted an unexpected command")]
    UnexpectedCommand,
}

const MAX_DRAIN_STEPS: usize = 64;
const DEFAULT_DRAIN_BATCH: usize = 32;

/// Executes `INSPECT DRAIN` plans one move at a time, re-planning after every
/// committed step so each command is applied against the newest placement.
/// Moves are the same verified owner/follower machinery used by operator-driven
/// movement; nothing is a metadata-only swap.
pub struct StorageDrainExecutor {
    control: Arc<ControlController>,
    commands: Arc<dyn DrainCommandAuthority>,
    follower_control: Arc<dyn FollowerMoveControl>,
    owner_control: Arc<dyn OwnerMoveControl>,
    transport: Arc<dyn ReplicaTransport>,
    batch_size: usize,
}

impl StorageDrainExecutor {
    pub fn new(
        control: Arc<ControlController>,
        commands: Arc<dyn DrainCommandAuthority>,
        follower_control: Arc<dyn FollowerMoveControl>,
        owner_control: Arc<dyn OwnerMoveControl>,
        transport: Arc<dyn ReplicaTransport>,
    ) -> Self {
        Self {
            control,
            commands,
            follower_control,
            owner_control,
            transport,
            batch_size: DEFAULT_DRAIN_BATCH,
        }
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size.max(1);
        self
    }

    pub async fn plan(&self, node: &StorageNodeId) -> Result<StorageDrainPlan, StorageDrainError> {
        self.control
            .storage_drain_plan(node)
            .await
            .map_err(|error| StorageDrainError::Plan(error.to_string()))
    }

    /// Vacates every Active Range and Subscription progress reference held by
    /// `node`. Returns when the drain plan is empty; `ready_to_retire` in the
    /// report means `RETIRE STORAGE NODE` will be accepted.
    pub async fn drain(
        &self,
        node: &StorageNodeId,
        services: &BTreeMap<StorageNodeId, Arc<ReplicaAppendService>>,
    ) -> Result<StorageDrainReport, StorageDrainError> {
        let mut completed_moves = 0_usize;
        loop {
            let plan = self.plan(node).await?;
            if plan.moves.is_empty() {
                return Ok(StorageDrainReport {
                    node: node.clone(),
                    completed_moves,
                    unplannable: plan.unplannable,
                    ready_to_retire: plan.ready_to_retire,
                });
            }
            if completed_moves >= MAX_DRAIN_STEPS {
                return Err(StorageDrainError::Stalled);
            }
            self.execute_step(plan.moves[0].clone(), services).await?;
            completed_moves += 1;
        }
    }

    /// Executes a single planned drain step against the provided in-process
    /// replica services.
    pub async fn execute_step(
        &self,
        command: Command,
        services: &BTreeMap<StorageNodeId, Arc<ReplicaAppendService>>,
    ) -> Result<(), StorageDrainError> {
        match command.clone() {
            Command::PrepareFollowerMove { feed, .. } => {
                let data = self.execute(command).await?;
                let plan: RangeMovePlan = serde_json::from_value(data)
                    .map_err(|error| StorageDrainError::Command(error.to_string()))?;
                let source = services
                    .get(&plan.source_assignment.owner)
                    .cloned()
                    .ok_or_else(|| {
                        StorageDrainError::MissingService(plan.source_assignment.owner.clone())
                    })?;
                let replacement = services
                    .get(&plan.replacement_replica)
                    .cloned()
                    .ok_or_else(|| {
                        StorageDrainError::MissingService(plan.replacement_replica.clone())
                    })?;
                let coordinator = Arc::new(MajorityAppendCoordinator::new(
                    source.clone(),
                    self.control.clone(),
                    self.transport.clone(),
                ));
                let executor = FollowerMoveExecutor::new(
                    self.control.clone(),
                    coordinator,
                    source,
                    replacement,
                );
                executor
                    .finalize(
                        self.follower_control.as_ref(),
                        &feed,
                        &plan,
                        self.batch_size,
                    )
                    .await?;
            }
            Command::PrepareOwnerMove { feed, .. } => {
                let data = self.execute(command).await?;
                let plan: RangeOwnerMovePlan = serde_json::from_value(data)
                    .map_err(|error| StorageDrainError::Command(error.to_string()))?;
                let source = services
                    .get(&plan.source_assignment.owner)
                    .cloned()
                    .ok_or_else(|| {
                        StorageDrainError::MissingService(plan.source_assignment.owner.clone())
                    })?;
                let target = services
                    .get(&plan.candidate_assignment.owner)
                    .cloned()
                    .ok_or_else(|| {
                        StorageDrainError::MissingService(plan.candidate_assignment.owner.clone())
                    })?;
                let coordinator = Arc::new(MajorityAppendCoordinator::new(
                    source.clone(),
                    self.control.clone(),
                    self.transport.clone(),
                ));
                let executor =
                    OwnerMoveExecutor::new(self.control.clone(), coordinator, source, target);
                executor
                    .finalize(self.owner_control.as_ref(), &feed, &plan)
                    .await?;
            }
            command @ (Command::MoveSubscriptionProgressReplica { .. }
            | Command::RecoverSubscriptionProgressOwner { .. }) => {
                self.execute(command).await?;
            }
            _ => return Err(StorageDrainError::UnexpectedCommand),
        }
        Ok(())
    }

    async fn execute(&self, command: Command) -> Result<serde_json::Value, StorageDrainError> {
        self.commands
            .execute(command)
            .await
            .map_err(StorageDrainError::Command)
    }
}

/// Drives planned drain steps through the in-process `StorageDrainExecutor`.
/// Suitable for single-process deployments and tests where every replica
/// service is reachable locally; multi-Node deployments use an HTTP driver
/// that invokes the admin move endpoints on the live cluster instead.
pub struct LocalDrainDriver {
    executor: StorageDrainExecutor,
    services: Arc<BTreeMap<StorageNodeId, Arc<ReplicaAppendService>>>,
}

impl LocalDrainDriver {
    pub fn new(
        executor: StorageDrainExecutor,
        services: Arc<BTreeMap<StorageNodeId, Arc<ReplicaAppendService>>>,
    ) -> Self {
        Self { executor, services }
    }
}

#[async_trait]
impl DrainMoveDriver for LocalDrainDriver {
    async fn apply(&self, command: Command) -> Result<(), String> {
        self.executor
            .execute_step(command, &self.services)
            .await
            .map_err(|error| error.to_string())
    }
}

/// Leader-side scheduler that walks every Node marked `DRAIN STORAGE NODE`
/// and executes its drain plan through a `DrainMoveDriver`. Each tick drains
/// the first pending move per Node; plans are re-derived after every step so
/// the driver never acts on stale placement. A Node is only reported
/// `ready_to_retire` when re-planning finds zero remaining references.
pub struct StorageDrainSupervisor {
    control: Arc<ControlController>,
    driver: Arc<dyn DrainMoveDriver>,
    max_steps_per_node: usize,
}

impl StorageDrainSupervisor {
    pub fn new(control: Arc<ControlController>, driver: Arc<dyn DrainMoveDriver>) -> Self {
        Self {
            control,
            driver,
            max_steps_per_node: MAX_DRAIN_STEPS,
        }
    }

    pub fn with_max_steps_per_node(mut self, max_steps_per_node: usize) -> Self {
        self.max_steps_per_node = max_steps_per_node.max(1);
        self
    }

    /// Drains every marked Node until its plan is exhausted, one Node at a
    /// time. A failing driver surfaces the error for that Node without
    /// blocking the others; the next tick retries from a fresh plan.
    pub async fn tick(&self) -> Vec<StorageDrainOutcome> {
        let mut outcomes = Vec::new();
        for node in self.control.draining_storage_nodes().await {
            outcomes.push(StorageDrainOutcome {
                node: node.clone(),
                result: self
                    .drain_node(&node)
                    .await
                    .map_err(|error| error.to_string()),
            });
        }
        outcomes
    }

    async fn drain_node(
        &self,
        node: &StorageNodeId,
    ) -> Result<StorageDrainReport, StorageDrainError> {
        let mut completed_moves = 0_usize;
        loop {
            let plan = self
                .control
                .storage_drain_plan(node)
                .await
                .map_err(|error| StorageDrainError::Plan(error.to_string()))?;
            if plan.moves.is_empty() {
                return Ok(StorageDrainReport {
                    node: node.clone(),
                    completed_moves,
                    unplannable: plan.unplannable,
                    ready_to_retire: plan.ready_to_retire,
                });
            }
            if completed_moves >= self.max_steps_per_node {
                return Err(StorageDrainError::Stalled);
            }
            self.driver
                .apply(plan.moves[0].clone())
                .await
                .map_err(StorageDrainError::Command)?;
            completed_moves += 1;
        }
    }
}
