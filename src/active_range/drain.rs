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

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct StorageDrainReport {
    pub node: StorageNodeId,
    pub completed_moves: usize,
    pub unplannable: Vec<String>,
    pub ready_to_retire: bool,
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
            match plan.moves[0].clone() {
                Command::PrepareFollowerMove { feed, .. } => {
                    let data = self.execute(plan.moves[0].clone()).await?;
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
                        source,
                        self.control.clone(),
                        self.transport.clone(),
                    ));
                    let executor = FollowerMoveExecutor::new(
                        self.control.clone(),
                        coordinator,
                        services
                            .get(&plan.source_assignment.owner)
                            .cloned()
                            .ok_or_else(|| {
                                StorageDrainError::MissingService(
                                    plan.source_assignment.owner.clone(),
                                )
                            })?,
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
                    let data = self.execute(plan.moves[0].clone()).await?;
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
                            StorageDrainError::MissingService(
                                plan.candidate_assignment.owner.clone(),
                            )
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
            completed_moves += 1;
        }
    }

    async fn execute(&self, command: Command) -> Result<serde_json::Value, StorageDrainError> {
        self.commands
            .execute(command)
            .await
            .map_err(StorageDrainError::Command)
    }
}
