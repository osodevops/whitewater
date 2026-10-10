use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};

use finnstream::active_range::{RangeId, StorageNodeId};
use finnstream::effect::{
    EffectCommitEvidence, EffectConsume, EffectCoordinator, EffectJournalAssignment,
    EffectJournalError, EffectJournalInspection, EffectJournalTransport, EffectMutation,
    EffectOutput, EffectPrepareVote, EffectReplicaReply, EffectTransition,
    FjallEffectJournalReplica,
};
use tempfile::TempDir;
use uuid::Uuid;

fn nodes() -> [StorageNodeId; 3] {
    ["effect-a", "effect-b", "effect-c"].map(|name| StorageNodeId::try_new(name).unwrap())
}

fn output(sequence: u64) -> EffectOutput {
    EffectOutput {
        feed_id: Uuid::new_v4(),
        key_base64: "a2V5".to_owned(),
        payload_base64: "cGF5bG9hZA==".to_owned(),
        event_time_ns: 1,
        writer_session_id: Uuid::new_v4(),
        sequence,
    }
}

fn declare(effect_id: Uuid, sequence: u64, epoch: u64) -> EffectMutation {
    EffectMutation {
        effect_id,
        ownership_epoch: epoch,
        sequence,
        request_id: Uuid::new_v4(),
        transition: EffectTransition::Declare {
            consume: Some(EffectConsume {
                subscription_id: Uuid::new_v4(),
                feed_id: Uuid::new_v4(),
                expected_cursor: None,
                cursor: "cursor-1".to_owned(),
                positions: BTreeMap::from([(
                    RangeId::from_uuid(Uuid::from_u128(9)),
                    "position-1".to_owned(),
                )]),
            }),
            outputs: vec![output(1)],
        },
    }
}

struct TestEffectTransport {
    stores: BTreeMap<StorageNodeId, Arc<FjallEffectJournalReplica>>,
    prepare_down: Mutex<BTreeSet<StorageNodeId>>,
    commit_down: Mutex<BTreeSet<StorageNodeId>>,
    read_down: Mutex<BTreeSet<StorageNodeId>>,
    wrong_node: Mutex<Option<StorageNodeId>>,
}

impl TestEffectTransport {
    fn new(directories: &[TempDir; 3], nodes: &[StorageNodeId; 3]) -> Self {
        Self {
            stores: nodes
                .iter()
                .cloned()
                .zip(directories.iter().map(|directory| {
                    Arc::new(FjallEffectJournalReplica::open(directory.path()).unwrap())
                }))
                .collect(),
            prepare_down: Mutex::new(BTreeSet::new()),
            commit_down: Mutex::new(BTreeSet::new()),
            read_down: Mutex::new(BTreeSet::new()),
            wrong_node: Mutex::new(None),
        }
    }

    fn claimed(&self, node: &StorageNodeId) -> StorageNodeId {
        self.wrong_node
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_else(|| node.clone())
    }
}

#[async_trait::async_trait]
impl EffectJournalTransport for TestEffectTransport {
    async fn prepare(
        &self,
        node: &StorageNodeId,
        mutation: EffectMutation,
    ) -> Result<EffectReplicaReply<EffectPrepareVote>, EffectJournalError> {
        if self.prepare_down.lock().unwrap().contains(node) {
            return Err(EffectJournalError::Unavailable);
        }
        let store = self
            .stores
            .get(node)
            .cloned()
            .ok_or(EffectJournalError::InvalidAssignment)?;
        let effect_id = mutation.effect_id;
        let ownership_epoch = mutation.ownership_epoch;
        let vote = tokio::task::spawn_blocking(move || store.prepare(mutation))
            .await
            .map_err(|_| EffectJournalError::Unavailable)??;
        Ok(EffectReplicaReply {
            replica: self.claimed(node),
            effect_id,
            ownership_epoch,
            result: vote,
        })
    }

    async fn commit(
        &self,
        node: &StorageNodeId,
        evidence: EffectCommitEvidence,
    ) -> Result<EffectReplicaReply<EffectMutation>, EffectJournalError> {
        if self.commit_down.lock().unwrap().contains(node) {
            return Err(EffectJournalError::Unavailable);
        }
        let store = self
            .stores
            .get(node)
            .cloned()
            .ok_or(EffectJournalError::InvalidAssignment)?;
        let result = tokio::task::spawn_blocking(move || store.commit_with_quorum(evidence))
            .await
            .map_err(|_| EffectJournalError::Unavailable)??;
        Ok(EffectReplicaReply {
            replica: self.claimed(node),
            effect_id: result.effect_id,
            ownership_epoch: result.ownership_epoch,
            result,
        })
    }

    async fn committed(
        &self,
        node: &StorageNodeId,
        effect_id: Uuid,
        ownership_epoch: u64,
    ) -> Result<EffectReplicaReply<Option<EffectMutation>>, EffectJournalError> {
        if self.read_down.lock().unwrap().contains(node) {
            return Err(EffectJournalError::Unavailable);
        }
        let store = self
            .stores
            .get(node)
            .cloned()
            .ok_or(EffectJournalError::InvalidAssignment)?;
        let result = tokio::task::spawn_blocking(move || store.local_committed(effect_id))
            .await
            .map_err(|_| EffectJournalError::Unavailable)??;
        Ok(EffectReplicaReply {
            replica: self.claimed(node),
            effect_id,
            ownership_epoch,
            result,
        })
    }

    async fn inspect(
        &self,
        node: &StorageNodeId,
        effect_id: Uuid,
        ownership_epoch: u64,
    ) -> Result<EffectReplicaReply<EffectJournalInspection>, EffectJournalError> {
        let store = self
            .stores
            .get(node)
            .cloned()
            .ok_or(EffectJournalError::InvalidAssignment)?;
        let result = tokio::task::spawn_blocking(move || store.local_state(effect_id))
            .await
            .map_err(|_| EffectJournalError::Unavailable)??;
        Ok(EffectReplicaReply {
            replica: node.clone(),
            effect_id,
            ownership_epoch,
            result,
        })
    }

    async fn adopt(
        &self,
        node: &StorageNodeId,
        effect_id: Uuid,
        ownership_epoch: u64,
        committed: Option<EffectMutation>,
    ) -> Result<EffectReplicaReply<Option<EffectMutation>>, EffectJournalError> {
        let store = self
            .stores
            .get(node)
            .cloned()
            .ok_or(EffectJournalError::InvalidAssignment)?;
        let result = tokio::task::spawn_blocking(move || {
            store.adopt_recovered(effect_id, ownership_epoch, committed)
        })
        .await
        .map_err(|_| EffectJournalError::Unavailable)??;
        Ok(EffectReplicaReply {
            replica: node.clone(),
            effect_id,
            ownership_epoch,
            result,
        })
    }
}

fn effect_coordinator(
    transport: Arc<TestEffectTransport>,
    nodes: &[StorageNodeId; 3],
    effect_id: Uuid,
) -> EffectCoordinator {
    EffectCoordinator::new(
        EffectJournalAssignment {
            effect_id,
            owner: nodes[0].clone(),
            replicas: nodes.to_vec(),
            ownership_epoch: 1,
        },
        transport,
    )
}

#[tokio::test]
async fn effect_declare_commits_across_a_three_replica_quorum() {
    let directories = [
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
    ];
    let nodes = nodes();
    let transport = Arc::new(TestEffectTransport::new(&directories, &nodes));
    let mutation = declare(Uuid::new_v4(), 1, 1);
    let coordinator = effect_coordinator(transport, &nodes, mutation.effect_id);
    let committed = coordinator.apply(mutation.clone()).await.unwrap();
    assert_eq!(committed, mutation);
    assert_eq!(coordinator.read_committed().await.unwrap(), Some(mutation));
}

#[tokio::test]
async fn declare_survives_one_unavailable_replica_but_needs_the_owner() {
    let directories = [
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
    ];
    let nodes = nodes();
    let transport = Arc::new(TestEffectTransport::new(&directories, &nodes));
    let mutation = declare(Uuid::new_v4(), 1, 1);
    transport
        .prepare_down
        .lock()
        .unwrap()
        .insert(nodes[2].clone());
    let coordinator = effect_coordinator(transport.clone(), &nodes, mutation.effect_id);
    coordinator.apply(mutation.clone()).await.unwrap();

    // The owner itself going down denies quorum even when members are up.
    transport.prepare_down.lock().unwrap().clear();
    transport
        .prepare_down
        .lock()
        .unwrap()
        .insert(nodes[0].clone());
    let fresh = declare(Uuid::new_v4(), 1, 1);
    let coordinator_b = effect_coordinator(transport.clone(), &nodes, fresh.effect_id);
    assert!(matches!(
        coordinator_b.apply(fresh).await,
        Err(EffectJournalError::NoQuorum) | Err(EffectJournalError::AmbiguousCommit)
    ));
}

#[tokio::test]
async fn reconcile_retry_completes_a_partially_committed_declare() {
    let directories = [
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
    ];
    let nodes = nodes();
    let transport = Arc::new(TestEffectTransport::new(&directories, &nodes));
    let mutation = declare(Uuid::new_v4(), 1, 1);
    // The owner prepared but never commits, while a member does: the outcome
    // is ambiguous to the caller until reconcile probes the same identity.
    transport
        .commit_down
        .lock()
        .unwrap()
        .insert(nodes[0].clone());
    let coordinator = effect_coordinator(transport.clone(), &nodes, mutation.effect_id);
    assert!(matches!(
        coordinator.apply(mutation.clone()).await,
        Err(EffectJournalError::AmbiguousCommit) | Err(EffectJournalError::NoQuorum)
    ));
    transport.commit_down.lock().unwrap().clear();
    let reconciled = coordinator.reconcile_retry(mutation.clone()).await.unwrap();
    assert_eq!(reconciled, mutation);
    assert_eq!(coordinator.read_committed().await.unwrap(), Some(mutation));
}

#[tokio::test]
async fn wrong_replica_identities_and_conflicting_reads_are_refused() {
    let directories = [
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
    ];
    let nodes = nodes();
    let transport = Arc::new(TestEffectTransport::new(&directories, &nodes));
    let mutation = declare(Uuid::new_v4(), 1, 1);
    *transport.wrong_node.lock().unwrap() = Some(nodes[1].clone());
    let coordinator = effect_coordinator(transport.clone(), &nodes, mutation.effect_id);
    assert!(matches!(
        coordinator.apply(mutation.clone()).await,
        Err(EffectJournalError::InvalidAssignment)
            | Err(EffectJournalError::AmbiguousCommit)
            | Err(EffectJournalError::NoQuorum)
    ));
}

#[tokio::test]
async fn applied_transition_commits_as_a_second_sequenced_mutation() {
    let directories = [
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
    ];
    let nodes = nodes();
    let transport = Arc::new(TestEffectTransport::new(&directories, &nodes));
    let mutation = declare(Uuid::new_v4(), 1, 1);
    let coordinator = effect_coordinator(transport, &nodes, mutation.effect_id);
    coordinator.apply(mutation.clone()).await.unwrap();

    let applied = EffectMutation {
        effect_id: mutation.effect_id,
        ownership_epoch: 1,
        sequence: 2,
        request_id: Uuid::new_v4(),
        transition: EffectTransition::Applied {
            declares: mutation.request_id,
        },
    };
    coordinator.apply(applied.clone()).await.unwrap();
    assert_eq!(coordinator.read_committed().await.unwrap(), Some(applied));
}
