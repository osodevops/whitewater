use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};

use finnstream::active_range::{RangeId, ReplicaSet, StorageNodeId};
use finnstream::effect::{
    effect_apply_request_id, EffectApply, EffectCommitEvidence, EffectConsume, EffectCoordinator,
    EffectJournalError, EffectJournalInspection, EffectJournalTransport, EffectMutation,
    EffectOutput, EffectPrepareVote, EffectReplicaReply, EffectTransition,
    FjallEffectJournalReplica,
};
use finnstream::reader::SubscriptionProgressAssignment;
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
        writer_epoch: 1,
        sequence,
    }
}

fn scope() -> Uuid {
    Uuid::from_u128(777)
}

fn declare(effect_id: Uuid, sequence: u64, epoch: u64) -> EffectMutation {
    EffectMutation {
        effect_id,
        subscription_id: scope(),
        ownership_epoch: epoch,
        sequence,
        request_id: Uuid::new_v4(),
        transition: EffectTransition::Declare {
            consume: Some(EffectConsume {
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
            store.adopt_recovered(effect_id, scope(), ownership_epoch, committed)
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
) -> EffectCoordinator {
    EffectCoordinator::new(
        SubscriptionProgressAssignment::try_new(
            scope(),
            nodes[0].clone(),
            ReplicaSet::try_new(nodes.clone()).unwrap(),
            1,
        )
        .unwrap(),
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
    let coordinator = effect_coordinator(transport, &nodes);
    let committed = coordinator.apply(mutation.clone()).await.unwrap();
    assert_eq!(committed, mutation);
    assert_eq!(
        coordinator
            .read_committed(mutation.effect_id)
            .await
            .unwrap(),
        Some(mutation)
    );
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
    let coordinator = effect_coordinator(transport.clone(), &nodes);
    coordinator.apply(mutation.clone()).await.unwrap();

    // The owner itself going down denies quorum even when members are up.
    transport.prepare_down.lock().unwrap().clear();
    transport
        .prepare_down
        .lock()
        .unwrap()
        .insert(nodes[0].clone());
    let fresh = declare(Uuid::new_v4(), 1, 1);
    let coordinator_b = effect_coordinator(transport.clone(), &nodes);
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
    let coordinator = effect_coordinator(transport.clone(), &nodes);
    assert!(matches!(
        coordinator.apply(mutation.clone()).await,
        Err(EffectJournalError::AmbiguousCommit) | Err(EffectJournalError::NoQuorum)
    ));
    transport.commit_down.lock().unwrap().clear();
    let reconciled = coordinator.reconcile_retry(mutation.clone()).await.unwrap();
    assert_eq!(reconciled, mutation);
    assert_eq!(
        coordinator
            .read_committed(mutation.effect_id)
            .await
            .unwrap(),
        Some(mutation)
    );
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
    let coordinator = effect_coordinator(transport.clone(), &nodes);
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
    let coordinator = effect_coordinator(transport, &nodes);
    coordinator.apply(mutation.clone()).await.unwrap();

    let applied = EffectMutation {
        effect_id: mutation.effect_id,
        subscription_id: scope(),
        ownership_epoch: 1,
        sequence: 2,
        request_id: Uuid::new_v4(),
        transition: EffectTransition::Applied {
            declares: mutation.request_id,
        },
    };
    coordinator.apply(applied.clone()).await.unwrap();
    assert_eq!(
        coordinator
            .read_committed(mutation.effect_id)
            .await
            .unwrap(),
        Some(applied)
    );
}

#[derive(Default)]
struct RecordingApply {
    appended: Mutex<Vec<EffectOutput>>,
    frontier_requests: Mutex<Vec<Uuid>>,
    fail_once: Mutex<BTreeSet<Uuid>>,
}

#[async_trait::async_trait]
impl EffectApply for RecordingApply {
    async fn append_output(&self, output: &EffectOutput) -> Result<(), EffectJournalError> {
        if self
            .fail_once
            .lock()
            .unwrap()
            .remove(&output.writer_session_id)
        {
            return Err(EffectJournalError::Unavailable);
        }
        let mut appended = self.appended.lock().unwrap();
        if !appended
            .iter()
            .any(|seen| seen.writer_session_id == output.writer_session_id)
        {
            appended.push(output.clone());
        }
        Ok(())
    }

    async fn commit_frontier(
        &self,
        _consume: &EffectConsume,
        request_id: Uuid,
    ) -> Result<(), EffectJournalError> {
        self.frontier_requests.lock().unwrap().push(request_id);
        Ok(())
    }
}

#[tokio::test]
async fn apply_committed_replicates_the_applied_marker_on_the_journal_quorum() {
    let directories = [
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
    ];
    let nodes = nodes();
    let transport = Arc::new(TestEffectTransport::new(&directories, &nodes));
    let mutation = declare(Uuid::new_v4(), 1, 1);
    let coordinator = effect_coordinator(transport.clone(), &nodes);
    coordinator.apply(mutation.clone()).await.unwrap();

    let sink = RecordingApply::default();
    let applied = coordinator
        .apply_committed(mutation.effect_id, &sink)
        .await
        .unwrap();
    assert_eq!(
        applied.transition,
        EffectTransition::Applied {
            declares: mutation.request_id
        }
    );
    // The Applied marker is quorum-visible through the normal committed read.
    assert_eq!(
        coordinator
            .read_committed(mutation.effect_id)
            .await
            .unwrap(),
        Some(applied)
    );
    assert_eq!(sink.appended.lock().unwrap().len(), 1);
    assert_eq!(
        sink.frontier_requests.lock().unwrap().as_slice(),
        &[effect_apply_request_id(mutation.request_id, b"frontier")]
    );
}

#[tokio::test]
async fn apply_committed_replays_deterministically_after_sink_failure() {
    let directories = [
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
    ];
    let nodes = nodes();
    let transport = Arc::new(TestEffectTransport::new(&directories, &nodes));
    let mutation = declare(Uuid::new_v4(), 1, 1);
    let coordinator = effect_coordinator(transport.clone(), &nodes);
    coordinator.apply(mutation.clone()).await.unwrap();

    let sink = RecordingApply::default();
    let EffectTransition::Declare { outputs, .. } = &mutation.transition else {
        panic!("expected declare")
    };
    sink.fail_once
        .lock()
        .unwrap()
        .insert(outputs[0].writer_session_id);
    assert!(matches!(
        coordinator.apply_committed(mutation.effect_id, &sink).await,
        Err(EffectJournalError::Unavailable)
    ));
    assert!(sink.frontier_requests.lock().unwrap().is_empty());

    let applied = coordinator
        .apply_committed(mutation.effect_id, &sink)
        .await
        .unwrap();
    assert!(matches!(
        applied.transition,
        EffectTransition::Applied { .. }
    ));
    // The retried apply appended each declared output exactly once because
    // the sink dedupes on deterministic writer identity.
    assert_eq!(sink.appended.lock().unwrap().len(), outputs.len());
}

#[tokio::test]
async fn apply_committed_recovers_an_ambiguous_terminal_commit() {
    let directories = [
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
    ];
    let nodes = nodes();
    let transport = Arc::new(TestEffectTransport::new(&directories, &nodes));
    let mutation = declare(Uuid::new_v4(), 1, 1);
    let coordinator = effect_coordinator(transport.clone(), &nodes);
    coordinator.apply(mutation.clone()).await.unwrap();

    // The Applied transition commits on the owner but the member commit call
    // fails: the outcome is ambiguous and reconcile_retry must settle it.
    transport
        .commit_down
        .lock()
        .unwrap()
        .insert(nodes[2].clone());
    let sink = RecordingApply::default();
    let applied = coordinator
        .apply_committed(mutation.effect_id, &sink)
        .await
        .unwrap();
    assert!(matches!(
        applied.transition,
        EffectTransition::Applied { .. }
    ));
    assert_eq!(
        coordinator
            .read_committed(mutation.effect_id)
            .await
            .unwrap(),
        Some(applied)
    );
}
