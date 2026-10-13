use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};

use tempfile::TempDir;
use uuid::Uuid;
use whitewater::active_range::{RangeId, RangePosition, ReplicaSet, StorageNodeId};
use whitewater::effect::{
    effect_apply_request_id, EffectApply, EffectCommitEvidence, EffectConsume, EffectCoordinator,
    EffectJournalError, EffectJournalInspection, EffectJournalTransport, EffectMutation,
    EffectOutput, EffectOutputReceipt, EffectPrepareVote, EffectReleaseOutcome,
    EffectReleaseTarget, EffectReplicaReply, EffectTransition, FjallEffectJournalReplica,
};
use whitewater::reader::SubscriptionProgressAssignment;

fn nodes() -> [StorageNodeId; 3] {
    ["effect-a", "effect-b", "effect-c"].map(|name| StorageNodeId::try_new(name).unwrap())
}

fn output(sequence: u64) -> EffectOutput {
    EffectOutput {
        feed_id: Uuid::new_v4(),
        key_base64: "a2V5".to_owned(),
        payload_base64: "cGF5bG9hZA==".to_owned(),
        metadata_base64: BTreeMap::new(),
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
            state_mutations: Vec::new(),
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
            outputs: vec![],
            state_mutations: Vec::new(),
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
    frontier_request_log: Mutex<Vec<Uuid>>,
    releases: Mutex<Vec<EffectReleaseTarget>>,
    state_mutated: Mutex<Vec<whitewater::effect::EffectStateMutation>>,
    call_log: Mutex<Vec<&'static str>>,
    resolved: Mutex<Vec<whitewater::effect::EffectOccupant>>,
    fail_once: Mutex<BTreeSet<Uuid>>,
    fail_frontier: Mutex<u8>,
    fail_releases: Mutex<u8>,
    fail_state_mutations: Mutex<u8>,
    transient_appends: Mutex<u8>,
    fail_resolves: Mutex<u8>,
    occupied_appends: Mutex<u8>,
    occupied_releases: Mutex<u8>,
    not_pending_releases: Mutex<u8>,
    permanent_appends: Mutex<BTreeSet<Uuid>>,
    permanent_frontier: Mutex<bool>,
    permanent_releases: Mutex<u8>,
    discards: Mutex<Vec<(Uuid, Uuid, Uuid)>>,
    fail_discards: Mutex<u8>,
    occupied_discards: Mutex<u8>,
    not_pending_discards: Mutex<u8>,
}

#[async_trait::async_trait]
impl EffectApply for RecordingApply {
    async fn append_output(
        &self,
        output: &EffectOutput,
        _effect_id: Uuid,
    ) -> Result<EffectOutputReceipt, EffectJournalError> {
        {
            let mut remaining = self.transient_appends.lock().unwrap();
            if *remaining > 0 {
                *remaining -= 1;
                return Err(EffectJournalError::Unavailable);
            }
        }
        if self
            .fail_once
            .lock()
            .unwrap()
            .remove(&output.writer_session_id)
        {
            return Err(EffectJournalError::Unavailable);
        }
        if self
            .permanent_appends
            .lock()
            .unwrap()
            .contains(&output.writer_session_id)
        {
            return Err(EffectJournalError::Conflict);
        }
        let mut appended = self.appended.lock().unwrap();
        if !appended
            .iter()
            .any(|seen| seen.writer_session_id == output.writer_session_id)
        {
            appended.push(output.clone());
        }
        {
            let mut remaining = self.occupied_appends.lock().unwrap();
            if *remaining > 0 {
                *remaining -= 1;
                return Err(EffectJournalError::PendingOccupied {
                    effect_id: Uuid::from_u128(4242),
                    subscription_id: Uuid::from_u128(4343),
                });
            }
        }
        Ok(EffectOutputReceipt {
            feed_id: output.feed_id,
            range_id: RangeId::from_uuid(Uuid::from_u128(9)),
            position: RangePosition::new(1),
        })
    }

    async fn release_output(
        &self,
        target: &EffectReleaseTarget,
    ) -> Result<EffectReleaseOutcome, EffectJournalError> {
        self.releases.lock().unwrap().push(target.clone());
        self.call_log.lock().unwrap().push("release");
        {
            let mut remaining = self.fail_releases.lock().unwrap();
            if *remaining > 0 {
                *remaining -= 1;
                return Err(EffectJournalError::Unavailable);
            }
        }
        {
            let mut remaining = self.occupied_releases.lock().unwrap();
            if *remaining > 0 {
                *remaining -= 1;
                return Ok(EffectReleaseOutcome::Occupied {
                    occupant: whitewater::effect::EffectOccupant {
                        effect_id: Uuid::from_u128(5252),
                        subscription_id: Uuid::from_u128(5353),
                    },
                });
            }
        }
        {
            let mut remaining = self.not_pending_releases.lock().unwrap();
            if *remaining > 0 {
                *remaining -= 1;
                return Ok(EffectReleaseOutcome::NotPending { committed: 0 });
            }
        }
        {
            let mut remaining = self.permanent_releases.lock().unwrap();
            if *remaining > 0 {
                *remaining -= 1;
                return Err(EffectJournalError::Conflict);
            }
        }
        Ok(EffectReleaseOutcome::Released { committed: 1 })
    }

    async fn mutate_state(
        &self,
        mutation: &whitewater::effect::EffectStateMutation,
    ) -> Result<(), EffectJournalError> {
        {
            let mut remaining = self.fail_state_mutations.lock().unwrap();
            if *remaining > 0 {
                *remaining -= 1;
                return Err(EffectJournalError::Unavailable);
            }
        }
        self.call_log.lock().unwrap().push("state");
        let mut mutated = self.state_mutated.lock().unwrap();
        if !mutated
            .iter()
            .any(|seen| seen.mutation_id == mutation.mutation_id)
        {
            mutated.push(mutation.clone());
        }
        Ok(())
    }

    async fn resolve_pending(
        &self,
        occupant: whitewater::effect::EffectOccupant,
    ) -> Result<(), EffectJournalError> {
        {
            let mut remaining = self.fail_resolves.lock().unwrap();
            if *remaining > 0 {
                *remaining -= 1;
                return Err(EffectJournalError::Unavailable);
            }
        }
        self.resolved.lock().unwrap().push(occupant);
        Ok(())
    }

    async fn commit_frontier(
        &self,
        _consume: &EffectConsume,
        request_id: Uuid,
    ) -> Result<(), EffectJournalError> {
        if *self.permanent_frontier.lock().unwrap() {
            return Err(EffectJournalError::Conflict);
        }
        {
            let mut remaining = self.fail_frontier.lock().unwrap();
            if *remaining > 0 {
                *remaining -= 1;
                return Err(EffectJournalError::Unavailable);
            }
        }
        self.frontier_request_log.lock().unwrap().push(request_id);
        Ok(())
    }

    async fn discard_output(
        &self,
        output: &EffectOutput,
        effect_id: Uuid,
        subscription_id: Uuid,
    ) -> Result<whitewater::effect::EffectDiscardOutcome, EffectJournalError> {
        self.discards
            .lock()
            .unwrap()
            .push((output.feed_id, effect_id, subscription_id));
        {
            let mut remaining = self.fail_discards.lock().unwrap();
            if *remaining > 0 {
                *remaining -= 1;
                return Err(EffectJournalError::Unavailable);
            }
        }
        {
            let mut remaining = self.occupied_discards.lock().unwrap();
            if *remaining > 0 {
                *remaining -= 1;
                return Ok(whitewater::effect::EffectDiscardOutcome::Occupied {
                    occupant: whitewater::effect::EffectOccupant {
                        effect_id: Uuid::from_u128(6262),
                        subscription_id: Uuid::from_u128(6363),
                    },
                });
            }
        }
        {
            let mut remaining = self.not_pending_discards.lock().unwrap();
            if *remaining > 0 {
                *remaining -= 1;
                return Ok(whitewater::effect::EffectDiscardOutcome::NotPending);
            }
        }
        Ok(whitewater::effect::EffectDiscardOutcome::Discarded { dropped_frames: 1 })
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
    let EffectTransition::Declare { outputs, .. } = &mutation.transition else {
        panic!("expected declare")
    };
    assert_eq!(
        applied.transition,
        EffectTransition::Applied {
            declares: mutation.request_id,
            outputs: outputs.clone(),
            state_mutations: Vec::new(),
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
        sink.frontier_request_log.lock().unwrap().as_slice(),
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
    // Fail past the bounded per-step retry: the apply reports a retryable
    // failure before the frontier commits, so replay re-runs everything
    // idempotently once the dependency recovers.
    *sink.transient_appends.lock().unwrap() = 3;
    assert!(matches!(
        coordinator.apply_committed(mutation.effect_id, &sink).await,
        Err(EffectJournalError::Unavailable)
    ));
    assert!(sink.frontier_request_log.lock().unwrap().is_empty());

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

#[tokio::test]
async fn apply_committed_sweeps_an_occupied_append_through_resolve_pending() {
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
    // The first output append finds another effect's tail holding the
    // Range; resolve_pending settles it and the retry proceeds.
    *sink.occupied_appends.lock().unwrap() = 1;
    let applied = coordinator
        .apply_committed(mutation.effect_id, &sink)
        .await
        .unwrap();
    assert!(matches!(
        applied.transition,
        EffectTransition::Applied { .. }
    ));
    assert_eq!(
        sink.resolved.lock().unwrap().as_slice(),
        &[whitewater::effect::EffectOccupant {
            effect_id: Uuid::from_u128(4242),
            subscription_id: Uuid::from_u128(4343),
        }]
    );
    assert_eq!(sink.appended.lock().unwrap().len(), 1);
    assert_eq!(sink.releases.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn apply_committed_sweeps_an_occupied_release_through_resolve_pending() {
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
    *sink.occupied_releases.lock().unwrap() = 1;
    let applied = coordinator
        .apply_committed(mutation.effect_id, &sink)
        .await
        .unwrap();
    assert!(matches!(
        applied.transition,
        EffectTransition::Applied { .. }
    ));
    assert_eq!(
        sink.resolved.lock().unwrap().as_slice(),
        &[whitewater::effect::EffectOccupant {
            effect_id: Uuid::from_u128(5252),
            subscription_id: Uuid::from_u128(5353),
        }]
    );
    // Occupied once, released on the retry.
    assert_eq!(sink.releases.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn apply_committed_reappends_outputs_when_the_tail_was_truncated() {
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
    // The first release finds no marker and the receipts sit above the
    // committed boundary: owner loss truncated the held tail. The apply
    // re-appends under a fresh marker and releases again.
    *sink.not_pending_releases.lock().unwrap() = 1;
    let applied = coordinator
        .apply_committed(mutation.effect_id, &sink)
        .await
        .unwrap();
    assert!(matches!(
        applied.transition,
        EffectTransition::Applied { .. }
    ));
    assert_eq!(sink.releases.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn apply_committed_fails_closed_when_occupant_sweeps_exhaust() {
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
    // Every release lands behind a new occupant; bounded sweeps refuse to
    // loop forever.
    *sink.occupied_releases.lock().unwrap() = 10;
    assert!(matches!(
        coordinator.apply_committed(mutation.effect_id, &sink).await,
        Err(EffectJournalError::Unavailable)
    ));
    assert_eq!(sink.resolved.lock().unwrap().len(), 4);
}

#[tokio::test]
async fn apply_committed_converges_when_the_frontier_commit_crashes() {
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
    // Crash after outputs replicated but before the frontier commits: no
    // terminal marker was written, so replay re-runs everything idempotently.
    *sink.fail_frontier.lock().unwrap() = 3;
    assert!(matches!(
        coordinator.apply_committed(mutation.effect_id, &sink).await,
        Err(EffectJournalError::Unavailable)
    ));
    assert!(matches!(
        coordinator
            .read_committed(mutation.effect_id)
            .await
            .unwrap()
            .unwrap()
            .transition,
        EffectTransition::Declare { .. }
    ));

    let applied = coordinator
        .apply_committed(mutation.effect_id, &sink)
        .await
        .unwrap();
    assert!(matches!(
        applied.transition,
        EffectTransition::Applied { .. }
    ));
    // Outputs deduped on writer identity: still one logical append.
    assert_eq!(sink.appended.lock().unwrap().len(), 1);
    assert_eq!(sink.frontier_request_log.lock().unwrap().len(), 1);
    assert_eq!(sink.releases.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn apply_committed_rereleases_a_tail_the_terminal_marker_left_pending() {
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
    // The Applied marker commits but the release fails: replay finds the
    // terminal row, re-runs ensure idempotently, and releases the tail.
    *sink.fail_releases.lock().unwrap() = 3;
    assert!(matches!(
        coordinator.apply_committed(mutation.effect_id, &sink).await,
        Err(EffectJournalError::Unavailable)
    ));
    assert!(matches!(
        coordinator
            .read_committed(mutation.effect_id)
            .await
            .unwrap()
            .unwrap()
            .transition,
        EffectTransition::Applied { .. }
    ));

    let applied = coordinator
        .apply_committed(mutation.effect_id, &sink)
        .await
        .unwrap();
    assert!(matches!(
        applied.transition,
        EffectTransition::Applied { .. }
    ));
    // The replay appended nothing new and completed the release; the
    // frontier committed exactly once across both attempts because the
    // Applied row no longer carries consume evidence.
    assert_eq!(sink.appended.lock().unwrap().len(), 1);
    assert_eq!(sink.frontier_request_log.lock().unwrap().len(), 1);
    assert_eq!(sink.releases.lock().unwrap().len(), 4);
}

fn state_mutation(index: usize) -> whitewater::effect::EffectStateMutation {
    whitewater::effect::EffectStateMutation {
        store_id: Uuid::from_u128(700 + index as u128),
        mutation_id: whitewater::effect::effect_state_mutation_id(Uuid::from_u128(1), index),
        op: whitewater::state_store::StateStoreMutationOp::PutRow {
            key: format!("row-{index}").into_bytes(),
            payload: b"payload".to_vec(),
            named: Vec::new(),
        },
    }
}

fn declare_with_state(
    effect_id: Uuid,
    sequence: u64,
    epoch: u64,
    mutations: Vec<whitewater::effect::EffectStateMutation>,
) -> EffectMutation {
    let mut mutation = declare(effect_id, sequence, epoch);
    let EffectTransition::Declare {
        state_mutations, ..
    } = &mut mutation.transition
    else {
        panic!("expected declare")
    };
    *state_mutations = mutations;
    mutation
}

#[tokio::test]
async fn apply_committed_applies_state_mutations_after_output_release() {
    let directories = [
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
    ];
    let nodes = nodes();
    let transport = Arc::new(TestEffectTransport::new(&directories, &nodes));
    let mutation = declare_with_state(
        Uuid::new_v4(),
        1,
        1,
        vec![state_mutation(0), state_mutation(1)],
    );
    let coordinator = effect_coordinator(transport, &nodes);
    coordinator.apply(mutation.clone()).await.unwrap();

    let sink = RecordingApply::default();
    let applied = coordinator
        .apply_committed(mutation.effect_id, &sink)
        .await
        .unwrap();

    // The Applied row restates the declared mutations so a terminal row
    // alone still drives them after a crash.
    let EffectTransition::Applied {
        state_mutations, ..
    } = &applied.transition
    else {
        panic!("expected applied")
    };
    assert_eq!(state_mutations.len(), 2);

    // Every declared mutation committed exactly once, strictly after the
    // pending output tail released.
    let mutated = sink.state_mutated.lock().unwrap();
    assert_eq!(mutated.len(), 2);
    assert_eq!(mutated[0].mutation_id, state_mutation(0).mutation_id);
    assert_eq!(mutated[1].mutation_id, state_mutation(1).mutation_id);
    let log = sink.call_log.lock().unwrap();
    let first_state = log.iter().position(|call| *call == "state").unwrap();
    assert!(log[..first_state].iter().all(|call| *call == "release"));
    assert_eq!(log.len(), 3);
}

#[tokio::test]
async fn apply_committed_replays_state_mutations_after_a_mutation_failure() {
    let directories = [
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
    ];
    let nodes = nodes();
    let transport = Arc::new(TestEffectTransport::new(&directories, &nodes));
    let mutation = declare_with_state(
        Uuid::new_v4(),
        1,
        1,
        vec![state_mutation(0), state_mutation(1)],
    );
    let coordinator = effect_coordinator(transport, &nodes);
    coordinator.apply(mutation.clone()).await.unwrap();

    // The first attempt applies the marker and the first mutation, then
    // fails partway through the release phase.
    let sink = RecordingApply::default();
    *sink.fail_state_mutations.lock().unwrap() = 3;
    assert!(matches!(
        coordinator.apply_committed(mutation.effect_id, &sink).await,
        Err(EffectJournalError::Unavailable)
    ));

    // The retried apply converges: each mutation commits exactly once
    // under its declared dedupe identity.
    coordinator
        .apply_committed(mutation.effect_id, &sink)
        .await
        .unwrap();
    let mutated = sink.state_mutated.lock().unwrap();
    assert_eq!(mutated.len(), 2);
    assert_eq!(
        mutated
            .iter()
            .map(|mutation| mutation.mutation_id)
            .collect::<BTreeSet<_>>()
            .len(),
        2
    );
}

#[tokio::test]
async fn terminal_applied_row_replays_state_mutations_after_a_marker_crash() {
    let directories = [
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
    ];
    let nodes = nodes();
    let transport = Arc::new(TestEffectTransport::new(&directories, &nodes));
    let mutation = declare_with_state(Uuid::new_v4(), 1, 1, vec![state_mutation(0)]);
    let coordinator = effect_coordinator(transport, &nodes);
    coordinator.apply(mutation.clone()).await.unwrap();

    // The marker committed but the apply crashed before the mutation
    // reached the store journal.
    let applied = EffectMutation {
        effect_id: mutation.effect_id,
        subscription_id: scope(),
        ownership_epoch: 1,
        sequence: 2,
        request_id: Uuid::new_v4(),
        transition: EffectTransition::Applied {
            declares: mutation.request_id,
            outputs: Vec::new(),
            state_mutations: vec![state_mutation(0)],
        },
    };
    coordinator.apply(applied).await.unwrap();

    // Replaying the terminal row alone still commits the declared
    // mutation under its dedupe identity.
    let sink = RecordingApply::default();
    coordinator
        .apply_committed(mutation.effect_id, &sink)
        .await
        .unwrap();
    assert_eq!(sink.appended.lock().unwrap().len(), 0);
    assert_eq!(sink.state_mutated.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn oversized_state_mutations_are_refused_before_durability() {
    let directories = [
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
    ];
    let nodes = nodes();
    let transport = Arc::new(TestEffectTransport::new(&directories, &nodes));
    let coordinator = effect_coordinator(transport, &nodes);

    // More declared mutations than the journal bound refuses at prepare.
    let mutation = declare_with_state(
        Uuid::new_v4(),
        1,
        1,
        (0..=whitewater::effect::EFFECT_MAX_STATE_MUTATIONS)
            .map(state_mutation)
            .collect(),
    );
    assert!(matches!(
        coordinator.apply(mutation).await,
        Err(EffectJournalError::TooLarge)
    ));

    // A mutation whose serialized op exceeds the field bound is refused.
    let oversized = whitewater::effect::EffectStateMutation {
        store_id: Uuid::new_v4(),
        mutation_id: Uuid::new_v4(),
        op: whitewater::state_store::StateStoreMutationOp::PutRow {
            key: b"row".to_vec(),
            payload: vec![b'x'; whitewater::effect::EFFECT_MAX_OUTPUT_FIELD],
            named: Vec::new(),
        },
    };
    let mutation = declare_with_state(Uuid::new_v4(), 1, 1, vec![oversized]);
    assert!(matches!(
        coordinator.apply(mutation).await,
        Err(EffectJournalError::TooLarge)
    ));
}

#[tokio::test]
async fn apply_committed_settles_transient_failures_by_bounded_retry() {
    let directories = [
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
    ];
    let nodes = nodes();
    let transport = Arc::new(TestEffectTransport::new(&directories, &nodes));
    let mutation = declare_with_state(Uuid::new_v4(), 1, 1, vec![state_mutation(0)]);
    let coordinator = effect_coordinator(transport, &nodes);
    coordinator.apply(mutation.clone()).await.unwrap();

    // Every phase absorbs transient ambiguity below the retry bound by
    // replaying the identical deterministic identity.
    let sink = RecordingApply::default();
    *sink.transient_appends.lock().unwrap() = 2;
    *sink.fail_frontier.lock().unwrap() = 2;
    *sink.fail_releases.lock().unwrap() = 2;
    *sink.fail_state_mutations.lock().unwrap() = 2;
    coordinator
        .apply_committed(mutation.effect_id, &sink)
        .await
        .unwrap();
    assert_eq!(sink.appended.lock().unwrap().len(), 1);
    assert_eq!(sink.frontier_request_log.lock().unwrap().len(), 1);
    assert_eq!(sink.state_mutated.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn apply_committed_fails_closed_after_bounded_transient_retries() {
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

    // A dependency that stays ambiguous past the bounded retry count fails
    // the apply as retryable Unavailable rather than wedging or inventing
    // an outcome.
    let sink = RecordingApply::default();
    *sink.transient_appends.lock().unwrap() = 8;
    assert!(matches!(
        coordinator.apply_committed(mutation.effect_id, &sink).await,
        Err(EffectJournalError::Unavailable)
    ));
    // The bounded apply attempted the append exactly three times, then a
    // later re-drive still converges once the dependency recovers.
    *sink.transient_appends.lock().unwrap() = 0;
    coordinator
        .apply_committed(mutation.effect_id, &sink)
        .await
        .unwrap();
}

#[tokio::test]
async fn apply_committed_retries_a_failing_occupant_sweep() {
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

    // A transient sweep failure retries the occupied append; the second
    // occupancy resolves through the occupant's own apply and the output
    // commits exactly once.
    let sink = RecordingApply::default();
    *sink.occupied_appends.lock().unwrap() = 2;
    *sink.fail_resolves.lock().unwrap() = 1;
    coordinator
        .apply_committed(mutation.effect_id, &sink)
        .await
        .unwrap();
    assert_eq!(sink.resolved.lock().unwrap().len(), 1);
    assert_eq!(sink.appended.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn apply_committed_aborts_a_permanent_output_failure_and_discards_declared_ranges() {
    let directories = [
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
    ];
    let nodes = nodes();
    let transport = Arc::new(TestEffectTransport::new(&directories, &nodes));
    let mut mutation = declare_with_state(Uuid::new_v4(), 1, 1, vec![state_mutation(0)]);
    let second = output(2);
    let EffectTransition::Declare { outputs, .. } = &mut mutation.transition else {
        panic!("expected declare")
    };
    outputs.push(second.clone());
    let coordinator = effect_coordinator(transport, &nodes);
    coordinator.apply(mutation.clone()).await.unwrap();

    let sink = RecordingApply::default();
    sink.permanent_appends
        .lock()
        .unwrap()
        .insert(second.writer_session_id);
    let terminal = coordinator
        .apply_committed(mutation.effect_id, &sink)
        .await
        .unwrap();

    let EffectTransition::Aborted {
        declares,
        reason,
        outputs,
    } = &terminal.transition
    else {
        panic!("expected aborted, got {:?}", terminal.transition)
    };
    assert_eq!(*declares, mutation.request_id);
    assert!(!reason.is_empty() && reason.len() <= 256);
    // The terminal row restates the declared outputs so replay can
    // discard any tail the first attempt left pending.
    assert_eq!(outputs.len(), 2);
    // Both declared outputs' Ranges were told to discard this effect's
    // tail; the frontier never moved and no state mutation ran.
    let discards = sink.discards.lock().unwrap();
    assert_eq!(discards.len(), 2);
    assert!(discards
        .iter()
        .all(|(_, effect, sub)| *effect == mutation.effect_id && *sub == scope()));
    drop(discards);
    assert!(sink.state_mutated.lock().unwrap().is_empty());
    assert!(sink.frontier_request_log.lock().unwrap().is_empty());
    // The first output did append under the pending marker before the
    // second's permanent refusal.
    assert_eq!(sink.appended.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn apply_committed_aborts_before_any_output_append() {
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

    let sink = RecordingApply::default();
    let first = output(1);
    sink.permanent_appends
        .lock()
        .unwrap()
        .insert(first.writer_session_id);
    let EffectTransition::Declare { outputs, .. } = &mutation.transition else {
        panic!("expected declare")
    };
    sink.permanent_appends
        .lock()
        .unwrap()
        .insert(outputs[0].writer_session_id);
    let terminal = coordinator
        .apply_committed(mutation.effect_id, &sink)
        .await
        .unwrap();
    assert!(matches!(
        terminal.transition,
        EffectTransition::Aborted { .. }
    ));
    assert!(sink.appended.lock().unwrap().is_empty());
    assert_eq!(sink.discards.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn apply_committed_aborts_a_permanent_frontier_failure_after_outputs_appended() {
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

    let sink = RecordingApply::default();
    *sink.permanent_frontier.lock().unwrap() = true;
    let terminal = coordinator
        .apply_committed(mutation.effect_id, &sink)
        .await
        .unwrap();
    assert!(matches!(
        terminal.transition,
        EffectTransition::Aborted { .. }
    ));
    // The appended output's tail was discarded and the frontier stayed
    // uncommitted so the input redelivers to a later effect.
    assert_eq!(sink.discards.lock().unwrap().len(), 1);
    assert!(sink.frontier_request_log.lock().unwrap().is_empty());
}

#[tokio::test]
async fn apply_committed_aborted_replay_is_idempotent_and_never_reapplies() {
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

    let sink = RecordingApply::default();
    let EffectTransition::Declare { outputs, .. } = &mutation.transition else {
        panic!("expected declare")
    };
    let session = outputs[0].writer_session_id;
    sink.permanent_appends.lock().unwrap().insert(session);
    coordinator
        .apply_committed(mutation.effect_id, &sink)
        .await
        .unwrap();
    assert_eq!(sink.discards.lock().unwrap().len(), 1);

    // Replaying the terminal row drives the discard again (idempotent
    // cleanup) but never re-appends outputs or moves the frontier.
    sink.permanent_appends.lock().unwrap().clear();
    let replayed = coordinator
        .apply_committed(mutation.effect_id, &sink)
        .await
        .unwrap();
    assert!(matches!(
        replayed.transition,
        EffectTransition::Aborted { .. }
    ));
    assert!(sink.appended.lock().unwrap().is_empty());
    assert!(sink.frontier_request_log.lock().unwrap().is_empty());
    assert_eq!(sink.discards.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn apply_committed_abort_sweeps_a_foreign_discard_occupant() {
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

    let sink = RecordingApply::default();
    let EffectTransition::Declare { outputs, .. } = &mutation.transition else {
        panic!("expected declare")
    };
    sink.permanent_appends
        .lock()
        .unwrap()
        .insert(outputs[0].writer_session_id);
    *sink.occupied_discards.lock().unwrap() = 1;
    let terminal = coordinator
        .apply_committed(mutation.effect_id, &sink)
        .await
        .unwrap();
    assert!(matches!(
        terminal.transition,
        EffectTransition::Aborted { .. }
    ));
    assert_eq!(
        sink.resolved.lock().unwrap().as_slice(),
        &[whitewater::effect::EffectOccupant {
            effect_id: Uuid::from_u128(6262),
            subscription_id: Uuid::from_u128(6363),
        }]
    );
}

#[tokio::test]
async fn apply_committed_does_not_abort_once_applied_is_durable() {
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

    let sink = RecordingApply::default();
    coordinator
        .apply_committed(mutation.effect_id, &sink)
        .await
        .unwrap();

    // A permanent release failure on the replayed Applied row cannot
    // rewrite the committed decision: the error propagates and the row
    // stays Applied.
    *sink.permanent_releases.lock().unwrap() = 1;
    let error = coordinator
        .apply_committed(mutation.effect_id, &sink)
        .await
        .unwrap_err();
    assert!(matches!(error, EffectJournalError::Conflict));
    let committed = coordinator
        .read_committed(mutation.effect_id)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        committed.transition,
        EffectTransition::Applied { .. }
    ));
}

#[tokio::test]
async fn apply_committed_aborts_through_an_ambiguous_terminal_commit() {
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
    sink.permanent_appends
        .lock()
        .unwrap()
        .insert(outputs[0].writer_session_id);
    // A lost commit reply on the terminal row reconciles to the same
    // Aborted mutation instead of leaving the effect non-terminal.
    transport
        .commit_down
        .lock()
        .unwrap()
        .insert(nodes[2].clone());
    let terminal = coordinator
        .apply_committed(mutation.effect_id, &sink)
        .await
        .unwrap();
    assert!(matches!(
        terminal.transition,
        EffectTransition::Aborted { .. }
    ));
}

#[tokio::test]
async fn apply_committed_next_effect_applies_after_an_aborted_tail_frees() {
    let directories = [
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
    ];
    let nodes = nodes();
    let transport = Arc::new(TestEffectTransport::new(&directories, &nodes));
    let coordinator = effect_coordinator(transport, &nodes);
    let aborted = declare(Uuid::new_v4(), 1, 1);
    coordinator.apply(aborted.clone()).await.unwrap();

    let sink = RecordingApply::default();
    let EffectTransition::Declare { outputs, .. } = &aborted.transition else {
        panic!("expected declare")
    };
    sink.permanent_appends
        .lock()
        .unwrap()
        .insert(outputs[0].writer_session_id);
    coordinator
        .apply_committed(aborted.effect_id, &sink)
        .await
        .unwrap();

    let next = declare(Uuid::new_v4(), 1, 1);
    coordinator.apply(next.clone()).await.unwrap();
    let applied = coordinator
        .apply_committed(next.effect_id, &sink)
        .await
        .unwrap();
    assert!(matches!(
        applied.transition,
        EffectTransition::Applied { .. }
    ));
    assert_eq!(sink.appended.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn aborted_rows_obey_the_reason_and_output_bounds() {
    let directories = [
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
    ];
    let nodes = nodes();
    let transport = Arc::new(TestEffectTransport::new(&directories, &nodes));
    let declared = declare(Uuid::new_v4(), 1, 1);
    let coordinator = effect_coordinator(transport, &nodes);
    coordinator.apply(declared.clone()).await.unwrap();

    let aborted = |reason: String| EffectMutation {
        effect_id: declared.effect_id,
        subscription_id: scope(),
        ownership_epoch: 1,
        sequence: 2,
        request_id: Uuid::new_v4(),
        transition: EffectTransition::Aborted {
            declares: declared.request_id,
            reason,
            outputs: Vec::new(),
        },
    };
    for reason in [String::new(), "x".repeat(257)] {
        let error = coordinator.apply(aborted(reason)).await.unwrap_err();
        assert!(matches!(error, EffectJournalError::TooLarge));
    }
    // In-bound reasons commit as the terminal row.
    coordinator
        .apply(aborted("sink refused the output".to_owned()))
        .await
        .unwrap();
}
