//! Pipe driver integration tests: a declared Pipe drives committed
//! `Declare -> Applied` effects over real Fjall journal and Subscription
//! progress replicas with an injected fault transport, so deterministic
//! identities, replay safety, and frontier CAS fencing are exercised through
//! the same protocol the production Node runs.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};

use finnstream::active_range::{RangeId, ReplicaSet, StorageNodeId};
use finnstream::control::{PipeDefinition, PipeOperation, PipeStage, ResourceStatus};
use finnstream::effect::{
    effect_apply_request_id, EffectApply, EffectCommitEvidence, EffectConsume, EffectCoordinator,
    EffectJournalError, EffectJournalInspection, EffectJournalTransport, EffectMutation,
    EffectOutput, EffectPrepareVote, EffectReplicaReply, EffectTransition,
    FjallEffectJournalReplica,
};
use finnstream::pipe::{
    pipe_declare_request_id, pipe_effect_id, pipe_output_writer_session, FetchedPage,
    FetchedRecord, PipeDriver,
};
use finnstream::reader::{
    FjallSubscriptionProgressReplica, SubscriptionCommitEvidence, SubscriptionMemberState,
    SubscriptionPrepareVote, SubscriptionProgressAssignment, SubscriptionProgressCoordinator,
    SubscriptionProgressError, SubscriptionProgressInspection, SubscriptionProgressMutation,
    SubscriptionProgressTransport, SubscriptionReplicaReply,
};
use tempfile::TempDir;
use uuid::Uuid;

const SUBSCRIPTION: u128 = 0x5EED;
const OUTPUT_FEED: u128 = 0x0E7;

fn nodes() -> [StorageNodeId; 3] {
    ["pipe-a", "pipe-b", "pipe-c"].map(|name| StorageNodeId::try_new(name).unwrap())
}

fn range() -> RangeId {
    RangeId::from_uuid(Uuid::from_u128(0xA11CE))
}

fn feed() -> Uuid {
    Uuid::from_u128(0xFEED)
}

fn assignment() -> SubscriptionProgressAssignment {
    SubscriptionProgressAssignment::try_new(
        Uuid::from_u128(SUBSCRIPTION),
        nodes()[0].clone(),
        ReplicaSet::try_new(nodes()).unwrap(),
        7,
    )
    .unwrap()
}

fn pipe() -> PipeDefinition {
    PipeDefinition {
        pipe_id: Uuid::from_u128(0x91),
        name: "orders.forward".to_owned(),
        space_id: Uuid::from_u128(0xD0),
        subscription_id: Uuid::from_u128(SUBSCRIPTION),
        operation: PipeOperation::Forward,
        output_feed_id: Uuid::from_u128(OUTPUT_FEED),
        stage: PipeStage::Declared,
        status: ResourceStatus::Active,
        created_at_ns: 0,
    }
}

/// A sequence-1 frontier mutation like the one first-member join (or the
/// Pipe bootstrap) commits.
fn frontier_mutation(cursor: &str) -> SubscriptionProgressMutation {
    SubscriptionProgressMutation {
        subscription_id: Uuid::from_u128(SUBSCRIPTION),
        feed_id: feed(),
        ownership_epoch: 7,
        sequence: 1,
        request_id: Uuid::new_v4(),
        expected_cursor: None,
        cursor: cursor.to_owned(),
        positions: BTreeMap::from([(range(), "root".to_owned())]),
        tick: 1,
        lease_ops: Vec::new(),
    }
}

/// Seed a committed frontier mutation (sequence 1, cursor `cursor`).
async fn seed_frontier(
    progress: &SubscriptionProgressCoordinator,
    cursor: &str,
) -> SubscriptionProgressMutation {
    let mutation = frontier_mutation(cursor);
    progress.apply(mutation.clone()).await.unwrap();
    mutation
}

/// A bootstrap result that would commit `cursor` if no frontier existed.
fn bootstrap_with(cursor: &str) -> SubscriptionProgressMutation {
    frontier_mutation(cursor)
}

struct PipeProgressTransport {
    stores: BTreeMap<StorageNodeId, Arc<FjallSubscriptionProgressReplica>>,
    commit_down: Mutex<BTreeSet<Uuid>>,
    read_down: Mutex<BTreeSet<StorageNodeId>>,
}

impl PipeProgressTransport {
    fn new(directories: &[TempDir; 3]) -> Self {
        Self {
            stores: nodes()
                .iter()
                .cloned()
                .zip(directories.iter().map(|directory| {
                    Arc::new(FjallSubscriptionProgressReplica::open(directory.path()).unwrap())
                }))
                .collect(),
            commit_down: Mutex::new(BTreeSet::new()),
            read_down: Mutex::new(BTreeSet::new()),
        }
    }
}

#[async_trait::async_trait]
impl SubscriptionProgressTransport for PipeProgressTransport {
    async fn prepare(
        &self,
        node: &StorageNodeId,
        mutation: SubscriptionProgressMutation,
    ) -> Result<SubscriptionReplicaReply<SubscriptionPrepareVote>, SubscriptionProgressError> {
        let store = self
            .stores
            .get(node)
            .cloned()
            .ok_or(SubscriptionProgressError::InvalidAssignment)?;
        let subscription_id = mutation.subscription_id;
        let ownership_epoch = mutation.ownership_epoch;
        let vote = tokio::task::spawn_blocking(move || store.prepare(mutation))
            .await
            .map_err(|_| SubscriptionProgressError::Unavailable)??;
        Ok(SubscriptionReplicaReply {
            replica: node.clone(),
            subscription_id,
            ownership_epoch,
            result: vote,
        })
    }

    async fn commit(
        &self,
        node: &StorageNodeId,
        evidence: SubscriptionCommitEvidence,
    ) -> Result<SubscriptionReplicaReply<SubscriptionProgressMutation>, SubscriptionProgressError>
    {
        if self
            .commit_down
            .lock()
            .unwrap()
            .contains(&evidence.request_id())
        {
            return Err(SubscriptionProgressError::Unavailable);
        }
        let store = self
            .stores
            .get(node)
            .cloned()
            .ok_or(SubscriptionProgressError::InvalidAssignment)?;
        let result = tokio::task::spawn_blocking(move || store.commit_with_quorum(evidence))
            .await
            .map_err(|_| SubscriptionProgressError::Unavailable)??;
        Ok(SubscriptionReplicaReply {
            replica: node.clone(),
            subscription_id: result.subscription_id,
            ownership_epoch: result.ownership_epoch,
            result,
        })
    }

    async fn committed(
        &self,
        node: &StorageNodeId,
        subscription_id: Uuid,
        ownership_epoch: u64,
    ) -> Result<
        SubscriptionReplicaReply<Option<SubscriptionProgressMutation>>,
        SubscriptionProgressError,
    > {
        if self.read_down.lock().unwrap().contains(node) {
            return Err(SubscriptionProgressError::Unavailable);
        }
        let store = self
            .stores
            .get(node)
            .cloned()
            .ok_or(SubscriptionProgressError::InvalidAssignment)?;
        let result = tokio::task::spawn_blocking(move || store.local_committed(subscription_id))
            .await
            .map_err(|_| SubscriptionProgressError::Unavailable)??;
        Ok(SubscriptionReplicaReply {
            replica: node.clone(),
            subscription_id,
            ownership_epoch,
            result,
        })
    }

    async fn inspect(
        &self,
        node: &StorageNodeId,
        subscription_id: Uuid,
        ownership_epoch: u64,
    ) -> Result<SubscriptionReplicaReply<SubscriptionProgressInspection>, SubscriptionProgressError>
    {
        let store = self
            .stores
            .get(node)
            .cloned()
            .ok_or(SubscriptionProgressError::InvalidAssignment)?;
        let result = tokio::task::spawn_blocking(move || store.local_state(subscription_id))
            .await
            .map_err(|_| SubscriptionProgressError::Unavailable)??;
        Ok(SubscriptionReplicaReply {
            replica: node.clone(),
            subscription_id,
            ownership_epoch,
            result,
        })
    }

    async fn adopt(
        &self,
        node: &StorageNodeId,
        _owner: &StorageNodeId,
        subscription_id: Uuid,
        ownership_epoch: u64,
        committed: Option<SubscriptionProgressMutation>,
        members: SubscriptionMemberState,
    ) -> Result<
        SubscriptionReplicaReply<Option<SubscriptionProgressMutation>>,
        SubscriptionProgressError,
    > {
        let store = self
            .stores
            .get(node)
            .cloned()
            .ok_or(SubscriptionProgressError::InvalidAssignment)?;
        let result = tokio::task::spawn_blocking(move || {
            store.adopt_recovered(subscription_id, feed(), ownership_epoch, committed, members)
        })
        .await
        .map_err(|_| SubscriptionProgressError::Unavailable)??;
        Ok(SubscriptionReplicaReply {
            replica: node.clone(),
            subscription_id,
            ownership_epoch,
            result,
        })
    }
}

struct PipeEffectTransport {
    stores: BTreeMap<StorageNodeId, Arc<FjallEffectJournalReplica>>,
    commit_down: Mutex<BTreeSet<Uuid>>,
    prepare_down: Mutex<BTreeSet<StorageNodeId>>,
}

impl PipeEffectTransport {
    fn new(directories: &[TempDir; 3]) -> Self {
        Self {
            stores: nodes()
                .iter()
                .cloned()
                .zip(directories.iter().map(|directory| {
                    Arc::new(FjallEffectJournalReplica::open(directory.path()).unwrap())
                }))
                .collect(),
            commit_down: Mutex::new(BTreeSet::new()),
            prepare_down: Mutex::new(BTreeSet::new()),
        }
    }
}

#[async_trait::async_trait]
impl EffectJournalTransport for PipeEffectTransport {
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
            replica: node.clone(),
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
        if self
            .commit_down
            .lock()
            .unwrap()
            .contains(&evidence.request_id)
        {
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
            replica: node.clone(),
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
        let store = self
            .stores
            .get(node)
            .cloned()
            .ok_or(EffectJournalError::InvalidAssignment)?;
        let result = tokio::task::spawn_blocking(move || store.local_committed(effect_id))
            .await
            .map_err(|_| EffectJournalError::Unavailable)??;
        Ok(EffectReplicaReply {
            replica: node.clone(),
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
            store.adopt_recovered(
                effect_id,
                Uuid::from_u128(SUBSCRIPTION),
                ownership_epoch,
                committed,
            )
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

/// The test apply sink records output calls and commits the frontier through
/// the real progress coordinator, exactly as the production sink does.
struct RecordingApply {
    progress: SubscriptionProgressCoordinator,
    appends: Mutex<Vec<EffectOutput>>,
}

#[async_trait::async_trait]
impl EffectApply for RecordingApply {
    async fn append_output(&self, output: &EffectOutput) -> Result<(), EffectJournalError> {
        self.appends.lock().unwrap().push(output.clone());
        Ok(())
    }

    async fn commit_frontier(
        &self,
        consume: &EffectConsume,
        request_id: Uuid,
    ) -> Result<(), EffectJournalError> {
        let committed = self
            .progress
            .read_committed()
            .await
            .map_err(progress_to_effect)?;
        if committed
            .as_ref()
            .is_some_and(|current| current.cursor == consume.cursor)
        {
            return Ok(());
        }
        let mutation = SubscriptionProgressMutation {
            subscription_id: Uuid::from_u128(SUBSCRIPTION),
            feed_id: consume.feed_id,
            ownership_epoch: 7,
            sequence: committed.as_ref().map_or(1, |prior| prior.sequence + 1),
            request_id,
            expected_cursor: consume.expected_cursor.clone(),
            cursor: consume.cursor.clone(),
            positions: consume.positions.clone(),
            tick: 0,
            lease_ops: Vec::new(),
        };
        match self.progress.apply(mutation.clone()).await {
            Ok(_) => Ok(()),
            Err(SubscriptionProgressError::AmbiguousCommit)
            | Err(SubscriptionProgressError::NoQuorum) => self
                .progress
                .reconcile_retry(mutation)
                .await
                .map(|_| ())
                .map_err(progress_to_effect),
            Err(error) => Err(progress_to_effect(error)),
        }
    }
}

fn progress_to_effect(error: SubscriptionProgressError) -> EffectJournalError {
    match error {
        SubscriptionProgressError::Unavailable
        | SubscriptionProgressError::AmbiguousCommit
        | SubscriptionProgressError::NoQuorum
        | SubscriptionProgressError::Engine(_)
        | SubscriptionProgressError::Serialization(_) => EffectJournalError::Unavailable,
        _ => EffectJournalError::Conflict,
    }
}

struct Harness {
    _progress_dirs: [TempDir; 3],
    _effect_dirs: [TempDir; 3],
    driver: PipeDriver,
    journal: Arc<EffectCoordinator>,
    progress: Arc<SubscriptionProgressCoordinator>,
    sink: RecordingApply,
    effect_transport: Arc<PipeEffectTransport>,
    progress_transport: Arc<PipeProgressTransport>,
}

fn harness() -> Harness {
    let progress_dirs = [
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
    ];
    let effect_dirs = [
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
    ];
    let progress_transport = Arc::new(PipeProgressTransport::new(&progress_dirs));
    let effect_transport = Arc::new(PipeEffectTransport::new(&effect_dirs));
    let progress = Arc::new(SubscriptionProgressCoordinator::new(
        assignment(),
        progress_transport.clone(),
    ));
    let journal = Arc::new(EffectCoordinator::new(
        assignment(),
        effect_transport.clone(),
    ));
    let sink = RecordingApply {
        progress: SubscriptionProgressCoordinator::new(assignment(), progress_transport.clone()),
        appends: Mutex::new(Vec::new()),
    };
    Harness {
        _progress_dirs: progress_dirs,
        _effect_dirs: effect_dirs,
        driver: PipeDriver::new(pipe(), journal.clone(), progress.clone()),
        journal,
        progress,
        sink,
        effect_transport,
        progress_transport,
    }
}

fn record(byte: u8) -> FetchedRecord {
    FetchedRecord {
        message_id: Uuid::from_u128(byte as u128 + 1),
        key_base64: "a2V5".to_owned(),
        payload_base64: "cGF5bG9hZA==".to_owned(),
        metadata_base64: BTreeMap::new(),
        event_time_ns: byte as i64,
    }
}

fn page(records: Vec<FetchedRecord>, cursor: &str) -> FetchedPage {
    FetchedPage {
        records,
        cursor: cursor.to_owned(),
        positions: BTreeMap::from([(range(), cursor.to_owned())]),
    }
}

#[tokio::test]
async fn pipe_driver_declares_applies_outputs_and_moves_the_frontier() {
    let harness = harness();
    seed_frontier(&harness.progress, "p0").await;

    let applied = harness
        .driver
        .drive_once(
            64,
            |frontier, _limit| {
                let frontier = frontier.clone();
                async move {
                    assert_eq!(frontier.cursor, "p0");
                    assert_eq!(frontier.feed_id, feed());
                    Ok(page(vec![record(1), record(2), record(3)], "p1"))
                }
            },
            || async { Ok(bootstrap_with("p0")) },
            &harness.sink,
        )
        .await
        .unwrap()
        .expect("applied");

    assert!(matches!(
        applied.transition,
        EffectTransition::Applied { .. }
    ));
    assert_eq!(
        applied.effect_id,
        pipe_effect_id(pipe().pipe_id, Uuid::from_u128(SUBSCRIPTION), "p0")
    );

    // Every output got the deterministic per-record writer identity.
    {
        let appends = harness.sink.appends.lock().unwrap();
        assert_eq!(appends.len(), 3);
        for (index, output) in appends.iter().enumerate() {
            assert_eq!(output.feed_id, Uuid::from_u128(OUTPUT_FEED));
            assert_eq!(
                output.writer_session_id,
                pipe_output_writer_session(pipe().pipe_id, Uuid::from_u128(index as u128 + 2))
            );
            assert_eq!(output.sequence, 1);
            assert_eq!(output.writer_epoch, 1);
        }
    }

    // The consumed frontier moved exactly once, to the page cursor.
    let frontier = harness
        .progress
        .read_committed()
        .await
        .unwrap()
        .expect("frontier");
    assert_eq!(frontier.cursor, "p1");
    assert_eq!(frontier.sequence, 2);

    // The journal row is terminal Applied on all three replicas.
    for node in nodes() {
        let row = harness.effect_transport.stores[&node]
            .local_committed(applied.effect_id)
            .unwrap()
            .expect("committed");
        assert!(matches!(row.transition, EffectTransition::Applied { .. }));
    }
}

#[tokio::test]
async fn pipe_driver_returns_none_when_the_source_is_caught_up() {
    let harness = harness();
    seed_frontier(&harness.progress, "p0").await;
    let applied = harness
        .driver
        .drive_once(
            64,
            |_frontier, _limit| async { Ok(page(Vec::new(), "p0")) },
            || async { Ok(bootstrap_with("p0")) },
            &harness.sink,
        )
        .await
        .unwrap();
    assert!(applied.is_none());
    assert!(harness.sink.appends.lock().unwrap().is_empty());
    let frontier = harness
        .progress
        .read_committed()
        .await
        .unwrap()
        .expect("frontier");
    assert_eq!(frontier.cursor, "p0");
}

#[tokio::test]
async fn pipe_driver_replays_safely_after_an_ambiguous_declare_commit() {
    let harness = harness();
    seed_frontier(&harness.progress, "p0").await;
    let effect_id = pipe_effect_id(pipe().pipe_id, Uuid::from_u128(SUBSCRIPTION), "p0");
    let declare_request = pipe_declare_request_id(effect_id);
    harness
        .effect_transport
        .commit_down
        .lock()
        .unwrap()
        .insert(declare_request);

    // The Declare's commit phase loses quorum; the drive errors but the
    // replicas hold the identical prepared row.
    let error = harness
        .driver
        .drive_once(
            64,
            |_f, _l| async { Ok(page(vec![record(1)], "p1")) },
            || async { Ok(bootstrap_with("p0")) },
            &harness.sink,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        EffectJournalError::AmbiguousCommit
            | EffectJournalError::NoQuorum
            | EffectJournalError::Unavailable
    ));
    assert!(harness.sink.appends.lock().unwrap().is_empty());

    // Clear the fault and re-drive: the identical Declare replays, the apply
    // completes, and outputs append exactly once.
    harness.effect_transport.commit_down.lock().unwrap().clear();
    let applied = harness
        .driver
        .drive_once(
            64,
            |_f, _l| async { Ok(page(vec![record(1)], "p1")) },
            || async { Ok(bootstrap_with("p0")) },
            &harness.sink,
        )
        .await
        .unwrap()
        .expect("applied");
    assert_eq!(applied.effect_id, effect_id);
    assert_eq!(harness.sink.appends.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn pipe_driver_replays_outputs_identically_when_applied_commit_is_ambiguous() {
    let harness = harness();
    seed_frontier(&harness.progress, "p0").await;
    let effect_id = pipe_effect_id(pipe().pipe_id, Uuid::from_u128(SUBSCRIPTION), "p0");
    let applied_request = effect_apply_request_id(pipe_declare_request_id(effect_id), b"applied");
    harness
        .effect_transport
        .commit_down
        .lock()
        .unwrap()
        .insert(applied_request);

    // Declare commits, outputs append, the frontier moves — then the Applied
    // marker's commit loses quorum and the drive reports an error.
    let error = harness
        .driver
        .drive_once(
            64,
            |_f, _l| async { Ok(page(vec![record(1), record(2)], "p1")) },
            || async { Ok(bootstrap_with("p0")) },
            &harness.sink,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        EffectJournalError::AmbiguousCommit
            | EffectJournalError::NoQuorum
            | EffectJournalError::Unavailable
    ));
    assert_eq!(harness.sink.appends.lock().unwrap().len(), 2);
    let frontier = harness
        .progress
        .read_committed()
        .await
        .unwrap()
        .expect("frontier");
    assert_eq!(frontier.cursor, "p1");

    // Re-drive: the frontier already moved, so the driver builds the NEXT
    // effect over "p1" — the stranded Declare stays committed-but-unapplied
    // until reconcile completes it. Re-drive with the new frontier page.
    harness.effect_transport.commit_down.lock().unwrap().clear();
    let applied = harness
        .driver
        .drive_once(
            64,
            |_f, _l| async { Ok(page(Vec::new(), "p1")) },
            || async { Ok(bootstrap_with("p0")) },
            &harness.sink,
        )
        .await
        .unwrap();
    assert!(applied.is_none());

    // The stranded effect still applies exactly once via apply_committed:
    // output replays carry identical deterministic writer identities.
    let completed = harness
        .journal
        .apply_committed(effect_id, &harness.sink)
        .await
        .unwrap();
    assert!(matches!(
        completed.transition,
        EffectTransition::Applied { .. }
    ));
    let appends = harness.sink.appends.lock().unwrap();
    assert_eq!(appends.len(), 4);
    assert_eq!(appends[0].writer_session_id, appends[2].writer_session_id);
    assert_eq!(appends[1].writer_session_id, appends[3].writer_session_id);
}

#[tokio::test]
async fn pipe_driver_fences_a_frontier_that_moved_mid_apply() {
    let harness = harness();
    seed_frontier(&harness.progress, "p0").await;

    // A competing consumer moves the frontier to "pX" while the drive holds
    // the fetched page; the frontier CAS must refuse the stale evidence.
    let mover =
        SubscriptionProgressCoordinator::new(assignment(), harness.progress_transport.clone());
    let moved = SubscriptionProgressMutation {
        subscription_id: Uuid::from_u128(SUBSCRIPTION),
        feed_id: feed(),
        ownership_epoch: 7,
        sequence: 2,
        request_id: Uuid::new_v4(),
        expected_cursor: Some("p0".to_owned()),
        cursor: "pX".to_owned(),
        positions: BTreeMap::from([(range(), "other".to_owned())]),
        tick: 0,
        lease_ops: Vec::new(),
    };
    // Move it inside the fetch so the driver has already read "p0".
    let moved = Mutex::new(Some(moved));
    let mover = Mutex::new(Some(mover));
    let error = harness
        .driver
        .drive_once(
            64,
            |_f, _l| {
                let moved = moved.lock().unwrap().take().unwrap();
                let mover = mover.lock().unwrap().take().unwrap();
                async move {
                    mover.apply(moved).await.unwrap();
                    Ok(page(vec![record(1)], "p1"))
                }
            },
            || async { Ok(bootstrap_with("p0")) },
            &harness.sink,
        )
        .await
        .unwrap_err();
    assert!(matches!(error, EffectJournalError::Conflict));

    // The Declare stays committed but un-applied; the frontier is the
    // competing consumer's "pX", not the stale "p1".
    let effect_id = pipe_effect_id(pipe().pipe_id, Uuid::from_u128(SUBSCRIPTION), "p0");
    let row = harness
        .journal
        .read_committed(effect_id)
        .await
        .unwrap()
        .expect("declare committed");
    assert!(matches!(row.transition, EffectTransition::Declare { .. }));
    let frontier = harness
        .progress
        .read_committed()
        .await
        .unwrap()
        .expect("frontier");
    assert_eq!(frontier.cursor, "pX");
}

#[tokio::test]
async fn pipe_driver_bootstraps_the_frontier_for_an_unjoined_subscription() {
    let harness = harness();
    // No member ever joined: the driver must establish the sequence-1
    // frontier through the supplied bootstrap before fetching.
    let applied = harness
        .driver
        .drive_once(
            64,
            |frontier, _limit| {
                let frontier = frontier.clone();
                async move {
                    assert_eq!(frontier.sequence, 1);
                    assert_eq!(frontier.cursor, "start");
                    Ok(page(vec![record(7)], "p1"))
                }
            },
            || async { Ok(bootstrap_with("start")) },
            &harness.sink,
        )
        .await
        .unwrap()
        .expect("applied");
    assert!(matches!(
        applied.transition,
        EffectTransition::Applied { .. }
    ));
    let frontier = harness
        .progress
        .read_committed()
        .await
        .unwrap()
        .expect("frontier");
    assert_eq!(frontier.cursor, "p1");
    assert_eq!(frontier.sequence, 2);
    assert_eq!(harness.sink.appends.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn pipe_driver_surfaces_a_failing_bootstrap() {
    let harness = harness();
    let error = harness
        .driver
        .drive_once(
            64,
            |_f, _l| async { Ok(page(Vec::new(), "p0")) },
            || async { Err(EffectJournalError::Unavailable) },
            &harness.sink,
        )
        .await
        .unwrap_err();
    assert!(matches!(error, EffectJournalError::Unavailable));
}
