//! Pipe driver integration tests: a declared Pipe drives committed
//! `Declare -> Applied` effects over real Fjall journal and Subscription
//! progress replicas with an injected fault transport, so deterministic
//! identities, replay safety, and frontier CAS fencing are exercised through
//! the same protocol the production Node runs.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};

use tempfile::TempDir;
use uuid::Uuid;
use whitewater::active_range::{RangeId, RangePosition, ReplicaSet, StorageNodeId};
use whitewater::control::{
    IndexFieldSpec, PipeDefinition, PipeOperation, PipeStage, ResourceStatus, WindowAggregation,
};
use whitewater::effect::{
    effect_apply_request_id, EffectApply, EffectCommitEvidence, EffectConsume, EffectCoordinator,
    EffectJournalError, EffectJournalInspection, EffectJournalTransport, EffectMutation,
    EffectOutput, EffectOutputReceipt, EffectPrepareVote, EffectReleaseOutcome,
    EffectReleaseTarget, EffectReplicaReply, EffectTransition, FjallEffectJournalReplica,
};
use whitewater::index::IndexValue;
use whitewater::pipe::{
    pipe_declare_request_id, pipe_emission_writer_session, pipe_output_writer_session,
    window_group_key_bytes, window_pane_row_key, window_refused_groups, EnrichResolution,
    FetchedPage, FetchedRecord, PipeDriver, WindowEmission,
};
use whitewater::reader::{
    FjallSubscriptionProgressReplica, SubscriptionCommitEvidence, SubscriptionMemberState,
    SubscriptionPrepareVote, SubscriptionProgressAssignment, SubscriptionProgressCoordinator,
    SubscriptionProgressError, SubscriptionProgressInspection, SubscriptionProgressMutation,
    SubscriptionProgressTransport, SubscriptionReplicaReply,
};
use whitewater::state_store::StateStoreMutationOp;

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
        output_feed_id: Some(Uuid::from_u128(OUTPUT_FEED)),
        output_store_id: None,
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
        member_progress: BTreeMap::new(),
        member_feed: None,
        expected_cursor: None,
        cursor: cursor.to_owned(),
        positions: BTreeMap::from([(range(), "root".to_owned())]),
        quarantined: BTreeMap::new(),
        quarantine_add: BTreeMap::new(),
        quarantine_remove: BTreeMap::new(),
        skipped: BTreeMap::new(),
        skip_add: BTreeMap::new(),
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
    mutations: Mutex<Vec<whitewater::effect::EffectStateMutation>>,
    fail_releases: Mutex<u8>,
    discards: Mutex<Vec<(Uuid, Uuid, Uuid)>>,
}

#[async_trait::async_trait]
impl EffectApply for RecordingApply {
    async fn append_output(
        &self,
        output: &EffectOutput,
        _effect_id: Uuid,
    ) -> Result<EffectOutputReceipt, EffectJournalError> {
        let mut appends = self.appends.lock().unwrap();
        if !appends
            .iter()
            .any(|seen| seen.writer_session_id == output.writer_session_id)
        {
            appends.push(output.clone());
        }
        Ok(EffectOutputReceipt {
            feed_id: output.feed_id,
            range_id: RangeId::from_uuid(Uuid::from_u128(9)),
            position: RangePosition::new(1),
        })
    }

    async fn release_output(
        &self,
        _target: &EffectReleaseTarget,
    ) -> Result<EffectReleaseOutcome, EffectJournalError> {
        let mut remaining = self.fail_releases.lock().unwrap();
        if *remaining > 0 {
            *remaining -= 1;
            return Err(EffectJournalError::Unavailable);
        }
        Ok(EffectReleaseOutcome::Released { committed: 1 })
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
            member_progress: BTreeMap::new(),
            member_feed: None,
            expected_cursor: consume.expected_cursor.clone(),
            cursor: consume.cursor.clone(),
            positions: consume.positions.clone(),
            quarantined: BTreeMap::new(),
            quarantine_add: BTreeMap::new(),
            quarantine_remove: BTreeMap::new(),
            skipped: BTreeMap::new(),
            skip_add: BTreeMap::new(),
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

    async fn mutate_state(
        &self,
        mutation: &whitewater::effect::EffectStateMutation,
    ) -> Result<(), EffectJournalError> {
        self.mutations.lock().unwrap().push(mutation.clone());
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
        Ok(whitewater::effect::EffectDiscardOutcome::Discarded { dropped_frames: 1 })
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
    harness_for(pipe())
}

fn harness_for(pipe: PipeDefinition) -> Harness {
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
        mutations: Mutex::new(Vec::new()),
        fail_releases: Mutex::new(0),
        discards: Mutex::new(Vec::new()),
    };
    Harness {
        _progress_dirs: progress_dirs,
        _effect_dirs: effect_dirs,
        driver: PipeDriver::new(pipe, journal.clone(), progress.clone()),
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
        ingest_time_ns: byte as i64,
        resolution: None,
    }
}

fn page(records: Vec<FetchedRecord>, cursor: &str) -> FetchedPage {
    page_with_emissions(records, Vec::new(), cursor)
}

fn page_with_emissions(
    records: Vec<FetchedRecord>,
    emissions: Vec<WindowEmission>,
    cursor: &str,
) -> FetchedPage {
    FetchedPage {
        records,
        cursor: cursor.to_owned(),
        positions: BTreeMap::from([(range(), cursor.to_owned())]),
        member_feed: None,
        emissions,
        expired: Vec::new(),
        refused: Vec::new(),
        pane_rows: Default::default(),
        pane_rows_saturated: false,
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
        harness
            .driver
            .planned_effect_id(
                Uuid::from_u128(SUBSCRIPTION),
                feed(),
                "p0",
                &page(vec![record(1), record(2), record(3)], "p1"),
            )
            .unwrap()
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
    let effect_id = harness
        .driver
        .planned_effect_id(
            Uuid::from_u128(SUBSCRIPTION),
            feed(),
            "p0",
            &page(vec![record(1)], "p1"),
        )
        .unwrap();
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
async fn window_pipe_escapes_prepared_residue_when_the_replan_shifts() {
    let harness = harness_for(window_pipe());
    seed_frontier(&harness.progress, "p0").await;

    let record = json_record(1, b"key-a", 0, 5);
    let drifting = |expired: Vec<Vec<u8>>| FetchedPage {
        records: vec![record.clone()],
        cursor: "p1".to_owned(),
        positions: BTreeMap::from([(range(), "p1".to_owned())]),
        member_feed: None,
        emissions: Vec::new(),
        expired,
        refused: Vec::new(),
        pane_rows: Default::default(),
        pane_rows_saturated: false,
    };

    // Drive one's commit dies in flight: the replicas hold a prepared
    // Declare for one expired row — the residue that used to wedge every
    // later drive on a same-request conflict.
    let first_id = harness
        .driver
        .planned_effect_id(
            Uuid::from_u128(SUBSCRIPTION),
            feed(),
            "p0",
            &drifting(vec![b"row-a".to_vec()]),
        )
        .unwrap();
    harness
        .effect_transport
        .commit_down
        .lock()
        .unwrap()
        .insert(pipe_declare_request_id(first_id));
    let error = harness
        .driver
        .drive_once(
            64,
            |_f, _l| async { Ok(drifting(vec![b"row-a".to_vec()])) },
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
    harness.effect_transport.commit_down.lock().unwrap().clear();

    // Wall clock moved: the same frontier now scans a second expired row,
    // so the plan legitimately diverges. A cursor-only identity would have
    // re-declared different content under the residue's request id and
    // Conflicted forever; the plan-covered identity lands on a fresh
    // effect and commits.
    let applied = harness
        .driver
        .drive_once(
            64,
            move |_f, _l| async move { Ok(drifting(vec![b"row-a".to_vec(), b"row-b".to_vec()])) },
            || async { Ok(bootstrap_with("p0")) },
            &harness.sink,
        )
        .await
        .unwrap()
        .expect("applied");
    assert_ne!(applied.effect_id, first_id);
    let EffectTransition::Applied {
        state_mutations, ..
    } = &applied.transition
    else {
        panic!("expected Applied")
    };
    // One accumulate for the folded record plus both expired-row deletes.
    assert_eq!(state_mutations.len(), 3);
    let frontier = harness
        .progress
        .read_committed()
        .await
        .unwrap()
        .expect("frontier");
    assert_eq!(frontier.cursor, "p1");
}

#[tokio::test]
async fn pipe_driver_replays_outputs_identically_when_applied_commit_is_ambiguous() {
    let harness = harness();
    seed_frontier(&harness.progress, "p0").await;
    let effect_id = harness
        .driver
        .planned_effect_id(
            Uuid::from_u128(SUBSCRIPTION),
            feed(),
            "p0",
            &page(vec![record(1), record(2)], "p1"),
        )
        .unwrap();
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
    // output replays carry identical deterministic writer identities, so
    // the sink's dedupe records each output exactly once across both
    // attempts and the explicit completion.
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
    assert_eq!(appends.len(), 2);
    assert_eq!(
        appends
            .iter()
            .map(|output| output.writer_session_id)
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        2
    );
}

#[tokio::test]
async fn pipe_driver_aborts_a_declare_whose_frontier_moved_mid_apply() {
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
        member_progress: BTreeMap::new(),
        member_feed: None,
        expected_cursor: Some("p0".to_owned()),
        cursor: "pX".to_owned(),
        positions: BTreeMap::from([(range(), "other".to_owned())]),
        quarantined: BTreeMap::new(),
        quarantine_add: BTreeMap::new(),
        quarantine_remove: BTreeMap::new(),
        skipped: BTreeMap::new(),
        skip_add: BTreeMap::new(),
        tick: 0,
        lease_ops: Vec::new(),
    };
    // Move it inside the fetch so the driver has already read "p0".
    let moved = Mutex::new(Some(moved));
    let mover = Mutex::new(Some(mover));
    let terminal = harness
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
        .unwrap()
        .expect("terminal row");

    // The stale-frontier refusal terminalizes the Declare as Aborted
    // rather than leaving the output tail fenced forever: the row names
    // the committed Declare, the discard freed the Range, and the
    // competing consumer's "pX" frontier is untouched.
    let effect_id = harness
        .driver
        .planned_effect_id(
            Uuid::from_u128(SUBSCRIPTION),
            feed(),
            "p0",
            &page(vec![record(1)], "p1"),
        )
        .unwrap();
    let row = harness
        .journal
        .read_committed(effect_id)
        .await
        .unwrap()
        .expect("declare committed");
    let EffectTransition::Aborted { declares, .. } = &row.transition else {
        panic!("expected aborted, got {:?}", row.transition)
    };
    assert_eq!(*declares, pipe_declare_request_id(effect_id));
    assert!(matches!(
        terminal.transition,
        EffectTransition::Aborted { .. }
    ));
    assert_eq!(harness.sink.discards.lock().unwrap().len(), 1);
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

#[tokio::test]
async fn pipe_driver_re_drives_once_after_a_transient_apply_failure() {
    let harness = harness();
    seed_frontier(&harness.progress, "p0").await;
    // The first apply exhausts its bounded release retry; the drive's own
    // bounded re-drive replays the identical effect identity and converges.
    *harness.sink.fail_releases.lock().unwrap() = 3;
    let applied = harness
        .driver
        .drive_once(
            64,
            |_frontier, _limit| async move { Ok(page(vec![record(1)], "p1")) },
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
    // The frontier moved exactly once and each output committed once
    // despite the re-driven apply.
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
async fn pipe_driver_fails_closed_when_apply_stays_transient() {
    let harness = harness();
    seed_frontier(&harness.progress, "p0").await;
    // Both the per-step retries and the bounded re-drive exhaust; the
    // effect stays non-terminal so a later drive can still converge it.
    *harness.sink.fail_releases.lock().unwrap() = 12;
    assert!(matches!(
        harness
            .driver
            .drive_once(
                64,
                |_f, _l| async { Ok(page(vec![record(1)], "p1")) },
                || async { Ok(bootstrap_with("p0")) },
                &harness.sink,
            )
            .await,
        Err(EffectJournalError::Unavailable)
    ));
    *harness.sink.fail_releases.lock().unwrap() = 0;
    let applied = harness
        .driver
        .drive_once(
            64,
            |_frontier, _limit| async move { Ok(page(vec![record(1)], "p1")) },
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
    assert_eq!(harness.sink.appends.lock().unwrap().len(), 1);
}

const OUTPUT_STORE: u128 = 0x57E;

fn window_pipe() -> PipeDefinition {
    PipeDefinition {
        pipe_id: Uuid::from_u128(0x92),
        name: "orders.window".to_owned(),
        space_id: Uuid::from_u128(0xD0),
        subscription_id: Uuid::from_u128(SUBSCRIPTION),
        operation: PipeOperation::Window {
            window_ns: 160,
            key: IndexFieldSpec::Key,
            time: whitewater::control::WindowTime::Event,
            max_late_ns: None,
            expire_ns: None,
            key_limit: None,
            byte_limit: None,
            aggregation: WindowAggregation::Sum {
                value: IndexFieldSpec::Projection {
                    extractor: "json".to_owned(),
                    version: 1,
                    field: "v".to_owned(),
                },
            },
        },
        output_feed_id: None,
        output_store_id: Some(Uuid::from_u128(OUTPUT_STORE)),
        stage: PipeStage::Declared,
        status: ResourceStatus::Active,
        created_at_ns: 0,
    }
}

fn b64(bytes: &[u8]) -> String {
    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, bytes)
}

fn json_record(id: u128, key: &[u8], event_time_ns: i64, value: i64) -> FetchedRecord {
    FetchedRecord {
        message_id: Uuid::from_u128(id),
        key_base64: b64(key),
        payload_base64: b64(format!("{{\"v\":{value}}}").as_bytes()),
        metadata_base64: BTreeMap::new(),
        event_time_ns,
        ingest_time_ns: 0,
        resolution: None,
    }
}

#[tokio::test]
async fn window_pipe_quarantines_late_events_into_a_countable_row() {
    let mut pipe = window_pipe();
    pipe.operation = PipeOperation::Window {
        window_ns: 160,
        key: IndexFieldSpec::Key,
        aggregation: WindowAggregation::Count,
        time: whitewater::control::WindowTime::Event,
        max_late_ns: Some(500),
        expire_ns: None,
        key_limit: None,
        byte_limit: None,
    };
    let harness = harness_for(pipe);
    seed_frontier(&harness.progress, "p0").await;

    // event_time 0 with ingest 1000 is 1000ns late (> 500 bound);
    // event_time 990 with ingest 1000 is 10ns late (inside the bound).
    let mut late_record = json_record(1, b"key-a", 0, 0);
    late_record.ingest_time_ns = 1000;
    let mut on_time = json_record(2, b"key-a", 990, 0);
    on_time.ingest_time_ns = 1000;
    let applied = harness
        .driver
        .drive_once(
            64,
            |_f, _l| async { Ok(page(vec![late_record, on_time], "p1")) },
            || async { Ok(bootstrap_with("p0")) },
            &harness.sink,
        )
        .await
        .unwrap()
        .expect("applied");

    let EffectTransition::Applied {
        state_mutations, ..
    } = &applied.transition
    else {
        panic!("expected Applied")
    };
    let StateStoreMutationOp::AccumulateRows { entries } = &state_mutations[0].op else {
        panic!("expected AccumulateRows")
    };
    assert_eq!(entries.len(), 2);
    // On-time record lands on pane 99 (990 / 10ns panes); the late record
    // lands on the sentinel late pane with a count-only contribution.
    assert_eq!(entries[0].named[1].values, vec![IndexValue::I64(99)]);
    // Same group bytes, sentinel pane — the late row sorts after every
    // real pane for the group and can never collide with one.
    assert_eq!(
        entries[0].key[..entries[0].key.len() - 8],
        entries[1].key[..entries[1].key.len() - 8]
    );
    assert_eq!(
        entries[1].key[entries[1].key.len() - 8..],
        u64::MAX.to_be_bytes()
    );
    assert_eq!(entries[1].named[1].values, vec![IndexValue::I64(-1)]);
    assert_eq!(entries[1].count_delta, 1);
    assert_eq!(entries[1].sum_delta, 0);
}

fn ingest_window_pipe() -> PipeDefinition {
    let mut pipe = window_pipe();
    pipe.operation = PipeOperation::Window {
        window_ns: 160,
        key: IndexFieldSpec::Key,
        aggregation: WindowAggregation::Count,
        time: whitewater::control::WindowTime::Ingest,
        max_late_ns: None,
        expire_ns: None,
        key_limit: None,
        byte_limit: None,
    };
    pipe
}

#[tokio::test]
async fn ingest_time_window_panes_on_the_ingest_clock() {
    let harness = harness_for(ingest_window_pipe());
    seed_frontier(&harness.progress, "p0").await;

    // Same record, two clocks: event_time 0 lands in pane 0 but
    // ingest_time 15 lands in pane 1 under the 10ns pane size.
    let mut record = json_record(1, b"k", 0, 1);
    record.ingest_time_ns = 15;
    let applied = harness
        .driver
        .drive_once(
            64,
            |_frontier, _limit| async { Ok(page(vec![record], "p1")) },
            || async { Ok(bootstrap_with("p0")) },
            &harness.sink,
        )
        .await
        .unwrap()
        .expect("applied");
    let EffectTransition::Applied {
        state_mutations, ..
    } = &applied.transition
    else {
        panic!("expected Applied")
    };
    let StateStoreMutationOp::AccumulateRows { entries } = &state_mutations[0].op else {
        panic!("expected AccumulateRows")
    };
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].count_delta, 1);
    assert_eq!(entries[0].named[1].values, vec![IndexValue::I64(1)]);
}

#[tokio::test]
async fn window_pipe_folds_a_page_into_one_accumulate_mutation() {
    let harness = harness_for(window_pipe());
    seed_frontier(&harness.progress, "p0").await;

    // window 160ns -> 10ns panes: pane 0 covers event_time 0..10,
    // pane 1 covers 10..20. Two records share (key-a, pane 0).
    let applied = harness
        .driver
        .drive_once(
            64,
            |_frontier, _limit| async {
                Ok(page(
                    vec![
                        json_record(1, b"key-a", 0, 5),
                        json_record(2, b"key-a", 9, 7),
                        json_record(3, b"key-b", 12, -3),
                    ],
                    "p1",
                ))
            },
            || async { Ok(bootstrap_with("p0")) },
            &harness.sink,
        )
        .await
        .unwrap()
        .expect("applied");

    assert_eq!(
        applied.effect_id,
        harness
            .driver
            .planned_effect_id(
                Uuid::from_u128(SUBSCRIPTION),
                feed(),
                "p0",
                &page(
                    vec![
                        json_record(1, b"key-a", 0, 5),
                        json_record(2, b"key-a", 9, 7),
                        json_record(3, b"key-b", 12, -3),
                    ],
                    "p1",
                ),
            )
            .unwrap()
    );
    // The terminal row restates the mutation so post-marker crashes replay it.
    let EffectTransition::Applied {
        outputs,
        state_mutations,
        ..
    } = &applied.transition
    else {
        panic!("expected Applied")
    };
    assert!(outputs.is_empty());
    assert_eq!(state_mutations.len(), 1);
    assert_eq!(
        state_mutations[0].mutation_id,
        whitewater::effect::effect_state_mutation_id(applied.effect_id, 0)
    );
    assert_eq!(state_mutations[0].store_id, Uuid::from_u128(OUTPUT_STORE));

    let StateStoreMutationOp::AccumulateRows { entries } = &state_mutations[0].op else {
        panic!("expected AccumulateRows")
    };
    assert_eq!(entries.len(), 2);
    // Deterministic order: (group bytes, pane) sorted.
    assert_eq!(entries[0].count_delta, 2);
    assert_eq!(entries[0].sum_delta, 12);
    assert_eq!(entries[1].count_delta, 1);
    assert_eq!(entries[1].sum_delta, -3);
    // Pane postings make the view enumerable by group key and pane index.
    assert_eq!(
        entries[0].named[0].values,
        vec![IndexValue::Bytes(b"key-a".to_vec())]
    );
    assert_eq!(entries[0].named[1].values, vec![IndexValue::I64(0)]);
    assert_eq!(entries[1].named[1].values, vec![IndexValue::I64(1)]);
    // Distinct (key, pane) composite row keys.
    assert_ne!(entries[0].key, entries[1].key);

    // The mutation reached the apply sink exactly once with the same shape.
    let recorded = harness.sink.mutations.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0], state_mutations[0]);
    // No feed outputs were declared.
    assert!(harness.sink.appends.lock().unwrap().is_empty());
}

#[tokio::test]
async fn window_pipe_skips_unprojectable_records_and_empty_pages_mutate_nothing() {
    let harness = harness_for(window_pipe());
    seed_frontier(&harness.progress, "p0").await;

    // A non-JSON payload cannot project "v"; a null-key record groups
    // nothing. Both contribute zero panes but the frontier still advances.
    let mut bad = json_record(1, b"key-a", 0, 5);
    bad.payload_base64 = b64(b"not json");
    let applied = harness
        .driver
        .drive_once(
            64,
            |_frontier, _limit| async { Ok(page(vec![bad], "p1")) },
            || async { Ok(bootstrap_with("p0")) },
            &harness.sink,
        )
        .await
        .unwrap()
        .expect("applied");
    let EffectTransition::Applied {
        state_mutations, ..
    } = &applied.transition
    else {
        panic!("expected Applied")
    };
    assert!(state_mutations.is_empty());
    assert!(harness.sink.mutations.lock().unwrap().is_empty());
}

#[tokio::test]
async fn window_pane_row_keys_order_by_group_then_pane() {
    // Two sequential pages accumulate separate mutations whose row keys
    // agree on the same (group, pane) composite encoding.
    let harness = harness_for(window_pipe());
    seed_frontier(&harness.progress, "p0").await;
    harness
        .driver
        .drive_once(
            64,
            |_frontier, _limit| async { Ok(page(vec![json_record(1, b"k", 0, 1)], "p1")) },
            || async { Ok(bootstrap_with("p0")) },
            &harness.sink,
        )
        .await
        .unwrap();
    harness
        .driver
        .drive_once(
            64,
            |_frontier, _limit| async { Ok(page(vec![json_record(2, b"k", 20, 2)], "p2")) },
            || async { Ok(bootstrap_with("p0")) },
            &harness.sink,
        )
        .await
        .unwrap();
    let recorded = harness.sink.mutations.lock().unwrap();
    assert_eq!(recorded.len(), 2);
    let StateStoreMutationOp::AccumulateRows { entries: first } = &recorded[0].op else {
        panic!()
    };
    let StateStoreMutationOp::AccumulateRows { entries: second } = &recorded[1].op else {
        panic!()
    };
    // Same group -> shared key prefix; bigger pane -> bigger row key.
    assert!(first[0].key.starts_with(&first[0].key[..4]));
    assert!(first[0].key < second[0].key);
    assert_eq!(
        first[0].key[..first[0].key.len() - 8],
        second[0].key[..second[0].key.len() - 8]
    );
}

fn enrich_pipe() -> PipeDefinition {
    PipeDefinition {
        pipe_id: Uuid::from_u128(0x93),
        name: "orders.enrich".to_owned(),
        space_id: Uuid::from_u128(0xD0),
        subscription_id: Uuid::from_u128(SUBSCRIPTION),
        operation: PipeOperation::Enrich {
            key: IndexFieldSpec::Key,
            freshness: whitewater::control::EnrichFreshness::Latest,
            named: None,
            on_miss: whitewater::control::EnrichMiss::Forward,
            quarantine_feed_id: None,
        },
        output_feed_id: Some(Uuid::from_u128(OUTPUT_FEED)),
        output_store_id: Some(Uuid::from_u128(OUTPUT_STORE)),
        stage: PipeStage::Declared,
        status: ResourceStatus::Active,
        created_at_ns: 0,
    }
}

#[tokio::test]
async fn enrich_pipe_stamps_lookup_outcomes_into_output_metadata() {
    let harness = harness_for(enrich_pipe());
    seed_frontier(&harness.progress, "p0").await;

    let mut hit = record(1);
    hit.resolution = Some(EnrichResolution {
        matched: true,
        payload: b"doc-bytes".to_vec(),
        applied_seq: 41,
    });
    let miss = record(2); // unresolved lookups forward as matched=false
    harness
        .driver
        .drive_once(
            64,
            |_f, _l| async { Ok(page(vec![hit, miss], "p1")) },
            || async { Ok(bootstrap_with("p0")) },
            &harness.sink,
        )
        .await
        .unwrap()
        .expect("applied");

    let appends = harness.sink.appends.lock().unwrap();
    assert_eq!(appends.len(), 2);
    assert_eq!(appends[0].metadata_base64["enrich.matched"], "dHJ1ZQ==");
    assert_eq!(
        appends[0].metadata_base64["enrich.payload"],
        b64(b"doc-bytes")
    );
    // Versioned evidence: the serving replica's applied sequence travels
    // with the output so "which version enriched this record" is answerable.
    assert_eq!(appends[0].metadata_base64["enrich.seq"], b64(b"41"));
    assert_eq!(appends[1].metadata_base64["enrich.matched"], "ZmFsc2U=");
    assert!(!appends[1].metadata_base64.contains_key("enrich.payload"));
    // Input payload and key pass through untouched.
    assert_eq!(appends[0].payload_base64, "cGF5bG9hZA==");
}

#[tokio::test]
async fn enrich_pipe_drop_miss_consumes_without_emitting() {
    let mut pipe = enrich_pipe();
    pipe.operation = PipeOperation::Enrich {
        key: IndexFieldSpec::Key,
        freshness: whitewater::control::EnrichFreshness::Latest,
        named: None,
        on_miss: whitewater::control::EnrichMiss::Drop,
        quarantine_feed_id: None,
    };
    let harness = harness_for(pipe);
    seed_frontier(&harness.progress, "p0").await;

    let mut hit = record(1);
    hit.resolution = Some(EnrichResolution {
        matched: true,
        payload: b"doc-bytes".to_vec(),
        applied_seq: 41,
    });
    let miss = record(2); // unresolved — dropped under the declared policy
    harness
        .driver
        .drive_once(
            64,
            |_f, _l| async { Ok(page(vec![hit, miss], "p1")) },
            || async { Ok(bootstrap_with("p0")) },
            &harness.sink,
        )
        .await
        .unwrap()
        .expect("applied");

    {
        let appends = harness.sink.appends.lock().unwrap();
        // Only the hit emits; the miss consumed with the same frontier.
        assert_eq!(appends.len(), 1);
        assert_eq!(appends[0].metadata_base64["enrich.matched"], "dHJ1ZQ==");
    }
    let frontier = harness
        .progress
        .read_committed()
        .await
        .unwrap()
        .expect("frontier");
    assert_eq!(frontier.cursor, "p1");
}

#[tokio::test]
async fn enrich_pipe_quarantines_misses_to_the_declared_feed() {
    let quarantine = Uuid::from_u128(0x77);
    let mut pipe = enrich_pipe();
    pipe.operation = PipeOperation::Enrich {
        key: IndexFieldSpec::Key,
        freshness: whitewater::control::EnrichFreshness::Latest,
        named: None,
        on_miss: whitewater::control::EnrichMiss::Quarantine,
        quarantine_feed_id: Some(quarantine),
    };
    let harness = harness_for(pipe);
    seed_frontier(&harness.progress, "p0").await;

    let mut hit = record(1);
    hit.resolution = Some(EnrichResolution {
        matched: true,
        payload: b"doc-bytes".to_vec(),
        applied_seq: 41,
    });
    let miss = record(2);
    harness
        .driver
        .drive_once(
            64,
            |_f, _l| async { Ok(page(vec![hit, miss], "p1")) },
            || async { Ok(bootstrap_with("p0")) },
            &harness.sink,
        )
        .await
        .unwrap()
        .expect("applied");

    let appends = harness.sink.appends.lock().unwrap();
    // Hit lands on the output Feed; the miss lands on quarantine with
    // evidence — nothing is lost and nothing contaminates the output.
    assert_eq!(appends.len(), 2);
    assert_eq!(appends[0].feed_id, Uuid::from_u128(OUTPUT_FEED));
    assert_eq!(appends[1].feed_id, quarantine);
    assert_eq!(appends[1].metadata_base64["enrich.matched"], "ZmFsc2U=");
    assert_eq!(appends[1].metadata_base64["enrich.quarantine"], "dHJ1ZQ==");
}

#[tokio::test]
async fn enrich_pipe_adopts_a_committed_divergent_declare() {
    let harness = harness_for(enrich_pipe());
    seed_frontier(&harness.progress, "p0").await;
    // Our drive plans this resolution; the committed row below diverges
    // under the same identity and must win.
    let mut ours = record(1);
    ours.resolution = Some(EnrichResolution {
        matched: true,
        payload: b"fresher-but-lost".to_vec(),
        applied_seq: 42,
    });
    let effect_id = harness
        .driver
        .planned_effect_id(
            Uuid::from_u128(SUBSCRIPTION),
            feed(),
            "p0",
            &page(vec![ours.clone()], "p1"),
        )
        .unwrap();

    // A raced drive resolved the lookup differently and committed its
    // Declare first: the committed row is the single source of truth.
    let committed_output = EffectOutput {
        feed_id: Uuid::from_u128(OUTPUT_FEED),
        key_base64: "a2V5".to_owned(),
        payload_base64: "cGF5bG9hZA==".to_owned(),
        metadata_base64: BTreeMap::from([
            ("enrich.matched".to_owned(), "dHJ1ZQ==".to_owned()),
            ("enrich.payload".to_owned(), b64(b"stale-but-committed")),
        ]),
        event_time_ns: 1,
        writer_session_id: pipe_output_writer_session(enrich_pipe().pipe_id, Uuid::from_u128(2)),
        writer_epoch: 1,
        sequence: 1,
    };
    let committed_declare = EffectMutation {
        effect_id,
        subscription_id: Uuid::from_u128(SUBSCRIPTION),
        ownership_epoch: 7,
        sequence: 1,
        request_id: pipe_declare_request_id(effect_id),
        transition: EffectTransition::Declare {
            consume: Some(EffectConsume {
                feed_id: feed(),
                expected_cursor: Some("p0".to_owned()),
                cursor: "p1".to_owned(),
                positions: BTreeMap::from([(range(), "p1".to_owned())]),
            }),
            outputs: vec![committed_output],
            state_mutations: Vec::new(),
        },
    };
    harness.journal.apply(committed_declare).await.unwrap();

    // Our drive plans a divergent resolution — it must adopt the committed
    // Declare rather than conflict or double-decide.
    let applied = harness
        .driver
        .drive_once(
            64,
            |_f, _l| async { Ok(page(vec![ours], "p1")) },
            || async { Ok(bootstrap_with("p0")) },
            &harness.sink,
        )
        .await
        .unwrap()
        .expect("applied");

    let EffectTransition::Applied { outputs, .. } = &applied.transition else {
        panic!("expected Applied")
    };
    assert_eq!(outputs.len(), 1);
    assert_eq!(
        outputs[0].metadata_base64["enrich.payload"],
        b64(b"stale-but-committed")
    );
    let appends = harness.sink.appends.lock().unwrap();
    assert_eq!(appends.len(), 1);
    assert_eq!(
        appends[0].metadata_base64["enrich.payload"],
        b64(b"stale-but-committed")
    );
}
fn emit_window_pipe() -> PipeDefinition {
    let mut pipe = window_pipe();
    pipe.operation = PipeOperation::Window {
        window_ns: 160,
        key: IndexFieldSpec::Key,
        aggregation: WindowAggregation::Sum {
            value: IndexFieldSpec::Projection {
                extractor: "json".to_owned(),
                version: 1,
                field: "v".to_owned(),
            },
        },
        time: whitewater::control::WindowTime::Event,
        max_late_ns: Some(20),
        expire_ns: None,
        key_limit: None,
        byte_limit: None,
    };
    pipe.output_feed_id = Some(Uuid::from_u128(OUTPUT_FEED));
    pipe
}

/// A window Pipe that retains pane rows only up to its declared
/// `EXPIRE` bound and never emits to a Feed.
fn expire_window_pipe() -> PipeDefinition {
    let mut pipe = window_pipe();
    pipe.operation = PipeOperation::Window {
        window_ns: 160,
        key: IndexFieldSpec::Key,
        aggregation: WindowAggregation::Count,
        time: whitewater::control::WindowTime::Event,
        max_late_ns: None,
        expire_ns: Some(30),
        key_limit: None,
        byte_limit: None,
    };
    pipe
}

fn pane_emission(group: IndexValue, pane: u64, count: u64, sum: i128) -> WindowEmission {
    let tagged = whitewater::pipe::window_group_key_bytes(&group).unwrap();
    WindowEmission {
        row_key: window_pane_row_key(&tagged, pane),
        group,
        group_tagged: tagged,
        pane,
        count,
        sum,
        store_seq: 7,
    }
}

#[tokio::test]
async fn window_pipe_emits_a_closed_pane_and_deletes_its_row_atomically() {
    let harness = harness_for(emit_window_pipe());
    seed_frontier(&harness.progress, "p0").await;

    let emission = pane_emission(IndexValue::Bytes(b"key-a".to_vec()), 3, 4, 30);
    let applied = harness
        .driver
        .drive_once(
            64,
            |_frontier, _limit| async {
                Ok(page_with_emissions(
                    vec![json_record(1, b"key-a", 0, 5)],
                    vec![emission.clone()],
                    "p1",
                ))
            },
            || async { Ok(bootstrap_with("p0")) },
            &harness.sink,
        )
        .await
        .unwrap()
        .expect("applied");

    let EffectTransition::Applied {
        outputs,
        state_mutations,
        ..
    } = &applied.transition
    else {
        panic!("expected Applied")
    };
    // One emitted output + [accumulate, delete] inside one boundary.
    assert_eq!(outputs.len(), 1);
    assert_eq!(state_mutations.len(), 2);
    let output = &outputs[0];
    assert_eq!(output.feed_id, Uuid::from_u128(OUTPUT_FEED));
    // The emitted record keys on the tag-prefixed group bytes.
    assert_eq!(
        output.key_base64,
        b64(
            &whitewater::pipe::window_group_key_bytes(&IndexValue::Bytes(b"key-a".to_vec()))
                .unwrap()
        )
    );
    let payload: serde_json::Value = serde_json::from_slice(
        &base64::Engine::decode(
            &base64::engine::general_purpose::STANDARD,
            &output.payload_base64,
        )
        .unwrap(),
    )
    .unwrap();
    // pane 3 under a 10ns pane span (window 160ns / 16 panes).
    // Bytes groups encode base64 (b"key-a" -> a2V5LWE=).
    assert_eq!(payload["group"], "a2V5LWE=");
    assert_eq!(payload["pane"], 3);
    assert_eq!(payload["pane_start_ns"], "30");
    assert_eq!(payload["pane_end_ns"], "40");
    assert_eq!(payload["count"], 4);
    assert_eq!(payload["sum"], "30");
    assert_eq!(
        output
            .metadata_base64
            .get("window.emitted")
            .map(String::as_str),
        Some("dHJ1ZQ==")
    );
    // Emission dedupe identity is the pane row itself.
    assert_eq!(
        output.writer_session_id,
        pipe_emission_writer_session(emit_window_pipe().pipe_id, &emission.row_key)
    );
    assert_eq!(output.sequence, 1);
    // pane_end - 1 is the output's event-time edge.
    assert_eq!(output.event_time_ns, 39);
    // The pane row deletes inside the same effect.
    let StateStoreMutationOp::DeleteRow { key } = &state_mutations[1].op else {
        panic!("expected DeleteRow")
    };
    assert_eq!(*key, emission.row_key);
    assert_eq!(
        state_mutations[1].mutation_id,
        whitewater::effect::effect_state_mutation_id(applied.effect_id, 1)
    );
    let StateStoreMutationOp::AccumulateRows { .. } = &state_mutations[0].op else {
        panic!("expected AccumulateRows first")
    };
    // The append sink saw the emitted record.
    assert_eq!(harness.sink.appends.lock().unwrap().len(), 1);
    // Both mutations applied post-release.
    assert_eq!(harness.sink.mutations.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn idle_pages_close_panes_with_an_emission_only_effect() {
    let harness = harness_for(emit_window_pipe());
    seed_frontier(&harness.progress, "p0").await;
    let emission = pane_emission(IndexValue::Bytes(b"key-a".to_vec()), 3, 4, 30);

    // Nothing fetched and one closed pane: the drive applies an effect
    // over a no-op frontier move, keyed on the emission set itself so a
    // partially drained scan can never collide with this one.
    let applied = harness
        .driver
        .drive_once(
            64,
            |_frontier, _limit| async {
                Ok(page_with_emissions(vec![], vec![emission.clone()], "p0"))
            },
            || async { Ok(bootstrap_with("p0")) },
            &harness.sink,
        )
        .await
        .unwrap()
        .expect("applied");
    assert_eq!(
        applied.effect_id,
        whitewater::pipe::pipe_emission_effect_id(
            emit_window_pipe().pipe_id,
            Uuid::from_u128(SUBSCRIPTION),
            feed(),
            "p0",
            std::slice::from_ref(&emission),
            &[],
        )
    );
    let EffectTransition::Applied {
        outputs,
        state_mutations,
        ..
    } = &applied.transition
    else {
        panic!("expected Applied")
    };
    // One emitted output + one DeleteRow; no accumulate on an idle page.
    assert_eq!(outputs.len(), 1);
    assert_eq!(state_mutations.len(), 1);
    assert_eq!(harness.sink.appends.lock().unwrap().len(), 1);
    assert_eq!(harness.sink.mutations.lock().unwrap().len(), 1);
    // The frontier never moved.
    let committed = harness.progress.read_committed().await.unwrap().unwrap();
    assert_eq!(committed.cursor, "p0");

    // Replaying the identical scan replays the committed effect: no new
    // append, no new mutation.
    let replayed = harness
        .driver
        .drive_once(
            64,
            |_frontier, _limit| async {
                Ok(page_with_emissions(vec![], vec![emission.clone()], "p0"))
            },
            || async { Ok(bootstrap_with("p0")) },
            &harness.sink,
        )
        .await
        .unwrap()
        .expect("applied");
    assert_eq!(replayed.effect_id, applied.effect_id);
    assert_eq!(harness.sink.appends.lock().unwrap().len(), 1);
    // The replay re-drives an identical mutation — the store journal
    // dedupes on mutation_id, so the second call is a no-op there.
    {
        let mutations = harness.sink.mutations.lock().unwrap();
        assert_eq!(mutations.len(), 2);
        assert_eq!(mutations[1], mutations[0]);
    }

    // A different closed set over the same frontier is a distinct effect.
    let other = pane_emission(IndexValue::Bytes(b"key-a".to_vec()), 5, 2, 7);
    let second = harness
        .driver
        .drive_once(
            64,
            |_frontier, _limit| async {
                Ok(page_with_emissions(vec![], vec![other.clone()], "p0"))
            },
            || async { Ok(bootstrap_with("p0")) },
            &harness.sink,
        )
        .await
        .unwrap()
        .expect("applied");
    assert_ne!(second.effect_id, applied.effect_id);

    // Truly idle pages still take the fast path with no effect at all.
    assert!(harness
        .driver
        .drive_once(
            64,
            |_frontier, _limit| async { Ok(page(vec![], "p0")) },
            || async { Ok(bootstrap_with("p0")) },
            &harness.sink,
        )
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn window_pipe_expires_pane_rows_without_emitting() {
    let harness = harness_for(expire_window_pipe());
    seed_frontier(&harness.progress, "p0").await;
    let tagged =
        whitewater::pipe::window_group_key_bytes(&IndexValue::Bytes(b"key-a".to_vec())).unwrap();
    let expired_key = window_pane_row_key(&tagged, 2);

    let mut page = page(vec![json_record(1, b"key-a", 0, 5)], "p1");
    page.expired = vec![expired_key.clone()];
    let applied = harness
        .driver
        .drive_once(
            64,
            |_frontier, _limit| {
                let page = page.clone();
                async move { Ok(page) }
            },
            || async { Ok(bootstrap_with("p0")) },
            &harness.sink,
        )
        .await
        .unwrap()
        .expect("applied");
    let EffectTransition::Applied {
        outputs,
        state_mutations,
        ..
    } = &applied.transition
    else {
        panic!("expected Applied")
    };
    // No emit Feed: expiry produces no output, just the delete.
    assert!(outputs.is_empty());
    assert_eq!(state_mutations.len(), 2);
    let StateStoreMutationOp::DeleteRow { key } = &state_mutations[1].op else {
        panic!("expected DeleteRow")
    };
    assert_eq!(*key, expired_key);
    assert_eq!(harness.sink.mutations.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn expired_panes_delete_on_an_idle_page() {
    let harness = harness_for(expire_window_pipe());
    seed_frontier(&harness.progress, "p0").await;
    let tagged =
        whitewater::pipe::window_group_key_bytes(&IndexValue::Bytes(b"key-a".to_vec())).unwrap();
    let expired_key = window_pane_row_key(&tagged, 2);

    let mut page = page(vec![], "p0");
    page.expired = vec![expired_key.clone()];
    let applied = harness
        .driver
        .drive_once(
            64,
            |_frontier, _limit| {
                let page = page.clone();
                async move { Ok(page) }
            },
            || async { Ok(bootstrap_with("p0")) },
            &harness.sink,
        )
        .await
        .unwrap()
        .expect("applied");
    // Expiry rides the emission-effect identity keyed on the delete set.
    assert_eq!(
        applied.effect_id,
        whitewater::pipe::pipe_emission_effect_id(
            expire_window_pipe().pipe_id,
            Uuid::from_u128(SUBSCRIPTION),
            feed(),
            "p0",
            &[],
            std::slice::from_ref(&expired_key),
        )
    );
    let EffectTransition::Applied {
        outputs,
        state_mutations,
        ..
    } = &applied.transition
    else {
        panic!("expected Applied")
    };
    assert!(outputs.is_empty());
    // Delete-only: no accumulate mutation on an empty page.
    assert_eq!(state_mutations.len(), 1);
    let StateStoreMutationOp::DeleteRow { key } = &state_mutations[0].op else {
        panic!("expected DeleteRow")
    };
    assert_eq!(*key, expired_key);
    let committed = harness.progress.read_committed().await.unwrap().unwrap();
    assert_eq!(committed.cursor, "p0");
}

#[tokio::test]
async fn emitted_panes_fill_the_delete_budget_before_expired_ones() {
    let harness = harness_for(emit_window_pipe());
    seed_frontier(&harness.progress, "p0").await;
    let emissions: Vec<WindowEmission> = (0..15)
        .map(|pane| pane_emission(IndexValue::Bytes(b"key-a".to_vec()), pane, 1, 1))
        .collect();
    let tagged =
        whitewater::pipe::window_group_key_bytes(&IndexValue::Bytes(b"key-b".to_vec())).unwrap();
    let expired: Vec<Vec<u8>> = (20..23)
        .map(|pane| window_pane_row_key(&tagged, pane))
        .collect();

    let mut page = page_with_emissions(vec![json_record(1, b"key-a", 0, 5)], emissions, "p1");
    page.expired = expired.clone();
    let applied = harness
        .driver
        .drive_once(
            64,
            |_frontier, _limit| {
                let page = page.clone();
                async move { Ok(page) }
            },
            || async { Ok(bootstrap_with("p0")) },
            &harness.sink,
        )
        .await
        .unwrap()
        .expect("applied");
    let EffectTransition::Applied {
        outputs,
        state_mutations,
        ..
    } = &applied.transition
    else {
        panic!("expected Applied")
    };
    assert_eq!(outputs.len(), 15);
    // 1 accumulate + 15 deletes exhaust the 16-mutation budget; the
    // expired rows wait for a later drive rather than escape the bound.
    assert_eq!(state_mutations.len(), 16);
    let deleted: Vec<&Vec<u8>> = state_mutations[1..]
        .iter()
        .filter_map(|mutation| match &mutation.op {
            StateStoreMutationOp::DeleteRow { key } => Some(key),
            _ => None,
        })
        .collect();
    assert_eq!(deleted.len(), 15);
    for key in &expired {
        assert!(!deleted.contains(&key));
    }
}

#[tokio::test]
async fn window_emissions_cap_at_the_mutation_budget_per_drive() {
    let harness = harness_for(emit_window_pipe());
    seed_frontier(&harness.progress, "p0").await;
    let emissions = (0..20)
        .map(|pane| pane_emission(IndexValue::Bytes(b"key-a".to_vec()), pane, 1, 1))
        .collect();
    let applied = harness
        .driver
        .drive_once(
            64,
            |_frontier, _limit| async {
                Ok(page_with_emissions(
                    vec![json_record(1, b"key-a", 0, 5)],
                    emissions,
                    "p1",
                ))
            },
            || async { Ok(bootstrap_with("p0")) },
            &harness.sink,
        )
        .await
        .unwrap()
        .expect("applied");
    let EffectTransition::Applied {
        outputs,
        state_mutations,
        ..
    } = &applied.transition
    else {
        panic!("expected Applied")
    };
    // 15 emissions fit the 16-mutation budget alongside the accumulate.
    assert_eq!(outputs.len(), 15);
    assert_eq!(state_mutations.len(), 16);
    // Row-key order is deterministic regardless of input order.
    let mut sorted: Vec<u64> = outputs
        .iter()
        .map(|output| {
            let payload: serde_json::Value = serde_json::from_slice(
                &base64::Engine::decode(
                    &base64::engine::general_purpose::STANDARD,
                    &output.payload_base64,
                )
                .unwrap(),
            )
            .unwrap();
            payload["pane"].as_u64().unwrap()
        })
        .collect();
    let observed = sorted.clone();
    sorted.sort_unstable();
    assert_eq!(observed, sorted);
}

#[tokio::test]
async fn window_emissions_without_an_emit_feed_fail_closed() {
    // A page carrying emissions under a pipe that never declared EMIT TO
    // FEED refuses the whole drive — silently dropping closed panes
    // would wedge the store.
    let harness = harness_for(window_pipe());
    seed_frontier(&harness.progress, "p0").await;
    let result = harness
        .driver
        .drive_once(
            64,
            |_frontier, _limit| async {
                Ok(page_with_emissions(
                    vec![json_record(1, b"key-a", 0, 5)],
                    vec![pane_emission(IndexValue::Bytes(b"key-a".to_vec()), 1, 2, 8)],
                    "p1",
                ))
            },
            || async { Ok(bootstrap_with("p0")) },
            &harness.sink,
        )
        .await;
    assert!(matches!(result, Err(EffectJournalError::Conflict)));
    assert!(harness.sink.appends.lock().unwrap().is_empty());
    assert!(harness.sink.mutations.lock().unwrap().is_empty());
}

#[tokio::test]
async fn window_limit_keys_quarantines_refused_groups_into_a_shared_sentinel() {
    let harness = harness_for(window_pipe());
    seed_frontier(&harness.progress, "p0").await;

    // Fetch-time resolution refused new groups "b"/"c"; "a" admitted.
    let tagged = |bytes: &[u8]| window_group_key_bytes(&IndexValue::Bytes(bytes.to_vec())).unwrap();
    let records = vec![
        json_record(1, b"a", 10, 5),
        json_record(2, b"b", 10, 7),
        json_record(3, b"c", 20, 9),
        json_record(4, b"b", 30, 11),
    ];
    let refused = vec![tagged(b"b"), tagged(b"c")];
    let applied = harness
        .driver
        .drive_once(
            64,
            |_frontier, _limit| async {
                Ok(FetchedPage {
                    records,
                    cursor: "p1".to_owned(),
                    positions: BTreeMap::from([(range(), "p1".to_owned())]),
                    member_feed: None,
                    emissions: Vec::new(),
                    expired: Vec::new(),
                    refused,
                    pane_rows: Default::default(),
                    pane_rows_saturated: false,
                })
            },
            || async { Ok(bootstrap_with("p0")) },
            &harness.sink,
        )
        .await
        .unwrap()
        .expect("applied");

    let EffectTransition::Applied {
        state_mutations, ..
    } = &applied.transition
    else {
        panic!("expected Applied")
    };
    let StateStoreMutationOp::AccumulateRows { entries } = &state_mutations[0].op else {
        panic!("expected AccumulateRows")
    };
    // One real pane row for "a" plus the single shared refusal row —
    // refused groups never materialize per-group state.
    assert_eq!(entries.len(), 2);
    assert_eq!(
        entries[0].named[0].values,
        vec![IndexValue::Bytes(b"a".to_vec())]
    );
    assert_eq!(entries[0].named[1].values, vec![IndexValue::I64(1)]);
    // Shared sentinel: `__keycap__` carries the count of every refused
    // record regardless of which group it arrived under.
    assert_eq!(
        entries[1].named[0].values,
        vec![IndexValue::Text("__keycap__".to_owned())]
    );
    assert_eq!(entries[1].named[1].values, vec![IndexValue::I64(-1)]);
    assert_eq!(entries[1].count_delta, 3);
    assert_eq!(entries[1].sum_delta, 0);
    assert_eq!(
        entries[1].key[entries[1].key.len() - 8..],
        u64::MAX.to_be_bytes()
    );
}

#[test]
fn window_refused_groups_admits_seats_in_tagged_order() {
    let key = IndexFieldSpec::Key;
    let records = vec![
        json_record(1, b"zeta", 10, 1),
        json_record(2, b"alpha", 10, 1),
        json_record(3, b"mid", 10, 1),
        // A repeated key is one group, not four seats.
        json_record(4, b"alpha", 20, 1),
    ];
    let tagged = |bytes: &[u8]| window_group_key_bytes(&IndexValue::Bytes(bytes.to_vec())).unwrap();

    // One seat left after the existing key → the smallest tagged new key
    // (byte order, not arrival order) admits; the rest refuse.
    let active: BTreeSet<Vec<u8>> = [tagged(b"existing")].into_iter().collect();
    let refused = window_refused_groups(&records, &key, 2, &active, false);
    assert_eq!(refused, vec![tagged(b"mid"), tagged(b"zeta")]);

    // Capacity for every incoming group → nothing refuses.
    let refused = window_refused_groups(&records, &key, 10, &BTreeSet::new(), false);
    assert!(refused.is_empty());

    // A saturated scan provably exceeds the bound → every new key refuses.
    let refused = window_refused_groups(&records, &key, 10, &BTreeSet::new(), true);
    assert_eq!(refused.len(), 3);

    // Already-active keys never refuse even at capacity; their records
    // keep accumulating on the rows they already own.
    let active: BTreeSet<Vec<u8>> = [tagged(b"alpha")].into_iter().collect();
    let refused = window_refused_groups(&records, &key, 1, &active, false);
    assert_eq!(refused, vec![tagged(b"mid"), tagged(b"zeta")]);
}

#[tokio::test]
async fn window_limit_bytes_refuses_new_pane_rows_into_bytecap() {
    let mut pipe = window_pipe();
    let PipeOperation::Window { byte_limit, .. } = &mut pipe.operation else {
        panic!("window pipe")
    };
    // 120 bytes: one existing 38-byte row leaves 82 — two further rows
    // (38 each) admit, the third would exceed and refuses.
    *byte_limit = Some(120);
    let harness = harness_for(pipe);
    seed_frontier(&harness.progress, "p0").await;

    let tagged = |bytes: &[u8]| window_group_key_bytes(&IndexValue::Bytes(bytes.to_vec())).unwrap();
    // The fetch-time scan proved group "a" already owns pane 0.
    let existing: BTreeSet<(Vec<u8>, u64)> = [(tagged(b"a"), 0u64)].into_iter().collect();
    let records = vec![
        json_record(1, b"b", 5, 7),   // new pane (b,0): admits
        json_record(2, b"c", 15, 9),  // new pane (c,1): admits, budget spent
        json_record(3, b"d", 5, 11),  // new pane (d,0): refuses
        json_record(4, b"b", 25, 13), // new pane (b,2): refuses
        json_record(5, b"b", 7, 13),  // merges into admitted (b,0)
    ];
    let applied = harness
        .driver
        .drive_once(
            64,
            |_frontier, _limit| async {
                Ok(FetchedPage {
                    records,
                    cursor: "p1".to_owned(),
                    positions: BTreeMap::from([(range(), "p1".to_owned())]),
                    member_feed: None,
                    emissions: Vec::new(),
                    expired: Vec::new(),
                    refused: Vec::new(),
                    pane_rows: existing,
                    pane_rows_saturated: false,
                })
            },
            || async { Ok(bootstrap_with("p0")) },
            &harness.sink,
        )
        .await
        .unwrap()
        .expect("applied");

    let EffectTransition::Applied {
        state_mutations, ..
    } = &applied.transition
    else {
        panic!("expected Applied")
    };
    let StateStoreMutationOp::AccumulateRows { entries } = &state_mutations[0].op else {
        panic!("expected AccumulateRows")
    };
    // (b,0) and (c,1) admitted; (d,0) and (b,2) refused into the shared
    // `__bytecap__` sentinel; (a,0) untouched — rows never revise on merge.
    assert_eq!(entries.len(), 3);
    assert_eq!(
        entries[0].named[0].values,
        vec![IndexValue::Bytes(b"b".to_vec())]
    );
    assert_eq!(entries[0].named[1].values, vec![IndexValue::I64(0)]);
    assert_eq!(entries[0].count_delta, 2);
    assert_eq!(entries[0].sum_delta, 20);
    assert_eq!(
        entries[1].named[0].values,
        vec![IndexValue::Bytes(b"c".to_vec())]
    );
    assert_eq!(entries[1].named[1].values, vec![IndexValue::I64(1)]);
    assert_eq!(
        entries[2].named[0].values,
        vec![IndexValue::Text("__bytecap__".to_owned())]
    );
    assert_eq!(entries[2].named[1].values, vec![IndexValue::I64(-1)]);
    assert_eq!(entries[2].count_delta, 2);
    assert_eq!(entries[2].sum_delta, 0);
    assert_eq!(
        entries[2].key[entries[2].key.len() - 8..],
        u64::MAX.to_be_bytes()
    );
}

#[tokio::test]
async fn window_limit_bytes_saturated_scan_refuses_every_new_row() {
    let mut pipe = window_pipe();
    let PipeOperation::Window { byte_limit, .. } = &mut pipe.operation else {
        panic!("window pipe")
    };
    *byte_limit = Some(200_000);
    let harness = harness_for(pipe);
    seed_frontier(&harness.progress, "p0").await;

    let tagged = |bytes: &[u8]| window_group_key_bytes(&IndexValue::Bytes(bytes.to_vec())).unwrap();
    let existing: BTreeSet<(Vec<u8>, u64)> = [(tagged(b"a"), 0u64)].into_iter().collect();
    let records = vec![
        json_record(1, b"a", 5, 7), // merges into scanned (a,0): admits
        json_record(2, b"b", 5, 9), // unproven budget: refuses
    ];
    let applied = harness
        .driver
        .drive_once(
            64,
            |_frontier, _limit| async {
                Ok(FetchedPage {
                    records,
                    cursor: "p1".to_owned(),
                    positions: BTreeMap::from([(range(), "p1".to_owned())]),
                    member_feed: None,
                    emissions: Vec::new(),
                    expired: Vec::new(),
                    refused: Vec::new(),
                    pane_rows: existing,
                    pane_rows_saturated: true,
                })
            },
            || async { Ok(bootstrap_with("p0")) },
            &harness.sink,
        )
        .await
        .unwrap()
        .expect("applied");

    let EffectTransition::Applied {
        state_mutations, ..
    } = &applied.transition
    else {
        panic!("expected Applied")
    };
    let StateStoreMutationOp::AccumulateRows { entries } = &state_mutations[0].op else {
        panic!("expected AccumulateRows")
    };
    assert_eq!(entries.len(), 2);
    assert_eq!(
        entries[0].named[0].values,
        vec![IndexValue::Bytes(b"a".to_vec())]
    );
    assert_eq!(entries[0].count_delta, 1);
    assert_eq!(
        entries[1].named[0].values,
        vec![IndexValue::Text("__bytecap__".to_owned())]
    );
    assert_eq!(entries[1].count_delta, 1);
}

#[test]
fn window_scan_row_limit_tracks_the_tighter_declared_bound() {
    use whitewater::pipe::{
        window_pane_row_bytes, window_scan_row_limit, WINDOW_KEY_ROW_FOOTPRINT,
    };
    assert_eq!(window_pane_row_bytes(b"ab"), 38);
    // Key bound: 7 Keys * 17-row footprint + sentinel/probe headroom.
    assert_eq!(
        window_scan_row_limit(Some(7), None),
        usize::try_from(7 * WINDOW_KEY_ROW_FOOTPRINT + 3).unwrap()
    );
    // Byte bound: floor(limit / min-row) + headroom — tighter here.
    assert_eq!(window_scan_row_limit(Some(7), Some(4096)), 4096 / 36 + 3);
    // The largest declared byte bound stays inside the 8192-row scan.
    assert!(window_scan_row_limit(None, Some(whitewater::pipe::WINDOW_MAX_BYTE_LIMIT)) <= 8192);
}
