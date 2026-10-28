//! Controlled shadow-generation Index rebuilds.
//!
//! A declared [`crate::control::StoredIndexDefinition`] carries a
//! [`IndexBuildStage`]. `REBUILD INDEX` (or `REBUILD INDEXES IN DOMAIN`)
//! moves it to `Building { generation }`; every Data Node then
//! materializes that generation's postings under a derived
//! [`shadow_index_id`] while the previously activated generation — if any —
//! keeps serving. Once a Node's pass consumes the source Feed's committed
//! frontier it commits `CompleteIndexBuild` through the Control Plane,
//! which atomically flips the stage to `Active`. Completing Nodes race the
//! same commit; losers observe the generation is no longer building and
//! treat their pass as superseded.
//!
//! Refusal is explicit and safe: a missing source Feed or an unresolvable
//! resume Cursor yields [`IndexBuildError`] and the definition stays in
//! `Building` until history returns or an operator intervenes — a shadow
//! is never activated off partial history.

use std::sync::Arc;

use thiserror::Error;
use uuid::Uuid;

use crate::control::{
    Command, ControlController, ControlError, IndexBuildStage, IndexFieldSpec,
    StoredIndexDefinition,
};
use crate::index::{
    shadow_index_id, IndexEntry, IndexId, IndexStore, IndexStoreError, IndexValue, PrimaryRef,
};
use crate::state_store::{FeedReplaySource, ReplaySourceRecord, StateStoreReplayError};

/// Maximum records one build pass applies before yielding; keeps every
/// pass bounded so a busy Node never monopolizes an executor worker on a
/// single rebuild.
pub const MAX_BUILD_PASS_RECORDS: usize = 512;
/// Maximum posting removals one retire sweep performs per call.
const RETIRE_PAGE: usize = 512;
/// Upper bound on postings retired per supervisor tick, so a huge old
/// generation defers the remainder to later ticks rather than stalling the
/// driver.
const RETIRE_BUDGET_PER_TICK: usize = 4_096;

#[derive(Debug, Error)]
pub enum IndexBuildError {
    /// The declared source Feed no longer resolves to committed history.
    #[error("the declared source Feed no longer resolves; its history is gone")]
    SourceUnavailable,
    /// The shadow's resume Cursor fell outside retained history; the
    /// rebuild refuses rather than activate a silently partial shadow.
    #[error(
        "the shadow generation's resume Cursor no longer resolves in retained history; \
         the rebuild cannot continue from where it left off"
    )]
    FrontierOutOfRange,
    /// The committed history could not be read.
    #[error("source Feed read failed: {0}")]
    SourceRead(String),
    /// A field extraction failed on record content.
    #[error("Index field extraction failed: {0}")]
    Extract(String),
    /// The Index engine rejected a shadow write or sweep.
    #[error("Index engine error: {0}")]
    Engine(#[from] IndexStoreError),
    /// The Control Plane refused a build state transition.
    #[error("Control Plane error: {0}")]
    Control(#[from] ControlError),
}

impl From<StateStoreReplayError> for IndexBuildError {
    fn from(error: StateStoreReplayError) -> Self {
        match error {
            StateStoreReplayError::SourceUnavailable => Self::SourceUnavailable,
            StateStoreReplayError::FrontierOutOfRange => Self::FrontierOutOfRange,
            StateStoreReplayError::SourceRead(message) => Self::SourceRead(message),
            other => Self::SourceRead(other.to_string()),
        }
    }
}

/// The outcome of one bounded build pass over one Index definition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexBuildPass {
    pub index_id: Uuid,
    /// The shadow generation this pass materialized.
    pub generation: u64,
    /// Records applied to the shadow this pass.
    pub applied: usize,
    /// The shadow's covered Cursor after this pass.
    pub frontier: Option<String>,
    /// The pass consumed the committed frontier; activation was attempted.
    pub complete: bool,
    /// `CompleteIndexBuild` committed — this generation is serving.
    pub activated: bool,
    /// Another completer activated the generation first; this pass's
    /// outcome is idempotently superseded.
    pub superseded: bool,
}

/// Drives every `Building` Index definition visible in the catalog one
/// bounded pass at a time. Deterministic: every Node builds the same
/// postings from the same committed history, so per-Node local builds
/// converge without a placement protocol for this first cut.
pub struct IndexBuildDriver {
    control: Arc<ControlController>,
    source: Arc<dyn FeedReplaySource>,
    index: Arc<dyn IndexStore>,
    /// Serializes passes so the periodic driver and the commit-boundary
    /// trigger never interleave two applies of the same generation.
    pass_lock: tokio::sync::Mutex<()>,
}

impl IndexBuildDriver {
    pub fn new(
        control: Arc<ControlController>,
        source: Arc<dyn FeedReplaySource>,
        index: Arc<dyn IndexStore>,
    ) -> Self {
        Self {
            control,
            source,
            index,
            pass_lock: tokio::sync::Mutex::new(()),
        }
    }

    /// One bounded pass over every definition that needs local work:
    /// `Building` generations materialize and try to activate; `Active`
    /// generations keep applying until the local shadow reaches the
    /// activated frontier — that is how every Data Node, including one
    /// that joins after activation, materializes the serving generation.
    /// Failures are returned per definition so one refused build never
    /// stalls another.
    pub async fn run(&self, limit: usize) -> Vec<(Uuid, Result<IndexBuildPass, IndexBuildError>)> {
        let definitions = self.control.index_definitions().await;
        let mut outcomes = Vec::new();
        for definition in definitions {
            match definition.stage {
                IndexBuildStage::Building { .. } => {}
                IndexBuildStage::Active { generation } => {
                    // Skip Nodes whose serving shadow has already reached
                    // the live committed frontier. The catalog's
                    // activation watermark is a floor, not the target:
                    // commit-boundary apply keeps pushing past it, and a
                    // restart can drop an in-flight apply kick, so the
                    // periodic pass probes for one committed record beyond
                    // the applied frontier instead of trusting the frozen
                    // cursor.
                    let shadow = shadow_index_id(definition.index_id, generation);
                    let applied = self.index.last_applied(shadow).unwrap_or_default();
                    let pending = match self
                        .source
                        .read_committed_records(definition.feed_id, applied.as_deref(), 1)
                        .await
                    {
                        Ok(records) => !records.is_empty(),
                        Err(_) => true,
                    };
                    if !pending {
                        continue;
                    }
                }
                IndexBuildStage::Declared => continue,
            }
            let result = self.build_pass(&definition, limit).await;
            outcomes.push((definition.index_id, result));
        }
        outcomes
    }

    /// Commit-boundary apply for one Feed: every `Active` generation
    /// targeting `feed_id` takes one bounded pass toward the committed
    /// frontier. The mutations derive deterministically from the committed
    /// frames themselves, so every replica running this at its own commit
    /// boundary converges on identical postings — the committed log is the
    /// replicated intent carrier, and `last_applied` keeps retries
    /// idempotent. Lag or refusal reports per definition; nothing here may
    /// roll back a commit that already landed.
    pub async fn apply_committed(
        &self,
        feed_id: Uuid,
        limit: usize,
    ) -> Vec<(Uuid, Result<IndexBuildPass, IndexBuildError>)> {
        let mut outcomes = Vec::new();
        for definition in self.control.index_definitions().await {
            // Unlike the periodic driver's activation-frontier skip, the
            // commit path must carry a serving generation past its
            // activation watermark: the index tracks the live committed
            // frontier, not just the history it was built over.
            if definition.feed_id != feed_id {
                continue;
            }
            if !matches!(definition.stage, IndexBuildStage::Active { .. }) {
                continue;
            }
            let result = self.build_pass(&definition, limit).await;
            outcomes.push((definition.index_id, result));
        }
        outcomes
    }

    /// Apply up to `limit` committed records to `definition`'s current
    /// generation (building or activated). A `Building` pass that consumed
    /// the committed frontier attempts atomic activation; an `Active` pass
    /// only materializes the local shadow toward the committed frontier.
    pub async fn build_pass(
        &self,
        definition: &StoredIndexDefinition,
        limit: usize,
    ) -> Result<IndexBuildPass, IndexBuildError> {
        let _pass = self.pass_lock.lock().await;
        let (generation, building) = match definition.stage {
            IndexBuildStage::Building { generation } => (generation, true),
            IndexBuildStage::Active { generation } => (generation, false),
            IndexBuildStage::Declared => {
                return Err(IndexBuildError::Control(ControlError::InvalidOperation(
                    format!("Index {} is not building", definition.name),
                )));
            }
        };
        let shadow = shadow_index_id(definition.index_id, generation);
        if !self.source.source_exists(definition.feed_id).await {
            return Err(IndexBuildError::SourceUnavailable);
        }
        let mut frontier = self.index.last_applied(shadow)?;
        let records = self
            .source
            .read_committed_records(
                definition.feed_id,
                frontier.as_deref(),
                limit.clamp(1, MAX_BUILD_PASS_RECORDS),
            )
            .await?;
        let mut applied = 0usize;
        for record in &records {
            self.apply_record(definition, shadow, record)?;
            frontier = Some(record.cursor.clone());
            applied += 1;
        }
        let complete = records.len() < limit.clamp(1, MAX_BUILD_PASS_RECORDS)
            && (building || frontier.as_deref() == definition.applied_cursor.as_deref());
        let mut pass = IndexBuildPass {
            index_id: definition.index_id,
            generation,
            applied,
            frontier: frontier.clone(),
            complete,
            activated: false,
            superseded: false,
        };
        if complete && building {
            match self.activate(definition, generation, &frontier).await {
                Ok(()) => {
                    pass.activated = true;
                    self.retire_prior_generations(definition, generation).await;
                }
                Err(IndexBuildError::Control(ControlError::InvalidOperation(_))) => {
                    // Another Node completed first, or this Node holds a
                    // synced catalog replica that cannot commit mutations
                    // — either way the activation belongs elsewhere and
                    // this pass's outcome is idempotently superseded.
                    pass.superseded = true;
                }
                Err(error) => return Err(error),
            }
        }
        Ok(pass)
    }

    /// Write one record's projection into the shadow generation, merging
    /// with the row's other generations/Indexes so sibling entries survive.
    fn apply_record(
        &self,
        definition: &StoredIndexDefinition,
        shadow: IndexId,
        record: &ReplaySourceRecord,
    ) -> Result<(), IndexBuildError> {
        let values = extract_index_values(&definition.fields, record)?;
        let reference = PrimaryRef {
            feed_id: definition.feed_id,
            application_key: record.key.clone(),
        };
        let existing = self.index.get(&reference)?;
        let mut entries = existing
            .as_ref()
            .map(|row| row.entries.clone())
            .unwrap_or_default();
        entries.retain(|entry| entry.id != shadow);
        // Unique claims arbitrate by committed order: the earliest record
        // to claim a value wins identically on every replica because all
        // replicas replay the same merged committed history. A later
        // record keeps its row and other postings but never resolves
        // through this Index — `conflicted` records the loss honestly.
        let mut conflicted = Vec::new();
        if definition.unique && !values.contains(&IndexValue::Null) {
            match self.index.claim_owner(shadow, &values)? {
                Some(owner)
                    if owner
                        != crate::index::primary_key(&reference)
                            .map_err(IndexStoreError::from)? =>
                {
                    conflicted.push(shadow);
                }
                _ => entries.push(IndexEntry {
                    id: shadow,
                    values,
                    unique: true,
                }),
            }
        } else {
            entries.push(IndexEntry {
                id: shadow,
                values,
                unique: definition.unique,
            });
        }
        self.index.upsert(
            &reference,
            &record.payload,
            &entries,
            &conflicted,
            &record.cursor,
        )?;
        Ok(())
    }

    /// Commit `CompleteIndexBuild` — the atomic activation. The catalog
    /// flips `Building{g}` to `Active{g}` in one majority commit, so no
    /// reader ever observes a partially-activated generation.
    async fn activate(
        &self,
        definition: &StoredIndexDefinition,
        generation: u64,
        frontier: &Option<String>,
    ) -> Result<(), IndexBuildError> {
        self.control
            .execute_commands(vec![Command::CompleteIndexBuild {
                index_id: definition.index_id,
                generation,
                applied_cursor: frontier.clone().unwrap_or_else(|| "genesis".to_owned()),
            }])
            .await?;
        Ok(())
    }

    /// Bounded sweep of postings from generations superseded by
    /// `activated`. Deterministic ids mean every generation this Index ever
    /// built is derivable; only ones strictly below the activated
    /// generation can hold stale postings.
    async fn retire_prior_generations(&self, definition: &StoredIndexDefinition, activated: u64) {
        let mut budget = RETIRE_BUDGET_PER_TICK;
        for prior in 1..activated {
            let stale = shadow_index_id(definition.index_id, prior);
            loop {
                match self
                    .index
                    .drop_index_postings(stale, RETIRE_PAGE.min(budget))
                {
                    Ok(0) => break,
                    Ok(removed) => {
                        budget = budget.saturating_sub(removed);
                        if budget == 0 {
                            return;
                        }
                    }
                    // A failed sweep retries next tick; stale postings are
                    // unreachable behind the activated generation either way.
                    Err(_) => return,
                }
            }
        }
    }
}

/// Extract the ordered Index values one record projects under `fields`.
/// Extraction is pure and deterministic — every Node computes identical
/// values for the same committed record.
pub fn extract_index_values(
    fields: &[IndexFieldSpec],
    record: &ReplaySourceRecord,
) -> Result<Vec<IndexValue>, IndexBuildError> {
    fields
        .iter()
        .map(|field| match field {
            IndexFieldSpec::Key => Ok(IndexValue::Bytes(record.key.clone())),
            IndexFieldSpec::Metadata { name } => Ok(record
                .metadata
                .get(name)
                .cloned()
                .map(IndexValue::Bytes)
                .unwrap_or(IndexValue::Null)),
            IndexFieldSpec::Projection {
                extractor,
                version,
                field,
            } if extractor == "json" && *version == 1 => json_projection(field, &record.payload),
            IndexFieldSpec::Projection { extractor, .. } => Err(IndexBuildError::Extract(format!(
                "unsupported extractor {extractor}"
            ))),
        })
        .collect()
}

/// `a.b.c` walk of a JSON payload. Scalars map to their natural
/// `IndexValue`; missing fields and non-scalars yield `Null` so records
/// without the projected field still carry a posting at a defined position.
fn json_projection(field: &str, payload: &[u8]) -> Result<IndexValue, IndexBuildError> {
    let document: serde_json::Value = serde_json::from_slice(payload)
        .map_err(|error| IndexBuildError::Extract(format!("payload is not JSON: {error}")))?;
    let mut value = &document;
    for segment in field.split('.') {
        match value.get(segment) {
            Some(next) => value = next,
            None => return Ok(IndexValue::Null),
        }
    }
    Ok(match value {
        serde_json::Value::Null => IndexValue::Null,
        serde_json::Value::Bool(value) => IndexValue::Bool(*value),
        serde_json::Value::Number(number) => number
            .as_i64()
            .map(IndexValue::I64)
            .unwrap_or(IndexValue::Null),
        serde_json::Value::String(text) => IndexValue::Text(text.clone()),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => IndexValue::Null,
    })
}

/// Coalesces commit-boundary apply triggers per Feed: at most one apply
/// task runs per Feed at a time, and a commit observed mid-flight marks
/// the pass to run again so the latest committed record is not stranded
/// waiting for the periodic driver.
pub struct IndexCommitApply {
    driver: Arc<IndexBuildDriver>,
    state: std::sync::Mutex<std::collections::HashMap<Uuid, bool>>,
}

impl IndexCommitApply {
    pub fn new(driver: Arc<IndexBuildDriver>) -> Self {
        Self {
            driver,
            state: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// Kick one bounded apply for `feed_id`. Cheap on every commit: if an
    /// apply is already running it is marked to re-run once rather than
    /// queueing a task per commit.
    pub fn kick(self: &Arc<Self>, feed_id: Uuid) {
        {
            let mut state = match self.state.lock() {
                Ok(state) => state,
                Err(_) => return,
            };
            match state.get_mut(&feed_id) {
                Some(again) => {
                    *again = true;
                    return;
                }
                None => {
                    state.insert(feed_id, false);
                }
            }
        }
        let this = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                this.driver.apply_committed(feed_id, 512).await;
                let mut state = match this.state.lock() {
                    Ok(state) => state,
                    Err(_) => return,
                };
                match state.get(&feed_id) {
                    Some(true) => {
                        state.insert(feed_id, false);
                    }
                    _ => {
                        state.remove(&feed_id);
                        return;
                    }
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use tempfile::TempDir;

    use crate::control::Command;
    use crate::index::{FjallIndexStore, IndexId};
    use crate::state_store::ReplaySourceRecord;
    use crate::storage::FileLogStore;
    use std::time::Duration;

    struct StubSource {
        feed_id: Uuid,
        exists: bool,
        retained: std::sync::Mutex<Vec<ReplaySourceRecord>>,
    }

    #[async_trait::async_trait]
    impl FeedReplaySource for StubSource {
        async fn source_exists(&self, feed_id: Uuid) -> bool {
            self.exists && feed_id == self.feed_id
        }

        async fn read_committed_records(
            &self,
            feed_id: Uuid,
            after: Option<&str>,
            limit: usize,
        ) -> Result<Vec<ReplaySourceRecord>, StateStoreReplayError> {
            assert_eq!(feed_id, self.feed_id);
            let retained = self.retained.lock().unwrap();
            let start = match after {
                None => 0,
                Some(cursor) => retained
                    .iter()
                    .position(|record| record.cursor == cursor)
                    .map(|position| position + 1)
                    .ok_or(StateStoreReplayError::FrontierOutOfRange)?,
            };
            Ok(retained.iter().skip(start).take(limit).cloned().collect())
        }
    }

    fn record(sequence: u64, key: &str) -> ReplaySourceRecord {
        ReplaySourceRecord {
            message_id: Uuid::from_u128(0xCAFE + sequence as u128),
            cursor: format!("cursor-{sequence:04}"),
            key: key.as_bytes().to_vec(),
            metadata: BTreeMap::new(),
            payload: format!(r#"{{"customer":"cust-{sequence}","total":{sequence}}}"#).into_bytes(),
        }
    }

    struct Fixture {
        control: Arc<ControlController>,
        index: Arc<FjallIndexStore>,
        feed_id: Uuid,
        _catalog_dir: TempDir,
        _index_dir: TempDir,
    }

    async fn fixture(fields: Vec<IndexFieldSpec>) -> Fixture {
        let catalog_dir = TempDir::new().unwrap();
        let index_dir = TempDir::new().unwrap();
        let store: Arc<dyn crate::storage::LogStore> =
            Arc::new(FileLogStore::open(catalog_dir.path().join("data")).unwrap());
        let control = Arc::new(
            ControlController::open_with_storage_nodes(
                catalog_dir.path().join("catalog.json"),
                store,
                ["storage-1", "storage-2", "storage-3"]
                    .into_iter()
                    .map(|node| crate::active_range::StorageNodeId::try_new(node).unwrap())
                    .collect(),
            )
            .unwrap(),
        );
        let created = control
            .execute_commands(vec![
                Command::CreateSpace {
                    name: "orders".to_owned(),
                },
                Command::CreateFeed {
                    name: "orders.events".to_owned(),
                    encrypted: false,
                },
            ])
            .await
            .unwrap();
        let feed_id =
            serde_json::from_value::<Uuid>(created.results[1].data["feed_id"].clone()).unwrap();
        control
            .execute_commands(vec![Command::DefineIndex {
                name: "orders.events.by.customer".to_owned(),
                feed: "orders.events".to_owned(),
                fields,
                unique: false,
                collation: crate::control::IndexCollation::Binary,
                consistency: crate::control::IndexConsistency::Eventual,
            }])
            .await
            .unwrap();
        control
            .declared_index_by_name("orders.events.by.customer")
            .await
            .unwrap();
        Fixture {
            control,
            index: Arc::new(FjallIndexStore::open(index_dir.path()).unwrap()),
            feed_id,
            _catalog_dir: catalog_dir,
            _index_dir: index_dir,
        }
    }

    impl Fixture {
        fn driver(&self, source: Arc<StubSource>) -> IndexBuildDriver {
            IndexBuildDriver::new(self.control.clone(), source, self.index.clone())
        }

        async fn rebuild(&self) -> StoredIndexDefinition {
            self.control
                .execute_commands(vec![Command::RebuildIndex {
                    name: "orders.events.by.customer".to_owned(),
                }])
                .await
                .unwrap();
            self.control
                .declared_index_by_name("orders.events.by.customer")
                .await
                .unwrap()
        }

        fn lookup(&self, index_id: IndexId, values: &[IndexValue]) -> Vec<crate::index::IndexRow> {
            self.index.lookup_exact(index_id, values, 64).unwrap()
        }
    }

    #[test]
    fn extract_key_metadata_and_json_projection() {
        let record = ReplaySourceRecord {
            message_id: Uuid::from_u128(1),
            cursor: "cursor-1".to_owned(),
            key: b"k-1".to_vec(),
            metadata: BTreeMap::from([("tenant".to_owned(), b"acme".to_vec())]),
            payload: br#"{"customer":"c-7","total":42,"flag":true,"nested":{"deep":"x"}}"#.to_vec(),
        };
        let values = extract_index_values(
            &[
                IndexFieldSpec::Key,
                IndexFieldSpec::Metadata {
                    name: "tenant".to_owned(),
                },
                IndexFieldSpec::Metadata {
                    name: "absent".to_owned(),
                },
                IndexFieldSpec::Projection {
                    extractor: "json".to_owned(),
                    version: 1,
                    field: "customer".to_owned(),
                },
                IndexFieldSpec::Projection {
                    extractor: "json".to_owned(),
                    version: 1,
                    field: "total".to_owned(),
                },
                IndexFieldSpec::Projection {
                    extractor: "json".to_owned(),
                    version: 1,
                    field: "flag".to_owned(),
                },
                IndexFieldSpec::Projection {
                    extractor: "json".to_owned(),
                    version: 1,
                    field: "nested.deep".to_owned(),
                },
                IndexFieldSpec::Projection {
                    extractor: "json".to_owned(),
                    version: 1,
                    field: "missing".to_owned(),
                },
            ],
            &record,
        )
        .unwrap();
        assert_eq!(
            values,
            vec![
                IndexValue::Bytes(b"k-1".to_vec()),
                IndexValue::Bytes(b"acme".to_vec()),
                IndexValue::Null,
                IndexValue::Text("c-7".to_owned()),
                IndexValue::I64(42),
                IndexValue::Bool(true),
                IndexValue::Text("x".to_owned()),
                IndexValue::Null,
            ]
        );
    }

    #[test]
    fn extract_refuses_unknown_extractor_and_bad_json() {
        let record = ReplaySourceRecord {
            message_id: Uuid::from_u128(1),
            cursor: "cursor-1".to_owned(),
            key: Vec::new(),
            metadata: BTreeMap::new(),
            payload: b"not json".to_vec(),
        };
        assert!(matches!(
            extract_index_values(
                &[IndexFieldSpec::Projection {
                    extractor: "regex".to_owned(),
                    version: 1,
                    field: "x".to_owned(),
                }],
                &record
            ),
            Err(IndexBuildError::Extract(_))
        ));
        assert!(matches!(
            extract_index_values(
                &[IndexFieldSpec::Projection {
                    extractor: "json".to_owned(),
                    version: 1,
                    field: "x".to_owned(),
                }],
                &record
            ),
            Err(IndexBuildError::Extract(_))
        ));
    }

    #[tokio::test]
    async fn shadow_build_populates_then_activates_atomically() {
        let fixture = fixture(vec![IndexFieldSpec::Key]).await;
        let source = Arc::new(StubSource {
            feed_id: fixture.feed_id,
            exists: true,
            retained: std::sync::Mutex::new(vec![record(1, "a"), record(2, "b"), record(3, "a")]),
        });
        let definition = fixture.rebuild().await;
        assert!(matches!(
            definition.stage,
            IndexBuildStage::Building { generation: 1 }
        ));
        let shadow = shadow_index_id(definition.index_id, 1);
        // The pass drives through the catalog driver so activation commits.
        let pass = fixture
            .driver(source)
            .build_pass(&definition, 64)
            .await
            .unwrap();
        assert_eq!(pass.applied, 3);
        assert!(pass.complete);
        assert!(pass.activated);
        let key_a = IndexValue::Bytes(b"a".to_vec());
        let hits = fixture.lookup(shadow, std::slice::from_ref(&key_a));
        assert_eq!(hits.len(), 1, "key 'a' resolves through the shadow");
        assert_eq!(
            fixture
                .lookup(shadow, &[IndexValue::Bytes(b"b".to_vec())])
                .len(),
            1
        );
        // The definition flipped to Active with the covered cursor.
        let updated = fixture
            .control
            .declared_index_by_name("orders.events.by.customer")
            .await
            .unwrap();
        assert!(matches!(
            updated.stage,
            IndexBuildStage::Active { generation: 1 }
        ));
        assert_eq!(updated.applied_cursor.as_deref(), Some("cursor-0003"));
    }

    #[tokio::test]
    async fn rebuild_resumes_shadow_from_durable_frontier() {
        let fixture = fixture(vec![IndexFieldSpec::Key]).await;
        let source = Arc::new(StubSource {
            feed_id: fixture.feed_id,
            exists: true,
            retained: std::sync::Mutex::new(vec![record(1, "a"), record(2, "b"), record(3, "c")]),
        });
        let definition = fixture.rebuild().await;
        let driver = fixture.driver(source);
        // limit=2 leaves one record pending: pass is not complete.
        let pass = driver.build_pass(&definition, 2).await.unwrap();
        assert_eq!(pass.applied, 2);
        assert!(!pass.complete);
        let shadow = shadow_index_id(definition.index_id, 1);
        assert_eq!(
            fixture.index.last_applied(shadow).unwrap().as_deref(),
            Some("cursor-0002")
        );
        // Next pass resumes at the durable cursor, not the beginning.
        let pass = driver.build_pass(&definition, 64).await.unwrap();
        assert_eq!(pass.applied, 1);
        assert!(pass.activated);
    }

    #[tokio::test]
    async fn apply_committed_materializes_active_generations_at_the_commit_boundary() {
        // The deterministic replay of committed frames onto Active
        // generations is what every replica runs when its committed
        // watermark advances: same committed stream in, same postings out.
        let fixture = fixture(vec![IndexFieldSpec::Key]).await;
        let source = Arc::new(StubSource {
            feed_id: fixture.feed_id,
            exists: true,
            retained: std::sync::Mutex::new(vec![record(1, "a"), record(2, "b")]),
        });
        let definition = fixture.rebuild().await;
        let driver = fixture.driver(source.clone());
        // Activate the shadow so the generation is Active for the commit
        // path; this also proves apply_committed ignores Building work.
        driver.build_pass(&definition, 64).await.unwrap();
        let updated = fixture
            .control
            .declared_index_by_name("orders.events.by.customer")
            .await
            .unwrap();
        assert!(matches!(updated.stage, IndexBuildStage::Active { .. }));

        // New records commit after activation; the periodic driver has not
        // run. The commit-boundary apply picks them up.
        source
            .retained
            .lock()
            .unwrap()
            .extend([record(3, "c"), record(4, "d")]);
        let outcomes = driver.apply_committed(fixture.feed_id, 64).await;
        assert_eq!(outcomes.len(), 1);
        let (_, pass) = outcomes.into_iter().next().unwrap();
        let pass = pass.unwrap();
        assert_eq!(pass.applied, 2);
        let shadow = shadow_index_id(updated.index_id, 1);
        assert_eq!(
            fixture.index.last_applied(shadow).unwrap().as_deref(),
            Some("cursor-0004")
        );
        assert_eq!(
            fixture
                .lookup(shadow, &[IndexValue::Bytes(b"d".to_vec())])
                .len(),
            1
        );

        // Idempotent at the frontier: a second commit boundary for the
        // same watermark applies nothing.
        let outcomes = driver.apply_committed(fixture.feed_id, 64).await;
        assert_eq!(outcomes.len(), 1);
        assert_eq!(outcomes[0].1.as_ref().unwrap().applied, 0);
    }

    #[tokio::test]
    async fn unique_index_arbitrates_claims_by_committed_order() {
        // The replicated conditional claim: every replica replays the same
        // merged committed history, so the earliest committed record to
        // claim a value wins identically everywhere. The loser keeps its
        // row but is suppressed from the Index and marked conflicted.
        let fixture = fixture(vec![IndexFieldSpec::Key]).await;
        fixture
            .control
            .execute_commands(vec![Command::DefineIndex {
                name: "orders.events.unique.customer".to_owned(),
                feed: "orders.events".to_owned(),
                fields: vec![IndexFieldSpec::Projection {
                    extractor: "json".to_owned(),
                    version: 1,
                    field: "customer".to_owned(),
                }],
                unique: true,
                collation: crate::control::IndexCollation::Binary,
                consistency: crate::control::IndexConsistency::Eventual,
            }])
            .await
            .unwrap();
        // Two distinct application keys project the same unique value;
        // a third claims a different value.
        let mut first = record(1, "k-a");
        first.payload = br#"{"customer":"dup","total":1}"#.to_vec();
        let mut second = record(2, "k-b");
        second.payload = br#"{"customer":"dup","total":2}"#.to_vec();
        let third = record(3, "k-c");
        let source = Arc::new(StubSource {
            feed_id: fixture.feed_id,
            exists: true,
            retained: std::sync::Mutex::new(vec![first, second, third]),
        });
        fixture
            .control
            .execute_commands(vec![Command::RebuildIndex {
                name: "orders.events.unique.customer".to_owned(),
            }])
            .await
            .unwrap();
        let definition = fixture
            .control
            .declared_index_by_name("orders.events.unique.customer")
            .await
            .unwrap();
        let driver = fixture.driver(source.clone());
        driver.build_pass(&definition, 64).await.unwrap();
        let shadow = shadow_index_id(definition.index_id, 1);

        // Winner resolves; loser is suppressed with conflict evidence.
        let rows = fixture.lookup(shadow, &[IndexValue::Text("dup".to_owned())]);
        assert_eq!(rows.len(), 1, "only the winner's posting resolves");
        assert_eq!(rows[0].reference.application_key, b"k-a");
        let loser = fixture
            .index
            .get(&crate::index::PrimaryRef {
                feed_id: fixture.feed_id,
                application_key: b"k-b".to_vec(),
            })
            .unwrap()
            .expect("loser keeps its row");
        assert_eq!(loser.conflicted, vec![shadow]);
        // The apply pass itself stayed healthy: all three records consumed.
        assert_eq!(
            fixture.index.last_applied(shadow).unwrap().as_deref(),
            Some("cursor-0003")
        );

        // A re-apply of the same prefix is idempotent and keeps the mark.
        driver.apply_committed(fixture.feed_id, 64).await;
        let loser = fixture
            .index
            .get(&crate::index::PrimaryRef {
                feed_id: fixture.feed_id,
                application_key: b"k-b".to_vec(),
            })
            .unwrap()
            .unwrap();
        assert_eq!(loser.conflicted, vec![shadow]);
        assert_eq!(
            fixture
                .lookup(shadow, &[IndexValue::Text("dup".to_owned())])
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn apply_committed_ignores_other_feeds_and_non_active_stages() {
        let fixture = fixture(vec![IndexFieldSpec::Key]).await;
        let source = Arc::new(StubSource {
            feed_id: fixture.feed_id,
            exists: true,
            retained: std::sync::Mutex::new(vec![record(1, "a")]),
        });
        let driver = fixture.driver(source);
        // Declared (never rebuilt) definitions take no commit-boundary work.
        let outcomes = driver.apply_committed(fixture.feed_id, 64).await;
        assert!(outcomes.is_empty());
        // A foreign feed id touches nothing either.
        let outcomes = driver.apply_committed(Uuid::new_v4(), 64).await;
        assert!(outcomes.is_empty());
    }

    #[tokio::test]
    async fn commit_apply_kick_materializes_and_coalesces() {
        let fixture = fixture(vec![IndexFieldSpec::Key]).await;
        let source = Arc::new(StubSource {
            feed_id: fixture.feed_id,
            exists: true,
            retained: std::sync::Mutex::new(vec![record(1, "a"), record(2, "b")]),
        });
        let definition = fixture.rebuild().await;
        let driver = Arc::new(fixture.driver(source.clone()));
        driver.build_pass(&definition, 64).await.unwrap();
        let gate = Arc::new(IndexCommitApply::new(driver));
        source.retained.lock().unwrap().push(record(3, "c"));
        // Repeated kicks coalesce into one running pass plus one re-run.
        for _ in 0..5 {
            gate.kick(fixture.feed_id);
        }
        let updated = fixture
            .control
            .declared_index_by_name("orders.events.by.customer")
            .await
            .unwrap();
        let shadow = shadow_index_id(updated.index_id, 1);
        for _ in 0..200 {
            if fixture.index.last_applied(shadow).unwrap().as_deref() == Some("cursor-0003") {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("commit-boundary apply did not materialize the record");
    }

    #[tokio::test]
    async fn rebuild_refuses_when_source_history_is_gone() {
        let fixture = fixture(vec![IndexFieldSpec::Key]).await;
        // Missing source Feed: refuse.
        let gone = Arc::new(StubSource {
            feed_id: fixture.feed_id,
            exists: false,
            retained: std::sync::Mutex::new(Vec::new()),
        });
        let definition = fixture.rebuild().await;
        let result = fixture.driver(gone).build_pass(&definition, 64).await;
        assert!(matches!(result, Err(IndexBuildError::SourceUnavailable)));
        let still = fixture
            .control
            .declared_index_by_name("orders.events.by.customer")
            .await
            .unwrap();
        assert!(matches!(still.stage, IndexBuildStage::Building { .. }));

        // Retained history dropped beneath the shadow's resume frontier:
        // refuse rather than activate a partial index.
        let source = Arc::new(StubSource {
            feed_id: fixture.feed_id,
            exists: true,
            retained: std::sync::Mutex::new(vec![record(1, "a"), record(2, "b"), record(3, "c")]),
        });
        let driver = fixture.driver(source.clone());
        driver.build_pass(&definition, 2).await.unwrap();
        source.retained.lock().unwrap().drain(0..2);
        let result = driver.build_pass(&definition, 64).await;
        assert!(matches!(result, Err(IndexBuildError::FrontierOutOfRange)));
    }

    #[tokio::test]
    async fn shadow_generation_is_isolated_until_activation() {
        let fixture = fixture(vec![IndexFieldSpec::Key]).await;
        let source = Arc::new(StubSource {
            feed_id: fixture.feed_id,
            exists: true,
            retained: std::sync::Mutex::new(vec![record(1, "a"), record(2, "b")]),
        });
        let definition = fixture.rebuild().await;
        let shadow = shadow_index_id(definition.index_id, 1);
        let driver = fixture.driver(source.clone());
        driver.build_pass(&definition, 64).await.unwrap();
        // A second rebuild starts a fresh shadow while generation 1 serves.
        let definition = fixture.rebuild().await;
        assert!(matches!(
            definition.stage,
            IndexBuildStage::Building { generation: 2 }
        ));
        let shadow2 = shadow_index_id(definition.index_id, 2);
        assert!(
            fixture.index.last_applied(shadow2).unwrap().is_none(),
            "new shadow starts uncovered"
        );
        // Generation 1 postings are untouched by generation 2's build.
        let before: Vec<_> = fixture
            .lookup(shadow, &[IndexValue::Bytes(b"a".to_vec())])
            .into_iter()
            .map(|row| row.reference)
            .collect();
        let pass = driver.build_pass(&definition, 1).await.unwrap();
        assert_eq!(pass.applied, 1);
        assert!(!pass.complete);
        let after: Vec<_> = fixture
            .lookup(shadow, &[IndexValue::Bytes(b"a".to_vec())])
            .into_iter()
            .map(|row| row.reference)
            .collect();
        assert_eq!(before, after, "generation 1 postings still resolve");
        let shadow2_hits = fixture.lookup(shadow2, &[IndexValue::Bytes(b"b".to_vec())]);
        assert!(
            shadow2_hits.is_empty(),
            "unapplied record has no g2 posting"
        );
    }

    #[tokio::test]
    async fn activation_retires_prior_generation_postings() {
        let fixture = fixture(vec![IndexFieldSpec::Key]).await;
        let source = Arc::new(StubSource {
            feed_id: fixture.feed_id,
            exists: true,
            retained: std::sync::Mutex::new(vec![record(1, "a"), record(2, "b")]),
        });
        let driver = fixture.driver(source);
        let definition = fixture.rebuild().await;
        driver.build_pass(&definition, 64).await.unwrap();
        let shadow1 = shadow_index_id(definition.index_id, 1);
        assert_eq!(
            fixture
                .lookup(shadow1, &[IndexValue::Bytes(b"a".to_vec())])
                .len(),
            1
        );
        // Rebuild generation 2 and activate: generation 1 postings retire.
        let definition = fixture.rebuild().await;
        driver.build_pass(&definition, 64).await.unwrap();
        assert!(
            fixture
                .lookup(shadow1, &[IndexValue::Bytes(b"a".to_vec())])
                .is_empty(),
            "retired generation postings are swept"
        );
        assert!(
            fixture.index.last_applied(shadow1).unwrap().is_none(),
            "retired generation checkpoint is dropped"
        );
        // The row still exists under the activated generation.
        let shadow2 = shadow_index_id(definition.index_id, 2);
        assert_eq!(
            fixture
                .lookup(shadow2, &[IndexValue::Bytes(b"a".to_vec())])
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn concurrent_activation_is_superseded_not_conflicted() {
        let fixture = fixture(vec![IndexFieldSpec::Key]).await;
        let source = Arc::new(StubSource {
            feed_id: fixture.feed_id,
            exists: true,
            retained: std::sync::Mutex::new(vec![record(1, "a")]),
        });
        let definition = fixture.rebuild().await;
        let driver = fixture.driver(source);
        // First completion activates generation 1.
        let first = driver.build_pass(&definition, 64).await.unwrap();
        assert!(first.activated);
        // A stale driver holding the Building snapshot completes: its
        // activation attempt is superseded, never an error.
        let stale = driver.build_pass(&definition, 64).await.unwrap();
        assert!(
            stale.superseded,
            "second completer is superseded, never an error"
        );
    }
}
