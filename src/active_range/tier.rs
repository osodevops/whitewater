//! Sealed-segment tiering: upload immutable, manifest-verified segment
//! artifacts to object storage behind a narrow `ObjectStore` contract.
//!
//! The pipeline per the architecture is `seal and verify -> object
//! storage`: `sealed_segment_manifest` binds a whole-file BLAKE3 digest to
//! the segment identity, `verify_sealed_segment` rehashes local bytes
//! against that manifest, and only then do artifacts move. Object keys are
//! content-addressed on the segment digest, so the three RF3 replicas of a
//! range — holding byte-identical sealed files — converge on the same keys
//! and dedupe naturally: the second uploader adopts the existing object
//! after re-verifying its bytes rather than writing a copy.
//!
//! A `segment-tier-{n:06}.json` marker records each completed verified
//! upload beside the range's local files; re-ticks skip marked segments
//! and a restart resumes cleanly because the marker is durable. Remote
//! verification re-reads the object and rehashes it against the manifest
//! digest — honest for the filesystem backend used by development and
//! test topologies, and the contract an S3-style backend must satisfy.

use std::{
    collections::HashMap,
    fs::{self, File},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::{Arc, Condvar, Mutex},
};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use super::store::{
    manifest_path, sealed_segment_manifest, segment_path, sync_directory, verify_sealed_segment,
    ActiveRangeStoreError, PersistedRangeState, RemoteSegmentSource, SegmentManifest,
};
use crate::control::HistoryPolicy;

#[derive(Debug, Error)]
pub enum TierError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("range store error: {0}")]
    Store(#[from] ActiveRangeStoreError),
    #[error("object store error: {0}")]
    Remote(String),
    #[error("object {key} already holds different content (remote digest {remote}, manifest digest {expected})")]
    DigestConflict {
        key: String,
        remote: String,
        expected: String,
    },
}

/// The narrow contract every tier backend satisfies. Keys are flat UTF-8
/// paths; objects are immutable once visible.
pub trait ObjectStore: Send + Sync {
    /// Atomically publishes a local file's bytes under `key` and returns
    /// the byte count written.
    fn put(&self, key: &str, source: &Path) -> Result<u64, TierError>;
    /// Returns the object's bytes, or an error naming the missing key.
    fn get(&self, key: &str) -> Result<Vec<u8>, TierError>;
    /// `Some(len)` when the object exists, `None` when it does not.
    fn head(&self, key: &str) -> Result<Option<u64>, TierError>;

    /// Removes an object; `NotFound` succeeds — deletion is idempotent
    /// because every RF3 replica runs the same retention pass and may
    /// race to delete shared objects.
    fn delete(&self, key: &str) -> Result<(), TierError>;
}

/// Directory-rooted object store for development and test topologies:
/// `put` stages through a unique temporary name and renames atomically so
/// a key is never half-visible, matching the durability contract real
/// object stores give a completed PUT.
#[derive(Debug)]
pub struct FileSystemObjectStore {
    root: PathBuf,
}

impl FileSystemObjectStore {
    pub fn open(root: impl AsRef<Path>) -> Result<Self, TierError> {
        fs::create_dir_all(root.as_ref())?;
        Ok(Self {
            root: root.as_ref().to_path_buf(),
        })
    }

    fn target(&self, key: &str) -> Result<PathBuf, TierError> {
        // Keys are constructed internally, but refuse traversal outright so
        // a future caller cannot escape the store root.
        if key
            .split('/')
            .any(|part| part.is_empty() || part == ".." || part == ".")
            || key.contains('\\')
        {
            return Err(TierError::Remote(format!("invalid object key {key}")));
        }
        Ok(self.root.join(key))
    }
}

impl ObjectStore for FileSystemObjectStore {
    fn put(&self, key: &str, source: &Path) -> Result<u64, TierError> {
        let target = self.target(key)?;
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)?;
        }
        let temporary = target.with_extension(format!("{}.part", Uuid::new_v4()));
        let bytes = {
            let mut input = File::open(source)?;
            let mut output = File::create(&temporary)?;
            io::copy(&mut input, &mut output)?;
            output.sync_all()?;
            output.metadata()?.len()
        };
        fs::rename(&temporary, &target)?;
        sync_parent(&target)?;
        Ok(bytes)
    }

    fn get(&self, key: &str) -> Result<Vec<u8>, TierError> {
        let target = self.target(key)?;
        fs::read(&target).map_err(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                TierError::Remote(format!("object {key} does not exist"))
            } else {
                TierError::Io(error)
            }
        })
    }

    fn head(&self, key: &str) -> Result<Option<u64>, TierError> {
        let target = self.target(key)?;
        match target.metadata() {
            Ok(meta) => Ok(Some(meta.len())),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(TierError::Io(error)),
        }
    }

    fn delete(&self, key: &str) -> Result<(), TierError> {
        let target = self.target(key)?;
        match fs::remove_file(&target) {
            Ok(()) => {
                let _ = sync_parent(&target);
                Ok(())
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(TierError::Io(error)),
        }
    }
}

fn sync_parent(path: &Path) -> Result<(), io::Error> {
    if let Some(parent) = path.parent() {
        sync_directory(parent)?;
    }
    Ok(())
}

/// A durable record that one sealed segment's artifacts reached object
/// storage verified. Lives beside the range files as
/// `segment-tier-{n:06}.json`; the tick treats a valid marker as proof of
/// a completed upload and never re-reads remote bytes for it.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TierMarker {
    pub version: u32,
    pub segment: u64,
    pub object_key: String,
    pub manifest_key: String,
    pub checkpoint_key: Option<String>,
    pub bytes: u64,
    pub segment_digest: String,
    pub uploaded_at_ns: i64,
}

const TIER_MARKER_VERSION: u32 = 1;

pub(crate) fn tier_marker_path(directory: &Path, segment: u64) -> PathBuf {
    directory.join(format!("segment-tier-{segment:06}.json"))
}

pub(crate) fn read_tier_marker(directory: &Path, segment: u64) -> Option<TierMarker> {
    let bytes = fs::read(tier_marker_path(directory, segment)).ok()?;
    let marker = serde_json::from_slice::<TierMarker>(&bytes).ok()?;
    (marker.version == TIER_MARKER_VERSION && marker.segment == segment).then_some(marker)
}

pub(crate) fn write_tier_marker(directory: &Path, marker: &TierMarker) -> Result<(), TierError> {
    let serialized = serde_json::to_vec(marker)?;
    let target = tier_marker_path(directory, marker.segment);
    let temporary = target.with_extension(format!("{}.tmp", Uuid::new_v4()));
    {
        let mut file = File::create(&temporary)?;
        file.write_all(&serialized)?;
        file.sync_all()?;
    }
    fs::rename(&temporary, &target)?;
    sync_parent(&target)?;
    Ok(())
}

/// Local cache file a fetched remote segment lands in. The `cache-`
/// prefix keeps it out of every `segment-*` scan while letting a reopen
/// adopt it without a fresh remote read.
fn cache_segment_path(directory: &Path, segment: u64) -> PathBuf {
    directory.join(format!("cache-segment-{segment:06}.log"))
}

/// One remote fetch's shared outcome: the winner records the verdict and
/// releases every waiter, so N concurrent Readers cost one object `get`.
#[derive(Debug)]
struct FetchSlot {
    outcome: Mutex<Option<Result<PathBuf, String>>>,
    ready: Condvar,
}

/// Read-side counterpart of the uploader — the `RemoteSegmentSource`
/// implementation the range store consults when a sealed segment's local
/// file is absent.
///
/// Guarantees:
/// - marker + manifest provenance is re-checked before every fetch, so a
///   demoted file without a verified remote copy fails closed;
/// - fetches dedupe per object key (single-flight) and bound remote
///   concurrency Node-wide — a burst of historical Readers cannot
///   stampede object storage;
/// - admitted bytes are BLAKE3-verified against the manifest digest and
///   installed atomically before their path is ever returned;
/// - the per-range byte budget surfaces through `remote_cache_bytes`,
///   enforced by the store's coldest-first demotion.
pub struct TierSource {
    object_root: Option<PathBuf>,
    store: Mutex<Option<Arc<dyn ObjectStore>>>,
    remote_cache_bytes: u64,
    max_concurrent_fetches: usize,
    inflight: Mutex<HashMap<String, Arc<FetchSlot>>>,
    permits: Mutex<usize>,
    permits_ready: Condvar,
}

impl std::fmt::Debug for TierSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TierSource")
            .field("remote_cache_bytes", &self.remote_cache_bytes)
            .finish_non_exhaustive()
    }
}

impl TierSource {
    /// `remote_cache_bytes` is the per-range ceiling on materialized
    /// remote segments; remote fetches never exceed
    /// `MAX_CONCURRENT_FETCHES` in flight across every range on the Node.
    pub fn new(object_root: PathBuf, remote_cache_bytes: u64) -> Self {
        Self {
            object_root: Some(object_root),
            store: Mutex::new(None),
            remote_cache_bytes,
            max_concurrent_fetches: Self::MAX_CONCURRENT_FETCHES,
            inflight: Mutex::new(HashMap::new()),
            permits: Mutex::new(0),
            permits_ready: Condvar::new(),
        }
    }

    /// Test seam: serve fetches from an injected backend so single-flight
    /// and verification behavior are observable without a directory root.
    #[cfg(test)]
    pub(crate) fn with_store(store: Arc<dyn ObjectStore>, remote_cache_bytes: u64) -> Self {
        Self {
            object_root: None,
            store: Mutex::new(Some(store)),
            remote_cache_bytes,
            max_concurrent_fetches: Self::MAX_CONCURRENT_FETCHES,
            inflight: Mutex::new(HashMap::new()),
            permits: Mutex::new(0),
            permits_ready: Condvar::new(),
        }
    }

    /// A small fixed bound: historical fetches are already single-flight
    /// per object, so four parallel object reads is plenty of overlap
    /// without letting a cold-reader burst queue behind itself.
    const MAX_CONCURRENT_FETCHES: usize = 4;

    fn acquire_permit(&self) {
        let mut permits = self
            .permits
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while *permits >= self.max_concurrent_fetches {
            permits = self
                .permits_ready
                .wait(permits)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        *permits += 1;
    }

    fn release_permit(&self) {
        let mut permits = self
            .permits
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *permits = permits.saturating_sub(1);
        self.permits_ready.notify_one();
    }

    /// The object store root opens lazily and caches: a late-mounted
    /// backend fails the first reads with a clear error and self-heals as
    /// soon as the directory appears, matching the uploader's contract.
    fn object_store(&self) -> Result<Arc<dyn ObjectStore>, ActiveRangeStoreError> {
        let mut store = self
            .store
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(store) = store.as_ref() {
            return Ok(store.clone());
        }
        let root = self.object_root.clone().ok_or_else(|| {
            ActiveRangeStoreError::RemoteFetch("no object store root configured".to_owned())
        })?;
        let opened: Arc<dyn ObjectStore> = Arc::new(
            FileSystemObjectStore::open(root)
                .map_err(|error| ActiveRangeStoreError::RemoteFetch(error.to_string()))?,
        );
        *store = Some(opened.clone());
        Ok(opened)
    }

    /// Re-verifies a cache file's whole-file digest against the manifest
    /// before reuse — a torn or tampered cache write never serves.
    fn cache_file_valid(&self, path: &Path, manifest: &SegmentManifest) -> bool {
        path.is_file()
            && fs::read(path)
                .map(|bytes| {
                    bytes.len() as u64 == manifest.bytes
                        && blake3::hash(&bytes).to_hex().as_str() == manifest.segment_digest
                })
                .unwrap_or(false)
    }

    /// Winner's half of the single-flight: bounded remote read, digest
    /// verification, atomic cache install, then record + release.
    fn fetch_and_admit(
        &self,
        directory: &Path,
        key: &str,
        manifest: &SegmentManifest,
        cache: &Path,
    ) -> Result<PathBuf, String> {
        self.acquire_permit();
        let outcome = self
            .fetch_and_admit_inner(directory, key, manifest, cache)
            .map_err(|error| error.to_string());
        self.release_permit();
        outcome
    }

    fn fetch_and_admit_inner(
        &self,
        directory: &Path,
        key: &str,
        manifest: &SegmentManifest,
        cache: &Path,
    ) -> Result<PathBuf, ActiveRangeStoreError> {
        let bytes = self
            .object_store()?
            .get(key)
            .map_err(|error| ActiveRangeStoreError::RemoteFetch(error.to_string()))?;
        if bytes.len() as u64 != manifest.bytes
            || blake3::hash(&bytes).to_hex().as_str() != manifest.segment_digest
        {
            return Err(ActiveRangeStoreError::RemoteFetch(format!(
                "remote object {key} failed digest verification against manifest {}",
                manifest.segment_digest
            )));
        }
        // A concurrent eviction may have removed a previous cache file
        // between the existence check and this install — the write is
        // atomic, so whichever order wins leaves a complete file.
        let temporary = cache.with_extension("tmp");
        fs::write(&temporary, &bytes).map_err(ActiveRangeStoreError::Io)?;
        // Windows grants fsync only on handles opened with write access.
        std::fs::OpenOptions::new()
            .write(true)
            .open(&temporary)
            .and_then(|file| file.sync_all())
            .map_err(ActiveRangeStoreError::Io)?;
        fs::rename(&temporary, cache).map_err(ActiveRangeStoreError::Io)?;
        sync_directory(directory).map_err(ActiveRangeStoreError::Io)?;
        Ok(cache.to_path_buf())
    }
}

impl RemoteSegmentSource for TierSource {
    fn fetch_sealed(
        &self,
        directory: &Path,
        segment: u64,
        manifest: &SegmentManifest,
    ) -> Result<PathBuf, ActiveRangeStoreError> {
        // Provenance is cheap and rechecked on every fetch: a demoted
        // file without a verified remote copy fails closed rather than
        // trusting an object that may belong to another range.
        let marker_ok = read_tier_marker(directory, segment).is_some_and(|marker| {
            marker.segment == segment && marker.segment_digest == manifest.segment_digest
        });
        if !marker_ok {
            return Err(ActiveRangeStoreError::RemoteFetch(format!(
                "sealed segment {segment} has no verified remote copy marker"
            )));
        }
        let cache = cache_segment_path(directory, segment);
        if self.cache_file_valid(&cache, manifest) {
            return Ok(cache);
        }
        if cache.exists() {
            let _ = fs::remove_file(&cache);
        }
        let key = segment_object_key(manifest);
        let (slot, winner) = {
            let mut inflight = self
                .inflight
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match inflight.get(&key) {
                Some(slot) => (slot.clone(), false),
                None => {
                    let slot = Arc::new(FetchSlot {
                        outcome: Mutex::new(None),
                        ready: Condvar::new(),
                    });
                    inflight.insert(key.clone(), slot.clone());
                    (slot, true)
                }
            }
        };
        if winner {
            let outcome = self.fetch_and_admit(directory, &key, manifest, &cache);
            {
                let mut recorded = slot
                    .outcome
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                *recorded = Some(outcome.clone());
            }
            slot.ready.notify_all();
            self.inflight
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&key);
            return outcome.map_err(ActiveRangeStoreError::RemoteFetch);
        }
        let mut outcome = slot
            .outcome
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while outcome.is_none() {
            outcome = slot
                .ready
                .wait(outcome)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        outcome
            .clone()
            .ok_or_else(|| ActiveRangeStoreError::Worker("fetch slot poisoned".to_owned()))?
            .map_err(ActiveRangeStoreError::RemoteFetch)
    }

    fn remote_cache_bytes(&self) -> u64 {
        self.remote_cache_bytes
    }
}

/// Object keys bind the segment's content digest, so replicas dedupe and
/// every fetch-back path can verify bytes by name.
fn object_key_prefix(manifest: &SegmentManifest) -> String {
    format!(
        "{}/{}/generation-{}/{}",
        manifest.feed_id,
        manifest.range_id.as_uuid(),
        manifest.generation.value(),
        manifest.segment_digest
    )
}

pub(crate) fn segment_object_key(manifest: &SegmentManifest) -> String {
    format!("{}.log", object_key_prefix(manifest))
}

fn manifest_object_key(manifest: &SegmentManifest) -> String {
    format!("{}.manifest.json", object_key_prefix(manifest))
}

fn checkpoint_object_key(manifest: &SegmentManifest) -> String {
    format!("{}.checkpoint", object_key_prefix(manifest))
}

/// Uploads one artifact, or adopts the existing object after re-verifying
/// its bytes. Returns the remote byte count. Either way the remote digest
/// is recomputed and compared to the manifest digest — a mismatch refuses
/// rather than overwriting, because content under a content-addressed key
/// must be identical or something upstream is corrupt.
fn put_or_adopt(
    store: &dyn ObjectStore,
    key: &str,
    local: &Path,
    expected_digest: &str,
) -> Result<u64, TierError> {
    if let Some(len) = store.head(key)? {
        if len != local.metadata()?.len() {
            return Err(TierError::DigestConflict {
                key: key.to_owned(),
                remote: format!("length {len}"),
                expected: expected_digest.to_owned(),
            });
        }
        let remote = blake3::hash(&store.get(key)?).to_hex().to_string();
        if remote != expected_digest {
            return Err(TierError::DigestConflict {
                key: key.to_owned(),
                remote,
                expected: expected_digest.to_owned(),
            });
        }
        return Ok(len);
    }
    let written = store.put(key, local)?;
    let remote = blake3::hash(&store.get(key)?).to_hex().to_string();
    if remote != expected_digest {
        return Err(TierError::DigestConflict {
            key: key.to_owned(),
            remote,
            expected: expected_digest.to_owned(),
        });
    }
    Ok(written)
}

/// The per-segment upload path the tick runs: ensure the manifest exists
/// (healing it from the sealed file when absent), verify local bytes
/// against it, then publish segment, manifest, and index checkpoint under
/// content-addressed keys with remote re-verification, and persist the
/// tier marker. Fails closed on any integrity drift.
pub(crate) fn upload_sealed_segment(
    directory: &Path,
    segment: u64,
    persisted: &PersistedRangeState,
    store: &dyn ObjectStore,
    key: Option<&crate::control::FeedKeyState>,
) -> Result<TierMarker, TierError> {
    sealed_segment_manifest(directory, segment, persisted, key)?;
    let manifest = verify_sealed_segment(directory, segment, persisted)?;
    let manifest_bytes = manifest.segment_digest.clone();

    let segment_path = super::store::segment_path(directory, segment);
    let manifest_path = super::store::manifest_path(directory, segment);
    let checkpoint_path = directory.join(format!("index-checkpoint-{segment:06}.bin"));

    let segment_key = segment_object_key(&manifest);
    let bytes = put_or_adopt(store, &segment_key, &segment_path, &manifest_bytes)?;

    let manifest_digest = {
        let bytes = fs::read(&manifest_path)?;
        blake3::hash(&bytes).to_hex().to_string()
    };
    let manifest_key = manifest_object_key(&manifest);
    put_or_adopt(store, &manifest_key, &manifest_path, &manifest_digest)?;

    let checkpoint_key = if checkpoint_path.exists() {
        let digest = {
            let bytes = fs::read(&checkpoint_path)?;
            blake3::hash(&bytes).to_hex().to_string()
        };
        let key = checkpoint_object_key(&manifest);
        put_or_adopt(store, &key, &checkpoint_path, &digest)?;
        Some(key)
    } else {
        None
    };

    let marker = TierMarker {
        version: TIER_MARKER_VERSION,
        segment,
        object_key: segment_key,
        manifest_key,
        checkpoint_key,
        bytes,
        segment_digest: manifest_bytes,
        uploaded_at_ns: unix_ns(),
    };
    write_tier_marker(directory, &marker)?;
    Ok(marker)
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TierScanReport {
    pub uploaded: u64,
    pub already_tiered: u64,
    /// Sealed segments whose Feed's History Policy keeps them local —
    /// they are not tiered yet but will be once they outlast the
    /// declared hot-local duration.
    pub hot_deferred: u64,
    pub failed: Vec<String>,
}

/// One bounded pass over every range directory under an active-ranges
/// root: sealed segments lacking a valid tier marker are verified and
/// uploaded; anything else is skipped. Failures are collected per segment
/// so one corrupt range cannot stall tiering for the rest — the next tick
/// retries the same segment.
/// `policy` resolves a Feed's effective History Policy. A segment whose
/// manifest is younger than the declared `tier_after_ns` hot-local
/// duration is deferred — the copy only leaves local disk once the
/// segment has outlived its hot window, so policy-driven tiering keeps
/// recently sealed history local without stopping the continuous pass.
/// An unresolvable policy (`None`) means no declared bound — the
/// built-in default tiers immediately, which also lets history
/// orphaned by a dropped Feed move off local disk.
pub fn tier_scan_once(
    root: &Path,
    store: &dyn ObjectStore,
    policy: &dyn Fn(Uuid) -> Option<HistoryPolicy>,
    keys: &dyn Fn(Uuid) -> Option<crate::control::FeedKeyState>,
) -> TierScanReport {
    let mut report = TierScanReport::default();
    let now = unix_ns();
    for generation_dir in generation_directories(root) {
        let state_path = generation_dir.join("range-state.json");
        let persisted: PersistedRangeState = match fs::read(&state_path)
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        {
            Some(state) => state,
            None => continue,
        };
        let hot_local = policy(persisted.feed_id).and_then(|p| p.tier_after_ns);
        for segment in 0..persisted.active_segment {
            if read_tier_marker(&generation_dir, segment).is_some() {
                report.already_tiered += 1;
                continue;
            }
            if let Some(hot) = hot_local {
                let sealed_at = sealed_at(&generation_dir, segment);
                if sealed_at.is_none_or(|sealed| {
                    sealed.saturating_add(i64::try_from(hot).unwrap_or(i64::MAX)) > now
                }) {
                    report.hot_deferred += 1;
                    continue;
                }
            }
            match upload_sealed_segment(
                &generation_dir,
                segment,
                &persisted,
                store,
                keys(persisted.feed_id).as_ref(),
            ) {
                Ok(_) => report.uploaded += 1,
                Err(error) => report.failed.push(format!(
                    "{}/segment-{segment:06}: {error}",
                    generation_dir.display()
                )),
            }
        }
    }
    report
}

/// Seal time for hot-local gating: the sealed `.log`'s own mtime — the
/// file stops being written the moment rotation seals it. A manifest
/// may not exist yet (it heals during upload), so its mtime is only a
/// fallback. `None` means neither artifact is readable — the scan then
/// treats the segment as still hot rather than rushing it out.
fn sealed_at(generation_dir: &Path, segment: u64) -> Option<i64> {
    for path in [
        segment_path(generation_dir, segment),
        manifest_path(generation_dir, segment),
    ] {
        if let Some(nanos) = fs::metadata(&path)
            .ok()
            .and_then(|meta| meta.modified().ok())
            .and_then(|modified| modified.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|elapsed| elapsed.as_nanos())
        {
            return Some(nanos.try_into().unwrap_or(i64::MAX));
        }
    }
    None
}

pub(crate) fn generation_directories(root: &Path) -> Vec<PathBuf> {
    let mut directories = Vec::new();
    let Ok(feeds) = fs::read_dir(root) else {
        return directories;
    };
    for feed in feeds.flatten() {
        let Ok(ranges) = fs::read_dir(feed.path()) else {
            continue;
        };
        for range in ranges.flatten() {
            let Ok(generations) = fs::read_dir(range.path()) else {
                continue;
            };
            for generation in generations.flatten() {
                let path = generation.path();
                if path
                    .file_name()
                    .is_some_and(|name| name.to_string_lossy().starts_with("generation-"))
                {
                    directories.push(path);
                }
            }
        }
    }
    directories
}

fn unix_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .try_into()
        .unwrap_or(i64::MAX)
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::active_range::{
        model::{OwnershipEpoch, RangeGeneration, RangeId},
        store::{manifest_path, seed_committed_history, segment_path, ActiveRangeDescriptor},
    };
    use std::sync::Mutex;

    fn descriptor() -> ActiveRangeDescriptor {
        ActiveRangeDescriptor {
            feed_id: Uuid::from_u128(1),
            range_id: RangeId::from_uuid(Uuid::from_u128(2)),
            generation: RangeGeneration::new(1),
            ownership_epoch: OwnershipEpoch::new(1),
        }
    }

    fn range_directory(root: &Path, descriptor: &ActiveRangeDescriptor) -> PathBuf {
        root.join(descriptor.feed_id.to_string())
            .join(descriptor.range_id.as_uuid().to_string())
            .join(format!("generation-{}", descriptor.generation.value()))
    }

    /// Seeds committed history in segment 0, then fabricates the
    /// post-rotation persisted state — `active_segment = 1` plus an empty
    /// tail file — so segment 0 stands sealed exactly as a real rotation
    /// leaves it. The manifest heals itself during the first upload.
    fn sealed_range_root(root: &Path, descriptor: &ActiveRangeDescriptor, entries: u64) -> PathBuf {
        seed_committed_history(root, descriptor, entries);
        let directory = range_directory(root, descriptor);
        let state_path = directory.join("range-state.json");
        let mut persisted: PersistedRangeState =
            serde_json::from_slice(&fs::read(&state_path).unwrap()).unwrap();
        persisted.active_segment = 1;
        fs::write(&state_path, serde_json::to_vec(&persisted).unwrap()).unwrap();
        File::create(segment_path(&directory, 1)).unwrap();
        directory
    }

    /// Counts `put` calls so adopt-vs-upload behavior is observable: a
    /// replica copy whose objects already exist must adopt, never rewrite.
    struct SpyStore {
        inner: FileSystemObjectStore,
        puts: Mutex<u64>,
    }

    impl SpyStore {
        fn open(root: &Path) -> Self {
            Self {
                inner: FileSystemObjectStore::open(root).unwrap(),
                puts: Mutex::new(0),
            }
        }

        fn puts(&self) -> u64 {
            *self.puts.lock().unwrap()
        }
    }

    impl ObjectStore for SpyStore {
        fn put(&self, key: &str, source: &Path) -> Result<u64, TierError> {
            *self.puts.lock().unwrap() += 1;
            self.inner.put(key, source)
        }

        fn get(&self, key: &str) -> Result<Vec<u8>, TierError> {
            self.inner.get(key)
        }

        fn head(&self, key: &str) -> Result<Option<u64>, TierError> {
            self.inner.head(key)
        }

        fn delete(&self, key: &str) -> Result<(), TierError> {
            self.inner.delete(key)
        }
    }

    #[test]
    fn filesystem_object_store_round_trips_and_refuses_traversal() {
        let root = tempfile::TempDir::new().unwrap();
        let store = FileSystemObjectStore::open(root.path()).unwrap();
        let source = tempfile::TempDir::new().unwrap();
        let file = source.path().join("payload.bin");
        fs::write(&file, b"sealed bytes").unwrap();

        assert_eq!(store.head("a/b/c.log").unwrap(), None);
        assert_eq!(store.put("a/b/c.log", &file).unwrap(), 12);
        assert_eq!(store.head("a/b/c.log").unwrap(), Some(12));
        assert_eq!(store.get("a/b/c.log").unwrap(), b"sealed bytes");
        assert!(store.put("../escape", &file).is_err());
        assert!(store.put("a//b", &file).is_err());
        assert!(store.get("missing").is_err());
        // No staging artifacts remain visible after a completed put.
        let staged: Vec<_> = fs::read_dir(root.path().join("a/b"))
            .unwrap()
            .flatten()
            .filter(|entry| entry.file_name().to_string_lossy().contains(".part"))
            .collect();
        assert!(staged.is_empty());
    }

    #[test]
    fn sealed_segment_uploads_verified_artifacts_under_content_keys() {
        let ranges = tempfile::TempDir::new().unwrap();
        let objects = tempfile::TempDir::new().unwrap();
        let descriptor = descriptor();
        let directory = sealed_range_root(ranges.path(), &descriptor, 20);
        let store = FileSystemObjectStore::open(objects.path()).unwrap();

        let report = tier_scan_once(ranges.path(), &store, &|_| None, &|_| None);
        assert_eq!(report.uploaded, 1);
        assert!(report.failed.is_empty(), "{:?}", report.failed);

        let local_segment = fs::read(segment_path(&directory, 0)).unwrap();
        let digest = blake3::hash(&local_segment).to_hex().to_string();
        let marker_path = tier_marker_path(&directory, 0);
        assert!(marker_path.exists());
        let marker: TierMarker = serde_json::from_slice(&fs::read(marker_path).unwrap()).unwrap();
        assert_eq!(marker.segment, 0);
        assert_eq!(marker.bytes as usize, local_segment.len());
        assert_eq!(marker.segment_digest, digest);
        // Seeded history bypasses a real seal, so no index checkpoint
        // exists to publish — the key is honestly absent.
        assert!(marker.checkpoint_key.is_none());

        // Remote bytes are identical, named by the segment's own digest.
        assert_eq!(store.get(&marker.object_key).unwrap(), local_segment);
        assert_eq!(
            store.get(&marker.manifest_key).unwrap(),
            fs::read(manifest_path(&directory, 0)).unwrap()
        );
        assert!(marker.object_key.contains(&digest));

        // The healed manifest matches the sealed coverage.
        let manifest: SegmentManifest =
            serde_json::from_slice(&store.get(&marker.manifest_key).unwrap()).unwrap();
        assert_eq!(manifest.entries, 20);
        assert_eq!(manifest.first_position, 1);
        assert_eq!(manifest.last_position, 20);

        // A second tick recognizes the marker and does nothing.
        let again = tier_scan_once(ranges.path(), &store, &|_| None, &|_| None);
        assert_eq!(again.uploaded, 0);
        assert_eq!(again.already_tiered, 1);
        assert!(again.failed.is_empty());
    }

    #[test]
    fn replica_copies_adopt_identical_objects_without_rewriting() {
        let replica_one = tempfile::TempDir::new().unwrap();
        let replica_two = tempfile::TempDir::new().unwrap();
        let objects = tempfile::TempDir::new().unwrap();
        let descriptor = descriptor();
        let directory_one = sealed_range_root(replica_one.path(), &descriptor, 12);

        // A second RF3 replica holds the same sealed bytes in its own root.
        let directory_two = range_directory(replica_two.path(), &descriptor);
        fs::create_dir_all(&directory_two).unwrap();
        for entry in fs::read_dir(&directory_one).unwrap().flatten() {
            let name = entry.file_name();
            if name.to_string_lossy().starts_with("segment-tier-") {
                continue;
            }
            fs::copy(entry.path(), directory_two.join(&name)).unwrap();
        }

        let first = FileSystemObjectStore::open(objects.path()).unwrap();
        let report = tier_scan_once(replica_one.path(), &first, &|_| None, &|_| None);
        assert_eq!(report.uploaded, 1);
        let remote_len = first.head(
            &serde_json::from_slice::<TierMarker>(
                &fs::read(tier_marker_path(&directory_one, 0)).unwrap(),
            )
            .unwrap()
            .object_key,
        );

        // Replica two scans the same object keys: all adopted, zero puts.
        let spy = SpyStore::open(objects.path());
        let second = tier_scan_once(replica_two.path(), &spy, &|_| None, &|_| None);
        assert_eq!(second.uploaded, 1, "{:?}", second.failed);
        assert_eq!(spy.puts(), 0, "replica must adopt, not rewrite");
        assert!(tier_marker_path(&directory_two, 0).exists());
        assert!(remote_len.unwrap().is_some());
    }

    #[test]
    fn conflicting_remote_content_is_refused_without_a_marker() {
        let ranges = tempfile::TempDir::new().unwrap();
        let objects = tempfile::TempDir::new().unwrap();
        let descriptor = descriptor();
        let directory = sealed_range_root(ranges.path(), &descriptor, 8);
        let store = FileSystemObjectStore::open(objects.path()).unwrap();

        // Pre-place different bytes under the digest-derived key: the tick
        // must refuse rather than overwrite or mark the upload complete.
        let local = fs::read(segment_path(&directory, 0)).unwrap();
        let key = format!(
            "{}/{}/generation-1/{}.log",
            descriptor.feed_id,
            descriptor.range_id.as_uuid(),
            blake3::hash(&local).to_hex()
        );
        let poison = tempfile::TempDir::new().unwrap();
        let poison_file = poison.path().join("poison");
        fs::write(&poison_file, b"not the segment").unwrap();
        store.put(&key, &poison_file).unwrap();

        let report = tier_scan_once(ranges.path(), &store, &|_| None, &|_| None);
        assert_eq!(report.uploaded, 0);
        assert_eq!(report.failed.len(), 1);
        assert!(report.failed[0].contains("different content"));
        assert!(!tier_marker_path(&directory, 0).exists());
    }

    #[test]
    fn a_missing_manifest_heals_during_upload() {
        let ranges = tempfile::TempDir::new().unwrap();
        let objects = tempfile::TempDir::new().unwrap();
        let descriptor = descriptor();
        // Seeded committed history never went through a real seal, so no
        // manifest exists — the upload must heal it from the sealed bytes.
        let directory = sealed_range_root(ranges.path(), &descriptor, 6);
        assert!(!manifest_path(&directory, 0).exists());

        let store = FileSystemObjectStore::open(objects.path()).unwrap();
        let report = tier_scan_once(ranges.path(), &store, &|_| None, &|_| None);
        assert_eq!(report.uploaded, 1, "{:?}", report.failed);
        // Healed manifest binds the file's real digest.
        let manifest: SegmentManifest =
            serde_json::from_slice(&fs::read(manifest_path(&directory, 0)).unwrap()).unwrap();
        assert_eq!(
            manifest.segment_digest,
            blake3::hash(&fs::read(segment_path(&directory, 0)).unwrap())
                .to_hex()
                .to_string()
        );
    }

    #[test]
    fn the_active_tail_is_never_uploaded() {
        let ranges = tempfile::TempDir::new().unwrap();
        let objects = tempfile::TempDir::new().unwrap();
        let descriptor = descriptor();
        let directory = sealed_range_root(ranges.path(), &descriptor, 4);
        let store = FileSystemObjectStore::open(objects.path()).unwrap();
        tier_scan_once(ranges.path(), &store, &|_| None, &|_| None);

        // Segment 1 is the open tail: no manifest, no marker, no remote key.
        assert!(!tier_marker_path(&directory, 1).exists());
        assert!(!manifest_path(&directory, 1).exists());
    }

    // --- TierSource: remote fetch, single-flight, verification ---

    use crate::active_range::store::RemoteSegmentSource;
    use std::time::Duration;

    /// A `get` that sleeps before serving — the only way to prove
    /// concurrent fetches dedupe is to leave the first fetch in flight
    /// long enough for the rest to arrive.
    struct SlowGetStore {
        inner: FileSystemObjectStore,
        gets: Mutex<u64>,
        delay: Duration,
    }

    impl std::fmt::Debug for SlowGetStore {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("SlowGetStore")
        }
    }

    impl ObjectStore for SlowGetStore {
        fn put(&self, key: &str, source: &Path) -> Result<u64, TierError> {
            self.inner.put(key, source)
        }

        fn get(&self, key: &str) -> Result<Vec<u8>, TierError> {
            *self.gets.lock().unwrap() += 1;
            std::thread::sleep(self.delay);
            self.inner.get(key)
        }

        fn head(&self, key: &str) -> Result<Option<u64>, TierError> {
            self.inner.head(key)
        }

        fn delete(&self, key: &str) -> Result<(), TierError> {
            self.inner.delete(key)
        }
    }

    /// A manifest + marker + remote object for one synthetic sealed span —
    /// the provenance `fetch_sealed` requires, without a full range.
    fn staged_remote_segment(
        directory: &Path,
        objects: &FileSystemObjectStore,
        descriptor: &ActiveRangeDescriptor,
        segment: u64,
        bytes: &[u8],
    ) -> SegmentManifest {
        let manifest = SegmentManifest {
            version: 1,
            feed_id: descriptor.feed_id,
            range_id: descriptor.range_id,
            generation: descriptor.generation,
            segment,
            bytes: bytes.len() as u64,
            entries: 0,
            first_position: 0,
            last_position: 0,
            last_ingest_time_ns: Some(0),
            segment_digest: blake3::hash(bytes).to_hex().to_string(),
        };
        let staging = tempfile::TempDir::new().unwrap();
        let source_path = staging.path().join("segment.log");
        fs::write(&source_path, bytes).unwrap();
        objects
            .put(&segment_object_key(&manifest), &source_path)
            .unwrap();
        write_tier_marker(
            directory,
            &TierMarker {
                version: TIER_MARKER_VERSION,
                segment,
                object_key: segment_object_key(&manifest),
                manifest_key: manifest_object_key(&manifest),
                checkpoint_key: None,
                bytes: manifest.bytes,
                segment_digest: manifest.segment_digest.clone(),
                uploaded_at_ns: 0,
            },
        )
        .unwrap();
        manifest
    }

    #[test]
    fn concurrent_fetches_share_one_object_read() {
        let directory = tempfile::TempDir::new().unwrap();
        let objects_root = tempfile::TempDir::new().unwrap();
        let objects = FileSystemObjectStore::open(objects_root.path()).unwrap();
        let descriptor = descriptor();
        let manifest = staged_remote_segment(
            directory.path(),
            &objects,
            &descriptor,
            0,
            b"sealed segment bytes",
        );
        let slow = Arc::new(SlowGetStore {
            inner: FileSystemObjectStore::open(objects_root.path()).unwrap(),
            gets: Mutex::new(0),
            delay: Duration::from_millis(50),
        });
        let source = Arc::new(TierSource::with_store(slow.clone(), 1 << 20));

        let mut handles = Vec::new();
        for _ in 0..4 {
            let source = source.clone();
            let directory = directory.path().to_path_buf();
            let manifest = manifest.clone();
            handles.push(std::thread::spawn(move || {
                source.fetch_sealed(&directory, 0, &manifest)
            }));
        }
        for handle in handles {
            assert!(handle.join().unwrap().is_ok());
        }
        assert_eq!(
            *slow.gets.lock().unwrap(),
            1,
            "concurrent fetches stampeded"
        );
    }

    #[test]
    fn fetch_refuses_a_span_without_verified_remote_provenance() {
        let directory = tempfile::TempDir::new().unwrap();
        let objects_root = tempfile::TempDir::new().unwrap();
        let objects = FileSystemObjectStore::open(objects_root.path()).unwrap();
        let descriptor = descriptor();
        let manifest = staged_remote_segment(
            directory.path(),
            &objects,
            &descriptor,
            0,
            b"sealed segment bytes",
        );
        fs::remove_file(tier_marker_path(directory.path(), 0)).unwrap();
        let source = TierSource::with_store(
            Arc::new(FileSystemObjectStore::open(objects_root.path()).unwrap()),
            1 << 20,
        );
        let error = source.fetch_sealed(directory.path(), 0, &manifest);
        assert!(
            matches!(error, Err(ActiveRangeStoreError::RemoteFetch(_))),
            "fetch without a marker must fail closed: {error:?}"
        );
    }

    #[test]
    fn fetch_rejects_remote_bytes_that_forged_the_manifest_digest() {
        let directory = tempfile::TempDir::new().unwrap();
        let objects_root = tempfile::TempDir::new().unwrap();
        let objects = FileSystemObjectStore::open(objects_root.path()).unwrap();
        let descriptor = descriptor();
        let manifest = staged_remote_segment(
            directory.path(),
            &objects,
            &descriptor,
            0,
            b"sealed segment bytes",
        );
        // Overwrite the content-addressed object with different bytes.
        fs::write(
            objects_root.path().join(segment_object_key(&manifest)),
            b"forged replacement",
        )
        .unwrap();
        let source = TierSource::with_store(
            Arc::new(FileSystemObjectStore::open(objects_root.path()).unwrap()),
            1 << 20,
        );
        let error = source.fetch_sealed(directory.path(), 0, &manifest);
        assert!(
            matches!(error, Err(ActiveRangeStoreError::RemoteFetch(_))),
            "forged remote bytes must fail digest verification: {error:?}"
        );
        assert!(!cache_segment_path(directory.path(), 0).exists());
    }

    #[test]
    fn tier_scan_defers_segments_inside_the_hot_local_window() {
        let ranges = tempfile::TempDir::new().unwrap();
        let objects = tempfile::TempDir::new().unwrap();
        let descriptor = descriptor();
        sealed_range_root(ranges.path(), &descriptor, 8);
        let store = FileSystemObjectStore::open(objects.path()).unwrap();
        let feed_id = descriptor.feed_id;

        // A fresh segment inside the declared hot-local window must not
        // tier — the scan defers it until the policy's duration passes.
        let hot = tier_scan_once(
            ranges.path(),
            &store,
            &|feed| {
                (feed == feed_id).then(|| HistoryPolicy {
                    tier_after_ns: Some(30 * 24 * 60 * 60 * 1_000_000_000),
                    ..HistoryPolicy::default()
                })
            },
            &|_| None,
        );
        assert_eq!(hot.uploaded, 0, "{:?}", hot.failed);
        assert_eq!(hot.hot_deferred, 1);

        // The same segment past its hot window tiers normally.
        let aged = tier_scan_once(
            ranges.path(),
            &store,
            &|feed| {
                (feed == feed_id).then(|| HistoryPolicy {
                    tier_after_ns: Some(1),
                    ..HistoryPolicy::default()
                })
            },
            &|_| None,
        );
        assert_eq!(aged.uploaded, 1, "{:?}", aged.failed);
        assert_eq!(aged.hot_deferred, 0);

        // No declared bound anywhere — `None` and an empty policy both
        // mean the built-in default: tier immediately.
        let orphan = tempfile::TempDir::new().unwrap();
        sealed_range_root(orphan.path(), &descriptor, 8);
        let unresolved = tier_scan_once(orphan.path(), &store, &|_| None, &|_| None);
        assert_eq!(unresolved.uploaded, 1, "{:?}", unresolved.failed);
        assert_eq!(unresolved.hot_deferred, 0);
    }
}
