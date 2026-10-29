//! Per-Node crash-consistent backup and boot-time restore.
//!
//! A backup is a copy of the Node's data directory taken while it runs.
//! Every store under that directory already tolerates a power loss mid-
//! write — fjall trees recover to their last sync, segment files replay
//! to their last intact frame, the raft log writes atomically — so a
//! file-level copy restores exactly the state an unclean shutdown would
//! leave behind, and RF3 repair converges whatever the cut missed.
//!
//! Two details make the copy better than a blind `cp`:
//!
//! * The catalog is written from `ControlController::snapshot_bytes()`
//!   (the committed in-memory state) rather than copying the on-disk
//!   file, so the backup always carries one complete catalog revision.
//! * Copy order puts data first and `manifest.json` last — a reader
//!   that finds the manifest finds a complete backup.
//!
//! Restore is fail-closed: it only runs into an empty data directory —
//! an existing Riverbed's volumes are never overwritten implicitly.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::control::CATALOG_SCHEMA_VERSION;

/// Manifest filename — written last so its presence marks completeness.
pub const MANIFEST_FILE: &str = "manifest.json";
/// Bump when the on-disk backup layout changes incompatibly.
pub const BACKUP_FORMAT_VERSION: u32 = 1;

/// What the Node was, when the backup was taken, and how much it holds.
/// The manifest is the operator-facing receipt for a backup and the
/// restore-side guard: a restore refuses anything it cannot interpret.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct BackupManifest {
    pub format_version: u32,
    /// Durable storage identity bound to the volume, when this Node has
    /// one; Control-Plane-only Nodes restore without it.
    pub storage_node_id: Option<String>,
    pub build: String,
    pub protocol_version: u32,
    /// Schema version of the catalog inside this backup — restore
    /// refuses versions this binary cannot read (rollback boundary).
    pub catalog_schema_version: u32,
    /// Committed catalog revision at snapshot time — the backup's
    /// logical "as-of" cut.
    pub catalog_revision: u64,
    pub taken_at_ns: i64,
    pub files: u64,
    pub bytes: u64,
}

#[derive(Debug)]
pub enum BackupError {
    Io(io::Error),
    Invalid(String),
}

impl std::fmt::Display for BackupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(f, "{error}"),
            Self::Invalid(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for BackupError {}

impl From<io::Error> for BackupError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// Canonical prefix test that also works for a `path` that does not
/// exist yet: `canonicalize` fails on missing paths, and on Windows a
/// canonical root (`\\?\C:\…`) never prefixes a verbatim relative form —
/// so fall back to canonicalizing the nearest existing ancestor and
/// appending the remainder lexically.
fn is_relative_to(path: &Path, root: &Path) -> bool {
    let resolved = match path.ancestors().find(|candidate| candidate.exists()) {
        Some(existing) => {
            let mut resolved = existing
                .canonicalize()
                .unwrap_or_else(|_| existing.to_path_buf());
            if let Ok(rest) = path.strip_prefix(existing) {
                resolved = resolved.join(rest);
            }
            resolved
        }
        None => path.to_path_buf(),
    };
    let root = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
    resolved.starts_with(root)
}

/// Copy `source` into `destination` recursively, skipping transient
/// files (`*.tmp` atomic-write scratch) and `except` — the catalog file
/// name, whose bytes come from the committed snapshot instead of disk.
/// Returns (files, bytes) copied.
fn copy_tree(
    source: &Path,
    destination: &Path,
    except: &str,
    counts: &mut (u64, u64),
) -> Result<(), BackupError> {
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name == except || name.ends_with(".tmp") || name == MANIFEST_FILE {
            continue;
        }
        let target = destination.join(name.as_ref());
        if entry.file_type()?.is_dir() {
            fs::create_dir_all(&target)?;
            copy_tree(&entry.path(), &target, except, counts)?;
        } else if entry.file_type()?.is_file() {
            let bytes = fs::copy(entry.path(), &target)?;
            counts.0 += 1;
            counts.1 += bytes;
        }
    }
    Ok(())
}

/// Take a crash-consistent copy of `data_dir` into `destination` (which
/// must not already hold data). `catalog_snapshot` replaces the on-disk
/// catalog file so the backup always carries one committed revision.
/// The destination must live outside `data_dir` — a backup inside the
/// directory it copies is self-referential.
pub fn create_backup(
    data_dir: &Path,
    destination: &Path,
    catalog_snapshot: &[u8],
    manifest: BackupManifest,
) -> Result<BackupManifest, BackupError> {
    if !destination.is_absolute() {
        return Err(BackupError::Invalid(
            "backup destination must be an absolute path".to_owned(),
        ));
    }
    if is_relative_to(destination, data_dir) || is_relative_to(data_dir, destination) {
        return Err(BackupError::Invalid(
            "backup destination must not overlap the data directory".to_owned(),
        ));
    }
    if destination.exists() && fs::read_dir(destination)?.next().is_some() {
        return Err(BackupError::Invalid(format!(
            "backup destination {} is not empty",
            destination.display()
        )));
    }
    fs::create_dir_all(destination)?;
    let mut counts = (0u64, 0u64);
    // Data first: range directories, fjall trees, streams — copied
    // verbatim; each store replays torn tails exactly like a crash.
    copy_tree(
        data_dir,
        destination,
        "control-plane-catalog.json",
        &mut counts,
    )?;
    // Metadata last: the committed catalog snapshot, then the manifest
    // that marks the backup complete.
    fs::write(
        destination.join("control-plane-catalog.json"),
        catalog_snapshot,
    )?;
    counts.0 += 1;
    counts.1 += catalog_snapshot.len() as u64;
    let manifest = BackupManifest {
        files: counts.0,
        bytes: counts.1,
        ..manifest
    };
    fs::write(
        destination.join(MANIFEST_FILE),
        serde_json::to_vec_pretty(&manifest)
            .map_err(|error| BackupError::Invalid(error.to_string()))?,
    )?;
    Ok(manifest)
}

/// Marker written after a successful restore: its presence proves the
/// data directory's contents came from a restore, so a restart with
/// `WHITEWATER_RESTORE_FROM` still set resumes idempotently instead of
/// either clobbering live state or refusing to boot.
pub const RESTORED_MARKER: &str = ".restored-manifest.json";

/// What `restore_backup` did.
#[derive(Debug)]
pub enum RestoreOutcome {
    /// The backup was copied in fresh.
    Restored(BackupManifest),
    /// A previous restore's marker was found — nothing copied.
    AlreadyRestored(BackupManifest),
}

/// Restore a backup created by [`create_backup`] into an *empty*
/// `data_dir`. Refuses to touch a directory that already holds state
/// unless that state is itself the product of a restore (marker file) —
/// restoring over a live volume would destroy the very thing it exists
/// to protect.
pub fn restore_backup(data_dir: &Path, source: &Path) -> Result<RestoreOutcome, BackupError> {
    if data_dir.exists() && fs::read_dir(data_dir)?.next().is_some() {
        let marker = data_dir.join(RESTORED_MARKER);
        return match fs::read(&marker) {
            Ok(bytes) => {
                let manifest: BackupManifest = serde_json::from_slice(&bytes).map_err(|error| {
                    BackupError::Invalid(format!("restore marker is unreadable: {error}"))
                })?;
                Ok(RestoreOutcome::AlreadyRestored(manifest))
            }
            Err(_) => Err(BackupError::Invalid(format!(
                "refusing to restore into non-empty data directory {}",
                data_dir.display()
            ))),
        };
    }
    let manifest_path = source.join(MANIFEST_FILE);
    let manifest: BackupManifest =
        serde_json::from_slice(&fs::read(&manifest_path).map_err(|_| {
            BackupError::Invalid(format!(
                "{} is not a Whitewater backup (no {MANIFEST_FILE})",
                source.display()
            ))
        })?)
        .map_err(|error| BackupError::Invalid(format!("backup manifest is unreadable: {error}")))?;
    if manifest.format_version > BACKUP_FORMAT_VERSION {
        return Err(BackupError::Invalid(format!(
            "backup format version {} is newer than this binary supports ({BACKUP_FORMAT_VERSION})",
            manifest.format_version
        )));
    }
    if manifest.catalog_schema_version > CATALOG_SCHEMA_VERSION {
        return Err(BackupError::Invalid(format!(
            "backup catalog schema version {} is newer than this binary supports \
             ({CATALOG_SCHEMA_VERSION}): restoring it would roll back across an \
             irreversible catalog migration",
            manifest.catalog_schema_version
        )));
    }
    fs::create_dir_all(data_dir)?;
    let mut counts = (0u64, 0u64);
    // The catalog lands as a plain file copy — the backup already holds
    // the committed snapshot bytes under the runtime filename.
    copy_tree(source, data_dir, "", &mut counts)?;
    // The marker makes a restart with the env still set idempotent —
    // and distinguishes "dir came from restore" from arbitrary state.
    fs::write(
        data_dir.join(RESTORED_MARKER),
        serde_json::to_vec_pretty(&manifest)
            .map_err(|error| BackupError::Invalid(error.to_string()))?,
    )?;
    Ok(RestoreOutcome::Restored(manifest))
}

/// The data directory a [`crate::control::ControlController`] works
/// under — the catalog file always sits at its root.
pub fn data_dir_for_catalog(catalog_path: &Path) -> PathBuf {
    catalog_path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| catalog_path.to_path_buf())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(path: &Path, contents: &str) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, contents).unwrap();
    }

    fn fake_node_dir(root: &Path) {
        write(
            &root.join("control-plane-catalog.json"),
            r#"{"stale":true}"#,
        );
        write(&root.join("control-plane-raft.json"), r#"{"log":1}"#);
        write(
            &root.join("node-identity.json"),
            r#"{"node_id":"control-1"}"#,
        );
        write(
            &root.join("active-ranges/range-a/00000000000000000000.log"),
            "frame-bytes",
        );
        write(
            &root.join("subscription-progress/data/partition/0001.sst"),
            "fjall-bytes",
        );
        // Atomic-write scratch never belongs in a backup.
        write(&root.join("control-plane-raft.tmp"), "partial");
    }

    #[test]
    fn backup_copies_data_and_substitutes_the_committed_catalog() {
        let source = tempfile::tempdir().unwrap();
        let destination = tempfile::tempdir().unwrap();
        fake_node_dir(source.path());
        let manifest = create_backup(
            source.path(),
            &destination.path().join("snap"),
            b"{\"committed\":true}",
            BackupManifest {
                format_version: BACKUP_FORMAT_VERSION,
                storage_node_id: Some("control-1".to_owned()),
                build: "0.1.0".to_owned(),
                protocol_version: 1,
                catalog_schema_version: 1,
                catalog_revision: 42,
                taken_at_ns: 7,
                files: 0,
                bytes: 0,
            },
        )
        .unwrap();
        let backup = destination.path().join("snap");
        assert_eq!(
            fs::read_to_string(backup.join("control-plane-catalog.json")).unwrap(),
            r#"{"committed":true}"#
        );
        assert!(backup
            .join("active-ranges/range-a/00000000000000000000.log")
            .exists());
        assert!(backup
            .join("subscription-progress/data/partition/0001.sst")
            .exists());
        assert!(!backup.join("control-plane-raft.tmp").exists());
        let on_disk: BackupManifest =
            serde_json::from_slice(&fs::read(backup.join(MANIFEST_FILE)).unwrap()).unwrap();
        assert_eq!(on_disk, manifest);
        assert_eq!(manifest.catalog_revision, 42);
        assert!(manifest.files >= 4);
        assert!(manifest.bytes > 0);
    }

    #[test]
    fn backup_refuses_overlapping_or_non_empty_destinations() {
        let source = tempfile::tempdir().unwrap();
        fake_node_dir(source.path());
        // Inside the data dir — self-referential.
        assert!(create_backup(
            source.path(),
            &source.path().join("backups/x"),
            b"{}",
            blank_manifest(),
        )
        .is_err());
        // Relative path.
        assert!(create_backup(
            source.path(),
            Path::new("relative/backup"),
            b"{}",
            blank_manifest(),
        )
        .is_err());
        // Non-empty destination.
        let occupied = tempfile::tempdir().unwrap();
        fs::write(occupied.path().join("existing"), "x").unwrap();
        assert!(create_backup(source.path(), occupied.path(), b"{}", blank_manifest(),).is_err());
    }

    fn blank_manifest() -> BackupManifest {
        BackupManifest {
            format_version: BACKUP_FORMAT_VERSION,
            storage_node_id: None,
            build: String::new(),
            protocol_version: 0,
            catalog_schema_version: 1,
            catalog_revision: 0,
            taken_at_ns: 0,
            files: 0,
            bytes: 0,
        }
    }

    #[test]
    fn restore_roundtrips_a_backup_into_an_empty_directory() {
        let source = tempfile::tempdir().unwrap();
        let backup_dir = tempfile::tempdir().unwrap();
        let restore_dir = tempfile::tempdir().unwrap();
        fake_node_dir(source.path());
        create_backup(
            source.path(),
            &backup_dir.path().join("snap"),
            b"{\"committed\":true}",
            blank_manifest(),
        )
        .unwrap();
        let outcome = restore_backup(
            &restore_dir.path().join("data"),
            &backup_dir.path().join("snap"),
        )
        .unwrap();
        assert!(matches!(outcome, RestoreOutcome::Restored(_)));
        let restored = restore_dir.path().join("data");
        assert_eq!(
            fs::read_to_string(restored.join("control-plane-catalog.json")).unwrap(),
            r#"{"committed":true}"#
        );
        assert!(restored.join("node-identity.json").exists());
        assert!(restored
            .join("active-ranges/range-a/00000000000000000000.log")
            .exists());
        assert!(restored
            .join("subscription-progress/data/partition/0001.sst")
            .exists());
        // The manifest is backup metadata, not Node state.
        assert!(!restored.join(MANIFEST_FILE).exists());
        // A restart with the env still set resumes idempotently — the
        // marker proves the dir's contents came from a restore.
        let again = restore_backup(
            &restore_dir.path().join("data"),
            &backup_dir.path().join("snap"),
        )
        .unwrap();
        assert!(matches!(again, RestoreOutcome::AlreadyRestored(_)));
        // The marker survives even when the source went away.
        assert!(matches!(
            restore_backup(&restore_dir.path().join("data"), Path::new("/gone")).unwrap(),
            RestoreOutcome::AlreadyRestored(_)
        ));
    }

    #[test]
    fn restore_refuses_to_clobber_an_existing_data_dir() {
        let backup_dir = tempfile::tempdir().unwrap();
        let source = tempfile::tempdir().unwrap();
        fake_node_dir(source.path());
        create_backup(
            source.path(),
            &backup_dir.path().join("snap"),
            b"{}",
            blank_manifest(),
        )
        .unwrap();
        let occupied = tempfile::tempdir().unwrap();
        write(&occupied.path().join("node-identity.json"), "{}");
        assert!(restore_backup(occupied.path(), &backup_dir.path().join("snap")).is_err());
    }

    #[test]
    fn restore_refuses_a_catalog_newer_than_the_binary() {
        let backup_dir = tempfile::tempdir().unwrap();
        let source = tempfile::tempdir().unwrap();
        fake_node_dir(source.path());
        let mut manifest = blank_manifest();
        manifest.catalog_schema_version = CATALOG_SCHEMA_VERSION + 1;
        create_backup(
            source.path(),
            &backup_dir.path().join("snap"),
            b"{}",
            manifest,
        )
        .unwrap();
        let restore_dir = tempfile::tempdir().unwrap();
        let error = restore_backup(
            &restore_dir.path().join("data"),
            &backup_dir.path().join("snap"),
        )
        .err()
        .unwrap();
        assert!(error.to_string().contains("irreversible"));
    }
}
