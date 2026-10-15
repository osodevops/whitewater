use std::{
    fs,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::active_range::StorageNodeId;

/// The data-plane roles one Node declares. The Control Plane voter role
/// is deliberately absent: voter membership derives from the configured
/// Control topology and joint consensus, never from a capability label a
/// Node could assert about itself.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeCapabilities {
    /// May hold Feed range, Subscription progress, and StateStore replicas.
    /// `false` marks a compute/gateway-only Node (the burst default) which
    /// placement must never target.
    #[serde(default)]
    pub storage: bool,
    /// May execute Pipe drives and other compute work.
    #[serde(default)]
    pub compute: bool,
    /// May serve client gateway traffic.
    #[serde(default)]
    pub gateway: bool,
}

impl Default for NodeCapabilities {
    /// A Node that predates capability descriptors ran every data-plane
    /// role; an absent descriptor keeps it fully eligible.
    fn default() -> Self {
        Self {
            storage: true,
            compute: true,
            gateway: true,
        }
    }
}

impl NodeCapabilities {
    /// Every data-plane role enabled.
    pub fn all() -> Self {
        Self::default()
    }

    /// Only the roles a burst or ephemeral Node may safely run: work that
    /// can be abandoned without a durability consequence.
    pub fn stateless() -> Self {
        Self {
            storage: false,
            compute: true,
            gateway: true,
        }
    }

    /// Parse a capability subset (`storage,compute,gateway`; `|` also
    /// accepted to match WCL clause style). `voter` is refused because the
    /// Control Plane role derives from voter configuration rather than a
    /// self-asserted label.
    pub fn parse_list(value: &str) -> Result<Self, String> {
        let mut capabilities = Self {
            storage: false,
            compute: false,
            gateway: false,
        };
        let mut declared = false;
        for entry in value
            .split([',', '|'])
            .map(str::trim)
            .filter(|e| !e.is_empty())
        {
            declared = true;
            match entry.to_ascii_lowercase().as_str() {
                "storage" => capabilities.storage = true,
                "compute" => capabilities.compute = true,
                "gateway" => capabilities.gateway = true,
                "voter" => {
                    return Err("voter is Control Plane configuration, not a capability".to_owned())
                }
                other => return Err(format!("unknown Node capability {other:?}")),
            }
        }
        if !declared {
            return Err("Node capabilities must name at least one role".to_owned());
        }
        Ok(capabilities)
    }
}

/// Self-reported Node resources, sampled on the Node itself. Zero values
/// mean the platform did not report; consumers must treat 0 as unknown
/// rather than empty.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeCapacity {
    /// Logical CPU cores visible to the process.
    pub cpu_cores: u32,
    /// Total physical memory bytes.
    pub memory_bytes: u64,
    /// Total bytes of the volume holding the data directory.
    pub disk_total_bytes: u64,
    /// Bytes currently free on that volume.
    pub disk_free_bytes: u64,
    /// Sum of physical interface link rates in megabits per second
    /// (sysfs `speed`); 0 means the platform did not report. A link's
    /// measured throughput belongs to pressure, not capacity.
    #[serde(default)]
    pub network_mbps: u64,
    /// Operator-declared failure domain (zone/rack/host label); empty means
    /// unknown, in which case placement must treat every Node as sharing a
    /// domain.
    #[serde(default)]
    pub failure_domain: String,
}

impl NodeCapacity {
    /// Measure the resources this Node actually has right now. The result
    /// is evidence, not authority: gossip carries it and placement may
    /// consult it, but a stale or absent report must fail toward
    /// conservative placement rather than trust.
    pub fn sample(data_dir: &Path, failure_domain: String) -> Self {
        Self {
            cpu_cores: std::thread::available_parallelism().map_or(0, |count| count.get() as u32),
            memory_bytes: total_memory_bytes(),
            disk_total_bytes: fs2::total_space(data_dir).unwrap_or(0),
            disk_free_bytes: fs2::free_space(data_dir).unwrap_or(0),
            network_mbps: total_link_mbps(),
            failure_domain,
        }
    }
}

/// `/sys/class/net/*/speed` reports each link's negotiated rate in Mb/s;
/// virtual and down links report `-1` or nothing and are skipped.
#[cfg(target_os = "linux")]
fn total_link_mbps() -> u64 {
    fs::read_dir("/sys/class/net")
        .ok()
        .into_iter()
        .flatten()
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| {
            let speed = fs::read_to_string(entry.path().join("speed")).ok()?;
            speed.trim().parse::<u64>().ok()
        })
        .sum()
}

#[cfg(not(target_os = "linux"))]
fn total_link_mbps() -> u64 {
    0
}

/// `/proc/meminfo` reports MemTotal in kibibytes.
#[cfg(target_os = "linux")]
fn total_memory_bytes() -> u64 {
    fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|body| {
            body.lines().find_map(|line| {
                let rest = line.strip_prefix("MemTotal:")?;
                rest.trim().strip_suffix("kB")?.trim().parse::<u64>().ok()
            })
        })
        .map_or(0, |kib| kib.saturating_mul(1024))
}

#[cfg(not(target_os = "linux"))]
fn total_memory_bytes() -> u64 {
    // Portable memory probing is not wired for this platform yet; 0 means
    // unreported rather than zero.
    0
}

/// Name of the marker file that binds a data directory to a storage
/// identity. The file is what survives container replacement: a new
/// container that mounts the same volume resumes the same identity, so a
/// persistent volume reattach is a re-registration rather than a new Node.
pub const NODE_IDENTITY_FILE: &str = "node-identity.json";

#[derive(Serialize, Deserialize)]
struct StoredNodeIdentity {
    storage_node_id: StorageNodeId,
    written_at_ns: i64,
}

/// Resolve the durable storage identity for this data directory.
///
/// - A volume that already carries an identity returns it. When the
///   configuration also names an identity they must agree; a mismatch means
///   a fresh container is claiming a foreign volume and startup fails
///   closed.
/// - A volume without an identity adopts the configured one by persisting
///   it, so the next container on this volume inherits the identity even if
///   the environment forgets it.
/// - A Node with no storage identity at all (gateway/compute only) leaves
///   the volume unbound.
pub fn resolve_durable_identity(
    data_dir: &Path,
    configured: Option<StorageNodeId>,
) -> Result<Option<StorageNodeId>> {
    let path = data_dir.join(NODE_IDENTITY_FILE);
    if path.exists() {
        let stored: StoredNodeIdentity = serde_json::from_slice(
            &fs::read(&path).context("data directory node identity is unreadable")?,
        )
        .context("data directory node identity is corrupt; repair or retire the volume")?;
        if let Some(configured) = configured {
            anyhow::ensure!(
                configured == stored.storage_node_id,
                "data directory is bound to storage Node {} but configuration resolved {}; \
                 refusing to start with a mismatched volume",
                stored.storage_node_id,
                configured,
            );
        }
        return Ok(Some(stored.storage_node_id));
    }
    let Some(identity) = configured else {
        return Ok(None);
    };
    fs::create_dir_all(data_dir).context("create data directory for node identity")?;
    let stored = StoredNodeIdentity {
        storage_node_id: identity.clone(),
        written_at_ns: unix_ns(),
    };
    fs::write(&path, serde_json::to_vec_pretty(&stored)?)
        .context("persist data directory node identity")?;
    Ok(Some(identity))
}

fn unix_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capability_lists_parse_and_refuse_voter_labels() {
        let parsed = NodeCapabilities::parse_list("storage,compute").unwrap();
        assert!(parsed.storage && parsed.compute && !parsed.gateway);
        // WCL-style pipe separators parse identically.
        assert_eq!(
            NodeCapabilities::parse_list("storage|gateway").unwrap(),
            NodeCapabilities {
                storage: true,
                compute: false,
                gateway: true,
            }
        );
        // The voter role is control topology, never a declared capability.
        assert!(NodeCapabilities::parse_list("voter").is_err());
        assert!(NodeCapabilities::parse_list("storage,quantum").is_err());
        assert!(NodeCapabilities::parse_list("").is_err());
        // A Node declaring no descriptor is legacy: every role.
        assert!(NodeCapabilities::default().storage);
        // A burst profile drops storage but keeps abandonable work.
        let stateless = NodeCapabilities::stateless();
        assert!(!stateless.storage && stateless.compute && stateless.gateway);
    }

    #[test]
    fn capability_descriptors_deserialize_partials_as_exact() {
        // An explicit descriptor is exact: unlisted roles are off.
        let parsed: NodeCapabilities = serde_json::from_str(r#"{"storage": true}"#).unwrap();
        assert!(parsed.storage && !parsed.compute && !parsed.gateway);
    }

    #[test]
    fn durable_identity_persists_adopts_and_refuses_mismatch() {
        let dir = tempfile::TempDir::new().unwrap();
        let storage = StorageNodeId::try_new("storage-7").unwrap();
        // First boot binds the configured identity to the volume.
        assert_eq!(
            resolve_durable_identity(dir.path(), Some(storage.clone())).unwrap(),
            Some(storage.clone())
        );
        assert!(dir.path().join(NODE_IDENTITY_FILE).exists());
        // A replacement container with no configured identity adopts the
        // volume's persisted one — the PV reattach path.
        assert_eq!(
            resolve_durable_identity(dir.path(), None).unwrap(),
            Some(storage.clone())
        );
        // A replacement container configured as a different Node refuses
        // the foreign volume rather than stealing its identity.
        let other = StorageNodeId::try_new("storage-8").unwrap();
        assert!(resolve_durable_identity(dir.path(), Some(other)).is_err());
        // A stateless Node on a fresh volume binds nothing.
        let empty = tempfile::TempDir::new().unwrap();
        assert!(resolve_durable_identity(empty.path(), None)
            .unwrap()
            .is_none());
        assert!(!empty.path().join(NODE_IDENTITY_FILE).exists());
        // A corrupt identity file fails closed.
        fs::write(dir.path().join(NODE_IDENTITY_FILE), b"not json").unwrap();
        assert!(resolve_durable_identity(dir.path(), None).is_err());
    }

    #[test]
    fn capacity_sampling_reports_cores_and_volume_space() {
        let dir = tempfile::TempDir::new().unwrap();
        let capacity = NodeCapacity::sample(dir.path(), "zone-a".to_owned());
        assert!(capacity.cpu_cores > 0);
        assert!(capacity.disk_total_bytes > 0);
        assert!(capacity.disk_free_bytes <= capacity.disk_total_bytes);
        assert_eq!(capacity.failure_domain, "zone-a");
        #[cfg(target_os = "linux")]
        assert!(capacity.memory_bytes > 0);
    }
}
