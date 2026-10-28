use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket},
    path::PathBuf,
    time::Duration,
};

use anyhow::{Context, Result};

use crate::{active_range::StorageNodeId, node::NodeCapabilities};

/// How the inter-Node plane authenticates and encrypts. `mtls` (the
/// default) requires the full `WHITEWATER_SUBSCRIPTION_MTLS_*` certificate
/// bundle — or an operator-verified orchestrator transport that satisfies
/// the same peer-identity, rotation, audit, and downgrade guarantees —
/// so a deployment never silently falls back to plaintext. `insecure`
/// is the explicit development/test opt-down that keeps the shared-key
/// prototype transport; it is a named posture, never an accident.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InternalTransport {
    Mtls,
    Insecure,
}

#[derive(Clone, Debug)]
pub struct SubscriptionMtlsConfig {
    pub bind_addr: SocketAddr,
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
    pub ca_path: PathBuf,
    pub peer_pins: BTreeMap<StorageNodeId, BTreeSet<[u8; 32]>>,
    pub peer_endpoints: BTreeMap<StorageNodeId, String>,
}

#[derive(Clone, Debug)]
pub struct NodeConfig {
    pub node_id: String,
    pub bind_addr: SocketAddr,
    pub advertise_url: String,
    pub seeds: Vec<String>,
    pub data_dir: PathBuf,
    pub heartbeat_interval: Duration,
    pub member_timeout: Duration,
    pub capacity: u32,
    pub control_node_id: Option<u64>,
    pub control_nodes: Vec<(u64, String)>,
    pub control_plane_key: Option<String>,
    /// This Node's storage identity on the internal data plane. Voters
    /// default to `control-N`; a Node without a Control Plane voter role
    /// sets it explicitly to serve as an independently registered storage
    /// Node.
    pub storage_node_id: Option<StorageNodeId>,
    /// Declared data-plane roles (`WHITEWATER_NODE_CAPABILITIES`, a
    /// `storage,compute,gateway` subset). Defaults to every role; a burst
    /// Node should run `compute,gateway` so placement never targets it.
    pub node_capabilities: NodeCapabilities,
    /// Voter ids still eligible for storage placement
    /// (`WHITEWATER_STORAGE_VOTERS`); `None` means every configured voter.
    /// The same list must be configured on every Node because each Node
    /// derives eligibility locally — a divergent view would diverge
    /// placement.
    pub storage_voters: Option<BTreeSet<u64>>,
    /// Operator-declared failure domain label reported with capacity
    /// signals (`WHITEWATER_FAILURE_DOMAIN`).
    pub failure_domain: String,
    /// How often this Node commits its measured capacity into the
    /// catalog (`WHITEWATER_CAPACITY_REPORT_MS`, default 15s). Reports
    /// only matter while a baseline policy is enabled.
    pub capacity_report_interval: Duration,
    /// Maximum age of a committed capacity report before the Node is
    /// excluded from placement (`WHITEWATER_CAPACITY_REPORT_MAX_AGE_MS`,
    /// default 0 = disabled). Must be identical on every Node because
    /// eligibility is derived inside deterministic apply.
    pub capacity_report_max_age: Duration,
    /// Minimum disk headroom a Node's committed report must show before
    /// placement may target it (`WHITEWATER_STORAGE_MIN_DISK_FREE_BYTES`,
    /// default 0 = disabled). Same shared-config requirement.
    pub min_disk_free_bytes: u64,
    pub subscription_mtls: Option<SubscriptionMtlsConfig>,
    /// Inter-Node transport posture (`WHITEWATER_INTERNAL_TRANSPORT`).
    /// Defaults to `InternalTransport::Mtls`, which refuses to boot
    /// without certificate material; `Insecure` is the explicit
    /// development/test opt-down.
    pub internal_transport: InternalTransport,
    /// Root of the object store sealed segments upload to
    /// (`WHITEWATER_OBJECT_STORE_DIR`). `None` disables tiering entirely;
    /// the filesystem implementation used by development and test
    /// topologies treats this as a shared or per-Node directory.
    pub object_store_dir: Option<PathBuf>,
    /// How often the tiering pass scans for newly sealed segments
    /// (`WHITEWATER_TIER_SCAN_MS`, default 30s).
    pub tier_scan_interval: Duration,
    /// Committed-segment rotation bound (`WHITEWATER_MAX_SEGMENT_BYTES`,
    /// default 256 MiB). Identical semantics on every replica; a divergent
    /// bound diverges segment boundaries and therefore manifest digests,
    /// so it must be configured uniformly.
    pub max_segment_bytes: u64,
    /// Per-range ceiling on cached remote sealed-segment bytes
    /// (`WHITEWATER_REMOTE_CACHE_BYTES`, default 1 GiB). Materialized
    /// segments beyond the budget demote back to remote coldest-first;
    /// zero disables materialization entirely, which fails historical
    /// reads on demoted ranges rather than caching unbounded.
    pub remote_cache_bytes: u64,
}

impl NodeConfig {
    pub fn from_env() -> Result<Self> {
        let bind_addr: SocketAddr = env::var("WHITEWATER_BIND_ADDR")
            .unwrap_or_else(|_| "0.0.0.0:7070".to_owned())
            .parse()
            .context("WHITEWATER_BIND_ADDR must be a socket address")?;
        let node_id = env::var("WHITEWATER_NODE_ID")
            .or_else(|_| env::var("HOSTNAME"))
            .unwrap_or_else(|_| uuid::Uuid::new_v4().to_string());
        let advertise_url = env::var("WHITEWATER_ADVERTISE_URL")
            .unwrap_or_else(|_| format!("http://{}:{}", discover_local_ip(), bind_addr.port()));
        let seeds = env::var("WHITEWATER_SEEDS")
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|seed| !seed.is_empty())
            .map(ToOwned::to_owned)
            .collect();
        let heartbeat_interval =
            Duration::from_millis(parse_env("WHITEWATER_HEARTBEAT_MS", 2_000)?);
        let member_timeout =
            Duration::from_millis(parse_env("WHITEWATER_MEMBER_TIMEOUT_MS", 10_000)?);
        if heartbeat_interval >= member_timeout {
            anyhow::bail!(
                "WHITEWATER_HEARTBEAT_MS must be lower than WHITEWATER_MEMBER_TIMEOUT_MS"
            );
        }
        let control_node_id = env::var("WHITEWATER_CONTROL_NODE_ID")
            .ok()
            .map(|value| value.parse::<u64>())
            .transpose()
            .context("WHITEWATER_CONTROL_NODE_ID must be an unsigned integer")?;
        let control_nodes =
            parse_control_nodes(&env::var("WHITEWATER_CONTROL_NODES").unwrap_or_default())?;
        let control_plane_key = env::var("WHITEWATER_CONTROL_PLANE_KEY").ok();
        validate_control_topology(
            control_node_id,
            &control_nodes,
            control_plane_key.as_deref(),
        )?;
        let storage_node_id = resolve_storage_node_id(
            env::var("WHITEWATER_STORAGE_NODE_ID").ok().as_deref(),
            control_node_id,
        )?;
        let node_capabilities = match env::var("WHITEWATER_NODE_CAPABILITIES") {
            Ok(value) => NodeCapabilities::parse_list(&value)
                .map_err(|error| anyhow::anyhow!("WHITEWATER_NODE_CAPABILITIES: {error}"))?,
            Err(_) => NodeCapabilities::all(),
        };
        let storage_voters = parse_storage_voters(
            &env::var("WHITEWATER_STORAGE_VOTERS").unwrap_or_default(),
            &control_nodes,
        )?;
        validate_capability_topology(
            &node_capabilities,
            control_node_id,
            storage_voters.as_ref(),
            env::var("WHITEWATER_STORAGE_NODE_ID").is_ok(),
            env::var("WHITEWATER_NODE_CAPABILITIES").is_ok(),
        )?;
        let failure_domain = env::var("WHITEWATER_FAILURE_DOMAIN")
            .unwrap_or_default()
            .trim()
            .to_owned();
        if failure_domain.len() > 128 {
            anyhow::bail!("WHITEWATER_FAILURE_DOMAIN must be at most 128 bytes");
        }
        let capacity_report_interval =
            Duration::from_millis(parse_env("WHITEWATER_CAPACITY_REPORT_MS", 15_000)?);
        let capacity_report_max_age =
            Duration::from_millis(parse_env("WHITEWATER_CAPACITY_REPORT_MAX_AGE_MS", 0)?);
        let min_disk_free_bytes = parse_env("WHITEWATER_STORAGE_MIN_DISK_FREE_BYTES", 0)?;
        let object_store_dir = env::var("WHITEWATER_OBJECT_STORE_DIR")
            .ok()
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
            .map(PathBuf::from);
        let tier_scan_interval =
            Duration::from_millis(parse_env("WHITEWATER_TIER_SCAN_MS", 30_000)?);
        let max_segment_bytes = parse_env("WHITEWATER_MAX_SEGMENT_BYTES", 256 * 1024 * 1024)?;
        if max_segment_bytes == 0 {
            anyhow::bail!("WHITEWATER_MAX_SEGMENT_BYTES must be greater than zero");
        }
        if tier_scan_interval.is_zero() {
            anyhow::bail!("WHITEWATER_TIER_SCAN_MS must be greater than zero");
        }
        let remote_cache_bytes = parse_env("WHITEWATER_REMOTE_CACHE_BYTES", 1024 * 1024 * 1024)?;
        let tls = [
            env::var("WHITEWATER_SUBSCRIPTION_MTLS_BIND").ok(),
            env::var("WHITEWATER_SUBSCRIPTION_MTLS_CERT").ok(),
            env::var("WHITEWATER_SUBSCRIPTION_MTLS_KEY").ok(),
            env::var("WHITEWATER_SUBSCRIPTION_MTLS_CA").ok(),
            env::var("WHITEWATER_SUBSCRIPTION_MTLS_PEER_PINS").ok(),
            env::var("WHITEWATER_SUBSCRIPTION_MTLS_ENDPOINTS").ok(),
        ];
        if tls.iter().any(Option::is_some) && !tls.iter().all(Option::is_some) {
            anyhow::bail!("Subscription mTLS bind, certificate, key, CA, peer pins and peer endpoints must be configured together");
        }
        let subscription_mtls = match tls {
            [Some(bind), Some(cert), Some(key), Some(ca), Some(pins), Some(endpoints)] => {
                let local_id = storage_node_id.clone().ok_or_else(|| {
                    anyhow::anyhow!("Subscription mTLS requires a configured storage Node identity")
                })?;
                let peer_pins = parse_peer_pins(&pins)?;
                let peer_endpoints = parse_peer_endpoints(&endpoints)?;
                if !peer_pins.contains_key(&local_id)
                    || !peer_endpoints.contains_key(&local_id)
                    || control_nodes.iter().any(|(node, _)| {
                        StorageNodeId::try_new(format!("control-{node}")).map_or(true, |id| {
                            !peer_pins.contains_key(&id) || !peer_endpoints.contains_key(&id)
                        })
                    })
                {
                    anyhow::bail!("Subscription mTLS peer pins and endpoints must include this Node and every configured Control Node");
                }

                Some(SubscriptionMtlsConfig {
                    bind_addr: bind
                        .parse()
                        .context("invalid Subscription mTLS bind address")?,
                    cert_path: PathBuf::from(cert),
                    key_path: PathBuf::from(key),
                    ca_path: PathBuf::from(ca),
                    peer_pins,
                    peer_endpoints,
                })
            }
            _ => None,
        };
        if subscription_mtls
            .as_ref()
            .is_some_and(|tls| tls.bind_addr == bind_addr)
        {
            anyhow::bail!("Subscription mTLS listener must not share the public HTTP bind address");
        }
        let internal_transport =
            parse_internal_transport(env::var("WHITEWATER_INTERNAL_TRANSPORT").ok())?;
        validate_internal_transport(internal_transport, subscription_mtls.as_ref())?;
        Ok(Self {
            node_id,
            bind_addr,
            advertise_url: advertise_url.trim_end_matches('/').to_owned(),
            seeds,
            data_dir: PathBuf::from(
                env::var("WHITEWATER_DATA_DIR").unwrap_or_else(|_| "./data".to_owned()),
            ),
            heartbeat_interval,
            member_timeout,
            capacity: parse_env("WHITEWATER_NODE_CAPACITY", 100)?,
            control_node_id,
            control_nodes,
            control_plane_key,
            storage_node_id,
            node_capabilities,
            storage_voters,
            failure_domain,
            capacity_report_interval,
            capacity_report_max_age,
            min_disk_free_bytes,
            subscription_mtls,
            internal_transport,
            object_store_dir,
            tier_scan_interval,
            max_segment_bytes,
            remote_cache_bytes,
        })
    }
}

fn parse_peer_pins(value: &str) -> Result<BTreeMap<StorageNodeId, BTreeSet<[u8; 32]>>> {
    let mut pins = BTreeMap::new();
    let mut fingerprints = BTreeSet::new();
    for entry in value
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
    {
        let (node, digests) = entry
            .split_once('@')
            .with_context(|| format!("invalid Subscription mTLS peer entry: {entry}"))?;
        let node = StorageNodeId::try_new(node)?;
        let mut accepted = BTreeSet::new();
        for digest in digests.split('|') {
            let fingerprint = *blake3::Hash::from_hex(digest)
                .with_context(|| {
                    format!("invalid Subscription mTLS certificate fingerprint for {node}")
                })?
                .as_bytes();
            if !fingerprints.insert(fingerprint) || !accepted.insert(fingerprint) {
                anyhow::bail!("duplicate Subscription mTLS certificate fingerprint");
            }
        }
        if accepted.is_empty() || pins.insert(node, accepted).is_some() {
            anyhow::bail!("duplicate Subscription mTLS Node ID or missing peer pins");
        }
    }
    if pins.is_empty() {
        anyhow::bail!("Subscription mTLS requires at least one peer pin");
    }
    Ok(pins)
}

fn parse_peer_endpoints(value: &str) -> Result<BTreeMap<StorageNodeId, String>> {
    let mut endpoints = BTreeMap::new();
    for entry in value
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
    {
        let (node, endpoint) = entry
            .split_once('@')
            .with_context(|| format!("invalid Subscription mTLS endpoint entry: {entry}"))?;
        let node = StorageNodeId::try_new(node)?;
        let endpoint = endpoint.trim_end_matches('/').to_owned();
        if endpoint.is_empty() {
            anyhow::bail!("Subscription mTLS endpoint for {node} is empty");
        }
        let url = reqwest::Url::parse(&endpoint)
            .with_context(|| format!("invalid Subscription mTLS endpoint for {node}"))?;
        if url.scheme() != "https" || url.host_str() != Some(node.as_str()) {
            anyhow::bail!("Subscription mTLS endpoint for {node} must be https://{node}:port");
        }
        if endpoints.insert(node.clone(), endpoint).is_some() {
            anyhow::bail!("duplicate Subscription mTLS endpoint for {node}");
        }
    }
    if endpoints.is_empty() {
        anyhow::bail!("Subscription mTLS requires at least one peer endpoint");
    }
    Ok(endpoints)
}

fn validate_control_topology(
    control_node_id: Option<u64>,
    control_nodes: &[(u64, String)],
    control_plane_key: Option<&str>,
) -> Result<()> {
    if control_node_id.is_some() && (control_nodes.is_empty() || control_plane_key.is_none()) {
        anyhow::bail!(
            "WHITEWATER_CONTROL_NODE_ID requires WHITEWATER_CONTROL_NODES and WHITEWATER_CONTROL_PLANE_KEY"
        );
    }
    if !control_nodes.is_empty() && control_plane_key.is_none() {
        anyhow::bail!("WHITEWATER_CONTROL_NODES requires WHITEWATER_CONTROL_PLANE_KEY");
    }
    if control_node_id.is_some_and(|id| !control_nodes.iter().any(|(peer, _)| *peer == id)) {
        anyhow::bail!("WHITEWATER_CONTROL_NODE_ID must appear in WHITEWATER_CONTROL_NODES");
    }
    Ok(())
}

/// Resolves this Node's storage identity on the internal data plane.
/// Voters derive `control-N`; a Node without a voter role sets
/// `WHITEWATER_STORAGE_NODE_ID` explicitly to serve as an independently
/// registered storage Node.
fn resolve_storage_node_id(
    explicit: Option<&str>,
    control_node_id: Option<u64>,
) -> Result<Option<StorageNodeId>> {
    let derived = control_node_id.map(|node_id| {
        StorageNodeId::try_new(format!("control-{node_id}"))
            .expect("control-N storage Node IDs are valid")
    });
    match explicit {
        Some(value) => {
            let identity = StorageNodeId::try_new(value.trim().to_owned())
                .context("WHITEWATER_STORAGE_NODE_ID must be a valid storage Node ID")?;
            if derived.is_some_and(|node| node != identity) {
                anyhow::bail!(
                    "WHITEWATER_STORAGE_NODE_ID must match control-N on Control Plane voters"
                );
            }
            Ok(Some(identity))
        }
        None => Ok(derived),
    }
}

/// Parse `WHITEWATER_STORAGE_VOTERS`: the subset of configured voter ids
/// still eligible for storage placement. Every Node derives eligibility
/// from the shared voter list, so this must be identical cluster-wide —
/// which is why it is a voter-id list rather than a per-Node toggle.
fn parse_storage_voters(
    value: &str,
    control_nodes: &[(u64, String)],
) -> Result<Option<BTreeSet<u64>>> {
    let entries: Vec<u64> = value
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(|entry| {
            entry
                .parse::<u64>()
                .with_context(|| format!("invalid WHITEWATER_STORAGE_VOTERS entry: {entry}"))
        })
        .collect::<Result<Vec<_>>>()?;
    if entries.is_empty() {
        return Ok(None);
    }
    let voters: BTreeSet<u64> = entries.into_iter().collect();
    for id in &voters {
        if !control_nodes.iter().any(|(node, _)| node == id) {
            anyhow::bail!("WHITEWATER_STORAGE_VOTERS entry {id} is not a configured Control Node");
        }
    }
    Ok(Some(voters))
}

/// A capability declaration must not contradict the placement inputs: a
/// Node that drops the storage role while remaining placement-eligible
/// would accept work it refuses to serve, and a registered storage Node
/// without the storage role is a contradiction outright.
fn validate_capability_topology(
    capabilities: &NodeCapabilities,
    control_node_id: Option<u64>,
    storage_voters: Option<&BTreeSet<u64>>,
    explicit_storage_id: bool,
    explicit_capabilities: bool,
) -> Result<()> {
    if capabilities.storage {
        if explicit_storage_id || control_node_id.is_none() {
            // A non-voter explicitly declaring storage needs an identity
            // to be registered under. Default capabilities (env unset)
            // leave storage inert on a Node with no identity instead of
            // breaking deployments that never served storage.
            if control_node_id.is_none() && explicit_capabilities && !explicit_storage_id {
                anyhow::bail!(
                    "WHITEWATER_NODE_CAPABILITIES including storage on a non-voter requires WHITEWATER_STORAGE_NODE_ID"
                );
            }
            return Ok(());
        }
        let id = control_node_id.expect("checked above");
        if storage_voters.is_some_and(|voters| !voters.contains(&id)) {
            anyhow::bail!(
                "WHITEWATER_NODE_CAPABILITIES declares storage but WHITEWATER_STORAGE_VOTERS excludes voter {id}"
            );
        }
        return Ok(());
    }
    if explicit_storage_id {
        anyhow::bail!(
            "WHITEWATER_STORAGE_NODE_ID is set but WHITEWATER_NODE_CAPABILITIES omits storage"
        );
    }
    if let Some(id) = control_node_id {
        let still_eligible = storage_voters.is_none_or(|voters| voters.contains(&id));
        if still_eligible {
            anyhow::bail!(
                "WHITEWATER_NODE_CAPABILITIES omits storage but voter {id} is still storage-eligible; \
                 also exclude it with WHITEWATER_STORAGE_VOTERS on every Node"
            );
        }
    }
    Ok(())
}

fn parse_control_nodes(value: &str) -> Result<Vec<(u64, String)>> {
    value
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(|entry| {
            let (id, address) = entry
                .split_once('@')
                .with_context(|| format!("invalid Control Plane Node entry: {entry}"))?;
            let id = id
                .parse::<u64>()
                .with_context(|| format!("invalid Control Plane Node ID: {id}"))?;
            if address.is_empty() {
                anyhow::bail!("Control Plane Node {id} has no address");
            }
            Ok((id, address.trim_end_matches('/').to_owned()))
        })
        .collect()
}

fn parse_env<T>(name: &str, default: T) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    env::var(name).map_or(Ok(default), |value| {
        value
            .parse()
            .with_context(|| format!("{name} has an invalid value"))
    })
}

fn discover_local_ip() -> IpAddr {
    UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0))
        .and_then(|socket| {
            socket.connect((Ipv4Addr::new(1, 1, 1, 1), 80))?;
            socket.local_addr().map(|address| address.ip())
        })
        .unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST))
}

/// Parse `WHITEWATER_INTERNAL_TRANSPORT`: unset means `mtls` (the secure
/// default); the only other accepted values are `mtls` and `insecure`.
fn parse_internal_transport(value: Option<String>) -> Result<InternalTransport> {
    match value {
        None => Ok(InternalTransport::Mtls),
        Some(value) if value.eq_ignore_ascii_case("mtls") => Ok(InternalTransport::Mtls),
        Some(value) if value.eq_ignore_ascii_case("insecure") => Ok(InternalTransport::Insecure),
        Some(value) => anyhow::bail!(
            "WHITEWATER_INTERNAL_TRANSPORT must be `mtls` or `insecure`, got {value:?}"
        ),
    }
}

/// The inter-Node transport contract: `mtls` requires certificate
/// material; `insecure` must never coexist with it — a bundle plus the
/// opt-down is a contradictory posture and fails closed.
fn validate_internal_transport(
    transport: InternalTransport,
    subscription_mtls: Option<&SubscriptionMtlsConfig>,
) -> Result<()> {
    match (transport, subscription_mtls.is_some()) {
        (InternalTransport::Mtls, false) => anyhow::bail!(
            "inter-Node transport defaults to authenticated mTLS: configure the full              WHITEWATER_SUBSCRIPTION_MTLS_* certificate bundle (or rely on a verified              orchestrator-provided transport), or set              WHITEWATER_INTERNAL_TRANSPORT=insecure for development and test Riverbeds              that accept the shared-key prototype transport"
        ),
        (InternalTransport::Insecure, true) => anyhow::bail!(
            "WHITEWATER_INTERNAL_TRANSPORT=insecure contradicts a configured              WHITEWATER_SUBSCRIPTION_MTLS_* bundle; remove the certificate material or              drop the opt-down"
        ),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_topology_requires_voter_self_membership_but_allows_non_voters() {
        let voters = vec![
            (1_u64, "control-1:7070".to_owned()),
            (2_u64, "control-2:7070".to_owned()),
            (3_u64, "control-3:7070".to_owned()),
        ];
        validate_control_topology(Some(1), &voters, Some("key")).unwrap();
        // A storage-only Node knows the voters and the internal key but
        // holds no voter id of its own.
        validate_control_topology(None, &voters, Some("key")).unwrap();
        validate_control_topology(Some(1), &voters, None).unwrap_err();
        validate_control_topology(Some(9), &voters, Some("key")).unwrap_err();
        validate_control_topology(Some(1), &[], Some("key")).unwrap_err();
        validate_control_topology(None, &voters, None).unwrap_err();
        validate_control_topology(None, &[], Some("key")).unwrap();
        validate_control_topology(None, &[], None).unwrap();
    }

    #[test]
    fn storage_voters_must_be_a_subset_of_configured_voters() {
        let voters = vec![
            (1_u64, "control-1:7070".to_owned()),
            (2_u64, "control-2:7070".to_owned()),
            (3_u64, "control-3:7070".to_owned()),
        ];
        assert!(parse_storage_voters("", &voters).unwrap().is_none());
        let subset = parse_storage_voters("1,3", &voters).unwrap().unwrap();
        assert!(subset.contains(&1) && subset.contains(&3) && !subset.contains(&2));
        // Opting out of storage is shared cluster config: a voter id that
        // does not exist fails closed rather than silently narrowing.
        assert!(parse_storage_voters("9", &voters).is_err());
        assert!(parse_storage_voters("one", &voters).is_err());
    }

    #[test]
    fn capability_topology_refuses_contradictions() {
        // Default: every role on a voter that remains storage-eligible.
        validate_capability_topology(&NodeCapabilities::all(), Some(1), None, false, true).unwrap();
        // A voter dropping storage must also leave the shared eligible set.
        let stateless = NodeCapabilities::stateless();
        assert!(validate_capability_topology(&stateless, Some(1), None, false, true).is_err());
        validate_capability_topology(
            &stateless,
            Some(1),
            Some(&BTreeSet::from([2, 3])),
            false,
            true,
        )
        .unwrap();
        // A registered storage Node cannot drop the storage role.
        assert!(validate_capability_topology(&stateless, None, None, true, true).is_err());
        // A non-voter explicitly declaring storage needs an identity to
        // register under; default capabilities without an identity stay
        // inert so existing non-voter deployments keep booting.
        assert!(
            validate_capability_topology(&NodeCapabilities::all(), None, None, false, true)
                .is_err()
        );
        validate_capability_topology(&NodeCapabilities::all(), None, None, false, false).unwrap();
        validate_capability_topology(&NodeCapabilities::all(), None, None, true, true).unwrap();
        // A voter opted out of placement may not still declare it.
        assert!(validate_capability_topology(
            &NodeCapabilities::all(),
            Some(1),
            Some(&BTreeSet::from([2])),
            false,
            true,
        )
        .is_err());
    }

    #[test]
    fn storage_node_identity_defaults_to_control_n_and_overrides_on_non_voters() {
        assert_eq!(
            resolve_storage_node_id(None, Some(2)).unwrap(),
            Some(StorageNodeId::try_new("control-2").unwrap())
        );
        assert_eq!(resolve_storage_node_id(None, None).unwrap(), None);
        assert_eq!(
            resolve_storage_node_id(Some("storage-9"), None).unwrap(),
            Some(StorageNodeId::try_new("storage-9").unwrap())
        );
        // Matching an explicit id on a voter is allowed; a conflicting one
        // is refused so storage identity can never diverge from the voter.
        assert!(resolve_storage_node_id(Some("control-2"), Some(2))
            .unwrap()
            .is_some());
        assert!(resolve_storage_node_id(Some("storage-9"), Some(2)).is_err());
        assert!(resolve_storage_node_id(Some("!!!"), None).is_err());
    }

    #[test]
    fn subscription_peer_pins_reject_duplicate_identity_and_certificate() {
        let first = blake3::hash(b"node-1").to_hex();
        let second = blake3::hash(b"node-2").to_hex();
        let rotated = blake3::hash(b"rotated-node-1").to_hex();
        let pins =
            parse_peer_pins(&format!("control-1@{first}|{rotated},control-2@{second}")).unwrap();
        assert_eq!(pins.len(), 2);
        assert_eq!(pins[&StorageNodeId::try_new("control-1").unwrap()].len(), 2);
        assert!(parse_peer_pins(&format!("control-1@{first},control-1@{second}")).is_err());
        assert!(parse_peer_pins(&format!("control-1@{first},control-2@{first}")).is_err());
        assert!(parse_peer_pins("control-1@bad").is_err());
        assert!(parse_peer_pins("").is_err());
    }

    #[test]
    fn internal_transport_defaults_secure_and_fences_contradictions() {
        assert_eq!(
            parse_internal_transport(None).unwrap(),
            InternalTransport::Mtls
        );
        assert_eq!(
            parse_internal_transport(Some("mtls".to_owned())).unwrap(),
            InternalTransport::Mtls
        );
        assert_eq!(
            parse_internal_transport(Some("INSECURE".to_owned())).unwrap(),
            InternalTransport::Insecure
        );
        assert!(parse_internal_transport(Some("plaintext".to_owned())).is_err());
        // The secure default refuses to run without certificate material.
        assert!(validate_internal_transport(InternalTransport::Mtls, None).is_err());
        assert!(validate_internal_transport(InternalTransport::Insecure, None).is_ok());
        // An opt-down beside configured certificates is contradictory.
        let bundle = SubscriptionMtlsConfig {
            bind_addr: "0.0.0.0:7271".parse().unwrap(),
            cert_path: PathBuf::from("cert.pem"),
            key_path: PathBuf::from("key.pem"),
            ca_path: PathBuf::from("ca.pem"),
            peer_pins: BTreeMap::new(),
            peer_endpoints: BTreeMap::new(),
        };
        assert!(validate_internal_transport(InternalTransport::Mtls, Some(&bundle)).is_ok());
        assert!(validate_internal_transport(InternalTransport::Insecure, Some(&bundle)).is_err());
    }
    #[test]
    fn subscription_peer_endpoints_reject_duplicates_and_missing_values() {
        let endpoints = parse_peer_endpoints(
            "control-1@https://control-1:7271, control-2@https://control-2:7272",
        )
        .unwrap();
        assert_eq!(endpoints.len(), 2);
        assert_eq!(
            endpoints[&StorageNodeId::try_new("control-1").unwrap()],
            "https://control-1:7271"
        );
        assert!(parse_peer_endpoints(
            "control-1@https://control-1:7271,control-1@https://control-1:7272"
        )
        .is_err());
        assert!(parse_peer_endpoints("control-1@").is_err());
        assert!(parse_peer_endpoints("missing-node").is_err());
        assert!(parse_peer_endpoints("control-1@http://control-1:7271").is_err());
        assert!(parse_peer_endpoints("control-1@https://control-2:7271").is_err());
        assert!(parse_peer_endpoints("").is_err());
    }
}
