use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket},
    path::PathBuf,
    time::Duration,
};

use anyhow::{Context, Result};

use crate::active_range::StorageNodeId;

#[derive(Clone, Debug)]
pub struct SubscriptionMtlsConfig {
    pub bind_addr: SocketAddr,
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
    pub ca_path: PathBuf,
    pub peer_pins: BTreeMap<StorageNodeId, BTreeSet<[u8; 32]>>,
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
    pub subscription_mtls: Option<SubscriptionMtlsConfig>,
}

impl NodeConfig {
    pub fn from_env() -> Result<Self> {
        let bind_addr: SocketAddr = env::var("FINNSTREAM_BIND_ADDR")
            .unwrap_or_else(|_| "0.0.0.0:7070".to_owned())
            .parse()
            .context("FINNSTREAM_BIND_ADDR must be a socket address")?;
        let node_id = env::var("FINNSTREAM_NODE_ID")
            .or_else(|_| env::var("HOSTNAME"))
            .unwrap_or_else(|_| uuid::Uuid::new_v4().to_string());
        let advertise_url = env::var("FINNSTREAM_ADVERTISE_URL")
            .unwrap_or_else(|_| format!("http://{}:{}", discover_local_ip(), bind_addr.port()));
        let seeds = env::var("FINNSTREAM_SEEDS")
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|seed| !seed.is_empty())
            .map(ToOwned::to_owned)
            .collect();
        let heartbeat_interval =
            Duration::from_millis(parse_env("FINNSTREAM_HEARTBEAT_MS", 2_000)?);
        let member_timeout =
            Duration::from_millis(parse_env("FINNSTREAM_MEMBER_TIMEOUT_MS", 10_000)?);
        if heartbeat_interval >= member_timeout {
            anyhow::bail!(
                "FINNSTREAM_HEARTBEAT_MS must be lower than FINNSTREAM_MEMBER_TIMEOUT_MS"
            );
        }
        let control_node_id = env::var("FINNSTREAM_CONTROL_NODE_ID")
            .ok()
            .map(|value| value.parse::<u64>())
            .transpose()
            .context("FINNSTREAM_CONTROL_NODE_ID must be an unsigned integer")?;
        let control_nodes =
            parse_control_nodes(&env::var("FINNSTREAM_CONTROL_NODES").unwrap_or_default())?;
        let control_plane_key = env::var("FINNSTREAM_CONTROL_PLANE_KEY").ok();
        let control_values = [
            control_node_id.is_some(),
            !control_nodes.is_empty(),
            control_plane_key.is_some(),
        ];
        if control_values.iter().any(|value| *value) && !control_values.iter().all(|value| *value) {
            anyhow::bail!("FINNSTREAM_CONTROL_NODE_ID, FINNSTREAM_CONTROL_NODES, and FINNSTREAM_CONTROL_PLANE_KEY must be configured together");
        }
        let tls = [
            env::var("FINNSTREAM_SUBSCRIPTION_MTLS_BIND").ok(),
            env::var("FINNSTREAM_SUBSCRIPTION_MTLS_CERT").ok(),
            env::var("FINNSTREAM_SUBSCRIPTION_MTLS_KEY").ok(),
            env::var("FINNSTREAM_SUBSCRIPTION_MTLS_CA").ok(),
            env::var("FINNSTREAM_SUBSCRIPTION_MTLS_PEER_PINS").ok(),
        ];
        if tls.iter().any(Option::is_some) && !tls.iter().all(Option::is_some) {
            anyhow::bail!("Subscription mTLS bind, certificate, key, CA and peer pins must be configured together");
        }
        let subscription_mtls = match tls {
            [Some(bind), Some(cert), Some(key), Some(ca), Some(pins)] => {
                let local_node = control_node_id.ok_or_else(|| {
                    anyhow::anyhow!("Subscription mTLS requires a configured Control Node ID")
                })?;
                let peer_pins = parse_peer_pins(&pins)?;
                let local_id = StorageNodeId::try_new(format!("control-{local_node}"))?;
                if !peer_pins.contains_key(&local_id)
                    || control_nodes.iter().any(|(node, _)| {
                        StorageNodeId::try_new(format!("control-{node}"))
                            .map_or(true, |id| !peer_pins.contains_key(&id))
                    })
                {
                    anyhow::bail!("Subscription mTLS peer pins must include this Node and every configured Control Node");
                }
                Some(SubscriptionMtlsConfig {
                    bind_addr: bind
                        .parse()
                        .context("invalid Subscription mTLS bind address")?,
                    cert_path: PathBuf::from(cert),
                    key_path: PathBuf::from(key),
                    ca_path: PathBuf::from(ca),
                    peer_pins,
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
        Ok(Self {
            node_id,
            bind_addr,
            advertise_url: advertise_url.trim_end_matches('/').to_owned(),
            seeds,
            data_dir: PathBuf::from(
                env::var("FINNSTREAM_DATA_DIR").unwrap_or_else(|_| "./data".to_owned()),
            ),
            heartbeat_interval,
            member_timeout,
            capacity: parse_env("FINNSTREAM_NODE_CAPACITY", 100)?,
            control_node_id,
            control_nodes,
            control_plane_key,
            subscription_mtls,
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
