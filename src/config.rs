use std::{
    env,
    net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket},
    path::PathBuf,
    time::Duration,
};

use anyhow::{Context, Result};

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
        })
    }
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
