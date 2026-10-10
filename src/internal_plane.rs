use std::collections::BTreeMap;
use std::sync::Arc;

use base64::{engine::general_purpose::STANDARD, Engine as _};

use crate::active_range::StorageNodeId;
use crate::control::ControlController;

/// Resolves the internal Node-to-Node endpoint for a storage Node.
///
/// Control Plane voter endpoints are configured statically, while
/// independently registered storage Nodes carry their endpoint in the
/// replicated catalog; resolution prefers the configured map and falls
/// back to the registered record so placement onto registered Nodes
/// remains reachable.
#[derive(Clone)]
pub struct InternalEndpoints {
    configured: Arc<BTreeMap<StorageNodeId, String>>,
    control: Option<Arc<ControlController>>,
}

impl InternalEndpoints {
    pub fn new(
        configured: BTreeMap<StorageNodeId, String>,
        control: Arc<ControlController>,
    ) -> Self {
        Self {
            configured: Arc::new(configured),
            control: Some(control),
        }
    }

    /// Endpoints known without catalog state: the configured voter map.
    /// Registered storage Nodes are intentionally absent here.
    pub fn configured(&self) -> &BTreeMap<StorageNodeId, String> {
        &self.configured
    }

    pub fn contains_configured(&self, node: &StorageNodeId) -> bool {
        self.configured.contains_key(node)
    }

    pub async fn resolve(&self, node: &StorageNodeId) -> Option<String> {
        if let Some(endpoint) = self.configured.get(node) {
            return Some(endpoint.clone());
        }
        if let Some(control) = &self.control {
            return control.storage_node_endpoint(node).await;
        }
        None
    }

    /// Every reachable internal endpoint: configured voter endpoints plus
    /// registered storage Node records. Configured entries win on conflict.
    pub async fn all(&self) -> BTreeMap<StorageNodeId, String> {
        let mut endpoints = match &self.control {
            Some(control) => control.registered_storage_node_endpoints().await,
            None => BTreeMap::new(),
        };
        endpoints.extend(self.configured.iter().map(|(n, e)| (n.clone(), e.clone())));
        endpoints
    }
}

impl From<BTreeMap<StorageNodeId, String>> for InternalEndpoints {
    fn from(configured: BTreeMap<StorageNodeId, String>) -> Self {
        Self {
            configured: Arc::new(configured),
            control: None,
        }
    }
}

impl From<Arc<BTreeMap<StorageNodeId, String>>> for InternalEndpoints {
    fn from(configured: Arc<BTreeMap<StorageNodeId, String>>) -> Self {
        Self {
            configured,
            control: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use tempfile::TempDir;

    use super::InternalEndpoints;
    use crate::active_range::StorageNodeId;
    use crate::control::{Command, ControlController};
    use crate::storage::FileLogStore;

    fn node(name: &str) -> StorageNodeId {
        StorageNodeId::try_new(name).unwrap()
    }

    fn controller(directory: &TempDir) -> Arc<ControlController> {
        Arc::new(
            ControlController::open_with_storage_nodes(
                directory.path().join("catalog.json"),
                Arc::new(FileLogStore::open(directory.path().join("data")).unwrap()),
                vec![node("control-1"), node("control-2"), node("control-3")],
            )
            .unwrap(),
        )
    }

    async fn register(control: &ControlController, name: &str, endpoint: &str) {
        control
            .execute_commands(vec![Command::RegisterStorageNode {
                node: node(name),
                endpoint: endpoint.to_owned(),
                cert_pins: BTreeSet::new(),
            }])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn resolve_prefers_configured_endpoints_over_registered_records() {
        let directory = TempDir::new().unwrap();
        let control = controller(&directory);
        register(&control, "control-1", "https://registered-control-1:7271").await;
        register(&control, "storage-9", "https://storage-9:7271").await;
        let endpoints = InternalEndpoints::new(
            std::collections::BTreeMap::from([
                (node("control-1"), "https://control-1:7271".to_owned()),
                (node("control-2"), "https://control-2:7271".to_owned()),
            ]),
            control,
        );
        assert_eq!(
            endpoints.resolve(&node("control-1")).await.as_deref(),
            Some("https://control-1:7271")
        );
        assert_eq!(
            endpoints.resolve(&node("storage-9")).await.as_deref(),
            Some("https://storage-9:7271")
        );
        assert_eq!(endpoints.resolve(&node("missing")).await, None);
    }

    #[tokio::test]
    async fn all_merges_registered_records_with_configured_endpoints() {
        let directory = TempDir::new().unwrap();
        let control = controller(&directory);
        register(&control, "storage-9", "https://storage-9:7271").await;
        register(&control, "control-2", "https://registered-control-2:7271").await;
        let endpoints = InternalEndpoints::new(
            std::collections::BTreeMap::from([
                (node("control-1"), "https://control-1:7271".to_owned()),
                (node("control-2"), "https://control-2:7271".to_owned()),
            ]),
            control,
        );
        let all = endpoints.all().await;
        assert_eq!(all.len(), 3);
        assert_eq!(
            all.get(&node("control-2")).map(String::as_str),
            Some("https://control-2:7271")
        );
        assert_eq!(
            all.get(&node("storage-9")).map(String::as_str),
            Some("https://storage-9:7271")
        );
    }

    #[tokio::test]
    async fn static_resolution_ignores_the_catalog() {
        let directory = TempDir::new().unwrap();
        let control = controller(&directory);
        register(&control, "storage-9", "https://storage-9:7271").await;
        let endpoints = InternalEndpoints::from(std::collections::BTreeMap::from([(
            node("control-1"),
            "https://control-1:7271".to_owned(),
        )]));
        assert_eq!(
            endpoints.resolve(&node("control-1")).await.as_deref(),
            Some("https://control-1:7271")
        );
        assert_eq!(endpoints.resolve(&node("storage-9")).await, None);
        assert_eq!(endpoints.all().await.len(), 1);
        let _ = control;
    }
}

/// Upper bound on a replicated catalog snapshot payload; larger responses
/// are refused rather than streamed into memory.
pub const MAX_CATALOG_SNAPSHOT_BYTES: usize = 32 * 1024 * 1024;

/// A committed catalog snapshot offered by a peer for non-voter storage
/// Nodes to install.
#[derive(Clone, Debug)]
pub struct CatalogSnapshot {
    pub revision: u64,
    pub bytes: Vec<u8>,
}

/// Supplies the freshest committed catalog snapshot visible to this Node.
/// Implemented by the authenticated internal HTTP transport in
/// production and by in-memory fixtures in tests.
#[async_trait::async_trait]
pub trait CatalogSnapshotSource: Send + Sync {
    /// Returns the newest reachable snapshot, or `None` when every
    /// endpoint was unavailable or refused.
    async fn latest_snapshot(&self) -> Option<CatalogSnapshot>;
}

/// Fetches committed catalog snapshots from every reachable internal
/// endpoint and returns the highest revision observed.
pub struct HttpCatalogSnapshotSource {
    endpoints: InternalEndpoints,
    key: Option<String>,
    client: reqwest::Client,
    max_bytes: usize,
}

#[derive(serde::Deserialize)]
struct CatalogSnapshotResponse {
    revision: u64,
    snapshot_base64: String,
}

impl HttpCatalogSnapshotSource {
    pub fn new(endpoints: InternalEndpoints, key: Option<String>, client: reqwest::Client) -> Self {
        Self {
            endpoints,
            key,
            client,
            max_bytes: MAX_CATALOG_SNAPSHOT_BYTES,
        }
    }
}

#[async_trait::async_trait]
impl CatalogSnapshotSource for HttpCatalogSnapshotSource {
    async fn latest_snapshot(&self) -> Option<CatalogSnapshot> {
        let mut best: Option<CatalogSnapshot> = None;
        for (node, endpoint) in self.endpoints.all().await {
            let mut request = self
                .client
                .post(format!(
                    "{}/internal/catalog/snapshot",
                    endpoint.trim_end_matches('/')
                ))
                .body(Vec::new());
            if let Some(key) = &self.key {
                request = request.header("x-whitewater-control-key", key);
            }
            let response = match request.send().await {
                Ok(response) => response,
                Err(error) => {
                    tracing::debug!(node = %node, %error, "catalog snapshot request failed");
                    continue;
                }
            };
            if !response.status().is_success() {
                tracing::debug!(node = %node, status = %response.status(), "catalog snapshot refused");
                continue;
            }
            let body = match response.bytes().await {
                Ok(body) => body,
                Err(_) => continue,
            };
            let parsed: CatalogSnapshotResponse = match serde_json::from_slice(&body) {
                Ok(parsed) => parsed,
                Err(_) => continue,
            };
            let bytes = match STANDARD.decode(&parsed.snapshot_base64) {
                Ok(bytes) => bytes,
                Err(_) => continue,
            };
            if bytes.len() > self.max_bytes {
                tracing::warn!(node = %node, bytes = bytes.len(), "catalog snapshot exceeds the accepted bound");
                continue;
            }
            if best
                .as_ref()
                .is_none_or(|current: &CatalogSnapshot| parsed.revision > current.revision)
            {
                best = Some(CatalogSnapshot {
                    revision: parsed.revision,
                    bytes,
                });
            }
        }
        best
    }
}

/// Outcome of one catalog synchronization pass.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CatalogSyncOutcome {
    /// A newer committed snapshot was installed.
    Installed { revision: u64 },
    /// The local catalog is already at or ahead of every reachable
    /// snapshot; nothing was installed.
    Current { revision: u64 },
    /// No endpoint offered a snapshot this pass.
    Unavailable,
}

/// Replicates committed Control Plane catalog state onto a Node that is
/// not a Control Plane voter. Independently registered storage Nodes
/// cannot receive Raft log replication, so they periodically install the
/// newest committed snapshot offered by any reachable internal endpoint;
/// installs are revision-gated so a lagging peer can never roll the
/// catalog backwards.
pub struct CatalogSyncSupervisor<S: CatalogSnapshotSource> {
    control: Arc<ControlController>,
    source: S,
    interval: std::time::Duration,
}

impl<S: CatalogSnapshotSource> CatalogSyncSupervisor<S> {
    pub fn new(control: Arc<ControlController>, source: S, interval: std::time::Duration) -> Self {
        Self {
            control,
            source,
            interval,
        }
    }

    pub async fn sync_once(&self) -> CatalogSyncOutcome {
        let local = self.control.revision().await;
        let Some(snapshot) = self.source.latest_snapshot().await else {
            return CatalogSyncOutcome::Unavailable;
        };
        if snapshot.revision <= local {
            return CatalogSyncOutcome::Current { revision: local };
        }
        match self.control.install_snapshot_bytes(&snapshot.bytes).await {
            Ok(()) => CatalogSyncOutcome::Installed {
                revision: snapshot.revision,
            },
            Err(error) => {
                tracing::warn!(%error, "catalog snapshot install failed");
                CatalogSyncOutcome::Unavailable
            }
        }
    }

    pub async fn run(self, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        let mut interval = tokio::time::interval(self.interval);
        loop {
            tokio::select! {
                _ = interval.tick() => match self.sync_once().await {
                    CatalogSyncOutcome::Installed { revision } => {
                        tracing::info!(revision, "installed committed catalog snapshot")
                    }
                    CatalogSyncOutcome::Unavailable => {
                        tracing::debug!("no catalog snapshot source reachable")
                    }
                    CatalogSyncOutcome::Current { .. } => {}
                },
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() { break; }
                }
            }
        }
    }
}
