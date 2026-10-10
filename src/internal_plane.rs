use std::collections::BTreeMap;
use std::sync::Arc;

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
