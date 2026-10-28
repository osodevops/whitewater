use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use anyhow::Result;
use openraft::BasicNode;
use tokio::{
    net::TcpListener,
    sync::{watch, Mutex},
};
use tower_http::trace::TraceLayer;
use tracing::info;
use tracing_subscriber::EnvFilter;
use whitewater::{
    active_range::{
        cold_adjacent_pairs, ColdRangeTracker, CommitProofReporter, HttpRecoveryTransport,
        HttpReplicaTransport, LocalRepairSupervisor, MajorityAppendCoordinator, RecoverySupervisor,
        ReplicaAppendService, SplitPressureTracker, StorageDrainStatus, StorageDrainSupervisor,
        StorageNodeId,
    },
    admin::AdminAuthenticator,
    api::{
        internal_mtls_router, router, AdminDrainDriver, AppState, CatalogCommitProofSink,
        InternalMtlsMaterial, SubscriptionTlsServer,
    },
    autoscale::AutoscaleController,
    config::NodeConfig,
    control::{CapacityPolicy, ControlController},
    control_plane::ControlPlane,
    demand::{isolation_split_boundary, DemandMetrics, HotKeyObservation, HotKeyTracker},
    internal_plane::{
        CapacityReporter, CatalogSyncSupervisor, HttpCatalogSnapshotSource, InternalEndpoints,
    },
    membership::{MemberAnnouncement, MembershipService},
    node::{self, NodeCapacity},
    reader::{
        FjallSubscriptionProgressReplica, SubscriptionPlacementAuthority,
        SubscriptionProgressReplicaService,
    },
    storage::{FileLogStore, LogStore},
};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("whitewater=info,tower_http=info")),
        )
        .json()
        .init();

    let mut config = NodeConfig::from_env()?;
    // The volume carries the storage identity across container replacement:
    // adopt a persisted identity when the environment forgot it, and refuse
    // a configured identity that contradicts the volume's bound owner.
    config.storage_node_id =
        node::resolve_durable_identity(&config.data_dir, config.storage_node_id.clone())?;
    // An announcement may only claim storage the Node can actually serve:
    // no resolved identity means no replica service, so a non-voter on
    // default capabilities reports storage as unavailable rather than
    // advertising a role placement can never reach.
    if config.storage_node_id.is_none() {
        config.node_capabilities.storage = false;
    }
    let admin_auth = AdminAuthenticator::from_env()?;
    let store: Arc<dyn LogStore> = Arc::new(FileLogStore::open(&config.data_dir)?);
    let catalog_file = if config.control_node_id.is_some() {
        "control-plane-catalog.json"
    } else {
        "control-catalog.json"
    };
    let eligible_storage_nodes = config
        .control_nodes
        .iter()
        .filter(|(node_id, _)| {
            config
                .storage_voters
                .as_ref()
                .is_none_or(|voters| voters.contains(node_id))
        })
        .map(|(node_id, _)| StorageNodeId::try_new(format!("control-{node_id}")))
        .collect::<Result<Vec<_>, _>>()?;
    let controller = ControlController::open_with_storage_nodes(
        config.data_dir.join(catalog_file),
        store.clone(),
        eligible_storage_nodes,
    )?
    .with_capacity_policy(CapacityPolicy {
        report_max_age_ns: (config.capacity_report_max_age > Duration::ZERO)
            .then_some(config.capacity_report_max_age.as_nanos() as i64),
        min_disk_free_bytes: config.min_disk_free_bytes,
    });
    let control = Arc::new(
        if config.control_node_id.is_none() && !config.control_nodes.is_empty() {
            controller.into_synced_replica()
        } else {
            controller
        },
    );
    let internal_mtls = match &config.subscription_mtls {
        Some(tls) => Some(Arc::new(InternalMtlsMaterial::from_config(tls).await?)),
        None => None,
    };
    let build_internal_client = |timeout: Duration| -> Result<reqwest::Client, reqwest::Error> {
        match &internal_mtls {
            Some(material) => material.client(timeout),
            None => reqwest::Client::builder().timeout(timeout).build(),
        }
    };
    let configured_endpoints: BTreeMap<StorageNodeId, String> = match &config.subscription_mtls {
        Some(tls) => tls.peer_endpoints.clone(),
        None => config
            .control_nodes
            .iter()
            .map(|(node_id, address)| {
                StorageNodeId::try_new(format!("control-{node_id}"))
                    .map(|node| (node, format!("http://{address}")))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?,
    };
    // Configured voter endpoints take priority; independently registered
    // storage Nodes resolve through their replicated catalog record.
    let control_endpoints = InternalEndpoints::new(configured_endpoints.clone(), control.clone());
    let control_plane = match (config.control_node_id, config.control_plane_key.clone()) {
        (Some(node_id), Some(key)) => {
            let peers = config
                .control_nodes
                .iter()
                .map(|(id, address)| {
                    let address = config
                        .subscription_mtls
                        .as_ref()
                        .and_then(|tls| {
                            StorageNodeId::try_new(format!("control-{id}"))
                                .ok()
                                .and_then(|node| tls.peer_endpoints.get(&node).cloned())
                        })
                        .unwrap_or_else(|| address.clone());
                    (*id, BasicNode::new(address))
                })
                .collect::<BTreeMap<_, _>>();
            Some(Arc::new(
                ControlPlane::start(
                    node_id,
                    peers,
                    key,
                    config.data_dir.join("control-plane-raft.json"),
                    control.clone(),
                    Some(build_internal_client(Duration::from_secs(30))?),
                )
                .await?,
            ))
        }
        _ => None,
    };
    // A Node that resolved a storage identity for internal-plane auth but
    // declared no storage capability serves no replica services; placement
    // already excludes it through the eligible set.
    // The read side of tiering is a single TierSource shared by every
    // range the store opens: remote fetches dedupe and bound concurrency
    // Node-wide, and each range enforces the configured per-range cache
    // budget through coldest-first demotion.
    let tier_source = config.object_store_dir.clone().map(|root| {
        std::sync::Arc::new(whitewater::active_range::tier::TierSource::new(
            root,
            config.remote_cache_bytes,
        ))
    });
    let replica_append = config
        .storage_node_id
        .clone()
        .filter(|_| config.node_capabilities.storage)
        .map(|local_node| {
            Arc::new(ReplicaAppendService::with_options(
                config.data_dir.join("active-ranges"),
                local_node,
                control.clone(),
                whitewater::active_range::store::FileActiveRangeStoreOptions {
                    max_segment_bytes: config.max_segment_bytes,
                    tier: tier_source.clone().map(|source| {
                        source
                            as std::sync::Arc<
                                dyn whitewater::active_range::store::RemoteSegmentSource,
                            >
                    }),
                    ..whitewater::active_range::store::FileActiveRangeStoreOptions::default()
                },
            ))
        });

    // Sealed-segment tiering runs only on storage Nodes with a configured
    // object store: each tick verifies manifests and uploads every newly
    // sealed segment's artifacts exactly once, marked durably so restarts
    // resume cleanly. Blocking filesystem work stays on spawn_blocking.
    // Sealed-segment tiering + history retention share one tick: the
    // upload pass runs only with a configured object store, while the
    // retention sweep runs on every storage Node — expiry and the
    // retained-bytes ceiling apply whether or not history tiers.
    if replica_append.is_some() {
        let service = replica_append.clone().expect("checked above");
        let object_root = config.object_store_dir.clone();
        let ranges_root = config.data_dir.join("active-ranges");
        let interval = config.tier_scan_interval;
        let policy_control = Arc::clone(&control);
        tokio::spawn(async move {
            use whitewater::active_range::tier::{
                tier_scan_once, FileSystemObjectStore, ObjectStore,
            };
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                let policies = policy_control.effective_history_policies().await;
                if let Some(object_root) = &object_root {
                    let root = ranges_root.clone();
                    let objects = object_root.clone();
                    let policies = policies.clone();
                    // The object store root opens lazily every tick: a
                    // late-mounted or briefly unavailable backend self-heals
                    // on the next pass instead of disabling tiering for the
                    // process lifetime.
                    let report = tokio::task::spawn_blocking(move || {
                        FileSystemObjectStore::open(&objects).map(|store| {
                            tier_scan_once(root.as_path(), &store, &|feed_id| {
                                policies.get(&feed_id).cloned()
                            })
                        })
                    })
                    .await;
                    match report {
                        Ok(Ok(report)) if !report.failed.is_empty() => {
                            tracing::warn!(
                                uploaded = report.uploaded,
                                failed = ?report.failed,
                                "sealed-segment tier scan completed with failures"
                            );
                        }
                        Ok(Ok(report)) if report.uploaded > 0 => {
                            tracing::info!(
                                uploaded = report.uploaded,
                                already_tiered = report.already_tiered,
                                "sealed-segment tier scan uploaded artifacts"
                            );
                        }
                        Ok(Ok(_)) => {}
                        Ok(Err(error)) => {
                            tracing::warn!(%error, "object store root is not usable")
                        }
                        Err(error) => {
                            tracing::warn!(%error, "sealed-segment tier scan worker failed")
                        }
                    }
                }
                // Retention deletes sealed spans the policy expired or
                // over-budgeted; without an object store there are simply
                // no remote artifacts to delete alongside the local ones.
                let objects: Option<Arc<dyn ObjectStore>> = object_root
                    .as_ref()
                    .and_then(|root| FileSystemObjectStore::open(root).ok())
                    .map(|store| Arc::new(store) as Arc<dyn ObjectStore>);
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|elapsed| elapsed.as_nanos() as i64)
                    .unwrap_or(0);
                let sweep = service.retention_sweep(&policies, objects, now).await;
                if !sweep.skipped.is_empty() {
                    tracing::warn!(
                        deleted = sweep.deleted_segments,
                        skipped = ?sweep.skipped,
                        "history retention sweep completed with skips"
                    );
                } else if sweep.deleted_segments > 0 {
                    tracing::info!(
                        deleted = sweep.deleted_segments,
                        reclaimed_bytes = sweep.reclaimed_bytes,
                        "history retention sweep deleted expired spans"
                    );
                }
            }
        });
    }
    let subscription_progress = if let Some(local) = &replica_append {
        let path = config.data_dir.join("subscription-progress");
        let replica =
            tokio::task::spawn_blocking(move || FjallSubscriptionProgressReplica::open(path))
                .await??;
        Some(Arc::new(SubscriptionProgressReplicaService::new(
            local.local_node().clone(),
            control.clone(),
            Arc::new(replica),
        )))
    } else {
        None
    };
    let effect_journal = if let Some(local) = &replica_append {
        let path = config.data_dir.join("effect-journal");
        let replica = tokio::task::spawn_blocking(move || {
            whitewater::effect::FjallEffectJournalReplica::open(path)
        })
        .await??;
        Some(Arc::new(whitewater::effect::EffectJournalService::new(
            local.local_node().clone(),
            control.clone(),
            Arc::new(replica),
        )))
    } else {
        None
    };

    let state_store_journal = if let Some(local) = &replica_append {
        let journal_path = config.data_dir.join("state-store-journal");
        let index_path = config.data_dir.join("state-store-index");
        let journal = tokio::task::spawn_blocking(move || {
            whitewater::state_store::FjallStateStoreMutationJournal::open(journal_path)
        })
        .await??;
        let index = tokio::task::spawn_blocking(move || {
            whitewater::index::FjallIndexStore::open(index_path)
        })
        .await??;
        Some(Arc::new(
            whitewater::state_store::StateStoreMutationService::new(
                local.local_node().clone(),
                control.clone(),
                Arc::new(journal),
                Arc::new(whitewater::state_store::IndexStoreStateApply::new(
                    Arc::new(index),
                )),
            ),
        ))
    } else {
        None
    };

    let majority_append = match (
        replica_append.clone(),
        config.control_plane_key.clone(),
        config.storage_node_id.is_some(),
    ) {
        (Some(local), Some(key), true) => {
            let transport = Arc::new(HttpReplicaTransport::with_client(
                control_endpoints.clone(),
                key,
                build_internal_client(Duration::from_secs(2))?,
            )?);
            let proofs = CommitProofReporter::new(Arc::new(CatalogCommitProofSink::new(
                control.clone(),
                control_plane.clone(),
            )));
            Some(Arc::new(
                MajorityAppendCoordinator::new(local, control.clone(), transport)
                    .with_commit_proofs(proofs),
            ))
        }
        _ => None,
    };
    let recovery_supervisor = match (control_plane.clone(), config.control_plane_key.clone()) {
        (Some(control_plane), Some(key)) => {
            let transport = Arc::new(HttpRecoveryTransport::with_client(
                control_endpoints.clone(),
                key,
                build_internal_client(Duration::from_secs(2))?,
            )?);
            Some(RecoverySupervisor::new(
                control.clone(),
                control_plane,
                transport,
                3,
            ))
        }
        _ => None,
    };
    let repair_supervisor = match (
        replica_append.clone(),
        config.storage_node_id.clone(),
        config.control_plane_key.clone(),
    ) {
        (Some(local), Some(node_id), Some(key)) => Some(LocalRepairSupervisor::with_client(
            node_id,
            local,
            control.clone(),
            control_endpoints.clone(),
            key,
            build_internal_client(Duration::from_secs(5))?,
            256,
        )?),
        _ => None,
    };
    if let (Some(control_plane), Some(first_node)) = (
        control_plane.clone(),
        config.control_nodes.iter().map(|(id, _)| *id).min(),
    ) {
        if config.control_node_id == Some(first_node) {
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(2)).await;
                if let Err(error) = control_plane.initialize().await {
                    tracing::warn!(%error, "Control Plane initialization deferred");
                }
            });
        }
    }
    let capacity_dir = config.data_dir.clone();
    let capacity_domain = config.failure_domain.clone();
    let membership = Arc::new(
        MembershipService::new(
            MemberAnnouncement {
                node_id: config.node_id.clone(),
                api_url: config.advertise_url.clone(),
                capacity: config.capacity,
                capabilities: config.node_capabilities,
                resources: NodeCapacity::sample(&capacity_dir, capacity_domain.clone()),
            },
            config.seeds.clone(),
            config.heartbeat_interval,
            config.member_timeout,
        )
        .with_capacity_probe(move || NodeCapacity::sample(&capacity_dir, capacity_domain.clone())),
    );
    let demand = DemandMetrics::default();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    // A storage-capable Node re-commits its measured capacity on an
    // interval so a configured baseline policy reads fresh committed
    // evidence instead of gossip. No configured voters means nothing
    // authoritative to report to, so local-prototype Nodes stay silent.
    let capacity_reporter_task = if config.node_capabilities.storage
        && !configured_endpoints.is_empty()
    {
        config.storage_node_id.clone().map(|identity| {
            let reporter = CapacityReporter::new(
                identity,
                config.data_dir.clone(),
                config.failure_domain.clone(),
                configured_endpoints.values().cloned().collect(),
                config.control_plane_key.clone(),
                match build_internal_client(Duration::from_secs(10)) {
                    Ok(client) => client,
                    Err(error) => {
                        tracing::warn!(%error, "capacity reporter disabled: internal client failed to build");
                        return tokio::spawn(std::future::pending());
                    }
                },
                config.capacity_report_interval,
            );
            let shutdown = shutdown_tx.subscribe();
            tokio::spawn(async move { reporter.run(shutdown).await })
        })
    } else {
        None
    };
    let membership_task = tokio::spawn(membership.clone().run(shutdown_rx));
    let recovery_task = recovery_supervisor.map(|supervisor| {
        let mut shutdown = shutdown_tx.subscribe();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(2));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        for result in supervisor.tick().await {
                            if let Err(error) = result {
                                tracing::warn!(%error, "Active Range owner recovery attempt failed");
                            }
                        }
                    }
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            break;
                        }
                    }
                }
            }
        })
    });
    let repair_task = repair_supervisor.map(|supervisor| {
        let mut shutdown = shutdown_tx.subscribe();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        for result in supervisor.tick().await {
                            match result {
                                Ok(progress) if progress.ready => {
                                    tracing::info!(transferred_records = progress.transferred_records, transferred_bytes = progress.transferred_bytes, "Active Range replica catch-up completed");
                                }
                                Ok(_) => {}
                                Err(error) => tracing::warn!(%error, "Active Range replica repair attempt failed"),
                            }
                        }
                    }
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() { break; }
                    }
                }
            }
        })
    });
    // Non-voter storage Nodes cannot receive Raft log replication; they
    // install the newest committed catalog snapshot from any reachable
    // internal endpoint so replica fencing uses real placement state.
    let catalog_sync_task = if config.control_node_id.is_none() && !config.control_nodes.is_empty()
    {
        let interval_ms = std::env::var("WHITEWATER_CATALOG_SYNC_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(1_000)
            .max(100);
        let source = HttpCatalogSnapshotSource::new(
            control_endpoints.clone(),
            config.control_plane_key.clone(),
            build_internal_client(Duration::from_secs(5))?,
        );
        let supervisor =
            CatalogSyncSupervisor::new(control.clone(), source, Duration::from_millis(interval_ms));
        let shutdown = shutdown_tx.subscribe();
        Some(tokio::spawn(supervisor.run(shutdown)))
    } else {
        None
    };
    let drain_task = match (
        control_plane.clone(),
        std::env::var("WHITEWATER_ADMIN_API_KEY").ok(),
    ) {
        (Some(control_plane), Some(admin_key)) => {
            let driver = AdminDrainDriver::new(
                format!("http://127.0.0.1:{}", config.bind_addr.port()),
                admin_key,
                Duration::from_secs(60),
            )?;
            let move_budget = std::env::var("WHITEWATER_DRAIN_MOVE_BUDGET")
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(2);
            let supervisor = StorageDrainSupervisor::new(control.clone(), Arc::new(driver))
                .with_move_budget(move_budget);
            let mut shutdown = shutdown_tx.subscribe();
            Some(tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(5));
                loop {
                    tokio::select! {
                        _ = interval.tick() => {
                            if control_plane.status().await.state != "leader" {
                                continue;
                            }
                            for outcome in supervisor.tick().await {
                                match outcome.status {
                                    StorageDrainStatus::Drained(report) if report.ready_to_retire => {
                                        tracing::info!(node = %report.node, completed_moves = report.completed_moves, "storage Node drain completed; Node reports safe-to-remove")
                                    }
                                    StorageDrainStatus::Drained(report) => {
                                        tracing::warn!(node = %report.node, unplannable = ?report.unplannable, "storage Node drain blocked")
                                    }
                                    StorageDrainStatus::Throttled { pending_plans } => {
                                        tracing::info!(node = %outcome.node, pending_plans, "storage Node drain deferred by movement budget")
                                    }
                                    StorageDrainStatus::Failed(error) => {
                                        tracing::warn!(node = %outcome.node, %error, "storage Node drain step failed")
                                    }
                                }
                            }
                        }
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow() { break; }
                        }
                    }
                }
            }))
        }
        _ => None,
    };
    let split_task = control_plane.clone().and_then(|control_plane| {
        let admin_key = std::env::var("WHITEWATER_ADMIN_API_KEY").ok()?;
        let interval_ms = std::env::var("WHITEWATER_AUTO_SPLIT_INTERVAL_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(2_000)
            .max(100);
        let threshold = std::env::var("WHITEWATER_AUTO_SPLIT_APPEND_RATE")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(10_000);
        let sustained = std::env::var("WHITEWATER_AUTO_SPLIT_SUSTAINED_SAMPLES")
            .ok()
            .and_then(|value| value.parse::<u32>().ok())
            .unwrap_or(3);
        let cooldown = std::env::var("WHITEWATER_AUTO_SPLIT_COOLDOWN_SAMPLES")
            .ok()
            .and_then(|value| value.parse::<u32>().ok())
            .unwrap_or(30);
        let cold_threshold = std::env::var("WHITEWATER_AUTO_MERGE_APPEND_RATE")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(10);
        let cold_sustained = std::env::var("WHITEWATER_AUTO_MERGE_SUSTAINED_SAMPLES")
            .ok()
            .and_then(|value| value.parse::<u32>().ok())
            .unwrap_or(30);
        let cold_cooldown = std::env::var("WHITEWATER_AUTO_MERGE_COOLDOWN_SAMPLES")
            .ok()
            .and_then(|value| value.parse::<u32>().ok())
            .unwrap_or(60);
        let hot_sustained = std::env::var("WHITEWATER_HOT_KEY_SUSTAINED_SAMPLES")
            .ok()
            .and_then(|value| value.parse::<u32>().ok())
            .unwrap_or(2);
        let hot_clear = std::env::var("WHITEWATER_HOT_KEY_CLEAR_SAMPLES")
            .ok()
            .and_then(|value| value.parse::<u32>().ok())
            .unwrap_or(15);
        let internal_key = config.control_plane_key.clone()?;
        let peer_endpoints = control_endpoints.clone();
        let peer_client = match build_internal_client(Duration::from_secs(5)) {
            Ok(client) => client,
            Err(_) => return None,
        };
        let mut shutdown = shutdown_tx.subscribe();
        let demand = demand.clone();
        let control = control.clone();
        let endpoint = format!("http://127.0.0.1:{}", config.bind_addr.port());
        Some(tokio::spawn(async move {
            let mut split_trackers = BTreeMap::new();
            let mut hot_trackers = BTreeMap::new();
            let mut cold_trackers = BTreeMap::new();
            let mut previous_totals = BTreeMap::new();
            let client = reqwest::Client::new();
            let mut interval = tokio::time::interval(Duration::from_millis(interval_ms));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        for sample in demand.take_range_pressure_samples() {
                            // Hot-Key detection observes every windowed
                            // sample, not just windows that pass the split
                            // threshold: a dominant Key deserves isolation
                            // evidence even below the range-split rate.
                            let observation = hot_trackers
                                .entry(sample.range_id)
                                .or_insert_with(|| {
                                    HotKeyTracker::new(hot_sustained, hot_clear)
                                })
                                .observe(&sample);
                            match observation {
                                HotKeyObservation::Flagged(evidence) => {
                                    tracing::warn!(
                                        range_id = %sample.range_id,
                                        token = %evidence.token,
                                        share_bps = evidence.share_bps,
                                        "sustained hot Key flagged; isolating splits and tightened Writer feedback apply"
                                    );
                                    demand.set_hot_key(evidence);
                                }
                                HotKeyObservation::Cleared(range_id) => {
                                    tracing::info!(range_id = %range_id, "hot Key flag cleared after sustained sub-threshold share");
                                    demand.clear_hot_key(range_id);
                                }
                                HotKeyObservation::Unchanged => {
                                    if let Some(current) = hot_trackers
                                        .get(&sample.range_id)
                                        .and_then(|tracker| tracker.flagged())
                                    {
                                        demand.set_hot_key(current);
                                    }
                                }
                            }
                            let rate = sample.records.saturating_mul(1_000) / interval_ms;
                            let tracker = split_trackers.entry(sample.range_id)
                                .or_insert_with(|| SplitPressureTracker::new(threshold, sustained, cooldown));
                            if !tracker.observe(rate) { continue; }
                            let Some((feed, bounds)) = control
                                .active_feed_range_maps()
                                .await
                                .into_iter()
                                .find_map(|(feed, map)| {
                                    map.routes()
                                        .iter()
                                        .find(|route| route.range_id == sample.range_id)
                                        .map(|route| (feed, route.bounds.clone()))
                                })
                            else { continue; };
                            let split_at = match demand.hot_key(sample.range_id) {
                                Some(hot) => match isolation_split_boundary(&bounds, &hot) {
                                    Some(boundary) => {
                                        tracing::info!(
                                            range_id = %sample.range_id,
                                            token = %hot.token,
                                            boundary = %boundary,
                                            "splitting at hot Key edge to isolate it from unrelated Keys"
                                        );
                                        boundary
                                    }
                                    None => {
                                        tracing::info!(
                                            range_id = %sample.range_id,
                                            token = %hot.token,
                                            "hot Key is the only observed Key in this range; nothing to isolate"
                                        );
                                        continue;
                                    }
                                },
                                None => {
                                    let Some(split_at) = sample.split_token else { continue; };
                                    split_at
                                }
                            };
                            tokio::time::sleep(Duration::from_secs(5)).await;
                            let response = client
                                .post(format!("{endpoint}/v1/admin/ranges/split"))
                                .bearer_auth(&admin_key)
                                .json(&serde_json::json!({
                                    "request_id": uuid::Uuid::new_v4(),
                                    "feed": feed,
                                    "split_at": split_at,
                                    "batch_size": 256
                                }))
                                .send()
                                .await;
                            match response {
                                Ok(response) if response.status().is_success() => {
                                    tracing::info!(range_id = %sample.range_id, append_rate = rate, "automatic Active Range split activated");
                                }
                                Ok(response) => {
                                    let status = response.status();
                                    let detail = response.text().await.unwrap_or_default();
                                    tracing::warn!(range_id = %sample.range_id, %status, %detail, "automatic Active Range split deferred");
                                }
                                Err(error) => tracing::warn!(range_id = %sample.range_id, %error, "automatic Active Range split request failed"),
                            }
                        }
                        // Retire flags and trackers whose ranges no longer
                        // exist — splits and merges decided on any Node
                        // replace range IDs, and lingering evidence for a
                        // dead range can never clear itself.
                        let live_ranges: BTreeSet<_> = control
                            .active_feed_range_maps()
                            .await
                            .iter()
                            .flat_map(|(_, map)| {
                                map.routes().iter().map(|route| route.range_id)
                            })
                            .collect();
                        hot_trackers.retain(|range_id, _| live_ranges.contains(range_id));
                        for evidence in demand.hot_keys() {
                            if !live_ranges.contains(&evidence.range_id) {
                                demand.clear_hot_key(evidence.range_id);
                            }
                        }
                        if control_plane.status().await.state == "leader" {
                            let mut totals = BTreeMap::new();
                            for peer in peer_endpoints.all().await.values() {
                                let response = peer_client
                                    .get(format!("{}/internal/active-range/pressure", peer.trim_end_matches('/')))
                                    .header("x-whitewater-control-key", &internal_key)
                                    .send()
                                    .await;
                                let Ok(response) = response else { continue; };
                                let Ok(samples) = response.json::<Vec<whitewater::demand::RangePressureSample>>().await else { continue; };
                                for sample in samples {
                                    let entry = totals.entry(sample.range_id).or_insert(0_u64);
                                    *entry = entry.saturating_add(sample.records);
                                }
                            }
                            let rates = totals
                                .iter()
                                .map(|(range_id, total)| {
                                    let previous = previous_totals.insert(*range_id, *total).unwrap_or(*total);
                                    (*range_id, total.saturating_sub(previous).saturating_mul(1_000) / interval_ms)
                                })
                                .collect::<BTreeMap<_, _>>();
                            for (feed, map) in control.active_feed_range_maps().await {
                                for (left, right) in cold_adjacent_pairs(&map, &rates, cold_threshold) {
                                    let tracker = cold_trackers.entry((left, right)).or_insert_with(|| {
                                        ColdRangeTracker::new(cold_threshold, cold_sustained, cold_cooldown)
                                    });
                                    let left_rate = rates.get(&left).copied().unwrap_or(0);
                                    let right_rate = rates.get(&right).copied().unwrap_or(0);
                                    if !tracker.observe(left_rate, right_rate) { continue; }
                                    let response = client
                                        .post(format!("{endpoint}/v1/admin/ranges/merge"))
                                        .bearer_auth(&admin_key)
                                        .json(&serde_json::json!({
                                            "request_id": uuid::Uuid::new_v4(),
                                            "feed": feed,
                                            "left_range_id": left,
                                            "right_range_id": right
                                        }))
                                        .send()
                                        .await;
                                    match response {
                                        Ok(response) if response.status().is_success() => {
                                            tracing::info!(%left, %right, left_rate, right_rate, "automatic adjacent range merge activated");
                                        }
                                        Ok(response) => {
                                            let status = response.status();
                                            let detail = response.text().await.unwrap_or_default();
                                            tracing::warn!(%left, %right, %status, %detail, "automatic adjacent range merge deferred");
                                        }
                                        Err(error) => tracing::warn!(%left, %right, %error, "automatic adjacent range merge request failed"),
                                    }
                                }
                            }
                        }
                    }
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() { break; }
                    }
                }
            }
        }))
    });
    let subscription_tls = match (&config.subscription_mtls, &replica_append) {
        (Some(tls), Some(local)) => Some(
            SubscriptionTlsServer::from_config(tls, local.local_node())
                .await?
                .with_control(control.clone()),
        ),
        (Some(_), None) => anyhow::bail!("Subscription mTLS requires a storage replica"),
        _ => None,
    };
    let progress_authority: Arc<dyn SubscriptionPlacementAuthority> = match &control_plane {
        Some(plane) => plane.clone(),
        None => control.clone(),
    };
    let app_state = AppState {
        store,
        membership,
        demand: demand.clone(),
        autoscaler: Arc::new(Mutex::new(AutoscaleController::default())),
        reader_pacing: whitewater::reader::ReaderPacingTable::default(),
        control,
        control_plane: control_plane.clone(),
        progress_authority,
        storage_node_id: replica_append
            .as_ref()
            .map(|service| service.local_node().clone()),
        replica_append,
        subscription_progress,
        effect_journal,
        state_store_journal,
        subscription_mtls_enabled: config.subscription_mtls.is_some(),
        majority_append,
        control_endpoints: Arc::new(control_endpoints.clone()),
        internal_key: config.control_plane_key.clone(),
        internal_http: build_internal_client(Duration::from_secs(5))?,
        internal_mtls: internal_mtls.clone(),
        admin_auth,
        subscription_recovery_gates: Default::default(),
    };
    // Commit-boundary Index apply: each replica materializes the same
    // deterministic postings when its committed watermark advances — the
    // committed log is the replicated intent carrier. The periodic build
    // driver remains the catch-up backstop.
    if let (Some(service), Some(driver)) = (
        app_state.replica_append.as_ref(),
        whitewater::api::index_build_driver(&app_state),
    ) {
        service.set_index_apply(Arc::new(whitewater::index_build::IndexCommitApply::new(
            Arc::new(driver),
        )));
    }
    let tls_router = config
        .subscription_mtls
        .as_ref()
        .map(|_| internal_mtls_router(app_state.clone()));
    let state_store_replay_task = if app_state.storage_node_id.is_some() {
        let state = app_state.clone();
        let interval_ms = std::env::var("WHITEWATER_STATE_STORE_REPLAY_INTERVAL_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(250)
            .max(50);
        let mut shutdown = shutdown_tx.subscribe();
        Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(interval_ms));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        for outcome in whitewater::api::drive_local_state_stores(&state, 64).await {
                            use whitewater::api::StateStoreDriveOutcome as Outcome;
                            match outcome {
                                Outcome::Synced { store_id, applied, complete } => {
                                    tracing::debug!(%store_id, applied, complete, "state store replay pass committed")
                                }
                                Outcome::Refused { store_id, message } => {
                                    tracing::warn!(%store_id, %message, "state store replay refused")
                                }
                                Outcome::Failed { store_id, message } => {
                                    tracing::warn!(%store_id, %message, "state store replay pass failed")
                                }
                                Outcome::Idle | Outcome::NotOwner => {}
                            }
                        }
                        for outcome in
                            whitewater::api::drive_local_index_builds(&state, 128).await
                        {
                            use whitewater::api::IndexBuildDriveOutcome as Outcome;
                            match outcome {
                                Outcome::Building { index_id, generation, applied } => {
                                    tracing::debug!(%index_id, generation, applied, "index shadow build pass applied")
                                }
                                Outcome::Activated { index_id, generation } => {
                                    tracing::info!(%index_id, generation, "index shadow generation activated")
                                }
                                Outcome::Refused { index_id, message } => {
                                    tracing::warn!(%index_id, %message, "index rebuild refused")
                                }
                                Outcome::Failed { index_id, message } => {
                                    tracing::warn!(%index_id, %message, "index build pass failed")
                                }
                                Outcome::Superseded { .. } => {}
                            }
                        }
                        for outcome in
                            whitewater::api::drive_local_state_store_catch_ups(&state, 128).await
                        {
                            use whitewater::api::StateStoreCatchUpDriveOutcome as Outcome;
                            match outcome {
                                Outcome::Progress { store_id, installed, applied, complete } => {
                                    if installed > 0 || applied > 0 {
                                        tracing::debug!(%store_id, installed, applied, complete, "state store replica caught up")
                                    }
                                }
                                Outcome::Failed { store_id, message } => {
                                    tracing::warn!(%store_id, %message, "state store catch-up failed")
                                }
                                Outcome::NotReplica => {}
                            }
                        }
                    }
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() { break; }
                    }
                }
            }
        }))
    } else {
        None
    };
    let pipe_task = if app_state.storage_node_id.is_some() {
        let state = app_state.clone();
        let interval_ms = std::env::var("WHITEWATER_PIPE_DRIVE_INTERVAL_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(1_000)
            .max(100);
        let mut shutdown = shutdown_tx.subscribe();
        Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(interval_ms));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        for outcome in whitewater::api::drive_local_pipes(&state, 64).await {
                            match outcome {
                                whitewater::api::PipeDriveOutcome::Applied {
                                    pipe_id,
                                    effect_id,
                                } => {
                                    tracing::debug!(%pipe_id, %effect_id, "pipe cycle applied")
                                }
                                whitewater::api::PipeDriveOutcome::Failed {
                                    pipe_id,
                                    message,
                                } => {
                                    tracing::warn!(%pipe_id, %message, "pipe drive cycle failed")
                                }
                                whitewater::api::PipeDriveOutcome::Idle
                                | whitewater::api::PipeDriveOutcome::NotOwner => {}
                            }
                        }
                    }
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() { break; }
                    }
                }
            }
        }))
    } else {
        None
    };
    // Member operations recover a lost Subscription progress owner inside
    // their own request window, but that path needs live member traffic. The
    // leader-driven supervisor is the backstop: it probes placements even
    // when no member request is arriving, so a dead owner cannot wait for
    // traffic that may already have drained or stopped retrying.
    let subscription_recovery_task = if app_state.control_plane.is_some() {
        let state = app_state.clone();
        let supervisor = whitewater::api::SubscriptionProgressRecoverySupervisor::new(3);
        let mut shutdown = shutdown_tx.subscribe();
        Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(2));
            loop {
                tokio::select! {
                    _ = interval.tick() => supervisor.tick(&state).await,
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() { break; }
                    }
                }
            }
        }))
    } else {
        None
    };
    // StateStore journal mutations allocate their sequence on the journal
    // owner, so a dead owner wedges every writer until placement moves.
    // Pipe drives retry inside their own cycles, but no request path
    // triggers owner recovery for stores — this leader-driven backstop
    // probes owners and CASes dead ones onto the freshest surviving
    // replica; periodic catch-up converges the rest.
    let state_store_recovery_task = if app_state.control_plane.is_some() {
        let state = app_state.clone();
        let supervisor = whitewater::api::StateStoreOwnerRecoverySupervisor::new(3);
        let mut shutdown = shutdown_tx.subscribe();
        Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(2));
            loop {
                tokio::select! {
                    _ = interval.tick() => supervisor.tick(&state).await,
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() { break; }
                    }
                }
            }
        }))
    } else {
        None
    };
    // Pipe compute leases are drive authority for each Pipe's background
    // pump: the owner's `drive_local_pipes` tick is the only mover, and no
    // request path recovers a dead owner — so a lease can sit on a stopped
    // Node and silently stall forwards and window expiry until an
    // operator drains it. This leader-driven backstop probes lease owners
    // through /health and CASes dead ones onto a surviving storage Node.
    let pipe_lease_recovery_task = if app_state.control_plane.is_some() {
        let state = app_state.clone();
        let supervisor = whitewater::api::PipeComputeLeaseRecoverySupervisor::new(3);
        let mut shutdown = shutdown_tx.subscribe();
        Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(2));
            loop {
                tokio::select! {
                    _ = interval.tick() => supervisor.tick(&state).await,
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() { break; }
                    }
                }
            }
        }))
    } else {
        None
    };
    let app = router(app_state).layer(TraceLayer::new_for_http());
    let listener = TcpListener::bind(config.bind_addr).await?;
    let tls_task = match (subscription_tls, tls_router, &config.subscription_mtls) {
        (Some(server), Some(router), Some(tls)) => {
            let listener = TcpListener::bind(tls.bind_addr).await?;
            let shutdown = shutdown_tx.subscribe();
            Some(tokio::spawn(async move {
                server.serve(listener, router, shutdown).await
            }))
        }
        _ => None,
    };
    info!(node_id = %config.node_id, bind = %config.bind_addr, advertise = %config.advertise_url, "Whitewater node started");

    let public =
        axum::serve(listener, app).with_graceful_shutdown(shutdown_signal(shutdown_tx.clone()));
    if let Some(mut tls_task) = tls_task {
        let public = std::future::IntoFuture::into_future(public);
        tokio::pin!(public);
        tokio::select! {
            result = &mut public => {
                result?;
                tls_task.await??;
            }
            result = &mut tls_task => {
                result??;
                if !*shutdown_tx.borrow() {
                    anyhow::bail!("Subscription mTLS listener stopped unexpectedly");
                }
                public.await?;
            }
        }
    } else {
        public.await?;
    }
    if let Some(task) = capacity_reporter_task {
        task.abort();
    }
    let _ = membership_task.await;
    if let Some(recovery_task) = recovery_task {
        let _ = recovery_task.await;
    }
    if let Some(repair_task) = repair_task {
        let _ = repair_task.await;
    }
    if let Some(drain_task) = drain_task {
        let _ = drain_task.await;
    }
    if let Some(split_task) = split_task {
        let _ = split_task.await;
    }
    if let Some(pipe_task) = pipe_task {
        let _ = pipe_task.await;
    }
    if let Some(task) = subscription_recovery_task {
        let _ = task.await;
    }
    if let Some(task) = state_store_recovery_task {
        let _ = task.await;
    }
    if let Some(task) = pipe_lease_recovery_task {
        let _ = task.await;
    }
    if let Some(task) = state_store_replay_task {
        let _ = task.await;
    }
    if let Some(control_plane) = control_plane {
        drop(catalog_sync_task);
        let _ = control_plane.raft().shutdown().await;
    }
    Ok(())
}

async fn shutdown_signal(shutdown: watch::Sender<bool>) {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut terminate = signal(SignalKind::terminate()).expect("SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await.expect("Ctrl+C handler");
    let _ = shutdown.send(true);
}
