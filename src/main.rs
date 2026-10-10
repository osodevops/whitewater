use std::{collections::BTreeMap, sync::Arc, time::Duration};

use anyhow::Result;
use finnstream::{
    active_range::{
        cold_adjacent_pairs, ColdRangeTracker, HttpRecoveryTransport, HttpReplicaTransport,
        LocalRepairSupervisor, MajorityAppendCoordinator, RecoverySupervisor, ReplicaAppendService,
        SplitPressureTracker, StorageDrainStatus, StorageDrainSupervisor, StorageNodeId,
    },
    admin::AdminAuthenticator,
    api::{
        internal_mtls_router, router, AdminDrainDriver, AppState, InternalMtlsMaterial,
        SubscriptionTlsServer,
    },
    autoscale::AutoscaleController,
    config::NodeConfig,
    control::ControlController,
    control_plane::ControlPlane,
    demand::DemandMetrics,
    internal_plane::{CatalogSyncSupervisor, HttpCatalogSnapshotSource, InternalEndpoints},
    membership::{MemberAnnouncement, MembershipService},
    reader::{
        FjallSubscriptionProgressReplica, SubscriptionPlacementAuthority,
        SubscriptionProgressReplicaService,
    },
    storage::{FileLogStore, LogStore},
};
use openraft::BasicNode;
use tokio::{
    net::TcpListener,
    sync::{watch, Mutex},
};
use tower_http::trace::TraceLayer;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("finnstream=info,tower_http=info")),
        )
        .json()
        .init();

    let config = NodeConfig::from_env()?;
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
        .map(|(node_id, _)| StorageNodeId::try_new(format!("control-{node_id}")))
        .collect::<Result<Vec<_>, _>>()?;
    let controller = ControlController::open_with_storage_nodes(
        config.data_dir.join(catalog_file),
        store.clone(),
        eligible_storage_nodes,
    )?;
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
    let replica_append = config.storage_node_id.clone().map(|local_node| {
        Arc::new(ReplicaAppendService::new(
            config.data_dir.join("active-ranges"),
            local_node,
            control.clone(),
        ))
    });
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
            finnstream::effect::FjallEffectJournalReplica::open(path)
        })
        .await??;
        Some(Arc::new(finnstream::effect::EffectJournalService::new(
            local.local_node().clone(),
            control.clone(),
            Arc::new(replica),
        )))
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
            Some(Arc::new(MajorityAppendCoordinator::new(
                local,
                control.clone(),
                transport,
            )))
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
    let membership = Arc::new(MembershipService::new(
        MemberAnnouncement {
            node_id: config.node_id.clone(),
            api_url: config.advertise_url.clone(),
            capacity: config.capacity,
        },
        config.seeds.clone(),
        config.heartbeat_interval,
        config.member_timeout,
    ));
    let demand = DemandMetrics::default();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
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
        std::env::var("FINNSTREAM_ADMIN_API_KEY").ok(),
    ) {
        (Some(control_plane), Some(admin_key)) => {
            let driver = AdminDrainDriver::new(
                format!("http://127.0.0.1:{}", config.bind_addr.port()),
                admin_key,
                control_plane.clone(),
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
        let admin_key = std::env::var("FINNSTREAM_ADMIN_API_KEY").ok()?;
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
            let mut cold_trackers = BTreeMap::new();
            let mut previous_totals = BTreeMap::new();
            let client = reqwest::Client::new();
            let mut interval = tokio::time::interval(Duration::from_millis(interval_ms));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        for sample in demand.take_range_pressure_samples() {
                            let rate = sample.records.saturating_mul(1_000) / interval_ms;
                            let tracker = split_trackers.entry(sample.range_id)
                                .or_insert_with(|| SplitPressureTracker::new(threshold, sustained, cooldown));
                            if !tracker.observe(rate) { continue; }
                            let Some(split_at) = sample.split_token else { continue; };
                            let Some(feed) = control.active_feed_name_for_range(sample.range_id).await else { continue; };
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
                        if control_plane.status().await.state == "leader" {
                            let mut totals = BTreeMap::new();
                            for peer in peer_endpoints.all().await.values() {
                                let response = peer_client
                                    .get(format!("{}/internal/active-range/pressure", peer.trim_end_matches('/')))
                                    .header("x-whitewater-control-key", &internal_key)
                                    .send()
                                    .await;
                                let Ok(response) = response else { continue; };
                                let Ok(samples) = response.json::<Vec<finnstream::demand::RangePressureSample>>().await else { continue; };
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
        control,
        control_plane: control_plane.clone(),
        progress_authority,
        storage_node_id: replica_append
            .as_ref()
            .map(|service| service.local_node().clone()),
        replica_append,
        subscription_progress,
        effect_journal,
        subscription_mtls_enabled: config.subscription_mtls.is_some(),
        majority_append,
        control_endpoints: Arc::new(control_endpoints.clone()),
        internal_key: config.control_plane_key.clone(),
        internal_http: build_internal_client(Duration::from_secs(5))?,
        internal_mtls: internal_mtls.clone(),
        admin_auth,
    };
    let tls_router = config
        .subscription_mtls
        .as_ref()
        .map(|_| internal_mtls_router(app_state.clone()));
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
    info!(node_id = %config.node_id, bind = %config.bind_addr, advertise = %config.advertise_url, "FinnStream node started");

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
