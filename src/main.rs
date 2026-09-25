use std::{collections::BTreeMap, sync::Arc, time::Duration};

use anyhow::Result;
use finnstream::{
    admin::AdminAuthenticator,
    api::{router, AppState},
    autoscale::AutoscaleController,
    config::NodeConfig,
    control::ControlController,
    control_plane::ControlPlane,
    demand::DemandMetrics,
    membership::{MemberAnnouncement, MembershipService},
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
    let control = Arc::new(ControlController::open(
        config.data_dir.join(catalog_file),
        store.clone(),
    )?);
    let control_plane = match (config.control_node_id, config.control_plane_key.clone()) {
        (Some(node_id), Some(key)) => {
            let peers = config
                .control_nodes
                .iter()
                .map(|(id, address)| (*id, BasicNode::new(address)))
                .collect::<BTreeMap<_, _>>();
            Some(Arc::new(
                ControlPlane::start(
                    node_id,
                    peers,
                    key,
                    config.data_dir.join("control-plane-raft.json"),
                    control.clone(),
                )
                .await?,
            ))
        }
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
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let membership_task = tokio::spawn(membership.clone().run(shutdown_rx));
    let app = router(AppState {
        store,
        membership,
        demand: DemandMetrics::default(),
        autoscaler: Arc::new(Mutex::new(AutoscaleController::default())),
        control,
        control_plane: control_plane.clone(),
        admin_auth,
    })
    .layer(TraceLayer::new_for_http());
    let listener = TcpListener::bind(config.bind_addr).await?;
    info!(node_id = %config.node_id, bind = %config.bind_addr, advertise = %config.advertise_url, "FinnStream node started");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(shutdown_tx))
        .await?;
    let _ = membership_task.await;
    if let Some(control_plane) = control_plane {
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
