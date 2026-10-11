use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use axum::{
    body::Body,
    extract::{DefaultBodyLimit, Query, State},
    http::{header::AUTHORIZATION, HeaderMap, Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use base64::{
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
    Engine,
};
use futures_util::future::join_all;
use openraft::{
    error::{InstallSnapshotError, RaftError},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, SnapshotResponse, VoteRequest, VoteResponse,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::{watch, Mutex, Semaphore};
use uuid::Uuid;

use crate::{
    active_range::{
        stage_candidate_ranges_local, stage_merged_range_local, ActiveRangeAssignment,
        AppendIdentity, CandidateSplitStagingResult, CommitPosition, ControlPlaneFollowerMove,
        DrainMoveDriver, FollowerMoveControl, FollowerMoveCopyResult, KeyToken,
        MajorityAppendCoordinator, MajorityAppendError, MergeStagingResult, OwnerMoveEvidence,
        RangeId, RangePosition, ReadReplicaEvidence, RepairExportRequest, RepairExportResponse,
        RepairFrame, ReplicaAppendRequest, ReplicaAppendResponse, ReplicaAppendService,
        ReplicaCommitRequest, ReplicaCommitResponse, ReplicaProgressRequest,
        ReplicaProgressResponse, ReplicaReconcileRequest, ReplicaReconcileResponse, StorageNodeId,
        StoredRangeFrame, MAX_COMMITTED_READ_BYTES, MAX_REPLICA_FRAME_BASE64_BYTES,
    },
    admin::{AdminAuthError, AdminAuthenticator, CommandBatchRequest, WclRequest},
    autoscale::{AutoscaleController, AutoscalePolicy, ScaleDecision},
    codec::{decode_record, encode_record, MAX_FRAME_BYTES},
    config::SubscriptionMtlsConfig,
    control::{
        ControlController, ControlError, RangeMergePlan, RangeMovePlan, RangeMoveStage,
        RangeOwnerMovePlan, RangeSplitPlan, ReaderCutoverSnapshot, ReaderFrontierTranslation,
        ReplicatedCommand,
    },
    control_plane::{
        ControlNodeId, ControlPlane, ControlPlaneError, ControlTypeConfig, FullSnapshotRequest,
        InternalCommandsRequest, InternalCommandsResponse, InternalWriteResponse,
    },
    demand::{DemandMetrics, DemandSnapshot},
    domain::{AppendInput, CursorRecord, StorageStats, StoredRecord},
    membership::{JoinResponse, MemberAnnouncement, MemberView, MembershipService},
    reader::{
        translate_merge_reader_frontier, translate_split_reader_frontier, ReaderLineageEntry,
        SubscriptionCommitRequest, SubscriptionCommittedReadRequest, SubscriptionPrepareRequest,
        SubscriptionProgressError, SubscriptionProgressReplicaService,
    },
    storage::{LogStore, StorageError},
    writer::WriterServerFeedback,
};

#[derive(Clone)]
pub struct AuthenticatedSubscriptionPeer(pub StorageNodeId);

#[derive(Clone)]
pub struct AppState {
    pub store: Arc<dyn LogStore>,
    pub membership: Arc<MembershipService>,
    pub demand: DemandMetrics,
    pub autoscaler: Arc<Mutex<AutoscaleController>>,
    pub control: Arc<ControlController>,
    pub control_plane: Option<Arc<ControlPlane>>,
    pub progress_authority: Arc<dyn crate::reader::SubscriptionPlacementAuthority>,
    pub replica_append: Option<Arc<ReplicaAppendService>>,
    pub subscription_progress: Option<Arc<SubscriptionProgressReplicaService>>,
    pub effect_journal: Option<Arc<crate::effect::EffectJournalService>>,
    pub subscription_mtls_enabled: bool,
    pub majority_append: Option<Arc<MajorityAppendCoordinator>>,
    pub storage_node_id: Option<StorageNodeId>,
    pub control_endpoints: Arc<crate::internal_plane::InternalEndpoints>,
    pub internal_key: Option<String>,
    pub internal_http: reqwest::Client,
    pub internal_mtls: Option<Arc<InternalMtlsMaterial>>,
    pub admin_auth: AdminAuthenticator,
}

pub struct SubscriptionTlsServer {
    acceptor: tokio_rustls::TlsAcceptor,
    peer_pins: Arc<BTreeMap<StorageNodeId, std::collections::BTreeSet<[u8; 32]>>>,
    /// Catalog lookup for certificate pins carried by independently
    /// registered storage Nodes rather than static peer configuration.
    control: Option<Arc<ControlController>>,
}

/// Certificate material one Node presents to its peers for internal
/// mTLS traffic: the Node's client identity plus the trust root used to
/// verify pinned peers.
#[derive(Clone)]
pub struct InternalMtlsMaterial {
    pub identity_pem: Vec<u8>,
    pub ca_pem: Vec<u8>,
}

impl InternalMtlsMaterial {
    pub async fn from_config(
        config: &crate::config::SubscriptionMtlsConfig,
    ) -> anyhow::Result<Self> {
        let mut files = Vec::new();
        for path in [&config.cert_path, &config.key_path, &config.ca_path] {
            if tokio::fs::metadata(path).await?.len() > 1024 * 1024 {
                anyhow::bail!("internal mTLS certificate material exceeds the 1 MiB limit");
            }
            files.push(tokio::fs::read(path).await?);
        }
        let mut identity_pem = files[0].clone();
        identity_pem.extend_from_slice(&files[1]);
        let material = Self {
            identity_pem,
            ca_pem: files[2].clone(),
        };
        material.client(Duration::from_secs(1))?;
        Ok(material)
    }

    pub fn client(&self, timeout: Duration) -> Result<reqwest::Client, reqwest::Error> {
        reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(1))
            .timeout(timeout)
            .https_only(true)
            .tls_built_in_root_certs(false)
            .add_root_certificate(reqwest::Certificate::from_pem(&self.ca_pem)?)
            .identity(reqwest::Identity::from_pem(&self.identity_pem)?)
            .no_proxy()
            .build()
    }
}

impl SubscriptionTlsServer {
    pub async fn from_config(
        config: &SubscriptionMtlsConfig,
        local_node: &StorageNodeId,
    ) -> anyhow::Result<Self> {
        use rustls::pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};
        let mut files = Vec::new();
        for path in [&config.cert_path, &config.key_path, &config.ca_path] {
            if tokio::fs::metadata(path).await?.len() > 1024 * 1024 {
                anyhow::bail!("Subscription mTLS certificate material exceeds the 1 MiB limit");
            }
            files.push(tokio::fs::read(path).await?);
        }
        let certs = CertificateDer::pem_slice_iter(&files[0]).collect::<Result<Vec<_>, _>>()?;
        let leaf = certs
            .first()
            .ok_or_else(|| anyhow::anyhow!("Subscription mTLS certificate is missing"))?;
        if config
            .peer_pins
            .get(local_node)
            .is_none_or(|pins| !pins.contains(blake3::hash(leaf.as_ref()).as_bytes()))
        {
            anyhow::bail!("local Subscription mTLS certificate does not match this Node's pin");
        }
        let mut all_pins = std::collections::BTreeSet::new();
        for pins in config.peer_pins.values() {
            if pins.is_empty() || pins.iter().any(|pin| !all_pins.insert(*pin)) {
                anyhow::bail!("Subscription mTLS peer certificates must map to one Node each");
            }
        }
        let key = PrivateKeyDer::from_pem_slice(&files[1])?;
        let mut roots = rustls::RootCertStore::empty();
        for ca in CertificateDer::pem_slice_iter(&files[2]) {
            roots.add(ca?)?;
        }
        if roots.is_empty() {
            anyhow::bail!("Subscription mTLS trust roots are missing");
        }
        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots)).build()?;
        let mut server_config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()?
        .with_client_cert_verifier(verifier)
        .with_single_cert(certs, key)?;
        server_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        Ok(Self {
            acceptor: tokio_rustls::TlsAcceptor::from(Arc::new(server_config)),
            peer_pins: Arc::new(config.peer_pins.clone()),
            control: None,
        })
    }

    /// Registered storage Nodes distribute their certificate pins through
    /// the replicated catalog; supply the controller so pinned identities
    /// include registered Nodes, not only statically configured peers.
    pub fn with_control(mut self, control: Arc<ControlController>) -> Self {
        self.control = Some(control);
        self
    }

    pub async fn serve(
        self,
        listener: tokio::net::TcpListener,
        app: Router,
        mut shutdown: watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        let capacity = Arc::new(Semaphore::new(128));
        if *shutdown.borrow() {
            return Ok(());
        }
        loop {
            let permit = tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() { break; }
                    continue;
                }
                permit = capacity.clone().acquire_owned() => permit?,
            };
            let (socket, _) = tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() { break; }
                    continue;
                }
                accepted = listener.accept() => accepted?,
            };
            let acceptor = self.acceptor.clone();
            let pins = self.peer_pins.clone();
            let control = self.control.clone();
            let app = app.clone();
            tokio::spawn(async move {
                let _permit = permit;
                let stream =
                    match tokio::time::timeout(Duration::from_secs(5), acceptor.accept(socket))
                        .await
                    {
                        Ok(Ok(stream)) => stream,
                        Ok(Err(error)) => {
                            tracing::warn!(%error, "Subscription mTLS handshake rejected");
                            return;
                        }
                        Err(_) => {
                            tracing::warn!("Subscription mTLS handshake timed out");
                            return;
                        }
                    };
                let Some(leaf) = stream
                    .get_ref()
                    .1
                    .peer_certificates()
                    .and_then(|chain| chain.first())
                else {
                    tracing::warn!("Subscription mTLS peer certificate is missing");
                    return;
                };
                let fingerprint = blake3::hash(leaf.as_ref());
                let configured = pins.iter().find_map(|(node, pin)| {
                    pin.contains(fingerprint.as_bytes()).then(|| node.clone())
                });
                let node = match configured {
                    Some(node) => Some(node),
                    None => match &control {
                        Some(control) => {
                            control
                                .storage_node_by_cert_pin(fingerprint.as_bytes())
                                .await
                        }
                        None => None,
                    },
                };
                let Some(node) = node else {
                    tracing::warn!("Subscription mTLS peer certificate is not pinned to a Node");
                    return;
                };
                let service = hyper_util::service::TowerToHyperService::new(
                    app.layer(axum::Extension(AuthenticatedSubscriptionPeer(node))),
                );
                if let Err(error) = hyper_util::server::conn::auto::Builder::new(
                    hyper_util::rt::TokioExecutor::new(),
                )
                .serve_connection(hyper_util::rt::TokioIo::new(stream), service)
                .await
                {
                    tracing::warn!(%error, "Subscription mTLS connection failed");
                }
            });
        }
        Ok(())
    }
}

fn internal_routes(state: &AppState, require_shared_key: bool) -> Router<AppState> {
    let replica_append_route = post(replica_append)
        .layer(DefaultBodyLimit::max(
            MAX_REPLICA_FRAME_BASE64_BYTES + 1024 * 1024,
        ))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            authorize_replica_append,
        ));
    let replica_commit_route = post(replica_commit).layer(middleware::from_fn_with_state(
        state.clone(),
        authorize_replica_append,
    ));
    // On the mTLS plane the pinned peer certificate is the credential; the
    // shared Control Plane key stays required on the plain internal plane.
    let subscription_key_layer = |route: axum::routing::MethodRouter<AppState>| {
        if require_shared_key {
            route.layer(middleware::from_fn_with_state(
                state.clone(),
                authorize_replica_append,
            ))
        } else {
            route
        }
    };
    let subscription_prepare_route = subscription_key_layer(
        post(subscription_prepare_local).layer(DefaultBodyLimit::max(512 * 1024)),
    );
    let subscription_commit_route = subscription_key_layer(
        post(subscription_commit_local).layer(DefaultBodyLimit::max(512 * 1024)),
    );
    let subscription_committed_route = subscription_key_layer(
        post(subscription_committed_local).layer(DefaultBodyLimit::max(64 * 1024)),
    );
    let subscription_inspect_route = subscription_key_layer(
        post(subscription_inspect_local).layer(DefaultBodyLimit::max(64 * 1024)),
    );
    let subscription_adopt_route = subscription_key_layer(
        post(subscription_adopt_local).layer(DefaultBodyLimit::max(512 * 1024)),
    );
    let owner_append_route = post(owner_append).layer(middleware::from_fn_with_state(
        state.clone(),
        authorize_replica_append,
    ));
    let recovery_progress_route = post(recovery_progress).layer(middleware::from_fn_with_state(
        state.clone(),
        authorize_replica_append,
    ));
    let recovery_reconcile_route = post(recovery_reconcile).layer(middleware::from_fn_with_state(
        state.clone(),
        authorize_replica_append,
    ));
    let repair_export_route = post(repair_export).layer(middleware::from_fn_with_state(
        state.clone(),
        authorize_replica_append,
    ));
    let split_stage_route = post(split_stage_local).layer(middleware::from_fn_with_state(
        state.clone(),
        authorize_replica_append,
    ));
    let split_unfreeze_route = post(split_unfreeze_local).layer(middleware::from_fn_with_state(
        state.clone(),
        authorize_replica_append,
    ));
    let split_freeze_route = post(split_freeze_local).layer(middleware::from_fn_with_state(
        state.clone(),
        authorize_replica_append,
    ));
    let merge_stage_route = post(merge_stage_local).layer(middleware::from_fn_with_state(
        state.clone(),
        authorize_replica_append,
    ));
    let range_pressure_route = get(range_pressure_totals).layer(middleware::from_fn_with_state(
        state.clone(),
        authorize_replica_append,
    ));
    let move_export_route = post(move_export)
        .layer(DefaultBodyLimit::max(64 * 1024))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            authorize_replica_append,
        ));
    let move_stage_route = post(move_stage_local)
        .layer(DefaultBodyLimit::max(64 * 1024))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            authorize_replica_append,
        ));
    let move_freeze_route = post(move_freeze_owner)
        .layer(DefaultBodyLimit::max(64 * 1024))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            authorize_replica_append,
        ));
    let move_unfreeze_route = post(move_unfreeze_owner)
        .layer(DefaultBodyLimit::max(64 * 1024))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            authorize_replica_append,
        ));
    let owner_move_export_route = post(owner_move_export)
        .layer(DefaultBodyLimit::max(64 * 1024))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            authorize_replica_append,
        ));
    let owner_move_freeze_route = post(owner_move_freeze)
        .layer(DefaultBodyLimit::max(64 * 1024))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            authorize_replica_append,
        ));
    let owner_move_verify_route = post(owner_move_verify)
        .layer(DefaultBodyLimit::max(64 * 1024))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            authorize_replica_append,
        ));
    let owner_move_unfreeze_route = post(owner_move_unfreeze)
        .layer(DefaultBodyLimit::max(64 * 1024))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            authorize_replica_append,
        ));
    let read_range_route = post(read_owned_range_page)
        .layer(DefaultBodyLimit::max(64 * 1024))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            authorize_replica_append,
        ));
    let read_evidence_route = post(read_range_evidence)
        .layer(DefaultBodyLimit::max(64 * 1024))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            authorize_replica_append,
        ));
    let catalog_snapshot_route = post(catalog_snapshot)
        .layer(DefaultBodyLimit::max(16 * 1024))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            authorize_replica_append,
        ));
    Router::new()
        .route(
            "/internal/control-plane/raft/append",
            post(control_plane_append),
        )
        .route(
            "/internal/control-plane/raft/vote",
            post(control_plane_vote),
        )
        .route(
            "/internal/control-plane/raft/install-snapshot",
            post(control_plane_install_snapshot),
        )
        .route(
            "/internal/control-plane/raft/snapshot",
            post(control_plane_full_snapshot),
        )
        .route("/internal/catalog/snapshot", catalog_snapshot_route)
        .route("/internal/control-plane/write", post(control_plane_write))
        .route(
            "/internal/control-plane/commands",
            post(control_plane_commands),
        )
        .route(
            "/internal/active-range/replica/append",
            replica_append_route,
        )
        .route(
            "/internal/active-range/replica/commit",
            replica_commit_route,
        )
        .route("/internal/active-range/owner/append", owner_append_route)
        .route(
            "/internal/active-range/recovery/progress",
            recovery_progress_route,
        )
        .route(
            "/internal/active-range/recovery/reconcile",
            recovery_reconcile_route,
        )
        .route("/internal/active-range/repair/export", repair_export_route)
        .route(
            "/internal/active-range/split/stage-local",
            split_stage_route,
        )
        .route(
            "/internal/active-range/split/unfreeze-local",
            split_unfreeze_route,
        )
        .route(
            "/internal/active-range/split/freeze-local",
            split_freeze_route,
        )
        .route(
            "/internal/active-range/merge/stage-local",
            merge_stage_route,
        )
        .route("/internal/active-range/pressure", range_pressure_route)
        .route("/internal/active-range/move/export", move_export_route)
        .route("/internal/active-range/move/stage", move_stage_route)
        .route("/internal/active-range/move/freeze", move_freeze_route)
        .route("/internal/active-range/move/unfreeze", move_unfreeze_route)
        .route(
            "/internal/active-range/owner-move/export",
            owner_move_export_route,
        )
        .route(
            "/internal/active-range/owner-move/freeze",
            owner_move_freeze_route,
        )
        .route(
            "/internal/active-range/owner-move/verify",
            owner_move_verify_route,
        )
        .route(
            "/internal/active-range/owner-move/unfreeze",
            owner_move_unfreeze_route,
        )
        .route("/internal/active-range/read/committed", read_range_route)
        .route("/internal/active-range/read/evidence", read_evidence_route)
        .route(
            "/internal/subscription-progress/prepare",
            subscription_prepare_route,
        )
        .route(
            "/internal/subscription-progress/commit",
            subscription_commit_route,
        )
        .route(
            "/internal/subscription-progress/committed",
            subscription_committed_route,
        )
        .route(
            "/internal/subscription-progress/inspect",
            subscription_inspect_route,
        )
        .route(
            "/internal/subscription-progress/adopt",
            subscription_adopt_route,
        )
        .route(
            "/internal/effect-journal/prepare",
            subscription_key_layer(
                post(effect_journal_prepare_local).layer(DefaultBodyLimit::max(512 * 1024)),
            ),
        )
        .route(
            "/internal/effect-journal/commit",
            subscription_key_layer(
                post(effect_journal_commit_local).layer(DefaultBodyLimit::max(512 * 1024)),
            ),
        )
        .route(
            "/internal/effect-journal/committed",
            subscription_key_layer(
                post(effect_journal_committed_local).layer(DefaultBodyLimit::max(64 * 1024)),
            ),
        )
        .route(
            "/internal/effect-journal/inspect",
            subscription_key_layer(
                post(effect_journal_inspect_local).layer(DefaultBodyLimit::max(64 * 1024)),
            ),
        )
        .route(
            "/internal/effect-journal/adopt",
            subscription_key_layer(
                post(effect_journal_adopt_local).layer(DefaultBodyLimit::max(512 * 1024)),
            ),
        )
        .route(
            "/internal/effect-journal/apply",
            subscription_key_layer(
                post(effect_journal_apply_local).layer(DefaultBodyLimit::max(64 * 1024)),
            ),
        )
}

pub fn router(state: AppState) -> Router {
    let mut app = Router::new()
        .route("/health", get(health))
        .route("/v1/streams", get(list_streams).post(create_stream))
        .route("/v1/streams/describe", get(describe_stream))
        .route("/v1/records", get(read_records).post(append_record))
        .route("/v1/feeds/append", post(client_append))
        .route("/v1/writers/append", post(writer_session_append))
        .route("/v1/writers/append-batch", post(writer_batch_append))
        .route("/v1/readers/open", post(reader_open))
        .route("/v1/readers/fetch", post(reader_fetch))
        .route("/v1/readers/ack", post(reader_ack))
        .route("/v1/readers/close", post(reader_close))
        .route("/v1/readers/temporary/fetch", post(temporary_reader_fetch))
        .route(
            "/v1/subscriptions/members/join",
            post(subscription_member_join),
        )
        .route(
            "/v1/subscriptions/members/claim",
            post(subscription_member_claim),
        )
        .route(
            "/v1/subscriptions/members/renew",
            post(subscription_member_renew),
        )
        .route(
            "/v1/subscriptions/members/release",
            post(subscription_member_release),
        )
        .route(
            "/v1/subscriptions/members/ack",
            post(subscription_member_ack),
        )
        .route(
            "/v1/subscriptions/members/fetch",
            post(subscription_member_fetch),
        )
        .route(
            "/v1/subscriptions/members/state",
            get(subscription_member_state),
        )
        .route("/v1/feeds/records", get(read_feed_records))
        .route("/v1/admin/wcl", post(execute_admin_wcl))
        .route("/v1/admin/commands", post(execute_admin_commands))
        .route("/v1/admin/ranges/split", post(admin_split_range))
        .route("/v1/admin/ranges/merge", post(admin_merge_ranges))
        .route("/v1/admin/ranges/move-follower", post(admin_move_follower))
        .route("/v1/admin/ranges/move-owner", post(admin_move_owner))
        .route("/v1/admin/control-plane", get(control_plane_status))
        .route("/v1/control/execute", post(execute_admin_wcl))
        .route("/v1/node/metrics", get(node_metrics))
        .route("/v1/cluster/members", get(cluster_members))
        .route(
            "/v1/cluster/autoscale/recommend",
            post(autoscale_recommendation),
        )
        .route("/v1/cluster/join", post(cluster_join))
        .route("/v1/cluster/leave", post(cluster_leave));
    if !state.subscription_mtls_enabled {
        app = app.merge(internal_routes(&state, true));
    }
    app.with_state(state)
}

pub fn internal_mtls_router(state: AppState) -> Router {
    internal_routes(&state, false).with_state(state)
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    node_id: String,
    members: usize,
}

async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        node_id: state.membership.local().node_id.clone(),
        members: state.membership.members().await.len(),
    })
}

async fn execute_admin_wcl(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<WclRequest>,
) -> Result<impl IntoResponse, ApiError> {
    authorize_admin(&state, &headers)?;
    let _request = state.demand.begin_request();
    let request_id = request.request_id.unwrap_or_else(Uuid::new_v4);
    Ok(Json(match &state.control_plane {
        Some(control_plane) => {
            control_plane
                .execute_wcl_with_request_id(&request.script, request_id)
                .await?
        }
        None => {
            state
                .control
                .execute_with_request_id(&request.script, request_id)
                .await?
        }
    }))
}

async fn execute_admin_commands(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<CommandBatchRequest>,
) -> Result<impl IntoResponse, ApiError> {
    authorize_admin(&state, &headers)?;
    if request
        .commands
        .iter()
        .any(crate::control::Command::requires_internal_replica_authority)
    {
        return Err(ApiError::bad_request(
            "replica recovery and movement cutover require internal Node authority",
        ));
    }
    let _request = state.demand.begin_request();
    let request_id = request.request_id.unwrap_or_else(Uuid::new_v4);
    Ok(Json(match &state.control_plane {
        Some(control_plane) => {
            control_plane
                .execute_commands_with_request_id(request.commands, request_id)
                .await?
        }
        None => {
            state
                .control
                .execute_commands_with_request_id(request.commands, request_id)
                .await?
        }
    }))
}

fn authorize_admin(state: &AppState, headers: &HeaderMap) -> Result<(), ApiError> {
    let authorization = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok());
    state.admin_auth.authorize(authorization)?;
    Ok(())
}

async fn control_plane_status(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ApiError> {
    authorize_admin(&state, &headers)?;
    let control_plane = require_control_plane(&state)?;
    Ok(Json(control_plane.status().await))
}

fn require_control_plane(state: &AppState) -> Result<&Arc<ControlPlane>, ApiError> {
    state.control_plane.as_ref().ok_or_else(|| ApiError {
        status: StatusCode::SERVICE_UNAVAILABLE,
        message: "Control Plane is not configured on this Node".to_owned(),

        code: None,
    })
}

fn authorize_internal<'a>(
    state: &'a AppState,
    headers: &HeaderMap,
) -> Result<&'a Arc<ControlPlane>, ApiError> {
    let control_plane = require_control_plane(state)?;
    let supplied = headers
        .get("x-whitewater-control-key")
        .and_then(|value| value.to_str().ok());
    if !control_plane.authorize_internal(supplied) {
        return Err(ApiError {
            status: StatusCode::UNAUTHORIZED,
            message: "invalid Control Plane credential".to_owned(),

            code: None,
        });
    }
    Ok(control_plane)
}

/// Shared-key authentication for internal data-plane routes. Unlike
/// `authorize_internal`, a Control Plane is not required: non-voter
/// storage Nodes serve replica/progress routes with only the configured
/// internal credential.
fn authorize_internal_key(state: &AppState, headers: &HeaderMap) -> Result<(), ApiError> {
    let supplied = headers
        .get("x-whitewater-control-key")
        .and_then(|value| value.to_str().ok());
    match &state.control_plane {
        Some(control_plane) => {
            if control_plane.authorize_internal(supplied) {
                Ok(())
            } else {
                Err(ApiError {
                    status: StatusCode::UNAUTHORIZED,
                    message: "invalid Control Plane credential".to_owned(),
                    code: None,
                })
            }
        }
        None => match state.internal_key.as_deref() {
            Some(key)
                if supplied.is_some_and(|supplied| {
                    crate::control_plane::constant_time_equal(key.as_bytes(), supplied.as_bytes())
                }) =>
            {
                Ok(())
            }
            Some(_) => Err(ApiError {
                status: StatusCode::UNAUTHORIZED,
                message: "invalid Control Plane credential".to_owned(),
                code: None,
            }),
            None => Err(ApiError {
                status: StatusCode::SERVICE_UNAVAILABLE,
                message: "internal Control Plane credential is not configured on this Node"
                    .to_owned(),
                code: None,
            }),
        },
    }
}

async fn authorize_replica_append(
    State(state): State<AppState>,
    request: Request<Body>,
    next: Next,
) -> Result<Response, ApiError> {
    authorize_internal_key(&state, request.headers())?;
    Ok(next.run(request).await)
}

#[derive(Serialize)]
struct CatalogSnapshotResponse {
    revision: u64,
    snapshot_base64: String,
}

/// Serves the committed catalog snapshot to non-voter storage Nodes so
/// they can fence replica requests against real placement state. The
/// payload is bounded; a catalog exceeding the bound refuses rather than
/// streaming unbounded state into an internal caller.
async fn catalog_snapshot(
    State(state): State<AppState>,
) -> Result<Json<CatalogSnapshotResponse>, ApiError> {
    let bytes = state
        .control
        .snapshot_bytes()
        .await
        .map_err(|error| ApiError::unavailable(error.to_string()))?;
    if bytes.len() > crate::internal_plane::MAX_CATALOG_SNAPSHOT_BYTES {
        return Err(ApiError::unavailable(
            "catalog snapshot exceeds the internal transfer bound",
        ));
    }
    let revision = state.control.revision().await;
    Ok(Json(CatalogSnapshotResponse {
        revision,
        snapshot_base64: STANDARD.encode(bytes),
    }))
}

async fn control_plane_append(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<AppendEntriesRequest<ControlTypeConfig>>,
) -> Result<Json<Result<AppendEntriesResponse<ControlNodeId>, RaftError<ControlNodeId>>>, ApiError>
{
    let control_plane = authorize_internal(&state, &headers)?;
    Ok(Json(control_plane.raft().append_entries(request).await))
}

async fn control_plane_vote(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<VoteRequest<ControlNodeId>>,
) -> Result<Json<Result<VoteResponse<ControlNodeId>, RaftError<ControlNodeId>>>, ApiError> {
    let control_plane = authorize_internal(&state, &headers)?;
    Ok(Json(control_plane.raft().vote(request).await))
}

async fn control_plane_install_snapshot(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<InstallSnapshotRequest<ControlTypeConfig>>,
) -> Result<
    Json<
        Result<
            InstallSnapshotResponse<ControlNodeId>,
            RaftError<ControlNodeId, InstallSnapshotError>,
        >,
    >,
    ApiError,
> {
    let control_plane = authorize_internal(&state, &headers)?;
    Ok(Json(control_plane.raft().install_snapshot(request).await))
}

async fn control_plane_full_snapshot(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<FullSnapshotRequest>,
) -> Result<
    Json<Result<SnapshotResponse<ControlNodeId>, openraft::error::Fatal<ControlNodeId>>>,
    ApiError,
> {
    let control_plane = authorize_internal(&state, &headers)?;
    Ok(Json(
        control_plane
            .raft()
            .install_full_snapshot(
                request.vote,
                openraft::storage::Snapshot {
                    meta: request.meta,
                    snapshot: Box::new(std::io::Cursor::new(request.data)),
                },
            )
            .await,
    ))
}

async fn control_plane_commands(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<InternalCommandsRequest>,
) -> Result<Json<InternalCommandsResponse>, ApiError> {
    let control_plane = authorize_internal(&state, &headers)?;
    let leader_id = control_plane.status().await.leader_id;
    let response = match control_plane
        .execute_commands_with_request_id(request.commands, request.request_id)
        .await
    {
        Ok(execution) => InternalCommandsResponse {
            execution: Some(execution),
            error: None,
            leader_id,
        },
        Err(error) => InternalCommandsResponse {
            execution: None,
            error: Some(error.to_string()),
            leader_id,
        },
    };
    Ok(Json(response))
}

async fn control_plane_write(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<ReplicatedCommand>,
) -> Result<Json<InternalWriteResponse>, ApiError> {
    let control_plane = authorize_internal(&state, &headers)?;
    let leader_id = control_plane.status().await.leader_id;
    let response = match control_plane.write_local(request).await {
        Ok(result) => InternalWriteResponse {
            result: Some(result),
            error: None,
            leader_id,
        },
        Err(error) => InternalWriteResponse {
            result: None,
            error: Some(error.to_string()),
            leader_id,
        },
    };
    Ok(Json(response))
}

async fn replica_append(
    State(state): State<AppState>,
    Json(request): Json<ReplicaAppendRequest>,
) -> Result<Json<ReplicaAppendResponse>, ApiError> {
    let service = state.replica_append.as_ref().ok_or_else(|| ApiError {
        status: StatusCode::SERVICE_UNAVAILABLE,
        message: "Active Range replica storage is not configured on this Node".to_owned(),

        code: None,
    })?;
    Ok(Json(match service.append(request).await {
        Ok(result) => ReplicaAppendResponse {
            result: Some(result),
            error: None,
        },
        Err(error) => ReplicaAppendResponse {
            result: None,
            error: Some(error),
        },
    }))
}

async fn replica_commit(
    State(state): State<AppState>,
    Json(request): Json<ReplicaCommitRequest>,
) -> Result<Json<ReplicaCommitResponse>, ApiError> {
    let service = state.replica_append.as_ref().ok_or_else(|| ApiError {
        status: StatusCode::SERVICE_UNAVAILABLE,
        message: "Active Range replica storage is not configured on this Node".to_owned(),

        code: None,
    })?;
    Ok(Json(match service.commit(request).await {
        Ok(result) => ReplicaCommitResponse {
            result: Some(result),
            error: None,
        },
        Err(error) => ReplicaCommitResponse {
            result: None,
            error: Some(error),
        },
    }))
}

fn check_subscription_peer(
    state: &AppState,
    peer: Option<&AuthenticatedSubscriptionPeer>,
) -> Result<(), ApiError> {
    // Member requests land on any Node, so the coordinator driving progress
    // mutations is not necessarily the assigned owner. The pinned peer
    // certificate proves the caller is a cluster Node; ownership_epoch
    // fencing and quorum evidence reject a stale coordinator's writes.
    if state.subscription_mtls_enabled && peer.is_none() {
        return Err(ApiError {
            status: StatusCode::UNAUTHORIZED,
            message: "Subscription progress requires a pinned Node mTLS identity".to_owned(),

            code: None,
        });
    }
    Ok(())
}

pub const SUBSCRIPTION_MEMBER_DEFAULT_LEASE_TICKS: u64 = 30;
pub const SUBSCRIPTION_MEMBER_MAX_LEASE_TICKS: u64 = 600;

#[derive(Deserialize)]
struct SubscriptionMemberJoinRequest {
    subscription: String,
    request_id: Uuid,
    member_id: Uuid,
}

#[derive(Deserialize)]
struct SubscriptionMemberLeaseRequest {
    subscription: String,
    request_id: Uuid,
    member_id: Uuid,
    member_epoch: u64,
    work_id: Uuid,
    lease_ticks: Option<u64>,
    lease_epoch: Option<u64>,
}

#[derive(Deserialize)]
struct SubscriptionMemberReleaseRequest {
    subscription: String,
    request_id: Uuid,
    member_id: Uuid,
    member_epoch: u64,
    work_id: Uuid,
    lease_epoch: u64,
}

#[derive(Deserialize)]
struct SubscriptionMemberAckRequest {
    subscription: String,
    request_id: Uuid,
    member_id: Uuid,
    member_epoch: u64,
    work_id: Uuid,
    lease_epoch: u64,
    cursor: Option<String>,
    positions: Option<BTreeMap<RangeId, String>>,
}

#[derive(Deserialize)]
struct SubscriptionMemberStateQuery {
    subscription: String,
}

#[derive(Serialize)]
struct SubscriptionMemberResponse {
    subscription: String,
    member_epoch: Option<u64>,
    tick: u64,
    ownership_epoch: u64,
    lease: Option<crate::reader::SubscriptionWorkLease>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    member_epochs: BTreeMap<Uuid, u64>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    leases: BTreeMap<Uuid, crate::reader::SubscriptionWorkLease>,
}

async fn member_coordinator(
    state: &AppState,
    subscription: &str,
) -> Result<
    (
        crate::reader::SubscriptionProgressCoordinator,
        crate::reader::SubscriptionProgressAssignment,
        crate::control::SubscriptionDefinition,
    ),
    ApiError,
> {
    let definition = state
        .control
        .active_subscription_by_name(subscription)
        .await
        .ok_or_else(|| ApiError::bad_request("Subscription does not exist or is not active"))?;
    let assignment = state
        .control
        .active_subscription_progress_assignment_by_id(definition.subscription_id)
        .await
        .ok_or_else(|| ApiError::unavailable("Subscription progress placement is unavailable"))?;
    let (coordinator, assignment) = member_assignment_coordinator(state, assignment)?;
    Ok((coordinator, assignment, definition))
}

fn member_assignment_coordinator(
    state: &AppState,
    assignment: crate::reader::SubscriptionProgressAssignment,
) -> Result<
    (
        crate::reader::SubscriptionProgressCoordinator,
        crate::reader::SubscriptionProgressAssignment,
    ),
    ApiError,
> {
    let transport = match &state.internal_mtls {
        Some(material) => crate::reader::HttpSubscriptionProgressTransport::new_mtls(
            assignment.clone(),
            state.control_endpoints.as_ref().clone(),
            &material.ca_pem,
            &material.identity_pem,
            Duration::from_secs(5),
        )
        .map_err(|_| {
            ApiError::unavailable("Subscription progress mTLS transport failed to build")
        })?,
        None => crate::reader::HttpSubscriptionProgressTransport::new(
            assignment.clone(),
            state.control_endpoints.as_ref().clone(),
            state
                .internal_key
                .as_ref()
                .ok_or_else(|| ApiError::unavailable("internal credential is not configured"))?
                .clone(),
            Duration::from_secs(5),
        )
        .map_err(|_| ApiError::unavailable("Subscription progress transport failed to build"))?,
    };
    Ok((
        crate::reader::SubscriptionProgressCoordinator::new(
            assignment.clone(),
            std::sync::Arc::new(transport),
        ),
        assignment,
    ))
}

async fn member_coordinator_recovering(
    state: &AppState,
    subscription: &str,
) -> Result<
    (
        crate::reader::SubscriptionProgressCoordinator,
        crate::reader::SubscriptionProgressAssignment,
        crate::control::SubscriptionDefinition,
    ),
    ApiError,
> {
    let (coordinator, assignment, definition) = member_coordinator(state, subscription).await?;
    let evidence = match coordinator.inspect_evidence().await {
        Ok(evidence) => evidence,
        Err(SubscriptionProgressError::NoQuorum) => {
            return member_coordinator_recovered(state, &coordinator, definition).await;
        }
        Err(error) => return Err(subscription_progress_api_error(error)),
    };
    if !evidence.iter().any(|(node, _)| node == &assignment.owner)
        || subscription_placement_diverged(&evidence)
    {
        // The owner is unreachable, or reachable replicas hold divergent
        // committed/member state. Recovery advances the ownership epoch
        // (possibly keeping the same owner) and adopts the recovered
        // frontier under the new epoch, which heals lagging replicas.
        return member_coordinator_recovered(state, &coordinator, definition).await;
    }
    Ok((coordinator, assignment, definition))
}

/// True when reachable replicas hold divergent committed progress or member
/// state: recovery must advance the ownership epoch and adopt the recovered
/// frontier so lagging replicas heal before further mutations.
fn subscription_placement_diverged(
    evidence: &[(
        crate::active_range::StorageNodeId,
        crate::reader::SubscriptionProgressInspection,
    )],
) -> bool {
    let mut states = evidence
        .iter()
        .map(|(_, inspection)| (&inspection.committed, &inspection.members));
    match states.next() {
        None => false,
        Some(first) => states.any(|state| state != first),
    }
}

async fn member_coordinator_recovered(
    state: &AppState,
    coordinator: &crate::reader::SubscriptionProgressCoordinator,
    definition: crate::control::SubscriptionDefinition,
) -> Result<
    (
        crate::reader::SubscriptionProgressCoordinator,
        crate::reader::SubscriptionProgressAssignment,
        crate::control::SubscriptionDefinition,
    ),
    ApiError,
> {
    // The ownership CAS must replicate through the placement authority (the
    // Control Plane when configured); a local-catalog write would strand the
    // new epoch on the serving Node only. While a killed voter leaves the
    // Control Plane briefly leaderless the authority reports Unavailable:
    // retrying is safe because every attempt re-inspects replica evidence
    // and re-derives the CAS expected epoch, so a CAS that already landed
    // converges through adoption instead of guessing.
    let mut result = Err(SubscriptionProgressError::Unavailable);
    for attempt in 0..6 {
        result = coordinator
            .recover_lost_owner(state.progress_authority.as_ref())
            .await;
        let retryable = matches!(
            result,
            Err(SubscriptionProgressError::Unavailable) | Err(SubscriptionProgressError::NoQuorum)
        );
        if !retryable || attempt == 5 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(750)).await;
    }
    let outcome = result.map_err(subscription_progress_api_error)?;
    let (coordinator, assignment) = member_assignment_coordinator(state, outcome.assignment)?;
    Ok((coordinator, assignment, definition))
}

fn member_lease_ticks(requested: Option<u64>) -> Result<u64, ApiError> {
    let ticks = requested.unwrap_or(SUBSCRIPTION_MEMBER_DEFAULT_LEASE_TICKS);
    if ticks == 0 || ticks > SUBSCRIPTION_MEMBER_MAX_LEASE_TICKS {
        return Err(ApiError::bad_request(
            "lease_ticks exceeds the bounded Subscription member lease duration",
        ));
    }
    Ok(ticks)
}

fn member_response(
    subscription: String,
    ownership_epoch: u64,
    member_state: crate::reader::SubscriptionMemberState,
) -> SubscriptionMemberResponse {
    SubscriptionMemberResponse {
        subscription,
        member_epoch: None,
        tick: member_state.last_tick,
        ownership_epoch,
        lease: None,
        member_epochs: member_state.member_epochs,
        leases: member_state.leases,
    }
}

async fn read_member_state(
    coordinator: &crate::reader::SubscriptionProgressCoordinator,
) -> Result<crate::reader::SubscriptionMemberState, ApiError> {
    coordinator
        .member_state()
        .await
        .map_err(subscription_progress_api_error)
}

/// Committed request replay: returns the recorded member state when the latest
/// mutation already carries `request_id` and `lease_ops` match; conflicts when
/// the request identity was reused with different operations.
async fn member_replayed(
    coordinator: &crate::reader::SubscriptionProgressCoordinator,
    request_id: Uuid,
    lease_ops: &[crate::reader::SubscriptionLeaseOp],
) -> Result<Option<crate::reader::SubscriptionMemberState>, ApiError> {
    let Some(latest) = coordinator
        .read_committed()
        .await
        .map_err(subscription_progress_api_error)?
    else {
        return Ok(None);
    };
    if latest.request_id != request_id {
        return Ok(None);
    }
    if latest.lease_ops != lease_ops {
        return Err(ApiError {
            status: StatusCode::CONFLICT,
            message: "request_id was already used with different Subscription member operations"
                .to_string(),

            code: None,
        });
    }
    Ok(Some(read_member_state(coordinator).await?))
}

/// Applies member ops with request-identity dedup: a retry of a committed
/// request returns the recorded outcome rather than a second mutation.
async fn member_apply(
    coordinator: &crate::reader::SubscriptionProgressCoordinator,
    request_id: Uuid,
    lease_ops: Vec<crate::reader::SubscriptionLeaseOp>,
) -> Result<crate::reader::SubscriptionMemberState, ApiError> {
    if let Some(state) = member_replayed(coordinator, request_id, &lease_ops).await? {
        return Ok(state);
    }
    let tick = read_member_state(coordinator)
        .await?
        .last_tick
        .saturating_add(1);
    coordinator
        .apply_member_ops(request_id, tick, lease_ops)
        .await
        .map_err(subscription_progress_api_error)?;
    read_member_state(coordinator).await
}

async fn subscription_member_join(
    State(state): State<AppState>,
    Json(request): Json<SubscriptionMemberJoinRequest>,
) -> Result<Json<SubscriptionMemberResponse>, ApiError> {
    let (coordinator, assignment, definition) =
        member_coordinator_recovering(&state, &request.subscription).await?;
    let replayed = coordinator
        .read_committed()
        .await
        .map_err(subscription_progress_api_error)?
        .filter(|latest| latest.request_id == request.request_id);
    let member_state = if let Some(latest) = replayed {
        let joined = latest.lease_ops.iter().any(|op| {
            matches!(
                op,
                crate::reader::SubscriptionLeaseOp::Join { member_id, .. }
                    if *member_id == request.member_id
            )
        });
        if !joined {
            return Err(ApiError {
                status: StatusCode::CONFLICT,
                message:
                    "request_id was already used with different Subscription member operations"
                        .to_string(),

                code: None,
            });
        }
        read_member_state(&coordinator).await?
    } else if coordinator
        .read_committed()
        .await
        .map_err(subscription_progress_api_error)?
        .is_some()
    {
        // An established frontier already exists: the Join rides the next
        // sequence under the current cursor boundary.
        let prior = read_member_state(&coordinator).await?;
        let member_epoch = prior
            .member_epochs
            .get(&request.member_id)
            .copied()
            .map_or(1, |epoch| epoch.saturating_add(1));
        coordinator
            .apply_member_ops(
                request.request_id,
                prior.last_tick.saturating_add(1),
                vec![crate::reader::SubscriptionLeaseOp::Join {
                    member_id: request.member_id,
                    member_epoch,
                }],
            )
            .await
            .map_err(subscription_progress_api_error)?;
        read_member_state(&coordinator).await?
    } else {
        // First member: establish the declared-start frontier and the Join
        // lease transition in one atomic sequence-1 quorum mutation.
        let range_assignments = state
            .control
            .active_range_assignments_for_feed(definition.feed_id)
            .await;
        if range_assignments.is_empty() {
            return Err(ApiError::unavailable(
                "Subscription source Feed has no Active Range placement",
            ));
        }
        let (start_cursor, positions) = match &definition.start {
            crate::control::ReaderStart::Beginning => (
                "beginning".to_owned(),
                range_assignments
                    .iter()
                    .map(|assignment| (assignment.range_id, String::new()))
                    .collect::<BTreeMap<_, _>>(),
            ),
            crate::control::ReaderStart::Now => {
                // Pin each range's committed tail Cursor so only records
                // appended after the first join are delivered.
                let mut positions = BTreeMap::new();
                let mut tail: Option<((i64, Uuid), String)> = None;
                for range_assignment in &range_assignments {
                    let page = fetch_range_page(
                        &state,
                        ReadRangePageRequest {
                            assignment: range_assignment.clone(),
                            after: None,
                            expected_commit: None,
                            after_cursor: None,
                            tail_count: Some(1),
                            single_range: true,
                            page_limit: Some(1),
                        },
                    )
                    .await?;
                    let frame = page.frames.last();
                    positions.insert(
                        range_assignment.range_id,
                        frame.map(|frame| frame.cursor.clone()).unwrap_or_default(),
                    );
                    if let Some(frame) = frame {
                        let decoded = STANDARD
                            .decode(frame.frame_base64.as_bytes())
                            .map_err(|_| {
                                ApiError::unavailable(
                                    "current owner returned an invalid frame encoding",
                                )
                            })?;
                        let record = decode_record(&decoded)
                            .map_err(|error| ApiError::unavailable(error.to_string()))?;
                        let key = (record.ingest_time_ns, record.message_id);
                        if tail.as_ref().is_none_or(|(current, _)| key > *current) {
                            tail = Some((key, frame.cursor.clone()));
                        }
                    }
                }
                (
                    tail.map(|(_, cursor)| cursor)
                        .unwrap_or_else(|| "now".to_owned()),
                    positions,
                )
            }
            crate::control::ReaderStart::Cursor(_) | crate::control::ReaderStart::Timestamp(_) => {
                return Err(ApiError {
                    status: StatusCode::CONFLICT,
                    message: "Subscription member sessions support beginning and now starts; explicit-Cursor and timestamp starts need merged-order position resolution"
                        .to_owned(),

                    code: None,
})
            }
        };
        coordinator
            .apply(crate::reader::SubscriptionProgressMutation {
                subscription_id: assignment.subscription_id,
                feed_id: definition.feed_id,
                ownership_epoch: assignment.ownership_epoch,
                sequence: 1,
                request_id: request.request_id,
                expected_cursor: None,
                cursor: start_cursor,
                positions,
                tick: 1,
                lease_ops: vec![crate::reader::SubscriptionLeaseOp::Join {
                    member_id: request.member_id,
                    member_epoch: 1,
                }],
            })
            .await
            .map_err(subscription_progress_api_error)?;
        read_member_state(&coordinator).await?
    };
    let mut response = member_response(
        request.subscription,
        assignment.ownership_epoch,
        member_state,
    );
    response.member_epoch = response.member_epochs.get(&request.member_id).copied();
    Ok(Json(response))
}

async fn subscription_member_claim(
    State(state): State<AppState>,
    Json(request): Json<SubscriptionMemberLeaseRequest>,
) -> Result<Json<SubscriptionMemberResponse>, ApiError> {
    let lease_ticks = member_lease_ticks(request.lease_ticks)?;
    let (coordinator, assignment, _definition) =
        member_coordinator_recovering(&state, &request.subscription).await?;
    let member_state = member_apply(
        &coordinator,
        request.request_id,
        vec![crate::reader::SubscriptionLeaseOp::Claim {
            work_id: request.work_id,
            member_id: request.member_id,
            member_epoch: request.member_epoch,
            lease_ticks,
        }],
    )
    .await?;
    let mut response = member_response(
        request.subscription,
        assignment.ownership_epoch,
        member_state,
    );
    response.member_epoch = Some(request.member_epoch);
    response.lease = response.leases.get(&request.work_id).cloned();
    Ok(Json(response))
}

async fn subscription_member_renew(
    State(state): State<AppState>,
    Json(request): Json<SubscriptionMemberLeaseRequest>,
) -> Result<Json<SubscriptionMemberResponse>, ApiError> {
    let lease_ticks = member_lease_ticks(request.lease_ticks)?;
    let lease_epoch = request.lease_epoch.ok_or_else(|| {
        ApiError::bad_request("lease_epoch is required to renew a Subscription member lease")
    })?;
    let (coordinator, assignment, _definition) =
        member_coordinator_recovering(&state, &request.subscription).await?;
    let member_state = member_apply(
        &coordinator,
        request.request_id,
        vec![crate::reader::SubscriptionLeaseOp::Renew {
            work_id: request.work_id,
            member_id: request.member_id,
            member_epoch: request.member_epoch,
            lease_epoch,
            lease_ticks,
        }],
    )
    .await?;
    let mut response = member_response(
        request.subscription,
        assignment.ownership_epoch,
        member_state,
    );
    response.member_epoch = Some(request.member_epoch);
    response.lease = response.leases.get(&request.work_id).cloned();
    Ok(Json(response))
}

async fn subscription_member_release(
    State(state): State<AppState>,
    Json(request): Json<SubscriptionMemberReleaseRequest>,
) -> Result<Json<SubscriptionMemberResponse>, ApiError> {
    let (coordinator, assignment, _definition) =
        member_coordinator_recovering(&state, &request.subscription).await?;
    let member_state = member_apply(
        &coordinator,
        request.request_id,
        vec![crate::reader::SubscriptionLeaseOp::Release {
            work_id: request.work_id,
            member_id: request.member_id,
            member_epoch: request.member_epoch,
            lease_epoch: request.lease_epoch,
        }],
    )
    .await?;
    let mut response = member_response(
        request.subscription,
        assignment.ownership_epoch,
        member_state,
    );
    response.member_epoch = Some(request.member_epoch);
    Ok(Json(response))
}

async fn subscription_member_ack(
    State(state): State<AppState>,
    Json(request): Json<SubscriptionMemberAckRequest>,
) -> Result<Json<SubscriptionMemberResponse>, ApiError> {
    let (coordinator, assignment, _definition) =
        member_coordinator_recovering(&state, &request.subscription).await?;
    match (request.cursor, request.positions) {
        (Some(cursor), Some(positions)) => {
            if cursor.is_empty() || positions.is_empty() {
                return Err(ApiError::bad_request(
                    "Subscription acknowledgement requires a non-empty Cursor and progress positions",
                ));
            }
            if let Some(latest) = coordinator
                .read_committed()
                .await
                .map_err(subscription_progress_api_error)?
                .filter(|latest| latest.request_id == request.request_id)
            {
                if latest.cursor != cursor || latest.positions != positions {
                    return Err(ApiError {
                        status: StatusCode::CONFLICT,
                        message: "request_id was already used with a different Subscription acknowledgement"
                            .to_string(),

                        code: None,
});
                }
                let member_state = read_member_state(&coordinator).await?;
                let mut response = member_response(
                    request.subscription,
                    assignment.ownership_epoch,
                    member_state,
                );
                response.member_epoch = Some(request.member_epoch);
                return Ok(Json(response));
            }
            let grant = crate::reader::SubscriptionWorkLease {
                work_id: request.work_id,
                member_id: request.member_id,
                member_epoch: request.member_epoch,
                lease_epoch: request.lease_epoch,
                expires_at_tick: 0,
            };
            let tick = read_member_state(&coordinator)
                .await?
                .last_tick
                .saturating_add(1);
            coordinator
                .acknowledge(&grant, tick, request.request_id, cursor, positions)
                .await
                .map_err(subscription_progress_api_error)?;
            let member_state = read_member_state(&coordinator).await?;
            let mut response = member_response(
                request.subscription,
                assignment.ownership_epoch,
                member_state,
            );
            response.member_epoch = Some(request.member_epoch);
            Ok(Json(response))
        }
        (None, None) => {
            let member_state = member_apply(
                &coordinator,
                request.request_id,
                vec![crate::reader::SubscriptionLeaseOp::Release {
                    work_id: request.work_id,
                    member_id: request.member_id,
                    member_epoch: request.member_epoch,
                    lease_epoch: request.lease_epoch,
                }],
            )
            .await?;
            let mut response = member_response(
                request.subscription,
                assignment.ownership_epoch,
                member_state,
            );
            response.member_epoch = Some(request.member_epoch);
            Ok(Json(response))
        }
        _ => Err(ApiError::bad_request(
            "cursor and positions must be supplied together for a Subscription acknowledgement",
        )),
    }
}

pub const SUBSCRIPTION_MEMBER_FETCH_MAX_LIMIT: usize = 256;

#[derive(Deserialize)]
struct SubscriptionMemberFetchRequest {
    subscription: String,
    request_id: Uuid,
    member_id: Uuid,
    member_epoch: u64,
    work_id: Uuid,
    lease_epoch: u64,
    limit: Option<usize>,
}

#[derive(Serialize)]
struct SubscriptionMemberFetchResponse {
    request_id: Uuid,
    records: Vec<RecordResponse>,
    cursor: Option<String>,
    positions: BTreeMap<RangeId, String>,
    ownership_epoch: u64,
}

/// Delivers the bounded page of records after the shared Subscription frontier
/// to a member holding a live work lease. The returned per-range positions and
/// Cursor are what the member supplies to `ack`; concurrent members may fetch
/// overlapping windows because acknowledgement is fenced by the committed
/// Cursor, not by delivery.
async fn subscription_member_fetch(
    State(state): State<AppState>,
    Json(request): Json<SubscriptionMemberFetchRequest>,
) -> Result<Json<SubscriptionMemberFetchResponse>, ApiError> {
    let limit = request
        .limit
        .unwrap_or(64)
        .clamp(1, SUBSCRIPTION_MEMBER_FETCH_MAX_LIMIT);
    let (coordinator, progress_assignment, _definition) =
        member_coordinator(&state, &request.subscription).await?;
    let members = read_member_state(&coordinator).await?;
    let grant = crate::reader::SubscriptionWorkLease {
        work_id: request.work_id,
        member_id: request.member_id,
        member_epoch: request.member_epoch,
        lease_epoch: request.lease_epoch,
        expires_at_tick: 0,
    };
    let tracker = crate::reader::SubscriptionLeaseTracker::from_state(
        members.clone(),
        crate::reader::SUBSCRIPTION_LEASE_MAX_WORK,
    );
    if !tracker.can_ack(&grant, members.last_tick) {
        return Err(ApiError {
            status: StatusCode::CONFLICT,
            message: "Subscription work lease is stale, expired, or fenced".to_owned(),

            code: None,
        });
    }
    let frontier = coordinator
        .read_committed()
        .await
        .map_err(subscription_progress_api_error)?
        .ok_or_else(|| {
            ApiError::unavailable("Subscription progress frontier is not established")
        })?;
    let range_assignments = state
        .control
        .active_range_assignments_for_feed(frontier.feed_id)
        .await;
    let mut streams: Vec<(RangeId, RangeFrameStream)> = Vec::with_capacity(range_assignments.len());
    for range_assignment in &range_assignments {
        let Some(position) = frontier.positions.get(&range_assignment.range_id) else {
            return Err(ApiError {
                status: StatusCode::CONFLICT,
                message:
                    "Subscription frontier does not cover a current Active Range; reconcile progress"
                        .to_owned(),

                code: None,
});
        };
        let mut stream = RangeFrameStream::new(&state, range_assignment);
        if !position.is_empty() {
            stream.after_cursor = Some(position.clone());
        }
        streams.push((range_assignment.range_id, stream));
    }
    if streams.len() != frontier.positions.len() {
        return Err(ApiError {
            status: StatusCode::CONFLICT,
            message: "Subscription frontier predates an Active Range change; reconcile progress"
                .to_owned(),

            code: None,
        });
    }

    let mut heads: Vec<Option<((i64, Uuid), StoredRangeFrame)>> =
        (0..streams.len()).map(|_| None).collect();
    let mut records: Vec<RecordResponse> = Vec::new();
    let mut positions = frontier.positions.clone();
    let mut cursor = None;
    let mut bytes = 0_usize;
    loop {
        for index in 0..streams.len() {
            if heads[index].is_none() {
                heads[index] = match streams[index].1.next_frame().await? {
                    Some(frame) => {
                        let record = decode_record(&frame.frame)
                            .map_err(|error| ApiError::unavailable(error.to_string()))?;
                        Some(((record.ingest_time_ns, record.message_id), frame))
                    }
                    None => None,
                };
            }
        }
        let Some(index) = heads
            .iter()
            .enumerate()
            .filter_map(|(index, head)| head.as_ref().map(|(key, _)| (index, *key)))
            .min_by_key(|(index, key)| (*key, *index))
            .map(|(index, _)| index)
        else {
            break;
        };
        let Some((_, frame)) = heads[index].take() else {
            continue;
        };
        if records.len() >= limit {
            break;
        }
        let frame_bytes = frame.frame.len() + frame.cursor.len();
        if bytes.saturating_add(frame_bytes) > MAX_LOGICAL_READ_BYTES {
            if records.is_empty() {
                return Err(ApiError::unavailable(
                    "record exceeds the bounded Subscription fetch byte budget",
                ));
            }
            break;
        }
        bytes = bytes.saturating_add(frame_bytes);
        let record = decode_record(&frame.frame)
            .map_err(|error| ApiError::unavailable(error.to_string()))?;
        positions.insert(streams[index].0, frame.cursor.clone());
        cursor = Some(frame.cursor.clone());
        records.push(record_response(crate::domain::CursorRecord {
            cursor: frame.cursor,
            record,
        }));
    }
    Ok(Json(SubscriptionMemberFetchResponse {
        request_id: request.request_id,
        records,
        cursor,
        positions,
        ownership_epoch: progress_assignment.ownership_epoch,
    }))
}

async fn subscription_member_state(
    State(state): State<AppState>,
    axum::extract::Query(query): axum::extract::Query<SubscriptionMemberStateQuery>,
) -> Result<Json<SubscriptionMemberResponse>, ApiError> {
    let (coordinator, assignment, _definition) =
        member_coordinator(&state, &query.subscription).await?;
    let member_state = read_member_state(&coordinator).await?;
    Ok(Json(member_response(
        query.subscription,
        assignment.ownership_epoch,
        member_state,
    )))
}

fn subscription_progress_api_error(error: SubscriptionProgressError) -> ApiError {
    let code = error.code();
    let status = match error {
        SubscriptionProgressError::Unavailable
        | SubscriptionProgressError::AmbiguousCommit
        | SubscriptionProgressError::Engine(_)
        | SubscriptionProgressError::Serialization(_) => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::CONFLICT,
    };
    ApiError {
        status,
        message: error.to_string(),
        code: Some(code),
    }
}

fn effect_journal_api_error(error: crate::effect::EffectJournalError) -> ApiError {
    let code = error.code();
    let status = match error {
        crate::effect::EffectJournalError::Unavailable
        | crate::effect::EffectJournalError::AmbiguousCommit
        | crate::effect::EffectJournalError::NoQuorum
        | crate::effect::EffectJournalError::Engine(_)
        | crate::effect::EffectJournalError::Serialization(_) => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::CONFLICT,
    };
    ApiError {
        status,
        message: error.to_string(),
        code: Some(code),
    }
}

async fn effect_journal_prepare_local(
    State(state): State<AppState>,
    peer: Option<axum::Extension<AuthenticatedSubscriptionPeer>>,
    Json(request): Json<crate::effect::EffectPrepareRequest>,
) -> Result<Json<crate::effect::EffectReplicaReply<crate::effect::EffectPrepareVote>>, ApiError> {
    check_subscription_peer(&state, peer.as_ref().map(|peer| &peer.0))?;
    let service = state
        .effect_journal
        .as_ref()
        .ok_or_else(|| ApiError::unavailable("effect journal replica is not configured"))?;
    Ok(Json(
        service
            .prepare(request)
            .await
            .map_err(effect_journal_api_error)?,
    ))
}

async fn effect_journal_commit_local(
    State(state): State<AppState>,
    peer: Option<axum::Extension<AuthenticatedSubscriptionPeer>>,
    Json(request): Json<crate::effect::EffectCommitRequest>,
) -> Result<Json<crate::effect::EffectReplicaReply<crate::effect::EffectMutation>>, ApiError> {
    check_subscription_peer(&state, peer.as_ref().map(|peer| &peer.0))?;
    let service = state
        .effect_journal
        .as_ref()
        .ok_or_else(|| ApiError::unavailable("effect journal replica is not configured"))?;
    Ok(Json(
        service
            .commit(request)
            .await
            .map_err(effect_journal_api_error)?,
    ))
}

async fn effect_journal_committed_local(
    State(state): State<AppState>,
    peer: Option<axum::Extension<AuthenticatedSubscriptionPeer>>,
    Json(request): Json<crate::effect::EffectReadRequest>,
) -> Result<Json<crate::effect::EffectReplicaReply<Option<crate::effect::EffectMutation>>>, ApiError>
{
    check_subscription_peer(&state, peer.as_ref().map(|peer| &peer.0))?;
    let service = state
        .effect_journal
        .as_ref()
        .ok_or_else(|| ApiError::unavailable("effect journal replica is not configured"))?;
    Ok(Json(
        service
            .committed(request)
            .await
            .map_err(effect_journal_api_error)?,
    ))
}

async fn effect_journal_inspect_local(
    State(state): State<AppState>,
    peer: Option<axum::Extension<AuthenticatedSubscriptionPeer>>,
    Json(request): Json<crate::effect::EffectReadRequest>,
) -> Result<Json<crate::effect::EffectReplicaReply<crate::effect::EffectJournalInspection>>, ApiError>
{
    check_subscription_peer(&state, peer.as_ref().map(|peer| &peer.0))?;
    let service = state
        .effect_journal
        .as_ref()
        .ok_or_else(|| ApiError::unavailable("effect journal replica is not configured"))?;
    Ok(Json(
        service
            .inspect(request)
            .await
            .map_err(effect_journal_api_error)?,
    ))
}

async fn effect_journal_adopt_local(
    State(state): State<AppState>,
    peer: Option<axum::Extension<AuthenticatedSubscriptionPeer>>,
    Json(request): Json<crate::effect::EffectAdoptRequest>,
) -> Result<Json<crate::effect::EffectReplicaReply<Option<crate::effect::EffectMutation>>>, ApiError>
{
    check_subscription_peer(&state, peer.as_ref().map(|peer| &peer.0))?;
    let service = state
        .effect_journal
        .as_ref()
        .ok_or_else(|| ApiError::unavailable("effect journal replica is not configured"))?;
    Ok(Json(
        service
            .adopt(request)
            .await
            .map_err(effect_journal_api_error)?,
    ))
}

/// Drives a committed `Declare` on one Subscription-scoped effect journal
/// to its `Applied` marker. Runs on the progress owner so side effects and
/// the terminal journal transition share the frontier authority's epoch
/// fencing; output appends dedupe on deterministic writer identity and the
/// frontier move replays by request identity, so repeating this call after
/// an ambiguous result is safe.
async fn effect_journal_apply_local(
    State(state): State<AppState>,
    peer: Option<axum::Extension<AuthenticatedSubscriptionPeer>>,
    Json(request): Json<crate::effect::EffectReadRequest>,
) -> Result<Json<crate::effect::EffectMutation>, ApiError> {
    check_subscription_peer(&state, peer.as_ref().map(|peer| &peer.0))?;
    let assignment = state
        .control
        .active_subscription_progress_assignment_by_id(request.subscription_id)
        .await
        .ok_or_else(|| ApiError::unavailable("Subscription progress placement is unavailable"))?;
    if assignment.owner != request.receiver || assignment.owner != request.owner {
        return Err(ApiError::conflict(
            "effect apply must run on the current progress owner",
        ));
    }
    if assignment.ownership_epoch != request.ownership_epoch {
        return Err(ApiError::conflict(
            "effect apply epoch does not match the current progress placement",
        ));
    }
    if state.storage_node_id.as_ref() != Some(&request.receiver) {
        return Err(ApiError::conflict(format!(
            "Node {} is not the apply receiver {}",
            state
                .storage_node_id
                .as_ref()
                .map(|node| node.as_str())
                .unwrap_or("unconfigured"),
            request.receiver
        )));
    }
    let effect_transport: Arc<dyn crate::effect::EffectJournalTransport> = match &state
        .internal_mtls
    {
        Some(material) => Arc::new(
            crate::effect::HttpEffectJournalTransport::new_mtls(
                assignment.clone(),
                state.control_endpoints.as_ref().clone(),
                &material.ca_pem,
                &material.identity_pem,
                Duration::from_secs(5),
            )
            .map_err(effect_journal_api_error)?,
        ),
        None => Arc::new(
            crate::effect::HttpEffectJournalTransport::new(
                assignment.clone(),
                state.control_endpoints.as_ref().clone(),
                state
                    .internal_key
                    .as_ref()
                    .ok_or_else(|| ApiError::unavailable("internal credential is not configured"))?
                    .clone(),
                Duration::from_secs(5),
            )
            .map_err(|error| ApiError::unavailable(error.to_string()))?,
        ),
    };
    let journal = crate::effect::EffectCoordinator::new(assignment.clone(), effect_transport);
    let (progress, _) = member_assignment_coordinator(&state, assignment)?;
    let sink = LiveEffectApply {
        state: &state,
        progress,
        subscription_id: request.subscription_id,
        ownership_epoch: request.ownership_epoch,
    };
    journal
        .apply_committed(request.effect_id, &sink)
        .await
        .map(Json)
        .map_err(effect_journal_api_error)
}

/// Wires committed effect side effects to the real Feed append path and the
/// Subscription progress journal.
struct LiveEffectApply<'a> {
    state: &'a AppState,
    progress: crate::reader::SubscriptionProgressCoordinator,
    subscription_id: Uuid,
    ownership_epoch: u64,
}

fn progress_apply_error(
    error: crate::reader::SubscriptionProgressError,
) -> crate::effect::EffectJournalError {
    match error {
        crate::reader::SubscriptionProgressError::Unavailable
        | crate::reader::SubscriptionProgressError::AmbiguousCommit
        | crate::reader::SubscriptionProgressError::NoQuorum
        | crate::reader::SubscriptionProgressError::Engine(_)
        | crate::reader::SubscriptionProgressError::Serialization(_) => {
            crate::effect::EffectJournalError::Unavailable
        }
        _ => crate::effect::EffectJournalError::Conflict,
    }
}

#[async_trait::async_trait]
impl crate::effect::EffectApply for LiveEffectApply<'_> {
    async fn append_output(
        &self,
        output: &crate::effect::EffectOutput,
    ) -> Result<(), crate::effect::EffectJournalError> {
        let feed = self
            .state
            .control
            .active_feed_by_id(output.feed_id)
            .await
            .ok_or(crate::effect::EffectJournalError::Conflict)?;
        let request = ClientAppendRequest {
            request_id: crate::effect::effect_apply_request_id(output.writer_session_id, b"append"),
            feed: feed.name,
            writer_session_id: output.writer_session_id,
            writer_epoch: output.writer_epoch,
            sequence: output.sequence,
            event_time_ns: Some(output.event_time_ns.to_string()),
            key_base64: output.key_base64.clone(),
            payload_base64: output.payload_base64.clone(),
            metadata_base64: BTreeMap::new(),
        };
        route_append_to_owner(self.state, feed.feed_id, request)
            .await
            .map(|_| ())
            .map_err(|error| {
                if error.status.is_server_error() {
                    crate::effect::EffectJournalError::Unavailable
                } else {
                    crate::effect::EffectJournalError::Conflict
                }
            })
    }

    async fn commit_frontier(
        &self,
        consume: &crate::effect::EffectConsume,
        request_id: Uuid,
    ) -> Result<(), crate::effect::EffectJournalError> {
        let committed = self
            .progress
            .read_committed()
            .await
            .map_err(progress_apply_error)?;
        if let Some(current) = &committed {
            if current.cursor == consume.cursor {
                return Ok(());
            }
        }
        let mutation = crate::reader::SubscriptionProgressMutation {
            subscription_id: self.subscription_id,
            feed_id: consume.feed_id,
            ownership_epoch: self.ownership_epoch,
            sequence: committed.as_ref().map_or(1, |prior| prior.sequence + 1),
            request_id,
            expected_cursor: consume.expected_cursor.clone(),
            cursor: consume.cursor.clone(),
            positions: consume.positions.clone(),
            tick: 0,
            lease_ops: Vec::new(),
        };
        match self.progress.apply(mutation.clone()).await {
            Ok(_) => Ok(()),
            Err(crate::reader::SubscriptionProgressError::AmbiguousCommit)
            | Err(crate::reader::SubscriptionProgressError::NoQuorum) => self
                .progress
                .reconcile_retry(mutation)
                .await
                .map(|_| ())
                .map_err(progress_apply_error),
            Err(error) => Err(progress_apply_error(error)),
        }
    }
}

async fn subscription_prepare_local(
    State(state): State<AppState>,
    peer: Option<axum::Extension<AuthenticatedSubscriptionPeer>>,
    Json(request): Json<SubscriptionPrepareRequest>,
) -> Result<
    Json<crate::reader::SubscriptionReplicaReply<crate::reader::SubscriptionPrepareVote>>,
    ApiError,
> {
    check_subscription_peer(&state, peer.as_ref().map(|peer| &peer.0))?;
    let service = state
        .subscription_progress
        .as_ref()
        .ok_or_else(|| ApiError::unavailable("Subscription progress replica is not configured"))?;
    Ok(Json(
        service
            .prepare(request)
            .await
            .map_err(subscription_progress_api_error)?,
    ))
}

async fn subscription_commit_local(
    State(state): State<AppState>,
    peer: Option<axum::Extension<AuthenticatedSubscriptionPeer>>,
    Json(request): Json<SubscriptionCommitRequest>,
) -> Result<
    Json<crate::reader::SubscriptionReplicaReply<crate::reader::SubscriptionProgressMutation>>,
    ApiError,
> {
    check_subscription_peer(&state, peer.as_ref().map(|peer| &peer.0))?;
    let service = state
        .subscription_progress
        .as_ref()
        .ok_or_else(|| ApiError::unavailable("Subscription progress replica is not configured"))?;
    Ok(Json(
        service
            .commit(request)
            .await
            .map_err(subscription_progress_api_error)?,
    ))
}

async fn subscription_committed_local(
    State(state): State<AppState>,
    peer: Option<axum::Extension<AuthenticatedSubscriptionPeer>>,
    Json(request): Json<SubscriptionCommittedReadRequest>,
) -> Result<
    Json<
        crate::reader::SubscriptionReplicaReply<
            Option<crate::reader::SubscriptionProgressMutation>,
        >,
    >,
    ApiError,
> {
    check_subscription_peer(&state, peer.as_ref().map(|peer| &peer.0))?;
    let service = state
        .subscription_progress
        .as_ref()
        .ok_or_else(|| ApiError::unavailable("Subscription progress replica is not configured"))?;
    Ok(Json(
        service
            .committed(request)
            .await
            .map_err(subscription_progress_api_error)?,
    ))
}

async fn subscription_inspect_local(
    State(state): State<AppState>,
    peer: Option<axum::Extension<AuthenticatedSubscriptionPeer>>,
    Json(request): Json<crate::reader::SubscriptionCommittedReadRequest>,
) -> Result<
    Json<crate::reader::SubscriptionReplicaReply<crate::reader::SubscriptionProgressInspection>>,
    ApiError,
> {
    check_subscription_peer(&state, peer.as_ref().map(|peer| &peer.0))?;
    let service = state
        .subscription_progress
        .as_ref()
        .ok_or_else(|| ApiError::unavailable("Subscription progress replica is not configured"))?;
    Ok(Json(
        service
            .inspect(request)
            .await
            .map_err(subscription_progress_api_error)?,
    ))
}

async fn subscription_adopt_local(
    State(state): State<AppState>,
    peer: Option<axum::Extension<AuthenticatedSubscriptionPeer>>,
    Json(request): Json<crate::reader::SubscriptionProgressAdoptRequest>,
) -> Result<
    Json<
        crate::reader::SubscriptionReplicaReply<
            Option<crate::reader::SubscriptionProgressMutation>,
        >,
    >,
    ApiError,
> {
    check_subscription_peer(&state, peer.as_ref().map(|peer| &peer.0))?;
    let service = state
        .subscription_progress
        .as_ref()
        .ok_or_else(|| ApiError::unavailable("Subscription progress replica is not configured"))?;
    Ok(Json(
        service
            .adopt(request)
            .await
            .map_err(subscription_progress_api_error)?,
    ))
}

async fn recovery_progress(
    State(state): State<AppState>,
    Json(request): Json<ReplicaProgressRequest>,
) -> Json<ReplicaProgressResponse> {
    let response = match &state.replica_append {
        Some(service) => match service.recovery_status(request.feed_id).await {
            Ok(status) => ReplicaProgressResponse {
                status: Some(status),
                error: None,
            },
            Err(error) => ReplicaProgressResponse {
                status: None,
                error: Some(error.to_string()),
            },
        },
        None => ReplicaProgressResponse {
            status: None,
            error: Some("replica storage is not configured".to_owned()),
        },
    };
    Json(response)
}

async fn recovery_reconcile(
    State(state): State<AppState>,
    Json(request): Json<ReplicaReconcileRequest>,
) -> Json<ReplicaReconcileResponse> {
    let response = match &state.replica_append {
        Some(service) => match service
            .reconcile_recovery(request.feed_id, request.committed_prefix)
            .await
        {
            Ok(removed_records) => ReplicaReconcileResponse {
                removed_records: Some(removed_records),
                error: None,
            },
            Err(error) => ReplicaReconcileResponse {
                removed_records: None,
                error: Some(error.to_string()),
            },
        },
        None => ReplicaReconcileResponse {
            removed_records: None,
            error: Some("replica storage is not configured".to_owned()),
        },
    };
    Json(response)
}

async fn repair_export(
    State(state): State<AppState>,
    Json(request): Json<RepairExportRequest>,
) -> Json<RepairExportResponse> {
    let response = match &state.replica_append {
        Some(service) => match service
            .export_committed(request.feed_id, request.after, request.limit)
            .await
        {
            Ok(frames) => RepairExportResponse {
                frames: frames
                    .into_iter()
                    .map(|item| RepairFrame {
                        position: item.position,
                        identity: item.identity,
                        cursor: item.cursor,
                        frame_base64: STANDARD.encode(item.frame),
                    })
                    .collect(),
                error: None,
            },
            Err(error) => RepairExportResponse {
                frames: Vec::new(),
                error: Some(error.to_string()),
            },
        },
        None => RepairExportResponse {
            frames: Vec::new(),
            error: Some("replica storage is not configured".to_owned()),
        },
    };
    Json(response)
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReaderOpenRequest {
    pub request_id: Uuid,
    pub reader: String,
    pub capacity: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReaderSessionResponse {
    pub reader_id: Uuid,
    pub feed_id: Uuid,
    pub session_epoch: u64,
    pub capacity: usize,
    pub delivered_cursor: Option<String>,
    pub acknowledged_cursor: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReaderFetchRequest {
    pub request_id: Uuid,
    pub reader: String,
    pub session_epoch: u64,
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReaderFetchResponse {
    pub records: Vec<RecordResponse>,
    pub delivered_cursor: Option<String>,
    pub acknowledged_cursor: Option<String>,
    pub recommended_capacity: usize,
    pub retry_after_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReaderAckRequest {
    pub request_id: Uuid,
    pub reader: String,
    pub session_epoch: u64,
    pub cursor: String,
}

async fn execute_reader_command(
    state: &AppState,
    command: crate::control::Command,
    request_id: Uuid,
) -> Result<crate::control::ControlExecution, ApiError> {
    match &state.control_plane {
        Some(control_plane) => control_plane
            .execute_commands_with_request_id(vec![command], request_id)
            .await
            .map_err(|error| ApiError::bad_request(error.to_string())),
        None => state
            .control
            .execute_commands_with_request_id(vec![command], request_id)
            .await
            .map_err(ApiError::from),
    }
}

async fn reader_open(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<ReaderOpenRequest>,
) -> Result<Json<ReaderSessionResponse>, ApiError> {
    authorize_admin(&state, &headers)?;
    let execution = execute_reader_command(
        &state,
        crate::control::Command::OpenReaderSession {
            reader: request.reader,
            capacity: request.capacity,
        },
        request.request_id,
    )
    .await?;
    let reader: crate::control::ReaderDefinition =
        serde_json::from_value(execution.results[0].data.clone())
            .map_err(|error| ApiError::unavailable(error.to_string()))?;
    Ok(Json(reader_session_response(reader)))
}

fn verify_reader_frontier_result(
    execution: &crate::control::ControlExecution,
    cursor: &str,
    positions: &BTreeMap<RangeId, String>,
    fetch_request_id: Uuid,
) -> Result<(), ApiError> {
    let data = execution
        .results
        .first()
        .map(|result| &result.data)
        .ok_or_else(|| ApiError::unavailable("Reader frontier commit returned no result"))?;
    let applied =
        serde_json::from_value::<BTreeMap<RangeId, String>>(data["positions"].clone()).ok();
    if applied.as_ref() != Some(positions)
        || data["fetch_request_id"] != json!(fetch_request_id)
        || data["reader"]["delivered_cursor"] != json!(cursor)
    {
        return Err(ApiError::unavailable("Reader fetch identity resolved to a different delivery; reopen the session from acknowledged progress"));
    }
    Ok(())
}

async fn reader_fetch(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<ReaderFetchRequest>,
) -> Result<Json<ReaderFetchResponse>, ApiError> {
    authorize_admin(&state, &headers)?;
    let reader = state
        .control
        .active_reader_by_name(&request.reader)
        .await
        .ok_or_else(|| ApiError::bad_request("Reader does not exist"))?;
    if !reader.session_active || reader.session_epoch != request.session_epoch {
        return Err(ApiError::bad_request("Reader session is stale or inactive"));
    }
    let frontier = state.control.active_reader_frontier(reader.reader_id).await;
    if frontier
        .as_ref()
        .and_then(|value| value.last_fetch_request_id)
        == Some(request.request_id)
    {
        return Err(ApiError::unavailable("Reader fetch request already delivered; reopen the session to replay unacknowledged progress"));
    }
    let feed = state
        .control
        .active_feed_by_id(reader.feed_id)
        .await
        .ok_or_else(|| ApiError::unavailable("Reader Feed is not applied on this Node"))?;
    let limit = request
        .limit
        .unwrap_or(reader.session_capacity)
        .min(reader.session_capacity)
        .max(1);
    let timestamp_start = match (&reader.start, &reader.delivered_cursor) {
        (crate::control::ReaderStart::Timestamp(value), None) => Some(*value),
        _ => None,
    };
    let use_frontier = frontier.is_some()
        || timestamp_start.is_none()
            && reader.delivered_cursor.is_none()
            && matches!(&reader.start, crate::control::ReaderStart::Beginning)
            && read_placement(&state, reader.feed_id, &feed.name)
                .await?
                .len()
                > 1;
    let (mut frames, next_frontier) = if use_frontier {
        let previous = frontier.map(|value| value.delivered).unwrap_or_default();
        let (frames, progress) =
            read_reader_frontier(&state, reader.feed_id, &feed.name, &previous, limit).await?;
        (frames, Some(progress))
    } else {
        (
            read_complete_feed(
                &state,
                reader.feed_id,
                &feed.name,
                reader.delivered_cursor.as_deref(),
                if timestamp_start.is_some() {
                    10_000
                } else {
                    limit
                },
                false,
                timestamp_start.is_some(),
            )
            .await?,
            None,
        )
    };
    if let Some(timestamp) = timestamp_start {
        frames.retain(|item| {
            decode_record(&item.frame)
                .map(|record| record.event_time_ns >= timestamp)
                .unwrap_or(false)
        });
        frames.truncate(limit);
    }
    let records = frames
        .into_iter()
        .map(|item| {
            decode_record(&item.frame).map(|record| {
                record_response(CursorRecord {
                    cursor: item.cursor,
                    record,
                })
            })
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| ApiError::unavailable(error.to_string()))?;
    let delivered_cursor = records
        .last()
        .map(|record| {
            if next_frontier.is_some() {
                reader_frontier_cursor(reader.reader_id, request.session_epoch, request.request_id)
            } else {
                record.cursor.clone()
            }
        })
        .or(reader.delivered_cursor.clone());
    if !records.is_empty() {
        let cursor = delivered_cursor
            .clone()
            .ok_or_else(|| ApiError::unavailable("Reader progress token is unavailable"))?;
        let expected_frontier = next_frontier.clone();
        let persisted_token = cursor.clone();
        let command = if let Some(positions) = next_frontier {
            crate::control::Command::RecordReaderFrontier {
                reader: request.reader.clone(),
                session_epoch: request.session_epoch,
                cursor,
                positions,
                expected_cursor: reader.delivered_cursor.clone(),
                fence_delivery: true,
                fetch_request_id: Some(request.request_id),
            }
        } else {
            crate::control::Command::RecordReaderDelivery {
                reader: request.reader.clone(),
                session_epoch: request.session_epoch,
                cursor,
            }
        };
        let execution = execute_reader_command(&state, command, request.request_id)
            .await
            .map_err(|error| {
                if expected_frontier.is_some() && error.status == StatusCode::BAD_REQUEST {
                    ApiError::unavailable(format!(
                        "Reader progress was not committed; reopen the session to retry safely: {}",
                        error.message
                    ))
                } else {
                    error
                }
            })?;
        if let Some(positions) = expected_frontier {
            verify_reader_frontier_result(
                &execution,
                &persisted_token,
                &positions,
                request.request_id,
            )?;
        }
    }
    state.demand.record_read(records.len());
    let pressure = (state.demand.snapshot().requests_in_flight as f64 / 100.0).clamp(0.0, 1.0);
    Ok(Json(ReaderFetchResponse {
        records,
        delivered_cursor,
        acknowledged_cursor: reader.acknowledged_cursor,
        recommended_capacity: if pressure > 0.75 {
            reader.session_capacity.min(16)
        } else {
            reader.session_capacity
        },
        retry_after_ms: if pressure > 0.9 { 50 } else { 0 },
    }))
}

async fn reader_ack(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<ReaderAckRequest>,
) -> Result<Json<ReaderSessionResponse>, ApiError> {
    authorize_admin(&state, &headers)?;
    let execution = execute_reader_command(
        &state,
        crate::control::Command::AcknowledgeReader {
            reader: request.reader,
            session_epoch: request.session_epoch,
            cursor: request.cursor,
        },
        request.request_id,
    )
    .await?;
    let reader: crate::control::ReaderDefinition =
        serde_json::from_value(execution.results[0].data.clone())
            .map_err(|error| ApiError::unavailable(error.to_string()))?;
    Ok(Json(reader_session_response(reader)))
}

async fn reader_close(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<ReaderFetchRequest>,
) -> Result<Json<ReaderSessionResponse>, ApiError> {
    authorize_admin(&state, &headers)?;
    let execution = execute_reader_command(
        &state,
        crate::control::Command::CloseReaderSession {
            reader: request.reader,
            session_epoch: request.session_epoch,
        },
        request.request_id,
    )
    .await?;
    let reader: crate::control::ReaderDefinition =
        serde_json::from_value(execution.results[0].data.clone())
            .map_err(|error| ApiError::unavailable(error.to_string()))?;
    Ok(Json(reader_session_response(reader)))
}

fn reader_session_response(reader: crate::control::ReaderDefinition) -> ReaderSessionResponse {
    ReaderSessionResponse {
        reader_id: reader.reader_id,
        feed_id: reader.feed_id,
        session_epoch: reader.session_epoch,
        capacity: reader.session_capacity,
        delivered_cursor: reader.delivered_cursor,
        acknowledged_cursor: reader.acknowledged_cursor,
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TemporaryReaderFetchRequest {
    pub feed: String,
    pub after: Option<String>,
    pub limit: Option<usize>,
    #[serde(default)]
    pub new_only: bool,
    #[serde(default)]
    pub tail: bool,
    pub wait_ms: Option<u64>,
    pub after_event_time_ns: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TemporaryReaderFetchResponse {
    pub records: Vec<RecordResponse>,
    pub next_cursor: Option<String>,
}

async fn temporary_reader_fetch(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<TemporaryReaderFetchRequest>,
) -> Result<Json<TemporaryReaderFetchResponse>, ApiError> {
    authorize_admin(&state, &headers)?;
    if request.new_only && request.after.is_some() {
        return Err(ApiError::bad_request(
            "new_only cannot be combined with after",
        ));
    }
    if request.after_event_time_ns.is_some() && (request.after.is_some() || request.new_only) {
        return Err(ApiError::bad_request(
            "after_event_time_ns cannot be combined with after or new_only",
        ));
    }
    let after_event_time_ns = request
        .after_event_time_ns
        .as_deref()
        .map(str::parse::<i64>)
        .transpose()
        .map_err(|_| {
            ApiError::bad_request("after_event_time_ns must be signed epoch nanoseconds")
        })?;
    let feed = state
        .control
        .active_feed_by_name(&request.feed)
        .await
        .ok_or_else(|| ApiError::bad_request("Feed does not exist"))?;
    let limit = request.limit.unwrap_or(100).clamp(1, 10_000);
    if request.new_only {
        let existing =
            read_complete_feed(&state, feed.feed_id, &feed.name, None, 1, true, false).await?;
        return Ok(Json(TemporaryReaderFetchResponse {
            next_cursor: existing.last().map(|item| item.cursor.clone()),
            records: Vec::new(),
        }));
    }
    let deadline = tokio::time::Instant::now()
        + Duration::from_millis(request.wait_ms.unwrap_or(0).min(30_000));
    loop {
        let mut frames = read_complete_feed(
            &state,
            feed.feed_id,
            &feed.name,
            request.after.as_deref(),
            if after_event_time_ns.is_some() {
                10_000
            } else {
                limit
            },
            request.tail && after_event_time_ns.is_none(),
            after_event_time_ns.is_some(),
        )
        .await?;
        if let Some(timestamp) = after_event_time_ns {
            frames.retain(|item| {
                decode_record(&item.frame)
                    .map(|record| record.event_time_ns >= timestamp)
                    .unwrap_or(false)
            });
            frames.truncate(limit);
        }
        if request.tail && frames.len() > limit {
            frames = frames.split_off(frames.len() - limit);
        }
        if !frames.is_empty() || tokio::time::Instant::now() >= deadline {
            let records = frames
                .into_iter()
                .map(|item| {
                    decode_record(&item.frame).map(|record| {
                        record_response(CursorRecord {
                            cursor: item.cursor,
                            record,
                        })
                    })
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| ApiError::unavailable(error.to_string()))?;
            let next_cursor = records
                .last()
                .map(|record| record.cursor.clone())
                .or(request.after.clone());
            return Ok(Json(TemporaryReaderFetchResponse {
                records,
                next_cursor,
            }));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct WriterSessionAppendRequest {
    pub request_id: Uuid,
    pub writer: String,
    pub session_epoch: u64,
    pub event_time_ns: Option<String>,
    pub key_base64: String,
    pub payload_base64: String,
    #[serde(default)]
    pub metadata_base64: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LocalSplitStageRequest {
    pub plan: RangeSplitPlan,
    pub source_assignment: ActiveRangeAssignment,
    pub source_commit: CommitPosition,
    pub freeze_source: bool,
    pub batch_size: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LocalSplitStageResponse {
    pub result: Option<CandidateSplitStagingResult>,
    pub error: Option<String>,
}

async fn split_stage_local(
    State(state): State<AppState>,
    Json(request): Json<LocalSplitStageRequest>,
) -> Json<LocalSplitStageResponse> {
    let Some(service) = &state.replica_append else {
        return Json(LocalSplitStageResponse {
            result: None,
            error: Some("replica storage is not configured".to_owned()),
        });
    };
    if request.freeze_source {
        service
            .freeze_generation(
                request.source_assignment.range_id,
                request.source_assignment.generation,
            )
            .await;
    }
    let local_commit = match service.recovery_status(request.plan.feed_id).await {
        Ok(status) => status.committed,
        Err(error) => {
            return Json(LocalSplitStageResponse {
                result: None,
                error: Some(error.to_string()),
            })
        }
    };
    if local_commit != request.source_commit {
        return Json(LocalSplitStageResponse {
            result: None,
            error: Some(format!(
                "local source CommitPosition {local_commit} does not match cutover boundary {}",
                request.source_commit
            )),
        });
    }
    match stage_candidate_ranges_local(
        &request.plan,
        service.clone(),
        service.clone(),
        request.batch_size,
        request.source_commit,
    )
    .await
    {
        Ok(result) => Json(LocalSplitStageResponse {
            result: Some(result),
            error: None,
        }),
        Err(error) => Json(LocalSplitStageResponse {
            result: None,
            error: Some(error.to_string()),
        }),
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LocalSplitUnfreezeRequest {
    pub source_assignment: ActiveRangeAssignment,
}

async fn split_unfreeze_local(
    State(state): State<AppState>,
    Json(request): Json<LocalSplitUnfreezeRequest>,
) -> Json<serde_json::Value> {
    if let Some(service) = &state.replica_append {
        service
            .unfreeze_generation(
                request.source_assignment.range_id,
                request.source_assignment.generation,
            )
            .await;
    }
    Json(json!({ "status": "ok" }))
}

async fn split_freeze_local(
    State(state): State<AppState>,
    Json(request): Json<LocalSplitUnfreezeRequest>,
) -> Result<Json<ReplicaProgressResponse>, ApiError> {
    let service = state
        .replica_append
        .as_ref()
        .ok_or_else(|| ApiError::unavailable("replica storage is not configured"))?;
    if service.local_node() == &request.source_assignment.owner {
        state
            .majority_append
            .as_ref()
            .ok_or_else(|| ApiError::unavailable("majority append is not configured"))?
            .freeze_for_split(&request.source_assignment)
            .await
            .map_err(|error| ApiError::unavailable(error.to_string()))?;
    } else {
        service
            .freeze_generation(
                request.source_assignment.range_id,
                request.source_assignment.generation,
            )
            .await;
    }
    let status = service
        .recovery_status(request.source_assignment.feed_id)
        .await
        .map_err(|error| ApiError::unavailable(error.to_string()))?;
    Ok(Json(ReplicaProgressResponse {
        status: Some(status),
        error: None,
    }))
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AdminSplitRangeRequest {
    pub request_id: Uuid,
    pub feed: String,
    pub split_at: KeyToken,
    pub batch_size: Option<usize>,
}

async fn reconcile_split_node(
    state: &AppState,
    endpoint: &str,
    key: &str,
    request: &ReplicaReconcileRequest,
) -> Result<(), ApiError> {
    let mut last_error = "replica reconciliation did not complete".to_owned();
    for _ in 0..20 {
        let attempt = state
            .internal_http
            .post(format!(
                "{}/internal/active-range/recovery/reconcile",
                endpoint.trim_end_matches('/')
            ))
            .header("x-whitewater-control-key", key)
            .json(request)
            .send()
            .await;
        if let Ok(response) = attempt {
            if let Ok(result) = response.json::<ReplicaReconcileResponse>().await {
                if result.error.is_none() {
                    return Ok(());
                }
                last_error = result.error.unwrap_or(last_error);
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Err(ApiError::unavailable(last_error))
}

async fn stage_split_node(
    state: &AppState,
    endpoint: &str,
    key: &str,
    request: &LocalSplitStageRequest,
) -> Result<CandidateSplitStagingResult, ApiError> {
    let response: LocalSplitStageResponse = state
        .internal_http
        .post(format!(
            "{}/internal/active-range/split/stage-local",
            endpoint.trim_end_matches('/')
        ))
        .header("x-whitewater-control-key", key)
        .json(request)
        .send()
        .await
        .map_err(|error| ApiError::unavailable(error.to_string()))?
        .json()
        .await
        .map_err(|error| ApiError::unavailable(error.to_string()))?;
    response.result.ok_or_else(|| {
        ApiError::unavailable(
            response
                .error
                .unwrap_or_else(|| "local split staging failed".to_owned()),
        )
    })
}

async fn unfreeze_split_nodes(
    state: &AppState,
    plan: &RangeSplitPlan,
    source_assignment: &ActiveRangeAssignment,
    key: &str,
) {
    for node in plan.right_assignment.replicas.iter() {
        if let Some(endpoint) = state.control_endpoints.resolve(node).await {
            let _ = state
                .internal_http
                .post(format!(
                    "{}/internal/active-range/split/unfreeze-local",
                    endpoint.trim_end_matches('/')
                ))
                .header("x-whitewater-control-key", key)
                .json(&LocalSplitUnfreezeRequest {
                    source_assignment: source_assignment.clone(),
                })
                .send()
                .await;
        }
    }
}

fn split_freeze_order(assignment: &ActiveRangeAssignment) -> Vec<StorageNodeId> {
    let mut nodes = vec![assignment.owner.clone()];
    nodes.extend(
        assignment
            .replicas
            .iter()
            .filter(|node| *node != &assignment.owner)
            .cloned(),
    );
    nodes
}

async fn read_cutover_lineage(
    state: &AppState,
    assignment: &ActiveRangeAssignment,
    boundary: CommitPosition,
) -> Result<Vec<ReaderLineageEntry>, ApiError> {
    if boundary.value() > MAX_LOGICAL_READ_FRAMES as u64 {
        return Err(ApiError::unavailable(
            "Reader transition exceeds the bounded source history",
        ));
    }
    let mut entries = Vec::new();
    let mut after = None;
    let mut bytes = 0_usize;
    while after.map_or(0, RangePosition::value) < boundary.value() {
        let page = fetch_range_page(
            state,
            ReadRangePageRequest {
                assignment: assignment.clone(),
                after,
                expected_commit: after.map(|_| boundary),
                after_cursor: None,
                tail_count: None,
                single_range: true,
                page_limit: Some(32),
            },
        )
        .await?;
        if page.committed != boundary || page.frames.is_empty() {
            return Err(ApiError::unavailable(
                "Reader transition source boundary is incomplete",
            ));
        }
        for frame in page.frames {
            let expected = after.map_or(Some(1), |position: RangePosition| {
                position.value().checked_add(1)
            });
            if Some(frame.position.value()) != expected
                || frame.position.value() > boundary.value()
                || frame.cursor.is_empty()
                || frame.cursor.len() > 256
            {
                return Err(ApiError::unavailable(
                    "Reader transition source has a position gap",
                ));
            }
            let decoded = STANDARD
                .decode(&frame.frame_base64)
                .map_err(|error| ApiError::unavailable(error.to_string()))?;
            bytes = bytes
                .checked_add(decoded.len() + frame.cursor.len())
                .ok_or_else(|| ApiError::unavailable("Reader transition size overflow"))?;
            if bytes > MAX_LOGICAL_READ_BYTES {
                return Err(ApiError::unavailable(
                    "Reader transition exceeds its read byte budget",
                ));
            }
            let record = decode_record(&decoded)
                .map_err(|error| ApiError::unavailable(error.to_string()))?;
            entries.push(ReaderLineageEntry {
                range_id: assignment.range_id,
                position: frame.position,
                cursor: frame.cursor,
                key_token: KeyToken::from_key(&record.key),
                ingest_time_ns: record.ingest_time_ns,
                message_id: record.message_id,
            });
            after = Some(frame.position);
        }
    }
    Ok(entries)
}

fn prior_reader_positions(
    snapshot: &ReaderCutoverSnapshot,
    assignments: &[ActiveRangeAssignment],
) -> Result<BTreeMap<RangeId, String>, ApiError> {
    let mut positions = assignments
        .iter()
        .map(|assignment| (assignment.range_id, String::new()))
        .collect::<BTreeMap<_, _>>();
    if let Some(frontier) = &snapshot.frontier {
        if !frontier.acknowledged.is_empty()
            && (frontier.acknowledged.len() != positions.len()
                || frontier
                    .acknowledged
                    .keys()
                    .any(|range| !positions.contains_key(range)))
        {
            return Err(ApiError::unavailable(
                "Reader acknowledged frontier references stale ranges",
            ));
        }
        positions.extend(frontier.acknowledged.clone());
        if frontier.acknowledged.is_empty() {
            if let Some(cursor) = &snapshot.reader.acknowledged_cursor {
                if assignments.len() != 1 || cursor.starts_with("rf1_") {
                    return Err(ApiError::unavailable(
                        "Reader acknowledged progress has no source frontier",
                    ));
                }
                positions.insert(assignments[0].range_id, cursor.clone());
            }
        }
    } else if let Some(cursor) = &snapshot.reader.acknowledged_cursor {
        if assignments.len() != 1 {
            return Err(ApiError::unavailable(
                "Reader record Cursor has no unambiguous source range",
            ));
        }
        positions.insert(assignments[0].range_id, cursor.clone());
    }
    Ok(positions)
}

fn reader_frontier_translation(
    snapshot: &ReaderCutoverSnapshot,
    acknowledged: BTreeMap<RangeId, String>,
) -> ReaderFrontierTranslation {
    ReaderFrontierTranslation {
        reader_id: snapshot.reader.reader_id,
        expected_session_epoch: snapshot.reader.session_epoch,
        expected_acknowledged_cursor: snapshot.reader.acknowledged_cursor.clone(),
        expected_delivered_cursor: snapshot.reader.delivered_cursor.clone(),
        expected_acknowledged: snapshot
            .frontier
            .as_ref()
            .map(|value| value.acknowledged.clone())
            .unwrap_or_default(),
        expected_delivered: snapshot
            .frontier
            .as_ref()
            .map(|value| value.delivered.clone())
            .unwrap_or_default(),
        expected_has_frontier: snapshot.frontier.is_some(),
        acknowledged,
    }
}

fn bounded_reader_translations(
    translations: Vec<ReaderFrontierTranslation>,
) -> Result<Vec<ReaderFrontierTranslation>, ApiError> {
    if serde_json::to_vec(&translations)
        .map_err(|error| ApiError::unavailable(error.to_string()))?
        .len()
        > 512 * 1024
    {
        return Err(ApiError::unavailable(
            "Reader frontier cutover exceeds the bounded evidence budget",
        ));
    }
    Ok(translations)
}

fn split_reader_translations(
    snapshots: &[ReaderCutoverSnapshot],
    assignments: &[ActiveRangeAssignment],
    plan: &RangeSplitPlan,
    entries: &[ReaderLineageEntry],
) -> Result<Vec<ReaderFrontierTranslation>, ApiError> {
    if snapshots.len() > 1024 {
        return Err(ApiError::unavailable(
            "Reader transition exceeds its Reader budget",
        ));
    }
    let mut translations = Vec::with_capacity(snapshots.len());
    for snapshot in snapshots {
        let mut positions = prior_reader_positions(snapshot, assignments)?;
        let source_cursor = positions
            .get(&plan.source_range_id)
            .ok_or_else(|| ApiError::unavailable("Reader split source position is unavailable"))?;
        let child = translate_split_reader_frontier(
            entries,
            plan.source_range_id,
            plan.source_range_id,
            plan.right_assignment.range_id,
            plan.split_at,
            source_cursor,
        )
        .map_err(|error| ApiError::unavailable(error.to_string()))?;
        positions.extend(child);
        translations.push(reader_frontier_translation(snapshot, positions));
    }
    bounded_reader_translations(translations)
}

fn merge_reader_translations(
    snapshots: &[ReaderCutoverSnapshot],
    assignments: &[ActiveRangeAssignment],
    plan: &RangeMergePlan,
    left: &[ReaderLineageEntry],
    right: &[ReaderLineageEntry],
) -> Result<Vec<ReaderFrontierTranslation>, ApiError> {
    if snapshots.len() > 1024 {
        return Err(ApiError::unavailable(
            "Reader transition exceeds its Reader budget",
        ));
    }
    let mut translations = Vec::with_capacity(snapshots.len());
    for snapshot in snapshots {
        let mut positions = prior_reader_positions(snapshot, assignments)?;
        let left_cursor = positions
            .get(&plan.left_range_id)
            .ok_or_else(|| ApiError::unavailable("Reader merge left position is unavailable"))?;
        let right_cursor = positions
            .get(&plan.right_range_id)
            .ok_or_else(|| ApiError::unavailable("Reader merge right position is unavailable"))?;
        let cursor = translate_merge_reader_frontier(
            left,
            right,
            plan.left_range_id,
            plan.right_range_id,
            left_cursor,
            right_cursor,
        )
        .map_err(|error| ApiError::unavailable(error.to_string()))?;
        positions.remove(&plan.right_range_id);
        positions.insert(plan.left_range_id, cursor);
        translations.push(reader_frontier_translation(snapshot, positions));
    }
    bounded_reader_translations(translations)
}

async fn admin_split_range(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<AdminSplitRangeRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    authorize_admin(&state, &headers)?;
    let control_plane = state
        .control_plane
        .as_ref()
        .ok_or_else(|| ApiError::unavailable("Control Plane is unavailable"))?;
    let prepared = control_plane
        .execute_commands_with_request_id(
            vec![crate::control::Command::PrepareActiveRangeSplit {
                feed: request.feed.clone(),
                split_at: request.split_at,
            }],
            request.request_id,
        )
        .await
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let plan: RangeSplitPlan = serde_json::from_value(prepared.results[0].data.clone())
        .map_err(|error| ApiError::unavailable(error.to_string()))?;
    let source_assignment = state
        .control
        .active_range_assignment(plan.feed_id)
        .await
        .ok_or_else(|| ApiError::unavailable("source assignment is unavailable"))?;
    let key = state
        .internal_key
        .as_ref()
        .ok_or_else(|| ApiError::unavailable("internal credential is unavailable"))?;
    let mut source_commit = CommitPosition::new(0);
    for node in split_freeze_order(&source_assignment) {
        let endpoint = state
            .control_endpoints
            .resolve(&node)
            .await
            .ok_or_else(|| {
                ApiError::unavailable(format!("source replica {node} endpoint is unavailable"))
            })?;
        let progress: ReplicaProgressResponse = state
            .internal_http
            .post(format!(
                "{}/internal/active-range/split/freeze-local",
                endpoint.trim_end_matches('/')
            ))
            .header("x-whitewater-control-key", key)
            .json(&LocalSplitUnfreezeRequest {
                source_assignment: source_assignment.clone(),
            })
            .send()
            .await
            .map_err(|error| ApiError::unavailable(error.to_string()))?
            .json()
            .await
            .map_err(|error| ApiError::unavailable(error.to_string()))?;
        let committed = progress
            .status
            .ok_or_else(|| ApiError::unavailable("source freeze returned no progress"))?
            .committed;
        source_commit = source_commit.max(committed);
    }
    let mut staged = Vec::new();
    for node in plan.right_assignment.replicas.iter() {
        let attempt = match state.control_endpoints.resolve(node).await {
            Some(endpoint) => {
                match reconcile_split_node(
                    &state,
                    &endpoint,
                    key,
                    &ReplicaReconcileRequest {
                        feed_id: plan.feed_id,
                        committed_prefix: source_commit,
                    },
                )
                .await
                {
                    Ok(()) => {
                        stage_split_node(
                            &state,
                            &endpoint,
                            key,
                            &LocalSplitStageRequest {
                                plan: plan.clone(),
                                source_assignment: source_assignment.clone(),
                                source_commit,
                                freeze_source: true,
                                batch_size: request.batch_size.unwrap_or(256).clamp(1, 10_000),
                            },
                        )
                        .await
                    }
                    Err(error) => Err(error),
                }
            }
            None => Err(ApiError::unavailable(format!(
                "staged replica {node} endpoint is unavailable"
            ))),
        };
        match attempt {
            Ok(result) => staged.push(result),
            Err(error) => {
                unfreeze_split_nodes(&state, &plan, &source_assignment, key).await;
                return Err(error);
            }
        }
    }
    let evidence = staged
        .first()
        .cloned()
        .ok_or_else(|| ApiError::unavailable("no staging evidence"))?;
    if staged.iter().any(|item| {
        item.source_commit != evidence.source_commit
            || item.left.target_commit != evidence.left.target_commit
            || item.left.checksum != evidence.left.checksum
            || item.left.writer_sequences != evidence.left.writer_sequences
            || item.right.target_commit != evidence.right.target_commit
            || item.right.checksum != evidence.right.checksum
            || item.right.writer_sequences != evidence.right.writer_sequences
    }) {
        unfreeze_split_nodes(&state, &plan, &source_assignment, key).await;
        return Err(ApiError::unavailable(
            "RF3 split staging evidence does not match",
        ));
    }
    let snapshots = state
        .control
        .active_reader_cutover_snapshots(plan.feed_id)
        .await;
    let reader_translations = if snapshots.is_empty() {
        Vec::new()
    } else {
        let calculated = async {
            let source = read_cutover_lineage(&state, &source_assignment, source_commit).await?;
            let assignments = state
                .control
                .active_range_assignments_for_feed(plan.feed_id)
                .await;
            split_reader_translations(&snapshots, &assignments, &plan, &source)
        }
        .await;
        match calculated {
            Ok(translations) => translations,
            Err(error) => {
                unfreeze_split_nodes(&state, &plan, &source_assignment, key).await;
                return Err(error);
            }
        }
    };
    if let Err(error) = control_plane
        .execute_commands(vec![
            crate::control::Command::RecordActiveRangeSplitCatchUp {
                feed: request.feed.clone(),
                plan_id: plan.plan_id,
                source_commit,
                source_scanned_through: evidence.source_commit,
                right_commit: evidence.right.target_commit,
                checksum_verified: true,
            },
        ])
        .await
    {
        unfreeze_split_nodes(&state, &plan, &source_assignment, key).await;
        return Err(ApiError::unavailable(error.to_string()));
    }
    if let Err(error) = control_plane
        .execute_commands(vec![crate::control::Command::ActivateActiveRangeSplit {
            feed: request.feed,
            plan_id: plan.plan_id,
            left_writer_sequences: evidence.left.writer_sequences.clone(),
            right_writer_sequences: evidence.right.writer_sequences.clone(),
            reader_translations,
        }])
        .await
    {
        unfreeze_split_nodes(&state, &plan, &source_assignment, key).await;
        return Err(ApiError::unavailable(error.to_string()));
    }
    Ok(Json(json!({
        "status": "activated",
        "source_commit": source_commit,
        "left_commit": evidence.left.target_commit,
        "right_commit": evidence.right.target_commit,
        "range_map": plan.candidate_map
    })))
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LocalMergeStageRequest {
    pub plan: RangeMergePlan,
    pub left_assignment: ActiveRangeAssignment,
    pub right_assignment: ActiveRangeAssignment,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LocalMergeStageResponse {
    pub result: Option<MergeStagingResult>,
    pub error: Option<String>,
}

async fn range_pressure_totals(
    State(state): State<AppState>,
) -> Json<Vec<crate::demand::RangePressureSample>> {
    Json(state.demand.range_pressure_totals())
}

async fn merge_stage_local(
    State(state): State<AppState>,
    Json(request): Json<LocalMergeStageRequest>,
) -> Json<LocalMergeStageResponse> {
    let Some(service) = &state.replica_append else {
        return Json(LocalMergeStageResponse {
            result: None,
            error: Some("replica storage is not configured".to_owned()),
        });
    };
    match stage_merged_range_local(
        &request.plan,
        &request.left_assignment,
        &request.right_assignment,
        service.clone(),
    )
    .await
    {
        Ok(result) => Json(LocalMergeStageResponse {
            result: Some(result),
            error: None,
        }),
        Err(error) => Json(LocalMergeStageResponse {
            result: None,
            error: Some(error.to_string()),
        }),
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AdminMergeRangesRequest {
    pub request_id: Uuid,
    pub feed: String,
    pub left_range_id: crate::active_range::RangeId,
    pub right_range_id: crate::active_range::RangeId,
}

async fn unfreeze_merge_nodes(
    state: &AppState,
    plan: &RangeMergePlan,
    left: &ActiveRangeAssignment,
    right: &ActiveRangeAssignment,
    key: &str,
) {
    for node in plan.merged_assignment.replicas.iter() {
        if let Some(endpoint) = state.control_endpoints.resolve(node).await {
            for assignment in [left, right] {
                let _ = state
                    .internal_http
                    .post(format!(
                        "{}/internal/active-range/split/unfreeze-local",
                        endpoint.trim_end_matches('/')
                    ))
                    .header("x-whitewater-control-key", key)
                    .json(&LocalSplitUnfreezeRequest {
                        source_assignment: assignment.clone(),
                    })
                    .send()
                    .await;
            }
        }
    }
}

async fn admin_merge_ranges(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<AdminMergeRangesRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    authorize_admin(&state, &headers)?;
    let control_plane = state
        .control_plane
        .as_ref()
        .ok_or_else(|| ApiError::unavailable("Control Plane is unavailable"))?;
    let prepared = control_plane
        .execute_commands_with_request_id(
            vec![crate::control::Command::PrepareActiveRangeMerge {
                feed: request.feed.clone(),
                left_range_id: request.left_range_id,
                right_range_id: request.right_range_id,
            }],
            request.request_id,
        )
        .await
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let plan: RangeMergePlan = serde_json::from_value(prepared.results[0].data.clone())
        .map_err(|error| ApiError::unavailable(error.to_string()))?;
    let left = state
        .control
        .active_range_assignment_by_id(plan.left_range_id)
        .await
        .ok_or_else(|| ApiError::unavailable("left assignment unavailable"))?;
    let right = state
        .control
        .active_range_assignment_by_id(plan.right_range_id)
        .await
        .ok_or_else(|| ApiError::unavailable("right assignment unavailable"))?;
    let key = state
        .internal_key
        .as_ref()
        .ok_or_else(|| ApiError::unavailable("internal credential unavailable"))?;
    let mut evidence = Vec::new();
    for node in plan.merged_assignment.replicas.iter() {
        let staged = async {
            let endpoint = state.control_endpoints.resolve(node).await.ok_or_else(|| {
                ApiError::unavailable(format!("merge replica {node} endpoint unavailable"))
            })?;
            let response: LocalMergeStageResponse = state
                .internal_http
                .post(format!(
                    "{}/internal/active-range/merge/stage-local",
                    endpoint.trim_end_matches('/')
                ))
                .header("x-whitewater-control-key", key)
                .json(&LocalMergeStageRequest {
                    plan: plan.clone(),
                    left_assignment: left.clone(),
                    right_assignment: right.clone(),
                })
                .send()
                .await
                .map_err(|error| ApiError::unavailable(error.to_string()))?
                .json()
                .await
                .map_err(|error| ApiError::unavailable(error.to_string()))?;
            response.result.ok_or_else(|| {
                ApiError::unavailable(
                    response
                        .error
                        .unwrap_or_else(|| "merge staging failed".to_owned()),
                )
            })
        }
        .await;
        match staged {
            Ok(result) => evidence.push(result),
            Err(error) => {
                unfreeze_merge_nodes(&state, &plan, &left, &right, key).await;
                return Err(error);
            }
        }
    }
    let staged = evidence
        .first()
        .cloned()
        .ok_or_else(|| ApiError::unavailable("no merge evidence"))?;
    if evidence.iter().any(|item| {
        item.left_commit != staged.left_commit
            || item.right_commit != staged.right_commit
            || item.merged_commit != staged.merged_commit
            || item.checksum != staged.checksum
            || item.writer_sequences != staged.writer_sequences
    }) {
        unfreeze_merge_nodes(&state, &plan, &left, &right, key).await;
        return Err(ApiError::unavailable(
            "RF3 merge staging evidence does not match",
        ));
    }
    let snapshots = state
        .control
        .active_reader_cutover_snapshots(plan.feed_id)
        .await;
    let reader_translations = if snapshots.is_empty() {
        Vec::new()
    } else {
        let calculated = async {
            let left_rows = read_cutover_lineage(&state, &left, staged.left_commit).await?;
            let right_rows = read_cutover_lineage(&state, &right, staged.right_commit).await?;
            let assignments = state
                .control
                .active_range_assignments_for_feed(plan.feed_id)
                .await;
            merge_reader_translations(&snapshots, &assignments, &plan, &left_rows, &right_rows)
        }
        .await;
        match calculated {
            Ok(translations) => translations,
            Err(error) => {
                unfreeze_merge_nodes(&state, &plan, &left, &right, key).await;
                return Err(error);
            }
        }
    };
    control_plane
        .execute_commands(vec![
            crate::control::Command::RecordActiveRangeMergeStaging {
                feed: request.feed.clone(),
                plan_id: plan.plan_id,
                left_commit: staged.left_commit,
                right_commit: staged.right_commit,
                merged_commit: staged.merged_commit,
                checksum_verified: true,
            },
        ])
        .await
        .map_err(|error| ApiError::unavailable(error.to_string()))?;
    control_plane
        .execute_commands(vec![crate::control::Command::ActivateActiveRangeMerge {
            feed: request.feed,
            plan_id: plan.plan_id,
            writer_sequences: staged.writer_sequences.clone(),
            reader_translations,
        }])
        .await
        .map_err(|error| ApiError::unavailable(error.to_string()))?;
    Ok(Json(
        json!({ "status": "activated", "range_map": plan.candidate_map, "merged_commit": staged.merged_commit }),
    ))
}

const MAX_MOVE_HISTORY_RECORDS: u64 = 10_000;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct MovePlanRequest {
    plan_id: Uuid,
    range_id: RangeId,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct MoveExportRequest {
    plan_id: Uuid,
    range_id: RangeId,
    after: Option<RangePosition>,
    expected_commit: Option<CommitPosition>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct MoveExportResponse {
    source_commit: CommitPosition,
    frames: Vec<RepairFrame>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct MoveStageRequest {
    plan_id: Uuid,
    range_id: RangeId,
    expected_commit: Option<CommitPosition>,
}

async fn active_move_plan(
    state: &AppState,
    request: &MovePlanRequest,
) -> Result<RangeMovePlan, ApiError> {
    for _ in 0..30 {
        if let Some(plan) = state.control.follower_move_plan(request.range_id).await {
            if plan.plan_id == request.plan_id {
                return Ok(plan);
            }
            return Err(ApiError::bad_request("follower move plan is stale"));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(ApiError::unavailable(
        "follower move plan is not applied on this Node",
    ))
}

async fn move_export(
    State(state): State<AppState>,
    Json(request): Json<MoveExportRequest>,
) -> Result<Json<MoveExportResponse>, ApiError> {
    let plan = active_move_plan(
        &state,
        &MovePlanRequest {
            plan_id: request.plan_id,
            range_id: request.range_id,
        },
    )
    .await?;
    let service = state
        .replica_append
        .as_ref()
        .ok_or_else(|| ApiError::unavailable("replica storage unavailable"))?;
    if service.local_node() != &plan.source_assignment.owner
        || state
            .control
            .active_range_assignment_by_id(request.range_id)
            .await
            != Some(plan.source_assignment.clone())
    {
        return Err(ApiError::unavailable(
            "source owner or range assignment changed during movement",
        ));
    }
    let committed = service
        .recovery_status_for_assignment(&plan.source_assignment)
        .await
        .map_err(|error| ApiError::unavailable(error.to_string()))?
        .committed;
    if request
        .expected_commit
        .is_some_and(|expected| expected != committed)
    {
        return Err(ApiError::unavailable(
            "source CommitPosition moved after movement freeze",
        ));
    }
    let frames = service
        .export_assignment_committed(&plan.source_assignment, request.after, 1)
        .await
        .map_err(|error| ApiError::unavailable(error.to_string()))?
        .into_iter()
        .filter(|frame| frame.position.value() <= committed.value())
        .map(|frame| RepairFrame {
            position: frame.position,
            identity: frame.identity,
            cursor: frame.cursor,
            frame_base64: STANDARD.encode(frame.frame),
        })
        .collect();
    Ok(Json(MoveExportResponse {
        source_commit: committed,
        frames,
    }))
}

async fn fetch_move_export(
    state: &AppState,
    endpoint: &str,
    key: &str,
    request: MoveExportRequest,
) -> Result<MoveExportResponse, ApiError> {
    state
        .internal_http
        .post(format!(
            "{}/internal/active-range/move/export",
            endpoint.trim_end_matches('/')
        ))
        .header("x-whitewater-control-key", key)
        .json(&request)
        .send()
        .await
        .map_err(|error| ApiError::unavailable(error.to_string()))?
        .error_for_status()
        .map_err(|error| ApiError::unavailable(error.to_string()))?
        .json()
        .await
        .map_err(|error| ApiError::unavailable(error.to_string()))
}

async fn move_stage_local(
    State(state): State<AppState>,
    Json(request): Json<MoveStageRequest>,
) -> Result<Json<FollowerMoveCopyResult>, ApiError> {
    let plan = active_move_plan(
        &state,
        &MovePlanRequest {
            plan_id: request.plan_id,
            range_id: request.range_id,
        },
    )
    .await?;
    let service = state
        .replica_append
        .as_ref()
        .ok_or_else(|| ApiError::unavailable("replica storage unavailable"))?;
    if service.local_node() != &plan.replacement_replica
        || state
            .control
            .active_range_assignment_by_id(request.range_id)
            .await
            != Some(plan.source_assignment.clone())
    {
        return Err(ApiError::unavailable(
            "replacement Node or range assignment changed during movement",
        ));
    }
    let endpoint = state
        .control_endpoints
        .resolve(&plan.source_assignment.owner)
        .await
        .ok_or_else(|| ApiError::unavailable("source owner endpoint unavailable"))?;
    let key = state
        .internal_key
        .as_ref()
        .ok_or_else(|| ApiError::unavailable("internal credential unavailable"))?;
    let prior_commit = service
        .recovery_status_for_assignment(&plan.candidate_assignment)
        .await
        .map_err(|error| ApiError::unavailable(error.to_string()))?
        .committed;
    // Every position at or below prior_commit was staged with its digest
    // verified against this source before commit advanced, so an interrupted
    // or retried stage resumes at the proven boundary instead of restarting.
    let resume = (prior_commit.value() > 0).then(|| RangePosition::new(prior_commit.value()));
    let first = fetch_move_export(
        &state,
        &endpoint,
        key,
        MoveExportRequest {
            plan_id: plan.plan_id,
            range_id: request.range_id,
            after: resume,
            expected_commit: request.expected_commit,
        },
    )
    .await?;
    let captured = first.source_commit;
    if captured.value() > MAX_MOVE_HISTORY_RECORDS {
        return Err(ApiError::unavailable(
            "range exceeds the 10,000-record prototype transfer bound; keep the old RF3 assignment until checkpointed streaming movement is available",
        ));
    }
    if prior_commit > captured {
        return Err(ApiError::unavailable(
            "replacement replica is ahead of source CommitPosition",
        ));
    }
    let mut after = resume;
    let skipped_records = prior_commit.value();
    let mut checksum = blake3::Hasher::new();
    let mut transferred_records = 0_u64;
    let mut transferred_bytes = 0_u64;
    let mut next = first;
    while after.map_or(0, RangePosition::value) < captured.value() {
        if next.frames.is_empty() {
            return Err(ApiError::unavailable(
                "source ended before captured CommitPosition",
            ));
        }
        for frame in &next.frames {
            if frame.position.value() > captured.value() {
                break;
            }
            let expected = after.map_or(1, |position: RangePosition| {
                position.value().saturating_add(1)
            });
            if frame.position.value() != expected {
                return Err(ApiError::unavailable("source export has a position gap"));
            }
            let bytes = STANDARD
                .decode(frame.frame_base64.as_bytes())
                .map_err(|_| ApiError::unavailable("source export contains invalid base64"))?;
            if bytes.len() > MAX_FRAME_BYTES {
                return Err(ApiError::unavailable(
                    "source export frame exceeds size limit",
                ));
            }
            let digest = *blake3::hash(&bytes).as_bytes();
            let accepted = service
                .stage_split_frame(
                    &plan.candidate_assignment,
                    frame.position,
                    frame.identity.clone(),
                    frame.cursor.clone(),
                    bytes.clone(),
                )
                .await
                .map_err(|error| ApiError::unavailable(error.to_string()))?;
            if accepted.position != frame.position
                || accepted.frame_digest != digest
                || accepted.cursor != frame.cursor
            {
                return Err(ApiError::unavailable(
                    "replacement frame verification failed",
                ));
            }
            if frame.position.value() > prior_commit.value() {
                service
                    .commit_staged_split(
                        &plan.candidate_assignment,
                        CommitPosition::new(frame.position.value()),
                    )
                    .await
                    .map_err(|error| ApiError::unavailable(error.to_string()))?;
                transferred_records = transferred_records.saturating_add(1);
                transferred_bytes = transferred_bytes.saturating_add(bytes.len() as u64);
            }
            checksum.update(&frame.position.value().to_be_bytes());
            checksum.update(&digest);
            after = Some(frame.position);
        }
        if after.map_or(0, RangePosition::value) < captured.value() {
            next = fetch_move_export(
                &state,
                &endpoint,
                key,
                MoveExportRequest {
                    plan_id: plan.plan_id,
                    range_id: request.range_id,
                    after,
                    expected_commit: request.expected_commit,
                },
            )
            .await?;
        }
    }
    let latest = fetch_move_export(
        &state,
        &endpoint,
        key,
        MoveExportRequest {
            plan_id: plan.plan_id,
            range_id: request.range_id,
            after,
            expected_commit: request.expected_commit,
        },
    )
    .await?
    .source_commit;
    let target_commit = service
        .recovery_status_for_assignment(&plan.candidate_assignment)
        .await
        .map_err(|error| ApiError::unavailable(error.to_string()))?
        .committed;
    Ok(Json(FollowerMoveCopyResult {
        source_commit: latest,
        target_commit,
        transferred_records,
        transferred_bytes,
        skipped_records,
        checksum: *checksum.finalize().as_bytes(),
        ready: latest == captured && target_commit == captured,
    }))
}

async fn move_freeze_owner(
    State(state): State<AppState>,
    Json(request): Json<MovePlanRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let plan = active_move_plan(&state, &request).await?;
    let coordinator = state
        .majority_append
        .as_ref()
        .ok_or_else(|| ApiError::unavailable("Append Owner coordinator is unavailable"))?;
    let commit = coordinator
        .freeze_for_follower_move(&plan.source_assignment)
        .await
        .map_err(|error| ApiError::unavailable(error.to_string()))?;
    Ok(Json(json!({ "source_commit": commit })))
}

async fn move_unfreeze_owner(
    State(state): State<AppState>,
    Json(request): Json<MovePlanRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let current = state
        .control
        .active_range_assignment_by_id(request.range_id)
        .await
        .ok_or_else(|| ApiError::unavailable("source range assignment unavailable"))?;
    let plan = state.control.follower_move_plan(request.range_id).await;
    let permitted = plan.as_ref().is_some_and(|plan| {
        plan.plan_id == request.plan_id
            && plan.source_assignment == current
            && matches!(
                plan.stage,
                RangeMoveStage::Prepared | RangeMoveStage::CatchingUp
            )
    }) || plan.is_none()
        && state
            .control
            .completed_follower_move(request.range_id)
            .await
            .is_some_and(|completed| {
                completed.plan_id == request.plan_id && completed.candidate_assignment == current
            });
    if !permitted {
        return Err(ApiError::unavailable(
            "movement cutover is unresolved; source stays frozen",
        ));
    }
    let service = state
        .replica_append
        .as_ref()
        .ok_or_else(|| ApiError::unavailable("replica storage unavailable"))?;
    if service.local_node() != &current.owner {
        return Err(ApiError::unavailable(
            "Node is not the current Append Owner",
        ));
    }
    service
        .unfreeze_generation(current.range_id, current.generation)
        .await;
    Ok(Json(json!({ "status": "unfrozen" })))
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AdminMoveFollowerRequest {
    pub request_id: Uuid,
    pub feed: String,
    pub range_id: RangeId,
    pub removed_replica: StorageNodeId,
    pub replacement_replica: StorageNodeId,
}

async fn post_move_request<T: Serialize>(
    state: &AppState,
    endpoint: &str,
    key: &str,
    path: &str,
    request: &T,
) -> Result<serde_json::Value, ApiError> {
    let timeout = if path.ends_with("/unfreeze") {
        Duration::from_millis(750)
    } else if matches!(
        path,
        "/internal/active-range/move/stage" | "/internal/active-range/owner-move/verify"
    ) {
        Duration::from_secs(120)
    } else {
        Duration::from_secs(5)
    };
    let response = state
        .internal_http
        .post(format!("{}{}", endpoint.trim_end_matches('/'), path))
        .header("x-whitewater-control-key", key)
        .json(request)
        .timeout(timeout)
        .send()
        .await
        .map_err(|error| ApiError::unavailable(error.to_string()))?;
    if !response.status().is_success() {
        let status = response.status();
        let detail = response.text().await.unwrap_or_default();
        return Err(ApiError::unavailable(format!(
            "movement request {path} returned {status}: {}",
            detail.chars().take(512).collect::<String>()
        )));
    }
    response
        .json()
        .await
        .map_err(|error| ApiError::unavailable(error.to_string()))
}

async fn unfreeze_move(
    state: &AppState,
    endpoint: &str,
    key: &str,
    request: &MovePlanRequest,
) -> Result<(), ApiError> {
    for _ in 0..50 {
        if post_move_request(
            state,
            endpoint,
            key,
            "/internal/active-range/move/unfreeze",
            request,
        )
        .await
        .is_ok()
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(ApiError::unavailable(
        "movement cutover is unresolved; Append Owner remains frozen",
    ))
}

async fn admin_move_follower(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<AdminMoveFollowerRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    authorize_admin(&state, &headers)?;
    let control_plane = state
        .control_plane
        .as_ref()
        .ok_or_else(|| ApiError::unavailable("Control Plane is unavailable"))?;
    let prepared = control_plane
        .execute_commands_with_request_id(
            vec![crate::control::Command::PrepareFollowerMove {
                feed: request.feed.clone(),
                range_id: request.range_id,
                removed_replica: request.removed_replica,
                replacement_replica: request.replacement_replica,
            }],
            request.request_id,
        )
        .await
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let plan: RangeMovePlan = serde_json::from_value(prepared.results[0].data.clone())
        .map_err(|error| ApiError::unavailable(error.to_string()))?;
    let source_endpoint = state
        .control_endpoints
        .resolve(&plan.source_assignment.owner)
        .await
        .ok_or_else(|| ApiError::unavailable("source endpoint unavailable"))?;
    let target_endpoint = state
        .control_endpoints
        .resolve(&plan.replacement_replica)
        .await
        .ok_or_else(|| ApiError::unavailable("replacement endpoint unavailable"))?;
    let key = state
        .internal_key
        .as_ref()
        .ok_or_else(|| ApiError::unavailable("internal credential unavailable"))?;
    let plan_request = MovePlanRequest {
        plan_id: plan.plan_id,
        range_id: plan.source_assignment.range_id,
    };
    if state
        .control
        .active_range_assignment_by_id(plan_request.range_id)
        .await
        == Some(plan.candidate_assignment.clone())
    {
        unfreeze_move(&state, &source_endpoint, key, &plan_request).await?;
        return Ok(Json(
            json!({ "status": "activated", "assignment": plan.candidate_assignment }),
        ));
    }
    post_move_request(
        &state,
        &target_endpoint,
        key,
        "/internal/active-range/move/stage",
        &MoveStageRequest {
            plan_id: plan.plan_id,
            range_id: plan_request.range_id,
            expected_commit: None,
        },
    )
    .await?;
    let frozen = post_move_request(
        &state,
        &source_endpoint,
        key,
        "/internal/active-range/move/freeze",
        &plan_request,
    )
    .await?;
    let boundary: CommitPosition = match serde_json::from_value(frozen["source_commit"].clone()) {
        Ok(commit) => commit,
        Err(error) => {
            unfreeze_move(&state, &source_endpoint, key, &plan_request).await?;
            return Err(ApiError::unavailable(error.to_string()));
        }
    };
    let final_copy = post_move_request(
        &state,
        &target_endpoint,
        key,
        "/internal/active-range/move/stage",
        &MoveStageRequest {
            plan_id: plan.plan_id,
            range_id: plan_request.range_id,
            expected_commit: Some(boundary),
        },
    )
    .await;
    let copied: FollowerMoveCopyResult = match final_copy {
        Ok(data) => match serde_json::from_value(data) {
            Ok(copied) => copied,
            Err(error) => {
                unfreeze_move(&state, &source_endpoint, key, &plan_request).await?;
                return Err(ApiError::unavailable(error.to_string()));
            }
        },
        Err(error) => {
            unfreeze_move(&state, &source_endpoint, key, &plan_request).await?;
            return Err(error);
        }
    };
    if !copied.ready || copied.source_commit != boundary || copied.target_commit != boundary {
        unfreeze_move(&state, &source_endpoint, key, &plan_request).await?;
        return Err(ApiError::unavailable(
            "replacement did not commit the frozen source prefix",
        ));
    }
    let cutover = ControlPlaneFollowerMove::new(control_plane.clone());
    cutover
        .record_ready(&request.feed, &plan, &copied)
        .await
        .map_err(ApiError::unavailable)?;
    cutover
        .activate(&request.feed, &plan)
        .await
        .map_err(ApiError::unavailable)?;
    unfreeze_move(&state, &source_endpoint, key, &plan_request).await?;
    Ok(Json(
        json!({ "status": "activated", "assignment": plan.candidate_assignment,
        "committed_position": copied.target_commit, "transferred_records": copied.transferred_records,
        "transferred_bytes": copied.transferred_bytes }),
    ))
}

async fn active_owner_move_plan(
    state: &AppState,
    request: &MovePlanRequest,
) -> Result<RangeOwnerMovePlan, ApiError> {
    for _ in 0..30 {
        if let Some(plan) = state.control.owner_move_plan(request.range_id).await {
            if plan.plan_id == request.plan_id {
                return Ok(plan);
            }
            return Err(ApiError::bad_request("owner move plan is stale"));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(ApiError::unavailable(
        "owner move plan is not applied on this Node",
    ))
}

async fn owner_move_export(
    State(state): State<AppState>,
    Json(request): Json<MoveExportRequest>,
) -> Result<Json<MoveExportResponse>, ApiError> {
    let plan = active_owner_move_plan(
        &state,
        &MovePlanRequest {
            plan_id: request.plan_id,
            range_id: request.range_id,
        },
    )
    .await?;
    let service = state
        .replica_append
        .as_ref()
        .ok_or_else(|| ApiError::unavailable("replica storage unavailable"))?;
    if service.local_node() != &plan.source_assignment.owner
        || state
            .control
            .active_range_assignment_by_id(request.range_id)
            .await
            != Some(plan.source_assignment.clone())
        || !service
            .generation_is_frozen(request.range_id, plan.source_assignment.generation)
            .await
    {
        return Err(ApiError::unavailable(
            "owner move source is not frozen at the current assignment",
        ));
    }
    let committed = service
        .recovery_status_for_assignment(&plan.source_assignment)
        .await
        .map_err(|error| ApiError::unavailable(error.to_string()))?
        .committed;
    if request.expected_commit != Some(committed) {
        return Err(ApiError::unavailable(
            "source CommitPosition differs from frozen boundary",
        ));
    }
    let frames = service
        .export_assignment_committed(&plan.source_assignment, request.after, 1)
        .await
        .map_err(|error| ApiError::unavailable(error.to_string()))?
        .into_iter()
        .filter(|frame| frame.position.value() <= committed.value())
        .map(|frame| RepairFrame {
            position: frame.position,
            identity: frame.identity,
            cursor: frame.cursor,
            frame_base64: STANDARD.encode(frame.frame),
        })
        .collect();
    Ok(Json(MoveExportResponse {
        source_commit: committed,
        frames,
    }))
}

async fn fetch_owner_move_export(
    state: &AppState,
    endpoint: &str,
    key: &str,
    request: MoveExportRequest,
) -> Result<MoveExportResponse, ApiError> {
    state
        .internal_http
        .post(format!(
            "{}/internal/active-range/owner-move/export",
            endpoint.trim_end_matches('/')
        ))
        .header("x-whitewater-control-key", key)
        .json(&request)
        .send()
        .await
        .map_err(|error| ApiError::unavailable(error.to_string()))?
        .error_for_status()
        .map_err(|error| ApiError::unavailable(error.to_string()))?
        .json()
        .await
        .map_err(|error| ApiError::unavailable(error.to_string()))
}

async fn owner_move_freeze(
    State(state): State<AppState>,
    Json(request): Json<MovePlanRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let plan = active_owner_move_plan(&state, &request).await?;
    let coordinator = state
        .majority_append
        .as_ref()
        .ok_or_else(|| ApiError::unavailable("Append Owner coordinator is unavailable"))?;
    let commit = coordinator
        .freeze_for_follower_move(&plan.source_assignment)
        .await
        .map_err(|error| ApiError::unavailable(error.to_string()))?;
    Ok(Json(json!({ "source_commit": commit })))
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct OwnerMoveVerifyRequest {
    plan_id: Uuid,
    range_id: RangeId,
    expected_commit: CommitPosition,
}

async fn owner_move_verify(
    State(state): State<AppState>,
    Json(request): Json<OwnerMoveVerifyRequest>,
) -> Result<Json<OwnerMoveEvidence>, ApiError> {
    if request.expected_commit.value() > MAX_MOVE_HISTORY_RECORDS {
        return Err(ApiError::unavailable(
            "range exceeds the 10,000-record prototype verification bound",
        ));
    }
    let plan = active_owner_move_plan(
        &state,
        &MovePlanRequest {
            plan_id: request.plan_id,
            range_id: request.range_id,
        },
    )
    .await?;
    let service = state
        .replica_append
        .as_ref()
        .ok_or_else(|| ApiError::unavailable("replica storage unavailable"))?;
    if service.local_node() != &plan.candidate_assignment.owner
        || state
            .control
            .active_range_assignment_by_id(request.range_id)
            .await
            != Some(plan.source_assignment.clone())
    {
        return Err(ApiError::unavailable(
            "candidate owner or range placement has changed",
        ));
    }
    service
        .freeze_generation(request.range_id, plan.source_assignment.generation)
        .await;
    let target_commit = service
        .recovery_status_for_assignment(&plan.source_assignment)
        .await
        .map_err(|error| ApiError::unavailable(error.to_string()))?
        .committed;
    if target_commit != request.expected_commit {
        return Err(ApiError::unavailable(
            "candidate owner is not committed through frozen source boundary",
        ));
    }
    service
        .truncate_uncommitted_for_assignment(&plan.source_assignment)
        .await
        .map_err(|error| ApiError::unavailable(error.to_string()))?;
    let endpoint = state
        .control_endpoints
        .resolve(&plan.source_assignment.owner)
        .await
        .ok_or_else(|| ApiError::unavailable("current owner endpoint unavailable"))?;
    let key = state
        .internal_key
        .as_ref()
        .ok_or_else(|| ApiError::unavailable("internal credential unavailable"))?;
    let mut after = None;
    let mut checksum = blake3::Hasher::new();
    while after.map_or(0, RangePosition::value) < request.expected_commit.value() {
        let exported = fetch_owner_move_export(
            &state,
            &endpoint,
            key,
            MoveExportRequest {
                plan_id: plan.plan_id,
                range_id: request.range_id,
                after,
                expected_commit: Some(request.expected_commit),
            },
        )
        .await?;
        let frame = exported
            .frames
            .first()
            .ok_or_else(|| ApiError::unavailable("source ended before frozen boundary"))?;
        let expected = after.map_or(1, |position: RangePosition| {
            position.value().saturating_add(1)
        });
        if frame.position.value() != expected
            || frame.position.value() > request.expected_commit.value()
        {
            return Err(ApiError::unavailable(
                "source export has a position gap or crossed frozen boundary",
            ));
        }
        let bytes = STANDARD
            .decode(frame.frame_base64.as_bytes())
            .map_err(|_| ApiError::unavailable("invalid source frame encoding"))?;
        if bytes.len() > MAX_FRAME_BYTES {
            return Err(ApiError::unavailable("source frame exceeds the size limit"));
        }
        let local = service
            .read_staged_committed(&plan.source_assignment, after, 1)
            .await
            .map_err(|error| ApiError::unavailable(error.to_string()))?;
        let target = local
            .first()
            .ok_or_else(|| ApiError::unavailable("candidate owner is missing a committed frame"))?;
        if target.position != frame.position
            || target.identity != frame.identity
            || target.cursor != frame.cursor
            || target.frame != bytes
        {
            return Err(ApiError::unavailable(
                "candidate owner committed bytes differ from source",
            ));
        }
        let digest = blake3::hash(&bytes);
        checksum.update(&frame.position.value().to_be_bytes());
        checksum.update(digest.as_bytes());
        after = Some(frame.position);
    }
    let latest = fetch_owner_move_export(
        &state,
        &endpoint,
        key,
        MoveExportRequest {
            plan_id: plan.plan_id,
            range_id: request.range_id,
            after,
            expected_commit: Some(request.expected_commit),
        },
    )
    .await?
    .source_commit;
    let target_commit = service
        .recovery_status_for_assignment(&plan.source_assignment)
        .await
        .map_err(|error| ApiError::unavailable(error.to_string()))?
        .committed;
    if latest != request.expected_commit || target_commit != request.expected_commit {
        return Err(ApiError::unavailable(
            "owner move CommitPosition changed during verification",
        ));
    }
    Ok(Json(OwnerMoveEvidence {
        source_commit: latest,
        target_commit,
        checksum: *checksum.finalize().as_bytes(),
        ready: true,
    }))
}

async fn owner_move_unfreeze(
    State(state): State<AppState>,
    Json(request): Json<MovePlanRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let current = state
        .control
        .active_range_assignment_by_id(request.range_id)
        .await
        .ok_or_else(|| ApiError::unavailable("source assignment unavailable"))?;
    let plan = state.control.owner_move_plan(request.range_id).await;
    let permitted = plan.as_ref().is_some_and(|plan| {
        plan.plan_id == request.plan_id
            && plan.source_assignment == current
            && matches!(
                plan.stage,
                RangeMoveStage::Prepared | RangeMoveStage::CatchingUp
            )
    }) || plan.is_none()
        && state
            .control
            .completed_owner_move(request.range_id)
            .await
            .is_some_and(|completed| {
                completed.plan_id == request.plan_id && completed.candidate_assignment == current
            });
    if !permitted {
        return Err(ApiError::unavailable(
            "owner movement cutover is unresolved; replica stays frozen",
        ));
    }
    let service = state
        .replica_append
        .as_ref()
        .ok_or_else(|| ApiError::unavailable("replica storage unavailable"))?;
    if !current.replicas.contains(service.local_node()) {
        return Err(ApiError::unavailable("Node is not a current replica"));
    }
    service
        .unfreeze_generation(current.range_id, current.generation)
        .await;
    Ok(Json(json!({ "status": "unfrozen" })))
}

async fn unfreeze_owner_move(
    state: &AppState,
    endpoint: &str,
    key: &str,
    request: &MovePlanRequest,
) -> Result<(), ApiError> {
    for _ in 0..10 {
        if post_move_request(
            state,
            endpoint,
            key,
            "/internal/active-range/owner-move/unfreeze",
            request,
        )
        .await
        .is_ok()
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(ApiError::unavailable(
        "owner movement is unresolved; replica remains frozen",
    ))
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AdminMoveOwnerRequest {
    pub request_id: Uuid,
    pub feed: String,
    pub range_id: RangeId,
    pub new_owner: StorageNodeId,
}

/// Drives `StorageDrainSupervisor` steps on a live multi-Node cluster: Active
/// Range moves post to the local admin move endpoints (which run the verified
/// copy/freeze/activate orchestration against internal Node endpoints), while
/// Subscription progress moves are single catalog commands routed through the
/// Control Plane.
pub struct AdminDrainDriver {
    endpoint: String,
    admin_key: String,
    control_plane: Arc<ControlPlane>,
    http: reqwest::Client,
}

impl AdminDrainDriver {
    pub fn new(
        endpoint: String,
        admin_key: String,
        control_plane: Arc<ControlPlane>,
        timeout: Duration,
    ) -> Result<Self, reqwest::Error> {
        Ok(Self {
            endpoint: endpoint.trim_end_matches('/').to_owned(),
            admin_key,
            control_plane,
            http: reqwest::Client::builder().timeout(timeout).build()?,
        })
    }

    async fn post(&self, path: &str, request: &impl Serialize) -> Result<(), String> {
        let response = self
            .http
            .post(format!("{}{path}", self.endpoint))
            .bearer_auth(&self.admin_key)
            .json(request)
            .send()
            .await
            .map_err(|error| error.to_string())?;
        if !response.status().is_success() {
            let status = response.status();
            let detail = response.text().await.unwrap_or_default();
            return Err(format!("{path} failed with {status}: {detail}"));
        }
        Ok(())
    }
}

#[async_trait]
impl DrainMoveDriver for AdminDrainDriver {
    async fn apply(&self, command: crate::control::Command) -> Result<(), String> {
        match command {
            crate::control::Command::PrepareFollowerMove {
                feed,
                range_id,
                removed_replica,
                replacement_replica,
            } => {
                self.post(
                    "/v1/admin/ranges/move-follower",
                    &AdminMoveFollowerRequest {
                        request_id: Uuid::new_v4(),
                        feed,
                        range_id,
                        removed_replica,
                        replacement_replica,
                    },
                )
                .await
            }
            crate::control::Command::PrepareOwnerMove {
                feed,
                range_id,
                new_owner,
            } => {
                self.post(
                    "/v1/admin/ranges/move-owner",
                    &AdminMoveOwnerRequest {
                        request_id: Uuid::new_v4(),
                        feed,
                        range_id,
                        new_owner,
                    },
                )
                .await
            }
            command @ (crate::control::Command::MoveSubscriptionProgressReplica { .. }
            | crate::control::Command::RecoverSubscriptionProgressOwner { .. }) => self
                .control_plane
                .execute_commands(vec![command])
                .await
                .map(|_| ())
                .map_err(|error| error.to_string()),
            _ => Err("drain plan emitted an unexpected command".to_owned()),
        }
    }
}

async fn admin_move_owner(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<AdminMoveOwnerRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    authorize_admin(&state, &headers)?;
    let control_plane = state
        .control_plane
        .as_ref()
        .ok_or_else(|| ApiError::unavailable("Control Plane is unavailable"))?;
    let prepared = control_plane
        .execute_commands_with_request_id(
            vec![crate::control::Command::PrepareOwnerMove {
                feed: request.feed.clone(),
                range_id: request.range_id,
                new_owner: request.new_owner,
            }],
            request.request_id,
        )
        .await
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let plan: RangeOwnerMovePlan = serde_json::from_value(prepared.results[0].data.clone())
        .map_err(|error| ApiError::unavailable(error.to_string()))?;
    let source_endpoint = state
        .control_endpoints
        .resolve(&plan.source_assignment.owner)
        .await
        .ok_or_else(|| ApiError::unavailable("source owner endpoint unavailable"))?;
    let target_endpoint = state
        .control_endpoints
        .resolve(&plan.candidate_assignment.owner)
        .await
        .ok_or_else(|| ApiError::unavailable("candidate owner endpoint unavailable"))?;
    let key = state
        .internal_key
        .as_ref()
        .ok_or_else(|| ApiError::unavailable("internal credential unavailable"))?;
    let plan_request = MovePlanRequest {
        plan_id: plan.plan_id,
        range_id: plan.source_assignment.range_id,
    };
    if state
        .control
        .active_range_assignment_by_id(plan_request.range_id)
        .await
        == Some(plan.candidate_assignment.clone())
    {
        unfreeze_owner_move(&state, &source_endpoint, key, &plan_request).await?;
        unfreeze_owner_move(&state, &target_endpoint, key, &plan_request).await?;
        return Ok(Json(
            json!({ "status": "activated", "assignment": plan.candidate_assignment }),
        ));
    }
    let frozen = match post_move_request(
        &state,
        &source_endpoint,
        key,
        "/internal/active-range/owner-move/freeze",
        &plan_request,
    )
    .await
    {
        Ok(frozen) => frozen,
        Err(error) => {
            unfreeze_owner_move(&state, &source_endpoint, key, &plan_request).await?;
            return Err(error);
        }
    };
    let boundary: CommitPosition = match serde_json::from_value(frozen["source_commit"].clone()) {
        Ok(commit) => commit,
        Err(error) => {
            unfreeze_owner_move(&state, &source_endpoint, key, &plan_request).await?;
            return Err(ApiError::unavailable(error.to_string()));
        }
    };
    let evidence = post_move_request(
        &state,
        &target_endpoint,
        key,
        "/internal/active-range/owner-move/verify",
        &OwnerMoveVerifyRequest {
            plan_id: plan.plan_id,
            range_id: plan_request.range_id,
            expected_commit: boundary,
        },
    )
    .await;
    let evidence: OwnerMoveEvidence = match evidence {
        Ok(data) => match serde_json::from_value(data) {
            Ok(evidence) => evidence,
            Err(error) => {
                unfreeze_owner_move(&state, &source_endpoint, key, &plan_request).await?;
                unfreeze_owner_move(&state, &target_endpoint, key, &plan_request).await?;
                return Err(ApiError::unavailable(error.to_string()));
            }
        },
        Err(error) => {
            unfreeze_owner_move(&state, &source_endpoint, key, &plan_request).await?;
            unfreeze_owner_move(&state, &target_endpoint, key, &plan_request).await?;
            return Err(error);
        }
    };
    if !evidence.ready || evidence.source_commit != boundary || evidence.target_commit != boundary {
        unfreeze_owner_move(&state, &source_endpoint, key, &plan_request).await?;
        unfreeze_owner_move(&state, &target_endpoint, key, &plan_request).await?;
        return Err(ApiError::unavailable(
            "candidate owner is not verified through frozen source boundary",
        ));
    }
    control_plane
        .execute_commands(vec![crate::control::Command::RecordOwnerMoveCatchUp {
            feed: request.feed.clone(),
            plan_id: plan.plan_id,
            source_commit: evidence.source_commit,
            target_commit: evidence.target_commit,
            checksum_verified: evidence.ready,
        }])
        .await
        .map_err(|error| ApiError::unavailable(error.to_string()))?;
    control_plane
        .execute_commands(vec![crate::control::Command::ActivateOwnerMove {
            feed: request.feed,
            plan_id: plan.plan_id,
        }])
        .await
        .map_err(|error| ApiError::unavailable(error.to_string()))?;
    unfreeze_owner_move(&state, &source_endpoint, key, &plan_request).await?;
    unfreeze_owner_move(&state, &target_endpoint, key, &plan_request).await?;
    Ok(Json(
        json!({ "status": "activated", "assignment": plan.candidate_assignment,
        "committed_position": evidence.target_commit }),
    ))
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ClientAppendRequest {
    request_id: Uuid,
    feed: String,
    writer_session_id: Uuid,
    writer_epoch: u64,
    sequence: u64,
    event_time_ns: Option<String>,
    key_base64: String,
    payload_base64: String,
    #[serde(default)]
    metadata_base64: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WriterBatchAppendRequest {
    pub records: Vec<WriterSessionAppendRequest>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WriterBatchAppendResponse {
    pub results: Vec<WriterAppendResponse>,
    pub feedback: WriterServerFeedback,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WriterAppendResponse {
    pub message_id: Uuid,
    pub cursor: String,
    pub deduplicated: bool,
    pub durability: String,
    pub feedback: WriterServerFeedback,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct InternalOwnerAppendResponse {
    result: Option<WriterAppendResponse>,
    error: Option<String>,
    retryable: bool,
}

async fn writer_batch_append(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<WriterBatchAppendRequest>,
) -> Result<Json<WriterBatchAppendResponse>, ApiError> {
    authorize_admin(&state, &headers)?;
    if request.records.is_empty() || request.records.len() > 1_000 {
        return Err(ApiError::bad_request(
            "Writer batch must contain between 1 and 1000 records",
        ));
    }
    let encoded_bytes = request.records.iter().fold(0_usize, |total, record| {
        total
            .saturating_add(record.key_base64.len())
            .saturating_add(record.payload_base64.len())
            .saturating_add(
                record
                    .metadata_base64
                    .values()
                    .map(String::len)
                    .sum::<usize>(),
            )
    });
    if encoded_bytes > crate::codec::MAX_FRAME_BYTES {
        return Err(ApiError::bad_request(
            "Writer batch encoded payload exceeds the maximum frame budget",
        ));
    }
    let mut results = Vec::with_capacity(request.records.len());
    for record in request.records {
        results.push(
            writer_session_append(State(state.clone()), headers.clone(), Json(record))
                .await?
                .0,
        );
    }
    Ok(Json(WriterBatchAppendResponse {
        results,
        feedback: writer_feedback(&state.demand),
    }))
}

async fn feed_when_applied(
    control: &ControlController,
    name: &str,
    wait_for_replication: bool,
) -> Option<crate::control::FeedDefinition> {
    let mut feed = control.active_feed_by_name(name).await;
    if feed.is_none() && wait_for_replication {
        for _ in 0..20 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            feed = control.active_feed_by_name(name).await;
            if feed.is_some() {
                break;
            }
        }
    }
    feed
}

async fn writer_when_applied(
    control: &ControlController,
    name: &str,
    wait_for_replication: bool,
) -> Option<crate::control::WriterDefinition> {
    let mut writer = control.active_writer_by_name(name).await;
    if writer.is_none() && wait_for_replication {
        for _ in 0..20 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            writer = control.active_writer_by_name(name).await;
            if writer.is_some() {
                break;
            }
        }
    }
    writer
}

async fn writer_session_append(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<WriterSessionAppendRequest>,
) -> Result<Json<WriterAppendResponse>, ApiError> {
    authorize_admin(&state, &headers)?;
    let writer = writer_when_applied(&state.control, &request.writer, state.control_plane.is_some())
        .await
        .ok_or_else(|| {
        if state.control_plane.is_some() {
            ApiError::unavailable(format!(
                "Writer {} is not available on this ingress Node; verify it exists or retry with the same request ID",
                request.writer
            ))
        } else {
            ApiError::bad_request(format!("Writer does not exist: {}", request.writer))
        }
    })?;
    let feed = state
        .control
        .active_feed_by_id(writer.feed_id)
        .await
        .ok_or_else(|| ApiError::bad_request("Writer Feed does not exist"))?;
    let routing_key = decode_base64("key_base64", &request.key_base64)?;
    if routing_key.is_empty() {
        return Err(ApiError::bad_request(
            "key_base64 must contain a non-empty key",
        ));
    }
    let (route, _) = state
        .control
        .active_range_for_key(feed.feed_id, &routing_key)
        .await
        .ok_or_else(|| ApiError::unavailable("Active Range route is unavailable"))?;
    let commands = vec![crate::control::Command::AllocateWriterRangeSequence {
        writer: request.writer,
        session_epoch: request.session_epoch,
        range_id: route.range_id,
    }];
    let allocation_request_id = deterministic_uuid(
        request.request_id,
        &format!("writer-range:{}", route.range_id),
    );
    let execution = match &state.control_plane {
        Some(control_plane) => control_plane
            .execute_commands_with_request_id(commands, allocation_request_id)
            .await
            .map_err(|error| ApiError::bad_request(error.to_string()))?,
        None => {
            state
                .control
                .execute_commands_with_request_id(commands, allocation_request_id)
                .await?
        }
    };
    let sequence = execution.results[0].data["sequence"]
        .as_u64()
        .ok_or_else(|| ApiError::unavailable("Writer sequence allocation returned no sequence"))?;
    client_append(
        State(state),
        headers,
        Json(ClientAppendRequest {
            request_id: request.request_id,
            feed: feed.name,
            writer_session_id: writer.writer_id,
            writer_epoch: request.session_epoch,
            sequence,
            event_time_ns: request.event_time_ns,
            key_base64: request.key_base64,
            payload_base64: request.payload_base64,
            metadata_base64: request.metadata_base64,
        }),
    )
    .await
}

async fn client_append(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<ClientAppendRequest>,
) -> Result<Json<WriterAppendResponse>, ApiError> {
    authorize_admin(&state, &headers)?;
    let feed = state
        .control
        .active_feed_by_name(&request.feed)
        .await
        .ok_or_else(|| ApiError::bad_request(format!("Feed does not exist: {}", request.feed)))?;
    route_append_to_owner(&state, feed.feed_id, request)
        .await
        .map(Json)
}

/// Resolve the Active Range for one routing key and forward the append to its
/// current Append Owner, locally or over the internal plane. Effect apply
/// sinks share this path so declared outputs dedupe on the same writer
/// identity rules as public appends.
async fn route_append_to_owner(
    state: &AppState,
    feed_id: Uuid,
    request: ClientAppendRequest,
) -> Result<WriterAppendResponse, ApiError> {
    let routing_key = decode_base64("key_base64", &request.key_base64)?;
    if routing_key.is_empty() {
        return Err(ApiError::bad_request(
            "key_base64 must contain a non-empty key",
        ));
    }
    let (_, assignment) = state
        .control
        .active_range_for_key(feed_id, &routing_key)
        .await
        .ok_or_else(|| ApiError::unavailable("Active Range route is unavailable"))?;
    let local = state.storage_node_id.as_ref().ok_or_else(|| {
        ApiError::unavailable("this Node is not configured for Active Range routing")
    })?;
    if &assignment.owner == local {
        return owner_append_local(state, request).await;
    }
    let endpoint = state
        .control_endpoints
        .resolve(&assignment.owner)
        .await
        .ok_or_else(|| {
            ApiError::unavailable(format!(
                "current Append Owner {} has no endpoint",
                assignment.owner
            ))
        })?;
    let key = state
        .internal_key
        .as_ref()
        .ok_or_else(|| ApiError::unavailable("internal forwarding credential is unavailable"))?;
    let response = state
        .internal_http
        .post(format!(
            "{}/internal/active-range/owner/append",
            endpoint.trim_end_matches('/')
        ))
        .header("x-whitewater-control-key", key)
        .json(&request)
        .send()
        .await
        .map_err(|error| ApiError::unavailable(error.to_string()))?;
    if !response.status().is_success() {
        return Err(ApiError::unavailable(format!(
            "Append Owner returned HTTP {}",
            response.status()
        )));
    }
    let response: InternalOwnerAppendResponse = response
        .json()
        .await
        .map_err(|error| ApiError::unavailable(error.to_string()))?;
    response.result.ok_or_else(|| ApiError {
        status: if response.retryable {
            StatusCode::SERVICE_UNAVAILABLE
        } else {
            StatusCode::CONFLICT
        },
        message: response
            .error
            .unwrap_or_else(|| "Append Owner returned no result".to_owned()),

        code: None,
    })
}

async fn owner_append(
    State(state): State<AppState>,
    Json(request): Json<ClientAppendRequest>,
) -> Json<InternalOwnerAppendResponse> {
    Json(match owner_append_local(&state, request).await {
        Ok(result) => InternalOwnerAppendResponse {
            result: Some(result),
            error: None,
            retryable: false,
        },
        Err(error) => InternalOwnerAppendResponse {
            result: None,
            retryable: error.status == StatusCode::SERVICE_UNAVAILABLE,
            error: Some(error.message),
        },
    })
}

async fn owner_append_local(
    state: &AppState,
    request: ClientAppendRequest,
) -> Result<WriterAppendResponse, ApiError> {
    let feed = feed_when_applied(&state.control, &request.feed, state.control_plane.is_some())
        .await
        .ok_or_else(|| {
            if state.control_plane.is_some() {
                ApiError::unavailable(format!(
                    "Feed {} is not available on this Append Owner; verify it exists or retry with the same request ID",
                    request.feed
                ))
            } else {
                ApiError::bad_request(format!("Feed does not exist: {}", request.feed))
            }
        })?;
    let routing_key = decode_base64("key_base64", &request.key_base64)?;
    if routing_key.is_empty() {
        return Err(ApiError::bad_request(
            "key_base64 must contain a non-empty key",
        ));
    }
    let (_, assignment) = state
        .control
        .active_range_for_key(feed.feed_id, &routing_key)
        .await
        .ok_or_else(|| ApiError::unavailable("Active Range route is unavailable"))?;
    if state.storage_node_id.as_ref() != Some(&assignment.owner) {
        return Err(ApiError::unavailable(format!(
            "Node is not current Append Owner {}; refresh assignment and retry",
            assignment.owner
        )));
    }
    let key = routing_key;
    let pressure_key = key.clone();
    let payload = decode_base64("payload_base64", &request.payload_base64)?;
    let pressure_bytes = key.len().saturating_add(payload.len());
    let metadata = request
        .metadata_base64
        .into_iter()
        .map(|(name, value)| decode_base64("metadata_base64", &value).map(|value| (name, value)))
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    let ingest_time_ns = unix_ns();
    let event_time_ns = request
        .event_time_ns
        .as_deref()
        .map(str::parse::<i64>)
        .transpose()
        .map_err(|_| ApiError::bad_request("event_time_ns must be a signed 64-bit decimal string"))?
        .unwrap_or(ingest_time_ns);
    let message_id = deterministic_uuid(request.request_id, "message");
    let cursor = feed_cursor(feed.feed_id, request.request_id);
    let frame = encode_record(&StoredRecord {
        message_id,
        producer_id: request.writer_session_id,
        producer_sequence: request.sequence,
        event_time_ns,
        ingest_time_ns,
        key,
        payload,
        metadata,
    })
    .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let coordinator = state
        .majority_append
        .as_ref()
        .ok_or_else(|| ApiError::unavailable("majority append coordinator is unavailable"))?;
    let result = coordinator
        .append(ReplicaAppendRequest {
            feed_id: feed.feed_id,
            range_id: assignment.range_id,
            generation: assignment.generation,
            ownership_epoch: assignment.ownership_epoch,
            append_owner: assignment.owner,
            expected_position: crate::active_range::RangePosition::new(0),
            identity: AppendIdentity {
                writer_session_id: request.writer_session_id,
                writer_epoch: request.writer_epoch,
                sequence: request.sequence,
            },
            cursor,
            frame_base64: STANDARD.encode(frame),
        })
        .await
        .map_err(majority_api_error)?;
    state
        .demand
        .record_range_append(assignment.range_id, &pressure_key, pressure_bytes);
    Ok(WriterAppendResponse {
        message_id: result.message_id,
        cursor: result.cursor,
        deduplicated: result.deduplicated,
        durability: "majority_committed".to_owned(),
        feedback: writer_feedback(&state.demand),
    })
}

fn writer_feedback(demand: &DemandMetrics) -> WriterServerFeedback {
    let snapshot = demand.snapshot();
    let pressure = (snapshot.requests_in_flight as f64 / 100.0).clamp(0.0, 1.0);
    WriterServerFeedback {
        recommended_batch_count: if pressure > 0.75 { 16 } else { 100 },
        recommended_batch_bytes: if pressure > 0.75 {
            256 * 1024
        } else {
            1024 * 1024
        },
        pressure,
        retry_after_ms: if pressure > 0.9 { 50 } else { 0 },
        max_frame_bytes: crate::codec::MAX_FRAME_BYTES,
    }
}

fn feed_cursor(feed_id: Uuid, request_id: Uuid) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"whitewater-feed-cursor-v2");
    hasher.update(feed_id.as_bytes());
    hasher.update(request_id.as_bytes());
    URL_SAFE_NO_PAD.encode(hasher.finalize().as_bytes())
}

fn reader_frontier_cursor(reader_id: Uuid, session_epoch: u64, request_id: Uuid) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"whitewater-reader-frontier-v1");
    hasher.update(reader_id.as_bytes());
    hasher.update(&session_epoch.to_be_bytes());
    hasher.update(request_id.as_bytes());
    format!(
        "rf1_{}",
        URL_SAFE_NO_PAD.encode(hasher.finalize().as_bytes())
    )
}

fn deterministic_uuid(request_id: Uuid, label: &str) -> Uuid {
    let mut bytes: [u8; 16] = blake3::hash(&[request_id.as_bytes(), label.as_bytes()].concat())
        .as_bytes()[..16]
        .try_into()
        .unwrap_or([0; 16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

fn majority_api_error(error: MajorityAppendError) -> ApiError {
    ApiError {
        status: if error.retryable {
            StatusCode::SERVICE_UNAVAILABLE
        } else {
            StatusCode::CONFLICT
        },
        message: error.to_string(),
        code: None,
    }
}

const MAX_LOGICAL_READ_BYTES: usize = MAX_COMMITTED_READ_BYTES;
const MAX_LOGICAL_READ_FRAMES: usize = 10_000;
const MAX_LOGICAL_READ_RANGES: usize = 128;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ReadRangePageRequest {
    assignment: ActiveRangeAssignment,
    after: Option<RangePosition>,
    expected_commit: Option<CommitPosition>,
    #[serde(default)]
    after_cursor: Option<String>,
    #[serde(default)]
    tail_count: Option<usize>,
    #[serde(default)]
    single_range: bool,
    #[serde(default)]
    page_limit: Option<usize>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ReadRangePageResponse {
    committed: CommitPosition,
    #[serde(default)]
    resolved_after: Option<RangePosition>,
    frames: Vec<RepairFrame>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ReadRangeEvidenceRequest {
    assignment: ActiveRangeAssignment,
    position: CommitPosition,
}

async fn read_range_evidence(
    State(state): State<AppState>,
    Json(request): Json<ReadRangeEvidenceRequest>,
) -> Result<Json<ReadReplicaEvidence>, ApiError> {
    let service = state
        .replica_append
        .as_ref()
        .ok_or_else(|| ApiError::unavailable("replica storage is not available"))?;
    service
        .read_replica_evidence(&request.assignment, request.position)
        .await
        .map(Json)
        .map_err(|error| ApiError::unavailable(error.message))
}

fn require_read_quorum(
    assignment: &ActiveRangeAssignment,
    position: CommitPosition,
    owner: &ReadReplicaEvidence,
    followers: &[ReadReplicaEvidence],
) -> Result<(), ApiError> {
    if owner.node != assignment.owner
        || owner.committed < position
        || position.value() > 0 && owner.digest.is_none()
    {
        return Err(ApiError::unavailable(
            "Append Owner cannot prove its committed read boundary",
        ));
    }
    let mut confirmed = false;
    for follower in followers {
        if follower.node == owner.node || !assignment.replicas.contains(&follower.node) {
            return Err(ApiError::unavailable(
                "read evidence came from a non-replica Node",
            ));
        }
        if follower.committed > owner.committed {
            return Err(ApiError::unavailable(
                "Append Owner is behind another committed replica; retry after repair or recovery",
            ));
        }
        if follower.committed >= position && follower.digest == owner.digest {
            confirmed = true;
        }
    }
    if !confirmed {
        return Err(ApiError::unavailable(
            "no other replica confirms the committed prefix and digest; retry after catch-up",
        ));
    }
    Ok(())
}

async fn verify_read_quorum(
    state: &AppState,
    assignment: &ActiveRangeAssignment,
    position: CommitPosition,
) -> Result<(), ApiError> {
    let service = state
        .replica_append
        .as_ref()
        .ok_or_else(|| ApiError::unavailable("owner replica storage is not available"))?;
    let owner = service
        .read_replica_evidence(assignment, position)
        .await
        .map_err(|error| ApiError::unavailable(error.message))?;
    let key = state
        .internal_key
        .as_ref()
        .ok_or_else(|| ApiError::unavailable("read evidence credential is unavailable"))?;
    let followers = join_all(
        assignment
            .replicas
            .iter()
            .filter(|node| **node != assignment.owner)
            .map(|node| async {
                let endpoint = state.control_endpoints.resolve(node).await?;
                let response = state
                    .internal_http
                    .post(format!(
                        "{}/internal/active-range/read/evidence",
                        endpoint.trim_end_matches('/')
                    ))
                    .header("x-whitewater-control-key", key)
                    .json(&ReadRangeEvidenceRequest {
                        assignment: assignment.clone(),
                        position,
                    })
                    .timeout(Duration::from_secs(2))
                    .send()
                    .await
                    .ok()?
                    .error_for_status()
                    .ok()?
                    .json::<ReadReplicaEvidence>()
                    .await
                    .ok()?;
                (response.node == *node).then_some(response)
            }),
    )
    .await
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();
    require_read_quorum(assignment, position, &owner, &followers)
}

async fn read_owned_range_page(
    State(state): State<AppState>,
    Json(request): Json<ReadRangePageRequest>,
) -> Result<Json<ReadRangePageResponse>, ApiError> {
    let service = state
        .replica_append
        .as_ref()
        .ok_or_else(|| ApiError::unavailable("Active Range storage is not available"))?;
    let expected_commit = if request.expected_commit.is_none() && state.control_plane.is_some() {
        let owner = service
            .read_replica_evidence(&request.assignment, CommitPosition::new(0))
            .await
            .map_err(|error| ApiError::unavailable(error.message))?;
        verify_read_quorum(&state, &request.assignment, owner.committed).await?;
        Some(owner.committed)
    } else {
        request.expected_commit
    };
    let (committed, resolved_after, frames) = if request.single_range {
        service
            .read_owned_range_cursor_page(
                &request.assignment,
                request.after_cursor.as_deref(),
                request.after,
                expected_commit,
                request.tail_count,
                request.page_limit.unwrap_or(1),
            )
            .await
    } else if request.after_cursor.is_some() || request.tail_count.is_some() {
        return Err(ApiError::bad_request(
            "Cursor and tail read modes require single-range routing",
        ));
    } else {
        service
            .read_owned_range_page(
                &request.assignment,
                request.after,
                expected_commit,
                request.page_limit.unwrap_or(32),
            )
            .await
            .map(|(commit, frames)| (commit, request.after, frames))
    }
    .map_err(|error| ApiError {
        status: if error.retryable {
            StatusCode::SERVICE_UNAVAILABLE
        } else {
            StatusCode::BAD_REQUEST
        },
        message: error.message,

        code: None,
    })?;
    if frames
        .iter()
        .any(|frame| frame.frame.len() > MAX_LOGICAL_READ_BYTES)
    {
        return Err(ApiError::unavailable(
            "record exceeds the bounded Feed read byte budget",
        ));
    }
    Ok(Json(ReadRangePageResponse {
        committed,
        resolved_after,
        frames: frames
            .into_iter()
            .map(|frame| RepairFrame {
                position: frame.position,
                identity: frame.identity,
                cursor: frame.cursor,
                frame_base64: STANDARD.encode(frame.frame),
            })
            .collect(),
    }))
}

async fn read_placement(
    state: &AppState,
    feed_id: Uuid,
    feed_name: &str,
) -> Result<Vec<ActiveRangeAssignment>, ApiError> {
    let (routes, assignments): (Vec<RangeId>, Vec<ActiveRangeAssignment>) =
        if let Some(control_plane) = &state.control_plane {
            let execution = control_plane
                .execute_commands(vec![crate::control::Command::InspectPlacement {
                    feed: feed_name.to_owned(),
                }])
                .await
                .map_err(|error| ApiError::unavailable(error.to_string()))?;
            let data = &execution.results[0].data;
            let routes = data["range_map"]["routes"]
                .as_array()
                .ok_or_else(|| ApiError::unavailable("Control Plane RangeMap is unavailable"))?
                .iter()
                .map(|route| serde_json::from_value(route["range_id"].clone()))
                .collect::<Result<_, _>>()
                .map_err(|error| ApiError::unavailable(error.to_string()))?;
            let assignments = serde_json::from_value(data["range_assignments"].clone())
                .map_err(|error| ApiError::unavailable(error.to_string()))?;
            (routes, assignments)
        } else {
            let map = state
                .control
                .active_range_map(feed_id)
                .await
                .ok_or_else(|| ApiError::unavailable("Feed RangeMap is unavailable"))?;
            let routes = map.routes().iter().map(|route| route.range_id).collect();
            let assignments = state
                .control
                .active_range_assignments_for_feed(feed_id)
                .await;
            (routes, assignments)
        };
    if routes.is_empty()
        || routes.len() > MAX_LOGICAL_READ_RANGES
        || routes.len() != assignments.len()
        || routes
            .iter()
            .zip(&assignments)
            .any(|(range_id, assignment)| {
                *range_id != assignment.range_id || assignment.feed_id != feed_id
            })
    {
        return Err(ApiError::unavailable(
            "complete current Feed placement is unavailable or exceeds the bounded read limit",
        ));
    }
    Ok(assignments)
}

async fn fetch_range_page(
    state: &AppState,
    request: ReadRangePageRequest,
) -> Result<ReadRangePageResponse, ApiError> {
    let service = state.replica_append.as_ref().ok_or_else(|| {
        ApiError::unavailable("Active Range replica storage is not configured on this Node")
    })?;
    if service.local_node() == &request.assignment.owner {
        return Ok(read_owned_range_page(State(state.clone()), Json(request))
            .await?
            .0);
    }
    let endpoint = state
        .control_endpoints
        .resolve(&request.assignment.owner)
        .await
        .ok_or_else(|| ApiError::unavailable("current owner endpoint is unavailable"))?;
    let key = state
        .internal_key
        .as_ref()
        .ok_or_else(|| ApiError::unavailable("internal read credential is unavailable"))?;
    let response = state
        .internal_http
        .post(format!(
            "{}/internal/active-range/read/committed",
            endpoint.trim_end_matches('/')
        ))
        .header("x-whitewater-control-key", key)
        .json(&request)
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .map_err(|error| ApiError::unavailable(error.to_string()))?;
    if response.status() == StatusCode::BAD_REQUEST {
        return Err(ApiError::bad_request(
            "Cursor is unknown, uncommitted, or belongs to another Feed",
        ));
    }
    let mut response = response
        .error_for_status()
        .map_err(|error| ApiError::unavailable(error.to_string()))?;
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| ApiError::unavailable(error.to_string()))?
    {
        if bytes
            .len()
            .checked_add(chunk.len())
            .is_none_or(|size| size > MAX_LOGICAL_READ_BYTES * 2)
        {
            return Err(ApiError::unavailable(
                "owner range page exceeds the bounded response budget",
            ));
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice::<ReadRangePageResponse>(&bytes)
        .map_err(|error| ApiError::unavailable(error.to_string()))
}

fn merge_feed_frames(
    mut frames: Vec<StoredRangeFrame>,
    after: Option<&str>,
    limit: usize,
) -> Result<Vec<StoredRangeFrame>, ApiError> {
    let mut ordered = frames
        .iter()
        .map(|item| {
            decode_record(&item.frame)
                .map(|record| (record.ingest_time_ns, record.message_id))
                .map_err(|error| ApiError::unavailable(error.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut indexed = frames.drain(..).zip(ordered.drain(..)).collect::<Vec<_>>();
    indexed.sort_by_key(|(_, order)| *order);
    let start = match after {
        Some(cursor) => indexed
            .iter()
            .position(|(item, _)| item.cursor == cursor)
            .map(|index| index + 1)
            .ok_or_else(|| {
                ApiError::bad_request("Cursor is unknown, uncommitted, or belongs to another Feed")
            })?,
        None => 0,
    };
    Ok(indexed
        .into_iter()
        .skip(start)
        .take(limit.min(MAX_LOGICAL_READ_FRAMES))
        .map(|(frame, _)| frame)
        .collect())
}

#[derive(Clone, Copy)]
struct SingleRangeReadMode {
    tail: bool,
    require_complete: bool,
}

async fn read_single_range(
    state: &AppState,
    feed_id: Uuid,
    feed_name: &str,
    assignment: &ActiveRangeAssignment,
    after: Option<&str>,
    limit: usize,
    mode: SingleRangeReadMode,
) -> Result<Vec<StoredRangeFrame>, ApiError> {
    let mut frames = Vec::new();
    let mut bytes = 0_usize;
    let mut after_position = None;
    let mut boundary = None;
    let mut after_cursor = after.map(str::to_owned);
    let limit = limit.min(MAX_LOGICAL_READ_FRAMES);
    loop {
        let page = fetch_range_page(
            state,
            ReadRangePageRequest {
                assignment: assignment.clone(),
                after: after_position,
                expected_commit: boundary,
                after_cursor: after_cursor.take(),
                tail_count: (mode.tail && boundary.is_none()).then_some(limit),
                single_range: true,
                page_limit: Some(limit.saturating_sub(frames.len()).clamp(1, 32)),
            },
        )
        .await?;
        if boundary.is_some_and(|expected| expected != page.committed)
            || mode.require_complete && page.committed.value() > MAX_LOGICAL_READ_FRAMES as u64
        {
            return Err(ApiError::unavailable(
                "committed range changed or full-history scan exceeds bounded read support",
            ));
        }
        boundary = Some(page.committed);
        let start = page.resolved_after.map_or(0, RangePosition::value);
        if start > page.committed.value() {
            return Err(ApiError::unavailable(
                "Cursor points beyond the committed range boundary",
            ));
        }
        if limit == 0 || start == page.committed.value() {
            break;
        }
        let before = frames.len();
        let mut expected = start.saturating_add(1);
        for frame in page.frames {
            if frame.position.value() != expected || frame.position.value() > page.committed.value()
            {
                return Err(ApiError::unavailable(
                    "current owner returned a gapped committed range",
                ));
            }
            let decoded = STANDARD
                .decode(frame.frame_base64.as_bytes())
                .map_err(|_| {
                    ApiError::unavailable("current owner returned an invalid frame encoding")
                })?;
            bytes = bytes.saturating_add(decoded.len() + frame.cursor.len());
            if decoded.len() > MAX_LOGICAL_READ_BYTES || bytes > MAX_LOGICAL_READ_BYTES {
                return Err(ApiError::unavailable(
                    "Feed page exceeds the bounded read byte budget",
                ));
            }
            frames.push(StoredRangeFrame {
                position: frame.position,
                identity: frame.identity,
                cursor: frame.cursor,
                frame: decoded,
            });
            after_position = Some(frame.position);
            expected = expected.saturating_add(1);
        }
        if frames.len() == before {
            return Err(ApiError::unavailable(
                "current owner omitted a committed range frame",
            ));
        }
        if frames.len() >= limit
            || after_position.is_some_and(|position| position.value() >= page.committed.value())
        {
            break;
        }
    }
    if read_placement(state, feed_id, feed_name).await? != vec![assignment.clone()] {
        return Err(ApiError::unavailable(
            "Feed placement changed during read; retry with the same Cursor",
        ));
    }
    Ok(frames)
}

struct ReaderRangeHead {
    assignment: ActiveRangeAssignment,
    committed: CommitPosition,
    next: Option<(StoredRangeFrame, (i64, Uuid))>,
}

async fn reader_range_head(
    state: &AppState,
    assignment: &ActiveRangeAssignment,
    after_cursor: Option<&str>,
    after: Option<RangePosition>,
    boundary: Option<CommitPosition>,
) -> Result<(CommitPosition, Option<(StoredRangeFrame, (i64, Uuid))>), ApiError> {
    let page = fetch_range_page(
        state,
        ReadRangePageRequest {
            assignment: assignment.clone(),
            after,
            expected_commit: boundary,
            after_cursor: after_cursor.map(str::to_owned),
            tail_count: None,
            single_range: true,
            page_limit: Some(1),
        },
    )
    .await?;
    if boundary.is_some_and(|expected| expected != page.committed) {
        return Err(ApiError::unavailable(
            "range read boundary changed; retry with the same Reader session",
        ));
    }
    let start = page.resolved_after.map_or(0, RangePosition::value);
    if start > page.committed.value() {
        return Err(ApiError::unavailable(
            "Reader frontier is beyond the committed range",
        ));
    }
    let Some(frame) = page.frames.into_iter().next() else {
        if start == page.committed.value() {
            return Ok((page.committed, None));
        }
        return Err(ApiError::unavailable(
            "current owner omitted a committed Reader frame",
        ));
    };
    if frame.position.value() != start.saturating_add(1)
        || frame.position.value() > page.committed.value()
    {
        return Err(ApiError::unavailable(
            "current owner returned a gapped Reader frame",
        ));
    }
    let decoded = STANDARD
        .decode(frame.frame_base64.as_bytes())
        .map_err(|_| ApiError::unavailable("invalid committed Reader frame encoding"))?;
    let record =
        decode_record(&decoded).map_err(|error| ApiError::unavailable(error.to_string()))?;
    Ok((
        page.committed,
        Some((
            StoredRangeFrame {
                position: frame.position,
                identity: frame.identity,
                cursor: frame.cursor,
                frame: decoded,
            },
            (record.ingest_time_ns, record.message_id),
        )),
    ))
}

async fn read_reader_frontier(
    state: &AppState,
    feed_id: Uuid,
    feed_name: &str,
    previous: &BTreeMap<RangeId, String>,
    limit: usize,
) -> Result<(Vec<StoredRangeFrame>, BTreeMap<RangeId, String>), ApiError> {
    let assignments = read_placement(state, feed_id, feed_name).await?;
    if !previous.is_empty()
        && (previous.len() != assignments.len()
            || assignments
                .iter()
                .any(|assignment| !previous.contains_key(&assignment.range_id)))
    {
        return Err(ApiError::unavailable(
            "Reader topology changed; retry after frontier translation or seek explicitly",
        ));
    }
    let mut progress = assignments
        .iter()
        .map(|assignment| {
            (
                assignment.range_id,
                previous
                    .get(&assignment.range_id)
                    .cloned()
                    .unwrap_or_default(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut heads = Vec::with_capacity(assignments.len());
    let mut bytes = 0_usize;
    for assignment in &assignments {
        let cursor = progress
            .get(&assignment.range_id)
            .filter(|value| !value.is_empty());
        let (committed, next) =
            reader_range_head(state, assignment, cursor.map(String::as_str), None, None).await?;
        if let Some((frame, _)) = &next {
            bytes = bytes.saturating_add(frame.frame.len() + frame.cursor.len());
            if bytes > MAX_LOGICAL_READ_BYTES {
                return Err(ApiError::unavailable(
                    "Reader range heads exceed the bounded read byte budget",
                ));
            }
        }
        heads.push(ReaderRangeHead {
            assignment: assignment.clone(),
            committed,
            next,
        });
    }
    let mut records = Vec::new();
    while records.len() < limit.min(MAX_LOGICAL_READ_FRAMES) {
        let Some(index) = heads
            .iter()
            .enumerate()
            .filter_map(|(index, head)| head.next.as_ref().map(|(_, key)| (index, *key)))
            .min_by_key(|(index, key)| (*key, *index))
            .map(|(index, _)| index)
        else {
            break;
        };
        let head = &mut heads[index];
        let (record, _) = head
            .next
            .take()
            .ok_or_else(|| ApiError::unavailable("Reader head disappeared"))?;
        progress.insert(head.assignment.range_id, record.cursor.clone());
        let after = record.position;
        records.push(record);
        if records.len() == limit.min(MAX_LOGICAL_READ_FRAMES) {
            break;
        }
        let (_, next) = reader_range_head(
            state,
            &head.assignment,
            None,
            Some(after),
            Some(head.committed),
        )
        .await?;
        if let Some((frame, _)) = &next {
            bytes = bytes.saturating_add(frame.frame.len() + frame.cursor.len());
            if bytes > MAX_LOGICAL_READ_BYTES {
                return Err(ApiError::unavailable(
                    "Reader page exceeds the bounded read byte budget",
                ));
            }
        }
        head.next = next;
    }
    if read_placement(state, feed_id, feed_name).await? != assignments {
        return Err(ApiError::unavailable(
            "Feed placement changed during Reader fetch; retry with the same session",
        ));
    }
    Ok((records, progress))
}

struct RangeFrameStream<'a> {
    state: &'a AppState,
    assignment: &'a ActiveRangeAssignment,
    boundary: Option<CommitPosition>,
    after: Option<RangePosition>,
    after_cursor: Option<String>,
    pending: std::collections::VecDeque<StoredRangeFrame>,
    exhausted: bool,
}

impl RangeFrameStream<'_> {
    fn new<'a>(state: &'a AppState, assignment: &'a ActiveRangeAssignment) -> RangeFrameStream<'a> {
        RangeFrameStream {
            state,
            assignment,
            boundary: None,
            after: None,
            after_cursor: None,
            pending: std::collections::VecDeque::new(),
            exhausted: false,
        }
    }

    async fn next_frame(&mut self) -> Result<Option<StoredRangeFrame>, ApiError> {
        loop {
            if let Some(frame) = self.pending.pop_front() {
                return Ok(Some(frame));
            }
            if self.exhausted {
                return Ok(None);
            }
            let after_cursor = self.after_cursor.take();
            let page = fetch_range_page(
                self.state,
                ReadRangePageRequest {
                    assignment: self.assignment.clone(),
                    after: self.after,
                    expected_commit: self.boundary,
                    single_range: after_cursor.is_some(),
                    after_cursor,
                    tail_count: None,
                    page_limit: Some(128),
                },
            )
            .await?;
            if self
                .boundary
                .is_some_and(|expected| expected != page.committed)
            {
                return Err(ApiError::unavailable(
                    "committed range boundary changed during read; retry with the same Cursor",
                ));
            }
            self.boundary = Some(page.committed);
            let mut expected = page.resolved_after.map_or(1, |position: RangePosition| {
                position.value().saturating_add(1)
            });
            for frame in page.frames {
                if frame.position.value() != expected
                    || frame.position.value() > page.committed.value()
                {
                    return Err(ApiError::unavailable(
                        "current owner returned a gapped committed range",
                    ));
                }
                let decoded = STANDARD
                    .decode(frame.frame_base64.as_bytes())
                    .map_err(|_| {
                        ApiError::unavailable("current owner returned an invalid frame encoding")
                    })?;
                if decoded.len() > MAX_LOGICAL_READ_BYTES {
                    return Err(ApiError::unavailable(
                        "record exceeds the bounded Feed read byte budget",
                    ));
                }
                self.pending.push_back(StoredRangeFrame {
                    position: frame.position,
                    identity: frame.identity,
                    cursor: frame.cursor,
                    frame: decoded,
                });
                self.after = Some(frame.position);
                expected = expected.saturating_add(1);
            }
            if self.after.is_none() {
                self.after = page.resolved_after;
            }
            if self.after.map_or(0, RangePosition::value) >= page.committed.value() {
                self.exhausted = true;
            } else if self.pending.is_empty() {
                return Err(ApiError::unavailable(
                    "current owner omitted committed range frames",
                ));
            }
        }
    }
}

async fn read_paginated_feed(
    state: &AppState,
    feed_id: Uuid,
    feed_name: &str,
    after: Option<&str>,
    limit: usize,
) -> Result<Vec<StoredRangeFrame>, ApiError> {
    let assignments = read_placement(state, feed_id, feed_name).await?;
    if assignments.len() == 1 {
        return read_single_range(
            state,
            feed_id,
            feed_name,
            &assignments[0],
            after,
            limit,
            SingleRangeReadMode {
                tail: false,
                require_complete: false,
            },
        )
        .await;
    }
    let limit = limit.min(MAX_LOGICAL_READ_FRAMES);
    let mut streams: Vec<RangeFrameStream> = assignments
        .iter()
        .map(|assignment| RangeFrameStream::new(state, assignment))
        .collect();
    let mut heads: Vec<Option<((i64, Uuid), StoredRangeFrame)>> =
        (0..streams.len()).map(|_| None).collect();
    let mut emitted: Vec<StoredRangeFrame> = Vec::new();
    let mut bytes = 0_usize;
    let mut found_after = after.is_none();
    loop {
        for index in 0..streams.len() {
            if heads[index].is_none() {
                heads[index] = match streams[index].next_frame().await? {
                    Some(frame) => {
                        let record = decode_record(&frame.frame)
                            .map_err(|error| ApiError::unavailable(error.to_string()))?;
                        Some(((record.ingest_time_ns, record.message_id), frame))
                    }
                    None => None,
                };
            }
        }
        let Some(index) = heads
            .iter()
            .enumerate()
            .filter_map(|(index, head)| head.as_ref().map(|(key, _)| (index, *key)))
            .min_by_key(|(index, key)| (*key, *index))
            .map(|(index, _)| index)
        else {
            break;
        };
        let Some((_, frame)) = heads[index].take() else {
            continue;
        };
        if !found_after {
            if after == Some(frame.cursor.as_str()) {
                found_after = true;
            }
            continue;
        }
        if emitted.len() >= limit {
            break;
        }
        let frame_bytes = frame.frame.len() + frame.cursor.len();
        if bytes.saturating_add(frame_bytes) > MAX_LOGICAL_READ_BYTES {
            if emitted.is_empty() {
                return Err(ApiError::unavailable(
                    "record exceeds the bounded Feed read byte budget",
                ));
            }
            break;
        }
        bytes = bytes.saturating_add(frame_bytes);
        emitted.push(frame);
    }
    if !found_after {
        return Err(ApiError::bad_request(
            "Cursor is unknown, uncommitted, or belongs to another Feed",
        ));
    }
    if read_placement(state, feed_id, feed_name).await? != assignments {
        return Err(ApiError::unavailable(
            "Feed placement changed during read; retry with the same Cursor",
        ));
    }
    Ok(emitted)
}

async fn read_complete_feed(
    state: &AppState,
    feed_id: Uuid,
    feed_name: &str,
    after: Option<&str>,
    limit: usize,
    tail: bool,
    require_complete: bool,
) -> Result<Vec<StoredRangeFrame>, ApiError> {
    let assignments = read_placement(state, feed_id, feed_name).await?;
    if assignments.len() == 1 && !(tail && after.is_some()) {
        return read_single_range(
            state,
            feed_id,
            feed_name,
            &assignments[0],
            after,
            limit,
            SingleRangeReadMode {
                tail,
                require_complete,
            },
        )
        .await;
    }
    if !tail && !require_complete {
        return read_paginated_feed(state, feed_id, feed_name, after, limit).await;
    }
    let mut merged = Vec::new();
    let mut bytes = 0_usize;
    for assignment in &assignments {
        let mut after_position = None;
        let mut boundary = None;
        loop {
            let request = ReadRangePageRequest {
                assignment: assignment.clone(),
                after: after_position,
                expected_commit: boundary,
                after_cursor: None,
                tail_count: None,
                single_range: false,
                page_limit: Some(128),
            };
            let page = fetch_range_page(state, request).await?;
            if boundary.is_some_and(|expected| expected != page.committed)
                || page.committed.value() > MAX_LOGICAL_READ_FRAMES as u64
            {
                return Err(ApiError::unavailable(
                    "committed range boundary changed or exceeds bounded Feed reads",
                ));
            }
            boundary = Some(page.committed);
            if after_position.map_or(0, RangePosition::value) >= page.committed.value() {
                break;
            }
            let mut expected = after_position.map_or(1, |position: RangePosition| {
                position.value().saturating_add(1)
            });
            for frame in page.frames {
                if frame.position.value() != expected
                    || frame.position.value() > page.committed.value()
                {
                    return Err(ApiError::unavailable(
                        "current owner returned a gapped committed range",
                    ));
                }
                let decoded = STANDARD
                    .decode(frame.frame_base64.as_bytes())
                    .map_err(|_| {
                        ApiError::unavailable("current owner returned an invalid frame encoding")
                    })?;
                bytes = bytes.saturating_add(decoded.len() + frame.cursor.len());
                if decoded.len() > MAX_LOGICAL_READ_BYTES
                    || bytes > MAX_LOGICAL_READ_BYTES
                    || merged.len() >= MAX_LOGICAL_READ_FRAMES
                {
                    return Err(ApiError::unavailable("Feed history exceeds the bounded read budget; scalable continuation is not available"));
                }
                merged.push(StoredRangeFrame {
                    position: frame.position,
                    identity: frame.identity,
                    cursor: frame.cursor,
                    frame: decoded,
                });
                after_position = Some(frame.position);
                expected = expected.saturating_add(1);
            }
        }
    }
    if read_placement(state, feed_id, feed_name).await? != assignments {
        return Err(ApiError::unavailable(
            "Feed placement changed during read; retry with the same Cursor",
        ));
    }
    if tail {
        let ordered = merge_feed_frames(merged, after, MAX_LOGICAL_READ_FRAMES)?;
        Ok(ordered
            .into_iter()
            .rev()
            .take(limit)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect())
    } else {
        merge_feed_frames(merged, after, limit)
    }
}

#[derive(Deserialize)]
struct FeedReadQuery {
    feed: String,
    after: Option<String>,
    limit: Option<usize>,
}

async fn read_feed_records(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<FeedReadQuery>,
) -> Result<Json<Vec<RecordResponse>>, ApiError> {
    authorize_admin(&state, &headers)?;
    let feed = state
        .control
        .active_feed_by_name(&query.feed)
        .await
        .ok_or_else(|| ApiError::bad_request(format!("Feed does not exist: {}", query.feed)))?;
    let frames = read_complete_feed(
        &state,
        feed.feed_id,
        &feed.name,
        query.after.as_deref(),
        query.limit.unwrap_or(100),
        false,
        false,
    )
    .await?;
    frames
        .into_iter()
        .map(|item| {
            decode_record(&item.frame)
                .map(|record| {
                    record_response(CursorRecord {
                        cursor: item.cursor,
                        record,
                    })
                })
                .map_err(|error| ApiError::unavailable(error.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Json)
}

#[derive(Deserialize)]
struct CreateStreamRequest {
    name: String,
}

async fn create_stream(
    State(state): State<AppState>,
    Json(request): Json<CreateStreamRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let _request = state.demand.begin_request();
    let description = state.store.create_stream(&request.name).await?;
    Ok((StatusCode::CREATED, Json(description)))
}

async fn list_streams(State(state): State<AppState>) -> Result<impl IntoResponse, ApiError> {
    let _request = state.demand.begin_request();
    Ok(Json(state.store.list_streams().await?))
}

#[derive(Deserialize)]
struct DescribeQuery {
    name: String,
}

async fn describe_stream(
    State(state): State<AppState>,
    Query(query): Query<DescribeQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let _request = state.demand.begin_request();
    Ok(Json(state.store.describe(&query.name).await?))
}

#[derive(Deserialize)]
struct AppendRequest {
    stream: String,
    producer_id: Uuid,
    sequence: u64,
    message_id: Option<Uuid>,
    event_time_ns: Option<String>,
    timestamp_ms: Option<i64>,
    key_base64: String,
    payload_base64: String,
    #[serde(default, alias = "headers_base64")]
    metadata_base64: BTreeMap<String, String>,
}

#[derive(Serialize)]
struct AppendResponse {
    cursor: String,
    message_id: Uuid,
    deduplicated: bool,
}

async fn append_record(
    State(state): State<AppState>,
    Json(request): Json<AppendRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let _request = state.demand.begin_request();
    let key = decode_base64("key_base64", &request.key_base64)?;
    if key.is_empty() {
        return Err(ApiError::bad_request(
            "key_base64 must contain a non-empty key",
        ));
    }
    let payload = decode_base64("payload_base64", &request.payload_base64)?;
    let metadata: BTreeMap<String, Vec<u8>> = request
        .metadata_base64
        .into_iter()
        .map(|(name, value)| {
            decode_base64("metadata_base64", &value).map(|decoded| (name, decoded))
        })
        .collect::<Result<_, _>>()?;
    let append_bytes = key.len() + payload.len() + metadata.values().map(Vec::len).sum::<usize>();
    let ingest_time_ns = unix_ns();
    let requested_event_time_ns = request
        .event_time_ns
        .as_deref()
        .map(str::parse::<i64>)
        .transpose()
        .map_err(|_| {
            ApiError::bad_request("event_time_ns must be a signed 64-bit decimal string")
        })?;
    let event_time_ns = resolve_event_time_ns(
        requested_event_time_ns,
        request.timestamp_ms,
        ingest_time_ns,
    )?;
    let result = state
        .store
        .append(
            &request.stream,
            AppendInput {
                message_id: request.message_id.unwrap_or_else(Uuid::new_v4),
                producer_id: request.producer_id,
                producer_sequence: request.sequence,
                event_time_ns,
                ingest_time_ns,
                key,
                payload,
                metadata,
            },
        )
        .await?;
    state.demand.record_append(append_bytes);
    Ok((
        StatusCode::CREATED,
        Json(AppendResponse {
            cursor: result.cursor,
            message_id: result.message_id,
            deduplicated: result.deduplicated,
        }),
    ))
}

#[derive(Deserialize)]
struct ReadQuery {
    stream: String,
    after: Option<String>,
    limit: Option<usize>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecordResponse {
    pub cursor: String,
    pub message_id: Uuid,
    pub producer_id: Uuid,
    pub sequence: u64,
    pub event_time_ns: String,
    pub ingest_time_ns: String,
    pub key_base64: String,
    pub payload_base64: String,
    pub metadata_base64: BTreeMap<String, String>,
}

async fn read_records(
    State(state): State<AppState>,
    Query(query): Query<ReadQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let _request = state.demand.begin_request();
    let records = state
        .store
        .read(
            &query.stream,
            query.after.as_deref(),
            query.limit.unwrap_or(100),
        )
        .await?;
    state.demand.record_read(records.len());
    Ok(Json(
        records.into_iter().map(record_response).collect::<Vec<_>>(),
    ))
}

#[derive(Serialize)]
struct NodeMetricsResponse {
    node_id: String,
    capacity: u32,
    demand: DemandSnapshot,
    storage: StorageStats,
}

async fn node_metrics(
    State(state): State<AppState>,
) -> Result<Json<NodeMetricsResponse>, ApiError> {
    Ok(Json(NodeMetricsResponse {
        node_id: state.membership.local().node_id.clone(),
        capacity: state.membership.local().capacity,
        demand: state.demand.snapshot(),
        storage: state.store.stats().await?,
    }))
}

#[derive(Deserialize)]
struct AutoscaleRecommendationRequest {
    policy: AutoscalePolicy,
    pressure: f64,
    removable_nodes: usize,
}

#[derive(Serialize)]
struct AutoscaleRecommendationResponse {
    current_nodes: usize,
    decision: ScaleDecision,
}

async fn autoscale_recommendation(
    State(state): State<AppState>,
    Json(request): Json<AutoscaleRecommendationRequest>,
) -> Result<Json<AutoscaleRecommendationResponse>, ApiError> {
    if !request.pressure.is_finite() || request.pressure < 0.0 {
        return Err(ApiError::bad_request(
            "pressure must be a finite non-negative number",
        ));
    }
    request.policy.validate().map_err(ApiError::bad_request)?;
    let current_nodes = state.membership.members().await.len();
    let decision = state.autoscaler.lock().await.evaluate(
        &request.policy,
        request.pressure,
        current_nodes,
        request.removable_nodes,
    );
    Ok(Json(AutoscaleRecommendationResponse {
        current_nodes,
        decision,
    }))
}

async fn cluster_members(State(state): State<AppState>) -> Json<Vec<MemberView>> {
    Json(state.membership.members().await)
}

async fn cluster_join(
    State(state): State<AppState>,
    Json(announcement): Json<MemberAnnouncement>,
) -> Json<JoinResponse> {
    state.membership.observe(announcement).await;
    Json(JoinResponse {
        member: state.membership.local().clone(),
        members: state.membership.members().await,
    })
}

async fn cluster_leave(
    State(state): State<AppState>,
    Json(announcement): Json<MemberAnnouncement>,
) -> StatusCode {
    state.membership.remove(&announcement.node_id).await;
    StatusCode::NO_CONTENT
}

fn record_response(value: CursorRecord) -> RecordResponse {
    let StoredRecord {
        message_id,
        producer_id,
        producer_sequence,
        event_time_ns,
        ingest_time_ns,
        key,
        payload,
        metadata,
    } = value.record;
    RecordResponse {
        cursor: value.cursor,
        message_id,
        producer_id,
        sequence: producer_sequence,
        event_time_ns: event_time_ns.to_string(),
        ingest_time_ns: ingest_time_ns.to_string(),
        key_base64: STANDARD.encode(key),
        payload_base64: STANDARD.encode(payload),
        metadata_base64: metadata
            .into_iter()
            .map(|(name, value)| (name, STANDARD.encode(value)))
            .collect(),
    }
}

fn decode_base64(field: &str, value: &str) -> Result<Vec<u8>, ApiError> {
    STANDARD
        .decode(value)
        .map_err(|_| ApiError::bad_request(format!("{field} is not valid base64")))
}

fn resolve_event_time_ns(
    event_time_ns: Option<i64>,
    legacy_timestamp_ms: Option<i64>,
    ingest_time_ns: i64,
) -> Result<i64, ApiError> {
    match (event_time_ns, legacy_timestamp_ms) {
        (Some(_), Some(_)) => Err(ApiError::bad_request(
            "event_time_ns and timestamp_ms cannot both be provided",
        )),
        (Some(value), None) => Ok(value),
        (None, Some(value)) => value
            .checked_mul(1_000_000)
            .ok_or_else(|| ApiError::bad_request("timestamp_ms is outside nanosecond range")),
        (None, None) => Ok(ingest_time_ns),
    }
}

fn unix_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .try_into()
        .unwrap_or(i64::MAX)
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    message: String,
    code: Option<&'static str>,
}

impl ApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
            code: None,
        }
    }

    fn unavailable(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            message: message.into(),
            code: None,
        }
    }

    fn conflict(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            message: message.into(),
            code: None,
        }
    }
}

impl From<StorageError> for ApiError {
    fn from(error: StorageError) -> Self {
        let status = match error {
            StorageError::StreamNotFound(_) => StatusCode::NOT_FOUND,
            StorageError::StaleSequence { .. }
            | StorageError::SequenceGap { .. }
            | StorageError::SequenceConflict => StatusCode::CONFLICT,
            StorageError::InvalidStreamName
            | StorageError::EmptyKey
            | StorageError::InvalidCursor(_)
            | StorageError::CursorOutOfRange => StatusCode::BAD_REQUEST,
            StorageError::Codec(_) | StorageError::Io(_) | StorageError::Worker(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        };
        Self {
            status,
            message: error.to_string(),
            code: None,
        }
    }
}

impl From<AdminAuthError> for ApiError {
    fn from(error: AdminAuthError) -> Self {
        Self {
            status: match error {
                AdminAuthError::NotConfigured | AdminAuthError::WeakKey => {
                    StatusCode::SERVICE_UNAVAILABLE
                }
                AdminAuthError::Missing | AdminAuthError::Invalid => StatusCode::UNAUTHORIZED,
            },
            message: error.to_string(),
            code: None,
        }
    }
}

impl From<ControlPlaneError> for ApiError {
    fn from(error: ControlPlaneError) -> Self {
        Self {
            status: match error {
                ControlPlaneError::Control(ref error) if error.is_client_error() => {
                    StatusCode::BAD_REQUEST
                }
                ControlPlaneError::Rejected(_) => StatusCode::BAD_REQUEST,
                ControlPlaneError::Unavailable(_)
                | ControlPlaneError::Storage(_)
                | ControlPlaneError::Io(_)
                | ControlPlaneError::Serialization(_) => StatusCode::SERVICE_UNAVAILABLE,
                ControlPlaneError::Control(_) => StatusCode::INTERNAL_SERVER_ERROR,
            },
            message: error.to_string(),
            code: None,
        }
    }
}

impl From<ControlError> for ApiError {
    fn from(error: ControlError) -> Self {
        Self {
            status: if error.is_client_error() {
                StatusCode::BAD_REQUEST
            } else {
                StatusCode::INTERNAL_SERVER_ERROR
            },
            message: error.to_string(),
            code: None,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut body = json!({ "error": self.message });
        if let Some(code) = self.code {
            body["code"] = json!(code);
        }
        (self.status, Json(body)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, time::Duration};

    use axum::{body::Body, http::Request};
    use openraft::BasicNode;
    use serde_json::Value;
    use tempfile::TempDir;
    use tower::ServiceExt;

    use super::*;
    use crate::{
        active_range::{
            orchestrate_split_cutover,
            store::{seed_committed_history, seed_committed_history_shaped},
            ActiveRangeDescriptor, FrozenSplitBoundary, OwnershipEpoch, RangeGeneration,
            ReplicaSet, SplitCutoverControl,
        },
        control::Command,
        membership::MemberAnnouncement,
        storage::FileLogStore,
    };

    #[test]
    fn append_request_uses_metadata_and_accepts_the_prototype_header_alias() {
        let request: AppendRequest = serde_json::from_value(json!({
            "stream": "/orders/eu",
            "producer_id": Uuid::new_v4(),
            "sequence": 1,
            "event_time_ns": "1700000000123456789",
            "key_base64": "aw==",
            "payload_base64": "dg==",
            "metadata_base64": { "trace-id": "dHJhY2U=" }
        }))
        .unwrap();
        assert_eq!(
            request.event_time_ns.as_deref(),
            Some("1700000000123456789")
        );
        assert_eq!(
            request.metadata_base64.get("trace-id").map(String::as_str),
            Some("dHJhY2U=")
        );

        let legacy: AppendRequest = serde_json::from_value(json!({
            "stream": "/orders/eu",
            "producer_id": Uuid::new_v4(),
            "sequence": 1,
            "key_base64": "aw==",
            "payload_base64": "dg==",
            "headers_base64": { "trace-id": "dHJhY2U=" }
        }))
        .unwrap();
        assert_eq!(
            legacy.metadata_base64.get("trace-id").map(String::as_str),
            Some("dHJhY2U=")
        );
    }

    #[test]
    fn event_time_supports_nanoseconds_and_converts_legacy_milliseconds() {
        assert_eq!(
            resolve_event_time_ns(Some(1_700_000_000_123_456_789), None, 5).unwrap(),
            1_700_000_000_123_456_789
        );
        assert_eq!(
            resolve_event_time_ns(None, Some(1_700_000_000_123), 5).unwrap(),
            1_700_000_000_123_000_000
        );
        assert_eq!(resolve_event_time_ns(None, None, 5).unwrap(), 5);
        assert!(resolve_event_time_ns(Some(1), Some(1), 5).is_err());
    }

    #[test]
    fn record_response_serializes_metadata_not_headers() {
        let response = record_response(CursorRecord {
            cursor: "cursor".to_owned(),
            record: StoredRecord {
                message_id: Uuid::new_v4(),
                producer_id: Uuid::new_v4(),
                producer_sequence: 1,
                event_time_ns: 1_000_000_001,
                ingest_time_ns: 1_000_000_002,
                key: b"key".to_vec(),
                payload: b"value".to_vec(),
                metadata: BTreeMap::from([("trace-id".to_owned(), b"trace".to_vec())]),
            },
        });
        let json = serde_json::to_value(response).unwrap();
        assert!(json.get("metadata_base64").is_some());
        assert!(json.get("headers_base64").is_none());
        assert_eq!(json["event_time_ns"], "1000000001");
        assert_eq!(json["ingest_time_ns"], "1000000002");
        assert!(json.get("timestamp_ms").is_none());
    }

    #[test]
    fn new_feed_cursors_are_stable_within_one_feed_and_distinct_between_feeds() {
        let request_id = Uuid::from_u128(5);
        let first = Uuid::from_u128(1);
        let second = Uuid::from_u128(2);
        assert_eq!(
            feed_cursor(first, request_id),
            feed_cursor(first, request_id)
        );
        assert_ne!(
            feed_cursor(first, request_id),
            feed_cursor(second, request_id)
        );
        assert_ne!(
            feed_cursor(first, request_id),
            URL_SAFE_NO_PAD.encode(blake3::hash(request_id.as_bytes()).as_bytes())
        );
    }

    #[test]
    fn split_freeze_order_drains_moved_owner_before_other_replicas() {
        let nodes = ["storage-1", "storage-2", "storage-3"]
            .map(|name| StorageNodeId::try_new(name).unwrap());
        let assignment = ActiveRangeAssignment::try_new(
            Uuid::from_u128(81),
            RangeId::from_uuid(Uuid::from_u128(82)),
            RangeGeneration::new(1),
            nodes[1].clone(),
            ReplicaSet::try_new(nodes.clone()).unwrap(),
            OwnershipEpoch::new(2),
        )
        .unwrap();
        assert_eq!(
            split_freeze_order(&assignment),
            vec![nodes[1].clone(), nodes[0].clone(), nodes[2].clone()]
        );
    }

    #[test]
    fn read_quorum_refuses_a_lost_or_behind_owner_and_digest_disagreement() {
        let nodes = ["storage-1", "storage-2", "storage-3"]
            .map(|name| StorageNodeId::try_new(name).unwrap());
        let assignment = ActiveRangeAssignment::try_new(
            Uuid::from_u128(1),
            RangeId::from_uuid(Uuid::from_u128(2)),
            RangeGeneration::new(1),
            nodes[0].clone(),
            ReplicaSet::try_new(nodes.clone()).unwrap(),
            OwnershipEpoch::new(1),
        )
        .unwrap();
        let evidence =
            |node: StorageNodeId, committed: u64, digest: Option<[u8; 32]>| ReadReplicaEvidence {
                node,
                committed: CommitPosition::new(committed),
                digest,
            };
        let owner = evidence(nodes[0].clone(), 5, Some([7; 32]));
        let caught_up = evidence(nodes[1].clone(), 5, Some([7; 32]));
        assert!(require_read_quorum(
            &assignment,
            CommitPosition::new(5),
            &owner,
            std::slice::from_ref(&caught_up)
        )
        .is_ok());
        let lost = evidence(nodes[0].clone(), 0, None);
        let ahead = evidence(nodes[2].clone(), 5, Some([7; 32]));
        assert!(require_read_quorum(
            &assignment,
            CommitPosition::new(0),
            &lost,
            &[caught_up.clone(), ahead]
        )
        .is_err());
        let ahead = evidence(nodes[2].clone(), 6, Some([8; 32]));
        assert!(require_read_quorum(
            &assignment,
            CommitPosition::new(5),
            &owner,
            &[caught_up.clone(), ahead]
        )
        .is_err());
        let divergent = evidence(nodes[1].clone(), 5, Some([9; 32]));
        let behind = evidence(nodes[2].clone(), 4, None);
        assert!(require_read_quorum(
            &assignment,
            CommitPosition::new(5),
            &owner,
            &[divergent, behind]
        )
        .is_err());
        assert!(require_read_quorum(&assignment, CommitPosition::new(5), &owner, &[]).is_err());
    }

    #[test]
    fn reader_frontier_tokens_are_stable_and_scoped_to_reader_session() {
        let reader = Uuid::from_u128(4);
        let request = Uuid::from_u128(5);
        let token = reader_frontier_cursor(reader, 1, request);
        assert!(token.starts_with("rf1_"));
        assert_eq!(token, reader_frontier_cursor(reader, 1, request));
        assert_ne!(token, reader_frontier_cursor(reader, 2, request));
        assert_ne!(
            token,
            reader_frontier_cursor(Uuid::from_u128(6), 1, request)
        );
    }

    #[tokio::test]
    async fn repeated_fetch_request_cannot_return_a_different_frontier() {
        let directory = TempDir::new().unwrap();
        let store: Arc<dyn LogStore> =
            Arc::new(FileLogStore::open(directory.path().join("data")).unwrap());
        let control = ControlController::open_with_storage_nodes(
            directory.path().join("catalog.json"),
            store,
            ["storage-1", "storage-2", "storage-3"]
                .into_iter()
                .map(|node| StorageNodeId::try_new(node).unwrap())
                .collect(),
        )
        .unwrap();
        control.execute("CREATE SPACE orders; CREATE FEED orders.events; CREATE READER audit FROM orders.events START AT BEGINNING;").await.unwrap();
        let reader = control.active_reader_by_name("audit").await.unwrap();
        let assignment = control
            .active_range_assignment(reader.feed_id)
            .await
            .unwrap();
        control
            .execute_commands(vec![Command::OpenReaderSession {
                reader: "audit".to_owned(),
                capacity: 2,
            }])
            .await
            .unwrap();
        let request_id = Uuid::from_u128(7_100);
        let first = BTreeMap::from([(assignment.range_id, "event-1".to_owned())]);
        let retry = BTreeMap::from([(assignment.range_id, "event-2".to_owned())]);
        let delivery = |positions: BTreeMap<RangeId, String>| Command::RecordReaderFrontier {
            reader: "audit".to_owned(),
            session_epoch: 1,
            cursor: "rf1_same-request".to_owned(),
            positions,
            expected_cursor: None,
            fence_delivery: true,
            fetch_request_id: Some(request_id),
        };
        let applied = control
            .execute_commands_with_request_id(vec![delivery(first.clone())], request_id)
            .await
            .unwrap();
        assert!(
            verify_reader_frontier_result(&applied, "rf1_same-request", &first, request_id).is_ok()
        );
        let repeated = control
            .execute_commands_with_request_id(vec![delivery(retry.clone())], request_id)
            .await
            .unwrap();
        assert_eq!(
            verify_reader_frontier_result(&repeated, "rf1_same-request", &retry, request_id)
                .unwrap_err()
                .status,
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            control
                .active_reader_frontier(reader.reader_id)
                .await
                .unwrap()
                .delivered,
            first
        );
    }

    #[test]
    fn complete_feed_merge_orders_remote_frames_and_continues_from_opaque_cursor() {
        let frame = |index: u64, ingest: i64| StoredRangeFrame {
            position: RangePosition::new(index),
            identity: AppendIdentity {
                writer_session_id: Uuid::from_u128(1),
                writer_epoch: 1,
                sequence: index,
            },
            cursor: format!("cursor-{index}"),
            frame: encode_record(&StoredRecord {
                message_id: Uuid::from_u128(index as u128),
                producer_id: Uuid::from_u128(1),
                producer_sequence: index,
                event_time_ns: ingest,
                ingest_time_ns: ingest,
                key: b"user".to_vec(),
                payload: Vec::new(),
                metadata: BTreeMap::new(),
            })
            .unwrap(),
        };
        let frames = vec![frame(3, 3), frame(1, 1), frame(2, 2)];
        let resumed = merge_feed_frames(frames.clone(), Some("cursor-1"), 2).unwrap();
        assert_eq!(
            resumed
                .iter()
                .map(|frame| frame.cursor.as_str())
                .collect::<Vec<_>>(),
            vec!["cursor-2", "cursor-3"]
        );
        assert_eq!(
            merge_feed_frames(frames, Some("missing"), 2)
                .unwrap_err()
                .status,
            StatusCode::BAD_REQUEST
        );
    }

    #[tokio::test]
    async fn append_owner_waits_for_committed_feed_metadata_to_apply() {
        let directory = TempDir::new().unwrap();
        let store: Arc<dyn LogStore> =
            Arc::new(FileLogStore::open(directory.path().join("data")).unwrap());
        let control = Arc::new(
            ControlController::open_with_storage_nodes(
                directory.path().join("catalog.json"),
                store,
                ["storage-1", "storage-2", "storage-3"]
                    .into_iter()
                    .map(|node| StorageNodeId::try_new(node).unwrap())
                    .collect(),
            )
            .unwrap(),
        );
        assert!(feed_when_applied(&control, "orders.events", false)
            .await
            .is_none());
        let delayed = control.clone();
        let applied = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            delayed
                .execute_commands(vec![
                    Command::CreateSpace {
                        name: "orders".to_owned(),
                    },
                    Command::CreateFeed {
                        name: "orders.events".to_owned(),
                    },
                ])
                .await
                .unwrap();
        });
        let feed = feed_when_applied(&control, "orders.events", true)
            .await
            .unwrap();
        assert_eq!(feed.name, "orders.events");
        applied.await.unwrap();
    }

    #[tokio::test]
    async fn writer_lookup_waits_for_committed_metadata_to_apply_on_ingress() {
        let directory = TempDir::new().unwrap();
        let store: Arc<dyn LogStore> =
            Arc::new(FileLogStore::open(directory.path().join("data")).unwrap());
        let control = Arc::new(
            ControlController::open_with_storage_nodes(
                directory.path().join("catalog.json"),
                store,
                ["storage-1", "storage-2", "storage-3"]
                    .into_iter()
                    .map(|node| StorageNodeId::try_new(node).unwrap())
                    .collect(),
            )
            .unwrap(),
        );
        assert!(writer_when_applied(&control, "orders.writer", false)
            .await
            .is_none());
        let delayed = control.clone();
        let applied = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            delayed
                .execute_commands(vec![
                    Command::CreateSpace {
                        name: "orders".to_owned(),
                    },
                    Command::CreateFeed {
                        name: "orders.events".to_owned(),
                    },
                    Command::CreateWriter {
                        name: "orders.writer".to_owned(),
                        feed: "orders.events".to_owned(),
                    },
                ])
                .await
                .unwrap();
        });
        let writer = writer_when_applied(&control, "orders.writer", true)
            .await
            .unwrap();
        assert_eq!(writer.name, "orders.writer");
        applied.await.unwrap();
    }

    #[tokio::test]
    async fn single_range_read_continues_beyond_ten_thousand_and_reports_unsupported_full_scan() {
        let directory = TempDir::new().unwrap();
        let store: Arc<dyn LogStore> =
            Arc::new(FileLogStore::open(directory.path().join("data")).unwrap());
        let control = Arc::new(
            ControlController::open_with_storage_nodes(
                directory.path().join("catalog.json"),
                store.clone(),
                ["storage-1", "storage-2", "storage-3"]
                    .into_iter()
                    .map(|node| StorageNodeId::try_new(node).unwrap())
                    .collect(),
            )
            .unwrap(),
        );
        let created = control
            .execute_commands(vec![
                Command::CreateSpace {
                    name: "orders".to_owned(),
                },
                Command::CreateFeed {
                    name: "orders.events".to_owned(),
                },
            ])
            .await
            .unwrap();
        let feed_id = serde_json::from_value(created.results[1].data["feed_id"].clone()).unwrap();
        let assignment = control.active_range_assignment(feed_id).await.unwrap();
        let root = directory.path().join("ranges");
        seed_committed_history(
            &root,
            &ActiveRangeDescriptor {
                feed_id,
                range_id: assignment.range_id,
                generation: assignment.generation,
                ownership_epoch: assignment.ownership_epoch,
            },
            10_001,
        );
        let membership = Arc::new(MembershipService::new(
            MemberAnnouncement {
                node_id: "test-node".to_owned(),
                api_url: "http://test-node:7070".to_owned(),
                capacity: 100,
            },
            vec![],
            Duration::from_secs(1),
            Duration::from_secs(5),
        ));
        let state = AppState {
            store,
            membership,
            demand: DemandMetrics::default(),
            autoscaler: Arc::new(Mutex::new(AutoscaleController::default())),
            replica_append: Some(Arc::new(ReplicaAppendService::new(
                root,
                assignment.owner.clone(),
                control.clone(),
            ))),
            control: control.clone(),
            control_plane: None,
            progress_authority: control.clone(),
            majority_append: None,
            subscription_progress: None,
            effect_journal: None,
            subscription_mtls_enabled: false,
            storage_node_id: Some(assignment.owner),
            control_endpoints: Arc::new(BTreeMap::new().into()),
            internal_key: None,
            internal_http: reqwest::Client::new(),
            internal_mtls: None,
            admin_auth: AdminAuthenticator::new(Some(
                "this-is-a-long-development-api-key".to_owned(),
            ))
            .unwrap(),
        };
        let first = read_complete_feed(&state, feed_id, "orders.events", None, 100, false, false)
            .await
            .unwrap();
        assert_eq!(first.len(), 100);
        assert_eq!(first[0].cursor, "cursor-1");
        assert_eq!(first[99].cursor, "cursor-100");
        let continued = read_complete_feed(
            &state,
            feed_id,
            "orders.events",
            Some("cursor-10000"),
            100,
            false,
            false,
        )
        .await
        .unwrap();
        assert_eq!(continued.len(), 1);
        assert_eq!(continued[0].cursor, "cursor-10001");
        let tail = read_complete_feed(&state, feed_id, "orders.events", None, 2, true, false)
            .await
            .unwrap();
        assert_eq!(
            tail.iter()
                .map(|frame| frame.cursor.as_str())
                .collect::<Vec<_>>(),
            vec!["cursor-10000", "cursor-10001"]
        );
        assert_eq!(
            read_complete_feed(
                &state,
                feed_id,
                "orders.events",
                Some("unknown"),
                1,
                false,
                false
            )
            .await
            .unwrap_err()
            .status,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            read_complete_feed(&state, feed_id, "orders.events", None, 10_000, false, true)
                .await
                .unwrap_err()
                .status,
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    fn admin_test_router(directory: &TempDir) -> Router {
        let store: Arc<dyn LogStore> =
            Arc::new(FileLogStore::open(directory.path().join("data")).unwrap());
        let control = Arc::new(
            ControlController::open_with_storage_nodes(
                directory.path().join("catalog.json"),
                store.clone(),
                ["storage-1", "storage-2", "storage-3"]
                    .into_iter()
                    .map(|node| crate::active_range::StorageNodeId::try_new(node).unwrap())
                    .collect(),
            )
            .unwrap(),
        );
        let membership = Arc::new(MembershipService::new(
            MemberAnnouncement {
                node_id: "test-node".to_owned(),
                api_url: "http://test-node:7070".to_owned(),
                capacity: 100,
            },
            vec![],
            Duration::from_secs(1),
            Duration::from_secs(5),
        ));
        router(AppState {
            store,
            membership,
            demand: DemandMetrics::default(),
            autoscaler: Arc::new(Mutex::new(AutoscaleController::default())),
            control: control.clone(),
            control_plane: None,
            progress_authority: control.clone(),
            replica_append: None,
            majority_append: None,
            subscription_progress: None,
            effect_journal: None,
            subscription_mtls_enabled: false,
            storage_node_id: None,
            control_endpoints: Arc::new(BTreeMap::new().into()),
            internal_key: None,
            internal_http: reqwest::Client::new(),
            internal_mtls: None,
            admin_auth: AdminAuthenticator::new(Some(
                "this-is-a-long-development-api-key".to_owned(),
            ))
            .unwrap(),
        })
    }

    async fn internal_replica_test_state(
        directory: &TempDir,
    ) -> (
        AppState,
        Arc<ControlPlane>,
        Arc<ControlController>,
        Arc<crate::reader::FjallSubscriptionProgressReplica>,
    ) {
        let store: Arc<dyn LogStore> =
            Arc::new(FileLogStore::open(directory.path().join("data")).unwrap());
        let control = Arc::new(
            ControlController::open_with_storage_nodes(
                directory.path().join("catalog.json"),
                store.clone(),
                ["control-1", "control-2", "control-3"]
                    .into_iter()
                    .map(|node| crate::active_range::StorageNodeId::try_new(node).unwrap())
                    .collect(),
            )
            .unwrap(),
        );
        control.execute("CREATE SPACE orders; CREATE FEED orders.events; CREATE SUBSCRIPTION orders.billing FROM orders.events;").await.unwrap();
        let subscription_replica = Arc::new(
            crate::reader::FjallSubscriptionProgressReplica::open(
                directory.path().join("subscription-progress"),
            )
            .unwrap(),
        );
        let peers = [1_u64, 2, 3]
            .into_iter()
            .map(|node| (node, BasicNode::new(format!("127.0.0.1:{}", 9000 + node))))
            .collect::<BTreeMap<_, _>>();
        let control_plane = Arc::new(
            ControlPlane::start(
                1,
                peers,
                "this-is-a-long-control-plane-key".to_owned(),
                directory.path().join("raft.json"),
                control.clone(),
                None,
            )
            .await
            .unwrap(),
        );
        let membership = Arc::new(MembershipService::new(
            MemberAnnouncement {
                node_id: "test-node".to_owned(),
                api_url: "http://test-node:7070".to_owned(),
                capacity: 100,
            },
            vec![],
            Duration::from_secs(1),
            Duration::from_secs(5),
        ));
        let replica_append = Arc::new(ReplicaAppendService::new(
            directory.path().join("active-ranges"),
            crate::active_range::StorageNodeId::try_new("control-1").unwrap(),
            control.clone(),
        ));
        let subscription_progress = Arc::new(SubscriptionProgressReplicaService::new(
            crate::active_range::StorageNodeId::try_new("control-1").unwrap(),
            control.clone(),
            subscription_replica.clone(),
        ));
        (
            AppState {
                store,
                membership,
                demand: DemandMetrics::default(),
                autoscaler: Arc::new(Mutex::new(AutoscaleController::default())),
                control: control.clone(),
                control_plane: Some(control_plane.clone()),
                progress_authority: control.clone(),
                replica_append: Some(replica_append),
                majority_append: None,
                subscription_progress: Some(subscription_progress),
                effect_journal: Some(Arc::new(crate::effect::EffectJournalService::new(
                    crate::active_range::StorageNodeId::try_new("control-1").unwrap(),
                    control.clone(),
                    Arc::new(
                        crate::effect::FjallEffectJournalReplica::open(
                            directory.path().join("effect-journal"),
                        )
                        .unwrap(),
                    ),
                ))),
                subscription_mtls_enabled: false,
                storage_node_id: Some(
                    crate::active_range::StorageNodeId::try_new("control-1").unwrap(),
                ),
                control_endpoints: Arc::new(BTreeMap::new().into()),
                internal_key: Some("this-is-a-long-control-plane-key".to_owned()),
                internal_http: reqwest::Client::new(),
                internal_mtls: None,
                admin_auth: AdminAuthenticator::new(Some(
                    "this-is-a-long-development-api-key".to_owned(),
                ))
                .unwrap(),
            },
            control_plane,
            control,
            subscription_replica,
        )
    }

    async fn internal_replica_test_router(
        directory: &TempDir,
    ) -> (
        Router,
        Arc<ControlPlane>,
        Arc<ControlController>,
        Arc<crate::reader::FjallSubscriptionProgressReplica>,
    ) {
        let (state, plane, control, replica) = internal_replica_test_state(directory).await;
        (router(state), plane, control, replica)
    }

    fn admin_request(path: &str, body: Value, api_key: Option<&str>) -> Request<Body> {
        let mut request = Request::builder()
            .method("POST")
            .uri(path)
            .header("content-type", "application/json");
        if let Some(api_key) = api_key {
            request = request.header("authorization", format!("Bearer {api_key}"));
        }
        request
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap()
    }

    #[tokio::test]
    async fn replica_append_authentication_runs_before_json_body_decoding() {
        let directory = TempDir::new().unwrap();
        let (app, control_plane, _, _) = internal_replica_test_router(&directory).await;
        let request = |credential: Option<&str>| {
            let mut request = Request::builder()
                .method("POST")
                .uri("/internal/active-range/replica/append")
                .header("content-type", "application/json");
            if let Some(credential) = credential {
                request = request.header("x-whitewater-control-key", credential);
            }
            request.body(Body::from("not-json")).unwrap()
        };

        let missing = app.clone().oneshot(request(None)).await.unwrap();
        assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);
        let wrong = app
            .clone()
            .oneshot(request(Some("wrong-key")))
            .await
            .unwrap();
        assert_eq!(wrong.status(), StatusCode::UNAUTHORIZED);
        let authenticated = app
            .oneshot(request(Some("this-is-a-long-control-plane-key")))
            .await
            .unwrap();
        assert_ne!(authenticated.status(), StatusCode::UNAUTHORIZED);
        assert!(authenticated.status().is_client_error());
        control_plane.raft().shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn subscription_progress_internal_routes_authenticate_and_fence_placement() {
        let directory = TempDir::new().unwrap();
        let (app, control_plane, control, replica) = internal_replica_test_router(&directory).await;
        let subscription = control
            .active_subscription_by_name("orders.billing")
            .await
            .unwrap();
        let assignment = control
            .active_subscription_progress_assignment_by_id(subscription.subscription_id)
            .await
            .unwrap();
        let local = crate::active_range::StorageNodeId::try_new("control-1").unwrap();
        let follower = assignment
            .replicas
            .iter()
            .find(|node| *node != &assignment.owner)
            .unwrap()
            .clone();
        let mutation = crate::reader::SubscriptionProgressMutation {
            subscription_id: subscription.subscription_id,
            feed_id: subscription.feed_id,
            ownership_epoch: assignment.ownership_epoch,
            sequence: 1,
            request_id: Uuid::from_u128(877),
            expected_cursor: None,
            cursor: "rf1_test".to_owned(),
            positions: BTreeMap::from([(
                RangeId::from_uuid(Uuid::from_u128(878)),
                "event-1".to_owned(),
            )]),

            tick: 0,
            lease_ops: Vec::new(),
        };
        let prepare = SubscriptionPrepareRequest {
            owner: assignment.owner.clone(),
            receiver: local.clone(),
            mutation: mutation.clone(),
        };
        let request = |path: &str, body: serde_json::Value, key: Option<&str>| {
            let mut builder = Request::builder()
                .method("POST")
                .uri(path)
                .header("content-type", "application/json");
            if let Some(key) = key {
                builder = builder.header("x-whitewater-control-key", key);
            }
            builder
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap()
        };
        let path = "/internal/subscription-progress/prepare";
        let missing = app
            .clone()
            .oneshot(request(path, json!(prepare), None))
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);
        let mut wrong = prepare.clone();
        wrong.receiver = assignment
            .replicas
            .iter()
            .find(|node| *node != &local)
            .unwrap()
            .clone();
        let rejected = app
            .clone()
            .oneshot(request(
                path,
                json!(wrong),
                Some("this-is-a-long-control-plane-key"),
            ))
            .await
            .unwrap();
        assert_eq!(rejected.status(), StatusCode::CONFLICT);
        assert!(replica
            .local_committed(subscription.subscription_id)
            .unwrap()
            .is_none());
        let accepted = app
            .clone()
            .oneshot(request(
                path,
                json!(prepare),
                Some("this-is-a-long-control-plane-key"),
            ))
            .await
            .unwrap();
        assert_eq!(accepted.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(accepted.into_body(), 1024)
            .await
            .unwrap();
        let vote: crate::reader::SubscriptionReplicaReply<crate::reader::SubscriptionPrepareVote> =
            serde_json::from_slice(&bytes).unwrap();
        assert_eq!(vote.replica, local);
        assert!(replica
            .local_committed(subscription.subscription_id)
            .unwrap()
            .is_none());
        let evidence = json!({"votes": [[assignment.owner, vote.result.digest], [follower, vote.result.digest]],
            "subscription_id": subscription.subscription_id, "request_id": mutation.request_id});
        let commit = json!({"owner": assignment.owner, "receiver": local,
            "subscription_id": subscription.subscription_id,
            "ownership_epoch": assignment.ownership_epoch, "evidence": evidence});
        let result = app
            .oneshot(request(
                "/internal/subscription-progress/commit",
                commit,
                Some("this-is-a-long-control-plane-key"),
            ))
            .await
            .unwrap();
        assert_eq!(result.status(), StatusCode::OK);
        assert_eq!(
            replica
                .local_committed(subscription.subscription_id)
                .unwrap(),
            Some(mutation)
        );
        control_plane.raft().shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn subscription_progress_http_transport_targets_authoritative_replica() {
        let directory = TempDir::new().unwrap();
        let (app, control_plane, control, replica) = internal_replica_test_router(&directory).await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let subscription = control
            .active_subscription_by_name("orders.billing")
            .await
            .unwrap();
        let assignment = control
            .active_subscription_progress_assignment_by_id(subscription.subscription_id)
            .await
            .unwrap();
        let local = crate::active_range::StorageNodeId::try_new("control-1").unwrap();
        let endpoints = BTreeMap::from([(local.clone(), endpoint)]);
        let transport = crate::reader::HttpSubscriptionProgressTransport::new(
            assignment.clone(),
            endpoints.clone(),
            "this-is-a-long-control-plane-key".to_owned(),
            Duration::from_secs(2),
        )
        .unwrap();
        let mutation = crate::reader::SubscriptionProgressMutation {
            subscription_id: subscription.subscription_id,
            feed_id: subscription.feed_id,
            ownership_epoch: assignment.ownership_epoch,
            sequence: 1,
            request_id: Uuid::from_u128(900),
            expected_cursor: None,
            cursor: "rf1_page".to_owned(),
            positions: BTreeMap::from([(
                RangeId::from_uuid(Uuid::from_u128(901)),
                "record".to_owned(),
            )]),

            tick: 0,
            lease_ops: Vec::new(),
        };
        let vote = crate::reader::SubscriptionProgressTransport::prepare(
            &transport,
            &local,
            mutation.clone(),
        )
        .await
        .unwrap();
        assert_eq!(vote.replica, local);
        assert_eq!(vote.result.request_id, mutation.request_id);
        assert!(replica
            .local_committed(subscription.subscription_id)
            .unwrap()
            .is_none());
        assert!(crate::reader::SubscriptionProgressTransport::committed(
            &transport,
            &local,
            subscription.subscription_id,
            assignment.ownership_epoch,
        )
        .await
        .unwrap()
        .result
        .is_none());
        let other = assignment
            .replicas
            .iter()
            .find(|node| *node != &assignment.owner)
            .unwrap()
            .clone();
        let evidence: crate::reader::SubscriptionCommitEvidence = serde_json::from_value(json!({
            "votes": [[assignment.owner, vote.result.digest], [other, vote.result.digest]],
            "subscription_id": subscription.subscription_id, "request_id": mutation.request_id,
        }))
        .unwrap();
        let committed =
            crate::reader::SubscriptionProgressTransport::commit(&transport, &local, evidence)
                .await
                .unwrap();
        assert_eq!(committed.replica, local);
        assert_eq!(committed.ownership_epoch, assignment.ownership_epoch);
        assert_eq!(committed.result, mutation);
        assert_eq!(
            replica
                .local_committed(subscription.subscription_id)
                .unwrap(),
            Some(mutation.clone())
        );
        assert_eq!(
            crate::reader::SubscriptionProgressTransport::committed(
                &transport,
                &local,
                subscription.subscription_id,
                assignment.ownership_epoch,
            )
            .await
            .unwrap()
            .result,
            Some(mutation.clone())
        );
        let wrong_key = crate::reader::HttpSubscriptionProgressTransport::new(
            assignment,
            endpoints,
            "not-the-control-key".to_owned(),
            Duration::from_secs(2),
        )
        .unwrap();
        assert!(matches!(
            crate::reader::SubscriptionProgressTransport::prepare(&wrong_key, &local, mutation)
                .await,
            Err(SubscriptionProgressError::Unavailable)
        ));
        assert!(matches!(
            crate::reader::SubscriptionProgressTransport::committed(
                &wrong_key,
                &local,
                subscription.subscription_id,
                1,
            )
            .await,
            Err(SubscriptionProgressError::Unavailable)
        ));
        server.abort();
        control_plane.raft().shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn subscription_mtls_refuses_downgrade_unpinned_peers_and_wrong_owner() {
        let directory = TempDir::new().unwrap();
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let mut ca_params =
            rcgen::CertificateParams::new(vec!["riverbed-test-ca".to_owned()]).unwrap();
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        ca_params.key_usages = vec![rcgen::KeyUsagePurpose::KeyCertSign];
        let ca = ca_params.self_signed(&ca_key).unwrap();
        let nodes = ["control-1", "control-2", "control-3"]
            .map(|name| StorageNodeId::try_new(name).unwrap());
        let mut pins = BTreeMap::new();
        let mut certificates = BTreeMap::new();
        for node in &nodes {
            let key = rcgen::KeyPair::generate().unwrap();
            let mut params = rcgen::CertificateParams::new(vec![node.as_str().to_owned()]).unwrap();
            params.extended_key_usages = vec![
                rcgen::ExtendedKeyUsagePurpose::ClientAuth,
                rcgen::ExtendedKeyUsagePurpose::ServerAuth,
            ];
            let cert = params.signed_by(&key, &ca, &ca_key).unwrap();
            pins.insert(
                node.clone(),
                std::collections::BTreeSet::from([*blake3::hash(cert.der().as_ref()).as_bytes()]),
            );
            certificates.insert(
                node.clone(),
                format!("{}{}", cert.pem(), key.serialize_pem()),
            );
            if node == &nodes[0] {
                tokio::fs::write(directory.path().join("node.pem"), cert.pem())
                    .await
                    .unwrap();
                tokio::fs::write(directory.path().join("node.key"), key.serialize_pem())
                    .await
                    .unwrap();
            }
        }
        tokio::fs::write(directory.path().join("ca.pem"), ca.pem())
            .await
            .unwrap();
        let config = SubscriptionMtlsConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            cert_path: directory.path().join("node.pem"),
            key_path: directory.path().join("node.key"),
            ca_path: directory.path().join("ca.pem"),
            peer_pins: pins,
            peer_endpoints: BTreeMap::new(),
        };
        let mut wrong_config = config.clone();
        wrong_config.peer_pins.insert(
            nodes[0].clone(),
            std::collections::BTreeSet::from([*blake3::hash(b"wrong").as_bytes()]),
        );
        assert!(SubscriptionTlsServer::from_config(&wrong_config, &nodes[0])
            .await
            .is_err());
        let server = SubscriptionTlsServer::from_config(&config, &nodes[0])
            .await
            .unwrap();
        let (mut state, control_plane, control, replica) =
            internal_replica_test_state(&directory).await;
        state.subscription_mtls_enabled = true;
        let public = router(state.clone());
        let unauthenticated = public
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/internal/subscription-progress/prepare")
                    .header(
                        "x-whitewater-control-key",
                        "this-is-a-long-control-plane-key",
                    )
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unauthenticated.status(), StatusCode::NOT_FOUND);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let tls_task =
            tokio::spawn(server.serve(listener, internal_mtls_router(state.clone()), shutdown_rx));
        let endpoint = format!(
            "https://control-1:{}/internal/subscription-progress",
            address.port()
        );
        let make_client = |identity: Option<&str>| {
            let mut builder = reqwest::Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(3))
                .tls_built_in_root_certs(false)
                .add_root_certificate(reqwest::Certificate::from_pem(ca.pem().as_bytes()).unwrap())
                .resolve("control-1", address);
            if let Some(identity) = identity {
                builder =
                    builder.identity(reqwest::Identity::from_pem(identity.as_bytes()).unwrap());
            }
            builder.build().unwrap()
        };
        let subscription = control
            .active_subscription_by_name("orders.billing")
            .await
            .unwrap();
        let assignment = control
            .active_subscription_progress_assignment_by_id(subscription.subscription_id)
            .await
            .unwrap();
        let mutation = crate::reader::SubscriptionProgressMutation {
            subscription_id: subscription.subscription_id,
            feed_id: subscription.feed_id,
            ownership_epoch: assignment.ownership_epoch,
            sequence: 1,
            request_id: Uuid::from_u128(960),
            expected_cursor: None,
            cursor: "rf1_page".to_owned(),
            positions: BTreeMap::from([(
                RangeId::from_uuid(Uuid::from_u128(961)),
                "record".to_owned(),
            )]),

            tick: 0,
            lease_ops: Vec::new(),
        };
        let request = SubscriptionPrepareRequest {
            owner: assignment.owner.clone(),
            receiver: nodes[0].clone(),
            mutation: mutation.clone(),
        };
        let no_peer = internal_mtls_router(state.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/internal/subscription-progress/prepare")
                    .header(
                        "x-whitewater-control-key",
                        "this-is-a-long-control-plane-key",
                    )
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&request).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(no_peer.status(), StatusCode::UNAUTHORIZED);
        assert!(matches!(
            crate::reader::HttpSubscriptionProgressTransport::new_mtls(
                assignment.clone(),
                BTreeMap::from([(nodes[0].clone(), "http://control-1:7070".to_owned())]),
                ca.pem().as_bytes(),
                certificates[&assignment.owner].as_bytes(),
                Duration::from_secs(2),
            ),
            Err(SubscriptionProgressError::InvalidAssignment)
        ));
        assert!(crate::reader::HttpSubscriptionProgressTransport::new_mtls(
            assignment.clone(),
            BTreeMap::from([(nodes[0].clone(), "https://control-1:7070".to_owned())]),
            ca.pem().as_bytes(),
            certificates[&assignment.owner].as_bytes(),
            Duration::from_secs(2),
        )
        .is_ok());
        assert!(make_client(None)
            .post(format!("{endpoint}/prepare"))
            .json(&request)
            .send()
            .await
            .is_err());
        let other = nodes
            .iter()
            .find(|node| *node != &assignment.owner)
            .unwrap();
        let other_client = make_client(Some(&certificates[other]));
        let owner_client = make_client(Some(&certificates[&assignment.owner]));
        // A request claiming a different owner is rejected by placement checks
        // even when the caller presents the real owner's certificate.
        let mut claimed_other = request.clone();
        claimed_other.owner = other.clone();
        assert_eq!(
            owner_client
                .post(format!("{endpoint}/prepare"))
                .json(&claimed_other)
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::CONFLICT
        );
        let rogue_key = rcgen::KeyPair::generate().unwrap();
        let rogue_cert = rcgen::CertificateParams::new(vec!["rogue-node".to_owned()])
            .unwrap()
            .signed_by(&rogue_key, &ca, &ca_key)
            .unwrap();
        let rogue_identity = format!("{}{}", rogue_cert.pem(), rogue_key.serialize_pem());
        assert!(make_client(Some(&rogue_identity))
            .post(format!("{endpoint}/prepare"))
            .json(&request)
            .send()
            .await
            .is_err());
        // Member requests are topology-free: any pinned Node may coordinate
        // progress mutations on the assigned replicas.
        let accepted = other_client
            .post(format!("{endpoint}/prepare"))
            .json(&request)
            .send()
            .await
            .unwrap();
        assert_eq!(accepted.status(), StatusCode::OK);
        let vote: crate::reader::SubscriptionReplicaReply<crate::reader::SubscriptionPrepareVote> =
            accepted.json().await.unwrap();
        assert_eq!(vote.replica, nodes[0]);
        assert_eq!(vote.result.request_id, mutation.request_id);
        assert!(replica
            .local_committed(subscription.subscription_id)
            .unwrap()
            .is_none());
        let read = SubscriptionCommittedReadRequest {
            owner: assignment.owner.clone(),
            receiver: nodes[0].clone(),
            subscription_id: subscription.subscription_id,
            ownership_epoch: assignment.ownership_epoch,
        };
        let uncommitted: crate::reader::SubscriptionReplicaReply<
            Option<crate::reader::SubscriptionProgressMutation>,
        > = other_client
            .post(format!("{endpoint}/committed"))
            .json(&read)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(uncommitted.result.is_none());
        let evidence = json!({
            "votes": [[assignment.owner, vote.result.digest], [other, vote.result.digest]],
            "subscription_id": subscription.subscription_id, "request_id": mutation.request_id,
        });
        let commit = json!({"owner": assignment.owner, "receiver": nodes[0],
            "subscription_id": subscription.subscription_id,
            "ownership_epoch": assignment.ownership_epoch, "evidence": evidence});
        let committed: crate::reader::SubscriptionReplicaReply<
            crate::reader::SubscriptionProgressMutation,
        > = other_client
            .post(format!("{endpoint}/commit"))
            .json(&commit)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(committed.result, mutation);
        let observed: crate::reader::SubscriptionReplicaReply<
            Option<crate::reader::SubscriptionProgressMutation>,
        > = owner_client
            .post(format!("{endpoint}/committed"))
            .json(&read)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(observed.result, Some(mutation.clone()));
        let wrong_server = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(3))
            .tls_built_in_root_certs(false)
            .add_root_certificate(reqwest::Certificate::from_pem(ca.pem().as_bytes()).unwrap())
            .identity(
                reqwest::Identity::from_pem(certificates[&assignment.owner].as_bytes()).unwrap(),
            )
            .resolve("control-2", address)
            .build()
            .unwrap();
        assert!(wrong_server
            .post(format!(
                "https://control-2:{}/internal/subscription-progress/committed",
                address.port()
            ))
            .json(&read)
            .send()
            .await
            .is_err());
        shutdown_tx.send(true).unwrap();
        tls_task.await.unwrap().unwrap();
        let rotated_key = rcgen::KeyPair::generate().unwrap();
        let mut rotated_params =
            rcgen::CertificateParams::new(vec!["control-1".to_owned()]).unwrap();
        rotated_params.extended_key_usages = vec![
            rcgen::ExtendedKeyUsagePurpose::ClientAuth,
            rcgen::ExtendedKeyUsagePurpose::ServerAuth,
        ];
        let rotated = rotated_params
            .signed_by(&rotated_key, &ca, &ca_key)
            .unwrap();
        let mut rotating_config = config.clone();
        rotating_config
            .peer_pins
            .get_mut(&nodes[0])
            .unwrap()
            .insert(*blake3::hash(rotated.der().as_ref()).as_bytes());
        tokio::fs::write(&config.cert_path, rotated.pem())
            .await
            .unwrap();
        tokio::fs::write(&config.key_path, rotated_key.serialize_pem())
            .await
            .unwrap();
        let new_server = SubscriptionTlsServer::from_config(&rotating_config, &nodes[0])
            .await
            .unwrap();
        let new_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let new_address = new_listener.local_addr().unwrap();
        let (stop_tx, stop_rx) = watch::channel(false);
        let new_task =
            tokio::spawn(new_server.serve(new_listener, internal_mtls_router(state), stop_rx));
        let rotation_client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(3))
            .tls_built_in_root_certs(false)
            .add_root_certificate(reqwest::Certificate::from_pem(ca.pem().as_bytes()).unwrap())
            .identity(
                reqwest::Identity::from_pem(certificates[&assignment.owner].as_bytes()).unwrap(),
            )
            .resolve("control-1", new_address)
            .build()
            .unwrap();
        let after_rotation: crate::reader::SubscriptionReplicaReply<
            Option<crate::reader::SubscriptionProgressMutation>,
        > = rotation_client
            .post(format!(
                "https://control-1:{}/internal/subscription-progress/committed",
                new_address.port()
            ))
            .json(&read)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(after_rotation.result, Some(mutation));
        stop_tx.send(true).unwrap();
        new_task.await.unwrap().unwrap();
        control_plane.raft().shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn subscription_progress_transport_refuses_oversized_replica_response() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let app = Router::new().route(
            "/internal/subscription-progress/committed",
            post(|| async { "x".repeat(512 * 1024 + 1) }),
        );
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let nodes = ["storage-1", "storage-2", "storage-3"]
            .map(|name| StorageNodeId::try_new(name).unwrap());
        let assignment = crate::reader::SubscriptionProgressAssignment::try_new(
            Uuid::from_u128(951),
            nodes[0].clone(),
            ReplicaSet::try_new(nodes.clone()).unwrap(),
            1,
        )
        .unwrap();
        let transport = crate::reader::HttpSubscriptionProgressTransport::new(
            assignment,
            BTreeMap::from([(nodes[0].clone(), endpoint)]),
            "test-development-key".to_owned(),
            Duration::from_secs(2),
        )
        .unwrap();
        assert!(matches!(
            crate::reader::SubscriptionProgressTransport::committed(
                &transport,
                &nodes[0],
                Uuid::from_u128(951),
                1,
            )
            .await,
            Err(SubscriptionProgressError::TooLarge)
        ));
        server.abort();
    }

    #[tokio::test]
    async fn internal_mtls_plane_serves_internal_endpoints_only_to_pinned_peers() {
        let directory = TempDir::new().unwrap();
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let mut ca_params =
            rcgen::CertificateParams::new(vec!["riverbed-test-ca".to_owned()]).unwrap();
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        ca_params.key_usages = vec![rcgen::KeyUsagePurpose::KeyCertSign];
        let ca = ca_params.self_signed(&ca_key).unwrap();
        let nodes = ["control-1", "control-2", "control-3"]
            .map(|name| StorageNodeId::try_new(name).unwrap());
        let mut pins = BTreeMap::new();
        let mut certificates = BTreeMap::new();
        for node in &nodes {
            let key = rcgen::KeyPair::generate().unwrap();
            let mut params = rcgen::CertificateParams::new(vec![node.as_str().to_owned()]).unwrap();
            params.extended_key_usages = vec![
                rcgen::ExtendedKeyUsagePurpose::ClientAuth,
                rcgen::ExtendedKeyUsagePurpose::ServerAuth,
            ];
            let cert = params.signed_by(&key, &ca, &ca_key).unwrap();
            pins.insert(
                node.clone(),
                std::collections::BTreeSet::from([*blake3::hash(cert.der().as_ref()).as_bytes()]),
            );
            certificates.insert(
                node.clone(),
                format!("{}{}", cert.pem(), key.serialize_pem()),
            );
            if node == &nodes[0] {
                tokio::fs::write(directory.path().join("node.pem"), cert.pem())
                    .await
                    .unwrap();
                tokio::fs::write(directory.path().join("node.key"), key.serialize_pem())
                    .await
                    .unwrap();
            }
        }
        tokio::fs::write(directory.path().join("ca.pem"), ca.pem())
            .await
            .unwrap();
        let config = SubscriptionMtlsConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            cert_path: directory.path().join("node.pem"),
            key_path: directory.path().join("node.key"),
            ca_path: directory.path().join("ca.pem"),
            peer_pins: pins,
            peer_endpoints: BTreeMap::new(),
        };
        let server = SubscriptionTlsServer::from_config(&config, &nodes[0])
            .await
            .unwrap();
        let (mut state, control_plane, control, _replica) =
            internal_replica_test_state(&directory).await;
        state.subscription_mtls_enabled = true;
        let public = router(state.clone());
        for path in [
            "/internal/active-range/replica/append",
            "/internal/active-range/recovery/progress",
            "/internal/control-plane/raft/vote",
        ] {
            let response = public
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(path)
                        .header(
                            "x-whitewater-control-key",
                            "this-is-a-long-control-plane-key",
                        )
                        .header("content-type", "application/json")
                        .body(Body::from("{}"))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path} leaked");
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let tls_task =
            tokio::spawn(server.serve(listener, internal_mtls_router(state.clone()), shutdown_rx));
        let make_client = |identity: Option<&str>| {
            let mut builder = reqwest::Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(3))
                .tls_built_in_root_certs(false)
                .add_root_certificate(reqwest::Certificate::from_pem(ca.pem().as_bytes()).unwrap())
                .resolve("control-1", address);
            if let Some(identity) = identity {
                builder =
                    builder.identity(reqwest::Identity::from_pem(identity.as_bytes()).unwrap());
            }
            builder.build().unwrap()
        };
        let endpoint = format!("https://control-1:{}", address.port());
        assert!(make_client(None)
            .get(format!("{endpoint}/internal/active-range/pressure"))
            .header(
                "x-whitewater-control-key",
                "this-is-a-long-control-plane-key"
            )
            .send()
            .await
            .is_err());
        let rogue_key = rcgen::KeyPair::generate().unwrap();
        let rogue_cert = rcgen::CertificateParams::new(vec!["rogue-node".to_owned()])
            .unwrap()
            .signed_by(&rogue_key, &ca, &ca_key)
            .unwrap();
        let rogue_identity = format!("{}{}", rogue_cert.pem(), rogue_key.serialize_pem());
        assert!(make_client(Some(&rogue_identity))
            .get(format!("{endpoint}/internal/active-range/pressure"))
            .header(
                "x-whitewater-control-key",
                "this-is-a-long-control-plane-key"
            )
            .send()
            .await
            .is_err());
        let peer_client = make_client(Some(&certificates[&nodes[1]]));
        let samples: Vec<crate::demand::RangePressureSample> = peer_client
            .get(format!("{endpoint}/internal/active-range/pressure"))
            .header(
                "x-whitewater-control-key",
                "this-is-a-long-control-plane-key",
            )
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(samples.is_empty());
        let feed_id = control
            .active_feed_by_name("orders.events")
            .await
            .unwrap()
            .feed_id;
        let progress: crate::active_range::ReplicaProgressResponse = peer_client
            .post(format!(
                "{endpoint}/internal/active-range/recovery/progress"
            ))
            .header(
                "x-whitewater-control-key",
                "this-is-a-long-control-plane-key",
            )
            .json(&serde_json::json!({ "feed_id": feed_id }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(progress.error.is_none());
        assert!(progress.status.is_some());
        assert_eq!(
            peer_client
                .get(format!("{endpoint}/internal/active-range/pressure"))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        shutdown_tx.send(true).unwrap();
        tls_task.await.unwrap().unwrap();
        control_plane.raft().shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn internal_mtls_plane_accepts_catalog_registered_certificate_pins() {
        let directory = TempDir::new().unwrap();
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let mut ca_params =
            rcgen::CertificateParams::new(vec!["riverbed-test-ca".to_owned()]).unwrap();
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        ca_params.key_usages = vec![rcgen::KeyUsagePurpose::KeyCertSign];
        let ca = ca_params.self_signed(&ca_key).unwrap();
        let issue = |name: &str| {
            let key = rcgen::KeyPair::generate().unwrap();
            let mut params = rcgen::CertificateParams::new(vec![name.to_owned()]).unwrap();
            params.extended_key_usages = vec![
                rcgen::ExtendedKeyUsagePurpose::ClientAuth,
                rcgen::ExtendedKeyUsagePurpose::ServerAuth,
            ];
            let cert = params.signed_by(&key, &ca, &ca_key).unwrap();
            (cert, key)
        };
        let nodes = ["control-1", "control-2", "control-3"]
            .map(|name| StorageNodeId::try_new(name).unwrap());
        let mut pins = BTreeMap::new();
        let (server_cert, server_key) = issue(nodes[0].as_str());
        pins.insert(
            nodes[0].clone(),
            std::collections::BTreeSet::from(
                [*blake3::hash(server_cert.der().as_ref()).as_bytes()],
            ),
        );
        // Static pins cover the configured voters only.
        for node in &nodes[1..] {
            let (cert, _) = issue(node.as_str());
            pins.insert(
                node.clone(),
                std::collections::BTreeSet::from([*blake3::hash(cert.der().as_ref()).as_bytes()]),
            );
        }
        tokio::fs::write(directory.path().join("node.pem"), server_cert.pem())
            .await
            .unwrap();
        tokio::fs::write(
            directory.path().join("node.key"),
            server_key.serialize_pem(),
        )
        .await
        .unwrap();
        tokio::fs::write(directory.path().join("ca.pem"), ca.pem())
            .await
            .unwrap();
        let config = SubscriptionMtlsConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            cert_path: directory.path().join("node.pem"),
            key_path: directory.path().join("node.key"),
            ca_path: directory.path().join("ca.pem"),
            peer_pins: pins,
            peer_endpoints: BTreeMap::new(),
        };
        let server = SubscriptionTlsServer::from_config(&config, &nodes[0])
            .await
            .unwrap();
        let (mut state, control_plane, control, _replica) =
            internal_replica_test_state(&directory).await;
        state.subscription_mtls_enabled = true;
        // A registered storage Node's certificate pin arrives through the
        // replicated catalog rather than the static peer-pin configuration.
        let (registered_cert, registered_key) = issue("storage-9");
        let registered_identity = format!(
            "{}{}",
            registered_cert.pem(),
            registered_key.serialize_pem()
        );
        let registered_pin = blake3::hash(registered_cert.der().as_ref())
            .to_hex()
            .to_string();
        control
            .execute_commands(vec![crate::control::Command::RegisterStorageNode {
                node: StorageNodeId::try_new("storage-9").unwrap(),
                endpoint: "https://storage-9:7271".to_owned(),
                cert_pins: std::collections::BTreeSet::from([registered_pin]),
            }])
            .await
            .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let tls_task = tokio::spawn(server.with_control(control.clone()).serve(
            listener,
            internal_mtls_router(state.clone()),
            shutdown_rx,
        ));
        let make_client = |identity: &str| {
            reqwest::Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(3))
                .tls_built_in_root_certs(false)
                .add_root_certificate(reqwest::Certificate::from_pem(ca.pem().as_bytes()).unwrap())
                .identity(reqwest::Identity::from_pem(identity.as_bytes()).unwrap())
                .resolve("control-1", address)
                .build()
                .unwrap()
        };
        let endpoint = format!("https://control-1:{}", address.port());
        let samples: Vec<crate::demand::RangePressureSample> = make_client(&registered_identity)
            .get(format!("{endpoint}/internal/active-range/pressure"))
            .header(
                "x-whitewater-control-key",
                "this-is-a-long-control-plane-key",
            )
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(samples.is_empty());
        // A valid CA-signed certificate with no registered or configured pin
        // is still refused.
        let (rogue_cert, rogue_key) = issue("storage-8");
        let rogue_identity = format!("{}{}", rogue_cert.pem(), rogue_key.serialize_pem());
        assert!(make_client(&rogue_identity)
            .get(format!("{endpoint}/internal/active-range/pressure"))
            .header(
                "x-whitewater-control-key",
                "this-is-a-long-control-plane-key",
            )
            .send()
            .await
            .is_err());
        shutdown_tx.send(true).unwrap();
        tls_task.await.unwrap().unwrap();
        control_plane.raft().shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn effect_journal_internal_routes_authenticate_and_fence_placement() {
        let directory = TempDir::new().unwrap();
        let (app, control_plane, control, _replica) =
            internal_replica_test_router(&directory).await;
        let subscription = control
            .active_subscription_by_name("orders.billing")
            .await
            .unwrap();
        let assignment = control
            .active_subscription_progress_assignment_by_id(subscription.subscription_id)
            .await
            .unwrap();
        let local = crate::active_range::StorageNodeId::try_new("control-1").unwrap();
        let effect_id = Uuid::from_u128(941);
        let mutation = crate::effect::EffectMutation {
            effect_id,
            subscription_id: subscription.subscription_id,
            ownership_epoch: assignment.ownership_epoch,
            sequence: 1,
            request_id: Uuid::from_u128(942),
            transition: crate::effect::EffectTransition::Declare {
                consume: Some(crate::effect::EffectConsume {
                    feed_id: subscription.feed_id,
                    expected_cursor: None,
                    cursor: "effect-1".to_owned(),
                    positions: BTreeMap::from([(
                        RangeId::from_uuid(Uuid::from_u128(943)),
                        "position-1".to_owned(),
                    )]),
                }),
                outputs: vec![crate::effect::EffectOutput {
                    feed_id: Uuid::from_u128(944),
                    key_base64: "a2V5".to_owned(),
                    payload_base64: "cGF5bG9hZA==".to_owned(),
                    event_time_ns: 1,
                    writer_session_id: Uuid::from_u128(945),
                    writer_epoch: 1,
                    sequence: 1,
                }],
            },
        };
        let prepare = crate::effect::EffectPrepareRequest {
            owner: assignment.owner.clone(),
            receiver: local.clone(),
            mutation: mutation.clone(),
        };
        let request = |path: &str, body: serde_json::Value, key: Option<&str>| {
            let mut builder = Request::builder()
                .method("POST")
                .uri(path)
                .header("content-type", "application/json");
            if let Some(key) = key {
                builder = builder.header("x-whitewater-control-key", key);
            }
            builder
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap()
        };
        let path = "/internal/effect-journal/prepare";
        let missing = app
            .clone()
            .oneshot(request(path, json!(prepare), None))
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);
        let mut wrong = prepare.clone();
        wrong.receiver = assignment
            .replicas
            .iter()
            .find(|node| *node != &local)
            .unwrap()
            .clone();
        let rejected = app
            .clone()
            .oneshot(request(
                path,
                json!(wrong),
                Some("this-is-a-long-control-plane-key"),
            ))
            .await
            .unwrap();
        assert_eq!(rejected.status(), StatusCode::CONFLICT);
        let accepted = app
            .clone()
            .oneshot(request(
                path,
                json!(prepare),
                Some("this-is-a-long-control-plane-key"),
            ))
            .await
            .unwrap();
        assert_eq!(accepted.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(accepted.into_body(), 64 * 1024)
            .await
            .unwrap();
        let reply: crate::effect::EffectReplicaReply<crate::effect::EffectPrepareVote> =
            serde_json::from_slice(&bytes).unwrap();
        assert_eq!(reply.replica, local);
        assert_eq!(reply.effect_id, effect_id);
        // The prepared mutation is invisible to committed reads until the
        // quorum commit lands.
        let read = crate::effect::EffectReadRequest {
            owner: assignment.owner.clone(),
            receiver: local.clone(),
            subscription_id: subscription.subscription_id,
            effect_id,
            ownership_epoch: assignment.ownership_epoch,
        };
        let response = app
            .clone()
            .oneshot(request(
                "/internal/effect-journal/committed",
                json!(read),
                Some("this-is-a-long-control-plane-key"),
            ))
            .await
            .unwrap();
        let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .unwrap();
        let reply: crate::effect::EffectReplicaReply<Option<crate::effect::EffectMutation>> =
            serde_json::from_slice(&bytes).unwrap();
        assert_eq!(reply.result, None);
        control_plane.raft().shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn effect_journal_apply_requires_the_progress_owner_and_epoch() {
        let directory = TempDir::new().unwrap();
        let (app, control_plane, control, _replica) =
            internal_replica_test_router(&directory).await;
        let subscription = control
            .active_subscription_by_name("orders.billing")
            .await
            .unwrap();
        let assignment = control
            .active_subscription_progress_assignment_by_id(subscription.subscription_id)
            .await
            .unwrap();
        let local = crate::active_range::StorageNodeId::try_new("control-1").unwrap();
        let apply = crate::effect::EffectReadRequest {
            owner: assignment.owner.clone(),
            receiver: local.clone(),
            subscription_id: subscription.subscription_id,
            effect_id: Uuid::from_u128(951),
            ownership_epoch: assignment.ownership_epoch,
        };
        let request = |body: serde_json::Value, key: Option<&str>| {
            let mut builder = Request::builder()
                .method("POST")
                .uri("/internal/effect-journal/apply")
                .header("content-type", "application/json");
            if let Some(key) = key {
                builder = builder.header("x-whitewater-control-key", key);
            }
            builder
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap()
        };
        assert_eq!(
            app.clone()
                .oneshot(request(json!(apply), None))
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        let mut wrong_receiver = apply.clone();
        wrong_receiver.receiver = assignment
            .replicas
            .iter()
            .find(|node| *node != &local)
            .unwrap()
            .clone();
        assert_eq!(
            app.clone()
                .oneshot(request(
                    json!(wrong_receiver),
                    Some("this-is-a-long-control-plane-key")
                ))
                .await
                .unwrap()
                .status(),
            StatusCode::CONFLICT
        );
        let mut wrong_epoch = apply.clone();
        wrong_epoch.ownership_epoch = assignment.ownership_epoch + 9;
        assert_eq!(
            app.clone()
                .oneshot(request(
                    json!(wrong_epoch),
                    Some("this-is-a-long-control-plane-key")
                ))
                .await
                .unwrap()
                .status(),
            StatusCode::CONFLICT
        );
        // A well-formed apply is fenced unless it targets the local progress
        // owner: when this fixture's placement elects control-1 the driver
        // reaches the journal quorum and surfaces unreachable peers as
        // retryable; otherwise the receiver check refuses the call.
        let expected = if assignment.owner == local {
            StatusCode::SERVICE_UNAVAILABLE
        } else {
            StatusCode::CONFLICT
        };
        let response = app
            .clone()
            .oneshot(request(
                json!(apply),
                Some("this-is-a-long-control-plane-key"),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), expected);
        control_plane.raft().shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn non_voter_internal_routes_authenticate_by_shared_key_without_a_control_plane() {
        let directory = TempDir::new().unwrap();
        let (mut state, control_plane, _control, _replica) =
            internal_replica_test_state(&directory).await;
        // A registered storage-only Node runs no Raft: its internal plane
        // still authenticates the shared credential without a voter.
        state.control_plane = None;
        let app = router(state);
        let request = |credential: Option<&str>| {
            let mut request = Request::builder()
                .method("GET")
                .uri("/internal/active-range/pressure");
            if let Some(credential) = credential {
                request = request.header("x-whitewater-control-key", credential);
            }
            request.body(Body::empty()).unwrap()
        };
        assert_eq!(
            app.clone().oneshot(request(None)).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            app.clone()
                .oneshot(request(Some("wrong-key")))
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        let response = app
            .clone()
            .oneshot(request(Some("this-is-a-long-control-plane-key")))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        // Raft RPC routes still require an actual Control Plane.
        let vote = serde_json::to_vec(&VoteRequest::<ControlNodeId> {
            vote: openraft::Vote::new(1, 1),
            last_log_id: None,
        })
        .unwrap();
        let raft = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/internal/control-plane/raft/vote")
                    .header(
                        "x-whitewater-control-key",
                        "this-is-a-long-control-plane-key",
                    )
                    .header("content-type", "application/json")
                    .body(Body::from(vote))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(raft.status(), StatusCode::SERVICE_UNAVAILABLE);
        control_plane.raft().shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn catalog_snapshot_endpoint_serves_committed_state_to_internal_peers() {
        let directory = TempDir::new().unwrap();
        let (app, control_plane, control, _replica) =
            internal_replica_test_router(&directory).await;
        let request = |credential: Option<&str>| {
            let mut request = Request::builder()
                .method("POST")
                .uri("/internal/catalog/snapshot");
            if let Some(credential) = credential {
                request = request.header("x-whitewater-control-key", credential);
            }
            request.body(Body::empty()).unwrap()
        };
        assert_eq!(
            app.clone().oneshot(request(None)).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            app.clone()
                .oneshot(request(Some("wrong-key")))
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        let response = app
            .oneshot(request(Some("this-is-a-long-control-plane-key")))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 40 * 1024 * 1024)
            .await
            .unwrap();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        assert!(payload["revision"].as_u64().unwrap() > 0);
        let bytes = STANDARD
            .decode(payload["snapshot_base64"].as_str().unwrap())
            .unwrap();
        // The snapshot installs onto an empty non-voter catalog and
        // materializes the committed placement state.
        let storage_dir = TempDir::new().unwrap();
        let storage = ControlController::open_with_storage_nodes(
            storage_dir.path().join("catalog.json"),
            Arc::new(FileLogStore::open(storage_dir.path().join("data")).unwrap()),
            vec![],
        )
        .unwrap();
        storage.install_snapshot_bytes(&bytes).await.unwrap();
        assert!(storage.active_feed_by_name("orders.events").await.is_some());
        assert_eq!(
            control.revision().await,
            payload["revision"].as_u64().unwrap()
        );
        control_plane.raft().shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn internal_range_transport_authenticates_before_decoding_requests() {
        let directory = TempDir::new().unwrap();
        let (app, control_plane, _, _) = internal_replica_test_router(&directory).await;
        for path in [
            "/internal/active-range/move/export",
            "/internal/active-range/move/stage",
            "/internal/active-range/move/freeze",
            "/internal/active-range/move/unfreeze",
            "/internal/active-range/owner-move/export",
            "/internal/active-range/owner-move/freeze",
            "/internal/active-range/owner-move/verify",
            "/internal/active-range/owner-move/unfreeze",
            "/internal/active-range/read/committed",
            "/internal/active-range/read/evidence",
            "/internal/subscription-progress/prepare",
            "/internal/subscription-progress/commit",
            "/internal/subscription-progress/committed",
        ] {
            let request = |credential: Option<&str>| {
                let mut builder = Request::builder()
                    .method("POST")
                    .uri(path)
                    .header("content-type", "application/json");
                if let Some(credential) = credential {
                    builder = builder.header("x-whitewater-control-key", credential);
                }
                builder.body(Body::from("not-json")).unwrap()
            };
            assert_eq!(
                app.clone().oneshot(request(None)).await.unwrap().status(),
                StatusCode::UNAUTHORIZED
            );
            assert_eq!(
                app.clone()
                    .oneshot(request(Some("wrong-key")))
                    .await
                    .unwrap()
                    .status(),
                StatusCode::UNAUTHORIZED
            );
            assert!(app
                .clone()
                .oneshot(request(Some("this-is-a-long-control-plane-key")))
                .await
                .unwrap()
                .status()
                .is_client_error());
        }
        control_plane.raft().shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn admin_api_requires_bearer_authentication() {
        let directory = TempDir::new().unwrap();
        let app = admin_test_router(&directory);
        let missing = app
            .clone()
            .oneshot(admin_request(
                "/v1/admin/wcl",
                json!({ "script": "SHOW SPACES;" }),
                None,
            ))
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);

        let wrong = app
            .clone()
            .oneshot(admin_request(
                "/v1/admin/wcl",
                json!({ "script": "SHOW SPACES;" }),
                Some("wrong-key"),
            ))
            .await
            .unwrap();
        assert_eq!(wrong.status(), StatusCode::UNAUTHORIZED);

        let valid = app
            .oneshot(admin_request(
                "/v1/admin/wcl",
                json!({ "script": "CREATE SPACE orders;" }),
                Some("this-is-a-long-development-api-key"),
            ))
            .await
            .unwrap();
        assert_eq!(valid.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn placement_inspection_requires_admin_authentication() {
        let directory = TempDir::new().unwrap();
        let app = admin_test_router(&directory);
        let create = serde_json::to_value(CommandBatchRequest {
            request_id: Some(Uuid::new_v4()),
            commands: vec![
                Command::CreateSpace {
                    name: "orders".to_owned(),
                },
                Command::CreateFeed {
                    name: "orders.created".to_owned(),
                },
            ],
        })
        .unwrap();
        let created = app
            .clone()
            .oneshot(admin_request(
                "/v1/admin/commands",
                create,
                Some("this-is-a-long-development-api-key"),
            ))
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::OK);

        let inspect = serde_json::to_value(CommandBatchRequest {
            request_id: Some(Uuid::new_v4()),
            commands: vec![Command::InspectPlacement {
                feed: "orders.created".to_owned(),
            }],
        })
        .unwrap();
        let unauthorized = app
            .clone()
            .oneshot(admin_request("/v1/admin/commands", inspect.clone(), None))
            .await
            .unwrap();
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
        let authorized = app
            .oneshot(admin_request(
                "/v1/admin/commands",
                inspect,
                Some("this-is-a-long-development-api-key"),
            ))
            .await
            .unwrap();
        assert_eq!(authorized.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn typed_state_store_declarations_require_admin_access_and_remain_declared() {
        let directory = TempDir::new().unwrap();
        let app = admin_test_router(&directory);
        let request = json!({
            "request_id": Uuid::new_v4(),
            "commands": [
                { "command": "create_space", "name": "accounts" },
                { "command": "define_state_store", "name": "accounts.users", "source": { "kind": "manual" } }
            ]
        });
        let unauthorized = app
            .clone()
            .oneshot(admin_request("/v1/admin/commands", request.clone(), None))
            .await
            .unwrap();
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
        let response = app
            .oneshot(admin_request(
                "/v1/admin/commands",
                request,
                Some("this-is-a-long-development-api-key"),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let execution: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(execution["results"][1]["data"]["stage"], "declared");
    }

    #[tokio::test]
    async fn typed_pipe_declarations_require_admin_access_and_validate_scope() {
        let directory = TempDir::new().unwrap();
        let app = admin_test_router(&directory);
        let request = json!({
            "request_id": Uuid::new_v4(),
            "commands": [
                { "command": "create_space", "name": "accounts" },
                { "command": "create_feed", "name": "accounts.events" },
                { "command": "create_feed", "name": "accounts.enriched" },
                { "command": "create_subscription", "name": "accounts.flow", "feed": "accounts.events", "start": { "kind": "beginning" } },
                { "command": "define_pipe", "name": "accounts.forward", "subscription": "accounts.flow", "output_feed": "accounts.enriched", "operation": { "operation": "forward" } }
            ]
        });
        let unauthorized = app
            .clone()
            .oneshot(admin_request("/v1/admin/commands", request.clone(), None))
            .await
            .unwrap();
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
        let response = app
            .clone()
            .oneshot(admin_request(
                "/v1/admin/commands",
                request,
                Some("this-is-a-long-development-api-key"),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let parsed: Value = serde_json::from_slice(&body).unwrap();
        let pipe = parsed["results"][4]["data"].clone();
        assert_eq!(pipe["stage"], "declared");
        assert_eq!(pipe["operation"]["operation"], "forward");
        // A cross-Domain or self-loop Pipe is refused by the typed path too.
        let invalid = json!({
            "request_id": Uuid::new_v4(),
            "commands": [
                { "command": "define_pipe", "name": "accounts.loop", "subscription": "accounts.flow", "output_feed": "accounts.events", "operation": { "operation": "forward" } }
            ]
        });
        let rejected = app
            .oneshot(admin_request(
                "/v1/admin/commands",
                invalid,
                Some("this-is-a-long-development-api-key"),
            ))
            .await
            .unwrap();
        assert_ne!(rejected.status(), StatusCode::OK);
    }
    #[tokio::test]
    async fn public_admin_commands_cannot_forge_reader_frontiers() {
        let directory = TempDir::new().unwrap();
        let app = admin_test_router(&directory);
        let response = app
            .oneshot(admin_request(
                "/v1/admin/commands",
                serde_json::to_value(CommandBatchRequest {
                    request_id: Some(Uuid::new_v4()),
                    commands: vec![Command::RecordReaderFrontier {
                        reader: "audit".to_owned(),
                        session_epoch: 1,
                        cursor: "forged".to_owned(),
                        positions: BTreeMap::new(),
                        expected_cursor: None,
                        fence_delivery: true,
                        fetch_request_id: Some(Uuid::new_v4()),
                    }],
                })
                .unwrap(),
                Some("this-is-a-long-development-api-key"),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn public_admin_commands_cannot_forge_owner_cutover_evidence() {
        let directory = TempDir::new().unwrap();
        let app = admin_test_router(&directory);
        let response = app
            .oneshot(admin_request(
                "/v1/admin/commands",
                serde_json::to_value(CommandBatchRequest {
                    request_id: Some(Uuid::new_v4()),
                    commands: vec![Command::RecordOwnerMoveCatchUp {
                        feed: "orders.events".to_owned(),
                        plan_id: Uuid::new_v4(),
                        source_commit: CommitPosition::new(1),
                        target_commit: CommitPosition::new(1),
                        checksum_verified: true,
                    }],
                })
                .unwrap(),
                Some("this-is-a-long-development-api-key"),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        assert!(String::from_utf8_lossy(&body).contains("internal Node authority"));
    }

    #[tokio::test]
    async fn typed_admin_commands_use_the_same_controller() {
        let directory = TempDir::new().unwrap();
        let app = admin_test_router(&directory);
        let response = app
            .oneshot(admin_request(
                "/v1/admin/commands",
                serde_json::to_value(CommandBatchRequest {
                    request_id: Some(Uuid::new_v4()),
                    commands: vec![
                        Command::CreateSpace {
                            name: "orders".to_owned(),
                        },
                        Command::CreateFeed {
                            name: "orders.created".to_owned(),
                        },
                    ],
                })
                .unwrap(),
                Some("this-is-a-long-development-api-key"),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    struct FeedPaginationCutover {
        control: Arc<ControlController>,
    }

    #[async_trait::async_trait]
    impl SplitCutoverControl for FeedPaginationCutover {
        async fn mark_ready(
            &self,
            feed: &str,
            plan: &RangeSplitPlan,
            boundary: &FrozenSplitBoundary,
        ) -> Result<(), String> {
            self.control
                .execute_commands(vec![Command::RecordActiveRangeSplitCatchUp {
                    feed: feed.to_owned(),
                    plan_id: plan.plan_id,
                    source_commit: boundary.final_commit,
                    source_scanned_through: boundary.staging.source_commit,
                    right_commit: boundary.staging.right.target_commit,
                    checksum_verified: true,
                }])
                .await
                .map(|_| ())
                .map_err(|error| error.to_string())
        }

        async fn activate(
            &self,
            feed: &str,
            plan: &RangeSplitPlan,
            boundary: &FrozenSplitBoundary,
        ) -> Result<(), String> {
            self.control
                .execute_commands(vec![Command::ActivateActiveRangeSplit {
                    feed: feed.to_owned(),
                    plan_id: plan.plan_id,
                    left_writer_sequences: boundary.staging.left.writer_sequences.clone(),
                    right_writer_sequences: boundary.staging.right.writer_sequences.clone(),
                    reader_translations: Vec::new(),
                }])
                .await
                .map(|_| ())
                .map_err(|error| error.to_string())
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn seed_split_feed(
        directory: &TempDir,
        control: &Arc<ControlController>,
        services: &BTreeMap<StorageNodeId, Arc<ReplicaAppendService>>,
        feed: &str,
        records: u64,
        key_for: &dyn Fn(u64) -> Vec<u8>,
        ingest_for: &dyn Fn(u64) -> i64,
        payload_len: usize,
        split_at: KeyToken,
    ) -> Uuid {
        let created = control
            .execute_commands(vec![Command::CreateFeed {
                name: feed.to_owned(),
            }])
            .await
            .unwrap();
        let feed_id: Uuid =
            serde_json::from_value(created.results[0].data["feed_id"].clone()).unwrap();
        let parent = control.active_range_assignment(feed_id).await.unwrap();
        let descriptor = ActiveRangeDescriptor {
            feed_id,
            range_id: parent.range_id,
            generation: parent.generation,
            ownership_epoch: parent.ownership_epoch,
        };
        for node in parent.replicas.iter() {
            let index = ["storage-1", "storage-2", "storage-3"]
                .iter()
                .position(|candidate| candidate == &node.as_str())
                .unwrap();
            seed_committed_history_shaped(
                &directory.path().join(format!("node-{index}")),
                &descriptor,
                records,
                key_for,
                ingest_for,
                payload_len,
            );
        }
        let prepared = control
            .execute_commands(vec![Command::PrepareActiveRangeSplit {
                feed: feed.to_owned(),
                split_at,
            }])
            .await
            .unwrap();
        let plan: RangeSplitPlan =
            serde_json::from_value(prepared.results[0].data.clone()).unwrap();
        orchestrate_split_cutover(
            &FeedPaginationCutover {
                control: control.clone(),
            },
            feed,
            &plan,
            &parent,
            services[&parent.owner].clone(),
            services,
            64,
        )
        .await
        .unwrap();
        feed_id
    }

    fn routed_key(side: u8, split_at: KeyToken) -> Vec<u8> {
        for probe in 0..u16::MAX {
            let key = format!("route-{side}-{probe}").into_bytes();
            let is_left = KeyToken::from_key(&key).as_bytes() < split_at.as_bytes();
            if (side == 0) == is_left {
                return key;
            }
        }
        panic!("no {side} key found");
    }

    #[tokio::test]
    async fn multi_range_feed_reads_merge_pages_across_owner_nodes() {
        let directory = TempDir::new().unwrap();
        let log_store: Arc<dyn LogStore> =
            Arc::new(FileLogStore::open(directory.path().join("data")).unwrap());
        let control = Arc::new(
            ControlController::open_with_storage_nodes(
                directory.path().join("catalog.json"),
                log_store.clone(),
                ["storage-1", "storage-2", "storage-3"]
                    .into_iter()
                    .map(|node| StorageNodeId::try_new(node).unwrap())
                    .collect(),
            )
            .unwrap(),
        );
        control
            .execute_commands(vec![Command::CreateSpace {
                name: "orders".to_owned(),
            }])
            .await
            .unwrap();
        let nodes: Vec<StorageNodeId> = ["storage-1", "storage-2", "storage-3"]
            .into_iter()
            .map(|node| StorageNodeId::try_new(node).unwrap())
            .collect();
        let mut services = BTreeMap::new();
        for (index, node) in nodes.iter().enumerate() {
            services.insert(
                node.clone(),
                Arc::new(ReplicaAppendService::new(
                    directory.path().join(format!("node-{index}")),
                    node.clone(),
                    control.clone(),
                )),
            );
        }
        let split_at = KeyToken::from_bytes([0x80; 16]);
        let left_key = routed_key(0, split_at);
        let right_key = routed_key(1, split_at);
        let key_for = move |sequence: u64| {
            if sequence.is_multiple_of(2) {
                left_key.clone()
            } else {
                right_key.clone()
            }
        };
        for (feed, records, payload_len) in [
            ("orders.events", 12_u64, 0_usize),
            ("orders.deep", 10_005_u64, 0_usize),
            ("orders.blobs", 4_u64, 6 * 1024 * 1024),
        ] {
            seed_split_feed(
                &directory,
                &control,
                &services,
                feed,
                records,
                &key_for,
                &|sequence| sequence as i64,
                payload_len,
                split_at,
            )
            .await;
        }

        let control_key = "this-is-a-long-control-plane-key".to_owned();
        let peers: BTreeMap<u64, BasicNode> = [1_u64, 2, 3]
            .into_iter()
            .map(|node| (node, BasicNode::new(format!("127.0.0.1:{}", 9_700 + node))))
            .collect();
        let mut listeners = Vec::new();
        let mut endpoints = BTreeMap::new();
        for node in &nodes {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            endpoints.insert(
                node.clone(),
                format!("http://{}", listener.local_addr().unwrap()),
            );
            listeners.push(listener);
        }
        let endpoints = Arc::new(endpoints);
        let mut handles = Vec::new();
        let mut listeners = listeners.into_iter();
        for (index, node) in nodes.iter().enumerate() {
            let listener = listeners.next().unwrap();
            let plane = if index == 0 {
                None
            } else {
                Some(Arc::new(
                    ControlPlane::start(
                        index as u64 + 1,
                        peers.clone(),
                        control_key.clone(),
                        directory.path().join(format!("raft-{index}.json")),
                        control.clone(),
                        None,
                    )
                    .await
                    .unwrap(),
                ))
            };
            let membership = Arc::new(MembershipService::new(
                MemberAnnouncement {
                    node_id: format!("test-node-{index}"),
                    api_url: "http://test-node:7070".to_owned(),
                    capacity: 100,
                },
                vec![],
                Duration::from_secs(1),
                Duration::from_secs(5),
            ));
            let state = AppState {
                store: log_store.clone(),
                membership,
                demand: DemandMetrics::default(),
                autoscaler: Arc::new(Mutex::new(AutoscaleController::default())),
                replica_append: Some(services[node].clone()),
                control: control.clone(),
                control_plane: plane,
                progress_authority: control.clone(),
                majority_append: None,
                subscription_progress: None,
                effect_journal: None,
                subscription_mtls_enabled: false,
                storage_node_id: Some(node.clone()),
                control_endpoints: Arc::new(endpoints.clone().into()),
                internal_key: Some(control_key.clone()),
                internal_http: reqwest::Client::new(),
                internal_mtls: None,
                admin_auth: AdminAuthenticator::new(Some(
                    "this-is-a-long-development-api-key".to_owned(),
                ))
                .unwrap(),
            };
            handles.push(tokio::spawn(async move {
                axum::serve(listener, router(state)).await.unwrap();
            }));
        }
        let client = reqwest::Client::new();
        let base = format!("{}/v1/feeds/records", endpoints[&nodes[0]]);
        let read_feed = |feed: &str, after: Option<&str>, limit: usize| {
            let client = client.clone();
            let mut url = reqwest::Url::parse(&base).unwrap();
            url.query_pairs_mut()
                .append_pair("feed", feed)
                .append_pair("limit", &limit.to_string());
            if let Some(after) = after {
                url.query_pairs_mut().append_pair("after", after);
            }
            async move {
                let response = client
                    .get(url)
                    .bearer_auth("this-is-a-long-development-api-key")
                    .send()
                    .await
                    .unwrap();
                let status = response.status();
                let body: Value = response.json().await.unwrap();
                (status, body)
            }
        };
        let cursors_of = |body: &Value| -> Vec<String> {
            body.as_array()
                .unwrap()
                .iter()
                .map(|record| record["cursor"].as_str().unwrap().to_owned())
                .collect()
        };

        let (status, body) = read_feed("orders.events", None, 5).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            cursors_of(&body),
            vec!["cursor-1", "cursor-2", "cursor-3", "cursor-4", "cursor-5"]
        );
        let (status, body) = read_feed("orders.events", Some("cursor-5"), 5).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            cursors_of(&body),
            vec!["cursor-6", "cursor-7", "cursor-8", "cursor-9", "cursor-10"]
        );
        let (status, body) = read_feed("orders.events", Some("cursor-10"), 5).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(cursors_of(&body), vec!["cursor-11", "cursor-12"]);
        let (status, body) = read_feed("orders.events", Some("cursor-12"), 5).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.as_array().unwrap().is_empty());
        let (status, _) = read_feed("orders.events", Some("cursor-404"), 5).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        let (status, body) = read_feed("orders.deep", None, 12_000).await;
        assert_eq!(status, StatusCode::OK);
        let expected: Vec<String> = (1..=10_000_u64)
            .map(|sequence| format!("cursor-{sequence}"))
            .collect();
        assert_eq!(cursors_of(&body), expected);
        let (status, body) = read_feed("orders.deep", Some("cursor-10000"), 10).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            cursors_of(&body),
            vec![
                "cursor-10001",
                "cursor-10002",
                "cursor-10003",
                "cursor-10004",
                "cursor-10005"
            ]
        );

        let (status, body) = read_feed("orders.blobs", None, 10).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(cursors_of(&body), vec!["cursor-1", "cursor-2"]);
        let (status, body) = read_feed("orders.blobs", Some("cursor-2"), 10).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(cursors_of(&body), vec!["cursor-3", "cursor-4"]);

        for handle in handles {
            handle.abort();
        }
    }

    #[tokio::test]
    async fn subscription_member_endpoints_fence_and_apply_through_quorum() {
        let directory = TempDir::new().unwrap();
        let log_store: Arc<dyn LogStore> =
            Arc::new(FileLogStore::open(directory.path().join("data")).unwrap());
        let control = Arc::new(
            ControlController::open_with_storage_nodes(
                directory.path().join("catalog.json"),
                log_store.clone(),
                ["storage-1", "storage-2", "storage-3"]
                    .into_iter()
                    .map(|node| StorageNodeId::try_new(node).unwrap())
                    .collect(),
            )
            .unwrap(),
        );
        control
            .execute_commands(vec![
                Command::CreateSpace {
                    name: "orders".to_owned(),
                },
                Command::CreateFeed {
                    name: "orders.events".to_owned(),
                },
                Command::CreateSubscription {
                    name: "orders.billing".to_owned(),
                    feed: "orders.events".to_owned(),
                    start: crate::control::ReaderStart::Beginning,
                },
                Command::CreateSubscription {
                    name: "orders.fresh".to_owned(),
                    feed: "orders.events".to_owned(),
                    start: crate::control::ReaderStart::Beginning,
                },
                Command::CreateSubscription {
                    name: "orders.now".to_owned(),
                    feed: "orders.events".to_owned(),
                    start: crate::control::ReaderStart::Now,
                },
            ])
            .await
            .unwrap();
        let definition = control
            .active_subscription_by_name("orders.billing")
            .await
            .unwrap();
        let assignment = control
            .active_subscription_progress_assignment_by_id(definition.subscription_id)
            .await
            .unwrap();
        let nodes: Vec<StorageNodeId> = ["storage-1", "storage-2", "storage-3"]
            .into_iter()
            .map(|node| StorageNodeId::try_new(node).unwrap())
            .collect();
        // Seed committed Feed history on every assigned replica so member
        // fetches read real frames across Node boundaries.
        let feed = control.active_feed_by_name("orders.events").await.unwrap();
        let parent = control.active_range_assignment(feed.feed_id).await.unwrap();
        let descriptor = ActiveRangeDescriptor {
            feed_id: feed.feed_id,
            range_id: parent.range_id,
            generation: parent.generation,
            ownership_epoch: parent.ownership_epoch,
        };
        let mut services = BTreeMap::new();
        for (index, node) in nodes.iter().enumerate() {
            services.insert(
                node.clone(),
                Arc::new(ReplicaAppendService::new(
                    directory.path().join(format!("node-{index}")),
                    node.clone(),
                    control.clone(),
                )),
            );
        }
        for node in parent.replicas.iter() {
            let index = nodes
                .iter()
                .position(|candidate| candidate == node)
                .unwrap();
            seed_committed_history(
                &directory.path().join(format!("node-{index}")),
                &descriptor,
                12,
            );
        }
        let progress_range = parent.range_id;
        let control_key = "this-is-a-long-control-plane-key".to_owned();
        let peers: BTreeMap<u64, BasicNode> = [1_u64, 2, 3]
            .into_iter()
            .map(|node| (node, BasicNode::new(format!("127.0.0.1:{}", 9_800 + node))))
            .collect();
        let mut listeners = Vec::new();
        let mut endpoints = BTreeMap::new();
        for node in &nodes {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            endpoints.insert(
                node.clone(),
                format!("http://{}", listener.local_addr().unwrap()),
            );
            listeners.push(listener);
        }
        let endpoints = Arc::new(endpoints);
        let mut handles = Vec::new();
        let mut listeners = listeners.into_iter();
        for (index, node) in nodes.iter().enumerate() {
            let listener = listeners.next().unwrap();
            let replica = Arc::new(
                crate::reader::FjallSubscriptionProgressReplica::open(
                    directory.path().join(format!("progress-{index}")),
                )
                .unwrap(),
            );
            let plane = Arc::new(
                ControlPlane::start(
                    index as u64 + 1,
                    peers.clone(),
                    control_key.clone(),
                    directory.path().join(format!("raft-member-{index}.json")),
                    control.clone(),
                    None,
                )
                .await
                .unwrap(),
            );
            let membership = Arc::new(MembershipService::new(
                MemberAnnouncement {
                    node_id: format!("test-node-{index}"),
                    api_url: "http://test-node:7070".to_owned(),
                    capacity: 100,
                },
                vec![],
                Duration::from_secs(1),
                Duration::from_secs(5),
            ));
            let state = AppState {
                store: log_store.clone(),
                membership,
                demand: DemandMetrics::default(),
                autoscaler: Arc::new(Mutex::new(AutoscaleController::default())),
                replica_append: Some(services[node].clone()),
                control: control.clone(),
                control_plane: Some(plane),
                progress_authority: control.clone(),
                majority_append: None,
                subscription_progress: Some(Arc::new(
                    crate::reader::SubscriptionProgressReplicaService::new(
                        node.clone(),
                        control.clone(),
                        replica,
                    ),
                )),
                effect_journal: Some(Arc::new(crate::effect::EffectJournalService::new(
                    node.clone(),
                    control.clone(),
                    Arc::new(
                        crate::effect::FjallEffectJournalReplica::open(
                            directory.path().join(format!("effect-journal-{index}")),
                        )
                        .unwrap(),
                    ),
                ))),
                subscription_mtls_enabled: false,
                storage_node_id: Some(node.clone()),
                control_endpoints: Arc::new(endpoints.clone().into()),
                internal_key: Some(control_key.clone()),
                internal_http: reqwest::Client::new(),
                internal_mtls: None,
                admin_auth: AdminAuthenticator::new(Some(
                    "this-is-a-long-development-api-key".to_owned(),
                ))
                .unwrap(),
            };
            handles.push(tokio::spawn(async move {
                axum::serve(listener, router(state)).await.unwrap();
            }));
        }

        // Seed the committed frontier through the same quorum path the
        // member endpoints rely on.
        let seed_transport = crate::reader::HttpSubscriptionProgressTransport::new(
            assignment.clone(),
            endpoints.as_ref().clone(),
            control_key.clone(),
            Duration::from_secs(5),
        )
        .unwrap();
        let coordinator = crate::reader::SubscriptionProgressCoordinator::new(
            assignment.clone(),
            Arc::new(seed_transport),
        );
        coordinator
            .apply(crate::reader::SubscriptionProgressMutation {
                subscription_id: assignment.subscription_id,
                feed_id: definition.feed_id,
                ownership_epoch: assignment.ownership_epoch,
                sequence: 1,
                request_id: Uuid::from_u128(0x9001),
                expected_cursor: None,
                cursor: "cursor-2".to_owned(),
                positions: BTreeMap::from([(progress_range, "cursor-2".to_owned())]),
                tick: 0,
                lease_ops: Vec::new(),
            })
            .await
            .unwrap();

        let client = reqwest::Client::new();
        let base = format!("{}/v1/subscriptions/members", endpoints[&nodes[0]]);
        let post = |path: &str, body: Value| {
            let client = client.clone();
            let url = format!("{base}/{path}");
            async move {
                let response = client.post(url).json(&body).send().await.unwrap();
                let status = response.status();
                let body: Value = response.json().await.unwrap_or_default();
                (status, body)
            }
        };
        let member = Uuid::from_u128(0x1111);
        let work = Uuid::from_u128(0x2222);

        // Unknown Subscription is rejected before placement work begins.
        let (status, _) = post(
            "join",
            json!({"subscription": "orders.missing", "request_id": Uuid::from_u128(1),
                "member_id": member}),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        // Join assigns the first member epoch through the committed quorum.
        let (status, body) = post(
            "join",
            json!({"subscription": "orders.billing", "request_id": Uuid::from_u128(2),
                "member_id": member}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["member_epoch"], json!(1));
        let join_replay = post(
            "join",
            json!({"subscription": "orders.billing", "request_id": Uuid::from_u128(2),
                "member_id": member}),
        )
        .await;
        assert_eq!(join_replay.0, StatusCode::OK);
        assert_eq!(join_replay.1["member_epoch"], json!(1));

        // A fresh join request fences the earlier epoch.
        let (status, body) = post(
            "join",
            json!({"subscription": "orders.billing", "request_id": Uuid::from_u128(3),
                "member_id": member}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["member_epoch"], json!(2));

        // Reusing a request identity for a different operation conflicts.
        let (status, _) = post(
            "claim",
            json!({"subscription": "orders.billing", "request_id": Uuid::from_u128(3),
                "member_id": member, "member_epoch": 2, "work_id": work}),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);

        // The fenced member epoch cannot claim work.
        let (status, _) = post(
            "claim",
            json!({"subscription": "orders.billing", "request_id": Uuid::from_u128(4),
                "member_id": member, "member_epoch": 1, "work_id": work}),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);

        // Out-of-bounds lease durations are rejected at the edge.
        let (status, _) = post(
            "claim",
            json!({"subscription": "orders.billing", "request_id": Uuid::from_u128(5),
                "member_id": member, "member_epoch": 2, "work_id": work,
                "lease_ticks": SUBSCRIPTION_MEMBER_MAX_LEASE_TICKS + 1}),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        // The live member claims bounded work.
        let (status, body) = post(
            "claim",
            json!({"subscription": "orders.billing", "request_id": Uuid::from_u128(5),
                "member_id": member, "member_epoch": 2, "work_id": work,
                "lease_ticks": 60}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let lease = body["lease"].clone();
        assert_eq!(lease["work_id"], json!(work));
        assert_eq!(lease["member_epoch"], json!(2));
        assert_eq!(lease["lease_epoch"], json!(1));
        assert_eq!(
            lease["expires_at_tick"].as_u64().unwrap(),
            body["tick"].as_u64().unwrap() + 60
        );
        let claim_replay = post(
            "claim",
            json!({"subscription": "orders.billing", "request_id": Uuid::from_u128(5),
                "member_id": member, "member_epoch": 2, "work_id": work,
                "lease_ticks": 60}),
        )
        .await;
        assert_eq!(claim_replay.0, StatusCode::OK);
        assert_eq!(claim_replay.1["lease"], lease);

        // Renew advances the expiry and bumps the lease epoch.
        let (status, body) = post(
            "renew",
            json!({"subscription": "orders.billing", "request_id": Uuid::from_u128(6),
                "member_id": member, "member_epoch": 2, "work_id": work,
                "lease_epoch": 1, "lease_ticks": 90}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let renewed = body["lease"].clone();
        assert_eq!(renewed["lease_epoch"], json!(1));
        assert!(
            renewed["expires_at_tick"].as_u64().unwrap()
                > lease["expires_at_tick"].as_u64().unwrap()
        );

        // A stale lease epoch cannot renew or acknowledge.
        let (status, _) = post(
            "renew",
            json!({"subscription": "orders.billing", "request_id": Uuid::from_u128(7),
                "member_id": member, "member_epoch": 2, "work_id": work,
                "lease_epoch": 7, "lease_ticks": 30}),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        let (status, _) = post(
            "ack",
            json!({"subscription": "orders.billing", "request_id": Uuid::from_u128(8),
                "member_id": member, "member_epoch": 2, "work_id": work,
                "lease_epoch": 7, "cursor": "cursor-7",
                "positions": {progress_range.to_string(): "cursor-7"}}),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);

        // cursor and positions must travel together.
        let (status, _) = post(
            "ack",
            json!({"subscription": "orders.billing", "request_id": Uuid::from_u128(9),
                "member_id": member, "member_epoch": 2, "work_id": work,
                "lease_epoch": 1, "cursor": "cursor-7"}),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        // A valid grant acknowledges progress and releases the lease atomically.
        let (status, body) = post(
            "ack",
            json!({"subscription": "orders.billing", "request_id": Uuid::from_u128(9),
                "member_id": member, "member_epoch": 2, "work_id": work,
                "lease_epoch": 1, "cursor": "cursor-7",
                "positions": {progress_range.to_string(): "cursor-7"}}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert!(body["leases"]
            .as_object()
            .is_none_or(|leases| leases.is_empty()));
        assert_eq!(
            coordinator.read_committed().await.unwrap().unwrap().cursor,
            "cursor-7"
        );
        // The committed acknowledgement replays its recorded outcome.
        let (status, body) = post(
            "ack",
            json!({"subscription": "orders.billing", "request_id": Uuid::from_u128(9),
                "member_id": member, "member_epoch": 2, "work_id": work,
                "lease_epoch": 1, "cursor": "cursor-7",
                "positions": {progress_range.to_string(): "cursor-7"}}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body["leases"]
            .as_object()
            .is_none_or(|leases| leases.is_empty()));
        // Reusing the acknowledgement identity with a different Cursor conflicts.
        let (status, _) = post(
            "ack",
            json!({"subscription": "orders.billing", "request_id": Uuid::from_u128(9),
                "member_id": member, "member_epoch": 2, "work_id": work,
                "lease_epoch": 1, "cursor": "cursor-10",
                "positions": {progress_range.to_string(): "cursor-7"}}),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);

        // Member state reflects the fenced epochs and empty lease set.
        let response = client
            .get(format!("{base}/state"))
            .query(&[("subscription", "orders.billing")])
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = response.json().await.unwrap();
        assert_eq!(body["member_epochs"][member.to_string()], json!(2));
        assert!(body["leases"]
            .as_object()
            .is_none_or(|leases| leases.is_empty()));

        // A new claim after the release is issued a fresh lease epoch.
        let (status, body) = post(
            "claim",
            json!({"subscription": "orders.billing", "request_id": Uuid::from_u128(10),
                "member_id": member, "member_epoch": 2, "work_id": work,
                "lease_ticks": 30}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["lease"]["lease_epoch"], json!(1));

        // Fetch delivers committed records after the shared frontier to a
        // member holding a live work lease.
        let (status, _) = post(
            "fetch",
            json!({"subscription": "orders.billing", "request_id": Uuid::from_u128(11),
                "member_id": member, "member_epoch": 2, "work_id": work,
                "lease_epoch": 7}),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        let (status, body) = post(
            "claim",
            json!({"subscription": "orders.billing", "request_id": Uuid::from_u128(11),
                "member_id": member, "member_epoch": 2, "work_id": work}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let claimed_epoch = body["lease"]["lease_epoch"].as_u64().unwrap();
        let (status, body) = post(
            "fetch",
            json!({"subscription": "orders.billing", "request_id": Uuid::from_u128(12),
                "member_id": member, "member_epoch": 2, "work_id": work,
                "lease_epoch": claimed_epoch,
                "limit": 3}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(
            body["records"]
                .as_array()
                .unwrap()
                .iter()
                .map(|record| record["cursor"].as_str().unwrap().to_owned())
                .collect::<Vec<_>>(),
            vec!["cursor-8", "cursor-9", "cursor-10"]
        );
        assert_eq!(body["cursor"], json!("cursor-10"));
        assert_eq!(
            body["positions"][progress_range.to_string()],
            json!("cursor-10")
        );
        // The member acknowledges the delivered page atomically.
        let (status, body) = post(
            "ack",
            json!({"subscription": "orders.billing", "request_id": Uuid::from_u128(13),
                "member_id": member, "member_epoch": 2, "work_id": work,
                "lease_epoch": claimed_epoch,
                "cursor": body["cursor"], "positions": body["positions"]}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        // A second fetch continues from the acknowledged frontier.
        let (status, body) = post(
            "claim",
            json!({"subscription": "orders.billing", "request_id": Uuid::from_u128(14),
                "member_id": member, "member_epoch": 2, "work_id": work}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let reclaimed_epoch = body["lease"]["lease_epoch"].as_u64().unwrap();
        let (status, body) = post(
            "fetch",
            json!({"subscription": "orders.billing", "request_id": Uuid::from_u128(15),
                "member_id": member, "member_epoch": 2, "work_id": work,
                "lease_epoch": reclaimed_epoch}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(
            body["records"]
                .as_array()
                .unwrap()
                .iter()
                .map(|record| record["cursor"].as_str().unwrap().to_owned())
                .collect::<Vec<_>>(),
            vec!["cursor-11", "cursor-12"]
        );

        // A Subscription with no committed frontier bootstraps its declared
        // start atomically with the first member join.
        let (status, body) = post(
            "join",
            json!({"subscription": "orders.fresh", "request_id": Uuid::from_u128(20),
                "member_id": member}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["member_epoch"], json!(1));
        let fresh_assignment = control
            .active_subscription_progress_assignment_by_id(
                control
                    .active_subscription_by_name("orders.fresh")
                    .await
                    .unwrap()
                    .subscription_id,
            )
            .await
            .unwrap();
        let fresh_transport = crate::reader::HttpSubscriptionProgressTransport::new(
            fresh_assignment.clone(),
            endpoints.as_ref().clone(),
            control_key.clone(),
            Duration::from_secs(5),
        )
        .unwrap();
        let fresh_coordinator = crate::reader::SubscriptionProgressCoordinator::new(
            fresh_assignment,
            Arc::new(fresh_transport),
        );
        let frontier = fresh_coordinator.read_committed().await.unwrap().unwrap();
        assert_eq!(frontier.sequence, 1);
        assert_eq!(frontier.cursor, "beginning");
        assert!(!frontier.positions.is_empty());
        assert!(frontier.positions.values().all(|value| value.is_empty()));
        assert!(frontier.lease_ops.iter().any(|op| matches!(
            op,
            crate::reader::SubscriptionLeaseOp::Join { member_id, member_epoch }
                if *member_id == member && *member_epoch == 1
        )));
        // Members can claim work on the bootstrapped frontier.
        let (status, body) = post(
            "claim",
            json!({"subscription": "orders.fresh", "request_id": Uuid::from_u128(21),
                "member_id": member, "member_epoch": 1, "work_id": work}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let fresh_epoch = body["lease"]["lease_epoch"].as_u64().unwrap();
        let (status, body) = post(
            "fetch",
            json!({"subscription": "orders.fresh", "request_id": Uuid::from_u128(22),
                "member_id": member, "member_epoch": 1, "work_id": work,
                "lease_epoch": fresh_epoch,
                "limit": 4}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(
            body["records"]
                .as_array()
                .unwrap()
                .iter()
                .map(|record| record["cursor"].as_str().unwrap().to_owned())
                .collect::<Vec<_>>(),
            vec!["cursor-1", "cursor-2", "cursor-3", "cursor-4"]
        );

        // A `now` Subscription pins each range's committed tail at first join,
        // so the first fetch is empty and its positions name the tail Cursors.
        let (status, body) = post(
            "join",
            json!({"subscription": "orders.now", "request_id": Uuid::from_u128(30),
                "member_id": member}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let (status, body) = post(
            "claim",
            json!({"subscription": "orders.now", "request_id": Uuid::from_u128(31),
                "member_id": member, "member_epoch": 1, "work_id": work}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let now_epoch = body["lease"]["lease_epoch"].as_u64().unwrap();
        let (status, body) = post(
            "fetch",
            json!({"subscription": "orders.now", "request_id": Uuid::from_u128(32),
                "member_id": member, "member_epoch": 1, "work_id": work,
                "lease_epoch": now_epoch}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert!(body["records"].as_array().unwrap().is_empty());
        assert_eq!(
            body["positions"][progress_range.to_string()],
            json!("cursor-12")
        );

        // A lost progress owner leaves member reads on surviving quorum
        // evidence while the next mutation transparently recovers placement
        // onto an eligible replica.
        let owner_index = nodes
            .iter()
            .position(|node| *node == assignment.owner)
            .unwrap();
        handles[owner_index].abort();
        let survivor_index = nodes
            .iter()
            .position(|node| *node != assignment.owner)
            .unwrap();
        let response = client
            .get(format!(
                "{}/v1/subscriptions/members/state?subscription=orders.billing",
                endpoints[&nodes[survivor_index]]
            ))
            .send()
            .await
            .unwrap();
        let status = response.status();
        let body: Value = response.json().await.unwrap();
        assert_eq!(status, StatusCode::OK, "{body}");

        let survivor_base = format!(
            "{}/v1/subscriptions/members",
            endpoints[&nodes[survivor_index]]
        );
        let post_survivor = |path: &str, body: Value| {
            let client = client.clone();
            let url = format!("{survivor_base}/{path}");
            async move {
                let response = client.post(url).json(&body).send().await.unwrap();
                let status = response.status();
                let body: Value = response.json().await.unwrap_or_default();
                (status, body)
            }
        };
        let recovered_member = Uuid::from_u128(0x3333);
        let (status, body) = post_survivor(
            "join",
            json!({"subscription": "orders.billing", "request_id": Uuid::from_u128(40),
                "member_id": recovered_member}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["member_epoch"], json!(1));
        let recovered_assignment = control
            .active_subscription_progress_assignment_by_id(assignment.subscription_id)
            .await
            .unwrap();
        assert_eq!(
            recovered_assignment.ownership_epoch,
            assignment.ownership_epoch + 1
        );
        assert_ne!(recovered_assignment.owner, assignment.owner);

        // Later mutations run straight through on the recovered owner.
        let (status, body) = post_survivor(
            "claim",
            json!({"subscription": "orders.billing", "request_id": Uuid::from_u128(41),
                "member_id": recovered_member, "member_epoch": 1,
                "work_id": Uuid::from_u128(0x4444)}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");

        for handle in handles {
            handle.abort();
        }
    }

    #[test]
    fn subscription_placement_divergence_detects_mixed_replica_state() {
        let node = |name: &str| crate::active_range::StorageNodeId::try_new(name).unwrap();
        let mutation =
            |sequence: u64, ownership_epoch: u64| crate::reader::SubscriptionProgressMutation {
                subscription_id: Uuid::from_u128(9_100),
                feed_id: Uuid::from_u128(9_101),
                ownership_epoch,
                sequence,
                request_id: Uuid::from_u128(9_102),
                expected_cursor: None,
                cursor: format!("rf1_{sequence}"),
                positions: BTreeMap::from([(
                    RangeId::from_uuid(Uuid::from_u128(9_103)),
                    "position".to_owned(),
                )]),
                tick: 0,
                lease_ops: Vec::new(),
            };
        let inspection = |committed| crate::reader::SubscriptionProgressInspection {
            committed,
            prepared: None,
            members: crate::reader::SubscriptionMemberState::default(),
        };
        let aligned: Vec<_> = [node("storage-a"), node("storage-b"), node("storage-c")]
            .into_iter()
            .map(|node| (node, inspection(Some(mutation(8, 2)))))
            .collect();
        assert!(!subscription_placement_diverged(&aligned));

        // A replica an ownership epoch behind the others diverges.
        let mut lagging = aligned.clone();
        lagging[0].1 = inspection(Some(mutation(8, 1)));
        assert!(subscription_placement_diverged(&lagging));

        // A replica missing committed progress diverges.
        let mut missing = aligned.clone();
        missing[0].1 = inspection(None);
        assert!(subscription_placement_diverged(&missing));

        // A replica a sequence behind diverges.
        let mut behind = aligned.clone();
        behind[1].1 = inspection(Some(mutation(7, 2)));
        assert!(subscription_placement_diverged(&behind));

        // Divergent member state alone also triggers healing.
        let mut members = aligned.clone();
        members[2]
            .1
            .members
            .member_epochs
            .insert(Uuid::from_u128(9_104), 1);
        assert!(subscription_placement_diverged(&members));
    }
}
