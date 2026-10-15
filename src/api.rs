use std::{
    collections::{BTreeMap, BTreeSet},
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
    codec::{decode_record, MAX_FRAME_BYTES},
    config::SubscriptionMtlsConfig,
    control::{
        ControlController, ControlError, PermissionAction, RangeMergePlan, RangeMovePlan,
        RangeMoveStage, RangeOwnerMovePlan, RangeSplitPlan, ReaderCutoverSnapshot,
        ReaderFrontierTranslation, ReplicatedCommand,
    },
    control_plane::{
        ControlNodeId, ControlPlane, ControlPlaneError, ControlTypeConfig, FullSnapshotRequest,
        InternalCommandsRequest, InternalCommandsResponse, InternalWriteResponse,
    },
    demand::{isolation_split_boundary, DemandMetrics, DemandSnapshot, HotKeyEvidence},
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
    pub reader_pacing: crate::reader::ReaderPacingTable,
    pub autoscaler: Arc<Mutex<AutoscaleController>>,
    pub control: Arc<ControlController>,
    pub control_plane: Option<Arc<ControlPlane>>,
    pub progress_authority: Arc<dyn crate::reader::SubscriptionPlacementAuthority>,
    pub replica_append: Option<Arc<ReplicaAppendService>>,
    pub subscription_progress: Option<Arc<SubscriptionProgressReplicaService>>,
    pub effect_journal: Option<Arc<crate::effect::EffectJournalService>>,
    pub state_store_journal: Option<Arc<crate::state_store::StateStoreMutationService>>,
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
    let replica_discard_pending_route = post(replica_discard_pending).layer(
        middleware::from_fn_with_state(state.clone(), authorize_replica_append),
    );
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
    let owner_release_pending_route = post(owner_release_pending).layer(
        middleware::from_fn_with_state(state.clone(), authorize_replica_append),
    );
    let owner_discard_pending_route = post(owner_discard_pending).layer(
        middleware::from_fn_with_state(state.clone(), authorize_replica_append),
    );
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
    let index_query_route =
        subscription_key_layer(post(index_query_internal).layer(DefaultBodyLimit::max(64 * 1024)));
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
        .route("/internal/index/query", index_query_route)
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
        .route(
            "/internal/active-range/replica/discard-pending",
            replica_discard_pending_route,
        )
        .route("/internal/active-range/owner/append", owner_append_route)
        .route(
            "/internal/active-range/owner/release-pending",
            owner_release_pending_route,
        )
        .route(
            "/internal/active-range/owner/discard-pending",
            owner_discard_pending_route,
        )
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
        .route(
            "/internal/state-store-journal/prepare",
            subscription_key_layer(
                post(state_store_journal_prepare_local).layer(DefaultBodyLimit::max(512 * 1024)),
            ),
        )
        .route(
            "/internal/state-store-journal/commit",
            subscription_key_layer(
                post(state_store_journal_commit_local).layer(DefaultBodyLimit::max(512 * 1024)),
            ),
        )
        .route(
            "/internal/state-store-journal/inspect",
            subscription_key_layer(
                post(state_store_journal_inspect_local).layer(DefaultBodyLimit::max(64 * 1024)),
            ),
        )
        .route(
            "/internal/state-store-journal/lookup",
            subscription_key_layer(
                post(state_store_journal_lookup_local).layer(DefaultBodyLimit::max(64 * 1024)),
            ),
        )
        .route(
            "/internal/state-store-journal/read",
            subscription_key_layer(
                post(state_store_journal_read_local).layer(DefaultBodyLimit::max(64 * 1024)),
            ),
        )
        .route(
            "/internal/state-store-journal/entries",
            subscription_key_layer(
                post(state_store_journal_entries_local).layer(DefaultBodyLimit::max(64 * 1024)),
            ),
        )
        .route(
            "/internal/state-store-journal/transfer",
            subscription_key_layer(
                post(state_store_journal_transfer_local).layer(DefaultBodyLimit::max(64 * 1024)),
            ),
        )
        .route(
            "/internal/pipe/drive",
            subscription_key_layer(post(pipe_drive_local).layer(DefaultBodyLimit::max(64 * 1024))),
        )
}

pub fn router(state: AppState) -> Router {
    let mut app = Router::new()
        .route("/health", get(health))
        .route("/v1/streams", get(list_streams).post(create_stream))
        .route("/v1/streams/describe", get(describe_stream))
        .route("/v1/records", get(read_records).post(append_record))
        .route("/v1/feeds/append", post(client_append))
        .route("/v1/state-stores/mutate", post(state_store_mutate))
        .route("/v1/state-stores/sync", post(state_store_sync))
        .route("/v1/state-stores/rebuild", post(state_store_rebuild))
        .route("/v1/state-stores/lookup", post(state_store_lookup))
        .route("/v1/state-stores/get-by", post(state_store_get_by))
        .route("/v1/state-stores/iterate", post(state_store_iterate))
        .route("/v1/state-stores/clear", post(state_store_clear))
        .route("/v1/indexes/query", post(index_query))
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
            "/v1/subscriptions/members/transfer",
            post(subscription_member_transfer),
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
        .route("/v1/subscriptions/seek", post(subscription_seek))
        .route("/v1/feeds/records", get(read_feed_records))
        .route("/v1/feeds/records/search", get(search_feed_records))
        .route("/v1/feeds/records/lookup", get(lookup_feed_record))
        .route("/v1/admin/wcl", post(execute_admin_wcl))
        .route("/v1/admin/commands", post(execute_admin_commands))
        .route("/v1/admin/ranges/split", post(admin_split_range))
        .route("/v1/admin/ranges/merge", post(admin_merge_ranges))
        .route("/v1/admin/ranges/move-follower", post(admin_move_follower))
        .route("/v1/admin/ranges/move-owner", post(admin_move_owner))
        .route(
            "/v1/admin/subscriptions/move-progress",
            post(admin_move_subscription_progress),
        )
        .route(
            "/v1/admin/subscriptions/progress",
            get(admin_subscription_progress),
        )
        .route("/v1/admin/demand/hot-keys", get(admin_hot_keys))
        .route("/v1/admin/demand/pipes", get(admin_pipe_demand))
        .route(
            "/v1/admin/demand/subscriptions",
            get(admin_subscription_demand),
        )
        .route("/v1/admin/demand/readers", get(admin_reader_demand))
        .route("/v1/admin/demand/indexes", get(admin_index_demand))
        .route("/v1/admin/demand/feeds", get(admin_feed_demand))
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

/// Wire protocol revision reported by `/health`; SDKs verify `major`
/// before exercising operations (sdk/spec/PROTOCOL.md). Bumped only on
/// incompatible wire changes — additive fields never increment it.
#[derive(Serialize)]
struct HealthProtocol {
    major: u64,
    minor: u64,
}

/// Enforced transport bounds advertised by `/health` so SDKs negotiate
/// capacity before a request hits a 4xx refusal — the values are the
/// compiled-in constants, not configuration.
#[derive(Serialize)]
struct HealthLimits {
    max_frame_bytes: usize,
    max_committed_read_bytes: usize,
    max_logical_read_frames: usize,
    max_logical_read_ranges: usize,
    subscription_member_fetch_max_limit: usize,
    subscription_member_max_lease_ticks: u64,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    node_id: String,
    members: usize,
    protocol: HealthProtocol,
    limits: HealthLimits,
}

async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        node_id: state.membership.local().node_id.clone(),
        members: state.membership.members().await.len(),
        protocol: HealthProtocol { major: 1, minor: 0 },
        limits: HealthLimits {
            max_frame_bytes: MAX_FRAME_BYTES,
            max_committed_read_bytes: MAX_COMMITTED_READ_BYTES,
            max_logical_read_frames: MAX_LOGICAL_READ_FRAMES,
            max_logical_read_ranges: MAX_LOGICAL_READ_RANGES,
            subscription_member_fetch_max_limit: SUBSCRIPTION_MEMBER_FETCH_MAX_LIMIT,
            subscription_member_max_lease_ticks: SUBSCRIPTION_MEMBER_MAX_LEASE_TICKS,
        },
    })
}

async fn execute_admin_wcl(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<WclRequest>,
) -> Result<impl IntoResponse, ApiError> {
    authenticate_credential(&state, &headers)?;
    let _request = state.demand.begin_request();
    let request_id = request.request_id.unwrap_or_else(Uuid::new_v4);
    let commands = crate::control::parse_wcl(&request.script)?;
    authorize_wcl_commands(&state, &headers, &commands).await?;
    if let Some(execution) = evaluate_data_plane_reads(&state, commands.clone(), request_id).await?
    {
        return Ok(Json(execution));
    }
    Ok(Json(match &state.control_plane {
        Some(control_plane) => {
            control_plane
                .execute_commands_with_request_id(commands, request_id)
                .await?
        }
        None => {
            state
                .control
                .execute_commands_with_request_id(commands, request_id)
                .await?
        }
    }))
}

async fn execute_admin_commands(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<CommandBatchRequest>,
) -> Result<impl IntoResponse, ApiError> {
    authenticate_credential(&state, &headers)?;
    if request
        .commands
        .iter()
        .any(crate::control::Command::requires_internal_replica_authority)
    {
        return Err(ApiError::bad_request(
            "replica recovery and movement cutover require internal Node authority",
        ));
    }
    authorize_wcl_commands(&state, &headers, &request.commands).await?;
    let _request = state.demand.begin_request();
    let request_id = request.request_id.unwrap_or_else(Uuid::new_v4);
    if let Some(execution) =
        evaluate_data_plane_reads(&state, request.commands.clone(), request_id).await?
    {
        return Ok(Json(execution));
    }
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
    // The configured Admin key is checked first; otherwise the
    // replicated catalog's issued identities decide via their bound
    // Role's grants (currently `manage` on the root `*` namespace).
    state.admin_auth.authorize_issued(
        authorization,
        &state.control.issued_api_keys(),
        unix_ns(),
    )?;
    Ok(())
}

/// Authorize a data-plane `action` against the Feed named in the
/// request. The configured bootstrap key and `manage`-on-`*` issued
/// identities pass everywhere; other issued identities need a grant
/// whose namespace pattern covers `feed` for `action` (`manage`
/// implies read and write). Endpoints that resolve Writer, Reader,
/// Subscription, StateStore, or Index names still require admin scope.
fn authorize_namespace(
    state: &AppState,
    headers: &HeaderMap,
    namespace: &str,
    action: PermissionAction,
) -> Result<(), ApiError> {
    let authorization = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok());
    state.admin_auth.authorize_scoped(
        authorization,
        &state.control.issued_api_keys(),
        unix_ns(),
        namespace,
        action,
    )?;
    Ok(())
}

/// Authenticate the Bearer credential before a named resource is
/// resolved — an invalid credential receives `401` rather than a
/// not-found response that would leak resource existence. The
/// resource's owning Feed check runs afterward via `authorize_namespace`.
fn authenticate_credential(state: &AppState, headers: &HeaderMap) -> Result<(), ApiError> {
    let authorization = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok());
    state
        .admin_auth
        .authenticate(authorization, &state.control.issued_api_keys(), unix_ns())?;
    Ok(())
}

/// Authorize `read` against a Subscription's declared source — a
/// Feed source checks the Feed's namespace like any other read, while
/// a Domain source needs a grant spanning the whole Domain (`*` or
/// `{domain}.*`) because member Feeds attach incrementally.
async fn authorize_subscription(
    state: &AppState,
    headers: &HeaderMap,
    definition: &crate::control::SubscriptionDefinition,
    action: PermissionAction,
) -> Result<(), ApiError> {
    match &definition.source {
        crate::control::SubscriptionSource::Feed { feed_id } => {
            authorize_resource_feed(state, headers, *feed_id, action).await
        }
        crate::control::SubscriptionSource::Domain { domain_id } => {
            let domain = state
                .control
                .active_domain_name(*domain_id)
                .await
                .ok_or_else(|| ApiError::unavailable("owning Domain is unavailable"))?;
            let authorization = headers
                .get(AUTHORIZATION)
                .and_then(|value| value.to_str().ok());
            state.admin_auth.authorize_domain_scoped(
                authorization,
                &state.control.issued_api_keys(),
                unix_ns(),
                &domain,
                action,
            )?;
            Ok(())
        }
    }
}

/// Authorize `action` against the Feed that owns a resolved resource
/// (a Writer or Reader — their names may live in another Domain, so
/// the grant must cover the Feed's namespace, not the resource's own
/// name).
async fn authorize_resource_feed(
    state: &AppState,
    headers: &HeaderMap,
    feed_id: Uuid,
    action: PermissionAction,
) -> Result<(), ApiError> {
    let feed = state
        .control
        .active_feed_by_id(feed_id)
        .await
        .ok_or_else(|| ApiError::unavailable("owning Feed is unavailable"))?;
    authorize_namespace(state, headers, &feed.name, action)
}

/// A name a command's grants must cover: `Resource` resolves to its
/// owning Domain by longest dotted prefix (a Feed, Writer, Store,
/// etc.); `Domain` is already a Domain name (a Domain-wide
/// Subscription source, `REBUILD INDEXES IN`, DROP/RENAME DOMAIN).
enum ScopedName {
    Resource(String),
    Domain(String),
}

/// The grants one WCL or typed command needs beyond authentication:
/// `Domains` require `manage` spanning every listed Domain; `Reads`
/// require `read` spanning them; `CatalogRead` inspects metadata
/// Riverbed-wide (`read` on `*`); `Riverbed` keeps the historical
/// Riverbed-wide administrative scope (RBAC, Domains, sessions,
/// placement, Node membership).
enum WclCommandScope {
    Domains(Vec<ScopedName>),
    Reads(Vec<ScopedName>),
    CatalogRead,
    Riverbed,
}

fn wcl_command_scope(command: &crate::control::Command) -> WclCommandScope {
    use crate::control::{Command, ResourceKind, StateStoreSourceRequest};
    let resource = |name: &str| ScopedName::Resource(name.to_owned());
    let domain = |name: &str| ScopedName::Domain(name.to_owned());
    match command {
        Command::CreateFeed { name }
        | Command::RebuildIndex { name }
        | Command::PausePipe { name }
        | Command::ResumePipe { name }
        | Command::SeekReader { reader: name, .. }
        | Command::SeekSubscription { name, .. } => WclCommandScope::Domains(vec![resource(name)]),
        Command::DefineStateStore { name, source, .. } => {
            let mut scopes = vec![resource(name)];
            if let StateStoreSourceRequest::Feed { feed } = source {
                scopes.push(resource(feed));
            }
            WclCommandScope::Domains(scopes)
        }
        Command::DefineIndex { name, feed, .. }
        | Command::CreateWriter { name, feed }
        | Command::CreateReader { name, feed, .. } => {
            WclCommandScope::Domains(vec![resource(name), resource(feed)])
        }
        Command::DefinePipe {
            name, subscription, ..
        } => WclCommandScope::Domains(vec![resource(name), resource(subscription)]),
        Command::CreateSubscription {
            name, feed, domain, ..
        } => {
            let mut scopes = vec![resource(name)];
            if let Some(feed) = feed {
                scopes.push(resource(feed));
            }
            if let Some(domain) = domain {
                scopes.push(ScopedName::Domain(domain.clone()));
            }
            WclCommandScope::Domains(scopes)
        }
        Command::Rename { kind, current, new } => match kind {
            ResourceKind::Role => WclCommandScope::Riverbed,
            ResourceKind::Domain | ResourceKind::Space => {
                WclCommandScope::Domains(vec![domain(current), domain(new)])
            }
            _ => WclCommandScope::Domains(vec![resource(current), resource(new)]),
        },
        Command::Drop { kind, name } => match kind {
            ResourceKind::Role => WclCommandScope::Riverbed,
            ResourceKind::Domain | ResourceKind::Space => {
                WclCommandScope::Domains(vec![domain(name)])
            }
            _ => WclCommandScope::Domains(vec![resource(name)]),
        },
        Command::RebuildIndexesInDomain { domain } => {
            WclCommandScope::Domains(vec![ScopedName::Domain(domain.clone())])
        }
        Command::Describe { kind, name } => WclCommandScope::Reads(match kind {
            ResourceKind::Domain | ResourceKind::Space => vec![domain(name)],
            ResourceKind::Role => vec![],
            _ => vec![resource(name)],
        }),
        Command::Show { .. } | Command::ExplainAccess { .. } => WclCommandScope::CatalogRead,
        _ => WclCommandScope::Riverbed,
    }
}

/// Resolve a `ScopedName` to the Domain name a grant must span:
/// catalog longest-prefix for resources (first-segment fallback when
/// the Domain is missing — command apply still validates existence)
/// and the name itself for Domains.
async fn scoped_domain(state: &AppState, scoped: &ScopedName) -> String {
    match scoped {
        ScopedName::Domain(name) => name.clone(),
        ScopedName::Resource(name) => state
            .control
            .owning_domain_name(name)
            .await
            .unwrap_or_else(|| name.split('.').next().unwrap_or(name).to_owned()),
    }
}

/// Authenticate the caller, then require every command's scope:
/// bootstrap and riverbed-admin identities pass all scopes; issued
/// identities need `manage` (`read` for inspection) spanning each
/// command's Domains — or `*` for Riverbed-wide commands.
async fn authorize_wcl_commands(
    state: &AppState,
    headers: &HeaderMap,
    commands: &[crate::control::Command],
) -> Result<(), ApiError> {
    authenticate_credential(state, headers)?;
    let authorization = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok());
    for command in commands {
        let (scopes, action) = match wcl_command_scope(command) {
            WclCommandScope::Riverbed => {
                authorize_admin(state, headers)?;
                continue;
            }
            WclCommandScope::CatalogRead => {
                state.admin_auth.authorize_domain_scoped(
                    authorization,
                    &state.control.issued_api_keys(),
                    unix_ns(),
                    "*",
                    PermissionAction::Read,
                )?;
                continue;
            }
            WclCommandScope::Domains(scopes) => (scopes, PermissionAction::Manage),
            WclCommandScope::Reads(scopes) => (scopes, PermissionAction::Read),
        };
        for scoped in &scopes {
            let domain = scoped_domain(state, scoped).await;
            state.admin_auth.authorize_domain_scoped(
                authorization,
                &state.control.issued_api_keys(),
                unix_ns(),
                &domain,
                action,
            )?;
        }
    }
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
    let range_id = request.range_id;
    let wire_bytes = request.frame_base64.len();
    Ok(Json(match service.append(request).await {
        Ok(result) => {
            state.demand.record_range_replication(range_id, wire_bytes);
            ReplicaAppendResponse {
                result: Some(result),
                error: None,
            }
        }
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
    let range_id = request.range_id;
    Ok(Json(match service.commit(request).await {
        Ok(result) => {
            state.demand.record_range_replication_commit(range_id);
            ReplicaCommitResponse {
                result: Some(result),
                error: None,
            }
        }
        Err(error) => ReplicaCommitResponse {
            result: None,
            error: Some(error),
        },
    }))
}

async fn replica_discard_pending(
    State(state): State<AppState>,
    Json(request): Json<crate::active_range::ReplicaDiscardPendingRequest>,
) -> Result<Json<crate::active_range::ReplicaDiscardPendingResponse>, ApiError> {
    let service = state.replica_append.as_ref().ok_or_else(|| ApiError {
        status: StatusCode::SERVICE_UNAVAILABLE,
        message: "Active Range replica storage is not configured on this Node".to_owned(),
        code: None,
    })?;
    Ok(Json(match service.discard_pending(request).await {
        Ok(response) => response,
        Err(error) => crate::active_range::ReplicaDiscardPendingResponse {
            outcome: None,
            occupant: None,
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
struct SubscriptionMemberTransferRequest {
    subscription: String,
    request_id: Uuid,
    work_id: Uuid,
    /// Current holder + its grant epoch — fencing evidence so a stale
    /// transfer cannot clobber a newer assignment.
    member_id: Uuid,
    lease_epoch: u64,
    /// Target member and its current member epoch.
    to_member_id: Uuid,
    to_member_epoch: u64,
    lease_ticks: Option<u64>,
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
    /// Positions this acknowledgement declares delivered-but-not-processed
    /// (quarantine) or resolved-and-removable (release_quarantine); the
    /// cumulative set is durable poison evidence a later SEEK can revisit.
    #[serde(default)]
    quarantine: Option<BTreeMap<RangeId, Vec<String>>>,
    #[serde(default)]
    release_quarantine: Option<BTreeMap<RangeId, Vec<String>>>,
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
    let definition =
        subscription_when_applied(&state.control, subscription, state.control_plane.is_some())
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

/// A Subscription's committed progress frontier staged for an Active Range
/// cutover; captured before activation so missing or stale lineage refuses
/// the transition rather than stranding acknowledged positions on a retired
/// range map.
struct SubscriptionCutoverFrontier {
    definition: crate::control::SubscriptionDefinition,
    frontier: crate::reader::SubscriptionProgressMutation,
}

/// Load every Feed-scoped Subscription on `feed_id` that holds committed
/// progress. Declared Subscriptions without a frontier or a progress
/// placement have nothing to translate and never block a range transition;
/// Domain-scoped sources cannot hold per-Feed progress yet.
async fn subscription_cutover_frontiers(
    state: &AppState,
    feed_id: Uuid,
) -> Result<Vec<SubscriptionCutoverFrontier>, ApiError> {
    let mut frontiers = Vec::new();
    for definition in state.control.active_subscriptions().await {
        if definition.source.feed_id() != Some(feed_id) {
            continue;
        }
        let Some(assignment) = state
            .control
            .active_subscription_progress_assignment_by_id(definition.subscription_id)
            .await
        else {
            continue;
        };
        let (coordinator, _) = member_assignment_coordinator(state, assignment)?;
        // A freshly declared Subscription's progress placement can still be
        // propagating to its replicas when the cutover probes them; retry the
        // transient refusal codes briefly, then fail the transition closed —
        // a committed frontier that cannot be read must not be stranded on
        // the retired range map.
        let mut frontier = None;
        for attempt in 0..10_u8 {
            match coordinator.read_committed().await {
                Ok(result) => {
                    frontier = Some(result);
                    break;
                }
                Err(
                    crate::reader::SubscriptionProgressError::Unavailable
                    | crate::reader::SubscriptionProgressError::NoQuorum
                    | crate::reader::SubscriptionProgressError::AmbiguousCommit
                    | crate::reader::SubscriptionProgressError::InvalidAssignment,
                ) if attempt < 9 => {
                    tokio::time::sleep(Duration::from_millis(75)).await;
                }
                Err(error) => return Err(subscription_progress_api_error(error)),
            }
        }
        if let Some(frontier) = frontier.flatten() {
            frontiers.push(SubscriptionCutoverFrontier {
                definition,
                frontier,
            });
        }
    }
    Ok(frontiers)
}

/// The committed-lineage evidence a cutover reuses to restate Subscription
/// frontiers on the new range map — identical inputs to the Reader-session
/// translations so both move at the same frozen boundary.
enum CutoverFrontierTranslation<'a> {
    Split {
        entries: &'a [crate::reader::ReaderLineageEntry],
        plan: &'a RangeSplitPlan,
    },
    Merge {
        left_entries: &'a [crate::reader::ReaderLineageEntry],
        right_entries: &'a [crate::reader::ReaderLineageEntry],
        plan: &'a RangeMergePlan,
    },
}

impl CutoverFrontierTranslation<'_> {
    fn feed_id(&self) -> Uuid {
        match self {
            Self::Split { plan, .. } => plan.feed_id,
            Self::Merge { plan, .. } => plan.feed_id,
        }
    }

    fn translate(
        &self,
        frontier: &crate::reader::SubscriptionProgressMutation,
    ) -> Result<crate::reader::TranslatedSubscriptionFrontier, ApiError> {
        match *self {
            Self::Split { entries, plan } => crate::reader::translate_split_subscription_frontier(
                entries,
                plan.source_range_id,
                plan.right_assignment.range_id,
                plan.split_at,
                &frontier.positions,
                &frontier.quarantined,
            ),
            Self::Merge {
                left_entries,
                right_entries,
                plan,
            } => crate::reader::translate_merge_subscription_frontier(
                left_entries,
                right_entries,
                plan.left_range_id,
                plan.right_range_id,
                &frontier.positions,
                &frontier.quarantined,
            ),
        }
        .map_err(|error| ApiError::unavailable(error.to_string()))
    }
}

/// A deterministic reconcile identity per (cutover plan, Subscription,
/// target sequence): a retry after an ambiguous commit replays the same
/// mutation through the progress shard's request dedup instead of forking a
/// second transition.
fn subscription_reconcile_request_id(plan_id: Uuid, subscription_id: Uuid, sequence: u64) -> Uuid {
    let digest = blake3::hash(
        format!("whitewater-subscription-cutover:{plan_id}:{subscription_id}:{sequence}")
            .as_bytes(),
    );
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    Uuid::from_bytes(bytes)
}

/// Commit one pending Subscription cutover translation to its RF3 progress
/// shard, then clear the durable pending record. The translation was committed
/// inside the activation command, so the apply is deterministic: the staged
/// Cursor compare-and-set refuses to overwrite a frontier a member
/// acknowledgement advanced after staging, and the recorded `request_id`
/// deduplicates a replayed apply after an ambiguous commit. A frontier that
/// already covers the post-cutover range map converges without a write.
async fn apply_pending_subscription_cutover(
    state: &AppState,
    pending: &crate::control::SubscriptionCutoverTranslation,
    coordinator: &crate::reader::SubscriptionProgressCoordinator,
    assignment: &crate::reader::SubscriptionProgressAssignment,
) -> Result<&'static str, ApiError> {
    for attempt in 0..12_u8 {
        let current: BTreeSet<RangeId> = state
            .control
            .active_range_assignments_for_feed(pending.feed_id)
            .await
            .iter()
            .map(|assignment| assignment.range_id)
            .collect();
        let frontier = match coordinator.read_committed().await {
            Ok(frontier) => frontier,
            Err(
                crate::reader::SubscriptionProgressError::Unavailable
                | crate::reader::SubscriptionProgressError::NoQuorum
                | crate::reader::SubscriptionProgressError::AmbiguousCommit
                | crate::reader::SubscriptionProgressError::InvalidAssignment
                | crate::reader::SubscriptionProgressError::StaleEpoch,
            ) if attempt < 11 => {
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
            Err(error) => return Err(subscription_progress_api_error(error)),
        };
        let Some(frontier) = frontier else {
            // The translation was staged against a committed frontier that no
            // longer exists; keep the pending record rather than clearing
            // evidence of an interrupted cutover.
            return Err(ApiError::unavailable(
                "Subscription frontier vanished before its cutover translation applied",
            ));
        };
        if frontier
            .positions
            .keys()
            .copied()
            .collect::<BTreeSet<RangeId>>()
            == current
        {
            clear_subscription_cutover(state, pending.subscription_id, pending.plan_id).await?;
            return Ok("current");
        }
        // Only position-moving mutations rotate the frontier Cursor, so a
        // matching Cursor proves the committed positions are still the staged
        // set even when lease-only member ops drifted the sequence meanwhile.
        if frontier.cursor != pending.expected_cursor {
            // A member acknowledgement landed between staging and this apply:
            // its committed positions still reference the retired map, so
            // refusing keeps the Subscription fail-closed instead of
            // rewinding acknowledged work. SEEK SUBSCRIPTION repositions the
            // frontier and clears the pending record.
            return Err(ApiError::unavailable(
                "Subscription frontier advanced past its staged cutover; reconcile progress",
            ));
        }
        let sequence = frontier
            .sequence
            .checked_add(1)
            .ok_or_else(|| ApiError::unavailable("Subscription frontier sequence overflowed"))?;
        let mutation = crate::reader::SubscriptionProgressMutation {
            subscription_id: pending.subscription_id,
            feed_id: pending.feed_id,
            ownership_epoch: assignment.ownership_epoch,
            sequence,
            request_id: subscription_reconcile_request_id(
                pending.plan_id,
                pending.subscription_id,
                sequence,
            ),
            expected_cursor: Some(pending.expected_cursor.clone()),
            cursor: pending.cursor.clone(),
            positions: pending.positions.clone(),
            quarantined: pending.quarantined.clone(),
            quarantine_add: BTreeMap::new(),
            quarantine_remove: BTreeMap::new(),
            tick: pending.tick,
            lease_ops: Vec::new(),
        };
        match coordinator.apply(mutation).await {
            Ok(_) => {
                clear_subscription_cutover(state, pending.subscription_id, pending.plan_id).await?;
                return Ok("reconciled");
            }
            Err(
                crate::reader::SubscriptionProgressError::Conflict
                | crate::reader::SubscriptionProgressError::Sequence
                | crate::reader::SubscriptionProgressError::AmbiguousCommit
                | crate::reader::SubscriptionProgressError::Unavailable
                | crate::reader::SubscriptionProgressError::NoQuorum
                | crate::reader::SubscriptionProgressError::InvalidAssignment
                | crate::reader::SubscriptionProgressError::StaleEpoch,
            ) if attempt < 11 => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(error) => return Err(subscription_progress_api_error(error)),
        }
    }
    Err(ApiError::unavailable(
        "Subscription cutover apply exhausted its retries",
    ))
}

/// Drop one pending Subscription cutover record after its translation
/// committed (or the frontier already covered the new map). The `plan_id`
/// guard in the command keeps a stale clear from deleting a newer pending.
async fn clear_subscription_cutover(
    state: &AppState,
    subscription_id: Uuid,
    plan_id: Uuid,
) -> Result<(), ApiError> {
    let control_plane = state
        .control_plane
        .as_ref()
        .ok_or_else(|| ApiError::unavailable("Control Plane is unavailable"))?;
    control_plane
        .execute_commands(vec![crate::control::Command::ClearSubscriptionCutover {
            subscription_id,
            plan_id,
        }])
        .await
        .map_err(|error| ApiError::unavailable(error.to_string()))?;
    Ok(())
}

/// Translate every staged Subscription frontier and build the pending records
/// committed inside the activation command. A failed translation refuses the
/// cutover while the source is still frozen, exactly like the Reader
/// translations.
fn subscription_cutover_translations(
    frontiers: &[SubscriptionCutoverFrontier],
    translation: &CutoverFrontierTranslation<'_>,
    plan_id: Uuid,
) -> Result<Vec<crate::control::SubscriptionCutoverTranslation>, ApiError> {
    frontiers
        .iter()
        .map(|staged| {
            let (positions, quarantined) = translation.translate(&staged.frontier)?;
            let sequence = staged.frontier.sequence.checked_add(1).ok_or_else(|| {
                ApiError::unavailable("Subscription frontier sequence overflowed")
            })?;
            Ok(crate::control::SubscriptionCutoverTranslation {
                subscription_id: staged.definition.subscription_id,
                feed_id: translation.feed_id(),
                plan_id,
                expected_cursor: staged.frontier.cursor.clone(),
                frontier_sequence: staged.frontier.sequence,
                request_id: subscription_reconcile_request_id(
                    plan_id,
                    staged.definition.subscription_id,
                    sequence,
                ),
                cursor: format!("cutover:{plan_id}"),
                positions,
                quarantined,
                tick: staged.frontier.tick,
            })
        })
        .collect()
}

/// Resolve the Subscription progress coordinator, then apply any committed
/// cutover translation still pending for the Subscription. The pending record
/// is durable catalog evidence committed inside the range activation, so a
/// member touch heals a Subscription whose post-activation reconcile was
/// interrupted instead of stranding it until an operator seeks.
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
    let (coordinator, assignment, definition) =
        member_coordinator_inner(state, subscription).await?;
    if state.control_plane.is_some() {
        if let Some(pending) = state
            .control
            .pending_subscription_cutover(definition.subscription_id)
            .await
        {
            apply_pending_subscription_cutover(state, &pending, &coordinator, &assignment).await?;
        }
    }
    Ok((coordinator, assignment, definition))
}

async fn member_coordinator_inner(
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
    if evidence.iter().any(|(_, inspection)| {
        [&inspection.committed, &inspection.prepared]
            .into_iter()
            .flatten()
            .any(|mutation| mutation.ownership_epoch != assignment.ownership_epoch)
    }) {
        // A placement CAS landed while adoption missed at least one replica:
        // its rows still carry an older epoch, so every committed read fences
        // them as stale and quorum never forms. Re-adopt the recovered
        // frontier at the live epoch; committing another ownership move here
        // would only ratchet the epoch while the same rows stay behind.
        let outcome = coordinator
            .synchronize_placement()
            .await
            .map_err(subscription_progress_api_error)?;
        let (coordinator, assignment) = member_assignment_coordinator(state, outcome.assignment)?;
        return Ok((coordinator, assignment, definition));
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
    headers: HeaderMap,
    Json(request): Json<SubscriptionMemberJoinRequest>,
) -> Result<Json<SubscriptionMemberResponse>, ApiError> {
    authenticate_credential(&state, &headers)?;
    let definition = subscription_when_applied(
        &state.control,
        &request.subscription,
        state.control_plane.is_some(),
    )
    .await
    .ok_or_else(|| ApiError::bad_request("Subscription does not exist or is not active"))?;
    authorize_subscription(&state, &headers, &definition, PermissionAction::Read).await?;
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
        let source_feed = definition.source.feed_id().ok_or_else(|| ApiError {
            status: StatusCode::CONFLICT,
            message: "Subscription has a Domain source; member sessions arrive with Domain-wide progress tracking".to_owned(),
            code: None,
        })?;
        let range_assignments = state
            .control
            .active_range_assignments_for_feed(source_feed)
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
                        let decoded =
                            STANDARD
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
            crate::control::ReaderStart::Timestamp(threshold) => (
                format!("timestamp:{threshold}"),
                resolve_timestamp_positions(&state, &range_assignments, *threshold).await?,
            ),
            crate::control::ReaderStart::Cursor(cursor) => (
                cursor.clone(),
                resolve_cursor_positions(&state, &range_assignments, cursor).await?,
            ),
        };
        coordinator
            .apply(crate::reader::SubscriptionProgressMutation {
                subscription_id: assignment.subscription_id,
                feed_id: definition.source.feed_id().unwrap(),
                ownership_epoch: assignment.ownership_epoch,
                sequence: 1,
                request_id: request.request_id,
                expected_cursor: None,
                cursor: start_cursor,
                positions,
                quarantined: BTreeMap::new(),
                quarantine_add: BTreeMap::new(),
                quarantine_remove: BTreeMap::new(),
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
    state.demand.record_subscription_op(
        definition.subscription_id,
        crate::demand::SubscriptionTrafficSample {
            op: crate::demand::SubscriptionTrafficOp::Join,
            records: 0,
            ranges: 0,
            quarantine_add: 0,
            quarantine_remove: 0,
            members: member_state.member_epochs.len() as u64,
            sampled_at_ns: unix_ns(),
        },
    );
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
    headers: HeaderMap,
    Json(request): Json<SubscriptionMemberLeaseRequest>,
) -> Result<Json<SubscriptionMemberResponse>, ApiError> {
    authenticate_credential(&state, &headers)?;
    let definition = subscription_when_applied(
        &state.control,
        &request.subscription,
        state.control_plane.is_some(),
    )
    .await
    .ok_or_else(|| ApiError::bad_request("Subscription does not exist or is not active"))?;
    authorize_subscription(&state, &headers, &definition, PermissionAction::Read).await?;
    let (coordinator, assignment, definition) =
        member_coordinator_recovering(&state, &request.subscription).await?;
    let lease_ticks = member_lease_ticks(request.lease_ticks)?;
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
    state.demand.record_subscription_op(
        definition.subscription_id,
        crate::demand::SubscriptionTrafficSample {
            op: crate::demand::SubscriptionTrafficOp::Claim,
            records: 0,
            ranges: 0,
            quarantine_add: 0,
            quarantine_remove: 0,
            members: member_state.member_epochs.len() as u64,
            sampled_at_ns: unix_ns(),
        },
    );
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
    headers: HeaderMap,
    Json(request): Json<SubscriptionMemberLeaseRequest>,
) -> Result<Json<SubscriptionMemberResponse>, ApiError> {
    authenticate_credential(&state, &headers)?;
    let definition = subscription_when_applied(
        &state.control,
        &request.subscription,
        state.control_plane.is_some(),
    )
    .await
    .ok_or_else(|| ApiError::bad_request("Subscription does not exist or is not active"))?;
    authorize_subscription(&state, &headers, &definition, PermissionAction::Read).await?;
    let (coordinator, assignment, definition) =
        member_coordinator_recovering(&state, &request.subscription).await?;
    let lease_ticks = member_lease_ticks(request.lease_ticks)?;
    let lease_epoch = request.lease_epoch.ok_or_else(|| {
        ApiError::bad_request("lease_epoch is required to renew a Subscription member lease")
    })?;
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
    state.demand.record_subscription_op(
        definition.subscription_id,
        crate::demand::SubscriptionTrafficSample {
            op: crate::demand::SubscriptionTrafficOp::Renew,
            records: 0,
            ranges: 0,
            quarantine_add: 0,
            quarantine_remove: 0,
            members: member_state.member_epochs.len() as u64,
            sampled_at_ns: unix_ns(),
        },
    );
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
    headers: HeaderMap,
    Json(request): Json<SubscriptionMemberReleaseRequest>,
) -> Result<Json<SubscriptionMemberResponse>, ApiError> {
    authenticate_credential(&state, &headers)?;
    let definition = subscription_when_applied(
        &state.control,
        &request.subscription,
        state.control_plane.is_some(),
    )
    .await
    .ok_or_else(|| ApiError::bad_request("Subscription does not exist or is not active"))?;
    authorize_subscription(&state, &headers, &definition, PermissionAction::Read).await?;
    let (coordinator, assignment, definition) =
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
    state.demand.record_subscription_op(
        definition.subscription_id,
        crate::demand::SubscriptionTrafficSample {
            op: crate::demand::SubscriptionTrafficOp::Release,
            records: 0,
            ranges: 0,
            quarantine_add: 0,
            quarantine_remove: 0,
            members: member_state.member_epochs.len() as u64,
            sampled_at_ns: unix_ns(),
        },
    );
    let mut response = member_response(
        request.subscription,
        assignment.ownership_epoch,
        member_state,
    );
    response.member_epoch = Some(request.member_epoch);
    Ok(Json(response))
}

async fn subscription_member_transfer(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<SubscriptionMemberTransferRequest>,
) -> Result<Json<SubscriptionMemberResponse>, ApiError> {
    authenticate_credential(&state, &headers)?;
    let definition = subscription_when_applied(
        &state.control,
        &request.subscription,
        state.control_plane.is_some(),
    )
    .await
    .ok_or_else(|| ApiError::bad_request("Subscription does not exist or is not active"))?;
    authorize_subscription(&state, &headers, &definition, PermissionAction::Read).await?;
    let (coordinator, assignment, definition) =
        member_coordinator_recovering(&state, &request.subscription).await?;
    let lease_ticks = member_lease_ticks(request.lease_ticks)?;
    let member_state = member_apply(
        &coordinator,
        request.request_id,
        vec![crate::reader::SubscriptionLeaseOp::Transfer {
            work_id: request.work_id,
            from_member_id: request.member_id,
            from_lease_epoch: request.lease_epoch,
            to_member_id: request.to_member_id,
            to_member_epoch: request.to_member_epoch,
            lease_ticks,
        }],
    )
    .await?;
    state.demand.record_subscription_op(
        definition.subscription_id,
        crate::demand::SubscriptionTrafficSample {
            op: crate::demand::SubscriptionTrafficOp::Transfer,
            records: 0,
            ranges: 0,
            quarantine_add: 0,
            quarantine_remove: 0,
            members: member_state.member_epochs.len() as u64,
            sampled_at_ns: unix_ns(),
        },
    );
    let mut response = member_response(
        request.subscription,
        assignment.ownership_epoch,
        member_state,
    );
    response.member_epoch = Some(request.to_member_epoch);
    response.lease = response.leases.get(&request.work_id).cloned();
    Ok(Json(response))
}

async fn subscription_member_ack(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<SubscriptionMemberAckRequest>,
) -> Result<Json<SubscriptionMemberResponse>, ApiError> {
    authenticate_credential(&state, &headers)?;
    let definition = subscription_when_applied(
        &state.control,
        &request.subscription,
        state.control_plane.is_some(),
    )
    .await
    .ok_or_else(|| ApiError::bad_request("Subscription does not exist or is not active"))?;
    authorize_subscription(&state, &headers, &definition, PermissionAction::Read).await?;
    let (coordinator, assignment, definition) =
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
                if latest.cursor != cursor
                    || latest.positions != positions
                    || latest.quarantine_add != request.quarantine.clone().unwrap_or_default()
                    || latest.quarantine_remove
                        != request.release_quarantine.clone().unwrap_or_default()
                {
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
            let ack_ranges = positions.len() as u64;
            let quarantine_add = request.quarantine.as_ref().map_or(0_u64, |delta| {
                delta.values().map(Vec::len).sum::<usize>() as u64
            });
            let quarantine_remove = request.release_quarantine.as_ref().map_or(0_u64, |delta| {
                delta.values().map(Vec::len).sum::<usize>() as u64
            });
            coordinator
                .acknowledge(
                    &grant,
                    tick,
                    request.request_id,
                    cursor,
                    positions,
                    crate::reader::SubscriptionQuarantineDelta {
                        add: request.quarantine.unwrap_or_default(),
                        remove: request.release_quarantine.unwrap_or_default(),
                    },
                )
                .await
                .map_err(subscription_progress_api_error)?;
            let member_state = read_member_state(&coordinator).await?;
            state.demand.record_subscription_op(
                definition.subscription_id,
                crate::demand::SubscriptionTrafficSample {
                    op: crate::demand::SubscriptionTrafficOp::Ack,
                    records: 0,
                    ranges: ack_ranges,
                    quarantine_add,
                    quarantine_remove,
                    members: member_state.member_epochs.len() as u64,
                    sampled_at_ns: unix_ns(),
                },
            );
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
            state.demand.record_subscription_op(
                definition.subscription_id,
                crate::demand::SubscriptionTrafficSample {
                    op: crate::demand::SubscriptionTrafficOp::Release,
                    records: 0,
                    ranges: 0,
                    quarantine_add: 0,
                    quarantine_remove: 0,
                    members: member_state.member_epochs.len() as u64,
                    sampled_at_ns: unix_ns(),
                },
            );
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
    /// The current delivery attempt for the claimed work — the durable lease
    /// epoch, which advances each time work is reclaimed or re-claimed.
    delivery_attempt: u64,
    /// Unix epoch nanoseconds (decimal string) at which the earliest
    /// delivery-delayed head becomes eligible; `None` when nothing is parked.
    next_delivery_ns: Option<String>,
}

/// Delivers the bounded page of records after the shared Subscription frontier
/// to a member holding a live work lease. The returned per-range positions and
/// Cursor are what the member supplies to `ack`; concurrent members may fetch
/// overlapping windows because acknowledgement is fenced by the committed
/// Cursor, not by delivery.
async fn subscription_member_fetch(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<SubscriptionMemberFetchRequest>,
) -> Result<Json<SubscriptionMemberFetchResponse>, ApiError> {
    let limit = request
        .limit
        .unwrap_or(64)
        .clamp(1, SUBSCRIPTION_MEMBER_FETCH_MAX_LIMIT);
    authenticate_credential(&state, &headers)?;
    let definition = subscription_when_applied(
        &state.control,
        &request.subscription,
        state.control_plane.is_some(),
    )
    .await
    .ok_or_else(|| ApiError::bad_request("Subscription does not exist or is not active"))?;
    authorize_subscription(&state, &headers, &definition, PermissionAction::Read).await?;
    let (coordinator, progress_assignment, definition) =
        member_coordinator(&state, &request.subscription).await?;
    // Fetch is the hot member path, so it resolves the coordinator without
    // recovery; a committed-but-unapplied cutover still heals here before the
    // frontier coverage check would strand the member.
    if state.control_plane.is_some() {
        if let Some(pending) = state
            .control
            .pending_subscription_cutover(definition.subscription_id)
            .await
        {
            apply_pending_subscription_cutover(
                &state,
                &pending,
                &coordinator,
                &progress_assignment,
            )
            .await?;
        }
    }
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

            code: Some("lease.stale_lease"),
        });
    }
    let frontier = coordinator
        .read_committed()
        .await
        .map_err(subscription_progress_api_error)?
        .ok_or_else(|| {
            ApiError::unavailable("Subscription progress frontier is not established")
        })?;
    let eligible_ingest_ns = definition
        .delivery_delay_ns
        .map(|delay| unix_ns().saturating_sub(i64::try_from(delay).unwrap_or(i64::MAX)));
    let (delivered, positions, parked_ingest_ns) =
        fetch_frontier_page(&state, &frontier, limit, eligible_ingest_ns).await?;
    let next_delivery_ns =
        parked_ingest_ns
            .zip(definition.delivery_delay_ns)
            .map(|(ingest, delay)| {
                ingest
                    .saturating_add(i64::try_from(delay).unwrap_or(i64::MAX))
                    .to_string()
            });
    let delivered_records = delivered.len() as u64;
    let cursor = delivered.last().map(|(cursor, _)| cursor.clone());
    let source = record_source_by_id(&state, frontier.feed_id).await;
    let records = delivered
        .into_iter()
        .map(|(frame_cursor, record)| {
            record_response(
                crate::domain::CursorRecord {
                    cursor: frame_cursor,
                    record,
                },
                source.clone(),
            )
        })
        .collect();
    state.demand.record_subscription_op(
        definition.subscription_id,
        crate::demand::SubscriptionTrafficSample {
            op: crate::demand::SubscriptionTrafficOp::Fetch,
            records: delivered_records,
            ranges: 0,
            quarantine_add: 0,
            quarantine_remove: 0,
            members: members.member_epochs.len() as u64,
            sampled_at_ns: unix_ns(),
        },
    );
    Ok(Json(SubscriptionMemberFetchResponse {
        request_id: request.request_id,
        records,
        cursor,
        positions,
        ownership_epoch: progress_assignment.ownership_epoch,
        delivery_attempt: request.lease_epoch,
        next_delivery_ns,
    }))
}

/// Resolve the last committed frame Cursor in every Active Range plus the
/// globally newest (ingest_time, message_id) head, shared by Subscription
/// `now` starts and `now` seeks.
async fn resolve_tail_positions(
    state: &AppState,
    range_assignments: &[ActiveRangeAssignment],
) -> Result<(Option<((i64, Uuid), String)>, BTreeMap<RangeId, String>), ApiError> {
    let mut positions = BTreeMap::new();
    let mut tail: Option<((i64, Uuid), String)> = None;
    for range_assignment in range_assignments {
        let page = fetch_range_page(
            state,
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
                    ApiError::unavailable("current owner returned an invalid frame encoding")
                })?;
            let record = decode_record(&decoded)
                .map_err(|error| ApiError::unavailable(error.to_string()))?;
            let key = (record.ingest_time_ns, record.message_id);
            if tail.as_ref().is_none_or(|(current, _)| key > *current) {
                tail = Some((key, frame.cursor.clone()));
            }
        }
    }
    Ok((tail, positions))
}

/// Resolve per-Range positions behind one Feed Cursor for a Subscription
/// start: every Active Range resumes after the last frame it contributed
/// to the merged-order prefix at-or-before the Cursor, the same skip a
/// named Reader's `after` read applies. Frames on other Ranges that merge
/// after the Cursor stay undelivered because the min-head pick proves they
/// do not sort earlier. An unknown, uncommitted, or foreign-Feed Cursor
/// fails closed exactly like the Feed-read contract.
async fn resolve_cursor_positions(
    state: &AppState,
    range_assignments: &[ActiveRangeAssignment],
    after: &str,
) -> Result<BTreeMap<RangeId, String>, ApiError> {
    let mut streams: Vec<RangeFrameStream> = range_assignments
        .iter()
        .map(|assignment| RangeFrameStream::new(state, assignment))
        .collect();
    let mut heads: Vec<Option<((i64, Uuid), StoredRangeFrame)>> =
        (0..streams.len()).map(|_| None).collect();
    let mut positions: BTreeMap<RangeId, String> = range_assignments
        .iter()
        .map(|assignment| (assignment.range_id, String::new()))
        .collect();
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
        positions.insert(range_assignments[index].range_id, frame.cursor.clone());
        if frame.cursor == after {
            return Ok(positions);
        }
    }
    Err(ApiError::bad_request(
        "Cursor is unknown, uncommitted, or belongs to another Feed",
    ))
}

/// Resolve one opaque record Cursor into per-range seek positions. A
/// single-range source positions exactly: the owner maps the Cursor to its
/// committed frame and delivery resumes after it. A split Feed keeps the
/// honest refusal — one Cursor cannot describe every Active Range position.
async fn resolve_seek_cursor_positions(
    state: &AppState,
    range_assignments: &[ActiveRangeAssignment],
    cursor: &str,
) -> Result<BTreeMap<RangeId, String>, ApiError> {
    if range_assignments.len() != 1 {
        return Err(ApiError::bad_request(
            "one Cursor cannot describe every Active Range position; seek by beginning, now, or timestamp on a split Feed",
        ));
    }
    let assignment = &range_assignments[0];
    let page = fetch_range_page(
        state,
        ReadRangePageRequest {
            assignment: assignment.clone(),
            after: None,
            expected_commit: None,
            after_cursor: Some(cursor.to_owned()),
            tail_count: None,
            single_range: true,
            page_limit: Some(1),
        },
    )
    .await?;
    let resolved = page.resolved_after.ok_or_else(|| {
        ApiError::bad_request("Cursor did not resolve inside the committed range")
    })?;
    if resolved.value() > page.committed.value() {
        return Err(ApiError::bad_request(
            "Cursor points beyond the committed range boundary",
        ));
    }
    Ok(BTreeMap::from([(assignment.range_id, cursor.to_owned())]))
}

/// Resolve per-Range positions so the next delivered record in each Active
/// Range is the first one accepted at-or-after `threshold` on the ingest
/// clock. Ingest time is monotonic within a Range, so the scan stops at the
/// first eligible frame; the position itself is the Cursor of the last
/// earlier frame (empty when the Range starts eligible).
async fn resolve_timestamp_positions(
    state: &AppState,
    range_assignments: &[ActiveRangeAssignment],
    threshold: i64,
) -> Result<BTreeMap<RangeId, String>, ApiError> {
    let mut positions = BTreeMap::new();
    for range_assignment in range_assignments {
        let mut stream = RangeFrameStream::new(state, range_assignment);
        let mut last_before = String::new();
        while let Some(frame) = stream.next_frame().await? {
            let record = decode_record(&frame.frame)
                .map_err(|error| ApiError::unavailable(error.to_string()))?;
            if record.ingest_time_ns >= threshold {
                break;
            }
            last_before = frame.cursor;
        }
        positions.insert(range_assignment.range_id, last_before);
    }
    Ok(positions)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SubscriptionSeekRequest {
    subscription: String,
    request_id: Uuid,
    /// `beginning`, `now`, or `timestamp`.
    position: String,
    /// Decimal-string epoch nanoseconds on the ingest clock; required for
    /// `timestamp` seeks.
    timestamp_ns: Option<String>,
    /// The opaque record Cursor to resume after; required for `cursor`
    /// seeks, which resolve only on a single-range source.
    #[serde(default)]
    cursor: Option<String>,
}

#[derive(Serialize)]
struct SubscriptionSeekResponse {
    request_id: Uuid,
    cursor: String,
    positions: BTreeMap<RangeId, String>,
    ownership_epoch: u64,
}

/// Reposition a Subscription's shared frontier under one committed progress
/// mutation. A seek is an operator action, not a member operation: it is
/// refused while any member work lease is active, and the commit CAS-fences
/// on the current frontier Cursor so a racing acknowledgement cannot
/// silently overwrite the reposition. `request_id` replays idempotently.
async fn subscription_seek(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<SubscriptionSeekRequest>,
) -> Result<Json<SubscriptionSeekResponse>, ApiError> {
    authenticate_credential(&state, &headers)?;
    let definition = subscription_when_applied(
        &state.control,
        &request.subscription,
        state.control_plane.is_some(),
    )
    .await
    .ok_or_else(|| ApiError::bad_request("Subscription does not exist or is not active"))?;
    authorize_subscription(&state, &headers, &definition, PermissionAction::Manage).await?;
    let (coordinator, assignment, definition) =
        member_coordinator_inner(&state, &request.subscription).await?;
    let source_feed = definition.source.feed_id().ok_or_else(|| ApiError {
        status: StatusCode::CONFLICT,
        message:
            "Subscription has a Domain source; per-Feed seeking arrives with Domain member delivery"
                .to_owned(),
        code: None,
    })?;
    let frontier = coordinator
        .read_committed()
        .await
        .map_err(subscription_progress_api_error)?
        .ok_or_else(|| ApiError {
            status: StatusCode::CONFLICT,
            message: "Subscription has no committed frontier; the first member join establishes it"
                .to_owned(),
            code: None,
        })?;
    // The committed Cursor marker carries the request identity AND the
    // requested position so a replayed request_id with a different seek
    // conflicts instead of impersonating the earlier one.
    let seek_cursor = format!(
        "seek:{}:{}:{}{}",
        request.request_id,
        request.position.to_ascii_lowercase(),
        request.timestamp_ns.as_deref().unwrap_or(""),
        if request.position.eq_ignore_ascii_case("cursor") {
            format!(":{}", request.cursor.as_deref().unwrap_or(""))
        } else {
            String::new()
        }
    );
    if frontier.request_id == request.request_id {
        if frontier.cursor != seek_cursor {
            return Err(ApiError {
                status: StatusCode::CONFLICT,
                message: "request_id was already used with a different Subscription operation"
                    .to_owned(),
                code: None,
            });
        }
        return Ok(Json(SubscriptionSeekResponse {
            request_id: request.request_id,
            cursor: frontier.cursor,
            positions: frontier.positions,
            ownership_epoch: frontier.ownership_epoch,
        }));
    }
    // Validate the request shape before checking leases so malformed seeks
    // are BAD_REQUEST rather than masked by the in-flight-work conflict.
    enum SeekPosition {
        Beginning,
        Now,
        Timestamp(i64),
        Cursor(String),
    }
    let position = match request.position.to_ascii_lowercase().as_str() {
        "beginning" => SeekPosition::Beginning,
        "now" => SeekPosition::Now,
        "timestamp" => SeekPosition::Timestamp(
            request
                .timestamp_ns
                .as_deref()
                .ok_or_else(|| ApiError::bad_request("timestamp seeks require timestamp_ns"))?
                .parse::<i64>()
                .map_err(|_| {
                    ApiError::bad_request(
                        "timestamp_ns must be signed epoch nanoseconds as a decimal string",
                    )
                })?,
        ),
        "cursor" => SeekPosition::Cursor(
            request
                .cursor
                .as_deref()
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
                .ok_or_else(|| ApiError::bad_request("cursor seeks require a non-empty cursor"))?,
        ),
        _ => {
            return Err(ApiError::bad_request(
                "position must be beginning, now, timestamp, or cursor",
            ))
        }
    };
    let members = read_member_state(&coordinator).await?;
    if !members.leases.is_empty() {
        return Err(ApiError {
            status: StatusCode::CONFLICT,
            message: format!(
                "Subscription members hold {} active work leases; release them or let them expire before seeking",
                members.leases.len()
            ),
            code: None,
        });
    }
    let range_assignments = state
        .control
        .active_range_assignments_for_feed(source_feed)
        .await;
    if range_assignments.is_empty() {
        return Err(ApiError::unavailable(
            "Subscription source Feed has no Active Range placement",
        ));
    }
    let positions = match position {
        SeekPosition::Beginning => range_assignments
            .iter()
            .map(|assignment| (assignment.range_id, String::new()))
            .collect(),
        SeekPosition::Now => resolve_tail_positions(&state, &range_assignments).await?.1,
        SeekPosition::Timestamp(threshold) => {
            resolve_timestamp_positions(&state, &range_assignments, threshold).await?
        }
        SeekPosition::Cursor(cursor) => {
            resolve_seek_cursor_positions(&state, &range_assignments, &cursor).await?
        }
    };
    let mutation = crate::reader::SubscriptionProgressMutation {
        subscription_id: assignment.subscription_id,
        feed_id: source_feed,
        ownership_epoch: assignment.ownership_epoch,
        sequence: frontier
            .sequence
            .checked_add(1)
            .ok_or_else(|| ApiError::unavailable("Subscription frontier sequence overflowed"))?,
        request_id: request.request_id,
        expected_cursor: Some(frontier.cursor.clone()),
        cursor: seek_cursor.clone(),
        positions: positions.clone(),
        // A seek repositions delivery; quarantine evidence belongs to the
        // Subscription's processing history and carries forward so a
        // replay through the same positions can see it again.
        quarantined: frontier.quarantined.clone(),
        quarantine_add: BTreeMap::new(),
        quarantine_remove: BTreeMap::new(),
        tick: frontier.tick,
        lease_ops: Vec::new(),
    };
    coordinator
        .apply(mutation)
        .await
        .map_err(subscription_progress_api_error)?;
    state.demand.record_subscription_op(
        definition.subscription_id,
        crate::demand::SubscriptionTrafficSample {
            op: crate::demand::SubscriptionTrafficOp::Seek,
            records: 0,
            ranges: 0,
            quarantine_add: 0,
            quarantine_remove: 0,
            members: members.member_epochs.len() as u64,
            sampled_at_ns: unix_ns(),
        },
    );
    Ok(Json(SubscriptionSeekResponse {
        request_id: request.request_id,
        cursor: seek_cursor,
        positions,
        ownership_epoch: assignment.ownership_epoch,
    }))
}

/// Merge the committed pages of every Active Range after a committed
/// Subscription frontier into one bounded page in (ingest_time, message_id)
/// order. Returns each record with its frame Cursor, the updated per-range
/// positions, and the earliest delivery-delayed head's ingest timestamp when
/// a holdback parked one; member fetches and Pipe drives share this core so
/// both see identical delivery order and frontier evidence.
///
/// `eligible_ingest_ns` is the Subscription's delivery-delay cutoff: a head
/// whose ingest timestamp is newer than the cutoff stays parked. Parked
/// heads are simply never selected, so later records in the same Active
/// Range cannot overtake them and the frontier never skips them; records in
/// other ranges that are already eligible continue to deliver.
async fn fetch_frontier_page(
    state: &AppState,
    frontier: &crate::reader::SubscriptionProgressMutation,
    limit: usize,
    eligible_ingest_ns: Option<i64>,
) -> Result<
    (
        Vec<(String, StoredRecord)>,
        BTreeMap<RangeId, String>,
        Option<i64>,
    ),
    ApiError,
> {
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

                code: Some("conflict"),
            });
        };
        let mut stream = RangeFrameStream::new(state, range_assignment);
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

            code: Some("conflict"),
        });
    }

    let mut heads: Vec<Option<((i64, Uuid), StoredRangeFrame)>> =
        (0..streams.len()).map(|_| None).collect();
    let mut records: Vec<(String, StoredRecord)> = Vec::new();
    let mut positions = frontier.positions.clone();
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
        let Some(index) = earliest_eligible_head(&heads, eligible_ingest_ns) else {
            break;
        };
        let Some((_, frame)) = heads[index].take() else {
            continue;
        };
        if records.len() >= limit {
            break;
        }
        let frame_bytes = logical_frame_bytes(&frame.frame)?.saturating_add(frame.cursor.len());
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
        records.push((frame.cursor, record));
    }
    // The earliest parked head tells the caller when the next record becomes
    // eligible; with no cutoff nothing can be parked.
    let parked_ingest_ns = eligible_ingest_ns.and_then(|cutoff| {
        heads
            .iter()
            .filter_map(|head| head.as_ref().map(|(key, _)| key.0))
            .filter(|ingest| *ingest > cutoff)
            .min()
    });
    Ok((records, positions, parked_ingest_ns))
}

/// Pick the globally earliest head that is eligible for delivery under the
/// Subscription's optional delivery-delay cutoff. A head newer than the
/// cutoff stays parked: it is never selected, so records behind it in the
/// same Active Range cannot overtake it and positions only ever advance over
/// a delivered contiguous prefix.
fn earliest_eligible_head(
    heads: &[Option<((i64, Uuid), StoredRangeFrame)>],
    eligible_ingest_ns: Option<i64>,
) -> Option<usize> {
    heads
        .iter()
        .enumerate()
        .filter_map(|(index, head)| {
            head.as_ref().and_then(|(key, _)| {
                if eligible_ingest_ns.is_some_and(|cutoff| key.0 > cutoff) {
                    None
                } else {
                    Some((index, *key))
                }
            })
        })
        .min_by_key(|(index, key)| (*key, *index))
        .map(|(index, _)| index)
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PipeDriveRequest {
    pipe_id: Uuid,
    subscription_id: Uuid,
    owner: StorageNodeId,
    receiver: StorageNodeId,
    ownership_epoch: u64,
    limit: Option<usize>,
}

#[derive(Serialize)]
struct PipeDriveResponse {
    pipe_id: Uuid,
    applied: Option<crate::effect::EffectMutation>,
}

/// Drive one declared Pipe through a bounded Declare -> apply cycle on the
/// Subscription's current progress owner. The drive runs where the consumed
/// frontier and the effect journal are placed, and the cycle fences on the
/// frontier CAS rather than a member work lease: two drivers seeing the same
/// frontier produce identical mutations (the journal replays them), and one
/// seeing an older frontier loses the CAS when committing progress.
async fn pipe_drive_local(
    State(state): State<AppState>,
    peer: Option<axum::Extension<AuthenticatedSubscriptionPeer>>,
    Json(request): Json<PipeDriveRequest>,
) -> Result<Json<PipeDriveResponse>, ApiError> {
    check_subscription_peer(&state, peer.as_ref().map(|peer| &peer.0))?;
    let pipe = state
        .control
        .active_pipe_by_id(request.pipe_id)
        .await
        .ok_or_else(|| ApiError::conflict("Pipe does not exist or is not active"))?;
    if pipe.subscription_id != request.subscription_id {
        return Err(ApiError::conflict(
            "Pipe does not consume the requested Subscription",
        ));
    }
    if !matches!(
        pipe.operation,
        crate::control::PipeOperation::Forward
            | crate::control::PipeOperation::Window { .. }
            | crate::control::PipeOperation::Enrich { .. }
    ) {
        return Err(ApiError::conflict("Pipe operation is not driveable yet"));
    }
    let assignment = state
        .control
        .active_subscription_progress_assignment_by_id(request.subscription_id)
        .await
        .ok_or_else(|| ApiError::unavailable("Subscription progress placement is unavailable"))?;
    if assignment.owner != request.receiver || assignment.owner != request.owner {
        return Err(ApiError::conflict(
            "Pipe drive must run on the current progress owner",
        ));
    }
    if assignment.ownership_epoch != request.ownership_epoch {
        return Err(ApiError::conflict(
            "Pipe drive epoch does not match the current progress placement",
        ));
    }
    if state.storage_node_id.as_ref() != Some(&request.receiver) {
        return Err(ApiError::conflict(format!(
            "Node {} is not the drive receiver {}",
            state
                .storage_node_id
                .as_ref()
                .map(|node| node.as_str())
                .unwrap_or("unconfigured"),
            request.receiver
        )));
    }
    let limit = request
        .limit
        .unwrap_or(64)
        .clamp(1, SUBSCRIPTION_MEMBER_FETCH_MAX_LIMIT);
    let applied = drive_pipe(
        &state,
        &pipe,
        request.subscription_id,
        request.ownership_epoch,
        limit,
    )
    .await?;
    Ok(Json(PipeDriveResponse {
        pipe_id: pipe.pipe_id,
        applied,
    }))
}

/// Drive one Pipe cycle on this Node over the live Subscription fetch core:
/// coordinators bind to the Subscription's current placement and the sink
/// writes through the same owner-routed append path as the apply endpoint.
async fn drive_pipe(
    state: &AppState,
    pipe: &crate::control::PipeDefinition,
    subscription_id: Uuid,
    ownership_epoch: u64,
    limit: usize,
) -> Result<Option<crate::effect::EffectMutation>, ApiError> {
    let assignment = state
        .control
        .active_subscription_progress_assignment_by_id(subscription_id)
        .await
        .ok_or_else(|| ApiError::unavailable("Subscription progress placement is unavailable"))?;
    if assignment.ownership_epoch != ownership_epoch {
        return Err(ApiError::conflict(
            "Pipe drive epoch does not match the current progress placement",
        ));
    }
    let definition = state
        .control
        .active_subscription_by_id(subscription_id)
        .await
        .ok_or_else(|| ApiError::unavailable("Subscription definition is unavailable"))?;
    let delivery_delay_ns = definition.delivery_delay_ns;
    let journal = effect_coordinator_for(state, assignment.clone())?;
    let (progress, _) = member_assignment_coordinator(state, assignment.clone())?;
    let (sink_progress, _) = member_assignment_coordinator(state, assignment.clone())?;
    let sink = LiveEffectApply {
        state,
        progress: sink_progress,
        subscription_id,
        ownership_epoch,
        sweep_depth: 0,
    };
    let driver = crate::pipe::PipeDriver::new(pipe.clone(), Arc::new(journal), Arc::new(progress));
    let started = std::time::Instant::now();
    let tally = Arc::new(std::sync::Mutex::new(PipeFetchTally::default()));
    let tally_cell = Arc::clone(&tally);
    let result = driver
        .drive_once(
            limit,
            |frontier, limit| {
                let frontier = frontier.clone();
                let pipe = pipe.clone();
                let tally = Arc::clone(&tally_cell);
                async move {
                    let eligible_ingest_ns = delivery_delay_ns.map(|delay| {
                        unix_ns().saturating_sub(i64::try_from(delay).unwrap_or(i64::MAX))
                    });
                    let (delivered, positions, _parked) =
                        fetch_frontier_page(state, &frontier, limit, eligible_ingest_ns)
                            .await
                            .map_err(|_| crate::effect::EffectJournalError::Unavailable)?;
                    let cursor = delivered
                        .last()
                        .map(|(cursor, _)| cursor.clone())
                        .unwrap_or_else(|| frontier.cursor.clone());
                    let mut records = Vec::with_capacity(delivered.len());
                    for (_, record) in delivered {
                        if let Ok(mut tally) = tally.lock() {
                            tally.fetch_bytes = tally.fetch_bytes.saturating_add(
                                (record.key.len()
                                    + record.payload.len()
                                    + record
                                        .metadata
                                        .values()
                                        .map(|value| value.len())
                                        .sum::<usize>()) as u64,
                            );
                        }
                        // Enrich lookups resolve at fetch time: the outcome
                        // rides inside the Declare, so an applied output is
                        // byte-identical across replays and raced drives
                        // converge on whichever Declare committed first.
                        let resolution = if let crate::control::PipeOperation::Enrich {
                            key,
                            freshness,
                            named,
                            ..
                        } = &pipe.operation
                        {
                            Some(
                                resolve_enrich(
                                    state,
                                    &pipe,
                                    key,
                                    *freshness,
                                    named.as_deref(),
                                    &record,
                                )
                                .await
                                .map_err(|_| crate::effect::EffectJournalError::Unavailable)?,
                            )
                        } else {
                            None
                        };
                        records.push(crate::pipe::FetchedRecord {
                            message_id: record.message_id,
                            key_base64: STANDARD.encode(&record.key),
                            payload_base64: STANDARD.encode(&record.payload),
                            metadata_base64: record
                                .metadata
                                .into_iter()
                                .map(|(name, value)| (name, STANDARD.encode(value)))
                                .collect(),
                            event_time_ns: record.event_time_ns,
                            ingest_time_ns: record.ingest_time_ns,
                            resolution,
                        });
                    }
                    // Window pane-close and expiry resolve at fetch time
                    // alongside the page; the outcome rides inside the
                    // Declare so a replayed effect never re-scans.
                    let (emissions, expired) = match &pipe.operation {
                        crate::control::PipeOperation::Window {
                            window_ns,
                            max_late_ns,
                            expire_ns,
                            ..
                        } if pipe.output_feed_id.is_some() || expire_ns.is_some() => {
                            resolve_window_emissions(
                                state,
                                &pipe,
                                *window_ns,
                                *max_late_ns,
                                *expire_ns,
                                &records,
                            )
                            .await
                            .map_err(|_| crate::effect::EffectJournalError::Unavailable)?
                        }
                        _ => (Vec::new(), Vec::new()),
                    };
                    // `LIMIT KEYS`/`LIMIT BYTES` resolve alongside the
                    // page on the same serving replica: the admitted and
                    // refused split journals inside the Declare, so a
                    // replayed effect never re-scans.
                    let (refused, pane_rows, pane_rows_saturated) = match &pipe.operation {
                        crate::control::PipeOperation::Window {
                            key_limit,
                            byte_limit,
                            key,
                            ..
                        } if key_limit.is_some() || byte_limit.is_some() => {
                            let scan_limit =
                                crate::pipe::window_scan_row_limit(*key_limit, *byte_limit);
                            let (rows, saturated) =
                                resolve_window_pane_rows(state, &pipe, scan_limit)
                                    .await
                                    .map_err(|_| crate::effect::EffectJournalError::Unavailable)?;
                            let refused = match key_limit {
                                Some(limit) => {
                                    let active: std::collections::BTreeSet<Vec<u8>> =
                                        rows.iter().map(|(group, _)| group.clone()).collect();
                                    crate::pipe::window_refused_groups(
                                        &records, key, *limit, &active, saturated,
                                    )
                                }
                                None => Vec::new(),
                            };
                            let pane_rows = if byte_limit.is_some() {
                                rows
                            } else {
                                Default::default()
                            };
                            (refused, pane_rows, saturated && byte_limit.is_some())
                        }
                        _ => (Vec::new(), Default::default(), false),
                    };
                    let page = crate::pipe::FetchedPage {
                        cursor,
                        records,
                        positions,
                        emissions,
                        expired,
                        refused,
                        pane_rows,
                        pane_rows_saturated,
                    };
                    if let Ok(mut tally) = tally.lock() {
                        tally.records = page.records.len() as u64;
                        tally.emissions = page.emissions.len() as u64;
                        tally.pane_rows = page.pane_rows.len() as u64;
                        tally.pane_bytes = page
                            .pane_rows
                            .iter()
                            .map(|(group, _)| (4 + group.len() + 8 + 24) as u64)
                            .sum();
                        tally.page_limit = limit as u64;
                    }
                    Ok(page)
                }
            },
            || async move {
                bootstrap_pipe_frontier(state, &definition, &assignment)
                    .await
                    .map_err(pipe_bootstrap_error)
            },
            &sink,
        )
        .await
        .map_err(effect_journal_api_error);
    record_pipe_drive_telemetry(state, pipe, started, &tally, limit, &result);
    result
}

/// Count records a Window Pipe diverted into sentinel rows this drive:
/// `LIMIT KEYS`/`LIMIT BYTES` refusals and lateness quarantine all
/// accumulate `count_delta` under row keys with the `WINDOW_LATE_PANE`
/// pane index, so the applied mutations carry the honest refused-Record
/// total — fetch-time `page.refused` only names refused groups.
fn window_sentinel_refusals(state_mutations: &[crate::effect::EffectStateMutation]) -> u64 {
    state_mutations
        .iter()
        .map(|mutation| match &mutation.op {
            crate::state_store::StateStoreMutationOp::AccumulateRows { entries } => entries
                .iter()
                .filter(|entry| {
                    crate::pipe::window_pane_row_key_parse(&entry.key)
                        .is_some_and(|(_, pane)| pane == crate::pipe::WINDOW_LATE_PANE)
                })
                .map(|entry| entry.count_delta)
                .sum(),
            _ => 0,
        })
        .sum()
}

/// Per-cycle fetch measurements the drive closure reports back through a
/// shared cell: the closure runs inside `drive_once`, so its counts land
/// before the outcome is known and merge with the result below.
#[derive(Clone, Default)]
struct PipeFetchTally {
    records: u64,
    fetch_bytes: u64,
    emissions: u64,
    pane_rows: u64,
    pane_bytes: u64,
    page_limit: u64,
}

/// Fold a finished drive into the Pipe's Node-local attribution row on
/// `DemandMetrics` — the honest scope, since a compute-lease move
/// restarts driving on the new owner.
fn record_pipe_drive_telemetry(
    state: &AppState,
    pipe: &crate::control::PipeDefinition,
    started: std::time::Instant,
    tally: &std::sync::Mutex<PipeFetchTally>,
    page_limit: usize,
    result: &Result<Option<crate::effect::EffectMutation>, ApiError>,
) {
    let wall_ns = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
    let counts = tally
        .lock()
        .map(|counts| counts.clone())
        .unwrap_or_default();
    let (outcome, outputs, state_mutations, refused) = match result {
        Ok(Some(mutation)) => match &mutation.transition {
            crate::effect::EffectTransition::Applied {
                outputs,
                state_mutations,
                ..
            } => (
                crate::demand::PipeDriveResult::Applied,
                outputs.len() as u64,
                state_mutations.len() as u64,
                window_sentinel_refusals(state_mutations),
            ),
            _ => (crate::demand::PipeDriveResult::Applied, 0, 0, 0),
        },
        Ok(None) => (crate::demand::PipeDriveResult::Idle, 0, 0, 0),
        Err(error) => (
            crate::demand::PipeDriveResult::Failed(error.message.clone()),
            0,
            0,
            0,
        ),
    };
    state.demand.record_pipe_drive(
        pipe.pipe_id,
        crate::demand::PipeDriveSample {
            result: outcome,
            outputs,
            state_mutations,
            records: counts.records,
            fetch_bytes: counts.fetch_bytes,
            refused,
            emissions: counts.emissions,
            pane_rows: counts.pane_rows,
            pane_bytes: counts.pane_bytes,
            page_limit: counts.page_limit.max(page_limit as u64),
            wall_ns,
            sampled_at_ns: unix_ns(),
        },
    );
}

/// Build the sequence-1 frontier mutation a Pipe driver commits when no
/// member has ever joined the consumed Subscription: the Subscription's
/// declared start decides `beginning` vs `now` exactly as first-member join
/// does, and the request identity is deterministic so racing drivers replay
/// instead of forking.
async fn bootstrap_pipe_frontier(
    state: &AppState,
    definition: &crate::control::SubscriptionDefinition,
    assignment: &crate::reader::SubscriptionProgressAssignment,
) -> Result<crate::reader::SubscriptionProgressMutation, ApiError> {
    let source_feed = definition.source.feed_id().ok_or_else(|| ApiError {
        status: StatusCode::CONFLICT,
        message: "Subscription has a Domain source; member sessions arrive with Domain-wide progress tracking".to_owned(),
        code: None,
    })?;
    let range_assignments = state
        .control
        .active_range_assignments_for_feed(source_feed)
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
            let (tail, positions) = resolve_tail_positions(state, &range_assignments).await?;
            (
                tail.map(|(_, cursor)| cursor)
                    .unwrap_or_else(|| "now".to_owned()),
                positions,
            )
        }
        crate::control::ReaderStart::Cursor(_) | crate::control::ReaderStart::Timestamp(_) => {
            return Err(ApiError {
                status: StatusCode::CONFLICT,
                message: "Pipe drives support beginning and now starts; explicit-Cursor and timestamp starts are not driveable yet".to_owned(),

                code: None,
            })
        }
    };
    Ok(crate::reader::SubscriptionProgressMutation {
        subscription_id: assignment.subscription_id,
        feed_id: source_feed,
        ownership_epoch: assignment.ownership_epoch,
        sequence: 1,
        request_id: crate::pipe::pipe_frontier_request_id(assignment.subscription_id, source_feed),
        expected_cursor: None,
        cursor: start_cursor,
        positions,
        quarantined: BTreeMap::new(),
        quarantine_add: BTreeMap::new(),
        quarantine_remove: BTreeMap::new(),
        tick: 0,
        lease_ops: Vec::new(),
    })
}

fn pipe_bootstrap_error(error: ApiError) -> crate::effect::EffectJournalError {
    if error.status.is_server_error() {
        crate::effect::EffectJournalError::Unavailable
    } else {
        crate::effect::EffectJournalError::Conflict
    }
}

/// One Pipe cycle result for the Node-local supervisor tick.
#[derive(Debug)]
pub enum PipeDriveOutcome {
    /// This Node is not the Subscription's progress owner; the owner drives.
    NotOwner,
    /// The input was caught up; nothing applied.
    Idle,
    /// A cycle committed `Declare -> Applied` for this effect.
    Applied { pipe_id: Uuid, effect_id: Uuid },
    /// The cycle failed; deterministic identities make the next retry safe.
    Failed { pipe_id: Uuid, message: String },
}

/// One supervisor tick: drive every active Pipe whose progress owner is this
/// Node. Ownership lives in the Subscription's placement, so owner loss or
/// movement simply changes which Node's tick drives the Pipe next; every
/// cycle is fenced by the frontier CAS and deterministic identities, so a
/// drive split across an ownership change never duplicates or loses work.
pub async fn drive_local_pipes(state: &AppState, limit: usize) -> Vec<PipeDriveOutcome> {
    let Some(local) = state.storage_node_id.clone() else {
        return Vec::new();
    };
    let pipes = state.control.active_pipes().await;
    let mut outcomes = Vec::with_capacity(pipes.len());
    for pipe in pipes {
        // A paused Pipe keeps its durable declaration but yields no
        // drive — the frontier stays put so redelivery stays replay-safe.
        if pipe.stage == crate::control::PipeStage::Paused {
            continue;
        }
        if !matches!(
            pipe.operation,
            crate::control::PipeOperation::Forward
                | crate::control::PipeOperation::Window { .. }
                | crate::control::PipeOperation::Enrich { .. }
        ) {
            outcomes.push(PipeDriveOutcome::Failed {
                pipe_id: pipe.pipe_id,
                message: "Pipe operation is not driveable yet".to_owned(),
            });
            continue;
        }
        let Some(lease) = state.control.pipe_compute_lease(pipe.pipe_id).await else {
            outcomes.push(PipeDriveOutcome::Failed {
                pipe_id: pipe.pipe_id,
                message: "Pipe compute lease is unavailable".to_owned(),
            });
            continue;
        };
        if lease.owner != local {
            outcomes.push(PipeDriveOutcome::NotOwner);
            continue;
        }
        let Some(assignment) = state
            .control
            .active_subscription_progress_assignment_by_id(pipe.subscription_id)
            .await
        else {
            outcomes.push(PipeDriveOutcome::Failed {
                pipe_id: pipe.pipe_id,
                message: "Subscription progress placement is unavailable".to_owned(),
            });
            continue;
        };
        match drive_pipe(
            state,
            &pipe,
            pipe.subscription_id,
            assignment.ownership_epoch,
            limit,
        )
        .await
        {
            Ok(Some(applied)) => outcomes.push(PipeDriveOutcome::Applied {
                pipe_id: pipe.pipe_id,
                effect_id: applied.effect_id,
            }),
            Ok(None) => outcomes.push(PipeDriveOutcome::Idle),
            Err(error) => outcomes.push(PipeDriveOutcome::Failed {
                pipe_id: pipe.pipe_id,
                message: error.message,
            }),
        }
    }
    outcomes
}

async fn subscription_member_state(
    State(state): State<AppState>,
    headers: HeaderMap,
    axum::extract::Query(query): axum::extract::Query<SubscriptionMemberStateQuery>,
) -> Result<Json<SubscriptionMemberResponse>, ApiError> {
    authenticate_credential(&state, &headers)?;
    let definition = subscription_when_applied(
        &state.control,
        &query.subscription,
        state.control_plane.is_some(),
    )
    .await
    .ok_or_else(|| ApiError::bad_request("Subscription does not exist or is not active"))?;
    authorize_subscription(&state, &headers, &definition, PermissionAction::Read).await?;
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

fn state_store_api_error(error: crate::state_store::StateStoreMutationError) -> ApiError {
    let code = error.code();
    let status = match error {
        crate::state_store::StateStoreMutationError::Unavailable
        | crate::state_store::StateStoreMutationError::AmbiguousCommit
        | crate::state_store::StateStoreMutationError::NoQuorum
        | crate::state_store::StateStoreMutationError::ApplyFailed
        | crate::state_store::StateStoreMutationError::Engine(_)
        | crate::state_store::StateStoreMutationError::Serialization(_) => {
            StatusCode::SERVICE_UNAVAILABLE
        }
        crate::state_store::StateStoreMutationError::NotFound => StatusCode::NOT_FOUND,
        crate::state_store::StateStoreMutationError::TooLarge => StatusCode::PAYLOAD_TOO_LARGE,
        _ => StatusCode::CONFLICT,
    };
    ApiError {
        status,
        message: error.to_string(),
        code: Some(code),
    }
}

async fn state_store_journal_prepare_local(
    State(state): State<AppState>,
    peer: Option<axum::Extension<AuthenticatedSubscriptionPeer>>,
    Json(request): Json<crate::state_store::StateStorePrepareRequest>,
) -> Result<
    Json<crate::state_store::StateStoreReplicaReply<crate::state_store::StateStorePrepareVote>>,
    ApiError,
> {
    check_subscription_peer(&state, peer.as_ref().map(|peer| &peer.0))?;
    let service = state
        .state_store_journal
        .as_ref()
        .ok_or_else(|| ApiError::unavailable("StateStore journal replica is not configured"))?;
    Ok(Json(
        service
            .prepare(request)
            .await
            .map_err(state_store_api_error)?,
    ))
}

async fn state_store_journal_commit_local(
    State(state): State<AppState>,
    peer: Option<axum::Extension<AuthenticatedSubscriptionPeer>>,
    Json(request): Json<crate::state_store::StateStoreCommitRequest>,
) -> Result<
    Json<crate::state_store::StateStoreReplicaReply<crate::state_store::StateStoreMutation>>,
    ApiError,
> {
    check_subscription_peer(&state, peer.as_ref().map(|peer| &peer.0))?;
    let service = state
        .state_store_journal
        .as_ref()
        .ok_or_else(|| ApiError::unavailable("StateStore journal replica is not configured"))?;
    Ok(Json(
        service
            .commit(request)
            .await
            .map_err(state_store_api_error)?,
    ))
}

async fn state_store_journal_inspect_local(
    State(state): State<AppState>,
    peer: Option<axum::Extension<AuthenticatedSubscriptionPeer>>,
    Json(request): Json<crate::state_store::StateStoreInspectRequest>,
) -> Result<
    Json<
        crate::state_store::StateStoreReplicaReply<crate::state_store::StateStoreJournalInspection>,
    >,
    ApiError,
> {
    check_subscription_peer(&state, peer.as_ref().map(|peer| &peer.0))?;
    let service = state
        .state_store_journal
        .as_ref()
        .ok_or_else(|| ApiError::unavailable("StateStore journal replica is not configured"))?;
    Ok(Json(
        service
            .inspect(request)
            .await
            .map_err(state_store_api_error)?,
    ))
}

async fn state_store_journal_lookup_local(
    State(state): State<AppState>,
    peer: Option<axum::Extension<AuthenticatedSubscriptionPeer>>,
    Json(request): Json<crate::state_store::StateStoreLookupRequest>,
) -> Result<
    Json<
        crate::state_store::StateStoreReplicaReply<Option<crate::state_store::StateStoreMutation>>,
    >,
    ApiError,
> {
    check_subscription_peer(&state, peer.as_ref().map(|peer| &peer.0))?;
    let service = state
        .state_store_journal
        .as_ref()
        .ok_or_else(|| ApiError::unavailable("StateStore journal replica is not configured"))?;
    Ok(Json(
        service
            .lookup(request)
            .await
            .map_err(state_store_api_error)?,
    ))
}

async fn state_store_journal_read_local(
    State(state): State<AppState>,
    Json(request): Json<crate::state_store::StateStoreReadRequest>,
) -> Result<
    Json<crate::state_store::StateStoreReplicaReply<crate::state_store::StateStoreReadOutcome>>,
    ApiError,
> {
    state
        .state_store_journal
        .as_ref()
        .ok_or_else(|| ApiError::unavailable("StateStore journal replica is not configured"))?
        .read_row(request)
        .await
        .map(Json)
        .map_err(state_store_api_error)
}

async fn state_store_journal_entries_local(
    State(state): State<AppState>,
    peer: Option<axum::Extension<AuthenticatedSubscriptionPeer>>,
    Json(request): Json<crate::state_store::StateStoreEntriesRequest>,
) -> Result<
    Json<crate::state_store::StateStoreReplicaReply<crate::state_store::StateStoreEntriesPage>>,
    ApiError,
> {
    check_subscription_peer(&state, peer.as_ref().map(|peer| &peer.0))?;
    let service = state
        .state_store_journal
        .as_ref()
        .ok_or_else(|| ApiError::unavailable("StateStore journal replica is not configured"))?;
    Ok(Json(
        service
            .entries(request)
            .await
            .map_err(state_store_api_error)?,
    ))
}

async fn state_store_journal_transfer_local(
    State(state): State<AppState>,
    peer: Option<axum::Extension<AuthenticatedSubscriptionPeer>>,
    Json(request): Json<crate::state_store::StateStoreTransferRequest>,
) -> Result<
    Json<crate::state_store::StateStoreReplicaReply<crate::state_store::StateStoreTransferPage>>,
    ApiError,
> {
    check_subscription_peer(&state, peer.as_ref().map(|peer| &peer.0))?;
    let service = state
        .state_store_journal
        .as_ref()
        .ok_or_else(|| ApiError::unavailable("StateStore journal replica is not configured"))?;
    Ok(Json(
        service
            .transfer(request)
            .await
            .map_err(state_store_api_error)?,
    ))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StateStoreLookupApiRequest {
    store: String,
    key_base64: String,
    /// Wait (bounded) for the serving replica's applied position to reach
    /// its committed frontier; when no replica catches up in time the
    /// lookup reports `index_behind` rather than silently serving stale.
    #[serde(default)]
    strict: bool,
    #[serde(default)]
    wait_ms: Option<u64>,
}

#[derive(Serialize)]
struct StateStoreLookupApiResponse {
    store_id: Uuid,
    found: bool,
    payload_base64: Option<String>,
    applied_cursor: Option<String>,
    /// Freshness evidence from the serving replica: `applied_seq` behind
    /// `committed_seq` means the replica is still applying the journal.
    applied_seq: u64,
    committed_seq: u64,
    behind: bool,
    served_by: String,
}

// ---------------------------------------------------------------------------
// Logical Index queries
// ---------------------------------------------------------------------------

/// Default bounded wait for a strict Index read to observe its activation
/// frontier locally (and the forwarded bound each peer waits).
const INDEX_QUERY_DEFAULT_WAIT_MS: u64 = 500;
/// Cap on the caller-supplied wait so a strict read can never pin a request.
const INDEX_QUERY_MAX_WAIT_MS: u64 = 2_000;
/// Maximum peer Nodes a strict query may forward to. The logical contract
/// hides physical placement; the bound keeps scatter/gather predictable.
const INDEX_QUERY_MAX_PEERS: usize = 8;

fn index_query_inclusive() -> bool {
    true
}

/// Wire form of one typed Index field value. Bytes travel base64 like every
/// other binary field on the Admin API.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum IndexValueWire {
    Null,
    Bool { value: bool },
    I64 { value: i64 },
    Text { value: String },
    Bytes { value_base64: String },
}

impl IndexValueWire {
    fn into_value(self) -> Result<crate::index::IndexValue, ApiError> {
        Ok(match self {
            Self::Null => crate::index::IndexValue::Null,
            Self::Bool { value } => crate::index::IndexValue::Bool(value),
            Self::I64 { value } => crate::index::IndexValue::I64(value),
            Self::Text { value } => crate::index::IndexValue::Text(value),
            Self::Bytes { value_base64 } => {
                crate::index::IndexValue::Bytes(decode_base64("value_base64", &value_base64)?)
            }
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct IndexBoundWire {
    value: IndexValueWire,
    #[serde(default = "index_query_inclusive")]
    inclusive: bool,
}

/// `POST /v1/indexes/query` request. `mode` selects the read shape:
/// `exact` supplies every declared field, `prefix` matches leading fields
/// plus an optional text prefix, `range` bounds the field after `leading`.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct IndexQueryRequest {
    index: String,
    #[serde(flatten)]
    mode: IndexQueryModeWire,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    strict: bool,
    #[serde(default)]
    wait_ms: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
enum IndexQueryModeWire {
    Exact {
        values: Vec<IndexValueWire>,
    },
    Prefix {
        #[serde(default)]
        leading: Vec<IndexValueWire>,
        #[serde(default)]
        partial_text: Option<String>,
    },
    Range {
        #[serde(default)]
        leading: Vec<IndexValueWire>,
        lower: IndexBoundWire,
        upper: IndexBoundWire,
    },
}

impl IndexQueryModeWire {
    fn into_predicate(self) -> Result<crate::control::IndexQueryPredicate, ApiError> {
        Ok(match self {
            Self::Exact { values } => crate::control::IndexQueryPredicate::Exact {
                values: values
                    .into_iter()
                    .map(IndexValueWire::into_value)
                    .collect::<Result<_, _>>()?,
            },
            Self::Prefix {
                leading,
                partial_text,
            } => crate::control::IndexQueryPredicate::Prefix {
                leading: leading
                    .into_iter()
                    .map(IndexValueWire::into_value)
                    .collect::<Result<_, _>>()?,
                partial_text,
            },
            Self::Range {
                leading,
                lower,
                upper,
            } => crate::control::IndexQueryPredicate::Range {
                leading: leading
                    .into_iter()
                    .map(IndexValueWire::into_value)
                    .collect::<Result<_, _>>()?,
                lower: crate::control::IndexBound {
                    value: lower.value.into_value()?,
                    inclusive: lower.inclusive,
                },
                upper: crate::control::IndexBound {
                    value: upper.value.into_value()?,
                    inclusive: upper.inclusive,
                },
            },
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct IndexQueryRow {
    key_base64: String,
    payload_base64: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct IndexQueryResponse {
    index: String,
    index_id: Uuid,
    generation: u64,
    rows: Vec<IndexQueryRow>,
    /// Frontier this Node's materialized postings have applied, as recorded
    /// by the generation's durable checkpoint.
    applied_cursor: Option<String>,
    /// Frontier the catalog committed when the generation activated.
    target_cursor: Option<String>,
    /// True when the serving Node's postings lag the activation frontier.
    behind: bool,
    /// True when `limit` cut the result; callers narrow the predicate.
    truncated: bool,
    served_by: String,
}

fn index_store_api_error(error: crate::index::IndexStoreError) -> ApiError {
    match error {
        crate::index::IndexStoreError::InvalidLimit
        | crate::index::IndexStoreError::QueryTooLarge
        | crate::index::IndexStoreError::Key(_) => ApiError::bad_request(error.to_string()),
        crate::index::IndexStoreError::DanglingPosting
        | crate::index::IndexStoreError::CorruptPrimary => ApiError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: format!("Index postings failed verification: {error}"),
            code: Some("index_corrupt"),
        },
        other => ApiError::unavailable(other.to_string()),
    }
}

/// Answer one `GET INDEX` / `POST /v1/indexes/query` request against the
/// Node-local Index engine. Resolves the logical Index name, fences on the
/// catalog-persisted build stage, waits (bounded) for the activation
/// frontier under `strict`, then runs the typed predicate against the
/// active generation's postings.
async fn index_query_local(
    state: &AppState,
    index_name: &str,
    predicate: &crate::control::IndexQueryPredicate,
    limit: usize,
    strict: bool,
    wait_ms: u64,
) -> Result<IndexQueryResponse, ApiError> {
    let definition = state
        .control
        .declared_index_by_name(index_name)
        .await
        .ok_or_else(|| ApiError {
            status: StatusCode::NOT_FOUND,
            message: format!("Index does not exist: {index_name}"),
            code: Some("index_not_found"),
        })?;
    let generation = match definition.stage {
        crate::control::IndexBuildStage::Active { generation } => generation,
        ref stage => {
            let stage_label = match stage {
                crate::control::IndexBuildStage::Declared => "declared (no build has run)",
                crate::control::IndexBuildStage::Building { .. } => {
                    "building (a generation is materializing)"
                }
                crate::control::IndexBuildStage::Active { .. } => unreachable!(),
            };
            return Err(ApiError {
                status: StatusCode::CONFLICT,
                message: format!(
                    "Index {index_name} is {stage_label}; activate a completed build before querying"
                ),
                code: Some("index_not_ready"),
            });
        }
    };
    let index = state
        .state_store_journal
        .as_ref()
        .and_then(|service| service.index_store())
        .ok_or_else(|| ApiError::unavailable("this Node does not materialize Index postings"))?;
    let field_count = definition.fields.len();
    match predicate {
        crate::control::IndexQueryPredicate::Exact { values } if values.len() != field_count => {
            return Err(ApiError::bad_request(format!(
                "exact lookup requires {field_count} values (one per declared field), got {}",
                values.len()
            )));
        }
        crate::control::IndexQueryPredicate::Exact { .. } => {}
        crate::control::IndexQueryPredicate::Prefix {
            leading,
            partial_text,
        } => {
            let width = leading.len() + usize::from(partial_text.is_some());
            if leading.len() >= field_count || width > field_count {
                return Err(ApiError::bad_request(format!(
                    "prefix lookup may bound at most {field_count} leading fields, got {width}"
                )));
            }
        }
        crate::control::IndexQueryPredicate::Range { leading, .. } => {
            if leading.len() >= field_count {
                return Err(ApiError::bad_request(format!(
                    "range lookup leaves no field to bound: {} leading values for {field_count} fields",
                    leading.len()
                )));
            }
        }
    };
    let shadow = crate::index::shadow_index_id(definition.index_id, generation);
    let target = definition.applied_cursor.clone();
    let wait_ms = wait_ms.min(INDEX_QUERY_MAX_WAIT_MS);
    // Under strict, bounded-wait for this Node's checkpoint to reach the
    // activation frontier before reading — the catalog committed the target
    // cursor, so a materializing Node converges there.
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(wait_ms);
    let index_read = index.clone();
    let predicate = predicate.clone();
    let query_mode = match &predicate {
        crate::control::IndexQueryPredicate::Exact { .. } => crate::demand::IndexQueryMode::Exact,
        crate::control::IndexQueryPredicate::Prefix { .. } => crate::demand::IndexQueryMode::Prefix,
        crate::control::IndexQueryPredicate::Range { .. } => crate::demand::IndexQueryMode::Range,
    };
    if strict {
        // Bounded-wait for the checkpoint; filesystem reads stay off the
        // Tokio executor.
        let index_wait = index.clone();
        loop {
            let index_wait = index_wait.clone();
            let frontier = tokio::task::spawn_blocking(move || index_wait.last_applied(shadow))
                .await
                .map_err(|_| ApiError::unavailable("Index freshness check did not complete"))?
                .map_err(index_store_api_error)?;
            if frontier.as_deref() == target.as_deref() || std::time::Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }
    let rows = tokio::task::spawn_blocking(move || match &predicate {
        crate::control::IndexQueryPredicate::Exact { values } => {
            index_read.lookup_exact(shadow, values, limit)
        }
        crate::control::IndexQueryPredicate::Prefix {
            leading,
            partial_text,
        } => index_read.lookup_prefix(shadow, leading, partial_text.as_deref(), limit),
        crate::control::IndexQueryPredicate::Range {
            leading,
            lower,
            upper,
        } => index_read.lookup_range(
            shadow,
            leading,
            if lower.inclusive {
                std::ops::Bound::Included(lower.value.clone())
            } else {
                std::ops::Bound::Excluded(lower.value.clone())
            },
            if upper.inclusive {
                std::ops::Bound::Included(upper.value.clone())
            } else {
                std::ops::Bound::Excluded(upper.value.clone())
            },
            limit,
        ),
    })
    .await
    .map_err(|_| ApiError::unavailable("Index query task did not complete"))?
    .map_err(index_store_api_error)?;
    let frontier = index.last_applied(shadow).map_err(index_store_api_error)?;
    let behind = frontier.as_deref() != target.as_deref();
    state.demand.record_index_query(
        definition.index_id,
        crate::demand::IndexQuerySample {
            mode: query_mode,
            rows: rows.len() as u64,
            truncated: rows.len() == limit,
            behind,
            sampled_at_ns: unix_ns(),
        },
    );
    Ok(IndexQueryResponse {
        index: definition.name.clone(),
        index_id: definition.index_id,
        generation,
        truncated: rows.len() == limit,
        rows: rows
            .into_iter()
            .map(|row| IndexQueryRow {
                key_base64: STANDARD.encode(&row.reference.application_key),
                payload_base64: STANDARD.encode(&row.payload),
            })
            .collect(),
        applied_cursor: frontier,
        target_cursor: target,
        behind,
        served_by: state
            .storage_node_id
            .as_ref()
            .map(|node| node.to_string())
            .unwrap_or_else(|| state.membership.local().node_id.clone()),
    })
}

/// POST one query to a peer Node's internal Index query route. Shared-key
/// on the plain internal plane, pinned identity on the mTLS plane — the
/// same credentials the data-plane replica routes use.
async fn post_internal_index_query(
    state: &AppState,
    node: &StorageNodeId,
    request: &IndexQueryRequest,
) -> Result<IndexQueryResponse, ApiError> {
    let endpoint = state
        .control_endpoints
        .resolve(node)
        .await
        .ok_or_else(|| ApiError::unavailable(format!("Node {node} endpoint is unavailable")))?;
    let url = format!("{}/internal/index/query", endpoint.trim_end_matches('/'));
    let response = match &state.internal_mtls {
        Some(material) => {
            let ca = reqwest::Certificate::from_pem(&material.ca_pem)
                .map_err(|_| ApiError::unavailable("internal mTLS CA is invalid"))?;
            let identity = reqwest::Identity::from_pem(&material.identity_pem)
                .map_err(|_| ApiError::unavailable("internal mTLS identity is invalid"))?;
            reqwest::Client::builder()
                .connect_timeout(std::time::Duration::from_secs(1))
                .timeout(std::time::Duration::from_secs(5))
                .https_only(true)
                .tls_built_in_root_certs(false)
                .add_root_certificate(ca)
                .identity(identity)
                .build()
                .map_err(|_| ApiError::unavailable("internal mTLS client failed to build"))?
                .post(url)
                .json(request)
                .send()
                .await
        }
        None => {
            let key = state
                .internal_key
                .clone()
                .ok_or_else(|| ApiError::unavailable("internal credential is not configured"))?;
            state
                .internal_http
                .post(url)
                .header("x-whitewater-control-key", key)
                .json(request)
                .send()
                .await
        }
    }
    .map_err(|error| ApiError::unavailable(format!("Node {node}: {error}")))?;
    if response.status() == StatusCode::CONFLICT {
        let body: serde_json::Value = response.json().await.unwrap_or_default();
        return Err(ApiError {
            status: StatusCode::CONFLICT,
            message: body
                .get("message")
                .and_then(|value| value.as_str())
                .unwrap_or("peer Index is behind or not ready")
                .to_owned(),
            code: match body.get("code").and_then(|value| value.as_str()) {
                Some("index_behind") => Some("index_behind"),
                Some("index_not_ready") => Some("index_not_ready"),
                _ => None,
            },
        });
    }
    if !response.status().is_success() {
        return Err(ApiError::unavailable(format!(
            "Node {node} rejected the Index query ({})",
            response.status()
        )));
    }
    response
        .json()
        .await
        .map_err(|error| ApiError::unavailable(error.to_string()))
}

/// `POST /v1/indexes/query` — logical GET/exact/prefix/range against the
/// active Index generation. The local Node answers when it materializes the
/// postings; under `strict` a still-catching-up Node forwards (bounded) to
/// peers until one reaches the activation frontier, else `index_behind`.
async fn index_query(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<IndexQueryRequest>,
) -> Result<Json<IndexQueryResponse>, ApiError> {
    authenticate_credential(&state, &headers)?;
    if let Some(index) = state.control.declared_index_by_name(&request.index).await {
        authorize_namespace(&state, &headers, &index.name, PermissionAction::Read)?;
    }
    let predicate = request.mode.clone().into_predicate()?;
    run_index_query(
        &state,
        &request.index,
        &predicate,
        request.limit.unwrap_or(256),
        request.strict,
        request.wait_ms.unwrap_or(INDEX_QUERY_DEFAULT_WAIT_MS),
    )
    .await
    .map(Json)
}

/// `POST /internal/index/query` — peer-forwarded Index read. Answers from
/// local materialized postings only; the forwarding bound lives on the
/// ingress Node so a query can never fan out recursively.
async fn index_query_internal(
    State(state): State<AppState>,
    peer: Option<axum::Extension<AuthenticatedSubscriptionPeer>>,
    Json(request): Json<IndexQueryRequest>,
) -> Result<Json<IndexQueryResponse>, ApiError> {
    check_subscription_peer(&state, peer.as_ref().map(|peer| &peer.0))?;
    let predicate = request.mode.clone().into_predicate()?;
    index_query_local(
        &state,
        &request.index,
        &predicate,
        request.limit.unwrap_or(256),
        request.strict,
        request.wait_ms.unwrap_or(INDEX_QUERY_DEFAULT_WAIT_MS),
    )
    .await
    .map(Json)
}

async fn run_index_query(
    state: &AppState,
    index_name: &str,
    predicate: &crate::control::IndexQueryPredicate,
    limit: usize,
    strict: bool,
    wait_ms: u64,
) -> Result<IndexQueryResponse, ApiError> {
    match index_query_local(state, index_name, predicate, limit, strict, wait_ms).await {
        Ok(response) if !(strict && response.behind) => return Ok(response),
        Ok(response) => response,
        Err(error)
            if error.status == StatusCode::NOT_FOUND || error.status == StatusCode::BAD_REQUEST =>
        {
            return Err(error)
        }
        Err(error) => {
            let last_error = error;
            let mut peers: Vec<StorageNodeId> = state
                .control_endpoints
                .all()
                .await
                .into_keys()
                .filter(|node| state.storage_node_id.as_ref() != Some(node))
                .take(INDEX_QUERY_MAX_PEERS)
                .collect();
            peers.sort();
            for peer in peers {
                let wire = index_query_wire_request(index_name, predicate, limit, strict, wait_ms);
                match post_internal_index_query(state, &peer, &wire).await {
                    Ok(response) if !(strict && response.behind) => return Ok(response),
                    Ok(_) | Err(_) => {
                        continue;
                    }
                }
            }
            return Err(last_error);
        }
    };
    // Strict path: local postings lag the activation frontier. Try bounded
    // peer reads; a peer waits its own bound and may answer fresh.
    let mut last_error = ApiError {
        status: StatusCode::CONFLICT,
        message: "no Node materialized the Index at its activation frontier".to_owned(),
        code: Some("index_behind"),
    };
    let mut peers: Vec<StorageNodeId> = state
        .control_endpoints
        .all()
        .await
        .into_keys()
        .filter(|node| state.storage_node_id.as_ref() != Some(node))
        .take(INDEX_QUERY_MAX_PEERS)
        .collect();
    peers.sort();
    for peer in peers {
        let wire = index_query_wire_request(index_name, predicate, limit, strict, wait_ms);
        match post_internal_index_query(state, &peer, &wire).await {
            Ok(response) if !response.behind => return Ok(response),
            Ok(response) => {
                last_error = ApiError {
                    status: StatusCode::CONFLICT,
                    message: format!(
                        "Node {peer} applied cursor {:?} lags activation frontier {:?}",
                        response.applied_cursor, response.target_cursor
                    ),
                    code: Some("index_behind"),
                };
            }
            Err(error) => last_error = error,
        }
    }
    Err(last_error)
}

/// Intercept `GET INDEX` statements before the catalog executes them: the
/// catalog never materializes postings, so data-plane reads are answered by
/// the local Index engine (or a peer under strict). Returns `Ok(None)`
/// when no statement needs the data plane. A script mixing `GET INDEX`
/// with mutations is refused — reads in it could not observe the committed
/// outcome of sibling writes.
async fn evaluate_data_plane_reads(
    state: &AppState,
    commands: Vec<crate::control::Command>,
    request_id: Uuid,
) -> Result<Option<crate::control::ControlExecution>, ApiError> {
    if !commands
        .iter()
        .any(|command| matches!(command, crate::control::Command::GetIndex { .. }))
    {
        return Ok(None);
    }
    if !commands.iter().all(|command| command.is_read_only()) {
        return Err(ApiError::bad_request(
            "GET INDEX cannot be combined with mutating statements in one request",
        ));
    }
    let mut results = Vec::with_capacity(commands.len());
    for command in commands {
        match command {
            crate::control::Command::GetIndex {
                index,
                predicate,
                limit,
                strict,
                wait_ms,
            } => {
                let response = run_index_query(
                    state,
                    &index,
                    &predicate,
                    limit.unwrap_or(256),
                    strict,
                    wait_ms.unwrap_or(INDEX_QUERY_DEFAULT_WAIT_MS),
                )
                .await?;
                results.push(crate::control::StatementResult {
                    statement: "GET INDEX".to_owned(),
                    message: format!("Index {index} query returned {} rows", response.rows.len()),
                    data: serde_json::to_value(&response).unwrap_or_else(|_| serde_json::json!({})),
                });
            }
            other => {
                let result = match &state.control_plane {
                    Some(control_plane) => control_plane
                        .execute_commands_with_request_id(vec![other], request_id)
                        .await?
                        .results
                        .into_iter()
                        .next()
                        .ok_or_else(|| {
                            ApiError::unavailable("Control Plane returned no statement result")
                        })?,
                    None => state.control.execute_query(other).await?,
                };
                results.push(result);
            }
        }
    }
    let (authority, warning) = match &state.control_plane {
        Some(_) => ("control_plane".to_owned(), String::new()),
        None => (
            "local_prototype".to_owned(),
            "Catalog state is local to this Node and is not replicated authority until the Control Plane is implemented.".to_owned(),
        ),
    };
    Ok(Some(crate::control::ControlExecution {
        request_id,
        revision: state.control.revision().await,
        authority,
        warning,
        results,
    }))
}

fn index_query_wire_request(
    index_name: &str,
    predicate: &crate::control::IndexQueryPredicate,
    limit: usize,
    strict: bool,
    wait_ms: u64,
) -> IndexQueryRequest {
    let mode = match predicate {
        crate::control::IndexQueryPredicate::Exact { values } => IndexQueryModeWire::Exact {
            values: values.iter().cloned().map(index_value_wire).collect(),
        },
        crate::control::IndexQueryPredicate::Prefix {
            leading,
            partial_text,
        } => IndexQueryModeWire::Prefix {
            leading: leading.iter().cloned().map(index_value_wire).collect(),
            partial_text: partial_text.clone(),
        },
        crate::control::IndexQueryPredicate::Range {
            leading,
            lower,
            upper,
        } => IndexQueryModeWire::Range {
            leading: leading.iter().cloned().map(index_value_wire).collect(),
            lower: IndexBoundWire {
                value: index_value_wire(lower.value.clone()),
                inclusive: lower.inclusive,
            },
            upper: IndexBoundWire {
                value: index_value_wire(upper.value.clone()),
                inclusive: upper.inclusive,
            },
        },
    };
    IndexQueryRequest {
        index: index_name.to_owned(),
        mode,
        limit: Some(limit),
        strict,
        wait_ms: Some(wait_ms),
    }
}

fn index_value_wire(value: crate::index::IndexValue) -> IndexValueWire {
    match value {
        crate::index::IndexValue::Null => IndexValueWire::Null,
        crate::index::IndexValue::Bool(value) => IndexValueWire::Bool { value },
        crate::index::IndexValue::I64(value) => IndexValueWire::I64 { value },
        crate::index::IndexValue::Text(value) => IndexValueWire::Text { value },
        crate::index::IndexValue::Bytes(bytes) => IndexValueWire::Bytes {
            value_base64: STANDARD.encode(bytes),
        },
    }
}

/// `POST /v1/state-stores/lookup` — point lookup on a StateStore's primary
/// key served by any ingress Node. The handler prefers the local replica
/// and otherwise forwards over the authenticated internal read route,
/// returning freshness evidence so callers can see a lagging replica
/// rather than silently trusting stale state.
async fn state_store_lookup(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<StateStoreLookupApiRequest>,
) -> Result<Json<StateStoreLookupApiResponse>, ApiError> {
    authenticate_credential(&state, &headers)?;
    let store = state
        .control
        .declared_state_store_by_name(&request.store)
        .await
        .ok_or_else(|| {
            ApiError::bad_request(format!("StateStore does not exist: {}", request.store))
        })?;
    authorize_namespace(&state, &headers, &store.name, PermissionAction::Read)?;
    let assignment = state
        .control
        .active_state_store_assignment_by_id(store.store_id)
        .await
        .ok_or_else(|| ApiError::unavailable("StateStore has no mutation-journal placement"))?;
    let key = decode_base64("key_base64", &request.key_base64)?;
    if key.is_empty() || key.len() > crate::state_store::STATE_STORE_MAX_KEY_BYTES {
        return Err(ApiError::bad_request(
            "key_base64 is empty or exceeds the row-key budget",
        ));
    }
    let outcome = serve_state_store_read(
        &state,
        &assignment,
        store.store_id,
        key,
        None,
        request.strict,
        request.wait_ms,
    )
    .await?;
    Ok(Json(StateStoreLookupApiResponse {
        store_id: store.store_id,
        found: outcome.result.row.is_some(),
        payload_base64: outcome
            .result
            .row
            .as_ref()
            .map(|row| STANDARD.encode(&row.payload)),
        applied_cursor: outcome.result.row.map(|row| row.applied_cursor),
        applied_seq: outcome.result.applied_seq,
        committed_seq: outcome.result.committed_seq,
        behind: outcome.result.applied_seq < outcome.result.committed_seq,
        served_by: outcome.replica.to_string(),
    }))
}

/// Serve one row read against the freshest reachable replica: the local
/// replica first (no network hop), then the owner (freshest application),
/// then the remaining replicas. Strict reads skip a replica still applying
/// its journal; when none catches up in time the caller gets `index_behind`.
/// One named-read shape: equality on declared values or a bounded range
/// over the same posting order. The wire request carries both options
/// for compatibility; this selector makes the mutual exclusivity
/// unrepresentable inside the service path.
enum NamedReadSelection {
    Exact(crate::state_store::StateStoreNamedRead),
    Range(crate::state_store::StateStoreNamedRangeRead),
}

async fn serve_state_store_read(
    state: &AppState,
    assignment: &crate::state_store::StateStoreAssignment,
    store_id: Uuid,
    key: Vec<u8>,
    named: Option<NamedReadSelection>,
    strict: bool,
    wait_ms: Option<u64>,
) -> Result<
    crate::state_store::StateStoreReplicaReply<crate::state_store::StateStoreReadOutcome>,
    ApiError,
> {
    let mut candidates: Vec<StorageNodeId> = Vec::new();
    if let Some(local) = &state.storage_node_id {
        if assignment.replicas.contains(local) {
            candidates.push(local.clone());
        }
    }
    candidates.push(assignment.owner.clone());
    for replica in assignment.replicas.iter() {
        if !candidates.contains(replica) {
            candidates.push(replica.clone());
        }
    }
    let mut last_error: Option<ApiError> = None;
    for replica in &candidates {
        let read = crate::state_store::StateStoreReadRequest {
            owner: assignment.owner.clone(),
            receiver: replica.clone(),
            store_id,
            ownership_epoch: assignment.ownership_epoch,
            key: key.clone(),
            strict,
            wait_ms: wait_ms.unwrap_or(500),
            by_named: match &named {
                Some(NamedReadSelection::Exact(read)) => Some(read.clone()),
                _ => None,
            },
            by_named_range: match &named {
                Some(NamedReadSelection::Range(read)) => Some(read.clone()),
                _ => None,
            },
        };
        let outcome = if Some(replica) == state.storage_node_id.as_ref()
            && state.state_store_journal.is_some()
        {
            state
                .state_store_journal
                .as_ref()
                .unwrap()
                .read_row(read)
                .await
                .map_err(state_store_api_error)
        } else {
            let transport = state_store_transport_for(state, assignment)?;
            transport
                .read(replica, read)
                .await
                .map_err(state_store_api_error)
        };
        match outcome {
            Ok(reply) => {
                if strict && reply.result.applied_seq < reply.result.committed_seq {
                    last_error = Some(ApiError {
                        status: StatusCode::CONFLICT,
                        message: format!(
                            "replica applied sequence {} lags committed sequence {}",
                            reply.result.applied_seq, reply.result.committed_seq
                        ),
                        code: Some("index_behind"),
                    });
                    continue;
                }
                return Ok(reply);
            }
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.unwrap_or_else(|| {
        ApiError::unavailable("no StateStore journal replica could serve the read")
    }))
}

/// `POST /v1/state-stores/get-by`: resolve one declared named key's typed
/// value to the bounded set of primary rows carrying it — the "group/list"
/// lookup. Serves with the same freshness evidence as `lookup`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StateStoreGetByRequest {
    store: String,
    named_key: String,
    /// Equality values; must be empty when `lower`/`upper` bound a
    /// range read instead.
    #[serde(default)]
    values: Vec<IndexValueWire>,
    /// Optional inclusive/exclusive range bounds over the named key —
    /// the same vocabulary `POST /v1/indexes/query` `range` uses.
    #[serde(default)]
    lower: Option<IndexBoundWire>,
    #[serde(default)]
    upper: Option<IndexBoundWire>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    strict: bool,
    #[serde(default)]
    wait_ms: Option<u64>,
}

#[derive(Serialize)]
struct StateStoreGetByResponse {
    store_id: Uuid,
    rows: Vec<StateStoreGetByRow>,
    applied_seq: u64,
    committed_seq: u64,
    behind: bool,
    served_by: String,
}

#[derive(Serialize)]
struct StateStoreGetByRow {
    key_base64: String,
    payload_base64: String,
    applied_cursor: String,
}

async fn state_store_get_by(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<StateStoreGetByRequest>,
) -> Result<Json<StateStoreGetByResponse>, ApiError> {
    authenticate_credential(&state, &headers)?;
    let store = state
        .control
        .declared_state_store_by_name(&request.store)
        .await
        .ok_or_else(|| {
            ApiError::bad_request(format!("StateStore does not exist: {}", request.store))
        })?;
    authorize_namespace(&state, &headers, &store.name, PermissionAction::Read)?;
    if !store
        .named_keys
        .iter()
        .any(|key| key.name == request.named_key)
    {
        return Err(ApiError::bad_request(format!(
            "StateStore {} does not declare named key {}",
            store.name, request.named_key
        )));
    }
    let assignment = state
        .control
        .active_state_store_assignment_by_id(store.store_id)
        .await
        .ok_or_else(|| ApiError::unavailable("StateStore has no mutation-journal placement"))?;
    let is_range = request.lower.is_some() || request.upper.is_some();
    if is_range && !request.values.is_empty() {
        return Err(ApiError::bad_request(
            "values must be empty when range bounds are supplied",
        ));
    }
    if !is_range && request.values.is_empty() {
        return Err(ApiError::bad_request("values must not be empty"));
    }
    let limit = request.limit.unwrap_or(64).clamp(1, 1_000);
    let (by_named, by_named_range) = if is_range {
        let bound = |wire: Option<IndexBoundWire>| {
            wire.map(|bound| {
                Ok::<_, ApiError>(crate::control::IndexBound {
                    value: bound.value.into_value()?,
                    inclusive: bound.inclusive,
                })
            })
            .transpose()
        };
        (
            None,
            Some(crate::state_store::StateStoreNamedRangeRead {
                name: request.named_key,
                lower: bound(request.lower)?,
                upper: bound(request.upper)?,
                limit,
            }),
        )
    } else {
        let values: Vec<crate::index::IndexValue> = request
            .values
            .into_iter()
            .map(IndexValueWire::into_value)
            .collect::<Result<_, _>>()?;
        (
            Some(crate::state_store::StateStoreNamedRead {
                name: request.named_key,
                values,
                limit,
            }),
            None,
        )
    };
    let outcome = serve_state_store_read(
        &state,
        &assignment,
        store.store_id,
        Vec::new(),
        match (by_named, by_named_range) {
            (Some(read), None) => Some(NamedReadSelection::Exact(read)),
            (None, Some(read)) => Some(NamedReadSelection::Range(read)),
            _ => None,
        },
        request.strict,
        request.wait_ms,
    )
    .await?;
    Ok(Json(StateStoreGetByResponse {
        store_id: store.store_id,
        rows: outcome
            .result
            .named_rows
            .into_iter()
            .map(|row| StateStoreGetByRow {
                key_base64: STANDARD.encode(&row.key),
                payload_base64: STANDARD.encode(&row.payload),
                applied_cursor: row.applied_cursor,
            })
            .collect(),
        applied_seq: outcome.result.applied_seq,
        committed_seq: outcome.result.committed_seq,
        behind: outcome.result.applied_seq < outcome.result.committed_seq,
        served_by: outcome.replica.to_string(),
    }))
}

/// `POST /v1/state-stores/iterate`: bounded ordered page over the store's
/// applied rows — the same row set checkpoint transfer serves, so iteration
/// and replica catch-up agree on ordering and freshness (`pinned_seq`).
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StateStoreIterateRequest {
    store: String,
    #[serde(default)]
    after_key_base64: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Serialize)]
struct StateStoreIterateResponse {
    store_id: Uuid,
    rows: Vec<StateStoreIterateRow>,
    next_key_base64: Option<String>,
    /// The serving replica's applied journal position the page was pinned
    /// to — the iteration's freshness evidence.
    pinned_seq: u64,
    served_by: String,
}

#[derive(Serialize)]
struct StateStoreIterateRow {
    key_base64: String,
    payload_base64: String,
    applied_seq: u64,
}

async fn state_store_iterate(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<StateStoreIterateRequest>,
) -> Result<Json<StateStoreIterateResponse>, ApiError> {
    authenticate_credential(&state, &headers)?;
    let store = state
        .control
        .declared_state_store_by_name(&request.store)
        .await
        .ok_or_else(|| {
            ApiError::bad_request(format!("StateStore does not exist: {}", request.store))
        })?;
    authorize_namespace(&state, &headers, &store.name, PermissionAction::Read)?;
    let assignment = state
        .control
        .active_state_store_assignment_by_id(store.store_id)
        .await
        .ok_or_else(|| ApiError::unavailable("StateStore has no mutation-journal placement"))?;
    let after_key = request
        .after_key_base64
        .as_deref()
        .map(|value| decode_base64("after_key_base64", value))
        .transpose()?;
    let limit = request.limit.unwrap_or(256).clamp(1, 1_000);
    // Same candidate order as row reads: local, owner, replicas.
    let mut candidates: Vec<StorageNodeId> = Vec::new();
    if let Some(local) = &state.storage_node_id {
        if assignment.replicas.contains(local) {
            candidates.push(local.clone());
        }
    }
    candidates.push(assignment.owner.clone());
    for replica in assignment.replicas.iter() {
        if !candidates.contains(replica) {
            candidates.push(replica.clone());
        }
    }
    let mut last_error: Option<ApiError> = None;
    for replica in &candidates {
        let transfer = crate::state_store::StateStoreTransferRequest {
            owner: assignment.owner.clone(),
            receiver: replica.clone(),
            store_id: store.store_id,
            ownership_epoch: assignment.ownership_epoch,
            after_key: after_key.clone(),
            limit,
        };
        let outcome = if Some(replica) == state.storage_node_id.as_ref()
            && state.state_store_journal.is_some()
        {
            state
                .state_store_journal
                .as_ref()
                .unwrap()
                .transfer(transfer)
                .await
                .map_err(state_store_api_error)
        } else {
            let transport = state_store_transport_for(&state, &assignment)?;
            transport
                .transfer(replica, transfer)
                .await
                .map_err(state_store_api_error)
        };
        match outcome {
            Ok(reply) => {
                return Ok(Json(StateStoreIterateResponse {
                    store_id: store.store_id,
                    rows: reply
                        .result
                        .rows
                        .into_iter()
                        .map(|row| StateStoreIterateRow {
                            key_base64: STANDARD.encode(&row.key),
                            payload_base64: STANDARD.encode(&row.payload),
                            applied_seq: row.applied_seq,
                        })
                        .collect(),
                    next_key_base64: reply.result.next_key.map(|key| STANDARD.encode(&key)),
                    pinned_seq: reply.result.pinned_seq,
                    served_by: reply.replica.to_string(),
                }));
            }
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.unwrap_or_else(|| {
        ApiError::unavailable("no StateStore journal replica could serve the iteration")
    }))
}

/// `POST /v1/state-stores/clear`: journal one `ClearAll` mutation through
/// the store's RF3 quorum; replicas delete rows in bounded batches under
/// the mutation's cursor and a restart replays the whole clear idempotently.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StateStoreClearRequest {
    store: String,
    mutation_id: Uuid,
}

async fn state_store_clear(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<StateStoreClearRequest>,
) -> Result<Json<StateStoreMutateResponse>, ApiError> {
    authenticate_credential(&state, &headers)?;
    let store = state
        .control
        .declared_state_store_by_name(&request.store)
        .await
        .ok_or_else(|| {
            ApiError::bad_request(format!("StateStore does not exist: {}", request.store))
        })?;
    authorize_namespace(&state, &headers, &store.name, PermissionAction::Write)?;
    if !matches!(store.source, crate::control::StateStoreSource::Manual) {
        return Err(ApiError::bad_request(
            "clear applies only to manual StateStores; Feed-derived stores replay from their source",
        ));
    }
    let assignment = state
        .control
        .active_state_store_assignment_by_id(store.store_id)
        .await
        .ok_or_else(|| ApiError::unavailable("StateStore has no mutation-journal placement"))?;
    let mutation = crate::state_store::StateStoreMutation {
        store_id: store.store_id,
        mutation_id: request.mutation_id,
        request_id: Uuid::new_v4(),
        ownership_epoch: assignment.ownership_epoch,
        sequence: 0,
        op: crate::state_store::StateStoreMutationOp::ClearAll,
        issued_at_ns: unix_ns(),
        source: None,
    };
    let coordinator = state_store_coordinator_for(&state, assignment)?;
    let result = coordinator
        .mutate(mutation)
        .await
        .map_err(state_store_api_error)?;
    Ok(Json(StateStoreMutateResponse {
        store_id: result.store_id,
        mutation_id: result.mutation_id,
        sequence: result.sequence,
        deduplicated: result.deduplicated,
    }))
}

/// Build the mutation coordinator bound to a StateStore's RF3 placement over
/// the configured internal transport (mTLS or shared key).
fn state_store_transport_for(
    state: &AppState,
    assignment: &crate::state_store::StateStoreAssignment,
) -> Result<Arc<dyn crate::state_store::StateStoreMutationTransport>, ApiError> {
    match &state.internal_mtls {
        Some(material) => Ok(Arc::new(
            crate::state_store::HttpStateStoreMutationTransport::new_mtls(
                assignment.clone(),
                state.control_endpoints.as_ref().clone(),
                &material.ca_pem,
                &material.identity_pem,
                Duration::from_secs(5),
            )
            .map_err(state_store_api_error)?,
        )),
        None => Ok(Arc::new(
            crate::state_store::HttpStateStoreMutationTransport::new(
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
        )),
    }
}

/// Resolve one Enrich lookup for a drive fetch: extract the record's
/// projected key, translate it to `PutRow` primary-key bytes, and read the
/// row through the same placement-fenced replica fan-out public reads use.
/// `Null`/unprojectable keys resolve as misses; every replica failing is an
/// error so the drive retries rather than journaling a fake miss.
async fn resolve_enrich(
    state: &AppState,
    pipe: &crate::control::PipeDefinition,
    spec: &crate::control::IndexFieldSpec,
    freshness: crate::control::EnrichFreshness,
    named: Option<&str>,
    record: &crate::domain::StoredRecord,
) -> Result<crate::pipe::EnrichResolution, ApiError> {
    let store_id = pipe
        .output_store_id
        .ok_or_else(|| ApiError::conflict("Enrich Pipe has no lookup StateStore"))?;
    let source = crate::state_store::ReplaySourceRecord {
        message_id: record.message_id,
        cursor: String::new(),
        key: record.key.clone(),
        metadata: record.metadata.clone(),
        payload: record.payload.clone(),
    };
    let miss = || {
        Ok(crate::pipe::EnrichResolution {
            matched: false,
            payload: Vec::new(),
            applied_seq: 0,
        })
    };
    let Ok(values) = crate::index_build::extract_index_values(std::slice::from_ref(spec), &source)
    else {
        return miss();
    };
    let Some(first_value) = values.into_iter().next() else {
        return miss();
    };
    let (key, by_named) = match named {
        // Named-key resolution reads the same `by_named` postings contract
        // public row reads use — the extracted value addresses a declared
        // secondary key rather than the primary.
        Some(name) => (
            Vec::new(),
            Some(crate::state_store::StateStoreNamedRead {
                name: name.to_owned(),
                values: vec![first_value],
                limit: 1,
            }),
        ),
        None => match crate::pipe::enrich_lookup_key_bytes(&first_value) {
            Some(key) => (key, None),
            None => return miss(),
        },
    };
    let assignment = state
        .control
        .active_state_store_assignment_by_id(store_id)
        .await
        .ok_or_else(|| ApiError::unavailable("StateStore has no mutation-journal placement"))?;
    // Prefer a local replica read, then owner, then any member — the same
    // candidate order public row reads use.
    let mut candidates: Vec<StorageNodeId> = Vec::new();
    if let Some(local) = state.storage_node_id.clone() {
        if assignment.replicas.iter().any(|node| *node == local) {
            candidates.push(local);
        }
    }
    candidates.push(assignment.owner.clone());
    for replica in assignment.replicas.iter() {
        if !candidates.contains(replica) {
            candidates.push(replica.clone());
        }
    }
    let mut last_error: Option<ApiError> = None;
    for replica in &candidates {
        let read = crate::state_store::StateStoreReadRequest {
            owner: assignment.owner.clone(),
            receiver: replica.clone(),
            store_id,
            ownership_epoch: assignment.ownership_epoch,
            key: key.clone(),
            strict: matches!(freshness, crate::control::EnrichFreshness::Strict),
            wait_ms: 500,
            by_named: by_named.clone(),
            by_named_range: None,
        };
        let outcome = if Some(replica) == state.storage_node_id.as_ref()
            && state.state_store_journal.is_some()
        {
            state
                .state_store_journal
                .as_ref()
                .unwrap()
                .read_row(read)
                .await
                .map_err(state_store_api_error)
        } else {
            state_store_transport_for(state, &assignment)?
                .read(replica, read)
                .await
                .map_err(state_store_api_error)
        };
        match outcome {
            Ok(reply) => {
                // Strict freshness: a replica still behind its committed
                // sequence is skipped like a failure — the drive retries
                // rather than journaling a row older than the quorum's
                // committed frontier.
                if matches!(freshness, crate::control::EnrichFreshness::Strict)
                    && reply.result.applied_seq < reply.result.committed_seq
                {
                    last_error = Some(ApiError {
                        status: StatusCode::CONFLICT,
                        message: format!(
                            "replica applied sequence {} lags committed sequence {}",
                            reply.result.applied_seq, reply.result.committed_seq
                        ),
                        code: Some("index_behind"),
                    });
                    continue;
                }
                let (matched, payload) = match reply.result.named_rows.first() {
                    Some(row) => (true, row.payload.clone()),
                    None => (
                        reply.result.row.is_some(),
                        reply.result.row.map(|row| row.payload).unwrap_or_default(),
                    ),
                };
                return Ok(crate::pipe::EnrichResolution {
                    matched,
                    payload,
                    applied_seq: reply.result.applied_seq,
                });
            }
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.unwrap_or_else(|| {
        ApiError::unavailable("no StateStore journal replica could serve the lookup")
    }))
}

/// Resolve closed window panes for emission at fetch time. A single
/// bounded range scan over the `pane` posting enumerates every closable
/// pane across all groups — untouched groups' panes still close on
/// schedule instead of waiting for traffic to revisit them. Foreign
/// rows sharing the `pane` name are filtered out by
/// `window_emission_from_row` and never deleted.
async fn resolve_window_emissions(
    state: &AppState,
    pipe: &crate::control::PipeDefinition,
    window_ns: u64,
    max_late_ns: Option<u64>,
    expire_ns: Option<u64>,
    records: &[crate::pipe::FetchedRecord],
) -> Result<(Vec<crate::pipe::WindowEmission>, Vec<Vec<u8>>), ApiError> {
    let store_id = pipe
        .output_store_id
        .ok_or_else(|| ApiError::conflict("Window Pipe has no output StateStore"))?;
    let pane_ns = crate::pipe::window_pane_ns(window_ns);
    // Closed panes publish only when the Pipe declared an emission Feed.
    let emit_enabled = max_late_ns.is_some() && pipe.output_feed_id.is_some();
    // The deterministic "now" for close decisions: the newest ingest
    // timestamp this page observed, floored by the driver's own clock so
    // panes still close while input idles. The resolved set journals
    // inside the Declare, so a replay never re-resolves.
    let wall_now = u64::try_from(unix_ns().max(0)).unwrap_or(0);
    let now_ns = records
        .iter()
        .map(|record| u64::try_from(record.ingest_time_ns).unwrap_or(0))
        .max()
        .unwrap_or(0)
        .max(wall_now);
    let assignment = state
        .control
        .active_state_store_assignment_by_id(store_id)
        .await
        .ok_or_else(|| ApiError::unavailable("StateStore has no mutation-journal placement"))?;
    // Local replica first, then owner, then any member — the same
    // candidate order public row reads use.
    let mut candidates: Vec<StorageNodeId> = Vec::new();
    if let Some(local) = state.storage_node_id.clone() {
        if assignment.replicas.iter().any(|node| *node == local) {
            candidates.push(local);
        }
    }
    candidates.push(assignment.owner.clone());
    for replica in assignment.replicas.iter() {
        if !candidates.contains(replica) {
            candidates.push(replica.clone());
        }
    }
    // Closable panes satisfy (pane + 1) * pane_ns + max_late <= now and
    // expired panes satisfy pane_end + expire <= now; the range bound
    // prefilters whichever reaches furthest, and the per-row check
    // decides.
    let bound_max = |bound_ns: u64| {
        i64::try_from((now_ns.saturating_sub(bound_ns) / pane_ns).saturating_sub(1))
            .unwrap_or(i64::MAX)
    };
    let scan_max = [
        emit_enabled.then(|| bound_max(max_late_ns.unwrap_or(0))),
        expire_ns.map(bound_max),
    ]
    .into_iter()
    .flatten()
    .max()
    .unwrap_or(-1);
    let mut reply = None;
    let mut last_error: Option<ApiError> = None;
    for replica in &candidates {
        let read = crate::state_store::StateStoreReadRequest {
            owner: assignment.owner.clone(),
            receiver: replica.clone(),
            store_id,
            ownership_epoch: assignment.ownership_epoch,
            key: Vec::new(),
            strict: false,
            wait_ms: 500,
            by_named: None,
            by_named_range: Some(crate::state_store::StateStoreNamedRangeRead {
                name: crate::pipe::WINDOW_PANE_NAME.to_owned(),
                lower: None,
                upper: Some(crate::control::IndexBound {
                    value: crate::index::IndexValue::I64(scan_max),
                    inclusive: true,
                }),
                limit: 64,
            }),
        };
        let outcome = if Some(replica) == state.storage_node_id.as_ref()
            && state.state_store_journal.is_some()
        {
            state
                .state_store_journal
                .as_ref()
                .unwrap()
                .read_row(read)
                .await
                .map_err(state_store_api_error)
        } else {
            match state_store_transport_for(state, &assignment) {
                Ok(transport) => transport
                    .read(replica, read)
                    .await
                    .map_err(state_store_api_error),
                Err(error) => Err(error),
            }
        };
        match outcome {
            Ok(result) => {
                reply = Some(result);
                break;
            }
            Err(error) => last_error = Some(error),
        }
    }
    // A scan that cannot complete fails the whole resolve — the drive
    // retries rather than emitting a partial close set. Emission
    // deletes are atomic with their outputs, so a retry is idempotent.
    let Some(reply) = reply else {
        return Err(last_error.unwrap_or_else(|| {
            ApiError::unavailable("no StateStore replica could serve the emission scan")
        }));
    };
    let mut emissions = Vec::new();
    let mut expired = Vec::new();
    for row in reply.result.named_rows {
        match crate::pipe::window_row_disposition(
            &row.key,
            &row.payload,
            pane_ns,
            if emit_enabled { max_late_ns } else { None },
            expire_ns,
            now_ns,
        ) {
            crate::pipe::WindowPaneDisposition::Emit(mut emission) => {
                // The serving replica's applied journal sequence is the
                // version evidence for every result this scan produced.
                emission.store_seq = reply.result.applied_seq;
                emissions.push(emission)
            }
            crate::pipe::WindowPaneDisposition::Expire => expired.push(row.key.clone()),
            crate::pipe::WindowPaneDisposition::Keep => {}
        }
        if emissions.len() + expired.len() >= crate::pipe::WINDOW_MAX_EMISSIONS_PER_DRIVE {
            break;
        }
    }
    emissions.sort_by(|a, b| a.row_key.cmp(&b.row_key));
    expired.sort();
    Ok((emissions, expired))
}

/// Fetch-time pane-row resolution for `LIMIT KEYS`/`LIMIT BYTES`: scan
/// the store's `key` posting for the (group, pane) rows currently held.
/// `scan_limit` (`window_scan_row_limit`) exceeds every row an
/// in-budget store can hold, so a truncated result provably means the
/// bound is exceeded — nothing new admits. Local replica first, then
/// owner, then any member; an unreadable scan fails closed (the drive
/// retries) rather than guessing capacity.
async fn resolve_window_pane_rows(
    state: &AppState,
    pipe: &crate::control::PipeDefinition,
    scan_limit: usize,
) -> Result<(std::collections::BTreeSet<(Vec<u8>, u64)>, bool), ApiError> {
    let store_id = pipe
        .output_store_id
        .ok_or_else(|| ApiError::conflict("Window Pipe has no output StateStore"))?;
    let assignment = state
        .control
        .active_state_store_assignment_by_id(store_id)
        .await
        .ok_or_else(|| ApiError::unavailable("StateStore has no mutation-journal placement"))?;
    let mut candidates: Vec<StorageNodeId> = Vec::new();
    if let Some(local) = state.storage_node_id.clone() {
        if assignment.replicas.iter().any(|node| *node == local) {
            candidates.push(local);
        }
    }
    candidates.push(assignment.owner.clone());
    for replica in assignment.replicas.iter() {
        if !candidates.contains(replica) {
            candidates.push(replica.clone());
        }
    }
    // `limit * (panes + sentinel) + 1` rows proves saturation only past
    // the declared bound; WINDOW_MAX_KEY_LIMIT keeps the scan bounded.
    let mut reply = None;
    let mut last_error: Option<ApiError> = None;
    for replica in &candidates {
        let read = crate::state_store::StateStoreReadRequest {
            owner: assignment.owner.clone(),
            receiver: replica.clone(),
            store_id,
            ownership_epoch: assignment.ownership_epoch,
            key: Vec::new(),
            strict: false,
            wait_ms: 500,
            by_named: None,
            by_named_range: Some(crate::state_store::StateStoreNamedRangeRead {
                name: crate::pipe::WINDOW_KEY_NAME.to_owned(),
                lower: None,
                upper: None,
                limit: scan_limit,
            }),
        };
        let outcome = if Some(replica) == state.storage_node_id.as_ref()
            && state.state_store_journal.is_some()
        {
            state
                .state_store_journal
                .as_ref()
                .unwrap()
                .read_row(read)
                .await
                .map_err(state_store_api_error)
        } else {
            match state_store_transport_for(state, &assignment) {
                Ok(transport) => transport
                    .read(replica, read)
                    .await
                    .map_err(state_store_api_error),
                Err(error) => Err(error),
            }
        };
        match outcome {
            Ok(result) => {
                reply = Some(result);
                break;
            }
            Err(error) => last_error = Some(error),
        }
    }
    let Some(reply) = reply else {
        return Err(last_error.unwrap_or_else(|| {
            ApiError::unavailable("no StateStore replica could serve the active-Key scan")
        }));
    };
    let saturated = reply.result.named_rows.len() >= scan_limit;
    let mut rows = std::collections::BTreeSet::new();
    for row in reply.result.named_rows {
        if let Some(pair) = crate::pipe::window_pane_row_key_parse(&row.key) {
            rows.insert(pair);
        }
    }
    Ok((rows, saturated))
}

fn state_store_coordinator_for(
    state: &AppState,
    assignment: crate::state_store::StateStoreAssignment,
) -> Result<crate::state_store::StateStoreMutationCoordinator, ApiError> {
    let transport = state_store_transport_for(state, &assignment)?;
    Ok(crate::state_store::StateStoreMutationCoordinator::new(
        assignment, transport,
    ))
}

/// Public manual-StateStore write: `POST /v1/state-stores/mutate` replicates
/// one mutation to the store's RF3 journal quorum and applies it on each
/// replica. `mutation_id` is the caller-owned idempotency identity; a retry
/// of the same identity returns the committed outcome without re-applying.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StateStoreMutateRequest {
    store: String,
    mutation_id: Uuid,
    op: StateStoreMutateOpRequest,
}

/// One named-key value on a `put_row` mutation: the caller names a declared
/// key and supplies typed values; `unique` is never caller-supplied — the
/// server stamps it from the store's `named_keys` declaration.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StateStoreNamedValueRequest {
    name: String,
    values: Vec<IndexValueWire>,
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum StateStoreMutateOpRequest {
    PutRow {
        key_base64: String,
        payload_base64: String,
        #[serde(default)]
        named: Vec<StateStoreNamedValueRequest>,
    },
    DeleteRow {
        key_base64: String,
    },
}

#[derive(Serialize)]
struct StateStoreMutateResponse {
    store_id: Uuid,
    mutation_id: Uuid,
    sequence: u64,
    deduplicated: bool,
}

async fn state_store_mutate(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<StateStoreMutateRequest>,
) -> Result<Json<StateStoreMutateResponse>, ApiError> {
    authenticate_credential(&state, &headers)?;
    let store = state
        .control
        .declared_state_store_by_name(&request.store)
        .await
        .ok_or_else(|| {
            ApiError::bad_request(format!("StateStore does not exist: {}", request.store))
        })?;
    authorize_namespace(&state, &headers, &store.name, PermissionAction::Write)?;
    if !matches!(store.source, crate::control::StateStoreSource::Manual) {
        return Err(ApiError::bad_request(
            "mutations apply only to manual StateStores; Feed-derived stores replay from their source",
        ));
    }
    let assignment = state
        .control
        .active_state_store_assignment_by_id(store.store_id)
        .await
        .ok_or_else(|| ApiError::unavailable("StateStore has no mutation-journal placement"))?;
    let op = match request.op {
        StateStoreMutateOpRequest::PutRow {
            key_base64,
            payload_base64,
            named,
        } => {
            if named.len() > crate::control::MAX_STATE_STORE_NAMED_KEYS {
                return Err(ApiError::bad_request(
                    "a row may write at most the store's named-key budget",
                ));
            }
            let mut named_out = Vec::with_capacity(named.len());
            for entry in named {
                let declared = store
                    .named_keys
                    .iter()
                    .find(|key| key.name == entry.name)
                    .ok_or_else(|| {
                        ApiError::bad_request(format!(
                            "StateStore {} does not declare named key {}",
                            store.name, entry.name
                        ))
                    })?;
                named_out.push(crate::state_store::NamedKeyValue {
                    name: entry.name,
                    values: entry
                        .values
                        .into_iter()
                        .map(IndexValueWire::into_value)
                        .collect::<Result<_, _>>()?,
                    unique: declared.unique,
                });
            }
            crate::state_store::StateStoreMutationOp::PutRow {
                key: decode_base64("key_base64", &key_base64)?,
                payload: decode_base64("payload_base64", &payload_base64)?,
                named: named_out,
            }
        }
        StateStoreMutateOpRequest::DeleteRow { key_base64 } => {
            crate::state_store::StateStoreMutationOp::DeleteRow {
                key: decode_base64("key_base64", &key_base64)?,
            }
        }
    };
    let mutation = crate::state_store::StateStoreMutation {
        store_id: store.store_id,
        mutation_id: request.mutation_id,
        request_id: Uuid::new_v4(),
        ownership_epoch: assignment.ownership_epoch,
        sequence: 0,
        op,
        issued_at_ns: unix_ns(),
        source: None,
    };
    let coordinator = state_store_coordinator_for(&state, assignment)?;
    let result = coordinator
        .mutate(mutation)
        .await
        .map_err(state_store_api_error)?;
    Ok(Json(StateStoreMutateResponse {
        store_id: result.store_id,
        mutation_id: result.mutation_id,
        sequence: result.sequence,
        deduplicated: result.deduplicated,
    }))
}

/// `FeedReplaySource` over the bounded committed Feed read path: replay
/// consumes the same merged committed view Readers see, so a StateStore can
/// never materialize uncommitted or skipped history.
struct CommittedFeedReplaySource {
    state: AppState,
}

#[async_trait]
impl crate::state_store::FeedReplaySource for CommittedFeedReplaySource {
    async fn source_exists(&self, feed_id: Uuid) -> bool {
        self.state
            .control
            .active_feed_by_id(feed_id)
            .await
            .is_some()
    }

    async fn read_committed_records(
        &self,
        feed_id: Uuid,
        after: Option<&str>,
        limit: usize,
    ) -> Result<
        Vec<crate::state_store::ReplaySourceRecord>,
        crate::state_store::StateStoreReplayError,
    > {
        let feed = self
            .state
            .control
            .active_feed_by_id(feed_id)
            .await
            .ok_or(crate::state_store::StateStoreReplayError::SourceUnavailable)?;
        let frames =
            read_complete_feed(&self.state, feed_id, &feed.name, after, limit, false, false)
                .await
                .map_err(|error| match error.status {
                    StatusCode::BAD_REQUEST => {
                        crate::state_store::StateStoreReplayError::FrontierOutOfRange
                    }
                    _ => crate::state_store::StateStoreReplayError::SourceRead(error.message),
                })?;
        frames
            .iter()
            .map(|frame| {
                let record = decode_record(&frame.frame).map_err(|error| {
                    crate::state_store::StateStoreReplayError::SourceRead(error.to_string())
                })?;
                Ok(crate::state_store::ReplaySourceRecord {
                    message_id: record.message_id,
                    cursor: frame.cursor.clone(),
                    key: record.key,
                    metadata: record.metadata,
                    payload: record.payload,
                })
            })
            .collect()
    }
}

fn index_build_driver(state: &AppState) -> Option<crate::index_build::IndexBuildDriver> {
    let index = state.state_store_journal.as_ref()?.index_store()?;
    Some(crate::index_build::IndexBuildDriver::new(
        state.control.clone(),
        Arc::new(CommittedFeedReplaySource {
            state: state.clone(),
        }),
        index,
    ))
}

fn state_store_replay_driver(state: &AppState) -> crate::state_store::StateStoreReplayDriver {
    let factory_state = state.clone();
    crate::state_store::StateStoreReplayDriver::new(
        state.control.clone(),
        Arc::new(CommittedFeedReplaySource {
            state: state.clone(),
        }),
        Arc::new(move |assignment| {
            state_store_coordinator_for(&factory_state, assignment.clone())
                .map_err(|_| crate::state_store::StateStoreMutationError::Unavailable)
        }),
    )
}

fn state_store_replay_api_error(error: crate::state_store::StateStoreReplayError) -> ApiError {
    match error {
        // A dropped source Feed or a frontier that outlived retention is a
        // permanent condition: refuse rather than silently materialize an
        // incomplete store. A rebuild (or re-declaration) is the recovery.
        crate::state_store::StateStoreReplayError::SourceUnavailable
        | crate::state_store::StateStoreReplayError::FrontierOutOfRange => {
            ApiError::conflict(error.to_string())
        }
        crate::state_store::StateStoreReplayError::ManualSource => {
            ApiError::bad_request(error.to_string())
        }
        crate::state_store::StateStoreReplayError::SourceRead(_) => {
            ApiError::unavailable(error.to_string())
        }
        crate::state_store::StateStoreReplayError::Mutation(inner) => state_store_api_error(inner),
    }
}

/// Records one bounded replay pass can commit per request.
const STATE_STORE_REPLAY_PASS_LIMIT: usize = 64;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StateStoreSyncRequest {
    store: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StateStoreRebuildRequest {
    store: String,
    /// Caller-owned identity for the rebuild pass; a retried rebuild with
    /// the same token dedupes its already-committed replay mutations.
    token: Option<Uuid>,
}

#[derive(Serialize)]
struct StateStoreSyncResponse {
    store_id: Uuid,
    applied: u64,
    frontier: Option<String>,
    complete: bool,
}

async fn feed_derived_store(
    state: &AppState,
    name: &str,
) -> Result<crate::control::StateStoreDefinition, ApiError> {
    let store = state
        .control
        .declared_state_store_by_name(name)
        .await
        .ok_or_else(|| ApiError::bad_request(format!("StateStore does not exist: {name}")))?;
    if !matches!(store.source, crate::control::StateStoreSource::Feed { .. }) {
        return Err(ApiError::bad_request(
            "replay applies only to Feed-derived StateStores; manual stores accept mutations",
        ));
    }
    Ok(store)
}

/// `POST /v1/state-stores/sync` — resume a Feed-derived StateStore's replay
/// from its durable frontier, committing up to the bounded pass limit as
/// quorum-journaled mutations.
async fn state_store_sync(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<StateStoreSyncRequest>,
) -> Result<Json<StateStoreSyncResponse>, ApiError> {
    authenticate_credential(&state, &headers)?;
    let store = feed_derived_store(&state, &request.store).await?;
    authorize_namespace(&state, &headers, &store.name, PermissionAction::Write)?;
    let outcome = state_store_replay_driver(&state)
        .pass(&store, None, STATE_STORE_REPLAY_PASS_LIMIT)
        .await
        .map_err(state_store_replay_api_error)?;
    Ok(Json(StateStoreSyncResponse {
        store_id: outcome.store_id,
        applied: outcome.applied,
        frontier: outcome.frontier,
        complete: outcome.complete,
    }))
}

/// `POST /v1/state-stores/rebuild` — re-materialize a Feed-derived
/// StateStore from the beginning of its retained source history. Refuses
/// when the declared source Feed no longer resolves rather than producing
/// a partial store.
async fn state_store_rebuild(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<StateStoreRebuildRequest>,
) -> Result<Json<StateStoreSyncResponse>, ApiError> {
    authenticate_credential(&state, &headers)?;
    let store = feed_derived_store(&state, &request.store).await?;
    authorize_namespace(&state, &headers, &store.name, PermissionAction::Write)?;
    let outcome = state_store_replay_driver(&state)
        .pass(
            &store,
            Some(request.token.unwrap_or_else(Uuid::new_v4)),
            STATE_STORE_REPLAY_PASS_LIMIT,
        )
        .await
        .map_err(state_store_replay_api_error)?;
    Ok(Json(StateStoreSyncResponse {
        store_id: outcome.store_id,
        applied: outcome.applied,
        frontier: outcome.frontier,
        complete: outcome.complete,
    }))
}

/// Per-store outcome of one background replay drive cycle.
#[derive(Debug)]
pub enum StateStoreDriveOutcome {
    /// This Node is not the store's journal owner; the owner drives.
    NotOwner,
    /// The source had no uncommitted-to-store records this cycle.
    Idle,
    /// Records replayed this cycle.
    Synced {
        store_id: Uuid,
        applied: u64,
        complete: bool,
    },
    /// Replay refused: the source Feed or the retained frontier is gone.
    Refused { store_id: Uuid, message: String },
    /// The pass failed transiently; deterministic identities make the next
    /// cycle a safe retry.
    Failed { store_id: Uuid, message: String },
}

/// One supervisor tick: for every Feed-derived StateStore this Node owns,
/// run one bounded replay pass. Ownership lives in the persisted placement,
/// so owner loss or movement simply changes which Node's tick replays next;
/// deterministic replay identities keep a drive split across an ownership
/// change from duplicating or losing work.
pub async fn drive_local_state_stores(
    state: &AppState,
    limit: usize,
) -> Vec<StateStoreDriveOutcome> {
    let Some(local) = state.storage_node_id.clone() else {
        return Vec::new();
    };
    let stores = state.control.state_store_definitions().await;
    let mut outcomes = Vec::new();
    for store in stores {
        let crate::control::StateStoreSource::Feed { .. } = store.source else {
            continue;
        };
        let Some(assignment) = state
            .control
            .active_state_store_assignment_by_id(store.store_id)
            .await
        else {
            outcomes.push(StateStoreDriveOutcome::Failed {
                store_id: store.store_id,
                message: "StateStore journal placement is unavailable".to_owned(),
            });
            continue;
        };
        if assignment.owner != local {
            outcomes.push(StateStoreDriveOutcome::NotOwner);
            continue;
        }
        match state_store_replay_driver(state)
            .pass(&store, None, limit)
            .await
        {
            Ok(outcome) if outcome.applied == 0 => outcomes.push(StateStoreDriveOutcome::Idle),
            Ok(outcome) => outcomes.push(StateStoreDriveOutcome::Synced {
                store_id: outcome.store_id,
                applied: outcome.applied,
                complete: outcome.complete,
            }),
            Err(
                crate::state_store::StateStoreReplayError::SourceUnavailable
                | crate::state_store::StateStoreReplayError::FrontierOutOfRange,
            ) => outcomes.push(StateStoreDriveOutcome::Refused {
                store_id: store.store_id,
                message: "Feed-derived replay refused; rebuild or re-declare".to_owned(),
            }),
            Err(error) => outcomes.push(StateStoreDriveOutcome::Failed {
                store_id: store.store_id,
                message: error.to_string(),
            }),
        }
    }
    outcomes
}

/// Per-Index outcome of one shadow-generation build drive cycle.
#[derive(Clone, Debug)]
pub enum IndexBuildDriveOutcome {
    /// Records applied to the shadow generation this cycle.
    Building {
        index_id: Uuid,
        generation: u64,
        applied: usize,
    },
    /// The shadow consumed the committed frontier and the Control Plane
    /// committed the atomic activation.
    Activated { index_id: Uuid, generation: u64 },
    /// Another Node completed the same generation first; idempotent.
    Superseded { index_id: Uuid, generation: u64 },
    /// Rebuild refused: the source Feed or the retained resume frontier is
    /// gone; the definition stays Building until an operator intervenes.
    Refused { index_id: Uuid, message: String },
    /// The pass failed transiently; the durable shadow Cursor makes the
    /// next cycle a safe resume.
    Failed { index_id: Uuid, message: String },
}

/// One supervisor tick: for every catalog Index in `Building`, apply one
/// bounded pass of committed history to this Node's shadow generation, then
/// atomically activate through the Control Plane when caught up. Every Data
/// Node builds deterministically from the same immutable history, so local
/// shadows converge without placement; activation is idempotent.
pub async fn drive_local_index_builds(
    state: &AppState,
    limit: usize,
) -> Vec<IndexBuildDriveOutcome> {
    let Some(driver) = index_build_driver(state) else {
        return Vec::new();
    };
    driver
        .run(limit)
        .await
        .into_iter()
        .map(|(index_id, result)| match result {
            Ok(pass) if pass.activated => IndexBuildDriveOutcome::Activated {
                index_id,
                generation: pass.generation,
            },
            Ok(pass) if pass.superseded => IndexBuildDriveOutcome::Superseded {
                index_id,
                generation: pass.generation,
            },
            Ok(pass) => IndexBuildDriveOutcome::Building {
                index_id,
                generation: pass.generation,
                applied: pass.applied,
            },
            Err(
                crate::index_build::IndexBuildError::SourceUnavailable
                | crate::index_build::IndexBuildError::FrontierOutOfRange,
            ) => IndexBuildDriveOutcome::Refused {
                index_id,
                message: "Index rebuild refused; source history is gone or out of retention"
                    .to_owned(),
            },
            Err(error) => IndexBuildDriveOutcome::Failed {
                index_id,
                message: error.to_string(),
            },
        })
        .collect()
}

/// Outcome of one catch-up pass on a locally replicated StateStore.
#[derive(Clone, Debug)]
pub enum StateStoreCatchUpDriveOutcome {
    /// The replica installed/applied entries or is already caught up.
    Progress {
        store_id: Uuid,
        installed: u64,
        applied: u64,
        complete: bool,
    },
    /// This Node does not hold a journal replica for the store.
    NotReplica,
    /// The pass failed transiently; the next tick retries the same bounded
    /// work — installed entries and applied digests are idempotent.
    Failed { store_id: Uuid, message: String },
}

/// One supervisor tick of StateStore replica catch-up: for every store this
/// Node replicates, pull missing committed entries from the freshest peer
/// and apply them, verifying the applied chain digest wherever the source
/// attested one. Catch-up is what a replica that missed a quorum commit —
/// restart, network partition, fresh replacement — uses to rejoin without
/// application or operator involvement.
pub async fn drive_local_state_store_catch_ups(
    state: &AppState,
    budget: usize,
) -> Vec<StateStoreCatchUpDriveOutcome> {
    let Some(local) = state.storage_node_id.clone() else {
        return Vec::new();
    };
    let Some(service) = state.state_store_journal.clone() else {
        return Vec::new();
    };
    let mut outcomes = Vec::new();
    for store in state.control.state_store_definitions().await {
        let Some(assignment) = state
            .control
            .active_state_store_assignment_by_id(store.store_id)
            .await
        else {
            outcomes.push(StateStoreCatchUpDriveOutcome::Failed {
                store_id: store.store_id,
                message: "StateStore journal placement is unavailable".to_owned(),
            });
            continue;
        };
        if !assignment.replicas.contains(&local) {
            outcomes.push(StateStoreCatchUpDriveOutcome::NotReplica);
            continue;
        }
        let transport = match state_store_transport_for(state, &assignment) {
            Ok(transport) => transport,
            Err(error) => {
                outcomes.push(StateStoreCatchUpDriveOutcome::Failed {
                    store_id: store.store_id,
                    message: error.message,
                });
                continue;
            }
        };
        match service
            .catch_up_replica(&transport, &assignment, budget)
            .await
        {
            Ok(outcome) => outcomes.push(StateStoreCatchUpDriveOutcome::Progress {
                store_id: store.store_id,
                installed: outcome.installed,
                applied: outcome.applied,
                complete: outcome.complete,
            }),
            Err(error) => outcomes.push(StateStoreCatchUpDriveOutcome::Failed {
                store_id: store.store_id,
                message: error.to_string(),
            }),
        }
    }
    outcomes
}

/// Build the journal coordinator bound to a Subscription's progress
/// assignment over the configured internal transport (mTLS or shared key).
fn effect_coordinator_for(
    state: &AppState,
    assignment: crate::reader::SubscriptionProgressAssignment,
) -> Result<crate::effect::EffectCoordinator, ApiError> {
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
    Ok(crate::effect::EffectCoordinator::new(
        assignment,
        effect_transport,
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
    let journal = effect_coordinator_for(&state, assignment.clone())?;
    let (progress, _) = member_assignment_coordinator(&state, assignment)?;
    let sink = LiveEffectApply {
        state: &state,
        progress,
        subscription_id: request.subscription_id,
        ownership_epoch: request.ownership_epoch,
        sweep_depth: 0,
    };
    journal
        .apply_committed(request.effect_id, &sink)
        .await
        .map(Json)
        .map_err(effect_journal_api_error)
}

/// Wires committed effect side effects to the real Feed append path, the
/// Subscription progress journal, and the pending-tail release that makes
/// outputs visible after the terminal journal marker is durable.
struct LiveEffectApply<'a> {
    state: &'a AppState,
    progress: crate::reader::SubscriptionProgressCoordinator,
    subscription_id: Uuid,
    ownership_epoch: u64,
    /// How many nested occupant sweeps built this sink. Occupant resolution
    /// constructs a fresh sink for the occupant's own journal at depth+1 and
    /// refuses beyond one level so a wedged chain fails closed instead of
    /// recursing across Subscription scopes.
    sweep_depth: u8,
}

/// The owner-side result of a pending (effect-owned) append.
enum EffectAppendOutcome {
    Appended {
        range_id: crate::active_range::RangeId,
        position: crate::active_range::RangePosition,
    },
    /// A different effect holds the tail; its identity travels back so the
    /// apply can sweep the occupant through its own journal.
    Occupied {
        occupant: crate::active_range::PendingEffect,
    },
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
        effect_id: Uuid,
    ) -> Result<crate::effect::EffectOutputReceipt, crate::effect::EffectJournalError> {
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
            metadata_base64: output.metadata_base64.clone(),
            use_compression: false,
            pending_effect: Some(crate::active_range::PendingEffectRef {
                effect_id,
                subscription_id: self.subscription_id,
            }),
        };
        match route_effect_append_to_owner(self.state, feed.feed_id, request).await {
            Ok(EffectAppendOutcome::Appended { range_id, position }) => {
                Ok(crate::effect::EffectOutputReceipt {
                    feed_id: feed.feed_id,
                    range_id,
                    position,
                })
            }
            Ok(EffectAppendOutcome::Occupied { occupant }) => {
                Err(crate::effect::EffectJournalError::PendingOccupied {
                    effect_id: occupant.effect_id,
                    subscription_id: occupant.subscription_id,
                })
            }
            Err(error) => Err(if error.status.is_server_error() {
                crate::effect::EffectJournalError::Unavailable
            } else {
                crate::effect::EffectJournalError::Conflict
            }),
        }
    }

    async fn release_output(
        &self,
        target: &crate::effect::EffectReleaseTarget,
    ) -> Result<crate::effect::EffectReleaseOutcome, crate::effect::EffectJournalError> {
        match route_pending_release_to_owner(self.state, target).await {
            Ok(crate::active_range::PendingReleaseOutcome::Released { commit_position }) => {
                Ok(crate::effect::EffectReleaseOutcome::Released {
                    committed: commit_position.value(),
                })
            }
            Ok(crate::active_range::PendingReleaseOutcome::NotPending { committed }) => {
                Ok(crate::effect::EffectReleaseOutcome::NotPending { committed })
            }
            Ok(crate::active_range::PendingReleaseOutcome::Occupied { occupant }) => {
                Ok(crate::effect::EffectReleaseOutcome::Occupied {
                    occupant: crate::effect::EffectOccupant {
                        effect_id: occupant.effect_id,
                        subscription_id: occupant.subscription_id,
                    },
                })
            }
            Err(error) => Err(if error.status.is_server_error() {
                crate::effect::EffectJournalError::Unavailable
            } else {
                crate::effect::EffectJournalError::Conflict
            }),
        }
    }

    async fn discard_output(
        &self,
        output: &crate::effect::EffectOutput,
        effect_id: uuid::Uuid,
        subscription_id: uuid::Uuid,
    ) -> Result<crate::effect::EffectDiscardOutcome, crate::effect::EffectJournalError> {
        match route_pending_discard_to_owner(self.state, output, effect_id, subscription_id).await {
            Ok(crate::active_range::PendingDiscardOutcome::Discarded { dropped_frames }) => {
                Ok(crate::effect::EffectDiscardOutcome::Discarded { dropped_frames })
            }
            Ok(crate::active_range::PendingDiscardOutcome::NotPending) => {
                Ok(crate::effect::EffectDiscardOutcome::NotPending)
            }
            Ok(crate::active_range::PendingDiscardOutcome::Occupied(occupant)) => {
                Ok(crate::effect::EffectDiscardOutcome::Occupied {
                    occupant: crate::effect::EffectOccupant {
                        effect_id: occupant.effect_id,
                        subscription_id: occupant.subscription_id,
                    },
                })
            }
            Err(error) => Err(if error.status.is_server_error() {
                crate::effect::EffectJournalError::Unavailable
            } else {
                crate::effect::EffectJournalError::Conflict
            }),
        }
    }

    async fn mutate_state(
        &self,
        mutation: &crate::effect::EffectStateMutation,
    ) -> Result<(), crate::effect::EffectJournalError> {
        let assignment = self
            .state
            .control
            .active_state_store_assignment_by_id(mutation.store_id)
            .await
            .ok_or(crate::effect::EffectJournalError::Unavailable)?;
        let coordinator = state_store_coordinator_for(self.state, assignment.clone())
            .map_err(|_| crate::effect::EffectJournalError::Unavailable)?;
        coordinator
            .mutate(crate::state_store::StateStoreMutation {
                store_id: mutation.store_id,
                mutation_id: mutation.mutation_id,
                request_id: crate::effect::effect_apply_request_id(
                    mutation.mutation_id,
                    b"state-request",
                ),
                ownership_epoch: assignment.ownership_epoch,
                sequence: 0,
                op: mutation.op.clone(),
                issued_at_ns: unix_ns(),
                source: None,
            })
            .await
            .map(|_| ())
            .map_err(|error| match error {
                crate::state_store::StateStoreMutationError::NoQuorum
                | crate::state_store::StateStoreMutationError::Unavailable
                | crate::state_store::StateStoreMutationError::AmbiguousCommit
                | crate::state_store::StateStoreMutationError::ApplyFailed
                | crate::state_store::StateStoreMutationError::Diverged => {
                    crate::effect::EffectJournalError::Unavailable
                }
                _ => crate::effect::EffectJournalError::Conflict,
            })
    }

    async fn resolve_pending(
        &self,
        occupant: crate::effect::EffectOccupant,
    ) -> Result<(), crate::effect::EffectJournalError> {
        if self.sweep_depth >= 1 {
            // Nested sweeps would recurse across Subscription scopes; the
            // occupant resolves on its own drive instead.
            return Err(crate::effect::EffectJournalError::Unavailable);
        }
        let assignment = self
            .state
            .control
            .active_subscription_progress_assignment_by_id(occupant.subscription_id)
            .await
            .ok_or(crate::effect::EffectJournalError::Unavailable)?;
        let journal = effect_coordinator_for(self.state, assignment.clone())
            .map_err(|_| crate::effect::EffectJournalError::Unavailable)?;
        let (progress, _) = member_assignment_coordinator(self.state, assignment.clone())
            .map_err(|_| crate::effect::EffectJournalError::Unavailable)?;
        let sink = LiveEffectApply {
            state: self.state,
            progress,
            subscription_id: occupant.subscription_id,
            ownership_epoch: assignment.ownership_epoch,
            sweep_depth: self.sweep_depth + 1,
        };
        journal
            .apply_committed(occupant.effect_id, &sink)
            .await
            .map(|_| ())
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
            quarantined: committed
                .as_ref()
                .map_or_else(BTreeMap::new, |prior| prior.quarantined.clone()),
            quarantine_add: BTreeMap::new(),
            quarantine_remove: BTreeMap::new(),
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
    authenticate_credential(&state, &headers)?;
    let reader = reader_when_applied(
        &state.control,
        &request.reader,
        state.control_plane.is_some(),
    )
    .await
    .ok_or_else(|| ApiError::bad_request("Reader does not exist"))?;
    authorize_resource_feed(&state, &headers, reader.feed_id, PermissionAction::Read).await?;
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
    record_reader_traffic(
        &state,
        reader.reader_id,
        crate::demand::ReaderTrafficOp::Open,
        0,
    );
    Ok(Json(reader_session_response(reader)))
}

/// Fold one served named-Reader session request into Node-local demand
/// attribution. Replays count as served demand, matching the Subscription
/// contract: the replay still costs a committed-frontier read.
fn record_reader_traffic(
    state: &AppState,
    reader_id: Uuid,
    op: crate::demand::ReaderTrafficOp,
    records: usize,
) {
    state.demand.record_reader_op(
        reader_id,
        crate::demand::ReaderTrafficSample {
            op,
            records: records as u64,
            sampled_at_ns: unix_ns(),
        },
    );
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

/// Replay the exact page a duplicate `fetch_request_id` already committed:
/// the persisted `previous`/`delivered` positions bracket the last fetch and
/// `last_fetch_limit` its bound, so re-reading immutable history must yield
/// the same frames and the same deterministic progress token. A retry after
/// an ambiguous transport failure therefore observes the identical page
/// instead of a forced session reopen. If placement moved or the proof does
/// not hold, the caller gets the original reopen instruction rather than a
/// silently different page.
async fn replay_reader_fetch(
    state: &AppState,
    reader: &crate::control::ReaderDefinition,
    feed_name: &str,
    request: &ReaderFetchRequest,
) -> Result<Json<ReaderFetchResponse>, ApiError> {
    let reopen = || {
        ApiError::unavailable(
            "Reader fetch request already delivered; reopen the session to replay unacknowledged progress",
        )
    };
    let frontier = state
        .control
        .active_reader_frontier(reader.reader_id)
        .await
        .filter(|value| value.last_fetch_request_id == Some(request.request_id))
        .ok_or_else(reopen)?;
    if frontier.last_fetch_limit == 0 || frontier.delivered.is_empty() {
        return Err(reopen());
    }
    let (frames, progress) = read_reader_frontier(
        state,
        reader.feed_id,
        feed_name,
        &frontier.previous,
        frontier.last_fetch_limit,
    )
    .await?;
    if frames.is_empty() || progress != frontier.delivered {
        return Err(reopen());
    }
    let fresh = state
        .control
        .active_reader_by_name(&reader.name)
        .await
        .unwrap_or_else(|| reader.clone());
    let source = record_source_by_id(state, reader.feed_id).await;
    let records = frames
        .into_iter()
        .map(|item| {
            decode_record(&item.frame).map(|record| {
                record_response(
                    CursorRecord {
                        cursor: item.cursor,
                        record,
                    },
                    source.clone(),
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| ApiError::unavailable(error.to_string()))?;
    state.demand.record_read(records.len());
    record_reader_traffic(
        state,
        reader.reader_id,
        crate::demand::ReaderTrafficOp::Fetch,
        records.len(),
    );
    // A replay serves the identical page, so it reports the advice that
    // page was paced under: both values come from the persisted frontier
    // rather than a Node-local plan that may describe a sibling fetch on
    // this ingress.
    Ok(Json(ReaderFetchResponse {
        records,
        delivered_cursor: Some(reader_frontier_cursor(
            reader.reader_id,
            request.session_epoch,
            request.request_id,
        )),
        acknowledged_cursor: fresh.acknowledged_cursor,
        recommended_capacity: frontier.last_fetch_limit,
        retry_after_ms: frontier.last_fetch_retry_after_ms,
    }))
}

async fn reader_fetch(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<ReaderFetchRequest>,
) -> Result<Json<ReaderFetchResponse>, ApiError> {
    authenticate_credential(&state, &headers)?;
    let reader = reader_when_applied(
        &state.control,
        &request.reader,
        state.control_plane.is_some(),
    )
    .await
    .ok_or_else(|| ApiError::bad_request("Reader does not exist"))?;
    let feed = state
        .control
        .active_feed_by_id(reader.feed_id)
        .await
        .ok_or_else(|| ApiError::unavailable("Reader Feed is not applied on this Node"))?;
    authorize_namespace(&state, &headers, &feed.name, PermissionAction::Read)?;
    let mut reader = reader;
    // A committed open can lag on this Node's applied catalog; wait briefly
    // for the session to land instead of fencing a fresh epoch. An applied
    // epoch ahead of the request can never match again, so fail fast.
    for _ in 0..20 {
        if reader.session_active && reader.session_epoch == request.session_epoch {
            break;
        }
        if state.control_plane.is_none() || reader.session_epoch > request.session_epoch {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        if let Some(fresh) = state.control.active_reader_by_name(&request.reader).await {
            reader = fresh;
        }
    }
    if !reader.session_active || reader.session_epoch != request.session_epoch {
        return Err(ApiError::bad_request("Reader session is stale or inactive"));
    }
    let frontier = state.control.active_reader_frontier(reader.reader_id).await;
    if frontier
        .as_ref()
        .and_then(|value| value.last_fetch_request_id)
        == Some(request.request_id)
    {
        return replay_reader_fetch(&state, &reader, &feed.name, &request).await;
    }
    let client_limit = request
        .limit
        .unwrap_or(reader.session_capacity)
        .min(reader.session_capacity)
        .max(1);
    let node_pressure = (state.demand.snapshot().requests_in_flight as f64 / 100.0).clamp(0.0, 1.0);
    let pacing = state.reader_pacing.plan(
        reader.reader_id,
        reader.session_capacity,
        client_limit,
        node_pressure,
        MAX_LOGICAL_READ_BYTES,
    );
    if pacing.max_records == 0 {
        // The delivered-byte budget is exhausted or records exceed it:
        // defer delivery honestly through the response advice instead
        // of silently shrinking the page or erroring the session.
        record_reader_traffic(
            &state,
            reader.reader_id,
            crate::demand::ReaderTrafficOp::Fetch,
            0,
        );
        return Ok(Json(ReaderFetchResponse {
            records: Vec::new(),
            delivered_cursor: reader.delivered_cursor.clone(),
            acknowledged_cursor: reader.acknowledged_cursor.clone(),
            recommended_capacity: 0,
            retry_after_ms: pacing.retry_after.as_millis().min(u64::MAX as u128) as u64,
        }));
    }
    let limit = pacing.max_records;
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
    let source = record_source_by_id(&state, reader.feed_id).await;
    let page_bytes = frames
        .iter()
        .map(|item| item.frame.len().saturating_add(item.cursor.len()))
        .sum::<usize>();
    let records = frames
        .into_iter()
        .map(|item| {
            decode_record(&item.frame).map(|record| {
                record_response(
                    CursorRecord {
                        cursor: item.cursor,
                        record,
                    },
                    source.clone(),
                )
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
                fetch_limit: Some(limit),
                fetch_retry_after_ms: Some(
                    pacing.retry_after.as_millis().min(u64::MAX as u128) as u64
                ),
            }
        } else {
            crate::control::Command::RecordReaderDelivery {
                reader: request.reader.clone(),
                session_epoch: request.session_epoch,
                cursor,
            }
        };
        let execution = match execute_reader_command(&state, command, request.request_id).await {
            Ok(execution) => execution,
            Err(error) => {
                if expected_frontier.is_some() {
                    // A duplicate racing the same fetch_request_id loses the
                    // frontier commit after the winner landed; re-derive the
                    // stored page instead of refusing when it proves identical.
                    if let Ok(response) =
                        replay_reader_fetch(&state, &reader, &feed.name, &request).await
                    {
                        return Ok(response);
                    }
                }
                return Err(
                    if expected_frontier.is_some() && error.status == StatusCode::BAD_REQUEST {
                        ApiError::unavailable(format!(
                        "Reader progress was not committed; reopen the session to retry safely: {}",
                        error.message
                    ))
                    } else {
                        error
                    },
                );
            }
        };
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
    record_reader_traffic(
        &state,
        reader.reader_id,
        crate::demand::ReaderTrafficOp::Fetch,
        records.len(),
    );
    state
        .reader_pacing
        .record_page(reader.reader_id, records.len(), page_bytes, limit);
    Ok(Json(ReaderFetchResponse {
        records,
        delivered_cursor,
        acknowledged_cursor: reader.acknowledged_cursor,
        recommended_capacity: pacing.max_records,
        retry_after_ms: pacing.retry_after.as_millis().min(u64::MAX as u128) as u64,
    }))
}

async fn reader_ack(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<ReaderAckRequest>,
) -> Result<Json<ReaderSessionResponse>, ApiError> {
    authenticate_credential(&state, &headers)?;
    let reader = reader_when_applied(
        &state.control,
        &request.reader,
        state.control_plane.is_some(),
    )
    .await
    .ok_or_else(|| ApiError::bad_request("Reader does not exist"))?;
    authorize_resource_feed(&state, &headers, reader.feed_id, PermissionAction::Read).await?;
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
    state.reader_pacing.observe_ack(reader.reader_id);
    record_reader_traffic(
        &state,
        reader.reader_id,
        crate::demand::ReaderTrafficOp::Ack,
        0,
    );
    Ok(Json(reader_session_response(reader)))
}

async fn reader_close(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<ReaderFetchRequest>,
) -> Result<Json<ReaderSessionResponse>, ApiError> {
    authenticate_credential(&state, &headers)?;
    let reader = reader_when_applied(
        &state.control,
        &request.reader,
        state.control_plane.is_some(),
    )
    .await
    .ok_or_else(|| ApiError::bad_request("Reader does not exist"))?;
    authorize_resource_feed(&state, &headers, reader.feed_id, PermissionAction::Read).await?;
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
    state.reader_pacing.remove(reader.reader_id);
    record_reader_traffic(
        &state,
        reader.reader_id,
        crate::demand::ReaderTrafficOp::Close,
        0,
    );
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
    authorize_namespace(&state, &headers, &request.feed, PermissionAction::Read)?;
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
    let feed = feed_when_applied(&state.control, &request.feed, state.control_plane.is_some())
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
            let source = record_source_by_name(&state, &request.feed).await;
            let records = frames
                .into_iter()
                .map(|item| {
                    decode_record(&item.frame).map(|record| {
                        record_response(
                            CursorRecord {
                                cursor: item.cursor,
                                record,
                            },
                            source.clone(),
                        )
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
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub use_compression: bool,
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
                .checked_add(logical_frame_bytes(&decoded)?.saturating_add(frame.cursor.len()))
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
    let mut source = if snapshots.is_empty() {
        Vec::new()
    } else {
        match read_cutover_lineage(&state, &source_assignment, source_commit).await {
            Ok(entries) => entries,
            Err(error) => {
                unfreeze_split_nodes(&state, &plan, &source_assignment, key).await;
                return Err(error);
            }
        }
    };
    let reader_translations = if snapshots.is_empty() {
        Vec::new()
    } else {
        let assignments = state
            .control
            .active_range_assignments_for_feed(plan.feed_id)
            .await;
        match split_reader_translations(&snapshots, &assignments, &plan, &source) {
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
    // Stage Subscription frontiers only after the plan reaches Ready so the
    // window in which a member acknowledgement can still move them is one
    // consensus commit rather than the whole staging copy. The translations
    // then ride inside the activation command as durable pending evidence.
    let subscription_frontiers = match subscription_cutover_frontiers(&state, plan.feed_id).await {
        Ok(frontiers) => frontiers,
        Err(error) => {
            unfreeze_split_nodes(&state, &plan, &source_assignment, key).await;
            return Err(error);
        }
    };
    if snapshots.is_empty() && !subscription_frontiers.is_empty() {
        source = match read_cutover_lineage(&state, &source_assignment, source_commit).await {
            Ok(entries) => entries,
            Err(error) => {
                unfreeze_split_nodes(&state, &plan, &source_assignment, key).await;
                return Err(error);
            }
        };
    }
    let subscription_translations = match subscription_cutover_translations(
        &subscription_frontiers,
        &CutoverFrontierTranslation::Split {
            entries: &source,
            plan: &plan,
        },
        plan.plan_id,
    ) {
        Ok(translations) => translations,
        Err(error) => {
            unfreeze_split_nodes(&state, &plan, &source_assignment, key).await;
            return Err(error);
        }
    };
    if let Err(error) = control_plane
        .execute_commands(vec![crate::control::Command::ActivateActiveRangeSplit {
            feed: request.feed,
            plan_id: plan.plan_id,
            left_writer_sequences: evidence.left.writer_sequences.clone(),
            right_writer_sequences: evidence.right.writer_sequences.clone(),
            reader_translations,
            subscription_translations,
        }])
        .await
    {
        unfreeze_split_nodes(&state, &plan, &source_assignment, key).await;
        return Err(ApiError::unavailable(error.to_string()));
    }
    // The progress shards are a separate quorum, so committed Subscription
    // frontiers apply after activation from the pending translation records
    // the command just committed; a failure stalls that Subscription
    // fail-closed (member fetches conflict) instead of silently skipping
    // history, and the durable pending record lets any later member
    // operation retry the apply.
    let mut subscription_reconcile = Vec::new();
    let mut subscription_reconcile_failed = false;
    for staged in &subscription_frontiers {
        let outcome = async {
            // The activation just committed; a follower-served request can see
            // the pending record a few milliseconds later, so wait briefly
            // rather than reporting a phantom missing translation.
            let mut pending = None;
            for _ in 0..20 {
                if let Some(found) = state
                    .control
                    .pending_subscription_cutover(staged.definition.subscription_id)
                    .await
                {
                    pending = Some(found);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            let pending = pending.ok_or_else(|| {
                ApiError::unavailable("pending Subscription cutover was not committed")
            })?;
            let Some(assignment) = state
                .control
                .active_subscription_progress_assignment_by_id(staged.definition.subscription_id)
                .await
            else {
                return Ok("no_placement");
            };
            let (coordinator, assignment) = member_assignment_coordinator(&state, assignment)?;
            apply_pending_subscription_cutover(&state, &pending, &coordinator, &assignment).await
        }
        .await;
        match outcome {
            Ok(status) => subscription_reconcile
                .push(json!({ "subscription": staged.definition.name, "status": status })),
            Err(error) => {
                subscription_reconcile_failed = true;
                subscription_reconcile.push(
                    json!({ "subscription": staged.definition.name, "status": "failed", "error": error.message }),
                );
            }
        }
    }
    Ok(Json(json!({
        "status": "activated",
        "source_commit": source_commit,
        "left_commit": evidence.left.target_commit,
        "right_commit": evidence.right.target_commit,
        "range_map": plan.candidate_map,
        "subscription_frontiers": subscription_reconcile,
        "subscription_reconcile_failed": subscription_reconcile_failed,
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
    let lineage_loaded = !snapshots.is_empty();
    let (mut left_rows, mut right_rows) = if snapshots.is_empty() {
        (Vec::new(), Vec::new())
    } else {
        let calculated = async {
            Ok::<_, ApiError>((
                read_cutover_lineage(&state, &left, staged.left_commit).await?,
                read_cutover_lineage(&state, &right, staged.right_commit).await?,
            ))
        }
        .await;
        match calculated {
            Ok(rows) => rows,
            Err(error) => {
                unfreeze_merge_nodes(&state, &plan, &left, &right, key).await;
                return Err(error);
            }
        }
    };
    let reader_translations = if snapshots.is_empty() {
        Vec::new()
    } else {
        let assignments = state
            .control
            .active_range_assignments_for_feed(plan.feed_id)
            .await;
        match merge_reader_translations(&snapshots, &assignments, &plan, &left_rows, &right_rows) {
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
    // Stage Subscription frontiers only after the plan reaches Ready so the
    // window in which a member acknowledgement can still move them is one
    // consensus commit rather than the whole staging copy. The translations
    // then ride inside the activation command as durable pending evidence.
    let subscription_frontiers = match subscription_cutover_frontiers(&state, plan.feed_id).await {
        Ok(frontiers) => frontiers,
        Err(error) => {
            unfreeze_merge_nodes(&state, &plan, &left, &right, key).await;
            return Err(error);
        }
    };
    if !lineage_loaded && !subscription_frontiers.is_empty() {
        let calculated = async {
            Ok::<_, ApiError>((
                read_cutover_lineage(&state, &left, staged.left_commit).await?,
                read_cutover_lineage(&state, &right, staged.right_commit).await?,
            ))
        }
        .await;
        match calculated {
            Ok(rows) => {
                (left_rows, right_rows) = rows;
            }
            Err(error) => {
                unfreeze_merge_nodes(&state, &plan, &left, &right, key).await;
                return Err(error);
            }
        }
    }
    let subscription_translations = match subscription_cutover_translations(
        &subscription_frontiers,
        &CutoverFrontierTranslation::Merge {
            left_entries: &left_rows,
            right_entries: &right_rows,
            plan: &plan,
        },
        plan.plan_id,
    ) {
        Ok(translations) => translations,
        Err(error) => {
            unfreeze_merge_nodes(&state, &plan, &left, &right, key).await;
            return Err(error);
        }
    };
    control_plane
        .execute_commands(vec![crate::control::Command::ActivateActiveRangeMerge {
            feed: request.feed,
            plan_id: plan.plan_id,
            writer_sequences: staged.writer_sequences.clone(),
            reader_translations,
            subscription_translations,
        }])
        .await
        .map_err(|error| ApiError::unavailable(error.to_string()))?;
    // Same post-activation apply as the split path: the pending translation
    // records committed with the activation drive the progress-shard writes,
    // Cursor-CAS fencing keeps acknowledged work from rewinding, and a later
    // member operation retries any apply this handler could not finish.
    let mut subscription_reconcile = Vec::new();
    let mut subscription_reconcile_failed = false;
    for staged in &subscription_frontiers {
        let outcome = async {
            // The activation just committed; a follower-served request can see
            // the pending record a few milliseconds later, so wait briefly
            // rather than reporting a phantom missing translation.
            let mut pending = None;
            for _ in 0..20 {
                if let Some(found) = state
                    .control
                    .pending_subscription_cutover(staged.definition.subscription_id)
                    .await
                {
                    pending = Some(found);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            let pending = pending.ok_or_else(|| {
                ApiError::unavailable("pending Subscription cutover was not committed")
            })?;
            let Some(assignment) = state
                .control
                .active_subscription_progress_assignment_by_id(staged.definition.subscription_id)
                .await
            else {
                return Ok("no_placement");
            };
            let (coordinator, assignment) = member_assignment_coordinator(&state, assignment)?;
            apply_pending_subscription_cutover(&state, &pending, &coordinator, &assignment).await
        }
        .await;
        match outcome {
            Ok(status) => subscription_reconcile
                .push(json!({ "subscription": staged.definition.name, "status": status })),
            Err(error) => {
                subscription_reconcile_failed = true;
                subscription_reconcile.push(
                    json!({ "subscription": staged.definition.name, "status": "failed", "error": error.message }),
                );
            }
        }
    }
    Ok(Json(
        json!({ "status": "activated", "range_map": plan.candidate_map, "merged_commit": staged.merged_commit, "subscription_frontiers": subscription_reconcile, "subscription_reconcile_failed": subscription_reconcile_failed }),
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
    if let Some(feed) =
        feed_when_applied(&state.control, &request.feed, state.control_plane.is_some()).await
    {
        state.demand.record_feed_move(
            feed.feed_id,
            copied.transferred_records,
            copied.transferred_bytes,
            copied.skipped_records,
        );
    }
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
    http: reqwest::Client,
}

impl AdminDrainDriver {
    pub fn new(
        endpoint: String,
        admin_key: String,
        timeout: Duration,
    ) -> Result<Self, reqwest::Error> {
        Ok(Self {
            endpoint: endpoint.trim_end_matches('/').to_owned(),
            admin_key,
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
            | crate::control::Command::RecoverSubscriptionProgressOwner { .. }) => {
                self.post("/v1/admin/subscriptions/move-progress", &command)
                    .await
            }
            command @ crate::control::Command::MovePipeComputeLease { .. } => {
                self.post(
                    "/v1/admin/commands",
                    &CommandBatchRequest {
                        request_id: None,
                        commands: vec![command],
                    },
                )
                .await
            }
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
            feed: request.feed.clone(),
            plan_id: plan.plan_id,
        }])
        .await
        .map_err(|error| ApiError::unavailable(error.to_string()))?;
    unfreeze_owner_move(&state, &source_endpoint, key, &plan_request).await?;
    unfreeze_owner_move(&state, &target_endpoint, key, &plan_request).await?;
    // An owner move verifies the staged replica rather than copying outside
    // replication, so the event records zero transfer bytes — the data
    // arrived through replicated_frames/wire_bytes already attributed.
    if let Some(feed) =
        feed_when_applied(&state.control, &request.feed, state.control_plane.is_some()).await
    {
        state.demand.record_feed_move(feed.feed_id, 0, 0, 0);
    }
    Ok(Json(
        json!({ "status": "activated", "assignment": plan.candidate_assignment,
        "committed_position": evidence.target_commit }),
    ))
}

/// Applies a planned Subscription progress placement step — owner handoff or
/// replica move — through the progress coordinator's evidence/CAS/re-stamp/
/// adoption flow. Storage drain routes these commands here: committing the
/// placement CAS alone would strand progress replicas at the previous
/// ownership epoch and wedge the Subscription.
async fn admin_move_subscription_progress(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(command): Json<crate::control::Command>,
) -> Result<Json<serde_json::Value>, ApiError> {
    authorize_admin(&state, &headers)?;
    enum ProgressMove {
        Handoff(StorageNodeId),
        Replica(StorageNodeId, StorageNodeId),
    }
    let (subscription_id, step) = match command {
        crate::control::Command::RecoverSubscriptionProgressOwner {
            subscription_id,
            new_owner,
            ..
        } => (subscription_id, ProgressMove::Handoff(new_owner)),
        crate::control::Command::MoveSubscriptionProgressReplica {
            subscription_id,
            replaced,
            replacement,
            ..
        } => (
            subscription_id,
            ProgressMove::Replica(replaced, replacement),
        ),
        _ => {
            return Err(ApiError::bad_request(
                "expected a Subscription progress move command",
            ))
        }
    };
    let assignment = state
        .control
        .active_subscription_progress_assignment_by_id(subscription_id)
        .await
        .ok_or_else(|| ApiError::bad_request("Subscription progress placement is unknown"))?;
    let (coordinator, _) = member_assignment_coordinator(&state, assignment)?;
    let outcome = match step {
        ProgressMove::Handoff(new_owner) => {
            coordinator
                .handoff_owner(state.progress_authority.as_ref(), &new_owner)
                .await
        }
        ProgressMove::Replica(replaced, replacement) => {
            coordinator
                .move_replica(state.progress_authority.as_ref(), &replaced, &replacement)
                .await
        }
    }
    .map_err(subscription_progress_api_error)?;
    Ok(Json(json!({
        "status": "applied",
        "assignment": outcome.assignment,
        "adopted": outcome.adopted,
    })))
}

#[derive(Deserialize)]
struct AdminSubscriptionProgressQuery {
    subscription: String,
}

/// Read-only Subscription progress placement view for operators: the current
/// assignment, committed frontier, member/lease summary, and per-replica
/// state. An unreachable or epoch-fenced replica reports its own outcome
/// instead of failing the whole read, so a wedged Subscription stays
/// diagnosable while it is still wedged.
async fn admin_subscription_progress(
    State(state): State<AppState>,
    headers: HeaderMap,
    axum::extract::Query(query): axum::extract::Query<AdminSubscriptionProgressQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    authorize_admin(&state, &headers)?;
    let (coordinator, assignment, definition) =
        member_coordinator(&state, &query.subscription).await?;
    let committed_mutation = coordinator.read_committed().await.ok().flatten();
    let committed = committed_mutation.as_ref().map(|mutation| {
        json!({
            "cursor": mutation.cursor,
            "positions": mutation.positions,
            "sequence": mutation.sequence,
            "request_id": mutation.request_id,
            "ownership_epoch": mutation.ownership_epoch,
            "tick": mutation.tick,
            "quarantined": mutation.quarantined,
        })
    });
    let member_state = coordinator.member_state().await.ok();
    // Lag is the operator's first question for shared progress: records
    // committed at each Active Range tail but still undelivered under the
    // frontier. The frontier position resolves through the same read path
    // member fetches use, so lag is evidence rather than a guess; a range
    // whose position no longer resolves or cannot be read is reported
    // rather than silently dropped.
    let lag = match (committed_mutation.as_ref(), definition.source.feed_id()) {
        (Some(mutation), Some(feed_id)) => {
            let frontier = mutation.positions.clone();
            let mut behind_total = 0_u64;
            let mut ranges = serde_json::Map::new();
            for range_assignment in state
                .control
                .active_range_assignments_for_feed(feed_id)
                .await
            {
                let position = frontier
                    .get(&range_assignment.range_id)
                    .cloned()
                    .unwrap_or_default();
                let read = fetch_range_page(
                    &state,
                    ReadRangePageRequest {
                        assignment: range_assignment.clone(),
                        after: None,
                        expected_commit: None,
                        after_cursor: if position.is_empty() {
                            None
                        } else {
                            Some(position)
                        },
                        // One leading frame is enough: resolved_after names
                        // the committed position the frontier Cursor resolves
                        // to, and committed is the tail.
                        tail_count: None,
                        single_range: true,
                        page_limit: Some(1),
                    },
                )
                .await;
                match read {
                    Ok(page) => {
                        let consumed = page.resolved_after.map_or(0, |position| position.value());
                        let behind = page.committed.value().saturating_sub(consumed);
                        behind_total = behind_total.saturating_add(behind);
                        ranges.insert(
                            range_assignment.range_id.to_string(),
                            json!({ "behind": behind }),
                        );
                    }
                    Err(error) => {
                        ranges.insert(
                            range_assignment.range_id.to_string(),
                            json!({ "behind": null, "error": error.message }),
                        );
                    }
                }
            }
            json!({ "records_behind": behind_total, "ranges": ranges })
        }
        (Some(_), None) => json!({ "records_behind": null,
            "detail": "Domain-source Subscriptions report per-Feed lag when member delivery lands" }),
        (None, _) => serde_json::Value::Null,
    };
    let replicas = coordinator
        .replica_states()
        .await
        .into_iter()
        .map(|(node, result)| match result {
            Ok(inspection) => json!({
                "node": node,
                "reachable": true,
                "committed_sequence": inspection
                    .committed
                    .as_ref()
                    .map(|mutation| mutation.sequence),
                "committed_epoch": inspection
                    .committed
                    .as_ref()
                    .map(|mutation| mutation.ownership_epoch),
                "prepared_pending": inspection.prepared.is_some(),
                "member_count": inspection.members.member_epochs.len(),
                "lease_count": inspection.members.leases.len(),
            }),
            Err(error) => json!({
                "node": node,
                "reachable": false,
                "error": error.to_string(),
            }),
        })
        .collect::<Vec<_>>();
    Ok(Json(json!({
        "subscription": query.subscription,
        "subscription_id": definition.subscription_id,
        "source": definition.source,
        "assignment": {
            "owner": assignment.owner,
            "replicas": assignment.replicas,
            "ownership_epoch": assignment.ownership_epoch,
        },
        "committed": committed,
        "lag": lag,
        "members": member_state.as_ref().map(|members| json!({
            "member_count": members.member_epochs.len(),
            "lease_count": members.leases.len(),
            "last_tick": members.last_tick,
        })),
        "replicas": replicas,
    })))
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
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    use_compression: bool,
    /// Effect-journal appends only: the frame joins the effect's pending
    /// tail (durable on the output quorum but invisible until the effect's
    /// terminal marker releases it). Public append paths refuse it.
    #[serde(default)]
    pending_effect: Option<crate::active_range::PendingEffectRef>,
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
    /// Machine-readable refusal code forwarded from the owner's ApiError so
    /// ingress Nodes can distinguish retryable classes (e.g. sequence healing).
    #[serde(default)]
    code: Option<String>,
    /// The Range the append landed on, so pending callers can target the
    /// release that makes it visible.
    #[serde(default)]
    range_id: Option<crate::active_range::RangeId>,
    /// The appended position inside that Range.
    #[serde(default)]
    position: Option<u64>,
    /// The effect holding the tail when the refusal is `PendingOccupied`.
    #[serde(default)]
    pending_effect: Option<crate::active_range::PendingEffect>,
}

/// `GET /v1/admin/demand/hot-keys`
///
/// Node-local view of Keys flagged for sustained dominant append demand
/// inside an Active Range. Per-Key ordering means a hot Key cannot be
/// parallelized; the flag drives isolating splits and tightened Writer
/// feedback rather than pretending the range can fan out. Detection is
/// ingress-local, so query the Node receiving the appends (or every Node in
/// a multi-ingress topology).
async fn admin_hot_keys(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    authorize_admin(&state, &headers)?;
    let maps = state.control.active_feed_range_maps().await;
    let mut evidence = state.demand.hot_keys();
    evidence.sort_by(|left, right| left.range_id.cmp(&right.range_id));
    let hot_keys: Vec<serde_json::Value> = evidence
        .iter()
        .map(|evidence| {
            let feed_bounds = maps.iter().find_map(|(feed, map)| {
                map.routes()
                    .iter()
                    .find(|route| route.range_id == evidence.range_id)
                    .map(|route| (feed.clone(), route.bounds.clone()))
            });
            hot_key_entry(*evidence, feed_bounds)
        })
        .collect();
    Ok(Json(json!({ "hot_keys": hot_keys })))
}

/// `GET /v1/admin/demand/pipes`
/// Per-Pipe drive attribution on this Node — cycle counts, folded input,
/// declared outputs, window-pane state estimate, and wall time — plus a
/// per-Domain rollup so operators can attribute Pipe CPU/backfill/state
/// cost to its logical owner. Scope is Node-local since process start:
/// a compute-lease move restarts attribution on the new owner rather
/// than pretending a portable counter.
async fn admin_pipe_demand(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    authorize_admin(&state, &headers)?;
    let pipes = state.control.active_pipes().await;
    let live: std::collections::BTreeSet<Uuid> = pipes.iter().map(|pipe| pipe.pipe_id).collect();
    state.demand.prune_pipe_drives(&live);
    let telemetry = state.demand.pipe_drive_telemetry();
    let mut domains: BTreeMap<String, serde_json::Value> = BTreeMap::new();
    let mut rows = Vec::with_capacity(pipes.len());
    for pipe in &pipes {
        let domain = state
            .control
            .active_domain_name(pipe.space_id)
            .await
            .unwrap_or_else(|| pipe.space_id.to_string());
        let operation = match &pipe.operation {
            crate::control::PipeOperation::Forward => "forward",
            crate::control::PipeOperation::Window { .. } => "window",
            crate::control::PipeOperation::Enrich { .. } => "enrich",
        };
        let telemetry = telemetry.get(&pipe.pipe_id);
        let backfilling = telemetry
            .is_some_and(|t| t.last_page_records >= t.last_page_limit && t.last_page_records > 0);
        let row = json!({
            "pipe": pipe.name,
            "domain": domain,
            "operation": operation,
            "stage": pipe.stage,
            "telemetry": telemetry,
            "backfilling": backfilling,
        });
        let rollup = domains.entry(domain).or_insert_with(|| {
            json!({
                "pipes": 0, "drives": 0, "applied": 0, "failed": 0,
                "records_fetched": 0, "fetch_bytes": 0, "outputs": 0,
                "state_mutations": 0, "emissions": 0, "records_refused": 0,
                "pane_rows": 0, "pane_bytes": 0, "drive_wall_ns_total": 0,
            })
        });
        rollup["pipes"] = json!(rollup["pipes"].as_u64().unwrap_or(0) + 1);
        if let Some(t) = telemetry {
            for (field, value) in [
                ("drives", t.drives),
                ("applied", t.applied),
                ("failed", t.failed),
                ("records_fetched", t.records_fetched),
                ("fetch_bytes", t.fetch_bytes),
                ("outputs", t.outputs),
                ("state_mutations", t.state_mutations),
                ("emissions", t.emissions),
                ("records_refused", t.records_refused),
                ("pane_rows", t.pane_rows_last),
                ("pane_bytes", t.pane_bytes_last),
                ("drive_wall_ns_total", t.drive_wall_ns_total),
            ] {
                rollup[field] = json!(rollup[field].as_u64().unwrap_or(0) + value);
            }
        }
        rows.push(row);
    }
    Ok(Json(json!({
        "node": state.storage_node_id,
        "scope": "node-local since process start; a compute-lease move restarts attribution on the new owner",
        "pipes": rows,
        "domains": domains,
    })))
}

/// Per-Subscription member-traffic attribution on this Node. Counters are
/// Node-local since process start: a progress-placement move serves members
/// on the new owner rather than pretending a portable counter.
async fn admin_subscription_demand(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    authorize_admin(&state, &headers)?;
    let subscriptions = state.control.active_subscriptions().await;
    let live: std::collections::BTreeSet<Uuid> = subscriptions
        .iter()
        .map(|subscription| subscription.subscription_id)
        .collect();
    state.demand.prune_subscription_traffic(&live);
    let telemetry = state.demand.subscription_traffic();
    let mut domains: BTreeMap<String, serde_json::Value> = BTreeMap::new();
    let mut rows = Vec::with_capacity(subscriptions.len());
    for subscription in &subscriptions {
        let domain = state
            .control
            .active_domain_name(subscription.space_id)
            .await
            .unwrap_or_else(|| subscription.space_id.to_string());
        let telemetry = telemetry.get(&subscription.subscription_id);
        let row = json!({
            "subscription": subscription.name,
            "domain": domain,
            "stage": subscription.stage,
            "telemetry": telemetry,
        });
        let rollup = domains.entry(domain).or_insert_with(|| {
            json!({
                "subscriptions": 0, "requests": 0, "joins": 0, "claims": 0,
                "renews": 0, "releases": 0, "transfers": 0, "fetches": 0,
                "fetch_records": 0, "acks": 0, "ack_ranges": 0,
                "quarantined_records": 0, "released_quarantined_records": 0,
                "seeks": 0,
            })
        });
        rollup["subscriptions"] = json!(rollup["subscriptions"].as_u64().unwrap_or(0) + 1);
        if let Some(t) = telemetry {
            for (field, value) in [
                ("requests", t.requests),
                ("joins", t.joins),
                ("claims", t.claims),
                ("renews", t.renews),
                ("releases", t.releases),
                ("transfers", t.transfers),
                ("fetches", t.fetches),
                ("fetch_records", t.fetch_records),
                ("acks", t.acks),
                ("ack_ranges", t.ack_ranges),
                ("quarantined_records", t.quarantined_records),
                (
                    "released_quarantined_records",
                    t.released_quarantined_records,
                ),
                ("seeks", t.seeks),
            ] {
                rollup[field] = json!(rollup[field].as_u64().unwrap_or(0) + value);
            }
        }
        rows.push(row);
    }
    Ok(Json(json!({
        "node": state.storage_node_id,
        "scope": "node-local since process start; a progress-placement move restarts attribution on the new owner",
        "subscriptions": rows,
        "domains": domains,
    })))
}

/// Per-Reader session attribution on this Node. Counters are Node-local
/// since process start: an open session can fetch through any ingress,
/// so each row reports demand where it was served rather than on a fixed
/// owning Node.
async fn admin_reader_demand(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    authorize_admin(&state, &headers)?;
    let readers = state.control.active_readers().await;
    let live: std::collections::BTreeSet<Uuid> =
        readers.iter().map(|reader| reader.reader_id).collect();
    state.demand.prune_reader_traffic(&live);
    let telemetry = state.demand.reader_traffic();
    let mut domains: BTreeMap<String, serde_json::Value> = BTreeMap::new();
    let mut rows = Vec::with_capacity(readers.len());
    for reader in &readers {
        let feed = state.control.active_feed_by_id(reader.feed_id).await;
        let (feed_name, domain) = match &feed {
            Some(feed) => (
                feed.name.clone(),
                state
                    .control
                    .active_domain_name(feed.space_id)
                    .await
                    .unwrap_or_else(|| feed.space_id.to_string()),
            ),
            None => (reader.feed_id.to_string(), String::new()),
        };
        let row = json!({
            "reader": reader.name,
            "domain": domain,
            "feed": feed_name,
            "status": reader.status,
            "telemetry": telemetry.get(&reader.reader_id),
        });
        let rollup = domains.entry(domain).or_insert_with(|| {
            json!({
                "readers": 0, "requests": 0, "opens": 0, "fetches": 0,
                "fetch_records": 0, "acks": 0, "closes": 0,
            })
        });
        rollup["readers"] = json!(rollup["readers"].as_u64().unwrap_or(0) + 1);
        if let Some(t) = telemetry.get(&reader.reader_id) {
            for (field, value) in [
                ("requests", t.requests),
                ("opens", t.opens),
                ("fetches", t.fetches),
                ("fetch_records", t.fetch_records),
                ("acks", t.acks),
                ("closes", t.closes),
            ] {
                rollup[field] = json!(rollup[field].as_u64().unwrap_or(0) + value);
            }
        }
        rows.push(row);
    }
    Ok(Json(json!({
        "node": state.storage_node_id,
        "scope": "node-local since process start; each row attributes demand where it was served",
        "readers": rows,
        "domains": domains,
    })))
}

/// Per-Index query attribution on this Node. Counters are Node-local
/// since process start and count on the replica that served the posting
/// scan — a peer-forwarded read lands here rather than on the ingress
/// Node the client happened to reach.
async fn admin_index_demand(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    authorize_admin(&state, &headers)?;
    let indexes = state.control.index_definitions().await;
    let live: std::collections::BTreeSet<Uuid> =
        indexes.iter().map(|index| index.index_id).collect();
    state.demand.prune_index_queries(&live);
    let telemetry = state.demand.index_query_telemetry();
    let mut domains: BTreeMap<String, serde_json::Value> = BTreeMap::new();
    let mut rows = Vec::with_capacity(indexes.len());
    for index in &indexes {
        let domain = state
            .control
            .active_domain_name(index.space_id)
            .await
            .unwrap_or_else(|| index.space_id.to_string());
        let stage = match &index.stage {
            crate::control::IndexBuildStage::Declared => "declared",
            crate::control::IndexBuildStage::Building { .. } => "building",
            crate::control::IndexBuildStage::Active { .. } => "active",
        };
        let telemetry = telemetry.get(&index.index_id);
        let row = json!({
            "index": index.name,
            "domain": domain,
            "stage": stage,
            "telemetry": telemetry,
        });
        let rollup = domains.entry(domain).or_insert_with(|| {
            json!({
                "indexes": 0, "queries": 0, "exact_queries": 0,
                "prefix_queries": 0, "range_queries": 0, "rows_returned": 0,
                "truncated_queries": 0, "served_behind": 0,
            })
        });
        rollup["indexes"] = json!(rollup["indexes"].as_u64().unwrap_or(0) + 1);
        if let Some(t) = telemetry {
            for (field, value) in [
                ("queries", t.queries),
                ("exact_queries", t.exact_queries),
                ("prefix_queries", t.prefix_queries),
                ("range_queries", t.range_queries),
                ("rows_returned", t.rows_returned),
                ("truncated_queries", t.truncated_queries),
                ("served_behind", t.served_behind),
            ] {
                rollup[field] = json!(rollup[field].as_u64().unwrap_or(0) + value);
            }
        }
        rows.push(row);
    }
    Ok(Json(json!({
        "node": state.storage_node_id,
        "scope": "node-local since process start; counts on the replica that served the posting scan",
        "indexes": rows,
        "domains": domains,
    })))
}

/// Per-Feed storage-traffic attribution on this Node. Append counters are
/// cumulative per-Range totals recorded on the storing replica and joined
/// to Feeds through the catalog; served-read counters count committed
/// pages this replica answered; replication counters count replica-append
/// frames/commits this replica accepted; movement counters record
/// completed ownership/follower moves with their transferred/skipped copy
/// evidence. All scopes are Node-local since process start — an ownership
/// move serves traffic on the new owner rather than pretending a portable
/// counter.
async fn admin_feed_demand(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    authorize_admin(&state, &headers)?;
    let appends = state.demand.range_pressure_totals();
    let reads = state.demand.range_read_totals();
    let replication = state.demand.range_replication_totals();
    let moves = state.demand.feed_move_totals();
    let assignments = state.control.active_feed_assignments().await;
    let live_ranges: std::collections::BTreeSet<RangeId> = assignments
        .iter()
        .map(|(_, assignment)| assignment.range_id)
        .collect();
    let live_feeds: std::collections::BTreeSet<uuid::Uuid> = assignments
        .iter()
        .map(|(_, assignment)| assignment.feed_id)
        .collect();
    state.demand.prune_feed_demand(&live_feeds, &live_ranges);
    let mut feeds: BTreeMap<Uuid, (String, serde_json::Value)> = BTreeMap::new();
    for (feed_name, assignment) in &assignments {
        let entry = feeds.entry(assignment.feed_id).or_insert_with(|| {
            (
                feed_name.clone(),
                json!({
                    "appended_records": 0, "appended_bytes": 0,
                    "read_pages": 0, "records_read": 0, "read_bytes": 0,
                    "replicated_frames": 0, "replicated_wire_bytes": 0,
                    "replicated_commits": 0, "moves": 0, "moved_records": 0,
                    "moved_bytes": 0, "moved_skipped_records": 0,
                }),
            )
        });
        if let Some(sample) = appends
            .iter()
            .find(|sample| sample.range_id == assignment.range_id)
        {
            entry.1["appended_records"] =
                json!(entry.1["appended_records"].as_u64().unwrap_or(0) + sample.records);
            entry.1["appended_bytes"] =
                json!(entry.1["appended_bytes"].as_u64().unwrap_or(0) + sample.bytes);
        }
        if let Some(totals) = reads.get(&assignment.range_id) {
            entry.1["read_pages"] =
                json!(entry.1["read_pages"].as_u64().unwrap_or(0) + totals.pages);
            entry.1["records_read"] =
                json!(entry.1["records_read"].as_u64().unwrap_or(0) + totals.records);
            entry.1["read_bytes"] =
                json!(entry.1["read_bytes"].as_u64().unwrap_or(0) + totals.bytes);
        }
        if let Some(totals) = replication.get(&assignment.range_id) {
            entry.1["replicated_frames"] =
                json!(entry.1["replicated_frames"].as_u64().unwrap_or(0) + totals.frames);
            entry.1["replicated_wire_bytes"] =
                json!(entry.1["replicated_wire_bytes"].as_u64().unwrap_or(0) + totals.wire_bytes);
            entry.1["replicated_commits"] =
                json!(entry.1["replicated_commits"].as_u64().unwrap_or(0) + totals.commits);
        }
    }
    for (feed_id, totals) in &moves {
        if let Some(entry) = feeds.get_mut(feed_id) {
            entry.1["moves"] = json!(entry.1["moves"].as_u64().unwrap_or(0) + totals.moves);
            entry.1["moved_records"] =
                json!(entry.1["moved_records"].as_u64().unwrap_or(0) + totals.records);
            entry.1["moved_bytes"] =
                json!(entry.1["moved_bytes"].as_u64().unwrap_or(0) + totals.bytes);
            entry.1["moved_skipped_records"] = json!(
                entry.1["moved_skipped_records"].as_u64().unwrap_or(0) + totals.skipped_records
            );
        }
    }
    let mut domains: BTreeMap<String, serde_json::Value> = BTreeMap::new();
    let mut rows = Vec::with_capacity(feeds.len());
    for (feed_id, (feed_name, counters)) in &feeds {
        let domain = state
            .control
            .active_feed_by_id(*feed_id)
            .await
            .map(|feed| feed.space_id)
            .unwrap_or(*feed_id);
        let domain = state
            .control
            .active_domain_name(domain)
            .await
            .unwrap_or_else(|| domain.to_string());
        rows.push(json!({
            "feed": feed_name,
            "domain": domain,
            "appended_records": counters["appended_records"],
            "appended_bytes": counters["appended_bytes"],
            "read_pages": counters["read_pages"],
            "records_read": counters["records_read"],
            "read_bytes": counters["read_bytes"],
            "replicated_frames": counters["replicated_frames"],
            "replicated_wire_bytes": counters["replicated_wire_bytes"],
            "replicated_commits": counters["replicated_commits"],
            "moves": counters["moves"],
            "moved_records": counters["moved_records"],
            "moved_bytes": counters["moved_bytes"],
            "moved_skipped_records": counters["moved_skipped_records"],
        }));
        let rollup = domains.entry(domain).or_insert_with(|| {
            json!({
                "feeds": 0, "appended_records": 0, "appended_bytes": 0,
                "read_pages": 0, "records_read": 0, "read_bytes": 0,
                "replicated_frames": 0,
                "replicated_wire_bytes": 0, "replicated_commits": 0,
                "moves": 0, "moved_records": 0, "moved_bytes": 0,
                "moved_skipped_records": 0,
            })
        });
        rollup["feeds"] = json!(rollup["feeds"].as_u64().unwrap_or(0) + 1);
        for field in [
            "appended_records",
            "appended_bytes",
            "read_pages",
            "records_read",
            "read_bytes",
            "replicated_frames",
            "replicated_wire_bytes",
            "replicated_commits",
            "moves",
            "moved_records",
            "moved_bytes",
            "moved_skipped_records",
        ] {
            rollup[field] =
                json!(rollup[field].as_u64().unwrap_or(0) + counters[field].as_u64().unwrap_or(0));
        }
    }
    Ok(Json(json!({
        "node": state.storage_node_id,
        "scope": "node-local since process start; counts on the storing/serving replica",
        "feeds": rows,
        "domains": domains,
    })))
}

fn hot_key_entry(
    evidence: HotKeyEvidence,
    feed_bounds: Option<(String, crate::active_range::KeyRange)>,
) -> serde_json::Value {
    let (feed, bounds) = feed_bounds
        .map(|(feed, bounds)| (Some(feed), Some(bounds)))
        .unwrap_or((None, None));
    let isolating_boundary = bounds
        .as_ref()
        .and_then(|bounds| isolation_split_boundary(bounds, &evidence));
    json!({
        "feed": feed,
        "range_id": evidence.range_id,
        "range_start": bounds.as_ref().map(|bounds| bounds.start),
        "range_end_exclusive": bounds.as_ref().and_then(|bounds| bounds.end_exclusive),
        "token": evidence.token,
        "share_bps": evidence.share_bps,
        "window_records": evidence.window_records,
        "window_bytes": evidence.window_bytes,
        "next_token": evidence.next_token,
        // Where the autoscaler would split to isolate this Key; null when
        // every observed append uses this one Key and nothing can be carved
        // away — a strictly ordered hot Key cannot be parallelized.
        "isolating_boundary": isolating_boundary,
    })
}

async fn writer_batch_append(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<WriterBatchAppendRequest>,
) -> Result<Json<WriterBatchAppendResponse>, ApiError> {
    authenticate_credential(&state, &headers)?;
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
    let hot_key = results.iter().any(|result| result.feedback.hot_key);
    Ok(Json(WriterBatchAppendResponse {
        results,
        feedback: writer_feedback(&state.demand, hot_key),
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

async fn reader_when_applied(
    control: &ControlController,
    name: &str,
    wait_for_replication: bool,
) -> Option<crate::control::ReaderDefinition> {
    let mut reader = control.active_reader_by_name(name).await;
    if reader.is_none() && wait_for_replication {
        for _ in 0..20 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            reader = control.active_reader_by_name(name).await;
            if reader.is_some() {
                break;
            }
        }
    }
    reader
}

async fn subscription_when_applied(
    control: &ControlController,
    name: &str,
    wait_for_replication: bool,
) -> Option<crate::control::SubscriptionDefinition> {
    let mut subscription = control.active_subscription_by_name(name).await;
    if subscription.is_none() && wait_for_replication {
        for _ in 0..20 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            subscription = control.active_subscription_by_name(name).await;
            if subscription.is_some() {
                break;
            }
        }
    }
    subscription
}

async fn writer_session_append(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<WriterSessionAppendRequest>,
) -> Result<Json<WriterAppendResponse>, ApiError> {
    authenticate_credential(&state, &headers)?;
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
    authorize_namespace(&state, &headers, &feed.name, PermissionAction::Write)?;
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
    let result = session_append_with_sequence(
        &state,
        &headers,
        &request,
        &feed.name,
        writer.writer_id,
        route.range_id,
        "writer-range",
    )
    .await;
    // A refused append leaves its catalog-allocated Writer sequence
    // unconsumed; the deduped allocation returns the same number on
    // retry, so a burned sequence wedges the Writer permanently. The
    // range reports its boundary in the refusal — re-land this record
    // exactly there (cursor/digest dedup still fences any ambiguous
    // commit, and an in-flight rival loses loudly rather than
    // reordering silently). Bounded hops: each refusal moves the
    // boundary forward only when a concurrent append already landed.
    let mut healed = result;
    let mut boundary = None;
    for _ in 0..4 {
        let next = match &healed {
            Err(error) => writer_sequence_boundary(error),
            Ok(_) => break,
        };
        let Some(next) = next else { break };
        boundary = Some(next);
        healed = client_append(
            State(state.clone()),
            headers.clone(),
            Json(ClientAppendRequest {
                request_id: request.request_id,
                feed: feed.name.clone(),
                writer_session_id: writer.writer_id,
                writer_epoch: request.session_epoch,
                sequence: next,
                event_time_ns: request.event_time_ns.clone(),
                key_base64: request.key_base64.clone(),
                payload_base64: request.payload_base64.clone(),
                metadata_base64: request.metadata_base64.clone(),
                use_compression: request.use_compression,
                pending_effect: None,
            }),
        )
        .await
        .map(|Json(response)| response);
    }
    if healed.is_ok() {
        if let Some(boundary) = boundary {
            // Rewind the allocator past the burned span so later
            // appends allocate at the stream tail instead of paying a
            // gap+heal round trip each. Best-effort: a failed repair
            // leaves the counter ahead — correct, only slower.
            let _ = execute_writer_command(
                &state,
                vec![crate::control::Command::RepairWriterRangeSequence {
                    writer: request.writer.clone(),
                    session_epoch: request.session_epoch,
                    range_id: route.range_id,
                    next_sequence: boundary.saturating_add(1),
                }],
                deterministic_uuid(request.request_id, "writer-range-repair"),
            )
            .await;
        }
    }
    healed.map(Json)
}

/// Allocate the Writer's next per-range sequence under `allocation_scope`
/// and append the record through the routed owner path.
#[allow(clippy::too_many_arguments)]
async fn session_append_with_sequence(
    state: &AppState,
    headers: &HeaderMap,
    request: &WriterSessionAppendRequest,
    feed_name: &str,
    writer_id: Uuid,
    range_id: crate::active_range::RangeId,
    allocation_scope: &str,
) -> Result<WriterAppendResponse, ApiError> {
    let commands = vec![crate::control::Command::AllocateWriterRangeSequence {
        writer: request.writer.clone(),
        session_epoch: request.session_epoch,
        range_id,
    }];
    let allocation_request_id = deterministic_uuid(
        request.request_id,
        &format!("{allocation_scope}:{range_id}"),
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
        State(state.clone()),
        headers.clone(),
        Json(ClientAppendRequest {
            request_id: request.request_id,
            feed: feed_name.to_owned(),
            writer_session_id: writer_id,
            writer_epoch: request.session_epoch,
            sequence,
            event_time_ns: request.event_time_ns.clone(),
            key_base64: request.key_base64.clone(),
            payload_base64: request.payload_base64.clone(),
            metadata_base64: request.metadata_base64.clone(),
            use_compression: request.use_compression,
            pending_effect: None,
        }),
    )
    .await
    .map(|Json(response)| response)
}

async fn client_append(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<ClientAppendRequest>,
) -> Result<Json<WriterAppendResponse>, ApiError> {
    authorize_namespace(&state, &headers, &request.feed, PermissionAction::Write)?;
    if request.pending_effect.is_some() {
        return Err(ApiError::bad_request(
            "pending_effect is reserved for effect-journal appends; public Writers commit on success",
        ));
    }
    // A freshly committed CREATE FEED applies on follower replicas a
    // few ms later; the bounded wait hides that read-after-write gap.
    let feed = feed_when_applied(&state.control, &request.feed, state.control_plane.is_some())
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
        return owner_append_local(state, request)
            .await
            .map(|outcome| outcome.response)
            .map_err(|error| error.api);
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
        code: match response.code.as_deref() {
            Some("writer.stale_sequence") => Some("writer.stale_sequence"),
            Some("writer.sequence_gap") => Some("writer.sequence_gap"),
            _ => None,
        },
    })
}

/// Forward a pending (effect-owned) append to the current Append Owner.
/// Unlike `route_append_to_owner` the caller needs the Range placement back
/// for the later release, and an occupied tail surfaces the occupant's
/// identity rather than a bare refusal.
async fn route_effect_append_to_owner(
    state: &AppState,
    feed_id: Uuid,
    request: ClientAppendRequest,
) -> Result<EffectAppendOutcome, ApiError> {
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
        return match owner_append_local(state, request).await {
            Ok(outcome) => Ok(EffectAppendOutcome::Appended {
                range_id: outcome.range_id,
                position: outcome.position,
            }),
            Err(error) => match error.occupant {
                Some(occupant) => Ok(EffectAppendOutcome::Occupied { occupant }),
                None => Err(error.api),
            },
        };
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
    if let Some(occupant) = response.pending_effect {
        return Ok(EffectAppendOutcome::Occupied { occupant });
    }
    match (response.result, response.range_id, response.position) {
        (Some(_), Some(range_id), Some(position)) => Ok(EffectAppendOutcome::Appended {
            range_id,
            position: crate::active_range::RangePosition::new(position),
        }),
        _ => Err(ApiError {
            status: if response.retryable {
                StatusCode::SERVICE_UNAVAILABLE
            } else {
                StatusCode::CONFLICT
            },
            message: response
                .error
                .unwrap_or_else(|| "Append Owner returned no result".to_owned()),
            code: None,
        }),
    }
}

/// Forward a pending-tail release to the Range's current Append Owner. The
/// owner broadcasts the deferred commit position - the marker's last frame -
/// to the replica quorum and commits locally once a follower persists the
/// same evidence.
async fn route_pending_release_to_owner(
    state: &AppState,
    target: &crate::effect::EffectReleaseTarget,
) -> Result<crate::active_range::PendingReleaseOutcome, ApiError> {
    let assignment = state
        .control
        .active_range_assignment_by_id(target.range_id)
        .await
        .filter(|assignment| assignment.feed_id == target.feed_id)
        .ok_or_else(|| ApiError::unavailable("Active Range assignment is unavailable"))?;
    let request = crate::active_range::PendingReleaseRequest {
        feed_id: target.feed_id,
        range_id: target.range_id,
        generation: assignment.generation,
        ownership_epoch: assignment.ownership_epoch,
        append_owner: assignment.owner.clone(),
        effect_id: target.effect_id,
        subscription_id: target.subscription_id,
    };
    let local = state.storage_node_id.as_ref().ok_or_else(|| {
        ApiError::unavailable("this Node is not configured for Active Range routing")
    })?;
    if &assignment.owner == local {
        return state
            .majority_append
            .as_ref()
            .ok_or_else(|| ApiError::unavailable("majority append coordinator is unavailable"))?
            .commit_pending_effect(request)
            .await
            .map_err(majority_api_error);
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
            "{}/internal/active-range/owner/release-pending",
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
    let response: OwnerReleaseResponse = response
        .json()
        .await
        .map_err(|error| ApiError::unavailable(error.to_string()))?;
    response.outcome.ok_or_else(|| ApiError {
        status: if response.retryable {
            StatusCode::SERVICE_UNAVAILABLE
        } else {
            StatusCode::CONFLICT
        },
        message: response
            .error
            .unwrap_or_else(|| "Append Owner returned no release outcome".to_owned()),
        code: None,
    })
}

/// Route one declared output's pending-tail discard to its Range's
/// current Append Owner. Range resolution happens per call so a replayed
/// abort tracks a Range that split or moved after the append.
async fn route_pending_discard_to_owner(
    state: &AppState,
    output: &crate::effect::EffectOutput,
    effect_id: uuid::Uuid,
    subscription_id: uuid::Uuid,
) -> Result<crate::active_range::PendingDiscardOutcome, ApiError> {
    let routing_key = decode_base64("key_base64", &output.key_base64)?;
    if routing_key.is_empty() {
        return Err(ApiError::bad_request(
            "key_base64 must contain a non-empty key",
        ));
    }
    let (_, assignment) = state
        .control
        .active_range_for_key(output.feed_id, &routing_key)
        .await
        .ok_or_else(|| ApiError::unavailable("Active Range route is unavailable"))?;
    let request = crate::active_range::PendingReleaseRequest {
        feed_id: output.feed_id,
        range_id: assignment.range_id,
        generation: assignment.generation,
        ownership_epoch: assignment.ownership_epoch,
        append_owner: assignment.owner.clone(),
        effect_id,
        subscription_id,
    };
    let local = state.storage_node_id.as_ref().ok_or_else(|| {
        ApiError::unavailable("this Node is not configured for Active Range routing")
    })?;
    if &assignment.owner == local {
        return state
            .majority_append
            .as_ref()
            .ok_or_else(|| ApiError::unavailable("majority append coordinator is unavailable"))?
            .discard_pending_effect(request)
            .await
            .map_err(majority_api_error);
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
            "{}/internal/active-range/owner/discard-pending",
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
    let response: OwnerDiscardResponse = response
        .json()
        .await
        .map_err(|error| ApiError::unavailable(error.to_string()))?;
    response.outcome.ok_or_else(|| ApiError {
        status: if response.retryable {
            StatusCode::SERVICE_UNAVAILABLE
        } else {
            StatusCode::CONFLICT
        },
        message: response
            .error
            .unwrap_or_else(|| "Append Owner returned no discard outcome".to_owned()),
        code: None,
    })
}

/// `POST /internal/active-range/owner/discard-pending` response body.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct OwnerDiscardResponse {
    #[serde(default)]
    outcome: Option<crate::active_range::PendingDiscardOutcome>,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    retryable: bool,
}

/// `POST /internal/active-range/owner/release-pending` response body.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct OwnerReleaseResponse {
    #[serde(default)]
    outcome: Option<crate::active_range::PendingReleaseOutcome>,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    retryable: bool,
}

/// `POST /internal/active-range/owner/release-pending`
///
/// Owner-side endpoint that commits one effect's pending tail on an output
/// Range. The apply path calls it after the effect's terminal journal
/// marker is durable; `NotPending` means the tail already resolved and
/// `Occupied` reports the effect holding it instead.
async fn owner_release_pending(
    State(state): State<AppState>,
    Json(request): Json<crate::active_range::PendingReleaseRequest>,
) -> Json<OwnerReleaseResponse> {
    Json(match state.majority_append.as_ref() {
        Some(coordinator) => match coordinator.commit_pending_effect(request).await {
            Ok(outcome) => OwnerReleaseResponse {
                outcome: Some(outcome),
                error: None,
                retryable: false,
            },
            Err(error) => OwnerReleaseResponse {
                outcome: None,
                error: Some(error.to_string()),
                retryable: error.retryable,
            },
        },
        None => OwnerReleaseResponse {
            outcome: None,
            error: Some("majority append coordinator is unavailable".to_owned()),
            retryable: true,
        },
    })
}

/// `POST /internal/active-range/owner/discard-pending`
///
/// Owner-side endpoint that discards one effect's pending tail on an
/// output Range after its `Aborted` marker is durable. `NotPending`
/// means the tail already resolved; `Occupied` reports the effect
/// holding it instead.
async fn owner_discard_pending(
    State(state): State<AppState>,
    Json(request): Json<crate::active_range::PendingReleaseRequest>,
) -> Json<OwnerDiscardResponse> {
    Json(match state.majority_append.as_ref() {
        Some(coordinator) => match coordinator.discard_pending_effect(request).await {
            Ok(outcome) => OwnerDiscardResponse {
                outcome: Some(outcome),
                error: None,
                retryable: false,
            },
            Err(error) => OwnerDiscardResponse {
                outcome: None,
                error: Some(error.to_string()),
                retryable: error.retryable,
            },
        },
        None => OwnerDiscardResponse {
            outcome: None,
            error: Some("majority append coordinator is unavailable".to_owned()),
            retryable: true,
        },
    })
}

async fn owner_append(
    State(state): State<AppState>,
    Json(request): Json<ClientAppendRequest>,
) -> Json<InternalOwnerAppendResponse> {
    Json(match owner_append_local(&state, request).await {
        Ok(outcome) => InternalOwnerAppendResponse {
            result: Some(outcome.response),
            error: None,
            retryable: false,
            code: None,
            range_id: Some(outcome.range_id),
            position: Some(outcome.position.value()),
            pending_effect: None,
        },
        Err(error) => {
            let occupant = error.occupant.clone();
            InternalOwnerAppendResponse {
                result: None,
                retryable: error.api.status == StatusCode::SERVICE_UNAVAILABLE,
                error: Some(error.api.message),
                code: error.api.code.map(str::to_owned),
                range_id: None,
                position: None,
                pending_effect: occupant,
            }
        }
    })
}

/// The owner-side outcome of an append: the public response plus the Range
/// placement pending callers need to target the release with.
struct OwnerAppendOutcome {
    response: WriterAppendResponse,
    range_id: crate::active_range::RangeId,
    position: crate::active_range::RangePosition,
}

/// An owner-side append refusal that can carry the effect holding the tail
/// when the cause is `PendingEffectOccupied`.
struct OwnerAppendError {
    api: ApiError,
    occupant: Option<crate::active_range::PendingEffect>,
}

impl From<ApiError> for OwnerAppendError {
    fn from(api: ApiError) -> Self {
        Self {
            api,
            occupant: None,
        }
    }
}

async fn owner_append_local(
    state: &AppState,
    request: ClientAppendRequest,
) -> Result<OwnerAppendOutcome, OwnerAppendError> {
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
        return Err(ApiError::bad_request("key_base64 must contain a non-empty key").into());
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
        ))
        .into());
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
    let frame = crate::codec::encode_record_with_compression(
        &StoredRecord {
            message_id,
            producer_id: request.writer_session_id,
            producer_sequence: request.sequence,
            event_time_ns,
            ingest_time_ns,
            key,
            payload,
            metadata,
        },
        request.use_compression,
    )
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
            pending_effect: request.pending_effect,
        })
        .await
        .map_err(|error| {
            let occupant = error.pending_effect.clone();
            // A refused append leaves its allocated Writer sequence
            // unconsumed; once later appends advance the stream, the
            // deduped allocation can never fill the hole. Tag it so the
            // session append path can re-allocate and heal the retry.
            let api = if error.code
                == crate::active_range::MajorityAppendErrorCode::LocalStorageFailure
                && error.message.contains("is stale; latest accepted sequence")
            {
                ApiError {
                    status: StatusCode::CONFLICT,
                    message: error.message,
                    code: Some("writer.stale_sequence"),
                }
            } else if error.code
                == crate::active_range::MajorityAppendErrorCode::LocalStorageFailure
                && error.message.contains("has a gap; expected")
            {
                ApiError {
                    status: StatusCode::CONFLICT,
                    message: error.message,
                    code: Some("writer.sequence_gap"),
                }
            } else {
                majority_api_error(error)
            };
            OwnerAppendError { api, occupant }
        })?;
    state
        .demand
        .record_range_append(assignment.range_id, &pressure_key, pressure_bytes);
    let hot_key = state.demand.is_hot_key(assignment.range_id, &pressure_key);
    Ok(OwnerAppendOutcome {
        response: WriterAppendResponse {
            message_id: result.message_id,
            cursor: result.cursor,
            deduplicated: result.deduplicated,
            durability: "majority_committed".to_owned(),
            feedback: writer_feedback(&state.demand, hot_key),
        },
        range_id: assignment.range_id,
        position: result.position,
    })
}

/// The sequence the range can accept next, parsed out of a Writer
/// sequence refusal: `has a gap; expected N` reports N directly while
/// `is stale; latest accepted sequence is N` bounds at N+1. `None` for
/// any other error — only these two carry a heal-able boundary.
fn writer_sequence_boundary(error: &ApiError) -> Option<u64> {
    if error.code == Some("writer.sequence_gap")
        || (error.code.is_none() && error.message.contains("has a gap; expected"))
    {
        return error.message.rsplit(' ').next()?.parse::<u64>().ok();
    }
    if error.code == Some("writer.stale_sequence")
        || (error.code.is_none() && error.message.contains("is stale; latest accepted sequence"))
    {
        return error
            .message
            .rsplit(' ')
            .next()?
            .parse::<u64>()
            .ok()?
            .checked_add(1);
    }
    None
}

/// Execute typed Control commands through the Control Plane when one is
/// configured, falling back to the in-process controller otherwise.
async fn execute_writer_command(
    state: &AppState,
    commands: Vec<crate::control::Command>,
    request_id: Uuid,
) -> Result<(), ApiError> {
    match &state.control_plane {
        Some(control_plane) => control_plane
            .execute_commands_with_request_id(commands, request_id)
            .await
            .map_err(|error| ApiError::unavailable(error.to_string()))?,
        None => {
            state
                .control
                .execute_commands_with_request_id(commands, request_id)
                .await?
        }
    };
    Ok(())
}

fn writer_feedback(demand: &DemandMetrics, hot_key: bool) -> WriterServerFeedback {
    let snapshot = demand.snapshot();
    let pressure = (snapshot.requests_in_flight as f64 / 100.0).clamp(0.0, 1.0);
    let mut feedback = WriterServerFeedback {
        recommended_batch_count: if pressure > 0.75 { 16 } else { 100 },
        recommended_batch_bytes: if pressure > 0.75 {
            256 * 1024
        } else {
            1024 * 1024
        },
        pressure,
        retry_after_ms: if pressure > 0.9 { 50 } else { 0 },
        max_frame_bytes: crate::codec::MAX_FRAME_BYTES,
        hot_key,
    };
    if hot_key {
        // A flagged hot Key cannot be parallelized without relaxing same-Key
        // ordering; ask the Writer to shed per-Key throughput instead.
        feedback.recommended_batch_count = feedback.recommended_batch_count.min(8);
        feedback.recommended_batch_bytes = feedback.recommended_batch_bytes.min(64 * 1024);
        feedback.retry_after_ms = feedback.retry_after_ms.max(25);
    }
    feedback
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

fn logical_frame_bytes(frame: &[u8]) -> Result<usize, ApiError> {
    crate::codec::logical_frame_size(frame)
        .map_err(|error| ApiError::unavailable(error.to_string()))
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
    for frame in &frames {
        if logical_frame_bytes(&frame.frame)? > MAX_LOGICAL_READ_BYTES {
            return Err(ApiError::unavailable(
                "record exceeds the bounded Feed read byte budget",
            ));
        }
    }
    state.demand.record_range_read(
        request.assignment.range_id,
        frames.len(),
        frames.iter().map(|frame| frame.frame.len() as u64).sum(),
    );
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
            bytes = bytes
                .saturating_add(logical_frame_bytes(&decoded)?.saturating_add(frame.cursor.len()));
            if logical_frame_bytes(&decoded)? > MAX_LOGICAL_READ_BYTES
                || bytes > MAX_LOGICAL_READ_BYTES
            {
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
            bytes = bytes.saturating_add(
                logical_frame_bytes(&frame.frame)?.saturating_add(frame.cursor.len()),
            );
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
            bytes = bytes.saturating_add(
                logical_frame_bytes(&frame.frame)?.saturating_add(frame.cursor.len()),
            );
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
                if logical_frame_bytes(&decoded)? > MAX_LOGICAL_READ_BYTES {
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
        let frame_bytes = logical_frame_bytes(&frame.frame)?.saturating_add(frame.cursor.len());
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
                bytes = bytes.saturating_add(
                    logical_frame_bytes(&decoded)?.saturating_add(frame.cursor.len()),
                );
                if logical_frame_bytes(&decoded)? > MAX_LOGICAL_READ_BYTES
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

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FeedSearchQuery {
    feed: String,
    /// Exact record Key bytes, base64.
    key_base64: Option<String>,
    /// Inclusive event-time bounds as signed epoch-nanosecond decimals,
    /// matching the client timestamp convention.
    from_event_time_ns: Option<String>,
    to_event_time_ns: Option<String>,
    /// Metadata entry filter: the key alone asserts presence, and pairing
    /// it with metadata_value_base64 requires an exact value match.
    metadata_key: Option<String>,
    metadata_value_base64: Option<String>,
    after: Option<String>,
    limit: Option<usize>,
}

/// Committed frames one search call may scan before handing back a
/// continuation Cursor. Keeps a miss deterministic-cost instead of an
/// unbounded history walk per request.
const FEED_SEARCH_SCAN_FRAMES: usize = MAX_LOGICAL_READ_FRAMES;

/// Bounded exact-Key search over the Feed's committed history. Each call
/// scans at most `FEED_SEARCH_SCAN_FRAMES` frames in merged order and
/// returns `next_after` — the last scanned frame's Cursor — when the budget
/// ran out before the history did, so callers continue deterministically
/// rather than the server holding an open scan.
async fn search_feed_records(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<FeedSearchQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    authorize_namespace(&state, &headers, &query.feed, PermissionAction::Read)?;
    let parse_bound = |field: &str, value: Option<&str>| -> Result<Option<i64>, ApiError> {
        value
            .map(|bound| {
                bound.parse::<i64>().map_err(|_| {
                    ApiError::bad_request(format!(
                        "{field} must be a signed epoch-nanosecond decimal"
                    ))
                })
            })
            .transpose()
    };
    let key = query
        .key_base64
        .as_deref()
        .map(|value| decode_base64("key_base64", value))
        .transpose()?;
    let from_event_time_ns =
        parse_bound("from_event_time_ns", query.from_event_time_ns.as_deref())?;
    let to_event_time_ns = parse_bound("to_event_time_ns", query.to_event_time_ns.as_deref())?;
    if query.metadata_key.as_deref().is_some_and(str::is_empty) {
        return Err(ApiError::bad_request("metadata_key must not be empty"));
    }
    let metadata_value = query
        .metadata_value_base64
        .as_deref()
        .map(|value| decode_base64("metadata_value_base64", value))
        .transpose()?;
    if metadata_value.is_some() && query.metadata_key.is_none() {
        return Err(ApiError::bad_request(
            "metadata_value_base64 requires metadata_key",
        ));
    }
    if key.is_none()
        && from_event_time_ns.is_none()
        && to_event_time_ns.is_none()
        && query.metadata_key.is_none()
    {
        return Err(ApiError::bad_request(
            "search requires key_base64, an event-time bound, or metadata_key",
        ));
    }
    if from_event_time_ns.is_some_and(|from| to_event_time_ns.is_some_and(|to| from > to)) {
        return Err(ApiError::bad_request(
            "from_event_time_ns must not exceed to_event_time_ns",
        ));
    }
    let matches = |record: &crate::domain::StoredRecord| -> bool {
        key.as_ref().is_none_or(|key| record.key == *key)
            && from_event_time_ns.is_none_or(|from| record.event_time_ns >= from)
            && to_event_time_ns.is_none_or(|to| record.event_time_ns <= to)
            && query.metadata_key.as_ref().is_none_or(|name| {
                record.metadata.get(name).is_some_and(|value| {
                    metadata_value
                        .as_ref()
                        .is_none_or(|expected| value == expected)
                })
            })
    };
    let limit = query.limit.unwrap_or(64).clamp(1, 256);
    let feed = feed_when_applied(&state.control, &query.feed, state.control_plane.is_some())
        .await
        .ok_or_else(|| ApiError::bad_request(format!("Feed does not exist: {}", query.feed)))?;
    let source = RecordSource::of(&feed);
    let mut records: Vec<RecordResponse> = Vec::new();
    let mut scanned = 0_usize;
    let mut exhausted = false;
    let mut next_after: Option<String> = None;
    let mut after = query.after.clone();
    const SCAN_PAGE: usize = 256;
    while records.len() < limit && !exhausted {
        let budget = FEED_SEARCH_SCAN_FRAMES.saturating_sub(scanned);
        if budget == 0 {
            break;
        }
        let page = read_paginated_feed(
            &state,
            feed.feed_id,
            &feed.name,
            after.as_deref(),
            budget.min(SCAN_PAGE),
        )
        .await?;
        let page_len = page.len();
        let mut consumed = 0_usize;
        for frame in page {
            consumed = consumed.saturating_add(1);
            scanned = scanned.saturating_add(1);
            let record = decode_record(&frame.frame)
                .map_err(|error| ApiError::unavailable(error.to_string()))?;
            let cursor = frame.cursor;
            next_after = Some(cursor.clone());
            if matches(&record) {
                records.push(record_response(
                    CursorRecord {
                        cursor: cursor.clone(),
                        record,
                    },
                    source.clone(),
                ));
            }
            // Stop at the limit so the continuation Cursor resumes exactly
            // where the scan halted; draining the rest of the page would
            // strand the remaining matches behind a fabricated exhausted
            // marker.
            if records.len() == limit {
                break;
            }
        }
        // A fully consumed short page proves history drained. An unconsumed
        // page tail is still searchable history even when the page was
        // short, so the continuation must remain available.
        if consumed == page_len && page_len < budget.min(SCAN_PAGE) {
            exhausted = true;
        }
        after = next_after.clone();
    }
    Ok(Json(json!({
        "records": records,
        "scanned": scanned,
        "exhausted": exhausted,
        "next_after": if exhausted { None } else { next_after },
    })))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FeedLookupQuery {
    feed: String,
    cursor: String,
}

/// Single-record investigation: resolve one opaque Cursor to the committed
/// record it names. The Cursor says which Range holds it only implicitly,
/// so each committed Range's owner-side index is probed in turn; a
/// rejection means the Cursor lives elsewhere while an owner failure
/// propagates rather than masquerading as a miss.
async fn lookup_feed_record(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<FeedLookupQuery>,
) -> Result<Json<RecordResponse>, ApiError> {
    authorize_namespace(&state, &headers, &query.feed, PermissionAction::Read)?;
    let feed = feed_when_applied(&state.control, &query.feed, state.control_plane.is_some())
        .await
        .ok_or_else(|| ApiError::bad_request(format!("Feed does not exist: {}", query.feed)))?;
    let assignments = read_placement(&state, feed.feed_id, &feed.name).await?;
    for assignment in &assignments {
        let probe = fetch_range_page(
            &state,
            ReadRangePageRequest {
                assignment: assignment.clone(),
                after: None,
                expected_commit: None,
                after_cursor: Some(query.cursor.clone()),
                tail_count: None,
                single_range: true,
                page_limit: Some(1),
            },
        )
        .await;
        let (committed, position) = match probe {
            Ok(page) => (Some(page.committed), page.resolved_after),
            Err(error) if error.status == StatusCode::BAD_REQUEST => continue,
            Err(error) => return Err(error),
        };
        let Some(position) = position else {
            continue;
        };
        // The cursor page resolves positions after the Cursor; read the
        // single frame at the resolved position itself.
        let page = fetch_range_page(
            &state,
            ReadRangePageRequest {
                assignment: assignment.clone(),
                after: Some(RangePosition::new(position.value().saturating_sub(1))),
                expected_commit: committed,
                after_cursor: None,
                tail_count: None,
                single_range: true,
                page_limit: Some(1),
            },
        )
        .await?;
        let Some(frame) = page.frames.into_iter().next() else {
            return Err(ApiError::unavailable(
                "resolved Cursor position no longer returns a committed frame",
            ));
        };
        if frame.cursor != query.cursor {
            return Err(ApiError::unavailable(
                "resolved Cursor position does not hold the requested frame",
            ));
        }
        let decoded = STANDARD
            .decode(frame.frame_base64.as_bytes())
            .map_err(|_| {
                ApiError::unavailable("current owner returned an invalid frame encoding")
            })?;
        let record =
            decode_record(&decoded).map_err(|error| ApiError::unavailable(error.to_string()))?;
        return Ok(Json(record_response(
            CursorRecord {
                cursor: frame.cursor,
                record,
            },
            RecordSource::of(&feed),
        )));
    }
    Err(ApiError::bad_request(
        "Cursor is unknown, uncommitted, or belongs to another Feed",
    ))
}

async fn read_feed_records(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<FeedReadQuery>,
) -> Result<Json<Vec<RecordResponse>>, ApiError> {
    authorize_namespace(&state, &headers, &query.feed, PermissionAction::Read)?;
    let feed = feed_when_applied(&state.control, &query.feed, state.control_plane.is_some())
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
                    record_response(
                        CursorRecord {
                            cursor: item.cursor,
                            record,
                        },
                        RecordSource::of(&feed),
                    )
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

/// Server-owned delivery source attached to every consumer record —
/// typed separately from writer-provided `metadata_base64` so
/// applications cannot spoof or overwrite it. `feed_name` is the last
/// dotted segment, `full_feed_name` the Domain-qualified name, and
/// `feed_id` the immutable identity (no range topology is exposed).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecordSource {
    pub feed_name: String,
    pub full_feed_name: String,
    pub feed_id: Uuid,
}

impl RecordSource {
    fn of(feed: &crate::control::FeedDefinition) -> Self {
        Self {
            feed_name: feed
                .name
                .rsplit('.')
                .next()
                .unwrap_or(feed.name.as_str())
                .to_owned(),
            full_feed_name: feed.name.clone(),
            feed_id: feed.feed_id,
        }
    }

    /// Fallback for deliveries whose Feed is not catalog-resolvable
    /// (prototype streams): the name carries through with a nil identity.
    fn named(name: &str) -> Self {
        Self {
            feed_name: name.rsplit('.').next().unwrap_or(name).to_owned(),
            full_feed_name: name.to_owned(),
            feed_id: Uuid::nil(),
        }
    }
}

async fn record_source_by_name(state: &AppState, name: &str) -> RecordSource {
    state
        .control
        .active_feed_by_name(name)
        .await
        .map(|feed| RecordSource::of(&feed))
        .unwrap_or_else(|| RecordSource::named(name))
}

async fn record_source_by_id(state: &AppState, feed_id: Uuid) -> RecordSource {
    state
        .control
        .feed_definition_by_id(feed_id)
        .await
        .map(|feed| RecordSource::of(&feed))
        .unwrap_or_else(|| RecordSource {
            feed_name: feed_id.simple().to_string(),
            full_feed_name: feed_id.simple().to_string(),
            feed_id,
        })
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
    pub source: RecordSource,
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
    let source = record_source_by_name(&state, &query.stream).await;
    Ok(Json(
        records
            .into_iter()
            .map(|record| record_response(record, source.clone()))
            .collect::<Vec<_>>(),
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

fn record_response(value: CursorRecord, source: RecordSource) -> RecordResponse {
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
        source,
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
    fn writer_feedback_tightens_guidance_for_flagged_hot_key() {
        let demand = DemandMetrics::default();
        let normal = writer_feedback(&demand, false);
        let hot = writer_feedback(&demand, true);
        assert!(!normal.hot_key);
        assert!(hot.hot_key);
        assert!(hot.retry_after_ms >= 25);
        assert!(hot.recommended_batch_count <= 8);
        assert!(hot.recommended_batch_bytes <= 64 * 1024);
    }

    fn delayed_head(position: u64, ingest_time_ns: i64) -> Option<((i64, Uuid), StoredRangeFrame)> {
        Some((
            (ingest_time_ns, Uuid::from_u128(u128::from(position) + 1)),
            StoredRangeFrame {
                position: RangePosition::new(position),
                identity: AppendIdentity {
                    writer_session_id: Uuid::from_u128(9),
                    writer_epoch: 1,
                    sequence: position,
                },
                cursor: format!("cursor-{position}"),
                frame: Vec::new(),
            },
        ))
    }

    #[test]
    fn delivery_delay_parks_too_fresh_heads_without_skipping_them() {
        let heads = vec![delayed_head(1, 1_000), delayed_head(2, 500)];
        // With no cutoff the globally earliest head wins, as before.
        assert_eq!(earliest_eligible_head(&heads, None), Some(1));
        // The range-0 head is still within the delay window: it stays
        // parked and cannot be overtaken by records behind it in its own
        // range, while range 1's eligible head still delivers.
        assert_eq!(earliest_eligible_head(&heads, Some(750)), Some(1));
        // Both parked -> no eligible head; the page ends with positions
        // unchanged rather than skipping ahead.
        assert_eq!(earliest_eligible_head(&heads, Some(250)), None);
        // A caught-up range (None head) never blocks an eligible head.
        let heads = vec![None, delayed_head(2, 500)];
        assert_eq!(earliest_eligible_head(&heads, Some(750)), Some(1));
        assert_eq!(earliest_eligible_head(&heads, Some(250)), None);
        // Exactly at the cutoff is eligible.
        let heads = vec![delayed_head(1, 750)];
        assert_eq!(earliest_eligible_head(&heads, Some(750)), Some(0));
    }

    #[test]
    fn writer_and_feed_compression_defaults_off_and_round_trips_opt_in() {
        let writer_body = json!({
            "request_id": Uuid::from_u128(1), "writer": "orders.writer", "session_epoch": 7,
            "key_base64": "aw==", "payload_base64": "dg==", "metadata_base64": {}
        });
        let mut writer: WriterSessionAppendRequest = serde_json::from_value(writer_body).unwrap();
        assert!(!writer.use_compression);
        assert!(serde_json::to_value(&writer)
            .unwrap()
            .get("use_compression")
            .is_none());
        writer.use_compression = true;
        let encoded = serde_json::to_value(&writer).unwrap();
        assert_eq!(encoded["use_compression"], true);
        assert!(
            serde_json::from_value::<WriterSessionAppendRequest>(encoded)
                .unwrap()
                .use_compression
        );
        let mut feed_body = json!({
            "request_id": Uuid::from_u128(1), "feed": "orders.events",
            "writer_session_id": Uuid::from_u128(2), "writer_epoch": 1, "sequence": 1,
            "key_base64": "aw==", "payload_base64": "dg==", "metadata_base64": {}
        });
        assert!(
            !serde_json::from_value::<ClientAppendRequest>(feed_body.clone())
                .unwrap()
                .use_compression
        );
        feed_body["use_compression"] = json!(true);
        let feed: ClientAppendRequest = serde_json::from_value(feed_body).unwrap();
        assert!(feed.use_compression);
        assert_eq!(serde_json::to_value(feed).unwrap()["use_compression"], true);
    }

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
        let response = record_response(
            CursorRecord {
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
            },
            RecordSource::named("orders.created"),
        );
        let json = serde_json::to_value(response).unwrap();
        assert!(json.get("metadata_base64").is_some());
        // Server-owned source ships beside writer metadata: last dotted
        // segment, full Domain-qualified name, immutable FeedId.
        assert_eq!(json["source"]["feed_name"], "created");
        assert_eq!(json["source"]["full_feed_name"], "orders.created");
        assert!(json["source"]["feed_id"].is_string());
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
            fetch_limit: None,
            fetch_retry_after_ms: None,
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
            frame: crate::codec::encode_record(&StoredRecord {
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
    async fn state_store_mutation_replicates_applies_and_dedupes_across_replicas() {
        let directory = TempDir::new().unwrap();
        let store: Arc<dyn LogStore> =
            Arc::new(FileLogStore::open(directory.path().join("data")).unwrap());
        let nodes: Vec<crate::active_range::StorageNodeId> =
            ["storage-1", "storage-2", "storage-3"]
                .into_iter()
                .map(|name| crate::active_range::StorageNodeId::try_new(name).unwrap())
                .collect();
        let control = Arc::new(
            ControlController::open_with_storage_nodes(
                directory.path().join("catalog.json"),
                store.clone(),
                nodes.clone(),
            )
            .unwrap(),
        );
        control.execute("CREATE SPACE accounts;").await.unwrap();
        control
            .execute_commands(vec![Command::DefineStateStore {
                name: "accounts.profiles".to_owned(),
                source: crate::control::StateStoreSourceRequest::Manual,
                named_keys: vec![],
            }])
            .await
            .unwrap();
        let definition = control
            .declared_state_store_by_name("accounts.profiles")
            .await
            .unwrap();
        let assignment = control
            .active_state_store_assignment_by_id(definition.store_id)
            .await
            .unwrap();
        assert_eq!(assignment.ownership_epoch, 1);

        let control_key = "this-is-a-long-control-plane-key".to_owned();
        let mut listeners = Vec::new();
        let mut endpoints = BTreeMap::new();
        for node in &nodes {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            endpoints.insert(
                node.clone(),
                format!("http://{}", listener.local_addr().unwrap()),
            );
            listeners.push(listener);
        }
        let mut journals = Vec::new();
        let mut indexes = Vec::new();
        for (index, node) in nodes.iter().enumerate() {
            let listener = listeners.remove(0);
            listener.set_nonblocking(true).unwrap();
            let journal = Arc::new(
                crate::state_store::FjallStateStoreMutationJournal::open(
                    directory.path().join(format!("journal-{index}")),
                )
                .unwrap(),
            );
            let index_store: Arc<dyn crate::index::IndexStore> = Arc::new(
                crate::index::FjallIndexStore::open(
                    directory.path().join(format!("index-{index}")),
                )
                .unwrap(),
            );
            let service = crate::state_store::StateStoreMutationService::new(
                node.clone(),
                control.clone(),
                journal.clone(),
                Arc::new(crate::state_store::IndexStoreStateApply::new(
                    index_store.clone(),
                )),
            );
            journals.push(journal);
            indexes.push(index_store);
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
                store: store.clone(),
                membership,
                demand: DemandMetrics::default(),
                autoscaler: Arc::new(Mutex::new(AutoscaleController::default())),
                reader_pacing: crate::reader::ReaderPacingTable::default(),
                control: control.clone(),
                control_plane: None,
                progress_authority: control.clone(),
                replica_append: None,
                majority_append: None,
                subscription_progress: None,
                effect_journal: None,
                state_store_journal: Some(Arc::new(service)),
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
            let listener = tokio::net::TcpListener::from_std(listener).unwrap();
            tokio::spawn(async move {
                axum::serve(listener, router(state)).await.unwrap();
            });
        }

        // Mutation on the first node replicates through the journal quorum.
        let client = reqwest::Client::new();
        let mutation_id = Uuid::new_v4();
        let mutate = |mutation_id: Uuid, key: &str| {
            let client = client.clone();
            let url = format!("{}/v1/state-stores/mutate", endpoints[&nodes[0]]);
            let key = key.to_owned();
            async move {
                let response = client
                    .post(url)
                    .bearer_auth("this-is-a-long-development-api-key")
                    .json(&serde_json::json!({
                        "store": "accounts.profiles",
                        "mutation_id": mutation_id,
                        "op": {
                            "kind": "put_row",
                            "key_base64": STANDARD.encode(key.as_bytes()),
                            "payload_base64": STANDARD.encode(b"{\"plan\": \"pro\"}"),
                        }
                    }))
                    .send()
                    .await
                    .unwrap();
                (response.status(), response.json::<Value>().await.unwrap())
            }
        };
        let (status, body) = mutate(mutation_id, "user-1").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["sequence"], 1);
        assert_eq!(body["deduplicated"], false);

        // Every replica journaled the entry and applied the row.
        for journal in &journals {
            let inspection = journal.inspect(definition.store_id).unwrap();
            let committed = inspection.committed.unwrap();
            assert_eq!(committed.sequence, 1);
            assert_eq!(committed.mutation_id, mutation_id);
        }
        for index in &indexes {
            let row = index
                .get(&crate::index::PrimaryRef {
                    feed_id: definition.store_id,
                    application_key: b"user-1".to_vec(),
                })
                .unwrap()
                .expect("replica applied the row");
            assert_eq!(row.payload, b"{\"plan\": \"pro\"}");
        }

        // A retry of the same mutation identity settles as deduplicated.
        let (status, body) = mutate(mutation_id, "user-1").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["sequence"], 1);
        assert_eq!(body["deduplicated"], true);

        // Internal journal routes require the shared internal credential.
        let refused = client
            .post(format!(
                "{}/internal/state-store-journal/inspect",
                endpoints[&nodes[0]]
            ))
            .json(&serde_json::json!({}))
            .send()
            .await
            .unwrap();
        assert_eq!(refused.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn feed_derived_state_store_replays_committed_history_and_resumes() {
        let directory = TempDir::new().unwrap();
        let store: Arc<dyn LogStore> =
            Arc::new(FileLogStore::open(directory.path().join("data")).unwrap());
        let nodes: Vec<crate::active_range::StorageNodeId> =
            ["storage-1", "storage-2", "storage-3"]
                .into_iter()
                .map(|name| crate::active_range::StorageNodeId::try_new(name).unwrap())
                .collect();
        let control = Arc::new(
            ControlController::open_with_storage_nodes(
                directory.path().join("catalog.json"),
                store.clone(),
                nodes.clone(),
            )
            .unwrap(),
        );
        control
            .execute("CREATE SPACE orders; CREATE FEED orders.events;")
            .await
            .unwrap();
        control
            .execute_commands(vec![Command::DefineStateStore {
                name: "orders.profiles".to_owned(),
                source: crate::control::StateStoreSourceRequest::Feed {
                    feed: "orders.events".to_owned(),
                },
                named_keys: vec![],
            }])
            .await
            .unwrap();
        let definition = control
            .declared_state_store_by_name("orders.profiles")
            .await
            .unwrap();
        let feed = control.active_feed_by_name("orders.events").await.unwrap();
        let range = control.active_range_assignment(feed.feed_id).await.unwrap();
        let descriptor = ActiveRangeDescriptor {
            feed_id: feed.feed_id,
            range_id: range.range_id,
            generation: range.generation,
            ownership_epoch: range.ownership_epoch,
        };
        // Commit-keyed history on every range replica so the owner serves it.
        for replica in range.replicas.iter() {
            let index = nodes.iter().position(|node| node == replica).unwrap();
            crate::active_range::store::seed_committed_history_shaped(
                &directory.path().join(format!("ranges-{index}")),
                &descriptor,
                4,
                &|sequence| format!("user-{sequence}").into_bytes(),
                &|sequence| sequence as i64,
                8,
            );
        }

        let control_key = "this-is-a-long-control-plane-key".to_owned();
        let mut listeners = Vec::new();
        let mut endpoints = BTreeMap::new();
        for node in &nodes {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            endpoints.insert(
                node.clone(),
                format!("http://{}", listener.local_addr().unwrap()),
            );
            listeners.push(listener);
        }
        let mut indexes = Vec::new();
        for (index, node) in nodes.iter().enumerate() {
            let listener = listeners.remove(0);
            listener.set_nonblocking(true).unwrap();
            let journal = Arc::new(
                crate::state_store::FjallStateStoreMutationJournal::open(
                    directory.path().join(format!("journal-{index}")),
                )
                .unwrap(),
            );
            let index_store: Arc<dyn crate::index::IndexStore> = Arc::new(
                crate::index::FjallIndexStore::open(
                    directory.path().join(format!("index-{index}")),
                )
                .unwrap(),
            );
            indexes.push(index_store.clone());
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
                store: store.clone(),
                membership,
                demand: DemandMetrics::default(),
                autoscaler: Arc::new(Mutex::new(AutoscaleController::default())),
                reader_pacing: crate::reader::ReaderPacingTable::default(),
                control: control.clone(),
                control_plane: None,
                progress_authority: control.clone(),
                replica_append: Some(Arc::new(ReplicaAppendService::new(
                    directory.path().join(format!("ranges-{index}")),
                    node.clone(),
                    control.clone(),
                ))),
                majority_append: None,
                subscription_progress: None,
                effect_journal: None,
                state_store_journal: Some(Arc::new(
                    crate::state_store::StateStoreMutationService::new(
                        node.clone(),
                        control.clone(),
                        journal,
                        Arc::new(crate::state_store::IndexStoreStateApply::new(index_store)),
                    ),
                )),
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
            let listener = tokio::net::TcpListener::from_std(listener).unwrap();
            tokio::spawn(async move {
                axum::serve(listener, router(state)).await.unwrap();
            });
        }

        let client = reqwest::Client::new();
        let sync = |path: &str, body: serde_json::Value| {
            let client = client.clone();
            let url = format!("{}{path}", endpoints[&nodes[0]]);
            async move {
                let response = client
                    .post(url)
                    .bearer_auth("this-is-a-long-development-api-key")
                    .json(&body)
                    .send()
                    .await
                    .unwrap();
                (response.status(), response.json::<Value>().await.unwrap())
            }
        };

        // One sync pass materializes every committed source record.
        let (status, body) = sync(
            "/v1/state-stores/sync",
            json!({ "store": "orders.profiles" }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["applied"], 4);
        assert_eq!(body["complete"], true);
        assert_eq!(body["frontier"], "cursor-4");

        // Every replica applied the materialized rows through the journal.
        for index in &indexes {
            let row = index
                .get(&crate::index::PrimaryRef {
                    feed_id: definition.store_id,
                    application_key: b"user-3".to_vec(),
                })
                .unwrap()
                .expect("replica applied the replayed row");
            assert_eq!(row.payload, vec![0xAB; 8]);
        }

        // Resuming at the frontier applies nothing until new commits arrive.
        let (status, body) = sync(
            "/v1/state-stores/sync",
            json!({ "store": "orders.profiles" }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["applied"], 0);
        assert_eq!(body["complete"], true);

        // Rebuild recommits retained history under a namespaced identity;
        // a retried rebuild with the same token dedupes.
        let token = Uuid::new_v4();
        let (status, body) = sync(
            "/v1/state-stores/rebuild",
            json!({ "store": "orders.profiles", "token": token }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["applied"], 4);
        assert_eq!(body["complete"], true);
        let (status, body) = sync(
            "/v1/state-stores/rebuild",
            json!({ "store": "orders.profiles", "token": token }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["applied"], 4);

        // Manual mutation is refused on a Feed-derived store.
        let (status, _body) = sync(
            "/v1/state-stores/mutate",
            json!({
                "store": "orders.profiles",
                "mutation_id": Uuid::new_v4(),
                "op": { "kind": "put_row", "key_base64": STANDARD.encode(b"k"),
                        "payload_base64": STANDARD.encode(b"v") },
            }),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        // Sync on a manual store is refused.
        control
            .execute_commands(vec![Command::DefineStateStore {
                name: "orders.manual".to_owned(),
                source: crate::control::StateStoreSourceRequest::Manual,
                named_keys: vec![],
            }])
            .await
            .unwrap();
        let (status, _body) =
            sync("/v1/state-stores/sync", json!({ "store": "orders.manual" })).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn index_rebuild_builds_shadow_and_activates_over_http() {
        let directory = TempDir::new().unwrap();
        let store: Arc<dyn LogStore> =
            Arc::new(FileLogStore::open(directory.path().join("data")).unwrap());
        let nodes: Vec<crate::active_range::StorageNodeId> =
            ["storage-1", "storage-2", "storage-3"]
                .into_iter()
                .map(|name| crate::active_range::StorageNodeId::try_new(name).unwrap())
                .collect();
        let control = Arc::new(
            ControlController::open_with_storage_nodes(
                directory.path().join("catalog.json"),
                store.clone(),
                nodes.clone(),
            )
            .unwrap(),
        );
        control
            .execute("CREATE SPACE orders; CREATE FEED orders.events;")
            .await
            .unwrap();
        control
            .execute_commands(vec![Command::DefineIndex {
                name: "orders.events.by.key".to_owned(),
                feed: "orders.events".to_owned(),
                fields: vec![crate::control::IndexFieldSpec::Key],
                unique: false,
                collation: crate::control::IndexCollation::Binary,
                consistency: crate::control::IndexConsistency::Eventual,
            }])
            .await
            .unwrap();
        let feed = control.active_feed_by_name("orders.events").await.unwrap();
        let range = control.active_range_assignment(feed.feed_id).await.unwrap();
        let descriptor = ActiveRangeDescriptor {
            feed_id: feed.feed_id,
            range_id: range.range_id,
            generation: range.generation,
            ownership_epoch: range.ownership_epoch,
        };
        for replica in range.replicas.iter() {
            let index = nodes.iter().position(|node| node == replica).unwrap();
            crate::active_range::store::seed_committed_history_shaped(
                &directory.path().join(format!("ranges-{index}")),
                &descriptor,
                4,
                &|sequence| format!("user-{sequence}").into_bytes(),
                &|sequence| sequence as i64,
                8,
            );
        }

        let control_key = "this-is-a-long-control-plane-key".to_owned();
        let mut listeners = Vec::new();
        let mut endpoints = BTreeMap::new();
        for node in &nodes {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            endpoints.insert(
                node.clone(),
                format!("http://{}", listener.local_addr().unwrap()),
            );
            listeners.push(listener);
        }
        let mut states = Vec::new();
        for (index, node) in nodes.iter().enumerate() {
            let listener = listeners.remove(0);
            listener.set_nonblocking(true).unwrap();
            let journal = Arc::new(
                crate::state_store::FjallStateStoreMutationJournal::open(
                    directory.path().join(format!("journal-{index}")),
                )
                .unwrap(),
            );
            let index_store: Arc<dyn crate::index::IndexStore> = Arc::new(
                crate::index::FjallIndexStore::open(
                    directory.path().join(format!("index-{index}")),
                )
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
                store: store.clone(),
                membership,
                demand: DemandMetrics::default(),
                autoscaler: Arc::new(Mutex::new(AutoscaleController::default())),
                reader_pacing: crate::reader::ReaderPacingTable::default(),
                control: control.clone(),
                control_plane: None,
                progress_authority: control.clone(),
                replica_append: Some(Arc::new(ReplicaAppendService::new(
                    directory.path().join(format!("ranges-{index}")),
                    node.clone(),
                    control.clone(),
                ))),
                majority_append: None,
                subscription_progress: None,
                effect_journal: None,
                state_store_journal: Some(Arc::new(
                    crate::state_store::StateStoreMutationService::new(
                        node.clone(),
                        control.clone(),
                        journal,
                        Arc::new(crate::state_store::IndexStoreStateApply::new(
                            index_store.clone(),
                        )),
                    ),
                )),
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
            states.push(state.clone());
            let listener = tokio::net::TcpListener::from_std(listener).unwrap();
            tokio::spawn(async move {
                axum::serve(listener, router(state)).await.unwrap();
            });
        }

        let client = reqwest::Client::new();
        // The public WCL statement moves the Index into Building over HTTP.
        let response = client
            .post(format!("{}/v1/control/execute", endpoints[&nodes[0]]))
            .bearer_auth("this-is-a-long-development-api-key")
            .json(&serde_json::json!({
                "script": "REBUILD INDEX orders.events.by.key;",
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let definition = control
            .declared_index_by_name("orders.events.by.key")
            .await
            .unwrap();
        assert!(matches!(
            definition.stage,
            crate::control::IndexBuildStage::Building { generation: 1 }
        ));

        // One bounded drive pass on each Node materializes the local shadow
        // and the first completer atomically activates generation 1.
        for state in &states {
            let outcomes = drive_local_index_builds(state, 64).await;
            assert!(
                outcomes.iter().any(|outcome| matches!(
                    outcome,
                    IndexBuildDriveOutcome::Activated { .. }
                        | IndexBuildDriveOutcome::Superseded { .. }
                        | IndexBuildDriveOutcome::Building { .. }
                )),
                "{outcomes:?}"
            );
        }
        let updated = control
            .declared_index_by_name("orders.events.by.key")
            .await
            .unwrap();
        assert!(matches!(
            updated.stage,
            crate::control::IndexBuildStage::Active { generation: 1 }
        ));
        assert!(updated.applied_cursor.is_some());

        // Every Node's local shadow holds a posting per committed key.
        let shadow = crate::index::shadow_index_id(definition.index_id, 1);
        for state in &states {
            let index = state
                .state_store_journal
                .as_ref()
                .unwrap()
                .index_store()
                .unwrap();
            let rows = index
                .lookup_exact(
                    shadow,
                    &[crate::index::IndexValue::Bytes(b"user-2".to_vec())],
                    8,
                )
                .unwrap();
            assert_eq!(rows.len(), 1, "shadow resolves user-2 on every Node");
            assert_eq!(
                index.last_applied(shadow).unwrap(),
                updated.applied_cursor,
                "local shadow frontier matches the activated cursor"
            );
        }
    }

    #[tokio::test]
    async fn index_query_routes_logically_over_exact_prefix_range_and_wcl() {
        let directory = TempDir::new().unwrap();
        let store: Arc<dyn LogStore> =
            Arc::new(FileLogStore::open(directory.path().join("data")).unwrap());
        let nodes: Vec<crate::active_range::StorageNodeId> =
            ["storage-1", "storage-2", "storage-3"]
                .into_iter()
                .map(|name| crate::active_range::StorageNodeId::try_new(name).unwrap())
                .collect();
        let control = Arc::new(
            ControlController::open_with_storage_nodes(
                directory.path().join("catalog.json"),
                store.clone(),
                nodes.clone(),
            )
            .unwrap(),
        );
        control
            .execute("CREATE SPACE orders; CREATE FEED orders.events;")
            .await
            .unwrap();
        control
            .execute_commands(vec![Command::DefineIndex {
                name: "orders.events.by.key".to_owned(),
                feed: "orders.events".to_owned(),
                fields: vec![crate::control::IndexFieldSpec::Key],
                unique: false,
                collation: crate::control::IndexCollation::Binary,
                consistency: crate::control::IndexConsistency::Eventual,
            }])
            .await
            .unwrap();
        let feed = control.active_feed_by_name("orders.events").await.unwrap();
        let range = control.active_range_assignment(feed.feed_id).await.unwrap();
        let descriptor = ActiveRangeDescriptor {
            feed_id: feed.feed_id,
            range_id: range.range_id,
            generation: range.generation,
            ownership_epoch: range.ownership_epoch,
        };
        for replica in range.replicas.iter() {
            let index = nodes.iter().position(|node| node == replica).unwrap();
            crate::active_range::store::seed_committed_history_shaped(
                &directory.path().join(format!("ranges-{index}")),
                &descriptor,
                4,
                &|sequence| format!("user-{sequence}").into_bytes(),
                &|sequence| sequence as i64,
                8,
            );
        }

        let control_key = "this-is-a-long-control-plane-key".to_owned();
        let mut listeners = Vec::new();
        let mut endpoints = BTreeMap::new();
        for node in &nodes {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            endpoints.insert(
                node.clone(),
                format!("http://{}", listener.local_addr().unwrap()),
            );
            listeners.push(listener);
        }
        let mut states = Vec::new();
        for (index, node) in nodes.iter().enumerate() {
            let listener = listeners.remove(0);
            listener.set_nonblocking(true).unwrap();
            let journal = Arc::new(
                crate::state_store::FjallStateStoreMutationJournal::open(
                    directory.path().join(format!("journal-{index}")),
                )
                .unwrap(),
            );
            let index_store: Arc<dyn crate::index::IndexStore> = Arc::new(
                crate::index::FjallIndexStore::open(
                    directory.path().join(format!("index-{index}")),
                )
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
                store: store.clone(),
                membership,
                demand: DemandMetrics::default(),
                autoscaler: Arc::new(Mutex::new(AutoscaleController::default())),
                reader_pacing: crate::reader::ReaderPacingTable::default(),
                control: control.clone(),
                control_plane: None,
                progress_authority: control.clone(),
                replica_append: Some(Arc::new(ReplicaAppendService::new(
                    directory.path().join(format!("ranges-{index}")),
                    node.clone(),
                    control.clone(),
                ))),
                majority_append: None,
                subscription_progress: None,
                effect_journal: None,
                state_store_journal: Some(Arc::new(
                    crate::state_store::StateStoreMutationService::new(
                        node.clone(),
                        control.clone(),
                        journal,
                        Arc::new(crate::state_store::IndexStoreStateApply::new(
                            index_store.clone(),
                        )),
                    ),
                )),
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
            states.push(state.clone());
            let listener = tokio::net::TcpListener::from_std(listener).unwrap();
            tokio::spawn(async move {
                axum::serve(listener, router(state)).await.unwrap();
            });
        }

        let client = reqwest::Client::new();
        let admin = "this-is-a-long-development-api-key";
        let user1 = STANDARD.encode(b"user-1");

        // A declared-but-unbuilt Index refuses honestly rather than serving
        // an empty result set.
        let response = client
            .post(format!("{}/v1/indexes/query", endpoints[&nodes[0]]))
            .bearer_auth(admin)
            .json(&serde_json::json!({
                "index": "orders.events.by.key",
                "mode": "exact",
                "values": [{"kind": "bytes", "value_base64": user1}],
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
        assert_eq!(
            response.json::<serde_json::Value>().await.unwrap()["code"],
            "index_not_ready"
        );

        // An unknown Index is a 404 with an actionable code.
        let response = client
            .post(format!("{}/v1/indexes/query", endpoints[&nodes[0]]))
            .bearer_auth(admin)
            .json(&serde_json::json!({
                "index": "orders.events.missing",
                "mode": "exact",
                "values": [{"kind": "bytes", "value_base64": user1}],
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        // Mixing a data-plane read with a mutation is refused outright.
        let response = client
            .post(format!("{}/v1/control/execute", endpoints[&nodes[0]]))
            .bearer_auth(admin)
            .json(&serde_json::json!({
                "script": "GET INDEX orders.events.by.key EQ B64 'dXNlci0x'; CREATE FEED orders.other;",
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        // Build + activate generation 1: only the first Node drives, so the
        // other two legitimately lag the activation frontier.
        let response = client
            .post(format!("{}/v1/control/execute", endpoints[&nodes[0]]))
            .bearer_auth(admin)
            .json(&serde_json::json!({
                "script": "REBUILD INDEX orders.events.by.key;",
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let outcomes = drive_local_index_builds(&states[0], 64).await;
        assert!(outcomes.iter().any(|outcome| matches!(
            outcome,
            IndexBuildDriveOutcome::Activated { .. } | IndexBuildDriveOutcome::Building { .. }
        )));
        let definition = control
            .declared_index_by_name("orders.events.by.key")
            .await
            .unwrap();
        assert!(matches!(
            definition.stage,
            crate::control::IndexBuildStage::Active { generation: 1 }
        ));

        // Exact lookup on the builder Node.
        let response = client
            .post(format!("{}/v1/indexes/query", endpoints[&nodes[0]]))
            .bearer_auth(admin)
            .json(&serde_json::json!({
                "index": "orders.events.by.key",
                "mode": "exact",
                "values": [{"kind": "bytes", "value_base64": STANDARD.encode(b"user-2")}],
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value = response.json().await.unwrap();
        assert_eq!(body["rows"].as_array().unwrap().len(), 1);
        assert_eq!(body["behind"], false);
        assert_eq!(body["served_by"], "storage-1");
        assert_eq!(
            body["rows"][0]["key_base64"].as_str().unwrap(),
            STANDARD.encode(b"user-2")
        );

        // Prefix with no leading values pages the whole generation.
        let response = client
            .post(format!("{}/v1/indexes/query", endpoints[&nodes[0]]))
            .bearer_auth(admin)
            .json(&serde_json::json!({
                "index": "orders.events.by.key",
                "mode": "prefix",
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.json::<serde_json::Value>().await.unwrap()["rows"]
                .as_array()
                .unwrap()
                .len(),
            4
        );

        // Range over the single Bytes field respects byte ordering.
        let response = client
            .post(format!("{}/v1/indexes/query", endpoints[&nodes[0]]))
            .bearer_auth(admin)
            .json(&serde_json::json!({
                "index": "orders.events.by.key",
                "mode": "range",
                "lower": {"value": {"kind": "bytes", "value_base64": STANDARD.encode(b"user-2")}, "inclusive": true},
                "upper": {"value": {"kind": "bytes", "value_base64": STANDARD.encode(b"user-3")}, "inclusive": true},
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value = response.json().await.unwrap();
        assert_eq!(body["rows"].as_array().unwrap().len(), 2);

        // Non-strict ingress on a lagging Node answers honestly: behind=true.
        let response = client
            .post(format!("{}/v1/indexes/query", endpoints[&nodes[2]]))
            .bearer_auth(admin)
            .json(&serde_json::json!({
                "index": "orders.events.by.key",
                "mode": "exact",
                "values": [{"kind": "bytes", "value_base64": user1}],
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value = response.json().await.unwrap();
        assert_eq!(body["behind"], true);
        assert_eq!(body["served_by"], "storage-3");

        // Strict on the same lagging Node waits its bound, then forwards to
        // the builder peer which answers at the activation frontier.
        let response = client
            .post(format!("{}/v1/indexes/query", endpoints[&nodes[2]]))
            .bearer_auth(admin)
            .json(&serde_json::json!({
                "index": "orders.events.by.key",
                "mode": "exact",
                "values": [{"kind": "bytes", "value_base64": user1}],
                "strict": true,
                "wait_ms": 50,
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value = response.json().await.unwrap();
        assert_eq!(body["behind"], false);
        assert_eq!(body["served_by"], "storage-1");
        assert_eq!(body["rows"].as_array().unwrap().len(), 1);

        // WCL GET INDEX runs the same data-plane path through the control
        // endpoint and reports rows in the statement result.
        let response = client
            .post(format!("{}/v1/control/execute", endpoints[&nodes[0]]))
            .bearer_auth(admin)
            .json(&serde_json::json!({
                "script": "GET INDEX orders.events.by.key EQ B64 'dXNlci0x' LIMIT 5;",
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value = response.json().await.unwrap();
        assert_eq!(body["results"].as_array().unwrap().len(), 1);
        assert_eq!(body["results"][0]["statement"], "GET INDEX");
        assert_eq!(
            body["results"][0]["data"]["rows"].as_array().unwrap().len(),
            1
        );
    }

    #[tokio::test]
    async fn state_store_lookup_routes_from_any_ingress_node_and_reports_freshness() {
        let directory = TempDir::new().unwrap();
        let store: Arc<dyn LogStore> =
            Arc::new(FileLogStore::open(directory.path().join("data")).unwrap());
        // Four storage Nodes — the rendezvous placement picks three, so the
        // remaining Node exercises the ingress-forwarding path.
        let nodes: Vec<crate::active_range::StorageNodeId> =
            ["storage-1", "storage-2", "storage-3", "storage-4"]
                .into_iter()
                .map(|name| crate::active_range::StorageNodeId::try_new(name).unwrap())
                .collect();
        let control = Arc::new(
            ControlController::open_with_storage_nodes(
                directory.path().join("catalog.json"),
                store.clone(),
                nodes.clone(),
            )
            .unwrap(),
        );
        control.execute("CREATE SPACE accounts;").await.unwrap();
        control
            .execute_commands(vec![Command::DefineStateStore {
                name: "accounts.profiles".to_owned(),
                source: crate::control::StateStoreSourceRequest::Manual,
                named_keys: vec![],
            }])
            .await
            .unwrap();
        let definition = control
            .declared_state_store_by_name("accounts.profiles")
            .await
            .unwrap();
        let assignment = control
            .active_state_store_assignment_by_id(definition.store_id)
            .await
            .unwrap();
        assert_eq!(assignment.replicas.iter().count(), 3);
        let non_replica = nodes
            .iter()
            .find(|node| !assignment.replicas.contains(node))
            .expect("four-node pool leaves one non-replica")
            .clone();

        let control_key = "this-is-a-long-control-plane-key".to_owned();
        let mut listeners = Vec::new();
        let mut endpoints = BTreeMap::new();
        for node in &nodes {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            endpoints.insert(
                node.clone(),
                format!("http://{}", listener.local_addr().unwrap()),
            );
            listeners.push(listener);
        }
        let mut journals = Vec::new();
        for (index, node) in nodes.iter().enumerate() {
            let listener = listeners.remove(0);
            listener.set_nonblocking(true).unwrap();
            let journal = Arc::new(
                crate::state_store::FjallStateStoreMutationJournal::open(
                    directory.path().join(format!("journal-{index}")),
                )
                .unwrap(),
            );
            journals.push(journal.clone());
            let index_store: Arc<dyn crate::index::IndexStore> = Arc::new(
                crate::index::FjallIndexStore::open(
                    directory.path().join(format!("index-{index}")),
                )
                .unwrap(),
            );
            let service = crate::state_store::StateStoreMutationService::new(
                node.clone(),
                control.clone(),
                journal,
                Arc::new(crate::state_store::IndexStoreStateApply::new(index_store)),
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
                store: store.clone(),
                membership,
                demand: DemandMetrics::default(),
                autoscaler: Arc::new(Mutex::new(AutoscaleController::default())),
                reader_pacing: crate::reader::ReaderPacingTable::default(),
                control: control.clone(),
                control_plane: None,
                progress_authority: control.clone(),
                replica_append: None,
                majority_append: None,
                subscription_progress: None,
                effect_journal: None,
                state_store_journal: Some(Arc::new(service)),
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
            let listener = tokio::net::TcpListener::from_std(listener).unwrap();
            tokio::spawn(async move {
                axum::serve(listener, router(state)).await.unwrap();
            });
        }

        // One mutation through the quorum on a replica node.
        let client = reqwest::Client::new();
        let replica_node = assignment.replicas.iter().next().unwrap().clone();
        let response = client
            .post(format!(
                "{}/v1/state-stores/mutate",
                endpoints[&replica_node]
            ))
            .bearer_auth("this-is-a-long-development-api-key")
            .json(&serde_json::json!({
                "store": "accounts.profiles",
                "mutation_id": Uuid::new_v4(),
                "op": {
                    "kind": "put_row",
                    "key_base64": STANDARD.encode(b"user-9"),
                    "payload_base64": STANDARD.encode(b"plan-ent"),
                }
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let lookup = |node: &crate::active_range::StorageNodeId| {
            let client = client.clone();
            let url = format!("{}/v1/state-stores/lookup", endpoints[node]);
            async move {
                let response = client
                    .post(url)
                    .bearer_auth("this-is-a-long-development-api-key")
                    .json(&serde_json::json!({
                        "store": "accounts.profiles",
                        "key_base64": STANDARD.encode(b"user-9"),
                    }))
                    .send()
                    .await
                    .unwrap();
                (response.status(), response.json::<Value>().await.unwrap())
            }
        };

        // A non-replica ingress forwards to a journal replica and returns
        // the applied row with freshness evidence.
        let (status, body) = lookup(&non_replica).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["found"], true);
        assert_eq!(body["applied_seq"], 1);
        assert_eq!(body["committed_seq"], 1);
        assert_eq!(body["behind"], false);
        assert_eq!(
            STANDARD
                .decode(body["payload_base64"].as_str().unwrap())
                .unwrap(),
            b"plan-ent"
        );
        assert_ne!(body["served_by"].as_str().unwrap(), non_replica.as_str());

        // A replica answers the same lookup locally.
        let (status, body) = lookup(&replica_node).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["found"], true);

        // Unknown keys and unknown stores answer cleanly.
        let response = client
            .post(format!(
                "{}/v1/state-stores/lookup",
                endpoints[&replica_node]
            ))
            .bearer_auth("this-is-a-long-development-api-key")
            .json(&serde_json::json!({
                "store": "accounts.profiles",
                "key_base64": STANDARD.encode(b"absent"),
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = response.json().await.unwrap();
        assert_eq!(body["found"], false);
        let (status, _body) = {
            let response = client
                .post(format!(
                    "{}/v1/state-stores/lookup",
                    endpoints[&replica_node]
                ))
                .bearer_auth("this-is-a-long-development-api-key")
                .json(&serde_json::json!({
                    "store": "accounts.missing",
                    "key_base64": STANDARD.encode(b"k"),
                }))
                .send()
                .await
                .unwrap();
            (response.status(), ())
        };
        assert_eq!(status, StatusCode::BAD_REQUEST);

        // A strict lookup serves normally once replicas are caught up.
        let strict_lookup = |node: &crate::active_range::StorageNodeId| {
            let client = client.clone();
            let url = format!("{}/v1/state-stores/lookup", endpoints[node]);
            async move {
                let response = client
                    .post(url)
                    .bearer_auth("this-is-a-long-development-api-key")
                    .json(&serde_json::json!({
                        "store": "accounts.profiles",
                        "key_base64": STANDARD.encode(b"user-9"),
                        "strict": true,
                        "wait_ms": 200,
                    }))
                    .send()
                    .await
                    .unwrap();
                (response.status(), response.json::<Value>().await.unwrap())
            }
        };
        let (status, body) = strict_lookup(&non_replica).await;
        assert_eq!(status, StatusCode::OK, "{body}");

        // Commit a mutation directly on every replica journal without
        // applying it — all replicas now lag their committed frontier, and
        // the strict lookup must surface index_behind rather than serve
        // stale state.
        let unapplied = crate::state_store::StateStoreMutation {
            store_id: definition.store_id,
            mutation_id: Uuid::new_v4(),
            request_id: Uuid::new_v4(),
            ownership_epoch: assignment.ownership_epoch,
            sequence: 2,
            op: crate::state_store::StateStoreMutationOp::PutRow {
                key: b"user-9".to_vec(),
                payload: b"v2".to_vec(),
                named: Vec::new(),
            },
            issued_at_ns: 1,
            source: None,
        };
        for (index, node) in nodes.iter().enumerate() {
            if !assignment.replicas.contains(node) {
                continue;
            }
            let journal = &journals[index];
            let digest = journal.prepare(unapplied.clone()).unwrap().digest;
            journal
                .commit_with_quorum(
                    definition.store_id,
                    assignment.ownership_epoch,
                    &crate::state_store::StateStoreCommitEvidence {
                        store_id: definition.store_id,
                        request_id: unapplied.request_id,
                        mutation_id: unapplied.mutation_id,
                        sequence: 2,
                        votes: [
                            (assignment.owner.clone(), digest),
                            (
                                assignment
                                    .replicas
                                    .iter()
                                    .find(|n| *n != &assignment.owner)
                                    .unwrap()
                                    .clone(),
                                digest,
                            ),
                        ],
                    },
                )
                .unwrap();
        }
        let (status, body) = strict_lookup(&non_replica).await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        assert_eq!(body["code"], "index_behind");
        // A non-strict lookup still serves and reports the lag honestly.
        let (status, body) = lookup(&non_replica).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["behind"], true);
    }

    #[tokio::test]
    async fn state_store_named_key_lifecycle_over_http() {
        let directory = TempDir::new().unwrap();
        let store: Arc<dyn LogStore> =
            Arc::new(FileLogStore::open(directory.path().join("data")).unwrap());
        let nodes: Vec<crate::active_range::StorageNodeId> =
            ["storage-1", "storage-2", "storage-3"]
                .into_iter()
                .map(|name| crate::active_range::StorageNodeId::try_new(name).unwrap())
                .collect();
        let control = Arc::new(
            ControlController::open_with_storage_nodes(
                directory.path().join("catalog.json"),
                store.clone(),
                nodes.clone(),
            )
            .unwrap(),
        );
        control.execute("CREATE SPACE accounts;").await.unwrap();
        control
            .execute_commands(vec![Command::DefineStateStore {
                name: "accounts.profiles".to_owned(),
                source: crate::control::StateStoreSourceRequest::Manual,
                named_keys: vec![
                    crate::control::NamedStateStoreKey {
                        name: "passport".to_owned(),
                        unique: true,
                        extractor: None,
                    },
                    crate::control::NamedStateStoreKey {
                        name: "department".to_owned(),
                        unique: false,
                        extractor: None,
                    },
                ],
            }])
            .await
            .unwrap();

        let control_key = "this-is-a-long-control-plane-key".to_owned();
        let mut listeners = Vec::new();
        let mut endpoints = BTreeMap::new();
        for node in &nodes {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            endpoints.insert(
                node.clone(),
                format!("http://{}", listener.local_addr().unwrap()),
            );
            listeners.push(listener);
        }
        for (index, node) in nodes.iter().enumerate() {
            let listener = listeners.remove(0);
            listener.set_nonblocking(true).unwrap();
            let journal = Arc::new(
                crate::state_store::FjallStateStoreMutationJournal::open(
                    directory.path().join(format!("journal-{index}")),
                )
                .unwrap(),
            );
            let index_store: Arc<dyn crate::index::IndexStore> = Arc::new(
                crate::index::FjallIndexStore::open(
                    directory.path().join(format!("index-{index}")),
                )
                .unwrap(),
            );
            let service = crate::state_store::StateStoreMutationService::new(
                node.clone(),
                control.clone(),
                journal,
                Arc::new(crate::state_store::IndexStoreStateApply::new(index_store)),
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
                store: store.clone(),
                membership,
                demand: DemandMetrics::default(),
                autoscaler: Arc::new(Mutex::new(AutoscaleController::default())),
                reader_pacing: crate::reader::ReaderPacingTable::default(),
                control: control.clone(),
                control_plane: None,
                progress_authority: control.clone(),
                replica_append: None,
                majority_append: None,
                subscription_progress: None,
                effect_journal: None,
                state_store_journal: Some(Arc::new(service)),
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
            let listener = tokio::net::TcpListener::from_std(listener).unwrap();
            tokio::spawn(async move {
                axum::serve(listener, router(state)).await.unwrap();
            });
        }

        let client = reqwest::Client::new();
        let admin = "this-is-a-long-development-api-key";
        let ingress = &nodes[0];

        // Two rows land through the quorum path with named-key values.
        for (key, passport, department) in [("user-1", "P111", "eng"), ("user-2", "P222", "eng")] {
            let response = client
                .post(format!("{}/v1/state-stores/mutate", endpoints[ingress]))
                .bearer_auth(admin)
                .json(&serde_json::json!({
                    "store": "accounts.profiles",
                    "mutation_id": Uuid::new_v4(),
                    "op": {
                        "kind": "put_row",
                        "key_base64": STANDARD.encode(key),
                        "payload_base64": STANDARD.encode(format!("profile-{key}")),
                        "named": [
                            {"name": "passport", "values": [{"kind": "text", "value": passport}]},
                            {"name": "department", "values": [{"kind": "text", "value": department}]},
                        ],
                    }
                }))
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }

        // Mutating a key the store does not declare is refused, not ignored.
        let response = client
            .post(format!("{}/v1/state-stores/mutate", endpoints[ingress]))
            .bearer_auth(admin)
            .json(&serde_json::json!({
                "store": "accounts.profiles",
                "mutation_id": Uuid::new_v4(),
                "op": {
                    "kind": "put_row",
                    "key_base64": STANDARD.encode(b"user-3"),
                    "payload_base64": STANDARD.encode(b"x"),
                    "named": [{"name": "undeclared", "values": [{"kind": "text", "value": "v"}]}],
                }
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        // The group/list read: one department value resolves both rows.
        let response = client
            .post(format!("{}/v1/state-stores/get-by", endpoints[ingress]))
            .bearer_auth(admin)
            .json(&serde_json::json!({
                "store": "accounts.profiles",
                "named_key": "department",
                "values": [{"kind": "text", "value": "eng"}],
                "strict": true,
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = response.json().await.unwrap();
        let keys: Vec<String> = body["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["key_base64"].as_str().unwrap().to_owned())
            .collect();
        assert_eq!(keys.len(), 2, "{body}");
        assert!(keys.contains(&STANDARD.encode(b"user-1")));
        assert!(keys.contains(&STANDARD.encode(b"user-2")));

        // The unique key resolves to exactly one row.
        let response = client
            .post(format!("{}/v1/state-stores/get-by", endpoints[ingress]))
            .bearer_auth(admin)
            .json(&serde_json::json!({
                "store": "accounts.profiles",
                "named_key": "passport",
                "values": [{"kind": "text", "value": "P111"}],
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = response.json().await.unwrap();
        assert_eq!(body["rows"].as_array().unwrap().len(), 1);

        // Range reads bound the same named key: text range [a, f)
        // includes both "eng" rows; an exclusive upper on "eng" empties.
        let response = client
            .post(format!("{}/v1/state-stores/get-by", endpoints[ingress]))
            .bearer_auth(admin)
            .json(&serde_json::json!({
                "store": "accounts.profiles",
                "named_key": "department",
                "lower": {"value": {"kind": "text", "value": "a"}},
                "upper": {"value": {"kind": "text", "value": "f"}, "inclusive": false},
                "strict": true,
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = response.json().await.unwrap();
        assert_eq!(body["rows"].as_array().unwrap().len(), 2, "{body}");

        let response = client
            .post(format!("{}/v1/state-stores/get-by", endpoints[ingress]))
            .bearer_auth(admin)
            .json(&serde_json::json!({
                "store": "accounts.profiles",
                "named_key": "department",
                "lower": {"value": {"kind": "text", "value": "a"}},
                "upper": {"value": {"kind": "text", "value": "eng"}, "inclusive": false},
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = response.json().await.unwrap();
        assert_eq!(body["rows"].as_array().unwrap().len(), 0, "{body}");

        // Mixing equality values with range bounds is a request error.
        let response = client
            .post(format!("{}/v1/state-stores/get-by", endpoints[ingress]))
            .bearer_auth(admin)
            .json(&serde_json::json!({
                "store": "accounts.profiles",
                "named_key": "department",
                "values": [{"kind": "text", "value": "eng"}],
                "lower": {"value": {"kind": "text", "value": "a"}},
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        // An undeclared named key is a request error.
        let response = client
            .post(format!("{}/v1/state-stores/get-by", endpoints[ingress]))
            .bearer_auth(admin)
            .json(&serde_json::json!({
                "store": "accounts.profiles",
                "named_key": "absent",
                "values": [{"kind": "text", "value": "eng"}],
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        // Iterate returns the applied rows in order with freshness evidence.
        let response = client
            .post(format!("{}/v1/state-stores/iterate", endpoints[ingress]))
            .bearer_auth(admin)
            .json(&serde_json::json!({
                "store": "accounts.profiles",
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = response.json().await.unwrap();
        assert_eq!(body["rows"].as_array().unwrap().len(), 2);
        assert!(body["pinned_seq"].as_u64().unwrap() >= 2);

        // Clear is one journaled mutation; reads then see an empty store.
        let response = client
            .post(format!("{}/v1/state-stores/clear", endpoints[ingress]))
            .bearer_auth(admin)
            .json(&serde_json::json!({
                "store": "accounts.profiles",
                "mutation_id": Uuid::new_v4(),
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let response = client
            .post(format!("{}/v1/state-stores/iterate", endpoints[ingress]))
            .bearer_auth(admin)
            .json(&serde_json::json!({
                "store": "accounts.profiles",
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = response.json().await.unwrap();
        assert_eq!(body["rows"].as_array().unwrap().len(), 0);
        // The cleared rows' named postings are gone too.
        let response = client
            .post(format!("{}/v1/state-stores/get-by", endpoints[ingress]))
            .bearer_auth(admin)
            .json(&serde_json::json!({
                "store": "accounts.profiles",
                "named_key": "department",
                "values": [{"kind": "text", "value": "eng"}],
                "strict": true,
            }))
            .send()
            .await
            .unwrap();
        let body: Value = response.json().await.unwrap();
        assert_eq!(body["rows"].as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn state_store_replica_catches_up_over_internal_http() {
        let directory = TempDir::new().unwrap();
        let store: Arc<dyn LogStore> =
            Arc::new(FileLogStore::open(directory.path().join("data")).unwrap());
        let nodes: Vec<crate::active_range::StorageNodeId> =
            ["storage-1", "storage-2", "storage-3"]
                .into_iter()
                .map(|name| crate::active_range::StorageNodeId::try_new(name).unwrap())
                .collect();
        let control = Arc::new(
            ControlController::open_with_storage_nodes(
                directory.path().join("catalog.json"),
                store.clone(),
                nodes.clone(),
            )
            .unwrap(),
        );
        control.execute("CREATE SPACE accounts;").await.unwrap();
        control
            .execute_commands(vec![Command::DefineStateStore {
                name: "accounts.profiles".to_owned(),
                source: crate::control::StateStoreSourceRequest::Manual,
                named_keys: vec![],
            }])
            .await
            .unwrap();
        let definition = control
            .declared_state_store_by_name("accounts.profiles")
            .await
            .unwrap();
        let assignment = control
            .active_state_store_assignment_by_id(definition.store_id)
            .await
            .unwrap();

        let control_key = "this-is-a-long-control-plane-key".to_owned();
        let mut listeners = Vec::new();
        let mut endpoints = BTreeMap::new();
        for node in &nodes {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            endpoints.insert(
                node.clone(),
                format!("http://{}", listener.local_addr().unwrap()),
            );
            listeners.push(listener);
        }
        for (index, node) in nodes.iter().enumerate() {
            let listener = listeners.remove(0);
            listener.set_nonblocking(true).unwrap();
            let journal = Arc::new(
                crate::state_store::FjallStateStoreMutationJournal::open(
                    directory.path().join(format!("journal-{index}")),
                )
                .unwrap(),
            );
            let index_store: Arc<dyn crate::index::IndexStore> = Arc::new(
                crate::index::FjallIndexStore::open(
                    directory.path().join(format!("index-{index}")),
                )
                .unwrap(),
            );
            let service = crate::state_store::StateStoreMutationService::new(
                node.clone(),
                control.clone(),
                journal.clone(),
                Arc::new(crate::state_store::IndexStoreStateApply::new(
                    index_store.clone(),
                )),
            );
            let membership = Arc::new(MembershipService::new(
                MemberAnnouncement {
                    node_id: format!("catchup-node-{index}"),
                    api_url: "http://catchup-node:7070".to_owned(),
                    capacity: 100,
                },
                vec![],
                Duration::from_secs(1),
                Duration::from_secs(5),
            ));
            let state = AppState {
                store: store.clone(),
                membership,
                demand: DemandMetrics::default(),
                autoscaler: Arc::new(Mutex::new(AutoscaleController::default())),
                reader_pacing: crate::reader::ReaderPacingTable::default(),
                control: control.clone(),
                control_plane: None,
                progress_authority: control.clone(),
                replica_append: None,
                majority_append: None,
                subscription_progress: None,
                effect_journal: None,
                state_store_journal: Some(Arc::new(service)),
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
            let listener = tokio::net::TcpListener::from_std(listener).unwrap();
            tokio::spawn(async move {
                axum::serve(listener, router(state)).await.unwrap();
            });
        }

        // Mutate over the public API — replicas commit and apply.
        let client = reqwest::Client::new();
        for key in ["k1", "k2", "k3"] {
            let response = client
                .post(format!("{}/v1/state-stores/mutate", endpoints[&nodes[0]]))
                .bearer_auth("this-is-a-long-development-api-key")
                .json(&serde_json::json!({
                    "store": "accounts.profiles",
                    "mutation_id": Uuid::new_v4(),
                    "op": {
                        "kind": "put_row",
                        "key_base64": STANDARD.encode(key.as_bytes()),
                        "payload_base64": STANDARD.encode(format!("payload-{key}").as_bytes()),
                    }
                }))
                .send()
                .await
                .unwrap();
            let status = response.status();
            let body = response.text().await.unwrap();
            assert_eq!(status, StatusCode::OK, "{body}");
        }

        // A replacement replica with a wiped journal and Index rejoins
        // under the same Node identity and catches up over real HTTP.
        let lagging_node = assignment
            .replicas
            .iter()
            .find(|replica| **replica != assignment.owner)
            .unwrap()
            .clone();
        let fresh_journal = Arc::new(
            crate::state_store::FjallStateStoreMutationJournal::open(
                directory.path().join("journal-fresh"),
            )
            .unwrap(),
        );
        let fresh_index: Arc<dyn crate::index::IndexStore> = Arc::new(
            crate::index::FjallIndexStore::open(directory.path().join("index-fresh")).unwrap(),
        );
        let fresh_service = crate::state_store::StateStoreMutationService::new(
            lagging_node.clone(),
            control.clone(),
            fresh_journal.clone(),
            Arc::new(crate::state_store::IndexStoreStateApply::new(
                fresh_index.clone(),
            )),
        );
        let transport: Arc<dyn crate::state_store::StateStoreMutationTransport> = Arc::new(
            crate::state_store::HttpStateStoreMutationTransport::new(
                assignment.clone(),
                crate::internal_plane::InternalEndpoints::from(endpoints.clone()),
                control_key.clone(),
                Duration::from_secs(5),
            )
            .unwrap(),
        );
        let outcome = fresh_service
            .catch_up_replica(&transport, &assignment, 64)
            .await
            .unwrap();
        assert_eq!(outcome.installed, 3);
        assert_eq!(outcome.applied, 3);
        assert!(outcome.complete);

        // Chain digests converge with what the replicas durably recorded.
        let caught = fresh_journal.inspect(definition.store_id).unwrap();
        assert_eq!(caught.applied_seq, 3);
        assert!(caught.applied_digest.is_some());
        let row = fresh_index
            .get(&crate::index::PrimaryRef {
                feed_id: definition.store_id,
                application_key: b"k2".to_vec(),
            })
            .unwrap()
            .unwrap();
        assert_eq!(row.payload, b"payload-k2");
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
    async fn reader_lookup_waits_for_committed_metadata_to_apply_on_ingress() {
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
        assert!(reader_when_applied(&control, "orders.audit", false)
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
                    Command::CreateReader {
                        name: "orders.audit".to_owned(),
                        feed: "orders.events".to_owned(),
                        start: crate::control::ReaderStart::Beginning,
                    },
                ])
                .await
                .unwrap();
        });
        let reader = reader_when_applied(&control, "orders.audit", true)
            .await
            .unwrap();
        assert_eq!(reader.name, "orders.audit");
        applied.await.unwrap();
    }

    #[tokio::test]
    async fn subscription_lookup_waits_for_committed_metadata_to_apply_on_ingress() {
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
        assert!(subscription_when_applied(&control, "orders.billing", false)
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
                    Command::CreateSubscription {
                        name: "orders.billing".to_owned(),
                        feed: Some("orders.events".to_owned()),
                        domain: None,
                        start: crate::control::ReaderStart::Beginning,
                        delivery_delay_ns: None,
                    },
                ])
                .await
                .unwrap();
        });
        let subscription = subscription_when_applied(&control, "orders.billing", true)
            .await
            .unwrap();
        assert_eq!(subscription.name, "orders.billing");
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
            reader_pacing: crate::reader::ReaderPacingTable::default(),
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
            state_store_journal: None,
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
            reader_pacing: crate::reader::ReaderPacingTable::default(),
            control: control.clone(),
            control_plane: None,
            progress_authority: control.clone(),
            replica_append: None,
            majority_append: None,
            subscription_progress: None,
            effect_journal: None,
            state_store_journal: None,
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
                reader_pacing: crate::reader::ReaderPacingTable::default(),
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
                state_store_journal: Some(Arc::new(
                    crate::state_store::StateStoreMutationService::new(
                        crate::active_range::StorageNodeId::try_new("control-1").unwrap(),
                        control.clone(),
                        Arc::new(
                            crate::state_store::FjallStateStoreMutationJournal::open(
                                directory.path().join("state-store-journal"),
                            )
                            .unwrap(),
                        ),
                        Arc::new(crate::state_store::IndexStoreStateApply::new(Arc::new(
                            crate::index::FjallIndexStore::open(
                                directory.path().join("state-store-index"),
                            )
                            .unwrap(),
                        ))),
                    ),
                )),
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
            feed_id: subscription.source.feed_id().unwrap(),
            ownership_epoch: assignment.ownership_epoch,
            sequence: 1,
            request_id: Uuid::from_u128(877),
            expected_cursor: None,
            cursor: "rf1_test".to_owned(),
            positions: BTreeMap::from([(
                RangeId::from_uuid(Uuid::from_u128(878)),
                "event-1".to_owned(),
            )]),

            quarantined: BTreeMap::new(),
            quarantine_add: BTreeMap::new(),
            quarantine_remove: BTreeMap::new(),
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
            feed_id: subscription.source.feed_id().unwrap(),
            ownership_epoch: assignment.ownership_epoch,
            sequence: 1,
            request_id: Uuid::from_u128(900),
            expected_cursor: None,
            cursor: "rf1_page".to_owned(),
            positions: BTreeMap::from([(
                RangeId::from_uuid(Uuid::from_u128(901)),
                "record".to_owned(),
            )]),

            quarantined: BTreeMap::new(),
            quarantine_add: BTreeMap::new(),
            quarantine_remove: BTreeMap::new(),
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
            feed_id: subscription.source.feed_id().unwrap(),
            ownership_epoch: assignment.ownership_epoch,
            sequence: 1,
            request_id: Uuid::from_u128(960),
            expected_cursor: None,
            cursor: "rf1_page".to_owned(),
            positions: BTreeMap::from([(
                RangeId::from_uuid(Uuid::from_u128(961)),
                "record".to_owned(),
            )]),

            quarantined: BTreeMap::new(),
            quarantine_add: BTreeMap::new(),
            quarantine_remove: BTreeMap::new(),
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
    async fn live_effect_apply_mutate_state_fails_closed_without_a_reachable_store() {
        let directory = TempDir::new().unwrap();
        let (state, control_plane, control, _replica) =
            internal_replica_test_state(&directory).await;
        let nodes = ["control-1", "control-2", "control-3"]
            .map(|name| crate::active_range::StorageNodeId::try_new(name).unwrap());
        let assignment = crate::reader::SubscriptionProgressAssignment::try_new(
            Uuid::new_v4(),
            nodes[0].clone(),
            ReplicaSet::try_new(nodes).unwrap(),
            1,
        )
        .unwrap();
        let (progress, _) = member_assignment_coordinator(&state, assignment).unwrap();
        let sink = LiveEffectApply {
            state: &state,
            progress,
            subscription_id: Uuid::new_v4(),
            ownership_epoch: 1,
            sweep_depth: 0,
        };
        let mutation = |store_id: Uuid| crate::effect::EffectStateMutation {
            store_id,
            mutation_id: Uuid::new_v4(),
            op: crate::state_store::StateStoreMutationOp::ClearAll,
        };

        // A mutation declared against a store with no journal placement
        // fails closed rather than fabricating a commit.
        assert!(matches!(
            crate::effect::EffectApply::mutate_state(&sink, &mutation(Uuid::new_v4())).await,
            Err(crate::effect::EffectJournalError::Unavailable)
                | Err(crate::effect::EffectJournalError::Conflict)
        ));

        // A defined store whose mutation-journal replicas are unreachable
        // also fails closed; the retry settles through the mutation's
        // declared dedupe identity.
        control
            .execute("DEFINE STATESTORE orders.ledger;")
            .await
            .unwrap();
        let store = control
            .declared_state_store_by_name("orders.ledger")
            .await
            .unwrap();
        assert!(
            crate::effect::EffectApply::mutate_state(&sink, &mutation(store.store_id))
                .await
                .is_err()
        );
        control_plane.raft().shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn window_emission_scan_fails_closed_and_skips_unprojectable_pages() {
        let directory = TempDir::new().unwrap();
        let (state, control_plane, _control, _replica) =
            internal_replica_test_state(&directory).await;
        let pipe = crate::control::PipeDefinition {
            pipe_id: Uuid::new_v4(),
            name: "orders.window".to_owned(),
            space_id: Uuid::new_v4(),
            subscription_id: Uuid::new_v4(),
            operation: crate::control::PipeOperation::Window {
                window_ns: 60_000_000_000,
                key: crate::control::IndexFieldSpec::Key,
                aggregation: crate::control::WindowAggregation::Count,
                time: crate::control::WindowTime::Event,
                max_late_ns: Some(1_000),
                expire_ns: None,
                key_limit: None,
                byte_limit: None,
            },
            output_feed_id: Some(Uuid::new_v4()),
            output_store_id: Some(Uuid::new_v4()),
            stage: crate::control::PipeStage::Declared,
            status: crate::control::ResourceStatus::Active,
            created_at_ns: 0,
        };
        let record = |key: &str, ingest: i64| crate::pipe::FetchedRecord {
            message_id: Uuid::new_v4(),
            key_base64: STANDARD.encode(key),
            payload_base64: STANDARD.encode(b"{}"),
            metadata_base64: Default::default(),
            event_time_ns: ingest,
            ingest_time_ns: ingest,
            resolution: None,
        };

        // An empty page resolves "now" on the driver's own clock, so it
        // still scans the store — and fails closed when that store has
        // no reachable placement rather than skipping idle panes.
        assert!(
            resolve_window_emissions(&state, &pipe, 60_000_000_000, Some(1_000), None, &[],)
                .await
                .is_err()
        );

        // A real page under a store with no journal placement fails the
        // whole resolve — the drive retries rather than emitting a
        // partial close set.
        assert!(resolve_window_emissions(
            &state,
            &pipe,
            60_000_000_000,
            Some(1_000),
            None,
            &[record("acct", 1_000)],
        )
        .await
        .is_err());
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
                    feed_id: subscription.source.feed_id().unwrap(),
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
                    metadata_base64: BTreeMap::new(),
                    event_time_ns: 1,
                    writer_session_id: Uuid::from_u128(945),
                    writer_epoch: 1,
                    sequence: 1,
                }],
                state_mutations: Vec::new(),
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
    async fn pipe_drive_requires_the_progress_owner_and_epoch() {
        let directory = TempDir::new().unwrap();
        let (app, control_plane, control, _replica) =
            internal_replica_test_router(&directory).await;
        control
            .execute("CREATE FEED orders.enriched; CREATE PIPE orders.forward FROM SUBSCRIPTION orders.billing TO FEED orders.enriched;")
            .await
            .unwrap();
        let pipe = control.active_pipe_by_name("orders.forward").await.unwrap();
        let assignment = control
            .active_subscription_progress_assignment_by_id(pipe.subscription_id)
            .await
            .unwrap();
        let local = crate::active_range::StorageNodeId::try_new("control-1").unwrap();
        let drive = PipeDriveRequest {
            pipe_id: pipe.pipe_id,
            subscription_id: pipe.subscription_id,
            owner: assignment.owner.clone(),
            receiver: local.clone(),
            ownership_epoch: assignment.ownership_epoch,
            limit: Some(4),
        };
        let request = |body: serde_json::Value, key: Option<&str>| {
            let mut builder = Request::builder()
                .method("POST")
                .uri("/internal/pipe/drive")
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
                .oneshot(request(json!(drive), None))
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        let mut wrong_receiver = drive.clone();
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
        let mut wrong_epoch = drive.clone();
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
        // On the progress owner the drive reaches the coordinators: with no
        // established frontier and unreachable peers it degrades to a
        // retryable failure rather than writing anything.
        let expected = if assignment.owner == local {
            StatusCode::SERVICE_UNAVAILABLE
        } else {
            StatusCode::CONFLICT
        };
        let response = app
            .clone()
            .oneshot(request(
                json!(drive),
                Some("this-is-a-long-control-plane-key"),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), expected);
        // A pipe bound to another Subscription is refused before placement.
        let mut foreign = drive.clone();
        foreign.pipe_id = Uuid::from_u128(977);
        assert_eq!(
            app.clone()
                .oneshot(request(
                    json!(foreign),
                    Some("this-is-a-long-control-plane-key")
                ))
                .await
                .unwrap()
                .status(),
            StatusCode::CONFLICT
        );
        control_plane.raft().shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn drive_local_pipes_drives_only_locally_owned_progress_assignments() {
        let directory = TempDir::new().unwrap();
        let (state, control_plane, control, _replica) =
            internal_replica_test_state(&directory).await;
        control
            .execute("CREATE FEED orders.enriched; CREATE PIPE orders.forward FROM SUBSCRIPTION orders.billing TO FEED orders.enriched;")
            .await
            .unwrap();
        let pipe = control.active_pipe_by_name("orders.forward").await.unwrap();
        let assignment = control
            .active_subscription_progress_assignment_by_id(pipe.subscription_id)
            .await
            .unwrap();
        let local = crate::active_range::StorageNodeId::try_new("control-1").unwrap();
        let outcomes = drive_local_pipes(&state, 4).await;
        assert_eq!(outcomes.len(), 1);
        if assignment.owner == local {
            // Locally owned: the supervisor attempts the drive; with no
            // reachable progress quorum it surfaces a failure rather than a
            // silent skip.
            assert!(matches!(
                outcomes[0],
                PipeDriveOutcome::Failed { pipe_id, .. } if pipe_id == pipe.pipe_id
            ));
        } else {
            assert!(matches!(outcomes[0], PipeDriveOutcome::NotOwner));
        }
        // A paused Pipe keeps its frontier but yields no drive.
        control.execute("PAUSE PIPE orders.forward;").await.unwrap();
        assert!(drive_local_pipes(&state, 4).await.is_empty());
        control
            .execute("RESUME PIPE orders.forward;")
            .await
            .unwrap();
        assert_eq!(drive_local_pipes(&state, 4).await.len(), 1);
        // A Node with no storage identity drives nothing.
        let mut stateless = state;
        stateless.storage_node_id = None;
        assert!(drive_local_pipes(&stateless, 4).await.is_empty());
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
    async fn issued_api_keys_authorize_admin_requests_until_revoked() {
        let directory = TempDir::new().unwrap();
        let app = admin_test_router(&directory);
        let bootstrap = Some("this-is-a-long-development-api-key");
        let issued_secret = "issued-release-key-0123456789abcdef";
        let digest = blake3::hash(issued_secret.as_bytes()).to_hex().to_string();
        let wcl = |script: &str, key: Option<&str>| {
            admin_request(
                "/v1/admin/wcl",
                json!({ "request_id": Uuid::new_v4(), "script": script }),
                key,
            )
        };
        // Issue an identity whose Role holds `manage` on the root scope.
        let setup = app
            .clone()
            .oneshot(wcl(
                &format!(
                    "CREATE ROLE ops.platform; \
                     GRANT manage ON NAMESPACE * TO ROLE ops.platform; \
                     ISSUE API KEY ops.release ROLE ops.platform DIGEST {digest};"
                ),
                bootstrap,
            ))
            .await
            .unwrap();
        assert_eq!(setup.status(), StatusCode::OK);
        // SHOW API KEYS masks the verifier to a prefix.
        let shown = app
            .clone()
            .oneshot(wcl("SHOW API KEYS;", bootstrap))
            .await
            .unwrap();
        assert_eq!(shown.status(), StatusCode::OK);
        let body = axum::body::to_bytes(shown.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let execution: Value = serde_json::from_slice(&body).unwrap();
        let rows = execution["results"][0]["data"].as_array().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["name"], "ops.release");
        assert_eq!(rows[0]["role"], "ops.platform");
        assert_eq!(rows[0]["digest_prefix"].as_str().unwrap(), &digest[..16]);
        assert!(rows[0].get("digest").is_none());
        // The issued key authorizes an admin request end to end.
        let authorized = app
            .clone()
            .oneshot(wcl("SHOW ROLES;", Some(issued_secret)))
            .await
            .unwrap();
        assert_eq!(authorized.status(), StatusCode::OK);
        // A Domain-scoped Role does not authorize admin requests.
        let scoped_secret = "scoped-reader-key-0123456789abcdef01";
        let scoped_digest = blake3::hash(scoped_secret.as_bytes()).to_hex().to_string();
        let scoped = app
            .clone()
            .oneshot(wcl(
                &format!(
                    "CREATE DOMAIN orders; \
                     CREATE ROLE ops.reader; \
                     GRANT read ON NAMESPACE orders.* TO ROLE ops.reader; \
                     ISSUE API KEY ops.readkey ROLE ops.reader DIGEST {scoped_digest};"
                ),
                bootstrap,
            ))
            .await
            .unwrap();
        assert_eq!(scoped.status(), StatusCode::OK);
        let refused = app
            .clone()
            .oneshot(wcl("SHOW ROLES;", Some(scoped_secret)))
            .await
            .unwrap();
        assert_eq!(refused.status(), StatusCode::UNAUTHORIZED);
        // Revocation propagates into the request-time view immediately.
        let revoked = app
            .clone()
            .oneshot(wcl("REVOKE API KEY ops.release;", bootstrap))
            .await
            .unwrap();
        assert_eq!(revoked.status(), StatusCode::OK);
        let refused = app
            .clone()
            .oneshot(wcl("SHOW ROLES;", Some(issued_secret)))
            .await
            .unwrap();
        assert_eq!(refused.status(), StatusCode::UNAUTHORIZED);
        // The bootstrap env key is unaffected by catalog revocation.
        let still_ok = app.oneshot(wcl("SHOW API KEYS;", bootstrap)).await.unwrap();
        assert_eq!(still_ok.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn feed_data_plane_enforces_namespace_grants() {
        let directory = TempDir::new().unwrap();
        let app = admin_test_router(&directory);
        let bootstrap = Some("this-is-a-long-development-api-key");
        let reader_secret = "orders-reader-key-0123456789abcdef";
        let writer_secret = "orders-writer-key-0123456789abcdef";
        let manage_secret = "ops-manage-key-0123456789abcdef00";
        let billing_secret = "billing-ops-key-0123456789abcdef0";
        let domain_secret = "orders-domain-manage-key-0123456789";
        let digest = |secret: &str| blake3::hash(secret.as_bytes()).to_hex().to_string();
        let wcl = |script: &str, key: Option<&str>| {
            admin_request(
                "/v1/admin/wcl",
                json!({ "request_id": Uuid::new_v4(), "script": script }),
                key,
            )
        };
        let setup = app
            .clone()
            .oneshot(wcl(
                &format!(
                    "CREATE DOMAIN orders; CREATE FEED orders.events;                      CREATE DOMAIN billing; CREATE FEED billing.invoices;                      CREATE ROLE ops.reader;                      GRANT read ON NAMESPACE orders.* TO ROLE ops.reader;                      ISSUE API KEY ops.readerkey ROLE ops.reader DIGEST {};                      CREATE ROLE ops.writer;                      GRANT write ON NAMESPACE orders.* TO ROLE ops.writer;                      ISSUE API KEY ops.writerkey ROLE ops.writer DIGEST {};                      CREATE ROLE ops.admins;                      GRANT manage ON NAMESPACE * TO ROLE ops.admins;                      ISSUE API KEY ops.managekey ROLE ops.admins DIGEST {}; \
                     CREATE WRITER orders.writer TO orders.events; \
                     CREATE READER billing.sneaky FROM orders.events START AT BEGINNING; \
                     CREATE SUBSCRIPTION orders.billing FROM orders.events; \
                     CREATE SUBSCRIPTION orders.everything FROM DOMAIN orders; \
                     DEFINE STATESTORE orders.inventory; \
                     DEFINE STATESTORE orders.summary FROM FEED orders.events; \
                     CREATE INDEX orders.bykey ON orders.events FIELDS KEY; \
                     CREATE ROLE billing.ops; \
                     GRANT read, write ON NAMESPACE billing.* TO ROLE billing.ops; \
                     ISSUE API KEY ops.billingkey ROLE billing.ops DIGEST {}; \
                     CREATE ROLE ops.ordersmanage; \
                     GRANT manage ON NAMESPACE orders.* TO ROLE ops.ordersmanage; \
                     ISSUE API KEY ops.domainkey ROLE ops.ordersmanage DIGEST {};",
                    digest(reader_secret),
                    digest(writer_secret),
                    digest(manage_secret),
                    digest(billing_secret),
                    digest(domain_secret),
                ),
                bootstrap,
            ))
            .await
            .unwrap();
        assert_eq!(setup.status(), StatusCode::OK);

        let get = |path: &str, key: Option<&str>| {
            let mut request = Request::builder().method("GET").uri(path);
            if let Some(key) = key {
                request = request.header("authorization", format!("Bearer {key}"));
            }
            request.body(Body::empty()).unwrap()
        };
        let append = |key: Option<&str>| {
            admin_request(
                "/v1/feeds/append",
                json!({
                    "request_id": Uuid::new_v4(),
                    "feed": "orders.events",
                    "writer_session_id": Uuid::new_v4(),
                    "writer_epoch": 1,
                    "sequence": 1,
                    "key_base64": "a2V5LTE=",
                    "payload_base64": "cGF5bG9hZA==",
                }),
                key,
            )
        };
        // This test Node serves no Active Range storage, so an authorized
        // request clears the gate and fails downstream as UNAVAILABLE —
        // while an unauthorized request is refused as UNAUTHORIZED.
        let read_path = "/v1/feeds/records?feed=orders.events&limit=1";
        for (key, name) in [
            (Some(reader_secret), "reader"),
            (Some(manage_secret), "manager"),
            (bootstrap, "bootstrap"),
        ] {
            let response = app.clone().oneshot(get(read_path, key)).await.unwrap();
            assert_eq!(
                response.status(),
                StatusCode::SERVICE_UNAVAILABLE,
                "{name} should clear the read gate"
            );
        }
        // Read-only and write-only grants stay separated.
        let denied = app
            .clone()
            .oneshot(get(read_path, Some(writer_secret)))
            .await
            .unwrap();
        assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
        let denied = app
            .clone()
            .oneshot(append(Some(reader_secret)))
            .await
            .unwrap();
        assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
        let allowed = app
            .clone()
            .oneshot(append(Some(writer_secret)))
            .await
            .unwrap();
        assert_eq!(allowed.status(), StatusCode::SERVICE_UNAVAILABLE);
        let allowed = app.clone().oneshot(append(bootstrap)).await.unwrap();
        assert_eq!(allowed.status(), StatusCode::SERVICE_UNAVAILABLE);
        // An orders.* grant does not reach other Domains.
        let foreign = app
            .clone()
            .oneshot(get(
                "/v1/feeds/records?feed=billing.invoices&limit=1",
                Some(reader_secret),
            ))
            .await
            .unwrap();
        assert_eq!(foreign.status(), StatusCode::UNAUTHORIZED);
        // Missing credentials refuse before any data-plane work.
        let missing = app.clone().oneshot(get(read_path, None)).await.unwrap();
        assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);
        // Temporary fetch and search share the same gate.
        let fetch = app
            .clone()
            .oneshot(admin_request(
                "/v1/readers/temporary/fetch",
                json!({ "feed": "orders.events", "limit": 1 }),
                Some(reader_secret),
            ))
            .await
            .unwrap();
        assert_eq!(fetch.status(), StatusCode::SERVICE_UNAVAILABLE);
        let fetch = app
            .clone()
            .oneshot(admin_request(
                "/v1/readers/temporary/fetch",
                json!({ "feed": "orders.events", "limit": 1 }),
                Some(writer_secret),
            ))
            .await
            .unwrap();
        assert_eq!(fetch.status(), StatusCode::UNAUTHORIZED);
        let search = app
            .clone()
            .oneshot(get(
                "/v1/feeds/records/search?feed=orders.events&key_base64=a2V5LTE%3D",
                Some(reader_secret),
            ))
            .await
            .unwrap();
        assert_eq!(search.status(), StatusCode::SERVICE_UNAVAILABLE);
        let search = app
            .clone()
            .oneshot(get(
                "/v1/feeds/records/search?feed=orders.events&key_base64=a2V5LTE%3D",
                Some(writer_secret),
            ))
            .await
            .unwrap();
        assert_eq!(search.status(), StatusCode::UNAUTHORIZED);
        // Cursor lookup shares the read gate too.
        let lookup = app
            .clone()
            .oneshot(get(
                "/v1/feeds/records/lookup?feed=orders.events&cursor=opaque",
                Some(reader_secret),
            ))
            .await
            .unwrap();
        assert_eq!(lookup.status(), StatusCode::SERVICE_UNAVAILABLE);
        let lookup = app
            .clone()
            .oneshot(get(
                "/v1/feeds/records/lookup?feed=orders.events&cursor=opaque",
                Some(writer_secret),
            ))
            .await
            .unwrap();
        assert_eq!(lookup.status(), StatusCode::UNAUTHORIZED);
        // Writer appends authorize `write` against the Writer's Feed.
        let writer_append = |key: Option<&str>| {
            admin_request(
                "/v1/writers/append",
                json!({
                    "request_id": Uuid::new_v4(),
                    "writer": "orders.writer",
                    "session_epoch": 1,
                    "key_base64": "a2V5LTE=",
                    "payload_base64": "cGF5bG9hZA==",
                }),
                key,
            )
        };
        for (key, name) in [
            (Some(writer_secret), "writer"),
            (Some(manage_secret), "manager"),
            (bootstrap, "bootstrap"),
        ] {
            let response = app.clone().oneshot(writer_append(key)).await.unwrap();
            assert_ne!(
                response.status(),
                StatusCode::UNAUTHORIZED,
                "{name} should clear the writer gate"
            );
        }
        let denied = app
            .clone()
            .oneshot(writer_append(Some(reader_secret)))
            .await
            .unwrap();
        assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
        let denied = app.clone().oneshot(writer_append(None)).await.unwrap();
        assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
        // A Reader declared in a foreign Domain still authorizes against
        // its owning Feed: orders.* can open billing.sneaky, billing.*
        // cannot. The open itself succeeds, proving the grant cleared.
        let reader_open = |key: Option<&str>| {
            admin_request(
                "/v1/readers/open",
                json!({
                    "request_id": Uuid::new_v4(),
                    "reader": "billing.sneaky",
                    "capacity": 8,
                }),
                key,
            )
        };
        let opened = app
            .clone()
            .oneshot(reader_open(Some(reader_secret)))
            .await
            .unwrap();
        assert_eq!(opened.status(), StatusCode::OK);
        let cross = app
            .clone()
            .oneshot(reader_open(Some(billing_secret)))
            .await
            .unwrap();
        assert_eq!(cross.status(), StatusCode::UNAUTHORIZED);
        let missing = app.clone().oneshot(reader_open(None)).await.unwrap();
        assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);
        // Fetch, ack, and close share the same owning-Feed gate.
        for (path, body) in [
            (
                "/v1/readers/fetch",
                json!({ "request_id": Uuid::new_v4(), "reader": "billing.sneaky", "session_epoch": 1 }),
            ),
            (
                "/v1/readers/ack",
                json!({ "request_id": Uuid::new_v4(), "reader": "billing.sneaky", "session_epoch": 1, "cursor": "opaque" }),
            ),
            (
                "/v1/readers/close",
                json!({ "request_id": Uuid::new_v4(), "reader": "billing.sneaky", "session_epoch": 1 }),
            ),
        ] {
            let allowed = app
                .clone()
                .oneshot(admin_request(path, body.clone(), Some(reader_secret)))
                .await
                .unwrap();
            assert_ne!(
                allowed.status(),
                StatusCode::UNAUTHORIZED,
                "{path} should clear the read gate"
            );
            let denied = app
                .clone()
                .oneshot(admin_request(path, body, Some(billing_secret)))
                .await
                .unwrap();
            assert_eq!(
                denied.status(),
                StatusCode::UNAUTHORIZED,
                "{path} must deny"
            );
        }
        // Subscription member operations authenticate and authorize
        // read against the declared source — this Node runs no progress
        // replica, so authorized calls clear the gate into UNAVAILABLE.
        let member_join = |subscription: &str, key: Option<&str>| {
            admin_request(
                "/v1/subscriptions/members/join",
                json!({
                    "subscription": subscription,
                    "request_id": Uuid::new_v4(),
                    "member_id": Uuid::new_v4(),
                }),
                key,
            )
        };
        // Feed-sourced: the read grant on orders.* covers the source Feed.
        let joined = app
            .clone()
            .oneshot(member_join("orders.billing", Some(reader_secret)))
            .await
            .unwrap();
        assert_eq!(joined.status(), StatusCode::SERVICE_UNAVAILABLE);
        let denied = app
            .clone()
            .oneshot(member_join("orders.billing", Some(writer_secret)))
            .await
            .unwrap();
        assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
        let denied = app
            .clone()
            .oneshot(member_join("orders.billing", Some(billing_secret)))
            .await
            .unwrap();
        assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
        // Domain-sourced: orders.* spans the Domain, billing.* does not.
        let joined = app
            .clone()
            .oneshot(member_join("orders.everything", Some(reader_secret)))
            .await
            .unwrap();
        assert_eq!(joined.status(), StatusCode::SERVICE_UNAVAILABLE);
        let denied = app
            .clone()
            .oneshot(member_join("orders.everything", Some(billing_secret)))
            .await
            .unwrap();
        assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
        let missing = app
            .clone()
            .oneshot(member_join("orders.billing", None))
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);
        // StateStore reads need read on the store's own namespace;
        // mutate needs write. orders.* covers orders.inventory while
        // billing.* never reaches it.
        let store_lookup = |key: Option<&str>| {
            admin_request(
                "/v1/state-stores/lookup",
                json!({ "store": "orders.inventory", "key_base64": "a2V5" }),
                key,
            )
        };
        let looked = app
            .clone()
            .oneshot(store_lookup(Some(reader_secret)))
            .await
            .unwrap();
        assert_eq!(looked.status(), StatusCode::SERVICE_UNAVAILABLE);
        for key in [Some(writer_secret), Some(billing_secret)] {
            let denied = app.clone().oneshot(store_lookup(key)).await.unwrap();
            assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
        }
        let store_mutate = |key: Option<&str>| {
            admin_request(
                "/v1/state-stores/mutate",
                json!({
                    "store": "orders.inventory",
                    "mutation_id": Uuid::new_v4(),
                    "op": { "kind": "put_row", "key_base64": "a2V5", "payload_base64": "cGF5" },
                }),
                key,
            )
        };
        let mutated = app
            .clone()
            .oneshot(store_mutate(Some(writer_secret)))
            .await
            .unwrap();
        assert_eq!(mutated.status(), StatusCode::SERVICE_UNAVAILABLE);
        for key in [Some(reader_secret), Some(billing_secret)] {
            let denied = app.clone().oneshot(store_mutate(key)).await.unwrap();
            assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
        }
        // A feed-derived sync replays into the store: write scope.
        let store_sync = |key: Option<&str>| {
            admin_request(
                "/v1/state-stores/sync",
                json!({ "store": "orders.summary" }),
                key,
            )
        };
        let synced = app
            .clone()
            .oneshot(store_sync(Some(writer_secret)))
            .await
            .unwrap();
        assert_eq!(synced.status(), StatusCode::SERVICE_UNAVAILABLE);
        let denied = app
            .clone()
            .oneshot(store_sync(Some(reader_secret)))
            .await
            .unwrap();
        assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
        // Index queries need read on the Index's own name; a declared
        // but unbuilt Index reaches the readiness check downstream.
        let index_query = |key: Option<&str>| {
            admin_request(
                "/v1/indexes/query",
                json!({
                    "index": "orders.bykey",
                    "mode": "exact",
                    "values": [{ "kind": "text", "value": "k" }],
                }),
                key,
            )
        };
        let queried = app
            .clone()
            .oneshot(index_query(Some(reader_secret)))
            .await
            .unwrap();
        assert_eq!(queried.status(), StatusCode::CONFLICT);
        for key in [Some(writer_secret), Some(billing_secret), None] {
            let denied = app.clone().oneshot(index_query(key)).await.unwrap();
            assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
        }
        // Subscription seeking repositions shared progress: manage scope
        // on the source, not read or write.
        let seek = |key: Option<&str>| {
            admin_request(
                "/v1/subscriptions/seek",
                json!({
                    "subscription": "orders.billing",
                    "request_id": Uuid::new_v4(),
                    "position": "beginning",
                }),
                key,
            )
        };
        let sought = app
            .clone()
            .oneshot(seek(Some(manage_secret)))
            .await
            .unwrap();
        assert_eq!(sought.status(), StatusCode::SERVICE_UNAVAILABLE);
        for key in [
            Some(reader_secret),
            Some(writer_secret),
            Some(billing_secret),
            None,
        ] {
            let denied = app.clone().oneshot(seek(key)).await.unwrap();
            assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
        }
        // WCL declaration commands scope to the resource's Domain:
        // manage on orders.* declares inside orders, never billing.
        let created = app
            .clone()
            .oneshot(wcl("CREATE FEED orders.new;", Some(domain_secret)))
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::OK);
        for key in [
            Some(reader_secret),
            Some(writer_secret),
            Some(billing_secret),
        ] {
            let denied = app
                .clone()
                .oneshot(wcl("CREATE FEED orders.another;", key))
                .await
                .unwrap();
            assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
        }
        let denied = app
            .clone()
            .oneshot(wcl("CREATE FEED billing.foreign;", Some(domain_secret)))
            .await
            .unwrap();
        assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
        // DESCRIBE reads span the resource's Domain; SHOW enumerates the
        // whole catalog and needs read on *.
        let described = app
            .clone()
            .oneshot(wcl("DESCRIBE FEED orders.events;", Some(reader_secret)))
            .await
            .unwrap();
        assert_eq!(described.status(), StatusCode::OK);
        for key in [Some(reader_secret), Some(domain_secret)] {
            let denied = app.clone().oneshot(wcl("SHOW FEEDS;", key)).await.unwrap();
            assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
        }
        // RBAC administration stays Riverbed-wide even for Domain manages.
        let denied = app
            .clone()
            .oneshot(wcl("CREATE ROLE sneaky.role;", Some(domain_secret)))
            .await
            .unwrap();
        assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
        // An explicit DENY overrides covering allows: fencing the Index
        // name revokes just that resource for the reader while the rest
        // of the wildcard allow keeps working.
        let denied_grant = app
            .clone()
            .oneshot(wcl(
                "DENY read ON NAMESPACE orders.bykey TO ROLE ops.reader;",
                bootstrap,
            ))
            .await
            .unwrap();
        assert_eq!(denied_grant.status(), StatusCode::OK);
        let fenced = app
            .clone()
            .oneshot(index_query(Some(reader_secret)))
            .await
            .unwrap();
        assert_eq!(fenced.status(), StatusCode::UNAUTHORIZED);
        // The bootstrap key is break-glass and still reaches the service.
        let allowed = app.clone().oneshot(index_query(bootstrap)).await.unwrap();
        assert_eq!(allowed.status(), StatusCode::CONFLICT);
        // Reads elsewhere in the wildcard scope are unaffected.
        let still = app
            .clone()
            .oneshot(get(read_path, Some(reader_secret)))
            .await
            .unwrap();
        assert_eq!(still.status(), StatusCode::SERVICE_UNAVAILABLE);
        // REVOKE GRANT subtracts the wildcard allow — the reader's
        // remaining DENY is unaffected and nothing covers orders.* now.
        let revoked_grant = app
            .clone()
            .oneshot(wcl(
                "REVOKE read ON NAMESPACE orders.* FROM ROLE ops.reader;",
                bootstrap,
            ))
            .await
            .unwrap();
        assert_eq!(revoked_grant.status(), StatusCode::OK);
        let fenced = app
            .clone()
            .oneshot(get(read_path, Some(reader_secret)))
            .await
            .unwrap();
        assert_eq!(fenced.status(), StatusCode::UNAUTHORIZED);
        // Revoking what no longer exists fails closed.
        let gone = app
            .clone()
            .oneshot(wcl(
                "REVOKE read ON NAMESPACE orders.* FROM ROLE ops.reader;",
                bootstrap,
            ))
            .await
            .unwrap();
        assert_eq!(gone.status(), StatusCode::BAD_REQUEST);
        // Revocation fences the read grant immediately.
        let revoked = app
            .clone()
            .oneshot(wcl("REVOKE API KEY ops.readerkey;", bootstrap))
            .await
            .unwrap();
        assert_eq!(revoked.status(), StatusCode::OK);
        let fenced = app
            .clone()
            .oneshot(get(read_path, Some(reader_secret)))
            .await
            .unwrap();
        assert_eq!(fenced.status(), StatusCode::UNAUTHORIZED);
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
                        fetch_limit: None,
                        fetch_retry_after_ms: None,
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
                    subscription_translations: Vec::new(),
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
                reader_pacing: crate::reader::ReaderPacingTable::default(),
                replica_append: Some(services[node].clone()),
                control: control.clone(),
                control_plane: plane,
                progress_authority: control.clone(),
                majority_append: Some(Arc::new(
                    crate::active_range::MajorityAppendCoordinator::new(
                        services[node].clone(),
                        control.clone(),
                        Arc::new(LoopbackReplicaTransport {
                            services: Arc::new(services.clone()),
                        }),
                    ),
                )),
                subscription_progress: None,
                effect_journal: None,
                state_store_journal: None,
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

        // Bounded exact-Key search: the seeded keys alternate between the
        // two deterministic routing keys, so an exact match set is
        // deterministic and a miss is provable.
        let search_key = routed_key(0, split_at);
        let search = |query: &[(&str, &str)]| {
            let client = client.clone();
            let url = format!("{}/v1/feeds/records/search", endpoints[&nodes[0]]);
            let query: Vec<(String, String)> = query
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect();
            async move {
                let response = client
                    .get(url)
                    .query(&query)
                    .bearer_auth("this-is-a-long-development-api-key")
                    .send()
                    .await
                    .unwrap();
                let status = response.status();
                let body: Value = response.json().await.unwrap_or_default();
                (status, body)
            }
        };
        let (status, body) = search(&[
            ("feed", "orders.events"),
            ("key_base64", &STANDARD.encode(&search_key)),
        ])
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(
            body["records"]
                .as_array()
                .unwrap()
                .iter()
                .map(|record| record["cursor"].as_str().unwrap().to_owned())
                .collect::<Vec<_>>(),
            vec![
                "cursor-2",
                "cursor-4",
                "cursor-6",
                "cursor-8",
                "cursor-10",
                "cursor-12"
            ]
        );
        assert_eq!(body["exhausted"], json!(true));
        assert_eq!(body["scanned"], json!(12));
        // A bounded page reports the continuation Cursor so the caller
        // resumes exactly where the scan budget stopped.
        let (status, body) = search(&[
            ("feed", "orders.events"),
            ("key_base64", &STANDARD.encode(&search_key)),
            ("limit", "3"),
        ])
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["records"].as_array().unwrap().len(), 3);
        assert_eq!(body["exhausted"], json!(false));
        let next_after = body["next_after"].as_str().unwrap().to_owned();
        let (status, body) = search(&[
            ("feed", "orders.events"),
            ("key_base64", &STANDARD.encode(&search_key)),
            ("after", &next_after),
        ])
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(
            body["records"]
                .as_array()
                .unwrap()
                .iter()
                .map(|record| record["cursor"].as_str().unwrap().to_owned())
                .collect::<Vec<_>>(),
            vec!["cursor-8", "cursor-10", "cursor-12"]
        );
        assert_eq!(body["exhausted"], json!(true));
        // A miss scans the committed history, reports its cost, and returns
        // no fabricated continuation.
        let (status, body) = search(&[
            ("feed", "orders.events"),
            ("key_base64", &STANDARD.encode(b"absent")),
        ])
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert!(body["records"].as_array().unwrap().is_empty());
        assert_eq!(body["scanned"], json!(12));
        assert_eq!(body["exhausted"], json!(true));
        // Inclusive event-time bounds answer "what was written in this
        // window" without exposing ingest order; bounds combine with the
        // Key predicate.
        let (status, body) = search(&[
            ("feed", "orders.events"),
            ("from_event_time_ns", "3"),
            ("to_event_time_ns", "6"),
        ])
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(
            body["records"]
                .as_array()
                .unwrap()
                .iter()
                .map(|record| record["cursor"].as_str().unwrap().to_owned())
                .collect::<Vec<_>>(),
            vec!["cursor-3", "cursor-4", "cursor-5", "cursor-6"]
        );
        let (status, body) = search(&[
            ("feed", "orders.events"),
            ("key_base64", &STANDARD.encode(&search_key)),
            ("from_event_time_ns", "4"),
            ("to_event_time_ns", "8"),
        ])
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(
            body["records"]
                .as_array()
                .unwrap()
                .iter()
                .map(|record| record["cursor"].as_str().unwrap().to_owned())
                .collect::<Vec<_>>(),
            vec!["cursor-4", "cursor-6", "cursor-8"]
        );
        let (status, _) = search(&[
            ("feed", "orders.events"),
            ("from_event_time_ns", "9"),
            ("to_event_time_ns", "2"),
        ])
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let (status, _) = search(&[
            ("feed", "orders.events"),
            ("from_event_time_ns", "not-a-number"),
        ])
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        // A bare scan without any predicate is refused: the plain read is
        // the contract for unfiltered history.
        let (status, _) = search(&[("feed", "orders.events")]).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let (status, _) = search(&[("feed", "orders.events"), ("key_base64", "!!!")]).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        // Single-record investigation: one opaque Cursor resolves to the
        // record it names regardless of which Range holds it.
        let right_search_key = routed_key(1, split_at);
        for (cursor, expected_key) in [("cursor-4", &search_key), ("cursor-3", &right_search_key)] {
            let response = client
                .get(format!("{}/v1/feeds/records/lookup", endpoints[&nodes[0]]))
                .query(&[("feed", "orders.events"), ("cursor", cursor)])
                .bearer_auth("this-is-a-long-development-api-key")
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let body: Value = response.json().await.unwrap();
            assert_eq!(body["cursor"], json!(cursor));
            assert_eq!(body["key_base64"], json!(STANDARD.encode(expected_key)));
        }
        let response = client
            .get(format!("{}/v1/feeds/records/lookup", endpoints[&nodes[0]]))
            .query(&[("feed", "orders.events"), ("cursor", "cursor-404")])
            .bearer_auth("this-is-a-long-development-api-key")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        // Metadata tracing: a record appended over the public path with a
        // Metadata entry is findable by that entry without a consumer.
        let appended = client
            .post(format!("{}/v1/feeds/append", endpoints[&nodes[0]]))
            .bearer_auth("this-is-a-long-development-api-key")
            .json(&json!({
                "request_id": Uuid::new_v4(),
                "feed": "orders.events",
                "writer_session_id": Uuid::new_v4(),
                "writer_epoch": 1,
                "sequence": 1,
                "key_base64": STANDARD.encode(&search_key),
                "payload_base64": STANDARD.encode(b"traced"),
                "metadata_base64": {"trace_id": STANDARD.encode(b"trace-42")},
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(appended.status(), StatusCode::OK);
        let (status, body) =
            search(&[("feed", "orders.events"), ("metadata_key", "trace_id")]).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let records = body["records"].as_array().unwrap();
        assert_eq!(records.len(), 1, "{body}");
        assert_eq!(
            records[0]["metadata_base64"]["trace_id"],
            json!(STANDARD.encode(b"trace-42"))
        );
        // The exact-value arm narrows the same entry; a non-matching value
        // scans the history and reports the miss.
        for (value, expected) in [
            (STANDARD.encode(b"trace-42"), 1_usize),
            (STANDARD.encode(b"trace-99"), 0),
        ] {
            let (status, body) = search(&[
                ("feed", "orders.events"),
                ("metadata_key", "trace_id"),
                ("metadata_value_base64", &value),
            ])
            .await;
            assert_eq!(status, StatusCode::OK, "{body}");
            assert_eq!(
                body["records"].as_array().unwrap().len(),
                expected,
                "{body}"
            );
            assert_eq!(body["exhausted"], json!(true));
        }
        let (status, _) = search(&[
            ("feed", "orders.events"),
            ("metadata_value_base64", &STANDARD.encode(b"trace-42")),
        ])
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let (status, _) = search(&[("feed", "orders.events"), ("metadata_key", "")]).await;
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
                    feed: Some("orders.events".to_owned()),
                    domain: None,
                    start: crate::control::ReaderStart::Beginning,
                    delivery_delay_ns: None,
                },
                Command::CreateSubscription {
                    name: "orders.fresh".to_owned(),
                    feed: Some("orders.events".to_owned()),
                    domain: None,
                    start: crate::control::ReaderStart::Beginning,
                    delivery_delay_ns: None,
                },
                Command::CreateSubscription {
                    name: "orders.now".to_owned(),
                    feed: Some("orders.events".to_owned()),
                    domain: None,
                    start: crate::control::ReaderStart::Now,
                    delivery_delay_ns: None,
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
                reader_pacing: crate::reader::ReaderPacingTable::default(),
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
                state_store_journal: Some(Arc::new(
                    crate::state_store::StateStoreMutationService::new(
                        node.clone(),
                        control.clone(),
                        Arc::new(
                            crate::state_store::FjallStateStoreMutationJournal::open(
                                directory
                                    .path()
                                    .join(format!("state-store-journal-{index}")),
                            )
                            .unwrap(),
                        ),
                        Arc::new(crate::state_store::IndexStoreStateApply::new(Arc::new(
                            crate::index::FjallIndexStore::open(
                                directory.path().join(format!("state-store-index-{index}")),
                            )
                            .unwrap(),
                        ))),
                    ),
                )),
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
                feed_id: definition.source.feed_id().unwrap(),
                ownership_epoch: assignment.ownership_epoch,
                sequence: 1,
                request_id: Uuid::from_u128(0x9001),
                expected_cursor: None,
                cursor: "cursor-2".to_owned(),
                positions: BTreeMap::from([(progress_range, "cursor-2".to_owned())]),
                quarantined: BTreeMap::new(),
                quarantine_add: BTreeMap::new(),
                quarantine_remove: BTreeMap::new(),
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
                let response = client
                    .post(url)
                    .header("authorization", "Bearer this-is-a-long-development-api-key")
                    .json(&body)
                    .send()
                    .await
                    .unwrap();
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
            .bearer_auth("this-is-a-long-development-api-key")
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
        // The member acknowledges the delivered page atomically, marking
        // one delivered record as delivered-but-not-processed — durable
        // poison/quarantine evidence the operator can inspect and a later
        // reposition can reprocess.
        let (status, body) = post(
            "ack",
            json!({"subscription": "orders.billing", "request_id": Uuid::from_u128(13),
                "member_id": member, "member_epoch": 2, "work_id": work,
                "lease_epoch": claimed_epoch,
                "cursor": body["cursor"], "positions": body["positions"],
                "quarantine": {progress_range.to_string(): ["cursor-9"]}}),
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

        // Incremental lease transfer: a second member joins, the work
        // moves to it with a bumped lease epoch — the old holder's grant
        // is fenced instantly and only the target can continue.
        let other = Uuid::from_u128(0x4444);
        let (status, body) = post(
            "join",
            json!({"subscription": "orders.billing", "request_id": Uuid::from_u128(16),
                "member_id": other}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["member_epoch"], json!(1));
        // Stale transfer evidence refuses rather than clobbering.
        let (status, _) = post(
            "transfer",
            json!({"subscription": "orders.billing", "request_id": Uuid::from_u128(17),
                "work_id": work, "member_id": member, "lease_epoch": 99,
                "to_member_id": other, "to_member_epoch": 1, "lease_ticks": 60}),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        let (status, body) = post(
            "transfer",
            json!({"subscription": "orders.billing", "request_id": Uuid::from_u128(17),
                "work_id": work, "member_id": member, "lease_epoch": reclaimed_epoch,
                "to_member_id": other, "to_member_epoch": 1, "lease_ticks": 60}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let moved = body["lease"].clone();
        assert_eq!(moved["member_id"], json!(other));
        assert_eq!(moved["member_epoch"], json!(1));
        assert_eq!(moved["lease_epoch"], json!(reclaimed_epoch + 1));
        // The old holder's grant is fenced: its fetch with the stale epoch
        // conflicts while the new holder's succeeds.
        let (status, _) = post(
            "fetch",
            json!({"subscription": "orders.billing", "request_id": Uuid::from_u128(18),
                "member_id": member, "member_epoch": 2, "work_id": work,
                "lease_epoch": reclaimed_epoch}),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        let (status, body) = post(
            "fetch",
            json!({"subscription": "orders.billing", "request_id": Uuid::from_u128(18),
                "member_id": other, "member_epoch": 1, "work_id": work,
                "lease_epoch": moved["lease_epoch"]}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");

        // SEEK SUBSCRIPTION is an operator reposition through the data
        // plane; it needs the admin credential and refuses while a member
        // holds an active work lease so in-flight grants cannot be
        // clobbered.
        let seek = |body: Value| {
            let client = client.clone();
            let url = format!("{}/v1/subscriptions/seek", endpoints[&nodes[0]]);
            async move {
                let response = client
                    .post(url)
                    .bearer_auth("this-is-a-long-development-api-key")
                    .json(&body)
                    .send()
                    .await
                    .unwrap();
                let status = response.status();
                let body: Value = response.json().await.unwrap_or_default();
                (status, body)
            }
        };
        let (status, _) = seek(json!({"subscription": "orders.billing",
            "request_id": Uuid::from_u128(40), "position": "timestamp",
            "timestamp_ns": "5"}))
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        let (status, _) = seek(json!({"subscription": "orders.billing",
            "request_id": Uuid::from_u128(40), "position": "cursor"}))
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let (status, _) = seek(json!({"subscription": "orders.billing",
            "request_id": Uuid::from_u128(40), "position": "timestamp"}))
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let (status, _) = seek(json!({"subscription": "orders.missing",
            "request_id": Uuid::from_u128(40), "position": "beginning"}))
        .await;
        assert!(status.is_client_error());
        // Releasing the moved lease unblocks the operator action.
        let (status, body) = post(
            "release",
            json!({"subscription": "orders.billing", "request_id": Uuid::from_u128(41),
                "work_id": work, "member_id": other, "member_epoch": 1,
                "lease_epoch": moved["lease_epoch"]}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        // The timestamp seek repositions the committed frontier so the next
        // delivery is the first record accepted at-or-after ingest time 5.
        let (status, body) = seek(json!({"subscription": "orders.billing",
            "request_id": Uuid::from_u128(42), "position": "timestamp",
            "timestamp_ns": "5"}))
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(
            body["positions"][progress_range.to_string()],
            json!("cursor-4")
        );
        // Replaying the same request identity returns the committed
        // reposition instead of seeking again.
        let (status, replay) = seek(json!({"subscription": "orders.billing",
            "request_id": Uuid::from_u128(42), "position": "timestamp",
            "timestamp_ns": "5"}))
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(replay["cursor"], body["cursor"]);
        assert_eq!(replay["positions"], body["positions"]);
        // The same identity with a different seek conflicts.
        let (status, _) = seek(json!({"subscription": "orders.billing",
            "request_id": Uuid::from_u128(42), "position": "beginning"}))
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        // The next member fetch delivers from the repositioned frontier.
        let (status, body) = post(
            "claim",
            json!({"subscription": "orders.billing", "request_id": Uuid::from_u128(43),
                "member_id": other, "member_epoch": 1, "work_id": work}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let sought_epoch = body["lease"]["lease_epoch"].as_u64().unwrap();
        let (status, body) = post(
            "fetch",
            json!({"subscription": "orders.billing", "request_id": Uuid::from_u128(44),
                "member_id": other, "member_epoch": 1, "work_id": work,
                "lease_epoch": sought_epoch, "limit": 4}),
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
            vec!["cursor-5", "cursor-6", "cursor-7", "cursor-8"]
        );
        // Release, then a beginning seek replays the whole history.
        let (status, _) = post(
            "release",
            json!({"subscription": "orders.billing", "request_id": Uuid::from_u128(45),
                "work_id": work, "member_id": other, "member_epoch": 1,
                "lease_epoch": sought_epoch}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{status}");
        let (status, _) = seek(json!({"subscription": "orders.billing",
            "request_id": Uuid::from_u128(46), "position": "beginning"}))
        .await;
        assert_eq!(status, StatusCode::OK, "{status}");

        // A cursor seek resolves the opaque record Cursor through the range
        // owner and repositions delivery immediately after its frame.
        let (status, body) = seek(json!({"subscription": "orders.billing",
            "request_id": Uuid::from_u128(47), "position": "cursor",
            "cursor": "cursor-8"}))
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(
            body["positions"][progress_range.to_string()],
            json!("cursor-8")
        );
        // The same request identity replays the committed reposition.
        let (status, replay) = seek(json!({"subscription": "orders.billing",
            "request_id": Uuid::from_u128(47), "position": "cursor",
            "cursor": "cursor-8"}))
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(replay["cursor"], body["cursor"]);
        // A different cursor under the same identity conflicts.
        let (status, _) = seek(json!({"subscription": "orders.billing",
            "request_id": Uuid::from_u128(47), "position": "cursor",
            "cursor": "cursor-9"}))
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        // A cursor that never committed is refused by the owner, not
        // translated into a guessed position.
        let (status, _) = seek(json!({"subscription": "orders.billing",
            "request_id": Uuid::from_u128(48), "position": "cursor",
            "cursor": "cursor-does-not-exist"}))
        .await;
        assert!(status.is_client_error());
        let (status, body) = post(
            "claim",
            json!({"subscription": "orders.billing", "request_id": Uuid::from_u128(49),
                "member_id": other, "member_epoch": 1, "work_id": work}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let sought_epoch = body["lease"]["lease_epoch"].as_u64().unwrap();
        let (status, body) = post(
            "fetch",
            json!({"subscription": "orders.billing", "request_id": Uuid::from_u128(50),
                "member_id": other, "member_epoch": 1, "work_id": work,
                "lease_epoch": sought_epoch, "limit": 8}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(
            body["records"]
                .as_array()
                .unwrap()
                .first()
                .and_then(|record| record["cursor"].as_str()),
            Some("cursor-9")
        );
        let (status, _) = post(
            "release",
            json!({"subscription": "orders.billing", "request_id": Uuid::from_u128(51),
                "work_id": work, "member_id": other, "member_epoch": 1,
                "lease_epoch": sought_epoch}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{status}");
        // Restoring the beginning position keeps the remaining legs
        // independent of the cursor reposition above.
        let (status, _) = seek(json!({"subscription": "orders.billing",
            "request_id": Uuid::from_u128(52), "position": "beginning"}))
        .await;
        assert_eq!(status, StatusCode::OK, "{status}");

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
        // The fetch exposes the delivering claim's durable attempt so members
        // can budget retries against redelivery.
        assert_eq!(body["delivery_attempt"], json!(fresh_epoch));
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

        // The admin progress view reports placement, committed frontier,
        // member state, and per-replica health in one read.
        let unauthorized = client
            .get(format!(
                "{}/v1/admin/subscriptions/progress?subscription=orders.billing",
                endpoints[&nodes[0]]
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
        let response = client
            .get(format!(
                "{}/v1/admin/subscriptions/progress?subscription=orders.billing",
                endpoints[&nodes[0]]
            ))
            .header("authorization", "Bearer this-is-a-long-development-api-key")
            .send()
            .await
            .unwrap();
        let status = response.status();
        let body: Value = response.json().await.unwrap();
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["subscription"], json!("orders.billing"));
        assert_eq!(
            body["assignment"]["owner"].as_str().unwrap(),
            assignment.owner.to_string()
        );
        assert_eq!(
            body["assignment"]["ownership_epoch"].as_u64().unwrap(),
            assignment.ownership_epoch
        );
        assert_eq!(body["assignment"]["replicas"].as_array().unwrap().len(), 3);
        assert!(body["committed"]["cursor"].is_string(), "{body}");
        assert_eq!(body["members"]["member_count"], json!(2));
        // Quarantine evidence carries forward through seeks: the member
        // ack marked cursor-9 as delivered-but-not-processed, and the
        // operator can see it while repositioning for reprocessing.
        assert_eq!(
            body["committed"]["quarantined"][progress_range.to_string()],
            json!(["cursor-9"])
        );
        // Lag answers the operator's first question: after the seek to
        // BEGINNING, all twelve seeded records are committed but undelivered.
        assert_eq!(body["lag"]["records_behind"], json!(12));
        assert_eq!(
            body["lag"]["ranges"][progress_range.to_string()]["behind"],
            json!(12)
        );
        assert_eq!(body["replicas"].as_array().unwrap().len(), 3);
        assert!(body["replicas"]
            .as_array()
            .unwrap()
            .iter()
            .all(|replica| replica["reachable"] == json!(true)));

        // Member-traffic demand attribution reports per-Subscription op
        // counters and a per-Domain rollup on the serving Node.
        let response = client
            .get(format!(
                "{}/v1/admin/demand/subscriptions",
                endpoints[&nodes[0]]
            ))
            .header("authorization", "Bearer this-is-a-long-development-api-key")
            .send()
            .await
            .unwrap();
        let status = response.status();
        let body: Value = response.json().await.unwrap();
        assert_eq!(status, StatusCode::OK, "{body}");
        let rows = body["subscriptions"].as_array().unwrap();
        let fresh = rows
            .iter()
            .find(|row| row["subscription"] == json!("orders.fresh"))
            .unwrap();
        let telemetry = &fresh["telemetry"];
        assert!(telemetry["requests"].as_u64().unwrap() >= 3, "{telemetry}");
        assert!(telemetry["joins"].as_u64().unwrap() >= 1, "{telemetry}");
        assert!(telemetry["claims"].as_u64().unwrap() >= 1, "{telemetry}");
        assert!(telemetry["fetches"].as_u64().unwrap() >= 1, "{telemetry}");
        assert_eq!(telemetry["fetch_records"], json!(4));
        assert_eq!(telemetry["last_op"], json!("fetch"));
        let orders = &body["domains"]["orders"];
        assert!(orders["subscriptions"].as_u64().unwrap() >= 3, "{orders}");
        assert!(orders["fetch_records"].as_u64().unwrap() >= 4, "{orders}");

        // Feed-level storage attribution lands on the storing replica:
        // member fetches drove committed range reads on the owning Node,
        // so the merged rows show served-read evidence for the Feed.
        // Replica-accept replication counters live on the internal HTTP
        // handler, which the in-process LoopbackReplicaTransport bypasses
        // by design — live Compose scripts (m17, m4 moves) cover that
        // path on a real Riverbed.
        let mut records_read = 0_u64;
        let mut read_bytes = 0_u64;
        let mut feed_seen = false;
        for node in &nodes {
            let response = client
                .get(format!("{}/v1/admin/demand/feeds", endpoints[node]))
                .header("authorization", "Bearer this-is-a-long-development-api-key")
                .send()
                .await
                .unwrap();
            let body: Value = response.json().await.unwrap();
            for row in body["feeds"].as_array().unwrap() {
                if row["feed"] == json!("orders.events") {
                    feed_seen = true;
                    records_read += row["records_read"].as_u64().unwrap_or(0);
                    read_bytes += row["read_bytes"].as_u64().unwrap_or(0);
                }
            }
        }
        assert!(feed_seen, "orders.events must appear in feed demand rows");
        assert!(
            records_read >= 4,
            "member fetches must attribute served records: {records_read}"
        );
        assert!(
            read_bytes >= 1,
            "served reads must attribute egress frame bytes: {read_bytes}"
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
            .header("authorization", "Bearer this-is-a-long-development-api-key")
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
                let response = client
                    .post(url)
                    .header("authorization", "Bearer this-is-a-long-development-api-key")
                    .json(&body)
                    .send()
                    .await
                    .unwrap();
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

        // The progress view now marks the dead owner's replica unreachable
        // while the placement names the recovered owner.
        let response = client
            .get(format!(
                "{}/v1/admin/subscriptions/progress?subscription=orders.billing",
                endpoints[&nodes[survivor_index]]
            ))
            .header("authorization", "Bearer this-is-a-long-development-api-key")
            .send()
            .await
            .unwrap();
        let status = response.status();
        let body: Value = response.json().await.unwrap();
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(
            body["assignment"]["owner"].as_str().unwrap(),
            recovered_assignment.owner.to_string()
        );
        let dead = body["replicas"]
            .as_array()
            .unwrap()
            .iter()
            .find(|replica| replica["node"].as_str().unwrap() == assignment.owner.to_string())
            .unwrap();
        assert_eq!(dead["reachable"], json!(false), "{body}");

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
                quarantined: BTreeMap::new(),
                quarantine_add: BTreeMap::new(),
                quarantine_remove: BTreeMap::new(),
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

    /// The M7 proof: Domain-wide membership and server-owned source
    /// Metadata behave correctly on an isolated multi-Node Riverbed —
    /// empty+populated Domain declaration, incremental attach, concurrent
    /// independent Subscriptions, in-flight Reader across rename,
    /// cross-Domain drain/reattach, fenced drop + same-name replacement,
    /// and reads that keep working with one replica unavailable.
    #[tokio::test]
    async fn domain_subscriptions_and_source_metadata_on_an_isolated_riverbed() {
        let directory = TempDir::new().unwrap();
        let store: Arc<dyn LogStore> =
            Arc::new(FileLogStore::open(directory.path().join("data")).unwrap());
        let nodes: Vec<crate::active_range::StorageNodeId> =
            ["storage-1", "storage-2", "storage-3"]
                .into_iter()
                .map(|name| crate::active_range::StorageNodeId::try_new(name).unwrap())
                .collect();
        let control = Arc::new(
            ControlController::open_with_storage_nodes(
                directory.path().join("catalog.json"),
                store.clone(),
                nodes.clone(),
            )
            .unwrap(),
        );
        control
            .execute("CREATE SPACE orders; CREATE SPACE other;")
            .await
            .unwrap();

        // Empty-Domain declaration is legal; no member Feeds yet.
        control
            .execute("CREATE SUBSCRIPTION orders.all FROM DOMAIN orders;")
            .await
            .unwrap();
        let described = control
            .execute("DESCRIBE SUBSCRIPTION orders.all;")
            .await
            .unwrap();
        assert_eq!(
            described.results[0].data["member_feeds"]
                .as_array()
                .unwrap()
                .len(),
            0
        );

        // Feeds attach in the same commit that creates them — new Feeds
        // start at their first retained record.
        control
            .execute("CREATE FEED orders.events; CREATE FEED orders.audit;")
            .await
            .unwrap();
        // Independent Subscriptions coexist: a second Domain source, an
        // exact-Feed one, and a named Reader for the delivery proof.
        control
            .execute("CREATE SUBSCRIPTION orders.tail FROM DOMAIN orders START AT NOW;")
            .await
            .unwrap();
        control
            .execute("CREATE SUBSCRIPTION orders.pinned FROM orders.events;")
            .await
            .unwrap();
        control
            .execute("CREATE READER orders.reader FROM orders.events;")
            .await
            .unwrap();
        control
            .execute("CREATE SUBSCRIPTION other.cover FROM DOMAIN other;")
            .await
            .unwrap();

        // Seed committed history on both Feeds' replicas (one replica root
        // per Node; range directories key by feed_id underneath).
        for feed_name in ["orders.events", "orders.audit"] {
            let feed = control.active_feed_by_name(feed_name).await.unwrap();
            let range = control.active_range_assignment(feed.feed_id).await.unwrap();
            let descriptor = ActiveRangeDescriptor {
                feed_id: feed.feed_id,
                range_id: range.range_id,
                generation: range.generation,
                ownership_epoch: range.ownership_epoch,
            };
            for replica in range.replicas.iter() {
                let index = nodes.iter().position(|node| node == replica).unwrap();
                crate::active_range::store::seed_committed_history_shaped(
                    &directory.path().join(format!("ranges-{index}")),
                    &descriptor,
                    4,
                    &|sequence| format!("key-{feed_name}-{sequence}").into_bytes(),
                    &|sequence| sequence as i64,
                    8,
                );
            }
        }

        let control_key = "this-is-a-long-control-plane-key".to_owned();
        let mut listeners = Vec::new();
        let mut endpoints = BTreeMap::new();
        for node in &nodes {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            endpoints.insert(
                node.clone(),
                format!("http://{}", listener.local_addr().unwrap()),
            );
            listeners.push(listener);
        }
        let mut servers = Vec::new();
        for (index, node) in nodes.iter().enumerate() {
            let listener = listeners.remove(0);
            listener.set_nonblocking(true).unwrap();
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
                store: store.clone(),
                membership,
                demand: DemandMetrics::default(),
                autoscaler: Arc::new(Mutex::new(AutoscaleController::default())),
                reader_pacing: crate::reader::ReaderPacingTable::default(),
                control: control.clone(),
                control_plane: None,
                progress_authority: control.clone(),
                replica_append: Some(Arc::new(ReplicaAppendService::new(
                    directory.path().join(format!("ranges-{index}")),
                    node.clone(),
                    control.clone(),
                ))),
                majority_append: None,
                subscription_progress: None,
                effect_journal: None,
                state_store_journal: None,
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
            let listener = tokio::net::TcpListener::from_std(listener).unwrap();
            servers.push(tokio::spawn(async move {
                axum::serve(listener, router(state)).await.unwrap();
            }));
        }
        let client = reqwest::Client::new();
        let admin = "this-is-a-long-development-api-key";

        // Member Feeds attached atomically: orders.all covers both orders
        // Feeds at Beginning; orders.tail honors its declared NOW start.
        let described = control
            .execute("DESCRIBE SUBSCRIPTION orders.all;")
            .await
            .unwrap();
        let members = described.results[0].data["member_feeds"]
            .as_array()
            .unwrap();
        assert_eq!(members.len(), 2);
        assert!(members
            .iter()
            .all(|member| member["state"] == "active" && member["start"]["kind"] == "beginning"));
        let described = control
            .execute("DESCRIBE SUBSCRIPTION orders.tail;")
            .await
            .unwrap();
        assert!(described.results[0].data["member_feeds"]
            .as_array()
            .unwrap()
            .iter()
            .all(|member| member["start"]["kind"] == "now"));

        // Every delivered record carries server-owned source Metadata:
        // last-segment feed_name, Domain-qualified full_feed_name, and the
        // immutable feed_id — separate from writer metadata.
        let events = control.active_feed_by_name("orders.events").await.unwrap();
        for feed_name in ["orders.events", "orders.audit"] {
            let feed = control.active_feed_by_name(feed_name).await.unwrap();
            let response = client
                .get(format!(
                    "{}/v1/feeds/records?feed={feed_name}&limit=10",
                    endpoints[&nodes[0]]
                ))
                .bearer_auth(admin)
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let records = response.json::<serde_json::Value>().await.unwrap();
            assert_eq!(records.as_array().unwrap().len(), 4);
            let source = &records[0]["source"];
            let short = feed_name.rsplit('.').next().unwrap();
            assert_eq!(source["feed_name"], short);
            assert_eq!(source["full_feed_name"], feed_name);
            assert_eq!(source["feed_id"], feed.feed_id.to_string());
        }

        // A named Reader in flight: open, fetch, then rename the Feed
        // within the Domain — Cursor stays stable, new deliveries report
        // the new names, membership is retained by immutable FeedId.
        let opened = client
            .post(format!("{}/v1/readers/open", endpoints[&nodes[1]]))
            .bearer_auth(admin)
            .json(&serde_json::json!({
                "request_id": Uuid::new_v4(),
                "reader": "orders.reader",
                "capacity": 8,
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(opened.status(), StatusCode::OK);
        let session_epoch = opened.json::<serde_json::Value>().await.unwrap()["session_epoch"]
            .as_u64()
            .unwrap();
        let fetched = client
            .post(format!("{}/v1/readers/fetch", endpoints[&nodes[1]]))
            .bearer_auth(admin)
            .json(&serde_json::json!({
                "request_id": Uuid::new_v4(),
                "reader": "orders.reader",
                "session_epoch": session_epoch,
                "limit": 2,
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(fetched.status(), StatusCode::OK);
        let records = fetched.json::<serde_json::Value>().await.unwrap()["records"]
            .as_array()
            .unwrap()
            .clone();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["source"]["full_feed_name"], "orders.events");

        control
            .execute("RENAME FEED orders.events TO orders.renamed;")
            .await
            .unwrap();
        let described = control
            .execute("DESCRIBE SUBSCRIPTION orders.all;")
            .await
            .unwrap();
        let member = described.results[0].data["member_feeds"]
            .as_array()
            .unwrap()
            .iter()
            .find(|member| member["feed_id"] == events.feed_id.to_string())
            .unwrap();
        assert_eq!(member["state"], "active");

        // The in-flight Reader keeps working under the new name — same
        // FeedId in source Metadata, no Cursor reset.
        let fetched = client
            .post(format!("{}/v1/readers/fetch", endpoints[&nodes[1]]))
            .bearer_auth(admin)
            .json(&serde_json::json!({
                "request_id": Uuid::new_v4(),
                "reader": "orders.reader",
                "session_epoch": session_epoch,
                "limit": 4,
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(fetched.status(), StatusCode::OK);
        let records = fetched.json::<serde_json::Value>().await.unwrap()["records"]
            .as_array()
            .unwrap()
            .clone();
        assert!(!records.is_empty());
        assert_eq!(records[0]["source"]["full_feed_name"], "orders.renamed");
        assert_eq!(records[0]["source"]["feed_name"], "renamed");
        assert_eq!(records[0]["source"]["feed_id"], events.feed_id.to_string());

        // Cross-Domain move: drains orders.all coverage (fenced, not
        // discarded) and attaches under other.cover as a new discovery.
        let audit_id = control
            .active_feed_by_name("orders.audit")
            .await
            .unwrap()
            .feed_id;
        control
            .execute("RENAME FEED orders.audit TO other.audit;")
            .await
            .unwrap();
        let described = control
            .execute("DESCRIBE SUBSCRIPTION orders.all;")
            .await
            .unwrap();
        let member = described.results[0].data["member_feeds"]
            .as_array()
            .unwrap()
            .iter()
            .find(|member| member["feed_id"] == audit_id.to_string())
            .unwrap();
        assert_eq!(member["state"], "draining");
        let described = control
            .execute("DESCRIBE SUBSCRIPTION other.cover;")
            .await
            .unwrap();
        let member = described.results[0].data["member_feeds"]
            .as_array()
            .unwrap()
            .iter()
            .find(|member| member["feed_id"] == audit_id.to_string())
            .unwrap();
        assert_eq!(member["state"], "active");

        // One replica Node is unavailable: reads on the other Feed still
        // succeed through the live owner plus a corroborating replica.
        let events_range = control
            .active_range_assignment(events.feed_id)
            .await
            .unwrap();
        let kill = nodes
            .iter()
            .position(|node| node != &events_range.owner)
            .unwrap();
        servers[kill].abort();
        let survivor = (0..3).find(|index| *index != kill).unwrap();
        let response = client
            .get(format!(
                "{}/v1/feeds/records?feed=orders.renamed&limit=10",
                endpoints[&nodes[survivor]]
            ))
            .bearer_auth(admin)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .json::<serde_json::Value>()
                .await
                .unwrap()
                .as_array()
                .unwrap()
                .len(),
            4
        );

        // Fenced drop: a covered Feed cannot be dropped under a Domain
        // Subscription; an explicit subscription drop is the disposition,
        // and a same-name replacement gets fresh progress via a new FeedId.
        assert!(control.execute("DROP FEED orders.renamed;").await.is_err());
        // Every attached consumer — covering Domain Subscriptions, the
        // exact-Feed one, and the in-flight Reader — is an explicit
        // disposition before the Feed may drop.
        control
            .execute("DROP SUBSCRIPTION orders.all;")
            .await
            .unwrap();
        assert!(control.execute("DROP FEED orders.renamed;").await.is_err());
        control
            .execute("DROP SUBSCRIPTION orders.tail; DROP SUBSCRIPTION orders.pinned; DROP READER orders.reader;")
            .await
            .unwrap();
        control.execute("DROP FEED orders.renamed;").await.unwrap();
        // Dropped names tombstone — a same-name replacement cannot
        // silently alias the old Feed's identity or progress.
        assert!(control
            .execute("CREATE FEED orders.renamed;")
            .await
            .is_err());
        // A fresh Subscription over the Domain starts with no members (the
        // only orders Feed is dropped; audit moved away); moving audit back
        // reattaches it as a fresh Active member under the new
        // Subscription — the drained row died with the dropped one.
        control
            .execute("CREATE SUBSCRIPTION orders.cover2 FROM DOMAIN orders;")
            .await
            .unwrap();
        control
            .execute("RENAME FEED other.audit TO orders.audit;")
            .await
            .unwrap();
        let described = control
            .execute("DESCRIBE SUBSCRIPTION orders.cover2;")
            .await
            .unwrap();
        let members = described.results[0].data["member_feeds"]
            .as_array()
            .unwrap();
        assert_eq!(members.len(), 1);
        assert!(members.iter().any(|member| {
            member["feed_id"] == audit_id.to_string() && member["state"] == "active"
        }));
        // ... while the move drained its coverage under other.cover.
        let described = control
            .execute("DESCRIBE SUBSCRIPTION other.cover;")
            .await
            .unwrap();
        let member = described.results[0].data["member_feeds"]
            .as_array()
            .unwrap()
            .iter()
            .find(|member| member["feed_id"] == audit_id.to_string())
            .unwrap();
        assert_eq!(member["state"], "draining");

        // Membership state survives catalog restart.
        let reopened = ControlController::open_with_storage_nodes(
            directory.path().join("catalog.json"),
            store.clone(),
            nodes.clone(),
        )
        .unwrap();
        let described = reopened
            .execute("DESCRIBE SUBSCRIPTION orders.cover2;")
            .await
            .unwrap();
        assert!(!described.results[0].data["member_feeds"]
            .as_array()
            .unwrap()
            .is_empty());
    }

    struct LoopbackReplicaTransport {
        services: Arc<
            BTreeMap<
                crate::active_range::StorageNodeId,
                Arc<crate::active_range::ReplicaAppendService>,
            >,
        >,
    }

    #[async_trait::async_trait]
    impl crate::active_range::ReplicaTransport for LoopbackReplicaTransport {
        async fn append(
            &self,
            replica: &crate::active_range::StorageNodeId,
            request: crate::active_range::ReplicaAppendRequest,
        ) -> Result<
            crate::active_range::ReplicaAppendAccepted,
            crate::active_range::ReplicaTransportError,
        > {
            self.services[replica]
                .append(request)
                .await
                .map_err(|error| crate::active_range::ReplicaTransportError {
                    message: error.message,
                    retryable: error.retryable,
                })
        }

        async fn commit(
            &self,
            replica: &crate::active_range::StorageNodeId,
            request: crate::active_range::ReplicaCommitRequest,
        ) -> Result<
            crate::active_range::ReplicaCommitAccepted,
            crate::active_range::ReplicaTransportError,
        > {
            self.services[replica]
                .commit(request)
                .await
                .map_err(|error| crate::active_range::ReplicaTransportError {
                    message: error.message,
                    retryable: error.retryable,
                })
        }

        async fn discard_pending(
            &self,
            replica: &crate::active_range::StorageNodeId,
            request: crate::active_range::ReplicaDiscardPendingRequest,
        ) -> Result<
            crate::active_range::ReplicaDiscardPendingResponse,
            crate::active_range::ReplicaTransportError,
        > {
            self.services[replica]
                .discard_pending(request)
                .await
                .map_err(|error| crate::active_range::ReplicaTransportError {
                    message: error.message,
                    retryable: error.retryable,
                })
        }
    }

    #[tokio::test]
    async fn pending_outputs_hold_and_release_through_the_owner_endpoints() {
        let directory = TempDir::new().unwrap();
        let (mut state, plane, control, _replica) = internal_replica_test_state(&directory).await;
        let feed = control.active_feed_by_name("orders.events").await.unwrap();
        let assignment = control.active_range_assignment(feed.feed_id).await.unwrap();
        let services: BTreeMap<_, _> = assignment
            .replicas
            .iter()
            .map(|node| {
                (
                    node.clone(),
                    Arc::new(crate::active_range::ReplicaAppendService::new(
                        directory.path().join("owner-ranges").join(node.as_str()),
                        node.clone(),
                        control.clone(),
                    )),
                )
            })
            .collect();
        let owner = assignment.owner.clone();
        let services = Arc::new(services);
        state.storage_node_id = Some(owner.clone());
        state.replica_append = Some(services[&owner].clone());
        state.majority_append = Some(Arc::new(
            crate::active_range::MajorityAppendCoordinator::new(
                services[&owner].clone(),
                control.clone(),
                Arc::new(LoopbackReplicaTransport {
                    services: services.clone(),
                }),
            ),
        ));
        let app = router(state);

        let effect = Uuid::from_u128(910);
        let subscription = Uuid::from_u128(911);
        let append_body = |writer: u128, sequence: u64, pending: Option<serde_json::Value>| {
            let mut body = serde_json::json!({
                "request_id": Uuid::from_u128(912 + writer),
                "feed": "orders.events",
                "writer_session_id": Uuid::from_u128(writer),
                "writer_epoch": 1,
                "sequence": sequence,
                "key_base64": base64::engine::general_purpose::STANDARD.encode(b"customer-1"),
                "payload_base64": base64::engine::general_purpose::STANDARD.encode(b"held"),
            });
            if let Some(pending) = pending {
                body["pending_effect"] = pending;
            }
            body
        };

        // The pending append commits durably on the quorum but stays
        // fenced: the owner reports its placement, committed reads hide it.
        let request = Request::builder()
            .method("POST")
            .uri("/internal/active-range/owner/append")
            .header("content-type", "application/json")
            .header(
                "x-whitewater-control-key",
                "this-is-a-long-control-plane-key",
            )
            .body(Body::from(
                serde_json::to_vec(&append_body(
                    913,
                    1,
                    Some(serde_json::json!({
                        "effect_id": effect,
                        "subscription_id": subscription,
                    })),
                ))
                .unwrap(),
            ))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let reply: InternalOwnerAppendResponse = serde_json::from_slice(&bytes).unwrap();
        assert!(reply.result.is_some());
        assert_eq!(reply.range_id, Some(assignment.range_id));
        assert_eq!(reply.position, Some(1));
        assert_eq!(
            services[&owner]
                .pending_effect(&assignment)
                .await
                .unwrap()
                .unwrap()
                .effect_id,
            effect
        );
        assert!(services[&owner]
            .read_staged_committed(&assignment, None, 10)
            .await
            .unwrap()
            .is_empty());

        // A foreign effect is refused and the reply carries the occupant.
        let request = Request::builder()
            .method("POST")
            .uri("/internal/active-range/owner/append")
            .header("content-type", "application/json")
            .header(
                "x-whitewater-control-key",
                "this-is-a-long-control-plane-key",
            )
            .body(Body::from(
                serde_json::to_vec(&append_body(
                    915,
                    1,
                    Some(serde_json::json!({
                        "effect_id": Uuid::from_u128(914),
                        "subscription_id": subscription,
                    })),
                ))
                .unwrap(),
            ))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let reply: InternalOwnerAppendResponse = serde_json::from_slice(&bytes).unwrap();
        assert!(reply.result.is_none());
        assert_eq!(reply.pending_effect.unwrap().effect_id, effect);

        // The public append path refuses the internal pending flag.
        let public = Request::builder()
            .method("POST")
            .uri("/v1/feeds/append")
            .header("content-type", "application/json")
            .header("authorization", "Bearer this-is-a-long-development-api-key")
            .body(Body::from(
                serde_json::to_vec(&append_body(
                    917,
                    1,
                    Some(serde_json::json!({
                        "effect_id": effect,
                        "subscription_id": subscription,
                    })),
                ))
                .unwrap(),
            ))
            .unwrap();
        let response = app.clone().oneshot(public).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        // The release commits the held tail across the quorum.
        let request = Request::builder()
            .method("POST")
            .uri("/internal/active-range/owner/release-pending")
            .header("content-type", "application/json")
            .header(
                "x-whitewater-control-key",
                "this-is-a-long-control-plane-key",
            )
            .body(Body::from(
                serde_json::to_vec(&serde_json::json!({
                    "feed_id": feed.feed_id,
                    "range_id": assignment.range_id,
                    "generation": assignment.generation,
                    "ownership_epoch": assignment.ownership_epoch,
                    "append_owner": owner,
                    "effect_id": effect,
                    "subscription_id": subscription,
                }))
                .unwrap(),
            ))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let reply: OwnerReleaseResponse = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            reply.outcome,
            Some(crate::active_range::PendingReleaseOutcome::Released {
                commit_position: crate::active_range::CommitPosition::new(1),
            })
        );
        for service in services.values() {
            let status = service
                .recovery_status_for_assignment(&assignment)
                .await
                .unwrap();
            assert_eq!(
                status.committed,
                crate::active_range::CommitPosition::new(1)
            );
            assert_eq!(service.pending_effect(&assignment).await.unwrap(), None);
            assert_eq!(
                service
                    .read_staged_committed(&assignment, None, 10)
                    .await
                    .unwrap()
                    .len(),
                1
            );
        }
        plane.raft().shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn pending_outputs_discard_through_the_owner_endpoint() {
        let directory = TempDir::new().unwrap();
        let (mut state, plane, control, _replica) = internal_replica_test_state(&directory).await;
        let feed = control.active_feed_by_name("orders.events").await.unwrap();
        let assignment = control.active_range_assignment(feed.feed_id).await.unwrap();
        let services: BTreeMap<_, _> = assignment
            .replicas
            .iter()
            .map(|node| {
                (
                    node.clone(),
                    Arc::new(crate::active_range::ReplicaAppendService::new(
                        directory.path().join("owner-ranges").join(node.as_str()),
                        node.clone(),
                        control.clone(),
                    )),
                )
            })
            .collect();
        let owner = assignment.owner.clone();
        let services = Arc::new(services);
        state.storage_node_id = Some(owner.clone());
        state.replica_append = Some(services[&owner].clone());
        state.majority_append = Some(Arc::new(
            crate::active_range::MajorityAppendCoordinator::new(
                services[&owner].clone(),
                control.clone(),
                Arc::new(LoopbackReplicaTransport {
                    services: services.clone(),
                }),
            ),
        ));
        let app = router(state);

        let effect = Uuid::from_u128(920);
        let subscription = Uuid::from_u128(921);
        let append_body = serde_json::json!({
            "request_id": Uuid::from_u128(922),
            "feed": "orders.events",
            "writer_session_id": Uuid::from_u128(923),
            "writer_epoch": 1,
            "sequence": 1,
            "key_base64": base64::engine::general_purpose::STANDARD.encode(b"customer-1"),
            "payload_base64": base64::engine::general_purpose::STANDARD.encode(b"held"),
            "pending_effect": {
                "effect_id": effect,
                "subscription_id": subscription,
            },
        });
        let request = Request::builder()
            .method("POST")
            .uri("/internal/active-range/owner/append")
            .header("content-type", "application/json")
            .header(
                "x-whitewater-control-key",
                "this-is-a-long-control-plane-key",
            )
            .body(Body::from(serde_json::to_vec(&append_body).unwrap()))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(services[&owner]
            .pending_effect(&assignment)
            .await
            .unwrap()
            .is_some());

        // A discard naming a different effect reports the occupant and
        // leaves the held tail untouched.
        let discard = |effect_id: Uuid| {
            serde_json::json!({
                "feed_id": feed.feed_id,
                "range_id": assignment.range_id,
                "generation": assignment.generation,
                "ownership_epoch": assignment.ownership_epoch,
                "append_owner": owner,
                "effect_id": effect_id,
                "subscription_id": subscription,
            })
        };
        let request = Request::builder()
            .method("POST")
            .uri("/internal/active-range/owner/discard-pending")
            .header("content-type", "application/json")
            .header(
                "x-whitewater-control-key",
                "this-is-a-long-control-plane-key",
            )
            .body(Body::from(
                serde_json::to_vec(&discard(Uuid::from_u128(924))).unwrap(),
            ))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let reply: OwnerDiscardResponse = serde_json::from_slice(&bytes).unwrap();
        match reply.outcome {
            Some(crate::active_range::PendingDiscardOutcome::Occupied(marker)) => {
                assert_eq!(marker.effect_id, effect);
            }
            other => panic!("expected Occupied, got {other:?}"),
        }

        // The owning effect's discard truncates the tail across the
        // quorum; nothing became visible and markers are gone.
        let request = Request::builder()
            .method("POST")
            .uri("/internal/active-range/owner/discard-pending")
            .header("content-type", "application/json")
            .header(
                "x-whitewater-control-key",
                "this-is-a-long-control-plane-key",
            )
            .body(Body::from(serde_json::to_vec(&discard(effect)).unwrap()))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let reply: OwnerDiscardResponse = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            reply.outcome,
            Some(crate::active_range::PendingDiscardOutcome::Discarded { dropped_frames: 1 })
        );
        for service in services.values() {
            assert_eq!(service.pending_effect(&assignment).await.unwrap(), None);
            assert!(service
                .read_staged_committed(&assignment, None, 10)
                .await
                .unwrap()
                .is_empty());
        }

        // A retried discard reports the tail already resolved.
        let request = Request::builder()
            .method("POST")
            .uri("/internal/active-range/owner/discard-pending")
            .header("content-type", "application/json")
            .header(
                "x-whitewater-control-key",
                "this-is-a-long-control-plane-key",
            )
            .body(Body::from(serde_json::to_vec(&discard(effect)).unwrap()))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let reply: OwnerDiscardResponse = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            reply.outcome,
            Some(crate::active_range::PendingDiscardOutcome::NotPending)
        );

        // The freed Range accepts ordinary appends at the next position.
        let next = serde_json::json!({
            "request_id": Uuid::from_u128(925),
            "feed": "orders.events",
            "writer_session_id": Uuid::from_u128(926),
            "writer_epoch": 1,
            "sequence": 1,
            "key_base64": base64::engine::general_purpose::STANDARD.encode(b"customer-1"),
            "payload_base64": base64::engine::general_purpose::STANDARD.encode(b"next"),
        });
        let request = Request::builder()
            .method("POST")
            .uri("/internal/active-range/owner/append")
            .header("content-type", "application/json")
            .header(
                "x-whitewater-control-key",
                "this-is-a-long-control-plane-key",
            )
            .body(Body::from(serde_json::to_vec(&next).unwrap()))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let reply: InternalOwnerAppendResponse = serde_json::from_slice(&bytes).unwrap();
        assert!(reply.result.is_some());
        assert_eq!(reply.position, Some(1));
        plane.raft().shutdown().await.unwrap();
    }

    #[test]
    fn writer_sequence_boundary_parses_gap_and_stale_refusals() {
        let gap = ApiError {
            status: StatusCode::CONFLICT,
            message: "Writer sequence 31 has a gap; expected 22".to_owned(),
            code: Some("writer.sequence_gap"),
        };
        assert_eq!(writer_sequence_boundary(&gap), Some(22));
        let stale = ApiError {
            status: StatusCode::CONFLICT,
            message: "Writer sequence 12 is stale; latest accepted sequence is 13".to_owned(),
            code: Some("writer.stale_sequence"),
        };
        assert_eq!(writer_sequence_boundary(&stale), Some(14));
        // Forwarded refusals can lose the code field; the message shape
        // alone must still recover the boundary.
        let untagged_gap = ApiError {
            status: StatusCode::CONFLICT,
            message: "Writer sequence 31 has a gap; expected 22".to_owned(),
            code: None,
        };
        assert_eq!(writer_sequence_boundary(&untagged_gap), Some(22));
        let untagged_stale = ApiError {
            status: StatusCode::CONFLICT,
            message: "Writer sequence 12 is stale; latest accepted sequence is 13".to_owned(),
            code: None,
        };
        assert_eq!(writer_sequence_boundary(&untagged_stale), Some(14));
        // Unrelated refusals and malformed tails carry no boundary.
        let unrelated = ApiError {
            status: StatusCode::CONFLICT,
            message: "Writer epoch is stale".to_owned(),
            code: Some("writer.stale_epoch"),
        };
        assert_eq!(writer_sequence_boundary(&unrelated), None);
        let malformed = ApiError {
            status: StatusCode::CONFLICT,
            message: "Writer sequence 9 has a gap; expected soon".to_owned(),
            code: Some("writer.sequence_gap"),
        };
        assert_eq!(writer_sequence_boundary(&malformed), None);
    }

    #[test]
    fn window_sentinel_refusals_counts_sentinel_pane_deltas() {
        let entry = |key: Vec<u8>, count_delta: u64| crate::effect::EffectStateMutation {
            store_id: Uuid::nil(),
            mutation_id: Uuid::nil(),
            op: crate::state_store::StateStoreMutationOp::AccumulateRows {
                entries: vec![crate::state_store::AccumulateRowEntry {
                    key,
                    count_delta,
                    sum_delta: 0,
                    named: Vec::new(),
                }],
            },
        };
        let group =
            crate::pipe::window_group_key_bytes(&crate::index::IndexValue::Text("acme".to_owned()))
                .unwrap_or_else(|| vec![2]);
        let real_pane = entry(crate::pipe::window_pane_row_key(&group, 7), 3);
        let keycap = entry(
            crate::pipe::window_pane_row_key(&group, crate::pipe::WINDOW_LATE_PANE),
            5,
        );
        let malformed = crate::effect::EffectStateMutation {
            store_id: Uuid::nil(),
            mutation_id: Uuid::nil(),
            op: crate::state_store::StateStoreMutationOp::PutRow {
                key: b"not-a-pane-row".to_vec(),
                payload: Vec::new(),
                named: Vec::new(),
            },
        };
        // Only sentinel-pane accumulate deltas count: real pane rows and
        // non-accumulate ops carry no refused records.
        assert_eq!(window_sentinel_refusals(&[real_pane, keycap, malformed]), 5);
    }
}
