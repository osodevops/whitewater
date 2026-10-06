use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

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
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::{
    active_range::{
        stage_candidate_ranges_local, stage_merged_range_local, ActiveRangeAssignment,
        AppendIdentity, CandidateSplitStagingResult, CommitPosition, ControlPlaneFollowerMove,
        FollowerMoveControl, FollowerMoveCopyResult, KeyToken, MajorityAppendCoordinator,
        MajorityAppendError, MergeStagingResult, OwnerMoveEvidence, RangeId, RangePosition,
        ReadReplicaEvidence, RepairExportRequest, RepairExportResponse, RepairFrame,
        ReplicaAppendRequest, ReplicaAppendResponse, ReplicaAppendService, ReplicaCommitRequest,
        ReplicaCommitResponse, ReplicaProgressRequest, ReplicaProgressResponse,
        ReplicaReconcileRequest, ReplicaReconcileResponse, StorageNodeId, StoredRangeFrame,
        MAX_COMMITTED_READ_BYTES, MAX_REPLICA_FRAME_BASE64_BYTES,
    },
    admin::{AdminAuthError, AdminAuthenticator, CommandBatchRequest, WclRequest},
    autoscale::{AutoscaleController, AutoscalePolicy, ScaleDecision},
    codec::{decode_record, encode_record, MAX_FRAME_BYTES},
    control::{
        ControlController, ControlError, RangeMergePlan, RangeMovePlan, RangeMoveStage,
        RangeOwnerMovePlan, RangeSplitPlan, ReplicatedCommand,
    },
    control_plane::{
        ControlNodeId, ControlPlane, ControlPlaneError, ControlTypeConfig, FullSnapshotRequest,
        InternalCommandsRequest, InternalCommandsResponse, InternalWriteResponse,
    },
    demand::{DemandMetrics, DemandSnapshot},
    domain::{AppendInput, CursorRecord, StorageStats, StoredRecord},
    membership::{JoinResponse, MemberAnnouncement, MemberView, MembershipService},
    reader::{
        SubscriptionCommitRequest, SubscriptionPrepareRequest, SubscriptionProgressError,
        SubscriptionProgressReplicaService,
    },
    storage::{LogStore, StorageError},
    writer::WriterServerFeedback,
};

#[derive(Clone)]
pub struct AppState {
    pub store: Arc<dyn LogStore>,
    pub membership: Arc<MembershipService>,
    pub demand: DemandMetrics,
    pub autoscaler: Arc<Mutex<AutoscaleController>>,
    pub control: Arc<ControlController>,
    pub control_plane: Option<Arc<ControlPlane>>,
    pub replica_append: Option<Arc<ReplicaAppendService>>,
    pub subscription_progress: Option<Arc<SubscriptionProgressReplicaService>>,
    pub majority_append: Option<Arc<MajorityAppendCoordinator>>,
    pub storage_node_id: Option<StorageNodeId>,
    pub control_endpoints: Arc<BTreeMap<StorageNodeId, String>>,
    pub internal_key: Option<String>,
    pub internal_http: reqwest::Client,
    pub admin_auth: AdminAuthenticator,
}

pub fn router(state: AppState) -> Router {
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
    let subscription_prepare_route = post(subscription_prepare_local)
        .layer(DefaultBodyLimit::max(512 * 1024))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            authorize_replica_append,
        ));
    let subscription_commit_route = post(subscription_commit_local)
        .layer(DefaultBodyLimit::max(512 * 1024))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            authorize_replica_append,
        ));
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
    Router::new()
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
        .route("/v1/feeds/records", get(read_feed_records))
        .route("/v1/admin/wcl", post(execute_admin_wcl))
        .route("/v1/admin/commands", post(execute_admin_commands))
        .route("/v1/admin/ranges/split", post(admin_split_range))
        .route("/v1/admin/ranges/merge", post(admin_merge_ranges))
        .route("/v1/admin/ranges/move-follower", post(admin_move_follower))
        .route("/v1/admin/ranges/move-owner", post(admin_move_owner))
        .route("/v1/admin/control-plane", get(control_plane_status))
        .route("/v1/control/execute", post(execute_admin_wcl))
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
            "/internal/subscription-progress/prepare",
            subscription_prepare_route,
        )
        .route(
            "/internal/subscription-progress/commit",
            subscription_commit_route,
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
        .route("/v1/node/metrics", get(node_metrics))
        .route("/v1/cluster/members", get(cluster_members))
        .route(
            "/v1/cluster/autoscale/recommend",
            post(autoscale_recommendation),
        )
        .route("/v1/cluster/join", post(cluster_join))
        .route("/v1/cluster/leave", post(cluster_leave))
        .with_state(state)
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
        });
    }
    Ok(control_plane)
}

async fn authorize_replica_append(
    State(state): State<AppState>,
    request: Request<Body>,
    next: Next,
) -> Result<Response, ApiError> {
    authorize_internal(&state, request.headers())?;
    Ok(next.run(request).await)
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

fn subscription_progress_api_error(error: SubscriptionProgressError) -> ApiError {
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
    }
}

async fn subscription_prepare_local(
    State(state): State<AppState>,
    Json(request): Json<SubscriptionPrepareRequest>,
) -> Result<Json<crate::reader::SubscriptionPrepareVote>, ApiError> {
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
    Json(request): Json<SubscriptionCommitRequest>,
) -> Result<Json<crate::reader::SubscriptionProgressMutation>, ApiError> {
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
        if let Some(endpoint) = state.control_endpoints.get(node) {
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
        let endpoint = state.control_endpoints.get(&node).ok_or_else(|| {
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
        let attempt = match state.control_endpoints.get(node) {
            Some(endpoint) => {
                match reconcile_split_node(
                    &state,
                    endpoint,
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
                            endpoint,
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
        if let Some(endpoint) = state.control_endpoints.get(node) {
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
        let endpoint = state.control_endpoints.get(node).ok_or_else(|| {
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
        evidence.push(response.result.ok_or_else(|| {
            ApiError::unavailable(
                response
                    .error
                    .unwrap_or_else(|| "merge staging failed".to_owned()),
            )
        })?);
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
        .get(&plan.source_assignment.owner)
        .ok_or_else(|| ApiError::unavailable("source owner endpoint unavailable"))?;
    let key = state
        .internal_key
        .as_ref()
        .ok_or_else(|| ApiError::unavailable("internal credential unavailable"))?;
    let first = fetch_move_export(
        &state,
        endpoint,
        key,
        MoveExportRequest {
            plan_id: plan.plan_id,
            range_id: request.range_id,
            after: None,
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
    let prior_commit = service
        .recovery_status_for_assignment(&plan.candidate_assignment)
        .await
        .map_err(|error| ApiError::unavailable(error.to_string()))?
        .committed;
    if prior_commit > captured {
        return Err(ApiError::unavailable(
            "replacement replica is ahead of source CommitPosition",
        ));
    }
    let mut after = None;
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
                endpoint,
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
        endpoint,
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
        .get(&plan.source_assignment.owner)
        .ok_or_else(|| ApiError::unavailable("source endpoint unavailable"))?;
    let target_endpoint = state
        .control_endpoints
        .get(&plan.replacement_replica)
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
        unfreeze_move(&state, source_endpoint, key, &plan_request).await?;
        return Ok(Json(
            json!({ "status": "activated", "assignment": plan.candidate_assignment }),
        ));
    }
    post_move_request(
        &state,
        target_endpoint,
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
        source_endpoint,
        key,
        "/internal/active-range/move/freeze",
        &plan_request,
    )
    .await?;
    let boundary: CommitPosition = match serde_json::from_value(frozen["source_commit"].clone()) {
        Ok(commit) => commit,
        Err(error) => {
            unfreeze_move(&state, source_endpoint, key, &plan_request).await?;
            return Err(ApiError::unavailable(error.to_string()));
        }
    };
    let final_copy = post_move_request(
        &state,
        target_endpoint,
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
                unfreeze_move(&state, source_endpoint, key, &plan_request).await?;
                return Err(ApiError::unavailable(error.to_string()));
            }
        },
        Err(error) => {
            unfreeze_move(&state, source_endpoint, key, &plan_request).await?;
            return Err(error);
        }
    };
    if !copied.ready || copied.source_commit != boundary || copied.target_commit != boundary {
        unfreeze_move(&state, source_endpoint, key, &plan_request).await?;
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
    unfreeze_move(&state, source_endpoint, key, &plan_request).await?;
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
        .get(&plan.source_assignment.owner)
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
            endpoint,
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
        endpoint,
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
        .get(&plan.source_assignment.owner)
        .ok_or_else(|| ApiError::unavailable("source owner endpoint unavailable"))?;
    let target_endpoint = state
        .control_endpoints
        .get(&plan.candidate_assignment.owner)
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
        unfreeze_owner_move(&state, source_endpoint, key, &plan_request).await?;
        unfreeze_owner_move(&state, target_endpoint, key, &plan_request).await?;
        return Ok(Json(
            json!({ "status": "activated", "assignment": plan.candidate_assignment }),
        ));
    }
    let frozen = match post_move_request(
        &state,
        source_endpoint,
        key,
        "/internal/active-range/owner-move/freeze",
        &plan_request,
    )
    .await
    {
        Ok(frozen) => frozen,
        Err(error) => {
            unfreeze_owner_move(&state, source_endpoint, key, &plan_request).await?;
            return Err(error);
        }
    };
    let boundary: CommitPosition = match serde_json::from_value(frozen["source_commit"].clone()) {
        Ok(commit) => commit,
        Err(error) => {
            unfreeze_owner_move(&state, source_endpoint, key, &plan_request).await?;
            return Err(ApiError::unavailable(error.to_string()));
        }
    };
    let evidence = post_move_request(
        &state,
        target_endpoint,
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
                unfreeze_owner_move(&state, source_endpoint, key, &plan_request).await?;
                unfreeze_owner_move(&state, target_endpoint, key, &plan_request).await?;
                return Err(ApiError::unavailable(error.to_string()));
            }
        },
        Err(error) => {
            unfreeze_owner_move(&state, source_endpoint, key, &plan_request).await?;
            unfreeze_owner_move(&state, target_endpoint, key, &plan_request).await?;
            return Err(error);
        }
    };
    if !evidence.ready || evidence.source_commit != boundary || evidence.target_commit != boundary {
        unfreeze_owner_move(&state, source_endpoint, key, &plan_request).await?;
        unfreeze_owner_move(&state, target_endpoint, key, &plan_request).await?;
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
    unfreeze_owner_move(&state, source_endpoint, key, &plan_request).await?;
    unfreeze_owner_move(&state, target_endpoint, key, &plan_request).await?;
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
    let local = state.storage_node_id.as_ref().ok_or_else(|| {
        ApiError::unavailable("this Node is not configured for Active Range routing")
    })?;
    if &assignment.owner == local {
        return owner_append_local(&state, request).await.map(Json);
    }
    let endpoint = state
        .control_endpoints
        .get(&assignment.owner)
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
    response.result.map(Json).ok_or_else(|| ApiError {
        status: if response.retryable {
            StatusCode::SERVICE_UNAVAILABLE
        } else {
            StatusCode::CONFLICT
        },
        message: response
            .error
            .unwrap_or_else(|| "Append Owner returned no result".to_owned()),
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
                let endpoint = state.control_endpoints.get(node)?;
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
            .read_owned_range_page(&request.assignment, request.after, expected_commit)
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
        .get(&request.assignment.owner)
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
    response
        .error_for_status()
        .map_err(|error| ApiError::unavailable(error.to_string()))?
        .json::<ReadRangePageResponse>()
        .await
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
                page_limit: None,
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
            let frame = page.frames.into_iter().next().ok_or_else(|| {
                ApiError::unavailable("current owner omitted a committed range frame")
            })?;
            let next = after_position.map_or(1, |position: RangePosition| {
                position.value().saturating_add(1)
            });
            if frame.position.value() != next || frame.position.value() > page.committed.value() {
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
}

impl ApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    fn unavailable(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            message: message.into(),
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
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(json!({ "error": self.message }))).into_response()
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
            store::seed_committed_history, ActiveRangeDescriptor, OwnershipEpoch, RangeGeneration,
            ReplicaSet,
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
            control,
            control_plane: None,
            majority_append: None,
            subscription_progress: None,
            storage_node_id: Some(assignment.owner),
            control_endpoints: Arc::new(BTreeMap::new()),
            internal_key: None,
            internal_http: reqwest::Client::new(),
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
            control,
            control_plane: None,
            replica_append: None,
            majority_append: None,
            subscription_progress: None,
            storage_node_id: None,
            control_endpoints: Arc::new(BTreeMap::new()),
            internal_key: None,
            internal_http: reqwest::Client::new(),
            admin_auth: AdminAuthenticator::new(Some(
                "this-is-a-long-development-api-key".to_owned(),
            ))
            .unwrap(),
        })
    }

    async fn internal_replica_test_router(
        directory: &TempDir,
    ) -> (
        Router,
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
            router(AppState {
                store,
                membership,
                demand: DemandMetrics::default(),
                autoscaler: Arc::new(Mutex::new(AutoscaleController::default())),
                control: control.clone(),
                control_plane: Some(control_plane.clone()),
                replica_append: Some(replica_append),
                majority_append: None,
                subscription_progress: Some(subscription_progress),
                storage_node_id: Some(
                    crate::active_range::StorageNodeId::try_new("control-1").unwrap(),
                ),
                control_endpoints: Arc::new(BTreeMap::new()),
                internal_key: Some("this-is-a-long-control-plane-key".to_owned()),
                internal_http: reqwest::Client::new(),
                admin_auth: AdminAuthenticator::new(Some(
                    "this-is-a-long-development-api-key".to_owned(),
                ))
                .unwrap(),
            }),
            control_plane,
            control,
            subscription_replica,
        )
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
        let vote: crate::reader::SubscriptionPrepareVote = serde_json::from_slice(&bytes).unwrap();
        assert!(replica
            .local_committed(subscription.subscription_id)
            .unwrap()
            .is_none());
        let evidence = json!({"votes": [[assignment.owner, vote.digest], [follower, vote.digest]],
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
        };
        let vote = crate::reader::SubscriptionProgressTransport::prepare(
            &transport,
            &local,
            mutation.clone(),
        )
        .await
        .unwrap();
        assert_eq!(vote.request_id, mutation.request_id);
        assert!(replica
            .local_committed(subscription.subscription_id)
            .unwrap()
            .is_none());
        let other = assignment
            .replicas
            .iter()
            .find(|node| *node != &assignment.owner)
            .unwrap()
            .clone();
        let evidence: crate::reader::SubscriptionCommitEvidence = serde_json::from_value(json!({
            "votes": [[assignment.owner, vote.digest], [other, vote.digest]],
            "subscription_id": subscription.subscription_id, "request_id": mutation.request_id,
        }))
        .unwrap();
        let committed =
            crate::reader::SubscriptionProgressTransport::commit(&transport, &local, evidence)
                .await
                .unwrap();
        assert_eq!(committed, mutation);
        assert_eq!(
            replica
                .local_committed(subscription.subscription_id)
                .unwrap(),
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
        server.abort();
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
}
