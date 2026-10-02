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
        stage_candidate_ranges_local, ActiveRangeAssignment, AppendIdentity,
        CandidateSplitStagingResult, CommitPosition, KeyToken, MajorityAppendCoordinator,
        MajorityAppendError, RepairExportRequest, RepairExportResponse, RepairFrame,
        ReplicaAppendRequest, ReplicaAppendResponse, ReplicaAppendService, ReplicaCommitRequest,
        ReplicaCommitResponse, ReplicaProgressRequest, ReplicaProgressResponse,
        ReplicaReconcileRequest, ReplicaReconcileResponse, StorageNodeId,
        MAX_REPLICA_FRAME_BASE64_BYTES,
    },
    admin::{AdminAuthError, AdminAuthenticator, CommandBatchRequest, WclRequest},
    autoscale::{AutoscaleController, AutoscalePolicy, ScaleDecision},
    codec::{decode_record, encode_record},
    control::{ControlController, ControlError, RangeSplitPlan, ReplicatedCommand},
    control_plane::{
        ControlNodeId, ControlPlane, ControlPlaneError, ControlTypeConfig, FullSnapshotRequest,
        InternalCommandsRequest, InternalCommandsResponse, InternalWriteResponse,
    },
    demand::{DemandMetrics, DemandSnapshot},
    domain::{AppendInput, CursorRecord, StorageStats, StoredRecord},
    membership::{JoinResponse, MemberAnnouncement, MemberView, MembershipService},
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
    let service = state
        .replica_append
        .as_ref()
        .ok_or_else(|| ApiError::unavailable("replica storage is unavailable"))?;
    let limit = request
        .limit
        .unwrap_or(reader.session_capacity)
        .min(reader.session_capacity)
        .max(1);
    let timestamp_start = match (&reader.start, &reader.delivered_cursor) {
        (crate::control::ReaderStart::Timestamp(value), None) => Some(*value),
        _ => None,
    };
    let mut frames = service
        .read_committed(
            reader.feed_id,
            reader.delivered_cursor.as_deref(),
            if timestamp_start.is_some() {
                10_000
            } else {
                limit
            },
        )
        .await
        .map_err(|error| ApiError::unavailable(error.to_string()))?;
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
        .map(|record| record.cursor.clone())
        .or(reader.delivered_cursor.clone());
    if let Some(cursor) = records.last().map(|record| record.cursor.clone()) {
        execute_reader_command(
            &state,
            crate::control::Command::RecordReaderDelivery {
                reader: request.reader.clone(),
                session_epoch: request.session_epoch,
                cursor,
            },
            request.request_id,
        )
        .await?;
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
    let service = state
        .replica_append
        .as_ref()
        .ok_or_else(|| ApiError::unavailable("replica storage is unavailable"))?;
    let limit = request.limit.unwrap_or(100).clamp(1, 10_000);
    if request.new_only {
        let existing = service
            .read_committed(feed.feed_id, None, 10_000)
            .await
            .map_err(|error| ApiError::unavailable(error.to_string()))?;
        return Ok(Json(TemporaryReaderFetchResponse {
            next_cursor: existing.last().map(|item| item.cursor.clone()),
            records: Vec::new(),
        }));
    }
    let deadline = tokio::time::Instant::now()
        + Duration::from_millis(request.wait_ms.unwrap_or(0).min(30_000));
    loop {
        let mut frames = service
            .read_committed(
                feed.feed_id,
                request.after.as_deref(),
                if request.tail || after_event_time_ns.is_some() {
                    10_000
                } else {
                    limit
                },
            )
            .await
            .map_err(|error| ApiError::unavailable(error.to_string()))?;
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
    service
        .freeze_generation(
            request.source_assignment.range_id,
            request.source_assignment.generation,
        )
        .await;
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
    for node in source_assignment.replicas.iter() {
        let endpoint = state.control_endpoints.get(node).ok_or_else(|| {
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

async fn writer_session_append(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<WriterSessionAppendRequest>,
) -> Result<Json<WriterAppendResponse>, ApiError> {
    authorize_admin(&state, &headers)?;
    let writer = state
        .control
        .active_writer_by_name(&request.writer)
        .await
        .ok_or_else(|| {
            ApiError::bad_request(format!("Writer does not exist: {}", request.writer))
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
    let cursor = URL_SAFE_NO_PAD.encode(blake3::hash(request.request_id.as_bytes()).as_bytes());
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
    let service = state.replica_append.as_ref().ok_or_else(|| {
        ApiError::unavailable("Active Range replica storage is not configured on this Node")
    })?;
    let frames = service
        .read_committed(
            feed.feed_id,
            query.after.as_deref(),
            query.limit.unwrap_or(100),
        )
        .await
        .map_err(|error| ApiError {
            status: if error.retryable {
                StatusCode::SERVICE_UNAVAILABLE
            } else {
                StatusCode::BAD_REQUEST
            },
            message: error.message,
        })?;
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
    use crate::{control::Command, membership::MemberAnnouncement, storage::FileLogStore};

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

    async fn internal_replica_test_router(directory: &TempDir) -> (Router, Arc<ControlPlane>) {
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
        (
            router(AppState {
                store,
                membership,
                demand: DemandMetrics::default(),
                autoscaler: Arc::new(Mutex::new(AutoscaleController::default())),
                control,
                control_plane: Some(control_plane.clone()),
                replica_append: Some(replica_append),
                majority_append: None,
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
        let (app, control_plane) = internal_replica_test_router(&directory).await;
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
