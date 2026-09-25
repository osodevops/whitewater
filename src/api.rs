use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    extract::{Query, State},
    http::{header::AUTHORIZATION, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use base64::{engine::general_purpose::STANDARD, Engine};
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
    admin::{AdminAuthError, AdminAuthenticator, CommandBatchRequest, WclRequest},
    autoscale::{AutoscaleController, AutoscalePolicy, ScaleDecision},
    control::{ControlController, ControlError, ReplicatedCommand},
    control_plane::{
        ControlNodeId, ControlPlane, ControlPlaneError, ControlTypeConfig, FullSnapshotRequest,
        InternalCommandsRequest, InternalCommandsResponse, InternalWriteResponse,
    },
    demand::{DemandMetrics, DemandSnapshot},
    domain::{AppendInput, CursorRecord, StorageStats, StoredRecord},
    membership::{JoinResponse, MemberAnnouncement, MemberView, MembershipService},
    storage::{LogStore, StorageError},
};

#[derive(Clone)]
pub struct AppState {
    pub store: Arc<dyn LogStore>,
    pub membership: Arc<MembershipService>,
    pub demand: DemandMetrics,
    pub autoscaler: Arc<Mutex<AutoscaleController>>,
    pub control: Arc<ControlController>,
    pub control_plane: Option<Arc<ControlPlane>>,
    pub admin_auth: AdminAuthenticator,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/streams", get(list_streams).post(create_stream))
        .route("/v1/streams/describe", get(describe_stream))
        .route("/v1/records", get(read_records).post(append_record))
        .route("/v1/admin/wcl", post(execute_admin_wcl))
        .route("/v1/admin/commands", post(execute_admin_commands))
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

#[derive(Serialize)]
struct RecordResponse {
    cursor: String,
    message_id: Uuid,
    producer_id: Uuid,
    sequence: u64,
    event_time_ns: String,
    ingest_time_ns: String,
    key_base64: String,
    payload_base64: String,
    metadata_base64: BTreeMap<String, String>,
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
    use std::time::Duration;

    use axum::{body::Body, http::Request};
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
            ControlController::open(directory.path().join("catalog.json"), store.clone()).unwrap(),
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
            admin_auth: AdminAuthenticator::new(Some(
                "this-is-a-long-development-api-key".to_owned(),
            ))
            .unwrap(),
        })
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
