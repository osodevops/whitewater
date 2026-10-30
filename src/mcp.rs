//! Read-only MCP (Model Context Protocol) adapter — the Operations
//! Advisor's machine-facing surface and a tool interface for engineers.
//! This is a frontend to the authenticated Admin API's semantics
//! (`docs/operations-advisor.md`): it exposes no catalog authority of
//! its own, no unbounded shell, and no path around `ControlController`.
//! Every tool re-authorizes the caller's credentials at use time with
//! the same Bearer/scoped-grant checks as the HTTP endpoints it wraps.
//!
//! Transport: single JSON-RPC 2.0 request/response per POST, served by
//! any Node (the "any Node" access contract). Mutating tools do not
//! exist here — actions remain a separate policy-gated path.

use axum::http::HeaderMap;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::api::{self, ApiError, AppState};
use crate::control::{Command, PermissionAction, ResourceKind};

/// The MCP protocol revision this adapter declares. The tool surface
/// (`initialize`, `tools/list`, `tools/call`, `ping`, notifications) is
/// unchanged across the 2024-11 and 2025 revisions, so serving one
/// declared level is honest.
const PROTOCOL_VERSION: &str = "2024-11-05";

#[derive(Debug, Deserialize)]
pub(crate) struct JsonRpcRequest {
    /// Present on calls, absent on notifications. `Value` preserves the
    /// client's id type verbatim for the response.
    #[serde(default)]
    pub id: Option<Value>,
    pub method: String,
    #[serde(default)]
    pub params: Value,
}

#[derive(Debug, Serialize)]
pub(crate) struct JsonRpcResponse {
    pub jsonrpc: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
}

#[derive(Debug, Serialize)]
pub(crate) struct JsonRpcError {
    pub code: i64,
    pub message: String,
}

const INVALID_PARAMS: i64 = -32602;
const METHOD_NOT_FOUND: i64 = -32601;
const AUTH_FAILED: i64 = -32001;
const TOOL_FAILED: i64 = -32000;

fn result(id: Option<Value>, value: Value) -> JsonRpcResponse {
    JsonRpcResponse {
        jsonrpc: "2.0",
        id,
        result: Some(value),
        error: None,
    }
}

fn failure(id: Option<Value>, code: i64, message: impl Into<String>) -> JsonRpcResponse {
    JsonRpcResponse {
        jsonrpc: "2.0",
        id,
        result: None,
        error: Some(JsonRpcError {
            code,
            message: message.into(),
        }),
    }
}

/// One tool's contract for `tools/list`: name, human description, and a
/// JSON Schema for its arguments — the shape MCP clients render.
struct ToolSpec {
    name: &'static str,
    description: &'static str,
    input_schema: Value,
    /// `true` when the tool requires Riverbed-admin scope rather than a
    /// scoped read — the gate is enforced again at call time.
    admin: bool,
}

fn tools() -> Vec<ToolSpec> {
    vec![
        ToolSpec {
            name: "whitewater.riverbed.health",
            description: "Correlated, versioned health explanation for the whole Riverbed: \
                per-stage status (Control Plane, Nodes, storage, Writers, Readers, \
                Subscriptions, Pipes, Distributaries, Indexes) with findings carrying cause, \
                risk, automatic action already running, next safe step, confidence, and \
                evidence freshness. Read-only; ask on any Node.",
            input_schema: json!({ "type": "object", "properties": {}, "additionalProperties": false }),
            admin: false,
        },
        ToolSpec {
            name: "whitewater.riverbed.explain_pressure",
            description: "Scoped view of the Riverbed health explanation: returns only the \
                named stages' status and findings (e.g. [\"readers\", \"distributaries\"]).",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "stages": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Stage names to include; omit for all stages."
                    }
                },
                "additionalProperties": false
            }),
            admin: false,
        },
        ToolSpec {
            name: "whitewater.scale.recommend",
            description: "Ranked, evidence-linked operational advice derived from the same                 health report: every recommendation names its evidence, expected impact,                 durability risk, how to verify it worked, and how to roll it back.                 Read-only advice — Whitewater never performs the action.",
            input_schema: json!({ "type": "object", "properties": {}, "additionalProperties": false }),
            admin: false,
        },
        ToolSpec {
            name: "whitewater.diagnostics.signals",
            description: "Bounded, redacted, scoped evidence retrieval: pass an optional                 scope object with domain/feed/node/window_ns fields and receive finding                 and demand records only — never payloads, Cursors, or credentials.                 Window is capped at 24h; records are capped at 128 with a truncated count.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "domain": { "type": "string" },
                    "feed": { "type": "string" },
                    "node": { "type": "string" },
                    "window_ns": { "type": "integer", "minimum": 0 }
                },
                "additionalProperties": false
            }),
            admin: false,
        },
        ToolSpec {
            name: "whitewater.diagnostics.baseline",
            description: "Schedule baseline for this Node's demand: typical/busy/quiet                 classification against the bounded sample ring (minute samples, a week                 deep at most) with depth-driven confidence and flip-streak hysteresis.                 Shallow history honestly reports learning rather than guessing.",
            input_schema: json!({ "type": "object", "properties": {}, "additionalProperties": false }),
            admin: false,
        },
        ToolSpec {
            name: "whitewater.feed.describe",
            description: "Describe one Feed's definition: Domain, lifecycle, schema and \
                history-policy bindings, and declared bounds. The caller needs read scope \
                covering the Feed's namespace.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "feed": { "type": "string", "description": "Feed name, e.g. orders.events" }
                },
                "required": ["feed"],
                "additionalProperties": false
            }),
            admin: false,
        },
        ToolSpec {
            name: "whitewater.support.collect_redacted",
            description: "Collect the bounded, redacted support bundle: Node identity, Control \
                Plane status, member liveness, and bounded catalog state. Credential material \
                and payloads are excluded by construction. Operator (admin) scope required.",
            input_schema: json!({ "type": "object", "properties": {}, "additionalProperties": false }),
            admin: true,
        },
    ]
}

/// Entry point for `POST /v1/mcp`. Authentication happens at the HTTP
/// layer like every other Admin endpoint; per-tool scope checks run
/// again inside `call_tool` because a narrower credential must not
/// reach a wider resource through the same channel.
pub(crate) async fn handle(
    state: &AppState,
    headers: &HeaderMap,
    request: JsonRpcRequest,
) -> Result<JsonRpcResponse, ApiError> {
    let id = request.id;
    match request.method.as_str() {
        "initialize" => Ok(result(
            id,
            json!({
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": { "tools": { "listChanged": false } },
                "serverInfo": {
                    "name": "whitewater",
                    "version": env!("CARGO_PKG_VERSION"),
                },
                "instructions": "Read-only Whitewater inspection tools. Every call re-authorizes the Bearer credential; denied scopes return an error result, never privileged data.",
            }),
        )),
        // Notifications are one-way by protocol; acknowledge silently.
        method if method.starts_with("notifications/") => Ok(result(id, Value::Null)),
        "ping" => Ok(result(id, json!({}))),
        "tools/list" => Ok(result(
            id,
            json!({
                "tools": tools()
                    .iter()
                    .map(|tool| json!({
                        "name": tool.name,
                        "description": tool.description,
                        "inputSchema": tool.input_schema,
                    }))
                    .collect::<Vec<_>>()
            }),
        )),
        "tools/call" => {
            let name = request
                .params
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let arguments = request
                .params
                .get("arguments")
                .cloned()
                .unwrap_or(json!({}));
            match call_tool(state, headers, &name, &arguments).await {
                Ok(value) => Ok(result(
                    id,
                    json!({
                        "content": [{ "type": "text", "text": value.to_string() }],
                        "structuredContent": value,
                        "isError": false,
                    }),
                )),
                Err(failure_kind) => {
                    let (code, message) = match failure_kind {
                        ToolError::Unauthorized(message) => (AUTH_FAILED, message),
                        ToolError::InvalidArguments(message) => (INVALID_PARAMS, message),
                        ToolError::Unknown(name) => {
                            (METHOD_NOT_FOUND, format!("unknown tool {name}"))
                        }
                        ToolError::Internal(message) => (TOOL_FAILED, message),
                    };
                    Ok(failure(id, code, message))
                }
            }
        }
        other => Ok(failure(
            id,
            METHOD_NOT_FOUND,
            format!("unknown method {other}"),
        )),
    }
}

enum ToolError {
    Unauthorized(String),
    InvalidArguments(String),
    Unknown(String),
    Internal(String),
}

fn map_api_error(error: ApiError) -> ToolError {
    ToolError::Unauthorized(format!("{error:?}"))
}

/// Dispatch one tool call. Each branch re-authorizes against the same
/// scopes the HTTP endpoints enforce — the adapter grants nothing the
/// caller's credential does not already allow.
async fn call_tool(
    state: &AppState,
    headers: &HeaderMap,
    name: &str,
    arguments: &Value,
) -> Result<Value, ToolError> {
    let spec = tools()
        .into_iter()
        .find(|tool| tool.name == name)
        .ok_or_else(|| ToolError::Unknown(name.to_string()))?;
    if spec.admin {
        api::authorize_admin(state, headers).map_err(map_api_error)?;
    }
    match name {
        "whitewater.riverbed.health" => {
            api::authorize_namespace(state, headers, "*", PermissionAction::Read)
                .map_err(map_api_error)?;
            let input = api::diagnostics_input(state).await;
            let mut report = crate::diagnostics::explain(&input);
            report.correlation_id = api::diagnostics_correlation_id(headers);
            serde_json::to_value(report).map_err(|error| ToolError::Internal(error.to_string()))
        }
        "whitewater.riverbed.explain_pressure" => {
            api::authorize_namespace(state, headers, "*", PermissionAction::Read)
                .map_err(map_api_error)?;
            let wanted: Option<Vec<String>> = arguments
                .get("stages")
                .map(|value| {
                    serde_json::from_value(value.clone())
                        .map_err(|error| ToolError::InvalidArguments(error.to_string()))
                })
                .transpose()?;
            let input = api::diagnostics_input(state).await;
            let mut report = crate::diagnostics::explain(&input);
            report.correlation_id = api::diagnostics_correlation_id(headers);
            if let Some(wanted) = wanted {
                report.stages.retain(|stage| wanted.contains(&stage.stage));
                report.limited = report
                    .stages
                    .iter()
                    .any(|stage| stage.status == crate::diagnostics::StageStatus::Limited);
            }
            serde_json::to_value(report).map_err(|error| ToolError::Internal(error.to_string()))
        }
        "whitewater.scale.recommend" => {
            api::authorize_namespace(state, headers, "*", PermissionAction::Read)
                .map_err(map_api_error)?;
            let input = api::diagnostics_input(state).await;
            let mut report = crate::diagnostics::explain(&input);
            report.correlation_id = api::diagnostics_correlation_id(headers);
            serde_json::to_value(crate::diagnostics::recommendations_view(&report))
                .map_err(|error| ToolError::Internal(error.to_string()))
        }
        "whitewater.diagnostics.signals" => {
            api::authorize_namespace(state, headers, "*", PermissionAction::Read)
                .map_err(map_api_error)?;
            let request: crate::diagnostics::SignalsRequest =
                serde_json::from_value(arguments.clone())
                    .map_err(|error| ToolError::InvalidArguments(error.to_string()))?;
            let input = api::diagnostics_input(state).await;
            let mut report = crate::diagnostics::explain(&input);
            report.correlation_id = api::diagnostics_correlation_id(headers);
            let snapshot = state.demand.snapshot();
            let demand = crate::diagnostics::DemandEvidence {
                requests_total: snapshot.requests_total,
                requests_in_flight: snapshot.requests_in_flight,
                appends_total: snapshot.appends_total,
                append_bytes_total: snapshot.append_bytes_total,
                reads_total: snapshot.reads_total,
                records_read_total: snapshot.records_read_total,
            };
            let mut response = crate::diagnostics::collect_signals(&report, Some(demand), request);
            response.correlation_id = report.correlation_id;
            serde_json::to_value(response).map_err(|error| ToolError::Internal(error.to_string()))
        }
        "whitewater.diagnostics.baseline" => {
            api::authorize_namespace(state, headers, "*", PermissionAction::Read)
                .map_err(map_api_error)?;
            let samples = state
                .demand
                .baseline_samples()
                .iter()
                .map(|sample| crate::diagnostics::BaselineEvidence {
                    taken_at_ns: sample.taken_at_ns,
                    requests_total: sample.requests_total,
                    appends_total: sample.appends_total,
                    reads_total: sample.reads_total,
                })
                .collect::<Vec<_>>();
            serde_json::to_value(crate::diagnostics::classify_baseline(&samples))
                .map_err(|error| ToolError::Internal(error.to_string()))
        }
        "whitewater.feed.describe" => {
            let feed = arguments
                .get("feed")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    ToolError::InvalidArguments("missing required argument: feed".to_string())
                })?;
            api::authorize_namespace(state, headers, feed, PermissionAction::Read)
                .map_err(map_api_error)?;
            let describe = || Command::Describe {
                kind: ResourceKind::Feed,
                name: feed.to_string(),
            };
            let actor = api::request_actor(state, headers);
            let execution = match &state.control_plane {
                Some(plane) => plane
                    .execute_commands_as(actor, vec![describe()], uuid::Uuid::new_v4())
                    .await
                    .map_err(|error| ToolError::Internal(error.to_string())),
                None => state
                    .control
                    .execute_commands_as(actor, vec![describe()], uuid::Uuid::new_v4())
                    .await
                    .map_err(|error| ToolError::Internal(error.to_string())),
            };
            serde_json::to_value(execution?).map_err(|error| ToolError::Internal(error.to_string()))
        }
        "whitewater.support.collect_redacted" => {
            // Admin scope was enforced above; reuse the same collector as
            // GET /v1/admin/support-bundle so the bundle cannot drift.
            let bundle = api::support_bundle(state).await;
            serde_json::to_value(bundle).map_err(|error| ToolError::Internal(error.to_string()))
        }
        _ => Err(ToolError::Unknown(name.to_string())),
    }
}
