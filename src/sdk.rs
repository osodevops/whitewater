//! Typed application client for the Whitewater wire protocol
//! (`sdk/spec/PROTOCOL.md`, `sdk/spec/endpoints.json`). The public model
//! stays `Domain -> Feed -> Key -> Cursor -> Subscription`: no Range,
//! owner, replica, or partition concept crosses this boundary, and
//! Cursors are opaque pass-through strings.
//!
//! `Transport` is the seam unit tests drive: `HttpTransport` is the
//! production reqwest implementation, while tests inject a stub that
//! records requests and returns scripted replies. Every mutating call
//! carries a caller `request_id`; after an ambiguous outcome the SDK
//! retries the identical request rather than regenerating identity.
//!
//! Nanosecond fields are `i64` in Rust and decimal strings on the wire —
//! never floats — matching the spec's scalar contract.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use base64::Engine;
use serde_json::{json, Value};
use uuid::Uuid;

const SPEC_PROTOCOL_MAJOR: u64 = 1;

/// One record an application appends or receives. `event_time_ns` and
/// `ingest_time_ns` are signed nanoseconds; `metadata` is record
/// Metadata — never transport headers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    pub message_id: Option<Uuid>,
    pub key: Vec<u8>,
    pub payload: Vec<u8>,
    pub metadata: BTreeMap<String, Vec<u8>>,
    pub event_time_ns: i64,
    pub ingest_time_ns: i64,
    /// Opaque per-record Cursor. Never parsed or compared by the SDK;
    /// acknowledge a page's `delivered_cursor` instead.
    pub cursor: Option<String>,
}

/// What one append committed: the durable identity plus placement-free
/// feedback the caller may act on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendOutcome {
    pub message_id: Uuid,
    pub cursor: String,
    pub deduplicated: bool,
    /// Sustained dominant-Key share: spread or shed load for the Key;
    /// a strictly ordered hot Key is never parallelized.
    pub hot_key: bool,
    pub recommended_batch_count: Option<u64>,
    pub retry_after_ms: Option<u64>,
}

/// Enforced transport bounds the server advertises on `/health` — SDKs
/// negotiate capacity before a request hits a 4xx refusal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransportLimits {
    pub max_frame_bytes: u64,
    pub max_committed_read_bytes: u64,
    pub max_logical_read_frames: u64,
    pub max_logical_read_ranges: u64,
    pub member_fetch_max_limit: u64,
    pub member_max_lease_ticks: u64,
}

/// A page of delivered records plus the cursors that describe progress.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FetchPage {
    pub records: Vec<Record>,
    /// Advances when this page lands; ack this, never a record cursor.
    pub delivered_cursor: Option<String>,
    /// The durable restart point after the last ack.
    pub acknowledged_cursor: Option<String>,
    pub recommended_capacity: Option<u64>,
    pub retry_after_ms: Option<u64>,
}

/// Typed failure surfaced to the application. `(status, code)` on the
/// wire maps here; the `error` text is never branched on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SdkError {
    /// Writer/Reader/member epoch superseded: reopen the session.
    StaleEpoch,
    /// Sequence gap on the raw feed-append path.
    Sequence,
    /// Subscription work item claimed elsewhere; claim different work.
    LeaseBusy,
    /// Work lease expired or superseded.
    LeaseStale,
    /// Member or Subscription work cap reached; back off.
    LeaseCapacity,
    /// Generic fencing conflict without a wire `code`.
    Conflict(String),
    /// 5xx after an uncertain outcome: retry the identical request.
    Ambiguous(String),
    /// Quorum/replica unavailable: safe to retry the identical request.
    Unavailable(String),
    /// `index_behind`: strict freshness could not be reached inside the
    /// bounded wait; the read may be retried.
    IndexBehind,
    /// Refused before commit (syntax, validation, authentication).
    Refused(String),
    /// Server reports a different protocol major.
    IncompatibleServer {
        expected: u64,
        reported: Option<String>,
    },
    /// Transport-level failure before a status line arrived.
    Transport(String),
    /// A reply that violates the wire schema.
    Protocol(String),
}

impl SdkError {
    /// Whether an identical-`request_id` retry can settle the outcome.
    pub fn retryable(&self) -> bool {
        matches!(
            self,
            SdkError::Ambiguous(_)
                | SdkError::Unavailable(_)
                | SdkError::Transport(_)
                | SdkError::IndexBehind
        )
    }
}

fn map_error(status: u16, body: &Value) -> SdkError {
    let detail = body
        .get("error")
        .and_then(Value::as_str)
        .unwrap_or("no detail")
        .to_owned();
    let code = body.get("code").and_then(Value::as_str);
    match (status, code) {
        (409, Some("stale_epoch")) => SdkError::StaleEpoch,
        (409, Some("sequence")) => SdkError::Sequence,
        (409, Some("lease.busy")) => SdkError::LeaseBusy,
        (409, Some("lease.stale_lease")) => SdkError::LeaseStale,
        (409, Some("lease.capacity")) => SdkError::LeaseCapacity,
        (409, Some("index_behind")) => SdkError::IndexBehind,
        (409, _) => SdkError::Conflict(detail),
        (503, _) => SdkError::Unavailable(detail),
        _ if status >= 500 => SdkError::Ambiguous(detail),
        _ => SdkError::Refused(format!("{status}: {detail}")),
    }
}

/// Result of one HTTP exchange, decoupled from the client library so
/// tests can script replies without a socket.
#[derive(Clone, Debug)]
pub struct TransportReply {
    pub status: u16,
    pub body: Value,
}

/// The wire seam: one request/response against one Node base URL.
/// Implementations must not retry internally — ambiguity handling is the
/// SDK's contract.
#[async_trait]
pub trait Transport: Send + Sync {
    async fn request(
        &self,
        base: &str,
        method: &str,
        path: &str,
        body: Option<&Value>,
        api_key: &str,
    ) -> Result<TransportReply, SdkError>;
}

/// Production reqwest transport. `request_timeout` bounds one exchange;
/// the SDK, not the socket, owns retry policy.
pub struct HttpTransport {
    client: reqwest::Client,
}

impl HttpTransport {
    pub fn new() -> Result<Self, SdkError> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|error| SdkError::Transport(error.to_string()))?;
        Ok(Self { client })
    }
}

#[async_trait]
impl Transport for HttpTransport {
    async fn request(
        &self,
        base: &str,
        method: &str,
        path: &str,
        body: Option<&Value>,
        api_key: &str,
    ) -> Result<TransportReply, SdkError> {
        let url = format!("{}{}", base.trim_end_matches('/'), path);
        let builder = match method {
            "GET" => self.client.get(&url),
            _ => self.client.post(&url),
        }
        .bearer_auth(api_key);
        let builder = match body {
            Some(value) => builder.json(value),
            None => builder,
        };
        let response = builder
            .send()
            .await
            .map_err(|error| SdkError::Transport(error.to_string()))?;
        let status = response.status().as_u16();
        let body = response.json::<Value>().await.unwrap_or(Value::Null);
        Ok(TransportReply { status, body })
    }
}

/// One bound Node endpoint the client may target; any Node serves any
/// request because owner routing is a server detail. Calls spread across
/// the pool and hop on transport failure.
#[derive(Clone)]
pub struct Client {
    transport: Arc<dyn Transport>,
    bases: Vec<String>,
    api_key: String,
    use_compression: bool,
    cursor: Arc<std::sync::atomic::AtomicUsize>,
}

impl Client {
    pub fn new(
        transport: Arc<dyn Transport>,
        bases: Vec<String>,
        api_key: impl Into<String>,
    ) -> Self {
        assert!(!bases.is_empty(), "Client needs at least one Node base URL");
        Self {
            transport,
            bases,
            api_key: api_key.into(),
            use_compression: false,
            cursor: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    /// The spec's per-append compression preference (stored/replicated
    /// record frames only; wire bodies are unchanged).
    pub fn use_compression(mut self, enabled: bool) -> Self {
        self.use_compression = enabled;
        self
    }

    fn base(&self) -> &str {
        let index = self
            .cursor
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        &self.bases[index % self.bases.len()]
    }

    /// One exchange with endpoint hopping on transport failure only —
    /// the response (success or mapped error) is never re-targeted.
    async fn call(
        &self,
        method: &str,
        path: &str,
        body: Option<&Value>,
    ) -> Result<Value, SdkError> {
        let mut last = SdkError::Transport("no Node attempted".to_owned());
        for _ in 0..self.bases.len() {
            let base = self.base().to_owned();
            match self
                .transport
                .request(&base, method, path, body, &self.api_key)
                .await
            {
                Ok(reply) if (200..300).contains(&reply.status) => return Ok(reply.body),
                Ok(reply) => return Err(map_error(reply.status, &reply.body)),
                Err(error) => last = error,
            }
        }
        Err(last)
    }

    /// Raw protocol escape hatch for ops without a typed façade — the
    /// same endpoint pool, bearer auth, and transport failure policy as
    /// every call, but the caller owns argument encoding and reads the
    /// raw `(status, body)` reply (including error statuses). The
    /// conformance runner drives shared fixtures through this surface.
    pub async fn wire(
        &self,
        method: &str,
        path: &str,
        body: Option<&Value>,
    ) -> Result<TransportReply, SdkError> {
        let mut last = SdkError::Transport("no Node attempted".to_owned());
        for _ in 0..self.bases.len() {
            let base = self.base().to_owned();
            match self
                .transport
                .request(&base, method, path, body, &self.api_key)
                .await
            {
                Ok(reply) => return Ok(reply),
                Err(error) => last = error,
            }
        }
        Err(last)
    }

    /// `GET /health` capability check: reports the server's protocol
    /// metadata; `check_compatible` turns a mismatched major into an
    /// explicit error instead of a silent capability gap.
    pub async fn health(&self) -> Result<Value, SdkError> {
        self.call("GET", "/health", None).await
    }

    pub async fn check_compatible(&self) -> Result<(), SdkError> {
        let health = self.health().await?;
        let reported = health
            .get("protocol")
            .and_then(|value| value.get("major"))
            .and_then(Value::as_u64)
            .or_else(|| {
                health
                    .get("version")
                    .and_then(Value::as_str)
                    .and_then(|v| v.split('.').next()?.parse::<u64>().ok())
            });
        match reported {
            Some(major) if major == SPEC_PROTOCOL_MAJOR => Ok(()),
            other => Err(SdkError::IncompatibleServer {
                expected: SPEC_PROTOCOL_MAJOR,
                reported: other.map(|major| major.to_string()).or_else(|| {
                    health
                        .get("version")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                }),
            }),
        }
    }

    /// The transport bounds this server enforces, decoded from
    /// `/health.limits`. A missing or malformed section is a protocol
    /// error — capability negotiation refuses silent downgrade.
    pub async fn transport_limits(&self) -> Result<TransportLimits, SdkError> {
        let health = self.health().await?;
        let limits = health
            .get("limits")
            .ok_or_else(|| SdkError::Protocol("health carries no limits section".to_owned()))?;
        let field = |name: &str| {
            limits
                .get(name)
                .and_then(Value::as_u64)
                .ok_or_else(|| SdkError::Protocol(format!("health limits missing {name}")))
        };
        Ok(TransportLimits {
            max_frame_bytes: field("max_frame_bytes")?,
            max_committed_read_bytes: field("max_committed_read_bytes")?,
            max_logical_read_frames: field("max_logical_read_frames")?,
            max_logical_read_ranges: field("max_logical_read_ranges")?,
            member_fetch_max_limit: field("subscription_member_fetch_max_limit")?,
            member_max_lease_ticks: field("subscription_member_max_lease_ticks")?,
        })
    }

    /// Execute a WCL script; the response is the per-statement
    /// `ControlExecution` result array.
    pub async fn wcl(&self, script: &str) -> Result<Vec<Value>, SdkError> {
        let body = self
            .call("POST", "/v1/admin/wcl", Some(&json!({ "script": script })))
            .await?;
        Ok(body
            .get("results")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_else(|| body.as_array().cloned().unwrap_or_default()))
    }

    /// Execute typed Control commands (the same verbs WCL lowers to).
    pub async fn commands(&self, commands: Vec<Value>) -> Result<Vec<Value>, SdkError> {
        let body = self
            .call(
                "POST",
                "/v1/admin/commands",
                Some(&json!({ "commands": commands })),
            )
            .await?;
        Ok(body
            .get("results")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default())
    }

    /// Bind a declared Writer for appends: `OPEN WRITER SESSION` pins the
    /// session epoch the server fences every append against.
    pub async fn writer(&self, name: &str) -> Result<Writer, SdkError> {
        let results = self.wcl(&format!("OPEN WRITER SESSION {name};")).await?;
        let epoch = results
            .iter()
            .filter_map(|result| {
                result
                    .get("data")
                    .and_then(|data| data.get("session_epoch"))
                    .and_then(Value::as_u64)
                    .or_else(|| result.get("session_epoch").and_then(Value::as_u64))
            })
            .next()
            .ok_or_else(|| {
                SdkError::Protocol("OPEN WRITER SESSION returned no epoch".to_owned())
            })?;
        Ok(Writer {
            client: self.clone(),
            name: name.to_owned(),
            session_epoch: epoch,
        })
    }

    /// Bind a declared Reader: `reader.open` returns the session epoch
    /// and the current delivered/acknowledged cursors.
    pub async fn reader(&self, name: &str, capacity: u64) -> Result<Reader, SdkError> {
        let body = self
            .call(
                "POST",
                "/v1/readers/open",
                Some(&json!({
                    "request_id": Uuid::new_v4().to_string(),
                    "reader": name,
                    "capacity": capacity,
                })),
            )
            .await?;
        let session_epoch = body
            .get("session_epoch")
            .and_then(Value::as_u64)
            .ok_or_else(|| SdkError::Protocol("reader.open returned no epoch".to_owned()))?;
        Ok(Reader {
            client: self.clone(),
            name: name.to_owned(),
            session_epoch,
            delivered_cursor: body
                .get("delivered_cursor")
                .and_then(Value::as_str)
                .map(str::to_owned),
            acknowledged_cursor: body
                .get("acknowledged_cursor")
                .and_then(Value::as_str)
                .map(str::to_owned),
        })
    }

    /// Join a Subscription as a cooperating member.
    pub async fn member(&self, subscription: &str) -> Result<Member, SdkError> {
        let member_id = Uuid::new_v4();
        let body = self
            .call(
                "POST",
                "/v1/subscriptions/members/join",
                Some(&json!({
                    "subscription": subscription,
                    "request_id": member_id.to_string(),
                    "member_id": member_id.to_string(),
                })),
            )
            .await?;
        Ok(Member {
            client: self.clone(),
            subscription: subscription.to_owned(),
            member_id,
            member_epoch: body
                .get("member_epoch")
                .and_then(Value::as_u64)
                .unwrap_or(0),
        })
    }

    /// Read a page straight off a Feed: `after` resumes from an opaque
    /// Cursor, `limit` bounds the page.
    pub async fn records(
        &self,
        feed: &str,
        after: Option<&str>,
        limit: u64,
    ) -> Result<Vec<Record>, SdkError> {
        let mut path = format!("/v1/feeds/records?feed={feed}&limit={limit}");
        if let Some(cursor) = after {
            path.push_str(&format!("&after={}", urlencode(cursor)));
        }
        let body = self.call("GET", &path, None).await?;
        let records = body
            .as_array()
            .cloned()
            .or_else(|| body.get("records").and_then(Value::as_array).cloned())
            .unwrap_or_default();
        records.iter().map(decode_record).collect()
    }

    /// Bind a declared StateStore handle — no call; operations resolve
    /// the store per request and map its typed errors.
    pub fn state_store(&self, name: &str) -> StateStore {
        StateStore {
            client: self.clone(),
            name: name.to_owned(),
        }
    }

    /// Bind a declared Pipe for lifecycle operations: `describe`
    /// reports the live catalog row while `pause`/`resume`/`drop`
    /// drive the same typed commands WCL lowers to.
    pub fn pipe(&self, name: &str) -> Pipe {
        Pipe {
            client: self.clone(),
            name: name.to_owned(),
        }
    }

    /// `SHOW PIPES` decoded as [`PipeInfo`] rows.
    pub async fn pipes(&self) -> Result<Vec<PipeInfo>, SdkError> {
        let results = self
            .commands(vec![json!({ "command": "show", "kind": "pipes" })])
            .await?;
        let rows = results
            .first()
            .and_then(|result| result.get("data"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        Ok(rows.iter().filter_map(pipe_info).collect())
    }

    /// Temporary read against a Feed without declaring a Reader:
    /// bounded history walks, live tails, or fresh-only waits per
    /// `options` — see `TemporaryFetch`.
    pub async fn temporary_fetch(
        &self,
        feed: &str,
        options: TemporaryFetch,
    ) -> Result<TemporaryPage, SdkError> {
        let body = self
            .call(
                "POST",
                "/v1/readers/temporary/fetch",
                Some(&json!({
                    "feed": feed,
                    "after": options.after,
                    "limit": options.limit,
                    "new_only": options.new_only,
                    "tail": options.tail,
                    "wait_ms": options.wait_ms,
                    "after_event_time_ns": options
                        .after_event_time_ns
                        .map(|nanos| nanos.to_string()),
                })),
            )
            .await?;
        let records = body
            .get("records")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
            .iter()
            .map(decode_record)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(TemporaryPage {
            records,
            next_cursor: body
                .get("next_cursor")
                .and_then(Value::as_str)
                .map(str::to_owned),
        })
    }

    /// Query a declared persisted Index. `mode` selects exact,
    /// prefix, or range matching per `sdk/spec/endpoints.json`; `strict`
    /// bounded-waits for the serving Node's checkpoint to reach the
    /// activation frontier, surfacing `IndexBehind` when it cannot.
    pub async fn query_index(
        &self,
        index: &str,
        mode: IndexQuery,
        limit: Option<u64>,
        strict: bool,
    ) -> Result<IndexPage, SdkError> {
        let mode_fields = match &mode {
            IndexQuery::Exact(values) => json!({
                "mode": "exact",
                "values": values.iter().map(wire_typed).collect::<Vec<_>>(),
            }),
            IndexQuery::Prefix {
                leading,
                partial_text,
            } => json!({
                "mode": "prefix",
                "leading": leading.iter().map(wire_typed).collect::<Vec<_>>(),
                "partial_text": partial_text,
            }),
            IndexQuery::Range {
                leading,
                lower,
                upper,
            } => json!({
                "mode": "range",
                "leading": leading.iter().map(wire_typed).collect::<Vec<_>>(),
                "lower": {"value": wire_typed(&lower.value), "inclusive": lower.inclusive},
                "upper": {"value": wire_typed(&upper.value), "inclusive": upper.inclusive},
            }),
        };
        let mut request = json!({
            "index": index,
            "strict": strict,
        });
        if let Some(object) = request.as_object_mut() {
            if let Some(fields) = mode_fields.as_object() {
                for (key, value) in fields {
                    object.insert(key.clone(), value.clone());
                }
            }
            if let Some(limit) = limit {
                object.insert("limit".to_owned(), json!(limit));
            }
        }
        let body = self
            .call("POST", "/v1/indexes/query", Some(&request))
            .await?;
        let rows = body
            .get("rows")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
            .iter()
            .map(|row| {
                let decode = |field: &str| -> Result<Vec<u8>, SdkError> {
                    row.get(field)
                        .and_then(Value::as_str)
                        .and_then(|raw| STANDARD.decode(raw).ok())
                        .ok_or_else(|| SdkError::Protocol(format!("index row missing {field}")))
                };
                Ok((decode("key_base64")?, decode("payload_base64")?))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(IndexPage {
            rows,
            generation: body.get("generation").and_then(Value::as_u64).unwrap_or(0),
            applied_cursor: body
                .get("applied_cursor")
                .and_then(Value::as_str)
                .map(str::to_owned),
            target_cursor: body
                .get("target_cursor")
                .and_then(Value::as_str)
                .map(str::to_owned),
            behind: body.get("behind").and_then(Value::as_bool).unwrap_or(false),
            truncated: body
                .get("truncated")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            served_by: body
                .get("served_by")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
        })
    }
}

fn urlencode(value: &str) -> String {
    value
        .bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (byte as char).to_string()
            }
            _ => format!("%{byte:02X}"),
        })
        .collect()
}

/// A declared Writer bound to a session epoch. `append` carries a
/// caller `request_id`: identical retries dedupe server-side, so the
/// safe pattern after an ambiguous failure is the identical call.
#[derive(Clone)]
pub struct Writer {
    client: Client,
    name: String,
    session_epoch: u64,
}

impl Writer {
    pub fn session_epoch(&self) -> u64 {
        self.session_epoch
    }

    /// Reopen the session after `StaleEpoch` — the server supersedes the
    /// old epoch; continuing with it can never succeed.
    pub async fn reopen(&mut self) -> Result<(), SdkError> {
        *self = self.client.writer(&self.name).await?;
        Ok(())
    }

    /// Append one record. `request_id` is the idempotency identity: an
    /// identical retry returns the original committed outcome
    /// (`deduplicated: true`), a fresh identity may commit twice.
    pub async fn append(
        &self,
        request_id: Uuid,
        key: &[u8],
        payload: &[u8],
        event_time_ns: i64,
        metadata: BTreeMap<String, Vec<u8>>,
    ) -> Result<AppendOutcome, SdkError> {
        let body = self
            .client
            .call(
                "POST",
                "/v1/writers/append",
                Some(&json!({
                    "request_id": request_id.to_string(),
                    "writer": self.name,
                    "session_epoch": self.session_epoch,
                    "event_time_ns": event_time_ns.to_string(),
                    "key_base64": STANDARD.encode(key),
                    "payload_base64": STANDARD.encode(payload),
                    "metadata_base64": metadata_base64(&metadata),
                    "use_compression": self.client.use_compression,
                })),
            )
            .await?;
        decode_append_outcome(&body)
    }

    /// Append a batch as one request; per-record `request_id`s keep the
    /// same retry contract as `append`.
    pub async fn append_batch(
        &self,
        records: &[(Uuid, Vec<u8>, Vec<u8>, i64)],
    ) -> Result<Vec<AppendOutcome>, SdkError> {
        let items: Vec<Value> = records
            .iter()
            .map(|(request_id, key, payload, event_time_ns)| {
                json!({
                    "request_id": request_id.to_string(),
                    "writer": self.name,
                    "session_epoch": self.session_epoch,
                    "event_time_ns": event_time_ns.to_string(),
                    "key_base64": STANDARD.encode(key),
                    "payload_base64": STANDARD.encode(payload),
                })
            })
            .collect();
        let body = self
            .client
            .call(
                "POST",
                "/v1/writers/append-batch",
                Some(&json!({ "records": items })),
            )
            .await?;
        let outcomes = body
            .get("results")
            .and_then(Value::as_array)
            .cloned()
            .or_else(|| body.as_array().cloned())
            .unwrap_or_default();
        outcomes.iter().map(decode_append_outcome).collect()
    }
}

use base64::engine::general_purpose::STANDARD;

fn metadata_base64(metadata: &BTreeMap<String, Vec<u8>>) -> Value {
    metadata
        .iter()
        .map(|(name, value)| (name.clone(), Value::String(STANDARD.encode(value))))
        .collect::<serde_json::Map<String, Value>>()
        .into()
}

fn decode_append_outcome(body: &Value) -> Result<AppendOutcome, SdkError> {
    let message_id = body
        .get("message_id")
        .and_then(Value::as_str)
        .and_then(|value| Uuid::parse_str(value).ok())
        .ok_or_else(|| SdkError::Protocol("append returned no message_id".to_owned()))?;
    let cursor = body
        .get("cursor")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let feedback = body.get("feedback").cloned().unwrap_or(Value::Null);
    Ok(AppendOutcome {
        message_id,
        cursor,
        deduplicated: body
            .get("deduplicated")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        hot_key: feedback
            .get("hot_key")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        recommended_batch_count: feedback
            .get("recommended_batch_count")
            .and_then(Value::as_u64),
        retry_after_ms: feedback.get("retry_after_ms").and_then(Value::as_u64),
    })
}

fn decode_record(value: &Value) -> Result<Record, SdkError> {
    let decode = |field: &str| -> Result<Vec<u8>, SdkError> {
        value
            .get(field)
            .and_then(Value::as_str)
            .and_then(|encoded| STANDARD.decode(encoded).ok())
            .ok_or_else(|| SdkError::Protocol(format!("record missing {field}")))
    };
    let nanos = |field: &str| -> i64 {
        value
            .get(field)
            .and_then(|raw| match raw {
                Value::String(text) => text.parse::<i64>().ok(),
                Value::Number(number) => number.as_i64(),
                _ => None,
            })
            .unwrap_or(0)
    };
    let metadata = value
        .get("metadata_base64")
        .and_then(Value::as_object)
        .map(|entries| {
            entries
                .iter()
                .filter_map(|(name, encoded)| {
                    encoded
                        .as_str()
                        .and_then(|raw| STANDARD.decode(raw).ok())
                        .map(|bytes| (name.clone(), bytes))
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(Record {
        message_id: value
            .get("message_id")
            .and_then(Value::as_str)
            .and_then(|raw| Uuid::parse_str(raw).ok()),
        key: decode("key_base64")?,
        payload: decode("payload_base64")?,
        metadata,
        event_time_ns: nanos("event_time_ns"),
        ingest_time_ns: nanos("ingest_time_ns"),
        cursor: value
            .get("cursor")
            .and_then(Value::as_str)
            .map(str::to_owned),
    })
}

/// A declared Reader bound to a session epoch. Delivery is
/// at-least-once; `ack` persists the page's `delivered_cursor` — never a
/// record's own cursor, which may not cover other ranges.
#[derive(Clone)]
pub struct Reader {
    client: Client,
    name: String,
    session_epoch: u64,
    delivered_cursor: Option<String>,
    acknowledged_cursor: Option<String>,
}

impl Reader {
    pub fn session_epoch(&self) -> u64 {
        self.session_epoch
    }

    pub fn acknowledged_cursor(&self) -> Option<&str> {
        self.acknowledged_cursor.as_deref()
    }

    /// Fetch one bounded page. The page's `delivered_cursor` is the ack
    /// target; an ambiguous failure makes the identical retry safe.
    pub async fn fetch(&mut self, limit: u64) -> Result<FetchPage, SdkError> {
        let body = self
            .client
            .call(
                "POST",
                "/v1/readers/fetch",
                Some(&json!({
                    "request_id": Uuid::new_v4().to_string(),
                    "reader": self.name,
                    "session_epoch": self.session_epoch,
                    "limit": limit,
                })),
            )
            .await?;
        let records = body
            .get("records")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
            .iter()
            .map(decode_record)
            .collect::<Result<Vec<_>, _>>()?;
        self.delivered_cursor = body
            .get("delivered_cursor")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .or_else(|| self.delivered_cursor.clone());
        self.acknowledged_cursor = body
            .get("acknowledged_cursor")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .or_else(|| self.acknowledged_cursor.clone());
        Ok(FetchPage {
            records,
            delivered_cursor: self.delivered_cursor.clone(),
            acknowledged_cursor: self.acknowledged_cursor.clone(),
            recommended_capacity: body.get("recommended_capacity").and_then(Value::as_u64),
            retry_after_ms: body.get("retry_after_ms").and_then(Value::as_u64),
        })
    }

    /// Acknowledge the latest fetched page's `delivered_cursor`. Passing
    /// `None` acks nothing — the durable point stays put.
    pub async fn ack(&mut self) -> Result<(), SdkError> {
        let Some(cursor) = self.delivered_cursor.clone() else {
            return Ok(());
        };
        let body = self
            .client
            .call(
                "POST",
                "/v1/readers/ack",
                Some(&json!({
                    "request_id": Uuid::new_v4().to_string(),
                    "reader": self.name,
                    "session_epoch": self.session_epoch,
                    "cursor": cursor,
                })),
            )
            .await?;
        self.acknowledged_cursor = body
            .get("acknowledged_cursor")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .or(Some(cursor));
        Ok(())
    }

    /// Close the session; the acknowledged cursor stays the durable
    /// restart point for the next `open`.
    pub async fn close(self) -> Result<(), SdkError> {
        self.client
            .call(
                "POST",
                "/v1/readers/close",
                Some(&json!({
                    "request_id": Uuid::new_v4().to_string(),
                    "reader": self.name,
                    "session_epoch": self.session_epoch,
                })),
            )
            .await?;
        Ok(())
    }
}

/// One claimed unit of Subscription work a member holds a lease on.
/// `claim` grants the lease; records arrive through `fetch`.
#[derive(Clone, Debug)]
pub struct ClaimedWork {
    pub work_id: Uuid,
    pub lease_epoch: u64,
    pub expires_at_tick: Option<u64>,
}

/// The bounded page `member.fetch` delivers under a live lease. `cursor`
/// and `positions` are the opaque progress evidence `ack` commits back —
/// carry them through `Member::ack`, never reconstruct them.
#[derive(Clone, Debug)]
pub struct WorkPage {
    pub records: Vec<Record>,
    pub cursor: Option<String>,
    /// Opaque per-range progress evidence; internal detail passed back
    /// to `ack` verbatim so no physical layout crosses the SDK contract.
    pub positions: Value,
    /// The delivering claim's lease epoch — the durable per-work
    /// delivery-attempt counter retry budgets count against.
    pub delivery_attempt: u64,
    /// Unix epoch nanos when the earliest delayed head becomes eligible.
    pub next_delivery_ns: Option<i64>,
}

/// A cooperating Subscription member: joins under a generated identity,
/// claims bounded work, and acks committed progress through quorum
/// leases — the shared-frontier consumer-group contract.
#[derive(Clone)]
pub struct Member {
    client: Client,
    subscription: String,
    member_id: Uuid,
    member_epoch: u64,
}

impl Member {
    /// Constructs a member handle without joining — for conformance and
    /// fencing tests that must present an arbitrary member identity.
    #[doc(hidden)]
    pub fn for_testing(
        client: Client,
        subscription: &str,
        member_id: Uuid,
        member_epoch: u64,
    ) -> Self {
        Self {
            client,
            subscription: subscription.to_owned(),
            member_id,
            member_epoch,
        }
    }

    pub fn member_id(&self) -> Uuid {
        self.member_id
    }

    pub fn member_epoch(&self) -> u64 {
        self.member_epoch
    }

    /// Claim one work item up to the member/Subscription caps. A busy
    /// Subscription answers `LeaseBusy`; capacity pressure answers
    /// `LeaseCapacity` — both are typed, retry-safe signals.
    pub async fn claim(&self) -> Result<ClaimedWork, SdkError> {
        let work_id = Uuid::new_v4();
        let body = self
            .client
            .call(
                "POST",
                "/v1/subscriptions/members/claim",
                Some(&json!({
                    "subscription": self.subscription,
                    "request_id": Uuid::new_v4().to_string(),
                    "member_id": self.member_id.to_string(),
                    "member_epoch": self.member_epoch,
                    "work_id": work_id.to_string(),
                })),
            )
            .await?;
        let lease = body.get("lease").ok_or_else(|| {
            SdkError::Protocol("claim succeeded without a granted lease".to_owned())
        })?;
        Ok(ClaimedWork {
            work_id: lease
                .get("work_id")
                .and_then(Value::as_str)
                .and_then(|raw| Uuid::parse_str(raw).ok())
                .unwrap_or(work_id),
            lease_epoch: lease
                .get("lease_epoch")
                .and_then(Value::as_u64)
                .unwrap_or(0),
            expires_at_tick: lease.get("expires_at_tick").and_then(Value::as_u64),
        })
    }

    /// Fetch the bounded page of records bound to a claimed work item.
    /// The returned `cursor`/`positions` are the ack evidence — commit
    /// them with `Member::ack` after processing the page.
    pub async fn fetch(&self, work: &ClaimedWork, limit: u64) -> Result<WorkPage, SdkError> {
        let body = self
            .client
            .call(
                "POST",
                "/v1/subscriptions/members/fetch",
                Some(&json!({
                    "subscription": self.subscription,
                    "request_id": Uuid::new_v4().to_string(),
                    "member_id": self.member_id.to_string(),
                    "member_epoch": self.member_epoch,
                    "work_id": work.work_id.to_string(),
                    "lease_epoch": work.lease_epoch,
                    "limit": limit,
                })),
            )
            .await?;
        let records = body
            .get("records")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
            .iter()
            .map(decode_record)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(WorkPage {
            records,
            cursor: body
                .get("cursor")
                .and_then(Value::as_str)
                .map(str::to_owned),
            positions: body.get("positions").cloned().unwrap_or(Value::Null),
            delivery_attempt: body
                .get("delivery_attempt")
                .and_then(Value::as_u64)
                .unwrap_or(0),
            next_delivery_ns: body
                .get("next_delivery_ns")
                .and_then(Value::as_str)
                .and_then(|raw| raw.parse::<i64>().ok()),
        })
    }

    /// Commit the claimed work's progress under the lease epoch, carrying
    /// the fetch page's `cursor`/`positions` evidence verbatim; stale
    /// leases fence as `LeaseStale`/`StaleEpoch`, never silent loss.
    pub async fn ack(&self, work: &ClaimedWork, page: &WorkPage) -> Result<(), SdkError> {
        self.client
            .call(
                "POST",
                "/v1/subscriptions/members/ack",
                Some(&json!({
                    "subscription": self.subscription,
                    "request_id": Uuid::new_v4().to_string(),
                    "member_id": self.member_id.to_string(),
                    "member_epoch": self.member_epoch,
                    "work_id": work.work_id.to_string(),
                    "lease_epoch": work.lease_epoch,
                    "cursor": page.cursor,
                    "positions": page.positions,
                })),
            )
            .await?;
        Ok(())
    }

    /// Renew a held lease; expiry without renew is `LeaseStale` fencing.
    pub async fn renew(&self, work: &ClaimedWork, lease_ticks: u64) -> Result<(), SdkError> {
        self.client
            .call(
                "POST",
                "/v1/subscriptions/members/renew",
                Some(&json!({
                    "subscription": self.subscription,
                    "request_id": Uuid::new_v4().to_string(),
                    "member_id": self.member_id.to_string(),
                    "member_epoch": self.member_epoch,
                    "work_id": work.work_id.to_string(),
                    "lease_epoch": work.lease_epoch,
                    "lease_ticks": lease_ticks,
                })),
            )
            .await?;
        Ok(())
    }

    /// Release a claim without acking: the work redelivers to another
    /// member under a fresh lease.
    pub async fn release(&self, work: &ClaimedWork) -> Result<(), SdkError> {
        self.client
            .call(
                "POST",
                "/v1/subscriptions/members/release",
                Some(&json!({
                    "subscription": self.subscription,
                    "request_id": Uuid::new_v4().to_string(),
                    "member_id": self.member_id.to_string(),
                    "member_epoch": self.member_epoch,
                    "work_id": work.work_id.to_string(),
                    "lease_epoch": work.lease_epoch,
                })),
            )
            .await?;
        Ok(())
    }
}

/// One typed Index/StateStore field value — the wire contract shared by
/// `query_index`, `state_store.get_by`, and named-key mutations. Bytes
/// travel base64 like every binary field on the API.
#[derive(Clone, Debug, PartialEq)]
pub enum TypedValue {
    Null,
    Bool(bool),
    I64(i64),
    Text(String),
    Bytes(Vec<u8>),
}

fn wire_typed(value: &TypedValue) -> Value {
    match value {
        TypedValue::Null => json!({"kind": "null"}),
        TypedValue::Bool(value) => json!({"kind": "bool", "value": value}),
        TypedValue::I64(value) => json!({"kind": "i64", "value": value}),
        TypedValue::Text(value) => json!({"kind": "text", "value": value}),
        TypedValue::Bytes(value) => {
            json!({"kind": "bytes", "value_base64": STANDARD.encode(value)})
        }
    }
}

/// The catalog row `DESCRIBE PIPE`/`SHOW PIPES` returns for a declared
/// Pipe. `operation` stays an opaque [`Value`] — Forward, Window, and
/// Enrich variants evolve server-side without an SDK type bump.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PipeInfo {
    pub name: String,
    /// `declared` while driven, `paused` under `PAUSE PIPE`.
    pub stage: String,
    pub subscription_id: Uuid,
    pub operation: Value,
    pub output_feed_id: Option<Uuid>,
    pub output_store_id: Option<Uuid>,
    pub status: String,
    pub created_at_ns: i64,
}

fn pipe_info(value: &Value) -> Option<PipeInfo> {
    let uuid = |field: &str| {
        value
            .get(field)
            .and_then(Value::as_str)
            .and_then(|raw| raw.parse::<Uuid>().ok())
    };
    Some(PipeInfo {
        name: value.get("name")?.as_str()?.to_owned(),
        stage: value.get("stage")?.as_str()?.to_owned(),
        subscription_id: uuid("subscription_id")?,
        operation: value.get("operation").cloned().unwrap_or(Value::Null),
        output_feed_id: uuid("output_feed_id"),
        output_store_id: uuid("output_store_id"),
        status: value.get("status")?.as_str()?.to_owned(),
        created_at_ns: value.get("created_at_ns")?.as_i64()?,
    })
}

/// A declared Pipe's lifecycle surface. Pausing is cooperative — an
/// in-flight effect finishes and the frontier stays put so redelivery
/// stays replay-safe — and resume restarts driving from the same
/// durable frontier.
pub struct Pipe {
    client: Client,
    name: String,
}

impl Pipe {
    pub fn name(&self) -> &str {
        &self.name
    }

    /// `DESCRIBE PIPE`: the live catalog row including stage and the
    /// opaque operation definition.
    pub async fn describe(&self) -> Result<PipeInfo, SdkError> {
        let results = self
            .client
            .commands(vec![json!({
                "command": "describe",
                "kind": "pipe",
                "name": self.name,
            })])
            .await?;
        let data = results
            .first()
            .and_then(|result| result.get("data"))
            .cloned()
            .unwrap_or(Value::Null);
        pipe_info(&data)
            .ok_or_else(|| SdkError::Protocol("describe pipe returned no definition".to_owned()))
    }

    /// `PAUSE PIPE`: halts every drive cooperatively; idempotent.
    pub async fn pause(&self) -> Result<(), SdkError> {
        self.lifecycle("pause_pipe").await
    }

    /// `RESUME PIPE`: restarts driving from the durable frontier.
    pub async fn resume(&self) -> Result<(), SdkError> {
        self.lifecycle("resume_pipe").await
    }

    /// `DROP PIPE`: retires the declaration and its compute lease.
    pub async fn drop(self) -> Result<(), SdkError> {
        self.client
            .commands(vec![json!({
                "command": "drop",
                "kind": "pipe",
                "name": self.name,
            })])
            .await
            .map(|_| ())
    }

    async fn lifecycle(&self, command: &str) -> Result<(), SdkError> {
        self.client
            .commands(vec![json!({ "command": command, "name": self.name })])
            .await
            .map(|_| ())
    }
}

/// Options for `Client::temporary_fetch`: `new_only` waits for fresh
/// history, `tail` follows live delivery, `after`/`after_event_time_ns`
/// bound where the page starts.
#[derive(Clone, Debug, Default)]
pub struct TemporaryFetch {
    /// Opaque Cursor to resume after; `None` starts per `tail`/`new_only`.
    pub after: Option<String>,
    pub limit: u64,
    pub new_only: bool,
    pub tail: bool,
    /// Bounded server-side wait for records on a live tail.
    pub wait_ms: Option<u64>,
    /// Event-time lower bound (signed epoch nanoseconds).
    pub after_event_time_ns: Option<i64>,
}

/// A page returned by `Client::temporary_fetch`.
#[derive(Clone, Debug)]
pub struct TemporaryPage {
    pub records: Vec<Record>,
    /// Opaque continuation for the next page; `None` at the tip.
    pub next_cursor: Option<String>,
}

/// Inclusive/exclusive bound for an Index range query.
#[derive(Clone, Debug)]
pub struct IndexBound {
    pub value: TypedValue,
    pub inclusive: bool,
}

/// The query shape `Client::query_index` lowers onto the wire.
#[derive(Clone, Debug)]
pub enum IndexQuery {
    /// Every declared field, exact match.
    Exact(Vec<TypedValue>),
    /// Leading fields exact, plus an optional text prefix on the next.
    Prefix {
        leading: Vec<TypedValue>,
        partial_text: Option<String>,
    },
    /// Leading fields exact, the next field between `lower`/`upper`.
    Range {
        leading: Vec<TypedValue>,
        lower: IndexBound,
        upper: IndexBound,
    },
}

/// A page returned by `Client::query_index`.
#[derive(Clone, Debug)]
pub struct IndexPage {
    /// Decoded `(key, payload)` posting rows.
    pub rows: Vec<(Vec<u8>, Vec<u8>)>,
    /// Active generation that served — changes on rebuild.
    pub generation: u64,
    pub applied_cursor: Option<String>,
    pub target_cursor: Option<String>,
    /// Serving Node still materializing the activation frontier.
    pub behind: bool,
    /// The page hit a server bound; narrow the query to continue.
    pub truncated: bool,
    /// Node identity that served — diagnostics, never routing input.
    pub served_by: String,
}

/// Outcome of one committed StateStore mutation.
#[derive(Clone, Debug)]
pub struct MutationOutcome {
    pub store_id: Uuid,
    pub sequence: u64,
    /// Identical `mutation_id` replay — the row did not commit twice.
    pub deduplicated: bool,
}

/// One row as served by lookup/get_by/iterate.
#[derive(Clone, Debug)]
pub struct StoreRow {
    pub key: Vec<u8>,
    pub payload: Vec<u8>,
}

/// Freshness + identity envelope every StateStore read reports.
#[derive(Clone, Debug)]
pub struct ReadFreshness {
    /// Highest journal sequence the serving replica applied.
    pub applied_seq: u64,
    /// Highest committed sequence on the serving replica.
    pub committed_seq: u64,
    /// `applied_seq < committed_seq`: the replica is still applying.
    pub behind: bool,
    /// Node identity that served — diagnostics, never routing input.
    pub served_by: String,
}

#[derive(Clone, Debug)]
pub struct LookupOutcome {
    pub row: Option<StoreRow>,
    pub freshness: ReadFreshness,
}

#[derive(Clone, Debug)]
pub struct GetByPage {
    pub rows: Vec<StoreRow>,
    pub freshness: ReadFreshness,
}

#[derive(Clone, Debug)]
pub struct IteratePage {
    pub rows: Vec<StoreRow>,
    /// Continuation key for the next page; `None` at the end.
    pub next_key: Option<Vec<u8>>,
    /// Journal position the page was pinned to.
    pub pinned_seq: u64,
    pub served_by: String,
}

/// Outcome of `sync`/`rebuild`: records applied this pass plus whether
/// the store reached the committed tip of its source Feed.
#[derive(Clone, Debug)]
pub struct SyncOutcome {
    pub applied: u64,
    pub frontier: Option<String>,
    pub complete: bool,
}

/// A declared StateStore bound by name. Manual stores accept
/// `put_row`/`delete_row`/`clear` under caller `mutation_id`s (identical
/// retries dedupe); feed-sourced stores refuse mutation server-side and
/// are maintained by `sync`/`rebuild`.
#[derive(Clone)]
pub struct StateStore {
    client: Client,
    name: String,
}

fn decode_store_row(value: &Value, key_field: &str) -> Result<StoreRow, SdkError> {
    let decode = |field: &str| -> Result<Vec<u8>, SdkError> {
        value
            .get(field)
            .and_then(Value::as_str)
            .and_then(|raw| STANDARD.decode(raw).ok())
            .ok_or_else(|| SdkError::Protocol(format!("store row missing {field}")))
    };
    Ok(StoreRow {
        key: decode(key_field)?,
        payload: decode("payload_base64")?,
    })
}

fn decode_freshness(body: &Value) -> ReadFreshness {
    ReadFreshness {
        applied_seq: body.get("applied_seq").and_then(Value::as_u64).unwrap_or(0),
        committed_seq: body
            .get("committed_seq")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        behind: body.get("behind").and_then(Value::as_bool).unwrap_or(false),
        served_by: body
            .get("served_by")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
    }
}

fn decode_mutation_outcome(body: &Value) -> Result<MutationOutcome, SdkError> {
    Ok(MutationOutcome {
        store_id: body
            .get("store_id")
            .and_then(Value::as_str)
            .and_then(|raw| Uuid::parse_str(raw).ok())
            .ok_or_else(|| SdkError::Protocol("mutation returned no store_id".to_owned()))?,
        sequence: body.get("sequence").and_then(Value::as_u64).unwrap_or(0),
        deduplicated: body
            .get("deduplicated")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    })
}

fn decode_sync_outcome(body: &Value) -> SyncOutcome {
    SyncOutcome {
        applied: body.get("applied").and_then(Value::as_u64).unwrap_or(0),
        frontier: body
            .get("frontier")
            .and_then(Value::as_str)
            .map(str::to_owned),
        complete: body
            .get("complete")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    }
}

impl StateStore {
    /// Put one row with optional named-key postings. `mutation_id` is the
    /// idempotency identity: an identical retry dedupes, a different
    /// payload under the same id conflicts.
    pub async fn put_row(
        &self,
        mutation_id: Uuid,
        key: &[u8],
        payload: &[u8],
        named: &[(String, Vec<TypedValue>)],
    ) -> Result<MutationOutcome, SdkError> {
        let named_wire: Vec<Value> = named
            .iter()
            .map(|(name, values)| {
                json!({
                    "name": name,
                    "values": values.iter().map(wire_typed).collect::<Vec<_>>(),
                })
            })
            .collect();
        let body = self
            .client
            .call(
                "POST",
                "/v1/state-stores/mutate",
                Some(&json!({
                    "store": self.name,
                    "mutation_id": mutation_id.to_string(),
                    "op": {
                        "kind": "put_row",
                        "key_base64": STANDARD.encode(key),
                        "payload_base64": STANDARD.encode(payload),
                        "named": named_wire,
                    },
                })),
            )
            .await?;
        decode_mutation_outcome(&body)
    }

    /// Delete one row by primary key under a `mutation_id` identity.
    pub async fn delete_row(
        &self,
        mutation_id: Uuid,
        key: &[u8],
    ) -> Result<MutationOutcome, SdkError> {
        let body = self
            .client
            .call(
                "POST",
                "/v1/state-stores/mutate",
                Some(&json!({
                    "store": self.name,
                    "mutation_id": mutation_id.to_string(),
                    "op": {
                        "kind": "delete_row",
                        "key_base64": STANDARD.encode(key),
                    },
                })),
            )
            .await?;
        decode_mutation_outcome(&body)
    }

    /// Empty the store under a `mutation_id` identity — replay dedupes.
    pub async fn clear(&self, mutation_id: Uuid) -> Result<MutationOutcome, SdkError> {
        let body = self
            .client
            .call(
                "POST",
                "/v1/state-stores/clear",
                Some(&json!({
                    "store": self.name,
                    "mutation_id": mutation_id.to_string(),
                })),
            )
            .await?;
        decode_mutation_outcome(&body)
    }

    /// Point lookup by primary key. `strict` bounded-waits for the
    /// replica's applied sequence to reach its committed snapshot;
    /// unreachable freshness answers `IndexBehind`.
    pub async fn lookup(&self, key: &[u8], strict: bool) -> Result<LookupOutcome, SdkError> {
        let body = self
            .client
            .call(
                "POST",
                "/v1/state-stores/lookup",
                Some(&json!({
                    "store": self.name,
                    "key_base64": STANDARD.encode(key),
                    "strict": strict,
                })),
            )
            .await?;
        let row = match body.get("found").and_then(Value::as_bool) {
            Some(true) => Some(StoreRow {
                key: key.to_vec(),
                payload: body
                    .get("payload_base64")
                    .and_then(Value::as_str)
                    .and_then(|raw| STANDARD.decode(raw).ok())
                    .ok_or_else(|| {
                        SdkError::Protocol("lookup found a row without payload".to_owned())
                    })?,
            }),
            _ => None,
        };
        Ok(LookupOutcome {
            row,
            freshness: decode_freshness(&body),
        })
    }

    /// Rows whose declared `named_key` posting equals one of `values`.
    /// Bounded by `limit`; `strict` carries the same freshness contract
    /// as `lookup`.
    pub async fn get_by(
        &self,
        named_key: &str,
        values: &[TypedValue],
        limit: Option<u64>,
        strict: bool,
    ) -> Result<GetByPage, SdkError> {
        let body = self
            .client
            .call(
                "POST",
                "/v1/state-stores/get-by",
                Some(&json!({
                    "store": self.name,
                    "named_key": named_key,
                    "values": values.iter().map(wire_typed).collect::<Vec<_>>(),
                    "limit": limit,
                    "strict": strict,
                })),
            )
            .await?;
        let rows = body
            .get("rows")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
            .iter()
            .map(|row| decode_store_row(row, "key_base64"))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(GetByPage {
            rows,
            freshness: decode_freshness(&body),
        })
    }

    /// Primary-key ordered page; `after_key` continues from a prior
    /// page's `next_key`, `None` starts at the first row.
    pub async fn iterate(
        &self,
        after_key: Option<&[u8]>,
        limit: Option<u64>,
    ) -> Result<IteratePage, SdkError> {
        let body = self
            .client
            .call(
                "POST",
                "/v1/state-stores/iterate",
                Some(&json!({
                    "store": self.name,
                    "after_key_base64": after_key.map(|key| STANDARD.encode(key)),
                    "limit": limit,
                })),
            )
            .await?;
        let rows = body
            .get("rows")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
            .iter()
            .map(|row| decode_store_row(row, "key_base64"))
            .collect::<Result<Vec<_>, _>>()?;
        let next_key = body
            .get("next_key_base64")
            .and_then(Value::as_str)
            .and_then(|raw| STANDARD.decode(raw).ok());
        Ok(IteratePage {
            rows,
            next_key,
            pinned_seq: body.get("pinned_seq").and_then(Value::as_u64).unwrap_or(0),
            served_by: body
                .get("served_by")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
        })
    }

    /// Drive one catch-up pass on a feed-sourced store; `complete` means
    /// the committed tip was reached.
    pub async fn sync(&self) -> Result<SyncOutcome, SdkError> {
        let body = self
            .client
            .call(
                "POST",
                "/v1/state-stores/sync",
                Some(&json!({ "store": self.name })),
            )
            .await?;
        Ok(decode_sync_outcome(&body))
    }

    /// Rebuild a feed-sourced store from retained history; `token` is the
    /// caller's retry identity for the pass.
    pub async fn rebuild(&self, token: Option<Uuid>) -> Result<SyncOutcome, SdkError> {
        let body = self
            .client
            .call(
                "POST",
                "/v1/state-stores/rebuild",
                Some(&json!({
                    "store": self.name,
                    "token": token.map(|value| value.to_string()),
                })),
            )
            .await?;
        Ok(decode_sync_outcome(&body))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Scripted transport: every call lands in a FIFO reply queue and is
    /// recorded for wire-shape assertions.
    struct StubTransport {
        replies: Mutex<std::collections::VecDeque<(u16, Value)>>,
        requests: Mutex<Vec<(String, String, Value)>>,
    }

    impl StubTransport {
        fn new(replies: Vec<(u16, Value)>) -> Self {
            Self {
                replies: Mutex::new(replies.into()),
                requests: Mutex::new(Vec::new()),
            }
        }

        fn take_requests(&self) -> Vec<(String, String, Value)> {
            std::mem::take(&mut self.requests.lock().unwrap())
        }
    }

    #[async_trait]
    impl Transport for StubTransport {
        async fn request(
            &self,
            _base: &str,
            method: &str,
            path: &str,
            body: Option<&Value>,
            _api_key: &str,
        ) -> Result<TransportReply, SdkError> {
            self.requests.lock().unwrap().push((
                method.to_owned(),
                path.to_owned(),
                body.cloned().unwrap_or(Value::Null),
            ));
            let (status, body) = self
                .replies
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or((500, json!({"error": "no scripted reply"})));
            Ok(TransportReply { status, body })
        }
    }

    fn client(replies: Vec<(u16, Value)>) -> (Client, Arc<StubTransport>) {
        let transport = Arc::new(StubTransport::new(replies));
        let client = Client::new(
            transport.clone(),
            vec!["http://node1".to_owned(), "http://node2".to_owned()],
            "test-key",
        );
        (client, transport)
    }

    #[test]
    fn error_mapping_follows_status_and_code_not_text() {
        for (status, code, want) in [
            (409, Some("stale_epoch"), SdkError::StaleEpoch),
            (409, Some("sequence"), SdkError::Sequence),
            (409, Some("lease.busy"), SdkError::LeaseBusy),
            (409, Some("lease.stale_lease"), SdkError::LeaseStale),
            (409, Some("lease.capacity"), SdkError::LeaseCapacity),
            (409, None, SdkError::Conflict("x".to_owned())),
            (503, None, SdkError::Unavailable("x".to_owned())),
            (502, None, SdkError::Ambiguous("x".to_owned())),
            (400, None, SdkError::Refused("400: x".to_owned())),
            (
                401,
                Some("whatever"),
                SdkError::Refused("401: x".to_owned()),
            ),
        ] {
            let body = json!({"error": "x", "code": code});
            assert_eq!(map_error(status, &body), want, "{status}/{code:?}");
        }
        assert!(SdkError::Ambiguous(String::new()).retryable());
        assert!(SdkError::Unavailable(String::new()).retryable());
        assert!(!SdkError::StaleEpoch.retryable());
    }

    #[tokio::test]
    async fn transport_limits_decode_advertised_bounds_and_refuse_gaps() {
        let (ready, _transport) = client(vec![(
            200,
            json!({
                "status": "ok",
                "protocol": {"major": 1, "minor": 0},
                "limits": {
                    "max_frame_bytes": 67108864,
                    "max_committed_read_bytes": 16777216,
                    "max_logical_read_frames": 10000,
                    "max_logical_read_ranges": 128,
                    "subscription_member_fetch_max_limit": 256,
                    "subscription_member_max_lease_ticks": 600
                }
            }),
        )]);
        let limits = ready.transport_limits().await.unwrap();
        assert_eq!(limits.max_frame_bytes, 67_108_864);
        assert_eq!(limits.max_committed_read_bytes, 16_777_216);
        assert_eq!(limits.member_fetch_max_limit, 256);
        assert_eq!(limits.member_max_lease_ticks, 600);

        let (missing, _transport) = client(vec![(
            200,
            json!({"status": "ok", "protocol": {"major": 1, "minor": 0}}),
        )]);
        assert!(matches!(
            missing.transport_limits().await,
            Err(SdkError::Protocol(_))
        ));
    }

    #[tokio::test]
    async fn writer_append_carries_epoch_request_id_and_decimal_nanos() {
        let (client, transport) = client(vec![(
            200,
            json!({
                "message_id": Uuid::nil().to_string(),
                "cursor": "cur-1",
                "deduplicated": false,
                "feedback": {"hot_key": true, "retry_after_ms": 250}
            }),
        )]);
        let writer = Writer {
            client: client.clone(),
            name: "orders.wr".to_owned(),
            session_epoch: 7,
        };
        let request_id = Uuid::new_v4();
        let outcome = writer
            .append(
                request_id,
                b"order-1",
                b"created",
                1_700_000_000_000_000_001,
                BTreeMap::from([("source".to_owned(), b"conformance".to_vec())]),
            )
            .await
            .unwrap();
        assert!(!outcome.deduplicated);
        assert!(outcome.hot_key);
        assert_eq!(outcome.retry_after_ms, Some(250));
        let requests = transport.take_requests();
        let (method, path, body) = &requests[0];
        assert_eq!(method, "POST");
        assert_eq!(path, "/v1/writers/append");
        assert_eq!(body["request_id"], request_id.to_string());
        assert_eq!(body["session_epoch"], 7);
        // Nanoseconds ride the wire as a decimal string, never a float.
        assert_eq!(body["event_time_ns"], "1700000000000000001");
        assert_eq!(body["key_base64"], STANDARD.encode(b"order-1"));
        assert_eq!(body["use_compression"], false);
    }

    #[tokio::test]
    async fn append_maps_stale_epoch_for_caller_reopen() {
        let (client, _transport) = client(vec![(
            409,
            json!({"error": "superseded", "code": "stale_epoch"}),
        )]);
        let writer = Writer {
            client,
            name: "orders.wr".to_owned(),
            session_epoch: 3,
        };
        let error = writer
            .append(Uuid::new_v4(), b"k", b"v", 1, BTreeMap::new())
            .await
            .unwrap_err();
        assert_eq!(error, SdkError::StaleEpoch);
    }

    #[tokio::test]
    async fn writer_reopen_pulls_a_fresh_epoch() {
        let (client, _transport) = client(vec![(
            200,
            json!({"results": [{"data": {"session_epoch": 9}}]}),
        )]);
        let mut writer = Writer {
            client,
            name: "orders.wr".to_owned(),
            session_epoch: 3,
        };
        writer.reopen().await.unwrap();
        assert_eq!(writer.session_epoch(), 9);
    }

    #[tokio::test]
    async fn reader_acks_the_page_delivered_cursor() {
        let (client, transport) = client(vec![
            (
                200,
                json!({
                    "records": [{
                        "message_id": Uuid::nil().to_string(),
                        "key_base64": STANDARD.encode(b"k"),
                        "payload_base64": STANDARD.encode(b"v"),
                        "metadata_base64": {},
                        "event_time_ns": "1700000000000000001",
                        "ingest_time_ns": "1700000000000000002",
                        "cursor": "record-cursor"
                    }],
                    "delivered_cursor": "page-cursor",
                    "acknowledged_cursor": "ack-0"
                }),
            ),
            (200, json!({"acknowledged_cursor": "page-cursor"})),
        ]);
        let mut reader = Reader {
            client,
            name: "orders.rd".to_owned(),
            session_epoch: 2,
            delivered_cursor: None,
            acknowledged_cursor: None,
        };
        let page = reader.fetch(16).await.unwrap();
        assert_eq!(page.records.len(), 1);
        assert_eq!(page.records[0].event_time_ns, 1_700_000_000_000_000_001);
        assert_eq!(page.delivered_cursor.as_deref(), Some("page-cursor"));
        reader.ack().await.unwrap();
        let requests = transport.take_requests();
        // The ack commits the page's delivered cursor, never the record's.
        assert_eq!(requests[1].2["cursor"], "page-cursor");
        assert_eq!(reader.acknowledged_cursor(), Some("page-cursor"));
    }

    #[tokio::test]
    async fn member_claim_surfaces_lease_classes() {
        let (client, _transport) =
            client(vec![(409, json!({"error": "held", "code": "lease.busy"}))]);
        let member = Member {
            client,
            subscription: "orders.flow".to_owned(),
            member_id: Uuid::new_v4(),
            member_epoch: 1,
        };
        assert_eq!(member.claim().await.unwrap_err(), SdkError::LeaseBusy);
    }

    #[tokio::test]
    async fn member_claim_decodes_the_granted_lease() {
        let (client, _transport) = client(vec![(
            200,
            json!({
                "member_epoch": 1,
                "lease": {
                    "work_id": Uuid::nil().to_string(),
                    "member_id": Uuid::nil().to_string(),
                    "member_epoch": 1,
                    "lease_epoch": 4,
                    "expires_at_tick": 130
                }
            }),
        )]);
        let member = Member {
            client,
            subscription: "orders.flow".to_owned(),
            member_id: Uuid::new_v4(),
            member_epoch: 1,
        };
        let work = member.claim().await.unwrap();
        assert_eq!(work.work_id, Uuid::nil());
        assert_eq!(work.lease_epoch, 4);
        assert_eq!(work.expires_at_tick, Some(130));
    }

    #[tokio::test]
    async fn member_ack_carries_fetch_evidence_verbatim() {
        let (client, transport) = client(vec![(200, json!({}))]);
        let member = Member {
            client,
            subscription: "orders.flow".to_owned(),
            member_id: Uuid::new_v4(),
            member_epoch: 1,
        };
        let work = ClaimedWork {
            work_id: Uuid::new_v4(),
            lease_epoch: 4,
            expires_at_tick: Some(130),
        };
        let page = WorkPage {
            records: Vec::new(),
            cursor: Some("frontier-9".to_owned()),
            positions: json!({"9f5c": "pos-3"}),
            delivery_attempt: 4,
            next_delivery_ns: None,
        };
        member.ack(&work, &page).await.unwrap();
        let requests = transport.take_requests();
        let body = &requests[0].2;
        assert_eq!(body["cursor"], "frontier-9");
        assert_eq!(body["positions"], json!({"9f5c": "pos-3"}));
        assert_eq!(body["lease_epoch"], 4);
        assert!(body.get("request_id").is_some());
    }

    #[test]
    fn typed_values_serialize_to_the_wire_tags() {
        assert_eq!(wire_typed(&TypedValue::Null), json!({"kind": "null"}));
        assert_eq!(
            wire_typed(&TypedValue::Bool(true)),
            json!({"kind": "bool", "value": true})
        );
        assert_eq!(
            wire_typed(&TypedValue::I64(-7)),
            json!({"kind": "i64", "value": -7})
        );
        assert_eq!(
            wire_typed(&TypedValue::Text("acme".to_owned())),
            json!({"kind": "text", "value": "acme"})
        );
        assert_eq!(
            wire_typed(&TypedValue::Bytes(b"\x00\xff".to_vec())),
            json!({"kind": "bytes", "value_base64": "AP8="})
        );
    }

    #[tokio::test]
    async fn put_row_carries_mutation_id_and_named_postings() {
        let (client, transport) = client(vec![(
            200,
            json!({
                "store_id": Uuid::nil().to_string(),
                "mutation_id": Uuid::nil().to_string(),
                "sequence": 3,
                "deduplicated": false
            }),
        )]);
        let store = client.state_store("crm.accounts");
        let outcome = store
            .put_row(
                Uuid::new_v4(),
                b"acct-1",
                b"{\"tier\":\"gold\"}",
                &[(
                    "region".to_owned(),
                    vec![TypedValue::Text("emea".to_owned())],
                )],
            )
            .await
            .unwrap();
        assert_eq!(outcome.sequence, 3);
        let requests = transport.take_requests();
        let body = &requests[0].2;
        assert_eq!(requests[0].1, "/v1/state-stores/mutate");
        assert_eq!(body["op"]["kind"], "put_row");
        assert_eq!(body["op"]["named"][0]["values"][0]["kind"], "text");
        assert!(body.get("mutation_id").is_some());
    }

    #[tokio::test]
    async fn get_by_sends_typed_values_and_decodes_freshness() {
        let (client, transport) = client(vec![(
            200,
            json!({
                "store_id": Uuid::nil().to_string(),
                "rows": [{"key_base64": STANDARD.encode(b"k"), "payload_base64": STANDARD.encode(b"p")}],
                "applied_seq": 41,
                "committed_seq": 43,
                "behind": true,
                "served_by": "node-2"
            }),
        )]);
        let page = client
            .state_store("crm.accounts")
            .get_by(
                "region",
                &[TypedValue::Text("emea".to_owned())],
                Some(10),
                true,
            )
            .await
            .unwrap();
        assert_eq!(page.rows.len(), 1);
        assert_eq!(page.rows[0].payload, b"p");
        assert!(page.freshness.behind);
        assert_eq!(page.freshness.applied_seq, 41);
        let requests = transport.take_requests();
        let body = &requests[0].2;
        assert_eq!(body["named_key"], "region");
        assert_eq!(body["strict"], true);
        assert_eq!(body["limit"], 10);
    }

    #[tokio::test]
    async fn index_range_query_flattens_mode_and_bounds() {
        let (client, transport) = client(vec![(
            200,
            json!({
                "index": "crm.by_region",
                "index_id": Uuid::nil().to_string(),
                "generation": 2,
                "rows": [],
                "applied_cursor": null,
                "target_cursor": null,
                "behind": false,
                "truncated": false,
                "served_by": "node-1"
            }),
        )]);
        client
            .query_index(
                "crm.by_region",
                IndexQuery::Range {
                    leading: vec![TypedValue::Text("emea".to_owned())],
                    lower: IndexBound {
                        value: TypedValue::I64(0),
                        inclusive: true,
                    },
                    upper: IndexBound {
                        value: TypedValue::I64(99),
                        inclusive: false,
                    },
                },
                Some(50),
                true,
            )
            .await
            .unwrap();
        let requests = transport.take_requests();
        let body = &requests[0].2;
        assert_eq!(body["mode"], "range");
        assert_eq!(body["leading"][0]["value"], "emea");
        assert_eq!(body["lower"]["inclusive"], true);
        assert_eq!(body["upper"]["inclusive"], false);
        assert_eq!(body["strict"], true);
    }

    #[tokio::test]
    async fn index_behind_maps_to_a_retryable_typed_error() {
        let error = map_error(409, &json!({"error": "freshness", "code": "index_behind"}));
        assert_eq!(error, SdkError::IndexBehind);
        assert!(error.retryable());
    }

    #[tokio::test]
    async fn temporary_fetch_carries_flags_and_decimal_nanos() {
        let (client, transport) = client(vec![(200, json!({"records": [], "next_cursor": "c-9"}))]);
        let page = client
            .temporary_fetch(
                "orders.events",
                TemporaryFetch {
                    after: Some("resume-1".to_owned()),
                    limit: 25,
                    tail: true,
                    wait_ms: Some(400),
                    after_event_time_ns: Some(1_700_000_000_000_000_001),
                    ..TemporaryFetch::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(page.next_cursor.as_deref(), Some("c-9"));
        let requests = transport.take_requests();
        let body = &requests[0].2;
        assert_eq!(body["tail"], true);
        assert_eq!(body["new_only"], false);
        assert_eq!(body["wait_ms"], 400);
        assert_eq!(body["after_event_time_ns"], "1700000000000000001");
    }

    #[tokio::test]
    async fn records_passes_the_opaque_cursor_verbatim() {
        let (client, transport) = client(vec![(
            200,
            json!([{
                "key_base64": STANDARD.encode(b"k"),
                "payload_base64": STANDARD.encode(b"v"),
                "metadata_base64": {},
                "event_time_ns": "5",
                "ingest_time_ns": "6"
            }]),
        )]);
        let records = client
            .records("orders.events", Some("opaque+cursor/bytes="), 10)
            .await
            .unwrap();
        assert_eq!(records.len(), 1);
        let requests = transport.take_requests();
        assert_eq!(requests[0].0, "GET");
        assert!(requests[0].1.contains("after=opaque%2Bcursor%2Fbytes%3D"));
    }

    #[tokio::test]
    async fn pipe_facade_wires_lifecycle_and_decodes_catalog_rows() {
        let subscription_id = uuid::Uuid::new_v4().to_string();
        let output_feed_id = uuid::Uuid::new_v4().to_string();
        let row = json!({
            "name": "orders.relay",
            "stage": "paused",
            "subscription_id": subscription_id,
            "operation": { "operation": "forward" },
            "output_feed_id": output_feed_id,
            "output_store_id": null,
            "status": "active",
            "created_at_ns": 42
        });
        let (client, transport) = client(vec![
            (200, json!({"results": [{"data": row}]})),
            (200, json!({"results": [{"data": [row]}]})),
            (200, json!({"results": [{}]})),
            (200, json!({"results": [{}]})),
            (200, json!({"results": [{}]})),
        ]);
        // describe decodes the catalog row verbatim.
        let described = client.pipe("orders.relay").describe().await.unwrap();
        assert_eq!(described.name, "orders.relay");
        assert_eq!(described.stage, "paused");
        assert_eq!(described.subscription_id.to_string(), subscription_id);
        assert_eq!(described.operation["operation"], "forward");
        assert_eq!(
            described.output_feed_id.unwrap().to_string(),
            output_feed_id
        );
        assert_eq!(described.output_store_id, None);
        // pipes lists the same row; lifecycle commands post the typed verbs.
        let listed = client.pipes().await.unwrap();
        assert_eq!(listed.len(), 1);
        client.pipe("orders.relay").pause().await.unwrap();
        client.pipe("orders.relay").resume().await.unwrap();
        client.pipe("orders.relay").drop().await.unwrap();
        let requests = transport.take_requests();
        let command = |index: usize| requests[index].2["commands"][0].clone();
        assert_eq!(command(0)["command"], "describe");
        assert_eq!(command(0)["kind"], "pipe");
        assert_eq!(command(0)["name"], "orders.relay");
        assert_eq!(command(1)["kind"], "pipes");
        assert_eq!(command(2)["command"], "pause_pipe");
        assert_eq!(command(3)["command"], "resume_pipe");
        assert_eq!(command(4)["command"], "drop");
        assert_eq!(command(4)["kind"], "pipe");
        assert!(requests
            .iter()
            .all(|request| request.1 == "/v1/admin/commands"));
    }
}
