//! Shared conformance-fixture runner for the `sdk/spec/conformance`
//! corpus — the same op/args/expect contract every language SDK's
//! conformance runner implements (`sdk/spec/conformance/README.md`).
//!
//! A runner wraps a [`Client`]; scenario-level `api_key` overrides are
//! honored by cloning the client's transport with a different key, so a
//! deliberately-invalid key exercises authentication refusal exactly as
//! an application would see it. Every fixture's `requires` ops are
//! checked against the runner's dispatch table — an unmapped op reports
//! `unsupported` explicitly rather than skipping silently.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use base64::Engine;
use serde_json::{json, Value};
use uuid::Uuid;

use base64::engine::general_purpose::STANDARD;

use crate::sdk::Client;

/// One scenario's outcome: `failure` is `None` on a pass, `Some` with
/// the first failing step otherwise. `unsupported` marks an op the
/// runner does not dispatch — per the corpus contract that is reported
/// explicitly, never skipped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScenarioReport {
    pub name: String,
    pub failure: Option<ScenarioFailure>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScenarioFailure {
    /// Zero-based index of the failing step inside `steps`.
    pub step: usize,
    pub op: String,
    /// `true` when the runner has no dispatch entry for `op`.
    pub unsupported: bool,
    /// The step's wire status when it reached the server, if it did.
    pub status: Option<u16>,
    pub message: String,
}

impl ScenarioReport {
    pub fn passed(&self) -> bool {
        self.failure.is_none()
    }
}

/// Executes `sdk/spec/conformance` scenarios through [`Client::wire`]
/// — the raw protocol escape hatch — so fixtures can assert exact
/// wire statuses and codes the typed facades map away.
pub struct ConformanceRunner {
    client: Client,
    chaos_hook: Option<ChaosHook>,
}

/// Driver-injected fault action: `chaos.<action>` steps call it with
/// the action name and the step passes when it returns `Ok`. The hook
/// owns the meaning of each action (restart a Node, partition, pause)
/// and must not return until the Riverbed is usable again — fixtures
/// intentionally carry no timing waits.
pub type ChaosHook = std::sync::Arc<dyn Fn(&str) -> Result<(), String> + Send + Sync>;

impl ConformanceRunner {
    pub fn new(client: Client) -> Self {
        Self {
            client,
            chaos_hook: None,
        }
    }

    /// Install the driver-provided fault executor for `chaos.*` steps.
    /// Without one, a scenario listing `chaos.*` ops reports
    /// `unsupported` — the corpus contract requires explicit reporting
    /// rather than a silent skip.
    pub fn with_chaos_hook(mut self, hook: ChaosHook) -> Self {
        self.chaos_hook = Some(hook);
        self
    }

    /// Run one parsed scenario: `{ "api_key"?: string, "steps": [...] }`.
    /// A scenario `api_key` replaces the runner client's credential for
    /// this scenario only.
    pub async fn run_scenario(&self, name: impl Into<String>, scenario: &Value) -> ScenarioReport {
        let name = name.into();
        let client = match scenario.get("api_key").and_then(Value::as_str) {
            Some(api_key) => self.client.with_api_key(api_key),
            None => self.client.clone(),
        };
        let mut context = RunContext {
            uniq: Uuid::new_v4().simple().to_string()[..8].to_owned(),
            named_uuids: BTreeMap::new(),
            bindings: BTreeMap::new(),
        };
        let steps = scenario
            .get("steps")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        for (index, step) in steps.iter().enumerate() {
            let op = step
                .get("op")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_owned();
            if op.starts_with("chaos.") {
                // A chaos step binds nothing and has no expect: its only
                // outcome is the hook returning or reporting failure. An
                // optional `args` object renders through the same
                // substitutions as request args and appends each scalar
                // value space-separated to the hook invocation, so a driver
                // action can name its target (for example the Feed whose
                // owner should restart) without fixtures hardcoding
                // placement.
                let args =
                    context.substitute(&step.get("args").cloned().unwrap_or_else(|| json!({})));
                let rendered: Vec<String> = args
                    .as_object()
                    .map(|values| {
                        values
                            .values()
                            .map(|value| match value {
                                Value::String(text) => text.clone(),
                                other => other.to_string(),
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                let action = if rendered.is_empty() {
                    op.clone()
                } else {
                    format!("{} {}", op, rendered.join(" "))
                };
                match &self.chaos_hook {
                    Some(hook) => match hook(&action) {
                        Ok(()) => continue,
                        Err(message) => {
                            return ScenarioReport {
                                name,
                                failure: Some(ScenarioFailure {
                                    step: index,
                                    op,
                                    unsupported: false,
                                    status: None,
                                    message,
                                }),
                            }
                        }
                    },
                    None => {
                        return ScenarioReport {
                            name,
                            failure: Some(ScenarioFailure {
                                step: index,
                                op,
                                unsupported: true,
                                status: None,
                                message: "no chaos hook installed".to_owned(),
                            }),
                        }
                    }
                }
            }
            if endpoint(&op).is_none()
                && !matches!(op.as_str(), "feed.records" | "health" | "statestore.mutate")
            {
                return ScenarioReport {
                    name,
                    failure: Some(ScenarioFailure {
                        step: index,
                        op,
                        unsupported: true,
                        status: None,
                        message: "unsupported op".to_owned(),
                    }),
                };
            }
            let args = context.substitute(&step.get("args").cloned().unwrap_or_else(|| json!({})));
            // `poll` re-dispatches the step until `expect` passes (or
            // dispatch stops erroring): asynchronous surfaces — Pipe
            // drives, index builds, replica catch-up — settle on their
            // own schedule, so the corpus carries one bounded explicit
            // retry instead of sleeps. `times` is clamped to 30 and
            // `interval_ms` to 10s to keep suites honest.
            let poll_times = step
                .get("poll")
                .and_then(|poll| poll.get("times"))
                .and_then(Value::as_u64)
                .unwrap_or(1)
                .clamp(1, 30);
            let poll_interval_ms = step
                .get("poll")
                .and_then(|poll| poll.get("interval_ms"))
                .and_then(Value::as_u64)
                .unwrap_or(500)
                .min(10_000);
            let mut attempt = 0u64;
            let (_status, body) = loop {
                attempt += 1;
                match dispatch(&client, &op, &args).await {
                    Err(message) => {
                        if attempt < poll_times {
                            tokio::time::sleep(Duration::from_millis(poll_interval_ms)).await;
                            continue;
                        }
                        return ScenarioReport {
                            name,
                            failure: Some(ScenarioFailure {
                                step: index,
                                op,
                                unsupported: false,
                                status: None,
                                message,
                            }),
                        };
                    }
                    Ok((status, body)) => {
                        if let Some(expect) = step.get("expect").and_then(Value::as_object) {
                            // Literal expectations can carry tokens
                            // (`"sdk{{uniq}}.events"`); operator strings
                            // (`!=null`, `len>=`, `==bind.path`) contain no
                            // braces and pass substitution untouched.
                            let expect_value = context.substitute(&Value::Object(expect.clone()));
                            let expect_map = expect_value.as_object().cloned().unwrap_or_default();
                            if let Err(message) =
                                check_expect(&expect_map, status, &body, &context.bindings)
                            {
                                if attempt < poll_times {
                                    tokio::time::sleep(Duration::from_millis(poll_interval_ms))
                                        .await;
                                    continue;
                                }
                                return ScenarioReport {
                                    name,
                                    failure: Some(ScenarioFailure {
                                        step: index,
                                        op,
                                        unsupported: false,
                                        status: Some(status),
                                        message,
                                    }),
                                };
                            }
                        }
                        break (status, body);
                    }
                }
            };
            if let Some(bind) = step.get("bind").and_then(Value::as_str) {
                context.bindings.insert(bind.to_owned(), body.clone());
            }
        }
        ScenarioReport {
            name,
            failure: None,
        }
    }

    /// Run every `*.json` scenario under `directory` in filename order.
    /// Missing or unparseable files surface as failed reports rather
    /// than aborting the set.
    pub async fn run_directory(&self, directory: impl AsRef<Path>) -> Vec<ScenarioReport> {
        let mut fixtures: Vec<PathBuf> = std::fs::read_dir(directory)
            .map(|entries| {
                entries
                    .filter_map(|entry| entry.ok().map(|item| item.path()))
                    .filter(|path| path.extension().map(|ext| ext == "json").unwrap_or(false))
                    .collect()
            })
            .unwrap_or_default();
        fixtures.sort();
        let mut reports = Vec::new();
        for fixture in fixtures {
            let name = fixture
                .file_name()
                .map(|value| value.to_string_lossy().into_owned())
                .unwrap_or_default();
            let report = match std::fs::read_to_string(&fixture)
                .ok()
                .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
            {
                Some(scenario) => self.run_scenario(name, &scenario).await,
                None => ScenarioReport {
                    name,
                    failure: Some(ScenarioFailure {
                        step: 0,
                        op: "load".to_owned(),
                        unsupported: false,
                        status: None,
                        message: "fixture unreadable or invalid JSON".to_owned(),
                    }),
                },
            };
            reports.push(report);
        }
        reports
    }
}

/// Resolve `a.b[0].c` over a bound Value: dotted fields with `[n]`
/// array indexing.
fn get_path<'v>(value: &'v Value, path: &str) -> Option<&'v Value> {
    let mut current = value;
    for segment in path.split('.') {
        let (field, indices) = match segment.split_once('[') {
            Some((field, rest)) => (field, rest),
            None => (segment, ""),
        };
        if !field.is_empty() {
            current = current.get(field)?;
        }
        for index in indices
            .split(']')
            .filter_map(|part| part.trim_start_matches('[').parse::<usize>().ok())
        {
            current = current.get(index)?;
        }
    }
    Some(current)
}

struct RunContext {
    uniq: String,
    named_uuids: BTreeMap<String, String>,
    bindings: BTreeMap<String, Value>,
}

impl RunContext {
    fn named_uuid(&mut self, name: &str) -> String {
        self.named_uuids
            .entry(name.to_owned())
            .or_insert_with(|| Uuid::new_v4().to_string())
            .clone()
    }

    /// Substitute `{{uniq}}`, `{{uuid}}`, `{{uuid:name}}`, and
    /// `{{binding.path}}` tokens inside every string leaf. A string that
    /// is exactly one binding token adopts the bound value's own type
    /// (numbers, objects, arrays) rather than stringifying it.
    fn substitute(&mut self, value: &Value) -> Value {
        match value {
            Value::String(text) => self.substitute_string(text),
            Value::Array(items) => {
                Value::Array(items.iter().map(|item| self.substitute(item)).collect())
            }
            Value::Object(map) => Value::Object(
                map.iter()
                    .map(|(key, item)| (key.clone(), self.substitute(item)))
                    .collect(),
            ),
            other => other.clone(),
        }
    }

    fn substitute_string(&mut self, text: &str) -> Value {
        // Whole-string single token: preserve the resolved type.
        if let Some(inner) = text
            .strip_prefix("{{")
            .and_then(|rest| rest.strip_suffix("}}"))
        {
            if let Some(value) = self.resolve_token(inner.trim()) {
                return value;
            }
        }
        let mut out = String::new();
        let mut rest = text;
        while let Some(start) = rest.find("{{") {
            out.push_str(&rest[..start]);
            let end = rest[start..]
                .find("}}")
                .map(|offset| start + offset)
                .unwrap_or(rest.len() - 2);
            let token = rest[start + 2..end].trim();
            match self.resolve_token(token) {
                Some(Value::String(resolved)) => out.push_str(&resolved),
                Some(other) => out.push_str(&other.to_string()),
                None => out.push_str("null"),
            }
            rest = &rest[end + 2..];
        }
        out.push_str(rest);
        Value::String(out)
    }

    fn resolve_token(&mut self, token: &str) -> Option<Value> {
        if token == "uniq" {
            return Some(Value::String(self.uniq.clone()));
        }
        if token == "uuid" {
            return Some(Value::String(Uuid::new_v4().to_string()));
        }
        if let Some(name) = token.strip_prefix("uuid:") {
            return Some(Value::String(self.named_uuid(name)));
        }
        let (binding, path) = token.split_once('.')?;
        self.bindings
            .get(binding)
            .and_then(|bound| get_path(bound, path))
            .cloned()
    }
}

/// Decode a fixture `raw` argument: `{"utf8": "..."}` or `{"hex": ".."}`.
fn raw_bytes(value: &Value) -> Vec<u8> {
    if let Some(text) = value.get("utf8").and_then(Value::as_str) {
        return text.as_bytes().to_vec();
    }
    if let Some(hex) = value.get("hex").and_then(Value::as_str) {
        return (0..hex.len().saturating_sub(1))
            .step_by(2)
            .filter_map(|index| u8::from_str_radix(&hex[index..index + 2], 16).ok())
            .collect();
    }
    // Already a plain string: treat as utf8 bytes (fixtures use raw
    // objects, but a plain value cannot mean anything else here).
    value
        .as_str()
        .map_or_else(Vec::new, |text| text.as_bytes().to_vec())
}

/// Lower one fixture step's args into the wire body: `key`/`payload`
/// raw objects become `*_base64`, `metadata` becomes `metadata_base64`,
/// and nanosecond numbers become decimal strings. `records[]` entries
/// (batch ops) transform the same way.
fn wire_args(args: &Value) -> Value {
    let mut body = args.clone();
    if let Some(records) = body.get_mut("records").and_then(Value::as_array_mut) {
        for record in records {
            wire_record_fields(record);
        }
    }
    wire_record_fields(&mut body);
    body
}

fn wire_record_fields(body: &mut Value) {
    let Some(map) = body.as_object_mut() else {
        return;
    };
    for (raw, encoded) in [
        ("key", "key_base64"),
        ("payload", "payload_base64"),
        ("after_key", "after_key_base64"),
    ] {
        if let Some(value) = map.remove(raw) {
            map.insert(
                encoded.to_owned(),
                Value::String(STANDARD.encode(raw_bytes(&value))),
            );
        }
    }
    if let Some(metadata) = map.remove("metadata") {
        let encoded: serde_json::Map<String, Value> = metadata
            .as_object()
            .map(|entries| {
                entries
                    .iter()
                    .map(|(name, value)| {
                        (
                            name.clone(),
                            Value::String(STANDARD.encode(raw_bytes(value))),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default();
        map.insert("metadata_base64".to_owned(), Value::Object(encoded));
    }
    for field in ["event_time_ns", "after_event_time_ns"] {
        if let Some(number) = map.get(field).and_then(Value::as_i64) {
            map.insert(field.to_owned(), Value::String(number.to_string()));
        }
    }
}

fn endpoint(op: &str) -> Option<(&'static str, &'static str)> {
    Some(match op {
        "wcl" | "admin.wcl" => ("POST", "/v1/admin/wcl"),
        "admin.commands" => ("POST", "/v1/admin/commands"),
        "writer.append" => ("POST", "/v1/writers/append"),
        "writer.append_batch" => ("POST", "/v1/writers/append-batch"),
        "reader.open" => ("POST", "/v1/readers/open"),
        "reader.fetch" => ("POST", "/v1/readers/fetch"),
        "reader.ack" => ("POST", "/v1/readers/ack"),
        "reader.close" => ("POST", "/v1/readers/close"),
        "reader.temporary_fetch" => ("POST", "/v1/readers/temporary/fetch"),
        "feed.append" => ("POST", "/v1/feeds/append"),
        "member.join" => ("POST", "/v1/subscriptions/members/join"),
        "member.claim" => ("POST", "/v1/subscriptions/members/claim"),
        "member.renew" => ("POST", "/v1/subscriptions/members/renew"),
        "member.release" => ("POST", "/v1/subscriptions/members/release"),
        "member.transfer" => ("POST", "/v1/subscriptions/members/transfer"),
        "member.fetch" => ("POST", "/v1/subscriptions/members/fetch"),
        "member.ack" => ("POST", "/v1/subscriptions/members/ack"),
        "statestore.lookup" => ("POST", "/v1/state-stores/lookup"),
        "statestore.get_by" => ("POST", "/v1/state-stores/get-by"),
        "statestore.iterate" => ("POST", "/v1/state-stores/iterate"),
        "statestore.clear" => ("POST", "/v1/state-stores/clear"),
        _ => return None,
    })
}

async fn dispatch(client: &Client, op: &str, args: &Value) -> Result<(u16, Value), String> {
    if op == "health" {
        let reply = client
            .wire("GET", "/health", None)
            .await
            .map_err(|error| format!("transport: {error:?}"))?;
        return Ok((reply.status, reply.body));
    }
    if op == "feed.records" {
        let mut path = format!(
            "/v1/feeds/records?feed={}",
            args.get("feed").and_then(Value::as_str).unwrap_or("")
        );
        if let Some(limit) = args.get("limit") {
            path.push_str(&format!("&limit={limit}"));
        }
        if let Some(after) = args.get("after").and_then(Value::as_str) {
            path.push_str(&format!("&after={}", urlencoding(after)));
        }
        let reply = client
            .wire("GET", &path, None)
            .await
            .map_err(|error| format!("transport: {error:?}"))?;
        // The op's logical result is `{records: [...]}`; the wire carries
        // a bare array on success, so wrap it for `records[n]` paths.
        let body = match reply.body {
            Value::Array(records) => json!({"records": records}),
            other => other,
        };
        return Ok((reply.status, body));
    }
    if op == "statestore.mutate" {
        // `{store, mutation_id, kind, key, payload, named}` lowers to
        // `{store, mutation_id, op: {kind, key_base64?, payload_base64?,
        // named?}}` — the flat fixture vocabulary stays identical to the
        // other ops while the wire nests the mutation under `op`.
        let mut body = wire_args(args);
        let Some(map) = body.as_object_mut() else {
            return Err("statestore.mutate args must be an object".to_owned());
        };
        let mut operation = serde_json::Map::new();
        let kind = map
            .remove("kind")
            .and_then(|value| value.as_str().map(str::to_owned))
            .ok_or_else(|| "statestore.mutate needs a `kind`".to_owned())?;
        operation.insert("kind".to_owned(), Value::String(kind));
        for field in ["key_base64", "payload_base64", "named"] {
            if let Some(value) = map.remove(field) {
                operation.insert(field.to_owned(), value);
            }
        }
        map.insert("op".to_owned(), Value::Object(operation));
        let reply = client
            .wire("POST", "/v1/state-stores/mutate", Some(&body))
            .await
            .map_err(|error| format!("transport: {error:?}"))?;
        return Ok((reply.status, reply.body));
    }
    let (method, path) = endpoint(op).ok_or_else(|| format!("unsupported op {op}"))?;
    let body = wire_args(args);
    let reply = client
        .wire(method, path, Some(&body))
        .await
        .map_err(|error| format!("transport: {error:?}"))?;
    Ok((reply.status, reply.body))
}

fn urlencoding(value: &str) -> String {
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

/// Evaluate one `expect` map against a step's response. `error_status`
/// and `error_code` are reserved keys asserted against the wire status
/// and the reply's `code`; everything else is a path into the body.
fn check_expect(
    expect: &serde_json::Map<String, Value>,
    status: u16,
    body: &Value,
    bindings: &BTreeMap<String, Value>,
) -> Result<(), String> {
    for (key, expected) in expect {
        match key.as_str() {
            "error_status" => {
                let want = expected.as_u64().unwrap_or(0) as u16;
                if status != want {
                    return Err(format!("expected status {want}, got {status}: {body}"));
                }
            }
            "error_code" => {
                let want = expected.as_str().unwrap_or("");
                let got = body.get("code").and_then(Value::as_str).unwrap_or("");
                if got != want {
                    return Err(format!("expected code {want}, got body {body}"));
                }
            }
            path => {
                let actual = get_path(body, path);
                match expected.as_str() {
                    Some("present") => match actual {
                        Some(value) if !value.is_null() => {}
                        _ => return Err(format!("{path} absent in {body}")),
                    },
                    Some("!=null") => match actual {
                        Some(value) if !value.is_null() => {}
                        _ => return Err(format!("{path} null in {body}")),
                    },
                    Some("absent") => match actual {
                        None | Some(Value::Null) => {}
                        Some(value) => return Err(format!("{path} present: {value}")),
                    },
                    Some(bound) if bound.starts_with("==") => {
                        let (name, field) = bound[2..].split_once('.').unwrap_or((&bound[2..], ""));
                        let want = bindings.get(name).and_then(|value| get_path(value, field));
                        if actual != want {
                            return Err(format!(
                                "{path} = {actual:?}, expected {want:?} ({bound})"
                            ));
                        }
                    }
                    Some(length) if length.starts_with("len>=") => {
                        let want: usize = length[5..].parse().unwrap_or(0);
                        let got = actual
                            .and_then(|value| value.as_array().map(Vec::len))
                            .or_else(|| actual.and_then(|value| value.as_str().map(str::len)))
                            .unwrap_or(0);
                        if got < want {
                            return Err(format!("{path} len {got} < {want} in {body}"));
                        }
                    }
                    _ => {
                        if actual != Some(expected) {
                            return Err(format!(
                                "{path} = {actual:?}, expected {expected} in {body}"
                            ));
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use async_trait::async_trait;

    use crate::sdk::{Transport, TransportReply};

    struct StubTransport {
        replies: Mutex<std::collections::VecDeque<(u16, Value)>>,
        requests: Mutex<Vec<(String, String, Value)>>,
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
        ) -> Result<TransportReply, crate::sdk::SdkError> {
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
                .unwrap_or((500, json!({"error": "unscripted"})));
            Ok(TransportReply { status, body })
        }
    }

    fn runner(replies: Vec<(u16, Value)>) -> (ConformanceRunner, std::sync::Arc<StubTransport>) {
        let transport = std::sync::Arc::new(StubTransport {
            replies: Mutex::new(replies.into()),
            requests: Mutex::new(Vec::new()),
        });
        let client = Client::new(
            transport.clone(),
            vec!["http://node1".to_owned()],
            "test-key",
        );
        (ConformanceRunner::new(client), transport)
    }

    #[tokio::test]
    async fn runner_executes_steps_with_substitution_bindings_and_expects() {
        let (runner, transport) = runner(vec![
            (200, json!({"results": [{"data": {"cursor": "abc;123"}}]})),
            (200, json!([{"cursor": "abc;123", "key_base64": "aw=="}])),
        ]);
        let scenario = json!({
            "steps": [
                {
                    "op": "wcl",
                    "args": {"script": "CREATE FEED sdk{{uniq}}.events;"},
                    "bind": "setup",
                    "expect": {"results[0].data.cursor": "present"}
                },
                {
                    "op": "feed.records",
                    "args": {"feed": "sdk{{uniq}}.events", "after": "{{setup.results[0].data.cursor}}"},
                    "expect": {"records[0].cursor": "==setup.results[0].data.cursor"}
                }
            ]
        });
        let report = runner.run_scenario("mini", &scenario).await;
        assert!(report.passed(), "{report:?}");
        let requests = transport.requests.lock().unwrap();
        assert_eq!(requests[0].1, "/v1/admin/wcl");
        assert!(requests[1].1.starts_with("/v1/feeds/records?feed=sdk"));
        assert!(requests[1].1.contains("after=abc%3B123"));
    }

    #[tokio::test]
    async fn runner_dispatches_chaos_steps_through_the_hook() {
        let actions = std::sync::Arc::new(Mutex::new(Vec::new()));
        let (base, _) = runner(vec![(200, json!({"results": []}))]);
        let seen = actions.clone();
        let hooked = base.with_chaos_hook(std::sync::Arc::new(move |action| {
            seen.lock().unwrap().push(action.to_owned());
            Ok(())
        }));
        let report = hooked
            .run_scenario(
                "c",
                &json!({"steps": [
                    {"op": "chaos.restart"},
                    {"op": "chaos.restart_owner", "args": {"feed": "{{uuid:target}}.events"}},
                    {"op": "wcl", "args": {"script": "SHOW FEEDS;"}, "expect": {"results": []}}
                ]}),
            )
            .await;
        assert!(report.passed(), "{report:?}");
        let seen_actions = actions.lock().unwrap().clone();
        assert_eq!(seen_actions[0], "chaos.restart");
        assert!(seen_actions[1].starts_with("chaos.restart_owner "));
        assert!(seen_actions[1].ends_with(".events"));

        // Without a hook the same op reports unsupported explicitly.
        let (unhooked, _) = runner(vec![]);
        let report = unhooked
            .run_scenario("u", &json!({"steps": [{"op": "chaos.restart"}]}))
            .await;
        let failure = report.failure.expect("unsupported reported");
        assert!(failure.unsupported);
        assert!(failure.message.contains("no chaos hook"));

        // A hook failure surfaces as a normal step failure.
        let (base, _) = runner(vec![]);
        let failing = base.with_chaos_hook(std::sync::Arc::new(|_| {
            Err("riverbed never came back".to_owned())
        }));
        let report = failing
            .run_scenario("f", &json!({"steps": [{"op": "chaos.restart"}]}))
            .await;
        let failure = report.failure.expect("chaos failure reported");
        assert!(!failure.unsupported);
        assert_eq!(failure.message, "riverbed never came back");
    }

    #[tokio::test]
    async fn runner_lowers_statestore_mutate_into_the_op_envelope() {
        let (runner, transport) = runner(vec![
            (
                200,
                json!({"store_id": "s1", "mutation_id": "m1", "sequence": 1, "deduplicated": false}),
            ),
            (200, json!({"found": true, "payload_base64": "YWxpY2U="})),
        ]);
        let report = runner
            .run_scenario(
                "ss",
                &json!({"steps": [
                    {
                        "op": "statestore.mutate",
                        "args": {
                            "store": "ss.profiles",
                            "mutation_id": "11111111-1111-1111-1111-111111111111",
                            "kind": "put_row",
                            "key": {"utf8": "emp-1"},
                            "payload": {"utf8": "alice"},
                            "named": [{"name": "department", "values": [{"kind": "text", "value": "eng"}]}]
                        },
                        "expect": {"deduplicated": false}
                    },
                    {
                        "op": "statestore.lookup",
                        "args": {"store": "ss.profiles", "key": {"utf8": "emp-1"}},
                        "expect": {"found": true, "payload_base64": "YWxpY2U="}
                    }
                ]}),
            )
            .await;
        assert!(report.passed(), "{report:?}");
        let requests = transport.requests.lock().unwrap();
        assert_eq!(requests[0].1, "/v1/state-stores/mutate");
        let body = &requests[0].2;
        assert_eq!(body["store"], "ss.profiles");
        assert_eq!(body["op"]["kind"], "put_row");
        // Flat `key`/`payload` fixture args become base64 inside `op`.
        assert_eq!(body["op"]["key_base64"], "ZW1wLTE=");
        assert_eq!(body["op"]["payload_base64"], "YWxpY2U=");
        assert_eq!(body["op"]["named"][0]["name"], "department");
        // The lookup op uses the same lowering at the top level.
        assert_eq!(requests[1].1, "/v1/state-stores/lookup");
        assert_eq!(requests[1].2["key_base64"], "ZW1wLTE=");
    }

    #[tokio::test]
    async fn runner_polls_until_expect_passes_then_binds() {
        // First attempt returns an empty page; the poll retry sees the
        // record the asynchronous drive eventually produced.
        let (runner, transport) = runner(vec![
            (200, json!({"records": []})),
            (
                200,
                json!({"records": [{"payload_base64": "Zm9yd2FyZGVk"}]}),
            ),
            (
                200,
                json!({"records": [{"payload_base64": "Zm9yd2FyZGVk"}]}),
            ),
        ]);
        let report = runner
            .run_scenario(
                "poll",
                &json!({"steps": [
                    {
                        "op": "feed.records",
                        "args": {"feed": "p.events"},
                        "expect": {"records[0].payload_base64": "Zm9yd2FyZGVk"},
                        "bind": "seen",
                        "poll": {"times": 5, "interval_ms": 1}
                    },
                    {
                        "op": "feed.records",
                        "args": {"feed": "p.events"},
                        "expect": {"records[0].payload_base64": "==seen.records[0].payload_base64"}
                    }
                ]}),
            )
            .await;
        assert!(report.passed(), "{report:?}");
        // Two dispatches for the polled step (empty then populated) plus
        // one for the bind-consuming step.
        assert_eq!(transport.requests.lock().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn runner_exhausts_poll_and_reports_the_last_failure() {
        let (runner, _) = runner(vec![
            (200, json!({"records": []})),
            (200, json!({"records": []})),
            (200, json!({"records": []})),
        ]);
        let report = runner
            .run_scenario(
                "exhaust",
                &json!({"steps": [
                    {
                        "op": "feed.records",
                        "args": {"feed": "p.events"},
                        "expect": {"records": "len>=1"},
                        "poll": {"times": 3, "interval_ms": 1}
                    }
                ]}),
            )
            .await;
        let failure = report.failure.expect("poll exhaustion reported");
        assert!(!failure.unsupported);
        assert_eq!(failure.step, 0);
        assert_eq!(failure.status, Some(200));
    }

    #[tokio::test]
    async fn runner_reports_unsupported_ops_and_expect_failures() {
        let (runner, _) = runner(vec![(400, json!({"code": "bad"}))]);
        let unsupported = runner
            .run_scenario("u", &json!({"steps": [{"op": "mystery.op"}]}))
            .await;
        let failure = unsupported.failure.expect("unsupported reported");
        assert!(failure.unsupported);
        assert_eq!(failure.step, 0);
        let failed = runner
            .run_scenario(
                "f",
                &json!({"steps": [{"op": "wcl", "args": {"script": "x"}, "expect": {"error_status": 200}}]}),
            )
            .await;
        let failure = failed.failure.expect("expect failure reported");
        assert!(!failure.unsupported);
        assert_eq!(failure.status, Some(400));
        assert!(failure.message.contains("expected status 200"));
    }
}
