//! In-memory [`Transport`] double for application unit tests: `FakeTransport`
//! models the wire semantics an app-facing `Client` observes — Writer session
//! fencing and `request_id` dedup, Reader delivered/acknowledged progress,
//! StateStore mutation identities — without a live Riverbed, HTTP listener,
//! or ports. Drive it through the same `Client` calls production uses; the
//! catalog surface (`/v1/admin/wcl`, `/v1/admin/commands`) parses real WCL
//! and typed commands so declarations feed the data plane.
//!
//! ```ignore
//! let client = FakeTransport::client();
//! client
//!     .wcl("CREATE DOMAIN t; CREATE FEED t.events; CREATE WRITER w TO t.events;")
//!     .await?;
//! let writer = client.writer("w").await?;
//! ```

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::control::{self, Command, ControlError, ReaderStart, ResourceKind, ShowKind};
use crate::sdk::{Client, SdkError, Transport, TransportReply};

const NODE: &str = "fake-node-1";

fn ok(body: Value) -> TransportReply {
    TransportReply { status: 200, body }
}

fn refused(status: u16, message: impl Into<String>, code: Option<&str>) -> TransportReply {
    let mut body = json!({ "error": message.into() });
    if let Some(code) = code {
        body["code"] = json!(code);
    }
    TransportReply { status, body }
}

fn bad_request(message: impl Into<String>) -> TransportReply {
    refused(400, message, None)
}

/// The public session-fencing contract: superseded epochs surface as a
/// machine-actionable stale-epoch conflict so `SdkError::StaleEpoch` fires.
fn stale_epoch(message: impl Into<String>) -> TransportReply {
    refused(409, message, Some("stale_epoch"))
}

fn conflict(message: impl Into<String>, code: Option<&str>) -> TransportReply {
    refused(409, message, code)
}

fn unavailable(message: impl Into<String>) -> TransportReply {
    refused(503, message, None)
}

#[derive(Clone)]
struct FakeRecord {
    cursor: String,
    message_id: Uuid,
    producer_id: Uuid,
    sequence: u64,
    event_time_ns: i64,
    ingest_time_ns: i64,
    key: Vec<u8>,
    payload: Vec<u8>,
    metadata: BTreeMap<String, Vec<u8>>,
}

impl FakeRecord {
    fn wire(&self, feed_name: &str, feed_id: Uuid) -> Value {
        json!({
            "cursor": self.cursor,
            "message_id": self.message_id,
            "producer_id": self.producer_id,
            "sequence": self.sequence,
            "event_time_ns": self.event_time_ns.to_string(),
            "ingest_time_ns": self.ingest_time_ns.to_string(),
            "key_base64": STANDARD.encode(&self.key),
            "payload_base64": STANDARD.encode(&self.payload),
            "metadata_base64": self
                .metadata
                .iter()
                .map(|(name, value)| (name.clone(), STANDARD.encode(value)))
                .collect::<BTreeMap<String, String>>(),
            "source": {
                "feed_name": feed_name.rsplit('.').next().unwrap_or(feed_name),
                "full_feed_name": feed_name,
                "feed_id": feed_id,
            },
        })
    }
}

struct FakeFeed {
    feed_id: Uuid,
    space_id: Uuid,
    records: Vec<FakeRecord>,
}

struct FakeAppend {
    message_id: Uuid,
    cursor: String,
    /// The request's record bytes; a `request_id` retry carrying different
    /// content conflicts instead of silently deduping.
    identity: Value,
}

struct FakeWriter {
    writer_id: Uuid,
    feed: String,
    session_epoch: u64,
    session_active: bool,
    next_sequence: u64,
    /// (session_epoch, request_id) -> committed outcome; a reopened session
    /// is a fresh dedup scope.
    appends: HashMap<(u64, String), FakeAppend>,
}

struct FakeReader {
    reader_id: Uuid,
    feed: String,
    start: ReaderStart,
    acknowledged_cursor: Option<String>,
    delivered_cursor: Option<String>,
    /// (fetch request_id, previous delivered cursor, served limit) — the
    /// replay bracket a duplicate fetch request re-derives.
    last_delivery: Option<(Uuid, Option<String>, usize)>,
    session_epoch: u64,
    session_capacity: usize,
    session_active: bool,
    /// Position a `START AT NOW` session was pinned to at open.
    start_index: usize,
}

struct FakeRow {
    payload: Vec<u8>,
    /// (declared named-key name, typed wire values) postings.
    named: Vec<(String, Vec<Value>)>,
    applied_seq: u64,
}

struct FakeStore {
    store_id: Uuid,
    space_id: Uuid,
    source_feed: Option<Uuid>,
    /// (name, unique) declarations; put_row may reference only these.
    named_keys: Vec<(String, bool)>,
    rows: BTreeMap<Vec<u8>, FakeRow>,
    sequence: u64,
    /// mutation_id -> (journal sequence, normalized op) for dedup.
    mutations: HashMap<Uuid, (u64, Value)>,
}

struct FakeSubscription {
    subscription_id: Uuid,
    space_id: Uuid,
    feed_id: Option<Uuid>,
    domain_id: Option<Uuid>,
    start: ReaderStart,
}

struct FakePipe {
    pipe_id: Uuid,
    space_id: Uuid,
    subscription_id: Uuid,
    operation: Value,
    output_feed_id: Option<Uuid>,
    output_store_id: Option<Uuid>,
    stage: &'static str,
    created_at_ns: i64,
}

#[derive(Default)]
struct FakeRiverbed {
    domains: BTreeMap<String, Uuid>,
    feeds: BTreeMap<String, FakeFeed>,
    writers: BTreeMap<String, FakeWriter>,
    readers: BTreeMap<String, FakeReader>,
    subscriptions: BTreeMap<String, FakeSubscription>,
    stores: BTreeMap<String, FakeStore>,
    pipes: BTreeMap<String, FakePipe>,
    indexes: BTreeMap<String, Value>,
    /// `"domain:<name>"`/`"feed:<name>"` -> declared policy override.
    history_policies: BTreeMap<String, crate::control::HistoryPolicy>,
    /// Feed name -> declared legal hold (the mock fences DROP FEED on it).
    legal_holds: BTreeMap<String, crate::control::LegalHold>,
    /// `"domain:<name>"`/`"feed:<name>"` -> declared schema override.
    schemas: BTreeMap<String, crate::control::FeedSchema>,
    revision: u64,
    cursors: u64,
}

/// A [`Transport`] that answers the public wire surface from an in-memory
/// model: the same statuses, codes, and reply fields a Node serves. Cheap
/// `Clone` shares the model across endpoint-pool `Client`s.
#[derive(Clone)]
pub struct FakeTransport {
    state: Arc<Mutex<FakeRiverbed>>,
}

impl Default for FakeTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl FakeTransport {
    /// A fresh, empty Riverbed model.
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(FakeRiverbed::default())),
        }
    }

    /// One-call test setup: a `Client` bound to a fresh fake. Declare
    /// resources through `wcl`/`commands`, then use `writer`, `reader`,
    /// `state_store`, and friends exactly as in production.
    pub fn client() -> Client {
        Client::new(
            Arc::new(Self::new()),
            vec!["http://fake-node".to_owned()],
            "test-key",
        )
    }
}

#[async_trait]
impl Transport for FakeTransport {
    async fn request(
        &self,
        _base: &str,
        method: &str,
        path: &str,
        body: Option<&Value>,
        _api_key: &str,
    ) -> Result<TransportReply, SdkError> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .handle(method, path, body.cloned()))
    }
}

enum MutateOp {
    Put {
        key: Vec<u8>,
        payload: Vec<u8>,
        named: Vec<(String, Vec<Value>)>,
    },
    Delete {
        key: Vec<u8>,
    },
}

impl FakeRiverbed {
    fn handle(&mut self, method: &str, path: &str, body: Option<Value>) -> TransportReply {
        let (route, query) = path.split_once('?').unwrap_or((path, ""));
        let body = body.unwrap_or(Value::Null);
        match (method, route) {
            ("GET", "/health") => ok(health_body()),
            ("POST", "/v1/admin/wcl") => self.admin_wcl(&body),
            ("POST", "/v1/admin/commands") => self.admin_commands(&body),
            ("POST", "/v1/writers/append") => self.writer_append(&body),
            ("POST", "/v1/writers/append-batch") => self.writer_append_batch(&body),
            ("POST", "/v1/readers/open") => self.reader_open(&body),
            ("POST", "/v1/readers/fetch") => self.reader_fetch(&body),
            ("POST", "/v1/readers/ack") => self.reader_ack(&body),
            ("POST", "/v1/readers/close") => self.reader_close(&body),
            ("POST", "/v1/readers/temporary/fetch") => self.temporary_fetch(&body),
            ("GET", "/v1/feeds/records") => self.feed_records(query),
            ("POST", "/v1/state-stores/mutate") => self.store_mutate(&body),
            ("POST", "/v1/state-stores/lookup") => self.store_lookup(&body),
            ("POST", "/v1/state-stores/get-by") => self.store_get_by(&body),
            ("POST", "/v1/state-stores/iterate") => self.store_iterate(&body),
            ("POST", "/v1/state-stores/clear") => self.store_clear(&body),
            ("POST", "/v1/state-stores/sync") => self.store_sync(&body),
            ("POST", "/v1/state-stores/rebuild") => self.store_sync(&body),
            _ => refused(
                404,
                format!("no route for {method} {route}"),
                Some("not_found"),
            ),
        }
    }

    fn mint_cursor(&mut self) -> String {
        self.cursors += 1;
        format!("cursor-{:016x}", self.cursors)
    }

    fn mint_id(&mut self) -> Uuid {
        Uuid::new_v4()
    }

    /// Domain a name belongs to: the longest declared Domain that is a
    /// dotted prefix, mirroring `owning_space`.
    fn owning_domain(&self, name: &str) -> Result<Uuid, ControlError> {
        self.domains
            .iter()
            .filter(|(domain, _)| name.starts_with(&format!("{domain}.")))
            .max_by_key(|(domain, _)| domain.len())
            .map(|(_, id)| *id)
            .ok_or_else(|| ControlError::NotFound(format!("owning Domain for {name}")))
    }

    /// Records strictly after `cursor`; `None` starts at index 0. An
    /// unknown cursor fails like a stale frontier does on a Node.
    fn position_after(feed: &FakeFeed, cursor: Option<&str>) -> Result<usize, TransportReply> {
        match cursor {
            None => Ok(0),
            Some(cursor) => feed
                .records
                .iter()
                .position(|record| record.cursor == cursor)
                .map(|index| index + 1)
                .ok_or_else(|| bad_request("Cursor is not within retained history")),
        }
    }

    /// Where a session with no delivered progress begins: the declared
    /// `start` policy resolved against current history.
    fn start_position(reader: &FakeReader, feed: &FakeFeed) -> usize {
        match &reader.start {
            ReaderStart::Now => reader.start_index,
            ReaderStart::Timestamp(threshold) => feed
                .records
                .iter()
                .position(|record| record.event_time_ns >= *threshold)
                .unwrap_or(feed.records.len()),
            ReaderStart::Beginning | ReaderStart::Cursor(_) => reader.start_index,
        }
    }

    /// The position a session's next fetch serves from: the record after
    /// `delivered_cursor`, or the declared start when nothing has landed.
    fn reader_position(reader: &FakeReader, feed: &FakeFeed) -> Result<usize, TransportReply> {
        match &reader.delivered_cursor {
            Some(cursor) => Self::position_after(feed, Some(cursor)),
            None => Ok(Self::start_position(reader, feed)),
        }
    }

    fn validate_name(name: &str) -> Result<(), ControlError> {
        if name.is_empty() || name.len() > 512 || name.starts_with('.') || name.ends_with('.') {
            return Err(ControlError::InvalidName(name.to_owned()));
        }
        if name.split('.').any(|segment| {
            segment.is_empty()
                || !segment.as_bytes()[0].is_ascii_lowercase()
                || !segment
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        }) {
            return Err(ControlError::InvalidName(name.to_owned()));
        }
        Ok(())
    }

    // --- admin surface -------------------------------------------------

    fn admin_wcl(&mut self, body: &Value) -> TransportReply {
        let Some(script) = body.get("script").and_then(Value::as_str) else {
            return bad_request("admin.wcl requires a script");
        };
        let commands = match control::parse_wcl(script) {
            Ok(commands) => commands,
            Err(error) => return bad_request(error.to_string()),
        };
        let request_id = parse_uuid(body.get("request_id")).unwrap_or_else(Uuid::new_v4);
        self.execute(commands, request_id)
    }

    fn admin_commands(&mut self, body: &Value) -> TransportReply {
        let mut commands = Vec::new();
        for entry in body
            .get("commands")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
        {
            match serde_json::from_value::<Command>(entry) {
                Ok(command) => commands.push(command),
                Err(error) => return bad_request(format!("invalid command: {error}")),
            }
        }
        let request_id = parse_uuid(body.get("request_id")).unwrap_or_else(Uuid::new_v4);
        self.execute(commands, request_id)
    }

    fn execute(&mut self, commands: Vec<Command>, request_id: Uuid) -> TransportReply {
        let mut results = Vec::with_capacity(commands.len());
        for command in commands {
            let statement = command_label(&command);
            match self.apply(&command) {
                Ok((message, data)) => results.push(json!({
                    "statement": statement,
                    "message": message,
                    "data": data,
                })),
                Err(error) => return bad_request(error.to_string()),
            }
        }
        self.revision += 1;
        ok(json!({
            "request_id": request_id,
            "revision": self.revision,
            "authority": "control_plane",
            "warning": "",
            "results": results,
        }))
    }

    fn apply(&mut self, command: &Command) -> Result<(String, Value), ControlError> {
        match command {
            Command::CreateDomain { name } | Command::CreateSpace { name } => {
                Self::validate_name(name)?;
                if self.domains.contains_key(name) {
                    return Err(ControlError::AlreadyExists(format!("Domain {name}")));
                }
                let space_id = self.mint_id();
                self.domains.insert(name.clone(), space_id);
                Ok((
                    format!("created Domain {name}"),
                    json!({
                        "space_id": space_id,
                        "name": name,
                        "status": "active",
                        "created_at_ns": control::unix_ns(),
                    }),
                ))
            }
            Command::CreateFeed { name, .. } => {
                Self::validate_name(name)?;
                if self.feeds.contains_key(name) {
                    return Err(ControlError::AlreadyExists(format!("Feed {name}")));
                }
                let space_id = self.owning_domain(name)?;
                let feed_id = self.mint_id();
                self.feeds.insert(
                    name.clone(),
                    FakeFeed {
                        feed_id,
                        space_id,
                        records: Vec::new(),
                    },
                );
                Ok((
                    format!("created Feed {name}"),
                    json!({
                        "feed_id": feed_id,
                        "name": name,
                        "space_id": space_id,
                        "storage_name": format!("/feeds/{}", feed_id.simple()),
                        "status": "active",
                        "created_at_ns": control::unix_ns(),
                    }),
                ))
            }
            Command::CreateWriter { name, feed } => {
                Self::validate_name(name)?;
                if self.writers.contains_key(name) {
                    return Err(ControlError::AlreadyExists(format!("Writer {name}")));
                }
                let feed_id = self
                    .feeds
                    .get(feed)
                    .ok_or_else(|| ControlError::NotFound(format!("Feed {feed}")))?
                    .feed_id;
                let writer_id = self.mint_id();
                self.writers.insert(
                    name.clone(),
                    FakeWriter {
                        writer_id,
                        feed: feed.clone(),
                        session_epoch: 0,
                        session_active: false,
                        next_sequence: 1,
                        appends: HashMap::new(),
                    },
                );
                Ok((
                    format!("created Writer {name}"),
                    json!({
                        "writer_id": writer_id,
                        "name": name,
                        "feed_id": feed_id,
                        "status": "active",
                        "created_at_ns": control::unix_ns(),
                        "session_epoch": 0,
                        "next_sequence": 1,
                        "session_active": false,
                    }),
                ))
            }
            Command::OpenWriterSession { writer } => {
                let definition = self
                    .writers
                    .get_mut(writer)
                    .ok_or_else(|| ControlError::NotFound(format!("Writer {writer}")))?;
                definition.session_epoch += 1;
                definition.next_sequence = 1;
                definition.session_active = true;
                Ok((
                    format!("opened Writer session {writer}"),
                    self.writer_json(writer),
                ))
            }
            Command::RevokeWriterSession {
                writer,
                session_epoch,
            } => {
                let definition = self
                    .writers
                    .get_mut(writer)
                    .ok_or_else(|| ControlError::NotFound(format!("Writer {writer}")))?;
                Self::check_writer_epoch(definition, *session_epoch)?;
                definition.session_active = false;
                Ok((
                    format!("revoked Writer session {writer}"),
                    self.writer_json(writer),
                ))
            }
            Command::CreateReader { name, feed, start } => {
                Self::validate_name(name)?;
                if self.readers.contains_key(name) {
                    return Err(ControlError::AlreadyExists(format!("Reader {name}")));
                }
                let feed_id = self
                    .feeds
                    .get(feed)
                    .ok_or_else(|| ControlError::NotFound(format!("Feed {feed}")))?
                    .feed_id;
                let acknowledged_cursor = match start {
                    ReaderStart::Cursor(cursor) => Some(cursor.clone()),
                    _ => None,
                };
                let reader_id = self.mint_id();
                self.readers.insert(
                    name.clone(),
                    FakeReader {
                        reader_id,
                        feed: feed.clone(),
                        start: start.clone(),
                        acknowledged_cursor: acknowledged_cursor.clone(),
                        delivered_cursor: acknowledged_cursor.clone(),
                        last_delivery: None,
                        session_epoch: 0,
                        session_capacity: 0,
                        session_active: false,
                        start_index: 0,
                    },
                );
                Ok((
                    format!("created Reader {name}"),
                    json!({
                        "reader_id": reader_id,
                        "name": name,
                        "feed_id": feed_id,
                        "start": serde_json::to_value(start).unwrap_or(Value::Null),
                        "acknowledged_cursor": acknowledged_cursor,
                        "delivered_cursor": acknowledged_cursor,
                        "last_delivery": Value::Null,
                        "session_epoch": 0,
                        "session_capacity": 0,
                        "session_active": false,
                        "status": "active",
                        "created_at_ns": control::unix_ns(),
                    }),
                ))
            }
            Command::CreateSubscription {
                name,
                feed,
                domain,
                start,
                ..
            } => {
                Self::validate_name(name)?;
                if self.subscriptions.contains_key(name) {
                    return Err(ControlError::AlreadyExists(format!("Subscription {name}")));
                }
                let feed_id = match feed {
                    Some(feed) => Some(
                        self.feeds
                            .get(feed)
                            .ok_or_else(|| ControlError::NotFound(format!("Feed {feed}")))?
                            .feed_id,
                    ),
                    None => None,
                };
                let domain_id = domain
                    .as_deref()
                    .and_then(|name| self.domains.get(name))
                    .copied();
                let space_id = feed_id
                    .and_then(|feed_id| {
                        self.feeds
                            .values()
                            .find(|feed| feed.feed_id == feed_id)
                            .map(|feed| feed.space_id)
                    })
                    .or(domain_id)
                    .unwrap_or_else(|| self.mint_id());
                let subscription_id = self.mint_id();
                self.subscriptions.insert(
                    name.clone(),
                    FakeSubscription {
                        subscription_id,
                        space_id,
                        feed_id,
                        domain_id,
                        start: start.clone(),
                    },
                );
                let mut data = json!({
                    "subscription_id": subscription_id,
                    "name": name,
                    "space_id": space_id,
                    "start": serde_json::to_value(start).unwrap_or(Value::Null),
                    "stage": "declared",
                    "status": "active",
                    "created_at_ns": control::unix_ns(),
                });
                if let Some(feed_id) = feed_id {
                    data["feed_id"] = json!(feed_id);
                }
                if let Some(domain_id) = domain_id {
                    data["domain_id"] = json!(domain_id);
                }
                Ok((
                    format!("declared Subscription {name}; shared consumption is not active"),
                    data,
                ))
            }
            Command::DefineStateStore {
                name,
                source,
                named_keys,
            } => {
                Self::validate_name(name)?;
                if self.stores.contains_key(name) {
                    return Err(ControlError::AlreadyExists(format!("StateStore {name}")));
                }
                let space_id = self.owning_domain(name)?;
                let source_feed = match source {
                    control::StateStoreSourceRequest::Manual => None,
                    control::StateStoreSourceRequest::Feed { feed } => Some(
                        self.feeds
                            .get(feed)
                            .ok_or_else(|| ControlError::NotFound(format!("Feed {feed}")))?
                            .feed_id,
                    ),
                };
                let store_id = self.mint_id();
                self.stores.insert(
                    name.clone(),
                    FakeStore {
                        store_id,
                        space_id,
                        source_feed,
                        named_keys: named_keys
                            .iter()
                            .map(|key| (key.name.clone(), key.unique))
                            .collect(),
                        rows: BTreeMap::new(),
                        sequence: 0,
                        mutations: HashMap::new(),
                    },
                );
                Ok((
                    format!("declared StateStore {name} with RF3 mutation journal placement"),
                    self.store_json(name),
                ))
            }
            Command::DefineIndex {
                name,
                feed,
                fields,
                unique,
                ..
            } => {
                Self::validate_name(name)?;
                if self.indexes.contains_key(name) {
                    return Err(ControlError::AlreadyExists(format!("Index {name}")));
                }
                let feed_id = self
                    .feeds
                    .get(feed)
                    .ok_or_else(|| ControlError::NotFound(format!("Feed {feed}")))?
                    .feed_id;
                let space_id = self.owning_domain(name)?;
                let index_id = self.mint_id();
                let data = json!({
                    "index_id": index_id,
                    "name": name,
                    "space_id": space_id,
                    "feed_id": feed_id,
                    "fields": fields,
                    "unique": unique,
                    "stage": "declared",
                    "status": "active",
                    "created_at_ns": control::unix_ns(),
                });
                self.indexes.insert(name.clone(), data.clone());
                Ok((format!("defined Index {name}"), data))
            }
            Command::DefinePipe {
                name,
                subscription,
                output_feed,
                output_store,
                operation,
                ..
            } => {
                Self::validate_name(name)?;
                if self.pipes.contains_key(name) {
                    return Err(ControlError::AlreadyExists(format!("Pipe {name}")));
                }
                let declared = self.subscriptions.get(subscription).ok_or_else(|| {
                    ControlError::NotFound(format!("Subscription {subscription}"))
                })?;
                let (space_id, subscription_id) = (declared.space_id, declared.subscription_id);
                let output_feed_id = match output_feed {
                    Some(feed) => Some(
                        self.feeds
                            .get(feed)
                            .ok_or_else(|| ControlError::NotFound(format!("Feed {feed}")))?
                            .feed_id,
                    ),
                    None => None,
                };
                let output_store_id = match output_store {
                    Some(store) => Some(
                        self.stores
                            .get(store)
                            .ok_or_else(|| ControlError::NotFound(format!("StateStore {store}")))?
                            .store_id,
                    ),
                    None => None,
                };
                let pipe_id = self.mint_id();
                self.pipes.insert(
                    name.clone(),
                    FakePipe {
                        pipe_id,
                        space_id,
                        subscription_id,
                        operation: serde_json::to_value(operation).unwrap_or(Value::Null),
                        output_feed_id,
                        output_store_id,
                        stage: "declared",
                        created_at_ns: control::unix_ns(),
                    },
                );
                Ok((
                    format!("defined Pipe {name}"),
                    self.pipe_row(name).unwrap_or(Value::Null),
                ))
            }
            Command::PausePipe { name } => self.set_pipe_stage(name, "paused"),
            Command::ResumePipe { name } => self.set_pipe_stage(name, "declared"),
            Command::OpenReaderSession { reader, capacity } => {
                let data = self.open_reader(reader, *capacity)?;
                Ok((format!("opened Reader session {reader}"), data))
            }
            Command::AcknowledgeReader {
                reader,
                session_epoch,
                cursor,
            } => {
                let data = self.ack_reader(reader, *session_epoch, cursor)?;
                Ok((format!("acknowledged Reader {reader}"), data))
            }
            Command::CloseReaderSession {
                reader,
                session_epoch,
            } => {
                let data = self.close_reader(reader, *session_epoch)?;
                Ok((format!("closed Reader session {reader}"), data))
            }
            Command::SeekReader { reader, start } => {
                let feed_name = self
                    .readers
                    .get(reader)
                    .ok_or_else(|| ControlError::NotFound(format!("Reader {reader}")))?
                    .feed
                    .clone();
                let start_index = if matches!(start, ReaderStart::Now) {
                    self.feeds
                        .get(&feed_name)
                        .map(|feed| feed.records.len())
                        .unwrap_or(0)
                } else {
                    0
                };
                let definition = self.readers.get_mut(reader).expect("reader checked");
                definition.start = start.clone();
                let cursor = match start {
                    ReaderStart::Cursor(cursor) => Some(cursor.clone()),
                    _ => None,
                };
                definition.acknowledged_cursor = cursor.clone();
                definition.delivered_cursor = cursor;
                definition.last_delivery = None;
                definition.start_index = start_index;
                Ok((
                    format!("repositioned Reader {reader}"),
                    self.reader_json(reader),
                ))
            }
            Command::Drop { kind, name } => self.drop_resource(kind, name),
            Command::EraseFeed { feed } => {
                // Mirror the catalog fences: holds first, attachments second.
                if self.legal_holds.contains_key(feed.as_str()) {
                    return Err(ControlError::InvalidOperation(format!(
                        "drop the legal hold on Feed {feed} before erasing it"
                    )));
                }
                if !self.feeds.contains_key(feed.as_str()) {
                    return Err(ControlError::NotFound(format!("Feed {feed}")));
                }
                let removed = self.feeds.remove(feed.as_str()).is_some();
                self.legal_holds.remove(feed.as_str());
                self.history_policies.remove(&format!("feed:{feed}"));
                Ok((
                    format!("erased Feed {feed}: stored history is being purged"),
                    json!({ "name": feed, "erased": removed }),
                ))
            }
            Command::Show { kind, .. } => {
                Ok((format!("showing {}", show_label(kind)), self.show(kind)))
            }
            Command::Describe { kind, name } => {
                Ok((format!("described {name}"), self.describe(kind, name)?))
            }
            Command::Rename { kind, current, new } => self.rename(kind, current, new),
            Command::SetHistoryPolicy { scope, policy } => {
                let key = match &scope {
                    crate::control::PolicyScope::Domain { name } => {
                        if !self.domains.contains_key(name.as_str()) {
                            return Err(ControlError::NotFound(format!("Domain {name}")));
                        }
                        format!("domain:{name}")
                    }
                    crate::control::PolicyScope::Feed { name } => {
                        if !self.feeds.contains_key(name.as_str()) {
                            return Err(ControlError::NotFound(format!("Feed {name}")));
                        }
                        format!("feed:{name}")
                    }
                };
                self.history_policies.insert(key.clone(), policy.clone());
                Ok((
                    format!("set History Policy on {key}"),
                    json!({ "name": key }),
                ))
            }
            Command::ClearHistoryPolicy { scope } => {
                let key = match &scope {
                    crate::control::PolicyScope::Domain { name } => {
                        format!("domain:{name}")
                    }
                    crate::control::PolicyScope::Feed { name } => {
                        format!("feed:{name}")
                    }
                };
                if self.history_policies.remove(&key).is_none() {
                    return Err(ControlError::NotFound(format!("History Policy on {key}")));
                }
                Ok((format!("cleared History Policy on {key}"), json!({})))
            }
            Command::SetLegalHold { feed, reason } => {
                let feed_row = self
                    .feeds
                    .get(feed.as_str())
                    .ok_or_else(|| ControlError::NotFound(format!("Feed {feed}")))?;
                let hold = crate::control::LegalHold {
                    feed_id: feed_row.feed_id,
                    name: feed.clone(),
                    reason: reason.clone(),
                    set_at_ns: 0,
                };
                self.legal_holds.insert(feed.clone(), hold.clone());
                Ok((format!("set legal hold on Feed {feed}"), json!(hold)))
            }
            Command::DropLegalHold { feed } => {
                if self.legal_holds.remove(feed.as_str()).is_none() {
                    return Err(ControlError::NotFound(format!("legal hold on Feed {feed}")));
                }
                Ok((format!("dropped legal hold on Feed {feed}"), json!({})))
            }
            Command::SetSchema { scope, schema } => {
                let key = match &scope {
                    crate::control::PolicyScope::Domain { name } => {
                        if !self.domains.contains_key(name.as_str()) {
                            return Err(ControlError::NotFound(format!("Domain {name}")));
                        }
                        format!("domain:{name}")
                    }
                    crate::control::PolicyScope::Feed { name } => {
                        if !self.feeds.contains_key(name.as_str()) {
                            return Err(ControlError::NotFound(format!("Feed {name}")));
                        }
                        format!("feed:{name}")
                    }
                };
                self.schemas.insert(key.clone(), schema.clone());
                Ok((format!("set schema on {key}"), json!({ "name": key })))
            }
            Command::ClearSchema { scope } => {
                let key = match &scope {
                    crate::control::PolicyScope::Domain { name } => {
                        format!("domain:{name}")
                    }
                    crate::control::PolicyScope::Feed { name } => {
                        format!("feed:{name}")
                    }
                };
                if self.schemas.remove(&key).is_none() {
                    return Err(ControlError::NotFound(format!("schema on {key}")));
                }
                Ok((format!("cleared schema on {key}"), json!({})))
            }
            _ => Ok(("ok".to_owned(), json!({}))),
        }
    }

    fn set_pipe_stage(
        &mut self,
        name: &str,
        stage: &'static str,
    ) -> Result<(String, Value), ControlError> {
        let pipe = self
            .pipes
            .get_mut(name)
            .ok_or_else(|| ControlError::NotFound(format!("Pipe {name}")))?;
        pipe.stage = stage;
        Ok((
            format!("{stage} Pipe {name}"),
            self.pipe_row(name).unwrap_or(Value::Null),
        ))
    }

    fn drop_resource(
        &mut self,
        kind: &ResourceKind,
        name: &str,
    ) -> Result<(String, Value), ControlError> {
        let removed = match kind {
            ResourceKind::Domain | ResourceKind::Space => self.domains.remove(name).is_some(),
            ResourceKind::Feed => {
                if self.legal_holds.contains_key(name) {
                    return Err(ControlError::InvalidOperation(format!(
                        "drop the legal hold on Feed {name} before dropping it"
                    )));
                }
                self.feeds.remove(name).is_some()
            }
            ResourceKind::Writer => self.writers.remove(name).is_some(),
            ResourceKind::Reader => self.readers.remove(name).is_some(),
            ResourceKind::Subscription => self.subscriptions.remove(name).is_some(),
            ResourceKind::Pipe => self.pipes.remove(name).is_some(),
            ResourceKind::Index => self.indexes.remove(name).is_some(),
            ResourceKind::StateStore => self.stores.remove(name).is_some(),
            ResourceKind::Role => false,
        };
        if !removed {
            return Err(ControlError::NotFound(name.to_owned()));
        }
        Ok((
            format!("dropped {name}"),
            json!({ "name": name, "status": "dropped" }),
        ))
    }

    fn rename(
        &mut self,
        kind: &ResourceKind,
        current: &str,
        new: &str,
    ) -> Result<(String, Value), ControlError> {
        Self::validate_name(new)?;
        let renamed = match kind {
            ResourceKind::Domain | ResourceKind::Space => self
                .domains
                .remove(current)
                .map(|item| self.domains.insert(new.to_owned(), item))
                .is_some(),
            ResourceKind::Feed => self
                .feeds
                .remove(current)
                .map(|item| self.feeds.insert(new.to_owned(), item))
                .is_some(),
            ResourceKind::Writer => self
                .writers
                .remove(current)
                .map(|item| self.writers.insert(new.to_owned(), item))
                .is_some(),
            ResourceKind::Reader => self
                .readers
                .remove(current)
                .map(|item| self.readers.insert(new.to_owned(), item))
                .is_some(),
            ResourceKind::Subscription => self
                .subscriptions
                .remove(current)
                .map(|item| self.subscriptions.insert(new.to_owned(), item))
                .is_some(),
            ResourceKind::Pipe => self
                .pipes
                .remove(current)
                .map(|item| self.pipes.insert(new.to_owned(), item))
                .is_some(),
            ResourceKind::Index => self
                .indexes
                .remove(current)
                .map(|item| self.indexes.insert(new.to_owned(), item))
                .is_some(),
            ResourceKind::StateStore => self
                .stores
                .remove(current)
                .map(|item| self.stores.insert(new.to_owned(), item))
                .is_some(),
            ResourceKind::Role => false,
        };
        if !renamed {
            return Err(ControlError::NotFound(current.to_owned()));
        }
        Ok((
            format!("renamed {current} to {new}"),
            json!({ "name": new, "renamed_from": current }),
        ))
    }

    fn show(&self, kind: &ShowKind) -> Value {
        match kind {
            ShowKind::Domains | ShowKind::Spaces => json!(self
                .domains
                .iter()
                .map(|(name, space_id)| json!({
                    "space_id": space_id,
                    "name": name,
                    "status": "active",
                }))
                .collect::<Vec<_>>()),
            ShowKind::Feeds => json!(self
                .feeds
                .iter()
                .map(|(name, feed)| {
                    json!({
                        "feed_id": feed.feed_id,
                        "name": name,
                        "space_id": feed.space_id,
                        "status": "active",
                        "schema": self.schemas.get(&format!("feed:{name}")),
                        "effective_schema": self.effective_schema(name),
                    })
                })
                .collect::<Vec<_>>()),
            ShowKind::Writers => json!(self
                .writers
                .keys()
                .map(|name| self.writer_json(name))
                .collect::<Vec<_>>()),
            ShowKind::Readers => json!(self
                .readers
                .keys()
                .map(|name| self.reader_json(name))
                .collect::<Vec<_>>()),
            ShowKind::Subscriptions => json!(self
                .subscriptions
                .iter()
                .map(|(name, subscription)| json!({
                    "subscription_id": subscription.subscription_id,
                    "name": name,
                    "space_id": subscription.space_id,
                    "feed_id": subscription.feed_id,
                    "domain_id": subscription.domain_id,
                    "stage": "declared",
                    "status": "active",
                }))
                .collect::<Vec<_>>()),
            ShowKind::StateStores => json!(self
                .stores
                .keys()
                .map(|name| self.store_json(name))
                .collect::<Vec<_>>()),
            ShowKind::Pipes => json!(self
                .pipes
                .keys()
                .filter_map(|name| self.pipe_row(name))
                .collect::<Vec<_>>()),
            ShowKind::Indexes => json!(self.indexes.values().cloned().collect::<Vec<_>>()),
            ShowKind::HistoryPolicies => json!(self
                .history_policies
                .iter()
                .map(|(name, policy)| json!({ "name": name, "policy": policy }))
                .collect::<Vec<_>>()),
            ShowKind::LegalHolds => json!(self.legal_holds.values().collect::<Vec<_>>()),
            ShowKind::Schemas => json!(self
                .schemas
                .iter()
                .map(|(name, schema)| json!({ "name": name, "schema": schema }))
                .collect::<Vec<_>>()),
            _ => json!([]),
        }
    }

    fn describe(&self, kind: &ResourceKind, name: &str) -> Result<Value, ControlError> {
        let missing = || ControlError::NotFound(name.to_owned());
        match kind {
            ResourceKind::Domain | ResourceKind::Space => self
                .domains
                .get(name)
                .map(|space_id| {
                    json!({
                        "space_id": space_id,
                        "name": name,
                        "status": "active",
                        "schema": self.schemas.get(&format!("domain:{name}")),
                    })
                })
                .ok_or_else(missing),
            ResourceKind::Feed => self
                .feeds
                .get(name)
                .map(|feed| {
                    json!({
                        "feed_id": feed.feed_id,
                        "name": name,
                        "space_id": feed.space_id,
                        "storage_name": format!("/feeds/{}", feed.feed_id.simple()),
                        "status": "active",
                        "schema": self.schemas.get(&format!("feed:{name}")),
                        "effective_schema": self.effective_schema(name),
                    })
                })
                .ok_or_else(missing),
            ResourceKind::Writer => self
                .writers
                .contains_key(name)
                .then(|| self.writer_json(name))
                .ok_or_else(missing),
            ResourceKind::Reader => self
                .readers
                .contains_key(name)
                .then(|| self.reader_json(name))
                .ok_or_else(missing),
            ResourceKind::Subscription => self
                .subscriptions
                .get(name)
                .map(|subscription| {
                    json!({
                        "subscription_id": subscription.subscription_id,
                        "name": name,
                        "space_id": subscription.space_id,
                        "feed_id": subscription.feed_id,
                        "domain_id": subscription.domain_id,
                        "start": serde_json::to_value(&subscription.start).unwrap_or(Value::Null),
                        "stage": "declared",
                        "status": "active",
                    })
                })
                .ok_or_else(missing),
            ResourceKind::Pipe => self.pipe_row(name).ok_or_else(missing),
            ResourceKind::Index => self.indexes.get(name).cloned().ok_or_else(missing),
            ResourceKind::StateStore => self
                .stores
                .contains_key(name)
                .then(|| self.store_json(name))
                .ok_or_else(missing),
            ResourceKind::Role => Err(missing()),
        }
    }

    /// The Feed schema a client should honor: the feed's declared
    /// override layered over its Domain's — `Null` when undeclared.
    fn effective_schema(&self, feed_name: &str) -> Value {
        let Some(feed) = self.feeds.get(feed_name) else {
            return Value::Null;
        };
        let own = self.schemas.get(&format!("feed:{feed_name}"));
        let domain_name = self
            .domains
            .iter()
            .find(|(_, space_id)| **space_id == feed.space_id)
            .map(|(name, _)| name.clone());
        let domain = domain_name
            .as_ref()
            .and_then(|name| self.schemas.get(&format!("domain:{name}")));
        match (own, domain) {
            (Some(own), Some(domain)) => {
                serde_json::to_value(own.layered_over(domain)).unwrap_or(Value::Null)
            }
            (Some(own), None) => serde_json::to_value(own).unwrap_or(Value::Null),
            (None, Some(domain)) => serde_json::to_value(domain).unwrap_or(Value::Null),
            (None, None) => Value::Null,
        }
    }

    fn writer_json(&self, name: &str) -> Value {
        let Some(writer) = self.writers.get(name) else {
            return Value::Null;
        };
        json!({
            "writer_id": writer.writer_id,
            "name": name,
            "feed_id": self
                .feeds
                .get(&writer.feed)
                .map(|feed| feed.feed_id)
                .unwrap_or_default(),
            "status": "active",
            "session_epoch": writer.session_epoch,
            "next_sequence": writer.next_sequence,
            "session_active": writer.session_active,
        })
    }

    fn reader_json(&self, name: &str) -> Value {
        let Some(reader) = self.readers.get(name) else {
            return Value::Null;
        };
        json!({
            "reader_id": reader.reader_id,
            "name": name,
            "feed_id": self
                .feeds
                .get(&reader.feed)
                .map(|feed| feed.feed_id)
                .unwrap_or_default(),
            "start": serde_json::to_value(&reader.start).unwrap_or(Value::Null),
            "acknowledged_cursor": reader.acknowledged_cursor,
            "delivered_cursor": reader.delivered_cursor,
            "session_epoch": reader.session_epoch,
            "session_capacity": reader.session_capacity,
            "session_active": reader.session_active,
            "status": "active",
        })
    }

    /// The `ReaderSessionResponse` wire shape open/fetch/ack/close serve.
    fn reader_session_json(&self, name: &str) -> Value {
        let Some(reader) = self.readers.get(name) else {
            return Value::Null;
        };
        json!({
            "reader_id": reader.reader_id,
            "feed_id": self
                .feeds
                .get(&reader.feed)
                .map(|feed| feed.feed_id)
                .unwrap_or_default(),
            "session_epoch": reader.session_epoch,
            "capacity": reader.session_capacity,
            "delivered_cursor": reader.delivered_cursor,
            "acknowledged_cursor": reader.acknowledged_cursor,
        })
    }

    fn store_json(&self, name: &str) -> Value {
        let Some(store) = self.stores.get(name) else {
            return Value::Null;
        };
        json!({
            "store_id": store.store_id,
            "name": name,
            "space_id": store.space_id,
            "source": match store.source_feed {
                Some(feed_id) => json!({ "kind": "feed", "feed_id": feed_id }),
                None => json!({ "kind": "manual" }),
            },
            "stage": "declared",
            "named_keys": store
                .named_keys
                .iter()
                .map(|(name, unique)| json!({ "name": name, "unique": unique }))
                .collect::<Vec<_>>(),
            "created_at_ns": control::unix_ns(),
        })
    }

    fn pipe_row(&self, name: &str) -> Option<Value> {
        self.pipes.get(name).map(|pipe| {
            json!({
                "pipe_id": pipe.pipe_id,
                "name": name,
                "space_id": pipe.space_id,
                "subscription_id": pipe.subscription_id,
                "operation": pipe.operation,
                "output_feed_id": pipe.output_feed_id,
                "output_store_id": pipe.output_store_id,
                "stage": pipe.stage,
                "status": "active",
                "created_at_ns": pipe.created_at_ns,
            })
        })
    }

    fn check_writer_epoch(writer: &FakeWriter, epoch: u64) -> Result<(), ControlError> {
        if !writer.session_active {
            return Err(ControlError::InvalidOperation(format!(
                "Writer {} has no active session",
                writer.feed
            )));
        }
        if writer.session_epoch != epoch {
            return Err(ControlError::InvalidOperation(format!(
                "stale Writer session epoch: current={}, supplied={epoch}",
                writer.session_epoch
            )));
        }
        Ok(())
    }

    fn check_reader_epoch(reader: &FakeReader, epoch: u64) -> Result<(), ControlError> {
        if !reader.session_active {
            return Err(ControlError::InvalidOperation(format!(
                "Reader {} has no active session",
                reader.feed
            )));
        }
        if reader.session_epoch != epoch {
            return Err(ControlError::InvalidOperation(format!(
                "stale Reader session epoch: current={}, supplied={epoch}",
                reader.session_epoch
            )));
        }
        Ok(())
    }

    /// Session-active check a data-plane endpoint applies before the
    /// catalog-level validation — the wire stale-epoch refusal.
    fn session_stale(reader: &FakeReader, epoch: u64) -> bool {
        !reader.session_active || reader.session_epoch != epoch
    }

    // --- writer data plane ---------------------------------------------

    fn writer_append(&mut self, body: &Value) -> TransportReply {
        let name = body
            .get("writer")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let session_epoch = body
            .get("session_epoch")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let Some(request_id) = body
            .get("request_id")
            .and_then(Value::as_str)
            .and_then(|raw| Uuid::parse_str(raw).ok())
        else {
            return bad_request("request_id must be a UUID");
        };
        let dedup_key = (session_epoch, request_id.to_string());
        let Some(writer) = self.writers.get(name) else {
            return bad_request(format!("Writer does not exist: {name}"));
        };
        if !writer.session_active {
            return stale_epoch(format!("Writer {name} has no active session"));
        }
        if writer.session_epoch != session_epoch {
            return stale_epoch(format!(
                "stale Writer session epoch: current={}, supplied={session_epoch}",
                writer.session_epoch
            ));
        }
        let identity = json!({
            "key_base64": body.get("key_base64"),
            "payload_base64": body.get("payload_base64"),
            "metadata_base64": body.get("metadata_base64"),
            "event_time_ns": body.get("event_time_ns"),
        });
        if let Some(committed) = writer.appends.get(&dedup_key) {
            if committed.identity == identity {
                return ok(json!({
                    "message_id": committed.message_id,
                    "cursor": committed.cursor,
                    "deduplicated": true,
                    "durability": "majority_committed",
                    "feedback": feedback_body(),
                }));
            }
            return conflict(
                format!(
                    "request_id {request_id} was already committed with different record bytes"
                ),
                None,
            );
        }
        let Some(key) = body
            .get("key_base64")
            .and_then(Value::as_str)
            .and_then(|raw| STANDARD.decode(raw).ok())
        else {
            return bad_request("key_base64 is not valid base64");
        };
        if key.is_empty() {
            return bad_request("key_base64 must contain a non-empty key");
        }
        let payload = body
            .get("payload_base64")
            .and_then(Value::as_str)
            .and_then(|raw| STANDARD.decode(raw).ok())
            .unwrap_or_default();
        let metadata = body
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
        let ingest_time_ns = control::unix_ns();
        let event_time_ns = match body.get("event_time_ns") {
            None | Some(Value::Null) => ingest_time_ns,
            Some(raw) => match raw.as_str().and_then(|text| text.parse::<i64>().ok()) {
                Some(nanos) => nanos,
                None => return bad_request("event_time_ns must be signed epoch nanoseconds"),
            },
        };
        let sequence = writer.next_sequence;
        let writer_id = writer.writer_id;
        let feed_name = writer.feed.clone();
        let message_id = self.mint_id();
        let cursor = self.mint_cursor();
        let Some(feed) = self.feeds.get_mut(&feed_name) else {
            return bad_request("Writer Feed does not exist");
        };
        feed.records.push(FakeRecord {
            cursor: cursor.clone(),
            message_id,
            producer_id: writer_id,
            sequence,
            event_time_ns,
            ingest_time_ns,
            key,
            payload,
            metadata,
        });
        let writer = self.writers.get_mut(name).expect("writer checked");
        writer.next_sequence = sequence.saturating_add(1);
        writer.appends.insert(
            dedup_key,
            FakeAppend {
                message_id,
                cursor: cursor.clone(),
                identity,
            },
        );
        ok(json!({
            "message_id": message_id,
            "cursor": cursor,
            "deduplicated": false,
            "durability": "majority_committed",
            "feedback": feedback_body(),
        }))
    }

    fn writer_append_batch(&mut self, body: &Value) -> TransportReply {
        let records = body
            .get("records")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        if records.is_empty() || records.len() > 1_000 {
            return bad_request("Writer batch must contain between 1 and 1000 records");
        }
        let mut results = Vec::with_capacity(records.len());
        for record in &records {
            let reply = self.writer_append(record);
            if !(200..300).contains(&reply.status) {
                return reply;
            }
            results.push(reply.body);
        }
        ok(json!({
            "results": results,
            "feedback": feedback_body(),
        }))
    }

    // --- reader data plane ----------------------------------------------

    fn open_reader(&mut self, name: &str, capacity: usize) -> Result<Value, ControlError> {
        if capacity == 0 || capacity > 10_000 {
            return Err(ControlError::InvalidOperation(
                "Reader capacity must be between 1 and 10000".to_owned(),
            ));
        }
        let feed_name = self
            .readers
            .get(name)
            .ok_or_else(|| ControlError::NotFound(format!("Reader {name}")))?
            .feed
            .clone();
        let start_index = {
            let reader = self.readers.get(name).expect("reader checked");
            match (&reader.start, &reader.acknowledged_cursor) {
                (ReaderStart::Now, None) => self
                    .feeds
                    .get(&feed_name)
                    .map(|feed| feed.records.len())
                    .unwrap_or(0),
                _ => 0,
            }
        };
        let reader = self.readers.get_mut(name).expect("reader checked");
        reader.session_epoch += 1;
        reader.session_capacity = capacity;
        reader.session_active = true;
        reader.delivered_cursor = reader.acknowledged_cursor.clone();
        reader.last_delivery = None;
        reader.start_index = start_index;
        Ok(self.reader_session_json(name))
    }

    fn reader_open(&mut self, body: &Value) -> TransportReply {
        let name = body
            .get("reader")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let capacity = body.get("capacity").and_then(Value::as_u64).unwrap_or(0) as usize;
        if !self.readers.contains_key(name) {
            return bad_request("Reader does not exist");
        }
        match self.open_reader(name, capacity) {
            Ok(data) => ok(data),
            Err(error) => bad_request(error.to_string()),
        }
    }

    fn reader_fetch(&mut self, body: &Value) -> TransportReply {
        let name = body
            .get("reader")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let session_epoch = body
            .get("session_epoch")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let Some(request_id) = parse_uuid(body.get("request_id")) else {
            return bad_request("request_id must be a UUID");
        };
        let Some(reader) = self.readers.get(name) else {
            return bad_request("Reader does not exist");
        };
        if Self::session_stale(reader, session_epoch) {
            return stale_epoch("Reader session is stale or inactive");
        }
        let feed_name = reader.feed.clone();
        let Some(feed) = self.feeds.get(&feed_name) else {
            return unavailable("Reader Feed is not applied on this Node");
        };
        // A duplicate fetch request replays the identical committed page.
        if let Some((last_id, previous, served)) = reader.last_delivery.clone() {
            if last_id == request_id {
                let position = match previous {
                    Some(cursor) => match Self::position_after(feed, Some(&cursor)) {
                        Ok(position) => position,
                        Err(reply) => return reply,
                    },
                    None => Self::start_position(reader, feed),
                };
                let page: Vec<FakeRecord> = feed.records[position..]
                    .iter()
                    .take(served)
                    .cloned()
                    .collect();
                let delivered = page.last().map(|record| record.cursor.clone());
                if page.is_empty() || delivered != reader.delivered_cursor {
                    return unavailable(
                        "Reader fetch request already delivered; reopen the session to replay \
                         unacknowledged progress",
                    );
                }
                return ok(json!({
                    "records": page
                        .iter()
                        .map(|record| record.wire(&feed_name, feed.feed_id))
                        .collect::<Vec<_>>(),
                    "delivered_cursor": reader.delivered_cursor,
                    "acknowledged_cursor": reader.acknowledged_cursor,
                    "recommended_capacity": served,
                    "retry_after_ms": 0,
                }));
            }
        }
        let limit = body
            .get("limit")
            .and_then(Value::as_u64)
            .unwrap_or(reader.session_capacity as u64)
            .clamp(1, reader.session_capacity.max(1) as u64) as usize;
        let position = match Self::reader_position(reader, feed) {
            Ok(position) => position,
            Err(reply) => return reply,
        };
        let previous = reader.delivered_cursor.clone();
        let page: Vec<&FakeRecord> = feed.records[position..].iter().take(limit).collect();
        let delivered_cursor = page
            .last()
            .map(|record| record.cursor.clone())
            .or_else(|| reader.delivered_cursor.clone());
        let records: Vec<Value> = page
            .iter()
            .map(|record| record.wire(&feed_name, feed.feed_id))
            .collect();
        if !page.is_empty() {
            let reader = self.readers.get_mut(name).expect("reader checked");
            reader.last_delivery = Some((request_id, previous, limit));
            reader.delivered_cursor = delivered_cursor.clone();
        }
        let reader = self.readers.get(name).expect("reader checked");
        ok(json!({
            "records": records,
            "delivered_cursor": delivered_cursor.or_else(|| reader.delivered_cursor.clone()),
            "acknowledged_cursor": reader.acknowledged_cursor,
            "recommended_capacity": limit,
            "retry_after_ms": 0,
        }))
    }

    fn ack_reader(
        &mut self,
        name: &str,
        session_epoch: u64,
        cursor: &str,
    ) -> Result<Value, ControlError> {
        let reader = self
            .readers
            .get_mut(name)
            .ok_or_else(|| ControlError::NotFound(format!("Reader {name}")))?;
        Self::check_reader_epoch(reader, session_epoch)?;
        if reader.delivered_cursor.as_deref() != Some(cursor) {
            return Err(ControlError::InvalidOperation(
                "Reader acknowledgement must match the latest delivered Cursor".to_owned(),
            ));
        }
        reader.acknowledged_cursor = Some(cursor.to_owned());
        Ok(self.reader_session_json(name))
    }

    fn reader_ack(&mut self, body: &Value) -> TransportReply {
        let name = body
            .get("reader")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let session_epoch = body
            .get("session_epoch")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let cursor = body
            .get("cursor")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let Some(reader) = self.readers.get(name) else {
            return bad_request("Reader does not exist");
        };
        if Self::session_stale(reader, session_epoch) {
            return stale_epoch("Reader session is stale or inactive");
        }
        match self.ack_reader(name, session_epoch, &cursor) {
            Ok(data) => ok(data),
            Err(error) => conflict(error.to_string(), None),
        }
    }

    fn close_reader(&mut self, name: &str, session_epoch: u64) -> Result<Value, ControlError> {
        let reader = self
            .readers
            .get_mut(name)
            .ok_or_else(|| ControlError::NotFound(format!("Reader {name}")))?;
        Self::check_reader_epoch(reader, session_epoch)?;
        reader.session_active = false;
        reader.session_capacity = 0;
        reader.last_delivery = None;
        Ok(self.reader_session_json(name))
    }

    fn reader_close(&mut self, body: &Value) -> TransportReply {
        let name = body
            .get("reader")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let session_epoch = body
            .get("session_epoch")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let Some(reader) = self.readers.get(name) else {
            return bad_request("Reader does not exist");
        };
        if Self::session_stale(reader, session_epoch) {
            return stale_epoch("Reader session is stale or inactive");
        }
        match self.close_reader(name, session_epoch) {
            Ok(data) => ok(data),
            Err(error) => bad_request(error.to_string()),
        }
    }

    fn temporary_fetch(&mut self, body: &Value) -> TransportReply {
        let feed_name = body.get("feed").and_then(Value::as_str).unwrap_or_default();
        let after = body.get("after").and_then(Value::as_str);
        let new_only = body
            .get("new_only")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let tail = body.get("tail").and_then(Value::as_bool).unwrap_or(false);
        if new_only && after.is_some() {
            return bad_request("new_only cannot be combined with after");
        }
        let after_time = body.get("after_event_time_ns").and_then(Value::as_str);
        if after_time.is_some() && (after.is_some() || new_only) {
            return bad_request("after_event_time_ns cannot be combined with after or new_only");
        }
        let after_event_time_ns = match after_time.map(str::parse::<i64>) {
            Some(Ok(nanos)) => Some(nanos),
            Some(Err(_)) => {
                return bad_request("after_event_time_ns must be signed epoch nanoseconds")
            }
            None => None,
        };
        let Some(feed) = self.feeds.get(feed_name) else {
            return bad_request("Feed does not exist");
        };
        let limit = body
            .get("limit")
            .and_then(Value::as_u64)
            .unwrap_or(100)
            .clamp(1, 10_000) as usize;
        if new_only {
            return ok(json!({
                "records": [],
                "next_cursor": feed.records.last().map(|record| record.cursor.clone()),
            }));
        }
        let position = match Self::position_after(feed, after) {
            Ok(position) => position,
            Err(reply) => return reply,
        };
        let mut frames: Vec<&FakeRecord> = feed.records[position..].iter().collect();
        if let Some(threshold) = after_event_time_ns {
            frames.retain(|record| record.event_time_ns >= threshold);
            frames.truncate(limit);
        } else if tail && frames.len() > limit {
            frames = frames.split_off(frames.len() - limit);
        } else {
            frames.truncate(limit);
        }
        let records: Vec<Value> = frames
            .iter()
            .map(|record| record.wire(feed_name, feed.feed_id))
            .collect();
        let next_cursor = frames
            .last()
            .map(|record| record.cursor.clone())
            .or_else(|| after.map(str::to_owned));
        ok(json!({
            "records": records,
            "next_cursor": next_cursor,
        }))
    }

    fn feed_records(&mut self, query: &str) -> TransportReply {
        let mut feed_name = String::new();
        let mut after = None;
        let mut limit = 100_usize;
        for pair in query.split('&').filter(|pair| !pair.is_empty()) {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            match key {
                "feed" => feed_name = url_decode(value),
                "after" => after = Some(url_decode(value)),
                "limit" => limit = value.parse::<usize>().unwrap_or(100),
                _ => {}
            }
        }
        let Some(feed) = self.feeds.get(&feed_name) else {
            return bad_request(format!("Feed does not exist: {feed_name}"));
        };
        let position = match Self::position_after(feed, after.as_deref()) {
            Ok(position) => position,
            Err(reply) => return reply,
        };
        ok(json!(feed.records[position..]
            .iter()
            .take(limit.max(1))
            .map(|record| record.wire(&feed_name, feed.feed_id))
            .collect::<Vec<_>>()))
    }

    // --- state-store data plane -----------------------------------------

    fn store_mutate(&mut self, body: &Value) -> TransportReply {
        let name = body
            .get("store")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let Some(mutation_id) = parse_uuid(body.get("mutation_id")) else {
            return bad_request("mutation_id must be a UUID");
        };
        let Some(store) = self.stores.get(name) else {
            return bad_request(format!("StateStore does not exist: {name}"));
        };
        if store.source_feed.is_some() {
            return bad_request(
                "mutations apply only to manual StateStores; Feed-derived stores replay from \
                 their source",
            );
        }
        let store_id = store.store_id;
        let named_keys: Vec<(String, bool)> = store.named_keys.clone();
        let op = body.get("op").cloned().unwrap_or(Value::Null);
        let kind = op.get("kind").and_then(Value::as_str).unwrap_or_default();
        let decoded = match kind {
            "put_row" => {
                let Some(key) = op
                    .get("key_base64")
                    .and_then(Value::as_str)
                    .and_then(|raw| STANDARD.decode(raw).ok())
                else {
                    return bad_request("key_base64 is not valid base64");
                };
                if key.is_empty() || key.len() > crate::state_store::STATE_STORE_MAX_KEY_BYTES {
                    return bad_request("key_base64 is empty or exceeds the row-key budget");
                }
                let payload = op
                    .get("payload_base64")
                    .and_then(Value::as_str)
                    .and_then(|raw| STANDARD.decode(raw).ok())
                    .unwrap_or_default();
                let named: Vec<(String, Vec<Value>)> = op
                    .get("named")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default()
                    .iter()
                    .map(|entry| {
                        (
                            entry
                                .get("name")
                                .and_then(Value::as_str)
                                .unwrap_or_default()
                                .to_owned(),
                            entry
                                .get("values")
                                .and_then(Value::as_array)
                                .cloned()
                                .unwrap_or_default(),
                        )
                    })
                    .collect();
                if named.len() > control::MAX_STATE_STORE_NAMED_KEYS {
                    return bad_request("a row may write at most the store's named-key budget");
                }
                for (key_name, _) in &named {
                    if !named_keys.iter().any(|(name, _)| name == key_name) {
                        return bad_request(format!(
                            "StateStore {name} does not declare named key {key_name}"
                        ));
                    }
                }
                MutateOp::Put {
                    key,
                    payload,
                    named,
                }
            }
            "delete_row" => {
                let Some(key) = op
                    .get("key_base64")
                    .and_then(Value::as_str)
                    .and_then(|raw| STANDARD.decode(raw).ok())
                else {
                    return bad_request("key_base64 is not valid base64");
                };
                if key.is_empty() {
                    return bad_request("key_base64 is empty or exceeds the row-key budget");
                }
                MutateOp::Delete { key }
            }
            _ => return bad_request(format!("unknown mutation op {kind}")),
        };
        let store = self.stores.get_mut(name).expect("store checked");
        if let Some((sequence, committed)) = store.mutations.get(&mutation_id) {
            if *committed == op {
                return ok(json!({
                    "store_id": store_id,
                    "mutation_id": mutation_id,
                    "sequence": sequence,
                    "deduplicated": true,
                }));
            }
            return conflict(
                "StateStore mutation identity was already committed with different content",
                Some("state_store.conflict"),
            );
        }
        store.sequence += 1;
        let sequence = store.sequence;
        match decoded {
            MutateOp::Put {
                key,
                payload,
                named,
            } => {
                // Unique named-key claims arbitrate first-committed-wins: a
                // put colliding with an existing claim commits as a
                // journaled no-op rather than a refusal.
                let unique_lost = named.iter().any(|(key_name, values)| {
                    let unique = named_keys
                        .iter()
                        .any(|(declared, unique)| declared == key_name && *unique);
                    unique
                        && store.rows.iter().any(|(row_key, row)| {
                            *row_key != key
                                && row.named.iter().any(|(other_name, other_values)| {
                                    other_name == key_name
                                        && values.iter().any(|value| other_values.contains(value))
                                })
                        })
                });
                if !unique_lost {
                    store.rows.insert(
                        key,
                        FakeRow {
                            payload,
                            named,
                            applied_seq: sequence,
                        },
                    );
                }
            }
            MutateOp::Delete { key } => {
                store.rows.remove(&key);
            }
        }
        store.mutations.insert(mutation_id, (sequence, op));
        ok(json!({
            "store_id": store_id,
            "mutation_id": mutation_id,
            "sequence": sequence,
            "deduplicated": false,
        }))
    }

    fn store_lookup(&mut self, body: &Value) -> TransportReply {
        let name = body
            .get("store")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let Some(store) = self.stores.get(name) else {
            return bad_request(format!("StateStore does not exist: {name}"));
        };
        let Some(key) = body
            .get("key_base64")
            .and_then(Value::as_str)
            .and_then(|raw| STANDARD.decode(raw).ok())
        else {
            return bad_request("key_base64 is not valid base64");
        };
        if key.is_empty() || key.len() > crate::state_store::STATE_STORE_MAX_KEY_BYTES {
            return bad_request("key_base64 is empty or exceeds the row-key budget");
        }
        let row = store.rows.get(&key);
        ok(json!({
            "store_id": store.store_id,
            "found": row.is_some(),
            "payload_base64": row.map(|row| STANDARD.encode(&row.payload)),
            "applied_cursor": row.map(|row| format!("{:020}", row.applied_seq)),
            "applied_seq": store.sequence,
            "committed_seq": store.sequence,
            "behind": false,
            "served_by": NODE,
        }))
    }

    fn store_get_by(&mut self, body: &Value) -> TransportReply {
        let name = body
            .get("store")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let Some(store) = self.stores.get(name) else {
            return bad_request(format!("StateStore does not exist: {name}"));
        };
        let named_key = body
            .get("named_key")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if !store.named_keys.iter().any(|(key, _)| key == named_key) {
            return bad_request(format!(
                "StateStore {name} does not declare named key {named_key}"
            ));
        }
        let values = body
            .get("values")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        if values.is_empty() {
            return bad_request("values must not be empty");
        }
        let limit = body
            .get("limit")
            .and_then(Value::as_u64)
            .unwrap_or(64)
            .clamp(1, 1_000) as usize;
        let rows: Vec<Value> = store
            .rows
            .iter()
            .filter(|(_, row)| {
                row.named.iter().any(|(key_name, posted)| {
                    key_name == named_key && values.iter().any(|value| posted.contains(value))
                })
            })
            .take(limit)
            .map(|(key, row)| {
                json!({
                    "key_base64": STANDARD.encode(key),
                    "payload_base64": STANDARD.encode(&row.payload),
                    "applied_cursor": format!("{:020}", row.applied_seq),
                })
            })
            .collect();
        ok(json!({
            "store_id": store.store_id,
            "rows": rows,
            "applied_seq": store.sequence,
            "committed_seq": store.sequence,
            "behind": false,
            "served_by": NODE,
        }))
    }

    fn store_iterate(&mut self, body: &Value) -> TransportReply {
        let name = body
            .get("store")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let Some(store) = self.stores.get(name) else {
            return bad_request(format!("StateStore does not exist: {name}"));
        };
        let after_key = match body.get("after_key_base64") {
            None | Some(Value::Null) => None,
            Some(raw) => match raw.as_str().and_then(|text| STANDARD.decode(text).ok()) {
                Some(key) => Some(key),
                None => return bad_request("after_key_base64 is not valid base64"),
            },
        };
        let limit = body
            .get("limit")
            .and_then(Value::as_u64)
            .unwrap_or(256)
            .clamp(1, 1_000) as usize;
        let mut iter = store.rows.iter().filter(|(key, _)| {
            after_key
                .as_ref()
                .map(|after| key.as_slice() > after.as_slice())
                .unwrap_or(true)
        });
        let rows: Vec<(&Vec<u8>, &FakeRow)> = iter.by_ref().take(limit).collect();
        let next_key_base64 = iter
            .next()
            .is_some()
            .then(|| rows.last().map(|(key, _)| STANDARD.encode(*key)))
            .flatten();
        ok(json!({
            "store_id": store.store_id,
            "rows": rows
                .iter()
                .map(|(key, row)| json!({
                    "key_base64": STANDARD.encode(*key),
                    "payload_base64": STANDARD.encode(&row.payload),
                    "applied_seq": row.applied_seq,
                }))
                .collect::<Vec<_>>(),
            "next_key_base64": next_key_base64,
            "pinned_seq": store.sequence,
            "served_by": NODE,
        }))
    }

    fn store_clear(&mut self, body: &Value) -> TransportReply {
        let name = body
            .get("store")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let Some(mutation_id) = parse_uuid(body.get("mutation_id")) else {
            return bad_request("mutation_id must be a UUID");
        };
        let Some(store) = self.stores.get(name) else {
            return bad_request(format!("StateStore does not exist: {name}"));
        };
        if store.source_feed.is_some() {
            return bad_request(
                "clear applies only to manual StateStores; Feed-derived stores replay from \
                 their source",
            );
        }
        let store_id = store.store_id;
        let canonical = json!({ "kind": "clear" });
        let store = self.stores.get_mut(name).expect("store checked");
        if let Some((sequence, committed)) = store.mutations.get(&mutation_id) {
            if *committed == canonical {
                return ok(json!({
                    "store_id": store_id,
                    "mutation_id": mutation_id,
                    "sequence": sequence,
                    "deduplicated": true,
                }));
            }
            return conflict(
                "StateStore mutation identity was already committed with different content",
                Some("state_store.conflict"),
            );
        }
        store.sequence += 1;
        let sequence = store.sequence;
        store.rows.clear();
        store.mutations.insert(mutation_id, (sequence, canonical));
        ok(json!({
            "store_id": store_id,
            "mutation_id": mutation_id,
            "sequence": sequence,
            "deduplicated": false,
        }))
    }

    fn store_sync(&mut self, body: &Value) -> TransportReply {
        let name = body
            .get("store")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let Some(store) = self.stores.get(name) else {
            return bad_request(format!("StateStore does not exist: {name}"));
        };
        if store.source_feed.is_none() {
            return bad_request("sync applies only to Feed-derived StateStores");
        }
        ok(json!({
            "store_id": store.store_id,
            "applied": 0,
            "frontier": Value::Null,
            "complete": true,
        }))
    }
}

fn parse_uuid(value: Option<&Value>) -> Option<Uuid> {
    value
        .and_then(Value::as_str)
        .and_then(|raw| Uuid::parse_str(raw).ok())
}

fn health_body() -> Value {
    json!({
        "status": "ok",
        "node_id": NODE,
        "members": 1,
        "protocol": { "major": 1, "minor": 0 },
        "limits": {
            "max_frame_bytes": crate::codec::MAX_FRAME_BYTES,
            "max_committed_read_bytes": crate::active_range::replication::MAX_COMMITTED_READ_BYTES,
            "max_logical_read_frames": 10_000,
            "max_logical_read_ranges": 128,
            "subscription_member_fetch_max_limit": 256,
            "subscription_member_max_lease_ticks": 600,
        },
    })
}

fn feedback_body() -> Value {
    json!({
        "recommended_batch_count": 100,
        "recommended_batch_bytes": 1_048_576,
        "pressure": 0.0,
        "retry_after_ms": 0,
        "max_frame_bytes": crate::codec::MAX_FRAME_BYTES,
        "hot_key": false,
    })
}

fn url_decode(value: &str) -> String {
    let mut out = Vec::with_capacity(value.len());
    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' && index + 2 < bytes.len() {
            if let Ok(byte) = u8::from_str_radix(&value[index + 1..index + 3], 16) {
                out.push(byte);
                index += 3;
                continue;
            }
        }
        out.push(if bytes[index] == b'+' {
            b' '
        } else {
            bytes[index]
        });
        index += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn command_label(command: &Command) -> &'static str {
    match command {
        Command::CreateDomain { .. } => "CREATE DOMAIN",
        Command::CreateSpace { .. } => "CREATE SPACE",
        Command::CreateFeed { .. } => "CREATE FEED",
        Command::EraseFeed { .. } => "ERASE FEED",
        Command::DefineStateStore { .. } => "DEFINE STATE STORE",
        Command::DefineIndex { .. } => "DEFINE INDEX",
        Command::RebuildIndex { .. } => "REBUILD INDEX",
        Command::PausePipe { .. } => "PAUSE PIPE",
        Command::ResumePipe { .. } => "RESUME PIPE",
        Command::DefinePipe { .. } => "DEFINE PIPE",
        Command::CreateWriter { .. } => "CREATE WRITER",
        Command::OpenWriterSession { .. } => "OPEN WRITER SESSION",
        Command::RevokeWriterSession { .. } => "REVOKE WRITER SESSION",
        Command::CreateReader { .. } => "CREATE READER",
        Command::CreateSubscription { .. } => "CREATE SUBSCRIPTION",
        Command::OpenReaderSession { .. } => "OPEN READER SESSION",
        Command::AcknowledgeReader { .. } => "ACKNOWLEDGE READER",
        Command::CloseReaderSession { .. } => "CLOSE READER SESSION",
        Command::SeekReader { .. } => "SEEK READER",
        Command::CreateRole { .. } => "CREATE ROLE",
        Command::Drop { .. } => "DROP",
        Command::Show { .. } => "SHOW",
        Command::Describe { .. } => "DESCRIBE",
        Command::Rename { .. } => "RENAME",
        Command::SeekSubscription { .. } => "SEEK SUBSCRIPTION",
        Command::SetHistoryPolicy { .. } => "SET HISTORY POLICY",
        Command::ClearHistoryPolicy { .. } => "DROP HISTORY POLICY",
        Command::SetLegalHold { .. } => "SET LEGAL HOLD",
        Command::DropLegalHold { .. } => "DROP LEGAL HOLD",
        Command::SetSchema { .. } => "SET SCHEMA",
        Command::ClearSchema { .. } => "DROP SCHEMA",
        _ => "COMMAND",
    }
}

fn show_label(kind: &ShowKind) -> &'static str {
    match kind {
        ShowKind::Domains => "DOMAINS",
        ShowKind::Spaces => "SPACES",
        ShowKind::Feeds => "FEEDS",
        ShowKind::Writers => "WRITERS",
        ShowKind::Readers => "READERS",
        ShowKind::Subscriptions => "SUBSCRIPTIONS",
        ShowKind::Roles => "ROLES",
        ShowKind::Grants => "GRANTS",
        ShowKind::ApiKeys => "APIKEYS",
        ShowKind::StorageNodes => "STORAGE NODES",
        ShowKind::Pipes => "PIPES",
        ShowKind::Indexes => "INDEXES",
        ShowKind::StateStores => "STATESTORES",
        ShowKind::HistoryPolicies => "HISTORY POLICIES",
        ShowKind::LegalHolds => "LEGAL HOLDS",
        ShowKind::Schemas => "SCHEMAS",
        ShowKind::AuditEvents => "AUDIT EVENTS",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sdk::{TemporaryFetch, TypedValue};

    async fn client_with_feed() -> Client {
        let client = FakeTransport::client();
        client
            .wcl(
                "CREATE DOMAIN t; CREATE FEED t.events; CREATE WRITER w TO t.events; \
                 CREATE READER r FROM t.events START AT BEGINNING;",
            )
            .await
            .unwrap();
        client
    }

    #[tokio::test]
    async fn health_reports_protocol_and_limits() {
        let client = FakeTransport::client();
        client.check_compatible().await.unwrap();
        let limits = client.transport_limits().await.unwrap();
        assert_eq!(limits.max_frame_bytes, 64 * 1024 * 1024);
        assert_eq!(limits.member_fetch_max_limit, 256);
    }

    #[tokio::test]
    async fn append_fetch_ack_advances_without_redelivery() {
        let client = client_with_feed().await;
        let writer = client.writer("w").await.unwrap();
        for index in 0..3_u8 {
            writer
                .append(
                    Uuid::new_v4(),
                    b"k",
                    &[b'a' + index],
                    1 + i64::from(index),
                    BTreeMap::new(),
                )
                .await
                .unwrap();
        }
        let mut reader = client.reader("r", 16).await.unwrap();
        let page = reader.fetch(2).await.unwrap();
        assert_eq!(page.records.len(), 2);
        assert_eq!(page.records[0].payload, b"a");
        reader.ack().await.unwrap();
        let next = reader.fetch(8).await.unwrap();
        assert_eq!(next.records.len(), 1);
        assert_eq!(next.records[0].payload, b"c");
        reader.ack().await.unwrap();
        assert!(reader.fetch(8).await.unwrap().records.is_empty());
    }

    #[tokio::test]
    async fn close_without_ack_redelivers_from_acknowledged_progress() {
        let client = client_with_feed().await;
        let writer = client.writer("w").await.unwrap();
        writer
            .append(Uuid::new_v4(), b"k", b"one", 1, BTreeMap::new())
            .await
            .unwrap();
        writer
            .append(Uuid::new_v4(), b"k", b"two", 2, BTreeMap::new())
            .await
            .unwrap();
        let mut reader = client.reader("r", 16).await.unwrap();
        let page = reader.fetch(1).await.unwrap();
        assert_eq!(page.records.len(), 1);
        reader.close().await.unwrap();
        let mut reader = client.reader("r", 16).await.unwrap();
        let redelivered = reader.fetch(8).await.unwrap();
        assert_eq!(redelivered.records.len(), 2);
        assert_eq!(redelivered.records[0].payload, b"one");
        reader.ack().await.unwrap();
        reader.close().await.unwrap();
        let mut reader = client.reader("r", 16).await.unwrap();
        assert!(reader.fetch(8).await.unwrap().records.is_empty());
    }

    #[tokio::test]
    async fn same_request_id_dedupes_to_the_original_outcome() {
        let client = client_with_feed().await;
        let writer = client.writer("w").await.unwrap();
        let request_id = Uuid::new_v4();
        let first = writer
            .append(request_id, b"k", b"v", 1, BTreeMap::new())
            .await
            .unwrap();
        let retry = writer
            .append(request_id, b"k", b"v", 1, BTreeMap::new())
            .await
            .unwrap();
        assert!(!first.deduplicated);
        assert!(retry.deduplicated);
        assert_eq!(retry.message_id, first.message_id);
        assert_eq!(retry.cursor, first.cursor);
        let page = client
            .temporary_fetch("t.events", TemporaryFetch::default())
            .await
            .unwrap();
        assert_eq!(page.records.len(), 1);
    }

    #[tokio::test]
    async fn stale_writer_epoch_is_refused_until_reopen() {
        let client = client_with_feed().await;
        let mut writer = client.writer("w").await.unwrap();
        // A second open supersedes the epoch this Writer holds.
        let rival = client.writer("w").await.unwrap();
        assert!(rival.session_epoch() > writer.session_epoch());
        let error = writer
            .append(Uuid::new_v4(), b"k", b"v", 1, BTreeMap::new())
            .await
            .unwrap_err();
        assert_eq!(error, SdkError::StaleEpoch);
        writer.reopen().await.unwrap();
        writer
            .append(Uuid::new_v4(), b"k", b"v", 1, BTreeMap::new())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn state_store_round_trips_lookup_get_by_iterate_and_clear() {
        let client = FakeTransport::client();
        client
            .wcl("CREATE DOMAIN s; DEFINE STATESTORE s.profiles KEYS department;")
            .await
            .unwrap();
        let store = client.state_store("s.profiles");
        let mutation_id = Uuid::new_v4();
        let named = vec![(
            "department".to_owned(),
            vec![TypedValue::Text("eng".to_owned())],
        )];
        let put = store
            .put_row(mutation_id, b"emp-1", b"alice", &named)
            .await
            .unwrap();
        assert!(!put.deduplicated);
        let retry = store
            .put_row(mutation_id, b"emp-1", b"alice", &named)
            .await
            .unwrap();
        assert!(retry.deduplicated);
        assert_eq!(retry.sequence, put.sequence);
        let found = store.lookup(b"emp-1", false).await.unwrap();
        assert_eq!(found.row.unwrap().payload, b"alice");
        let page = store
            .get_by(
                "department",
                &[TypedValue::Text("eng".to_owned())],
                None,
                false,
            )
            .await
            .unwrap();
        assert_eq!(page.rows.len(), 1);
        assert_eq!(page.rows[0].key, b"emp-1");
        let iter = store.iterate(None, Some(10)).await.unwrap();
        assert_eq!(iter.rows.len(), 1);
        assert_eq!(iter.next_key, None);
        store.clear(Uuid::new_v4()).await.unwrap();
        assert!(store.lookup(b"emp-1", false).await.unwrap().row.is_none());
        assert!(store.iterate(None, Some(10)).await.unwrap().rows.is_empty());
    }

    #[tokio::test]
    async fn unknown_paths_surface_a_decodable_error() {
        let client = FakeTransport::client();
        let reply = client
            .wire("POST", "/v1/no-such-route", None)
            .await
            .unwrap();
        assert_eq!(reply.status, 404);
        assert!(reply.body.get("error").is_some());
    }
}
