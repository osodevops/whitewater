//! Distributary: the built-in cross-Riverbed copy mechanism. A
//! Distributary is a managed pull of one remote Feed into one local
//! Feed, driven on the Node holding the Distributary lease. Records are
//! copied with Key, `event_time_ns`, and Metadata preserved; new local
//! Cursors are minted on the destination.
//!
//! Durable progress lives on the *remote* as a named Reader frontier
//! derived from the Distributary's id: the driver opens a remote Reader
//! session, fetches a bounded page, appends each record to the local
//! target through a deterministic request id, and acknowledges the
//! delivered cursor only after every append lands. A crash between
//! append and ack causes the remote to redeliver; the same derived
//! request id dedups the local retry. A lease move is fenced by the
//! remote Reader session — the new owner's open revokes the stale
//! epoch, so two Nodes can never drive the same frontier.
//!
//! Feed-derived StateStores on the destination re-derive from the
//! copied Feed rather than receiving rows; manual StateStore journal
//! shipping is tracked separately.

use uuid::Uuid;

use crate::admin::{AdminClient, AdminClientError};
use crate::api::WriterSessionAppendRequest;
use crate::control::{DistributaryDefinition, ReaderStart, ResourceKind, RiverbedLink};

/// Records fetched per drive cycle — the same bound a Pipe cycle uses.
pub const DISTRIBUTARY_DRIVE_LIMIT: usize = 64;

/// The remote Reader and local Writer identities one Distributary owns.
/// Both derive from the immutable `distributary_id` so a lease move
/// reopens the same named resources instead of duplicating progress.
pub fn distributary_reader_name(distributary_id: Uuid) -> String {
    format!("distributary.d{}", distributary_id.simple())
}

pub fn distributary_writer_name(distributary_id: Uuid) -> String {
    format!("distributary.w{}", distributary_id.simple())
}

/// The append identity for one copied record. Deriving from the source
/// record's remote Cursor makes every redelivery — after a crash, a
/// session fence, or a lease move — replay the identical request id, so
/// the destination's append dedup returns the original outcome instead
/// of writing a duplicate.
pub fn record_request_id(distributary_id: Uuid, source_cursor: &str) -> Uuid {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"whitewater-distributary-record-v1");
    hasher.update(distributary_id.as_bytes());
    hasher.update(source_cursor.as_bytes());
    let digest = hasher.finalize();
    let mut bytes: [u8; 16] = digest.as_bytes()[..16]
        .try_into()
        .expect("UUID digest length");
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

/// A remote Riverbed reached through a `RiverbedLink`: one
/// `AdminClient` per endpoint with transport-only failover. An API
/// refusal is authoritative — retrying it on another Node would refuse
/// identically, so only transport errors rotate the endpoint.
pub struct RemotePool {
    clients: Vec<AdminClient>,
    next: usize,
}

impl RemotePool {
    pub fn new(link: &RiverbedLink) -> Result<Self, String> {
        let key = std::env::var(&link.key_env).map_err(|_| {
            format!(
                "Riverbed Link {} credential {} is not set on this Node",
                link.name, link.key_env
            )
        })?;
        if key.is_empty() {
            return Err(format!(
                "Riverbed Link {} credential {} is empty",
                link.name, link.key_env
            ));
        }
        let clients = link
            .endpoints
            .iter()
            .map(|endpoint| AdminClient::new(endpoint.trim_end_matches('/'), key.clone()))
            .collect::<Vec<_>>();
        if clients.is_empty() {
            return Err(format!("Riverbed Link {} has no endpoints", link.name));
        }
        Ok(Self { clients, next: 0 })
    }

    fn pick(&mut self) -> AdminClient {
        let client = self.clients[self.next % self.clients.len()].clone();
        self.next = self.next.wrapping_add(1);
        client
    }

    /// Run `call` against each endpoint in rotation until one answers.
    /// API errors return immediately; transport errors try the next.
    pub async fn run<T, F, Fut>(&mut self, call: F) -> Result<T, String>
    where
        F: Fn(AdminClient) -> Fut,
        Fut: std::future::Future<Output = Result<T, AdminClientError>>,
    {
        let mut last = String::new();
        for _ in 0..self.clients.len() {
            let client = self.pick();
            match call(client).await {
                Ok(value) => return Ok(value),
                Err(AdminClientError::Transport(error)) => {
                    last = format!("{error}");
                }
                Err(AdminClientError::Api { status, message }) => {
                    return Err(format!("remote refused ({status}): {message}"));
                }
            }
        }
        Err(format!("no reachable endpoint: {last}"))
    }
}

/// Live driver state for one locally-owned Distributary. Sessions are
/// re-opened lazily after any failure so a fenced epoch never persists.
#[derive(Default)]
pub struct DistributaryDriveState {
    /// Remote Reader session epoch; `None` forces a re-open.
    pub reader_epoch: Option<u64>,
    /// Local Writer session epoch; `None` forces a re-open.
    pub writer_epoch: Option<u64>,
    /// Whether the named Reader/Writer declarations have been verified.
    pub declared: bool,
    /// The lease epoch this driver state was built under — a bump
    /// discards session state even if ownership stayed local.
    pub lease_epoch: u64,
}

impl DistributaryDriveState {
    /// Abandon an in-flight remote page: reopening the Reader session
    /// rewinds the remote delivered frontier back to acknowledged
    /// progress, so records the page delivered but this driver never
    /// appended are fetched again instead of skipped.
    fn abandon_page(&mut self) {
        self.reader_epoch = None;
    }
}

/// One bounded drive cycle for a locally-owned Distributary: declare
/// the remote Reader and local Writer if missing, open sessions as
/// needed, copy up to `limit` records, then acknowledge the remote
/// frontier. `target_feed` is the local Feed name resolved from
/// `target_feed_id` by the caller. Returns the records copied.
pub async fn drive_distributary_once(
    local: &AdminClient,
    remote: &mut RemotePool,
    definition: &DistributaryDefinition,
    target_feed: &str,
    state: &mut DistributaryDriveState,
    limit: usize,
) -> Result<usize, String> {
    let reader = distributary_reader_name(definition.distributary_id);
    let writer = distributary_writer_name(definition.distributary_id);

    if !state.declared {
        // The Reader name is a deterministic function of the
        // Distributary id, so redeclaring can only mean this driver has
        // declared it before — a duplicate refusal is safe to treat as
        // already-declared.
        let described = remote
            .run(|client| {
                let name = reader.clone();
                async move { client.describe(ResourceKind::Reader, &name).await }
            })
            .await;
        if described.is_err() {
            let source_feed = definition.source_feed.clone();
            let name = reader.clone();
            remote
                .run(|client| {
                    let name = name.clone();
                    let feed = source_feed.clone();
                    async move {
                        client
                            .create_reader(name, feed, ReaderStart::Beginning)
                            .await
                    }
                })
                .await
                .map_err(|error| format!("remote Reader declaration failed: {error}"))?;
        }
        if local.describe(ResourceKind::Writer, &writer).await.is_err() {
            local
                .create_writer(&writer, target_feed)
                .await
                .map_err(|error| format!("local Writer declaration failed: {error}"))?;
        }
        state.declared = true;
    }

    if state.reader_epoch.is_none() {
        let name = reader.clone();
        let session = remote
            .run(|client| {
                let name = name.clone();
                async move {
                    client
                        .open_reader_session(Uuid::new_v4(), name, limit)
                        .await
                }
            })
            .await
            .map_err(|error| format!("remote Reader open failed: {error}"))?;
        state.reader_epoch = Some(session.session_epoch);
    }
    let reader_epoch = state.reader_epoch.expect("reader session opened");

    if state.writer_epoch.is_none() {
        let session = local
            .open_writer_session(&writer)
            .await
            .map_err(|error| format!("local Writer open failed: {error}"))?;
        state.writer_epoch = Some(
            session.results[0].data["session_epoch"]
                .as_u64()
                .ok_or_else(|| "local Writer open returned no session_epoch".to_owned())?,
        );
    }
    let writer_epoch = state.writer_epoch.expect("writer session opened");

    let page = {
        let name = reader.clone();
        remote
            .run(|client| {
                let name = name.clone();
                async move {
                    client
                        .reader_session(name, reader_epoch)
                        .fetch(Uuid::new_v4(), Some(limit))
                        .await
                }
            })
            .await
            .map_err(|error| {
                state.reader_epoch = None;
                format!("remote fetch failed: {error}")
            })?
    };
    if page.records.is_empty() {
        return Ok(0);
    }

    let mut appended = 0usize;
    for record in &page.records {
        let request = WriterSessionAppendRequest {
            request_id: record_request_id(definition.distributary_id, &record.cursor),
            writer: writer.clone(),
            session_epoch: writer_epoch,
            event_time_ns: Some(record.event_time_ns.clone()),
            key_base64: record.key_base64.clone(),
            payload_base64: record.payload_base64.clone(),
            metadata_base64: record.metadata_base64.clone(),
            use_compression: false,
        };
        match local.append_writer(&request).await {
            Ok(_) => appended += 1,
            Err(AdminClientError::Api { status, message })
                if status.as_u16() == 409 || status.is_client_error() =>
            {
                // A fenced or refused append must not be retried past
                // this cycle: surface it before acknowledging anything.
                // The reader session also resets — the page's delivered
                // frontier has already passed this Cursor, and only a
                // reopen rewinds it to acknowledged progress.
                state.writer_epoch = None;
                state.abandon_page();
                return Err(format!(
                    "local append refused for remote Cursor {} ({status}): {message}",
                    record.cursor
                ));
            }
            Err(error) => {
                state.writer_epoch = None;
                state.abandon_page();
                return Err(format!(
                    "local append failed for remote Cursor {}: {error}",
                    record.cursor
                ));
            }
        }
    }

    let Some(delivered) = page.delivered_cursor.clone() else {
        state.abandon_page();
        return Err("remote fetch returned records without a delivered cursor".to_owned());
    };
    {
        let name = reader.clone();
        remote
            .run(|client| {
                let name = name.clone();
                let delivered = delivered.clone();
                async move {
                    client
                        .reader_session(name, reader_epoch)
                        .acknowledge(Uuid::new_v4(), delivered)
                        .await
                }
            })
            .await
            .map_err(|error| {
                state.reader_epoch = None;
                format!("remote acknowledge failed: {error}")
            })?;
    }
    Ok(appended)
}

/// The Distributary a node currently drives; the supervisor reconciles
/// this set against committed leases every tick.
pub struct DistributaryDriver {
    pub state: DistributaryDriveState,
    pub remote: RemotePool,
}

/// What one supervisor tick observed for a locally-owned Distributary.
#[derive(Debug)]
pub enum DistributaryDriveOutcome {
    /// This Node does not hold the lease.
    NotOwner,
    /// No remote records were waiting.
    Idle,
    /// `count` records copied and acknowledged this cycle.
    Copied { distributary_id: Uuid, count: usize },
    /// The cycle failed; sessions are reset so the next tick re-opens.
    Failed {
        distributary_id: Uuid,
        message: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::ResourceStatus;

    #[test]
    fn record_request_id_is_deterministic_and_scoped() {
        let id = Uuid::new_v4();
        let one = record_request_id(id, "cursor-a");
        assert_eq!(one, record_request_id(id, "cursor-a"));
        // A different remote Cursor or Distributary must derive a
        // different append identity, or dedup would alias records.
        assert_ne!(one, record_request_id(id, "cursor-b"));
        assert_ne!(one, record_request_id(Uuid::new_v4(), "cursor-a"));
    }

    #[test]
    fn driver_names_are_valid_dotted_names() {
        let id = Uuid::new_v4();
        for name in [distributary_reader_name(id), distributary_writer_name(id)] {
            assert!(
                crate::control::validate_dotted_name(&name).is_ok(),
                "{name}"
            );
        }
        assert_ne!(distributary_reader_name(id), distributary_writer_name(id));
    }

    #[test]
    fn remote_pool_refuses_missing_or_empty_credentials() {
        let link = RiverbedLink {
            link_id: Uuid::new_v4(),
            name: "dr".to_owned(),
            endpoints: vec!["https://dr:9392".to_owned()],
            key_env: "WHITEWATER_TEST_MISSING_DIST_KEY".to_owned(),
            status: ResourceStatus::Active,
            created_at_ns: 0,
        };
        std::env::remove_var("WHITEWATER_TEST_MISSING_DIST_KEY");
        let error = RemotePool::new(&link).err().expect("missing key must fail");
        assert!(error.contains("not set"), "{error}");
        std::env::set_var("WHITEWATER_TEST_MISSING_DIST_KEY", "");
        let error = RemotePool::new(&link).err().expect("empty key must fail");
        assert!(error.contains("empty"), "{error}");
        std::env::set_var("WHITEWATER_TEST_MISSING_DIST_KEY", "secret");
        let pool = RemotePool::new(&link).unwrap();
        assert_eq!(pool.clients.len(), 1);
        std::env::remove_var("WHITEWATER_TEST_MISSING_DIST_KEY");
        // Key material stays in the process environment — the catalog
        // row carries only the variable name.
        assert!(!serde_json::to_string(&link).unwrap().contains("secret"));
    }

    #[test]
    fn abandon_page_forces_reader_reopen() {
        // A mid-page local append failure must rewind the remote
        // delivered frontier: reopening resets it to acknowledged
        // progress so delivered-but-unappended records are refetched
        // rather than skipped past.
        let mut state = DistributaryDriveState {
            reader_epoch: Some(7),
            writer_epoch: Some(3),
            declared: true,
            lease_epoch: 2,
        };
        state.abandon_page();
        assert_eq!(state.reader_epoch, None);
        assert_eq!(state.writer_epoch, Some(3));
        assert!(state.declared);
    }
}
