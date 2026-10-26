use std::{collections::BTreeSet, env};

use reqwest::StatusCode;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    active_range::StorageNodeId,
    api::{
        ReaderAckRequest, ReaderFetchRequest, ReaderFetchResponse, ReaderOpenRequest,
        ReaderSessionResponse, TemporaryReaderFetchRequest, TemporaryReaderFetchResponse,
        WriterAppendResponse, WriterBatchAppendRequest, WriterBatchAppendResponse,
        WriterSessionAppendRequest,
    },
    control::{
        namespace_matches, Command, ControlExecution, GrantEffect, IssuedApiKey, IssuedApiKeySet,
        PermissionAction, ReaderStart, ResourceKind, ShowKind,
    },
    reader::ReaderRetryPolicy,
};

pub const ADMIN_API_KEY_ENV: &str = "WHITEWATER_ADMIN_API_KEY";
pub const CLIENT_API_KEY_ENV: &str = "WHITEWATER_API_KEY";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct WclRequest {
    #[serde(default)]
    pub request_id: Option<Uuid>,
    pub script: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CommandBatchRequest {
    #[serde(default)]
    pub request_id: Option<Uuid>,
    pub commands: Vec<Command>,
}

#[derive(Clone)]
pub struct AdminAuthenticator {
    expected_digest: Option<[u8; 32]>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AdminAuthError {
    #[error("Admin API authentication is not configured")]
    NotConfigured,
    #[error("missing Bearer API key")]
    Missing,
    #[error("invalid Admin API key")]
    Invalid,
    #[error("{ADMIN_API_KEY_ENV} must contain at least 24 characters")]
    WeakKey,
}

impl AdminAuthenticator {
    pub fn from_env() -> Result<Self, AdminAuthError> {
        Self::new(env::var(ADMIN_API_KEY_ENV).ok())
    }

    pub fn new(api_key: Option<String>) -> Result<Self, AdminAuthError> {
        let expected_digest = match api_key {
            Some(api_key) if api_key.len() >= 24 => {
                Some(*blake3::hash(api_key.as_bytes()).as_bytes())
            }
            Some(_) => return Err(AdminAuthError::WeakKey),
            None => None,
        };
        Ok(Self { expected_digest })
    }

    pub fn authorize(&self, authorization: Option<&str>) -> Result<(), AdminAuthError> {
        self.authorize_inner(authorization, None)
    }

    /// Authorize against the configured Admin key plus catalog-issued
    /// identities from `issued` (the Control Plane-maintained view).
    /// An issued identity authorizes admin endpoints when its Role
    /// holds `manage` on the root `*` namespace and it has not expired
    /// as of `now_unix_ns`.
    pub fn authorize_issued(
        &self,
        authorization: Option<&str>,
        issued: &IssuedApiKeySet,
        now_unix_ns: i64,
    ) -> Result<(), AdminAuthError> {
        self.authorize_inner(authorization, Some((issued, now_unix_ns)))
    }

    /// Authenticate the Bearer credential without evaluating grants —
    /// the configured bootstrap key or any unexpired issued identity.
    /// Resource-named data-plane endpoints authenticate before
    /// resolution so an invalid credential cannot probe existence, then
    /// run `authorize_scoped` against the resolved owning Feed.
    pub fn authenticate(
        &self,
        authorization: Option<&str>,
        issued: &IssuedApiKeySet,
        now_unix_ns: i64,
    ) -> Result<(), AdminAuthError> {
        self.verified_identity(authorization, issued, now_unix_ns)
            .map(|_| ())
    }

    /// Resolve the supplied credential: `Ok(None)` when the configured
    /// bootstrap key matched, `Ok(Some(identity))` for an unexpired
    /// issued identity, and `Err` for missing, unknown, or expired
    /// credentials. Grant evaluation is the caller's concern.
    fn verified_identity<'a>(
        &self,
        authorization: Option<&str>,
        issued: &'a IssuedApiKeySet,
        now_unix_ns: i64,
    ) -> Result<Option<&'a IssuedApiKey>, AdminAuthError> {
        if self.expected_digest.is_none() && issued.is_empty() {
            return Err(AdminAuthError::NotConfigured);
        }
        let supplied = authorization
            .and_then(|value| value.strip_prefix("Bearer "))
            .filter(|value| !value.is_empty())
            .ok_or(AdminAuthError::Missing)?;
        let actual = *blake3::hash(supplied.as_bytes()).as_bytes();
        if self
            .expected_digest
            .is_some_and(|expected| constant_time_equal(&expected, &actual))
        {
            return Ok(None);
        }
        if let Some(identity) = issued.get(&actual) {
            if identity
                .expires_at_ns
                .is_none_or(|expiry| now_unix_ns < expiry)
            {
                return Ok(Some(identity));
            }
        }
        Err(AdminAuthError::Invalid)
    }

    /// Authorize a data-plane `action` against `namespace` (a Feed
    /// name, or any dotted resource inside a Domain). The configured
    /// bootstrap key and unexpired riverbed-admin identities always
    /// pass; otherwise an unexpired issued identity needs a grant whose
    /// pattern covers `namespace` for `action` — `manage` implies
    /// `read` and `write`.
    pub fn authorize_scoped(
        &self,
        authorization: Option<&str>,
        issued: &IssuedApiKeySet,
        now_unix_ns: i64,
        namespace: &str,
        action: PermissionAction,
    ) -> Result<(), AdminAuthError> {
        let Some(identity) = self.verified_identity(authorization, issued, now_unix_ns)? else {
            return Ok(());
        };
        // A covering DENY wins over every allow, `manage`-implied
        // coverage, and Riverbed-admin status. The bootstrap key is
        // the only break-glass credential: it returns `Ok(None)` from
        // `verified_identity` before this point.
        let denied = identity.grants.iter().any(|grant| {
            grant.effect == crate::control::GrantEffect::Deny
                && (grant.actions.contains(&action)
                    || grant.actions.contains(&PermissionAction::Manage))
                && namespace_matches(&grant.namespace, namespace)
        });
        if denied {
            return Err(AdminAuthError::Invalid);
        }
        if identity.riverbed_admin
            || identity.grants.iter().any(|grant| {
                grant.effect == crate::control::GrantEffect::Allow
                    && (grant.actions.contains(&action)
                        || grant.actions.contains(&PermissionAction::Manage))
                    && namespace_matches(&grant.namespace, namespace)
            })
        {
            return Ok(());
        }
        Err(AdminAuthError::Invalid)
    }

    /// Authorize `action` across a whole Domain — for Domain-scoped
    /// Subscription member operations, whose member Feeds attach
    /// incrementally. The bootstrap key and riverbed-admin identities
    /// pass; otherwise an unexpired issued identity needs a spanning
    /// grant (`*` or `{domain}.*`) for `action` — an exact grant names
    /// one resource, never a Domain's contents.
    pub fn authorize_domain_scoped(
        &self,
        authorization: Option<&str>,
        issued: &IssuedApiKeySet,
        now_unix_ns: i64,
        domain: &str,
        action: PermissionAction,
    ) -> Result<(), AdminAuthError> {
        let Some(identity) = self.verified_identity(authorization, issued, now_unix_ns)? else {
            return Ok(());
        };
        let denied = identity.grants.iter().any(|grant| {
            grant.effect == crate::control::GrantEffect::Deny
                && (grant.actions.contains(&action)
                    || grant.actions.contains(&PermissionAction::Manage))
                && crate::control::grant_covers_domain(&grant.namespace, domain)
        });
        if denied {
            return Err(AdminAuthError::Invalid);
        }
        if identity.riverbed_admin
            || identity.grants.iter().any(|grant| {
                grant.effect == crate::control::GrantEffect::Allow
                    && (grant.actions.contains(&action)
                        || grant.actions.contains(&PermissionAction::Manage))
                    && crate::control::grant_covers_domain(&grant.namespace, domain)
            })
        {
            return Ok(());
        }
        Err(AdminAuthError::Invalid)
    }

    fn authorize_inner(
        &self,
        authorization: Option<&str>,
        issued: Option<(&IssuedApiKeySet, i64)>,
    ) -> Result<(), AdminAuthError> {
        let issued_empty = issued.is_none_or(|(set, _)| set.is_empty());
        if self.expected_digest.is_none() && issued_empty {
            return Err(AdminAuthError::NotConfigured);
        }
        let supplied = authorization
            .and_then(|value| value.strip_prefix("Bearer "))
            .filter(|value| !value.is_empty())
            .ok_or(AdminAuthError::Missing)?;
        let actual = *blake3::hash(supplied.as_bytes()).as_bytes();
        if self
            .expected_digest
            .is_some_and(|expected| constant_time_equal(&expected, &actual))
        {
            return Ok(());
        }
        if let Some((set, now_unix_ns)) = issued {
            if let Some(identity) = set.get(&actual) {
                let unexpired = identity
                    .expires_at_ns
                    .is_none_or(|expiry| now_unix_ns < expiry);
                let denied = identity.grants.iter().any(|grant| {
                    grant.effect == crate::control::GrantEffect::Deny
                        && grant.actions.contains(&PermissionAction::Manage)
                        && namespace_matches(&grant.namespace, "*")
                });
                if identity.riverbed_admin && unexpired && !denied {
                    return Ok(());
                }
            }
        }
        Err(AdminAuthError::Invalid)
    }
}

fn constant_time_equal(left: &[u8; 32], right: &[u8; 32]) -> bool {
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

#[derive(Clone)]
pub struct AdminClient {
    endpoint: String,
    api_key: String,
    http: reqwest::Client,
}

#[derive(Debug, Error)]
pub enum AdminClientError {
    #[error("Admin API request failed: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("Admin API returned HTTP {status}: {message}")]
    Api { status: StatusCode, message: String },
}

impl AdminClient {
    pub fn new(endpoint: impl Into<String>, api_key: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into().trim_end_matches('/').to_owned(),
            api_key: api_key.into(),
            http: reqwest::Client::new(),
        }
    }

    pub async fn execute_wcl(
        &self,
        script: impl Into<String>,
    ) -> Result<ControlExecution, AdminClientError> {
        self.execute_wcl_with_request_id(script, Uuid::new_v4())
            .await
    }

    pub async fn execute_wcl_with_request_id(
        &self,
        script: impl Into<String>,
        request_id: Uuid,
    ) -> Result<ControlExecution, AdminClientError> {
        self.post(
            "/v1/admin/wcl",
            &WclRequest {
                request_id: Some(request_id),
                script: script.into(),
            },
        )
        .await
    }

    pub async fn execute_commands(
        &self,
        commands: Vec<Command>,
    ) -> Result<ControlExecution, AdminClientError> {
        self.execute_commands_with_request_id(commands, Uuid::new_v4())
            .await
    }

    pub async fn execute_commands_with_request_id(
        &self,
        commands: Vec<Command>,
        request_id: Uuid,
    ) -> Result<ControlExecution, AdminClientError> {
        self.post(
            "/v1/admin/commands",
            &CommandBatchRequest {
                request_id: Some(request_id),
                commands,
            },
        )
        .await
    }

    pub async fn create_domain(
        &self,
        name: impl Into<String>,
    ) -> Result<ControlExecution, AdminClientError> {
        self.execute_one(Command::CreateDomain { name: name.into() })
            .await
    }

    pub async fn create_space(
        &self,
        name: impl Into<String>,
    ) -> Result<ControlExecution, AdminClientError> {
        self.execute_one(Command::CreateSpace { name: name.into() })
            .await
    }

    pub async fn create_feed(
        &self,
        name: impl Into<String>,
    ) -> Result<ControlExecution, AdminClientError> {
        self.execute_one(Command::CreateFeed { name: name.into() })
            .await
    }

    pub async fn create_writer(
        &self,
        name: impl Into<String>,
        feed: impl Into<String>,
    ) -> Result<ControlExecution, AdminClientError> {
        self.execute_one(Command::CreateWriter {
            name: name.into(),
            feed: feed.into(),
        })
        .await
    }

    pub async fn open_writer_session(
        &self,
        writer: impl Into<String>,
    ) -> Result<ControlExecution, AdminClientError> {
        self.execute_one(Command::OpenWriterSession {
            writer: writer.into(),
        })
        .await
    }

    pub async fn allocate_writer_sequence(
        &self,
        writer: impl Into<String>,
        session_epoch: u64,
    ) -> Result<ControlExecution, AdminClientError> {
        self.execute_one(Command::AllocateWriterSequence {
            writer: writer.into(),
            session_epoch,
        })
        .await
    }

    pub async fn revoke_writer_session(
        &self,
        writer: impl Into<String>,
        session_epoch: u64,
    ) -> Result<ControlExecution, AdminClientError> {
        self.execute_one(Command::RevokeWriterSession {
            writer: writer.into(),
            session_epoch,
        })
        .await
    }

    pub async fn create_reader(
        &self,
        name: impl Into<String>,
        feed: impl Into<String>,
        start: ReaderStart,
    ) -> Result<ControlExecution, AdminClientError> {
        self.execute_one(Command::CreateReader {
            name: name.into(),
            feed: feed.into(),
            start,
        })
        .await
    }

    pub async fn create_role(
        &self,
        name: impl Into<String>,
    ) -> Result<ControlExecution, AdminClientError> {
        self.execute_one(Command::CreateRole { name: name.into() })
            .await
    }

    /// Issue a catalog-persisted API-key identity. `digest` is the
    /// BLAKE3-256 hex verifier of the caller-held key, computed
    /// client-side (e.g. `blake3(key)`); the plaintext key is never
    /// sent to the server.
    pub async fn issue_api_key(
        &self,
        name: impl Into<String>,
        role: impl Into<String>,
        digest: impl Into<String>,
        expires_at_ns: Option<i64>,
    ) -> Result<ControlExecution, AdminClientError> {
        self.execute_one(Command::IssueApiKey {
            name: name.into(),
            role: role.into(),
            digest: digest.into(),
            expires_at_ns,
        })
        .await
    }

    pub async fn revoke_api_key(
        &self,
        name: impl Into<String>,
    ) -> Result<ControlExecution, AdminClientError> {
        self.execute_one(Command::RevokeApiKey { name: name.into() })
            .await
    }

    pub async fn rename(
        &self,
        kind: ResourceKind,
        current: impl Into<String>,
        new: impl Into<String>,
    ) -> Result<ControlExecution, AdminClientError> {
        self.execute_one(Command::Rename {
            kind,
            current: current.into(),
            new: new.into(),
        })
        .await
    }

    pub async fn drop_resource(
        &self,
        kind: ResourceKind,
        name: impl Into<String>,
    ) -> Result<ControlExecution, AdminClientError> {
        self.execute_one(Command::Drop {
            kind,
            name: name.into(),
        })
        .await
    }

    /// Declare or replace the History Policy on a Domain or Feed —
    /// fields the scope names override the inherited policy.
    pub async fn set_history_policy(
        &self,
        scope: crate::control::PolicyScope,
        policy: crate::control::HistoryPolicy,
    ) -> Result<ControlExecution, AdminClientError> {
        self.execute_one(Command::SetHistoryPolicy { scope, policy })
            .await
    }

    /// Remove a Domain or Feed's declared History Policy override so it
    /// returns to pure inheritance.
    pub async fn clear_history_policy(
        &self,
        scope: crate::control::PolicyScope,
    ) -> Result<ControlExecution, AdminClientError> {
        self.execute_one(Command::ClearHistoryPolicy { scope })
            .await
    }

    /// Declare or replace record schema metadata on a Domain or Feed
    /// — sides the scope names override the inherited declaration.
    pub async fn set_schema(
        &self,
        scope: crate::control::PolicyScope,
        schema: crate::control::FeedSchema,
    ) -> Result<ControlExecution, AdminClientError> {
        self.execute_one(Command::SetSchema { scope, schema }).await
    }

    /// Remove a Domain or Feed's declared schema override so it
    /// returns to pure inheritance (or schema-agnostic bytes).
    pub async fn clear_schema(
        &self,
        scope: crate::control::PolicyScope,
    ) -> Result<ControlExecution, AdminClientError> {
        self.execute_one(Command::ClearSchema { scope }).await
    }

    pub async fn show(&self, kind: ShowKind) -> Result<ControlExecution, AdminClientError> {
        self.execute_one(Command::Show { kind }).await
    }

    pub async fn describe(
        &self,
        kind: ResourceKind,
        name: impl Into<String>,
    ) -> Result<ControlExecution, AdminClientError> {
        self.execute_one(Command::Describe {
            kind,
            name: name.into(),
        })
        .await
    }

    pub async fn inspect_placement(
        &self,
        feed: impl Into<String>,
    ) -> Result<ControlExecution, AdminClientError> {
        self.execute_one(Command::InspectPlacement { feed: feed.into() })
            .await
    }

    #[deprecated(
        note = "direct owner transfer is refused; a verified movement workflow is required"
    )]
    pub async fn transfer_active_range_ownership(
        &self,
        feed: impl Into<String>,
        owner: StorageNodeId,
    ) -> Result<ControlExecution, AdminClientError> {
        self.execute_one(Command::TransferActiveRangeOwnership {
            feed: feed.into(),
            owner,
        })
        .await
    }

    pub async fn grant_namespace(
        &self,
        actions: impl IntoIterator<Item = PermissionAction>,
        namespace: impl Into<String>,
        role: impl Into<String>,
    ) -> Result<ControlExecution, AdminClientError> {
        self.execute_one(Command::Grant {
            actions: actions.into_iter().collect::<BTreeSet<_>>(),
            namespace: namespace.into(),
            role: role.into(),
            effect: GrantEffect::Allow,
        })
        .await
    }

    /// Fence `actions` on `namespace` for `role` ahead of every
    /// covering allow — the explicit-deny counterpart to
    /// [`AdminClient::grant_namespace`].
    pub async fn deny_namespace(
        &self,
        actions: impl IntoIterator<Item = PermissionAction>,
        namespace: impl Into<String>,
        role: impl Into<String>,
    ) -> Result<ControlExecution, AdminClientError> {
        self.execute_one(Command::Grant {
            actions: actions.into_iter().collect::<BTreeSet<_>>(),
            namespace: namespace.into(),
            role: role.into(),
            effect: GrantEffect::Deny,
        })
        .await
    }

    /// Remove `actions` from `role`'s GRANT rows on `namespace` —
    /// the listed actions subtract from covering allow rows and a row
    /// that empties is dropped.
    pub async fn revoke_namespace(
        &self,
        actions: impl IntoIterator<Item = PermissionAction>,
        namespace: impl Into<String>,
        role: impl Into<String>,
    ) -> Result<ControlExecution, AdminClientError> {
        self.revoke_namespace_effect(actions, namespace, role, GrantEffect::Allow)
            .await
    }

    /// Remove `actions` from `role`'s DENY rows on `namespace`.
    pub async fn revoke_deny_namespace(
        &self,
        actions: impl IntoIterator<Item = PermissionAction>,
        namespace: impl Into<String>,
        role: impl Into<String>,
    ) -> Result<ControlExecution, AdminClientError> {
        self.revoke_namespace_effect(actions, namespace, role, GrantEffect::Deny)
            .await
    }

    async fn revoke_namespace_effect(
        &self,
        actions: impl IntoIterator<Item = PermissionAction>,
        namespace: impl Into<String>,
        role: impl Into<String>,
        effect: GrantEffect,
    ) -> Result<ControlExecution, AdminClientError> {
        self.execute_one(Command::RevokeGrant {
            actions: actions.into_iter().collect::<BTreeSet<_>>(),
            namespace: namespace.into(),
            role: role.into(),
            effect,
        })
        .await
    }

    pub async fn explain_access(
        &self,
        role: impl Into<String>,
        action: PermissionAction,
        feed: impl Into<String>,
    ) -> Result<ControlExecution, AdminClientError> {
        self.execute_one(Command::ExplainAccess {
            role: role.into(),
            action,
            feed: feed.into(),
        })
        .await
    }

    pub async fn seek_reader(
        &self,
        reader: impl Into<String>,
        start: ReaderStart,
    ) -> Result<ControlExecution, AdminClientError> {
        self.execute_one(Command::SeekReader {
            reader: reader.into(),
            start,
        })
        .await
    }

    pub fn writer_session(
        &self,
        writer: impl Into<String>,
        session_epoch: u64,
    ) -> WriterSessionClient {
        WriterSessionClient {
            admin: self.clone(),
            writer: writer.into(),
            session_epoch,
            use_compression: false,
        }
    }

    pub async fn append_writer(
        &self,
        request: &WriterSessionAppendRequest,
    ) -> Result<WriterAppendResponse, AdminClientError> {
        self.post("/v1/writers/append", request).await
    }

    pub async fn append_writer_batch(
        &self,
        records: Vec<WriterSessionAppendRequest>,
    ) -> Result<WriterBatchAppendResponse, AdminClientError> {
        self.post(
            "/v1/writers/append-batch",
            &WriterBatchAppendRequest { records },
        )
        .await
    }

    pub async fn open_reader_session(
        &self,
        request_id: Uuid,
        reader: impl Into<String>,
        capacity: usize,
    ) -> Result<ReaderSessionResponse, AdminClientError> {
        self.post(
            "/v1/readers/open",
            &ReaderOpenRequest {
                request_id,
                reader: reader.into(),
                capacity,
            },
        )
        .await
    }

    pub async fn fetch_temporary_reader(
        &self,
        request: &TemporaryReaderFetchRequest,
    ) -> Result<TemporaryReaderFetchResponse, AdminClientError> {
        self.post("/v1/readers/temporary/fetch", request).await
    }

    pub async fn fetch_temporary_reader_with_retry(
        &self,
        request: &TemporaryReaderFetchRequest,
        policy: ReaderRetryPolicy,
    ) -> Result<TemporaryReaderFetchResponse, AdminClientError> {
        let mut attempt = 0;
        loop {
            match self.fetch_temporary_reader(request).await {
                Ok(response) => return Ok(response),
                Err(error) if attempt + 1 < policy.max_attempts => {
                    tokio::time::sleep(policy.delay(attempt, 0)).await;
                    attempt += 1;
                    let _ = error;
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub fn reader_session(
        &self,
        reader: impl Into<String>,
        session_epoch: u64,
    ) -> ReaderSessionClient {
        ReaderSessionClient {
            admin: self.clone(),
            reader: reader.into(),
            session_epoch,
        }
    }

    async fn execute_one(&self, command: Command) -> Result<ControlExecution, AdminClientError> {
        self.execute_commands(vec![command]).await
    }

    async fn post<T, R>(&self, path: &str, request: &T) -> Result<R, AdminClientError>
    where
        T: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        let response = self
            .http
            .post(format!("{}{}", self.endpoint, path))
            .bearer_auth(&self.api_key)
            .json(request)
            .send()
            .await?;
        let status = response.status();
        let bytes = response.bytes().await?;
        if status.is_success() {
            Ok(
                serde_json::from_slice(&bytes).map_err(|error| AdminClientError::Api {
                    status,
                    message: format!("invalid success response: {error}"),
                })?,
            )
        } else {
            let message = serde_json::from_slice::<serde_json::Value>(&bytes)
                .ok()
                .and_then(|value| {
                    value
                        .get("error")
                        .and_then(|error| error.as_str())
                        .map(ToOwned::to_owned)
                })
                .unwrap_or_else(|| String::from_utf8_lossy(&bytes).into_owned());
            Err(AdminClientError::Api { status, message })
        }
    }
}

#[derive(Clone)]
pub struct ReaderSessionClient {
    admin: AdminClient,
    reader: String,
    session_epoch: u64,
}

impl ReaderSessionClient {
    pub async fn fetch(
        &self,
        request_id: Uuid,
        limit: Option<usize>,
    ) -> Result<ReaderFetchResponse, AdminClientError> {
        self.admin
            .post(
                "/v1/readers/fetch",
                &ReaderFetchRequest {
                    request_id,
                    reader: self.reader.clone(),
                    session_epoch: self.session_epoch,
                    limit,
                },
            )
            .await
    }

    pub async fn acknowledge(
        &self,
        request_id: Uuid,
        cursor: impl Into<String>,
    ) -> Result<ReaderSessionResponse, AdminClientError> {
        self.admin
            .post(
                "/v1/readers/ack",
                &ReaderAckRequest {
                    request_id,
                    reader: self.reader.clone(),
                    session_epoch: self.session_epoch,
                    cursor: cursor.into(),
                },
            )
            .await
    }

    pub async fn close(&self, request_id: Uuid) -> Result<ReaderSessionResponse, AdminClientError> {
        self.admin
            .post(
                "/v1/readers/close",
                &ReaderFetchRequest {
                    request_id,
                    reader: self.reader.clone(),
                    session_epoch: self.session_epoch,
                    limit: None,
                },
            )
            .await
    }
}

#[derive(Clone, Debug)]
pub struct WriterRecord {
    pub request_id: Uuid,
    pub event_time_ns: Option<i64>,
    pub key: Vec<u8>,
    pub payload: Vec<u8>,
    pub metadata: std::collections::BTreeMap<String, Vec<u8>>,
}

#[derive(Clone)]
pub struct WriterSessionClient {
    admin: AdminClient,
    writer: String,
    session_epoch: u64,
    use_compression: bool,
}

impl WriterSessionClient {
    pub fn use_compression(mut self, enabled: bool) -> Self {
        self.use_compression = enabled;
        self
    }

    pub async fn append(
        &self,
        request_id: Uuid,
        event_time_ns: Option<i64>,
        key: &[u8],
        payload: &[u8],
        metadata: &std::collections::BTreeMap<String, Vec<u8>>,
    ) -> Result<WriterAppendResponse, AdminClientError> {
        self.admin
            .append_writer(&WriterSessionAppendRequest {
                request_id,
                writer: self.writer.clone(),
                session_epoch: self.session_epoch,
                use_compression: self.use_compression,
                event_time_ns: event_time_ns.map(|value| value.to_string()),
                key_base64: base64::Engine::encode(&base64::engine::general_purpose::STANDARD, key),
                payload_base64: base64::Engine::encode(
                    &base64::engine::general_purpose::STANDARD,
                    payload,
                ),
                metadata_base64: metadata
                    .iter()
                    .map(|(name, value)| {
                        (
                            name.clone(),
                            base64::Engine::encode(
                                &base64::engine::general_purpose::STANDARD,
                                value,
                            ),
                        )
                    })
                    .collect(),
            })
            .await
    }

    pub async fn append_batch(
        &self,
        records: Vec<WriterRecord>,
    ) -> Result<WriterBatchAppendResponse, AdminClientError> {
        let requests = records
            .into_iter()
            .map(|record| WriterSessionAppendRequest {
                request_id: record.request_id,
                writer: self.writer.clone(),
                session_epoch: self.session_epoch,
                use_compression: self.use_compression,
                event_time_ns: record.event_time_ns.map(|value| value.to_string()),
                key_base64: base64::Engine::encode(
                    &base64::engine::general_purpose::STANDARD,
                    record.key,
                ),
                payload_base64: base64::Engine::encode(
                    &base64::engine::general_purpose::STANDARD,
                    record.payload,
                ),
                metadata_base64: record
                    .metadata
                    .into_iter()
                    .map(|(name, value)| {
                        (
                            name,
                            base64::Engine::encode(
                                &base64::engine::general_purpose::STANDARD,
                                value,
                            ),
                        )
                    })
                    .collect(),
            })
            .collect();
        self.admin.append_writer_batch(requests).await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::control::PermissionAction;

    #[test]
    fn writer_compression_is_opt_in_and_configured_views_are_independent() {
        let writer = AdminClient::new("http://localhost:7070", "test-key")
            .writer_session("orders.writer", 7);
        assert!(!writer.use_compression);
        let compressed = writer.clone().use_compression(true);
        assert!(compressed.use_compression);
        assert!(!writer.use_compression);
        let plain = compressed.clone().use_compression(false);
        assert!(!plain.use_compression);
        assert!(compressed.use_compression);
        assert_eq!(plain.writer, writer.writer);
        assert_eq!(plain.session_epoch, writer.session_epoch);
    }

    #[test]
    fn issued_identities_extend_and_fence_admin_authorization() {
        use crate::control::IssuedApiKey;

        let bootstrap = "this-is-a-long-development-api-key";
        let auth = AdminAuthenticator::new(Some(bootstrap.to_owned())).unwrap();
        let issued_secret = "issued-release-key-0123456789abcdef";
        let digest = *blake3::hash(issued_secret.as_bytes()).as_bytes();
        let header = format!("Bearer {issued_secret}");
        let mut issued = IssuedApiKeySet::new();
        issued.insert(
            digest,
            IssuedApiKey {
                name: "ops.release".to_owned(),
                riverbed_admin: true,
                grants: vec![],
                expires_at_ns: None,
            },
        );
        // An issued riverbed-admin identity authorizes.
        assert!(auth.authorize_issued(Some(&header), &issued, 1_000).is_ok());
        // A non-admin Role binding does not.
        issued.get_mut(&digest).unwrap().riverbed_admin = false;
        assert_eq!(
            auth.authorize_issued(Some(&header), &issued, 1_000),
            Err(AdminAuthError::Invalid)
        );
        // An expired identity refuses even with an admin Role.
        issued.get_mut(&digest).unwrap().riverbed_admin = true;
        issued.get_mut(&digest).unwrap().expires_at_ns = Some(500);
        assert_eq!(
            auth.authorize_issued(Some(&header), &issued, 1_000),
            Err(AdminAuthError::Invalid)
        );
        // ...but a not-yet-expired one authorizes.
        assert!(auth.authorize_issued(Some(&header), &issued, 400).is_ok());
        issued.get_mut(&digest).unwrap().expires_at_ns = None;
        // The configured bootstrap key still authorizes first.
        assert!(auth
            .authorize_issued(Some(&format!("Bearer {bootstrap}")), &issued, 1_000)
            .is_ok());
        // Issued identities work even when no env key is configured.
        let catalog_only = AdminAuthenticator::new(None).unwrap();
        assert!(catalog_only
            .authorize_issued(Some(&header), &issued, 1_000)
            .is_ok());
        // With neither an env key nor issued identities, the surface
        // stays NotConfigured rather than silently open.
        assert_eq!(
            catalog_only.authorize_issued(Some(&header), &IssuedApiKeySet::new(), 1_000),
            Err(AdminAuthError::NotConfigured)
        );
        // An unknown key refuses.
        assert_eq!(
            auth.authorize_issued(Some("Bearer unknown-key-0123456789012345"), &issued, 1_000),
            Err(AdminAuthError::Invalid)
        );
    }

    #[test]
    fn authenticate_accepts_any_unexpired_identity_before_scope_checks() {
        use crate::control::IssuedApiKey;

        let bootstrap = "this-is-a-long-development-api-key";
        let auth = AdminAuthenticator::new(Some(bootstrap.to_owned())).unwrap();
        let scoped_secret = "narrow-scope-key-0123456789abcdef0";
        let digest = *blake3::hash(scoped_secret.as_bytes()).as_bytes();
        let mut issued = IssuedApiKeySet::new();
        issued.insert(
            digest,
            IssuedApiKey {
                name: "narrow".to_owned(),
                riverbed_admin: false,
                grants: vec![],
                expires_at_ns: Some(5_000),
            },
        );
        // Bootstrap, scoped, and grantless-but-unexpired identities all
        // authenticate; grant checks are a separate concern.
        for secret in [bootstrap, scoped_secret] {
            let header = format!("Bearer {secret}");
            assert!(auth.authenticate(Some(&header), &issued, 1_000).is_ok());
        }
        // Missing, unknown, and expired credentials refuse.
        assert_eq!(
            auth.authenticate(None, &issued, 1_000),
            Err(AdminAuthError::Missing)
        );
        let unknown = "Bearer unknown-key-0123456789abcdef";
        assert_eq!(
            auth.authenticate(Some(unknown), &issued, 1_000),
            Err(AdminAuthError::Invalid)
        );
        let header = format!("Bearer {scoped_secret}");
        assert_eq!(
            auth.authenticate(Some(&header), &issued, 9_000),
            Err(AdminAuthError::Invalid)
        );
        // With no configured key and no issued identities the
        // authenticator reports NotConfigured rather than a bad request.
        let bare = AdminAuthenticator::new(None).unwrap();
        assert_eq!(
            bare.authenticate(Some(&header), &IssuedApiKeySet::new(), 1_000),
            Err(AdminAuthError::NotConfigured)
        );
    }

    #[test]
    fn scoped_authorization_enforces_namespace_grants() {
        use crate::control::{IssuedApiKey, IssuedGrant};

        let bootstrap = "this-is-a-long-development-api-key";
        let auth = AdminAuthenticator::new(Some(bootstrap.to_owned())).unwrap();
        let secret = "reader-scoped-key-0123456789abcdef0";
        let digest = *blake3::hash(secret.as_bytes()).as_bytes();
        let header = format!("Bearer {secret}");
        let mut issued = IssuedApiKeySet::new();
        issued.insert(
            digest,
            IssuedApiKey {
                name: "ops.reader".to_owned(),
                riverbed_admin: false,
                grants: vec![IssuedGrant {
                    namespace: "orders.*".to_owned(),
                    actions: [PermissionAction::Read].into_iter().collect(),
                    effect: GrantEffect::Allow,
                }],
                expires_at_ns: None,
            },
        );
        // A read grant on orders.* authorizes reads inside the Domain…
        assert!(auth
            .authorize_scoped(
                Some(&header),
                &issued,
                1_000,
                "orders.events",
                PermissionAction::Read,
            )
            .is_ok());
        // …but not writes…
        assert_eq!(
            auth.authorize_scoped(
                Some(&header),
                &issued,
                1_000,
                "orders.events",
                PermissionAction::Write,
            ),
            Err(AdminAuthError::Invalid)
        );
        // …and not a different namespace.
        assert_eq!(
            auth.authorize_scoped(
                Some(&header),
                &issued,
                1_000,
                "billing.invoices",
                PermissionAction::Read,
            ),
            Err(AdminAuthError::Invalid)
        );
        // manage on the same namespace implies read and write.
        issued.get_mut(&digest).unwrap().grants = vec![IssuedGrant {
            namespace: "orders.*".to_owned(),
            actions: [PermissionAction::Manage].into_iter().collect(),
            effect: GrantEffect::Allow,
        }];
        assert!(auth
            .authorize_scoped(
                Some(&header),
                &issued,
                1_000,
                "orders.events",
                PermissionAction::Write,
            )
            .is_ok());
        // An exact-Feed grant does not reach other Feeds in the Domain.
        issued.get_mut(&digest).unwrap().grants = vec![IssuedGrant {
            namespace: "orders.events".to_owned(),
            actions: [PermissionAction::Read].into_iter().collect(),
            effect: GrantEffect::Allow,
        }];
        assert!(auth
            .authorize_scoped(
                Some(&header),
                &issued,
                1_000,
                "orders.events",
                PermissionAction::Read,
            )
            .is_ok());
        assert_eq!(
            auth.authorize_scoped(
                Some(&header),
                &issued,
                1_000,
                "orders.other",
                PermissionAction::Read,
            ),
            Err(AdminAuthError::Invalid)
        );
        // The bootstrap key passes every scope check.
        assert!(auth
            .authorize_scoped(
                Some(&format!("Bearer {bootstrap}")),
                &issued,
                1_000,
                "billing.invoices",
                PermissionAction::Write,
            )
            .is_ok());
        // An expired scoped identity refuses.
        issued.get_mut(&digest).unwrap().expires_at_ns = Some(500);
        assert_eq!(
            auth.authorize_scoped(
                Some(&header),
                &issued,
                1_000,
                "orders.events",
                PermissionAction::Read,
            ),
            Err(AdminAuthError::Invalid)
        );
    }

    #[test]
    fn domain_scoped_authorization_requires_spanning_grants() {
        use crate::control::{IssuedApiKey, IssuedGrant};

        let bootstrap = "this-is-a-long-development-api-key";
        let auth = AdminAuthenticator::new(Some(bootstrap.to_owned())).unwrap();
        let secret = "subscription-scoped-key-01234567abcd";
        let digest = *blake3::hash(secret.as_bytes()).as_bytes();
        let header = format!("Bearer {secret}");
        let mut issued = IssuedApiKeySet::new();
        issued.insert(
            digest,
            IssuedApiKey {
                name: "ops.subscription".to_owned(),
                riverbed_admin: false,
                grants: vec![IssuedGrant {
                    namespace: "orders.*".to_owned(),
                    actions: [PermissionAction::Read].into_iter().collect(),
                    effect: GrantEffect::Allow,
                }],
                expires_at_ns: None,
            },
        );
        // orders.* spans the Domain for member reads.
        assert!(auth
            .authorize_domain_scoped(
                Some(&header),
                &issued,
                1_000,
                "orders",
                PermissionAction::Read,
            )
            .is_ok());
        // ...but does not imply writes on the Domain...
        assert_eq!(
            auth.authorize_domain_scoped(
                Some(&header),
                &issued,
                1_000,
                "orders",
                PermissionAction::Write,
            ),
            Err(AdminAuthError::Invalid)
        );
        // ...and does not reach a different Domain.
        assert_eq!(
            auth.authorize_domain_scoped(
                Some(&header),
                &issued,
                1_000,
                "billing",
                PermissionAction::Read,
            ),
            Err(AdminAuthError::Invalid)
        );
        // An exact-Feed grant never covers its Domain's member Feeds.
        issued.get_mut(&digest).unwrap().grants = vec![IssuedGrant {
            namespace: "orders.events".to_owned(),
            actions: [PermissionAction::Read].into_iter().collect(),
            effect: GrantEffect::Allow,
        }];
        assert_eq!(
            auth.authorize_domain_scoped(
                Some(&header),
                &issued,
                1_000,
                "orders",
                PermissionAction::Read,
            ),
            Err(AdminAuthError::Invalid)
        );
        // manage on a spanning namespace covers member reads too.
        issued.get_mut(&digest).unwrap().grants = vec![IssuedGrant {
            namespace: "orders.*".to_owned(),
            actions: [PermissionAction::Manage].into_iter().collect(),
            effect: GrantEffect::Allow,
        }];
        assert!(auth
            .authorize_domain_scoped(
                Some(&header),
                &issued,
                1_000,
                "orders",
                PermissionAction::Read,
            )
            .is_ok());
        // The bootstrap key and issued riverbed admins pass Domain scopes.
        assert!(auth
            .authorize_domain_scoped(
                Some(&format!("Bearer {bootstrap}")),
                &issued,
                1_000,
                "billing",
                PermissionAction::Write,
            )
            .is_ok());
        issued.get_mut(&digest).unwrap().riverbed_admin = true;
        issued.get_mut(&digest).unwrap().grants = vec![];
        assert!(auth
            .authorize_domain_scoped(
                Some(&header),
                &issued,
                1_000,
                "billing",
                PermissionAction::Write,
            )
            .is_ok());
        // An expired scoped identity refuses.
        issued.get_mut(&digest).unwrap().riverbed_admin = false;
        issued.get_mut(&digest).unwrap().grants = vec![IssuedGrant {
            namespace: "orders.*".to_owned(),
            actions: [PermissionAction::Read].into_iter().collect(),
            effect: GrantEffect::Allow,
        }];
        issued.get_mut(&digest).unwrap().expires_at_ns = Some(500);
        assert_eq!(
            auth.authorize_domain_scoped(
                Some(&header),
                &issued,
                1_000,
                "orders",
                PermissionAction::Read,
            ),
            Err(AdminAuthError::Invalid)
        );
    }

    #[test]
    fn deny_effect_fences_covering_allows() {
        use crate::control::IssuedGrant;

        let auth =
            AdminAuthenticator::new(Some("this-is-a-long-development-api-key".to_owned())).unwrap();
        let mut issued = IssuedApiKeySet::new();
        let secret = "orders-reader-key-0123456789abcdef";
        let digest = *blake3::hash(secret.as_bytes()).as_bytes();
        let header = format!("Bearer {secret}");
        let grants = |grants: Vec<IssuedGrant>| IssuedApiKey {
            name: "ops.reader".to_owned(),
            riverbed_admin: false,
            grants,
            expires_at_ns: None,
        };
        let grant = |namespace: &str, action: PermissionAction, effect: GrantEffect| IssuedGrant {
            namespace: namespace.to_owned(),
            actions: [action].into_iter().collect(),
            effect,
        };
        // A wildcard allow covers the Domain, but an exact deny on one
        // resource still wins.
        issued.insert(
            digest,
            grants(vec![
                grant("orders.*", PermissionAction::Read, GrantEffect::Allow),
                grant("orders.events", PermissionAction::Read, GrantEffect::Deny),
            ]),
        );
        assert_eq!(
            auth.authorize_scoped(
                Some(&header),
                &issued,
                1_000,
                "orders.events",
                PermissionAction::Read,
            ),
            Err(AdminAuthError::Invalid)
        );
        assert!(auth
            .authorize_scoped(
                Some(&header),
                &issued,
                1_000,
                "orders.other",
                PermissionAction::Read,
            )
            .is_ok());
        // A narrower deny does not fence resources outside its pattern.
        issued.get_mut(&digest).unwrap().grants = vec![
            grant("orders.*", PermissionAction::Read, GrantEffect::Allow),
            grant("orders.eu.*", PermissionAction::Read, GrantEffect::Deny),
        ];
        assert!(auth
            .authorize_scoped(
                Some(&header),
                &issued,
                1_000,
                "orders.events",
                PermissionAction::Read,
            )
            .is_ok());
        assert_eq!(
            auth.authorize_scoped(
                Some(&header),
                &issued,
                1_000,
                "orders.eu.events",
                PermissionAction::Read,
            ),
            Err(AdminAuthError::Invalid)
        );
        // A manage deny fences every implied action on the namespace.
        issued.get_mut(&digest).unwrap().grants = vec![
            grant("orders.*", PermissionAction::Manage, GrantEffect::Allow),
            grant("orders.events", PermissionAction::Manage, GrantEffect::Deny),
        ];
        for action in [
            PermissionAction::Read,
            PermissionAction::Write,
            PermissionAction::Manage,
        ] {
            assert_eq!(
                auth.authorize_scoped(Some(&header), &issued, 1_000, "orders.events", action,),
                Err(AdminAuthError::Invalid),
                "{action:?} must be fenced by the manage deny"
            );
        }
        // Deny rows fence Riverbed-admin identities on scoped checks too;
        // only the bootstrap key is break-glass.
        issued.insert(
            digest,
            IssuedApiKey {
                name: "ops.admin".to_owned(),
                riverbed_admin: true,
                grants: vec![grant(
                    "orders.*",
                    PermissionAction::Manage,
                    GrantEffect::Deny,
                )],
                expires_at_ns: None,
            },
        );
        assert_eq!(
            auth.authorize_scoped(
                Some(&header),
                &issued,
                1_000,
                "orders.events",
                PermissionAction::Read,
            ),
            Err(AdminAuthError::Invalid)
        );
        assert!(auth
            .authorize_scoped(
                Some("Bearer this-is-a-long-development-api-key"),
                &issued,
                1_000,
                "orders.events",
                PermissionAction::Read,
            )
            .is_ok());
        // Domain-spanning checks honor the same precedence: a covering
        // deny beats the spanning allow.
        issued.get_mut(&digest).unwrap().riverbed_admin = false;
        issued.get_mut(&digest).unwrap().grants = vec![
            grant("orders.*", PermissionAction::Read, GrantEffect::Allow),
            grant("orders.*", PermissionAction::Read, GrantEffect::Deny),
        ];
        assert_eq!(
            auth.authorize_domain_scoped(
                Some(&header),
                &issued,
                1_000,
                "orders",
                PermissionAction::Read,
            ),
            Err(AdminAuthError::Invalid)
        );
        // A `DENY manage ON *` also fences Riverbed-admin identities on
        // the unscoped admin surface; a narrower manage deny does not.
        issued.get_mut(&digest).unwrap().riverbed_admin = true;
        issued.get_mut(&digest).unwrap().grants =
            vec![grant("*", PermissionAction::Manage, GrantEffect::Deny)];
        assert_eq!(
            auth.authorize_issued(Some(&header), &issued, 1_000),
            Err(AdminAuthError::Invalid)
        );
        issued.get_mut(&digest).unwrap().grants = vec![grant(
            "orders.*",
            PermissionAction::Manage,
            GrantEffect::Deny,
        )];
        assert!(auth.authorize_issued(Some(&header), &issued, 1_000).is_ok());
        assert!(auth
            .authorize_issued(
                Some("Bearer this-is-a-long-development-api-key"),
                &issued,
                1_000
            )
            .is_ok());
    }

    #[tokio::test]
    #[ignore = "requires a disposable three-Node Riverbed and WHITEWATER_ENDPOINTS"]
    async fn optional_compression_conforms_against_live_nodes() {
        let endpoints = std::env::var("WHITEWATER_ENDPOINTS").unwrap();
        let endpoints: Vec<_> = endpoints.split(',').collect();
        assert_eq!(endpoints.len(), 3);
        let key = std::env::var("WHITEWATER_API_KEY")
            .unwrap_or_else(|_| "whitewater-local-development-admin-key".to_owned());
        let corpus: serde_json::Value = serde_json::from_str(include_str!(
            "../sdk/spec/conformance/optional-compression.json"
        ))
        .unwrap();
        let suffix = Uuid::new_v4().simple().to_string();
        let script = corpus["steps"][0]["args"]["script"]
            .as_str()
            .unwrap()
            .replace("{{uniq}}", &suffix);
        let feed = format!("sdk{suffix}.events");
        let writer = format!("wr{suffix}");
        let payload = corpus["steps"][2]["args"]["payload"]["utf8"]
            .as_str()
            .unwrap()
            .as_bytes();
        let clients: Vec<_> = endpoints
            .iter()
            .map(|endpoint| AdminClient::new(*endpoint, key.clone()))
            .collect();
        let setup = clients[0].execute_wcl(script).await.unwrap();
        let epoch = setup.results[3].data["session_epoch"].as_u64().unwrap();
        for (index, enabled) in [false, true, false, true].into_iter().enumerate() {
            clients[index % 3]
                .writer_session(&writer, epoch)
                .use_compression(enabled)
                .append(
                    Uuid::new_v4(),
                    Some(index as i64 + 1),
                    b"meter",
                    payload,
                    &Default::default(),
                )
                .await
                .unwrap();
        }
        let expected = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, payload);
        for client in clients {
            let records: serde_json::Value = client
                .http
                .get(format!("{}/v1/feeds/records", client.endpoint))
                .bearer_auth(&client.api_key)
                .query(&[("feed", &feed)])
                .send()
                .await
                .unwrap()
                .error_for_status()
                .unwrap()
                .json()
                .await
                .unwrap();
            let records = records.as_array().unwrap();
            assert_eq!(records.len(), 4);
            assert!(records
                .iter()
                .all(|record| record["payload_base64"] == expected));
        }
    }

    #[test]
    fn bearer_authentication_requires_the_configured_key() {
        let authenticator =
            AdminAuthenticator::new(Some("this-is-a-long-development-api-key".to_owned())).unwrap();
        assert_eq!(authenticator.authorize(None), Err(AdminAuthError::Missing));
        assert_eq!(
            authenticator.authorize(Some("Bearer wrong-key")),
            Err(AdminAuthError::Invalid)
        );
        assert_eq!(
            authenticator.authorize(Some("Basic abc")),
            Err(AdminAuthError::Missing)
        );
        assert_eq!(
            authenticator.authorize(Some("Bearer this-is-a-long-development-api-key")),
            Ok(())
        );
    }

    #[test]
    fn missing_and_weak_server_keys_are_not_silently_accepted() {
        assert_eq!(
            AdminAuthenticator::new(None)
                .unwrap()
                .authorize(Some("Bearer anything")),
            Err(AdminAuthError::NotConfigured)
        );
        assert!(matches!(
            AdminAuthenticator::new(Some("short".to_owned())),
            Err(AdminAuthError::WeakKey)
        ));
    }

    #[test]
    fn typed_commands_have_a_stable_json_shape() {
        let request = CommandBatchRequest {
            request_id: Some(Uuid::new_v4()),
            commands: vec![Command::Grant {
                actions: BTreeSet::from([PermissionAction::Read, PermissionAction::Write]),
                namespace: "orders.*".to_owned(),
                role: "orderapp".to_owned(),
                effect: GrantEffect::Allow,
            }],
        };
        let json = serde_json::to_value(&request).unwrap();
        assert_eq!(json["commands"][0]["command"], "grant");
        assert_eq!(json["commands"][0]["namespace"], "orders.*");
        assert_eq!(
            serde_json::from_value::<CommandBatchRequest>(json).unwrap(),
            request
        );
    }
}
