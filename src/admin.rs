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
    control::{Command, ControlExecution, PermissionAction, ReaderStart, ResourceKind, ShowKind},
    reader::ReaderRetryPolicy,
};

pub const ADMIN_API_KEY_ENV: &str = "FINNSTREAM_ADMIN_API_KEY";
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
        let expected = self.expected_digest.ok_or(AdminAuthError::NotConfigured)?;
        let supplied = authorization
            .and_then(|value| value.strip_prefix("Bearer "))
            .filter(|value| !value.is_empty())
            .ok_or(AdminAuthError::Missing)?;
        let actual = *blake3::hash(supplied.as_bytes()).as_bytes();
        if constant_time_equal(&expected, &actual) {
            Ok(())
        } else {
            Err(AdminAuthError::Invalid)
        }
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
}

impl WriterSessionClient {
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
