use std::{collections::BTreeSet, env};

use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    active_range::StorageNodeId,
    control::{Command, ControlExecution, PermissionAction, ReaderStart, ResourceKind, ShowKind},
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

    async fn execute_one(&self, command: Command) -> Result<ControlExecution, AdminClientError> {
        self.execute_commands(vec![command]).await
    }

    async fn post<T: Serialize + ?Sized>(
        &self,
        path: &str,
        request: &T,
    ) -> Result<ControlExecution, AdminClientError> {
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
