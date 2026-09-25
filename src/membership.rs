use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use tokio::{
    net::lookup_host,
    sync::{watch, RwLock},
    time,
};
use tracing::{debug, info, warn};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemberAnnouncement {
    pub node_id: String,
    pub api_url: String,
    pub capacity: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemberView {
    pub node_id: String,
    pub api_url: String,
    pub capacity: u32,
    pub last_seen_ms: u64,
    pub local: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JoinResponse {
    pub member: MemberAnnouncement,
    pub members: Vec<MemberView>,
}

#[derive(Clone)]
pub struct MembershipService {
    local: MemberAnnouncement,
    members: Arc<RwLock<HashMap<String, MemberState>>>,
    known_urls: Arc<RwLock<HashSet<String>>>,
    seeds: Arc<Vec<String>>,
    client: reqwest::Client,
    heartbeat_interval: Duration,
    member_timeout: Duration,
}

#[derive(Clone)]
struct MemberState {
    announcement: MemberAnnouncement,
    last_seen: Instant,
    last_seen_ms: u64,
}

impl MembershipService {
    pub fn new(
        local: MemberAnnouncement,
        seeds: Vec<String>,
        heartbeat_interval: Duration,
        member_timeout: Duration,
    ) -> Self {
        let now_ms = unix_ms();
        let local_state = MemberState {
            announcement: local.clone(),
            last_seen: Instant::now(),
            last_seen_ms: now_ms,
        };
        Self {
            local: local.clone(),
            members: Arc::new(RwLock::new(HashMap::from([(
                local.node_id.clone(),
                local_state,
            )]))),
            known_urls: Arc::new(RwLock::new(HashSet::new())),
            seeds: Arc::new(seeds),
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(2))
                .build()
                .expect("membership HTTP client"),
            heartbeat_interval,
            member_timeout,
        }
    }

    pub fn local(&self) -> &MemberAnnouncement {
        &self.local
    }

    pub async fn observe(&self, announcement: MemberAnnouncement) {
        if announcement.node_id.is_empty() || announcement.api_url.is_empty() {
            return;
        }
        self.known_urls
            .write()
            .await
            .insert(announcement.api_url.clone());
        self.members.write().await.insert(
            announcement.node_id.clone(),
            MemberState {
                announcement,
                last_seen: Instant::now(),
                last_seen_ms: unix_ms(),
            },
        );
    }

    pub async fn remove(&self, node_id: &str) {
        if node_id != self.local.node_id {
            self.members.write().await.remove(node_id);
        }
    }

    pub async fn members(&self) -> Vec<MemberView> {
        let members = self.members.read().await;
        let mut result: Vec<_> = members
            .values()
            .map(|state| MemberView {
                node_id: state.announcement.node_id.clone(),
                api_url: state.announcement.api_url.clone(),
                capacity: state.announcement.capacity,
                last_seen_ms: state.last_seen_ms,
                local: state.announcement.node_id == self.local.node_id,
            })
            .collect();
        result.sort_by(|left, right| left.node_id.cmp(&right.node_id));
        result
    }

    pub async fn run(self: Arc<Self>, mut shutdown: watch::Receiver<bool>) {
        self.heartbeat_once().await;
        let mut ticker = time::interval(self.heartbeat_interval);
        loop {
            tokio::select! {
                _ = ticker.tick() => self.heartbeat_once().await,
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        self.graceful_leave().await;
                        return;
                    }
                }
            }
        }
    }

    async fn heartbeat_once(&self) {
        self.refresh_local().await;
        let targets = self.discovery_targets().await;
        for target in targets {
            if target.trim_end_matches('/') == self.local.api_url.trim_end_matches('/') {
                continue;
            }
            let endpoint = format!("{}/v1/cluster/join", target.trim_end_matches('/'));
            match self.client.post(endpoint).json(&self.local).send().await {
                Ok(response) if response.status().is_success() => {
                    match response.json::<JoinResponse>().await {
                        Ok(joined) => {
                            self.observe(joined.member).await;
                            let mut known = self.known_urls.write().await;
                            for member in joined.members {
                                if member.node_id != self.local.node_id {
                                    known.insert(member.api_url);
                                }
                            }
                        }
                        Err(error) => debug!(%target, %error, "membership response was invalid"),
                    }
                }
                Ok(response) => {
                    debug!(%target, status = %response.status(), "membership heartbeat was rejected")
                }
                Err(error) => debug!(%target, %error, "membership target is unavailable"),
            }
        }
        self.expire_members().await;
    }

    async fn refresh_local(&self) {
        let mut members = self.members.write().await;
        members.insert(
            self.local.node_id.clone(),
            MemberState {
                announcement: self.local.clone(),
                last_seen: Instant::now(),
                last_seen_ms: unix_ms(),
            },
        );
    }

    async fn discovery_targets(&self) -> BTreeSet<String> {
        let mut targets = BTreeSet::new();
        for seed in self.seeds.iter() {
            let authority = seed
                .trim()
                .trim_start_matches("http://")
                .trim_start_matches("https://")
                .trim_end_matches('/');
            match lookup_host(authority).await {
                Ok(addresses) => {
                    for address in addresses {
                        targets.insert(format!("http://{address}"));
                    }
                }
                Err(error) => debug!(%seed, %error, "membership seed did not resolve"),
            }
        }
        targets.extend(self.known_urls.read().await.iter().cloned());
        targets
    }

    async fn expire_members(&self) {
        let mut members = self.members.write().await;
        let before = members.len();
        members.retain(|node_id, state| {
            node_id == &self.local.node_id || state.last_seen.elapsed() <= self.member_timeout
        });
        if members.len() != before {
            info!(
                members = members.len(),
                "expired unavailable cluster members"
            );
        }
    }

    pub async fn graceful_leave(&self) {
        let peers: Vec<_> = self
            .members
            .read()
            .await
            .values()
            .filter(|state| state.announcement.node_id != self.local.node_id)
            .map(|state| state.announcement.api_url.clone())
            .collect();
        for peer in peers {
            let endpoint = format!("{}/v1/cluster/leave", peer.trim_end_matches('/'));
            if let Err(error) = self.client.post(endpoint).json(&self.local).send().await {
                warn!(%peer, %error, "failed to announce graceful leave");
            }
        }
    }
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn membership_observes_and_removes_nodes_without_removing_self() {
        let local = MemberAnnouncement {
            node_id: "one".to_owned(),
            api_url: "http://one:7070".to_owned(),
            capacity: 100,
        };
        let service = MembershipService::new(
            local,
            vec![],
            Duration::from_secs(1),
            Duration::from_secs(10),
        );
        service
            .observe(MemberAnnouncement {
                node_id: "two".to_owned(),
                api_url: "http://two:7070".to_owned(),
                capacity: 50,
            })
            .await;
        assert_eq!(service.members().await.len(), 2);
        service.remove("two").await;
        service.remove("one").await;
        let members = service.members().await;
        assert_eq!(members.len(), 1);
        assert!(members[0].local);
    }
}
