//! Correlated, versioned operational diagnostics — the Operations
//! Advisor foundation (see `docs/operations-advisor.md`). This module
//! is a pure rule evaluator: the Admin API assembles scoped evidence
//! from the catalog, membership, and demand trackers, and the rules
//! here explain *why* a stage is unhealthy, what Whitewater is already
//! doing about it, and the next safe step a person can take. It never
//! mutates state, never calls the network, and stays honest when
//! evidence is missing (`Limited`) rather than fabricating certainty.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Wire/schema version of [`DiagnosticsReport`]; bump on any
/// incompatible shape change so Advisors and MCP adapters can refuse
/// reports they cannot interpret.
pub const DIAGNOSTICS_SCHEMA_VERSION: u32 = 1;

/// Findings are bounded so a degraded Riverbed can never turn a
/// diagnostic read into an unbounded response.
const MAX_FINDINGS_PER_STAGE: usize = 64;

/// A Distributary status report older than this is treated as stale:
/// the lease owner publishes on transitions and heartbeat report ticks
/// far more often, so silence means the reporter stopped — evidence,
/// not proof, since a dead owner does not announce itself.
const DISTRIBUTARY_REPORT_STALE_MS: u64 = 60_000;

/// How confident a finding is in its own evidence. Membership liveness
/// is discovery-layer evidence (gossip staleness is not death), so it
/// never reports higher than `Medium`.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Confidence {
    High,
    Medium,
    Low,
}

/// Worst-state rollup for one pipeline stage. `Limited` means evidence
/// could not be gathered — distinct from `Healthy` so callers never
/// read silence as wellness.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum StageStatus {
    Healthy,
    Advisory,
    Degraded,
    Unhealthy,
    Limited,
}

/// One explained problem: what is happening, what it endangers, what
/// the system is already doing, and the next safe step. `cause`,
/// `risk`, `automatic_action`, and `next_safe_step` are the required
/// fields the design doc's diagnostic schema calls out.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct DiagnosticFinding {
    /// Scoped resource name (Feed, Reader, Node, Distributary, ...).
    pub resource: String,
    pub severity: StageStatus,
    pub cause: String,
    pub risk: String,
    pub automatic_action: String,
    pub next_safe_step: String,
    pub confidence: Confidence,
    /// Freshness of the underlying evidence when the source carries a
    /// timestamp (status reports, heartbeats); `None` for state that is
    /// authoritative as of the report itself.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence_at_ns: Option<i64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct DiagnosticStage {
    /// Pipeline stage name: `control_plane`, `nodes`, `storage`,
    /// `writers`, `readers`, `subscriptions`, `pipes`, `distributaries`,
    /// `indexes`.
    pub stage: String,
    pub status: StageStatus,
    #[serde(default)]
    pub findings: Vec<DiagnosticFinding>,
    /// Findings dropped by [`MAX_FINDINGS_PER_STAGE`]; nonzero means the
    /// caller should narrow scope rather than assume this is complete.
    #[serde(default)]
    pub truncated: u32,
}

/// The correlated health explanation: per-stage status plus evidence-
/// linked findings across the Writer→quorum→storage→Index→Subscription
/// →Reader pipeline. Read-only and safe to call on any Node.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct DiagnosticsReport {
    pub schema_version: u32,
    /// Reporting Node — the "any Node" access contract; the report
    /// describes cluster state, not just this process.
    pub node: String,
    pub observed_at_ns: i64,
    /// `control_plane` or `local_prototype` — the authority this
    /// evidence was read under, so callers know its consistency level.
    pub authority: String,
    /// True when any stage is `Limited`.
    pub limited: bool,
    pub stages: Vec<DiagnosticStage>,
}

// --- Scoped evidence assembled by the Admin API ------------------------
// These plain views deliberately decouple the rules from catalog
// internals so tests can build fixtures without standing up state.

#[derive(Clone, Debug, Default)]
pub struct DiagnosticsInput {
    pub node: String,
    pub observed_at_ns: i64,
    /// `None` under `local_prototype` authority.
    pub control_plane: Option<ControlPlaneEvidence>,
    /// Membership liveness bound; a member whose heartbeat age exceeds
    /// it is stale.
    pub member_timeout_ms: u64,
    pub members: Vec<MemberEvidence>,
    pub storage_nodes: Vec<StorageEvidence>,
    pub readers: Vec<ReaderEvidence>,
    pub subscriptions: Vec<SubscriptionEvidence>,
    pub distributaries: Vec<DistributaryEvidence>,
    pub pipes: Vec<PipeEvidence>,
    pub indexes: Vec<IndexEvidence>,
    pub hot_keys: Vec<HotKeyEvidence>,
    /// Direct health-probe results for every storage owner referenced
    /// by a progress assignment or compute lease, keyed by storage
    /// identity — the same evidence the lease supervisor acts on.
    /// Member gossip cannot answer this: member `node_id` and
    /// `StorageNodeId` are distinct identity spaces.
    pub owner_liveness: BTreeMap<String, OwnerState>,
}

/// What a direct health probe proved about a storage owner.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OwnerState {
    /// The owner answered its health endpoint.
    Live,
    /// Transport to the owner's endpoint failed.
    Dead,
    /// No endpoint could be resolved to probe.
    Unverifiable,
}

#[derive(Clone, Debug)]
pub struct ControlPlaneEvidence {
    pub leader: Option<String>,
    pub term: u64,
    pub voters: usize,
}

#[derive(Clone, Debug)]
pub struct MemberEvidence {
    pub node: String,
    pub local: bool,
    /// Milliseconds since the last observed heartbeat.
    pub last_seen_age_ms: u64,
}

#[derive(Clone, Debug)]
pub struct StorageEvidence {
    pub node: String,
    pub eligible: bool,
    /// Placement exclusion reasons as stable snake_case labels.
    pub exclusions: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct ReaderEvidence {
    pub name: String,
    pub feed: String,
    pub session_active: bool,
    /// `retry_after_ms` the server attached to the last delivery —
    /// nonzero means the Reader was paced, not merely idle.
    pub last_retry_after_ms: u64,
}

#[derive(Clone, Debug)]
pub struct SubscriptionEvidence {
    pub name: String,
    /// Owner of the durable progress assignment, when one exists.
    pub progress_owner: Option<String>,
}

#[derive(Clone, Debug)]
pub struct DistributaryEvidence {
    pub name: String,
    pub run_state: String,
    pub last_error: Option<String>,
    pub lag_records: Option<u64>,
    /// Age of the latest status report; staleness implies a dead owner.
    pub report_age_ms: u64,
    /// When the latest report was published (Unix ns), for freshness.
    pub reported_at_ns: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct PipeEvidence {
    pub name: String,
    /// `declared`, `paused`, ... (serialized `PipeStage`).
    pub stage: String,
    pub compute_owner: Option<String>,
}

#[derive(Clone, Debug)]
pub struct IndexEvidence {
    pub name: String,
    pub feed: String,
    /// `declared`, `building`, `active` (serialized `IndexBuildStage`).
    pub stage: String,
}

#[derive(Clone, Debug)]
pub struct HotKeyEvidence {
    /// Owning Feed when the range could be attributed.
    pub feed: Option<String>,
    /// Range identity as an opaque placement token — the public surface
    /// never exposes partition structure.
    pub range: String,
    /// Stable token of the flagged Key — never the raw Key bytes.
    pub token: String,
    /// Share of the dominant window's records, in basis points.
    pub share_bps: u32,
}

// --- Rule evaluation ---------------------------------------------------

pub fn explain(input: &DiagnosticsInput) -> DiagnosticsReport {
    let mut stages = vec![
        control_plane_stage(input),
        nodes_stage(input),
        storage_stage(input),
        writers_stage(input),
        readers_stage(input),
        subscriptions_stage(input),
        pipes_stage(input),
        distributaries_stage(input),
        indexes_stage(input),
    ];
    for stage in &mut stages {
        stage.status = stage
            .findings
            .iter()
            .map(|finding| finding.severity)
            .max()
            .unwrap_or(StageStatus::Healthy)
            .max(stage.status);
    }
    let limited = stages
        .iter()
        .any(|stage| stage.status == StageStatus::Limited);
    DiagnosticsReport {
        schema_version: DIAGNOSTICS_SCHEMA_VERSION,
        node: input.node.clone(),
        observed_at_ns: input.observed_at_ns,
        authority: if input.control_plane.is_some() {
            "control_plane".to_string()
        } else {
            "local_prototype".to_string()
        },
        limited,
        stages,
    }
}

fn stage(name: &str, findings: Vec<DiagnosticFinding>) -> DiagnosticStage {
    let truncated = findings.len().saturating_sub(MAX_FINDINGS_PER_STAGE) as u32;
    DiagnosticStage {
        stage: name.to_string(),
        // Recomputed by `explain` from finding severities; `Limited` is
        // set directly when evidence is missing.
        status: StageStatus::Healthy,
        findings: findings.into_iter().take(MAX_FINDINGS_PER_STAGE).collect(),
        truncated,
    }
}

#[allow(clippy::too_many_arguments)] // positional mirror of DiagnosticFinding fields
fn finding(
    resource: impl Into<String>,
    severity: StageStatus,
    cause: impl Into<String>,
    risk: impl Into<String>,
    automatic_action: impl Into<String>,
    next_safe_step: impl Into<String>,
    confidence: Confidence,
    evidence_at_ns: Option<i64>,
) -> DiagnosticFinding {
    DiagnosticFinding {
        resource: resource.into(),
        severity,
        cause: cause.into(),
        risk: risk.into(),
        automatic_action: automatic_action.into(),
        next_safe_step: next_safe_step.into(),
        confidence,
        evidence_at_ns,
    }
}

fn control_plane_stage(input: &DiagnosticsInput) -> DiagnosticStage {
    match &input.control_plane {
        None => {
            let mut stage = stage("control_plane", vec![]);
            stage.status = StageStatus::Limited;
            stage.findings.push(finding(
                input.node.clone(),
                StageStatus::Advisory,
                "this Node runs under local_prototype authority — there is no replicated Control Plane",
                "catalog changes are not consensus-committed; the topology is a development fixture, not a supported Riverbed",
                "none — single-Node authority has nothing to elect",
                "run at least three Nodes with configured Control Plane voters for a supported deployment",
                Confidence::High,
                None,
            ));
            stage
        }
        Some(evidence) if evidence.leader.is_none() => stage(
            "control_plane",
            vec![finding(
                input.node.clone(),
                StageStatus::Unhealthy,
                format!(
                    "no Control Plane leader at term {} across {} configured voters",
                    evidence.term, evidence.voters
                ),
                "catalog writes, placement changes, and resource declarations stall until a majority elects a leader; in-flight appends and reads continue only while current owners remain reachable",
                "voters keep retrying election automatically",
                "check voter Node health and inter-Node reachability before restarting anything; do not decommission a voter while quorum is uncertain",
                Confidence::High,
                Some(input.observed_at_ns),
            )],
        ),
        Some(_) => stage("control_plane", vec![]),
    }
}

fn nodes_stage(input: &DiagnosticsInput) -> DiagnosticStage {
    let findings = input
        .members
        .iter()
        .filter(|member| !member.local && member.last_seen_age_ms > input.member_timeout_ms)
        .map(|member| {
            finding(
                member.node.clone(),
                StageStatus::Degraded,
                format!(
                    "no heartbeat observed for {}ms (timeout {}ms)",
                    member.last_seen_age_ms, input.member_timeout_ms
                ),
                "if the Node is dead, ranges it owns lose a replica until failover completes; RF3 tolerates one loss but not a second concurrent one",
                "range and progress ownership moves are fenced by the failover window and proceed automatically",
                "confirm the Node is actually down before replacing it; a partitioned-but-alive Node must not rejoin with stale authority",
                Confidence::Medium,
                Some(input.observed_at_ns),
            )
        })
        .collect();
    stage("nodes", findings)
}

fn storage_stage(input: &DiagnosticsInput) -> DiagnosticStage {
    let mut findings: Vec<DiagnosticFinding> = Vec::new();
    let eligible = input
        .storage_nodes
        .iter()
        .filter(|node| node.eligible)
        .count();
    for node in &input.storage_nodes {
        for exclusion in &node.exclusions {
            let (severity, cause, risk, automatic, next, confidence) = match exclusion.as_str() {
                "draining" => (
                    StageStatus::Advisory,
                    "Node is draining — an operator move or planned retirement is in progress".to_string(),
                    "premature removal would strand in-flight ownership moves".to_string(),
                    "the drain plan fences removal until every owned range reports safe-to-remove".to_string(),
                    "wait for the drain plan to report ready-to-retire; never scale in on a metadata-only owner transfer".to_string(),
                    Confidence::High,
                ),
                "retired" => (
                    StageStatus::Advisory,
                    "Node is retired and no longer accepts placement".to_string(),
                    "none — retirement is the intended end state of a completed drain".to_string(),
                    "placement skips the Node automatically".to_string(),
                    "no action needed; remove the identity from configuration when convenient".to_string(),
                    Confidence::High,
                ),
                "capacity_unreported" | "capacity_stale" => (
                    StageStatus::Degraded,
                    format!("Node's capacity evidence is {exclusion}"),
                    "placement policy excludes the Node while its self-measured headroom is unknown".to_string(),
                    "the Node's capacity reporter reposts automatically when it recovers".to_string(),
                    "check whether the Node is alive and its reporter is running; a persistently silent Node should be investigated like a dead one".to_string(),
                    Confidence::Medium,
                ),
                "below_baseline" => (
                    StageStatus::Degraded,
                    "Node's reported headroom is below the configured capacity floor".to_string(),
                    "the Node cannot take new ranges until resources free up".to_string(),
                    "placement excludes the Node automatically until headroom recovers".to_string(),
                    "free disk or expand the Node's resources; confirm the next capacity report clears the floor".to_string(),
                    Confidence::High,
                ),
                other => (
                    StageStatus::Degraded,
                    format!("Node is excluded from placement: {other}"),
                    "placement cannot use this Node, shrinking the candidate pool".to_string(),
                    "placement re-evaluates eligibility on every decision".to_string(),
                    "inspect the Node's declaration and capability flags for the exclusion's root cause".to_string(),
                    Confidence::Medium,
                ),
            };
            findings.push(finding(
                node.node.clone(),
                severity,
                cause,
                risk,
                automatic,
                next,
                confidence,
                Some(input.observed_at_ns),
            ));
        }
    }
    if !input.storage_nodes.is_empty() && eligible < 3 {
        findings.push(finding(
            "storage pool".to_string(),
            StageStatus::Unhealthy,
            format!("only {eligible} storage Node(s) are placement-eligible"),
            "RF3 needs three eligible replicas per range; below that, new Feeds cannot be placed and degraded ranges cannot self-heal",
            "new placements refuse rather than silently under-replicate",
            "restore excluded Nodes or add storage capacity before declaring more Feeds; do not lower replication to work around it",
            Confidence::High,
            Some(input.observed_at_ns),
        ));
    }
    stage("storage", findings)
}

fn writers_stage(input: &DiagnosticsInput) -> DiagnosticStage {
    let findings = input
        .hot_keys
        .iter()
        .map(|hot| {
            finding(
                hot.feed
                    .clone()
                    .unwrap_or_else(|| format!("feed owning range {}", hot.range)),
                StageStatus::Advisory,
                format!(
                    "one Key holds {:.1}% of the recent window's records",
                    hot.share_bps as f64 / 100.0
                ),
                "the owning range's append throughput is bounded by one ordered lane; an isolated Key is always ordered, never parallelized",
                "the range keeps serving but the skew is reported so operators can act on it",
                "consider a range split at the Key boundary if the skew is structural, or re-key the workload; splitting requires a committed boundary, never a metadata shortcut",
                Confidence::High,
                Some(input.observed_at_ns),
            )
        })
        .collect();
    stage("writers", findings)
}

fn readers_stage(input: &DiagnosticsInput) -> DiagnosticStage {
    let findings = input
        .readers
        .iter()
        .filter(|reader| reader.last_retry_after_ms > 0)
        .map(|reader| {
            finding(
                reader.name.clone(),
                StageStatus::Degraded,
                format!(
                    "server paced the Reader's last delivery by {}ms — credit or capacity exhaustion, not application latency",
                    reader.last_retry_after_ms
                ),
                "deliveries continue but the Reader's observed lag grows while paced",
                "the pacing policy retried automatically and applies back-pressure rather than failing the session",
                "check whether the source range is capacity-constrained or a hot Key is dominant before blaming the application",
                Confidence::High,
                Some(input.observed_at_ns),
            )
        })
        .collect();
    stage("readers", findings)
}

fn subscriptions_stage(input: &DiagnosticsInput) -> DiagnosticStage {
    let findings = input
        .subscriptions
        .iter()
        .filter_map(|subscription| match &subscription.progress_owner {
            None => Some(finding(
                subscription.name.clone(),
                StageStatus::Degraded,
                "Subscription has no durable progress assignment".to_string(),
                "member joins fail closed until placement is repaired; shared progress cannot be served from a missing owner".to_string(),
                "placement repair runs when member operations observe the gap".to_string(),
                "a member join or administrative reassignment restores placement; never serve progress from an unassigned Subscription".to_string(),
                Confidence::High,
                Some(input.observed_at_ns),
            )),
            Some(owner) => owner_finding(
                &input.owner_liveness,
                owner,
                &subscription.name,
                "member operations stall until ownership reassigns; a dead owner cannot serve prepare/commit",
                "ownership repair is fenced by epoch and proceeds automatically once the failover window passes",
                "confirm the owner is down; reassignment happens on the next member operation — do not force it while the owner may still be alive",
                input.observed_at_ns,
            ),
        })
        .collect();
    stage("subscriptions", findings)
}

fn pipes_stage(input: &DiagnosticsInput) -> DiagnosticStage {
    let mut findings = Vec::new();
    for pipe in &input.pipes {
        if pipe.stage == "paused" {
            findings.push(finding(
                pipe.name.clone(),
                StageStatus::Advisory,
                "Pipe is operator-paused",
                "deliveries stop advancing until resumed; in-flight effects finished before the pause committed",
                "every drive skips the Pipe while paused — the flag is durable, not a process state",
                "resume the Pipe when ready; input progress and output identity stay durable across the gap",
                Confidence::High,
                Some(input.observed_at_ns),
            ));
        }
        if let Some(owner) = &pipe.compute_owner {
            findings.extend(owner_finding(
                &input.owner_liveness,
                owner,
                &pipe.name,
                "the Pipe cannot drive until the lease reassigns; effects do not run while the owner is unreachable",
                "lease movement is epoch-fenced and proceeds automatically once the failover window passes",
                "confirm the owner is down; the next eligible Node takes over the lease",
                input.observed_at_ns,
            ));
        }
    }
    stage("pipes", findings)
}

/// One rule for every "owned work" liveness check: a dead owner is a
/// direct transport-probe verdict (high confidence), while an owner we
/// could not even resolve gets an advisory rather than a fabricated
/// death certificate.
fn owner_finding(
    liveness: &BTreeMap<String, OwnerState>,
    owner: &str,
    resource: &str,
    risk: &str,
    automatic_action: &str,
    next_safe_step: &str,
    observed_at_ns: i64,
) -> Option<DiagnosticFinding> {
    match liveness.get(owner) {
        Some(OwnerState::Live) => None,
        Some(OwnerState::Dead) => Some(finding(
            resource.to_string(),
            StageStatus::Degraded,
            format!("owner {owner} does not answer its health endpoint"),
            risk.to_string(),
            automatic_action.to_string(),
            next_safe_step.to_string(),
            Confidence::High,
            Some(observed_at_ns),
        )),
        Some(OwnerState::Unverifiable) | None => Some(finding(
            resource.to_string(),
            StageStatus::Advisory,
            format!(
                "owner {owner} liveness cannot be corroborated — no endpoint resolved for a health probe"
            ),
            "ownership may be fine or dead; treating unverifiable as dead would fabricate certainty"
                .to_string(),
            "no automatic recovery is triggered on unverifiable evidence alone".to_string(),
            "resolve the owner's registered endpoint; if it cannot be probed, inspect the Node directly"
                .to_string(),
            Confidence::Low,
            Some(observed_at_ns),
        )),
    }
}

fn distributaries_stage(input: &DiagnosticsInput) -> DiagnosticStage {
    let findings = input
        .distributaries
        .iter()
        .flat_map(|distributary| {
            let mut findings = Vec::new();
            if distributary.run_state == "stalled" {
                findings.push(finding(
                    distributary.name.clone(),
                    StageStatus::Degraded,
                    format!(
                        "copy cycle stalled: {}",
                        distributary
                            .last_error
                            .clone()
                            .unwrap_or_else(|| "unspecified refusal".to_string())
                    ),
                    "the destination stops receiving records or shipped StateStore mutations; source history remains durable upstream",
                    "the lease owner keeps retrying the cycle and republishes status each attempt",
                    "read the last_error first — it names the refused hop (auth, unreachable endpoint, missing Reader). Do not redeclare the route; the durable frontier resumes where it left off",
                    Confidence::High,
                    distributary.reported_at_ns,
                ));
            }
            if distributary.report_age_ms > DISTRIBUTARY_REPORT_STALE_MS {
                findings.push(finding(
                    distributary.name.clone(),
                    StageStatus::Degraded,
                    format!(
                        "no status report for {}ms (stale after {}ms)",
                        distributary.report_age_ms, DISTRIBUTARY_REPORT_STALE_MS
                    ),
                    "the lease owner may be dead; copying is paused until ownership recovers",
                    "lease recovery reassigns an epoch-fenced owner automatically once the failover window passes",
                    "check whether the reporting Node is in the live member set before assuming the route itself is broken",
                    Confidence::Medium,
                    distributary.reported_at_ns,
                ));
            }
            findings
        })
        .collect();
    stage("distributaries", findings)
}

fn indexes_stage(input: &DiagnosticsInput) -> DiagnosticStage {
    let findings = input
        .indexes
        .iter()
        .filter(|index| index.stage == "building")
        .map(|index| {
            finding(
                index.name.clone(),
                StageStatus::Advisory,
                format!("Index on {} is building a shadow generation", index.feed),
                "queries wait for the new generation to activate; the previous generation keeps serving meanwhile",
                "the build pass advances against committed source order and activates atomically",
                "no action needed unless the build stalls — check the Index's applied Cursor progress if it persists",
                Confidence::High,
                Some(input.observed_at_ns),
            )
        })
        .collect();
    stage("indexes", findings)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_input() -> DiagnosticsInput {
        DiagnosticsInput {
            node: "node-1".to_string(),
            observed_at_ns: 1_700_000_000_000_000_000,
            control_plane: Some(ControlPlaneEvidence {
                leader: Some("1".to_string()),
                term: 7,
                voters: 3,
            }),
            member_timeout_ms: 10_000,
            members: vec![
                MemberEvidence {
                    node: "node-1".into(),
                    local: true,
                    last_seen_age_ms: 0,
                },
                MemberEvidence {
                    node: "node-2".into(),
                    local: false,
                    last_seen_age_ms: 500,
                },
                MemberEvidence {
                    node: "node-3".into(),
                    local: false,
                    last_seen_age_ms: 800,
                },
            ],
            storage_nodes: (1..=3)
                .map(|index| StorageEvidence {
                    node: format!("control-{index}"),
                    eligible: true,
                    exclusions: vec![],
                })
                .collect(),
            ..DiagnosticsInput::default()
        }
    }

    fn stage_of<'a>(report: &'a DiagnosticsReport, name: &str) -> &'a DiagnosticStage {
        report
            .stages
            .iter()
            .find(|stage| stage.stage == name)
            .expect("stage must exist")
    }

    #[test]
    fn healthy_riverbed_reports_all_stages_healthy() {
        let report = explain(&base_input());
        assert_eq!(report.schema_version, DIAGNOSTICS_SCHEMA_VERSION);
        assert!(!report.limited);
        assert_eq!(report.authority, "control_plane");
        assert_eq!(report.stages.len(), 9);
        assert!(report
            .stages
            .iter()
            .all(|stage| stage.status == StageStatus::Healthy && stage.findings.is_empty()));
    }

    #[test]
    fn missing_leader_is_unhealthy_with_safe_next_step() {
        let mut input = base_input();
        input.control_plane = Some(ControlPlaneEvidence {
            leader: None,
            term: 12,
            voters: 3,
        });
        let report = explain(&input);
        let stage = stage_of(&report, "control_plane");
        assert_eq!(stage.status, StageStatus::Unhealthy);
        assert_eq!(stage.findings.len(), 1);
        assert!(stage.findings[0].next_safe_step.contains("voter"));
        assert!(stage.findings[0].automatic_action.contains("election"));
    }

    #[test]
    fn local_prototype_marks_control_plane_limited() {
        let mut input = base_input();
        input.control_plane = None;
        let report = explain(&input);
        assert!(report.limited);
        assert_eq!(report.authority, "local_prototype");
        assert_eq!(
            stage_of(&report, "control_plane").status,
            StageStatus::Limited
        );
    }

    #[test]
    fn stale_member_is_degraded_not_unhealthy() {
        let mut input = base_input();
        input.members[1].last_seen_age_ms = 45_000;
        let report = explain(&input);
        let stage = stage_of(&report, "nodes");
        assert_eq!(stage.status, StageStatus::Degraded);
        // Gossip staleness is evidence, not a death certificate.
        assert_eq!(stage.findings[0].confidence, Confidence::Medium);
    }

    #[test]
    fn fewer_than_three_eligible_storage_nodes_is_unhealthy() {
        let mut input = base_input();
        input.storage_nodes[1].eligible = false;
        input.storage_nodes[1].exclusions = vec!["below_baseline".into()];
        input.storage_nodes[2].eligible = false;
        input.storage_nodes[2].exclusions = vec!["capacity_stale".into()];
        let report = explain(&input);
        let stage = stage_of(&report, "storage");
        assert_eq!(stage.status, StageStatus::Unhealthy);
        assert!(stage
            .findings
            .iter()
            .any(|finding| finding.cause.contains("only 1 storage")));
    }

    #[test]
    fn stalled_distributary_reports_error_and_stale_silence() {
        let mut input = base_input();
        input.distributaries = vec![DistributaryEvidence {
            name: "mirror.orders".into(),
            run_state: "stalled".into(),
            last_error: Some("remote reader fetch refused: 401".into()),
            lag_records: Some(10),
            report_age_ms: 120_000,
            reported_at_ns: Some(1_699_999_000_000_000_000),
        }];
        let report = explain(&input);
        let stage = stage_of(&report, "distributaries");
        assert_eq!(stage.status, StageStatus::Degraded);
        assert_eq!(stage.findings.len(), 2);
        assert!(stage.findings[0].cause.contains("401"));
        assert!(stage.findings[1].cause.contains("no status report"));
    }

    #[test]
    fn paced_reader_is_degraded_with_credit_explanation() {
        let mut input = base_input();
        input.readers = vec![ReaderEvidence {
            name: "slow.reader".into(),
            feed: "orders.events".into(),
            session_active: true,
            last_retry_after_ms: 250,
        }];
        let report = explain(&input);
        let stage = stage_of(&report, "readers");
        assert_eq!(stage.status, StageStatus::Degraded);
        assert!(stage.findings[0]
            .cause
            .contains("credit or capacity exhaustion"));
    }

    #[test]
    fn subscription_owner_checks_direct_probe_not_member_gossip() {
        // member node-3 is fresh in gossip, but the owner probe is what
        // decides — the two identity spaces must not be conflated.
        let mut input = base_input();
        input
            .owner_liveness
            .insert("control-3".into(), OwnerState::Dead);
        input.subscriptions = vec![SubscriptionEvidence {
            name: "subs.daily".into(),
            progress_owner: Some("control-3".into()),
        }];
        let report = explain(&input);
        let stage = stage_of(&report, "subscriptions");
        assert_eq!(stage.status, StageStatus::Degraded);
        assert!(stage.findings[0].cause.contains("control-3"));
        assert_eq!(stage.findings[0].confidence, Confidence::High);
    }

    #[test]
    fn unverifiable_owner_is_advisory_not_death() {
        let mut input = base_input();
        input.subscriptions = vec![SubscriptionEvidence {
            name: "subs.unknown".into(),
            progress_owner: Some("ghost".into()),
        }];
        let report = explain(&input);
        let stage = stage_of(&report, "subscriptions");
        assert_eq!(stage.status, StageStatus::Advisory);
        assert_eq!(stage.findings[0].confidence, Confidence::Low);
    }

    #[test]
    fn unassigned_subscription_and_paused_pipe_report_politely() {
        let mut input = base_input();
        input.subscriptions = vec![SubscriptionEvidence {
            name: "subs.pending".into(),
            progress_owner: None,
        }];
        input
            .owner_liveness
            .insert("node-1".into(), OwnerState::Live);
        input
            .owner_liveness
            .insert("ghost-node".into(), OwnerState::Dead);
        input.pipes = vec![
            PipeEvidence {
                name: "pipe.rollup".into(),
                stage: "paused".into(),
                compute_owner: Some("node-1".into()),
            },
            PipeEvidence {
                name: "pipe.orphan".into(),
                stage: "declared".into(),
                compute_owner: Some("ghost-node".into()),
            },
        ];
        let report = explain(&input);
        assert_eq!(
            stage_of(&report, "subscriptions").status,
            StageStatus::Degraded
        );
        let pipes = stage_of(&report, "pipes");
        assert_eq!(pipes.status, StageStatus::Degraded);
        assert_eq!(pipes.findings.len(), 2); // advisory pause + degraded ghost owner
    }

    #[test]
    fn hot_key_is_advisory_never_fault() {
        let mut input = base_input();
        input.hot_keys = vec![HotKeyEvidence {
            feed: Some("orders.events".into()),
            range: "r-7".into(),
            token: "tk-1".into(),
            share_bps: 9_500,
        }];
        let report = explain(&input);
        let stage = stage_of(&report, "writers");
        assert_eq!(stage.status, StageStatus::Advisory);
        assert!(stage.findings[0]
            .next_safe_step
            .contains("never a metadata shortcut"));
    }

    #[test]
    fn findings_are_bounded_per_stage() {
        let mut input = base_input();
        input.storage_nodes = (0..200)
            .map(|index| StorageEvidence {
                node: format!("n-{index}"),
                eligible: false,
                exclusions: vec!["below_baseline".into()],
            })
            .collect();
        let report = explain(&input);
        let stage = stage_of(&report, "storage");
        assert_eq!(stage.findings.len(), MAX_FINDINGS_PER_STAGE);
        assert!(stage.truncated > 0);
    }

    #[test]
    fn report_serializes_stably() {
        let report = explain(&base_input());
        let value = serde_json::to_value(&report).expect("serialize");
        assert_eq!(value["schema_version"], 1);
        assert!(value["stages"].is_array());
        // Round-trip: the schema is versioned and must decode itself.
        let decoded: DiagnosticsReport =
            serde_json::from_value(value).expect("report must decode its own schema");
        assert_eq!(decoded, report);
    }
}
