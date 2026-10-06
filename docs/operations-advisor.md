# Whitewater Operations Advisor

> Proposed agentic analysis and **policy-gated** assistance for operating a Riverbed. This is a design and roadmap item, **not an implemented service or MCP server**.

## Why it exists

Whitewater aims to make scaling, balancing, recovery, retries, and Reader delivery less dependent on Kafka-style operational expertise. Deterministic Control Plane reconciliation must do the routine safety-critical work; an operations advisor should help people understand unusual situations and choose a safe next step. Merely moving operational complexity into a chatbot would not solve the problem.

The Advisor correlates Whitewater signals with relevant Kubernetes and network context, identifies likely causes, proposes bounded remedies, and—only where a separate policy permits it—requests a pre-approved action. It should help a developer answer “why is this Writer retrying?” and an operator answer “is this Node safe to drain?” without either person learning internal range placement. Its default mode is **read-only advice**.

## Roles and authority

```text
Node / Writer / Reader / Control Plane / Index / Subscription signals (as available)
              + scoped Kubernetes and network observations
              + versioned Whitewater docs and runbooks
                              |
                    Operations Advisor
                      /             \
       evidence-backed explanation    proposed action
                   |                       |
        human / dashboard / MCP        policy gate + human approval
                                           |
                          authenticated Whitewater Admin API
                          or separate orchestrator/Operator API
```

- **Nodes and Control Plane** produce authoritative health, placement, commit, capacity, and decision evidence. The Advisor is neither a consensus voter nor a replacement for fencing, quorum checks, admission control, or deterministic autoscaling.
- **MCP (Model Context Protocol):** The Advisor acts as an MCP client, using a Whitewater MCP server for narrowly scoped inspection, explanation, and planning tools and separately scoped MCP integrations for Kubernetes/network observations. The same tools can help engineers directly. Any Whitewater MCP server is a frontend to existing authenticated APIs, not a second catalog, an unbounded shell, or a path around `ControlController`.
- **An external orchestrator or Kubernetes Operator** owns infrastructure credentials and applies approved Kubernetes changes. Do not mount Kubernetes/Docker administration credentials in data Nodes or allow a language model to issue arbitrary `kubectl` commands.
- **The model** can rank hypotheses and draft human-readable explanations. Authorization, preconditions, disruption budgets, compare-and-set state transitions, audit, and execution are enforced by deterministic services outside the model.

## What it investigates

Start with structured, bounded, timestamped observations keyed by Riverbed, Space, Feed, Node, and correlation/request ID. Potential signals include:

| Scope | Examples of useful evidence |
|---|---|
| Riverbed and Control Plane | quorum and leader health, catalog revision, Node membership, ownership epochs, recovery plans, rejected control decisions |
| Active Range and storage | owner/replica progress, committed versus uncommitted positions, RF3 health, repair/movement status, disk headroom, IO/CPU pressure, range hotness |
| Writers | admission/throttling, retryable versus permanent errors, append and quorum latency, session fencing, deduplication, uneven Key concentration |
| Readers and future Subscriptions | delivered versus acknowledged Cursor, capacity credits, wait time, replay/seek demand, lease state and delay cause when Subscriptions exist |
| Later features | Index freshness and checkpoint lag, Pipe backlog, schema rejections, history-tier retrieval, policy/authorization failures |
| Kubernetes and network | pod readiness/restarts, node pressure and eviction, volume health, DNS/service discovery, connection and TLS failures, network RTT/loss where measurable |

The first diagnostic question is **what is happening, to whom, why, what Whitewater already did, what remains at risk, and what a person can safely do next**. For example, a slow Reader is not automatically a Reader bug: the cause may be insufficient credits, a hot Key, remote history retrieval, a storage quorum delay, or a failing network path. Distinguish measured facts from hypotheses, and link each conclusion to a bounded observation window, source, and confidence level. Never imply causality from a single correlated metric.

Ground explanations in versioned Whitewater architecture, the running Riverbed's feature/compatibility state, validated runbooks, and relevant Kubernetes API, scheduling, CNI/DNS, CSI/storage, TLS, and network-failure knowledge. Show the source and version used for a claim; Kubernetes or networking expertise must not be presented as certainty about a particular cluster without corresponding observations. Outdated runbooks and retrieved documents are untrusted evidence, not permission to act.

Logs and traces are fetched **on demand**, only for the affected scope and time window after structured signals indicate what to inspect. Redact credentials, payloads, Keys and sensitive Metadata by default; enforce size, time, and cost limits. Treat log lines, record Metadata, runbook text, MCP responses, and external tickets as **untrusted input**, never instructions to the agent or authorization to use another tool.

## Learning busy and quiet schedules

The Advisor may learn *patterns*, not silently rewrite safety policy:

1. Keep bounded historical aggregates of append/read demand, commit latency, queue pressure, Node utilization, and incidents per logical owner. Record time zones, weekday/holiday effects, maintenance windows, and data freshness; handle daylight-saving changes explicitly.
2. Compare recurring busy/quiet windows with a rolling baseline and confidence interval. Require sustained evidence and separate high/low thresholds so a transient spike or lull cannot cause oscillation. Detect concept drift, releases, and one-off incidents before calling a pattern “normal.”
3. Explain a forecast and its uncertainty: “This Feed is usually busiest on Mondays; capacity is projected to be constrained during this window,” not “scale in because it is quiet right now.”
4. Recommend or schedule *pre-approved* capacity/readiness checks ahead of predictable demand. A quiet schedule is never permission to remove a Node until ownership, replica durability, and drain gates are verified.
5. Compare the observed outcome against the forecast and the proposed action; allow operators to label false positives, pin known maintenance effects, and disable/retrain a pattern. Learning must not create a self-reinforcing loop where the agent treats its own interventions as natural traffic.

Aggregates should have retention and cardinality limits, per-Space access boundaries, and no default model training on event payloads or customer logs. An unavailable model or missing history falls back to ordinary deterministic alerts and reconciliation.

## Recommendation and action policy

Use an explicit action ladder; deployment defaults to level 0. Policies are versioned, scoped to an identity and Riverbed/Space, and auditable.

| Level | Behavior | Examples |
|---|---|---|
| 0 — Observe | Read-only diagnosis with sources and uncertainty. | Explain quorum latency; compare Writer retries against Node and network events. |
| 1 — Recommend | Propose a typed plan, expected effect/cost/risk, alternatives, rollback, and verification steps. No mutation. | Suggest reducing Reader fetch concurrency or investigating a hot Key; recommend adding storage capacity. |
| 2 — Approve | A human reviews a narrow, expiring proposal; the same preconditions are rechecked at execution. | Approve one verified range repair or a bounded Node-drain plan through existing APIs when those actions exist. |
| 3 — Pre-approved automation | Only explicitly allowlisted, reversible, low-blast-radius operations under independent deterministic guardrails. Start disabled. | Request one bounded, idempotent replica catch-up under verified RF3 and health gates, once that workflow is proven safe for unattended execution. |

**Never autonomously** lower RF3/quorum requirements, discard acknowledged history, bypass owner fencing, skip repair evidence, force a scale-in, reset a Reader/Subscription Cursor, delete a Feed, grant access, rotate credentials, or override retention/legal holds. Irreversible, security-sensitive, customer-visible, and disaster-recovery actions require an authorized human and the underlying service's own validation; some may remain permanently manual.

A proposed action includes: scope and immutable resource IDs; observed evidence and freshness; why it was chosen over alternatives; predicted availability/durability/SLO/cost impact; exact typed operation and idempotency key; prerequisites (current epoch, quorum, replica catch-up, credits, budget, and no conflicting plan); approval identity and expiry; rollback boundary; and post-action checks. If the Control Plane or external tool returns an ambiguous result, **inspect authoritative state before retrying**. The Advisor must not fabricate a success from a timeout.

## MCP tool boundaries

A possible first MCP surface is read-only, using the authenticated Whitewater Admin API and purpose-built structured diagnostics:

```text
whitewater.riverbed.health
whitewater.riverbed.explain_pressure
whitewater.feed.describe
whitewater.writer.explain_retries
whitewater.reader.explain_delay
whitewater.range.inspect_placement       (operator-only)
whitewater.scale.recommend
whitewater.support.collect_redacted      (approval/policy gated)
```

Kubernetes/network tools should expose only the namespaces, clusters, resource types, and observation windows permitted to that operator identity. Mutating MCP tools require **explicit human authorization and confirmation** for the specific typed, scoped plan; they re-authorize at use time, then invoke the same idempotent Whitewater Control API or external Operator actuator as every other frontend. Optional pre-approved low-risk automation runs through a separate deterministic policy/actuator path, not an unconfirmed model-issued MCP mutation. MCP credentials must be short-lived, scoped, rotated, and recorded without logging secrets. The Advisor must not make the MCP server or an LLM a dependency of the Writer/Reader hot path.

## Safety, security, and resource budgets

- Separate recommendation identity from execution identity; use least privilege, per-Space isolation, explicit human confirmation, and an immutable audit trail for proposals, approvals, denials, tool calls, and outcomes. Present operational internal topology only to authorized operators, never in application Writer/Reader APIs.
- Fail closed on stale snapshots, lost Control Plane quorum, inconsistent Node evidence, unknown TLS/peer identity, missing approval, or missing rollback preconditions. External service-mesh transport is acceptable only when identity, encryption, rotation, audit, and downgrade prevention meet Whitewater's security contract.
- Limit model prompts, retrieved logs, tool calls, concurrent investigations, movement bandwidth, and spending. Foreground appends and Reader delivery take precedence; an analysis surge must not cause a cluster incident.
- Support an immediate per-Riverbed/Space kill switch and audit-safe manual override. Restarting the Advisor must not replay non-idempotent actions; a durable action ledger and authoritative-state check are required before any mutation.
- Report costs and charge inference, storage, log retrieval, egress, and movement to logical owners where possible. The Advisor should be useful without sending customer data to an external model; deployment policy controls model location and data egress.

## Example investigations

**Writer incident:** A Writer's retry rate rises on one Feed. The Advisor correlates majority-commit latency, a slow replica, and Kubernetes volume pressure. It reports the acknowledged-versus-ambiguous boundary, notes any automatic repair in progress, and recommends investigating the volume or an approved replica replacement; it does not weaken acknowledgements to make the alert disappear.

**Reader delay:** Delivered Cursor advances but acknowledged Cursor does not. The Advisor distinguishes application processing time from server credit exhaustion and network errors, gives a developer-facing retry/capacity recommendation, and leaves the Reader's progress untouched. Future Subscription lease diagnostics add scoped lease evidence without triggering a global rebalance.

**Predictable busy window:** A Space's Feeds repeatedly spike during a business batch. The Advisor forecasts likely resource pressure, explains confidence and historical exceptions, suggests a bounded capacity plan before the window, and measures whether it helped. During the quieter period it recommends scale-in only after independent drain and durability checks.

## Delivery sequence and acceptance evidence

This proposal depends on structured health explanations, scoped identities, an audit trail, bounded movement/drain APIs, and production transport security. It belongs **after** the corresponding M4/M5/M9 correctness work; it does not block ordinary deterministic healing.

1. **Foundation:** Define versioned diagnostic schemas, bounded redacted support queries, causal correlation IDs, and a read-only MCP adapter. Test permission boundaries, evidence freshness, redaction, and failure without a model.
2. **Advisor:** Combine versioned Whitewater documentation/runbooks with scoped Kubernetes/network observations. Add schedule-aware baselines and evidence-linked recommendations. Test against known incidents, synthetic busy/quiet cycles, false positives, drift, missing data, and adversarial log/prompt injection.
3. **Controlled execution:** Introduce typed proposals, approval UX/API, policy allowlists, short-lived execution capability, action ledger, server-side precondition rechecks, cost/disruption budgets, and rollback verification. Exercise control-leader loss, approval expiry, partial action success, and restart after an ambiguous response.
4. **Optional automation:** Enable only individually proven low-risk actions by opt-in policy; measure recommendation quality, prevented incidents, incorrect actions, SLO impact, and operator time saved. Keep a read-only mode and kill switch at all times.

**Definition of done:** A developer or operator can ask “why?” and receive a sourced, scoped, actionable explanation; a false-positive recommendation causes no mutation; an approved action cannot weaken RF3, ordering, authentication, or retention; lost quorum or ambiguous tool results stop further action; and fault-injection tests prove that disabling the Advisor does not affect core Writer/Reader availability.

See [Whitewater operational experience](operational-experience.md#operator-facing-apis-and-tools), the [authenticated Admin API](admin-api.md), and the [milestone tracker](tasks.md). Until those prerequisites and tests exist, this document describes **intent, not shipped capability**.
