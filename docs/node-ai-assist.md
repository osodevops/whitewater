# Whitewater Node AI Assist

> Ask any available Node what is happening, why it might be happening, and what you can safely do next — without learning physical placement or piecing together an incident from unrelated dashboards.

## Status and purpose

**Proposed design, not implemented.** This document develops the Node-facing experience of the [Whitewater Operations Advisor](operations-advisor.md). It is the same Advisor, authorization model, diagnostic schemas, and action policy — not a second AI service or control plane. Delivery belongs to Milestone 10 in [tasks](tasks.md); it does not replace the current implementation focus.

The problem is operational context: a Writer retries, a Pipe stops producing, or a Node cannot drain, but the explanation is spread across metrics, logs, storage state, and control decisions. Node AI Assist should reduce the work of gathering and interpreting that evidence. Deterministic services still own replication, placement, recovery, admission, and safe scaling.

The public application model remains `riverbed -> domain -> feed -> key -> cursor -> subscription`. Developers investigate business resources; authorized operators may inspect the internal placement needed to explain a fault. Neither audience should have to locate a special diagnostic owner before asking a question.

## The experience: ask from wherever you are

A caller connects through any available Node and selects a permitted scope: this Node, a Feed, a Domain, a Pipe/Subscription, or the Riverbed. A question may include a request/correlation ID and a bounded time window. A sensible default is a short recent window, not an unrestricted scan of all logs and history.

The response should consistently answer:

1. **What is happening?** Separate the observed symptom from its possible cause.
2. **Who is affected?** Identify authorized logical resources and the extent of the impact.
3. **What evidence supports the explanation?** Link timestamped observations and their sources.
4. **What has Whitewater already done?** Show relevant repair, retry, placement, or throttling decisions.
5. **What remains uncertain or at risk?** Include missing Nodes, stale evidence, and durability/availability consequences.
6. **What is the next safe step?** Offer a bounded check or proposal, its expected impact, and how to verify the result.

The same structured result should support a UI explanation, a CLI summary, an API response, or future MCP tools. Equivalent access through different Nodes means consistent permissions and evidence semantics, not identical model wording.

## Practical ideas, ordered by usefulness

All items below are proposed delivery slices. Existing diagnostic foundations are identified later; the table does not claim these AI workflows already exist.

| Priority | Feature | Example question | What makes it useful |
|---|---|---|---|
| First release | **Explain this Node** | “Why is this Node slow?” | A concise health card separating process liveness, Control Plane connectivity, data readiness, storage pressure, and workload pressure. No inference that `/health = ok` means the data service is healthy. |
| First release | **Trace one failed operation** | “Why did this Writer request time out?” | Follow a request through admission, routing, replication, and commit evidence. Distinguish a refusal from an ambiguous result; recommend the correct retry identity rather than a new write. |
| First release | **Explain delivery and processing delay** | “Why is billing behind?” | Separate Reader capacity, lease ownership, progress persistence, state freshness, Pipe compute ownership, and upstream inactivity. A Pipe's compute owner need not be its progress owner. |
| First release | **Explain a blocked action** | “Why can't this Node drain?” | Reuse the authoritative drain plan and readiness checks to explain missing replacement capacity, unfinished transfers, or conflicting work. No separate AI interpretation of whether removal is safe. |
| Next | **What changed? incident timeline** | “It worked before the deployment. What changed?” | Correlate policy revisions, process restarts, ownership moves, repair, and latency changes. Show temporal order and clock uncertainty without claiming correlation proves causation. |
| Next | **Compare with a healthy peer** | “Why is only this Node struggling?” | Compare Nodes with similar roles, hardware, traffic, and observation windows; highlight differences instead of treating a quiet or differently provisioned Node as the universal baseline. |
| Next | **Explain effective policy** | “Why did this request wait, and why won't we scale down?” | Show the effective policy, its source/version, the constraint it applied, and the decision evidence. Read actual running capabilities; never assume a planned ACK/RF setting is already active. |
| Next | **Capacity and maintenance preview** | “Would another Node help?” | Explain the measured bottleneck and reuse the deterministic planner when available. Distinguish storage, compute, routing, and a strictly ordered hot Key; include uncertainty, cost, and possible write disruption. |
| Next | **Incident handover pack** | “Summarize this for the next on-call engineer.” | Produce a redacted evidence timeline, hypotheses checked, actions already taken, remaining risks, and next checks. Export only after scope/redaction review. |
| Later | **Incident-to-test draft** | “How can we reproduce this safely?” | Draft a synthetic sample/Playbook or fault-test outline for an isolated Riverbed. Do not copy production payloads or execute a replay/reset automatically. |
| Later | **Recurring-incident memory** | “Have we seen this before?” | Retrieve authorized, reviewed incident summaries with version and resolution evidence. Expire stale guidance; similar symptoms are not proof of the same cause. |
| Later | **Before-and-after verification** | “Did that approved repair actually help?” | Compare predeclared success criteria with measured outcomes. Report no improvement or insufficient evidence instead of declaring success because an action returned HTTP 200. |

Scheduled demand forecasts remain part of the existing Advisor plan. They should follow the diagnostic baseline rather than delay the first useful read-only release.

## Evidence, not a confident guess

Each diagnostic observation needs a versioned envelope with:

- Source identity, permitted logical resource IDs, collection time, observation interval, and evidence reference.
- Catalog/policy revision or ownership epoch where relevant; running server feature/version information.
- Units and measurement semantics: a counter, rate, gauge, committed position, or sampled estimate must not be confused with another.
- Freshness, coverage, and collection outcome: available, stale, unavailable, contradictory, or withheld by authorization. Public output must not reveal hidden resource names or counts through permission errors.
- Cost and truncation information: bytes, records, time, and fan-out limits reached.

A cross-Node investigation is not an atomic snapshot simply because its records have timestamps. Preserve per-source revisions and clock uncertainty, and explicitly flag observations taken across a placement or policy change. Cursors stay opaque; the model must not sort them or derive physical positions from them.

The explanatory layer should distinguish **observed facts**, **likely causes**, **alternatives**, and **evidence still needed**. Prefer explicit evidence-strength categories to invented numerical confidence percentages. Each recommendation should point back to observations or a compatible, versioned runbook. Contradictory evidence reduces certainty and blocks action; it is not averaged away.

### Illustrative answer, not a measured incident

```text
Symptom: Writer retries increased for the selected Feed.
Observed: Replication latency rose during a replica catch-up operation.
Coverage: Two permitted Nodes responded; the third did not.
Possible cause: Shared disk or network contention. Not yet confirmed.
Already happening: Whitewater reports a catch-up in progress.
Next check: Inspect bounded IO/network evidence for the affected interval.
Write outcome: Use the request's authoritative commit evidence; timeout
               alone does not prove that the append was rejected.
Action taken by AI: None.
```

## Architecture: available through a Node, not coupled to its data loop

```text
UI / CLI / API / future MCP caller
    -> any available Node's authenticated diagnostic entry point
    -> bounded investigation coordinator
    -> typed read-only collectors and authorized evidence cache
    -> deterministic checks + optional model explanation
    -> structured answer with sources and limitations
```

These are proposed boundaries, not new endpoint names or a requirement to introduce one service per box.

- **Node entry point:** authenticate the caller, validate scope/time/budgets, and route the investigation. Preserve delegated scope; never forward a broad Node admin credential as the caller's identity.
- **Collectors:** use existing Control/data diagnostic APIs with narrow adapters. Gather structured data first; fetch logs/traces only to answer a specific unresolved question. Collectors must not expose arbitrary shell, filesystem, or URL access.
- **Coordinator:** bound fan-out, deadlines, cancellation, retries, and concurrency. Use an investigation ID to correlate retries and avoid duplicate work/billing where possible. A resumed investigation must report missing history rather than fabricate continuity.
- **Evidence cache:** separate security scopes and policy revisions; bound size and retention. Reauthorize access to cached evidence and exports after permission changes. A cache is not an authoritative catalog or a durable commit source.
- **Explanation provider:** use a documented interface that supports a disabled/deterministic-only mode and deployment-approved local or external inference. Model selection, provider credentials, and data-egress policy remain outside prompts.
- **Core services:** continue to decide readiness, durability, leases, movement, and policy validity. AI suggestions do not become consensus evidence or bypass `ControlController`.

Any available Node should provide access, including newly registered Nodes, without requiring inference inside every Rust data process. A model outage, disabled Advisor, or overloaded investigation queue must not become a dependency of appends, reads, or recovery.

## Starting from the code that exists

| Foundation | Reuse | Do not assume |
|---|---|---|
| [Node/API diagnostics](../src/api.rs) | Health, metrics, Control Plane status, and authenticated administrative surfaces. | The simple health response is a full readiness or durability assessment. |
| [Demand metrics](../src/demand.rs) | Node/range demand and bounded hot-Key observations. | These counters already implement tenant quotas or identify a root cause. |
| [Replica repair](../src/active_range/repair.rs) | Verified catch-up progress, transferred bytes, readiness, and limiting-resource evidence. | Every diagnostic is already retained, queryable remotely, or correlated across an incident. |
| [Storage drain](../src/active_range/drain.rs) and [control planning](../src/control.rs) | Existing drain plans, movement budgets, exclusions, and safe-retirement decisions. | AI may override an unplannable move or infer safety from an idle CPU. |
| [Majority append](../src/active_range/majority.rs) | Actual durable-replica and commit evidence and ambiguity handling. | Strict ACK ALL is already the running default; current success uses quorum evidence. |
| [Operational requirements](operational-experience.md) | Existing health explanation, dry-run, cost, restore, and isolation requirements. | Diagnostic integration makes unfinished lifecycle or security features available. |

The first implementation task is a capability inventory: which signals can actually be collected safely now, which need a bounded API, and which remain unavailable. Answers must reflect the running version, not the most optimistic product document.

## Safety and degraded behavior

Read-only means read-only. In the first release there is no mutation tool exposed to the model. Investigation requests may create bounded audit/cache records, but cannot write business data, alter placement, or change policies.

| Condition | Required behavior |
|---|---|
| A Node cannot be contacted | Return scoped partial coverage; do not mark it healthy or claim a complete diagnosis. |
| Model/provider unavailable | Return deterministic observations and limitations; keep normal service and non-AI diagnostics available. |
| Lost quorum, stale placement, or conflicting evidence | Explain what is known and defer safety-sensitive recommendations requiring authoritative current state. |
| Investigation overload or budget exhaustion | Cancel/defer collection and report truncation; protect live data traffic and recovery resources. |
| Authorization revoked during an investigation | Recheck before collection, retrieval, and export; discard or redact results the caller is no longer entitled to see. |
| Model invents an operation or tool | Reject it outside the model; no arbitrary command execution or fallback shell. |
| A log/runbook contains instructions to the agent | Treat it as untrusted evidence, never authority or an instruction to call tools. |

Redact secrets, API keys, certificate private material, payloads, business Keys, and sensitive Metadata before they enter a model prompt or export by default. Explicitly permitted deeper inspection must remain scoped, minimized, audited, and subject to data-egress policy. Do not train on customer data by default. Scope retention and deletion of cached evidence, conversations, and incident packs independently from immutable Feed history.

Later action proposals use the existing Advisor approval ladder: a typed plan, current preconditions, authorization, expiry, audit, idempotency, and post-action verification. Ordinary inference cannot lower RF/ACK guarantees, reset Cursors, drop Feeds, grant access, remove Nodes, or perform disaster-recovery promotion. Infrastructure changes go through a separate authorized orchestrator; data Nodes receive no Docker/Kubernetes administration credentials. Unsupported or irreversible actions remain manual.

## Delivery slices and tests

### A. Evidence-first Node diagnostics

Define the observation/answer schemas, capability inventory, typed collectors, scope enforcement, and deterministic explanation templates. Build fixture-driven tests before connecting a model. Prove healthy, degraded, unknown, stale, contradictory, and unauthorized states stay distinct. Include exact time/unit handling, opaque Cursors, and redaction.

### B. Read-only access through any Node

Implement the bounded entry point/coordinator and a mockable explanation-provider boundary. Test equivalent permission checks through each Node of an isolated three-Node Riverbed and a newly registered fourth Node. Exercise contacted-Node loss, partial evidence, cancellation, repeated investigation IDs, permission revocation, disabled AI, and model timeout. Do not require a paid/external model or real credentials in unit/CI tests.

### C. Useful investigations and operator workflow

Deliver the four first-release investigations before predictive features. Add incident timelines, comparable-peer analysis, policy explanations, and redacted handover packs incrementally. Ground-truth incident fixtures should include a slow replica, disk pressure, Reader backpressure, stale state, hot Key, blocked drain, and a Pipe whose compute and progress owners differ. Correct output may be an explicit abstention when evidence is insufficient.

### D. Release evidence and optional later actions

Run live fault/load acceptance against isolated disposable infrastructure. Measure investigation overhead, latency, model/tool cost, misleading recommendations, missing-evidence reporting, and operator time to reach a supported conclusion. Set acceptance thresholds before the experiment rather than inventing improvement percentages afterward. Prove that an investigation surge or disabling AI does not change the platform's correctness guarantees or exceed its agreed foreground impact budget.

Only after read-only gates pass should the existing Advisor action work be enabled. Test stale approvals, conflicting concurrent changes, ambiguous results, restart, audit completeness, and rollback boundaries with a deterministic actuator. A convincing chat demo does not establish an autonomous repair guarantee.

## Decisions to settle before implementation

- How are supported providers selected, and which deployments permit external data egress? No provider is selected by this design.
- Where does the investigation coordinator run, and what bounded state is needed for reconnect/restart? Any-Node access does not prescribe inference placement.
- Which identity/capability permits Node-wide versus Domain-scoped evidence, and which diagnostic fields must remain operator-only?
- What are the per-identity/Domain/Riverbed time, byte, concurrency, retention, and spending limits?
- Which hypotheses can be evaluated deterministically, and what calibrated evidence categories will the model be permitted to emit?
- Which incident-to-test and predictive features justify their cost after the read-only release is measured?

## Related documents

- [Operations Advisor authority and action policy](operations-advisor.md)
- [Operational experience](operational-experience.md)
- [Whitewater vs Kafka](whitewater-vs-kafka.md)
- [Delivery tasks](tasks.md)
