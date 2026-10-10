# Whitewater Streams and cross-language client contract

**Status:** Product contract and delivery plan. The Rust Admin/Writer/Reader HTTP client foundation exists; there are no supported Python, Java, C#, Node.js/TypeScript, or Go libraries yet. Shared Subscriptions, Pipes, replicated StateStore queries, and atomic processing effects are not implemented. Illustrative processing flows below are **not runnable APIs**.

Whitewater Streams should not repeat Kafka Streams' Java-only experience. Rust, Python, Java, C#, Node.js/TypeScript, and Go applications must have the **same logical operations and guarantees**, with idiomatic language syntax. Server-side Pipes and StateStores are shared Riverbed capabilities, not six unrelated implementations of local state or co-partitioning. See [Kafka pain points](kafka-pain-points.md) and the [Index storage contract](why-whitewater.md#index-storage-contract-and-fjall-layout).

## Public model and ownership

```text
Riverbed -> Domain -> Feed -> Key -> Cursor -> Subscription
                        \-> StateStore / Index (named Domain resources)
                        \-> Pipe (managed processing)
```

Writers submit Feed, application Key, bytes, Metadata, event time, and a stable request identity. Readers fetch committed records and explicitly acknowledge; a named Subscription will share durable progress among cooperating Readers. A StateStore provides a logical primary-Key lookup and declared secondary Index lookups. A Pipe describes input, processing, state lookup/mutation, output, and failure policy. No public API asks for a partition count, Active Range, owner, replica, physical offset, or co-partitioning plan.

Same-Feed/same-Key accepted order is guaranteed; there is no total order across unrelated Keys or Feeds. A Reader can receive a record again after an ambiguous result or crash. Whitewater must deduplicate committed **effects**, not promise that every delivery occurs once. External databases, email, and other outside side effects are not part of a Whitewater transaction.

## One semantic protocol, six idiomatic libraries

| Operation | Semantics every SDK must preserve | Current server status |
|---|---|---|
| Admin | Typed commands and WCL converge on the authenticated Control API; request-ID retry returns the original result. | Typed Rust AdminClient and Control Plane available. |
| Writer | Stable request ID, WriterId and session epoch, per-Key order, idempotent append/batch, original MessageId/Cursor on identical retry, explicit ambiguous errors. | HTTP and Rust WriterSessionClient foundation available. |
| Reader | Open/fetch/ack/seek/close; delivered is distinct from acknowledged; Cursors remain opaque; bounded capacity and retry-safe ack. | HTTP and Rust ReaderSessionClient foundation available; bounded cross-Node reads fetch missing range owners; single-range Feeds seek deep Cursors and named Readers starting at Beginning have a Control Plane-backed multi-range progress frontier. Temporary/public Feed reads and split/merge frontier translation remain bounded or incomplete. |
| Subscription | One public durable name within a Domain; choose an exact Feed or all current/future Feeds in a Domain as the source. Cooperating member sessions share acknowledged progress and small epoch-fenced work leases, while distinct Subscriptions remain independent. | Only exact-Feed, Domain-scoped definitions are `declared` in the Control Plane; Domain-wide sources, joining, shared progress, and leases are not implemented. |
| StateStore | Named primary-Key get and bounded secondary lookup from any Node, freshness/version evidence, no local Fjall directory in application containers. | Only local Fjall prototype and declared metadata. |
| Pipe / processing effect | Read input, look up versioned state, produce Whitewater output and atomically record input progress with stable effect identity; explicit missing/lagging-state policy. | Planned. |

The wire format is an implementation detail; today's HTTP/JSON must have a versioned, language-neutral schema and conformance fixtures before any SDK claims parity. Library versions negotiate supported server capabilities and **fail explicitly** rather than silently omitting an operation or weakening durability.

Every language maps bytes to native byte arrays, a UUID to a native UUID type or canonical string, signed nanosecond time to a full signed 64-bit integer, and an opaque Cursor to an uninterpreted value. Node.js/TypeScript must use `bigint` (and decimal strings over JSON), **not** a JavaScript `number` that loses precision at nanosecond epoch values. Metadata contains bounded names and byte values; `Headers` is reserved for HTTP. A schema/JSON convenience layer is optional and must not change byte-level Writer/Reader behavior.

### Idiomatic façades, identical state transitions

- **Rust:** async `AdminClient`, `WriterSessionClient`, and `ReaderSessionClient` already exist. Add a non-admin application client, logical StateStore handle, shared Subscription Reader, and `process_effects` after the server contract passes fault tests.
- **Python:** provide `async` and synchronous façades around the same semantics, context-managed sessions, native `bytes`, `uuid.UUID`, integer nanoseconds, bounded streaming and cancellation. A Python example script is not an SDK.
- **Java:** provide asynchronous `CompletionStage`/stream consumption and optional blocking wrappers. No privileged server features, embedded mandatory RocksDB, or Java-only Pipe DSL.
- **C#:** provide `Task`/`IAsyncEnumerable`, `byte[]`/`ReadOnlyMemory<byte>`, `Guid`, `long` nanoseconds, cancellation tokens, and the same Writer/Reader/processing effect guarantees.
- **Node.js/TypeScript:** provide typed Promise/AsyncIterable APIs, `Buffer`/`Uint8Array`, `bigint` nanoseconds, `AbortSignal`, bounded backpressure, stable request IDs across reconnects, and explicit TLS/capability failures. Publish JavaScript consumption from the same supported Node package; never convert 64-bit record times to `number`.
- **Go:** provide `context.Context` cancellation, `[]byte` payload/Metadata, signed `int64` nanoseconds, bounded Reader iteration, and the same explicit retry/ambiguity contract without exposing internal placement.

A browser JavaScript SDK is **not** automatically the Node.js SDK: browser clients must not hold server-side admin keys. Consider a separate browser-safe gateway/scoped short-lived identity contract after production authentication is proven. C, C++, Ruby, PHP, and other language packages can follow demonstrated demand and the same conformance gate; adding their names does not silently commit Whitewater to supporting untested runtimes.

Keep shared behavior in a small language-neutral contract and per-language conformance suites, not a requirement to imitate one language's API names. Generated wire DTOs may share a schema; retry state, cancellation, and user-facing APIs should remain idiomatic and independently tested.

## User guide: append and consume today

1. Start the supported three-Node development Riverbed and use the authenticated Admin API/WCL to create a Domain, Feed, Writer, and Reader. The Rust `AdminClient` or `wwctl` exercises this existing path; Python acceptance scripts are not a published client.
2. Open a Writer session and append a Key and bytes. Retain the request ID across timeout/reconnect retries. Do not derive a new request ID for an ambiguous append. The returned MessageId and Cursor identify the same committed record on an identical retry.
3. Open a Reader session, fetch only committed records under a bounded capacity, process them, then acknowledge the response's **`delivered_cursor`** (not necessarily the Cursor on the last record). A multi-range named Reader can return an opaque progress token distinct from any Writer's record Cursor. Unacknowledged work may be delivered again after restart; do not infer physical positions from either token.

These operations are currently available at the prototype level; client TLS, role-based authorization, remote long-history Cursor continuation, and shared Subscription leases are not complete. See [Writer/Reader endpoints](admin-api.md) and [M2–M4 status](tasks.md#milestone-2--operational-writers).

## Multi-range Cursor continuation (named Reader prototype; other paths planned)

A Writer's record-attached Cursor identifies one committed event; it does **not** encode progress in every other range. Sorting each owner's events by ingest time and seeking after that one record cannot safely continue a busy multi-range Feed: a concurrent append on another owner can arrive with an earlier sort key. Whitewater promises same-Key order, not a public total Feed order. Do not convert a record Cursor into a guessed offset or silently skip another Key's history.

For named Readers starting at Beginning, a prototype versioned `delivered_cursor` now references a Control Plane-persisted per-range delivery frontier; acknowledgement copies that frontier to durable acknowledged progress. Records still carry their own Writer-compatible record Cursors. On reopen, the Reader starts from acknowledged progress, so no client manages ranges or a global sequencer. Movement with unchanged range identities works in the isolated four-Node acceptance test. The frontier is internal catalog state, not fields in public Reader definitions.

This is not yet the general solution: bounded RF3-corroborated split/merge cutovers now translate acknowledged frontiers atomically and fence old Reader sessions; the next session replays unacknowledged records, and a merge may conservatively replay already acknowledged records rather than skip a hole. If the old Cursor, complete source prefix, or cutover evidence is unavailable, cutover fails closed. Merging a Feed whose same Writer has overlapping post-split range sequences is still safely refused. Old record-Cursor seeks and timestamp starts still use bounded scans; temporary Reader and public Feed-read pagination still have bounded multi-range histories. The prototype fences competing deliveries and refuses a repeated fetch ID that could return a different page; it does not yet replay the exact original fetch response. Retention expiry, page cost, concurrent-append/clock-skew fault evidence, and SDK capability negotiation also remain. None should be reported as shipped parity across all six SDKs.

### Public identity and internal state (partially implemented)

A Subscription's Domain-scoped name is the intended single durable identity for an application processing independently or for several cooperating workers. Member sessions/leases remain separate, short-lived and fenced; they are not another user-configured `group.id`. Existing named Reader definitions still operate independently and must be migrated one-to-one without changing their acknowledged progress or silently joining them into a shared Subscription. `CREATE SUBSCRIPTION` currently records `stage=declared` only; it does not open a member session. A bounded local `SubscriptionLeaseTracker` prototype fences member epochs and rejects stale or expired work acknowledgements, but it has no RF3 durability, verified failover clock, or live join/fetch endpoint.

### Domain-wide sources and mandatory delivery Metadata (planned)

A Subscription may target one exact Feed or a whole Domain. The Domain source includes all active Feeds now and Feeds created later without editing the Subscription or interrupting work in other Feeds; it may start with no Feeds. Each Feed keeps independent acknowledged progress under its immutable FeedId. Existing Feeds follow the declared start policy; a newly created Feed starts from its first retained record by default, with a durable registration boundary so writes racing discovery are not skipped. Removing or moving a Feed cannot silently discard unacknowledged work: only that Feed drains or is explicitly disposed before its history can be deleted, while unrelated Feeds continue. Renaming a Feed does not reset its progress; deleting one and creating another with the same name creates a different FeedId and new progress. Work across distinct Feeds has no total order, even when the same business Key appears in both. Domain-level authorization, overrides and bounded cost must be checked again as Feeds appear, without leaking unauthorized Feed names.

Every delivery to a consumer of **one Feed or an entire Domain** supplies a typed `metadata` argument in the same processing method (conceptually `consume(record, metadata)`, in every SDK). Its reserved, server-generated `metadata.source.feed_name` is the **last** segment of the Feed name; `metadata.source.full_feed_name` is the **full Domain-qualified** name. For example, `orders.created` yields `created` and `orders.created`; `orders.billing.created` yields `created` and `orders.billing.created`. Application record Metadata remains available separately under `metadata.record`, so a Writer cannot spoof or collide with the source fields. Capture source names with each delivery receipt for identical retry/redelivery; subsequent new deliveries after a rename use the new canonical name while the immutable FeedId and Cursor remain stable. The on-wire schema, bounds, and equivalent callback shapes need conformance tests in Rust, Python, Java, C#, Node.js/TypeScript and Go.

**Not implemented:** today's `CREATE SUBSCRIPTION` accepts an exact source Feed only, joins are disabled, and current `RecordResponse.metadata_base64` contains Writer-provided bytes rather than these delivery source fields. The proposed `FROM DOMAIN` form and callback Metadata must not be presented as runnable APIs until the roadmap's RF3, recovery, and fault gates pass.

Do not create application-visible Feeds or Kafka-like internal topics for Subscription progress, leases, deduplication, or transaction/effect coordinator state. Whitewater may use a small fixed set of internal Fjall keyspaces plus an RF3 internal mutation protocol, checkpoints and recovery; a Fjall-local WAL/transaction is not cross-Node durability and does not atomically commit the separate Feed append. An isolated local Subscription-progress replica now separates durably prepared mutations from locally committed rows, validates bounded epochs/sequences/request identity, and requires two distinct matching prepare votes for a local commit; an in-process coordinator now rejects contradictory votes, requires owner-plus-follower prepare and owner-plus-follower commit, and fails ambiguously on a one-copy commit. New Subscription declarations now carry private Control Plane-persisted initial progress placement and epoch; the leader selects three replicas by SubscriptionId across the full configured eligible Node pool rather than inheriting the source Feed's three replicas. Deterministic 3/12/24-candidate tests prove placement distribution, **not** live 12/24-Node capacity. This topology is absent from public definitions and older declarations missing placement fail closed. Each Control Plane Node now has a private durable Fjall Subscription-progress replica. Development-only prepare/commit/read HTTP endpoints require the shared Control Plane credential; when optional Subscription-only mTLS is configured, these endpoints move to a separate CA-validated, certificate-pinned listener and are removed from the shared-key HTTP route to prevent downgrade. Both modes check receiver, owner, epoch and vote membership against current private placement; local Fjall calls run off Tokio. An async assigned-Node transport now rejects prepare, commit and read replies claiming the wrong assigned replica or epoch, but in development shared-key mode those claims are only JSON fields. Optional native mTLS pins the sender's CA-validated certificate to its NodeId and refuses a mismatch; forwarded commit-vote evidence is not independently signed, other internal protocols remain on the development transport, placement movement is not implemented, and no quorum-backed recovery exists. An internal committed-read prototype checks matching responses from two of three assigned replicas and fails closed on observed disagreement, missing quorum, stale epochs or uncorroborated empty state. An internal same-request retry now probes replicas, rejects contradictory progress, and can finish an interrupted prepare/commit after restart. Owner-loss recovery probes surviving assigned replicas through a fenced inspect, adopts the highest-sequence committed value while refusing same-sequence forks, moves ownership to an eligible surviving replica through a consensus compare-and-set that bumps the ownership epoch, and installs the recovered value on each reachable replica through an epoch-fenced adopt write; a placement synchronize heals lagging or returned replicas. Shared-key replies are not verified Node evidence, these operations are not public APIs, and replica-set membership changes plus a live consume path remain absent; this cannot yet be exposed as RF3 durability. No live replicated progress or transaction coordinator is implemented yet.

### Industrial multi-Reader gate (not implemented)

Every named standalone Reader has independent delivered and acknowledged progress; Subscriptions later share progress only among their own cooperating Readers. The current prototype writes each named Reader fetch and acknowledgement through the single consensus catalog, which serializes unrelated Readers and retains applied request history. This is **not** an industrial-scale progress backend, even though the four-Node two-Reader correctness test passes.

Before claiming scale, move Reader progress to ReaderId-sharded, epoch-fenced RF3 state with a bounded mutation journal, checkpoints and exact bounded fetch-result deduplication. A pinned Fjall **local prototype only** now proves transactional per-Reader rows, bounded latest fetch receipts, conflict fencing, and restart; it is not replicated, wired to HTTP, or a production engine choice. A separate bounded pacing-policy prototype adjusts suggested record/byte credits from backlog, acknowledgement latency, unacknowledged bytes, replica readiness, and Node pressure; it is not connected to live metrics or dispatch. Keep definitions/placement in the Control Plane, not every delivery. Cache immutable Feed pages safely without sharing progress, isolate a slow Reader, bound per-Reader in-flight bytes, and attribute read/egress/progress cost. Acceptance needs load curves across increasing independent Readers, steady-state and failover latency, progress-state bytes, restart catch-up, one-Node loss, split/merge translation, slow-Reader isolation and recovery without omitted acknowledged input. Publish measured capacity rather than equating a test count with an SLO.

## User guide: enrich by userId (target contract, not implemented)

```text
source Feed:    activity.events
state:          accounts.users, primary Key = userId
output Feed:    activity.enriched

for each committed input event:
    extract userId with a versioned field definition
    get accounts.users[userId] through the Riverbed at an explicit state version/consistency
    if absent or behind: follow declared retry/quarantine policy, do not ack success
    else: derive enriched output
    commit(input Cursor, output event, effect identity, chosen state version) as one Whitewater effect
```

The program never aligns partitions or installs Fjall on its application Node. Whitewater routes by logical Key and may optimize remote lookup, cache, or co-locate state internally, with visible network/latency costs. `latest committed at processing time` and `as-of event time/version` are different joins: the chosen state version must be recorded for deterministic retries. An event-time join additionally needs retained state versions and an explicit lateness policy. One strictly ordered hot Key cannot be parallelized just by scaling out.

The SDKs and managed Pipe must call **the same server-defined effect boundary**. An application must not simulate exactly-once by calling independent `append` then `ack`: a crash between those calls can duplicate output or lose input. Until server-side atomic effects exist, examples must clearly label separate calls as at-least-once with idempotent output and must not describe them as equivalent to a managed Pipe.

## User guide: rolling state without window plumbing (target contract, not implemented)

A user should express business intent such as **"count events per user in the last five minutes"**. Whitewater should make that a managed Pipe and a queryable, replicated StateStore view. The client must supply a Key/field, aggregation, and business duration; Whitewater cannot infer whether the product needs five minutes or thirty days. The client must **not** choose a slide interval, pane size, partition/co-partition layout, local Fjall store, changelog, timer schedule, or checkpoint strategy. A language-neutral conceptual request is:

```text
from activity.events
key by userId
count last 5m
materialize as analytics.recent_user_activity
```

`last 5m` means a rolling interval `(event_time - 5m, event_time]` evaluated for each accepted event, not five-minute tumbling buckets. Event time uses `event_time_ns` by default; an explicitly chosen ingest-time view has different replay semantics. All six SDKs must express the same `last(duration)`/`count`, `sum`, or `average` intent as a typed server-side Pipe definition, not separately implement window engines in six languages. A materialized view's StateStore/Index may answer current per-Key queries, with an applied Cursor/state version and a reported freshness policy.

The Pipe runtime owns bounded incremental aggregates, internally chosen panes, expiry even when no new events arrive, and output updates. A documented bounded lateness/watermark policy belongs to the Pipe or inherited Feed/Domain policy; users can override *business lateness* where necessary, but no event may be silently ignored. Late-but-admissible events require idempotent revised results; events beyond policy need an explicit retry/quarantine/final-disposition rule. The retained Feed/checkpoint horizon must cover the window plus allowed lateness and recovery headroom, or admission/rebuild fails clearly. Limit active Keys, state bytes, backfill rate, and CPU per Domain; expose cost, watermark, backlog, lateness, and limiting resource.

The **three-Node baseline remains supported**. A fourth Node is not a permanent "window worker" or extra durability quorum. Whitewater assigns small epoch-fenced processing leases to compute-capable Nodes and may move or add compute capacity when pressure warrants it; a fourth Node can carry compute work if present. The RF3 StateStore and input/output progress remain durable on storage Nodes. Worker failure transfers leases and resumes from verified progress without duplicating committed Whitewater effects or weakening availability; compute scale-in cannot remove unique state. A single strictly ordered hot userId still cannot be parallelized automatically.

A managed rolling view is not currently implemented. It depends on RF3 StateStores, Pipe definitions, watermark/lateness handling, and atomic Reader-progress/output/state effects; local Fjall index tests alone do not demonstrate any of these guarantees.

## Guide set to publish with each SDK

Each language's documentation must run the **same named acceptance scenarios** and show matching results: install/authentication and secure configuration; create or inspect logical resources; idempotent Writer append and batch; temporary and named Reader fetch/ack/replay; shared Subscription and capacity; logical StateStore primary/secondary lookup; manual and Feed-derived state; enrichment Pipe and rolling-window count/sum/average with late-event policy; retry/quarantine and restart/failure recovery; error handling; and migration from Kafka clients/Streams without co-partitioning. Unimplemented chapters are marked planned, not presented as runnable samples. Supply SDK test doubles for application unit tests without a live Riverbed, but run every example against a real three-Node Riverbed in CI before publishing it.

## Shared release and conformance gate

1. Specify canonical request/response schemas, protocol version/capability negotiation, typed error codes (`retryable`, `ambiguous`, `fenced`, `index_behind`, `authorization`), authentication/TLS, size bounds, and a golden fixture set. No API key is embedded in guides or test fixtures.
2. Run every SDK against the same three-Node server and the same fixtures: binary Keys/payload/Metadata, negative/large nanosecond times, stable Cursor round trips, duplicate and conflicting Writer retries, stale epochs, Reader redelivery and ack, capacity/cancellation, unavailable Node, mid-flight timeout, and restart.
3. Add StateStore and Pipe fixtures only when RF3 and atomic processing are implemented: multi-Index update/delete, remote lookup, state version, missing user, state lag, cross-Node routing, unique conflict (when supported), rolling-window boundary and idle expiry, admissible/too-late events, compute-lease failover, and snapshot/rebuild refusal when history is insufficient.
4. A supported-language release cannot quietly lack a documented core operation. If one language is experimental or a server capability is not ready, say so in its README, version negotiation, and published support matrix. Require package-version compatibility, tests on supported language runtimes, and reproducible build/publish provenance.

The Operations Advisor is not a data plane or SDK substitute; failures of optional tooling must not affect Writers, Readers, or StateStores.
