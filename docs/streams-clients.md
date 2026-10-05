# Whitewater Streams and cross-language client contract

**Status:** Product contract and delivery plan. The Rust Admin/Writer/Reader HTTP client foundation exists; there are no supported Python, Java, C#, Node.js/TypeScript, or Go libraries yet. Shared Subscriptions, Pipes, replicated StateStore queries, and atomic processing effects are not implemented. Illustrative processing flows below are **not runnable APIs**.

Whitewater Streams should not repeat Kafka Streams' Java-only experience. Rust, Python, Java, C#, Node.js/TypeScript, and Go applications must have the **same logical operations and guarantees**, with idiomatic language syntax. Server-side Pipes and StateStores are shared Fabric capabilities, not six unrelated implementations of local state or co-partitioning. See [Kafka pain points](kafka-pain-points.md) and the [Index storage contract](why-whitewater.md#index-storage-contract-and-fjall-layout).

## Public model and ownership

```text
Fabric -> Space -> Feed -> Key -> Cursor -> Subscription
                        \-> StateStore / Index (named Space resources)
                        \-> Pipe (managed processing)
```

Writers submit Feed, application Key, bytes, Metadata, event time, and a stable request identity. Readers fetch committed records and explicitly acknowledge; a named Subscription will share durable progress among cooperating Readers. A StateStore provides a logical primary-Key lookup and declared secondary Index lookups. A Pipe describes input, processing, state lookup/mutation, output, and failure policy. No public API asks for a partition count, Active Range, owner, replica, physical offset, or co-partitioning plan.

Same-Feed/same-Key accepted order is guaranteed; there is no total order across unrelated Keys or Feeds. A Reader can receive a record again after an ambiguous result or crash. Whitewater must deduplicate committed **effects**, not promise that every delivery occurs once. External databases, email, and other outside side effects are not part of a Whitewater transaction.

## One semantic protocol, six idiomatic libraries

| Operation | Semantics every SDK must preserve | Current server status |
|---|---|---|
| Admin | Typed commands and WCL converge on the authenticated Control API; request-ID retry returns the original result. | Typed Rust AdminClient and Control Plane available. |
| Writer | Stable request ID, WriterId and session epoch, per-Key order, idempotent append/batch, original MessageId/Cursor on identical retry, explicit ambiguous errors. | HTTP and Rust WriterSessionClient foundation available. |
| Reader | Open/fetch/ack/seek/close; delivered is distinct from acknowledged; Cursors remain opaque; bounded capacity and retry-safe ack. | HTTP and Rust ReaderSessionClient foundation available; scalable remote Feed reads remain incomplete. |
| Subscription | Shared durable acknowledged progress, capacity-aware small epoch-fenced leases, independent progress per Subscription, bounded redelivery. | Planned. |
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

1. Start the supported three-Node development Fabric and use the authenticated Admin API/WCL to create a Space, Feed, Writer, and Reader. The Rust `AdminClient` or `wwctl` exercises this existing path; Python acceptance scripts are not a published client.
2. Open a Writer session and append a Key and bytes. Retain the request ID across timeout/reconnect retries. Do not derive a new request ID for an ambiguous append. The returned MessageId and Cursor identify the same committed record on an identical retry.
3. Open a Reader session, fetch only committed records under a bounded capacity, process them, then acknowledge the delivered Cursor. An unacknowledged record may be delivered again after restart; do not infer physical positions from Cursors.

These operations are currently available at the prototype level; client TLS, role-based authorization, remote long-history Cursor continuation, and shared Subscription leases are not complete. See [Writer/Reader endpoints](admin-api.md) and [M2–M4 status](tasks.md#milestone-2--operational-writers).

## User guide: enrich by userId (target contract, not implemented)

```text
source Feed:    activity.events
state:          accounts.users, primary Key = userId
output Feed:    activity.enriched

for each committed input event:
    extract userId with a versioned field definition
    get accounts.users[userId] through the Fabric at an explicit state version/consistency
    if absent or behind: follow declared retry/quarantine policy, do not ack success
    else: derive enriched output
    commit(input Cursor, output event, effect identity, chosen state version) as one Whitewater effect
```

The program never aligns partitions or installs Fjall on its application Node. Whitewater routes by logical Key and may optimize remote lookup, cache, or co-locate state internally, with visible network/latency costs. `latest committed at processing time` and `as-of event time/version` are different joins: the chosen state version must be recorded for deterministic retries. An event-time join additionally needs retained state versions and an explicit lateness policy. One strictly ordered hot Key cannot be parallelized just by scaling out.

The SDKs and managed Pipe must call **the same server-defined effect boundary**. An application must not simulate exactly-once by calling independent `append` then `ack`: a crash between those calls can duplicate output or lose input. Until server-side atomic effects exist, examples must clearly label separate calls as at-least-once with idempotent output and must not describe them as equivalent to a managed Pipe.

## Guide set to publish with each SDK

Each language's documentation must run the **same named acceptance scenarios** and show matching results: install/authentication and secure configuration; create or inspect logical resources; idempotent Writer append and batch; temporary and named Reader fetch/ack/replay; shared Subscription and capacity; logical StateStore primary/secondary lookup; manual and Feed-derived state; enrichment Pipe and retry/quarantine; restart/failure recovery; error handling; and migration from Kafka clients/Streams without co-partitioning. Unimplemented chapters are marked planned, not presented as runnable samples. Supply SDK test doubles for application unit tests without a live Fabric, but run every example against a real three-Node Fabric in CI before publishing it.

## Shared release and conformance gate

1. Specify canonical request/response schemas, protocol version/capability negotiation, typed error codes (`retryable`, `ambiguous`, `fenced`, `index_behind`, `authorization`), authentication/TLS, size bounds, and a golden fixture set. No API key is embedded in guides or test fixtures.
2. Run every SDK against the same three-Node server and the same fixtures: binary Keys/payload/Metadata, negative/large nanosecond times, stable Cursor round trips, duplicate and conflicting Writer retries, stale epochs, Reader redelivery and ack, capacity/cancellation, unavailable Node, mid-flight timeout, and restart.
3. Add StateStore and Pipe fixtures only when RF3 and atomic processing are implemented: multi-Index update/delete, remote lookup, state version, missing user, state lag, cross-Node routing, unique conflict (when supported), and snapshot/rebuild refusal when history is insufficient.
4. A supported-language release cannot quietly lack a documented core operation. If one language is experimental or a server capability is not ready, say so in its README, version negotiation, and published support matrix. Require package-version compatibility, tests on supported language runtimes, and reproducible build/publish provenance.

The Operations Advisor is not a data plane or SDK substitute; failures of optional tooling must not affect Writers, Readers, or StateStores.
