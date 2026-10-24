# Whitewater SDKs: choose your language, not your capabilities

> Six languages. One streaming and state model. Simplicity without a second-class SDK.

Whitewater is a Rust-based streaming platform and distributed database designed around a straightforward idea: **your choice of language should not decide which parts of the platform you can use.** Rust, Python, Java, C#/.NET, Node.js/TypeScript, and Go belong to the same product from the outset — not one flagship streaming library and five producer/consumer clients.

**Status:** Product positioning and cross-language design contract. SDK implementations and a shared protocol specification exist; full feature parity is a release requirement, not a claim that every capability has already passed every language's tests. Pipes, windows, stateful processing, and failure recovery must be checked against the capability and conformance evidence for the particular SDK/server versions. See the [technical client contract](streams-clients.md) and [shared SDK specification](../sdk/spec/README.md).

## Kafka has clients in many languages. Kafka Streams is a different proposition.

Sending and receiving records is not the same as having a stream-processing toolkit.

Kafka Streams provides Java/JVM applications with a substantial suite: a high-level DSL, processing topologies, a lower-level Processor API, stream/table abstractions, joins, windowed aggregations, state stores, and processing guarantees integrated with Kafka. Developers describe a processing graph rather than building everything around a polling loop.

**That same Kafka Streams suite is not supplied as a native library for Python, C#, Node.js/TypeScript, Go, or Rust.** Those languages can use Kafka clients, but a producer and consumer do not bring the Streams topology model, DSL, and stateful processing runtime with them. Teams must introduce another framework or service, write the missing machinery, or move the processing into a JVM application.

Strictly speaking, Kafka Streams is not restricted to Java source code: Scala and other JVM languages can use it, and a Scala DSL exists. Third-party stream-processing frameworks and services such as ksqlDB also offer alternatives. The distinction is the absence of **the same native Kafka Streams function suite across these language SDKs**, not an inability to process Kafka data outside Java.

Whitewater's answer is not to clone the Java DSL six times. It is to make the streaming capabilities part of a shared platform contract and give every SDK the same access to them.

## Simple should not mean limited

**All Whitewater SDKs are designed to be basically identical in nature: the same conceptual API, the same streaming functions, the same state operations, and the same correctness rules.** Language conventions change; the product does not.

A Python developer should not have to learn a reduced version of the platform. A Go service should not need a Java sidecar just to use managed stateful processing. A C# team should not have to reconstruct the retry and acknowledgement rules already built into another SDK.

The aim is a small set of composable concepts:

| Capability | Shared Whitewater experience |
|---|---|
| Write | Choose a Feed and business Key; append individual records or batches with stable retry identities. |
| Read and replay | Fetch history, follow live data, seek with opaque Cursors, and acknowledge completed work. |
| Subscribe | Consume continuously with bounded capacity; cooperating members use a named Subscription and fenced work leases. |
| Query and maintain state | Define a StateStore, write rows, look up primary or named secondary keys, and iterate bounded pages. |
| Build processing flows | Use the shared Pipe contract for supported processing operations, including enrichment and windowed aggregation as those capabilities pass their delivery gates. |
| Commit processing effects | Coordinate Whitewater outputs and input progress through the same effect contract, rather than inventing a different correctness model in each language. |
| Manage resources | Use typed helpers or WCL through the same Control API. |

This is **functional parity, not identical spelling**. `get_by`, `getBy`, and `GetByAsync` can feel native in their respective languages while representing the same lookup. Async control flow, cancellation, byte types, and error handling should fit the host language without changing what an operation means.

## No partitions to configure. Three Nodes to start.

**Every SDK uses a partition-free application model.** Choose a Feed and a business Key — not a partition count, partitioning strategy, storage owner, or replica assignment. Applications do not need co-partitioning plans to look up state or build processing flows.

A supported Whitewater Riverbed starts with **a minimum of three Nodes**, including in standard development. Placement and replication are handled inside that Riverbed: each internal active range has at least three replicas on distinct Nodes, with normal durable writes acknowledged by a quorum. These are platform responsibilities, not topology settings repeated in every SDK or service.

Partition-free does not mean storage has no internal divisions. Whitewater uses internal ranges so placement can evolve without changing the Feed/Key/Cursor API. Automated movement and scaling remain subject to their implementation and verification gates; three Nodes are the minimum topology, not a promise of unlimited capacity. Adding Nodes also cannot parallelize one strictly ordered hot Key. Production replicas need separate failure domains — three containers on one laptop are a development topology, not protection against losing that laptop.

**The same simple interface in every language, with distributed storage behind it — not distributed-storage administration in your application.**

## Streaming functions, not just transport wrappers

The common model covers the work around a record, not only its HTTP request:

- **Writers:** epoch-fenced sessions, single/batch appends, and stable request identities for deduplication. Retrying an ambiguous append must not silently become a new logical record.
- **Readers:** open, fetch, acknowledge, seek, and close, with delivered work distinguished from durably acknowledged progress. Independent Readers do not silently share progress.
- **Subscriptions:** a durable name for cooperating members, with join, claim, renew, fetch, acknowledge, and release operations under fenced leases.
- **Feed helpers:** one-shot history loading and temporary reads for inspection or live tails, without forcing every exploratory read into a durable application Subscription.
- **Managed consumption:** a simpler consume loop that handles session lifecycle around application processing. A managed Reader helper is not automatically a shared Subscription; the distinction remains explicit.
- **Pipes and stateful processing:** a common platform model for input, transformation, state lookup, output, and failure handling. The goal is equal access to those capabilities, not six independently implemented engines with subtly different behavior.

Whitewater keeps placement inside the Riverbed. Applications address Feeds, Keys, Cursors, Subscriptions, and StateStores — not physical partitions, local store directories, or co-partitioning plans.

For example, the same enrichment flow should be expressible in every language:

```text
Read an activity event
    -> look up its user in accounts.users
    -> apply the business transformation
    -> write the enriched event
    -> commit the Whitewater output effect and input progress together
```

The processing boundary matters: a simple callback followed by acknowledgement is not automatically an atomic effect. External database writes, payments, or emails are not part of a Whitewater transaction. A missing or lagging state lookup must follow an explicit policy rather than silently producing an incomplete result.

## StateStores with multiple keys, built in

A useful state store needs more than one way to find a row. An employee might be identified by an internal ID, looked up uniquely by passport, and grouped by department.

Whitewater's SDK model includes a primary key plus **up to ten declared named secondary keys**, with unique and non-unique lookups. The server-side store owns the data; the SDK is a facade, not another local database to restore or synchronize. This supports enrichment and current-state queries without asking each application team to maintain its own secondary lookup maps.

The following fragments illustrate existing Java, Python, and TypeScript facade shapes; they assume an initialized client, the relevant imports, and application payload/export functions. They are not standalone programs or proof of cross-language conformance.

```java
var spec = StateStore.named("accounts.employees")
        .addKey("passport", true)        // unique named key
        .addKey("department", false);    // non-unique named key
var store = client.stateStores().define(spec, null);

store.put("emp-1".getBytes(), payload,
        Map.of("passport", "P123", "department", "eng"), null);

var row  = store.get("emp-1".getBytes(), false, null);        // by primary key
var rows = store.getBy("passport", "P123", null, false, null); // by named key
store.iterate(256, r -> export(r));                            // bounded pages
```

```python
store = client.state_stores.define(
    StateStore.named("accounts.employees")
    .add_key("passport", unique=True).add_key("department"))

store.put(b"emp-1", payload, named={"passport": "P123", "department": "eng"})
rows = store.get_by("department", "eng").rows          # every match
```

```ts
const spec = StateStore.named("accounts.employees")
  .addKey("passport", true).addKey("department", false);
const store = await client.stateStores.define(spec);
const rows = await store.getBy("passport", "P123");
```

The shared contract includes freshness evidence, explicit uniqueness behavior, bounded iteration, and retry-safe mutations. Secondary keys are declared at store creation; adding a key to a local builder does not retrofit an existing server-side store. Extra indexes also have storage, replication, and write costs — built in does not mean free.

A StateStore mutation is not automatically atomic with a Feed append. That coordination belongs to the explicit processing-effect contract, and its supported state/output boundary must be verified before claiming transactional enrichment.

## Same behavior when things go wrong

Parity matters most outside the happy path:

- **Opaque Cursors:** every SDK passes them through without parsing, sorting, or fabricating them. A page's delivered progress token need not be the Cursor on its last record.
- **Exact time:** event and ingest times preserve signed 64-bit nanoseconds. Node.js/TypeScript uses `bigint`, with decimal strings on the wire rather than lossy JavaScript numbers.
- **Explicit ambiguity:** a retryable refusal differs from an operation that may already have committed. Retries preserve the original operation identity.
- **Fenced work:** stale sessions and leases are refused rather than allowed to act as current owners.
- **Bounded work:** backpressure, cancellation, and page limits belong in the SDK experience, not a language-specific afterthought.
- **Honest capability reporting:** an unsupported operation must be reported, not silently omitted or replaced with weaker semantics.

## One contract, measured parity

The [versioned wire protocol](../sdk/spec/PROTOCOL.md) and [shared conformance corpus](../sdk/spec/conformance/README.md) define what every SDK must preserve. The release gate is the same scenario corpus against a real three-Node Riverbed, alongside language-specific unit tests. An `unsupported` result blocks a parity claim. The Rust SDK ships the corpus runner itself as `whitewater::conformance::ConformanceRunner`, so an application's own client instance can replay every fixture and report typed `ScenarioReport` outcomes.

That distinction keeps the promise meaningful: a shared method name or a working demo is not enough. Supported operations must agree on results, retries, recovery, and failure behavior. New features need equivalent conformance evidence before they can be sold as available across all six languages.

**Learn Whitewater once. Use it in the language your team already knows.** Simplicity is the common interface — not a reason to give non-Java developers less.

## Further reading

- [Application guides](guides/README.md) and the [SDK support matrix](sdk-support-matrix.md)
- [Whitewater Streams and cross-language technical contract](streams-clients.md)
- [Why Whitewater](why-whitewater.md)
- [Kafka pain points](kafka-pain-points.md)
- [Delivery milestones and evidence](tasks.md)
- Apache Kafka's [Streams DSL](https://kafka.apache.org/43/streams/developer-guide/dsl-api/) and [Processor API](https://kafka.apache.org/43/streams/developer-guide/processor-api/)
