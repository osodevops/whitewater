# Whitewater UI

> Explore, test, visualize, and manage your streaming data — a data-centric desktop workspace built on the excellent Kafka Streamyard, for Whitewater's partition-free Rust streaming database.

## Document status

- **Status:** Product vision and sales overview. Each area is marked **Live** (exists in the current codebase), **In progress**, or **Planned**. Planned items describe the committed direction, not shipped guarantees.
- **Product:** Whitewater UI — an Electron + React desktop application backed by a Spring Boot API, bringing data exploration, testing, stream development, and management into one workspace.
- **Lineage:** Based on [Kafka Streamyard](https://www.kafka-streamyard.com), the excellent data-centric Kafka desktop UI for developers, testers, and support engineers. Whitewater UI carries its workflow DNA forward onto the Whitewater platform.
- **Companion documents:** [Why Whitewater](why-whitewater.md), [Whitewater SDKs: choose your language, not your capabilities](whitewater-sdks.md), [Kafka pain points](kafka-pain-points.md), [Whitewater architecture](kafka-successor-architecture.md)

## Why a Streamyard heritage matters

Kafka Streamyard earned its following by being **data-centric** rather than broker-centric: it answers "what is in my data and what is it doing" instead of "what are my brokers doing." Live multi-topic Views, powerful Streamyard Language (SL) filters with schema-aware autocomplete and AI assist, reusable test-data samples, Playbooks for repeatable environment setup, and result views you can export as evidence — all in a fast desktop app that keeps your working set intact while you move around.

Whitewater UI keeps that workflow — and points it at a platform that removes the infrastructure the workflow used to have to step around. No partitions to select. No offsets to record. No consumer groups to rebalance. The data-centric experience finally gets a data-centric backend.

## The platform underneath: a streaming database, not a broker

Whitewater is designed **from the outset as both a streaming platform and a distributed database** — not a message broker with state bolted on, and not a database with a change feed attached:

```text
Feed         = immutable temporal history, replayable forever
Index        = persisted replicated current/queryable state
StateStore   = durable server-side key state with named secondary keys
Subscription = continuous capacity-aware delivery with shared progress
Pipe         = managed streaming computation with atomic effects
Cursor       = stable opaque position in temporal history
```

- **Partition-free by contract.** The entire public model is `riverbed -> domain -> feed -> key -> cursor -> subscription`. There is no partition count to choose, no offset to store, no assignment to reason about. Keys define ordering; internal ranges may split, merge, and move without the client contract ever changing.
- **Auto-scalable.** Nodes advertise capacity and pressure; the Riverbed splits, merges, and relocates internal ranges and compute leases incrementally — one node at a time, with hysteresis and drain gates — instead of demanding a reassignment project. Scale-in is refused until a Node has safely drained.
- **Business data first.** Navigate Domains and Feeds, follow an order or customer, and inspect the state behind an event without choosing a physical partition or managing placement.
- **All the performance of Rust.** One native Node binary with no GC pauses, predictable tail latency, low baseline memory, and fine-grained control over IO, batching, and backpressure. Rust does not make claims for us — every performance statement is backed by reproducible benchmarks — but the runtime removes entire categories of JVM overhead and operational tuning.
- **Safe defaults, not a correctness state space.** Minimum three Nodes, three active replicas, quorum writes, TLS everywhere, scoped API keys, one-way credential verifiers. Durability is protocol behavior, not the right combination of forty settings.

## What Whitewater UI will do

The workspace inherits Streamyard's data-centric feature set — samples, playback, filters, Views, Playbooks — and adds what only makes sense on Whitewater: visual stream development with real code behinds, and a full management suite built on the Domain/Feed access model.

### Test data samples and play features — **Live lineage**

- Curate reusable sample records — key, payload, Metadata — organized and tagged per connection and Feed.
- `@@` template tokens and session variables: `@@guid`, `@@rnd`, `@@now`, composite business-data generators (people, orders, invoices, devices, payments, shipments…) plus `@@s.*` session vars that stay stable across a workflow or regenerate per send.
- Sample loader tasks produce sequences with configurable quantity, randomized delays, and burst sizes — realistic traffic for soak tests and demos without writing producer scripts.
- Play any sample straight into a Feed, or turn a playback into a live tail with one click.

### Playbooks — **Live lineage**

Line-based, repeatable environment scripts — now operating on Domains, Feeds, StateStores, and Subscriptions instead of topics:

- Create, clear, copy, and populate resources with variables, loops, and patterns.
- Validation and **Dry Run** preview before anything touches a Riverbed; a visual wizard generates scripts from selected resources.
- Ideal for rebuilding a dev or test slice into a known state, or applying one controlled operation across a group of Feeds.
- AI assist drafts and explains playbook steps.

### Streamyard Language filters — autocomplete and AI assist — **Live lineage**

All the power of the Streamyard Language (SL) filter engine, carried onto Whitewater records:

- The same criteria language across record payload, key, Metadata, and system fields — deeply nested JSON and namespaced XML paths, collection predicates, numeric comparisons, `m.toString()`-style scan expressions for "search anywhere" work.
- Colorized editing with **schema- and shape-aware autocomplete** that learns record fields as data flows through.
- **AI assist** generates and repairs filters from natural language, using the same OpenAI-compatible and Anthropic provider support as Streamyard.
- Saved filters and valid history travel with your workspace.

### Views — comprehensive session views — **Live lineage / In progress on Whitewater**

Named, multi-Feed workspaces for testing, visualization, demos, and investigations:

- Follow many Feeds as one chronological timeline — historical playback or live tail.
- Per-Feed SL filters plus a **Session filter** that traces one customer, order, or correlation ID across the whole flow.
- Activity-aware **Flow Charts** place Feeds alongside services and annotations; live activity pulses through the relevant nodes.
- Customizable result columns — pick the Metadata, key, and payload fields that matter; add calculated columns; export the exact view as a CSV report or JSON/NDJSON for evidence and incident reviews.
- Local-first persistence: leave an investigation and return to the same working set.

### Visualized stream development — way beyond ksqlDB — **In progress**

Where ksqlDB gives you SQL over topics, Whitewater UI aims to give you **visual stream applications over Feeds with real Rust code behinds**:

- Compose Pipes visually — source Feeds, StateStore enrichment, windowed aggregation, filters, branches, sinks — then drop into the generated Rust code behind each stage when SQL-expressiveness runs out. The code behind is first-class, versioned, and testable, not an escape hatch.
- **AI assist** scaffolds stages, suggests transformations, and explains what a Pipe will do before it runs.
- **Built-in testing** *(in progress)*: replay recorded samples or generated data through a Pipe in the workspace, assert on outputs, and keep the tests beside the code.
- **Deployable apps**: what you build is not a diagram — it compiles to a deployable stream application.
- **GitHub backed**: stream apps, tests, and view definitions live in a GitHub repository — reviewable, diffable, and CI-friendly like the rest of your code.

Because Pipes and StateStores are Riverbed capabilities — not per-app local state — the visual design maps onto managed processing with atomic input-progress/output-effect commits rather than client-side RocksDB stores and changelog topics.

### Full management suite — **Planned**

Streamyard's management surface is intentionally data-centric — topics, consumers, schemas, connect. Whitewater UI's management suite diverges onto Whitewater's own model and goes further:

- **Metrics for Whitewater**: a first-class metrics feature — Riverbed and per-Node health, Feed throughput and lag, Subscription delivery, Pipe and Index cost — with the attribution the platform already keeps per Domain. Move from symptom to cause without leaving the workspace.
- **API key creation, all in the UI**: mint, scope, rotate, and revoke API keys with overlapping-validity rotation — stored as one-way verifiers, never plaintext.
- **Domain and Feed access — like ACLs, but more powerful and easier**: grant capabilities at Domain and Feed boundaries instead of maintaining operation-by-resource ACL matrices. Access follows the same model your applications use — read a Feed, write a Feed, administer a Domain — with authorization enforced by the platform rather than only by the UI.
- **AI Assist everywhere**: describe the access an application needs — "a reporting reader that can only consume `commerce.*` Feeds" — and let the assistant draft the keys and grants for review before applying.
- Declarative control underneath: every management action converges on the same typed Control API and WCL used by other management tools — the UI is a client of the platform, not a side channel.

## Where it stands today

| Area | Status |
|---|---|
| Desktop shell (Electron + React, Windows/macOS/Linux) with Spring Boot API | **Live** |
| Riverbed connectivity — browse Domains and Feeds, inspect streaming data | **Live / In progress** |
| SL filter engine, criteria autocomplete, AI provider backends | **Live lineage** (from the Streamyard codebase; Whitewater mapping in progress) |
| Samples, playback, Playbooks, Views, result customization | **Live lineage** (Feed/Domain adaptation in progress) |
| Visual stream development with Rust code behinds, built-in testing, GitHub-backed deployable apps | **In progress** |
| Management suite: metrics, API keys, Domain/Feed access with AI assist | **Planned** |

Whitewater UI is developed alongside the platform it serves in the sibling `whitewater-ui` repository. Features marked **Planned** are committed direction, not shipped guarantees — the same honesty standard as the rest of this documentation set.
