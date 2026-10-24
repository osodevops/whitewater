# Whitewater application guides

Runnable guides for every supported SDK: Rust, Python, Java, C#/.NET,
Node.js/TypeScript, and Go. Every chapter carries an evidence section naming
the conformance fixture, acceptance script, or example program that proves
the flow live — a chapter is not documentation of intent.

All six SDKs implement the same wire contract
([sdk/spec/PROTOCOL.md](../../sdk/spec/PROTOCOL.md)) and pass the same
22-scenario [conformance corpus](../../sdk/spec/conformance/README.md) through
`scripts/test-sdk-languages.py`. Code fragments assume the declared resources
exist — each chapter's "set up" section states what to create via WCL or the
typed command helpers. No fragment asks for a partition count, Active Range,
owner, replica, physical offset, or co-partitioning plan.

| Guide | Covers | Live evidence |
|---|---|---|
| [Append, read, and replay](append-read-replay.md) | Writer sessions, idempotent append, Reader fetch/ack, replay, managed consumer | `writer-idempotent-append`, `reader-fetch-ack-resume`, `nanoseconds-and-keys`, `optional-compression` fixtures; `examples/typedfeed` per SDK |
| [State and index queries](state-and-index-queries.md) | StateStore mutate/lookup/get_by/iterate/clear, declared Index exact/prefix/range | `statestore-manual-lifecycle`, `index-query` fixtures; `enrichusers`/`WorkWithStores` examples |
| [Enrichment](enrichment.md) | Reader → StateStore → Writer Enrich Pipe, freshness, miss policy, quarantine | `enrich-pipe` fixture; `definepipes`/`declare_pipes`/`DeclarePipes`/`WindowPipes`/`declare-pipes.mjs`/`pipes.rs` examples |
| [Rolling windows](rolling-windows.md) | Window Pipe declaration, aggregations, time basis, emitted records | `window-pipe` fixture; the Pipe examples above |
| [Late-event policy](late-event-policy.md) | Watermarks, grace, max_late, expiry, quarantine, revision | `window-pipe` fixture; `scripts/test-window-pipe.py`, `test-window-pipe-4n.py` |
| [Failure handling](failure-handling.md) | Retry identity, error taxonomy, epoch fencing, ambiguous results, outages | `ambiguous-commit`, `auth-refused`, `writer-sequence-fencing`, `subscription-member-lease`, `zz-chaos-*` fixtures |
| [Subscription members](subscription-members.md) | Shared progress, join/claim/fetch/ack/renew/release/transfer/seek, Key windows | `subscription-member-*`, `subscription-seek` fixtures; `test-m7-subscription-member.py`; Java `MemberWork` example |
| [Testing your application](testing-your-app.md) | Per-language shipped in-memory doubles | each SDK's `testing` module unit suite |
| [Migrating from Kafka Streams](migrate-from-kafka-streams.md) | Concept mapping: topics→Feeds, consumer groups→Subscriptions, DSL→Pipes | cross-references the evidence above |
| [Operator diagnostics](operator-diagnostics.md) | SHOW/DESCRIBE/EXPLAIN/INSPECT WCL, /health, demand telemetry, placement | `health-capabilities`, `index-query` fixtures; `test-subscription-member-acceptance.py` placements |

Capability and package status: [sdk-support-matrix.md](../sdk-support-matrix.md).
