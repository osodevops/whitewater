# Enrichment

A managed **Enrich Pipe** reads input records through a Subscription,
resolves a declared key extractor against a StateStore, and writes the
enriched record — input progress and output commit atomically through
the Pipe's effect journal. A declared miss policy decides what happens
when the lookup is absent or too stale: forward unenriched, or route the
record to a quarantine Feed for review. No consumer loop, offset
management, or local store directory lives in the application.

Set up:

```text
CREATE DOMAIN accounts;
CREATE FEED accounts.events;
CREATE FEED accounts.enriched;
CREATE FEED accounts.misses;                 -- quarantine target
CREATE STATESTORE accounts.users KEYS (user_id);
CREATE SUBSCRIPTION enrich-work FROM FEED accounts.events;
```

## Declare the Pipe

All six SDKs share one surface: `define_enrich_pipe` /
`DefineEnrichPipe(Async)` / `defineEnrichPipe` taking
`PipeOperation.enrich(extractor, freshness, named, on_miss)` — the
extractor names the record field that becomes the store primary key,
`freshness` bounds acceptable state lag (`LATEST` is the default), and
`on_miss` selects forward-vs-quarantine.

```rust
client.define_enrich_pipe("enrich-users", "enrich-work",
    "accounts.enriched", "accounts.users",
    PipeOperation::enrich(Extractor::json("user_id")?,
        EnrichFreshness::Latest, None, EnrichMiss::Quarantine),
    Some("accounts.misses")).await?;
```

```go
_, err := client.DefineEnrichPipe(ctx, "enrich-users", "enrich-work",
    "accounts.enriched", "accounts.users",
    whitewater.EnrichOperation(whitewater.JsonExtractor("user_id"),
        whitewater.EnrichOptions{
            Freshness: whitewater.EnrichFreshnessLatest,
            OnMiss:    whitewater.EnrichMissQuarantine}),
    "accounts.misses")
```

```python
client.define_enrich_pipe("enrich-users", "enrich-work",
    "accounts.enriched", "accounts.users",
    PipeOperation.enrich(Extractor.json("user_id"),
        freshness=EnrichFreshness.LATEST, on_miss=EnrichMiss.QUARANTINE),
    quarantine_feed="accounts.misses")
```

```java
client.commands(Commands.begin()
    .defineEnrichPipe("enrich-users", "enrich-work",
        "accounts.enriched", "accounts.users",
        PipeOperation.enrich(Extractor.json("user_id"),
            EnrichFreshness.LATEST, null, EnrichMiss.QUARANTINE),
        "accounts.misses"));
```

```ts
await client.defineEnrichPipe("enrich-users", "enrich-work",
  "accounts.enriched", "accounts.users",
  PipeOperation.enrich(Extractor.json("user_id"),
    { freshness: EnrichFreshness.LATEST, onMiss: EnrichMiss.QUARANTINE }),
  "accounts.misses");
```

```csharp
await client.DefineEnrichPipeAsync("enrich-users", "enrich-work",
    "accounts.enriched", "accounts.users",
    PipeOperation.Enrich(Extractor.Json("user_id"),
        EnrichFreshness.Latest, null, EnrichMiss.Quarantine),
    "accounts.misses");
```

Each SDK validates the operation kind, the required extractor, and the
quarantine↔miss-policy pairing locally — `on_miss: QUARANTINE` without a
`quarantine_feed` (or a quarantine feed without quarantine policy) is a
client-side refusal before the command reaches the Riverbed.

## What the output looks like

An enriched record lands on `accounts.enriched` carrying the source
payload plus server-set Metadata: `enrich.matched`,
`enrich.payload` (the resolved store row), and `enrich.seq` (the store
version evidence). A miss under `QUARANTINE` lands on `accounts.misses`
with `enrich.matched: false` and `enrich.quarantine`. Writer-provided
Metadata passes through untouched — enrichment never overwrites
application fields.

## Client-side alternative

For interactive or low-volume flows the same state reads work without a
managed Pipe: `store.get`/`get_by`/`lookup` per SDK (see
[state-and-index-queries](state-and-index-queries.md)) — the `enrichusers`
examples per SDK show the consumer-loop form. Use the managed Pipe when
progress/output atomicity and unattended operation matter.

## Evidence

- `enrich-pipe` conformance fixture: stored profile resolves into output
  Metadata; declared miss lands on the quarantine Feed — green on all six
  runners.
- Runnable declarations: `examples/pipes.rs`, `examples/definepipes`,
  `examples/declare_pipes.py`, `examples/WindowPipes.java`,
  `examples/declare-pipes.mjs`, `examples/DeclarePipes`.
