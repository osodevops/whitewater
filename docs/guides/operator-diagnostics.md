# Operator diagnostics

Whitewater's operational contract: every decision the platform makes —
placement, fencing, demand, lag — is explainable through the same
control surface applications use. No partition math, no replica spelunking.

## WCL inspection

Every SDK runs WCL through the same `wcl()`/`WclAsync`/`WCL()` call that
`wwctl` uses — the snippets below work identically in any SDK and the
CLI.

```text
SHOW DOMAINS;  SHOW FEEDS;  SHOW WRITERS;  SHOW READERS;
SHOW SUBSCRIPTIONS;  SHOW PIPES;  SHOW INDEXES;  SHOW STATESTORES;
SHOW ROLES;  SHOW GRANTS;  SHOW API KEYS;  SHOW STORAGE;
```

```text
DESCRIBE SUBSCRIPTION order-work;
-- stage, source, member_feeds with active/draining/blocked states,
-- lease counts, acknowledged progress evidence

DESCRIBE PIPE stats.hourly;
-- declared stage/status, subscription, operation document, output

DESCRIBE FEED orders.events;
-- immutable FeedId, name history, retention, append owner evidence
```

```text
EXPLAIN ACCESS FOR KEY '<key-id>' ON orders.events;
-- which grants/denies matched a caller on a namespace, and why

EXPLAIN RENAME orders.events;
-- identity continuity proof across the name change

INSPECT PLACEMENT orders.events;
-- which Nodes hold this Feed's replicas today (operator view —
-- never needed by application code)

INSPECT DRAIN control-3;
-- per-resource drain progress before a Node can report safe-to-remove
```

## Pipe lifecycle for operators

```text
PAUSE PIPE stats.hourly;    -- cooperative halt, durable frontier kept
RESUME PIPE stats.hourly;   -- resumes from the durable frontier, idempotent
DESCRIBE PIPE stats.hourly; -- status evidence after each transition
```

Subscription repositioning: `SEEK SUBSCRIPTION order-work TO BEGINNING`
(or a timestamp/explicit cursor) — refused while member leases are held;
see [subscription-members](subscription-members.md).

## Demand telemetry through the SDKs

The demand endpoints surface per-resource pressure, pacing, and hot Keys
— the same data the platform's scaling recommendations read:

```python
client.demand_feeds(); client.demand_readers()
client.demand_subscriptions(); client.demand_indexes()
client.demand_pipes();   client.hot_keys()
```
```go
client.DemandFeeds(ctx); client.DemandReaders(ctx)
client.DemandSubscriptions(ctx); client.DemandIndexes(ctx)
client.DemandPipes(ctx);           client.HotKeys(ctx)
```
```java
client.demandFeeds(); client.demandReaders(); client.demandSubscriptions();
client.demandIndexes(); client.demandPipes();  client.hotKeys();
```
```ts
await client.demandFeeds();   await client.demandReaders();
await client.demandSubscriptions(); await client.demandIndexes();
await client.demandPipes();     await client.hotKeys();
```
```csharp
await client.DemandFeedsAsync();   await client.DemandReadersAsync();
await client.DemandSubscriptionsAsync(); await client.DemandIndexesAsync();
await client.DemandPipesAsync();     await client.HotKeysAsync();
```
```rust
// Rust reaches the same endpoints through the raw wire escape hatch:
let report = client.wire("GET", "/v1/admin/demand/feeds", None).await?;
let hot = client.wire("GET", "/v1/admin/demand/hot-keys", None).await?;
```

Writer feedback also carries live pressure signals inline
(`recommended_batch_count`/`retry_after_ms`/`hot_key`) so producers
self-throttle without polling diagnostics.

## Health and compatibility

`GET /health` answers liveness; `client.check_compatible()` /
`health()` additionally verifies the protocol major before the SDK
silently degrades — an `IncompatibleServer` error is explicit, not a
missing method at runtime.

## Evidence

- `health-capabilities` fixture — health + capability reporting.
- `index-query` fixture — `applied_cursor`/`served_by` operator evidence
  on reads.
- `scripts/test-subscription-member-acceptance.py` — placement and
  member-state inspection across lane churn.
- Writer feedback assertions inside `writer-idempotent-append`.
