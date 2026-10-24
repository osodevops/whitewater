# State and index queries

A **StateStore** is a named, Domain-scoped replicated store: primary-Key
point reads, up to ten declared named secondary keys (unique or
non-unique), bounded iteration, mutation-id deduplication, and freshness
evidence on every read. There is no local store directory in the
application — the SDK is a facade over server RF3 state.

A declared **Index** is a replicated secondary structure over Feed
history: `DEFINE INDEX`, `REBUILD INDEX`, then exact / prefix / range
queries against activated postings, with `applied_cursor`/`served_by`
evidence and an explicit `index_behind` error instead of stale reads.

## StateStore

```java
// Java — dev.whitewater.StateStore facade
var spec = StateStore.named("accounts.employees")
        .addKey("passport", true)        // unique named key
        .addKey("department", false);    // non-unique named key
var store = client.stateStores().define(spec, null);
store.put("emp-1".getBytes(), payload,
        Map.of("passport", "P123", "department", "eng"), null);
var row  = store.get("emp-1".getBytes(), false, null);
var rows = store.getBy("passport", "P123", null, false, null);
store.iterate(256, r -> export(r));
```

```python
# Python
store = client.state_stores.define(
    StateStore.named("accounts.employees")
    .add_key("passport", unique=True).add_key("department"))
store.put(b"emp-1", payload, named={"passport": "P123", "department": "eng"})
rows = store.get_by("department", "eng").rows
profile = store.get_or_default(b"emp-1", None)
for row in store.iterate(limit=256): ...
store.clear()
```

```ts
// Node.js/TypeScript
const spec = StateStore.named("accounts.employees")
  .addKey("passport", true).addKey("department", false);
const store = await client.stateStores.define(spec);
const rows = await store.getBy("passport", "P123");
```

```go
// Go — store.go
store, _ := client.StateStore("accounts.employees")
store.AddKey("passport", true).AddKey("department", false)
_ = store.Put(ctx, key, payload, named, mutationID)
row, _ := store.Get(ctx, key, strict)
page, _ := store.GetBy(ctx, "department", "eng", strict)
_ = store.Delete(ctx, key, mutationID)
_ = store.Page(ctx, limit)
```

```rust
// Rust — client.state_store(name)
let store = client.state_store("accounts.employees");
store.put_row(mutation_id, key, payload, named).await?;
let row = store.lookup(key, strict).await?;
let hits = store.get_by("department", value, strict, limit).await?;
let page = store.iterate(limit).await?;
store.clear(mutation_id).await?;
```

```csharp
// .NET — client.StateStore(name) facade
var store = client.StateStore("accounts.employees");
await store.PutAsync(key, payload, named, mutationId);
var rows = await store.GetByAsync("department", "eng");
```

Value-level helpers exist beside row-level evidence in every SDK
(`find`/`findBy`/`getOrDefault`/`Optional`/`Option` per idiom). Mutations
deduplicate on `mutation_id`; reads report `store_seq`/freshness so a
caller can distinguish "absent" from "behind" rather than treating a lag
window as a miss.

## Declared Indexes

Indexes are declared and rebuilt through typed commands or WCL, then
queried:

```text
DEFINE INDEX by_amount ON demo.orders EXTRACT JSON 'amount';
REBUILD INDEX by_amount;
GET INDEX by_amount EQ B64 '...';
```

```python
page = client.query_index("by_amount",
    IndexQuery.exact(["100"]), strict=True, limit=64)
```
```go
page, _ := client.QueryIndex(ctx, "by_amount",
    whitewater.IndexExact("100"), 64, true)
```
```ts
const page = await client.queryIndex("by_amount",
  { mode: "exact", values: ["100"] }, { strict: true, limit: 64 });
```
```java
var page = client.queryIndex("by_amount",
    IndexPredicate.exact("100"), 64, true, null);
```
```rust
let page = client.query_index("by_amount",
    IndexQuery::Exact(vec![value]), Some(64), true).await?;
```
```csharp
var page = await client.QueryIndexAsync("by_amount",
    IndexPredicate.Exact("100"), 64, true);
```

Prefix queries pass leading terms plus a partial text bound; range
queries take `InclusiveBound`/`ExclusiveBound` pairs per SDK. `strict`
asks for corroborated freshness; a lagging posting surfaces
`index_behind` (retryable) rather than an empty page.

## Evidence

- `statestore-manual-lifecycle` — put / dedupe-retry / lookup / get_by /
  iterate / delete / not-found / clear.
- `index-query` — define, rebuild, shadow-generation activation, exact
  postings with `applied_cursor` evidence, absent-key emptiness.
- `enrichusers` / `WorkWithStores` / `enrich-users` examples per SDK.
