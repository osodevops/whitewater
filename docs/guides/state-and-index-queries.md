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
// Java — Domain-scoped StateStoreSpec + typed facade
var accounts = client.createDomain("accounts");
var spec = StateStoreSpec.named("employees")
        .addUniqueKey("passport")        // unique: one row per value
        .addNonUniqueKey("department");  // non-unique: value -> many rows
var store = accounts.defineStateStore(spec, null);
store.put("emp-1", payload, PassportKeySet.with("P123", "eng"));
var row  = store.get("emp-1");              // by primary key + freshness
var rows = store.getBy("passport", "P123"); // by named key
store.iterate(r -> export(r));              // bounded pages
```

```python
# Python
accounts = client.domain("accounts")
store = accounts.define_state_store(
    StateStore.named("employees")
    .add_unique_key("passport").add_non_unique_key("department"))
store.put("emp-1", payload, PassportKeySet(passport="P123", department="eng"))
rows = store.get_by("department", "eng").rows
profile = store.get_or_default("emp-1", None)
for row in store.iterate(limit=256): ...
store.clear()
```

```ts
// Node.js/TypeScript
const accounts = await client.createDomain("accounts");
const spec = StateStore.named("employees")
  .addUniqueKey("passport").addNonUniqueKey("department");
const store = await accounts.defineStateStore(spec);
await store.put("emp-1", payload, PassportKeySet.with("P123", "eng"));
const rows = await store.getBy("passport", "P123");
```

```go
// Go — Domain-scoped spec + PutKeySet
accounts, _ := client.CreateDomain(ctx, "accounts")
spec, _ := whitewater.NewStateStore("employees")
spec.AddUniqueKey("passport").AddNonUniqueKey("department")
store, _ := accounts.DefineStateStore(ctx, spec, nil)
_, _ = store.PutKeySet(ctx, "emp-1", payload,
    PassportKeySet{}.WithPassport("P123").WithDepartment("eng"), "")
row, _ := store.Get(ctx, key, strict)
page, _ := store.GetBy(ctx, "department", "eng", strict, 0)
_ = store.Delete(ctx, key, "")
_ = store.Page(ctx, limit)
```

```rust
// Rust — Domain-scoped spec + typed key sets
let accounts = client.domain("accounts")?;
let spec = StateStoreSpec::new("employees")?
    .add_unique_key("passport", None)?
    .add_non_unique_key("department", None)?;
let store = accounts.define_state_store(spec, None).await?;
store.put_key_set(mutation_id, key, payload,
                  &PassportKeySet::with("P123", "eng")).await?;
let row = store.lookup(key, strict).await?;
let hits = store.get_by("department", value, strict, limit).await?;
```

```csharp
// .NET — Domain-scoped StateStore.Named + IStoreKeySet
var accounts = await client.GetDomainAsync("accounts");
var spec = StateStore.Named("employees")
    .AddUniqueKey("passport").AddNonUniqueKey("department");
var store = await accounts.DefineStateStoreAsync(spec);
await store.PutAsync(key, payload, PassportKeySet.With("P123", "eng"), mutationId);
var rows = await store.GetByAsync("department", "eng");
```

Named-key postings accept whichever call-site form reads cleanest — a
typed `StoreKeySet`/`AbstractStoreKeySet` subclass (fluent, static, or
lambda-built), a raw map/dict/record, or a single string for stores with
one declared named key. All forms lower to the same wire shape.

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
