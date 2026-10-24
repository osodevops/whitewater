# Testing your application

Each SDK ships a **unit-test double**: an in-memory model of the wire
contract served over loopback HTTP (or behind the Rust `Transport`
seam), so application unit tests exercise the real client code path —
serialization, request identity, error mapping, ack gating — without a
live Riverbed.

The doubles model **wire semantics**, not canned scripts: appends commit
ordered records with opaque cursors and uuid `message_id`s; the same
`request_id` deduplicates to the original receipt; a superseded Writer
epoch fences as `stale_epoch`; a fetched page must be acked before the
next fetch; closing a session without acking redelivers; StateStore rows
mutate/lookup/get_by/iterate/clear with `mutation_id` dedup; declarations
through WCL/typed commands behave (duplicate `CREATE` refuses
"already exists" so declare-if-missing helpers work); unknown routes
surface the SDK-decodable 404.

## Java — `dev.whitewater.testing.FakeRiverbed`

```java
try (var riverbed = FakeRiverbed.start();
     var client = riverbed.client()) {          // connected WhitewaterClient
    var feed = client.feed("demo.orders", Order.class);
    var writer = feed.writer("ingest");
    writer.append("order-1", new Order("order-1", 42));
    assertEquals(1, riverbed.feedSize("demo.orders"));
}
```

`riverbed.endpoints()` feeds `WhitewaterClient.builder().endpoints(...)`;
inspection helpers (`feedSize`, `storeSize`, `writerEpoch`,
`readerProgress`, `requests`) expose the fake's state for assertions.

## Go — `testkit` package

```go
fake := testkit.NewRiverbed(t)              // loopback Node, t.Cleanup'd
defer fake.Close()
client := fake.Client()                     // *whitewater.Client
// or: whitewater.NewClient([]string{fake.Endpoints()}, apiKey, timeout)
```

`testkit/testkit.go` + `control.go`/`feeds.go`/`stores.go` model the
catalog and data planes; `testkit_test.go` exercises the public API.

## Node.js/TypeScript — `createFakeRiverbed`

```ts
import { createFakeRiverbed } from "whitewater-client";
const riverbed = await createFakeRiverbed();       // node:http loopback
const client = new WhitewaterClient({
  endpoints: riverbed.endpoints, apiKey: "test" });
// …
await riverbed.close();
```

## C#/.NET — `Whitewater.Testing.FakeRiverbed`

```csharp
await using var fake = new FakeRiverbed();
var client = new WhitewaterClient(apiKey, fake.Endpoints);
// fake.Requests lists served wire traffic for assertions
```

## Python — `whitewater.testing.FakeRiverbed`

```python
from whitewater.testing import FakeRiverbed
with FakeRiverbed() as fake:
    client = WhitewaterClient(api_key="test", endpoints=fake.endpoints)
    # …
```

## Rust — `whitewater::testing`

```rust
let client = whitewater::testing::FakeTransport::client();   // one call
// or bind the fake yourself: Client over FakeTransport::new()
// — in-memory model behind the Transport seam: no HTTP, no ports
```

## What the doubles deliberately do NOT model

Single-Node semantics only — no quorum, no persistence, no real
network. Membership leases, Index query, demand diagnostics, and Pipe
*drive* are outside the doubles' scope: declaration commands succeed so
data-plane calls work, but a Pipe does not move records inside the fake.
Use the live acceptance lanes (and the `zz-chaos-*` fixtures) for
durability/fault behavior — a double proves application logic, not
Riverbed guarantees.

## Evidence

Each SDK's `testing` unit suite runs through the public client API:
redelivery-after-unacked-close, request-id dedup, stale-epoch fencing,
store round-trips — see `testkit_test.go`,
`FakeRiverbedTest.java`, `testing-double.test.ts`,
`FakeRiverbedTests.cs`, `test_testing_double.py`, and
`testing::tests` in `src/testing.rs`.
