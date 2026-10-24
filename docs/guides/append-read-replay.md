# Append, read, and replay

Every Writer session is epoch-fenced; every append carries a stable
`request_id` that makes a retry safe — an identical retry returns the
original `message_id`/`cursor` with `deduplicated: true` instead of a
second record. A Reader session opens against a declared Reader, fetches
committed pages, and acknowledges the page's **delivered cursor** — an
opaque progress token covering every range the fetch drew from, not the
cursor on its last record. Unacknowledged work redelivers; acknowledged
work does not.

Set up once per environment (WCL, `wwctl`, or the typed helpers — the SDK
`Feed`/`writer`/`reader` facades below declare resources when missing):

```text
CREATE DOMAIN demo;
CREATE FEED demo.orders;
```

## Rust

```rust
let mut writer = client.writer("ingest").await?;      // opens epoch-fenced session
let outcome = writer
    .append(request_id, b"order-1", &payload, event_time_ns, metadata)
    .await?;                                          // keep request_id across retries
let mut reader = client.reader("audit", 64).await?;
let page = reader.fetch(50).await?;
// …process page.records…
reader.ack().await?;                                  // commits the page's delivered cursor
reader.close().await?;
```

Evidence: `tests/sdk_live.rs` legs via `scripts/test-sdk-rust.py`.

## Go

```go
reader := client.Reader("demo-reader")
session, _ := reader.Open(ctx, 64)
defer reader.Close(context.Background(), session)   // release the fence
page, _ := reader.Fetch(ctx, session, 50)
session, _ = reader.Ack(ctx, session, page.DeliveredCursor)
```

or the codec-bound facade: `orders, _ := whitewater.FeedFor[Order](client, "demo.orders")`,
`writer, _ := orders.Writer(ctx, "ingest")`, `writer.Append(ctx, "order-1", order)`.

Evidence: `examples/readfeed`, `examples/typedfeed`, `examples/tailfeed`.

## Python

```python
orders = client.feed("demo.orders", value_type=Order)
with orders.writer("ingest") as writer:
    writer.append("order-1", Order(id="order-1", total=42))
with orders.reader("demo-reader", capacity=64) as reader:
    page = reader.fetch(50)
    reader.ack()                                    # commits the page's delivered cursor
```

Evidence: `examples/read_feed.py`, `examples/tail_feed.py`.

## Java

```java
Feed<String, Order> orders = client.feed("demo.orders", Order.class);
FeedWriter<String, Order> writer = orders.writer("orders-writer");
var committed = writer.append("order-1", new Order("order-1", 42));
// explicit identity for a write you may retry across restarts:
writer.append(new FeedEvent<>(requestId, "order-2", new Order("order-2", 17)));
try (FeedReader<String, Order> reader = orders.reader("demo-reader", 64)) {
    for (var page = reader.fetch(); !page.records().isEmpty(); page = reader.fetch()) {
        // …process…
        reader.ack();                             // commits the page's frontier
    }
}
```

Evidence: `examples/WriteFeed.java`, `examples/ReadFeed.java`,
`examples/TailFeed.java`.

## Node.js/TypeScript

```ts
const reader = client.reader("demo-reader");
let session = await reader.open(64);
try {
  const page = await reader.fetch(session, 50);
  // …process page.records — ingestTimeNs is a bigint, never a number…
  if (page.deliveredCursor !== null) {
    session = await reader.ack(session, page.deliveredCursor);
  }
} finally {
  await reader.close(session);                    // release the session fence
}
```

Evidence: `examples/read-feed.mjs`, `examples/typed-feed.mjs`,
`examples/tail-feed.mjs`.

## C#/.NET

```csharp
var reader = client.Reader("demo-reader");
var session = await reader.OpenAsync(64);          // fences this client as owner
var page = await reader.FetchAsync(session, 50);
session = await reader.AckAsync(session, page.DeliveredCursor);
await reader.CloseAsync(session);
```

Evidence: `examples/ReadFeed`, `examples/TypedFeed`, `examples/TailFeed`.

## Replay

Three surfaces read history without touching a durable Reader's
acknowledged progress:

- `feed.records(...)` — public Feed history pages with opaque `after`
  continuation.
- `temporary_fetch` / `TemporaryFetchAsync` / `temporaryFetch` — stateless
  one-shot reads; the `tail`/`waitMs` option gives a live-tail page.
- A named Reader declared `AT BEGINNING` replays from durable progress on
  every `open`.

A Reader is not a long-poll: `fetch` returns what is committed now. Pace
empty pages yourself, or use `temporary_fetch(tail, wait_ms)`.

## Continuous consumption

Every SDK ships a supervised consumer — one idiomatic loop that owns the
session lifecycle, acks the delivered page only after the handler
returns, and leaves unacked work for redelivery on crash:

```go
for msg := range orders.Consumer("audit").Messages(ctx) { … }
```
```python
for msg in feed.consumer("audit"): …
```
```java
feed.consumer("audit").onRecord(msg -> …).run();
```
```ts
for await (const msg of feed.consumer("audit")) { … }
```
```csharp
await foreach (var msg in feed.ConsumeAsync("audit", ct)) { … }
```
```rust
while let Some(msg) = consumer.next().await? { … }
```

## Evidence

`writer-idempotent-append`, `writer-sequence-fencing`,
`reader-fetch-ack-resume`, `nanoseconds-and-keys`,
`optional-compression`, `ambiguous-commit` conformance fixtures —
22/22 green on every SDK through `scripts/test-sdk-languages.py`.
