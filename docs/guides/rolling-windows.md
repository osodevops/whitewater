# Rolling windows

A managed **Window Pipe** accumulates each delivered record into fixed
window-wide panes of a managed StateStore view — no sliding-step,
pane, watermark-thread, or local-state plumbing in application code. The
declaration names: the pane width, the business Key extractor, the
aggregation (`count` / `sum` / `average` over an extracted value), and
the record clock (`event` or `ingest`). Optional `emit_feed` emits each
closed pane as a `window.emitted` record carrying exact
`group`/`count`/`sum`/`store_seq` evidence — an emit feed requires an
explicit lateness bound (see [late-event policy](late-event-policy.md)).

Every duration is declared in the SDK's native time type — `Time.h(1)`
in Java and Node, `time.Hour` in Go, `timedelta` in Python,
`TimeSpan` in .NET, `Duration` in Rust — and the SDK extracts
nanoseconds internally for the wire; callers never spell raw nanosecond
counts.

Optional bounds: `key_limit`/`byte_limit` cap pane memory; `expire`
bounds pane retention after provable close; lateness is configured in
[late-event policy](late-event-policy.md) (`max_late`, or
`watermark` + `grace`).

Set up:

```text
CREATE DOMAIN stats;
CREATE FEED stats.measurements;
CREATE FEED stats.closed;                    -- emit feed
CREATE STATESTORE stats.by_customer;
CREATE SUBSCRIPTION windowing FROM FEED stats.measurements;
```

## Declare the Pipe

```rust
let hourly = PipeOperation::window(
    Duration::from_secs(3600),                    // 1h panes
    Extractor::record_key(),                      // group by application Key
    WindowAggregation::count(),
)?
.time(WindowTime::Event)
.watermark(Duration::from_secs(60))
.expire(Duration::from_secs(86_400));
client.define_window_pipe("stats.hourly", "stats.windowing",
    "stats.by_customer", hourly, Some("stats.closed")).await?;
```

```go
_, err := client.DefineWindowPipe(ctx, "stats.hourly", "stats.windowing",
    "stats.by_customer",
    whitewater.WindowOperation(time.Hour,
        whitewater.KeyExtractor(), whitewater.CountAggregation(),
        whitewater.WindowOptions{
            Time:      whitewater.WindowEventTime,
            Watermark: time.Minute,
            Expire:    24 * time.Hour}),
    "stats.closed")
```

```python
client.define_window_pipe("stats.hourly", "stats.windowing",
    "stats.by_customer",
    PipeOperation.window(timedelta(hours=1),
        Extractor.record_key(), WindowAggregation.count(),
        watermark=timedelta(minutes=1), expire=timedelta(days=1)),
    emit_feed="stats.closed")
```

```java
client.commands(Commands.begin()
    .defineWindowPipe("stats.hourly", "stats.windowing",
        "stats.by_customer",
        PipeOperation.window(Time.h(1),
            Extractor.recordKey(), WindowAggregation.count(),
            WindowTime.EVENT, null, Time.m(1), null,
            Time.d(1), null, null),
        "stats.closed"));
```

```ts
await client.defineWindowPipe("stats.hourly", "stats.windowing",
  "stats.by_customer",
  PipeOperation.window(Time.h(1), Extractor.recordKey(),
    WindowAggregation.count(),
    { watermark: Time.m(1), expire: Time.d(1) }),
  "stats.closed");
```

```csharp
await client.DefineWindowPipeAsync("stats.hourly", "stats.windowing",
    "stats.by_customer",
    PipeOperation.Window(TimeSpan.FromHours(1),
        Extractor.RecordKey(), WindowAggregation.Count(),
        WindowTime.Event, watermark: TimeSpan.FromMinutes(1),
        expire: TimeSpan.FromDays(1)),
    "stats.closed");
```

Aggregations: `count()` counts records per (group Key, pane); `sum(x)`
totals an extracted integer; `average(x)` tracks count+sum for readers
to divide. The extractor can be `record_key()`/`RecordKey()`/`KeyExtractor()`,
`metadata(name)`, or `json("dotted.path")` — `SUM JSON 'amount'` style
projections come from the payload.

Every SDK validates locally what the server re-checks authoritatively:
the window must be a positive duration in the SDK's native time type,
the operation kind must match the helper, and the lateness-model rules
below fail before the command ships.

## Reading results

Panes live in the declared `output_store` — query it like any StateStore
(`lookup`/`get_by`/`iterate`) for the current value of a Key's open or
closed panes. When `emit_feed` is set, each closing pane also publishes a
`window.emitted` record with the group, pane bounds, counts/sums, and
revision — read it like any Feed for downstream consumers that should
not poll state.

## Evidence

- `window-pipe` conformance fixture: two event groups fold into panes
  and emit one `window.emitted` each with exact group/count/sum/store_seq
  — green on all six runners.
- Runnable declarations: `examples/pipes.rs`, `examples/definepipes`,
  `examples/declare_pipes.py`, `examples/WindowPipes.java`,
  `examples/declare-pipes.mjs`, `examples/DeclarePipes`.
