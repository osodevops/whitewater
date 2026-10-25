# Late-event policy

Window Pipes face the question every streaming engine must answer
honestly: what happens to an event that arrives after its pane should
have closed? Whitewater makes the policy an explicit part of the
declaration — two alternative lateness models, one per Pipe, validated
client-side and enforced server-side. Every bound is declared in the
SDK's native time type — `Time.m(5)` in Java and Node, `5*time.Minute`
in Go, `timedelta` in Python, `TimeSpan` in .NET, `Duration` in Rust —
and the SDK extracts nanoseconds internally for the wire.

## The two models (declare exactly one)

**`max_late` — per-record lateness bound.** An event arriving more
than `max_late` behind its own `event_time_ns` (measured against the
Pipe's ingest frontier) does not fold into a pane; it quarantines into a
countable `late` row in the output store so late volume is observable
rather than silent. Simple, stateless per record.

**`watermark` — a durable event-time horizon.** The Pipe tracks
`max(event_time) − watermark` as a monotone, durable watermark. A
pane closes when the horizon passes its end — close decisions follow
event-time disorder, not ingest-clock skew or wall time. Watermarks
survive Node restart because they live in the Pipe's replicated state,
not a driver's memory.

Both are refused under `time: INGEST` — an ingest-ordered stream has no
event-time disorder to bound. `max_late` and `watermark` together
are refused: one lateness model per Pipe.

## `grace` — revision after emission (requires `watermark`)

Emitting a pane is not the end of its story. With `grace`, an event
landing on an already-emitted pane while the horizon stays within
`pane_end + grace` **revises** the pane: the accumulator re-folds the
event and re-emits the pane under a bumped `revision`. Past the grace
bound the event counts into the late row instead.

Without `grace`, any event landing on an emitted pane quarantines —
emission is final. Emitted records carry `pane` + `revision`; downstream
readers deduplicate on `(group, pane)` choosing the highest revision.

## `expire` — bounded pane retention

`expire` deletes a pane row once `pane_end + expire` is behind the
close clock. It must exceed `max_late` or `grace` when declared
with them — a pane must provably close before it may expire. Without it,
pane rows retain indefinitely.

## Validation matrix (enforced by every SDK + the server)

| Combination | Result |
|---|---|
| non-positive window | refused — panes need positive width |
| `max_late` + `watermark` | refused — alternative models |
| `watermark`/`max_late` under ingest clock | refused — no event-time disorder |
| `grace` without `watermark` | refused — revision rides the horizon |
| `expire` <= `max_late`/`grace` | refused — expiry needs provable close |
| `emit_feed` without a lateness bound | refused — emission needs a close rule |

## Evidence

- `window-pipe` conformance fixture exercises the declaration surface;
  `{{now_ns}}` fixtures keep event-clock records inside lateness bounds.
- `scripts/test-window-pipe.py` + `test-window-pipe-4n.py` drive live
  watermark close, grace revision (`revision` bumps with re-emitted
  payload), too-late quarantine, and three-Node hard-restart recovery.
