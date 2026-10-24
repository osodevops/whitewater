# Late-event policy

Window Pipes face the question every streaming engine must answer
honestly: what happens to an event that arrives after its pane should
have closed? Whitewater makes the policy an explicit part of the
declaration — two alternative lateness models, one per Pipe, validated
client-side and enforced server-side.

## The two models (declare exactly one)

**`max_late_ns` — per-record lateness bound.** An event arriving more
than `max_late_ns` behind its own `event_time_ns` (measured against the
Pipe's ingest frontier) does not fold into a pane; it quarantines into a
countable `late` row in the output store so late volume is observable
rather than silent. Simple, stateless per record.

**`watermark_ns` — a durable event-time horizon.** The Pipe tracks
`max(event_time) − watermark_ns` as a monotone, durable watermark. A
pane closes when the horizon passes its end — close decisions follow
event-time disorder, not ingest-clock skew or wall time. Watermarks
survive Node restart because they live in the Pipe's replicated state,
not a driver's memory.

Both are refused under `time: INGEST` — an ingest-ordered stream has no
event-time disorder to bound. `max_late_ns` and `watermark_ns` together
are refused: one lateness model per Pipe.

## `grace_ns` — revision after emission (requires `watermark_ns`)

Emitting a pane is not the end of its story. With `grace_ns`, an event
landing on an already-emitted pane while the horizon stays within
`pane_end + grace_ns` **revises** the pane: the accumulator re-folds the
event and re-emits the pane under a bumped `revision`. Past the grace
bound the event counts into the late row instead.

Without `grace_ns`, any event landing on an emitted pane quarantines —
emission is final. Emitted records carry `pane` + `revision`; downstream
readers deduplicate on `(group, pane)` choosing the highest revision.

## `expire_ns` — bounded pane retention

`expire_ns` deletes a pane row once `pane_end + expire_ns` is behind the
close clock. It must exceed `max_late_ns` or `grace_ns` when declared
with them — a pane must provably close before it may expire. Without it,
pane rows retain indefinitely.

## Validation matrix (enforced by every SDK + the server)

| Combination | Result |
|---|---|
| `window_ns <= 0` | refused — panes need positive width |
| `max_late_ns` + `watermark_ns` | refused — alternative models |
| `watermark_ns`/`max_late_ns` under ingest clock | refused — no event-time disorder |
| `grace_ns` without `watermark_ns` | refused — revision rides the horizon |
| `expire_ns <= max_late_ns`/`grace_ns` | refused — expiry needs provable close |
| `emit_feed` without a lateness bound | refused — emission needs a close rule |

## Evidence

- `window-pipe` conformance fixture exercises the declaration surface;
  `{{now_ns}}` fixtures keep event-clock records inside lateness bounds.
- `scripts/test-window-pipe.py` + `test-window-pipe-4n.py` drive live
  watermark close, grace revision (`revision` bumps with re-emitted
  payload), too-late quarantine, and three-Node hard-restart recovery.
