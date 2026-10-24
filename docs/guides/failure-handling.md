# Failure handling

Whitewater's contract is not "failures don't happen" — it is that every
failure has a defined, identical behavior in all six SDKs. Three rules
cover most of it:

1. **Keep the request identity.** Every mutating call carries a stable
   `request_id`/`mutation_id`. Retrying a timed-out or ambiguous append
   with the same identity returns the original `message_id`/`cursor`
   with `deduplicated: true` — a retry never creates a second logical
   record. **Never mint a fresh identity for a retry of the same logical
   operation.**
2. **Read the error kind, not the message.** Every SDK maps the wire
   `(status, code)` pair to a typed error so `retryable()`/kind checks
   are identical across languages.
3. **Ack after processing.** Ack commits durable progress; anything
   unacked redelivers. Process-then-ack survives a crash;
   ack-then-process loses the page.

## The taxonomy

| Wire code | Typed surface | Meaning | Retry shape |
|---|---|---|---|
| `refused` | `Refused` / `KindRefused` / `.Refused` | the request is permanently invalid | fix the request; do not retry |
| `conflict` | `Conflict` / `KindConflict` | identity reuse with different payload, or a frontier CAS lost | reconcile, then retry as a new operation |
| `ambiguous` | `Ambiguous`/`AmbiguousResult`/`KindAmbiguous` | the operation may already have committed | retry **the same request identity** to learn the outcome |
| `unavailable` | `Unavailable` / `KindUnavailable` | transient (election, quorum stall, restart gap) | retry with backoff |
| `stale_epoch` | `StaleEpoch` | a newer session superseded this epoch/lease | reopen — do not keep writing |
| `sequence` | `Sequence` | Writer sequence gap on retry | resync the sequence; the server fences skips |
| `lease_*` | `LeaseBusy`/`LeaseStale`/`LeaseCapacity` | member work-lease fencing | re-claim under the current epoch |
| `index_behind` | `IndexBehind` | a strict Index read is behind the applied frontier | retryable — wait for the posting |
| `incompatible` | `IncompatibleServer` | protocol-major mismatch on `/health` | upgrade; the SDK fails explicitly |
| transport | `Transport` | no status line arrived | retryable — nothing committed observably |

Rust exposes these as `SdkError` variants with `retryable()`; Go as
`*whitewater.Error` with `hasKind(err, whitewater.KindAmbiguous)`;
Python/Node as `WhitewaterError` subclasses
(`Refused`/`Conflict`/`Unavailable`/`AmbiguousResult`); Java/.NET as
`WhitewaterException` subtypes. The mapping lives in each SDK's error
module, keyed to `sdk/spec/PROTOCOL.md` `(status, code)` pairs.

## Worked retry

```python
result = writer.append(request_id, key, payload)      # keep request_id
# on timeout/5xx/connection drop:
result = writer.append(request_id, key, payload)      # same identity
# → same message_id/cursor, deduplicated=True — never a duplicate record
```

```go
_, err := writer.Append(ctx, req)        // req carries its request_id
if hasKind(err, whitewater.KindAmbiguous) {
    _, err = writer.Append(ctx, req)     // same identity, learn the outcome
}
if hasKind(err, whitewater.KindRefused) {
    // permanent — fix the request, don't loop
}
```

## Fencing — not just retrying

Sessions and leases carry epochs. When another client opens the same
Reader/Writer or claims the same member work, the older holder's next
call fails `stale_epoch`/`lease.stale_lease` — the fenced party stops
acting, it does not keep racing. The supervised consumers in every SDK
already implement this: on a fencing error they reopen or hand off
rather than looping against the fence.

## Outages

The `zz-chaos-*` fixtures define the outage contract every SDK passes:

- **Node restart** mid-scenario: appends continue through the surviving
  quorum; in-flight `unavailable` retries land after the Node returns.
- **Owner loss**: writes re-route to the new owner without client
  topology changes (there is no owner to configure).
- **Two-Node outage**: writes fail closed — a lone survivor cannot
  commit without a voter majority; reads degrade honestly rather than
  serving uncommitted state.
- **Network partition**: the partitioned Node stops answering; the
  majority side continues.

## Evidence

`ambiguous-commit`, `auth-refused`, `writer-sequence-fencing`,
`subscription-member-lease` (stale-holder fencing), and all four
`zz-chaos-*` fixtures — green on all six runners through
`scripts/test-sdk-languages.py`.
