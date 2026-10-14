# Whitewater wire protocol v1

**Status:** v1 draft. This document is the semantic contract every SDK must
implement. `endpoints.json` is the machine-readable route/schema table;
`conformance/` holds the named scenarios each SDK executes against a real
three-Node Riverbed before claiming parity.

The public model is `Riverbed -> Domain -> Feed -> Key -> Cursor ->
Subscription`. Nothing in this protocol exposes partitions, Active Ranges,
owners, replicas, or physical positions; an SDK that surfaces those internals
is non-conformant even if every call succeeds.

## Transport

- HTTP/1.1, `application/json` bodies. All requests carry
  `authorization: Bearer <api-key>`.
- `POST` for every operation except `GET /health`,
  `GET /v1/feeds/records`, `GET /v1/node/metrics`,
  `GET /v1/cluster/members`, `GET /v1/subscriptions/members/state`, and the
  admin diagnostics.
- An SDK may target **any** Node for any request; routing to owners is a
  server implementation detail. Clients SHOULD spread requests across known
  Node endpoints and retry on a different endpoint after connect errors.

## Scalar encodings

| Concept | Wire form | SDK requirement |
|---|---|---|
| `request_id`, `message_id`, `writer_id`, `feed_id`, `reader_id` | canonical UUID string | native UUID type where the language has one |
| `event_time_ns`, `ingest_time_ns`, `after_event_time_ns` | decimal **string** containing a signed 64-bit integer | `long`/`int64`/`bigint` — never a float; Node.js must use `bigint` |
| `sequence`, `session_epoch`, `limit`, `capacity` | JSON number | unsigned-safe integer |
| `key_base64`, `payload_base64`, `metadata_base64` values | standard base64 | native byte-array type |
| `cursor`, `delivered_cursor`, `acknowledged_cursor`, `next_cursor` | opaque string, possibly `null` | pass-through value; never parse, decompose, or infer ordering |
| `metadata_base64` | object mapping UTF-8 names to base64 byte values | `Map<String, bytes>` — record Metadata, never called "headers" |

Names (Domain, Feed, Writer, Reader, Subscription) match
`^[a-z][a-z0-9]*(\.[a-z][a-z0-9]*)*$`, maximum 512 bytes.

## Optional record compression

Writer and ad-hoc Feed append requests accept `use_compression: true`. SDK Feed
and Writer clients expose a fluent `useCompression()` / `UseCompression()` /
`use_compression(true)` option (Go takes an explicit boolean). The option returns
a configured client; retain that client to use the setting. Compression is off
by default and can be disabled with `false` at any time, including on a Feed
that already contains records. Batch Writers apply the option to each record.

The Node uses fast pure-Rust LZ4 for stored and replicated record frames. Records
below 1 KiB or whose compressed envelope is not smaller retain their original
format. SDK HTTP bodies and application payload bytes are unchanged. This is a
per-append preference, not a persisted Feed policy; it never rewrites history.
Readers transparently decode mixed legacy, uncompressed and compressed frames.
Checksums are verified before decoding and decompressed size is bounded by the
existing 64 MiB record-body limit. Corrupt or unsupported frames fail closed.

Upgrade all Nodes before opting in: compressed frames use the internal `FSR3`
format and old binaries cannot read them. Switching the option off does not
make already-compressed history readable by old Nodes, so downgrading after an
opt-in write requires a separately verified migration rather than a toggle.

## Error envelope

Failures return a non-2xx status with a JSON body:

```json
{ "error": "human-readable detail", "code": "lease.busy" }
```

`error` is always present; `code` is present when the failure class is
machine-actionable. An SDK maps `(status, code)` to a typed error and MUST
NOT branch on `error` text. Unknown `(status, code)` pairs map to the
status-class fallback (`conflict`, `unavailable`, `refused`).

Defined client-visible classes (exhaustive today, extendable):

| Class | HTTP | When |
|---|---|---|
| `stale_epoch` | 409 | Writer/Reader/member session epoch was superseded; reopen the session, never retry with the same identity |
| `sequence` | 409 | Writer sequence gap/stale on `/v1/feeds/append` (raw path); on `/v1/writers/*` the server heals automatically |
| `lease.busy` | 409 | Subscription work item is claimed by another member; claim different work |
| `lease.stale_lease` | 409 | Member's work lease expired or was superseded |
| `lease.capacity` | 409 | Member or Subscription work cap reached; back off |
| `conflict` | 409 | Generic fencing conflict not covered by a `code` |
| `ambiguous` | 5xx after uncertain outcome | Request may have landed; identical retry by `request_id` is required to discover the committed result |
| `unavailable` | 503 | Quorum/replica unavailable; safe to retry |

## Idempotency and ambiguity

- Every mutating request carries a caller-generated `request_id`. An
  **identical retry** (same `request_id`, same body) returns the original
  committed result — same `message_id`, same `cursor`, `deduplicated: true`
  on appends — even across reconnects and Node moves.
- After a timeout or 5xx the outcome is ambiguous: the SDK retries the
  **same** request, never a regenerated `request_id`. A new `request_id`
  for the same logical record can commit twice.
- Writer sessions pin (`writer`, `session_epoch`). `OPEN WRITER SESSION`
  returns the epoch; appends send it on every record. A
  `session_epoch`-fenced error means the session was superseded by a newer
  open; the SDK must reopen before appending.

## Ordering and delivery

- Accepted order is guaranteed per `(Feed, Key)` only. There is **no**
  cross-Key or cross-Feed total order; do not infer one from timestamps or
  cursor bytes.
- Reader delivery is at-least-once: after an ambiguous result or reopen a
  record may arrive again. `delivered_cursor` (fetch) advances when the
  page lands; `acknowledged_cursor` (ack) is the durable restart point.
  Ack the page's `delivered_cursor`, not the last record's `cursor`.
- `feedback.hot_key: true` on an append means the Key holds sustained
  dominant share in its range: spread or shed load for that Key rather
  than scaling batch sizes. A strictly ordered hot Key is isolated, never
  parallelized — `recommended_batch_count`/`retry_after_ms` already
  encode the tightening; honor them.

## Capability negotiation

`GET /health` returns `{ "status": "ok", "version": ... }`-shaped metadata.
An SDK pins `whitewater-protocol` major `1`; a server whose reported major
differs must produce an explicit `IncompatibleServerError`, never silent
degradation. Per-operation availability follows the conformance corpus —
a server that lacks an operation returns `refused`, which the SDK reports
verbatim.

`GET /health` also returns `limits` — the enforced transport bounds a
client should negotiate against before a request hits a 4xx refusal:
`max_frame_bytes`, `max_committed_read_bytes`, `max_logical_read_frames`,
`max_logical_read_ranges`, `subscription_member_fetch_max_limit`, and
`subscription_member_max_lease_ticks`. These are the compiled-in bounds,
not configuration; an SDK that cannot decode the section must surface a
protocol error rather than guess.
