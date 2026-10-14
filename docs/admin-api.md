# Whitewater Admin API v1

The Whitewater Admin API is the only remote entry point for administrative commands. WCL, `wwctl`, Rust clients, future SDKs, UIs, operators, and MCP tools all use this API and converge on the same typed `ControlController`. Front ends contain no catalog authority and may be replaced, rewritten, or moved to separate projects without changing Whitewater server semantics.

```text
WCL / wwctl / Rust AdminClient / any HTTP client / future MCP
    -> authenticated Admin API
    -> typed Command
    -> ControlController
    -> control-plane transaction
```

The standard three-Node Riverbed uses a persistent OpenRaft Control Plane. Commands submitted to any Node are forwarded to the elected leader and return only after majority commit. Nodes started without Control Plane configuration retain an explicit `local_prototype` fallback for isolated tests.

## Authentication

Every Admin API request requires:

```http
Authorization: Bearer <api-key>
```

The Node reads its expected key from:

```text
WHITEWATER_ADMIN_API_KEY
```

Keys shorter than 24 characters are rejected during configuration. The server stores only an in-memory BLAKE3 digest and compares supplied digests without early exit.

Catalog-issued API-key identities (`ISSUE API KEY`, `REVOKE API KEY`, `SHOW API KEYS` in WCL) extend this surface: an identity authorizes Admin API requests when its bound Role holds `manage` on the root `*` namespace and it has not expired. Only a BLAKE3-256 verifier digest is persisted and replicated — plaintext keys never enter WCL, typed commands, the replicated log, or catalog state. Revocation propagates to every Node's request-time authorization view through the same catalog commit. The Feed-named and resource-named data-plane endpoints also honor Domain-scoped identities: the bound Role needs a grant whose namespace pattern covers the accessed Feed for `read` or `write` as appropriate, with `manage` implying both. `POST /v1/feeds/append` and the Writer session endpoints (`POST /v1/writers/append`, `/v1/writers/append-batch`) require `write` on the target Feed; `GET /v1/feeds/records`, `/v1/feeds/records/search`, `/v1/feeds/records/lookup`, `POST /v1/readers/temporary/fetch`, and the Reader session endpoints (`POST /v1/readers/open`, `/v1/readers/fetch`, `/v1/readers/ack`, `/v1/readers/close`) require `read` on the owning Feed — a Writer or Reader declared in a foreign Domain still authorizes against its Feed's namespace, not its own name. Subscription member endpoints (`POST /v1/subscriptions/members/join`, `/claim`, `/renew`, `/release`, `/transfer`, `/ack`, `/fetch`, and `GET /v1/subscriptions/members/state`) require `read` on the Subscription's source: a Feed-sourced Subscription authorizes against its Feed's namespace, while a Domain-sourced one needs a spanning grant (`*` or `{domain}.*`) — an exact-name grant never covers a Domain's member Feeds. `POST /v1/subscriptions/seek` repositions shared progress and requires `manage` on the source under the same rules. StateStore endpoints authorize against the store's own namespace: `POST /v1/state-stores/lookup`, `/get-by`, and `/iterate` need `read` while `/mutate`, `/clear`, `/sync`, and `/rebuild` need `write`. `POST /v1/indexes/query` needs `read` on the Index's own name. WCL and typed command batches scope per command: declaration and lifecycle commands (`CREATE FEED`, `DEFINE STATESTORE`, `DEFINE INDEX`, `CREATE WRITER`/`READER`/`SUBSCRIPTION`/`PIPE`, `RENAME`, `DROP`, `REBUILD INDEX`, `SEEK`) require `manage` spanning the resource's Domain (resolved by longest dotted prefix) plus `manage` on each referenced source's Domain; `DESCRIBE` needs `read` on the resource's Domain and `SHOW`/`EXPLAIN ACCESS` need `read` on `*`. RBAC (`CREATE ROLE`, `GRANT`, `DENY`, `REVOKE`, `ISSUE`/`REVOKE API KEY`), `CREATE DOMAIN`, Writer/Reader session commands, placement, movement, and Node membership commands remain Riverbed-wide administrative operations. Explicit `DENY <actions> ON NAMESPACE <pattern> TO ROLE <name>` fences an action ahead of every covering allow, `manage`-implied coverage, and Riverbed-admin issued identities — only the configured bootstrap key is break-glass. A deny containing `manage` fences read, write, and manage on the namespace; `SHOW GRANTS` and `EXPLAIN ACCESS` surface the effect (`matched_denies`). `REVOKE <actions> ON NAMESPACE <pattern> FROM ROLE <name>` (and `REVOKE DENY ...`) subtracts the listed actions from covering grant rows, dropping any row that empties and failing closed when nothing matches.

If no key is configured, Admin API requests return `503 Service Unavailable`. Missing or incorrect credentials return `401 Unauthorized`.

Docker development files use this clearly non-production fallback:

```text
whitewater-local-development-admin-key
```

Override it before starting Compose:

```bash
export WHITEWATER_ADMIN_API_KEY='replace-with-a-random-development-key'
docker compose up --build -d
```

PowerShell:

```powershell
$env:WHITEWATER_ADMIN_API_KEY = 'replace-with-a-random-development-key'
docker compose up --build -d
```

Production must supply a generated secret through the orchestrator or secret manager. There is no production fallback in the binary.

## Replicated Feed records

Append through any Node with `POST /v1/feeds/append`. Read majority-committed records through any current ingress Node, even if some current ranges are owned elsewhere:

```http
GET /v1/feeds/records?feed=orders.events&limit=100
Authorization: Bearer <api-key>
```

Continue after an opaque Cursor without changing another caller's position:

```http
GET /v1/feeds/records?feed=orders.events&after=<cursor>&limit=100
Authorization: Bearer <api-key>
```

The ingress retrieves each current range from its Append Owner over authenticated internal HTTP, merges committed frames, and checks placement again before returning. On Control Plane Riverbeds, the first range page requires another current RF3 replica to confirm the owner's committed boundary and frame digest; an owner behind another replica, disagreement, missing corroboration, unavailable owner, or incomplete placement fails as retryable rather than returning a partial Feed. This per-read evidence is not yet a durable committed-frontier proof after multiple replica losses. New Cursors are derived from stable FeedId and request identity; older persisted tokens remain usable. An unknown, uncommitted, or wrong-Feed Cursor is rejected when it cannot resolve in that Feed. Single-range Feeds now seek an opaque committed Cursor through a rebuilt owner-side index and stream up to 32 records per byte-bounded internal page; the same path supports tail/new-only reads past record 10,000. Responses remain limited to 10,000 records and 16 MiB. Multi-range reads still scan bounded committed prefixes (at most 10,000 records per range and Feed, 16 MiB total, 128 ranges) and fail closed when the bound is exceeded. A full-history timestamp scan past the bound also fails rather than presenting an incomplete answer. Durable global multi-range ordering, checkpointed Cursor-index recovery, and production inter-Node mTLS remain planned. Every delivered record carries a server-owned `source` block — `source.feed_name` (last dotted segment, `created`), `source.full_feed_name` (Domain-qualified, `orders.created`), and `source.feed_id` (immutable identity) — separate from writer-provided `metadata_base64` so applications cannot spoof it. The same block rides Reader-session, temporary-Reader, and Subscription-member deliveries; names resolve at delivery time, so deliveries after a rename carry the new names while Cursor and FeedId stay stable.

### Bounded Key search

`GET /v1/feeds/records/search` finds committed records by exact Key without a consumer or a full-history read:

```http
GET /v1/feeds/records/search?feed=orders.events&key_base64=a2V5&limit=64
Authorization: Bearer <api-key>
```

Each call scans at most 10,000 committed frames in the same merged order the plain read serves, so a miss has a bounded, visible cost (`scanned`) instead of an open-ended history walk. When the record `limit` or the scan budget stops the call before history does, the response carries `next_after` — the last scanned frame's opaque Cursor — and `exhausted: false`; pass `next_after` back as `after` to resume exactly where the scan halted. `exhausted: true` means the committed history drained and no continuation is needed. Matching records are the same committed `RecordResponse` objects the plain read returns, including the server-owned `source` block.

```json
{
  "records": [ /* RecordResponse[] */ ],
  "scanned": 842,
  "exhausted": false,
  "next_after": "cursor-..."
}
```

Predicates combine with AND semantics: `key_base64` selects the exact Key; `from_event_time_ns` / `to_event_time_ns` bound the record's `event_time_ns` inclusively as signed epoch-nanosecond decimals; and `metadata_key` asserts a Metadata entry's presence — paired with `metadata_value_base64` it requires an exact value, so a correlation or trace id written into Metadata is a first-class way to find the records belonging to one business flow. At least one predicate is required because the plain read is the contract for unfiltered history. Inverted ranges, non-decimal bounds, an empty `metadata_key`, or a value without its key are refused; an unknown `after` Cursor fails closed like the plain read.

### Single-record lookup

`GET /v1/feeds/records/lookup` resolves one opaque Cursor to the committed record it names — the smallest investigation unit, for example the frame a search match or a support ticket points at:

```http
GET /v1/feeds/records/lookup?feed=orders.events&cursor=<cursor>
Authorization: Bearer <api-key>
```

The Cursor carries no public Range identity, so each committed Range's owner-side index is probed in turn; a Range that does not hold the Cursor rejects the probe and the next Range is tried, while an unavailable or disagreeing owner fails the whole lookup rather than masquerading as a miss. A Cursor no Range resolves returns the same `Cursor is unknown, uncommitted, or belongs to another Feed` rejection as the plain read. The response is the same committed `RecordResponse` shape the read and search endpoints return.

## Execute WCL

```http
POST /v1/admin/wcl
Authorization: Bearer <api-key>
Content-Type: application/json
```

```json
{
  "request_id": "018f5f65-5d87-7c2e-a9a3-3af92c48ed21",
  "script": "CREATE DOMAIN orders; CREATE FEED orders.created; SHOW FEEDS;"
}
```

Curl:

```bash
curl -X POST http://localhost:7071/v1/admin/wcl \
  -H 'authorization: Bearer whitewater-local-development-admin-key' \
  -H 'content-type: application/json' \
  -d '{"script":"SHOW FEEDS;"}'
```

`POST /v1/control/execute` remains an authenticated compatibility alias during the prototype and should not be used by new clients.

`CREATE SUBSCRIPTION orders.billing FROM orders.created;` or the typed `create_subscription` command declares one logical Subscription in the same Domain as its source Feed. `SHOW SUBSCRIPTIONS`, `DESCRIBE SUBSCRIPTION`, rename and safe drop operate on its metadata; the result has `stage=declared`. The source may instead name the whole Domain — `CREATE SUBSCRIPTION orders.everything FROM DOMAIN orders;` or `{"command":"create_subscription","name":"orders.everything","domain":"orders"}` — binding immutable Domain identity and covering every current and future Feed. Member Feeds attach in the same catalog commit that creates them — the durable discovery boundary — honoring the declared start for existing Feeds and the first retained record for newly discovered ones; a cross-Domain move drains fenced membership rather than silently discarding pending work, an explicit drop retires fenced rows, and `DESCRIBE SUBSCRIPTION` reports per-Feed `member_feeds` states. Membership is bounded (4096 member Feeds per Subscription; attach refuses rather than silently skipping). Member join on a Domain source still refuses 409 until shared delivery lands. No member can join it yet, no shared Cursor or lease is persisted, and no internal/public Feed is created for offsets or transaction decisions. Continue using existing named Reader sessions for working consumption until Subscription delivery is implemented. Internal `/internal/subscription-progress/prepare`, `/internal/subscription-progress/commit`, and `/internal/subscription-progress/committed` accept bounded Node requests and check receiver/owner/epoch against private placement. Development mode uses the shared Control Plane credential; optional mTLS mode moves these routes to a separate listener with CA-validated client certificates pinned to internal Node IDs and disables the HTTP routes. Setting only part of the mTLS configuration fails startup. The committed endpoint reports **one replica's local Fjall state**, not an acknowledgement: the internal coordinator compares assigned replica replies, checks their claimed NodeId/SubscriptionId/epoch, requires matching quorum evidence, and refuses disagreements. JSON claims alone are not Node identity; only the pinned mTLS listener authenticates request peers. These are not application APIs, forwarded commit-vote evidence is not independently signed, the other internal services still lack production transport security, and this does not make Subscriptions joinable or progress RF3-recoverable.

## Execute typed commands

Clients do not need to generate WCL text. They can submit typed JSON commands directly:

```http
POST /v1/admin/commands
Authorization: Bearer <api-key>
Content-Type: application/json
```

```json
{
  "request_id": "018f5f65-5d87-7c2e-a9a3-3af92c48ed21",
  "commands": [
    {
      "command": "create_domain",
      "name": "orders"
    },
    {
      "command": "create_feed",
      "name": "orders.created"
    },
    {
      "command": "create_writer",
      "name": "checkout",
      "feed": "orders.created"
    },
    {
      "command": "create_reader",
      "name": "audit",
      "feed": "orders.created",
      "start": {
        "kind": "beginning"
      }
    }
  ]
}
```

`create_domain`, `domain`, and `domains` are the preferred typed names. Existing `create_space`, `space`, and `spaces` remain accepted for compatibility and refer to the same resource. Persisted definitions and current JSON responses retain the `space_id` field; it is the Domain's stable identity, not a separate namespace. Do not re-create a Domain or change request IDs to migrate its name.

## Typed command shapes

### Create

```json
{ "command": "create_domain", "name": "orders" }
```

```json
{ "command": "create_feed", "name": "orders.created" }
```

```json
{
  "command": "create_writer",
  "name": "checkout",
  "feed": "orders.created"
}
```

```json
{
  "command": "create_reader",
  "name": "audit",
  "feed": "orders.created",
  "start": { "kind": "beginning" }
}
```

### Declare StateStore (metadata only)

The typed Admin API can commit an idempotent Domain-scoped StateStore declaration with either an explicit same-Domain source Feed or a manual source:

```json
{ "command": "define_state_store", "name": "accounts.users", "source": { "kind": "manual" },
  "named_keys": [ { "name": "department", "unique": false }, { "name": "passport", "unique": true } ] }
```

```json
{ "command": "define_state_store", "name": "accounts.profiles", "source": { "kind": "feed", "feed": "accounts.events" } }
```

The response reports `stage: "declared"` and an immutable `store_id`. The catalog rejects unknown source Feeds and Feeds outside the StateStore's Domain. Retry a timed-out declaration with the same request ID.

`named_keys` declares up to 10 secondary lookup keys. Key names are lowercase identifiers; `unique` claims enforce one primary row per value. Non-unique named keys map one value to many primary keys (a group/list lookup). Unique claims arbitrate by journal order — first-committed-wins: a conflicting `put_row` journals and applies as a deterministic no-op on every replica rather than poisoning replication.

Named-key population differs by source. **Manual** stores carry each key's typed values on the `put_row` mutation (below). **Feed-sourced** stores declare one `extractor` per named key — the same `IndexFieldSpec` spelling `CREATE INDEX FIELDS` uses — and replay projects values from every committed source record:

```json
{ "command": "define_state_store", "name": "accounts.tagged",
  "source": { "kind": "feed", "feed": "accounts.events" },
  "named_keys": [
    { "name": "passport", "unique": true,
      "extractor": { "kind": "metadata", "name": "pp" } },
    { "name": "department", "unique": false,
      "extractor": { "kind": "projection", "extractor": "json", "version": 1, "field": "dept" } } ] }
```

Extractor kinds: `{ "kind": "key" }` reads the record's application Key; `{ "kind": "metadata", "name": ... }` a Metadata entry; `{ "kind": "projection", "extractor": "json", "version": 1, "field": "dotted.path" }` a JSON-path over the payload. Missing fields and unparseable payloads project `Null` postings rather than stalling replay. Declarations are checked at define time: feed-sourced named keys without an extractor are refused, as are extractors on a manual store.

A declared store already has a working RF3 write path:

- **Manual stores** accept `POST /v1/state-stores/mutate` (`{ "store": name, "mutation_id": uuid, "op": "put_row" | "delete_row" }`). `mutation_id` is the caller-owned idempotency identity: retries dedupe, changed contents conflict. `put_row` ops carry the row plus optional named-key values: `{ "kind": "put_row", "key_base64": ..., "payload_base64": ..., "named": [ { "name": "department", "values": [ { "kind": "text", "value": "eng" } ] } ] }`. Values are typed (`null | bool | i64 | text | bytes` — strings and numbers never need manual byte conversion); names must match the store's `named_keys` declaration and the server stamps each entry's `unique` semantics from that declaration.
- **Feed-derived stores** materialize committed source history: `POST /v1/state-stores/sync` resumes from the durable journaled frontier; `POST /v1/state-stores/rebuild` re-materializes under a caller `token` namespace. Both refuse with 409 when the source Feed or journaled frontier is gone rather than silently producing a partial store. Owning Nodes also drive bounded replay passes automatically.

Primary-key lookups exist: `POST /v1/state-stores/lookup` (`{ "store": name, "key_base64": bytes }` returns the row plus freshness evidence (`applied_seq`, `committed_seq`, `behind`, `served_by`). It is served locally when the ingress Node holds a journal replica and otherwise forwarded internally — no application topology knowledge required. Strict reads are supported: `strict: true` with optional `wait_ms` (default 500, capped at 2000) makes the serving replica wait — bounded — for its applied sequence to reach the committed frontier observed when the read arrived; the ingress tries fresher eligible replicas and, when none catch up in time, fails with HTTP 409 `index_behind` instead of serving a stale snapshot. Non-strict requests always report `behind` honestly rather than claiming freshness. Replicas stay durable and convergent: every applied mutation advances a recorded chain digest (`blake3(previous | sequence | mutation_digest)` per entry), replicas page missing committed entries from the freshest peer through the internal `state-store-journal/entries` route — bounded pages, owner/epoch fenced — and a wiped or replaced replica installs its peers' committed history plus attested checkpoints through `state-store-journal/transfer`, with divergence reported as `state_store.diverged` rather than silently serving different state. Declared secondary Indexes now rebuild through `REBUILD INDEX` /
`REBUILD INDEXES IN DOMAIN`: each Node materializes a derived
shadow-generation posting set from retained committed Feed history
(`IndexFieldSpec` key/metadata/json-projection extraction), refuses the
pass when the source or resume frontier is gone, and commits
`CompleteIndexBuild` for atomic `building` → `active` activation; prior
generations retire through a bounded sweep. Active Indexes answer logical queries through `POST /v1/indexes/query` (`{ "index": name, "mode": "exact" | "prefix" | "range", ... }`) — the same semantics as WCL `GET INDEX` — resolving the logical name to the active generation and serving from any ingress Node: a Node that materializes the postings answers locally, a strict read whose local postings lag the activation frontier forwards (bounded) to fresher peers, and failures surface `index_not_found` (404), `index_not_ready` (409 while declared/building), or `index_behind` (409 strict, no Node reached the frontier in the bounded wait). Rows return `key_base64`/`payload_base64` plus freshness evidence (`applied_cursor`, `target_cursor`, `behind`, `truncated`, `served_by`); `limit` defaults to 256 (1..=1000) and results cap at 16 MiB.

StateStore named-key, iteration, and lifecycle operations share the same
any-ingress routing and freshness contract as primary-key `lookup`:

- `POST /v1/state-stores/get-by` (`{ "store": name, "named_key": name, "values": [typed], "limit": 1..=1000 default 64, "strict": bool, "wait_ms": bounded }`) resolves a declared named-key value to every primary row it points at — the group/list read: one `department = "eng"` value returns all matching rows. Rows return `key_base64`, `payload_base64`, and the `applied_cursor` each row last applied at, plus `applied_seq`/`committed_seq`/`behind`/`served_by` freshness evidence. Undeclared key names and empty `values` are 400s; strict reads honor the same bounded-wait `index_behind` contract as `lookup`.
- `POST /v1/state-stores/iterate` (`{ "store": name, "after_key_base64": cursor|null, "limit": 1..=1000 default 256 }`) returns a bounded page of applied rows in primary-key order plus `next_key_base64` for continuation and `pinned_seq` — the serving replica's applied journal position the page was pinned to, the same row set replica transfer serves. Iteration reports its freshness honestly through `pinned_seq` rather than waiting; a page reflects everything applied up to that sequence and nothing later.
- `POST /v1/state-stores/clear` (`{ "store": name, "mutation_id": uuid }`) empties a manual StateStore as one journaled `ClearAll` mutation through the RF3 quorum: replicas sweep rows and named postings in bounded 512-row batches under the mutation's journal cursor, so the clear is durable, idempotent under `mutation_id` retries, replayed on restart, and never one unbounded delete. Feed-derived stores refuse — their contents replay from the source Feed.
- `DROP STATESTORE <name>` (WCL / typed `drop` command) tombstones the catalog definition — the name cannot be recreated, `RENAME` onto it is refused — and removes the RF3 placement so mutation, lookup, sync, and iterate paths refuse. Rows already applied on replicas remain until a bounded reclaim pass (the same orphan-row model as dropped Feed history); the definition stays visible to operators as `stage: "dropped"`.

Reader start variants:

```json
{ "kind": "beginning" }
{ "kind": "now" }
{ "kind": "cursor", "cursor": "AbGTj_j2..." }
```

```json
{ "command": "create_role", "name": "orderanalytics" }
```

### Rename

```json
{
  "command": "rename",
  "kind": "feed",
  "current": "orders.created",
  "new": "orders.accepted"
}
```

Kinds are `domain`, `feed`, `writer`, `reader`, `subscription`, and `role`.

### Drop

```json
{
  "command": "drop",
  "kind": "reader",
  "name": "audit"
}
```

### Show

```json
{ "command": "show", "kind": "feeds" }
```

Kinds are `domains`, `feeds`, `writers`, `readers`, `subscriptions`, `roles`, and `grants`.

### Describe

```json
{
  "command": "describe",
  "kind": "feed",
  "name": "orders.created"
}
```

### Reader sessions

Named Reader sessions use the same authenticated API across Nodes:

```json
POST /v1/readers/open
{ "request_id": "...", "reader": "audit", "capacity": 100 }

POST /v1/readers/fetch
{ "request_id": "...", "reader": "audit", "session_epoch": 1, "limit": 100 }

POST /v1/readers/ack
{ "request_id": "...", "reader": "audit", "session_epoch": 1, "cursor": "..." }

POST /v1/readers/close
{ "request_id": "...", "reader": "audit", "session_epoch": 1 }
```

Delivered and acknowledged Cursors are separate. Fetch advances delivered progress only; acknowledgement is explicit and cumulative. Always acknowledge the fetch response's `delivered_cursor`, not `records[-1].cursor`: on a multi-range named Reader starting at Beginning, the former is a versioned opaque progress token and the latter identifies one record. The underlying bounded per-range frontier is persisted through the Control Plane without exposing range placement in public Reader definitions. Reopening a persistent Reader increments its epoch, fences the old session, resets delivered progress to the acknowledged frontier, and resumes on another Node. A verified bounded split/merge cutover translates the named Reader's acknowledged frontier with the RangeMap and fences the old session; reopen to replay unacknowledged work, which may include conservative duplicates after merge. Incomplete lineage refuses cutover rather than guessing a Cursor, and merges with overlapping post-split Writer sequences remain blocked. Public/temporary and legacy seek paths retain their documented multi-range history bounds. A repeated fetch `request_id` on the frontier path replays the identical page: the committed frontier persists the pre-delivery positions and record bound, so a retry after an ambiguous transport failure re-reads immutable history and serves the same records plus the same deterministic `delivered_cursor` — on any ingress Node — when the derived positions match the committed frontier exactly, and keeps the original reopen instruction when they cannot be proven identical.

Anonymous temporary Readers use `POST /v1/readers/temporary/fetch` with `feed`, optional `after`, signed nanosecond `after_event_time_ns`, `limit`, `tail`, `new_only`, and bounded `wait_ms`. They retain no server-side progress. `new_only` atomically returns the current end Cursor without historical records; the caller then waits after that Cursor.

### Subscription member sessions

Member sessions share a Subscription's durable acknowledged progress through
epoch-fenced work leases. All calls are POSTs under `/v1/subscriptions/members/`
and take `subscription`, `request_id`, `member_id`, and (except `join`) the
member's current `member_epoch`:

```json
POST /v1/subscriptions/members/join
{ "subscription": "orders.billing", "request_id": "...", "member_id": "..." }

POST /v1/subscriptions/members/claim
{ "subscription": "orders.billing", "request_id": "...", "member_id": "...",
  "member_epoch": 2, "work_id": "...", "lease_ticks": 60 }

POST /v1/subscriptions/members/renew
{ "...": "...", "lease_epoch": 1, "lease_ticks": 90 }

POST /v1/subscriptions/members/release
{ "...": "...", "lease_epoch": 1 }

POST /v1/subscriptions/members/transfer
{ "subscription": "orders.billing", "request_id": "...", "work_id": "...",
  "member_id": "...", "lease_epoch": 3,
  "to_member_id": "...", "to_member_epoch": 1, "lease_ticks": 60 }
  -> moves one work lease to another joined member, bumping lease_epoch so
     the previous holder can never acknowledge it; stale source evidence is
     refused (409), as is a full target. Bounded batched transfers ride the
     same journal ops, so a rebalance is incremental, not stop-the-world.

POST /v1/subscriptions/members/fetch
{ "...": "...", "work_id": "...", "lease_epoch": 1, "limit": 100 }

POST /v1/subscriptions/members/ack
{ "...": "...", "work_id": "...", "lease_epoch": 1,
  "cursor": "...", "positions": { "<range-id>": "<position>" },
  "quarantine": { "<range-id>": ["<cursor>", "..."] },
  "release_quarantine": { "<range-id>": ["<cursor>", "..."] } }

GET /v1/subscriptions/members/state?subscription=orders.billing
```

The first `join` on a declared Subscription atomically establishes the shared
frontier from its start position — `BEGINNING`, `NOW`, `TIMESTAMP <ns>` on the
ingest clock, or `CURSOR '<value>'` resolved internally to per-range positions
(the same merged-order skip a Reader `after` read applies; an unknown or
foreign-Feed Cursor fails closed) — and member epoch 1; rejoining fences the prior
epoch. `claim` grants a bounded expiring work lease, `fetch` returns a bounded
page of records after the shared frontier plus the per-range `positions` and
`cursor` to pass back to `ack`, and `ack` atomically advances the frontier and
releases the lease. An ack may also carry `quarantine` — positions the member
declares delivered-but-not-processed — and `release_quarantine` — positions a
later ack removes once resolved. The cumulative set rides the committed
frontier mutation durably and bounded (1024 positions, addable only from the
ranges the ack commits), is reported on the admin progress view as
`committed.quarantined`, and carries forward through `SEEK SUBSCRIPTION`
repositions so reprocessing the same window still shows its poison evidence. Concurrent members may fetch overlapping windows — progress
is fenced by the committed Cursor at `ack`, not by delivery, so failover is
at-least-once until per-Key work windows arrive. Per-Key work subdivision is
not yet implemented.

All member mutations first probe the private progress owner: if it is lost,
the endpoint runs the quorum-backed owner-recovery path — adopting the highest
committed frontier onto surviving replicas under a bumped ownership epoch —
then retries the operation once on the recovered placement, so member
operations survive a single Node loss without client-visible topology. `state`
and `fetch` are reads and continue on the surviving replica quorum without
moving ownership.

### Writer sessions

```json
{ "command": "open_writer_session", "writer": "checkout" }
{ "command": "allocate_writer_sequence", "writer": "checkout", "session_epoch": 1 }
{ "command": "revoke_writer_session", "writer": "checkout", "session_epoch": 1 }
```

`DESCRIBE WRITER checkout` returns the immutable WriterId/FeedId binding, current session epoch, next sequence, and active/revoked state. Retrying sequence allocation with the same batch request ID returns the original allocation.

Applications append without managing sequence numbers:

```http
POST /v1/writers/append
Authorization: Bearer <api-key>
Content-Type: application/json
```

```json
{
  "request_id": "018f5f65-5d87-7c2e-a9a3-3af92c48ed21",
  "writer": "checkout",
  "session_epoch": 1,
  "event_time_ns": "1700000000123456789",
  "key_base64": "b3JkZXItMTIz",
  "payload_base64": "eyJvcmRlcklkIjoiQTEwMCJ9",
  "metadata_base64": {}
}
```

The Control Plane allocates the sequence idempotently from `request_id`, validates the current epoch and immutable Feed binding, and rejects stale or revoked sessions before encoding. Successful responses include majority durability plus adaptive batching feedback.

Send a bounded ordered batch with `POST /v1/writers/append-batch` and `{ "records": [...] }`. The initial implementation commits records sequentially in request order, stops at the first failure, and relies on each record's stable request ID for safe retry of a partially completed batch. `WriterSessionClient::append_batch` uses the same endpoint.

### Inspect Active Range placement

Placement is an authenticated operator view. Writers and Readers never receive owner or replica topology.

```json
{
  "command": "inspect_placement",
  "feed": "orders.created"
}
```

The result contains the internal RangeId, generation, current owner, RF3 replica set, ownership epoch, and any candidate owner-movement plans. The standard development Riverbed currently treats its three statically configured Control Plane Nodes as storage-capable placement candidates.

The old `transfer_active_range_ownership` typed command is refused: changing the epoch alone can promote a lagging follower. The public typed Admin API also rejects owner recovery/readiness/activation commands requiring internal Node authority; operators must not synthesize catch-up evidence.

### Move the Append Owner (development workflow)

After ensuring that the chosen **current RF3 follower** has caught up, use the authenticated workflow rather than the old metadata-only command:

```http
POST /v1/admin/ranges/move-owner
Authorization: Bearer <admin-key>
Content-Type: application/json
```

```json
{
  "request_id": "98ab0cdf-80c2-45a9-bf62-04e7a2aa137b",
  "feed": "orders.created",
  "range_id": "632a51da-5945-4cac-a541-cdb9e63cd5b4",
  "new_owner": "control-2"
}
```

The Control Plane persists an idempotent plan while the old RF3 assignment remains authoritative. The current owner drains in-flight majority appends, freezes its generation, captures its committed boundary, and the candidate freezes and checks identical committed positions, record bytes, identities, and Cursors one frame at a time. Only verified readiness allows compare-and-set activation with a higher ownership epoch; a definite pre-readiness failure unfreezes both, while ambiguous readiness/activation leaves them frozen until the plan is resolved. Retry using the **same request ID** and inspect placement. The owner can change without changing the Feed, RF3 set, or application Cursor.

This is a correctness-first development workflow: the history scan refuses ranges above 10,000 committed records, and development inter-Node HTTP uses a shared internal credential without production mTLS/verified Node identity. `python scripts/test-m4-owner-move.py` exercises an **isolated three-Node** Riverbed (ports 7271–7273), including an unavailable target, retry, continued writes, Cursor reads, and a new-owner restart without touching the standard Compose Riverbed. Checkpointed large-history verification, throughput budgets, drain scheduling, and production transport security remain planned.

### Replace a follower replica

An operator can replace **one non-owner follower** after adding a fourth eligible storage-capable Node. This is an administrative operation; Writers and Readers still supply only Feed, Key, and Cursor.

```http
POST /v1/admin/ranges/move-follower
Authorization: Bearer <admin-key>
Content-Type: application/json
```

```json
{
  "request_id": "4ea965f3-17da-48d5-b654-ae1697d35d85",
  "feed": "orders.created",
  "range_id": "632a51da-5945-4cac-a541-cdb9e63cd5b4",
  "removed_replica": "control-3",
  "replacement_replica": "control-4"
}
```

The Control Plane persists the plan but keeps the original RF3 assignment authoritative during bounded, authenticated committed-frame copy. The Append Owner then drains its in-flight quorum writes, freezes the source, truncates only uncommitted tail records, and verifies that the replacement is committed through the final source boundary before one consensus assignment change. The owner is unchanged; the ownership epoch advances, and the removed follower is fenced. The application does not get a topology callback. Retry a failed or timed-out request with the **same** request ID; inspect placement and plan state if the outcome is ambiguous.

The current HTTP staging prototype transfers one bounded record per internal request and refuses ranges above 10,000 committed records; the original RF3 assignment remains active on refusal. The prepared plan remains visible in `INSPECT PLACEMENT`; after confirming activation was not submitted, an operator can clear it with the typed `abort_follower_move` command and its `plan_id`. Checkpointed streaming for larger histories and a foreground-SLO-aware movement budget remain planned. The regular three-Node development Riverbed has no spare eligible fourth Node. `compose.m4-move.yml` provides an **isolated, test-only four-voter** Riverbed for `python scripts/test-m4-follower-move.py`. This is not production role-separated storage placement. Append Owner movement uses the authenticated development workflow above; automatic Node drain remains planned.

### Move Subscription progress placement

Storage drain routes planned Subscription progress placement steps — owner handoff or replica replacement — through this endpoint rather than committing the placement command alone. The coordinator inspects replica evidence, compare-and-sets the placement through the Control Plane, re-stamps the recovered frontier at the new ownership epoch, and adopts it on the replica set; committing only the catalog step would strand progress replicas at the previous epoch and wedge the Subscription's progress until recovery.

```http
POST /v1/admin/subscriptions/move-progress
Authorization: Bearer <admin-key>
Content-Type: application/json
```

```json
{
  "command": "recover_subscription_progress_owner",
  "subscription_id": "632a51da-5945-4cac-a541-cdb9e63cd5b4",
  "expected_ownership_epoch": 1,
  "new_owner": "control-2"
}
```

or

```json
{
  "command": "move_subscription_progress_replica",
  "subscription_id": "632a51da-5945-4cac-a541-cdb9e63cd5b4",
  "expected_ownership_epoch": 1,
  "replaced": "control-3",
  "replacement": "control-4"
}
```

The response reports the adopted assignment and the replica Nodes that confirmed adoption. The step is safe to retry: if the placement change already landed — including under a different target chosen by a racing actor — the coordinator converges replicas at the live epoch instead of forcing the planned target.

### Inspect Subscription progress placement

```http
GET /v1/admin/subscriptions/progress?subscription=<domain>.<name>
Authorization: Bearer <admin-key>
```

Returns the Subscription's current progress assignment (owner, RF3 replica set, ownership epoch), the committed frontier and member/lease summary, and a per-replica health view. A replica that is unreachable, fenced at an older ownership epoch, or mid-recovery reports its own outcome instead of failing the whole read, so a wedged Subscription stays diagnosable while it is still wedged.

```json
{
  "subscription": "orders.billing",
  "subscription_id": "...",
  "feed_id": "...",
  "assignment": {"owner": "control-1", "replicas": ["control-1", "control-2", "control-3"], "ownership_epoch": 2},
  "committed": {"cursor": "...", "positions": {"<range-id>": "..."}, "sequence": 17, "request_id": "...", "ownership_epoch": 2, "tick": 42},
  "members": {"member_count": 3, "lease_count": 2, "last_tick": 42},
  "lag": {"records_behind": 1402, "ranges": {"<range-id>": {"behind": 1402}}},
  "replicas": [
    {"node": "control-1", "reachable": true, "committed_sequence": 17, "committed_epoch": 2, "prepared_pending": false, "member_count": 3, "lease_count": 2},
    {"node": "control-3", "reachable": false, "error": "unavailable"}
  ]
}
```

`committed` is `null` when no quorum-readable frontier exists (never joined, or mid-recovery). `lag.records_behind` is the sum of committed-but-undelivered records across the source Feed's Active Ranges — the committed-tail position minus the position each frontier Cursor resolves to. A range reports `behind: null` with `error` when its read is unavailable or its frontier position no longer resolves; `lag` is `null` when no committed frontier exists yet, and Domain-source Subscriptions report a detail note until per-Feed member delivery lands. `replicas[].committed_epoch` behind `assignment.ownership_epoch` marks a lagging replica that adoption has not yet caught up.

### Inspect flagged hot Keys

```http
GET /v1/admin/demand/hot-keys
Authorization: Bearer <admin-key>
```

Returns the Node-local set of Keys flagged for sustained dominant append demand inside their Active Range. Detection requires the dominant Key to hold at least half of a range's windowed appends across consecutive sampling windows (see `WHITEWATER_HOT_KEY_SUSTAINED_SAMPLES`), and the flag clears only after the Key's share stays below the hysteresis floor for `WHITEWATER_HOT_KEY_CLEAR_SAMPLES` consecutive windows, so short-lived bursts neither flag nor flap.

```json
{
  "hot_keys": [
    {
      "feed": "orders.events",
      "range_id": "...",
      "token": "<key-token>",
      "share_bps": 8900,
      "window_records": 420,
      "window_bytes": 186500,
      "next_token": "<key-token>",
      "isolating_boundary": "<key-token>"
    }
  ]
}
```

`token` is the deterministic KeyToken, not the raw Key bytes. `isolating_boundary` is where the autoscaler splits to push the hot Key toward a range edge; it is `null` when every observed append in the range uses this one Key — a strictly ordered hot Key cannot be parallelized, only isolated. Detection is ingress-local to the Append Owner, so in a multi-ingress topology query the owning Node (or every Node).

While a Key is flagged, appends for it return `feedback.hot_key: true` with tightened batch guidance and `retry_after_ms`; unrelated Keys in the same range keep normal guidance, and sustained range pressure splits at the Key's edge rather than the weighted median so unrelated Keys migrate away.


### Grant

```json
{
  "command": "grant",
  "actions": ["read", "write"],
  "namespace": "orders.*",
  "role": "orderapplication"
}
```

Actions are `read`, `write`, and `manage`.

### Explain access

```json
{
  "command": "explain_access",
  "role": "orderapplication",
  "action": "read",
  "feed": "orders.created"
}
```

### Seek Reader

```json
{
  "command": "seek_reader",
  "reader": "audit",
  "start": {
    "kind": "cursor",
    "cursor": "AbGTj_j2..."
  }
}
```

## Response

WCL and typed requests return the same structure:

```json
{
  "request_id": "018f5f65-5d87-7c2e-a9a3-3af92c48ed21",
  "revision": 4,
  "authority": "control_plane",
  "warning": "",
  "results": [
    {
      "statement": "CREATE FEED",
      "message": "created Feed orders.created",
      "data": {
        "feed_id": "c0153abc-2015-45fe-812e-0824f7543786",
        "name": "orders.created",
        "status": "active"
      }
    }
  ]
}
```

## Rust AdminClient

```rust
use whitewater::{
    admin::AdminClient,
    control::{Command, ReaderStart, ShowKind},
};

let admin = AdminClient::new(
    "http://localhost:7071",
    std::env::var("WHITEWATER_API_KEY")?,
);

let result = admin
    .execute_commands(vec![
        Command::CreateDomain {
            name: "orders".to_owned(),
        },
        Command::CreateFeed {
            name: "orders.created".to_owned(),
        },
    ])
    .await?;
```

Resource-oriented methods are available for applications that should not construct command enums:

```rust
admin.create_domain("orders").await?;
admin.create_feed("orders.created").await?;
admin
    .create_writer("checkout", "orders.created")
    .await?;
admin
    .create_reader(
        "audit",
        "orders.created",
        ReaderStart::Beginning,
    )
    .await?;
admin.show(ShowKind::Feeds).await?;
```

WCL through the same client:

```rust
let result = admin
    .execute_wcl("SHOW FEEDS;")
    .await?;
```

## Front-end boundary

The Admin API is the product boundary. A web UI, desktop application, IDE integration, Terraform provider, Operator, or customer-specific console can be built without embedding the Whitewater server or WCL parser. Typed JSON is sufficient for every command.

`wwctl` is deliberately a free-standing, replaceable front end. It depends on the public AdminClient/API contract and can move into a separate repository later. It never links to or opens catalog storage, and the server works without it.

## wwctl

`wwctl` is an interactive Admin API shell when started without `--execute` or `--file`:

```bash
export WHITEWATER_API_KEY='whitewater-local-development-admin-key'

wwctl
```

```text
Whitewater Control Language shell
whitewater> SHOW FEEDS;
whitewater> CREATE DOMAIN orders;
whitewater> \\help
```

One-shot execution remains available:

```bash
wwctl --execute "SHOW FEEDS;"
```

Or provide the key explicitly:

```bash
wwctl \
  --api-key 'whitewater-local-development-admin-key' \
  --execute "SHOW FEEDS;"
```

## Local wcl-cli command

Windows can use the API-only PowerShell frontend without Rust or Docker commands. Install it into `%USERPROFILE%\.local\bin` and persist the local development settings with:

```powershell
.\scripts\install-wcl-cli.ps1
```

Custom endpoints and key:

```powershell
.\scripts\install-wcl-cli.ps1 `
  -Endpoints 'server1:7070;server2:7070;server3:7070' `
  -ApiKey 'replace-with-a-random-development-key'
```

The installer copies `scripts/wcl-cli.ps1`, creates the `wcl-cli` launcher, updates the user `PATH` when required, and persists configuration for future terminals:

```powershell
[Environment]::SetEnvironmentVariable(
    'WHITEWATER_API_KEY',
    'whitewater-local-development-admin-key',
    'User'
)

[Environment]::SetEnvironmentVariable(
    'WHITEWATER_ENDPOINTS',
    '127.0.0.1:7071;127.0.0.1:7072;127.0.0.1:7073',
    'User'
)
```

Open a new terminal, then run:

```powershell
wcl-cli
```

Without a configured key, `wcl-cli` prompts using secure input. One-shot mutation retries can reuse a request ID:

```powershell
wcl-cli -Execute 'CREATE DOMAIN orders;' -RequestId '018f5f65-5d87-7c2e-a9a3-3af92c48ed21'
```

Override endpoints for one session with a quoted semicolon-separated list:

```powershell
wcl-cli "server1:7070;server2:7070;server3:7070"
```

The quote is required because PowerShell and shells treat semicolons as command separators.

Temporary PowerShell configuration:

```powershell
$env:WHITEWATER_API_KEY = 'whitewater-local-development-admin-key'
$env:WHITEWATER_ENDPOINTS = '127.0.0.1:7071;127.0.0.1:7072;127.0.0.1:7073'
wcl-cli
```

zsh or bash:

```bash
export WHITEWATER_API_KEY='whitewater-local-development-admin-key'
export WHITEWATER_ENDPOINTS='127.0.0.1:7071;127.0.0.1:7072;127.0.0.1:7073'
wcl-cli
```

Persist those exports in `~/.zshrc` or the shell's equivalent. Use a secret manager rather than a shell profile for production credentials.

Endpoint failover is safe for the standard three-Node Riverbed because every Node routes through one replicated Control Plane. Nodes deliberately started without Control Plane configuration report `local_prototype`; do not mix those isolated test Nodes into an endpoint list.

### Appending records with `wcl-cli write`

`wcl-cli write` posts the same `/v1/writers/append` data-API request as `wwctl write` and the SDKs. A declared Writer session is required first (`OPEN WRITER SESSION` returns the epoch):

```powershell
wcl-cli write -Writer orders.app -SessionEpoch 3 -Key order-42 -Payload 'total=99'
```

Optional inputs mirror `wwctl write`: `-RequestId <uuid>` pins the idempotency id (a fresh one is generated otherwise), `-EventTimeNs <signed-ns>` sets the record's event time, `-KeyBase64` supplies a binary key, `-PayloadBase64`/`-PayloadFile`/`-PayloadStdin` are alternate payload sources (exactly one payload source is required), and `-Metadata 'name=base64'` attaches record Metadata. If every endpoint fails, the error names the request_id — retry the same id and the server deduplicates rather than double-committing.

## Any language can use the API

JavaScript:

```javascript
const response = await fetch("http://localhost:7071/v1/admin/commands", {
  method: "POST",
  headers: {
    authorization: `Bearer ${process.env.WHITEWATER_API_KEY}`,
    "content-type": "application/json",
  },
  body: JSON.stringify({
    commands: [{ command: "show", kind: "feeds" }],
  }),
});

if (!response.ok) throw new Error(await response.text());
console.log(await response.json());
```

Python:

```python
import os
import requests

response = requests.post(
    "http://localhost:7071/v1/admin/commands",
    headers={"Authorization": f"Bearer {os.environ['WHITEWATER_API_KEY']}"},
    json={"commands": [{"command": "show", "kind": "feeds"}]},
    timeout=10,
)
response.raise_for_status()
print(response.json())
```

## Error behavior

```text
400 Bad Request         invalid WCL, command, name, or resource state
401 Unauthorized        missing or invalid Bearer API key
503 Service Unavailable Admin API key not configured
500 Internal Error      storage, serialization, or unexpected controller failure
```

Errors use:

```json
{
  "error": "human-readable message"
}
```

## Security roadmap

The development key proves the authenticated interface boundary. Production identity still requires:

- Hashed, persisted API-key records
- Key IDs and one-time secret display
- Rotation and overlapping validity
- Expiry
- Revocation
- Role attachment
- Per-command authorization enforcement
- Audit events
- TLS-only transport
- Rate limiting
- Replicated authentication state

Until these exist, the current Admin key is one Riverbed-wide prototype credential. Namespace grants are modeled and explainable but are not yet enforced against that credential.

## Control-plane roadmap

The API contract is designed to remain while implementation authority changes:

```text
Today:
Admin API -> ControlController -> local catalog file

Later:
Admin API -> ControlController -> Raft proposal -> replicated catalog
```

Future additions include idempotency keys, atomic command batches, dry-run plans, pagination, watch streams, signed audit records, and generated clients for supported languages.
