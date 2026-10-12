# Whitewater Control Language v0



Whitewater Control Language (WCL) is a SQL-inspired declarative language for logical control-plane resources. WCL is submitted through the authenticated Admin API, parses into a typed command model, and executes through the same `ControlController` used by typed HTTP commands, `wwctl`, SDKs, and future MCP interfaces.



See the [Whitewater Admin API](admin-api.md) for authentication, typed JSON commands, and client examples.



## Prototype status



WCL v0 uses the persistent three-Node Control Plane in the standard development Riverbed. Requests may reach any Node, are forwarded to the elected leader, and complete after majority commit.



Current limitations:



- Multi-statement scripts commit one successful statement at a time through the Control Plane; they are not atomic control transactions yet.

- Namespace grants can be created and explained but are not enforced until API-key principals are implemented.

- `DROP FEED` tombstones catalog metadata and does not purge stored history.

- Writer and Reader definitions are logical resources; prototype Writer and Reader sessions exist over the authenticated API.

- Named Reader Cursors are replicated Control Plane metadata; Subscription definitions are metadata-only (`stage=declared`), with shared progress and member leases still unimplemented.

- WCL-created Feeds use immutable FeedId-based physical storage names, so rename does not move files.



Every response includes:



```json

{

  "authority": "control_plane",

  "warning": ""

}

```



## Execute WCL over HTTP



```bash

curl -X POST http://localhost:7071/v1/admin/wcl \

  -H 'authorization: Bearer whitewater-local-development-admin-key' \

  -H 'content-type: application/json' \

  -d '{

    "script": "CREATE DOMAIN orders; SHOW DOMAINS;"

  }'

```



## Execute with wwctl



```bash

export WHITEWATER_API_KEY='whitewater-local-development-admin-key'



wwctl

```



```text

whitewater> SHOW DOMAINS;

whitewater> CREATE DOMAIN orders;

whitewater> \\help

```



One-shot and file execution:



```bash

wwctl --execute "SHOW DOMAINS;"

wwctl --file setup.wcl



wwctl --endpoint http://localhost:7072 \

  --execute "DESCRIBE FEED orders.created;"

```



`WHITEWATER_ENDPOINT` changes the default endpoint. The host default is `http://127.0.0.1:7071`.



The Docker image also contains `wwctl`:



```bash

docker exec <node-container> \

  wwctl --endpoint http://127.0.0.1:7070 \

  --api-key 'whitewater-local-development-admin-key' \

  --execute "SHOW FEEDS;"

```



## Names



Domains, Feeds, Writers, Readers, and Roles use lowercase dotted names:



```regex

^[a-z][a-z0-9]*(\.[a-z][a-z0-9]*)*$

```



Maximum encoded length is 512 bytes.



A Feed must belong to an existing Domain. The longest matching Domain prefix owns the Feed.



```sql

CREATE DOMAIN commerce;

CREATE DOMAIN commerce.orders;

CREATE FEED commerce.orders.created;

```



`commerce.orders.created` belongs to `commerce.orders`.



## Create resources



### Domain



```sql

CREATE DOMAIN orders;

```



`CREATE SPACE`, `SHOW SPACES`, `DESCRIBE SPACE`, `RENAME SPACE`, and `DROP SPACE` remain compatibility aliases for the same Domain. They do not create a second namespace or change existing Feed IDs, request IDs, or stored Cursors. New scripts should use `DOMAIN`/`DOMAINS`.



### Feed



```sql

CREATE FEED orders.created;

```



A Feed receives an immutable FeedId and FeedId-based storage location.



### Writer



```sql

CREATE WRITER checkout

  TO orders.created;

```



Writer identity remains attached to FeedId across Feed rename.



### Reader



```sql

CREATE READER audit

  FROM orders.created

  START AT BEGINNING;

```



Start at current time:



```sql

CREATE READER analytics

  FROM orders.created

  START AT NOW;

```



Start from an opaque Cursor:



```sql

CREATE READER recovery

  FROM orders.created

  START AT CURSOR 'AbGTj_j2...';

```



Every Reader has independent durable prototype progress.



### Subscription declaration



```sql

CREATE SUBSCRIPTION orders.billing

  FROM orders.created

  START AT BEGINNING;

```



A Subscription belongs to the same Domain as its Feed. The source can instead be the whole Domain — covering every current and future Feed under immutable Domain identity, with per-Feed progress tracked incrementally (no global cross-Feed order is promised):

```sql
CREATE SUBSCRIPTION orders.everything
  FROM DOMAIN orders
  START AT BEGINNING;
```

Either way the Subscription remains `declared` until member delivery lands: it does not create an internal Feed, permit joining members, or establish shared Cursor progress yet — member Feeds attach in the same catalog commit that creates them (existing Feeds honor the declared start, newly discovered Feeds start at their first retained record), a Feed moved to another Domain drains its fenced membership rather than silently discarding pending work, and `DESCRIBE SUBSCRIPTION` reports per-Feed `member_feeds` states; member join itself stays refused honestly until shared delivery lands. Existing named Readers remain independent and are not silently converted into shared Subscriptions.



### Pipe declaration



```sql

CREATE PIPE orders.enrich

  FROM SUBSCRIPTION orders.billing

  TO FEED orders.enriched;

```



A Pipe declares a managed consume-process-append binding: the named Subscription's durable frontier drives the input, and every delivered record flows through the declared operation into the declared output. Two operations exist:

- `TO FEED` - `forward`: copy each record to the output Feed unchanged.
- `TO FEED ... ENRICH` - point-lookup enrichment (below).
- `TO STATESTORE` - `window`: accumulate each record into event-time panes of a managed StateStore view, grouped by a projection:

```sql
CREATE PIPE orders.minute_totals
  FROM SUBSCRIPTION orders.billing
  TO STATESTORE orders.minute_totals
  WINDOW 5m BY JSON 'customer' SUM JSON 'amount';
```

- `TO FEED ... ENRICH STATESTORE <store> [STRICT] [NAMED '<key>'] BY <spec> [MISS DROP]` - `enrich`: resolve one StateStore row per record by the projected key and copy its payload into the output record's Metadata (`enrich.payload`, plus `enrich.matched` = `"true"`/`"false"` and `enrich.seq` = the serving replica's applied journal sequence as versioned evidence). `NAMED '<key>'` resolves through one of the store's declared secondary named keys (the same `by_named` postings contract public row reads use) instead of the primary key. Misses and unprojectable keys forward with `enrich.matched = false` by default; `MISS DROP` consumes a miss without emitting — the frontier still commits inside the same effect, so a dropped miss is an acknowledged, replay-identical decision rather than a lost record. An unreachable store makes the drive retry rather than journal a fake miss. The resolution is journaled inside the effect's `Declare`, so applied outputs never re-resolve and concurrent drives converge on whichever `Declare` committed first. Without `STRICT` (the default `Latest`) a reachable replica's applied state answers; `STRICT` requires the replica to have applied through its committed frontier — lagging replicas are skipped and a fully lagging store makes the drive retry rather than journal a stale row.

```sql
CREATE PIPE orders.customer_enrich
  FROM SUBSCRIPTION orders.billing
  TO FEED orders.enriched
  ENRICH STATESTORE orders.profiles STRICT NAMED 'email' BY JSON 'customer' MISS DROP;
```

The window clause takes a duration (`<n>ms|s|m|h|d`), an optional clock policy (`EVENT` or `INGEST`, defaulting to `EVENT` - `event_time_ns` vs `ingest_time_ns`), `BY <spec>` for the grouping projection, and an aggregation of `COUNT`, `SUM <spec>`, or `AVG <spec>`; specs use the same `KEY`, `METADATA 'name'`, `JSON 'path'` vocabulary as Index fields. The target StateStore must declare non-unique named keys `key` and `pane`, which is what makes pane rows enumerable by group or pane index without knowing composite row keys. Window state advances exactly once per committed input inside the effect's atomic boundary; watermarks, late-event revisions, and window-close emission are follow-on work.

A Pipe belongs to the same Domain as its input Subscription and output; `forward` Pipes may not write back to their own input Feed. The consumed Subscription and the output Feed cannot be dropped while the Pipe is active; drop the Pipe first.

Each Node runs a Pipe supervisor loop (`WHITEWATER_PIPE_DRIVE_INTERVAL_MS`, default 1000ms, minimum 100ms) that drives every active Pipe whose consumed Subscription's progress owner is that Node; ownership moves simply move the drive to the new owner. The internal `POST /internal/pipe/drive` route runs one bounded consume-declare-apply cycle on the progress owner for explicit drives and diagnostics.

A cycle fetches a page after the committed frontier, commits a `Declare` carrying the frontier evidence and per-record deterministic writer identities, appends outputs through the normal owner-routed append path, commits the consumed frontier, then marks the effect `Applied`. Drivers do not hold a member work lease; concurrent drives over the same frontier produce identical journal mutations, and a stale frontier loses the compare-and-set when committing progress. When no member has ever joined the consumed Subscription, the driver bootstraps the declared-start frontier itself under a deterministic request identity.

Two Pipes consuming the same Subscription share its frontier and split deliveries like cooperating members of one consumer group; declare one Subscription per Pipe for independent fan-out.



```sql

DROP PIPE orders.enrich;

```



### Role



```sql

CREATE ROLE orderanalytics;

```



## Namespace permissions



```sql

GRANT READ

  ON NAMEDOMAIN orders.*

  TO ROLE orderanalytics;

```



Multiple actions:



```sql

GRANT READ, WRITE

  ON NAMEDOMAIN orders.*

  TO ROLE orderapplication;

```



Available v0 actions:



```text

READ

WRITE

MANAGE

```



Namespace `orders.*` matches descendants such as `orders.created` and `orders.eu.created`. An exact namespace without `.*` matches only the exact Feed name.



WCL v0 uses default deny and allow grants. Explicit deny and permission enforcement are future control-plane work.



## Explain access



```sql

EXPLAIN ACCESS

  FOR ROLE orderanalytics

  ACTION READ

  ON FEED orders.created;

```



Example result data:



```json

{

  "allowed": true,

  "role": "orderanalytics",

  "action": "read",

  "feed": "orders.created",

  "matched_grants": [],

  "default": "deny"

}

```



This currently explains catalog grants; it does not authenticate the caller.



## Show resources



```sql

SHOW DOMAINS;

SHOW FEEDS;

SHOW WRITERS;

SHOW READERS;

SHOW SUBSCRIPTIONS;

SHOW ROLES;

SHOW GRANTS;

SHOW INDEXES;

SHOW STATESTORES;

```



Dropped resources are excluded from `SHOW` results.



## Describe resources



```sql

DESCRIBE DOMAIN orders;

DESCRIBE FEED orders.created;

DESCRIBE WRITER checkout;

DESCRIBE READER audit;

DESCRIBE SUBSCRIPTION orders.billing;

DESCRIBE ROLE orderanalytics;

DESCRIBE INDEX orders.by.customer;

```



## Declare a secondary Index

Secondary Indexes are declared via WCL `CREATE INDEX` or the typed
Control Plane command surface (`POST /v1/admin/commands`), then
inspected via `SHOW INDEXES;` and `DESCRIBE INDEX <name>;`. WCL form:

```sql
CREATE INDEX orders.by.customer ON orders.created
    FIELDS KEY, METADATA 'tenant', JSON 'customer.id';
```

The typed-command equivalent is Domain-scoped, names its source Feed,
and carries the same fields/extractors plus collation and consistency
intent:

```json
{"command": "define_index",
 "name": "orders.by.customer",
 "feed": "orders.created",
 "fields": [
   {"kind": "metadata", "name": "tenant"},
   {"kind": "projection", "extractor": "json.path", "version": 1,
    "field": "customer.id"}],
 "collation": "binary",
 "consistency": "eventual"}
```

`fields` take 1–16 entries of `key`, `metadata`, or versioned
`projection` extractor specs. `collation` defaults to `binary` and
`consistency` to `eventual`; `strict` is accepted as intent. `unique`
is rejected until the replicated conditional-claim protocol exists.

Declared Indexes persist in the replicated catalog and snapshots. The
stage stays `declared` until a rebuild materializes a generation:

```sql
REBUILD INDEX orders.by.customer;
REBUILD INDEXES IN DOMAIN orders;    -- SPACE is accepted as an alias
```

`REBUILD INDEX` moves the definition to `building` with a generation
number; every Data Node then materializes that generation's postings as a
**shadow** — derived posting ids coexist with the serving generation, so
the previous Index state is never mutated mid-rebuild. Each Node resumes
from its durable shadow Cursor across restarts; retained-history gaps or
a missing source Feed refuse the pass explicitly rather than activating a
partial Index. The first Node to consume the committed frontier commits
`CompleteIndexBuild`, which flips `building` to `active` in one majority
commit — readers never observe a partially-activated generation — and
retires prior-generation postings. Concurrent completers are superseded
idempotently. Nodes that activate before building locally keep applying
committed history until their shadow covers the committed
`applied_cursor`.

Index **queries** are served by `GET INDEX` and the typed
`POST /v1/indexes/query` endpoint — both resolve the logical Index name,
select the `active` generation, and answer from materialized postings.
The ingress Node serves locally when it holds the postings; under
`STRICT` a Node still catching up to the activation frontier waits a
bounded `WAIT` (default 500 ms, cap 2000 ms) and otherwise forwards to a
fresher peer — or fails `index_behind` rather than serve stale data.
Physical ranges and replica identity stay internal; the response reports
`applied_cursor`, `target_cursor`, `behind`, and `served_by` for
verifiable freshness.

```sql
GET INDEX orders.by.customer EQ 42, 'acme';
GET INDEX orders.by.region PREFIX 'US' MATCH 'sea' LIMIT 100;
GET INDEX orders.by.score RANGE 0 TO 500 STRICT WAIT 250;
```

Predicates: `EQ` supplies one typed literal per declared field
(comma-separated); `PREFIX` matches leading complete fields plus an
optional `MATCH 'text'` prefix on the next field; `RANGE` bounds the
first field between inclusive `lo TO hi`. Literals are `42` (i64),
`'text'` (quoted, `''` escapes a quote), `TEXT '42'` (force a digit
string to text), `TRUE`/`FALSE`, `NULL`, and `B64 'aGk='` for raw bytes.
`LIMIT` defaults to 256, bounded 1..=1000; result payloads cap at 16 MiB.
A `GET INDEX` statement is read-only and cannot be mixed with mutating
statements in one script; `declared`/`building` Indexes refuse with
`index_not_ready` (HTTP 409) instead of returning a misleadingly empty
result.

`SHOW INDEXES;` and `DESCRIBE INDEX <name>;` report `stage` (`declared`,
`{"building":{"generation":N}}`, or `{"active":{"generation":N}}`) and
`applied_cursor`. `RENAME INDEX` and `DROP INDEX` work on declared
definitions.

## Declare a StateStore

StateStores are declared via WCL `DEFINE STATESTORE` or the typed
command surface (`POST /v1/admin/commands` with `define_state_store`),
then inspected via `SHOW STATESTORES;` and `DESCRIBE STATESTORE <name>;`.
WCL form:

```sql
DEFINE STATESTORE accounts.employees;
DEFINE STATESTORE accounts.employees KEYS department, passport UNIQUE;
DEFINE STATESTORE accounts.tagged FROM FEED accounts.events
    KEYS passport = METADATA 'pp' UNIQUE, department = JSON 'dept';
```

`KEYS` lists up to 10 named secondary lookup keys; `UNIQUE` enforces
one primary row per value (first-committed-wins by journal order),
while non-unique keys map one value to many primaries. On a `FROM FEED`
store every named key needs an `= <extractor>` projection telling replay
how to derive its typed values — `KEY` (the record's Key), `METADATA
'name'`, or `JSON 'dotted.path'`, the same extractor spellings `CREATE
INDEX FIELDS` accepts. Extractors on a manual store are refused: `put`
mutations carry named values directly.

```json
{"command": "define_state_store", "name": "accounts.tagged",
 "source": {"kind": "feed", "feed": "accounts.events"},
 "named_keys": [
   {"name": "passport", "unique": true,
    "extractor": {"kind": "metadata", "name": "pp"}},
   {"name": "department", "unique": false,
    "extractor": {"kind": "projection", "extractor": "json",
                  "version": 1, "field": "dept"}}]}
```

Declared stores commit through the Control Plane majority, persist in
the replicated catalog, and carry an immutable `store_id`; the source
Feed must live in the same Domain. Manual stores take journaled
`put_row`/`delete_row`/`clear` mutations over
`POST /v1/state-stores/*`; feed-sourced stores materialize committed
history and refuse direct mutation. `DESCRIBE STATESTORE` reports
`stage` (`declared` or `dropped`); `DROP STATESTORE` tombstones the
definition so the name cannot be recreated, and `SHOW STATESTORES`
excludes dropped definitions.

## Inspect Active Range placement



Placement inspection is an authenticated operator control. Application Writers and Readers do not receive physical topology.



```sql

INSPECT PLACEMENT FOR FEED orders.created;

```



The standard three-Node Riverbed creates one RF3 Active Range at Feed creation. The legacy `TRANSFER ACTIVE RANGE OWNERSHIP` syntax is still recognized but deliberately rejected: metadata-only transfer could promote a follower without verified committed history. Local owner-movement verification exists, but an authenticated live operator cutover is not yet available.



## Storage capacity Nodes



Storage Node registration and retirement are authenticated operator controls. Application Writers and Readers do not receive physical topology.



```sql

REGISTER STORAGE NODE storage-4 AT https://storage-4:7271;

REGISTER STORAGE NODE storage-4 AT https://storage-4:7271 WITH CERT PIN 0123abcd...|fedcba98...;

SHOW STORAGE NODES;

RETIRE STORAGE NODE storage-4;

```



Configured Control Plane Nodes seed the eligible storage pool, but the pool is no longer limited to voters. `REGISTER STORAGE NODE` persists a storage-capable Node in the catalog without making it a Control Plane voter. The endpoint becomes the Node's internal-plane address: every Node-to-Node transport (replication, recovery, repair, movement, Subscription progress, pressure sampling) resolves configured voter endpoints first and falls back to the registered catalog record, so a registered Node is reachable without restarting or reconfiguring the voters.



`WITH CERT PIN` declares the Node's internal mTLS certificate fingerprints as blake3-256 hex (64 digits each), separated by `|`. The mTLS listener accepts a peer certificate when its fingerprint is pinned to the Node either in static configuration or in the replicated catalog, and every pin may belong to exactly one Node. Re-registering the same Node and endpoint replaces the pin set so certificates rotate without draining: supply old and new pins while the new certificate rolls out, then re-register with only the new pin once rotation completes. Registering an existing Node with a different endpoint is rejected so a typo cannot silently redirect placement traffic; retire the Node first to change its endpoint.



`SHOW STORAGE NODES` reports each Node's identity, `source` (`configured` or `registered`), endpoint, and current `eligible` flag. Registered eligible Nodes participate in RF3 Active Range placement, which spreads each new Feed's replicas and Append Owner across the widened pool rather than always taking the first three Nodes.



`RETIRE STORAGE NODE` removes an unreferenced Node's eligibility and persists across restart. Retirement is refused while the Node still holds an Active Range assignment; move or drain every placement off the Node first, so scale-in never reduces durability.



### Draining a Node before removal



```sql

DRAIN STORAGE NODE storage-4;

INSPECT DRAIN FOR STORAGE NODE storage-4;

UNDRAIN STORAGE NODE storage-4;

```



`DRAIN STORAGE NODE` persists a draining flag: the Node keeps serving its existing replicas but is immediately excluded from every new placement and from move/replacement targeting. `SHOW STORAGE NODES` reports the `draining` flag alongside `eligible`.



`INSPECT DRAIN FOR STORAGE NODE` is a read-only dry run returning the ordered move list required to vacate the Node — `PREPARE OWNER MOVE` before `PREPARE FOLLOWER MOVE` for owned ranges, `RECOVER SUBSCRIPTION PROGRESS OWNER` before `MOVE SUBSCRIPTION PROGRESS REPLICA` for owned progress shards — plus an `unplannable` list when no eligible replacement exists and a `ready_to_retire` flag once nothing references the Node.



`UNDRAIN STORAGE NODE` clears the flag and restores eligibility. `RETIRE` clears it too, so a fully drained Node can be removed in the same session.



`StorageDrainExecutor` (library) executes the inspected plan one move at a time through the same verified owner/follower and progress-replica machinery used by operator movement — never metadata-only swaps. It re-plans after every committed step, reports completed moves, `unplannable` references, and `ready_to_retire`, so automation can run a bounded drain loop rather than a blind multi-step script.



When the Control Plane is configured and `FINNSTREAM_ADMIN_API_KEY` is set, the leader runs `StorageDrainSupervisor` automatically: every few seconds it walks the marked Nodes and executes their plans through `AdminDrainDriver`, which invokes the same admin move endpoints an operator would call. The log reports each completed move, blocked `unplannable` references, and the `safe-to-remove` signal once a Node is fully drained; `RETIRE STORAGE NODE` then completes removal. The supervisor honors a movement disruption budget (`WHITEWATER_DRAIN_MOVE_BUDGET`, default 2): when that many Active Range move plans are already in flight — including plans wedged by a crashed driver — supervised drains wait instead of stacking freezes onto unresolved movement.



## Rename resources



```sql

RENAME FEED orders.created

  TO orders.accepted;

```



The FeedId and physical storage name remain unchanged. Writers and Readers continue to reference the same FeedId.



Other logical identities can also be renamed:



```sql

RENAME WRITER checkout TO checkoutapi;

RENAME READER audit TO complianceaudit;

RENAME SUBSCRIPTION orders.billing TO orders.billingapi;

RENAME ROLE orderanalytics TO businessanalytics;

```



A Domain containing active Feeds cannot currently be renamed. Rename or move its Feeds first.



## Writer sessions



```sql

OPEN WRITER SESSION checkout;

ALLOCATE WRITER SEQUENCE checkout EPOCH 1;

DESCRIBE WRITER checkout;

REVOKE WRITER SESSION checkout EPOCH 1;

```



Opening a new session increments the Writer epoch and fences every previous session. Sequence allocation is persisted by the Control Plane and idempotent when the caller retries with the same request ID. Revoked or stale epochs cannot allocate further sequences.



## Seek Readers



```sql

SEEK READER audit TO BEGINNING;

SEEK READER audit TO NOW;

SEEK READER audit TO CURSOR 'AbGTj_j2...';

```



Seeking one Reader does not affect another Reader.



## Drop resources



```sql

DROP READER audit;

DROP SUBSCRIPTION orders.billing;

DROP WRITER checkout;

DROP FEED orders.created;

DROP ROLE orderanalytics;

DROP DOMAIN orders;

```



Safety behavior:



- A Feed with active Writers or Readers cannot be dropped.

- Dropping a Feed does not purge physical history.

- A Domain with active Feeds cannot be dropped.

- Dropping a Role also removes its grants.

- Dropped names remain reserved in v0.



Permanent purge syntax is deliberately absent.



## Complete setup example



```sql

CREATE DOMAIN orders;



CREATE FEED orders.created;



CREATE WRITER checkout

  TO orders.created;



CREATE READER audit

  FROM orders.created

  START AT BEGINNING;



CREATE READER analytics

  FROM orders.created

  START AT NOW;



CREATE ROLE orderapplication;



GRANT READ, WRITE

  ON NAMEDOMAIN orders.*

  TO ROLE orderapplication;



SHOW FEEDS;

SHOW WRITERS;

SHOW READERS;

SHOW GRANTS;



EXPLAIN ACCESS

  FOR ROLE orderapplication

  ACTION WRITE

  ON FEED orders.created;

```



## Parser behavior



- Keywords are case-insensitive.

- Resource names are lowercase and case-sensitive.

- Statements are separated by semicolons.

- Single-quoted values preserve spaces and semicolons.

- Two adjacent single quotes represent one quote inside a quoted value.

- A trailing semicolon is optional.



## Planned language growth



The next useful statements are:



```sql

CREATE SUBSCRIPTION

CREATE PIPE

CREATE API KEY

DENY

REVOKE

SHOW EFFECTIVE PERMISSIONS

EXPLAIN RENAME

READ

TAIL

GET

BEGIN CONTROL

COMMIT

ROLLBACK

```



Control transactions will require replicated metadata consensus. Data-plane append/read and Index queries will continue to use typed protocol APIs on hot paths; WCL is primarily for control, inspection, testing, and administration.

