# Whitewater Admin API v1

The Whitewater Admin API is the only remote entry point for administrative commands. WCL, `wwctl`, Rust clients, future SDKs, UIs, operators, and MCP tools all use this API and converge on the same typed `ControlController`. Front ends contain no catalog authority and may be replaced, rewritten, or moved to separate projects without changing Whitewater server semantics.

```text
WCL / wwctl / Rust AdminClient / any HTTP client / future MCP
    -> authenticated Admin API
    -> typed Command
    -> ControlController
    -> control-plane transaction
```

The standard three-Node Fabric uses a persistent OpenRaft Control Plane. Commands submitted to any Node are forwarded to the elected leader and return only after majority commit. Nodes started without Control Plane configuration retain an explicit `local_prototype` fallback for isolated tests.

## Authentication

Every Admin API request requires:

```http
Authorization: Bearer <api-key>
```

The Node reads its expected key from:

```text
FINNSTREAM_ADMIN_API_KEY
```

Keys shorter than 24 characters are rejected during configuration. The server stores only an in-memory BLAKE3 digest and compares supplied digests without early exit.

If no key is configured, Admin API requests return `503 Service Unavailable`. Missing or incorrect credentials return `401 Unauthorized`.

Docker development files use this clearly non-production fallback:

```text
whitewater-local-development-admin-key
```

Override it before starting Compose:

```bash
export FINNSTREAM_ADMIN_API_KEY='replace-with-a-random-development-key'
docker compose up --build -d
```

PowerShell:

```powershell
$env:FINNSTREAM_ADMIN_API_KEY = 'replace-with-a-random-development-key'
docker compose up --build -d
```

Production must supply a generated secret through the orchestrator or secret manager. There is no production fallback in the binary.

## Replicated Feed records

Append through any Node with `POST /v1/feeds/append`. Read majority-committed records through any current replica:

```http
GET /v1/feeds/records?feed=orders.events&limit=100
Authorization: Bearer <api-key>
```

Continue after an opaque Cursor without changing another caller's position:

```http
GET /v1/feeds/records?feed=orders.events&after=<cursor>&limit=100
Authorization: Bearer <api-key>
```

Only records at or below the local durable CommitPosition are returned. A Cursor that is unknown, uncommitted, or belongs to another Feed is rejected rather than exposing an uncommitted tail.

## Execute WCL

```http
POST /v1/admin/wcl
Authorization: Bearer <api-key>
Content-Type: application/json
```

```json
{
  "request_id": "018f5f65-5d87-7c2e-a9a3-3af92c48ed21",
  "script": "CREATE SPACE orders; CREATE FEED orders.created; SHOW FEEDS;"
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
      "command": "create_space",
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

## Typed command shapes

### Create

```json
{ "command": "create_space", "name": "orders" }
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

The typed Admin API can commit an idempotent Space-scoped StateStore declaration with either an explicit same-Space source Feed or a manual source:

```json
{ "command": "define_state_store", "name": "accounts.users", "source": { "kind": "manual" } }
```

```json
{ "command": "define_state_store", "name": "accounts.profiles", "source": { "kind": "feed", "feed": "accounts.events" } }
```

The response reports `stage: "declared"` and an immutable `store_id`. This is **not** `CREATE STATESTORE`: there is no StateStore write, lookup, RF3 replica, or Pipe execution API yet. The catalog rejects unknown source Feeds and Feeds outside the StateStore's Space. Retry a timed-out declaration with the same request ID; do not attempt to use a `declared` store as live state.

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

Kinds are `space`, `feed`, `writer`, `reader`, and `role`.

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

Kinds are `spaces`, `feeds`, `writers`, `readers`, `roles`, and `grants`.

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

Delivered and acknowledged Cursors are separate. Fetch advances delivered progress only; acknowledgement is explicit and cumulative. Reopening a persistent Reader increments its epoch, fences the old session, resets delivered progress to the acknowledged Cursor, and resumes safely.

Anonymous temporary Readers use `POST /v1/readers/temporary/fetch` with `feed`, optional `after`, signed nanosecond `after_event_time_ns`, `limit`, `tail`, `new_only`, and bounded `wait_ms`. They retain no server-side progress. `new_only` atomically returns the current end Cursor without historical records; the caller then waits after that Cursor.

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

### Inspect and transfer Active Range placement

Placement is an authenticated operator view. Writers and Readers never receive owner or replica topology.

```json
{
  "command": "inspect_placement",
  "feed": "orders.created"
}
```

The result contains the internal RangeId, generation, current owner, RF3 replica set, and ownership epoch. The standard development Fabric currently treats its three statically configured Control Plane Nodes as storage-capable placement candidates.

Ownership transfer is epoch-fenced and limited to a current replica:

```json
{
  "command": "transfer_active_range_ownership",
  "feed": "orders.created",
  "owner": "control-2"
}
```

Repeating the same request ID returns the original result without incrementing the epoch twice.

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

The current HTTP staging prototype transfers one bounded record per internal request and refuses ranges above 10,000 committed records; the original RF3 assignment remains active on refusal. The prepared plan remains visible in `INSPECT PLACEMENT`; after confirming activation was not submitted, an operator can clear it with the typed `abort_follower_move` command and its `plan_id`. Checkpointed streaming for larger histories and a foreground-SLO-aware movement budget remain planned. The regular three-Node development Fabric has no spare eligible fourth Node. `compose.m4-move.yml` provides an **isolated, test-only four-voter** Fabric for `python scripts/test-m4-follower-move.py`. This is not production role-separated storage placement. Movement of the append owner and automatic Node drain remain planned.

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
use finnstream::{
    admin::AdminClient,
    control::{Command, ReaderStart, ShowKind},
};

let admin = AdminClient::new(
    "http://localhost:7071",
    std::env::var("WHITEWATER_API_KEY")?,
);

let result = admin
    .execute_commands(vec![
        Command::CreateSpace {
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
admin.create_space("orders").await?;
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
whitewater> CREATE SPACE orders;
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
wcl-cli -Execute 'CREATE SPACE orders;' -RequestId '018f5f65-5d87-7c2e-a9a3-3af92c48ed21'
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

Endpoint failover is safe for the standard three-Node Fabric because every Node routes through one replicated Control Plane. Nodes deliberately started without Control Plane configuration report `local_prototype`; do not mix those isolated test Nodes into an endpoint list.

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

Until these exist, the current Admin key is one Fabric-wide prototype credential. Namespace grants are modeled and explainable but are not yet enforced against that credential.

## Control-plane roadmap

The API contract is designed to remain while implementation authority changes:

```text
Today:
Admin API -> ControlController -> local catalog file

Later:
Admin API -> ControlController -> Raft proposal -> replicated catalog
```

Future additions include idempotency keys, atomic command batches, dry-run plans, pagination, watch streams, signed audit records, and generated clients for supported languages.
