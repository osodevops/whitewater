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

A Subscription belongs to the same Domain as its Feed. It currently remains `declared`: it does not create an internal Feed, permit joining members, or establish shared Cursor progress yet. Existing named Readers remain independent and are not silently converted into shared Subscriptions.

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
```

## Inspect Active Range placement

Placement inspection is an authenticated operator control. Application Writers and Readers do not receive physical topology.

```sql
INSPECT PLACEMENT FOR FEED orders.created;
```

The standard three-Node Riverbed creates one RF3 Active Range at Feed creation. The legacy `TRANSFER ACTIVE RANGE OWNERSHIP` syntax is still recognized but deliberately rejected: metadata-only transfer could promote a follower without verified committed history. Local owner-movement verification exists, but an authenticated live operator cutover is not yet available.

## Storage capacity Nodes

Storage Node registration and retirement are authenticated operator controls. Application Writers and Readers do not receive physical topology.

```sql
REGISTER STORAGE NODE storage-4 AT http://storage-4:7070;
SHOW STORAGE NODES;
RETIRE STORAGE NODE storage-4;
```

Configured Control Plane Nodes seed the eligible storage pool, but the pool is no longer limited to voters. `REGISTER STORAGE NODE` persists a storage-capable Node in the catalog without making it a Control Plane voter. Re-registering the same Node and endpoint is idempotent; registering an existing Node with a different endpoint is rejected so a typo cannot silently redirect placement traffic.

`SHOW STORAGE NODES` reports each Node's identity, `source` (`configured` or `registered`), endpoint, and current `eligible` flag. Registered eligible Nodes participate in RF3 Active Range placement, which spreads each new Feed's replicas and Append Owner across the widened pool rather than always taking the first three Nodes.

`RETIRE STORAGE NODE` removes an unreferenced Node's eligibility and persists across restart. Retirement is refused while the Node still holds an Active Range assignment; move or drain every placement off the Node first, so scale-in never reduces durability.

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
CREATE INDEX
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
