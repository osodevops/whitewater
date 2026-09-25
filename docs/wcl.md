# Whitewater Control Language v0

Whitewater Control Language (WCL) is a SQL-inspired declarative language for logical control-plane resources. WCL is submitted through the authenticated Admin API, parses into a typed command model, and executes through the same `ControlController` used by typed HTTP commands, `wwctl`, SDKs, and future MCP interfaces.

See the [Whitewater Admin API](admin-api.md) for authentication, typed JSON commands, and client examples.

## Prototype status

WCL v0 uses the persistent three-Node Control Plane in the standard development Fabric. Requests may reach any Node, are forwarded to the elected leader, and complete after majority commit.

Current limitations:

- Multi-statement scripts commit one successful statement at a time through the Control Plane; they are not atomic control transactions yet.
- Namespace grants can be created and explained but are not enforced until API-key principals are implemented.
- `DROP FEED` tombstones catalog metadata and does not purge stored history.
- Writer and Reader definitions are logical resources; runtime sessions are not implemented yet.
- Named Reader Cursors are replicated Control Plane metadata; runtime Reader sessions and Subscription leases are not implemented yet.
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
    "script": "CREATE SPACE orders; SHOW SPACES;"
  }'
```

## Execute with wwctl

```bash
export WHITEWATER_API_KEY='whitewater-local-development-admin-key'

wwctl
```

```text
whitewater> SHOW SPACES;
whitewater> CREATE SPACE orders;
whitewater> \\help
```

One-shot and file execution:

```bash
wwctl --execute "SHOW SPACES;"
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

Spaces, Feeds, Writers, Readers, and Roles use lowercase dotted names:

```regex
^[a-z][a-z0-9]*(\.[a-z][a-z0-9]*)*$
```

Maximum encoded length is 512 bytes.

A Feed must belong to an existing Space. The longest matching Space prefix owns the Feed.

```sql
CREATE SPACE commerce;
CREATE SPACE commerce.orders;
CREATE FEED commerce.orders.created;
```

`commerce.orders.created` belongs to `commerce.orders`.

## Create resources

### Space

```sql
CREATE SPACE orders;
```

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

### Role

```sql
CREATE ROLE orderanalytics;
```

## Namespace permissions

```sql
GRANT READ
  ON NAMESPACE orders.*
  TO ROLE orderanalytics;
```

Multiple actions:

```sql
GRANT READ, WRITE
  ON NAMESPACE orders.*
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
SHOW SPACES;
SHOW FEEDS;
SHOW WRITERS;
SHOW READERS;
SHOW ROLES;
SHOW GRANTS;
```

Dropped resources are excluded from `SHOW` results.

## Describe resources

```sql
DESCRIBE SPACE orders;
DESCRIBE FEED orders.created;
DESCRIBE WRITER checkout;
DESCRIBE READER audit;
DESCRIBE ROLE orderanalytics;
```

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
RENAME ROLE orderanalytics TO businessanalytics;
```

A Space containing active Feeds cannot currently be renamed. Rename or move its Feeds first.

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
DROP WRITER checkout;
DROP FEED orders.created;
DROP ROLE orderanalytics;
DROP SPACE orders;
```

Safety behavior:

- A Feed with active Writers or Readers cannot be dropped.
- Dropping a Feed does not purge physical history.
- A Space with active Feeds cannot be dropped.
- Dropping a Role also removes its grants.
- Dropped names remain reserved in v0.

Permanent purge syntax is deliberately absent.

## Complete setup example

```sql
CREATE SPACE orders;

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
  ON NAMESPACE orders.*
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
