# Whitewater

Whitewater by FinnStream is a clean-sheet distributed database and partitionless streaming platform built around `fabric -> space -> feed -> key -> cursor -> subscription`. Applications do not create or manage partitions.

Design documents:

- [Living tasks and milestones](docs/tasks.md)
- [Active Range replication contract](docs/active-range-replication.md)
- [Sequence diagrams for writes, reads, retries, ownership, and Readers](docs/sequence-diagrams.md)
- [Whitewater skills and contribution map](skills.md)
- [Kafka pain points Whitewater must address](docs/kafka-pain-points.md)
- [Why Whitewater and Kafka equivalents](docs/why-whitewater.md)
- [Kafka partitions versus Whitewater Active Ranges](docs/why-whitewater.md#kafka-partitions-versus-whitewater-active-ranges)
- [Core Index storage contract and proposed Fjall key layout](docs/why-whitewater.md#index-storage-contract-and-fjall-layout)
- [Whitewater Streams and cross-language client contract](docs/streams-clients.md)
- [Humane operational and developer experience](docs/operational-experience.md)
- [Proposed Whitewater Operations Advisor](docs/operations-advisor.md)
- [Reddit community introduction and pinned launch post](docs/reddit-whitewater-streams-introduction.md)
- [Lessons retained from Apache Kafka source](docs/kafka-source-lessons.md)
- [Authenticated Whitewater Admin API v1](docs/admin-api.md)
- [Three-Node replicated Control Plane](docs/control-plane.md)
- [Whitewater Control Language v0](docs/wcl.md)
- [Whitewater architecture](docs/kafka-successor-architecture.md)

This repository contains a replicated streaming correctness foundation, internal multi-range routing, and an early dynamic-membership prototype:

- Durable checksummed binary append log
- Active Range storage with atomic state, persisted durability positions and Writer deduplication, committed segment rotation, torn-tail recovery, and safe uncommitted-tail truncation
- Isolated local Fjall Index prototype with transactional current-state primary rows and shared secondary entries; not wired to replicated Feed commits or public Index queries
- Consensus-persisted fixed RF3 Active Range placement with epoch-fenced ownership and authenticated inspection
- Authenticated bounded internal replica append protocol with exact-frame durability, checksum validation, position fencing, and structured rejection
- Owner-side concurrent RF3 replication with two-of-three durable frame and CommitPosition evidence before success
- Topology-free committed Feed reads with authenticated cross-Node owner retrieval, indexed single-range paging, and prototype durable multi-range named Reader frontiers; public/temporary long-history continuation and split/merge frontier translation remain planned
- Automatic sustained owner-failure recovery with authenticated progress collection, consensus epoch transfer, stale-owner fencing, committed-prefix preservation, and tail truncation
- Automatic restarted-replica catch-up with bounded exact-frame transfer, checksum verification, deduplication rebuild, corruption quarantine, and readiness gating
- Development-only authenticated Append Owner movement after verified frozen-boundary catch-up, with isolated three-Node live acceptance; production inter-Node mTLS and drain remain unfinished
- Hierarchical Feed namespaces with a prototype legacy stream API
- Opaque stream-scoped Cursors with independent Reader positions
- Nanosecond `event_time_ns` and `ingest_time_ns` with legacy millisecond decoding
- Producer identity and sequence deduplication
- Read-after-cursor HTTP API
- DNS-seeded node discovery, heartbeats, expiry, and graceful leave
- WCL v0 controller for Spaces, Feeds, Writers, Readers, Roles, namespace grants, rename, seek, show, describe, explain, and safe drop
- Persistent OpenRaft Control Plane with three-voter majority commit, leader election, follower forwarding, and restart recovery
- Persisted local prototype catalog fallback and `wwctl` command runner
- Three-Node and arbitrary-scale Docker development environments

The standard three-Node Fabric uses Raft consensus for control metadata and two-of-three durable Active Range quorum commits for Feed writes. This is still a prototype: production transport security, scalable cross-Node Feed reads, complete movement and drain, and broader failure acceptance remain unfinished.

## Run a development Fabric

A supported Fabric always has at least three Nodes. The standard development setup publishes Nodes on ports `7071`, `7072`, and `7073`:

```bash
docker compose up --build -d
curl http://localhost:7071/health
```

For arbitrary scale, Compose assigns each replica a random published host port while Nodes communicate over port `7070` on the internal network:

```bash
docker compose -f compose.cluster.yml up --build -d --scale node=5
docker compose -f compose.cluster.yml ps
```

Windows helper:

```powershell
./scripts/cluster.ps1 up 5
./scripts/cluster.ps1 smoke 5
./scripts/cluster.ps1 scale 8
./scripts/cluster.ps1 smoke 8
./scripts/cluster.ps1 down
```

Linux/macOS helper:

```bash
./scripts/cluster.sh up 5
./scripts/cluster.sh smoke 5
./scripts/cluster.sh scale 8
./scripts/cluster.sh down
```

Every replica resolves the `node:7070` service DNS record, joins discovered peers, exchanges known addresses, sends heartbeats, and removes members that stop responding.

## Slow automatic scaling

Nodes expose cumulative demand and storage-safety telemetry. The host-side controller samples all replicas, asks a node for a stateful hysteresis recommendation, and changes the Compose replica count by one node at a time.

```powershell
./scripts/cluster.ps1 autoscale -MinNodes 3 -MaxNodes 12
```

Defaults require approximately one minute of sustained high pressure before adding a node, five minutes of sustained low pressure before considering removal, and a two-minute cooldown after each action. Thresholds and sample windows are configurable parameters.

Scale-in is blocked unless the highest-index Compose replica reports `safe_to_remove`. In this prototype that means the node has no records. Once replicated active ranges and draining exist, this signal will represent completed ownership transfer and verified durable replicas. Scaling out currently proves orchestration and membership behavior; it does not redistribute existing streams yet.

The autoscaler runs outside data nodes, so the nodes do not receive Docker socket access.

## Whitewater Control Language

The authenticated Admin API is the product boundary; WCL, UIs, Operators, SDKs, MCP, and the CLI are replaceable front ends. Start the installed local API-only shell with:

```text
wcl-cli
```

It reads `WHITEWATER_API_KEY` and `WHITEWATER_ENDPOINTS`, or securely prompts for a missing key. The Rust `wwctl` frontend remains available from Cargo and inside the Docker image.

```text
whitewater> CREATE SPACE orders;
whitewater> CREATE FEED orders.created;
whitewater> SHOW FEEDS;
```

Write through a durable Writer session without managing sequence or topology internals:

```bash
wwctl write --writer checkout --session-epoch 1 --key order-123 --payload '{"status":"created"}'
```

Use `--payload-file`, `--payload-stdin`, `--payload-base64`, repeated `--metadata name=base64`, `--event-time-ns`, and `--request-id` for binary and retry-safe workflows.

Read committed records with a temporary Reader:

```bash
wwctl read --feed orders.events --tail --limit 10
wwctl read --feed orders.events --new-only
wwctl read --feed orders.events --after "$CURSOR" --wait 30000 --payload-only
```

Or call the controller directly:

```bash
curl -X POST http://localhost:7071/v1/admin/wcl \
  -H 'authorization: Bearer whitewater-local-development-admin-key' \
  -H 'content-type: application/json' \
  -d '{"script":"SHOW FEEDS;"}'
```

All administrative commands pass through the authenticated Admin API. Clients may submit WCL to `/v1/admin/wcl` or typed commands to `/v1/admin/commands`. See the [Admin API](docs/admin-api.md) and [Whitewater Control Language v0](docs/wcl.md). The Compose key shown above is a public local-development credential; override `FINNSTREAM_ADMIN_API_KEY` outside local testing.

## API examples

Create a Feed using the current prototype endpoint:

```bash
curl -X POST http://localhost:7070/v1/streams \
  -H 'content-type: application/json' \
  -d '{"name":"/orders/europe"}'
```

Append bytes using base64:

```bash
curl -X POST http://localhost:7070/v1/records \
  -H 'content-type: application/json' \
  -d '{
    "stream":"/orders/europe",
    "producer_id":"d2719d72-9f39-49c8-a5a4-1ea846941bad",
    "sequence":1,
    "event_time_ns":"1700000000123456789",
    "key_base64":"Y3VzdG9tZXItMTIz",
    "payload_base64":"eyJvcmRlcklkIjoiQS0xIn0=",
    "metadata_base64":{}
  }'
```

Read from the beginning:

```bash
curl 'http://localhost:7070/v1/records?stream=%2Forders%2Feurope&limit=100'
```

Continue after an opaque Cursor by adding `&after=<cursor>`. Each Reader can retain and seek with its own Cursor independently; reading does not modify another Reader's position. Durable named Subscription checkpoints will be stored by the replicated control plane in a later phase.

Responses expose nanosecond-resolution `event_time_ns` and `ingest_time_ns` as decimal strings so JavaScript clients do not lose 64-bit precision. The binary record format stores signed `i64` values. The prototype still accepts legacy numeric `timestamp_ms` input and converts it to nanoseconds, but emits only nanosecond fields.

Inspect membership and node pressure:

```bash
curl http://localhost:7070/v1/cluster/members
curl http://localhost:7070/v1/node/metrics
```

The stateful recommendation endpoint is used by the host controller:

```bash
curl -X POST http://localhost:7070/v1/cluster/autoscale/recommend \
  -H 'content-type: application/json' \
  -d '{
    "policy":{"min_nodes":3,"max_nodes":12,"scale_out_threshold":0.75,"scale_in_threshold":0.2,"scale_out_samples":6,"scale_in_samples":30,"cooldown_samples":12},
    "pressure":0.82,
    "removable_nodes":0
  }'
```

## Development

With Rust installed:

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
```

For the complete live acceptance/fault gate, use the separate `compose.verify.yml` Fabric (ports 7371–7373) rather than restarting the standard development Fabric. Set `COMPOSE_FILE=compose.verify.yml` and `WHITEWATER_TEST_BASE_PORT=7371` in the shell running the default three-Node scripts; on PowerShell use `$env:COMPOSE_FILE` and `$env:WHITEWATER_TEST_BASE_PORT`. Run the scripts sequentially:

```bash
export COMPOSE_FILE=compose.verify.yml WHITEWATER_TEST_BASE_PORT=7371
docker compose up --build -d
python scripts/test-m111-fault-suite.py
python scripts/test-m2-writer-session.py
python scripts/test-m3-reader-session.py
python scripts/test-m4-live-split.py
python scripts/test-m4-auto-split.py
docker compose stop
python scripts/test-m4-follower-move.py
python scripts/test-m4-cross-node-read.py
python scripts/test-m4-owner-move.py
```

The M1.11 suite runs M1.7, M1.9, and M1.10 and restarts the isolated Fabric; auto-split force-recreates it with test thresholds and restores defaults. The M4 movement and cross-Node read scripts start and stop their own projects without deleting volumes. `docker compose stop` above only stops the isolated verification Fabric. Never point these scripts at a Fabric containing user data.

Without a host Rust toolchain:

```bash
docker run --rm -v "$PWD:/workspace" -w /workspace rust:1.90-bookworm cargo test --all-targets
```
