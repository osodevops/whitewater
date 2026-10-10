# Whitewater Control Plane



Each Whitewater Riverbed has one **Control Plane**. Internally, three control voters use Raft majority consensus, but applications and operators do not manage a user-visible “quorum” resource.



## Responsibilities



The Control Plane owns logical metadata and administrative ordering:



- Domains

- Feed identities and names

- Writers and Readers

- Reader Cursors

- Roles and namespace grants

- Fixed RF3 Active Range assignments and ownership epochs

- Future Subscriptions, Indexes, Pipes, schemas, policies, and credentials



It does not replicate Feed records yet. Active-range data replication remains a separate phase.



## Three-voter baseline



The standard development Riverbed configures three voters:



```text

Node 1: node1:7070

Node 2: node2:7070

Node 3: node3:7070

```



A metadata mutation commits only after a majority accepts it:



```text

3 healthy voters -> commits continue

2 healthy voters -> commits continue

1 healthy voter  -> mutations and linearizable reads are refused

```



There is no supported one-voter Control Plane mode.



## Public behavior



Any Node may receive an authenticated Admin API request.



```text

request reaches leader

    -> propose command

    -> replicate command log

    -> majority commit

    -> apply deterministic catalog transition on every Node

    -> reply success



request reaches follower

    -> discover elected leader

    -> forward command internally

    -> return committed result

```



Successful standard-development responses now report:



```json

{

  "authority": "control_plane",

  "warning": ""

}

```



A Node not configured as a Control Plane voter retains `local_prototype` behavior for isolated storage tests and arbitrary-scale membership experiments.



## Deterministic transitions



Every replicated mutation contains:



```text

request_id

issued_at_ns

command

```



Resource IDs are derived deterministically from request identity and resource type. Every replica therefore creates the same FeedId, RangeId, WriterId, ReaderId, RoleId, and GrantId.



For Feed creation, the leader selects one owner and three distinct eligible storage Nodes before submission and embeds that fixed placement in the replicated command. Followers apply the embedded assignment even if their local discovery view differs. The replicated state machine never performs Node-local data-plane provisioning.



Applied request results are persisted by request ID. Replaying one committed command after a crash returns its original result without applying it twice.



## Read behavior



`SHOW`, `DESCRIBE`, and `EXPLAIN ACCESS` are read-only commands. The receiving Node first obtains a Raft linearizability guarantee, then reads its applied catalog without appending another command or incrementing catalog revision.



If a majority cannot confirm leadership, linearizable reads fail rather than silently returning potentially stale metadata.



## Persistence



Each voter persists:



```text

/var/lib/finnstream/control-plane-raft.json

/var/lib/finnstream/control-plane-catalog.json

```



Persisted Raft state includes:



- Vote

- Committed LogId

- Replicated log entries

- Last purged LogId

- Last applied LogId

- Membership

- Current snapshot metadata and bytes



Writes use a temporary file, flush it, synchronize it, and rename it into place.



Catalog snapshots are included in Raft snapshots. Snapshot installation replaces the local catalog with the committed leader snapshot.



## Leader election and failover



The current timing policy is:



```text

heartbeat:          500 ms

election minimum:  1500 ms

election maximum:  3000 ms

```



When the leader stops, surviving voters elect another leader. Admin clients may keep using any surviving Node; followers forward to the new leader.



A restarted former leader reloads its vote, log, applied metadata, and catalog, then catches up before serving current linearizable metadata.



## Quorum-loss behavior



Control mutations have a five-second majority-commit deadline. On quorum loss they return an unavailable error describing likely majority loss. They do not fall back to local catalog mutation.



A timeout is an ambiguous network result: a proposal may commit later if the majority returns. Every Admin request therefore carries a client-visible `request_id`. Retrying the same script or typed command batch with the same ID returns the original persisted result and does not apply the catalog transition twice. Clients must not generate a new request ID when retrying an outcome they did not observe.



This is a safety choice:



```text

no majority

    -> no metadata write success

    -> no split-brain namespace or identity state

```



## Status API



```http

GET /v1/admin/control-plane

Authorization: Bearer <admin-api-key>

```



Example:



```json

{

  "node_id": 2,

  "leader_id": 2,

  "state": "leader",

  "current_term": 2,

  "last_log_index": 9,

  "last_applied_index": 9,

  "membership": [1, 2, 3],

  "catalog_revision": 7

}

```



## Internal transport



Nodes use authenticated internal HTTP endpoints for:



- AppendEntries

- Vote requests

- Snapshot installation

- Leader-forwarded application writes



The shared prototype credential is configured with:



```text

FINNSTREAM_CONTROL_PLANE_KEY

```



Keys shorter than 24 characters are rejected. The development fallback is not a production secret.



Production work still requires authenticated encryption for internal transport, credential rotation, and failure-domain identity. An internal shared key alone is not the final security model.



The deployment may provide inter-Node encryption through Whitewater-native mTLS, a trusted service mesh/sidecar, or an orchestrator/private-network transport with equivalent authenticated-encryption, identity, rotation, audit, and downgrade-prevention guarantees. Native mTLS is the default and recommended mechanism. The mechanism is configurable, but plaintext production traffic is not: Whitewater must fail closed if the selected transport cannot prove both encryption and peer identity.



### Internal-plane mTLS boundary



An optional dedicated listener now protects the entire internal Node-to-Node plane: Control Plane Raft RPCs and internal writes, replica append/commit, recovery and repair, split/merge staging, follower and owner movement, Subscription progress replication, and pressure sampling. Configure all six values together on each participating Control Node; supplying only part of the set fails startup:



```text

FINNSTREAM_SUBSCRIPTION_MTLS_BIND

FINNSTREAM_SUBSCRIPTION_MTLS_CERT

FINNSTREAM_SUBSCRIPTION_MTLS_KEY

FINNSTREAM_SUBSCRIPTION_MTLS_CA

FINNSTREAM_SUBSCRIPTION_MTLS_PEER_PINS

FINNSTREAM_SUBSCRIPTION_MTLS_ENDPOINTS

```



The bind address must differ from the public listener. The certificate/key and CA are PEM file paths; never place private keys in source control. Peer pins map each internal NodeId to one or more BLAKE3 digests of its leaf certificate's DER bytes, e.g. `control-1@<digest>|<next-digest>,control-2@<digest>`. An overlapping old/new pin set permits a deliberate rolling certificate restart; the listener loads new certificate material at startup, not on file changes. Peer endpoints map each internal NodeId to its mTLS address, e.g. `control-1@https://control-1:7271,control-2@https://control-2:7272`; the HTTPS hostname must equal the assigned NodeId so certificate identity binds to the addressed Node. Outbound internal clients present this Node's certificate, trust only the configured CA, and refuse plain HTTP. The listener requires a CA-validated, pinned client certificate before any internal route is served; the shared Control Plane key remains a second factor on non-Subscription routes, while Subscription progress routes authenticate purely on the pinned peer certificate since member coordination may land on any Node and placement fencing enforces the logical owner.



When mTLS is configured, the public listener does not serve any `/internal/*` route, so a failed TLS handshake cannot fall back to plaintext. The shared-key internal plane remains available only when mTLS is not configured.



Live Compose evidence exists (`scripts/test-mtls-plane.py` with `compose.mtls.yml` and `tests/dev_cert_gen.rs` provisioning): raft election, internal-route absence from the public listener, uncertified-client refusal, member joins driven from a non-owner Node, drain/retire, and continued appends all pass over the pinned-certificate plane. Independently registered storage Nodes distribute certificate pins through the replicated catalog (`REGISTER STORAGE NODE ... WITH CERT PIN`), and the listener accepts catalog-pinned peers exactly like configured ones. Remaining gaps: per-request caller identity binding beyond cert-to-Node pinning, independently signed commit-vote evidence, and live acceptance with a non-voter storage Node.



## Static membership limitation



Control voter membership is currently configured statically:



```text

FINNSTREAM_CONTROL_NODE_ID

FINNSTREAM_CONTROL_NODES

FINNSTREAM_CONTROL_PLANE_KEY

```



Example:



```text

FINNSTREAM_CONTROL_NODE_ID=1

FINNSTREAM_CONTROL_NODES=1@node1:7070,2@node2:7070,3@node3:7070

```



Dynamic learner addition and joint-consensus membership changes are future work. Arbitrary data capacity Nodes do not automatically become Control Plane voters.

### Storage-only Nodes

A Node without `FINNSTREAM_CONTROL_NODE_ID` holds no voter role but can still serve the data plane. `FINNSTREAM_STORAGE_NODE_ID` names its internal storage identity (voters derive `control-N` automatically and must not override it), and `FINNSTREAM_CONTROL_NODES` plus `FINNSTREAM_CONTROL_PLANE_KEY` give it the voter endpoints and internal credential without requiring the full voter triple; internal data-plane routes authenticate on that shared credential alone when no local voter exists, while Raft RPC handlers continue to refuse requests on a Node with no Control Plane. Registered in the catalog (`REGISTER STORAGE NODE <id> AT <endpoint> [WITH CERT PIN ...]`), the Node joins the eligible placement pool, and every internal transport resolves its endpoint through the catalog record.

Non-voters cannot receive Raft log replication, so each runs a catalog sync supervisor (`WHITEWATER_CATALOG_SYNC_MS`, default 1s) that installs the newest committed snapshot served by `POST /internal/catalog/snapshot` on any reachable voter. Installs are revision-gated so a lagging peer can never roll catalog state backwards, and the synced catalog runs in read-through replica mode: local mutations are refused rather than diverging silently. Until the first snapshot lands, replica append and progress fencing fail closed on the empty catalog, so a freshly started storage Node cannot serve placements it has not seen committed. `scripts/test-storage-node-join.py` (`compose.storage-join.yml`) demonstrates the lifecycle live: three voters plus a registered non-voter, catalog-distributed certificate-pin trust (voters carry no static pin for the Node), rendezvous placement onto the Node, mTLS replica appends, synced-catalog reads through its public API, and restart re-join with durable replica bytes.



For M1.4 fixed placement, the standard three configured Control Plane Nodes are also treated as storage-capable Nodes with stable internal IDs `control-1`, `control-2`, and `control-3`. The Control Plane now fixes each new Subscription progress replica set from the full configured eligible Node pool by SubscriptionId, while retaining RF3 per Subscription; 3/12/24-candidate tests are placement-only. Feed creation still initially takes the first three eligible Nodes, and current executable configuration derives eligible storage Nodes from the static Control Node list. Capability-aware placement, distinct storage roles, dynamic voter membership, and live 12/24-Node acceptance are Milestone 5 work—not a shipped promise of unlimited Riverbed size. Placement alone does not prove transport identity, recovery, or sustained capacity.



## Verified scenarios



The three-Node Docker Riverbed has demonstrated:



- Initial leader election

- Three-voter membership agreement

- Command submission through a follower

- Internal forwarding to the leader

- Majority commit

- Identical FeedId/catalog state on every Node

- Identical fixed RF3 Active Range assignment on every Node

- Authenticated placement inspection through every voter

- Placement recovery after complete Riverbed restart

- Leader stop and new leader election

- Successful mutation after leader failure

- Restart and catch-up of the former leader

- Persistent catalog recovery

- No success response when two of three voters are unavailable

- Deterministic request IDs and idempotent retry across Node restart

- Uncommitted command removal after leader loss and a new majority election



## Remaining production work



- Internal TLS/mTLS

- Dynamic voter replacement using learner catch-up and joint consensus

- Automated snapshots under sustained catalog volume

- Corruption and partial-write fault injection

- Disk-full handling

- Backup/restore procedures

- Metrics, alert thresholds, and election diagnostics

- Multi-region metadata policy

- Rolling compatibility tests across Control Plane versions

- Formalized migration from prototype local catalogs



The Control Plane is metadata consensus. Whitewater still needs a separate quorum replication protocol for active Feed ranges before event writes have distributed durability.

