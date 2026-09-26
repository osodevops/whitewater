# Whitewater Sequence Diagrams

> A visual guide to current and planned Whitewater behavior. Each section states whether the flow is implemented or planned.

## Status legend

- **Implemented:** available in the current three-Node prototype.
- **Next:** the current implementation milestone.
- **Planned:** architecture direction, not current behavior.

## Append through any Node

**Status: Implemented through M1.7.**

The application sends only Feed, Key, payload, Metadata, event time, and stable Writer/request identity. Physical topology remains internal.

```mermaid
sequenceDiagram
    participant App as Application
    participant Ingress as Any Node (Ingress)
    participant Owner as Current Append Owner
    participant FollowerA as Replica A
    participant FollowerB as Replica B

    App->>Ingress: Append Feed + Key + Payload
    Ingress->>Ingress: Resolve FeedId and committed assignment
    alt Ingress is not owner
        Ingress->>Owner: Forward unchanged request identity
    end
    Owner->>Owner: Encode once and flush exact frame
    par Replicate concurrently
        Owner->>FollowerA: Append position + exact frame
        Owner->>FollowerB: Append position + exact frame
    end
    FollowerA-->>Owner: Position and digest durable
    FollowerB-->>Owner: Position and digest durable
    Owner->>FollowerA: Persist CommitPosition + digest
    FollowerA-->>Owner: Commit evidence durable
    Owner->>Owner: Persist local CommitPosition
    Owner-->>Ingress: Majority committed
    Ingress-->>App: MessageId + opaque Cursor
```

The owner and at least one follower must hold both the exact frame and durable commit evidence before success.

## Append directly through the owner

**Status: Implemented.**

```mermaid
sequenceDiagram
    participant App as Application
    participant Owner as Current Append Owner
    participant FollowerA as Replica A
    participant FollowerB as Replica B

    App->>Owner: Append request
    Owner->>Owner: Encode and flush
    par Replicate
        Owner->>FollowerA: Exact frame
        Owner->>FollowerB: Exact frame
    end
    FollowerA-->>Owner: Durable acknowledgement
    FollowerB-->>Owner: Durable acknowledgement
    Owner->>FollowerA: CommitPosition evidence
    FollowerA-->>Owner: Commit evidence durable
    Owner->>Owner: Persist local CommitPosition
    Owner-->>App: majority_committed
```

There is no forwarding step when the ingress Node is already the owner.

## Retry through another Node

**Status: Implemented.**

```mermaid
sequenceDiagram
    participant App as Application
    participant NodeA as Node A
    participant Owner as Append Owner
    participant NodeB as Node B

    App->>NodeA: Request 500, Writer sequence 42
    NodeA->>Owner: Forward unchanged request
    Owner->>Owner: Majority commit
    Owner-->>NodeA: Success
    Note over App,NodeA: Response is lost
    App->>NodeB: Retry request 500, sequence 42
    NodeB->>Owner: Forward identical request
    Owner->>Owner: Find existing logical append
    Owner-->>NodeB: Original MessageId and Cursor
    NodeB-->>App: deduplicated = true
```

The client must preserve request ID, Writer session ID, Writer epoch, sequence, event time, key, payload, and Metadata when retrying an ambiguous result.

## No majority available

**Status: Implemented and integration tested.**

```mermaid
sequenceDiagram
    participant App as Application
    participant Owner as Append Owner
    participant FollowerA as Replica A
    participant FollowerB as Replica B

    App->>Owner: Append
    Owner->>Owner: Flush locally
    Owner-xFollowerA: Replica unavailable
    Owner-xFollowerB: Replica unavailable
    Note over Owner: Only one durable copy
    Owner-->>App: Retryable failure, no success
```

Whitewater does not weaken durability settings to keep accepting writes.

## Committed-only application read

**Status: Implemented through M1.8.**

```mermaid
sequenceDiagram
    participant App as Application
    participant Ingress as Any Node
    participant Replica as Active Range Replica

    App->>Ingress: Read Feed after opaque Cursor
    Ingress->>Ingress: Resolve FeedId and assignment
    Ingress->>Replica: Read committed records
    Replica->>Replica: Enforce position <= CommitPosition
    Replica-->>Ingress: Committed records only
    Ingress-->>App: Records + opaque Cursors
```

The replicated Feed path is exposed at `GET /v1/feeds/records`. The legacy `/v1/records` API remains separate during prototype migration.

## Why uncommitted frames remain hidden

**Status: Storage rule implemented; public Feed read path is M1.8.**

```mermaid
sequenceDiagram
    participant Owner as Append Owner
    participant FollowerA as Replica A
    participant FollowerB as Replica B
    participant Reader as Application Reader

    Owner->>Owner: Append position 10
    Owner-xFollowerA: Replication fails
    Owner-xFollowerB: Replication fails
    Note over Owner: Position 10 exists on one Node only
    Reader->>Owner: Read records
    Owner-->>Reader: Return only through CommitPosition 9
    Note over Reader: Position 10 remains invisible
```

A replica may physically contain an uncommitted tail. Physical presence is not permission to return a record.

## Ownership transfer and stale-owner fencing

**Status: Manual transfer and epoch fencing implemented; automatic failure recovery is M1.9.**

```mermaid
sequenceDiagram
    participant CP as Control Plane
    participant Old as Old Owner
    participant New as New Owner
    participant App as Application

    Note over Old: Owner at epoch 1
    CP->>CP: Majority-commit ownership transfer
    CP->>New: Owner at epoch 2
    Old->>New: Attempt append with epoch 1
    New-->>Old: Reject stale owner
    App->>New: Append using current routing
    New->>New: Accept under epoch 2
```

Only a Control Plane committed transition may increase the ownership epoch.

## Future shared Subscription readers

**Status: Planned — Reader sessions are M3; durable Subscription leases are M7.**

```mermaid
sequenceDiagram
    participant WW as Whitewater
    participant ReaderA as Microservice Pod A
    participant ReaderB as Microservice Pod B

    ReaderA->>WW: Join Subscription order-processor
    ReaderB->>WW: Join Subscription order-processor
    WW->>ReaderA: Lease key ranges 1 and 2
    WW->>ReaderB: Lease key ranges 3 and 4
    WW->>ReaderA: customer-1 event 1
    ReaderA->>WW: Acknowledge
    WW->>ReaderA: customer-1 event 2
    ReaderA->>WW: Acknowledge
    par Unrelated keys proceed concurrently
        WW->>ReaderA: customer-2 event
        WW->>ReaderB: customer-3 event
    end
```

One Subscription shares work among its Reader instances. One key remains in one ordered processing lane at a time; unrelated keys can run concurrently.

## Current system summary

```mermaid
flowchart LR
    App[Application] --> Any[Any healthy Whitewater Node]
    Any --> Owner[Current Append Owner]
    Owner --> ReplicaA[Replica A]
    Owner --> ReplicaB[Replica B]
    Owner --> Commit{Two durable frames and<br/>two commit records?}
    ReplicaA --> Commit
    ReplicaB --> Commit
    Commit -->|Yes| Success[MessageId + Cursor]
    Commit -->|No| Failure[No success]
    Success -.->|M1.8| Read[Committed-only Feed reads]
```

| Capability | Status |
|---|---|
| Create Feed without partitions | Implemented |
| Append through any Node | Implemented |
| Exact-frame RF3 replication | Implemented |
| Two-of-three majority commit | Implemented |
| Cross-Node idempotent retry | Implemented |
| Committed-only application Feed reads | Implemented |
| Automatic owner failure recovery | Planned: M1.9 |
| Replica catch-up and repair | Planned: M1.10 |
| Reader sessions | Planned: M3 |
| Shared durable Subscriptions | Planned: M7 |
