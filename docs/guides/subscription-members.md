# Subscription members

A named **Subscription** is the durable progress identity for a group of
cooperating workers — one acknowledged frontier shared by members, never
a client-managed `group.id`. Member sessions are short-lived and fenced:
each member **joins**, **claims** a work lease, **fetches** a bounded
page under that lease, **acks** it atomically, **renews** the lease while
working, **releases** when done, and may **transfer** a pending lease to
a peer. A member that loses its lease sees `lease.stale_lease` and stops.

Work subdivision is internal: members claim deterministic Key-window
slices (256 windows per member Feed) instead of overlapping
at-least-once fetches — two members never pull the same Key's work.
Domain-wide sources cover every member Feed with an independent durable
frontier; a member fetch names which member Feed it serves.

```text
CREATE SUBSCRIPTION order-work FROM FEED orders.events;
-- or every Feed in the Domain:
CREATE SUBSCRIPTION domain-work FROM DOMAIN orders;
```

## The worker loop

```java
// Java — client.member() joins internally
var member = client.member("order-work");
var work = member.claim();                    // fenced Key-window lease
var page = member.fetch(work, 64);
member.ack(work, page);                       // atomic under the lease
member.release(work);
member.transfer(pending, peer, 60);           // hand a pending lease on
```

```go
m := client.SubscriptionMember("order-work")
joined, _ := m.Join(ctx, memberID)
lease, _ := m.Claim(ctx, joined, workID, leaseTicks)
page, _ := m.Fetch(ctx, joined, lease, 64)
_ = m.Ack(ctx, joined, lease, page)
_ = m.Renew(ctx, joined, lease, leaseTicks)   // while still working
_ = m.Release(ctx, joined, lease)
```

```python
member = client.subscription_member("order-work")
joined = member.join(member_id)
lease = member.claim(joined, work_id, lease_ticks=60)
page = member.fetch(joined, lease, limit=64)
member.ack(joined, lease, page)
member.release(joined, lease)
```

```ts
const member = client.subscriptionMember("order-work");
const joined = await member.join(memberId);
const lease = await member.claim(joined, workId, 60);
const page = await member.fetch(joined, lease, 64);
await member.ack(joined, lease, page);
await member.release(joined, lease);
```

```csharp
var member = client.SubscriptionMember("order-work");
var joined = await member.JoinAsync(memberId);
var lease = await member.ClaimAsync(joined, workId, 60);
var page = await member.FetchAsync(joined, lease, 64);
await member.AckAsync(joined, lease, page);
await member.ReleaseAsync(joined, lease);
```

```rust
let member = client.member("order-work").await?;   // joins internally
let work = member.claim().await?;
let page = member.fetch(&work, 64).await?;
member.ack(&work, &page).await?;
member.renew(&work, 60).await?;
member.release(&work).await?;
```

## Repositioning — `subscriptions/seek`

Operators can reposition shared progress (requires `manage` on the
source): `beginning`, a `timestamp` boundary, or an explicit `cursor`.
Seek refuses while members hold work leases — drain or fence first —
and an identical `request_id` replays the same reposition result while a
different position under a reused `request_id` conflicts.

## Evidence

- `subscription-member-lease` — renew fencing (stale holder sees
  `lease.stale_lease`), transfer delivers to the recipient.
- `subscription-member-drain` — member drain with delivery evidence.
- `subscription-seek` — lease-refusal, beginning positions, request-id
  replay/conflict, timestamp seek inclusivity.
- `scripts/test-m7-subscription-member.py` — Domain source end-to-end:
  join, selector fencing, per-Feed fetch/ack, late attach on
  `CREATE FEED`, seek isolation, member-Feed split reconcile.
- Java `examples/MemberWork.java` — claim → fetch → ack → release loop.
