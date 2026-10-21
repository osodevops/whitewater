"""Many-member Subscription acceptance.

Exercises the cooperative-consumption contract at member scale on a live
three-Node Riverbed:

  * 12 members join one Subscription and drain 300 records concurrently from
    every ingress Node; overlapping deliveries are allowed but every committed
    acknowledgement advances the shared frontier exactly once (Cursor CAS).
  * Explicit budgets hold under load: fetch limit clamps at 256, lease tick
    bounds reject out-of-range requests, a contested work lease returns a
    clean busy conflict, and the per-member work cap refuses a 33rd claim.
  * Killing the progress owner mid-drain does not lose acknowledgements:
    member operations transparently recover placement onto a surviving
    replica and the drain completes.
  * Restarting a non-owner replica mid-drain leaves consumption running.

Run from the repository root: ``python scripts/test-many-readers.py``.
"""

import base64
import json
import os
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid

COMPOSE = "compose.readers.yml"
NODES = ["control-1", "control-2", "control-3"]
ENDPOINTS = ["http://localhost:7571", "http://localhost:7572", "http://localhost:7573"]
KEY = "whitewater-local-development-admin-key"
RECORD_COUNT = 300
MEMBER_COUNT = 12
PAGE = 25
LEASE_TICKS = 600
DEADLINE_SECONDS = 300
# How long membership may take to move the progress owner onto a
# surviving replica; CI overrides when runners are under load.
OWNER_RECOVERY_SECONDS = int(os.environ.get("MANY_READERS_OWNER_RECOVERY_TIMEOUT", "240"))


def compose(*args):
    result = subprocess.run(
        ["docker", "compose", "-f", COMPOSE, *args],
        capture_output=True,
        text=True,
    )
    return result.returncode, result.stdout + result.stderr


# Transient connection errors between the host and Docker-published ports are
# retried; HTTP statuses and assertion failures still propagate unchanged.
def _retried(operation):
    def wrapped(*args, **kwargs):
        last = None
        for _ in range(60):
            try:
                return operation(*args, **kwargs)
            except urllib.error.HTTPError:
                raise
            except (ConnectionError, urllib.error.URLError, TimeoutError) as error:
                last = error
                time.sleep(0.5)
        raise last

    return wrapped

@_retried
def post(endpoint, path, body, timeout=30):
    # Transient unavailability (election windows, quorum stalls, restart
    # gaps) retries with the same body â€” stable request_id values in the
    # payload keep the retry idempotent. Refusals (4xx) return at once
    # for the caller to assert.
    for _ in range(120):
        status, result = _post_once(endpoint, path, body, timeout)
        if status == 0 or status >= 500:
            time.sleep(0.5)
            continue
        return status, result
    return status, result

def _post_once(endpoint, path, body, timeout=30):
    request = urllib.request.Request(
        endpoint + path,
        data=json.dumps(body).encode(),
        headers={
            "authorization": f"Bearer {KEY}",
            "content-type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read() or b"{}")
    except OSError as error:
        return 0, {"error": str(error)}


@_retried
def get(endpoint, path, timeout=30):
    for _ in range(120):
        status, result = _get_once(endpoint, path, timeout)
        if status == 0 or status >= 500:
            time.sleep(0.5)
            continue
        return status, result
    return status, result

def _get_once(endpoint, path, timeout=30):
    request = urllib.request.Request(
        endpoint + path, headers={"authorization": f"Bearer {KEY}"}
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read() or b"{}")
    except OSError as error:
        return 0, {"error": str(error)}


def wcl(script):
    request_id = str(uuid.uuid4())
    for attempt in range(30):
        status, body = post(ENDPOINTS[0], "/v1/admin/wcl", {"request_id": request_id, "script": script})
        if status == 200:
            return body
        time.sleep(1)
    raise AssertionError(f"WCL never succeeded: {status} {body}")


def wait_ready():
    for endpoint in ENDPOINTS:
        for _ in range(120):
            status, _ = get(endpoint, "/health")
            if status == 200:
                break
            time.sleep(1)
        else:
            raise AssertionError(f"{endpoint} never became healthy")


def progress(subscription):
    """Return the admin progress view, retrying briefly while a Node recovers."""
    last = (0, {})
    for endpoint in ENDPOINTS:
        status, body = get(
            endpoint,
            f"/v1/admin/subscriptions/progress?subscription={subscription}",
        )
        if status == 200:
            return body
        last = (status, body)
    raise AssertionError(f"progress view unavailable: {last}")


class Stats:
    def __init__(self):
        self.lock = threading.Lock()
        self.delivered = set()
        self.deliveries = 0
        self.acks = 0
        self.conflicts = 0
        self.busy = 0
        self.errors = 0
        self.error_samples = set()
        self.done = set()

    def record_error(self, path, status, body):
        with self.lock:
            self.errors += 1
            self._sample(path, status, body)

    def record_sample(self, path, status, body):
        with self.lock:
            self._sample(path, status, body)

    def _sample(self, path, status, body):
        if len(self.error_samples) < 10:
            self.error_samples.add(f"{path}: HTTP {status} {body.get('code')} {str(body.get('error', ''))[:300]}")

    def record_fetch(self, records):
        with self.lock:
            self.deliveries += len(records)
            for record in records:
                self.delivered.add(record["cursor"])


def member_post(endpoint, path, body, stats, stop):
    delay = 0.1
    result = (0, {"error": "worker stopped before the response was known"})
    while not stop.is_set():
        try:
            result = post(endpoint, path, body)
        except (ConnectionError, urllib.error.URLError, TimeoutError) as error:
            result = (0, {"error": str(error)})
        status, response = result
        if status != 0 and status < 500 and not (status == 409 and response.get("code") == "no_quorum"):
            # A persistent non-5xx refusal (e.g. a 409 wedge) is not retried
            # but is diagnostic gold; routine lease contention stays in the
            # worker's conflict counters instead.
            if response.get("code") not in ("lease.busy", "lease.stale_lease"):
                stats.record_sample(path, status, response)
            return result
        stats.record_error(path, status, response)
        if stop.wait(delay):
            break
        endpoint = ENDPOINTS[(ENDPOINTS.index(endpoint) + 1) % len(ENDPOINTS)]
        delay = min(delay * 2, 1.0)
    return result


def member_worker(index, subscription, member_id, work_id, stats, stop):
    """Claim -> fetch -> ack loop until the frontier is drained."""
    endpoint = ENDPOINTS[index % len(ENDPOINTS)]
    members = "/v1/subscriptions/members"

    def request(endpoint, path, body):
        return member_post(endpoint, path, body, stats, stop)

    lease_epoch = None
    empties = 0
    while not stop.is_set() and empties < 3:
        try:
            if lease_epoch is None:
                status, body = request(
                    endpoint,
                    f"{members}/claim",
                    {
                        "subscription": subscription,
                        "request_id": str(uuid.uuid4()),
                        "member_id": member_id,
                        "member_epoch": 1,
                        "work_id": work_id,
                        "lease_ticks": LEASE_TICKS,
                    },
                )
                if status == 200 and body.get("lease"):
                    lease_epoch = body["lease"]["lease_epoch"]
                else:
                    with stats.lock:
                        stats.errors += 1
                    time.sleep(0.3)
                    continue
            status, body = request(
                endpoint,
                f"{members}/fetch",
                {
                    "subscription": subscription,
                    "request_id": str(uuid.uuid4()),
                    "member_id": member_id,
                    "member_epoch": 1,
                    "work_id": work_id,
                    "lease_epoch": lease_epoch,
                    "limit": PAGE,
                },
            )
            if status != 200:
                with stats.lock:
                    if body.get("code") in ("lease.stale_lease", "lease.busy"):
                        stats.conflicts += 1
                    else:
                        stats.errors += 1
                if status in (0, 500, 503):
                    time.sleep(0.3)
                    continue
                lease_epoch = None
                continue
            records = body["records"]
            stats.record_fetch(records)
            if not records:
                empties += 1
                time.sleep(0.2)
                continue
            empties = 0
            status, acked = request(
                endpoint,
                f"{members}/ack",
                {
                    "subscription": subscription,
                    "request_id": str(uuid.uuid4()),
                    "member_id": member_id,
                    "member_epoch": 1,
                    "work_id": work_id,
                    "lease_epoch": lease_epoch,
                    "cursor": body["cursor"],
                    "positions": body["positions"],
                },
            )
            with stats.lock:
                if status == 200:
                    stats.acks += 1
                    lease_epoch = None
                elif acked.get("code") == "lease.stale_lease":
                    lease_epoch = None
                    stats.conflicts += 1
                else:
                    stats.conflicts += 1
            if status != 200:
                continue
        except (ConnectionError, urllib.error.URLError, TimeoutError):
            # A stopped owner or a transient drop must not kill the worker:
            # count it and let the member keep driving after recovery.
            with stats.lock:
                stats.errors += 1
            time.sleep(0.3)
    if empties >= 3:
        with stats.lock:
            stats.done.add(index)


def main():
    stats = None
    compose("down", "-v")
    returncode, output = compose("up", "--build", "-d")
    assert returncode == 0, output
    try:
        wait_ready()
        suffix = f"{int(time.time())}{uuid.uuid4().hex[:4]}"
        domain = f"mr{suffix}"
        feed = f"{domain}.events"
        writer = f"writer{suffix}"
        subscription = f"{domain}.work"
        setup = wcl(
            f"CREATE DOMAIN {domain}; CREATE FEED {feed}; "
            f"CREATE WRITER {writer} TO {feed}; OPEN WRITER SESSION {writer}; "
            f"CREATE SUBSCRIPTION {subscription} FROM {feed} START AT BEGINNING;"
        )
        writer_epoch = setup["results"][3]["data"]["session_epoch"]

        # Seed 300 records across six Keys in batches.
        def b64(raw):
            return base64.b64encode(raw).decode()

        seeded = set()
        seeded_index = {}
        for batch_start in range(0, RECORD_COUNT, 50):
            records = [
                {
                    "request_id": str(uuid.uuid4()),
                    "writer": writer,
                    "session_epoch": writer_epoch,
                    "event_time_ns": str(1_700_000_000_000_000_000 + index),
                    "key_base64": b64(f"key-{index % 6}".encode()),
                    "payload_base64": b64(f"record-{index}".encode()),
                    "metadata_base64": {"source": "seed"},
                }
                for index in range(batch_start, min(batch_start + 50, RECORD_COUNT))
            ]
            batch = None
            for attempt in range(30):
                status, batch = post(
                    ENDPOINTS[batch_start // 50 % 3],
                    "/v1/writers/append-batch",
                    {"records": records},
                )
                if status == 200 and len(batch["results"]) == len(records):
                    break
                # A saturated runner can stall an append quorum briefly; the
                # writer keeps identity, so a retry is idempotent.
                if status >= 500 or status == 0:
                    time.sleep(2)
                    continue
                raise AssertionError(batch)
            else:
                raise AssertionError(f"seed batch {batch_start} never committed: {batch}")
            for index, result in enumerate(batch["results"]):
                seeded.add(result["cursor"])
                seeded_index[result["cursor"]] = batch_start + index
        assert len(seeded) == RECORD_COUNT

        members = "/v1/subscriptions/members"
        member_ids = [str(uuid.uuid4()) for _ in range(MEMBER_COUNT)]
        work_ids = [str(uuid.uuid4()) for _ in range(MEMBER_COUNT)]

        # Every member joins; the first join bootstraps the beginning frontier.
        for index, member_id in enumerate(member_ids):
            status, body = post(
                ENDPOINTS[index % 3],
                f"{members}/join",
                {
                    "subscription": subscription,
                    "request_id": str(uuid.uuid4()),
                    "member_id": member_id,
                },
            )
            assert status == 200 and body["member_epoch"] == 1, body

        first = member_ids[0]
        first_work = work_ids[0]

        # Budget: lease tick bounds reject out-of-range requests.
        for bad_ticks in (0, 601):
            status, body = post(
                ENDPOINTS[0],
                f"{members}/claim",
                {
                    "subscription": subscription,
                    "request_id": str(uuid.uuid4()),
                    "member_id": first,
                    "member_epoch": 1,
                    "work_id": str(uuid.uuid4()),
                    "lease_ticks": bad_ticks,
                },
            )
            assert status == 400, (bad_ticks, body)

        # Member 0 claims work and proves the fetch limit clamps at 256.
        status, claimed = post(
            ENDPOINTS[0],
            f"{members}/claim",
            {
                "subscription": subscription,
                "request_id": str(uuid.uuid4()),
                "member_id": first,
                "member_epoch": 1,
                "work_id": first_work,
                "lease_ticks": LEASE_TICKS,
            },
        )
        assert status == 200 and claimed["lease"], claimed
        first_lease = claimed["lease"]["lease_epoch"]
        status, page = post(
            ENDPOINTS[1],
            f"{members}/fetch",
            {
                "subscription": subscription,
                "request_id": str(uuid.uuid4()),
                "member_id": first,
                "member_epoch": 1,
                "work_id": first_work,
                "lease_epoch": first_lease,
                "limit": 99999,
            },
        )
        assert status == 200, page
        assert len(page["records"]) == 256, len(page["records"])
        # The page is deliberately left unacknowledged: the worker pool below
        # must see every seeded record, so the frontier stays at the start.

        # Budget: a contested work lease refuses the second claimant. Member 11
        # holds its pool work lease live (an ack would release it, so nothing
        # is acked on it here).
        held = work_ids[MEMBER_COUNT - 1]
        holder = member_ids[MEMBER_COUNT - 1]
        status, held_claim = post(
            ENDPOINTS[0],
            f"{members}/claim",
            {
                "subscription": subscription,
                "request_id": str(uuid.uuid4()),
                "member_id": holder,
                "member_epoch": 1,
                "work_id": held,
                "lease_ticks": LEASE_TICKS,
            },
        )
        assert status == 200 and held_claim["lease"], held_claim
        status, body = post(
            ENDPOINTS[1],
            f"{members}/claim",
            {
                "subscription": subscription,
                "request_id": str(uuid.uuid4()),
                "member_id": member_ids[1],
                "member_epoch": 1,
                "work_id": held,
                "lease_ticks": LEASE_TICKS,
            },
        )
        assert status == 409 and body.get("code") == "lease.busy", body

        # Budget: a lease epoch or member identity that does not match the live
        # grant cannot fetch.
        status, body = post(
            ENDPOINTS[1],
            f"{members}/fetch",
            {
                "subscription": subscription,
                "request_id": str(uuid.uuid4()),
                "member_id": first,
                "member_epoch": 1,
                "work_id": held,
                "lease_epoch": held_claim["lease"]["lease_epoch"],
            },
        )
        assert status == 409 and body.get("code") == "lease.stale_lease", body

        # Budget: the per-member work cap refuses a 33rd active claim. The
        # holder keeps 32 live leases (its pool work_id among them, so its
        # worker claim below still resolves to the existing grant).
        for extra in range(31):
            status, body = post(
                ENDPOINTS[extra % 3],
                f"{members}/claim",
                {
                    "subscription": subscription,
                    "request_id": str(uuid.uuid4()),
                    "member_id": holder,
                    "member_epoch": 1,
                    "work_id": str(uuid.uuid4()),
                    "lease_ticks": LEASE_TICKS,
                },
            )
            assert status == 200, (extra, body)
        status, body = post(
            ENDPOINTS[0],
            f"{members}/claim",
            {
                "subscription": subscription,
                "request_id": str(uuid.uuid4()),
                "member_id": holder,
                "member_epoch": 1,
                "work_id": str(uuid.uuid4()),
                "lease_ticks": LEASE_TICKS,
            },
        )
        assert status == 409 and body.get("code") == "lease.capacity", body

        # The admin progress view names the owner, replicas, frontier, and
        # member state that operators need during movement or recovery.
        view = progress(subscription)
        assert view["assignment"]["ownership_epoch"] == 1
        assert view["assignment"]["owner"] in NODES, view
        assert view["members"]["member_count"] == MEMBER_COUNT
        assert all(replica["reachable"] for replica in view["replicas"])
        owner_before = view["assignment"]["owner"]

        # Concurrent drain: 12 members race claim/fetch/ack until every member
        # sees the tail. Overlapping delivery is expected; the committed
        # frontier can only advance.
        stats = Stats()
        stop = threading.Event()
        threads = [
            threading.Thread(
                target=member_worker,
                args=(index, subscription, member_ids[index], work_ids[index], stats, stop),
                daemon=True,
            )
            for index in range(MEMBER_COUNT)
        ]
        for thread in threads:
            thread.start()

        # Mid-drain: kill the progress owner; member operations recover it.
        # SIGKILL, not stop: a graceful stop can linger on shutdown hooks and
        # never leave membership, while a kill is the crash the test means to
        # exercise â€” the container must be gone before recovery is polled.
        time.sleep(1.5)
        returncode, output = compose("kill", owner_before)
        assert returncode == 0, output
        _, ps_out = compose("ps", owner_before)
        _, owner_logs = compose("logs", owner_before, "--no-color", "--tail", "100")
        print(f"killed owner {owner_before}; ps:\n{ps_out}", file=sys.stderr)
        print(owner_logs, file=sys.stderr)
        _, still_running = compose("ps", "--status", "running", "-q", owner_before)
        if still_running.strip():
            # Compose reports success even when a container lingers; force it
            # out so membership sees the loss and `up -d` below recreates it.
            returncode, output = compose("rm", "-f", owner_before)
            assert returncode == 0, output
        recovered = None
        last_view = None
        deadline = time.time() + OWNER_RECOVERY_SECONDS
        while time.time() < deadline:
            try:
                view = progress(subscription)
                last_view = view
            except AssertionError as error:
                last_view = error
                time.sleep(1)
                continue
            if (
                view["assignment"]["owner"] != owner_before
                and view["assignment"]["ownership_epoch"] > 1
            ):
                recovered = view
                break
            time.sleep(1)
        assert recovered, (
            "progress owner never recovered after kill "
            f"(waited {OWNER_RECOVERY_SECONDS}s); last progress view: {last_view}"
        )
        owner_after = recovered["assignment"]["owner"]
        epoch_after = recovered["assignment"]["ownership_epoch"]
        dead_replica = next(
            replica
            for replica in recovered["replicas"]
            if replica["node"] == owner_before
        )
        assert dead_replica["reachable"] is False, dead_replica

        # Bring the killed owner back; `up -d` restarts the container when it
        # exists and recreates it when the lingering-kill fallback removed it.
        returncode, output = compose("up", "-d", owner_before)
        assert returncode == 0, output
        restart_target = next(
            node for node in NODES if node != owner_after and node != owner_before
        )
        returncode, output = compose("restart", restart_target)
        assert returncode == 0, output

        deadline = time.time() + DEADLINE_SECONDS
        while time.time() < deadline and len(stats.done) < MEMBER_COUNT:
            time.sleep(0.5)
        stop.set()
        for thread in threads:
            thread.join(timeout=30)
        assert len(stats.done) == MEMBER_COUNT, (
            f"only {len(stats.done)}/{MEMBER_COUNT} members drained: "
            f"acks={stats.acks} conflicts={stats.conflicts} errors={stats.errors}"
        )

        # Every committed record was delivered to at least one member; the
        # shared frontier ended at the Feed tail (all members saw empty pages).
        view = progress(subscription)
        missing = sorted(
            seeded_index[cursor] for cursor in seeded - stats.delivered
        )
        assert stats.delivered == seeded, (
            f"{len(stats.delivered)}/{RECORD_COUNT} distinct records delivered; "
            f"missing indices {missing[:40]} acks={stats.acks} "
            f"conflicts={stats.conflicts} errors={stats.errors}"
        )
        assert stats.acks >= 2, stats.acks
        assert stats.deliveries >= RECORD_COUNT

        report = {
            "status": "ok",
            "members": MEMBER_COUNT,
            "records": RECORD_COUNT,
            "distinct_delivered": len(stats.delivered),
            "total_deliveries": stats.deliveries,
            "committed_acks": stats.acks,
            "superseded_conflicts": stats.conflicts,
            "owner_before_stop": owner_before,
            "owner_after_recovery": owner_after,
            "epoch_after_recovery": epoch_after,
            "note": (
                "12 members cooperatively drained 300 records across three "
                "ingress Nodes; the fetch limit clamped at 256, lease bounds "
                "and per-member work caps held, contested claims returned "
                "busy, a progress-owner kill and a replica restart did not "
                "lose acknowledged progress, and every seeded record was "
                "delivered before the shared frontier reached the tail"
            ),
        }
        print(json.dumps(report, indent=2))
        return 0
    except Exception:
        if stats is not None:
            print(json.dumps(sorted(stats.error_samples)), file=sys.stderr)
        _, logs = compose("logs", "--no-color", "--tail", "2000")
        print(logs, file=sys.stderr)
        raise
    finally:
        compose("down", "-v")


if __name__ == "__main__":
    sys.exit(main())
