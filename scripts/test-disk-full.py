"""Real filesystem-full fault acceptance.

A three-Node Riverbed where node3's data directory is a small tmpfs
(`compose.disk-full.yml`). Unlike the integration test's synthetic
`max_range_bytes` cap, this fills a real filesystem so the store takes a
genuine ENOSPC mid-append while inter-Node replication keeps running.

Proves the degraded-mode contract:

  * before the fill, appends commit normally on all three Nodes;
  * after node3's volume is full, every acknowledged append remains
    readable â€” a quorum append may refuse (honest failure) but must never
    acknowledge and then lose the record;
  * the surviving two-Node quorum keeps the Feed writable and the
    committed prefix uncorrupted;
  * after node3 is recreated with a clean volume (the "disk replaced"
    recovery leg) the Riverbed heals: all Nodes healthy, the Control
    Plane converged, the full record set still readable, and new appends
    committing again.
"""

import base64
import json
import os
import subprocess
import time
import urllib.error
import urllib.request
import uuid

COMPOSE = "compose.disk-full.yml"
ENDPOINTS = ["http://localhost:7671", "http://localhost:7672", "http://localhost:7673"]
KEY = "whitewater-local-development-admin-key"
BASELINE_RECORDS = 20
# The degraded phase keeps offering batches until the surviving quorum
# accepts one; owner recovery off a live-but-full Node is fast locally but
# must not be raced on a loaded CI runner.
DEGRADED_DEADLINE = 180
RECOVERY_DEADLINE = 360


def compose(*args, timeout=120):
    result = subprocess.run(
        ["docker", "compose", "-f", COMPOSE, *args],
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    return result.returncode, result.stdout + result.stderr


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


def get(endpoint, path, timeout=10):
    for _ in range(120):
        status, result = _get_once(endpoint, path, timeout)
        if status == 0 or status >= 500:
            time.sleep(0.5)
            continue
        return status, result
    return status, result

def _get_once(endpoint, path, timeout=10):
    request = urllib.request.Request(
        endpoint + path, headers={"authorization": f"Bearer {KEY}"}
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, json.loads(response.read())
    except (urllib.error.HTTPError, OSError) as error:
        return getattr(error, "code", 0), {}


def wcl(script, deadline=120):
    request_id = str(uuid.uuid4())
    started = time.monotonic()
    while time.monotonic() - started < deadline:
        for endpoint in ENDPOINTS:
            status, body = post(endpoint, "/v1/admin/wcl", {"request_id": request_id, "script": script})
            if status == 200:
                return body
            if status == 400:
                raise AssertionError(f"WCL rejected: {body}")
        time.sleep(1)
    raise AssertionError("WCL never succeeded")


def wait_ready(nodes=ENDPOINTS, timeout=300):
    for endpoint in nodes:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            status, _ = get(endpoint, "/health")
            if status == 200:
                break
            time.sleep(1)
        else:
            raise AssertionError(f"{endpoint} never became healthy")


def control_plane_converged(nodes=ENDPOINTS, timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        statuses = [get(endpoint, "/v1/admin/control-plane")[1] for endpoint in nodes]
        if (
            len(statuses) == len(nodes)
            and all(status.get("leader_id") is not None for status in statuses)
            and all(
                status.get("last_applied_index") is not None
                and status["last_applied_index"] >= (status.get("last_log_index") or 0)
                for status in statuses
            )
        ):
            return
        time.sleep(1)
    raise AssertionError(f"Control Plane did not converge: {statuses}")


def b64(raw):
    return base64.b64encode(raw).decode()


def record(writer, epoch, index):
    return {
        "request_id": str(uuid.uuid4()),
        "writer": writer,
        "session_epoch": epoch,
        "event_time_ns": str(1_700_000_000_000_000_000 + index),
        "key_base64": b64(f"diskfull-{index:06d}".encode()),
        "payload_base64": b64(f"payload-{index:06d}".encode()),
        "metadata_base64": {"source": b64(b"disk-full-acceptance")},
    }


def append_batch(writer, epoch, index, count, attempts=6):
    """Post one batch; ambiguous failures retry with identical request_ids
    (server-side dedup makes the retry safe). Returns the acknowledged key
    names â€” response `results[i]` pairs with request `records[i]` â€” or
    None when every attempt was refused."""
    records = [record(writer, epoch, index + i) for i in range(count)]
    assert len(records) == count
    for _ in range(attempts):
        status, body = post(
            ENDPOINTS[1], "/v1/writers/append-batch", {"records": records}
        )
        if status == 200:
            results = body.get("results", [])
            assert len(results) == len(records), body
            return {
                f"diskfull-{index + i:06d}" for i in range(count)
            }
        time.sleep(2)
    return None


def fetch_all(feed, deadline_s=120):
    """Replay the whole committed Feed; returns the set of record keys.
    A transient non-200 (owner moving off the filled Node) retries the
    whole pass â€” the committed-prefix contract is what the deadline
    bounds, not any single page."""
    deadline = time.monotonic() + deadline_s
    last_error = None
    while time.monotonic() < deadline:
        keys = set()
        cursor = ""
        failed = None
        while True:
            status, page = post(
                ENDPOINTS[0],
                "/v1/readers/temporary/fetch",
                {"feed": feed, "after": cursor or None, "limit": 1000, "wait_ms": 0},
            )
            if status != 200:
                failed = f"{status}: {page}"
                break
            page_records = page.get("records", [])
            if not page_records:
                return keys
            for item in page_records:
                keys.add(base64.b64decode(item["key_base64"]).decode())
            next_cursor = page.get("next_cursor")
            if not next_cursor or next_cursor == cursor:
                return keys
            cursor = next_cursor
        last_error = failed
        time.sleep(2)
    raise AssertionError(f"Feed never served a complete pass: {last_error}")


def fill_node3_disk():
    """dd past the tmpfs capacity â€” a real ENOSPC on the store's volume.
    node3 may crash mid-fill if its own raft log hits ENOSPC first; that
    is still the fault being exercised, so exec failures are tolerated and
    reported rather than asserted away."""
    code, out = compose(
        "exec", "-T", "node3",
        "sh", "-c",
        "dd if=/dev/zero of=/var/lib/whitewater/.diskfill bs=1M count=256 2>&1 || true",
        timeout=120,
    )
    if code != 0:
        print(f"node3 exec failed during fill (Node may have fenced itself): {out.strip()[:300]}")
        code, state = compose("ps", "--format", "json", timeout=30)
        print(f"compose ps: {state.strip()[:500]}")
        return
    code, out = compose(
        "exec", "-T", "node3", "df", "-k", "/var/lib/whitewater", timeout=30
    )
    if code == 0:
        print(f"node3 volume after fill: {out.strip().splitlines()[-1]}")
        available_kb = int(out.strip().splitlines()[-1].split()[3])
        assert available_kb < 8192, f"tmpfs is not full: {out}"


def main():
    # A cold Node image build far exceeds the default compose timeout on a
    # fresh CI runner; the bring-up is bounded by the build, not readiness.
    code, out = compose("up", "-d", "--build", timeout=1200)
    assert code == 0, out
    try:
        wait_ready()
        suffix = f"{int(time.time())}{uuid.uuid4().hex[:4]}"
        domain = f"df{suffix}"
        feed = f"{domain}.events"
        writer_name = f"dfw{suffix}"
        setup = wcl(
            f"CREATE DOMAIN {domain}; CREATE FEED {feed}; "
            f"CREATE WRITER {writer_name} TO {feed}; "
            f"OPEN WRITER SESSION {writer_name};"
        )
        epoch = setup["results"][3]["data"]["session_epoch"]

        # Phase 1 â€” healthy baseline: every append commits on all replicas.
        acknowledged = append_batch(writer_name, epoch, 0, BASELINE_RECORDS)
        assert acknowledged is not None, "baseline appends refused on a healthy cluster"
        assert len(acknowledged) == BASELINE_RECORDS

        # Phase 2 â€” fill node3's tmpfs with real bytes.
        fill_node3_disk()

        # Phase 3 â€” degraded writes: the two-Node quorum keeps committing;
        # any refusal is honest, but an acknowledged record may never vanish.
        # `sent` is every key ever offered; `acknowledged` only keys whose
        # batch returned 200 (refused batches may still appear in reads â€”
        # ambiguous commits are legal, silent loss is not). The quorum
        # recovers by moving range ownership off the full Node, which takes
        # seconds locally but can take tens of seconds on a loaded CI
        # runner â€” the contract is deadline-bounded, not attempt-bounded.
        sent = set(acknowledged)
        refused_batches = 0
        index = BASELINE_RECORDS
        degraded_deadline = time.monotonic() + DEGRADED_DEADLINE
        while time.monotonic() < degraded_deadline and len(acknowledged) == BASELINE_RECORDS:
            offered = {f"diskfull-{index + i:06d}" for i in range(10)}
            sent |= offered
            acked = append_batch(writer_name, epoch, index, 10)
            index += 10
            if acked is None:
                refused_batches += 1
            else:
                acknowledged |= acked
        print(
            f"degraded appends: {refused_batches} refused batches, "
            f"{len(acknowledged)} acknowledged records"
        )
        if len(acknowledged) <= BASELINE_RECORDS:
            code, out = compose("logs", "--tail", "3000")
            print(f"node logs at stall:\n{out[-60000:]}")
        assert len(acknowledged) > BASELINE_RECORDS, (
            f"a single full disk stalled all writes for "
            f"{DEGRADED_DEADLINE}s â€” the surviving quorum must keep the "
            f"Feed writable"
        )

        # Phase 4 â€” the committed prefix is uncorrupted: every acknowledged
        # record is readable, and reads only ever serve offered keys.
        keys = fetch_all(feed)
        assert acknowledged <= keys, (
            f"acknowledged records lost during disk-full: "
            f"{sorted(acknowledged - keys)[:5]}"
        )
        assert keys <= sent, f"reads served unoffered keys: {sorted(keys - sent)[:5]}"

        # Phase 5 â€” recovery: recreate node3 with a fresh tmpfs (disk
        # replaced); the Riverbed must heal without operator repair.
        code, out = compose("up", "-d", "--force-recreate", "node3")
        assert code == 0, out
        deadline = time.monotonic() + RECOVERY_DEADLINE
        recovered = False
        while time.monotonic() < deadline:
            status, _ = get(ENDPOINTS[2], "/health")
            if status == 200:
                try:
                    control_plane_converged(timeout=10)
                    recovered = True
                    break
                except AssertionError:
                    pass
            time.sleep(2)
        assert recovered, "node3 never rejoined a converged Control Plane"
        keys = fetch_all(feed)
        assert acknowledged <= keys, "acknowledged records lost during recovery"
        post_recovery = append_batch(writer_name, epoch, index, 5, attempts=12)
        assert post_recovery is not None, "appends still refused after node3 recovered"
        keys = fetch_all(feed)
        assert post_recovery <= keys, "post-recovery appends not readable"

        print("DISK FULL ACCEPTANCE PASSED")
    finally:
        if not os.environ.get("WW_KEEP_UP"):
            compose("down", "-v")


if __name__ == "__main__":
    main()
