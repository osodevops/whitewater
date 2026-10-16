"""Ad-hoc Feed throughput/latency benchmark for a disposable Riverbed.

Provisions `compose.verify.yml` as project `whitewater-bench` on the bench
port block (default 7771-7773, disjoint from every acceptance port block)
and measures:

  * write mode — N records appended through `--writers` concurrent Writer
    sessions; reports records/s, MiB/s and per-call latency percentiles.
  * read mode — the appended Feed is replayed through temporary fetch
    pages; reports records/s and MiB/s.

This is an operator/developer aid, not an acceptance gate: it never runs
as part of `run-acceptance.py` (the `bench-*.py` prefix stays outside its
`test-*.py` glob). Results print as a `BENCH_RESULT {json}` line sharing
one metric contract with `bench-kafka.py` and the Java `BenchFeed`
example so runs are comparable across systems:

    python scripts/bench-feed.py --records 20000 --bytes 1024 --writers 4 --batch 50
    BENCH_MODE=read python scripts/bench-feed.py

Environment knobs mirror the flags: BENCH_RECORDS, BENCH_BYTES,
BENCH_WRITERS, BENCH_BATCH, BENCH_MODE, BENCH_BASE_PORT, WW_KEEP_UP.
"""

import base64
import json
import os
import statistics
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid

COMPOSE = "compose.verify.yml"
PROJECT = "whitewater-bench"
BASE_PORT = int(os.environ.get("BENCH_BASE_PORT", "7771"))
SUBNET = os.environ.get("BENCH_SUBNET", "10.250.245.0/24")
ENDPOINTS = [f"http://localhost:{BASE_PORT + offset}" for offset in range(3)]
KEY = os.environ.get("WHITEWATER_ADMIN_API_KEY", "whitewater-local-development-admin-key")

# The default workload is a smoke-sized pass — the quorum append path
# currently lands a Control Plane write per record (see
# docs/performance-testing.md), so 2000 records already exercises minutes
# of sustained load. Raise BENCH_RECORDS for soak runs.
RECORDS = int(os.environ.get("BENCH_RECORDS", "2000"))
BYTES = int(os.environ.get("BENCH_BYTES", "1024"))
WRITERS = int(os.environ.get("BENCH_WRITERS", "4"))
BATCH = int(os.environ.get("BENCH_BATCH", "50"))
MODE = os.environ.get("BENCH_MODE", "all")
BATCH_ATTEMPTS = int(os.environ.get("BENCH_BATCH_ATTEMPTS", "8"))


def compose(*args):
    env = dict(os.environ)
    env["COMPOSE_PROJECT_NAME"] = PROJECT
    env["WHITEWATER_VERIFY_PORT_1"] = str(BASE_PORT)
    env["WHITEWATER_VERIFY_PORT_2"] = str(BASE_PORT + 1)
    env["WHITEWATER_VERIFY_PORT_3"] = str(BASE_PORT + 2)
    env["WHITEWATER_VERIFY_SUBNET"] = SUBNET
    result = subprocess.run(
        ["docker", "compose", "-f", COMPOSE, *args],
        capture_output=True,
        text=True,
        env=env,
    )
    return result.returncode, result.stdout + result.stderr


def post(endpoint, path, body, timeout=60):
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


def get(endpoint, path, timeout=15):
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
    for _ in range(30):
        status, body = post(ENDPOINTS[0], "/v1/admin/wcl", {"script": script})
        if status == 200:
            return body
        if status == 400:
            raise AssertionError(f"WCL rejected: {body}")
        time.sleep(1)
    raise AssertionError(f"WCL never succeeded: {status} {body}")


def wait_ready():
    for endpoint in ENDPOINTS:
        for _ in range(300):
            try:
                with urllib.request.urlopen(
                    urllib.request.Request(
                        f"{endpoint}/health",
                        headers={"authorization": f"Bearer {KEY}"},
                    ),
                    timeout=3,
                ) as response:
                    if response.status == 200:
                        break
            except Exception:
                pass
            time.sleep(1)
        else:
            raise AssertionError(f"{endpoint} never became healthy")
    # `/health` only proves the listener; appends also need a converged
    # Control Plane (leader present, applied index caught the log tail).
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        statuses = [get(endpoint, "/v1/admin/control-plane")[1] for endpoint in ENDPOINTS]
        if (
            all(status.get("leader_id") is not None for status in statuses)
            and all(
                status.get("last_applied_index") is not None
                and status["last_applied_index"] >= (status.get("last_log_index") or 0)
                for status in statuses
            )
        ):
            return
        time.sleep(1)
    raise AssertionError(f"Control Plane did not converge: {statuses}")


def percentile_stats(samples):
    """Per-call latency percentiles (ms) over the recorded durations."""
    if not samples:
        return {}
    ordered = sorted(samples)

    def pct(quantile):
        index = min(len(ordered) - 1, max(0, round(quantile * (len(ordered) - 1))))
        return ordered[index]

    return {
        "count": len(ordered),
        "min_ms": round(ordered[0] * 1000, 3),
        "p50_ms": round(pct(0.50) * 1000, 3),
        "p95_ms": round(pct(0.95) * 1000, 3),
        "p99_ms": round(pct(0.99) * 1000, 3),
        "max_ms": round(ordered[-1] * 1000, 3),
        "mean_ms": round(statistics.fmean(ordered) * 1000, 3),
    }


def bench_result(system, mode, records, payload_bytes, seconds, latencies, extra=None):
    """One JSON line; identical shape across whitewater/kafka/java drivers."""
    result = {
        "system": system,
        "mode": mode,
        "records": records,
        "bytes_per_record": payload_bytes,
        "seconds": round(seconds, 3),
        "records_per_sec": round(records / seconds, 1) if seconds > 0 else 0,
        "mib_per_sec": round(records * payload_bytes / seconds / 1024 / 1024, 2)
        if seconds > 0
        else 0,
    }
    result.update(percentile_stats(latencies))
    if extra:
        result.update(extra)
    return result


def report(result):
    print(f"BENCH_RESULT {json.dumps(result)}")
    latency = (
        f"p50={result['p50_ms']}ms p95={result['p95_ms']}ms p99={result['p99_ms']}ms"
        if "p50_ms" in result
        else ""
    )
    print(
        f"  {result['system']} {result['mode']}: "
        f"{result['records_per_sec']:,.0f} records/s, "
        f"{result['mib_per_sec']:,.2f} MiB/s {latency}"
    )


def b64(raw):
    return base64.b64encode(raw).decode()


class WriteDriver:
    """One thread per Writer — a Writer's record sequence is ordered, so
    concurrent producers must not share a sequence stream. Each worker owns
    a disjoint share of the requested record count and a disjoint keyspace."""

    def __init__(self, writers, total_records, payload_bytes, batch):
        self.writers = writers
        per_writer, extra = divmod(total_records, len(writers))
        self.shares = [
            per_writer + (1 if worker < extra else 0)
            for worker in range(len(writers))
        ]
        self.batch = max(1, batch)
        # Deterministic payload of the requested size.
        self.payload = b64(bytes(((index % 26) + 97) for index in range(payload_bytes)))
        self.errors = []
        self.latencies = []
        self.acknowledged = 0
        self.refused = 0
        self.lock = threading.Lock()

    def record(self, writer, epoch, index):
        return {
            "request_id": str(uuid.uuid4()),
            "writer": writer,
            "session_epoch": epoch,
            "event_time_ns": str(1_700_000_000_000_000_000 + index),
            "key_base64": b64(f"key-{index:012d}".encode()),
            "payload_base64": self.payload,
            "metadata_base64": {"source": b64(b"bench-feed")},
        }

    def _run(self, worker):
        writer, epoch = self.writers[worker]
        endpoint = ENDPOINTS[worker % len(ENDPOINTS)]
        sent = 0
        index = worker * sum(self.shares)
        while sent < self.shares[worker]:
            take = min(self.batch, self.shares[worker] - sent)
            # One attempt set shares request_ids, so a retry after an
            # ambiguous failure deduplicates server-side instead of
            # double-counting records.
            records = [self.record(writer, epoch, index + i) for i in range(take)]
            index += take
            sent += take
            for _ in range(BATCH_ATTEMPTS):
                started = time.monotonic()
                status, body = post(
                    endpoint,
                    "/v1/writers/append-batch" if take > 1 else "/v1/writers/append",
                    {"records": records} if take > 1 else records[0],
                )
                with self.lock:
                    self.latencies.append(time.monotonic() - started)
                if status == 200:
                    with self.lock:
                        self.acknowledged += take
                    break
                time.sleep(1)
            else:
                body["http_status"] = status
                self.errors.append(body)
                with self.lock:
                    self.refused += take

    def run(self):
        threads = [
            threading.Thread(target=self._run, args=(worker,), daemon=True)
            for worker in range(len(self.writers))
        ]
        started = time.monotonic()
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        return time.monotonic() - started


def run_write(feed, writers):
    driver = WriteDriver(writers, RECORDS, BYTES, BATCH)
    seconds = driver.run()
    result = bench_result(
        "whitewater", "write", driver.acknowledged, BYTES, seconds, driver.latencies,
        {"writers": len(writers), "batch": driver.batch,
         "requested": RECORDS, "refused": driver.refused,
         "errors": len(driver.errors)},
    )
    if driver.errors:
        print(f"  first append errors: {driver.errors[:2]}")
    return [result]


def run_read(feed):
    """Replay the whole Feed through temporary fetch pages until a short or
    empty page marks the tail."""
    endpoint = ENDPOINTS[0]
    cursor = ""
    records = 0
    payload_bytes = 0
    started = time.monotonic()
    while True:
        status, page = post(
            endpoint,
            "/v1/readers/temporary/fetch",
            {"feed": feed, "after": cursor or None, "limit": 1000, "wait_ms": 0},
        )
        assert status == 200, page
        page_records = page.get("records", [])
        if not page_records:
            break
        records += len(page_records)
        for record in page_records:
            payload_bytes += len(base64.b64decode(record.get("payload_base64", "")))
        next_cursor = page.get("next_cursor")
        # A short page can be a byte-budget cut rather than the tail; the
        # read only ends on an empty page or a cursor that stops advancing.
        if not next_cursor or next_cursor == cursor:
            break
        cursor = next_cursor
    seconds = time.monotonic() - started
    per_record = payload_bytes // records if records else BYTES
    return bench_result("whitewater", "read", records, per_record, seconds, [], {})


def main():
    code, out = compose("up", "-d", "--build")
    assert code == 0, out
    try:
        wait_ready()
        suffix = f"{int(time.time())}{uuid.uuid4().hex[:4]}"
        domain = f"bench{suffix}"
        feed = f"{domain}.events"
        statements = [f"CREATE DOMAIN {domain}", f"CREATE FEED {feed}"]
        writer_names = [f"benchw{suffix}w{i}" for i in range(WRITERS)]
        for name in writer_names:
            statements.append(f"CREATE WRITER {name} TO {feed}")
            statements.append(f"OPEN WRITER SESSION {name}")
        setup = wcl("; ".join(statements) + ";")
        writers = [
            (name, setup["results"][2 + 2 * i + 1]["data"]["session_epoch"])
            for i, name in enumerate(writer_names)
        ]

        results = []
        if MODE in ("write", "all"):
            results.extend(run_write(feed, writers))
            report(results[-1])
        if MODE in ("read", "all"):
            results.append(run_read(feed))
            report(results[-1])
        assert all(
            result["mode"] != "write" or result.get("errors", 0) == 0
            for result in results
        ), "append errors during benchmark"
        print("BENCH PASSED")
    finally:
        if not os.environ.get("WW_KEEP_UP"):
            compose("down", "-v")


if __name__ == "__main__":
    main()
